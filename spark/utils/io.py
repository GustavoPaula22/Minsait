# -*- coding: utf-8 -*-
# app/utils/io.py
"""
I/O utilitários do projeto:
- Leitura Kudu (NFe, CTe, BPe, NF3e, EFD)
- Leitura Oracle (auxiliares IPM), com pushdown e paralelismo
- Leitura/Escrita Iceberg nativa (+ MERGE por chave)
- Escrita Parquet (fallback/export)
- Retries com backoff para operações sensíveis (ex.: JDBC)

Observações:
- JARs correspondentes (Oracle JDBC, Iceberg runtime, Kudu) devem ser
  passados no spark-submit (--packages/--jars) conforme o ambiente.
- Não usa pandas e é seguro para execução em cluster (YARN).
"""

from __future__ import annotations

import os
import time
import uuid
from typing import Iterable, List, Optional, Dict, Tuple, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.settings import Settings


# =============================================================================
# Helpers
# =============================================================================
def _qualify_iceberg(settings: Settings, table: str, namespace: Optional[str] = None) -> str:
    """
    Monta o nome totalmente qualificado de uma tabela Iceberg.
    Ex.: tdp_catalog.ipm.ipm_documento_partct_calc_ipm
    """
    ns = namespace or settings.iceberg.namespace
    return f"{settings.iceberg.catalog}.{ns}.{table}"


def _full_kudu_name(settings: Settings, table_name: str, database: Optional[str] = None) -> str:
    """
    Constrói o identificador totalmente qualificado esperado pelo conector:
    impala::<db>.<table>
    """
    db = database or settings.kudu.database
    return f"impala::{db}.{table_name}"


def _list_from_csv(csv_or_list: Optional[Union[str, List[str]]]) -> List[str]:
    """
    Aceita string "a,b,c" ou lista ["a","b","c"] e retorna lista normalizada.
    """
    if not csv_or_list:
        return []
    if isinstance(csv_or_list, list):
        return [x.strip() for x in csv_or_list if str(x).strip()]
    if isinstance(csv_or_list, str):
        return [x.strip() for x in csv_or_list.split(",") if x.strip()]
    return []


def _sleep_backoff(attempt: int, base: float = 1.0, cap: float = 30.0) -> None:
    """Espera exponencial com jitter leve, evitando tempestade de retries."""
    delay = min(cap, base * (2 ** attempt)) * (1.0 + 0.1 * attempt)
    time.sleep(delay)


def with_retries(fn, max_attempts: int = 3, base_sleep: float = 1.0, what: str = "operation"):
    """
    Executa função com retries exponenciais. Re-lança a última exceção se falhar.
    """
    last_err = None
    for attempt in range(max_attempts):
        try:
            return fn()
        except Exception as e:
            last_err = e
            if attempt < max_attempts - 1:
                print(
                    f"[io.with_retries] {what} falhou (tentativa {attempt+1}/{max_attempts}). "
                    f"Tentando novamente... Erro: {e}"
                )
                _sleep_backoff(attempt, base_sleep)
            else:
                print(
                    f"[io.with_retries] {what} falhou definitivamente após {max_attempts} tentativas. Erro: {e}"
                )
    raise last_err  # type: ignore[misc]


# =============================================================================
# Leitura: Kudu (genérica)
# =============================================================================
def read_kudu_table(
    spark: SparkSession,
    settings: Settings,
    *,
    table: str,
    where: Optional[str] = None,
    columns: Optional[Sequence[str]] = None,
) -> DataFrame:
    """
    Leitura genérica de tabela Kudu via Spark.

    Resolução do nome da tabela:
    - Se table começa com "impala::" → usa exatamente o valor informado.
    - Senão, se table contém "."    → assume "schema.tabela" e prefixa "impala::".
    - Senão                         → usa settings.kudu.db como schema:
                                      "impala::{settings.kudu.db}.{table}".
    """

    # ----------------------------------------------------------------------
    # 1) Resolve master/masters da configuração
    # ----------------------------------------------------------------------
    kudu_cfg = settings.kudu

    kudu_master = (
        getattr(kudu_cfg, "master", None)
        or getattr(kudu_cfg, "masters", None)
        or getattr(kudu_cfg, "kudu_master", None)
    )
    if not kudu_master:
        raise ValueError(
            "Configuração do Kudu inválida: não encontrei atributos "
            "'master', 'masters' ou 'kudu_master' em settings.kudu."
        )

    # ----------------------------------------------------------------------
    # 2) Resolve nome físico da tabela
    # ----------------------------------------------------------------------
    kudu_table = table

    if kudu_table.startswith("impala::"):
        # Já está totalmente qualificado
        full_name = kudu_table
    else:
        if "." in kudu_table:
            # Já veio como "schema.tabela" (ex.: "bpe.bpe_infbpe")
            full_name = f"impala::{kudu_table}"
        else:
            # Só o nome → usa schema padrão do settings (ex.: "nfe")
            db = getattr(kudu_cfg, "db", None)
            if not db:
                raise ValueError(
                    "settings.kudu.db não definido e 'table' não possui schema "
                    "(ex.: 'bpe.bpe_infbpe')."
                )
            full_name = f"impala::{db}.{kudu_table}"

    options = {
        "kudu.master": kudu_master,
        "kudu.table": full_name,
    }

    df = spark.read.format("kudu").options(**options).load()

    if columns:
        df = df.select(*columns)
    if where:
        df = df.where(where)

    return df


# =============================================================================
# Leitura: Oracle JDBC
# =============================================================================
def _jdbc_url_from_dsn(dsn: str) -> str:
    """
    DSN aceitos:
    - "(DESCRIPTION=...)" -> jdbc:oracle:thin:@(DESCRIPTION=...)
    - "host:port/service" -> jdbc:oracle:thin:@//host:port/service
    - "host:port:sid"     -> jdbc:oracle:thin:@host:port:sid
    - "jdbc:oracle:thin:@..." -> retorna como está
    """
    if not dsn or not str(dsn).strip():
        raise ValueError("Oracle DSN vazio.")
    dsn = dsn.strip()
    if dsn.lower().startswith("jdbc:oracle:"):
        return dsn
    if dsn.startswith("("):
        return f"jdbc:oracle:thin:@{dsn}"
    if "/" in dsn and ":" in dsn:
        return f"jdbc:oracle:thin:@//{dsn}"
    return f"jdbc:oracle:thin:@{dsn}"


def _jdbc_options(settings: Settings) -> Dict[str, str]:
    """Opções JDBC comuns para Oracle."""
    # Suporta ORA_DSN (preferido) e ORA_DNS (legado) + fallbacks
    dsn_env = os.getenv("ORA_DSN") or os.getenv("ORA_DNS") or settings.oracle.dsn
    dsn = _jdbc_url_from_dsn(dsn_env)
    user = os.getenv("ORA_USER", settings.oracle.user)
    password = os.getenv("ORA_PASS", settings.oracle.password)
    return {
        "url": dsn,
        "user": user,
        "password": password,
        "driver": "oracle.jdbc.OracleDriver",
        "fetchsize": str(settings.oracle.fetchsize),
        "oracle.net.CONNECT_TIMEOUT": "15000",
        "oracle.jdbc.ReadTimeout": "120000",
        "oracle.net.TCP_KEEPALIVE": "true",
    }


def read_oracle_query(
    spark: SparkSession,
    settings: Settings,
    query_sql: str,
    *,
    columns: Optional[List[str]] = None,
    retries: int = 3,
) -> DataFrame:
    """Lê uma QUERY Oracle via JDBC, encapsulando como subselect."""
    opts = _jdbc_options(settings)
    sub = f"({query_sql}) QRY"

    def _do_read():
        reader = (
            spark.read
            .format("jdbc")
            .option("url", opts["url"])
            .option("user", opts["user"])
            .option("password", opts["password"])
            .option("driver", opts["driver"])
            .option("dbtable", sub)
            .option("fetchsize", opts["fetchsize"])
            .option("oracle.net.CONNECT_TIMEOUT", opts["oracle.net.CONNECT_TIMEOUT"])
            .option("oracle.jdbc.ReadTimeout", opts["oracle.jdbc.ReadTimeout"])
            .option("oracle.net.TCP_KEEPALIVE", opts["oracle.net.TCP_KEEPALIVE"])
            .option("pushDownPredicate", "true")
        )
        df0 = reader.load()
        if columns:
            return df0.select([F.col(c) for c in columns])
        return df0

    return with_retries(_do_read, max_attempts=retries, what="read_oracle_query")


def _read_oracle_with_opts(
    spark: SparkSession,
    *,
    user: str,
    password: str,
    dsn_chain: List[str],
    fetchsize: int,
    table: str,
    columns: Optional[List[str]] = None,
    where: Optional[str] = None,
    partition_column: Optional[str] = None,
    lower_bound: Optional[int] = None,
    upper_bound: Optional[int] = None,
    num_partitions: Optional[int] = None,
) -> DataFrame:
    """Tenta ler usando cadeia de DSNs (útil para failover/scan). Faz fallback em ORA-12514."""
    last_exc = None
    for dsn in dsn_chain:
        try:
            url = _jdbc_url_from_dsn(dsn)
            reader = (
                spark.read.format("jdbc")
                .option("url", url)
                .option("user", os.getenv("ORA_USER", user))
                .option("password", os.getenv("ORA_PASS", password))
                .option("driver", "oracle.jdbc.OracleDriver")
                .option("fetchsize", str(fetchsize))
                .option("oracle.net.CONNECT_TIMEOUT", "15000")
                .option("oracle.jdbc.ReadTimeout", "120000")
                .option("oracle.net.TCP_KEEPALIVE", "true")
            )

            if where:
                reader = (
                    reader.option("dbtable", f"(SELECT * FROM {table} WHERE {where}) T")
                    .option("pushDownPredicate", "true")
                )
            else:
                reader = reader.option("dbtable", table)

            if partition_column and (lower_bound is not None) and (upper_bound is not None) and (num_partitions or 0) > 0:
                reader = (
                    reader
                    .option("partitionColumn", partition_column)
                    .option("lowerBound", str(lower_bound))
                    .option("upperBound", str(upper_bound))
                    .option("numPartitions", str(num_partitions))
                )

            df = reader.load()
            if columns:
                keep = [c for c in columns if c in df.columns]
                if keep:
                    df = df.select(*keep)
            return df

        except Exception as e:
            msg = str(e)
            if "ORA-12514" in msg or "listener does not currently know of service" in msg:
                print(f"[io] DSN '{dsn}' rejeitado (ORA-12514). Tentando próximo DSN…")
                last_exc = e
                continue
            raise

    if last_exc:
        raise last_exc
    raise RuntimeError("Falha Oracle: nenhum DSN disponível.")


def read_oracle_table(*args, **kwargs) -> DataFrame:
    """
    Estilo A (recomendado):
        read_oracle_table(spark, settings, table, *, columns=None, where=None, ...)
    Estilo B (explícito):
        read_oracle_table(spark, table, *, user=..., password=..., dsn_primary=..., ...)
    Usa ENV ORA_DSN/ORA_DSN_FALLBACKS (preferidos) ou ORA_DNS/ORA_DNS_FALLBACKS (legado).
    """
    if len(args) < 2:
        raise TypeError(
            "uso: read_oracle_table(spark, settings, table, ...) ou read_oracle_table(spark, table, user=..., ...)"
        )

    spark = args[0]
    second = args[1]

    # -------------------------
    # Estilo B: (spark, "SCHEMA.TAB", user=..., ...)
    # -------------------------
    if isinstance(second, str):
        table = second
        user = kwargs.pop("user")
        password = kwargs.pop("password")
        dsn_primary = kwargs.pop("dsn_primary")
        dsn_fallbacks = kwargs.pop("dsn_fallbacks", [])
        fetchsize = int(kwargs.pop("fetchsize", 10000))

        # Preferir DSN; manter compat DNS (legado)
        dsn_env = os.getenv("ORA_DSN") or os.getenv("ORA_DNS")
        if dsn_env:
            dsn_primary = dsn_env

        env_fallbacks = [s.strip() for s in (os.getenv("ORA_DSN_FALLBACKS") or os.getenv("ORA_DNS_FALLBACKS") or "").split(",") if s.strip()]

        dsn_chain = [dsn_primary] + list(dsn_fallbacks or []) + env_fallbacks

        return _read_oracle_with_opts(
            spark,
            user=user,
            password=password,
            dsn_chain=dsn_chain,
            fetchsize=fetchsize,
            table=table,
            columns=kwargs.pop("columns", None),
            where=kwargs.pop("where", None),
            partition_column=kwargs.pop("partition_column", None),
            lower_bound=kwargs.pop("lower_bound", None),
            upper_bound=kwargs.pop("upper_bound", None),
            num_partitions=kwargs.pop("num_partitions", None),
        )

    # -------------------------
    # Estilo A: (spark, settings, "SCHEMA.TAB", ...)
    # -------------------------
    settings = second
    if len(args) < 3 and "table" not in kwargs:
        raise TypeError("falta o parâmetro 'table' no estilo (spark, settings, table, ...)")

    table = args[2] if len(args) >= 3 else kwargs.pop("table")

    cfg = getattr(settings, "oracle_ipm", None) or getattr(settings, "oracle", None)
    if cfg is None:
        raise ValueError("Settings sem seção 'oracle' ou 'oracle_ipm'.")

    user = os.getenv("ORA_USER", getattr(cfg, "user"))
    password = os.getenv("ORA_PASS", getattr(cfg, "password"))
    fetchsize = int(getattr(cfg, "fetchsize", 10000))

    dsn_primary = os.getenv("ORA_DSN") or os.getenv("ORA_DNS") or getattr(cfg, "dsn", "")
    dsn_fallbacks_cfg = list(getattr(cfg, "dsn_fallbacks", []))
    env_fallbacks = [s.strip() for s in (os.getenv("ORA_DSN_FALLBACKS") or os.getenv("ORA_DNS_FALLBACKS") or "").split(",") if s.strip()]
    dsn_chain = [dsn_primary] + dsn_fallbacks_cfg + env_fallbacks

    return _read_oracle_with_opts(
        spark,
        user=user,
        password=password,
        dsn_chain=dsn_chain,
        fetchsize=fetchsize,
        table=table,
        columns=kwargs.pop("columns", None),
        where=kwargs.pop("where", None),
        partition_column=kwargs.pop("partition_column", None),
        lower_bound=kwargs.pop("lower_bound", None),
        upper_bound=kwargs.pop("upper_bound", None),
        num_partitions=kwargs.pop("num_partitions", None),
    )


# =============================================================================
# Leitura: Iceberg
# =============================================================================
def read_iceberg_table(
    spark: SparkSession,
    settings: Settings,
    table: str,
    *,
    namespace: Optional[str] = None,
    columns: Optional[List[str]] = None,
    where: Optional[str] = None,
) -> DataFrame:
    """Lê uma tabela Iceberg nativamente."""
    full = _qualify_iceberg(settings, table, namespace)
    df = spark.read.table(full)
    if columns:
        df = df.select([F.col(c) for c in columns])
    if where:
        df = df.filter(where)
    return df


# =============================================================================
# Escrita: Iceberg / Parquet  (+ MERGE por chave em Iceberg)
# =============================================================================
def _quote_ident(name: str) -> str:
    return f"`{name}`"


def _merge_into_iceberg(
    spark: SparkSession,
    df: DataFrame,
    *,
    settings: Settings,
    target_table: str,
    key_columns: List[str],
    namespace: Optional[str] = None,
) -> None:
    """
    Executa MERGE INTO Iceberg usando key_columns como condição de junção.
    """
    fq = _qualify_iceberg(settings, target_table, namespace)
    tmp_view = f"tmp_{uuid.uuid4().hex}"

    # View temporária com os dados novos
    df.createOrReplaceTempView(tmp_view)

    cols = df.columns
    if not cols:
        spark.catalog.dropTempView(tmp_view)
        return

    # Condição de junção (chaves)
    on_expr = " AND ".join([f"t.{_quote_ident(k)} <=> s.{_quote_ident(k)}" for k in key_columns])

    # UPDATE SET
    set_updates = ", ".join([f"t.{_quote_ident(c)} = s.{_quote_ident(c)}" for c in cols])

    # INSERT
    col_list = ", ".join([_quote_ident(c) for c in cols])
    values_list = ", ".join([f"s.{_quote_ident(c)}" for c in cols])

    merge_sql = f"""
        MERGE INTO {fq} t
        USING {tmp_view} s
        ON {on_expr}
        WHEN MATCHED THEN UPDATE SET {set_updates}
        WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({values_list})
    """

    spark.sql(merge_sql)
    spark.catalog.dropTempView(tmp_view)


def write_iceberg(
    df: DataFrame,
    settings: Settings,
    table: str,
    *,
    namespace: Optional[str] = None,
    mode: str = "append",
    partition_by: Optional[Union[str, List[str]]] = None,
    key_columns: Optional[List[str]] = None,
    spark_session: Optional[SparkSession] = None,
) -> None:
    """
    Escreve DataFrame em Iceberg.
    - mode="append"/"overwrite": usa saveAsTable nativo
    - mode="merge" + key_columns: faz MERGE INTO por chave
    """
    full_name = _qualify_iceberg(settings, table, namespace)

    if mode.lower() == "merge":
        if not key_columns or not isinstance(key_columns, list) or not key_columns:
            raise ValueError("Para MERGE em Iceberg, informe key_columns=[...]")
        if spark_session is None:
            spark_session = SparkSession.getActiveSession()
            if spark_session is None:
                raise ValueError("SparkSession não disponível para MERGE.")

        # Garante existência da tabela alvo
        if not spark_session.catalog.tableExists(full_name):
            writer = df.limit(0).write.mode("append").format("iceberg")
            parts = _list_from_csv(partition_by)
            if parts:
                writer = writer.partitionBy(*parts)
            writer.saveAsTable(full_name)

        _merge_into_iceberg(
            spark_session,
            df,
            settings=settings,
            target_table=table,
            key_columns=key_columns,
            namespace=namespace,
        )
        return

    writer = df.write.mode(mode).format("iceberg")
    parts = _list_from_csv(partition_by)
    if parts:
        writer = writer.partitionBy(*parts)
    writer.saveAsTable(full_name)


def write_parquet(
    df: DataFrame,
    output: str,
    *,
    mode: str = "append",
    partition_by: Optional[Union[str, List[str]]] = None,
) -> None:
    """Escreve DataFrame em Parquet (ex.: export ou debug)."""
    writer = df.write.mode(mode).format("parquet")
    parts = _list_from_csv(partition_by)
    if parts:
        writer = writer.partitionBy(*parts)
    writer.save(output)


def write_df(
    df: DataFrame,
    *,
    # nomes compatíveis (novos e antigos)
    settings: Optional[Settings] = None,
    format: Optional[str] = None,          # novo
    output_format: Optional[str] = None,   # legado
    table: Optional[str] = None,           # novo
    table_name: Optional[str] = None,      # legado
    output: Optional[str] = None,
    mode: str = "append",
    partition_by: Optional[Union[str, List[str]]] = None,
    namespace: Optional[str] = None,
    key_columns: Optional[List[str]] = None,
    spark_session: Optional[SparkSession] = None,
) -> None:
    """
    Wrapper único para escrita:
    - (format|output_format)="iceberg": exige settings e (table|table_name)
    - (format|output_format)="parquet": exige output (path)
    - mode="merge" em Iceberg permite upsert com key_columns=[...]
    """
    fmt = (format or output_format or "iceberg").lower()
    tab = table or table_name

    if fmt == "iceberg":
        if not settings or not tab:
            raise ValueError("Para escrever em Iceberg, informe settings e table/table_name.")
        return write_iceberg(
            df,
            settings,
            tab,
            namespace=namespace,
            mode=mode,
            partition_by=partition_by,
            key_columns=key_columns,
            spark_session=spark_session,
        )

    if fmt == "parquet":
        if not output:
            raise ValueError("Para escrever em Parquet, informe output= caminho de destino.")
        return write_parquet(df, output, mode=mode, partition_by=partition_by)

    raise ValueError(f"Formato de saída não suportado: {fmt}")


# =============================================================================
# Conveniências específicas do projeto (Kudu) — NFe, CTe, BPe, NF3e, EFD
# =============================================================================
# ---- NFe
def read_kudu_nfe_ident(
    spark: SparkSession,
    settings: Settings,
    *,
    columns: Optional[List[str]] = None,
    where: Optional[Union[str, List[Tuple[str, str, object]]]] = None,
) -> DataFrame:
    return read_kudu_table(
        spark=spark,
        settings=settings,
        table=settings.kudu.nfe_ident_table,
        database=settings.kudu.database,
        columns=columns,
        where=where,
    )


def read_kudu_nfe_itens(
    spark: SparkSession,
    settings: Settings,
    *,
    columns: Optional[List[str]] = None,
    where: Optional[Union[str, List[Tuple[str, str, object]]]] = None,
) -> DataFrame:
    return read_kudu_table(
        spark=spark,
        settings=settings,
        table=settings.kudu.nfe_item_table,
        database=settings.kudu.database,
        columns=columns,
        where=where,
    )


# ---- CTe
def read_kudu_cte_ident(
    spark: SparkSession,
    settings: Settings,
    *,
    columns: Optional[List[str]] = None,
    where: Optional[Union[str, List[Tuple[str, str, object]]]] = None,
) -> DataFrame:
    table = getattr(settings.kudu, "cte_ident_table", None) or "cte_b_identificacao"
    return read_kudu_table(
        spark=spark,
        settings=settings,
        table=table,
        database=settings.kudu.database,
        columns=columns,
        where=where,
    )


def read_kudu_cte_itens(
    spark: SparkSession,
    settings: Settings,
    *,
    columns: Optional[List[str]] = None,
    where: Optional[Union[str, List[Tuple[str, str, object]]]] = None,
) -> DataFrame:
    table = getattr(settings.kudu, "cte_item_table", None) or "cte_i_itens"
    return read_kudu_table(
        spark=spark,
        settings=settings,
        table=table,
        database=settings.kudu.database,
        columns=columns,
        where=where,
    )


# ---- BPe
def read_kudu_bpe_ident(
    spark: SparkSession,
    settings: Settings,
    *,
    columns: Optional[List[str]] = None,
    where: Optional[Union[str, List[Tuple[str, str, object]]]] = None,
) -> DataFrame:
    table = getattr(settings.kudu, "bpe_ident_table", None) or "bpe_b_identificacao"
    return read_kudu_table(
        spark=spark,
        settings=settings,
        table=table,
        database=settings.kudu.database,
        columns=columns,
        where=where,
    )


def read_kudu_bpe_itens(
    spark: SparkSession,
    settings: Settings,
    *,
    columns: Optional[List[str]] = None,
    where: Optional[Union[str, List[Tuple[str, str, object]]]] = None,
) -> DataFrame:
    table = getattr(settings.kudu, "bpe_item_table", None) or "bpe_i_itens"
    return read_kudu_table(
        spark=spark,
        settings=settings,
        table=table,
        database=settings.kudu.database,
        columns=columns,
        where=where,
    )


# ---- NF3e
def read_kudu_nf3e_ident(
    spark: SparkSession,
    settings: Settings,
    *,
    columns: Optional[List[str]] = None,
    where: Optional[Union[str, List[Tuple[str, str, object]]]] = None,
) -> DataFrame:
    table = getattr(settings.kudu, "nf3e_ident_table", None) or "nf3e_b_identificacao"
    return read_kudu_table(
        spark=spark,
        settings=settings,
        table=table,
        database=settings.kudu.database,
        columns=columns,
        where=where,
    )


def read_kudu_nf3e_itens(
    spark: SparkSession,
    settings: Settings,
    *,
    columns: Optional[List[str]] = None,
    where: Optional[Union[str, List[Tuple[str, str, object]]]] = None,
) -> DataFrame:
    table = getattr(settings.kudu, "nf3e_item_table", None) or "nf3e_i_itens"
    return read_kudu_table(
        spark=spark,
        settings=settings,
        table=table,
        database=settings.kudu.database,
        columns=columns,
        where=where,
    )


# ---- EFD (C100/C170)
def read_kudu_efd_c100(
    spark: SparkSession,
    settings: Settings,
    *,
    columns: Optional[List[str]] = None,
    where: Optional[Union[str, List[Tuple[str, str, object]]]] = None,
) -> DataFrame:
    table = getattr(settings.kudu, "efd_c100_table", None) or os.getenv("EFD_C100_TABLE", "efd_c100")
    db = getattr(settings.kudu, "efd_database", None) or settings.kudu.database or os.getenv("EFD_KUDU_DB", "kudu")
    return read_kudu_table(
        spark=spark,
        settings=settings,
        table=table,
        database=db,
        columns=columns,
        where=where,
    )


def read_kudu_efd_c170(
    spark: SparkSession,
    settings: Settings,
    *,
    columns: Optional[List[str]] = None,
    where: Optional[Union[str, List[Tuple[str, str, object]]]] = None,
) -> DataFrame:
    table = getattr(settings.kudu, "efd_c170_table", None) or os.getenv("EFD_C170_TABLE", "efd_c170")
    db = getattr(settings.kudu, "efd_database", None) or settings.kudu.database or os.getenv("EFD_KUDU_DB", "kudu")
    return read_kudu_table(
        spark=spark,
        settings=settings,
        table=table,
        database=db,
        columns=columns,
        where=where,
    )


# =============================================================================
# Conveniências específicas do projeto (Oracle / IPM)
# =============================================================================
def _safe_read_oracle_table(
    spark: SparkSession,
    settings: Settings,
    table_name: Optional[str],
    *,
    required: bool = False,
    what: Optional[str] = None,
    **reader_kwargs,
) -> Optional[DataFrame]:
    """Leitura tolerante: retorna None quando 'table_name' não existir/for vazio, exceto se required=True."""
    if not table_name:
        return None

    label = what or f"read_oracle_table({table_name})"
    try:
        return read_oracle_table(spark, settings, table_name, **reader_kwargs)
    except Exception as e:
        if required:
            print(f"[io.oracle] ERRO ao ler {table_name} (obrigatória).")
            raise
        print(f"[io.oracle] Aviso: não foi possível ler {table_name}. Pulando. Erro: {e}")
        return None


def read_oracle_ipm_auxiliares(
    spark: SparkSession,
    settings: Settings,
) -> Dict[str, Optional[DataFrame]]:
    """
    Lê as tabelas auxiliares necessárias às regras de negócio (Oracle IPM).
    Retorna dicionário com DFs. Parâmetros são obrigatórios; demais são opcionais.
    """
    o = getattr(settings, "oracle_ipm", None) or settings.oracle

    params_df = _safe_read_oracle_table(
        spark, settings, getattr(o, "tbl_parametros", None), required=True, what="parametros"
    )
    cfop_df = _safe_read_oracle_table(
        spark, settings, getattr(o, "tbl_cfop_participante", None), what="cfop_participante"
    )
    evento_df = _safe_read_oracle_table(
        spark, settings, getattr(o, "tbl_evento", None), what="evento"
    )
    ncm_df = _safe_read_oracle_table(
        spark, settings, getattr(o, "tbl_ncm_participante", None), what="ncm_participante"
    )
    mun_df = _safe_read_oracle_table(
        spark, settings, getattr(o, "tbl_municipios", None), what="municipios"
    )
    doc_ref = _safe_read_oracle_table(
        spark, settings, getattr(o, "tbl_documento", None), what="documento_ref"
    )

    out: Dict[str, Optional[DataFrame]] = {
        "params": params_df,
        "cfop": cfop_df,
        "evento": evento_df,
        "ncm": ncm_df,
        "mun": mun_df,
        "doc_ref": doc_ref,
    }

    # aliases p/ compatibilidade
    out["parametros"] = params_df
    out["cfop_participante"] = cfop_df
    out["ncm_participante"] = ncm_df
    out["municipios"] = mun_df
    out["documento"] = doc_ref

    return out
