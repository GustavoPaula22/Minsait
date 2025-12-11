# -*- coding: utf-8 -*-
# app/efd.py
from __future__ import annotations

import argparse
import json
import os
from typing import Dict, Optional, Tuple, List

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException

from app.settings import Settings, load_settings_from_env, build_spark, print_settings
from app.utils.io import write_df, read_kudu_table
from app.utils.rules import (
    build_rules_context,
    apply_rules_to_items,
    apply_rules_to_documents,
)

# =============================================================================
# Defaults e variáveis de ambiente
# =============================================================================

# DB/schema padrão no Kudu para EFD
_DEFAULT_KUDU_DB = os.getenv("EFD_KUDU_DB", "efd")

# Tabelas Kudu – mapeando bloco C100/C170
_DEFAULT_TB_C100 = os.getenv("EFD_C100_TABLE", "blcc_regc100")               # documentos (C100)
_DEFAULT_TB_C170 = os.getenv("EFD_C170_TABLE", "blcc_regc100_regc170")      # itens (C170)

# Desabilitar E100/E110 por padrão (pode ligar via env/arg se um dia existir)
_DEFAULT_TB_E100 = os.getenv("EFD_E100_TABLE", "")  # períodos (opcional)
_DEFAULT_TB_E110 = os.getenv("EFD_E110_TABLE", "")  # apurações (opcional)

# Colunas de pushdown (epoch seconds)
# C100 tem dt_doc_nums (epoch em segundos)
_DEFAULT_TS_DOC = os.getenv("EFD_TS_DOC_COL", "dt_doc_nums")   # C100
# Para C170 vamos, por padrão, NÃO aplicar pushdown de data (fica vazio)
_DEFAULT_TS_ITEM = os.getenv("EFD_TS_ITEM_COL", "")            # C170 (sem filtro por default)

# Chave canônica (sintética) do documento EFD
_DEFAULT_KEY_COL = os.getenv("EFD_KEY_COL", "doc_id")

# Tabelas Iceberg de saída (se vazias, usa Settings.iceberg.*)
_DEFAULT_OUT_DOC = os.getenv("EFD_OUT_DOC_TABLE", "")
_DEFAULT_OUT_ITEM = os.getenv("EFD_OUT_ITEM_TABLE", "")

# Particionamento opcional na escrita
_DEFAULT_REPARTITION_DOCS = int(os.getenv("EFD_REPARTITION_DOCS", "0"))  # 0 = não muda
_DEFAULT_REPARTITION_ITEMS = int(os.getenv("EFD_REPARTITION_ITEMS", "0"))

# =============================================================================
# Helpers
# =============================================================================


def _normalize_table_name(n: str, *, db: Optional[str]) -> str:
    """Permite passar 'tb' ou 'db.tb'; retorna 'impala::db.tb' para o leitor Kudu."""
    if "." not in n and db:
        n = f"{db}.{n}"
    return f"impala::{n}"


def _mk_between_ts(where_col: str, data_inicio: str, data_fim: str) -> str:
    """
    Filtro BETWEEN para colunas numéricas/epoch (caso C100.dt_doc_nums).

    Gera, por exemplo:
      dt_doc_nums BETWEEN unix_timestamp('2024-01-01 00:00:00')
                     AND unix_timestamp('2024-01-01 23:59:59')
    """
    di = f"{data_inicio.strip()} 00:00:00"
    df = f"{data_fim.strip()} 23:59:59"
    return (
        f"{where_col} BETWEEN unix_timestamp('{di}') "
        f"AND unix_timestamp('{df}')"
    )


def _safe_count(df: Optional[DataFrame], what: str) -> int:
    if df is None:
        return 0
    try:
        return int(df.count())
    except Exception as e:  # proteção
        print(f"[efd][_safe_count] Falha ao contar {what}: {e}")
        return -1


def _norm_str(col: Column) -> Column:
    return F.when(col.isNotNull(), F.trim(col.cast("string"))).otherwise(col)


def _parse_efd_date(col_name: str = "DT_DOC") -> Column:
    """
    Faz parse de datas da EFD que podem vir como:
      - 'yyyy-MM-dd'
      - 'ddMMyyyy'   (ex.: 29122021)
      - 'yyyyMMdd'   (ex.: 20211229)
      - 'dd/MM/yyyy' (ex.: 29/12/2021)
    """
    c = F.col(col_name)
    return F.coalesce(
        F.to_date(c, "yyyy-MM-dd"),
        F.to_date(c, "ddMMyyyy"),
        F.to_date(c, "yyyyMMdd"),
        F.to_date(c, "dd/MM/yyyy"),
    )


def _mk_id_ano_mes_from_date(col_date: str = "DT_DOC") -> Tuple[Column, Column]:
    """Gera (id_ano_lanc, id_mes) a partir de uma coluna de data da EFD."""
    d = _parse_efd_date(col_date)
    return F.year(d).alias("id_ano_lanc"), F.month(d).alias("id_mes")


def _normalize_c100_columns(df: DataFrame) -> DataFrame:
    """
    Normaliza nomes de colunas do C100 para o padrão usado no pipeline:
      - cnpj            -> CNPJ
      - cpf             -> CPF
      - cod_mod         -> COD_MOD
      - ser             -> SER
      - num_doc         -> NUM_DOC
      - dt_doc          -> DT_DOC
      - dt_e_s          -> DT_E_S
      - vl_doc          -> VL_DOC

    Além disso, cria aliases CNPJ/CPF a partir de reg0000_cnpj / reg0000_cpf,
    que são as colunas presentes no layout do Kudu (blcc_regc100).
    """
    cols = set(df.columns)

    def rename_if_exists(src: str, tgt: str) -> None:
        nonlocal df, cols
        if src in cols and tgt not in cols:
            df = df.withColumnRenamed(src, tgt)
            cols.remove(src)
            cols.add(tgt)

    # Renomes diretos (caso venham em minúsculo)
    rename_if_exists("cnpj", "CNPJ")
    rename_if_exists("cpf", "CPF")
    rename_if_exists("cod_mod", "COD_MOD")
    rename_if_exists("ser", "SER")
    rename_if_exists("num_doc", "NUM_DOC")
    rename_if_exists("dt_doc", "DT_DOC")
    rename_if_exists("dt_e_s", "DT_E_S")
    rename_if_exists("vl_doc", "VL_DOC")

    # Aliases a partir das colunas do registro 0000 (layout real do Kudu)
    if "reg0000_cnpj" in cols and "CNPJ" not in cols:
        df = df.withColumn("CNPJ", F.col("reg0000_cnpj"))
        cols.add("CNPJ")

    if "reg0000_cpf" in cols and "CPF" not in cols:
        df = df.withColumn("CPF", F.col("reg0000_cpf"))
        cols.add("CPF")

    return df


def _normalize_c170_columns(df: DataFrame) -> DataFrame:
    """
    Normaliza nomes de colunas do C170 para o padrão usado no pipeline e
    cria colunas "espelho" para os dados do C100:

      - num_item -> NUM_ITEM
      - cfop     -> CFOP
      - qtd      -> QTD
      - vl_item  -> VL_ITEM

      - COD_MOD  <- regc100_cod_mod
      - SER      <- regc100_ser
      - NUM_DOC  <- regc100_num_doc
      - DT_DOC   <- regc100_dt_doc

      - CNPJ     <- reg0000_cnpj (se existir)
      - CPF      <- reg0000_cpf  (se existir)
    """
    cols = set(df.columns)

    def rename_if_exists(src: str, tgt: str) -> None:
        nonlocal df, cols
        if src in cols and tgt not in cols:
            df = df.withColumnRenamed(src, tgt)
            cols.remove(src)
            cols.add(tgt)

    # Normalização básica de itens
    rename_if_exists("num_item", "NUM_ITEM")
    rename_if_exists("cfop", "CFOP")
    rename_if_exists("qtd", "QTD")
    rename_if_exists("vl_item", "VL_ITEM")

    # Espelhar campos de documento (C100) com base nos regc100_*
    if "regc100_cod_mod" in cols and "COD_MOD" not in cols:
        df = df.withColumn("COD_MOD", F.col("regc100_cod_mod"))
        cols.add("COD_MOD")

    if "regc100_ser" in cols and "SER" not in cols:
        df = df.withColumn("SER", F.col("regc100_ser"))
        cols.add("SER")

    if "regc100_num_doc" in cols and "NUM_DOC" not in cols:
        df = df.withColumn("NUM_DOC", F.col("regc100_num_doc"))
        cols.add("NUM_DOC")

    if "regc100_dt_doc" in cols and "DT_DOC" not in cols:
        df = df.withColumn("DT_DOC", F.col("regc100_dt_doc"))
        cols.add("DT_DOC")

    # CNPJ/CPF vindos do 0000 (cabeçalho)
    if "reg0000_cnpj" in cols and "CNPJ" not in cols:
        df = df.withColumn("CNPJ", F.col("reg0000_cnpj"))
        cols.add("CNPJ")

    if "reg0000_cpf" in cols and "CPF" not in cols:
        df = df.withColumn("CPF", F.col("reg0000_cpf"))
        cols.add("CPF")

    return df


def _build_doc_id(
    df_c100: DataFrame,
    *,
    key_col: str,
) -> DataFrame:
    """
    Chave canônica 'doc_id' (EFD) a partir de C100:
      - CNPJ/CPF (prioriza CNPJ; fallback CPF; incluindo reg0000_cnpj/cpf)
      - COD_MOD
      - SER
      - NUM_DOC
      - DT_DOC (yyyy-MM-dd)
    """
    cols = set(df_c100.columns)

    # Monta expressão para CNPJ/CPF com todos os candidatos disponíveis
    cnpj_cpf_exprs: List[Column] = []
    if "CNPJ" in cols:
        cnpj_cpf_exprs.append(_norm_str(F.col("CNPJ")))
    if "CPF" in cols:
        cnpj_cpf_exprs.append(_norm_str(F.col("CPF")))
    if "reg0000_cnpj" in cols:
        cnpj_cpf_exprs.append(_norm_str(F.col("reg0000_cnpj")))
    if "reg0000_cpf" in cols:
        cnpj_cpf_exprs.append(_norm_str(F.col("reg0000_cpf")))

    if cnpj_cpf_exprs:
        cnpj_cpf_norm_col = F.coalesce(*cnpj_cpf_exprs)
    else:
        cnpj_cpf_norm_col = F.lit(None).cast("string")

    df = (
        df_c100
        .withColumn("cnpj_cpf_norm", cnpj_cpf_norm_col)
        .withColumn("cod_mod_norm", _norm_str(F.col("COD_MOD")))
        .withColumn("ser_norm", _norm_str(F.col("SER")))
        .withColumn("num_doc_norm", _norm_str(F.col("NUM_DOC")))
        .withColumn(
            "dt_doc_norm",
            F.date_format(_parse_efd_date("DT_DOC"), "yyyy-MM-dd"),
        )
        .withColumn(
            key_col,
            F.concat_ws(
                "|",
                F.coalesce(F.col("cnpj_cpf_norm"), F.lit("")),
                F.coalesce(F.col("cod_mod_norm"), F.lit("")),
                F.coalesce(F.col("ser_norm"), F.lit("")),
                F.coalesce(F.col("num_doc_norm"), F.lit("")),
                F.coalesce(F.col("dt_doc_norm"), F.lit("")),
            ),
        )
        .drop(
            "cnpj_cpf_norm",
            "cod_mod_norm",
            "ser_norm",
            "num_doc_norm",
            "dt_doc_norm",
        )
    )

    # Garante colunas de partição padrão (id_ano_lanc, id_mes)
    id_ano, id_mes = _mk_id_ano_mes_from_date("DT_DOC")
    df = df.withColumn("id_ano_lanc", id_ano).withColumn("id_mes", id_mes)
    return df


def _prepare_efd_items_for_rules(df: DataFrame) -> DataFrame:
    """
    Adapta o layout de itens da EFD (C170) para o que as regras esperam
    (layout NFe), criando aliases:

      - vprod   <- VL_ITEM / vl_item
      - vdesc   <- VL_DESC / vl_desc (ou 0 se não existir)
      - qcom    <- QTD / qtd
      - vfrete  <- 0 (ou coluna equivalente, se existir)
      - vseg    <- 0 (ou coluna equivalente, se existir)
      - voutro  <- 0 (ou coluna equivalente, se existir)
      - ipi_vipi <- vl_ipi / VL_IPI (valor de IPI do item, ou 0 se não existir)
      - valor_item <- combinação de vprod, vdesc, vfrete, vseg, voutro
                      (valor líquido base para as regras)
    """
    cols = set(df.columns)

    def has(col_name: str) -> bool:
        return col_name in cols

    # vprod: valor do item
    if "vprod" not in cols:
        src = None
        if has("VL_ITEM"):
            src = "VL_ITEM"
        elif has("vl_item"):
            src = "vl_item"

        if src is not None:
            df = df.withColumn("vprod", F.col(src).cast(T.DecimalType(17, 2)))
        else:
            df = df.withColumn("vprod", F.lit(0).cast(T.DecimalType(17, 2)))
        cols.add("vprod")

    # vdesc: desconto do item
    if "vdesc" not in cols:
        src = None
        if has("VL_DESC"):
            src = "VL_DESC"
        elif has("vl_desc"):
            src = "vl_desc"

        if src is not None:
            df = df.withColumn("vdesc", F.col(src).cast(T.DecimalType(17, 2)))
        else:
            df = df.withColumn("vdesc", F.lit(0).cast(T.DecimalType(17, 2)))
        cols.add("vdesc")

    # qcom: quantidade comercial
    if "qcom" not in cols:
        src = None
        if has("QTD"):
            src = "QTD"
        elif has("qtd"):
            src = "qtd"

        if src is not None:
            df = df.withColumn("qcom", F.col(src).cast(T.DecimalType(17, 3)))
        else:
            df = df.withColumn("qcom", F.lit(0).cast(T.DecimalType(17, 3)))
        cols.add("qcom")

    # vfrete: frete do item (EFD não tem nativamente; criamos zero)
    if "vfrete" not in cols:
        df = df.withColumn("vfrete", F.lit(0).cast(T.DecimalType(17, 2)))
        cols.add("vfrete")

    # vseg: seguro do item
    if "vseg" not in cols:
        df = df.withColumn("vseg", F.lit(0).cast(T.DecimalType(17, 2)))
        cols.add("vseg")

    # voutro: outros valores do item
    if "voutro" not in cols:
        df = df.withColumn("voutro", F.lit(0).cast(T.DecimalType(17, 2)))
        cols.add("voutro")

    # ipi_vipi: valor de IPI do item – alias de vl_ipi / VL_IPI
    if "ipi_vipi" not in cols:
        src = None
        if has("vl_ipi"):
            src = "vl_ipi"
        elif has("VL_IPI"):
            src = "VL_IPI"

        if src is not None:
            df = df.withColumn("ipi_vipi", F.col(src).cast(T.DecimalType(17, 2)))
        else:
            df = df.withColumn("ipi_vipi", F.lit(0).cast(T.DecimalType(17, 2)))
        cols.add("ipi_vipi")

    # valor_item: valor líquido "base" usado pelas regras
    if "valor_item" not in cols:
        zero_dec = F.lit(0).cast(T.DecimalType(17, 2))
        df = df.withColumn(
            "valor_item",
            (
                F.coalesce(F.col("vprod"), zero_dec)
                - F.coalesce(F.col("vdesc"), zero_dec)
                + F.coalesce(F.col("vfrete"), zero_dec)
                + F.coalesce(F.col("vseg"), zero_dec)
                + F.coalesce(F.col("voutro"), zero_dec)
            ).cast(T.DecimalType(17, 2)),
        )
        cols.add("valor_item")

    return df


# =============================================================================
# Leitura Kudu (C100 / C170 / E100 / E110)
# =============================================================================


def _read_efd_c100(
    spark: SparkSession,
    settings: Settings,
    *,
    kudu_db: str,
    tb_c100: str,
    ts_col: str,
    data_inicio: str,
    data_fim: str,
) -> DataFrame:
    # C100: filtro por dt_doc_nums (epoch)
    where_sql = None
    if ts_col:
        where_sql = _mk_between_ts(ts_col, data_inicio, data_fim)

    df = read_kudu_table(
        spark,
        settings,
        table=_normalize_table_name(tb_c100, db=kudu_db),
        where=where_sql,
        columns=None,
    )
    return _normalize_c100_columns(df)


def _read_efd_c170(
    spark: SparkSession,
    settings: Settings,
    *,
    kudu_db: str,
    tb_c170: str,
    ts_col: str,
    data_inicio: str,
    data_fim: str,
) -> DataFrame:
    """
    Leitura de C170 (itens).

    Por padrão (ts_col vazio) NÃO aplicamos filtro de data aqui, pois:
      - a coluna dt_doc_nums não existe em blcc_regc100_regc170;
      - a data do documento está em regc100_dt_doc;
      - o filtro efetivo virá do join com C100.
    """
    where_sql = None
    if ts_col:
        where_sql = _mk_between_ts(ts_col, data_inicio, data_fim)

    df = read_kudu_table(
        spark,
        settings,
        table=_normalize_table_name(tb_c170, db=kudu_db),
        where=where_sql,
        columns=None,
    )
    return _normalize_c170_columns(df)


def _read_efd_e100(
    spark: SparkSession,
    settings: Settings,
    *,
    kudu_db: str,
    tb_e100: Optional[str],
    data_inicio: str,
    data_fim: str,
) -> Optional[DataFrame]:
    if not tb_e100:
        return None
    where_sql = (
        "( (unix_timestamp(DT_INI) <= unix_timestamp('{df}')) "
        "AND (unix_timestamp(DT_FIN) >= unix_timestamp('{di}')) )"
    ).format(di=data_inicio, df=data_fim)
    return read_kudu_table(
        spark,
        settings,
        table=_normalize_table_name(tb_e100, db=kudu_db),
        where=where_sql,
        columns=None,
    )


def _read_efd_e110(
    spark: SparkSession,
    settings: Settings,
    *,
    kudu_db: str,
    tb_e110: Optional[str],
    data_inicio: str,
    data_fim: str,
) -> Optional[DataFrame]:
    if not tb_e110:
        return None
    where_sql = (
        "( (unix_timestamp(DT_INI) <= unix_timestamp('{df}')) "
        "AND (unix_timestamp(DT_FIN) >= unix_timestamp('{di}')) )"
    ).format(di=data_inicio, df=data_fim)
    return read_kudu_table(
        spark,
        settings,
        table=_normalize_table_name(tb_e110, db=kudu_db),
        where=where_sql,
        columns=None,
    )


# =============================================================================
# Enriquecimento E100/E110 (opcional)
# =============================================================================


def _enrich_with_periodo(
    df_docs_base: DataFrame,
    df_e100: Optional[DataFrame],
    df_e110: Optional[DataFrame],
) -> DataFrame:
    """
    Integra informações de período/apuração para fins de partição e agregações.
    Mantém cardinalidade 1:1 por documento; usa marcação simples de pertencimento ao período.
    """
    out = df_docs_base

    if df_e100 is not None and {"DT_INI", "DT_FIN"}.issubset(set(df_e100.columns)):
        e100 = df_e100.select(
            F.to_date("DT_INI").alias("e100_dt_ini"),
            F.to_date("DT_FIN").alias("e100_dt_fin"),
        ).dropDuplicates()

        out = (
            out.join(
                e100,
                (_parse_efd_date("DT_DOC") >= F.col("e100_dt_ini"))
                & (_parse_efd_date("DT_DOC") <= F.col("e100_dt_fin")),
                how="left",
            )
            .withColumn(
                "pertem_e100",
                F.when(F.col("e100_dt_ini").isNotNull(), F.lit(1)).otherwise(F.lit(0)),
            )
        )

    _ = df_e110  # hook futuro
    return out


# =============================================================================
# Agregação de itens -> documento
# =============================================================================


def _aggregate_items_to_document(
    df_docs: DataFrame,
    df_items_rules: Optional[DataFrame],
    *,
    key_col: str,
) -> DataFrame:
    """
    Agrega dos ITENS (já com regras aplicadas) para o DOCUMENTO:
      - valr_adicionado_operacao: soma dos valr_adicionado por doc
      - indi_aprop: 'S' se houver ao menos 1 item participante por CFOP; senão 'N'
      - codg_tipo_doc_partct: máximo não nulo de codg_tipo_doc_partct_item
    """
    if df_items_rules is None:
        return (
            df_docs
            .withColumn("indi_aprop", F.lit(None).cast("string"))
            .withColumn(
                "valr_adicionado_operacao",
                F.lit(None).cast(T.DecimalType(17, 2)),
            )
            .withColumn("codg_tipo_doc_partct", F.lit(None).cast("int"))
        )

    grp = (
        df_items_rules
        .groupBy(key_col)
        .agg(
            F.sum(
                F.coalesce(
                    F.col("valr_adicionado").cast(T.DecimalType(17, 2)),
                    F.lit(0),
                )
            ).alias("valr_adicionado_operacao"),
            F.max(
                F.when(
                    F.col("is_cfop_participante") == F.lit(True),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("has_part_item"),
            F.max(
                F.coalesce(
                    F.col("codg_tipo_doc_partct_item").cast("int"),
                    F.lit(0),
                )
            ).alias("codg_tipo_doc_partct"),
        )
    )

    out = (
        df_docs.join(grp, on=key_col, how="left")
        .withColumn(
            "indi_aprop",
            F.when(F.col("has_part_item") == 1, F.lit("S")).otherwise(F.lit("N")),
        )
        .drop("has_part_item")
    )
    return out


# =============================================================================
# Alinhamento com schema do Iceberg
# =============================================================================


def _align_with_iceberg_table_schema(
    spark: SparkSession,
    table_name: str,
    df: DataFrame,
) -> DataFrame:
    """
    Alinha o DataFrame com o schema da tabela Iceberg:
      - Garante que todas as colunas da tabela existam no DF.
      - Faz cast explícito para o tipo da tabela.
      - Reordena as colunas na mesma ordem do Iceberg.
    """
    try:
        target_schema = spark.table(table_name).schema
    except AnalysisException:
        # Se a tabela ainda não existe (ex.: ambiente novo), apenas devolve o DF.
        return df

    cur = df
    for f in target_schema.fields:
        tgt = f.name
        if tgt not in cur.columns:
            # Cria coluna nula com o tipo correto
            cur = cur.withColumn(tgt, F.lit(None).cast(f.dataType))
        else:
            # Faz cast explícito para o tipo da tabela
            cur = cur.withColumn(tgt, F.col(tgt).cast(f.dataType))

    ordered = [f.name for f in target_schema.fields]
    cur = cur.select(*ordered)
    return cur


# =============================================================================
# Projeções finais (layout EFD → IPM)
# =============================================================================


def _project_document_efd(
    df_docs: DataFrame,
    *,
    key_col: str,
    audit_id: Optional[int],
) -> DataFrame:
    """
    Projeção para alinhar com IPM_DOCUMENTO_PARTCT_CALC_IPM.

    Importante: aqui usamos os MESMOS nomes de colunas do NFe/IPM,
    e depois o alinhamento _align_with_iceberg_table_schema garante que
    os tipos batam com o Iceberg.

    Para EFD, neste primeiro momento, os campos de cadastro/entrada/saída
    permanecem nulos.
    """
    cols = {
        # Chave do documento (usa o doc_id gerado no EFD)
        "codg_chave_acesso_nfe": F.col(key_col),

        # Tipo de documento fiscal – para EFD, mantemos nulo por enquanto
        "tipo_documento_fiscal": F.lit(None).cast("string"),

        # Dados de entrada (participante entrada) – mantidos nulos
        "numr_cpf_cnpj_entrada": F.lit(None).cast("string"),
        "numr_inscricao_entrada": F.lit(None).cast("string"),
        "stat_cadastro_entrada": F.lit(None).cast("string"),
        "tipo_enqdto_fiscal_entrada": F.lit(None).cast("string"),
        "indi_prod_rural_entrada": F.lit(None).cast("string"),
        "indi_prod_rural_exclus_entrada": F.lit(None).cast("string"),
        "codg_municipio_entrada_cad": F.lit(None).cast("string"),
        "codg_uf_entrada_cad": F.lit(None).cast("string"),
        "codg_municipio_entrada_doc": F.lit(None).cast("string"),
        "codg_uf_entrada_doc": F.lit(None).cast("string"),

        # Dados de saída (participante saída) – mantidos nulos
        "numr_cpf_cnpj_saida": F.lit(None).cast("string"),
        "numr_inscricao_saida": F.lit(None).cast("string"),
        "stat_cadastro_saida": F.lit(None).cast("string"),
        "tipo_enqdto_fiscal_saida": F.lit(None).cast("string"),
        "indi_prod_rural_saida": F.lit(None).cast("string"),
        "indi_prod_rural_exclus_saida": F.lit(None).cast("string"),
        "codg_municipio_saida_cad": F.lit(None).cast("string"),
        "codg_uf_saida_cad": F.lit(None).cast("string"),
        "codg_municipio_saida_doc": F.lit(None).cast("string"),
        "codg_uf_saida_doc": F.lit(None).cast("string"),

        # Datas e valores
        "data_emissao_nfe": _parse_efd_date("DT_DOC"),
        "valr_nota_fiscal": (
            F.col("VL_DOC").cast(T.DecimalType(17, 2))
            if "VL_DOC" in df_docs.columns
            else F.lit(None).cast(T.DecimalType(17, 2))
        ),

        # Resultados das regras / agregações
        "valr_adicionado_operacao": F.col("valr_adicionado_operacao").cast(
            T.DecimalType(17, 2)
        ),
        "indi_aprop": F.col("indi_aprop"),
        "codg_tipo_doc_partct_calc": F.col("codg_tipo_doc_partct").cast("int"),

        # Motivo de exclusão – para EFD mantemos nulo
        "codg_motivo_exclusao_calculo": F.lit(None).cast("int"),

        # Referência externa – mantido nulo
        "numr_referencia_documento": F.lit(None).cast("string"),

        # Auditoria – EFD ainda não integra com tabela de auditoria IPM
        "id_procesm_indice": F.lit(
            int(audit_id) if audit_id is not None else None
        ).cast("int"),
    }

    select_exprs = [expr.alias(name) for name, expr in cols.items()]
    return df_docs.select(*select_exprs)


def _project_item_efd(
    df_items: Optional[DataFrame],
    *,
    key_col: str,
    audit_id: Optional[int],
) -> Optional[DataFrame]:
    """
    Projeção mínima para alinhar com a escrita IPM_ITEM_DOCUMENTO.
    Para EFD sem auditoria, audit_id será sempre None, logo id_procesm_indice sai nulo.
    """
    if df_items is None:
        return None

    num_item_col = (
        F.col("NUM_ITEM").cast("int")
        if "NUM_ITEM" in df_items.columns
        else F.row_number()
        .over(Window.partitionBy(F.lit(1)).orderBy(F.monotonically_increasing_id()))
        .cast("int")
    )

    cols = {
        "id_procesm_indice": F.lit(
            int(audit_id) if audit_id is not None else None
        ).cast("int"),
        "codg_chave_acesso_nfe": F.col(key_col),
        "numr_item": num_item_col,
        "cfop": (
            _norm_str(F.col("CFOP"))
            if "CFOP" in df_items.columns
            else F.lit(None).cast("string")
        ),
        "ncm": (
            _norm_str(F.col("COD_NCM"))
            if "COD_NCM" in df_items.columns
            else (
                _norm_str(F.col("NCM"))
                if "NCM" in df_items.columns
                else F.lit(None).cast("string")
            )
        ),
        "valr_adicionado": F.col("valr_adicionado").cast(T.DecimalType(17, 2)),
        "is_cfop_participante": F.col("is_cfop_participante").cast("boolean"),
        "codg_tipo_doc_partct_item": F.col(
            "codg_tipo_doc_partct_item"
        ).cast("int"),

        # Informações adicionais úteis
        "vlr_item": (
            F.col("VL_ITEM").cast(T.DecimalType(17, 2))
            if "VL_ITEM" in df_items.columns
            else F.lit(None).cast(T.DecimalType(17, 2))
        ),
        "qtd_item": (
            F.col("QTD").cast(T.DecimalType(17, 3))
            if "QTD" in df_items.columns
            else F.lit(None).cast(T.DecimalType(17, 3))
        ),
    }

    select_exprs = [expr.alias(name) for name, expr in cols.items()]
    return df_items.select(*select_exprs)


# =============================================================================
# Pipeline principal
# =============================================================================


def run_efd(
    spark: SparkSession,
    settings: Settings,
    *,
    data_inicio: str,
    data_fim: str,
    kudu_db: str = _DEFAULT_KUDU_DB,
    efd_c100_table: str = _DEFAULT_TB_C100,
    efd_c170_table: str = _DEFAULT_TB_C170,
    efd_e100_table: Optional[str] = _DEFAULT_TB_E100,
    efd_e110_table: Optional[str] = _DEFAULT_TB_E110,
    ts_doc_col: str = _DEFAULT_TS_DOC,
    ts_item_col: str = _DEFAULT_TS_ITEM,
    key_col: str = _DEFAULT_KEY_COL,
    out_doc_table: Optional[str] = _DEFAULT_OUT_DOC or None,
    out_item_table: Optional[str] = _DEFAULT_OUT_ITEM or None,
    mode: str = "merge",
    print_settings_flag: bool = False,
    audit_enabled: bool = True,   # Mantido para compatibilidade, mas IGNORADO
    write_iceberg: bool = True,
    repartition_docs: int = _DEFAULT_REPARTITION_DOCS,
    repartition_items: int = _DEFAULT_REPARTITION_ITEMS,
) -> Dict[str, int]:

    if print_settings_flag:
        print_settings(settings)

    # ---------- Auditoria ----------
    audit_id: Optional[int] = None  # EFD sem auditoria IPM

    # ---------- Leitura Kudu ----------
    df_c100 = _read_efd_c100(
        spark,
        settings,
        kudu_db=kudu_db,
        tb_c100=efd_c100_table,
        ts_col=ts_doc_col,
        data_inicio=data_inicio,
        data_fim=data_fim,
    ).persist()

    df_c170 = _read_efd_c170(
        spark,
        settings,
        kudu_db=kudu_db,
        tb_c170=efd_c170_table,
        ts_col=ts_item_col,
        data_inicio=data_inicio,
        data_fim=data_fim,
    ).persist()

    df_e100 = _read_efd_e100(
        spark,
        settings,
        kudu_db=kudu_db,
        tb_e100=efd_e100_table,
        data_inicio=data_inicio,
        data_fim=data_fim,
    )

    df_e110 = _read_efd_e110(
        spark,
        settings,
        kudu_db=kudu_db,
        tb_e110=efd_e110_table,
        data_inicio=data_inicio,
        data_fim=data_fim,
    )

    metrics: Dict[str, int] = {}
    metrics["c100_lidos"] = _safe_count(df_c100, "C100")
    metrics["c170_lidos"] = _safe_count(df_c170, "C170")
    metrics["e100_lidos"] = _safe_count(df_e100, "E100")
    metrics["e110_lidos"] = _safe_count(df_e110, "E110")

    # ---------- Chave canônica (doc_id) ----------
    df_docs_base = _build_doc_id(df_c100, key_col=key_col)

    # E100/E110 (opcional)
    df_docs_base = _enrich_with_periodo(df_docs_base, df_e100, df_e110)

    # ---------- Vincula doc_id nos itens ----------
    join_keys: List[str] = []
    for k in ("COD_MOD", "SER", "NUM_DOC", "DT_DOC"):
        if k in df_c170.columns and k in df_docs_base.columns:
            join_keys.append(k)

    # Identificador do contribuinte (se existir)
    if "CNPJ" in df_c170.columns and "CNPJ" in df_docs_base.columns:
        join_keys.append("CNPJ")
    elif "CPF" in df_c170.columns and "CPF" in df_docs_base.columns:
        join_keys.append("CPF")

    if not join_keys:
        # fallback: (NUM_DOC, DT_DOC)
        for k in ("NUM_DOC", "DT_DOC"):
            if k in df_c170.columns and k in df_docs_base.columns:
                join_keys.append(k)

    df_items_base = df_c170.join(
        df_docs_base.select(key_col, *[c for c in join_keys]),
        on=join_keys,
        how="left",
    )

    # Adapta layout EFD → layout esperado pelas regras
    df_items_base = _prepare_efd_items_for_rules(df_items_base)

    # ---------- Carrega contexto de regras (Oracle) ----------
    ctx = build_rules_context(
        spark,
        settings,
        broadcast_params_flag=True,
        include_ncm_ref=True,
    )

    # ---------- Aplica regras ----------
    # ITENS (CFOP/NCM)
    df_items_rules, m_it = apply_rules_to_items(
        df_items_base,
        ctx,
        apply_ncm=None,
        filter_ncm_nonparticipants=None,
        apply_cfop_items=None,
        filter_cfop_nonparticipants_items=None,
    )
    metrics.update({f"itens_{k}": v for k, v in (m_it or {}).items()})

    # DOCUMENTOS (GEN/CCE off para EFD)
    df_docs_rules, m_doc = apply_rules_to_documents(
        df_docs_base,
        ctx,
        apply_gen=None,
        apply_cce=False,
        filter_cce_excluded=None,
    )
    metrics.update({f"docs_{k}": v for k, v in (m_doc or {}).items()})

    # ---------- Agrega itens → documento ----------
    df_docs_aggr = _aggregate_items_to_document(
        df_docs_rules,
        df_items_rules,
        key_col=key_col,
    )

    # ---------- Projeções finais ----------
    df_docs_final = _project_document_efd(
        df_docs_aggr,
        key_col=key_col,
        audit_id=audit_id,
    )
    df_items_final = _project_item_efd(
        df_items_rules,
        key_col=key_col,
        audit_id=audit_id,
    )

    # Ajuste opcional de particionamento antes da escrita
    if repartition_docs and repartition_docs > 0:
        df_docs_final = df_docs_final.repartition(
            repartition_docs,
            "codg_chave_acesso_nfe",
        )
    if df_items_final is not None and repartition_items and repartition_items > 0:
        df_items_final = df_items_final.repartition(
            repartition_items,
            "codg_chave_acesso_nfe",
        )

    metrics["docs_finais"] = _safe_count(df_docs_final, "docs_final")
    metrics["itens_finais"] = _safe_count(df_items_final, "itens_final")

    # ---------- Escrita Iceberg ----------
    if write_iceberg:
        # Resolve tabelas de saída:
        if not out_doc_table:
            out_doc_table = settings.iceberg.document_table()
        if not out_item_table:
            out_item_table = settings.iceberg.item_table()

        # Alinha com o schema real do Iceberg (documentos)
        df_docs_to_write = _align_with_iceberg_table_schema(
            spark, out_doc_table, df_docs_final
        )

        write_df(
            df_docs_to_write,
            settings=settings,
            format="iceberg",
            table=out_doc_table,
            mode=mode,
            key_columns=["codg_chave_acesso_nfe"],
        )

        # ITENS – também podemos alinhar com a tabela de itens, se existir
        if df_items_final is not None:
            df_items_to_write = _align_with_iceberg_table_schema(
                spark, out_item_table, df_items_final
            )
            write_df(
                df_items_to_write,
                settings=settings,
                format="iceberg",
                table=out_item_table,
                mode=mode,
                key_columns=["codg_chave_acesso_nfe", "numr_item"],
            )

    return metrics


# =============================================================================
# CLI (opcional, se quiser rodar efd.py direto – não é usado pelo main.py)
# =============================================================================


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Pipeline EFD (Kudu -> Regras -> Iceberg)."
    )

    # Período
    p.add_argument("--data-inicio", required=True, help="Data inicial (YYYY-MM-DD)")
    p.add_argument("--data-fim", required=True, help="Data final (YYYY-MM-DD)")

    # Kudu
    p.add_argument(
        "--kudu-db",
        default=_DEFAULT_KUDU_DB,
        help="Database/schema do Kudu (default: efd)",
    )
    p.add_argument(
        "--efd-c100-table",
        default=_DEFAULT_TB_C100,
        help="Tabela C100 (documentos)",
    )
    p.add_argument(
        "--efd-c170-table",
        default=_DEFAULT_TB_C170,
        help="Tabela C170 (itens)",
    )
    p.add_argument(
        "--efd-e100-table",
        default=_DEFAULT_TB_E100,
        help="Tabela E100 (período de apuração) [opcional]",
    )
    p.add_argument(
        "--efd-e110-table",
        default=_DEFAULT_TB_E110,
        help="Tabela E110 (apuração/ajustes) [opcional]",
    )

    # Colunas de pushdown e chave
    p.add_argument(
        "--ts-doc-col",
        default=_DEFAULT_TS_DOC,
        help="Coluna TS (epoch) para C100 (default: dt_doc_nums)",
    )
    p.add_argument(
        "--ts-item-col",
        default=_DEFAULT_TS_ITEM,
        help="Coluna TS para C170 (por padrão vazio = sem filtro)",
    )
    p.add_argument(
        "--key-col",
        default=_DEFAULT_KEY_COL,
        help="Nome da coluna de chave do documento (default: doc_id)",
    )

    # Saída
    p.add_argument(
        "--out-doc-table",
        default=_DEFAULT_OUT_DOC,
        help=(
            "Tabela Iceberg de documentos "
            "(default: settings.iceberg.document_table())"
        ),
    )
    p.add_argument(
        "--out-item-table",
        default=_DEFAULT_OUT_ITEM,
        help=(
            "Tabela Iceberg de itens "
            "(default: settings.iceberg.item_table())"
        ),
    )
    p.add_argument(
        "--mode",
        default="merge",
        choices=["merge", "append"],
        help="Modo de escrita no Iceberg",
    )

    # Execução
    p.add_argument(
        "--no-write",
        action="store_true",
        help="Não escreve no Iceberg (apenas lê/processa)",
    )
    p.add_argument(
        "--print-settings",
        action="store_true",
        help="Imprime Settings carregadas",
    )

    # Performance
    p.add_argument(
        "--repartition-docs",
        type=int,
        default=_DEFAULT_REPARTITION_DOCS,
        help="Repartition dos documentos antes da escrita",
    )
    p.add_argument(
        "--repartition-items",
        type=int,
        default=_DEFAULT_REPARTITION_ITEMS,
        help="Repartition dos itens antes da escrita",
    )

    return p


def main() -> None:
    args = _build_arg_parser().parse_args()

    settings: Settings = load_settings_from_env()
    spark: SparkSession = build_spark(settings, app_name="PROCESSO_CARGA_EFD_IPM")

    try:
        metrics = run_efd(
            spark,
            settings,
            data_inicio=args.data_inicio,
            data_fim=args.data_fim,
            kudu_db=args.kudu_db,
            efd_c100_table=args.efd_c100_table,
            efd_c170_table=args.efd_c170_table,
            efd_e100_table=(args.efd_e100_table or None),
            efd_e110_table=(args.efd_e110_table or None),
            ts_doc_col=args.ts_doc_col,
            ts_item_col=args.ts_item_col,
            key_col=args.key_col,
            out_doc_table=(args.out_doc_table or None),
            out_item_table=(args.out_item_table or None),
            mode=args.mode,
            print_settings_flag=bool(args.print_settings),
            write_iceberg=(not args.no_write),
            repartition_docs=args.repartition_docs,
            repartition_items=args.repartition_items,
        )
        print("[EFD] Métricas:", json.dumps(metrics, ensure_ascii=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
