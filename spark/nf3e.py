# -*- coding: utf-8 -*-
# app/nf3e.py
"""
Pipeline NF3e (Kudu -> Regras -> Iceberg), alinhado ao settings.py atual.

- Lê Kudu (ident, emit, dest, total; itens opcional) com pushdown por ide_dhemi (timestamp) e chave (chnf3e).
- Determina lado GO (EMIT/DEST) e projeta campos de cadastro/doc.
- Aplica GEN (municípios/UF) via RulesContext e, se disponíveis, eventos de cancelamento/substituição.
- Documento participante quando não houver motivo de exclusão (indi_aprop='S' e valr_adicionado_operacao = valor da nota).
- Projeção final para a tabela Iceberg de documentos IPM (alinhando tipos/nomes ao schema alvo).
- Escrita de itens vazia (compatibilidade), se a tabela existir.
- Auditoria integrada (start/bump/finish_success/finish_error).
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Dict, Optional, List, Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from app.settings import Settings, load_settings_from_env, build_spark, print_settings
from app.utils.io import write_df
from app.utils.audit import start_and_get_id, bump_counts, finish_success, finish_error

# Regras (GEN)
from app.utils.rules import build_rules_context, apply_rules_to_documents, ensure_business_columns

# =============================================================================
# Constantes NF3e
# =============================================================================
NF3E_MODELO = "66"
TIPO_DOC_PARTCT_NF3E = 30
TIPO_DOCUMENTO_FISCAL_NF3E = "66"
KEY_COL = "chnf3e"
TS_COL = "ide_dhemi"  # TIMESTAMP (ou string "yyyy-MM-dd HH:mm:ss")

DEFAULT_EVT_CANCEL = {"110111"}
DEFAULT_EVT_SUBST  = {"110112"}  # mantém tentativa segura

# =============================================================================
# Helpers
# =============================================================================

def _qualify_kudu_table(db: str, name: str, *, for_format: str = "kudu") -> str:
    n = (name or "").strip()
    if n.lower().startswith("impala::"):
        n = n.split("::", 1)[1]
    elif n.lower().startswith("kudu."):
        n = ".".join(n.split(".", 2)[1:])
    if "." not in n:
        n = f"{db}.{n}"
    return f"impala::{n}" if for_format.lower() in ("kudu", "impala") else n


def _mk_kudu_between_where(
    data_inicio: str, data_fim: str, chave: Optional[str] = None, ts_col: str = TS_COL, key_col: str = KEY_COL
) -> str:
    # Pushdown sem funções na coluna (preserva predicate pushdown no Kudu)
    di = f"{data_inicio.strip()} 00:00:00"
    df = f"{data_fim.strip()} 23:59:59"
    base = f"({ts_col} >= '{di}' AND {ts_col} <= '{df}')"
    if chave:
        base += f" AND {key_col} = '{chave.strip()}'"
    return base


def _mk_kudu_between_where_epoch(
    data_inicio: str, data_fim: str, chave: Optional[str], epoch_col: str, key_col: str = KEY_COL
) -> str:
    di = f"{data_inicio.strip()} 00:00:00"
    df = f"{data_fim.strip()} 23:59:59"
    base = f"({epoch_col} BETWEEN unix_timestamp('{di}') AND unix_timestamp('{df}'))"
    if chave:
        base += f" AND {key_col} = '{chave.strip()}'"
    return base


def _merge_where(a: Optional[str], b: Optional[str]) -> Optional[str]:
    if a and b:
        return f"({a}) AND ({b})"
    return a or b


def _digits_only(col: F.Column) -> F.Column:
    return F.regexp_replace(F.trim(col.cast("string")), r"\D", "")


def _safe_count(df: Optional[DataFrame], *, kind: str) -> int:
    if df is None:
        return 0
    try:
        return int(df.count())
    except Exception as e:
        msg = str(e)
        if ("Tablet is lagging too much" in msg) or ("SERVICE_UNAVAILABLE" in msg):
            print(f"[nf3e][WARN] Kudu instável em {kind}; retornando -1. Detalhe: {msg[:300]}")
            return -1
        print(f"[nf3e][WARN] Falha ao contar {kind}: {e}")
        return -1


def _uf_from_cuf_expr(colname: str) -> F.Column:
    return (
        F.when(F.col(colname) == 11, "RO").when(F.col(colname) == 12, "AC")
        .when(F.col(colname) == 13, "AM").when(F.col(colname) == 14, "RR")
        .when(F.col(colname) == 15, "PA").when(F.col(colname) == 16, "AP")
        .when(F.col(colname) == 17, "TO").when(F.col(colname) == 21, "MA")
        .when(F.col(colname) == 22, "PI").when(F.col(colname) == 23, "CE")
        .when(F.col(colname) == 24, "RN").when(F.col(colname) == 25, "PB")
        .when(F.col(colname) == 26, "PE").when(F.col(colname) == 27, "AL")
        .when(F.col(colname) == 28, "SE").when(F.col(colname) == 29, "BA")
        .when(F.col(colname) == 31, "MG").when(F.col(colname) == 32, "ES")
        .when(F.col(colname) == 33, "RJ").when(F.col(colname) == 35, "SP")
        .when(F.col(colname) == 41, "PR").when(F.col(colname) == 42, "SC")
        .when(F.col(colname) == 43, "RS").when(F.col(colname) == 50, "MS")
        .when(F.col(colname) == 51, "MT").when(F.col(colname) == 52, "GO")
        .when(F.col(colname) == 53, "DF").otherwise(F.lit(None))
    )


def _first_existing(cols: Iterable[str], df: DataFrame) -> Optional[str]:
    for c in cols:
        if c in df.columns:
            return c
    return None


def _pick(df: DataFrame, aliases: Iterable[str]) -> F.Column:
    name = _first_existing(aliases, df)
    return F.col(name) if name else F.lit(None)


def _read_kudu(
    spark: SparkSession,
    settings: Settings,
    table_attr: str,
    *,
    where: Optional[str],
    columns: Optional[List[str]] = None,
    allow_fail: bool = False,
) -> DataFrame:
    kudu_fmt = os.getenv("KUDU_FORMAT", "kudu")
    table_name = getattr(settings.kudu, table_attr)
    qualified = _qualify_kudu_table(settings.kudu.database, table_name, for_format=kudu_fmt)
    masters = os.getenv("KUDU_MASTERS", settings.kudu.masters)
    print(f"[nf3e] KUDU READ | format={kudu_fmt} | masters={masters} | kudu.table={qualified}")
    try:
        reader = (
            spark.read.format(kudu_fmt)
            .option("kudu.master", masters)
            .option("kudu.table", qualified)
            .option("kudu.faultTolerantScan", "false")
            .option("kudu.scanLocality", "leader_only")
            .option("kudu.readMode", "READ_LATEST")
            .option("kudu.scanRequestTimeoutMs", "600000")
            .option("kudu.operationTimeoutMs", "180000")
            .option("kudu.socketReadTimeoutMs", "180000")
            .option("kudu.keepAlivePeriodMs", "15000")
        )
        df = reader.load()
        if where:
            print(f"[nf3e] KUDU WHERE (pushdown): {where}")
            df = df.where(where)
        if columns:
            keep = [c for c in columns if c in df.columns]
            if keep:
                df = df.select(*keep)
        return df
    except Exception as e:
        if allow_fail:
            print(f"[nf3e] Falha ao ler {table_attr} ({table_name}): {e}. Retornando DF vazio.")
            return spark.createDataFrame([], T.StructType([]))
        raise

# =============================================================================
# Leitura Kudu (NF3e)
# =============================================================================

def _load_ident_nf3e(
    spark: SparkSession, settings: Settings, *, data_inicio: str, data_fim: str, chave: Optional[str], where_extra: Optional[str] = None
) -> DataFrame:
    where_range = _merge_where(_mk_kudu_between_where(data_inicio, data_fim, chave, ts_col=TS_COL, key_col=KEY_COL), where_extra)
    df = _read_kudu(
        spark, settings, "nf3e_infnf3e_table", where=where_range,
        columns=[
            KEY_COL, TS_COL, "ide_cuf", "ide_mod", "ide_tpamb",
            # emit/dest (alias comuns)
            "emit_cnpj", "emit_cpf", "emit_ie", "emit_cmun", "emit_uf",
            "dest_cnpj", "dest_cpf", "dest_ie", "dest_cmun", "dest_uf",
            # variantes (se existirem)
            "emit_enderemit_cmun", "emit_enderemit_uf", "dest_enderdest_cmun", "dest_enderdest_uf",
        ],
    )
    # modelo 66 e ambiente produção (1)
    df = df.filter(
        (F.col("ide_mod").cast("string") == NF3E_MODELO) &
        (F.col("ide_tpamb").cast("string") == "1")
    )
    return df


def _load_total_nf3e(
    spark: SparkSession,
    settings: Settings,
    *,
    data_inicio: str,
    data_fim: str,
    chave: Optional[str],
    where_extra: Optional[str] = None,
) -> DataFrame:
    """
    Consolida os totais da NF3e a partir de diferentes tabelas Kudu.

    Estratégia:
      - nf3e_gscee_gconsumidor: hoje não tem vnf/vprod, mas mantemos leitura
        por compatibilidade (pode evoluir no futuro).
      - nf3e_infnf3e: idem.
      - (opcional) nf3e_ganeel: caso você configure em settings.kudu
        'nf3e_ganeel_table', usamos o campo ggrandfat_vfat como total.
    """

    where_range = _merge_where(
        _mk_kudu_between_where(data_inicio, data_fim, chave, ts_col=TS_COL, key_col=KEY_COL),
        where_extra,
    )

    def _norm(df: Optional[DataFrame]) -> Optional[DataFrame]:
        """
        Normaliza um DF qualquer para [chnf3e, total_vnf, total_vprod],
        usando aliases quando existirem. Se não houver nenhuma coluna de
        valor, devolve NULL em total_vnf/total_vprod.
        """
        if df is None or df is None or len(df.columns) == 0:
            return None

        nv = df

        # tenta achar colunas candidatas
        vnf_name = _first_existing(["total_vnf", "vnf"], nv)
        vprod_name = _first_existing(["total_vprod", "vprod"], nv)

        if vnf_name:
            nv = nv.withColumn("total_vnf", F.col(vnf_name))
        else:
            nv = nv.withColumn("total_vnf", F.lit(None))

        if vprod_name:
            nv = nv.withColumn("total_vprod", F.col(vprod_name))
        else:
            nv = nv.withColumn("total_vprod", F.lit(None))

        # garante que KEY_COL exista
        if KEY_COL not in nv.columns:
            return None

        return nv.select(KEY_COL, "total_vnf", "total_vprod")

    # 1) gscee_gconsumidor (hoje serve mais como placeholder de schema)
    t1_raw = _read_kudu(
        spark,
        settings,
        "nf3e_gscee_gconsumidor_table",
        where=where_range,
        allow_fail=True,
        columns=[KEY_COL, "total_vnf", "total_vprod", "vnf", "vprod"],
    )
    t1 = _norm(t1_raw)

    # 2) fallback: totais dentro do infnf3e (caso apareçam no futuro)
    t2_raw = _read_kudu(
        spark,
        settings,
        "nf3e_infnf3e_table",
        where=where_range,
        allow_fail=True,
        columns=[KEY_COL, "total_vnf", "total_vprod", "vnf", "vprod"],
    )
    t2 = _norm(t2_raw)

    # 3) (opcional) nf3e_ganeel: usar ggrandfat_vfat como total
    t3 = None
    if getattr(settings.kudu, "nf3e_ganeel_table", None):
        t3_raw = _read_kudu(
            spark,
            settings,
            "nf3e_ganeel_table",
            where=where_range,
            allow_fail=True,
            columns=[KEY_COL, "ggrandfat_vfat"],
        )
        if t3_raw is not None and len(t3_raw.columns) > 0 and "ggrandfat_vfat" in t3_raw.columns:
            t3 = (
                t3_raw
                .withColumn("total_vnf", F.col("ggrandfat_vfat"))
                .withColumn("total_vprod", F.col("ggrandfat_vfat"))
                .select(KEY_COL, "total_vnf", "total_vprod")
            )

    # Consolida tudo que veio não-vazio
    base = None
    for cand in (t1, t2, t3):
        if cand is None or len(cand.columns) == 0:
            continue
        base = cand if base is None else base.unionByName(cand, allowMissingColumns=True)

    if base is None:
        # Sem nenhum total disponível: devolve DF vazio com schema esperado
        return spark.createDataFrame(
            [],
            T.StructType(
                [
                    T.StructField(KEY_COL, T.StringType(), True),
                    T.StructField("total_vnf", T.DecimalType(17, 2), True),
                    T.StructField("total_vprod", T.DecimalType(17, 2), True),
                ]
            ),
        )

    return (
        base.groupBy(KEY_COL)
        .agg(
            F.first(F.col("total_vnf"), ignorenulls=True).alias("total_vnf"),
            F.first(F.col("total_vprod"), ignorenulls=True).alias("total_vprod"),
        )
    )


def _load_itens_nf3e(
    spark: SparkSession, settings: Settings, *, data_inicio: str, data_fim: str, chave: Optional[str], where_extra: Optional[str] = None
) -> Optional[DataFrame]:
    if not getattr(settings.kudu, "nf3e_nfdet_det_table", None):
        return None
    where_range = _merge_where(_mk_kudu_between_where(data_inicio, data_fim, chave, ts_col=TS_COL, key_col=KEY_COL), where_extra)
    try:
        return _read_kudu(spark, settings, "nf3e_nfdet_det_table", where=where_range, allow_fail=True)
    except Exception:
        return None


def _load_eventos_nf3e(
    spark: SparkSession,
    settings: Settings,
    *,
    data_inicio: str,
    data_fim: str,
    chave: Optional[str],
) -> Optional[DataFrame]:
    """
    Leitura opcional de eventos NF3e.

    Importante: na base NF3E, a coluna de data/hora de evento é TIMESTAMP(3)
    (dhevento). Portanto, o filtro deve comparar TIMESTAMP com literal de
    timestamp, sem usar unix_timestamp/epoch.
    """
    if not hasattr(settings.kudu, "nf3e_infevento_table"):
        return None

    # Filtro por janela em cima do TIMESTAMP diretamente
    where_ts = _merge_where(
        _mk_kudu_between_where(
            data_inicio,
            data_fim,
            chave,
            ts_col="dhevento",   # nome exato na tabela nf3e_infevento
            key_col=KEY_COL,
        ),
        None,
    )

    try:
        df_evt = _read_kudu(
            spark,
            settings,
            "nf3e_infevento_table",
            where=where_ts,
            allow_fail=True,
            columns=[
                KEY_COL,
                "dhevento",
                "tpevento",
                "tpEvento",
                "descevento",
                "nseqevento",
                "nprot",
                "xjust",
                "evsubnf3e_chnf3esubstituta",
                "evsubnf3e_dhrecbto",
                "evajustenf3e_chnf3eajuste",
                "evajustenf3e_dhrecbto",
                "evnf3eliberaprazocanc_xobs",
            ],
        )
    except Exception as e:
        print(f"[nf3e][eventos] Falha leitura eventos (timestamp direto): {e}. Prosseguindo sem eventos.")
        return None

    if df_evt is None or len(df_evt.columns) == 0:
        return None

    cnt = _safe_count(df_evt, kind="eventos_ts")
    if cnt == 0:
        print("[nf3e][eventos] Nenhum evento encontrado no período informado.")
        return None

    # Normaliza o nome da coluna de data/hora para 'dhevento'
    col_evt_ts = (
        "dhevento"
        if "dhevento" in df_evt.columns
        else ("dhEvento" if "dhEvento" in df_evt.columns else None)
    )
    if col_evt_ts is None:
        print(
            "[nf3e][eventos] Tabela de eventos não possui coluna de data/hora "
            "esperada (dhevento/dhEvento). Prosseguindo sem eventos."
        )
        return None

    df_evt_norm = df_evt.withColumn("dhevento_norm", F.col(col_evt_ts)).drop(col_evt_ts)
    df_evt_norm = df_evt_norm.withColumnRenamed("dhevento_norm", "dhevento")

    return df_evt_norm

# =============================================================================
# Preparação & Regras
# =============================================================================

def _prepare_docs_nf3e(
    spark: SparkSession, settings: Settings, *,
    data_inicio: str, data_fim: str, chave: Optional[str], where_docs: Optional[str]
) -> DataFrame:
    ident = _load_ident_nf3e(spark, settings, data_inicio=data_inicio, data_fim=data_fim, chave=chave, where_extra=where_docs).persist()
    tota  = _load_total_nf3e(spark, settings, data_inicio=data_inicio, data_fim=data_fim, chave=chave, where_extra=where_docs)

    # datas e UF inferida a partir do CUF
    docs = (
        ident.alias("i")
        .join(tota.alias("t"), on=KEY_COL, how="left")
        .withColumn("data_emissao_ts", F.to_timestamp(F.col(TS_COL)))
        .withColumn("data_emissao_dt", F.to_date(F.col("data_emissao_ts")))
        .withColumn("i_uf_from_cuf", _uf_from_cuf_expr("i.ide_cuf"))
    )

    # documentos (cnpj/cpf) padronizados (emit/dest) com aliases
    emit_cnpj = _pick(ident, ["emit_cnpj"])
    emit_cpf  = _pick(ident, ["emit_cpf"])
    dest_cnpj = _pick(ident, ["dest_cnpj"])
    dest_cpf  = _pick(ident, ["dest_cpf"])

    emit_ie   = _pick(ident, ["emit_ie"])
    dest_ie   = _pick(ident, ["dest_ie"])

    # cmun/uf com variantes mapeadas
    emit_cmun = _pick(ident, ["emit_cmun", "emit_enderemit_cmun"])
    emit_uf   = _pick(ident, ["emit_uf", "emit_enderemit_uf"])
    dest_cmun = _pick(ident, ["dest_cmun", "dest_enderdest_cmun"])
    dest_uf   = _pick(ident, ["dest_uf", "dest_enderdest_uf"])

    docs = (
        docs
        .withColumn("emit_doc_ident", _digits_only(F.coalesce(emit_cnpj, emit_cpf)))
        .withColumn("dest_doc_ident", _digits_only(F.coalesce(dest_cnpj, dest_cpf)))
        .withColumn("emit_uf", F.upper(F.coalesce(emit_uf, F.col("i_uf_from_cuf"))))
        .withColumn("dest_uf", F.upper(dest_uf))
        .withColumn("e_ie", _digits_only(emit_ie))
        .withColumn("d_ie", _digits_only(dest_ie))
        .withColumn("e_cmun", _digits_only(emit_cmun))
        .withColumn("d_cmun", _digits_only(dest_cmun))
    )

    # Determinação do lado GO
    GO = F.lit("GO")
    lado_go = (
        F.when((F.col("emit_uf") == GO) | (F.col("emit_uf").isNull() & (F.col("i_uf_from_cuf") == GO)), F.lit("EMIT"))
         .when(F.col("dest_uf") == GO, F.lit("DEST"))
         .otherwise(F.lit(None))
    )
    docs = docs.withColumn("lado_go", lado_go).filter(F.col("lado_go").isNotNull())
    docs = docs.withColumn("tipo_doc_go", F.when(F.col("lado_go") == F.lit("EMIT"), F.lit("1")).otherwise(F.lit("0")))

    # Campos de cadastro/documento
    docs = (
        docs
        # ENTRADA (destinatário)
        .withColumn("cad_entrada_docnum", _digits_only(F.col("dest_doc_ident")))
        .withColumn("cad_entrada_ie", F.col("d_ie"))
        .withColumn("cad_entrada_cmun_ibge_str", F.col("d_cmun"))
        .withColumn("cad_entrada_uf_src", F.upper(F.col("dest_uf")))
        # SAÍDA (emitente)
        .withColumn("cad_saida_docnum", _digits_only(F.col("emit_doc_ident")))
        .withColumn("cad_saida_ie", F.col("e_ie"))
        .withColumn("cad_saida_cmun_ibge_str", F.col("e_cmun"))
        .withColumn("cad_saida_uf_src", F.upper(F.col("emit_uf")))
    )

    # Valor da NF3e: preferir total_vnf; fallback total_vprod
    vl_candidates: List[F.Column] = []
    if "total_vnf" in tota.columns:
        vl_candidates.append(F.col("t.total_vnf").cast("decimal(17,2)"))
    if "total_vprod" in tota.columns:
        vl_candidates.append(F.col("t.total_vprod").cast("decimal(17,2)"))
    vl_expr = F.coalesce(*vl_candidates) if vl_candidates else F.lit(None).cast("decimal(17,2)")
    docs = docs.withColumn("valr_nota_nf3e", vl_expr)

    # Flags de eventos (default = False)
    docs = docs.withColumn("is_cancelado", F.lit(False)).withColumn("is_substituido", F.lit(False))

    # Eventos (opcional)
    evt_df = _load_eventos_nf3e(spark, settings, data_inicio=data_inicio, data_fim=data_fim, chave=chave)
    if evt_df is not None:
        # normaliza nome de coluna
        evt_df = evt_df.withColumn("tp", F.coalesce(F.col("tpevento").cast("string"), F.col("tpEvento").cast("string")))
        evt_agg = (
            evt_df.groupBy(KEY_COL)
                  .agg(
                      F.max(F.when(F.col("tp").isin(list(DEFAULT_EVT_CANCEL)), F.lit(True)).otherwise(F.lit(False))).alias("evt_cancel"),
                      F.max(F.when(F.col("tp").isin(list(DEFAULT_EVT_SUBST)),  F.lit(True)).otherwise(F.lit(False))).alias("evt_subst"),
                  )
        )
        docs = (
            docs.alias("d").join(evt_agg.alias("e"), on=KEY_COL, how="left")
                .withColumn("is_cancelado",   F.coalesce(F.col("e.evt_cancel"), F.col("is_cancelado")))
                .withColumn("is_substituido", F.coalesce(F.col("e.evt_subst"),  F.col("is_substituido")))
        )

    # Exclusão por evento (compatível Django/IPM)
    docs = (
        docs.withColumn(
            "codg_motivo_exclusao_doc",
            F.when(F.col("is_cancelado") == True, F.lit(10))
             .when(F.col("is_substituido") == True, F.lit(13))
             .otherwise(F.lit(0))
        )
        .withColumn("indi_aprop_doc", F.when(F.col("codg_motivo_exclusao_doc") == 0, F.lit("S")).otherwise(F.lit("N")))
        .withColumn("valr_adicionado_doc",
                    F.when(F.col("codg_motivo_exclusao_doc") == 0, F.col("valr_nota_nf3e").cast("decimal(17,2)"))
                     .otherwise(F.lit(0).cast("decimal(17,2)")))
    )

    # Garante colunas mínimas esperadas (compat Regras)
    docs = ensure_business_columns(docs, for_items=False)
    return docs

# =============================================================================
# Projeção IPM (documentos)
# =============================================================================

def _project_document_ipm_nf3e(
    spark: SparkSession, settings: Settings, docs_part: DataFrame, audit_id: Optional[int]
) -> DataFrame:
    # Detecta precisão IBGE (6/7 dígitos) existente no Iceberg-alvo
    def _detect_digits(field_name: str, default: int = 6) -> int:
        full = f"{settings.iceberg.catalog}.{settings.iceberg.namespace}.{settings.iceberg.tbl_nf3e_documento_partct}"
        try:
            schema = spark.table(full).schema
            f = next((x for x in schema.fields if x.name.lower() == field_name.lower()), None)
            if f and isinstance(f.dataType, T.DecimalType):
                return 7 if getattr(f.dataType, "precision", 6) >= 7 else 6
        except Exception as e:
            print(f"[proj][nf3e] Não detectei {field_name} em {full}: {e}. Usando {default}.")
        return default

    def _cast_ibge(scol: str, digits: int) -> F.Column:
        return _digits_only(F.col(scol)).cast(f"decimal({digits},0)")

    ent_digits = _detect_digits("codg_municipio_entrada_cad", 6)
    sai_digits = _detect_digits("codg_municipio_saida_cad", 6)

    docs = docs_part.withColumn("numr_ref_aaaamm", F.date_format(F.col("data_emissao_dt"), "yyyyMM").cast("int"))
    id_proc = F.lit(int(audit_id) if audit_id is not None else None).cast("decimal(9,0)")

    out = docs.select(
        F.col(KEY_COL).alias("codg_chave_acesso_nfe"),
        F.lit(TIPO_DOCUMENTO_FISCAL_NF3E).alias("tipo_documento_fiscal"),
        # ENTRADA (destinatário)
        F.col("cad_entrada_docnum").alias("numr_cpf_cnpj_entrada"),
        F.col("cad_entrada_ie").cast("decimal(15,0)").alias("numr_inscricao_entrada"),
        F.lit(None).alias("stat_cadastro_entrada"),
        F.lit(None).alias("tipo_enqdto_fiscal_entrada"),
        F.lit(None).alias("indi_prod_rural_entrada"),
        F.lit(None).alias("indi_prod_rural_exclus_entrada"),
        _cast_ibge("cad_entrada_cmun_ibge_str", ent_digits).alias("codg_municipio_entrada_cad"),
        F.col("cad_entrada_uf_src").alias("codg_uf_entrada_cad"),
        _cast_ibge("cad_entrada_cmun_ibge_str", 7).alias("codg_municipio_entrada_doc"),
        F.col("cad_entrada_uf_src").alias("codg_uf_entrada_doc"),
        # SAÍDA (emitente)
        F.col("cad_saida_docnum").alias("numr_cpf_cnpj_saida"),
        F.col("cad_saida_ie").cast("decimal(14,0)").alias("numr_inscricao_saida"),
        F.lit(None).alias("stat_cadastro_saida"),
        F.lit(None).alias("tipo_enqdto_fiscal_saida"),
        F.lit(None).alias("indi_prod_rural_saida"),
        F.lit(None).alias("indi_prod_rural_exclus_saida"),
        _cast_ibge("cad_saida_cmun_ibge_str", sai_digits).alias("codg_municipio_saida_cad"),
        F.col("cad_saida_uf_src").alias("codg_uf_saida_cad"),
        _cast_ibge("cad_saida_cmun_ibge_str", 7).alias("codg_municipio_saida_doc"),
        F.col("cad_saida_uf_src").alias("codg_uf_saida_doc"),
        # datas/valores
        F.col("data_emissao_dt").alias("data_emissao_nfe"),
        F.col("valr_nota_nf3e").cast("decimal(17,2)").alias("valr_nota_fiscal"),
        # regras doc
        F.col("valr_adicionado_doc").cast("decimal(17,2)").alias("valr_adicionado_operacao"),
        F.col("indi_aprop_doc").alias("indi_aprop"),
        F.lit(TIPO_DOC_PARTCT_NF3E).cast("decimal(3,0)").alias("codg_tipo_doc_partct_calc"),
        F.col("codg_motivo_exclusao_doc").cast("decimal(2,0)").alias("codg_motivo_exclusao_calculo"),
        # referência e auditoria
        F.col("numr_ref_aaaamm").cast("decimal(6,0)").alias("numr_referencia_documento"),
        id_proc.alias("id_procesm_indice"),
    )
    return out

# =============================================================================
# Pipeline principal (NF3e)
# =============================================================================

def run_nf3e(
    spark: SparkSession,
    settings: Settings,
    *,
    data_inicio: str,
    data_fim: str,
    where_docs: Optional[str] = None,
    audit_params: Optional[Dict] = None,
    audit_enabled: bool = True,
    chave: Optional[str] = None,
    where_itens: Optional[str] = None,  # compat
    **kwargs
) -> Dict[str, int]:
    metrics: Dict[str, int] = {}

    # Auditoria START
    audit_id = None
    if audit_enabled:
        params_json = json.dumps(
            audit_params
            or {
                "fonte": "KUDU",
                "range_where": _mk_kudu_between_where(data_inicio, data_fim, chave),
                "where_docs": where_docs,
            },
            ensure_ascii=False,
        )
        audit_id = start_and_get_id(
            spark, settings, documento="NF3E",
            data_inicio=data_inicio, data_fim=data_fim, params_json=params_json,
        )
        metrics["audit_id"] = audit_id

    try:
        # Base (ident + totais)
        df_docs_base = _prepare_docs_nf3e(
            spark, settings, data_inicio=data_inicio, data_fim=data_fim, chave=chave, where_docs=where_docs
        ).persist()

        # GEN (enriquecimento municípios/UF/produtor rural quando disponível)
        rules_ctx = build_rules_context(spark, settings, broadcast_params_flag=True, include_ncm_ref=False)
        df_docs_rules, doc_metrics = apply_rules_to_documents(
            df_docs_base,
            rules_ctx,
            apply_gen=True,
            apply_cce=False,               # eventos NF3e tratados localmente acima
            filter_cce_excluded=False,
        )
        metrics.update({f"rules_{k}": v for k, v in doc_metrics.items() if isinstance(v, int)})

        metrics["docs_lidos"] = _safe_count(df_docs_rules, kind="docs_lidos")
        if audit_enabled and audit_id is not None and metrics["docs_lidos"] >= 0:
            bump_counts(spark, settings, id_procesm_indice=audit_id, add_docs=metrics["docs_lidos"], add_itens=0)

        # Participantes
        df_docs_part = df_docs_rules.filter(F.col("codg_motivo_exclusao_doc") == F.lit(0)).cache()
        metrics["docs_participantes"] = _safe_count(df_docs_part, kind="docs_participantes")

        # Projeção IPM
        df_docs_out = _project_document_ipm_nf3e(spark, settings, df_docs_part, audit_id=audit_id)

        # Alinhamento ao schema do Iceberg alvo
        target_docs_tbl = settings.iceberg.tbl_nf3e_documento_partct

        def _align_df_to_iceberg_table(df: DataFrame, table_name: str) -> DataFrame:
            full = f"{settings.iceberg.catalog}.{settings.iceberg.namespace}.{table_name}"
            try:
                target = spark.table(full).schema
            except Exception as e:
                print(f"[align][nf3e] Não consegui ler schema de {full}. Seguindo sem alinhamento. Detalhe: {e}")
                return df
            cols_lower = {c.lower(): c for c in df.columns}
            cur = df
            for f in target.fields:
                tgt = f.name
                src = cols_lower.get(tgt.lower())
                if src is None:
                    cur = cur.withColumn(tgt, F.lit(None).cast(f.dataType))
                else:
                    if src != tgt:
                        cur = cur.withColumnRenamed(src, tgt)
                    cur = cur.withColumn(tgt, F.col(tgt).cast(f.dataType))
            return cur.select(*[f.name for f in target.fields])

        df_docs_out = _align_df_to_iceberg_table(df_docs_out, target_docs_tbl)
        metrics["docs_finais"] = _safe_count(df_docs_out, kind="docs_finais")

        # Escrita Iceberg (merge pela chave)
        write_df(
            df_docs_out,
            settings=settings,
            format="iceberg",
            table=target_docs_tbl,
            mode="merge",
            key_columns=["codg_chave_acesso_nfe"],
            spark_session=spark,
        )

        if audit_enabled and audit_id is not None and metrics["docs_finais"] >= 0:
            bump_counts(spark, settings, id_procesm_indice=audit_id, add_docs=metrics["docs_finais"])

        # Itens NF3e (compatibilidade): escreve vazio se a tabela existir
        try:
            empty_items_schema_tbl = f"{settings.iceberg.catalog}.{settings.iceberg.namespace}.{settings.iceberg.tbl_nf3e_item_documento}"
            if spark.catalog.tableExists(empty_items_schema_tbl):
                empty_items = spark.createDataFrame([], spark.table(empty_items_schema_tbl).schema)
                write_df(
                    empty_items,
                    settings=settings,
                    format="iceberg",
                    table=settings.iceberg.tbl_nf3e_item_documento,
                    mode="append",
                    spark_session=spark,
                )
            metrics["itens_finais"] = 0
        except Exception:
            print("[nf3e] Tabela de itens NF3e não configurada; pulando escrita de itens.")
            metrics["itens_finais"] = 0

        if audit_enabled and audit_id is not None:
            finish_success(
                spark, settings, id_procesm_indice=audit_id,
                extra_updates={"qtd_docs": metrics["docs_finais"], "qtd_itens": metrics["itens_finais"]},
            )
        return metrics

    except Exception as e:
        if audit_enabled and audit_id is not None:
            finish_error(spark, settings, id_procesm_indice=audit_id, erro_msg=str(e))
        raise

# =============================================================================
# CLI
# =============================================================================

def _parse_bool(v: str) -> bool:
    return str(v).strip().lower() in {"1", "true", "t", "y", "yes", "sim", "s"}


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Pipeline NF3e (Kudu -> Iceberg)")
    p.add_argument("--data-inicio", required=True, help="AAAA-MM-DD")
    p.add_argument("--data-fim", required=True, help="AAAA-MM-DD")
    p.add_argument("--where-docs", default=None)
    p.add_argument("--print-settings", default="false")
    p.add_argument("--no-audit", action="store_true")
    p.add_argument("--chave", default=None)
    return p.parse_args(argv)


def main(argv=None) -> None:
    settings = load_settings_from_env()
    spark = build_spark(settings)
    args = parse_args(argv)

    if _parse_bool(args.print_settings):
        print_settings(settings)

    metrics = run_nf3e(
        spark,
        settings,
        data_inicio=args.data_inicio,
        data_fim=args.data_fim,
        where_docs=args.where_docs,
        audit_params={
            "cli": True,
            "range_where": _mk_kudu_between_where(args.data_inicio, args.data_fim, args.chave),
            "where_docs": args.where_docs,
        },
        audit_enabled=(not args.no_audit),
        chave=args.chave,
    )

    print("=== NF3e Metrics ===")
    for k in sorted(metrics.keys()):
        print(f"{k}: {metrics[k]}")

    spark.stop()


if __name__ == "__main__":
    main()
