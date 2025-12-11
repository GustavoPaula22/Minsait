# -*- coding: utf-8 -*-
# app/cte.py
"""
Pipeline CT-e (Kudu -> Regras -> Iceberg) com alinhamento ao Django/IPM.

- Leitura tolerante de tabelas Kudu (ident + campos de emitente/tomador + valores),
  com pushdown por ide_dhemi (TIMESTAMP) e, se informado, por chcte.
- Determinação de lado GO (EMIT/TOM) e projeção de campos de cadastro/doc.
- Regras de exclusão do documento (cancelado / substituído) com base em eventos (se disponíveis).
- Projeção final para a tabela Iceberg de documentos do CT-e.
- Escrita de tabela de itens vazia (compatibilidade), se existir.
- Auditoria integrada (start/bump/finish_success/finish_error).
"""

from __future__ import annotations

import argparse
import json
import os
import re
from typing import Dict, Optional, List, Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from app.settings import Settings, load_settings_from_env, build_spark, print_settings
from app.utils.io import write_df
from app.utils.audit import start_and_get_id, bump_counts, finish_success, finish_error

# =============================================================================
# Constantes específicas do CT-e
# =============================================================================
CTE_MODELO = "57"
TIPO_DOC_PARTCT_CTE = 20
TIPO_DOCUMENTO_FISCAL_CTE = "57"
KEY_COL = "chcte"
TS_COL  = "ide_dhemi"

DEFAULT_EVT_CANCEL = {"110111"}
DEFAULT_EVT_SUBST  = {"110165", "110170"}
DEFAULT_EVT_CCE    = {"110110"}
DEFAULT_EVT_INUT   = {"110"}

_BAD_COLS_CTE = {
    "ide_dhemi_nums", "ide_dhemi_epoch", "ide_dhemi_num", "dhemi_num", "dhemi_epoch", "dhemi_nums", "dhEmi_num"
}

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

def _mk_kudu_between_where(data_inicio: str, data_fim: str, chave: Optional[str] = None,
                           ts_col: str = TS_COL, key_col: str = KEY_COL) -> str:
    di = f"{data_inicio.strip()} 00:00:00"
    df = f"{data_fim.strip()} 23:59:59"
    base = f"({ts_col} >= '{di}' AND {ts_col} <= '{df}')"
    if chave:
        base += f" AND {key_col} = '{chave.strip()}'"
    return base

def _merge_where(base_where: Optional[str], extra_where: Optional[str]) -> Optional[str]:
    if base_where and extra_where:
        return f"({base_where}) AND ({extra_where})"
    return base_where or extra_where

def _digits_only(col: F.Column) -> F.Column:
    return F.regexp_replace(F.trim(col.cast("string")), r"\D", "")

def _safe_count(df: DataFrame, *, kind: str = "count") -> int:
    try:
        return int(df.count())
    except Exception as e:
        msg = str(e)
        if ("Tablet is lagging too much" in msg) or ("SERVICE_UNAVAILABLE" in msg):
            print(f"[cte][WARN] Kudu instável em {kind}; usando -1. Detalhe: {msg[:300]}")
            return -1
        raise

def _coalesce_doc_number(cnpj: F.Column, cpf: F.Column) -> F.Column:
    return F.coalesce(_digits_only(cnpj), _digits_only(cpf))

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

def _sanitize_where_for_cte(where_expr: Optional[str]) -> Optional[str]:
    if not where_expr:
        return None
    raw = where_expr.strip()
    lower = raw.lower()

    if re.search(r"\blength\s*\(\s*cast\b", lower) or re.search(r"\bcast\s*\(\s*\)", lower):
        print("[cte][where_sanitizer] Removendo where_docs por conter artefato 'length(cast' ou 'cast()'.")
        return None

    for bad in _BAD_COLS_CTE:
        if bad.lower() in lower:
            print(f"[cte][where_sanitizer] Removendo where_docs por conter coluna inválida: {bad}")
            return None

    expr = re.sub(r"\s+", " ", raw)
    expr = re.sub(r"\(\s*\)", "", expr).strip()
    expr = re.sub(r"^(AND|OR)\s+", "", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\s+(AND|OR)$", "", expr, flags=re.IGNORECASE).strip()
    return expr or None

def _read_kudu(
    spark: SparkSession,
    settings: Settings,
    table_name_attr: str,
    *,
    where: Optional[str],
    columns: Optional[List[str]] = None,
    allow_fail: bool = False,
) -> DataFrame:
    kudu_fmt = os.getenv("KUDU_FORMAT", "kudu")
    table_name = getattr(settings.kudu, table_name_attr)
    qualified = _qualify_kudu_table(settings.kudu.database, table_name, for_format=kudu_fmt)
    masters = os.getenv("KUDU_MASTERS", settings.kudu.masters)
    print(f"[cte] KUDU READ | format={kudu_fmt} | masters={masters} | kudu.table={qualified}")
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
            print(f"[cte] KUDU WHERE (pushdown): {where}")
            df = df.where(where)
        if columns:
            keep = [c for c in columns if c in df.columns]
            if keep:
                df = df.select(*keep)
        return df
    except Exception as e:
        if allow_fail:
            print(f"[cte] Falha ao ler {table_name_attr} ({table_name}): {e}. Retornando DF vazio.")
            return spark.createDataFrame([], T.StructType([]))
        raise

# =============================================================================
# Leitura Kudu (CT-e)
# =============================================================================

def _load_ident_cte(
    spark: SparkSession, settings: Settings, *, data_inicio: str, data_fim: str, chcte: Optional[str], where_extra: Optional[str] = None
) -> DataFrame:
    base = _mk_kudu_between_where(data_inicio, data_fim, chcte, ts_col=TS_COL, key_col=KEY_COL)
    safe_extra = _sanitize_where_for_cte(where_extra)

    df = _read_kudu(
        spark, settings, "cte_infcte_table",
        where=base,
        columns=None,
    )

    if safe_extra:
        try:
            print(f"[cte] KUDU WHERE (extra seguro): {safe_extra}")
            df = df.where(safe_extra)
        except Exception as e:
            print(f"[cte] Ignorando where_docs extra (ident) por erro: {e}")

    df = df.filter(
        (F.col("ide_mod").cast("string") == F.lit(CTE_MODELO)) &
        (F.col("ide_tpamb").cast("string") == F.lit("1"))
    ).select(
        KEY_COL, TS_COL, "ide_cuf", "ide_tpamb", "ide_mod",
        # Emitente (endereços/UF/IE/CMUN) – sem emit_cpf
        "emit_cnpj", "emit_ie", "emit_enderemit_cmun", "emit_enderemit_uf",
        # Tomador (toma4)
        "ide_toma4_cnpj", "ide_toma4_cpf", "ide_toma4_ie",
        "ide_toma4_endertoma_cmun", "ide_toma4_endertoma_uf",
    )
    return df

def _load_valores_cte(
    spark: SparkSession, settings: Settings, *, data_inicio: str, data_fim: str, chcte: Optional[str], where_extra: Optional[str] = None
) -> DataFrame:
    """
    Carrega valores do CT-e a partir do Kudu, sem depender de vprest_vdesc.
    vdesc é calculado como (vprest_vtprest - vprest_vrec) quando ambos existirem.
    """
    base = _mk_kudu_between_where(data_inicio, data_fim, chcte, ts_col=TS_COL, key_col=KEY_COL)
    safe_extra = _sanitize_where_for_cte(where_extra)

    v = _read_kudu(
        spark, settings, "cte_infcte_table",
        where=base, allow_fail=True,
        columns=[KEY_COL, "vprest_vtprest", "vprest_vrec"],
    )

    # Se não vier nada, devolve DF vazio com o esquema esperado
    if v is None or len(v.columns) == 0:
        return spark.createDataFrame([], T.StructType([
            T.StructField(KEY_COL, T.StringType(), True),
            T.StructField("vprest", T.StringType(), True),
            T.StructField("vrec",   T.StringType(), True),
            T.StructField("vdesc",  T.StringType(), True),
            T.StructField("vtot",   T.StringType(), True),
        ]))

    if safe_extra:
        try:
            print(f"[cte] KUDU WHERE (extra seguro – valores): {safe_extra}")
            v = v.where(safe_extra)
        except Exception as e:
            print(f"[cte] Ignorando where_docs extra (valores) por erro: {e}")

    # Normaliza os campos e calcula desconto
    v = (
        v.select(
            F.col(KEY_COL),
            F.col("vprest_vtprest").alias("vprest"),
            F.col("vprest_vrec").alias("vrec"),
        )
        .withColumn(
            "vdesc",
            F.when(
                F.col("vprest").isNotNull() & F.col("vrec").isNotNull(),
                F.col("vprest").cast("decimal(17,2)") - F.col("vrec").cast("decimal(17,2)"),
            ).otherwise(F.lit(None))
        )
        .withColumn("vtot", F.col("vprest"))
        .select(KEY_COL, "vprest", "vrec", "vdesc", "vtot")
    )

    # Garante 1 linha por chave
    return (
        v.groupBy(KEY_COL)
         .agg(
             F.first(F.col("vprest"), ignorenulls=True).alias("vprest"),
             F.first(F.col("vrec"),   ignorenulls=True).alias("vrec"),
             F.first(F.col("vdesc"),  ignorenulls=True).alias("vdesc"),
             F.first(F.col("vtot"),   ignorenulls=True).alias("vtot"),
         )
    )

def _load_eventos_cte(
    spark: SparkSession, settings: Settings, *, data_inicio: str, data_fim: str, chcte: Optional[str]
) -> Optional[DataFrame]:
    """
    Lê eventos de CT-e do Kudu.

    Usa a coluna de data/hora real da tabela de eventos:
        infevento_dhevento (TIMESTAMP(3))

    e a chave:
        chcte
    """
    if not hasattr(settings.kudu, "cte_evento_table"):
        return None

    # Pushdown usando a coluna correta de timestamp da tabela de eventos
    base = _mk_kudu_between_where(
        data_inicio, data_fim, chcte,
        ts_col="infevento_dhevento",  # coluna real na cte_evento
        key_col=KEY_COL,
    )
    try:
        df = _read_kudu(
            spark, settings, "cte_evento_table",
            where=base,
            allow_fail=False,
            columns=[KEY_COL, "tpEvento", "infevento_dhevento"],
        )
        return df
    except Exception as e:
        print(f"[cte][eventos] Falha ao ler eventos: {e}. Prosseguindo sem eventos.")
        return None

# =============================================================================
# Preparação de documentos CT-e
# =============================================================================

def _prepare_docs_cte(
    spark: SparkSession, settings: Settings, *,
    data_inicio: str, data_fim: str, chcte: Optional[str], where_docs: Optional[str]
) -> DataFrame:
    ident = _load_ident_cte(
        spark, settings, data_inicio=data_inicio, data_fim=data_fim, chcte=chcte, where_extra=where_docs
    ).persist()

    vals  = _load_valores_cte(
        spark, settings, data_inicio=data_inicio, data_fim=data_fim, chcte=chcte, where_extra=where_docs
    )

    def _pick(df: DataFrame, aliases: Iterable[str]) -> Optional[F.Column]:
        name = _first_existing(aliases, df)
        return F.col(name) if name else F.lit(None)

    # Emitente (somente CNPJ no teu layout)
    e_uf   = _pick(ident, ["emit_enderemit_uf"])
    e_ie   = _pick(ident, ["emit_ie"])
    e_cmun = _pick(ident, ["emit_enderemit_cmun"])
    e_doc  = _coalesce_doc_number(_pick(ident, ["emit_cnpj"]), F.lit(None))

    # Tomador (toma4)
    t_uf   = _pick(ident, ["ide_toma4_endertoma_uf"])
    t_ie   = _pick(ident, ["ide_toma4_ie"])
    t_cmun = _pick(ident, ["ide_toma4_endertoma_cmun"])
    t_doc  = _coalesce_doc_number(_pick(ident, ["ide_toma4_cnpj"]), _pick(ident, ["ide_toma4_cpf"]))

    docs = (
        ident.alias("i")
        .join(vals.alias("v"), on=KEY_COL, how="left")
        .withColumn("data_emissao_dt", F.to_date(F.col(TS_COL)))
        .withColumn("data_emissao_ts", F.to_timestamp(F.col(TS_COL)))
        .withColumn("i_uf_from_cuf", _uf_from_cuf_expr("i.ide_cuf"))
        .withColumn("emit_doc_ident", _digits_only(e_doc))
        .withColumn("tom_doc_ident",  _digits_only(t_doc))
        .withColumn("emit_uf", F.upper(e_uf))
        .withColumn("tom_uf",  F.upper(t_uf))
        .withColumn("e_ie",    _digits_only(e_ie))
        .withColumn("t_ie",    _digits_only(t_ie))
        .withColumn("e_cmun",  _digits_only(e_cmun))
        .withColumn("t_cmun",  _digits_only(t_cmun))
    )

    GO = F.lit("GO")
    lado_go = (
        F.when((F.col("emit_uf") == GO) | (F.col("emit_uf").isNull() & (F.col("i_uf_from_cuf") == GO)), F.lit("EMIT"))
         .when(F.col("tom_uf") == GO, F.lit("TOM"))
         .otherwise(F.lit(None))
    )
    docs = docs.withColumn("lado_go", lado_go).filter(F.col("lado_go").isNotNull())
    docs = docs.withColumn("tipo_doc_go", F.when(F.col("lado_go") == F.lit("EMIT"), F.lit("1")).otherwise(F.lit("0")))

    docs = (
        docs
        .withColumn("cad_entrada_docnum", _digits_only(F.col("tom_doc_ident")))
        .withColumn("cad_entrada_ie", F.col("t_ie"))
        .withColumn("cad_entrada_cmun_ibge_str", F.col("t_cmun"))
        .withColumn("cad_entrada_uf_src", F.upper(F.col("tom_uf")))
        .withColumn("cad_saida_docnum", _digits_only(F.col("emit_doc_ident")))
        .withColumn("cad_saida_ie", F.col("e_ie"))
        .withColumn("cad_saida_cmun_ibge_str", F.col("e_cmun"))
        .withColumn("cad_saida_uf_src", F.upper(F.col("emit_uf")))
    )

    vdesc = F.coalesce(F.col("v.vdesc").cast("decimal(17,2)"), F.lit(0).cast("decimal(17,2)"))
    vl_candidates: List[F.Column] = []
    if "vprest" in vals.columns:
        vl_candidates.append(F.col("v.vprest").cast("decimal(17,2)"))
    if "vtot" in vals.columns:
        vl_candidates.append((F.col("v.vtot").cast("decimal(17,2)") - vdesc).cast("decimal(17,2)"))
    if "vrec" in vals.columns:
        vl_candidates.append(F.col("v.vrec").cast("decimal(17,2)"))
    vl_expr = F.coalesce(*vl_candidates) if vl_candidates else F.lit(None).cast("decimal(17,2)")
    docs = docs.withColumn("vl_cte", vl_expr)

    docs = (
        docs.withColumn("is_cancelado",   F.lit(False).cast("boolean"))
            .withColumn("is_substituido", F.lit(False).cast("boolean"))
            .withColumn("has_inutil",     F.lit(False).cast("boolean"))
            .withColumn("has_cce",        F.lit(False).cast("boolean"))
    )

    evt_df = _load_eventos_cte(spark, settings, data_inicio=data_inicio, data_fim=data_fim, chcte=chcte)
    if evt_df is not None and all(c in evt_df.columns for c in [KEY_COL, "tpEvento"]):
        ev_cancel = DEFAULT_EVT_CANCEL
        ev_subst  = DEFAULT_EVT_SUBST
        ev_cce    = DEFAULT_EVT_CCE
        ev_inut   = DEFAULT_EVT_INUT

        evt_agg = (
            evt_df
            .withColumn("tp", F.col("tpEvento").cast("string"))
            .groupBy(KEY_COL)
            .agg(
                F.max(F.when(F.col("tp").isin(list(ev_cancel)), F.lit(True)).otherwise(F.lit(False))).alias("evt_cancel"),
                F.max(F.when(F.col("tp").isin(list(ev_subst)),  F.lit(True)).otherwise(F.lit(False))).alias("evt_subst"),
                F.max(F.when(F.col("tp").isin(list(ev_cce)),    F.lit(True)).otherwise(F.lit(False))).alias("evt_cce"),
                F.max(F.when(F.col("tp").isin(list(ev_inut)),   F.lit(True)).otherwise(F.lit(False))).alias("evt_inut"),
            )
        )
        docs = (
            docs.alias("d").join(evt_agg.alias("e"), on=KEY_COL, how="left")
                .withColumn("is_cancelado",   F.coalesce(F.col("e.evt_cancel"), F.col("is_cancelado")))
                .withColumn("is_substituido", F.coalesce(F.col("e.evt_subst"),  F.col("is_substituido")))
                .withColumn("has_cce",        F.coalesce(F.col("e.evt_cce"),    F.col("has_cce")))
                .withColumn("has_inutil",     F.coalesce(F.col("e.evt_inut"),   F.col("has_inutil")))
        )

    return docs

# =============================================================================
# Regras / projeções (Documento CT-e)
# =============================================================================

def _compute_codg_motivo_exclusao_doc_cte(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "codg_motivo_exclusao_doc",
        F.when(F.col("is_cancelado") == True, F.lit(10))
         .when(F.col("is_substituido") == True, F.lit(13))
         .otherwise(F.lit(0))
    ).withColumn(
        "indi_aprop_doc", F.when(F.col("codg_motivo_exclusao_doc") == F.lit(0), F.lit("S")).otherwise(F.lit("N"))
    ).withColumn(
        "valr_adicionado_doc",
        F.when(F.col("codg_motivo_exclusao_doc") == F.lit(0), F.col("vl_cte").cast("decimal(17,2)"))
         .otherwise(F.lit(0).cast("decimal(17,2)"))
    )

def _project_document_ipm_cte(
    spark: SparkSession, settings: Settings, docs_part: DataFrame, audit_id: Optional[int]
) -> DataFrame:
    def _detect_digits(field_name: str, default: int = 6) -> int:
        full = f"{settings.iceberg.catalog}.{settings.iceberg.namespace}.{settings.iceberg.tbl_cte_documento_partct}"
        try:
            schema = spark.table(full).schema
            f = next((x for x in schema.fields if x.name.lower() == field_name.lower()), None)
            if f and isinstance(f.dataType, T.DecimalType):
                return 7 if getattr(f.dataType, "precision", 6) >= 7 else 6
        except Exception as e:
            print(f"[proj][cte] Não detectei {field_name} em {full}: {e}. Usando {default}.")
        return default

    def _cast_ibge(scol: str, digits: int) -> F.Column:
        return _digits_only(F.col(scol)).cast(f"decimal({digits},0)")

    ent_digits = _detect_digits("codg_municipio_entrada_cad", 6)
    sai_digits = _detect_digits("codg_municipio_saida_cad", 6)

    docs = docs_part.withColumn("numr_ref_aaaamm", F.date_format(F.col("data_emissao_dt"), "yyyyMM").cast("int"))
    id_proc = F.lit(int(audit_id) if audit_id is not None else None).cast("decimal(9,0)")

    out = docs.select(
        F.col(KEY_COL).alias("codg_chave_acesso_nfe"),
        F.lit(TIPO_DOCUMENTO_FISCAL_CTE).alias("tipo_documento_fiscal"),
        # ENTRADA (tomador)
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
        F.col("vl_cte").cast("decimal(17,2)").alias("valr_nota_fiscal"),
        # regras doc
        F.col("valr_adicionado_doc").cast("decimal(17,2)").alias("valr_adicionado_operacao"),
        F.col("indi_aprop_doc").alias("indi_aprop"),
        F.lit(TIPO_DOC_PARTCT_CTE).cast("decimal(3,0)").alias("codg_tipo_doc_partct_calc"),
        F.col("codg_motivo_exclusao_doc").cast("decimal(2,0)").alias("codg_motivo_exclusao_calculo"),
        # referência e auditoria
        F.col("numr_ref_aaaamm").cast("decimal(6,0)").alias("numr_referencia_documento"),
        id_proc.alias("id_procesm_indice"),
    )
    return out

# =============================================================================
# Pipeline principal (CT-e)
# =============================================================================

def run_cte(
    spark: SparkSession,
    settings: Settings,
    *,
    data_inicio: str,
    data_fim: str,
    where_docs: Optional[str],
    prefer_day_partition: bool = False,
    audit_params: Optional[Dict] = None,
    audit_enabled: bool = True,
    chcte: Optional[str] = None,
    where_itens: Optional[str] = None,
    **kwargs
) -> Dict[str, int]:
    metrics: Dict[str, int] = {}

    audit_id = None
    if audit_enabled:
        params_json = json.dumps(
            audit_params
            or {
                "fonte": "KUDU",
                "prefer_day_partition": prefer_day_partition,
                "range_where": _mk_kudu_between_where(data_inicio, data_fim, chcte),
                "where_docs": where_docs,
            },
            ensure_ascii=False,
        )
        audit_id = start_and_get_id(
            spark, settings, documento="CTE",
            data_inicio=data_inicio, data_fim=data_fim, params_json=params_json,
        )
        metrics["audit_id"] = audit_id

    try:
        df_docs_base = _prepare_docs_cte(
            spark, settings,
            data_inicio=data_inicio, data_fim=data_fim, chcte=chcte, where_docs=where_docs
        ).persist()

        metrics["docs_lidos"] = _safe_count(df_docs_base, kind="docs_lidos")

        if audit_enabled and audit_id is not None and metrics["docs_lidos"] >= 0:
            bump_counts(spark, settings, id_procesm_indice=audit_id, add_docs=metrics["docs_lidos"], add_itens=0)

        df_docs_rules = _compute_codg_motivo_exclusao_doc_cte(df_docs_base)

        df_docs_part = df_docs_rules.filter(F.col("codg_motivo_exclusao_doc") == F.lit(0)).cache()
        metrics["docs_participantes"] = _safe_count(df_docs_part, kind="docs_participantes")

        df_docs_out = _project_document_ipm_cte(spark, settings, df_docs_part, audit_id=audit_id)

        target_docs_tbl = settings.iceberg.tbl_cte_documento_partct

        def _align_df_to_iceberg_table(df: DataFrame, table_name: str) -> DataFrame:
            full = f"{settings.iceberg.catalog}.{settings.iceberg.namespace}.{table_name}"
            try:
                target = spark.table(full).schema
            except Exception as e:
                print(f"[align][cte] Não consegui ler schema de {full}. Seguindo sem alinhamento. Detalhe: {e}")
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

        try:
            empty_items_schema_tbl = f"{settings.iceberg.catalog}.{settings.iceberg.namespace}.{settings.iceberg.tbl_cte_item_documento}"
            if spark.catalog.tableExists(empty_items_schema_tbl):
                empty_items = spark.createDataFrame([], spark.table(empty_items_schema_tbl).schema)
                write_df(
                    empty_items,
                    settings=settings,
                    format="iceberg",
                    table=settings.iceberg.tbl_cte_item_documento,
                    mode="append",
                    spark_session=spark,
                )
            metrics["itens_finais"] = 0
        except Exception:
            print("[cte] Tabela de itens CT-e não configurada; pulando escrita de itens.")
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
    p = argparse.ArgumentParser(description="Pipeline CT-e (Kudu -> Iceberg)")
    p.add_argument("--data-inicio", required=True, help="AAAA-MM-DD")
    p.add_argument("--data-fim", required=True, help="AAAA-MM-DD")
    p.add_argument("--where-docs", default=None)
    p.add_argument("--prefer-day-partition", default="false")
    p.add_argument("--print-settings", default="false")
    p.add_argument("--no-audit", action="store_true")
    p.add_argument("--chcte", default=None)
    return p.parse_args(argv)

def main(argv=None) -> None:
    settings = load_settings_from_env()
    spark = build_spark(settings)
    args = parse_args(argv)

    prefer_day_partition = _parse_bool(args.prefer_day_partition)

    if _parse_bool(args.print_settings):
        print_settings(settings)

    metrics = run_cte(
        spark,
        settings,
        data_inicio=args.data_inicio,
        data_fim=args.data_fim,
        where_docs=args.where_docs,
        prefer_day_partition=prefer_day_partition,
        audit_params={
            "cli": True,
            "range_where": _mk_kudu_between_where(args.data_inicio, args.data_fim, args.chcte),
            "where_docs": args.where_docs,
        },
        audit_enabled=(not args.no-audit),
        chcte=args.chcte,
    )

    print("=== CT-e Metrics ===")
    for k in sorted(metrics.keys()):
        print(f"{k}: {metrics[k]}")

    spark.stop()

if __name__ == "__main__":
    main()
