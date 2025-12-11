# -*- coding: utf-8 -*-
# app/nfe.py
from __future__ import annotations

import argparse
import json
import os
from typing import Dict, Optional, Tuple, List

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from app.settings import Settings, load_settings_from_env, build_spark, print_settings
from app.utils.io import write_df, read_oracle_table
from app.utils.audit import start_and_get_id, bump_counts, finish_success, finish_error
from app.utils.rules import (
    build_rules_context,
    apply_rules_to_documents,
    apply_rules_to_items,
)

# =============================================================================
# Helpers de qualificação de tabela / filtros
# =============================================================================
def _qualify_kudu_table(db: str, name: str, *, for_format: str = "kudu") -> str:
    n = (name or "").strip()
    if n.lower().startswith("impala::"):
        n = n.split("::", 1)[1]
    elif n.lower().startswith("kudu."):
        n = ".".join(n.split(".", 2)[1:])
    if "." not in n:
        n = f"{db}.{n}"
    if for_format.lower() in ("kudu", "impala"):
        return f"impala::{n}"
    return n


def _mk_kudu_between_where(
    data_inicio: str, data_fim: str, chavenfe: Optional[str] = None
) -> str:
    di = f"{data_inicio.strip()} 00:00:00"
    df = f"{data_fim.strip()} 23:59:59"
    base = (
        f"ide_dhemi_nums BETWEEN unix_timestamp('{di}') "
        f"AND unix_timestamp('{df}')"
    )
    if chavenfe:
        base += f" AND chavenfe = '{chavenfe.strip()}'"
    return base


def _merge_where(base_where: Optional[str], extra_where: Optional[str]) -> Optional[str]:
    if base_where and extra_where:
        return f"({base_where}) AND ({extra_where})"
    return base_where or extra_where


def _read_kudu(
    spark: SparkSession,
    settings: Settings,
    table_name: str,
    *,
    where: Optional[str],
    columns: Optional[List[str]] = None,
) -> DataFrame:
    kudu_fmt = os.getenv("KUDU_FORMAT", "kudu")
    qualified = _qualify_kudu_table(settings.kudu.database, table_name, for_format=kudu_fmt)
    masters = os.getenv("KUDU_MASTERS", settings.kudu.masters)
    print(f"[nfe] KUDU READ | format={kudu_fmt} | masters={masters} | kudu.table={qualified}")

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
        print(f"[nfe] KUDU WHERE (pushdown): {where}")
        df = df.where(where)
    if columns:
        keep = [c for c in columns if c in df.columns]
        if keep:
            df = df.select(*keep)
    return df


# =============================================================================
# Helpers de contagem/normalização
# =============================================================================
def _safe_count(df: DataFrame, *, kind: str = "count") -> int:
    try:
        return int(df.count())
    except Exception as e:
        msg = str(e)
        if ("Tablet is lagging too much" in msg) or ("SERVICE_UNAVAILABLE" in msg):
            print(f"[nfe][WARN] Kudu instável em {kind}; usando -1. Detalhe: {msg[:300]}")
            return -1
        raise


def _safe_approx_distinct(df: DataFrame, col: str, *, kind: str = "approx_count_distinct") -> int:
    try:
        return int(df.select(F.approx_count_distinct(col).alias("n")).collect()[0]["n"])
    except Exception as e:
        msg = str(e)
        if ("Tablet is lagging too much" in msg) or ("SERVICE_UNAVAILABLE" in msg):
            print(f"[nfe][WARN] Kudu instável em {kind}({col}); usando -1. Detalhe: {msg[:300]}")
            return -1
        raise


def _digits_only(col: F.Column) -> F.Column:
    return F.regexp_replace(F.trim(col.cast("string")), r"\D", "")


# =============================================================================
# Leitura (Kudu)
# =============================================================================
def _load_ident(
    spark: SparkSession,
    settings: Settings,
    *,
    data_inicio: str,
    data_fim: str,
    chavenfe: Optional[str],
    where_extra: Optional[str] = None,
) -> DataFrame:
    where_range = _merge_where(_mk_kudu_between_where(data_inicio, data_fim, chavenfe=chavenfe), where_extra)
    cols = [
        "chavenfe",
        "ide_dhemi_nums",
        "ide_mod",
        "ide_tpamb",
        "ide_cuf",
        "ide_cmunfg",
        "emit_cnpj", "emit_cpf",
        "dest_cnpj", "dest_cpf",
    ]
    df = _read_kudu(
        spark, settings, settings.kudu.nfe_ident_table,
        where=where_range, columns=cols
    )
    df = df.filter(
        (F.col("ide_mod").cast("string") == F.lit("55")) &
        (F.col("ide_tpamb").cast("string") == F.lit("1"))
    )
    return df


def _load_itens(
    spark: SparkSession,
    settings: Settings,
    *,
    data_inicio: str,
    data_fim: str,
    chavenfe: Optional[str],
    where_extra: Optional[str] = None,
) -> DataFrame:
    where_range = _merge_where(_mk_kudu_between_where(data_inicio, data_fim, chavenfe=chavenfe), where_extra)
    cols = [
        "chavenfe",
        "det_nitem",
        "cfop",
        "ncm",
        "qcom",
        "vprod",
        "vdesc",
        "vfrete",
        "vseg",
        "voutro",
        "cean",
        "ceantrib",
        "cbarra",
        "cest",
    ]
    return _read_kudu(spark, settings, settings.kudu.nfe_item_table, where=where_range, columns=cols)


def _load_emit(
    spark: SparkSession,
    settings: Settings,
    *,
    data_inicio: str,
    data_fim: str,
    chavenfe: Optional[str],
    where_extra: Optional[str] = None,
) -> DataFrame:
    return _read_kudu(
        spark, settings, settings.kudu.nfe_emit_table,
        where=_merge_where(_mk_kudu_between_where(data_inicio, data_fim, chavenfe), where_extra),
    )


def _load_dest(
    spark: SparkSession,
    settings: Settings,
    *,
    data_inicio: str,
    data_fim: str,
    chavenfe: Optional[str],
    where_extra: Optional[str] = None,
) -> DataFrame:
    return _read_kudu(
        spark, settings, settings.kudu.nfe_dest_table,
        where=_merge_where(_mk_kudu_between_where(data_inicio, data_fim, chavenfe), where_extra),
    )


def _load_total(
    spark: SparkSession,
    settings: Settings,
    *,
    data_inicio: str,
    data_fim: str,
    chavenfe: Optional[str],
    where_extra: Optional[str] = None,
) -> DataFrame:
    return _read_kudu(
        spark, settings, settings.kudu.nfe_total_table,
        where=_merge_where(_mk_kudu_between_where(data_inicio, data_fim, chavenfe), where_extra),
    )


def _load_icms_item(
    spark: SparkSession,
    settings: Settings,
    *,
    data_inicio: str,
    data_fim: str,
    chavenfe: Optional[str],
    where_extra: Optional[str] = None,
) -> DataFrame:
    """
    Lê a tabela de ICMS dos itens. Garante que as colunas de motivo de desoneração
    existam (preenchendo com NULL quando ausentes) para permitir o mapeamento de
    tipo_motivo_desonera_icms.
    """
    df = _read_kudu(
        spark, settings, settings.kudu.nfe_icms_table,
        where=_merge_where(_mk_kudu_between_where(data_inicio, data_fim, chavenfe), where_extra),
    )
    # Garante colunas de motivo (se não existirem no Kudu)
    needed = [
        "icms40_41_50_motdesicms",
        "icms30_motdesicms",
        "icms70_motdesicms",
        "icms90_motdesicms",
        "icms20_motdesicms",
    ]
    for c in needed:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))
    return df


def _load_ipi_item(
    spark: SparkSession,
    settings: Settings,
    *,
    data_inicio: str,
    data_fim: str,
    chavenfe: Optional[str],
    where_extra: Optional[str] = None,
) -> DataFrame:
    return _read_kudu(
        spark, settings, settings.kudu.nfe_ipi_item_table,
        where=_merge_where(_mk_kudu_between_where(data_inicio, data_fim, chavenfe), where_extra),
    )


def _load_ii_item(
    spark: SparkSession,
    settings: Settings,
    *,
    data_inicio: str,
    data_fim: str,
    chavenfe: Optional[str],
    where_extra: Optional[str] = None,
) -> DataFrame:
    return _read_kudu(
        spark, settings, settings.kudu.nfe_ii_item_table,
        where=_merge_where(_mk_kudu_between_where(data_inicio, data_fim, chavenfe), where_extra),
    )


def _load_comb_item(
    spark: SparkSession,
    settings: Settings,
    *,
    data_inicio: str,
    data_fim: str,
    chavenfe: Optional[str],
    where_extra: Optional[str] = None,
) -> DataFrame:
    return _read_kudu(
        spark, settings, settings.kudu.nfe_comb_table,
        where=_merge_where(_mk_kudu_between_where(data_inicio, data_fim, chavenfe), where_extra),
    )


# =============================================================================
# Normalizações auxiliares
# =============================================================================
def _coalesce_doc_number(cnpj_col: F.Column, cpf_col: F.Column) -> F.Column:
    return F.coalesce(_digits_only(cnpj_col), _digits_only(cpf_col))


def _str_upper(col: F.Column) -> F.Column:
    return F.upper(F.trim(col.cast("string")))


def _ensure_cols(df: DataFrame, mapping: Dict[str, F.Column]) -> DataFrame:
    cur = df
    for name, expr in mapping.items():
        if name not in cur.columns:
            cur = cur.withColumn(name, expr)
    return cur


def _ensure_alias(df: DataFrame, target: str, source: str) -> DataFrame:
    if target not in df.columns and source in df.columns:
        return df.withColumn(target, F.col(source))
    return df


def _prod_flag_expr(df: DataFrame, name: str) -> F.Column:
    base = F.col(name) if name in df.columns else F.lit(None)
    return F.coalesce(base.cast("boolean"), (F.upper(base.cast("string")) == F.lit("PROD")), F.lit(False))


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


# =============================================================================
# Preparação de documentos
# =============================================================================
def _prepare_docs(
    spark: SparkSession,
    settings: Settings,
    *,
    data_inicio: str,
    data_fim: str,
    chavenfe: Optional[str],
    where_docs: Optional[str] = None,
) -> DataFrame:
    ident = _load_ident(
        spark, settings, data_inicio=data_inicio, data_fim=data_fim, chavenfe=chavenfe, where_extra=where_docs
    )
    emit = _load_emit(
        spark, settings, data_inicio=data_inicio, data_fim=data_fim, chavenfe=chavenfe, where_extra=where_docs
    ).select(
        "chavenfe",
        _digits_only(F.col("cmun")).alias("emit_cmun_cad_str"),
        F.col("uf").alias("emit_uf_cad"),
        F.col("ie").alias("emit_ie"),
    )
    dest = _load_dest(
        spark, settings, data_inicio=data_inicio, data_fim=data_fim, chavenfe=chavenfe, where_extra=where_docs
    ).select(
        "chavenfe",
        _digits_only(F.col("cmun")).alias("dest_cmun_cad_str"),
        F.col("uf").alias("dest_uf_cad"),
        F.col("ie").alias("dest_ie"),
        F.col("indiedest").alias("dest_ind_ie"),
    )
    total = _load_total(
        spark, settings, data_inicio=data_inicio, data_fim=data_fim, chavenfe=chavenfe, where_extra=where_docs
    ).select(
        "chavenfe", "vnf", "vprod", "vfrete", "vseg", "vdesc", "voutro", "vii", "vipi", "vicmsdeson", "vst"
    )

    docs = (
        ident.alias("i")
        .join(emit.alias("e"), on="chavenfe", how="left")
        .join(dest.alias("d"), on="chavenfe", how="left")
        .join(total.alias("t"), on="chavenfe", how="left")
    )

    docs = (
        docs.withColumn("data_emissao_dt", F.to_date(F.from_unixtime(F.col("i.ide_dhemi_nums").cast("bigint"))))
        .withColumn("data_emissao_ts", F.to_timestamp(F.from_unixtime(F.col("i.ide_dhemi_nums").cast("bigint"))))
        .withColumn("emit_doc_ident", _coalesce_doc_number(F.col("i.emit_cnpj"), F.col("i.emit_cpf")))
        .withColumn("dest_doc_ident", _coalesce_doc_number(F.col("i.dest_cnpj"), F.col("i.dest_cpf")))
        .withColumn("emit_uf", _str_upper(F.col("e.emit_uf_cad")))
        .withColumn("dest_uf", _str_upper(F.col("d.dest_uf_cad")))
        .withColumn("doc_munfg", F.col("i.ide_cmunfg").cast("int"))
        .withColumn("i_uf_from_cuf", _uf_from_cuf_expr("i.ide_cuf"))
    )

    GO = F.lit("GO")
    lado_go = (
        F.when((F.col("emit_uf") == GO) | (F.col("emit_uf").isNull() & (F.col("i_uf_from_cuf") == GO)), F.lit("EMIT"))
        .when(F.col("dest_uf") == GO, F.lit("DEST"))
        .otherwise(F.lit(None))
    )
    docs = docs.withColumn("lado_go", lado_go).filter(F.col("lado_go").isNotNull())
    docs = docs.withColumn("tipo_doc_go", F.when(F.col("lado_go") == F.lit("EMIT"), F.lit("1")).otherwise(F.lit("0")))

    docs = (
        docs
        # ENTRADA
        .withColumn("cad_entrada_docnum", F.col("dest_doc_ident"))
        .withColumn("cad_entrada_ie", F.col("d.dest_ie"))
        .withColumn("cad_entrada_ie_digits", _digits_only(F.col("d.dest_ie")))
        .withColumn("cad_entrada_cmun_ibge_str", F.col("d.dest_cmun_cad_str"))
        .withColumn("cad_entrada_uf", F.col("d.dest_uf_cad"))
        .withColumn("doc_entrada_cmun", F.col("doc_munfg").cast("int"))
        .withColumn("doc_entrada_uf", F.col("i_uf_from_cuf"))
        # SAÍDA
        .withColumn("cad_saida_docnum", F.col("emit_doc_ident"))
        .withColumn("cad_saida_ie", F.col("e.emit_ie"))
        .withColumn("cad_saida_ie_digits", _digits_only(F.col("e.emit_ie")))
        .withColumn("cad_saida_cmun_ibge_str", F.col("e.emit_cmun_cad_str"))
        .withColumn("cad_saida_uf", F.col("e.emit_uf_cad"))
        .withColumn("doc_saida_cmun", F.col("doc_munfg").cast("int"))
        .withColumn("doc_saida_uf", F.col("i_uf_from_cuf"))
        # Backups para projeção
        .withColumn("cad_entrada_uf_src", F.upper(F.col("cad_entrada_uf")))
        .withColumn("doc_entrada_uf_src", F.upper(F.col("doc_entrada_uf")))
        .withColumn("cad_saida_uf_src", F.upper(F.col("cad_saida_uf")))
        .withColumn("doc_saida_uf_src", F.upper(F.col("doc_saida_uf")))
        .withColumn("doc_entrada_cmun_ibge", F.col("doc_entrada_cmun"))
        .withColumn("doc_entrada_cmun_ibge_str", F.col("doc_entrada_cmun").cast("string"))
        .withColumn("doc_saida_cmun_ibge", F.col("doc_saida_cmun"))
        .withColumn("doc_saida_cmun_ibge_str", F.col("doc_saida_cmun").cast("string"))
    )

    docs = docs.withColumn("vl_nf", F.col("t.vnf").cast(T.DecimalType(17, 2)))
    return docs


# =============================================================================
# Helpers específicos de ITENS (novos)
# =============================================================================
def _normalize_cfop(df: DataFrame) -> DataFrame:
    """Garante coluna 'cfop' normalizada (numérica em string, apenas dígitos).
       Se vier 'CFOP' (ou outra variação de caixa), renomeia; se não existir, cria nula."""
    cols_map = {c.lower(): c for c in df.columns}
    out = df
    if 'cfop' in cols_map and cols_map['cfop'] != 'cfop':
        out = out.withColumnRenamed(cols_map['cfop'], 'cfop')
    elif 'cfop' not in cols_map:
        out = out.withColumn('cfop', F.lit(None).cast('string'))
    # normaliza para somente dígitos
    out = out.withColumn('cfop', F.regexp_replace(F.upper(F.trim(F.col('cfop'))), r'[^0-9]', ''))
    return out


def _attach_codg_tipo_doc_partct_por_cfop(
    spark: SparkSession, settings: Settings, df_itens: DataFrame
) -> DataFrame:
    """
    Enriquece os itens com codg_tipo_doc_partct via CFOP (linha mais recente por CFOP).
    - À prova de ausência/variação de caixa da coluna 'cfop';
    - Se tabela de mapeamento não estiver acessível, apenas garante a coluna destino.
    """
    if df_itens is None:
        return None

    itens_enr = _normalize_cfop(df_itens)

    # Tenta ler o mapeamento do Oracle
    mapa_cfop = None
    try:
        mapa_cfop = read_oracle_table(spark, settings, "IPM.IPM_CFOP_PARTICIPANTE")
    except Exception as _e:
        mapa_cfop = None

    if mapa_cfop is not None:
        mcols = {c.lower(): c for c in mapa_cfop.columns}

        # padroniza 'cfop' no mapeamento
        if 'cfop' in mcols and mcols['cfop'] != 'cfop':
            mapa_cfop = mapa_cfop.withColumnRenamed(mcols['cfop'], 'cfop')
        elif 'cfop' not in mcols:
            mapa_cfop = None

        if mapa_cfop is not None:
            mapa_cfop = mapa_cfop.withColumn(
                'cfop', F.regexp_replace(F.upper(F.trim(F.col('cfop'))), r'[^0-9]', '')
            )
            # padroniza coluna do tipo doc
            if 'codg_tipo_doc_partct' not in mapa_cfop.columns:
                for cand in ['CODG_TIPO_DOC_PARTCT', 'tipo_doc_particip', 'tipo_participacao']:
                    if cand in mapa_cfop.columns:
                        mapa_cfop = mapa_cfop.withColumnRenamed(cand, 'codg_tipo_doc_partct')
                        break
            if 'codg_tipo_doc_partct' not in mapa_cfop.columns:
                mapa_cfop = mapa_cfop.withColumn('codg_tipo_doc_partct', F.lit(None).cast('int'))

            # vigência: pega a mais recente por CFOP
            w = Window.partitionBy('cfop').orderBy(F.col('DATA_INICIO_VIGENCIA').desc_nulls_last())
            mapa_cfop_latest = (
                mapa_cfop
                .withColumn('_rn', F.row_number().over(w))
                .where(F.col('_rn') == 1)
                .select('cfop', F.col('codg_tipo_doc_partct').alias('tp_doc_cfop'))
            )

            itens_enr = (
                itens_enr.alias('i')
                .join(F.broadcast(mapa_cfop_latest.alias('m')), on='cfop', how='left')
            )
        # fim if mapa_cfop is not None

    # garante a coluna final mesmo sem mapeamento
    if 'tp_doc_cfop' in itens_enr.columns:
        itens_enr = itens_enr.withColumn(
            'codg_tipo_doc_partct_documento',
            F.coalesce(F.col('codg_tipo_doc_partct_documento'), F.col('tp_doc_cfop')).cast('int')
        ).drop('tp_doc_cfop')
    else:
        if 'codg_tipo_doc_partct_documento' not in itens_enr.columns:
            itens_enr = itens_enr.withColumn('codg_tipo_doc_partct_documento', F.lit(None).cast('int'))

    return itens_enr


def _derive_tipo_motivo_desonera_icms(df: DataFrame) -> DataFrame:
    """
    Define tipo_motivo_desonera_icms conforme prioridade:
    40/41/50 → 30 → 70 → 90 → 20. Grava a origem textual (ex.: 'ICMS40_41_50').
    """
    motivo = F.coalesce(
        F.when(F.col("icms40_41_50_motdesicms").isNotNull(), F.col("icms40_41_50_motdesicms")),
        F.when(F.col("icms30_motdesicms").isNotNull(),      F.col("icms30_motdesicms")),
        F.when(F.col("icms70_motdesicms").isNotNull(),      F.col("icms70_motdesicms")),
        F.when(F.col("icms90_motdesicms").isNotNull(),      F.col("icms90_motdesicms")),
        F.when(F.col("icms20_motdesicms").isNotNull(),      F.col("icms20_motdesicms")),
    )

    origem = F.coalesce(
        F.when(F.col("icms40_41_50_motdesicms").isNotNull(), F.lit("ICMS40_41_50")),
        F.when(F.col("icms30_motdesicms").isNotNull(),       F.lit("ICMS30")),
        F.when(F.col("icms70_motdesicms").isNotNull(),       F.lit("ICMS70")),
        F.when(F.col("icms90_motdesicms").isNotNull(),       F.lit("ICMS90")),
        F.when(F.col("icms20_motdesicms").isNotNull(),       F.lit("ICMS20")),
    )

    return (
        df
        .withColumn("tipo_motivo_desonera_icms", F.when(motivo.isNotNull(), origem).otherwise(F.lit(None)))
    )


# =============================================================================
# Preparação de itens
# =============================================================================
def _prepare_items(
    spark: SparkSession,
    settings: Settings,
    *,
    data_inicio: str,
    data_fim: str,
    chavenfe: Optional[str],
    where_itens: Optional[str] = None,
) -> Tuple[DataFrame, Dict[str, int]]:
    itens = _load_itens(
        spark, settings, data_inicio=data_inicio, data_fim=data_fim, chavenfe=chavenfe, where_extra=where_itens
    )
    icms = _load_icms_item(
        spark, settings, data_inicio=data_inicio, data_fim=data_fim, chavenfe=chavenfe, where_extra=where_itens
    ).select(
        "chavenfe",
        "det_nitem",
        "icms10_vicmsst",
        "icms70_vicmsst",
        "icms30_vicmsst",
        "icms90_vicmsst",
        "icms40_41_50_vicmsdeson",
        "icms02_vicmsmono",
        "icms15_vicmsmono",
        "icms15_vicmsmonoreten",
        "icms53_vicmsmono",
        # motivos para derivar tipo_motivo_desonera_icms
        "icms40_41_50_motdesicms",
        "icms30_motdesicms",
        "icms70_motdesicms",
        "icms90_motdesicms",
        "icms20_motdesicms",
    )
    ipi = _load_ipi_item(
        spark, settings, data_inicio=data_inicio, data_fim=data_fim, chavenfe=chavenfe, where_extra=where_itens
    ).select(
        "chavenfe", "det_nitem", F.col("ipitrib_vipi").alias("ipi_vipi")
    )
    ii = _load_ii_item(
        spark, settings, data_inicio=data_inicio, data_fim=data_fim, chavenfe=chavenfe, where_extra=where_itens
    ).select(
        "chavenfe", "det_nitem", F.col("vii").alias("ii_vii")
    )
    comb = _load_comb_item(
        spark, settings, data_inicio=data_inicio, data_fim=data_fim, chavenfe=chavenfe, where_extra=where_itens
    ).select(
        "chavenfe",
        "det_nitem",
        F.col("cprodanp").alias("anp_codigo"),
        F.col("valiqprod").alias("anp_valiq"),
        F.col("qbcprod").alias("anp_qbc"),
    )

    items_rules_metrics: Dict[str, int] = {}

    itens_enr = (
        itens.join(icms, on=["chavenfe", "det_nitem"], how="left")
        .join(ipi, on=["chavenfe", "det_nitem"], how="left")
        .join(ii, on=["chavenfe", "det_nitem"], how="left")
        .join(comb, on=["chavenfe", "det_nitem"], how="left")
        .withColumn(
            "valr_icms_st",
            F.coalesce(F.col("icms10_vicmsst"), F.lit(0.0))
            + F.coalesce(F.col("icms70_vicmsst"), F.lit(0.0))
            + F.coalesce(F.col("icms30_vicmsst"), F.lit(0.0))
            + F.coalesce(F.col("icms90_vicmsst"), F.lit(0.0)),
        )
        .withColumn(
            "valr_icms_mono",
            F.coalesce(F.col("icms02_vicmsmono"), F.lit(0.0))
            + F.coalesce(F.col("icms15_vicmsmono"), F.lit(0.0))
            + F.coalesce(F.col("icms15_vicmsmonoreten"), F.lit(0.0))
            + F.coalesce(F.col("icms53_vicmsmono"), F.lit(0.0)),
        )
        .withColumn("valr_icms_deson", F.coalesce(F.col("icms40_41_50_vicmsdeson"), F.lit(0.0)))
    )

    # (a) mapeia tipo de doc participante pelo CFOP (CFOPClasse do Django), robusto
    itens_enr = _attach_codg_tipo_doc_partct_por_cfop(spark, settings, itens_enr) or itens_enr

    # (b) deriva o tipo do motivo de desoneração do ICMS (prioridade 40/41/50 → 30 → 70 → 90 → 20)
    itens_enr = _derive_tipo_motivo_desonera_icms(itens_enr)

    return itens_enr, items_rules_metrics


# =============================================================================
# Enriquecimento Oracle: IPM_CONTRIBUINTE_IPM (cadastro por vigência)
# =============================================================================
def _enrich_docs_with_cadastro(
    spark: SparkSession, settings: Settings, df_docs_part: DataFrame
) -> DataFrame:
    """
    Junta cadastro de contribuinte do IPM (por UF + IE) válido na data de emissão.
    Faz duas junções (entrada/saida) e escolhe o registro com maior DATA_INICIO_VIGENCIA
    dentre os válidos para a data do documento.
    """
    cad = read_oracle_table(spark, settings, "IPM.IPM_CONTRIBUINTE_IPM").select(
        F.upper(F.col("CODG_UF")).alias("cad_CODG_UF"),
        F.col("NUMR_INSCRICAO_CONTRIB").cast("decimal(15,0)").alias("cad_NUMR_INSCRICAO_CONTRIB"),
        F.col("DATA_INICIO_VIGENCIA").alias("cad_DATA_INICIO_VIGENCIA"),
        F.col("DATA_FIM_VIGENCIA").alias("cad_DATA_FIM_VIGENCIA"),
        F.col("STAT_CADASTRO_CONTRIB").alias("cad_STAT_CADASTRO_CONTRIB"),
        F.col("TIPO_ENQDTO_FISCAL").alias("cad_TIPO_ENQDTO_FISCAL"),
        F.col("INDI_PRODUTOR_RURAL").alias("cad_INDI_PRODUTOR_RURAL"),
        F.col("INDI_PRODUTOR_RURAL_EXCLUSIVO").alias("cad_INDI_PRODUTOR_RURAL_EXCLUSIVO"),
    ).cache()

    # -------- ENTRADA --------
    ent = (
        df_docs_part.alias("d")
        .join(
            cad.alias("c"),
            (F.col("d.cad_entrada_uf_src") == F.col("c.cad_CODG_UF"))
            & (F.col("d.cad_entrada_ie_digits").cast("decimal(15,0)") == F.col("c.cad_NUMR_INSCRICAO_CONTRIB"))
            & (F.col("d.data_emissao_ts") >= F.col("c.cad_DATA_INICIO_VIGENCIA"))
            & (F.col("d.data_emissao_ts") <= F.coalesce(F.col("c.cad_DATA_FIM_VIGENCIA"), F.col("d.data_emissao_ts"))),
            "left",
        )
        .withColumn(
            "_rn_ent",
            F.row_number().over(
                Window.partitionBy("d.chavenfe").orderBy(F.col("c.cad_DATA_INICIO_VIGENCIA").desc_nulls_last())
            ),
        )
        .where(F.col("_rn_ent") == 1)
        .select(
            "d.*",
            F.col("c.cad_STAT_CADASTRO_CONTRIB").alias("stat_cadastro_entrada"),
            F.col("c.cad_TIPO_ENQDTO_FISCAL").alias("tipo_enqdto_fiscal_entrada"),
            F.col("c.cad_INDI_PRODUTOR_RURAL").alias("indi_prod_rural_entrada"),
            F.col("c.cad_INDI_PRODUTOR_RURAL_EXCLUSIVO").alias("indi_prod_rural_exclus_entrada"),
        )
    )

    # -------- SAÍDA --------
    sai = (
        ent.alias("d")
        .join(
            cad.alias("c"),
            (F.col("d.cad_saida_uf_src") == F.col("c.cad_CODG_UF"))
            & (F.col("d.cad_saida_ie_digits").cast("decimal(15,0)") == F.col("c.cad_NUMR_INSCRICAO_CONTRIB"))
            & (F.col("d.data_emissao_ts") >= F.col("c.cad_DATA_INICIO_VIGENCIA"))
            & (F.col("d.data_emissao_ts") <= F.coalesce(F.col("c.cad_DATA_FIM_VIGENCIA"), F.col("d.data_emissao_ts"))),
            "left",
        )
        .withColumn(
            "_rn_sai",
            F.row_number().over(
                Window.partitionBy("d.chavenfe").orderBy(F.col("c.cad_DATA_INICIO_VIGENCIA").desc_nulls_last())
            ),
        )
        .where(F.col("_rn_sai") == 1)
        .select(
            "d.*",
            F.col("c.cad_STAT_CADASTRO_CONTRIB").alias("stat_cadastro_saida"),
            F.col("c.cad_TIPO_ENQDTO_FISCAL").alias("tipo_enqdto_fiscal_saida"),
            F.col("c.cad_INDI_PRODUTOR_RURAL").alias("indi_prod_rural_saida"),
            F.col("c.cad_INDI_PRODUTOR_RURAL_EXCLUSIVO").alias("indi_prod_rural_exclus_saida"),
        )
    )

    return sai


# =============================================================================
# Projeções (Iceberg)
# =============================================================================
def _add_missing_cols(df: DataFrame, colnames: Tuple[str, ...]) -> DataFrame:
    for c in colnames:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))
    return df


def _detect_ibge_digits_for_cad(
    spark: SparkSession,
    settings: Settings,
    field_name: str = "codg_municipio_entrada_cad",
    default_digits: int = 6,
) -> int:
    full = f"{settings.iceberg.catalog}.{settings.iceberg.namespace}.{settings.iceberg.tbl_documento_partct}"
    try:
        schema = spark.table(full).schema
        f = next((x for x in schema.fields if x.name.lower() == field_name.lower()), None)
        if f and isinstance(f.dataType, T.DecimalType):
            return 7 if getattr(f.dataType, "precision", 6) >= 7 else 6
    except Exception as e:
        print(f"[proj] Aviso: não foi possível detectar precisão de {field_name} no Iceberg ({e}). Usando {default_digits}.")
    return default_digits


def _truncate_ibge_str(col_str: F.Column, target_digits: int) -> F.Column:
    digits = F.regexp_replace(col_str.cast("string"), r"\D", "")
    return F.when(F.length(digits) > F.lit(target_digits), F.substring(digits, 1, target_digits)).otherwise(digits)


def _cast_ibge_num_or_str(num_col: str, str_col: str, digits: int) -> F.Column:
    str_cut = _truncate_ibge_str(F.col(str_col), digits)
    return F.coalesce(
        F.col(num_col).cast(f"decimal({digits},0)"),
        str_cut.cast(f"decimal({digits},0)"),
    ).cast(f"decimal({digits},0)")


def _project_document_ipm(
    spark: SparkSession,
    settings: Settings,
    docs_part: DataFrame,
    audit_id: Optional[int],
) -> DataFrame:
    entrada_cad_digits = _detect_ibge_digits_for_cad(
        spark, settings, field_name="codg_municipio_entrada_cad", default_digits=6
    )
    saida_cad_digits = _detect_ibge_digits_for_cad(
        spark, settings, field_name="codg_municipio_saida_cad", default_digits=6
    )

    docs = _add_missing_cols(
        docs_part,
        (
            "cad_entrada_cmun_ibge",
            "cad_entrada_cmun_ibge_str",
            "doc_entrada_cmun_ibge",
            "doc_entrada_cmun_ibge_str",
            "cad_saida_cmun_ibge",
            "cad_saida_cmun_ibge_str",
            "doc_saida_cmun_ibge",
            "doc_saida_cmun_ibge_str",
            "cad_entrada_uf_src",
            "doc_entrada_uf_src",
            "cad_saida_uf_src",
            "doc_saida_uf_src",
            "valr_adicionado_doc",
            "indi_aprop_doc",
            "codg_motivo_exclusao_doc",
            "id_procesm_indice",
            # novos: vindos do Oracle
            "stat_cadastro_entrada",
            "tipo_enqdto_fiscal_entrada",
            "indi_prod_rural_entrada",
            "indi_prod_rural_exclus_entrada",
            "stat_cadastro_saida",
            "tipo_enqdto_fiscal_saida",
            "indi_prod_rural_saida",
            "indi_prod_rural_exclus_saida",
        ),
    ).withColumn(
        "cad_entrada_cmun_ibge", F.col("cad_entrada_cmun_ibge").cast("decimal(7,0)")
    ).withColumn(
        "cad_saida_cmun_ibge", F.col("cad_saida_cmun_ibge").cast("decimal(7,0)")
    )

    docs = docs.withColumn("numr_ref_aaaamm", F.date_format(F.col("data_emissao_dt"), "yyyyMM").cast("int"))

    out = docs.select(
        F.col("chavenfe").alias("codg_chave_acesso_nfe"),
        F.col("tipo_doc_go").alias("tipo_documento_fiscal"),
        # ENTRADA
        F.col("cad_entrada_docnum").alias("numr_cpf_cnpj_entrada"),
        F.col("cad_entrada_ie").cast("decimal(15,0)").alias("numr_inscricao_entrada"),
        F.col("stat_cadastro_entrada").alias("stat_cadastro_entrada"),
        F.col("tipo_enqdto_fiscal_entrada").alias("tipo_enqdto_fiscal_entrada"),
        F.col("indi_prod_rural_entrada").alias("indi_prod_rural_entrada"),
        F.col("indi_prod_rural_exclus_entrada").alias("indi_prod_rural_exclus_entrada"),
        _cast_ibge_num_or_str("cad_entrada_cmun_ibge", "cad_entrada_cmun_ibge_str", entrada_cad_digits).alias("codg_municipio_entrada_cad"),
        F.col("cad_entrada_uf_src").alias("codg_uf_entrada_cad"),
        _cast_ibge_num_or_str("doc_entrada_cmun_ibge", "doc_entrada_cmun_ibge_str", 7).alias("codg_municipio_entrada_doc"),
        F.col("doc_entrada_uf_src").alias("codg_uf_entrada_doc"),
        # SAÍDA
        F.col("cad_saida_docnum").alias("numr_cpf_cnpj_saida"),
        F.col("cad_saida_ie").cast("decimal(14,0)").alias("numr_inscricao_saida"),
        F.col("stat_cadastro_saida").alias("stat_cadastro_saida"),
        F.col("tipo_enqdto_fiscal_saida").alias("tipo_enqdto_fiscal_saida"),
        F.col("indi_prod_rural_saida").alias("indi_prod_rural_saida"),
        F.col("indi_prod_rural_exclus_saida").alias("indi_prod_rural_exclus_saida"),
        _cast_ibge_num_or_str("cad_saida_cmun_ibge", "cad_saida_cmun_ibge_str", saida_cad_digits).alias("codg_municipio_saida_cad"),
        F.col("cad_saida_uf_src").alias("codg_uf_saida_cad"),
        _cast_ibge_num_or_str("doc_saida_cmun_ibge", "doc_saida_cmun_ibge_str", 7).alias("codg_municipio_saida_doc"),
        F.col("doc_saida_uf_src").alias("codg_uf_saida_doc"),
        # datas/valores
        F.col("data_emissao_dt").alias("data_emissao_nfe"),
        F.col("vl_nf").cast("decimal(17,2)").alias("valr_nota_fiscal"),
        # campos de regra (documento)
        F.col("valr_adicionado_doc").cast("decimal(17,2)").alias("valr_adicionado_operacao"),
        F.col("indi_aprop_doc").alias("indi_aprop"),
        F.lit(10).cast("decimal(3,0)").alias("codg_tipo_doc_partct_calc"),  # NFe = 10
        F.col("codg_motivo_exclusao_doc").cast("decimal(2,0)").alias("codg_motivo_exclusao_calculo"),
        # referência e auditoria
        F.col("numr_ref_aaaamm").cast("decimal(6,0)").alias("numr_referencia_documento"),
        F.col("id_procesm_indice").cast("decimal(9,0)").alias("id_procesm_indice"),
    )
    return out


def _project_item_ipm(itens_final: DataFrame, audit_id: Optional[int]) -> DataFrame:
    ean = F.coalesce(F.col("cean"), F.col("ceantrib"), F.col("cbarra"))
    itens_final = _add_missing_cols(
        itens_final,
        (
            "valr_adicionado",
            "codg_tipo_doc_partct_documento",
            "codg_motivo_exclusao_calculo",
            "id_procesm_indice",
            "tipo_motivo_desonera_icms",
        ),
    )
    out = itens_final.select(
        F.col("chavenfe").alias("codg_chave_acesso_nfe"),
        F.col("det_nitem").cast("decimal(3,0)").alias("numr_item"),
        F.col("cfop").cast("decimal(4,0)").alias("codg_cfop"),
        ean.alias("codg_ean"),
        F.col("cest").alias("codg_cest"),
        F.col("ncm").cast("decimal(9,0)").alias("codg_produto_ncm"),
        F.col("qcom").cast("decimal(19,4)").alias("qtde_comercial"),
        F.col("anp_codigo").cast("decimal(9,0)").alias("codg_produto_anp"),
        F.col("vprod").cast("decimal(25,2)").alias("valr_total_bruto"),
        F.col("vdesc").cast("decimal(15,2)").alias("valr_desconto"),
        F.col("valr_icms_deson").cast("decimal(15,2)").alias("valr_icms_desonera"),
        F.col("valr_icms_st").cast("decimal(17,2)").alias("valr_icms_subtrib"),
        F.col("valr_icms_mono").cast("decimal(15,2)").alias("valr_icms_monofasico"),
        F.col("vfrete").cast("decimal(15,2)").alias("valr_frete"),
        F.col("vseg").cast("decimal(15,2)").alias("valr_seguro"),
        F.col("voutro").cast("decimal(15,2)").alias("valr_outras_despesas"),
        F.col("ipi_vipi").cast("decimal(15,2)").alias("valr_ipi"),
        F.col("ii_vii").cast("decimal(15,2)").alias("valr_imposto_importacao"),
        F.col("valr_adicionado").cast("decimal(17,2)").alias("valr_adicionado"),
        F.col("codg_motivo_exclusao_calculo").cast("decimal(2,0)").alias("codg_motivo_exclusao_calculo"),
        F.col("codg_tipo_doc_partct_documento").cast("decimal(3,0)").alias("codg_tipo_doc_partct_documento"),
        F.col("tipo_motivo_desonera_icms").cast("string").alias("tipo_motivo_desonera_icms"),
        F.col("id_procesm_indice").cast("decimal(9,0)").alias("id_procesm_indice"),
    )
    return out


# =============================================================================
# Alinhamento de schema com a tabela Iceberg
# =============================================================================
def _align_df_to_iceberg_table(
    spark: SparkSession,
    settings: Settings,
    df: DataFrame,
    *,
    table_name: str,
    namespace: Optional[str] = None,
) -> DataFrame:
    ns = namespace or settings.iceberg.namespace
    full = f"{settings.iceberg.catalog}.{ns}.{table_name}"
    try:
        target_schema = spark.table(full).schema
    except Exception as e:
        print(f"[align] Não foi possível carregar schema da tabela {full}. Prosseguindo sem alinhamento. Detalhe: {e}")
        return df

    cols_lower_map = {c.lower(): c for c in df.columns}
    cur = df
    for f in target_schema.fields:
        tgt = f.name
        src = cols_lower_map.get(tgt.lower())
        if src is None:
            cur = cur.withColumn(tgt, F.lit(None).cast(f.dataType))
        else:
            if src != tgt:
                cur = cur.withColumnRenamed(src, tgt)
            cur = cur.withColumn(tgt, F.col(tgt).cast(f.dataType))
    ordered = [f.name for f in target_schema.fields]
    cur = cur.select(*ordered)
    return cur


# =============================================================================
# Regras doc-level: motivo de exclusão
# =============================================================================
def _compute_codg_motivo_exclusao_doc(df: DataFrame) -> DataFrame:
    """
    Define codg_motivo_exclusao_doc conforme regras do Django:
      10 = cancelado
      13 = substituído
      12 = sem item participante (nem exceção NCM+produtor rural)
       0 = participante
    """
    prod_flag = _prod_flag_expr(df, "s_prod_rural_dest") | _prod_flag_expr(df, "s_prod_rural_remet")

    return df.withColumn(
        "codg_motivo_exclusao_doc",
        F.when(F.col("is_cancelado") == True, F.lit(10))  # cancelado
         .when(F.col("is_substituido") == True, F.lit(13))  # substituído
         .when((F.col("is_cfop_participante_doc") == False) & ~(F.col("has_ncm_participante_doc") & prod_flag), F.lit(12))  # sem item participante
         .otherwise(F.lit(0))  # participante
    )


# =============================================================================
# Pipeline principal
# =============================================================================
def run_nfe(
    spark: SparkSession,
    settings: Settings,
    *,
    data_inicio: str,
    data_fim: str,
    where_docs: Optional[str],
    where_itens: Optional[str],
    prefer_day_partition: bool = False,
    audit_params: Optional[Dict] = None,
    audit_enabled: bool = True,
    chavenfe: Optional[str] = None,
) -> Dict[str, int]:
    metrics: Dict[str, int] = {}

    audit_id = None
    if audit_enabled:
        params_json = json.dumps(
            audit_params
            or {
                "fonte": "KUDU",
                "prefer_day_partition": prefer_day_partition,
                "range_where": _mk_kudu_between_where(data_inicio, data_fim, chavenfe),
                "where_docs": where_docs,
                "where_itens": where_itens,
            },
            ensure_ascii=False,
        )
        audit_id = start_and_get_id(
            spark, settings, documento="NFE",
            data_inicio=data_inicio, data_fim=data_fim, params_json=params_json,
        )
        metrics["audit_id"] = audit_id

    try:
        print(f"[oracle] DSN efetivo -> {settings.oracle.dsn}")

        # Base DOCS / ITENS
        df_docs_base = _prepare_docs(
            spark, settings,
            data_inicio=data_inicio, data_fim=data_fim, chavenfe=chavenfe, where_docs=where_docs
        ).persist()
        df_docs_base = _ensure_cols(
            df_docs_base,
            {
                "is_cancelado":   F.lit(False).cast("boolean"),
                "is_substituido": F.lit(False).cast("boolean"),
                "has_inutil":     F.lit(False).cast("boolean"),
                "has_cce":        F.lit(False).cast("boolean"),
            },
        )

        df_itens_base, _ = _prepare_items(
            spark, settings,
            data_inicio=data_inicio, data_fim=data_fim, chavenfe=chavenfe, where_itens=where_itens
        )
        df_itens_base = df_itens_base.persist()

        metrics["docs_lidos"] = _safe_approx_distinct(df_docs_base, "chavenfe", kind="docs_lidos")
        metrics["itens_lidos"] = _safe_count(df_itens_base, kind="itens_lidos")

        if audit_enabled and audit_id is not None:
            bump_counts(
                spark, settings, id_procesm_indice=audit_id,
                add_docs=int(metrics["docs_lidos"]) if metrics["docs_lidos"] is not None else None,
                add_itens=metrics["itens_lidos"],
            )

        # CONTEXTO & REGRAS
        ctx = build_rules_context(spark, settings, broadcast_params_flag=True, include_ncm_ref=True)

        # DOCUMENTOS (GEN/CCE e afins)
        df_docs_rules, m_docs = apply_rules_to_documents(df_docs_base, ctx)
        df_docs_rules = _ensure_alias(df_docs_rules, "s_prod_rural_remet", "s_prod_rural_rem")
        df_docs_rules = _ensure_cols(df_docs_rules, {"s_prod_rural_dest": F.lit(None), "s_prod_rural_remet": F.lit(None)})

        # ITENS (CFOP + NCM)
        df_itens_rules, m_itens = apply_rules_to_items(
            df_itens_base, ctx,
            apply_cfop_items=True, filter_cfop_nonparticipants_items=False,
            apply_ncm=True, filter_ncm_nonparticipants=False,
        )

        # Fallback em itens
        if "is_cfop_participante" not in df_itens_rules.columns:
            df_itens_rules = df_itens_rules.withColumn(
                "is_cfop_participante", F.col("cfop").isNotNull() & (F.col("cfop") != F.lit(""))
            )
        if "valr_adicionado" not in df_itens_rules.columns:
            df_itens_rules = df_itens_rules.withColumn(
                "valr_adicionado",
                F.when(F.col("is_cfop_participante") == True, F.col("vprod").cast("decimal(17,2)")).otherwise(F.lit(0)),
            )
        if "codg_tipo_doc_partct_documento" not in df_itens_rules.columns:
            df_itens_rules = df_itens_rules.withColumn("codg_tipo_doc_partct_documento", F.lit(None).cast("int"))

        # AGREGA CFOP/NCM -> DOC
        agg_item_flags = (
            df_itens_rules.select(
                F.col("chavenfe").alias("chave_join"),
                F.col("is_cfop_participante").cast("boolean").alias("cfop_part"),
                F.col("is_ncm_participante").cast("boolean").alias("ncm_part"),
                F.col("valr_adicionado").cast("decimal(17,2)").alias("va_item"),
                F.col("codg_tipo_doc_partct_documento").alias("tp_doc_item"),
            )
            .groupBy("chave_join")
            .agg(
                F.max(F.when(F.col("cfop_part") == True, F.lit(1)).otherwise(F.lit(0))).alias("max_cfop_part"),
                F.max(F.when(F.col("ncm_part") == True, F.lit(1)).otherwise(F.lit(0))).alias("max_ncm_part"),
                F.sum("va_item").alias("valr_adicionado_doc"),
                F.max("tp_doc_item").alias("codg_tipo_doc_partct_doc"),
            )
            .withColumn("is_cfop_participante_doc", (F.col("max_cfop_part") > F.lit(0)))
            .withColumn("has_ncm_participante_doc", (F.col("max_ncm_part") > F.lit(0)))
            .withColumn("indi_aprop_doc", F.when(F.col("is_cfop_participante_doc"), F.lit("S")).otherwise(F.lit("N")))
            .drop("max_cfop_part", "max_ncm_part")
        )

        df_docs_rules = (
            df_docs_rules.withColumn("chave_join", F.col("chavenfe"))
            .join(agg_item_flags, on="chave_join", how="left")
            .drop("chave_join")
        ).fillna({"is_cfop_participante_doc": False, "has_ncm_participante_doc": False, "indi_aprop_doc": "N", "valr_adicionado_doc": 0})

        # Motivo de exclusão (doc-level)
        df_docs_rules = _compute_codg_motivo_exclusao_doc(df_docs_rules)

        # Participantes = motivo 0
        df_docs_part = df_docs_rules.filter(F.col("codg_motivo_exclusao_doc") == F.lit(0)).cache()

        # Define id_procesm_indice diretamente pelo audit_id (paridade Django)
        df_docs_part = df_docs_part.withColumn(
            "id_procesm_indice", F.lit(int(audit_id) if audit_id is not None else None).cast("int")
        )

        # >>>>>>>>>>>>>>>> ENRIQUECIMENTO COM CADASTRO (ORACLE) <<<<<<<<<<<<<<<<
        df_docs_part = _enrich_docs_with_cadastro(spark, settings, df_docs_part)

        # Itens somente dos docs participantes
        df_docs_keys = df_docs_part.select(F.col("chavenfe").alias("chave_join")).dropDuplicates()
        df_itens_keep = (
            df_itens_rules.withColumn("chave_join", F.col("chavenfe"))
            .join(df_docs_keys, on="chave_join", how="inner")
            .drop("chave_join")
        ).cache()

        df_itens_final = df_itens_keep.join(
            df_docs_part.select("chavenfe", "id_procesm_indice"), on="chavenfe", how="left"
        )

        # Métricas
        for k, v in m_docs.items():
            metrics[f"docs_{k}"] = v
        for k, v in m_itens.items():
            metrics[f"itens_{k}"] = v
        metrics["docs_cfop_doc"] = _safe_count(
            df_docs_rules.filter(F.col("is_cfop_participante_doc")).select("chavenfe").distinct(), kind="docs_cfop_doc"
        )
        metrics["docs_ncm_doc"] = _safe_count(
            df_docs_rules.filter(F.col("has_ncm_participante_doc")).select("chavenfe").distinct(), kind="docs_ncm_doc"
        )
        metrics["docs_rural_doc"] = _safe_count(
            df_docs_rules.filter(_prod_flag_expr(df_docs_rules, "s_prod_rural_dest") | _prod_flag_expr(df_docs_rules, "s_prod_rural_remet")).select("chavenfe").distinct(),
            kind="docs_rural_doc",
        )
        metrics["docs_participantes"] = _safe_count(df_docs_part, kind="docs_participantes")

        # Projeções
        df_docs_out = _project_document_ipm(spark, settings, df_docs_part, audit_id=audit_id)
        df_items_out = _project_item_ipm(df_itens_final, audit_id=audit_id)

        # Alinha ao schema Iceberg
        df_docs_out = _align_df_to_iceberg_table(
            spark, settings, df_docs_out,
            table_name=settings.iceberg.tbl_documento_partct, namespace=settings.iceberg.namespace,
        )
        df_items_out = _align_df_to_iceberg_table(
            spark, settings, df_items_out,
            table_name=settings.iceberg.tbl_item_documento, namespace=settings.iceberg.namespace,
        )

        metrics["docs_finais"] = _safe_count(df_docs_out, kind="docs_finais")
        metrics["itens_finais"] = _safe_count(df_items_out, kind="itens_finais")

        # Escrita Iceberg
        write_df(
            df_docs_out, settings=settings, output_format="iceberg",
            table_name=settings.iceberg.tbl_documento_partct,
            mode=settings.partitioning.mode_documento, partition_by=None, namespace=settings.iceberg.namespace,
        )
        write_df(
            df_items_out, settings=settings, output_format="iceberg",
            table_name=settings.iceberg.tbl_item_documento,
            mode=settings.partitioning.mode_item, partition_by=None, namespace=settings.iceberg.namespace,
        )

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
    p = argparse.ArgumentParser(description="Pipeline NFe (Kudu -> Regras -> Iceberg)")
    p.add_argument("--data-inicio", required=True, help="AAAA-MM-DD")
    p.add_argument("--data-fim", required=True, help="AAAA-MM-DD")
    p.add_argument("--where-docs", default=None)
    p.add_argument("--where-itens", default=None)
    p.add_argument("--dt-inicio", default=None)
    p.add_argument("--dt-fim", default=None)
    p.add_argument("--prefer-day-partition", default="false")
    p.add_argument("--print-settings", default="false")
    p.add_argument("--no-audit", action="store_true")
    p.add_argument("--chavenfe", default=None)
    return p.parse_args(argv)


def main(argv=None) -> None:
    settings = load_settings_from_env()
    spark = build_spark(settings)
    args = parse_args(argv)

    prefer_day_partition = _parse_bool(args.prefer_day_partition)

    if _parse_bool(args.print_settings):
        print_settings(settings)

    metrics = run_nfe(
        spark,
        settings,
        data_inicio=args.data_inicio,
        data_fim=args.data_fim,
        where_docs=args.where_docs,
        where_itens=args.where_itens,
        prefer_day_partition=prefer_day_partition,
        audit_params={
            "cli": True,
            "range_where": _mk_kudu_between_where(args.data_inicio, args.data_fim, args.chavenfe),
            "where_docs": args.where_docs,
            "where_itens": args.where_itens,
        },
        audit_enabled=(not args.no_audit),
        chavenfe=args.chavenfe,
    )

    print("=== NFe Metrics ===")
    for k in sorted(metrics.keys()):
        print(f"{k}: {metrics[k]}")

    spark.stop()


if __name__ == "__main__":
    main()
