# -*- coding: utf-8 -*-
# app/bpe.py
from __future__ import annotations

import argparse
import json
import os
from typing import Dict, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window  # ✅ import correto

from app.settings import Settings, load_settings_from_env, build_spark, print_settings
from app.utils.io import read_kudu_table, write_df
from app.utils.audit import start_and_get_id, bump_counts, finish_success, finish_error
from app.utils.rules import (
    RulesContext,
    load_params_ref,
    load_cfop_ref,
    load_ncm_ref,
    load_cce_ref,
    load_gen_ref,
    apply_rules_to_items,
    apply_rules_to_documents,
)
from app.projections import project_document, project_item


# =============================================================================
# Defaults (pode sobrescrever por ENV ou CLI)
# =============================================================================
_DEFAULT_KUDU_DB = os.getenv("BPE_KUDU_DB", "bpe")

# Núcleo + complementos
_DEFAULT_TB_DOC     = os.getenv("BPE_DOC_TABLE",      "bpe_infbpe")
_DEFAULT_TB_EVENT   = os.getenv("BPE_EVENT_TABLE",    "bpe_evento")
_DEFAULT_TB_XML     = os.getenv("BPE_XMLINFO_TABLE",  "bpe_xml_info")
_DEFAULT_TB_PAG     = os.getenv("BPE_PAG_TABLE",      "bpe_infbpe_pag")
_DEFAULT_TB_VIAGEM  = os.getenv("BPE_VIAGEM_TABLE",   "bpe_infbpe_infviagem")
_DEFAULT_TB_COMP    = os.getenv("BPE_COMP_TABLE",     "bpe_infbpe_infvalorbpe_comp")

# Detalhamento tratado como “itens”
_DEFAULT_TB_DET_HDR  = os.getenv("BPE_DET_HDR_TABLE",  "bpe_infbpe_detbpetm")
_DEFAULT_TB_DET      = os.getenv("BPE_DET_TABLE",      "bpe_infbpe_detbpetm_det")
_DEFAULT_TB_DET_COMP = os.getenv("BPE_DET_COMP_TABLE", "bpe_infbpe_detbpetm_det_comp")

# Colunas-chave
_TS_COL  = os.getenv("BPE_TS_COL", "ide_dhemi_nums")   # epoch (int) para pushdown
_KEY_COL = os.getenv("BPE_KEY_COL", "chbpe")           # chave BPe (no Kudu)

# Códigos fixos herdados do legado (funcoes/constantes.py)
# EnumTipoDocumento.BPe.value = 7
_BPE_DOC_TYPE_CODE = 7

# EnumMotivoExclusao (trecho relevante):
#   DocumentoCancelado      = 10
#   DocumentoSubstituido    = 13
#   ValoresInconsistentes   = 14
_MOT_EXC_DOC_CANCELADO    = 10
_MOT_EXC_VALORES_INCONS   = 14  # usado também para "Não Embarque" no BPe


# =============================================================================
# Helpers
# =============================================================================
def _norm_tab(name: str, *, db: Optional[str], for_format: str = "kudu") -> str:
    if "." not in name and db:
        name = f"{db}.{name}"
    return f"impala::{name}" if for_format.lower() in ("kudu", "impala") else name


def _mk_where(data_inicio: str, data_fim: str, chave: Optional[str], ts_col: str = _TS_COL) -> str:
    """
    Filtro padrão de BPe:
      - usa *apenas* a coluna ts_col (ide_dhemi_nums por padrão)
      - não tenta usar dh_emis_nums (que não existe em BPe)
    """
    di = f"{data_inicio.strip()} 00:00:00"
    df = f"{data_fim.strip()} 23:59:59"
    base = f"{ts_col} BETWEEN unix_timestamp('{di}') AND unix_timestamp('{df}')"
    if chave:
        base += f" AND {_KEY_COL} = '{chave.strip()}'"
    return base


def _safe_count(df: Optional[DataFrame], what: str) -> int:
    if df is None:
        return 0
    try:
        return int(df.count())
    except Exception as e:
        print(f"[bpe][_safe_count] Falha ao contar {what}: {e}")
        return -1


def _normalize_iceberg_table_name(table: str, settings: Settings) -> str:
    """
    Normaliza o nome da tabela para o write_df/write_iceberg.

    Se o utils.io.write_iceberg já monta catalog/db (ex: tdp_catalog.ipm.<table>)
    e aqui vier 'tdp_catalog.ipm.ipm_documento_partct_calc_ipm', usamos apenas
    a última parte para evitar algo como:
        ipm.tdp_catalog.ipm.ipm_documento_partct_calc_ipm
    """
    parts = table.split(".")
    # Se vier catalog.db.table => pega só 'table'
    if len(parts) == 3:
        return parts[-1]
    # Se vier db.table e o settings já tem db configurado, usa só 'table'
    if len(parts) == 2 and getattr(settings.iceberg, "db", None):
        return parts[-1]
    # Caso já seja simples ou outro formato, retorna como está
    return table


# =============================================================================
# Leituras do Kudu
# =============================================================================
def _read_bpe_doc(
    spark: SparkSession,
    settings: Settings,
    *,
    kudu_db: str,
    tb_doc: str,
    tb_xml: Optional[str],
    tb_event: Optional[str],
    where_sql: str,
) -> DataFrame:
    # 1) Documento base (usa filtro por data)
    df_doc = read_kudu_table(
        spark, settings,
        table=_norm_tab(tb_doc, db=kudu_db),
        where=where_sql,
        columns=None,
    )

    # Normalizar chave BPe → chbpe
    if _KEY_COL not in df_doc.columns:
        if "ide_cbp" in df_doc.columns:
            df_doc = df_doc.withColumn(_KEY_COL, F.col("ide_cbp"))
        elif "chave" in df_doc.columns:
            df_doc = df_doc.withColumn(_KEY_COL, F.col("chave"))
        elif "infbpesub_chbpe" in df_doc.columns:
            df_doc = df_doc.withColumn(_KEY_COL, F.col("infbpesub_chbpe"))

    # 2) XML INFO (sem filtro)
    if tb_xml:
        df_xml = read_kudu_table(
            spark, settings,
            table=_norm_tab(tb_xml, db=kudu_db),
            where=None,
            columns=None,
        )

        if _KEY_COL not in df_xml.columns:
            if "chave" in df_xml.columns:
                df_xml = df_xml.withColumn(_KEY_COL, F.col("chave"))
            elif "ide_cbp" in df_xml.columns:
                df_xml = df_xml.withColumn(_KEY_COL, F.col("ide_cbp"))

        if _KEY_COL in df_xml.columns:
            df_xml = df_xml.select(_KEY_COL, *[c for c in df_xml.columns if c != _KEY_COL])
            df_doc = df_doc.join(df_xml, on=_KEY_COL, how="left")

    # 3) EVENTOS (sem filtro)
    if tb_event:
        df_evt = read_kudu_table(
            spark, settings,
            table=_norm_tab(tb_event, db=kudu_db),
            where=None,
            columns=None,
        )

        if _KEY_COL not in df_evt.columns:
            if "chave" in df_evt.columns:
                df_evt = df_evt.withColumn(_KEY_COL, F.col("chave"))
            elif "ide_cbp" in df_evt.columns:
                df_evt = df_evt.withColumn(_KEY_COL, F.col("ide_cbp"))

        if _KEY_COL in df_evt.columns:
            tpevento_col = (
                F.col("tpevento").cast("string")
                if "tpevento" in df_evt.columns
                else F.col("cdevento").cast("string")
            )
            desc_col = (
                F.col("descevento").cast("string")
                if "descevento" in df_evt.columns
                else F.lit(None).cast("string")
            )

            evt = (
                df_evt
                .select(
                    _KEY_COL,
                    tpevento_col.alias("tpevento_str"),
                    desc_col.alias("descevento_str"),
                )
                .withColumn(
                    "evt_cancel",
                    F.when(F.col("tpevento_str") == F.lit("110111"), F.lit(1)).otherwise(F.lit(0)),
                )
                .withColumn(
                    "evt_nao_emb",
                    F.when(
                        F.upper(F.col("descevento_str")).like("%NAO EMBARQUE%"),
                        F.lit(1),
                    ).otherwise(F.lit(0)),
                )
                .groupBy(_KEY_COL)
                .agg(
                    F.max("evt_cancel").alias("has_evt_cancel"),
                    F.max("evt_nao_emb").alias("has_evt_nao_emb"),
                )
            )
            df_doc = df_doc.join(evt, on=_KEY_COL, how="left")

    # Garante flags numéricas
    for c in ["has_evt_cancel", "has_evt_nao_emb"]:
        if c not in df_doc.columns:
            df_doc = df_doc.withColumn(c, F.lit(0).cast("int"))

    return df_doc


def _read_bpe_items(
    spark: SparkSession,
    settings: Settings,
    *,
    kudu_db: str,
    tb_det: Optional[str],
    tb_det_comp: Optional[str],
    tb_det_hdr: Optional[str],
    where_sql: str,
) -> Optional[DataFrame]:
    """Itens preferidos: bpe_infbpe_detbpetm_det (+comp, +hdr)."""
    if not tb_det:
        return None

    df_det = read_kudu_table(
        spark, settings,
        table=_norm_tab(tb_det, db=kudu_db),
        where=where_sql,
        columns=None,
    )

    # Enriquecimento com COMP (quando disponível)
    if tb_det_comp:
        try:
            df_c = read_kudu_table(
                spark, settings,
                table=_norm_tab(tb_det_comp, db=kudu_db),
                where=where_sql,
                columns=None,
            )
            if _KEY_COL in df_c.columns and _KEY_COL in df_det.columns:
                keep_c = [_KEY_COL, "id"] + [c for c in ["xnome", "qcomp"] if c in df_c.columns]
                df_det = df_det.join(df_c.select(*keep_c), on=[_KEY_COL, "id"], how="left")
        except Exception as e:
            print(f"[bpe] Aviso: falha lendo det_comp (opcional): {e}")

    # Enriquecimento com HDR (quando disponível)
    if tb_det_hdr:
        try:
            df_h = read_kudu_table(
                spark, settings,
                table=_norm_tab(tb_det_hdr, db=kudu_db),
                where=where_sql,
                columns=None,
            )
            if _KEY_COL in df_h.columns and _KEY_COL in df_det.columns:
                keep_h = [_KEY_COL, "id"] + [c for c in ["cmunini", "cmunfim", "ufiniviagem", "uffimviagem"] if c in df_h.columns]
                df_det = df_det.join(df_h.select(*keep_h), on=[_KEY_COL, "id"], how="left")
        except Exception as e:
            print(f"[bpe] Aviso: falha lendo det_hdr (opcional): {e}")

    return df_det


def _read_bpe_pag_viagem_comp(
    spark: SparkSession,
    settings: Settings,
    *,
    kudu_db: str,
    tb_pag: Optional[str],
    tb_viagem: Optional[str],
    tb_comp: Optional[str],
    where_sql: str,
) -> Dict[str, Optional[DataFrame]]:
    out: Dict[str, Optional[DataFrame]] = {"pag": None, "viagem": None, "comp": None}

    if tb_pag:
        out["pag"] = read_kudu_table(
            spark, settings,
            table=_norm_tab(tb_pag, db=kudu_db),
            where=where_sql,
            columns=None,
        )

    if tb_viagem:
        out["viagem"] = read_kudu_table(
            spark, settings,
            table=_norm_tab(tb_viagem, db=kudu_db),
            where=where_sql,
            columns=None,
        )

    if tb_comp:
        out["comp"] = read_kudu_table(
            spark, settings,
            table=_norm_tab(tb_comp, db=kudu_db),
            where=where_sql,
            columns=None,
        )

    return out


# =============================================================================
# Normalizações mínimas para Regras/Projeções
# =============================================================================
def _normalize_doc(df_doc: DataFrame) -> DataFrame:
    df = df_doc

    # data emissão (timestamp -> date)
    if "ide_dhemi" in df.columns and "dh_emissao" not in df.columns:
        df = df.withColumn("dh_emissao", F.col("ide_dhemi"))

    if "dh_emissao" in df.columns and "data_emissao_dt" not in df.columns:
        df = df.withColumn("data_emissao_dt", F.to_date(F.col("dh_emissao")))
    elif "ide_dhemi" in df.columns and "data_emissao_dt" not in df.columns:
        df = df.withColumn("data_emissao_dt", F.to_date(F.col("ide_dhemi")))

    # valor total do documento (vpgto/vbp)
    cand = [c for c in ["infvalorbpe_vpgto", "infvalorbpe_vbp"] if c in df.columns]
    tot_expr = None
    for c in cand:
        colc = F.col(c).cast(T.DecimalType(17, 2))
        tot_expr = colc if tot_expr is None else F.coalesce(tot_expr, colc)
    if tot_expr is not None and "valor_total_documento" not in df.columns:
        df = df.withColumn("valor_total_documento", tot_expr)

    # espelho para regras IPM (valr_nota_nf)
    if "valor_total_documento" in df.columns and "valr_nota_nf" not in df.columns:
        df = df.withColumn("valr_nota_nf", F.col("valor_total_documento").cast(T.DecimalType(17, 2)))

    # origem para GEN
    if "ide_cmunini" in df.columns and "doc_saida_cmun" not in df.columns:
        df = df.withColumn("doc_saida_cmun", F.col("ide_cmunini"))
    if "ide_ufini" in df.columns and "doc_saida_uf" not in df.columns:
        df = df.withColumn("doc_saida_uf", F.col("ide_ufini"))

    # CFOP documento
    if "ide_cfop" in df.columns and "cfop_doc" not in df.columns:
        df = df.withColumn(
            "cfop_doc",
            F.lpad(F.regexp_replace(F.col("ide_cfop").cast("string"), r"\D", ""), 4, "0"),
        )

    return df


def _normalize_items(df_items: Optional[DataFrame], df_doc_cfop: DataFrame) -> Optional[DataFrame]:
    if df_items is None:
        return None

    df = df_items

    # numr_item com ordem estável
    w = Window.partitionBy(_KEY_COL).orderBy(F.coalesce(F.col("nviagem"), F.col("id")))
    if "numr_item" not in df.columns:
        df = df.withColumn("numr_item", F.row_number().over(w))

    # valor do item (vbp ou imp_vtotdfe)
    cand_vals = [c for c in ["vbp", "imp_vtotdfe"] if c in df.columns]
    val_expr = None
    for c in cand_vals:
        colc = F.col(c).cast(T.DecimalType(17, 2))
        val_expr = colc if val_expr is None else F.coalesce(val_expr, colc)
    if val_expr is not None and "valor_item" not in df.columns:
        df = df.withColumn("valor_item", val_expr)

    # CFOP por linha; se não existir, herda do documento
    if "cfop" in df.columns:
        df = df.withColumn(
            "cfop",
            F.lpad(F.regexp_replace(F.col("cfop").cast("string"), r"\D", ""), 4, "0"),
        )
    elif "cfop_doc" in df_doc_cfop.columns:
        df = (
            df.join(df_doc_cfop.select(_KEY_COL, "cfop_doc"), on=_KEY_COL, how="left")
              .withColumnRenamed("cfop_doc", "cfop")
        )

    return df


# =============================================================================
# Regras Oracle (lookup)
# =============================================================================
def _load_refs_oracle(spark: SparkSession, settings: Settings) -> RulesContext:
    return RulesContext(
        df_params=load_params_ref(spark, settings),
        df_cfop_ref=load_cfop_ref(spark, settings),
        df_ncm_ref=load_ncm_ref(spark, settings),      # em BPe deve ser neutro
        df_eventos=load_cce_ref(spark, settings),
        df_gen=load_gen_ref(spark, settings),
        params_dict=load_params_ref(spark, settings, as_dict=True),
    )


def _aggregate_items_to_document(df_docs_rules: DataFrame, df_items_rules: Optional[DataFrame]) -> DataFrame:
    """
    Consolida regras de item -> documento:
      - valr_adicionado_operacao
      - indi_aprop
      - codg_tipo_doc_partct
      - codg_motivo_exclusao_calculo
    """
    if df_items_rules is not None:
        agg = (
            df_items_rules.groupBy(_KEY_COL)
            .agg(
                F.sum(F.col("valr_adicionado").cast(T.DecimalType(17, 2))).alias("valr_adicionado_operacao"),
                F.max(
                    F.when(F.col("is_cfop_participante") == F.lit(True), F.lit(1)).otherwise(F.lit(0))
                ).alias("has_part"),
                F.max(F.col("codg_tipo_doc_partct_item").cast("int")).alias("codg_tipo_doc_partct"),
            )
        )
        df = df_docs_rules.join(agg, on=_KEY_COL, how="left")
    else:
        df = (
            df_docs_rules
            .withColumn("valr_adicionado_operacao", F.lit(0).cast(T.DecimalType(17, 2)))
            .withColumn("has_part", F.lit(0).cast("int"))
            .withColumn("codg_tipo_doc_partct", F.lit(_BPE_DOC_TYPE_CODE).cast("int"))
        )

    if "codg_motivo_exclusao_doc" not in df.columns:
        df = df.withColumn("codg_motivo_exclusao_doc", F.lit(0).cast("int"))

    for c in ["has_evt_cancel", "has_evt_nao_emb"]:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(0).cast("int"))

    df = df.withColumn(
        "codg_motivo_exclusao_calculo_base",
        F.coalesce(F.col("codg_motivo_exclusao_doc").cast("int"), F.lit(0)),
    )

    df = df.withColumn(
        "codg_motivo_exclusao_calculo",
        F.when(F.col("has_evt_cancel") == 1, F.lit(_MOT_EXC_DOC_CANCELADO))
         .when(F.col("has_evt_nao_emb") == 1, F.lit(_MOT_EXC_VALORES_INCONS))
         .otherwise(F.col("codg_motivo_exclusao_calculo_base")),
    ).drop("codg_motivo_exclusao_calculo_base")

    df = df.withColumn(
        "indi_aprop",
        F.when(
            (F.col("has_part") == 1) & (F.col("codg_motivo_exclusao_calculo") == 0),
            F.lit("S"),
        ).otherwise(F.lit("N")),
    )

    df = df.withColumn(
        "valr_adicionado_operacao",
        F.when(
            F.col("indi_aprop") == F.lit("S"),
            F.col("valr_adicionado_operacao"),
        ).otherwise(F.lit(0).cast(T.DecimalType(17, 2))),
    )

    df = df.withColumn(
        "codg_tipo_doc_partct",
        F.when(
            F.col("indi_aprop") == F.lit("S"),
            F.lit(_BPE_DOC_TYPE_CODE).cast("int"),
        ).otherwise(
            F.coalesce(F.col("codg_tipo_doc_partct").cast("int"), F.lit(_BPE_DOC_TYPE_CODE))
        ),
    )

    df = df.drop("has_part")

    return df


# =============================================================================
# Pipeline
# =============================================================================
def run_bpe(
    spark: SparkSession,
    settings: Settings,
    *,
    data_inicio: str,
    data_fim: str,
    chave: Optional[str] = None,
    where_docs: Optional[str] = None,   # ignorado em BPE
    where_itens: Optional[str] = None,  # ignorado em BPE
    kudu_db: str = _DEFAULT_KUDU_DB,
    bpe_doc_table: str = _DEFAULT_TB_DOC,
    bpe_event_table: Optional[str] = _DEFAULT_TB_EVENT,
    bpe_xmlinfo_table: Optional[str] = _DEFAULT_TB_XML,
    bpe_pag_table: Optional[str] = _DEFAULT_TB_PAG,
    bpe_viagem_table: Optional[str] = _DEFAULT_TB_VIAGEM,
    bpe_comp_table: Optional[str] = _DEFAULT_TB_COMP,
    bpe_det_hdr_table: Optional[str] = _DEFAULT_TB_DET_HDR,
    bpe_det_table: Optional[str] = _DEFAULT_TB_DET,
    bpe_det_comp_table: Optional[str] = _DEFAULT_TB_DET_COMP,
    print_settings_flag: bool = False,
    audit_enabled: bool = True,
    write_iceberg: bool = True,
    mode: str = "merge",
    prefer_day_partition: bool = False,
    **_,
) -> Dict[str, int]:
    if print_settings_flag:
        print_settings(settings)

    # Auditoria START
    params_json = json.dumps(
        {
            "fonte": "KUDU",
            "chave": chave,
            "where_docs": where_docs,
            "where_itens": where_itens,
            "tables": {
                "doc": bpe_doc_table,
                "event": bpe_event_table,
                "xml": bpe_xmlinfo_table,
                "pag": bpe_pag_table,
                "viagem": bpe_viagem_table,
                "comp": bpe_comp_table,
                "det_hdr": bpe_det_hdr_table,
                "det": bpe_det_table,
                "det_comp": bpe_det_comp_table,
            },
        },
        ensure_ascii=False,
    )

    audit_id = -1
    if audit_enabled:
        audit_id = start_and_get_id(
            spark,
            settings,
            documento="BPE",
            data_inicio=data_inicio,
            data_fim=data_fim,
            params_json=params_json,
        )

    base_where = _mk_where(data_inicio, data_fim, chave, ts_col=_TS_COL)
    where_docs_sql = base_where
    where_itens_sql = base_where

    # 1) Documento base (+ xml + eventos)
    df_doc_base = _read_bpe_doc(
        spark,
        settings,
        kudu_db=kudu_db,
        tb_doc=bpe_doc_table,
        tb_xml=bpe_xmlinfo_table,
        tb_event=bpe_event_table,
        where_sql=where_docs_sql,
    )

    # 2) Complementos opcionais (pag/viagem/comp)
    extras = _read_bpe_pag_viagem_comp(
        spark,
        settings,
        kudu_db=kudu_db,
        tb_pag=bpe_pag_table,
        tb_viagem=bpe_viagem_table,
        tb_comp=bpe_comp_table,
        where_sql=where_docs_sql,
    )

    # Soma pagamentos
    if extras["pag"] is not None and _KEY_COL in extras["pag"].columns and "vpag" in extras["pag"].columns:
        agg_pag = extras["pag"].groupBy(_KEY_COL).agg(
            F.sum(F.col("vpag").cast(T.DecimalType(17, 2))).alias("sum_vpag")
        )
        df_doc_base = df_doc_base.join(agg_pag, on=_KEY_COL, how="left")

    # Última viagem
    if extras["viagem"] is not None and _KEY_COL in extras["viagem"].columns:
        wv = Window.partitionBy(_KEY_COL).orderBy(F.col("dhviagem_nums").desc_nulls_last())
        sel = [_KEY_COL] + [c for c in ["dhviagem", "prefixo", "poltrona"] if c in extras["viagem"].columns]
        df_last_viagem = (
            extras["viagem"]
            .withColumn("_rn", F.row_number().over(wv))
            .where(F.col("_rn") == 1)
            .select(*sel)
        )
        df_doc_base = df_doc_base.join(df_last_viagem, on=_KEY_COL, how="left")

    # Complementos de valores
    if extras["comp"] is not None and _KEY_COL in extras["comp"].columns and "vcomp" in extras["comp"].columns:
        agg_comp = extras["comp"].groupBy(_KEY_COL).agg(
            F.sum(F.col("vcomp").cast(T.DecimalType(17, 2))).alias("sum_vcomp")
        )
        df_doc_base = df_doc_base.join(agg_comp, on=_KEY_COL, how="left")

    # 3) Normalização documento
    df_doc_norm = _normalize_doc(df_doc_base)

    # 4) Itens (detalhes). Se não existir, item sintético (1/1)
    df_det = _read_bpe_items(
        spark,
        settings,
        kudu_db=kudu_db,
        tb_det=bpe_det_table,
        tb_det_comp=bpe_det_comp_table,
        tb_det_hdr=bpe_det_hdr_table,
        where_sql=where_itens_sql,
    )

    metrics: Dict[str, int] = {}
    metrics["docs_lidos"] = _safe_count(df_doc_norm, "docs_lidos")
    metrics["det_lidos"] = _safe_count(df_det, "det_lidos")

    if audit_enabled and metrics["docs_lidos"] >= 0:
        bump_counts(
            spark,
            settings,
            id_procesm_indice=audit_id,
            add_docs=metrics["docs_lidos"],
            add_itens=0,
        )

    df_doc_cfop = df_doc_norm.select(
        _KEY_COL,
        *(
            ["cfop_doc"]
            if "cfop_doc" in df_doc_norm.columns
            else []
        ),
    )

    if df_det is None or metrics["det_lidos"] == 0:
        base_for_item = df_doc_norm.select(
            _KEY_COL,
            "valor_total_documento",
            *(
                ["cfop_doc"]
                if "cfop_doc" in df_doc_norm.columns
                else []
            ),
        )
        df_items_base = (
            base_for_item
            .withColumn("numr_item", F.lit(1))
            .withColumn("valor_item", F.col("valor_total_documento"))
            .drop("valor_total_documento")
        )
    else:
        df_items_base = df_det

    df_items_norm = _normalize_items(df_items_base, df_doc_cfop)
    metrics["itens_norm"] = _safe_count(df_items_norm, "itens_norm")

    # 5) Regras Oracle
    ctx = _load_refs_oracle(spark, settings)

    df_items_rules = None
    if df_items_norm is not None:
        df_items_rules, m_it = apply_rules_to_items(
            df_items_norm,
            ctx,
            apply_ncm=None,
            filter_ncm_nonparticipants=None,
            apply_cfop_items=None,
            filter_cfop_nonparticipants_items=None,
        )
        if m_it:
            metrics.update({f"itens_{k}": v for k, v in m_it.items()})

    df_docs_rules, m_doc = apply_rules_to_documents(
        df_doc_norm,
        ctx,
        apply_gen=None,
        apply_cce=None,              # eventos Oracle; aqui usamos flags Kudu
        filter_cce_excluded=None,
    )
    if m_doc:
        metrics.update({f"docs_{k}": v for k, v in m_doc.items()})

    # 6) Agregação item → doc
    df_docs_aggr = _aggregate_items_to_document(df_docs_rules, df_items_rules)

    # 7) Projeções finais para IPM
    df_docs_final = project_document(df_docs_aggr, key_col=_KEY_COL)
    df_items_final = project_item(df_items_rules, key_col=_KEY_COL) if df_items_rules is not None else None

    metrics["docs_finais"] = _safe_count(df_docs_final, "docs_finais")
    metrics["itens_finais"] = _safe_count(df_items_final, "itens_finais")

    # 💥 Drop de colunas que não existem na tabela Iceberg
    drop_cols = [c for c in ["ANO", "MES"] if c in df_docs_final.columns]
    if drop_cols:
        df_docs_final = df_docs_final.drop(*drop_cols)

    # 8) Escrita Iceberg (merge idempotente)
    if write_iceberg:
        raw_doc_table = settings.iceberg.document_table()  # ex.: tdp_catalog.ipm.ipm_documento_partct_calc_ipm
        raw_item_table = settings.iceberg.item_table()     # ex.: tdp_catalog.ipm.ipm_item_documento

        doc_table = _normalize_iceberg_table_name(raw_doc_table, settings)
        item_table = _normalize_iceberg_table_name(raw_item_table, settings)

        # ⚠️ Chave de MERGE no Iceberg é CODG_CHAVE_ACESSO_NFE, não chbpe
        write_df(
            df_docs_final,
            settings=settings,
            format="iceberg",
            table=doc_table,
            mode=mode,
            key_columns=["CODG_CHAVE_ACESSO_NFE"],
        )
        if audit_enabled and metrics["docs_finais"] >= 0:
            bump_counts(
                spark,
                settings,
                id_procesm_indice=audit_id,
                add_docs=metrics["docs_finais"],
                add_itens=0,
            )

        if df_items_final is not None:
            # Itens: chave composta CODG_CHAVE_ACESSO_NFE + NUMR_ITEM
            write_df(
                df_items_final,
                settings=settings,
                format="iceberg",
                table=item_table,
                mode=mode,
                key_columns=["CODG_CHAVE_ACESSO_NFE", "NUMR_ITEM"],
            )
            if audit_enabled and metrics["itens_finais"] >= 0:
                bump_counts(
                    spark,
                    settings,
                    id_procesm_indice=audit_id,
                    add_docs=0,
                    add_itens=metrics["itens_finais"],
                )

    # Auditoria SUCCESS
    if audit_enabled:
        finish_success(
            spark,
            settings,
            id_procesm_indice=audit_id,
            extra_updates={
                "qtd_docs": metrics.get("docs_finais", 0),
                "qtd_itens": metrics.get("itens_finais", 0),
            },
        )
    return metrics


# =============================================================================
# CLI
# =============================================================================
def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Pipeline BPe (Kudu -> Regras -> Iceberg).")

    # Período/chave
    p.add_argument("--data-inicio", required=True, help="Data inicial (YYYY-MM-DD)")
    p.add_argument("--data-fim", required=True, help="Data final (YYYY-MM-DD)")
    p.add_argument("--chave", required=False, default=None, help="Filtrar por uma única chave BPe")

    # Kudu
    p.add_argument("--kudu-db", default=_DEFAULT_KUDU_DB)
    p.add_argument("--bpe-doc-table", default=_DEFAULT_TB_DOC)
    p.add_argument("--bpe-event-table", default=_DEFAULT_TB_EVENT)
    p.add_argument("--bpe-xmlinfo-table", default=_DEFAULT_TB_XML)
    p.add_argument("--bpe-pag-table", default=_DEFAULT_TB_PAG)
    p.add_argument("--bpe-viagem-table", default=_DEFAULT_TB_VIAGEM)
    p.add_argument("--bpe-comp-table", default=_DEFAULT_TB_COMP)
    p.add_argument("--bpe-det-hdr-table", default=_DEFAULT_TB_DET_HDR)
    p.add_argument("--bpe-det-table", default=_DEFAULT_TB_DET)
    p.add_argument("--bpe-det-comp-table", default=_DEFAULT_TB_DET_COMP)

    # Execução
    p.add_argument("--no-audit", action="store_true")
    p.add_argument("--print-settings", action="store_true")
    p.add_argument("--no-write", action="store_true")
    p.add_argument("--mode", default="merge", choices=["merge", "append"])

    return p


def main() -> None:
    args = _build_arg_parser().parse_args()

    settings: Settings = load_settings_from_env()
    spark: SparkSession = build_spark(settings, app_name="PROCESSO_CARGA_BPE_IPM")

    try:
        metrics = run_bpe(
            spark,
            settings,
            data_inicio=args.data_inicio,
            data_fim=args.data_fim,
            chave=args.chave,
            kudu_db=args.kudu_db,
            bpe_doc_table=args.bpe_doc_table,
            bpe_event_table=args.bpe_event_table,
            bpe_xmlinfo_table=args.bpe_xmlinfo_table,
            bpe_pag_table=args.bpe_pag_table,
            bpe_viagem_table=args.bpe_viagem_table,
            bpe_comp_table=args.bpe_comp_table,
            bpe_det_hdr_table=args.bpe_det_hdr_table,
            bpe_det_table=args.bpe_det_table,
            bpe_det_comp_table=args.bpe_det_comp_table,
            print_settings_flag=bool(args.print_settings),
            audit_enabled=(not args.no_audit),
            write_iceberg=(not args.no_write),
            mode=args.mode,
        )
        print("[BPe] Métricas:", json.dumps(metrics, ensure_ascii=False))
    except Exception as e:
        try:
            finish_error(
                spark,
                settings,
                id_procesm_indice=start_and_get_id(
                    spark,
                    settings,
                    documento="BPE",
                    data_inicio=args.data_inicio,
                    data_fim=args.data_fim,
                ),
                erro_msg=str(e),
            )
        finally:
            raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
