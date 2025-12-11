# -*- coding: utf-8 -*-
# app/projections.py
"""
Projeções finais (Documentos e Itens) compatíveis com as tabelas Iceberg:
- tdp_catalog.ipm.ipm_documento_partct_calc_ipm
- tdp_catalog.ipm.ipm_item_documento

Inclui dispatchers por documento (NFE, CTE, EFD, BPE, NF3E) e wrappers
project_document / project_item com parâmetros:
- doc_hint: "NFE", "CTE", "BPE", "NF3E", "EFD" (opcional)
- key_col: nome da coluna de chave no DF de entrada (opcional; se ausente, usa candidatos)

Observações:
- Prioriza a coluna passada por key_col; se vazia/ausente, tenta candidatos por doc_hint.
- Conversões são defensivas e não usam pandas; seguro para execução distribuída.
"""

from __future__ import annotations
from typing import Dict, Iterable, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F, types as T


# =============================================================================
# Utils básicos
# =============================================================================

def _first_present(cols: Iterable[str], cands: List[str]) -> Optional[str]:
    u = {c.lower(): c for c in cols}
    for cand in cands:
        if cand and cand.lower() in u:
            return u[cand.lower()]
    return None

def _digits_only(col: F.Column) -> F.Column:
    # remove tudo que não for dígito
    return F.regexp_replace(F.coalesce(col.cast("string"), F.lit("")), r"\D+", "")

def _strip(col: F.Column) -> F.Column:
    return F.trim(col.cast("string"))

def _to_str(col: F.Column, max_len: Optional[int] = None) -> F.Column:
    v = F.coalesce(col.cast("string"), F.lit(None).cast("string"))
    if max_len and isinstance(max_len, int) and max_len > 0:
        return F.when(v.isNotNull(), F.substring(v, 1, max_len)).otherwise(v)
    return v

def _to_int(col: F.Column) -> F.Column:
    s = _strip(col)
    return F.when(s.rlike(r"^\s*-?\d+\s*$"), s.cast("long")).otherwise(F.lit(None).cast("long"))

def _to_dec(col: F.Column, precision: int, scale: int) -> F.Column:
    s = F.regexp_replace(_strip(col), ",", ".")
    return F.when(s.rlike(r"^-?\d+(\.\d+)?$"),
                  s.cast(T.DecimalType(precision, scale))) \
            .otherwise(F.lit(None).cast(T.DecimalType(precision, scale)))

def _ensure_cols(df: DataFrame, defaults: Dict[str, F.Column]) -> DataFrame:
    cur = df
    for name, expr in defaults.items():
        if name not in cur.columns:
            cur = cur.withColumn(name, expr)
    return cur

def _coerce_date_any(col: F.Column) -> F.Column:
    """
    Converte col em DATE aceitando:
      - timestamp/strings parseáveis,
      - epoch em segundos/millis,
      - yyyyMMddHHmmss (14 dígitos) ou yyyyMMdd (8).
    """
    s = _strip(col)

    # 14 dígitos -> yyyyMMddHHmmss
    is14 = s.rlike(r"^\d{14}$")
    s14  = F.to_timestamp(F.concat_ws(" ",
                                     F.substring(s, 1, 8),
                                     F.concat_ws(":", F.substring(s, 9, 2), F.substring(s, 11, 2), F.substring(s, 13, 2))
                                     ), "yyyyMMdd HH:mm:ss")

    # 8 dígitos -> yyyyMMdd
    is8  = s.rlike(r"^\d{8}$")
    s8   = F.to_timestamp(s, "yyyyMMdd")

    # epoch (≥13 dígitos -> millis; ≤10 -> segundos)
    isnum = s.rlike(r"^\d{1,20}$")
    as_long = s.cast("long")
    ts_epoch = F.when(F.length(s) >= 13, F.from_unixtime((as_long / F.lit(1000)).cast("long"))) \
                .otherwise(F.from_unixtime(as_long))
    ts_epoch_ts = ts_epoch.cast("timestamp")

    # fallback: tentar to_timestamp direto
    s_any = F.to_timestamp(s)

    ts = F.when(is14, s14) \
           .when(is8, s8) \
           .when(isnum, ts_epoch_ts) \
           .otherwise(s_any)

    return F.to_date(ts)


# =============================================================================
# Candidatos de colunas por tipo de documento
# =============================================================================
# CHAVE (ordem importa – prioriza por doc)
_CANDS_CHAVE_BASE = [
    "codg_chave_acesso_nfe", "chave_doc", "chave", "chave_nfe", "chv_nfe", "id_chave",
]

_CANDS_CHAVE_BY_DOC = {
    "NFE":  ["chavenfe"] + _CANDS_CHAVE_BASE,
    "CTE":  ["chavecte"] + _CANDS_CHAVE_BASE,
    "NF3E": ["chnf3e", "chavenf3e", "chave_nf3e"] + _CANDS_CHAVE_BASE,  # inclui chnf3e
    "BPE":  ["chvbpe", "chave_bpe"] + _CANDS_CHAVE_BASE,
    # EFD usa doc_id (chave sintética)
    "EFD":  ["doc_id"] + _CANDS_CHAVE_BASE,
    # Fallback genérico
    "GEN":  _CANDS_CHAVE_BASE,
}

# DEMAIS CANDIDATAS (genéricas – valem pra todos)
_CANDS_DT_EMIS = ["data_emissao", "data_emissao_nfe", "dt_emissao", "dh_emissao", "ts_emissao", "datm_emis_documento"]
_CANDS_VL_TOT  = ["valor_total", "vl_total", "vl_nf", "valr_nota_fiscal", "vlr_total_documento"]

_CANDS_CNPJ_EMIT = ["cnpj_emit", "cnpj_emissor", "numr_cpf_cnpj_saida"]
_CANDS_CNPJ_DEST = ["cnpj_dest", "cnpj_destinatario", "numr_cpf_cnpj_entrada"]
_CANDS_IE_EMIT   = ["ie_emit", "inscricao_estadual_emit", "numr_inscricao_saida"]
_CANDS_IE_DEST   = ["ie_dest", "inscricao_estadual_dest", "numr_inscricao_entrada"]

_CANDS_TIPO_DOC  = ["tipo_documento_fiscal", "tp_doc_fiscal", "tipo_doc_fiscal"]

_CANDS_IBGE_EMIT = ["ibge_emit", "codg_municipio_saida_cad", "codg_municipio_emit"]
_CANDS_UF_EMIT   = ["uf_emit", "codg_uf_saida_cad", "uf_emissor", "codg_uf_emit"]
_CANDS_IBGE_DEST = ["ibge_dest", "codg_municipio_entrada_cad", "codg_municipio_dest"]
_CANDS_UF_DEST   = ["uf_dest", "codg_uf_entrada_cad", "uf_destinatario", "codg_uf_dest"]

_CANDS_IBGE_EMIT_DOC = ["codg_municipio_saida_doc", "ibge_emit_doc"]
_CANDS_UF_EMIT_DOC   = ["codg_uf_saida_doc", "uf_emit_doc"]
_CANDS_IBGE_DEST_DOC = ["codg_municipio_entrada_doc", "ibge_dest_doc"]
_CANDS_UF_DEST_DOC   = ["codg_uf_entrada_doc", "uf_dest_doc"]

_CANDS_STAT_CAD_ENTR   = ["stat_cadastro_entrada", "stat_cad_entrada", "stat_cadastro_contrib_entrada"]
_CANDS_STAT_CAD_SAIDA  = ["stat_cadastro_saida", "stat_cad_saida", "stat_cadastro_contrib_saida"]
_CANDS_ENQ_ENTR        = ["tipo_enqdto_fiscal_entrada", "enq_fiscal_entrada"]
_CANDS_ENQ_SAIDA       = ["tipo_enqdto_fiscal_saida", "enq_fiscal_saida"]
_CANDS_PR_ENTR         = ["indi_prod_rural_entrada", "ind_prod_rural_entrada"]
_CANDS_PR_EXC_ENTR     = ["indi_prod_rural_exclus_entrada", "ind_prod_rural_exclus_entrada"]
_CANDS_PR_SAIDA        = ["indi_prod_rural_saida", "ind_prod_rural_saida"]
_CANDS_PR_EXC_SAIDA    = ["indi_prod_rural_exclus_saida", "ind_prod_rural_exclus_saida"]

_CANDS_VALR_ADIC       = ["valr_adicionado_operacao", "valor_adicionado_operacao", "valor_adicionado"]
_CANDS_INDI_APROP      = ["indi_aprop", "indic_aprop"]
_CANDS_CODG_TIPO_PARTCT = ["codg_tipo_doc_partct_calc", "tipo_doc_participante_calc", "codg_tipo_doc_partct"]
_CANDS_CODG_MOT_EXC     = ["codg_motivo_exclusao_calculo", "motivo_exclusao_calculo"]
_CANDS_NUMR_REF_DOC     = ["numr_referencia_documento", "referencia_documento"]
_CANDS_ID_PROCESM       = ["id_procesm_indice", "id_processamento_indice", "id_proc_indice"]

# ITENS
_CANDS_ITEM_SEQ   = ["num_item", "num_item_doc", "item_seq", "sequencia_item", "nr_item", "n_item", "num_item_nfe", "num_item_cte", "num_item_nf3e", "num_item_bpe", "num_item_efd", "numr_item"]
_CANDS_CFOP_ITEM  = ["cfop", "codg_cfop", "cfop_item"]
_CANDS_EAN        = ["ean", "gtin", "codg_ean"]
_CANDS_CEST       = ["cest", "codg_cest"]
_CANDS_NCM_ITEM   = ["ncm_doc", "codg_produto_ncm", "ncm", "ncm_item", "cod_ncm", "cod_ncm_item", "COD_NCM"]
_CANDS_QTDE       = ["quantidade", "qtde_comercial", "qtd", "qtde", "qtd_item", "QTD"]
_CANDS_ANP        = ["codg_produto_anp", "anp", "codigo_anp", "COD_ANP"]
_CANDS_VL_BRUTO   = ["valor_bruto", "valr_total_bruto", "vl_total_item", "VALR_TOTAL_BRUTO", "VL_ITEM"]
_CANDS_VL_DESC    = ["valr_desconto", "desconto_item", "desc_item", "VALR_DESCONTO", "VL_DESC"]
_CANDS_VL_ICMS_DES= ["valr_icms_desonera", "icms_desonerado"]
_CANDS_VL_ST      = ["valr_icms_subtrib", "icms_st"]
_CANDS_VL_MONO    = ["valr_icms_monofasico", "icms_monofasico"]
_CANDS_VL_FRETE   = ["valr_frete", "frete_item"]
_CANDS_VL_SEG     = ["valr_seguro", "seguro_item"]
_CANDS_VL_OUTRAS  = ["valr_outras_despesas", "outras_despesas_item"]
_CANDS_VL_IPI     = ["valr_ipi", "ipi_item"]
_CANDS_VL_II      = ["valr_imposto_importacao", "ii_item"]
_CANDS_VL_ADIC    = ["valr_adicionado", "valor_adicionado_item", "VALR_ADICIONADO"]
_CANDS_CODG_MOTIVO_EXC = ["codg_motivo_exclusao_calculo_item", "codg_motivo_exclusao_calculo"]
_CANDS_CODG_TIPO_DOC_PART = ["codg_tipo_doc_partct_documento", "tipo_doc_partct_documento"]
_CANDS_TIPO_MOTIVO_DESON  = ["tipo_motivo_desonera_icms", "tipo_motivo_deson_icms"]
_CANDS_ID_PROCESM_ITEM    = ["id_procesm_indice", "id_proc_indice", "ID_PROCESM_INDICE"]


# =============================================================================
# Defaults por documento
# =============================================================================

# Modelos fiscais (string de 2 dígitos em geral)
_DEFAULT_TIPO_DOC = {
    "NFE":  "55",
    "CTE":  "57",
    "BPE":  "63",
    "NF3E": "66",
    "EFD":  None,  # EFD não é um documento fiscal modelo 55/57; deixar nulo salvo se informado
}

def _resolve_doc_hint(doc_hint: Optional[str]) -> str:
    if not doc_hint:
        return "GEN"
    d = doc_hint.strip().upper()
    return d if d in {"NFE", "CTE", "BPE", "NF3E", "EFD"} else "GEN"

def _key_candidates_for(doc_hint: Optional[str]) -> List[str]:
    d = _resolve_doc_hint(doc_hint)
    return _CANDS_CHAVE_BY_DOC.get(d, _CANDS_CHAVE_BY_DOC["GEN"])

def _default_tipo_documento_fiscal_for(doc_hint: Optional[str]) -> Optional[str]:
    d = _resolve_doc_hint(doc_hint)
    return _DEFAULT_TIPO_DOC.get(d)


# =============================================================================
# Projeção de DOCUMENTO (genérica com ajustes por doc_hint)
# =============================================================================

def project_document_ipm(df_docs: DataFrame, *, doc_hint: Optional[str] = None, key_col: Optional[str] = None) -> DataFrame:
    cols = df_docs.columns

    # chave (prioriza key_col se informado)
    col_chave = None
    if key_col and key_col in cols:
        col_chave = key_col
    else:
        col_chave = _first_present(cols, _key_candidates_for(doc_hint)) or "chave_doc"

    # principais
    col_dt_emis = _first_present(cols, _CANDS_DT_EMIS)
    col_vl_tot  = _first_present(cols, _CANDS_VL_TOT)

    col_cnpj_emit = _first_present(cols, _CANDS_CNPJ_EMIT)
    col_cnpj_dest = _first_present(cols, _CANDS_CNPJ_DEST)
    col_ie_emit   = _first_present(cols, _CANDS_IE_EMIT)
    col_ie_dest   = _first_present(cols, _CANDS_IE_DEST)

    # tipo doc
    col_tipo_doc  = _first_present(cols, _CANDS_TIPO_DOC)
    tipo_doc_fallback = _default_tipo_documento_fiscal_for(doc_hint)

    # localidade (cad/doc)
    col_ibge_emit_cad = _first_present(cols, _CANDS_IBGE_EMIT)
    col_uf_emit_cad   = _first_present(cols, _CANDS_UF_EMIT)
    col_ibge_dest_cad = _first_present(cols, _CANDS_IBGE_DEST)
    col_uf_dest_cad   = _first_present(cols, _CANDS_UF_DEST)
    col_ibge_emit_doc = _first_present(cols, _CANDS_IBGE_EMIT_DOC) or col_ibge_emit_cad
    col_uf_emit_doc   = _first_present(cols, _CANDS_UF_EMIT_DOC) or col_uf_emit_cad
    col_ibge_dest_doc = _first_present(cols, _CANDS_IBGE_DEST_DOC) or col_ibge_dest_cad
    col_uf_dest_doc   = _first_present(cols, _CANDS_UF_DEST_DOC) or col_uf_dest_cad

    # status/enquadramentos
    col_stat_cad_entr = _first_present(cols, _CANDS_STAT_CAD_ENTR)
    col_stat_cad_said = _first_present(cols, _CANDS_STAT_CAD_SAIDA)
    col_enq_entr      = _first_present(cols, _CANDS_ENQ_ENTR)
    col_enq_said      = _first_present(cols, _CANDS_ENQ_SAIDA)
    col_pr_entr       = _first_present(cols, _CANDS_PR_ENTR)
    col_pr_exc_entr   = _first_present(cols, _CANDS_PR_EXC_ENTR)
    col_pr_said       = _first_present(cols, _CANDS_PR_SAIDA)
    col_pr_exc_said   = _first_present(cols, _CANDS_PR_EXC_SAIDA)

    # regras e auditoria
    col_valr_adic     = _first_present(cols, _CANDS_VALR_ADIC)
    col_indi_aprop    = _first_present(cols, _CANDS_INDI_APROP)
    col_codg_tipo_partct  = _first_present(cols, _CANDS_CODG_TIPO_PARTCT)
    col_codg_mot_exc      = _first_present(cols, _CANDS_CODG_MOT_EXC)
    col_numr_ref_doc      = _first_present(cols, _CANDS_NUMR_REF_DOC)
    col_id_procesm        = _first_present(cols, _CANDS_ID_PROCESM)

    cur = df_docs

    # chave (trim, dígitos, limite 44)
    chave_base = _to_str(F.substring(_strip(F.col(col_chave)), 1, 44), max_len=44)
    cur = cur.withColumn("CODG_CHAVE_ACESSO_NFE", chave_base)

    # tipo (usa coluna se existir; senão fallback por documento)
    if col_tipo_doc:
        cur = cur.withColumn("TIPO_DOCUMENTO_FISCAL", _to_str(F.col(col_tipo_doc), max_len=2))
    else:
        tdef = tipo_doc_fallback
        cur = cur.withColumn("TIPO_DOCUMENTO_FISCAL", F.lit(tdef).cast("string") if tdef else F.lit(None).cast("string"))

    # participantes (CPF/CNPJ com somente dígitos e padding à esquerda quando aplicável)
    if col_cnpj_dest:
        cnpj_dest = _digits_only(F.col(col_cnpj_dest))
        cur = cur.withColumn("NUMR_CPF_CNPJ_ENTRADA",
                             F.when(F.length(cnpj_dest) <= 14, F.lpad(cnpj_dest, 11, "0"))
                              .otherwise(F.substring(cnpj_dest, -14, 14)))
    else:
        cur = cur.withColumn("NUMR_CPF_CNPJ_ENTRADA", F.lit(None).cast("string"))

    if col_cnpj_emit:
        cnpj_emit = _digits_only(F.col(col_cnpj_emit))
        cur = cur.withColumn("NUMR_CPF_CNPJ_SAIDA",
                             F.when(F.length(cnpj_emit) <= 14, F.lpad(cnpj_emit, 11, "0"))
                              .otherwise(F.substring(cnpj_emit, -14, 14)))
    else:
        cur = cur.withColumn("NUMR_CPF_CNPJ_SAIDA", F.lit(None).cast("string"))

    cur = cur.withColumn("NUMR_INSCRICAO_ENTRADA",
                         _to_int(F.col(col_ie_dest)).cast(T.DecimalType(15, 0)) if col_ie_dest else F.lit(None).cast(T.DecimalType(15, 0)))
    cur = cur.withColumn("NUMR_INSCRICAO_SAIDA",
                         _to_int(F.col(col_ie_emit)).cast(T.DecimalType(14, 0)) if col_ie_emit else F.lit(None).cast(T.DecimalType(14, 0)))

    cur = cur.withColumn("STAT_CADASTRO_ENTRADA",
                         _to_str(F.col(col_stat_cad_entr)) if col_stat_cad_entr else F.lit(None).cast("string"))
    cur = cur.withColumn("TIPO_ENQDTO_FISCAL_ENTRADA",
                         _to_str(F.col(col_enq_entr)) if col_enq_entr else F.lit(None).cast("string"))
    cur = cur.withColumn("INDI_PROD_RURAL_ENTRADA",
                         _to_str(F.col(col_pr_entr)) if col_pr_entr else F.lit(None).cast("string"))
    cur = cur.withColumn("INDI_PROD_RURAL_EXCLUS_ENTRADA",
                         _to_str(F.col(col_pr_exc_entr)) if col_pr_exc_entr else F.lit(None).cast("string"))
    cur = cur.withColumn("CODG_MUNICIPIO_ENTRADA_CAD",
                         _to_int(F.col(col_ibge_dest_cad)).cast(T.DecimalType(6, 0)) if col_ibge_dest_cad else F.lit(None).cast(T.DecimalType(6, 0)))
    cur = cur.withColumn("CODG_UF_ENTRADA_CAD",
                         _to_str(F.col(col_uf_dest_cad)) if col_uf_dest_cad else F.lit(None).cast("string"))
    cur = cur.withColumn("CODG_MUNICIPIO_ENTRADA_DOC",
                         _to_int(F.col(col_ibge_dest_doc)).cast(T.DecimalType(7, 0)) if col_ibge_dest_doc else F.lit(None).cast(T.DecimalType(7, 0)))
    cur = cur.withColumn("CODG_UF_ENTRADA_DOC",
                         _to_str(F.col(col_uf_dest_doc)) if col_uf_dest_doc else F.lit(None).cast("string"))

    cur = cur.withColumn("STAT_CADASTRO_SAIDA",
                         _to_str(F.col(col_stat_cad_said)) if col_stat_cad_said else F.lit(None).cast("string"))
    cur = cur.withColumn("TIPO_ENQDTO_FISCAL_SAIDA",
                         _to_str(F.col(col_enq_said)) if col_enq_said else F.lit(None).cast("string"))
    cur = cur.withColumn("INDI_PROD_RURAL_SAIDA",
                         _to_str(F.col(col_pr_said)) if col_pr_said else F.lit(None).cast("string"))
    cur = cur.withColumn("INDI_PROD_RURAL_EXCLUS_SAIDA",
                         _to_str(F.col(col_pr_exc_said)) if col_pr_exc_said else F.lit(None).cast("string"))
    cur = cur.withColumn("CODG_MUNICIPIO_SAIDA_CAD",
                         _to_int(F.col(col_ibge_emit_cad)).cast(T.DecimalType(6, 0)) if col_ibge_emit_cad else F.lit(None).cast(T.DecimalType(6, 0)))
    cur = cur.withColumn("CODG_UF_SAIDA_CAD",
                         _to_str(F.col(col_uf_emit_cad)) if col_uf_emit_cad else F.lit(None).cast("string"))
    cur = cur.withColumn("CODG_MUNICIPIO_SAIDA_DOC",
                         _to_int(F.col(col_ibge_emit_doc)).cast(T.DecimalType(7, 0)) if col_ibge_emit_doc else F.lit(None).cast(T.DecimalType(7, 0)))
    cur = cur.withColumn("CODG_UF_SAIDA_DOC",
                         _to_str(F.col(col_uf_emit_doc)) if col_uf_emit_doc else F.lit(None).cast("string"))

    # datas e valores
    cur = cur.withColumn("DATA_EMISSAO_NFE",
                         _coerce_date_any(F.col(col_dt_emis)) if col_dt_emis else F.lit(None).cast("date"))
    cur = cur.withColumn("VALR_NOTA_FISCAL",
                         _to_dec(F.col(col_vl_tot), 17, 2) if col_vl_tot else F.lit(None).cast(T.DecimalType(17, 2)))

    # regras e auditoria
    cur = cur.withColumn("VALR_ADICIONADO_OPERACAO",
                         _to_dec(F.col(col_valr_adic), 17, 2) if col_valr_adic else F.lit(None).cast(T.DecimalType(17, 2)))
    cur = cur.withColumn("INDI_APROP",
                         _to_str(F.col(col_indi_aprop)) if col_indi_aprop else F.lit(None).cast("string"))
    cur = cur.withColumn("CODG_TIPO_DOC_PARTCT_CALC",
                         _to_int(F.col(col_codg_tipo_partct)).cast(T.DecimalType(3, 0)) if col_codg_tipo_partct else F.lit(None).cast(T.DecimalType(3, 0)))
    cur = cur.withColumn("CODG_MOTIVO_EXCLUSAO_CALCULO",
                         _to_int(F.col(col_codg_mot_exc)).cast(T.DecimalType(2, 0)) if col_codg_mot_exc else F.lit(None).cast(T.DecimalType(2, 0)))

    # AAAAMM (se não vier pronto)
    cur = cur.withColumn(
        "NUMR_REFERENCIA_DOCUMENTO",
        _to_int(F.col(col_numr_ref_doc)).cast(T.DecimalType(6, 0)) if col_numr_ref_doc else
        F.when(F.col("DATA_EMISSAO_NFE").isNotNull(),
               (F.year("DATA_EMISSAO_NFE") * F.lit(100) + F.month("DATA_EMISSAO_NFE")).cast("long"))
         .otherwise(F.lit(None).cast("long")).cast(T.DecimalType(6, 0))
    )

    # id processamento
    cur = cur.withColumn("ID_PROCESM_INDICE",
                         _to_int(F.col(col_id_procesm)).cast(T.DecimalType(9, 0)) if col_id_procesm else F.lit(None).cast(T.DecimalType(9, 0)))

    # Derivados para particionamento (mantém compat com Settings.partitioning ano,mes)
    cur = cur.withColumn("ANO", F.year("DATA_EMISSAO_NFE").cast("int"))
    cur = cur.withColumn("MES", F.month("DATA_EMISSAO_NFE").cast("int"))

    # defaults + ordem
    defaults = {
        "CODG_CHAVE_ACESSO_NFE": F.lit(None).cast("string"),
        "TIPO_DOCUMENTO_FISCAL": F.lit(None).cast("string"),
        "NUMR_CPF_CNPJ_ENTRADA": F.lit(None).cast("string"),
        "NUMR_INSCRICAO_ENTRADA": F.lit(None).cast(T.DecimalType(15, 0)),
        "STAT_CADASTRO_ENTRADA": F.lit(None).cast("string"),
        "TIPO_ENQDTO_FISCAL_ENTRADA": F.lit(None).cast("string"),
        "INDI_PROD_RURAL_ENTRADA": F.lit(None).cast("string"),
        "INDI_PROD_RURAL_EXCLUS_ENTRADA": F.lit(None).cast("string"),
        "CODG_MUNICIPIO_ENTRADA_CAD": F.lit(None).cast(T.DecimalType(6, 0)),
        "CODG_UF_ENTRADA_CAD": F.lit(None).cast("string"),
        "CODG_MUNICIPIO_ENTRADA_DOC": F.lit(None).cast(T.DecimalType(7, 0)),
        "CODG_UF_ENTRADA_DOC": F.lit(None).cast("string"),
        "NUMR_CPF_CNPJ_SAIDA": F.lit(None).cast("string"),
        "NUMR_INSCRICAO_SAIDA": F.lit(None).cast(T.DecimalType(14, 0)),
        "STAT_CADASTRO_SAIDA": F.lit(None).cast("string"),
        "TIPO_ENQDTO_FISCAL_SAIDA": F.lit(None).cast("string"),
        "INDI_PROD_RURAL_SAIDA": F.lit(None).cast("string"),
        "INDI_PROD_RURAL_EXCLUS_SAIDA": F.lit(None).cast("string"),
        "CODG_MUNICIPIO_SAIDA_CAD": F.lit(None).cast(T.DecimalType(6, 0)),
        "CODG_UF_SAIDA_CAD": F.lit(None).cast("string"),
        "CODG_MUNICIPIO_SAIDA_DOC": F.lit(None).cast(T.DecimalType(7, 0)),
        "CODG_UF_SAIDA_DOC": F.lit(None).cast("string"),
        "DATA_EMISSAO_NFE": F.lit(None).cast("date"),
        "VALR_NOTA_FISCAL": F.lit(None).cast(T.DecimalType(17, 2)),
        "VALR_ADICIONADO_OPERACAO": F.lit(None).cast(T.DecimalType(17, 2)),
        "INDI_APROP": F.lit(None).cast("string"),
        "CODG_TIPO_DOC_PARTCT_CALC": F.lit(None).cast(T.DecimalType(3, 0)),
        "CODG_MOTIVO_EXCLUSAO_CALCULO": F.lit(None).cast(T.DecimalType(2, 0)),
        "NUMR_REFERENCIA_DOCUMENTO": F.lit(None).cast(T.DecimalType(6, 0)),
        "ID_PROCESM_INDICE": F.lit(None).cast(T.DecimalType(9, 0)),
        # Partições
        "ANO": F.lit(None).cast("int"),
        "MES": F.lit(None).cast("int"),
    }
    cur = _ensure_cols(cur, defaults)
    return cur.select(*list(defaults.keys()))


# =============================================================================
# Projeção de ITENS (genérica)
# =============================================================================

def project_item_ipm(df_itens: DataFrame, *, doc_hint: Optional[str] = None, key_col: Optional[str] = None) -> DataFrame:
    cols = df_itens.columns

    # chave (prioriza key_col se informado)
    col_chave = None
    if key_col and key_col in cols:
        col_chave = key_col
    else:
        col_chave = _first_present(cols, _key_candidates_for(doc_hint)) or "chave_doc"

    col_item   = _first_present(cols, _CANDS_ITEM_SEQ)
    col_cfop   = _first_present(cols, _CANDS_CFOP_ITEM)
    col_ean    = _first_present(cols, _CANDS_EAN)
    col_cest   = _first_present(cols, _CANDS_CEST)
    col_ncm    = _first_present(cols, _CANDS_NCM_ITEM)
    col_qtde   = _first_present(cols, _CANDS_QTDE)
    col_anp    = _first_present(cols, _CANDS_ANP)
    col_vl_br  = _first_present(cols, _CANDS_VL_BRUTO)
    col_vl_desc= _first_present(cols, _CANDS_VL_DESC)
    col_vl_icms_des = _first_present(cols, _CANDS_VL_ICMS_DES)
    col_vl_st  = _first_present(cols, _CANDS_VL_ST)
    col_vl_mono= _first_present(cols, _CANDS_VL_MONO)
    col_vl_fre = _first_present(cols, _CANDS_VL_FRETE)
    col_vl_seg = _first_present(cols, _CANDS_VL_SEG)
    col_vl_out = _first_present(cols, _CANDS_VL_OUTRAS)
    col_vl_ipi = _first_present(cols, _CANDS_VL_IPI)
    col_vl_ii  = _first_present(cols, _CANDS_VL_II)
    col_vl_adic= _first_present(cols, _CANDS_VL_ADIC)
    col_mot_exc   = _first_present(cols, _CANDS_CODG_MOTIVO_EXC)
    col_tipo_docp = _first_present(cols, _CANDS_CODG_TIPO_DOC_PART)
    col_tipo_des  = _first_present(cols, _CANDS_TIPO_MOTIVO_DESON)
    col_id_proc   = _first_present(cols, _CANDS_ID_PROCESM_ITEM)

    cur = df_itens
    cur = cur.withColumn("CODG_CHAVE_ACESSO_NFE", _to_str(F.substring(_strip(F.col(col_chave)), 1, 44), max_len=44))
    cur = cur.withColumn("NUMR_ITEM", _to_int(F.col(col_item)).cast(T.DecimalType(3, 0)) if col_item else F.lit(None).cast(T.DecimalType(3, 0)))
    cur = cur.withColumn("CODG_CFOP", _to_int(F.col(col_cfop)).cast(T.DecimalType(4, 0)) if col_cfop else F.lit(None).cast(T.DecimalType(4, 0)))
    cur = cur.withColumn("CODG_EAN", _to_str(F.col(col_ean)) if col_ean else F.lit(None).cast("string"))
    cur = cur.withColumn("CODG_CEST", _to_str(F.col(col_cest)) if col_cest else F.lit(None).cast("string"))
    cur = cur.withColumn("CODG_PRODUTO_NCM", _to_int(F.col(col_ncm)).cast(T.DecimalType(9, 0)) if col_ncm else F.lit(None).cast(T.DecimalType(9, 0)))
    cur = cur.withColumn("QTDE_COMERCIAL", _to_dec(F.col(col_qtde), 19, 4) if col_qtde else F.lit(None).cast(T.DecimalType(19, 4)))
    cur = cur.withColumn("CODG_PRODUTO_ANP", _to_int(F.col(col_anp)).cast(T.DecimalType(9, 0)) if col_anp else F.lit(None).cast(T.DecimalType(9, 0)))
    cur = cur.withColumn("VALR_TOTAL_BRUTO", _to_dec(F.col(col_vl_br), 25, 2) if col_vl_br else F.lit(None).cast(T.DecimalType(25, 2)))
    cur = cur.withColumn("VALR_DESCONTO", _to_dec(F.col(col_vl_desc), 15, 2) if col_vl_desc else F.lit(None).cast(T.DecimalType(15, 2)))
    cur = cur.withColumn("VALR_ICMS_DESONERA", _to_dec(F.col(col_vl_icms_des), 15, 2) if col_vl_icms_des else F.lit(None).cast(T.DecimalType(15, 2)))
    cur = cur.withColumn("VALR_ICMS_SUBTRIB", _to_dec(F.col(col_vl_st), 17, 2) if col_vl_st else F.lit(None).cast(T.DecimalType(17, 2)))
    cur = cur.withColumn("VALR_ICMS_MONOFASICO", _to_dec(F.col(col_vl_mono), 15, 2) if col_vl_mono else F.lit(None).cast(T.DecimalType(15, 2)))
    cur = cur.withColumn("VALR_FRETE", _to_dec(F.col(col_vl_fre), 15, 2) if col_vl_fre else F.lit(None).cast(T.DecimalType(15, 2)))
    cur = cur.withColumn("VALR_SEGURO", _to_dec(F.col(col_vl_seg), 15, 2) if col_vl_seg else F.lit(None).cast(T.DecimalType(15, 2)))
    cur = cur.withColumn("VALR_OUTRAS_DESPESAS", _to_dec(F.col(col_vl_out), 15, 2) if col_vl_out else F.lit(None).cast(T.DecimalType(15, 2)))
    cur = cur.withColumn("VALR_IPI", _to_dec(F.col(col_vl_ipi), 15, 2) if col_vl_ipi else F.lit(None).cast(T.DecimalType(15, 2)))
    cur = cur.withColumn("VALR_IMPOSTO_IMPORTACAO", _to_dec(F.col(col_vl_ii), 15, 2) if col_vl_ii else F.lit(None).cast(T.DecimalType(15, 2)))
    cur = cur.withColumn("VALR_ADICIONADO", _to_dec(F.col(col_vl_adic), 17, 2) if col_vl_adic else F.lit(None).cast(T.DecimalType(17, 2)))
    cur = cur.withColumn("CODG_MOTIVO_EXCLUSAO_CALCULO",
                         _to_int(F.col(col_mot_exc)).cast(T.DecimalType(2, 0)) if col_mot_exc else F.lit(None).cast(T.DecimalType(2, 0)))
    cur = cur.withColumn("CODG_TIPO_DOC_PARTCT_DOCUMENTO",
                         _to_int(F.col(col_tipo_docp)).cast(T.DecimalType(3, 0)) if col_tipo_docp else F.lit(None).cast(T.DecimalType(3, 0)))
    cur = cur.withColumn("TIPO_MOTIVO_DESONERA_ICMS",
                         _to_int(F.col(col_tipo_des)).cast(T.DecimalType(2, 0)) if col_tipo_des else F.lit(None).cast(T.DecimalType(2, 0)))
    cur = cur.withColumn("ID_PROCESM_INDICE",
                         _to_int(F.col(col_id_proc)).cast(T.DecimalType(9, 0)) if col_id_proc else F.lit(None).cast(T.DecimalType(9, 0)))

    defaults = {
        "CODG_CHAVE_ACESSO_NFE": F.lit(None).cast("string"),
        "NUMR_ITEM": F.lit(None).cast(T.DecimalType(3, 0)),
        "CODG_CFOP": F.lit(None).cast(T.DecimalType(4, 0)),
        "CODG_EAN": F.lit(None).cast("string"),
        "CODG_CEST": F.lit(None).cast("string"),
        "CODG_PRODUTO_NCM": F.lit(None).cast(T.DecimalType(9, 0)),
        "QTDE_COMERCIAL": F.lit(None).cast(T.DecimalType(19, 4)),
        "CODG_PRODUTO_ANP": F.lit(None).cast(T.DecimalType(9, 0)),
        "VALR_TOTAL_BRUTO": F.lit(None).cast(T.DecimalType(25, 2)),
        "VALR_DESCONTO": F.lit(None).cast(T.DecimalType(15, 2)),
        "VALR_ICMS_DESONERA": F.lit(None).cast(T.DecimalType(15, 2)),
        "VALR_ICMS_SUBTRIB": F.lit(None).cast(T.DecimalType(17, 2)),
        "VALR_ICMS_MONOFASICO": F.lit(None).cast(T.DecimalType(15, 2)),
        "VALR_FRETE": F.lit(None).cast(T.DecimalType(15, 2)),
        "VALR_SEGURO": F.lit(None).cast(T.DecimalType(15, 2)),
        "VALR_OUTRAS_DESPESAS": F.lit(None).cast(T.DecimalType(15, 2)),
        "VALR_IPI": F.lit(None).cast(T.DecimalType(15, 2)),
        "VALR_IMPOSTO_IMPORTACAO": F.lit(None).cast(T.DecimalType(15, 2)),
        "VALR_ADICIONADO": F.lit(None).cast(T.DecimalType(17, 2)),
        "CODG_MOTIVO_EXCLUSAO_CALCULO": F.lit(None).cast(T.DecimalType(2, 0)),
        "CODG_TIPO_DOC_PARTCT_DOCUMENTO": F.lit(None).cast(T.DecimalType(3, 0)),
        "TIPO_MOTIVO_DESONERA_ICMS": F.lit(None).cast(T.DecimalType(2, 0)),
        "ID_PROCESM_INDICE": F.lit(None).cast(T.DecimalType(9, 0)),
    }
    cur = _ensure_cols(cur, defaults)
    return cur.select(*list(defaults.keys()))


# =============================================================================
# Dispatchers: por documento
# =============================================================================

def project_document(df_docs: DataFrame, *, doc_hint: Optional[str] = None, key_col: Optional[str] = None, **_) -> DataFrame:
    """
    doc_hint: "NFE", "CTE", "BPE", "NF3E", "EFD" (case-insensitive). Opcional.
    key_col: nome da coluna da chave no DF (ex.: 'chavenfe', 'chnf3e', etc.). Opcional.
    """
    return project_document_ipm(df_docs, doc_hint=doc_hint, key_col=key_col)

def project_item(df_itens: DataFrame, *, doc_hint: Optional[str] = None, key_col: Optional[str] = None, **_) -> DataFrame:
    """
    doc_hint: "NFE", "CTE", "BPE", "NF3E", "EFD" (case-insensitive). Opcional.
    key_col: nome da coluna da chave no DF (ex.: 'chavenfe', 'chnf3e', etc.). Opcional.
    """
    return project_item_ipm(df_itens, doc_hint=doc_hint, key_col=key_col)
