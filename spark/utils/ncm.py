# -*- coding: utf-8 -*-
# app/utils/ncm.py
"""
Regras de NCM (Participante) e MonofÃÂÃÂÃÂÃÂ¡sico (opcional)

Este mÃÂÃÂÃÂÃÂ³dulo oferece:
- NormalizaÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ£o de NCM para 8 dÃÂÃÂÃÂÃÂ­gitos (string).
- NormalizaÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ£o da tabela de NCM participante (Oracle).
- AplicaÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ£o da regra no DF de itens/documentos: marca `is_ncm_participante`.
- (Opcional) DetecÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ£o/propagaÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ£o de `is_monofasico`:
    * se existir coluna no item (ex.: "icms_monofasico" / "monofasico" / "in_monofasico" ...), converte para boolean
    * ou se for fornecida uma referÃÂÃÂÃÂÃÂªncia externa (DF) por NCM
- `motivo_exclusao_ncm` para itens/documentos excluÃÂÃÂÃÂÃÂ­dos por NCM nÃÂÃÂÃÂÃÂ£o participante.

ObservaÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂµes:
- Se sua referÃÂÃÂÃÂÃÂªncia tiver colunas adicionais (ex.: EX-TIPI), vocÃÂÃÂÃÂÃÂª pode estender `normalize_ncm_participante`
  e `apply_ncm_rules` para chave composta (NCM, EX). Aqui padronizamos por NCM (8 dÃÂÃÂÃÂÃÂ­gitos) por default.
"""

from __future__ import annotations

from typing import Iterable, List, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F, types as T


# =============================================================================
# Colunas candidatas
# =============================================================================

# No DF de itens/documentos (bronze Kudu ou Oracle)
_DOC_NCM_CANDS = [
    "ncm",
    "co_ncm",
    "cod_ncm",
    "codigo_ncm",
    "ncm_item",
    "ncmproduto",
    "ncm_produto",
    "ncmmercadoria",
]

# Indicadores de monofÃÂÃÂÃÂÃÂ¡sico que podem existir direto no item
_DOC_MONOFLAG_CANDS = [
    "icms_monofasico",
    "in_monofasico",
    "monofasico",
    "fl_monofasico",
    "is_monofasico",
]

# Na referÃÂÃÂÃÂÃÂªncia NCM participante (Oracle)
_REF_NCM_CODE_CANDS = [
    "NCM", "CO_NCM", "COD_NCM", "CODIGO_NCM"
]
_REF_NCM_FLAG_CANDS = [
    "PARTICIPANTE",
    "IN_PARTICIPANTE",
    "FL_PARTICIPANTE",
    "FLAG_PARTICIPANTE",
    "IND_PARTICIPANTE",
    "IN_PARTICIPACAO",
]

# (Opcional) referÃÂÃÂÃÂÃÂªncia monofÃÂÃÂÃÂÃÂ¡sico por NCM
_REF_MONO_CODE_CANDS = _REF_NCM_CODE_CANDS
_REF_MONO_FLAG_CANDS = [
    "MONOFASICO",
    "ICMS_MONOFASICO",
    "IN_MONOFASICO",
    "FL_MONOFASICO",
    "IS_MONOFASICO",
]


# =============================================================================
# Helpers
# =============================================================================

def _first_present(cols: Iterable[str], candidates: List[str]) -> Optional[str]:
    u = {c.upper(): c for c in cols}
    for cand in candidates:
        if cand.upper() in u:
            return u[cand.upper()]
    return None


def _normalize_ncm_expr(col: F.Column) -> F.Column:
    """
    Normaliza NCM para string de 8 dÃÂÃÂÃÂÃÂ­gitos:
      - remove tudo que nÃÂÃÂÃÂÃÂ£o for dÃÂÃÂÃÂÃÂ­gito
      - corta/left-pad para 8
    """
    only_digits = F.regexp_replace(F.trim(col), r"\D", "")
    size = F.length(only_digits)
    ncm8 = (
        F.when(size >= 8, F.substring(only_digits, 1, 8))
         .otherwise(F.lpad(only_digits, 8, "0"))
    )
    # Se vazio, retorna null
    return F.when(F.length(only_digits) > 0, ncm8).otherwise(F.lit(None).cast("string"))


def _normalize_flag_expr(col: F.Column) -> F.Column:
    """
    Converte indicadores variados para boolean:
      - 'S', '1', 'TRUE', 'T', 'SIM', 'Y' -> True
      - 'N', '0', 'FALSE', 'F', 'NAO', 'NÃÂÃÂÃÂÃÂO' -> False
      - outros -> null
    """
    v = F.upper(F.trim(col))
    return (
        F.when(v.isNull(), F.lit(None).cast("boolean"))
         .when(v.isin("S", "1", "TRUE", "T", "SIM", "Y"), F.lit(True))
         .when(v.isin("N", "0", "FALSE", "F", "NAO", "NÃÂÃÂÃÂÃÂO", "NAO."), F.lit(False))
         .otherwise(F.lit(None).cast("boolean"))
    )


def _ensure_doc_ncm(df_doc: DataFrame, doc_ncm_col: Optional[str]) -> Tuple[DataFrame, str]:
    """
    Garante uma coluna `ncm_doc` normalizada (8 dÃÂÃÂÃÂÃÂ­gitos).
    """
    if doc_ncm_col is None:
        doc_ncm_col = next((c for c in _DOC_NCM_CANDS if c in df_doc.columns), None)

    if not doc_ncm_col:
        return df_doc.withColumn("ncm_doc", F.lit(None).cast("string")), "ncm_doc"

    dfn = df_doc.withColumn("ncm_doc", _normalize_ncm_expr(F.col(doc_ncm_col)))
    return dfn, "ncm_doc"


def _ensure_doc_monofasico_flag(df_doc: DataFrame) -> DataFrame:
    """
    Se existir alguma coluna sugestiva de monofÃÂÃÂÃÂÃÂ¡sico, cria/normaliza `is_monofasico`.
    Caso contrÃÂÃÂÃÂÃÂ¡rio, deixa `is_monofasico` como null (nÃÂÃÂÃÂÃÂ£o infere).
    """
    present = next((c for c in _DOC_MONOFLAG_CANDS if c in df_doc.columns), None)
    if not present:
        if "is_monofasico" not in df_doc.columns:
            return df_doc.withColumn("is_monofasico", F.lit(None).cast("boolean"))
        return df_doc

    v = F.upper(F.trim(F.col(present)))
    mono = (
        F.when(v.isin("S", "1", "TRUE", "T", "SIM", "Y"), F.lit(True))
         .when(v.isin("N", "0", "FALSE", "F", "NAO", "NÃÂÃÂÃÂÃÂO", "NAO."), F.lit(False))
         .otherwise(F.lit(None).cast("boolean"))
    )
    return df_doc.withColumn("is_monofasico", mono)


# =============================================================================
# NormalizaÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ£o de referÃÂÃÂÃÂÃÂªncias
# =============================================================================

def normalize_ncm_participante(df_ref_raw: DataFrame) -> DataFrame:
    """
    Padroniza a tabela de NCM participante para:
      [ncm STRING(8), is_participante BOOLEAN]
    Remove duplicatas por ncm priorizando flags nÃÂÃÂÃÂÃÂ£o nulas.
    """
    cols = df_ref_raw.columns
    col_ncm = _first_present(cols, _REF_NCM_CODE_CANDS)
    col_flag = _first_present(cols, _REF_NCM_FLAG_CANDS)

    if not col_ncm or not col_flag:
        raise ValueError(
            f"[NCM] NÃÂÃÂÃÂÃÂ£o encontrei colunas compatÃÂÃÂÃÂÃÂ­veis na referÃÂÃÂÃÂÃÂªncia. "
            f"Esperava NCM em {_REF_NCM_CODE_CANDS} e FLAG em {_REF_NCM_FLAG_CANDS}. "
            f"Colunas: {cols}"
        )

    dfn = (
        df_ref_raw
        .withColumn("__ncm_norm", _normalize_ncm_expr(F.col(col_ncm)))
        .withColumn("__flag_bool", _normalize_flag_expr(F.col(col_flag)))
        .select(
            F.col("__ncm_norm").alias("ncm"),
            F.col("__flag_bool").alias("is_participante")
        )
        .filter(F.col("ncm").isNotNull() & (F.length(F.col("ncm")) == 8))
    )

    dfn = (dfn
           .withColumn("__rank", F.when(F.col("is_participante").isNull(), F.lit(1)).otherwise(F.lit(0)))
           .orderBy("ncm", "__rank")
           .drop("__rank")
           .dropDuplicates(["ncm"])
    )
    return dfn


def normalize_monofasico_ref(df_ref_raw: DataFrame) -> DataFrame:
    """
    (Opcional) Normaliza referÃÂÃÂÃÂÃÂªncia de monofÃÂÃÂÃÂÃÂ¡sico por NCM:
      [ncm STRING(8), is_monofasico BOOLEAN]
    Use caso exista uma tabela com essa marcaÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ£o.
    """
    cols = df_ref_raw.columns
    col_ncm = _first_present(cols, _REF_MONO_CODE_CANDS)
    col_flag = _first_present(cols, _REF_MONO_FLAG_CANDS)

    if not col_ncm or not col_flag:
        raise ValueError(
            f"[NCM] NÃÂÃÂÃÂÃÂ£o encontrei colunas compatÃÂÃÂÃÂÃÂ­veis na referÃÂÃÂÃÂÃÂªncia monofÃÂÃÂÃÂÃÂ¡sico. "
            f"NCM em {_REF_MONO_CODE_CANDS} e FLAG em {_REF_MONO_FLAG_CANDS}. Colunas: {cols}"
        )

    dfn = (
        df_ref_raw
        .withColumn("__ncm_norm", _normalize_ncm_expr(F.col(col_ncm)))
        .withColumn("__flag_bool", _normalize_flag_expr(F.col(col_flag)))
        .select(
            F.col("__ncm_norm").alias("ncm"),
            F.col("__flag_bool").alias("is_monofasico")
        )
        .filter(F.col("ncm").isNotNull() & (F.length(F.col("ncm")) == 8))
    )

    dfn = (dfn
           .withColumn("__rank", F.when(F.col("is_monofasico").isNull(), F.lit(1)).otherwise(F.lit(0)))
           .orderBy("ncm", "__rank")
           .drop("__rank")
           .dropDuplicates(["ncm"])
    )
    return dfn


# =============================================================================
# AplicaÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ£o da regra no DF de itens/documentos
# =============================================================================

def apply_ncm_rules(
    df_doc_or_itens: DataFrame,
    df_ncm_participante: DataFrame,
    *,
    doc_ncm_col: Optional[str] = None,
    df_monofasico_ref: Optional[DataFrame] = None,
    filter_non_participants: bool = False,
    add_exclusion_reason: bool = True,
    exclusion_reason_value: str = "NCM nÃÂÃÂÃÂÃÂ£o participante"
) -> DataFrame:
    """
    Aplica regra de participaÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ£o por NCM. Opcionalmente adiciona flag monofÃÂÃÂÃÂÃÂ¡sico.
      - df_ncm_participante deve vir de normalize_ncm_participante().
      - df_monofasico_ref, se fornecido, deve vir de normalize_monofasico_ref().

    Colunas adicionadas:
      - ncm_doc (8 dÃÂÃÂÃÂÃÂ­gitos, string)
      - is_ncm_participante (boolean)
      - is_monofasico (boolean, se coluna jÃÂÃÂÃÂÃÂ¡ existir no item OU se df_monofasico_ref for fornecido)
      - motivo_exclusao_ncm (string, se add_exclusion_reason=True)
    """
    # Garante NCM no documento/itens
    dfn, ncm_col = _ensure_doc_ncm(df_doc_or_itens, doc_ncm_col)

    # Join com referÃÂÃÂÃÂÃÂªncia de participaÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ£o
    j = (dfn
         .join(df_ncm_participante.alias("ref"), on=[F.col(ncm_col) == F.col("ref.ncm")], how="left")
         .withColumn("is_ncm_participante", F.coalesce(F.col("ref.is_participante"), F.lit(False)))
         .drop("ref.ncm", "ref.is_participante")
    )

    # MonofÃÂÃÂÃÂÃÂ¡sico (prioridade: flag nativa do item ? referÃÂÃÂÃÂÃÂªncia externa, se fornecida)
    j = _ensure_doc_monofasico_flag(j)

    if df_monofasico_ref is not None:
        # join por NCM para preencher is_monofasico quando null
        j = (j
             .join(df_monofasico_ref.alias("mono"), on=[F.col(ncm_col) == F.col("mono.ncm")], how="left")
             .withColumn("is_monofasico",
                         F.when(F.col("is_monofasico").isNull(), F.col("mono.is_monofasico"))
                          .otherwise(F.col("is_monofasico")))
             .drop("mono.ncm", "mono.is_monofasico")
        )

    # Motivo de exclusÃÂÃÂÃÂÃÂ£o (opcional)
    if add_exclusion_reason:
        j = j.withColumn(
            "motivo_exclusao_ncm",
            F.when(F.col("is_ncm_participante") == F.lit(False), F.lit(exclusion_reason_value))
             .otherwise(F.lit(None).cast("string"))
        )

    # Filtragem (opcional)
    if filter_non_participants:
        j = j.filter(F.col("is_ncm_participante") == F.lit(True))

    return j
