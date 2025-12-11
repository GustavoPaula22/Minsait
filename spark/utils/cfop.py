# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
# -*- coding: latin-1 -*-
# app/utils/cfop.py
"""
Regras de CFOP (ParticipaÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ£o):

O que este mÃÂÃÂÃÂÃÂ³dulo faz:
- Normaliza a coluna de CFOP do documento (4 dÃÂÃÂÃÂÃÂ­gitos, string).
- Normaliza a tabela de CFOP participante (colunas CFOP e indicador).
- Realiza join para marcar `is_cfop_participante` (True/False).
- (Opcional) Filtra apenas as linhas participantes.
- (Opcional) Grava motivo de exclusÃÂÃÂÃÂÃÂ£o em `motivo_exclusao_cfop` quando nÃÂÃÂÃÂÃÂ£o participante.

ObservaÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂµes:
- A tabela de CFOP participante no Oracle pode ter nomes de colunas variados.
  Este mÃÂÃÂÃÂÃÂ³dulo tenta detectar automaticamente as colunas relevantes.
- Se necessÃÂÃÂÃÂÃÂ¡rio, inclua mais aliases nos candidatos abaixo.

Uso tÃÂÃÂÃÂÃÂ­pico:
    aux = read_oracle_ipm_auxiliares(spark, settings)
    df_cfop_ref = normalize_cfop_participante(aux["cfop_participante"])
    df_out = apply_cfop_rules(df_docs, df_cfop_ref, filter_non_participants=False)
"""

from __future__ import annotations

from typing import Iterable, List, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F, types as T


# =============================================================================
# Colunas candidatas
# =============================================================================

# No DF de documentos (Kudu/Oracle), procure por:
_DOC_CFOP_CANDS = [
    # nomes comuns em bronzes de NFe
    "cfop",
    "co_cfop",
    "cod_cfop",
    "codigo_cfop",
    "cfop_cod",
    "cfop_codigo",
    # itens:
    "cfop_item",
    "codigo_cfop_item",
]

# Na tabela de referÃÂÃÂÃÂÃÂªncia CFOP participante (Oracle):
_REF_CFOP_CODE_CANDS = [
    "CFOP", "CO_CFOP", "COD_CFOP", "CODIGO_CFOP"
]
_REF_CFOP_FLAG_CANDS = [
    # valores esperados: 'S'/'N', 1/0, TRUE/FALSE (qualquer case)
    "PARTICIPANTE",
    "IN_PARTICIPANTE",
    "FL_PARTICIPANTE",
    "FLAG_PARTICIPANTE",
    "IND_PARTICIPANTE",
    "IN_PARTICIPACAO",
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


def _normalize_cfop_expr(col: F.Column) -> F.Column:
    """
    Normaliza CFOP para string de 4 dÃÂÃÂÃÂÃÂ­gitos (zero-left pad quando aplicÃÂÃÂÃÂÃÂ¡vel).
    - Remove tudo que nÃÂÃÂÃÂÃÂ£o for dÃÂÃÂÃÂÃÂ­gito.
    - Se tiver 3 dÃÂÃÂÃÂÃÂ­gitos, left-pad para 4 (regra comum em bases antigas).
    - Se tiver mais que 4, mantÃÂÃÂÃÂÃÂ©m somente os 4 primeiros dÃÂÃÂÃÂÃÂ­gitos (heurÃÂÃÂÃÂÃÂ­stica defensiva).
    """
    only_digits = F.regexp_replace(F.trim(col), r"\D", "")
    size = F.length(only_digits)
    cfop4 = (
        F.when(size == 4, only_digits)
         .when(size == 3, F.lpad(only_digits, 4, "0"))
         .when(size > 4, F.substring(only_digits, 1, 4))
         .otherwise(only_digits)
    )
    # pad final para garantir 4 se ainda estiver curto (ex.: vazio -> "")
    return F.when(F.length(cfop4) == 4, cfop4).otherwise(cfop4)


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


# =============================================================================
# NormalizaÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ£o da tabela de referÃÂÃÂÃÂÃÂªncia (Oracle)
# =============================================================================

def normalize_cfop_participante(df_ref_raw: DataFrame) -> DataFrame:
    """
    Padroniza a tabela de CFOP participante para colunas:
      [cfop STRING(4), is_participante BOOLEAN]
    Remove duplicatas por cfop priorizando flags nÃÂÃÂÃÂÃÂ£o nulas (S/1/TRUE).
    """
    cols = df_ref_raw.columns
    col_cfop = _first_present(cols, _REF_CFOP_CODE_CANDS)
    col_flag = _first_present(cols, _REF_CFOP_FLAG_CANDS)

    if not col_cfop or not col_flag:
        raise ValueError(
            f"[CFOP] NÃÂÃÂÃÂÃÂ£o encontrei colunas compatÃÂÃÂÃÂÃÂ­veis na referÃÂÃÂÃÂÃÂªncia. "
            f"Esperava CFOP em {_REF_CFOP_CODE_CANDS}, e FLAG em {_REF_CFOP_FLAG_CANDS}. "
            f"Colunas: {cols}"
        )

    dfn = (
        df_ref_raw
        .withColumn("__cfop_norm", _normalize_cfop_expr(F.col(col_cfop)))
        .withColumn("__flag_bool", _normalize_flag_expr(F.col(col_flag)))
        .select(
            F.col("__cfop_norm").alias("cfop"),
            F.col("__flag_bool").alias("is_participante")
        )
        .filter(F.col("cfop").isNotNull() & (F.length(F.col("cfop")) == 4))
    )

    # Dedup: prioriza registros onde is_participante nÃÂÃÂÃÂÃÂ£o ÃÂÃÂÃÂÃÂ© nulo
    dfn = (dfn
           .withColumn("__rank", F.when(F.col("is_participante").isNull(), F.lit(1)).otherwise(F.lit(0)))
           .orderBy("cfop", "__rank")
           .drop("__rank")
           .dropDuplicates(["cfop"])
    )
    return dfn


# =============================================================================
# AplicaÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ£o da regra no DF de documentos
# =============================================================================

def _ensure_doc_cfop(df_doc: DataFrame, doc_cfop_col: Optional[str]) -> Tuple[DataFrame, str]:
    """
    Garante a existÃÂÃÂÃÂÃÂªncia de uma coluna `cfop_doc` normalizada (4 dÃÂÃÂÃÂÃÂ­gitos).
    Se doc_cfop_col nÃÂÃÂÃÂÃÂ£o for informado, tenta detectar dentre os candidatos.
    """
    if doc_cfop_col is None:
        doc_cfop_col = next((c for c in _DOC_CFOP_CANDS if c in df_doc.columns), None)

    if not doc_cfop_col:
        # cria coluna vazia (regra nÃÂÃÂÃÂÃÂ£o poderÃÂÃÂÃÂÃÂ¡ classificar; resultado serÃÂÃÂÃÂÃÂ¡ nÃÂÃÂÃÂÃÂ£o participante)
        return df_doc.withColumn("cfop_doc", F.lit(None).cast("string")), "cfop_doc"

    dfn = df_doc.withColumn("cfop_doc", _normalize_cfop_expr(F.col(doc_cfop_col)))
    return dfn, "cfop_doc"


def apply_cfop_rules(
    df_doc: DataFrame,
    df_cfop_participante: DataFrame,
    *,
    doc_cfop_col: Optional[str] = None,
    filter_non_participants: bool = False,
    add_exclusion_reason: bool = True,
    exclusion_reason_value: str = "CFOP nÃÂÃÂÃÂÃÂ£o participante"
) -> DataFrame:
    """
    Aplica regra de participaÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ£o por CFOP ao DF de documentos.

    :param df_cfop_participante: DF normalizado via `normalize_cfop_participante`
                                 [cfop, is_participante]
    :param doc_cfop_col: nome da coluna CFOP no documento (opcional; auto-detecta)
    :param filter_non_participants: se True, remove nÃÂÃÂÃÂÃÂ£o participantes
    :param add_exclusion_reason: se True, cria/atualiza `motivo_exclusao_cfop`
    :param exclusion_reason_value: texto padrÃÂÃÂÃÂÃÂ£o do motivo de exclusÃÂÃÂÃÂÃÂ£o
    """
    # Garante cfop_doc
    dfn, cfop_col = _ensure_doc_cfop(df_doc, doc_cfop_col)

    # Join com referÃÂÃÂÃÂÃÂªncia
    j = (dfn
         .join(df_cfop_participante.alias("ref"), on=[F.col(cfop_col) == F.col("ref.cfop")], how="left")
         .withColumn("is_cfop_participante", F.coalesce(F.col("ref.is_participante"), F.lit(False)))
         .drop("ref.cfop", "ref.is_participante")
    )

    # Motivo de exclusÃÂÃÂÃÂÃÂ£o (opcional)
    if add_exclusion_reason:
        j = j.withColumn(
            "motivo_exclusao_cfop",
            F.when(F.col("is_cfop_participante") == F.lit(False), F.lit(exclusion_reason_value)).otherwise(F.lit(None).cast("string"))
        )

    # Filtragem (opcional)
    if filter_non_participants:
        j = j.filter(F.col("is_cfop_participante") == F.lit(True))

    return j
