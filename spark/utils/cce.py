# -*- coding: utf-8 -*-
# app/utils/cce.py
"""
Eventos/CCE (cancelamento, substituiÃ§Ã£o, carta de correÃ§Ã£o)

O que este mÃ³dulo faz:
- Normaliza a tabela de eventos (Oracle) para colunas canÃ´nicas:
  [chave STRING, tipo STRING, ts_evento TIMESTAMP,
   is_cancelamento BOOLEAN, is_substituicao BOOLEAN, is_cce BOOLEAN]
- Consolida por chave (Ãºltimo evento por timestamp e flags agregadas).
- Aplica ao DF de documentos (join por chave) e marca:
  is_cancelado, dt_cancelamento, is_substituido, has_cce, motivo_exclusao_evento.
- Opcionalmente, filtra documentos cancelados/substituÃ­dos.

ObservaÃ§Ãµes:
- Detecta colunas mesmo com variaÃ§Ãµes de nomes (ver listas de candidatos).
- Reconhece cÃ³digos e textos para identificar o tipo de evento.
- CompatÃ­vel com NFe/CTe/BPe/NFCe, desde que a "chave" fique consistente.

Uso tÃ­pico:
    aux = read_oracle_ipm_auxiliares(spark, settings)
    df_evt_norm = normalize_eventos(aux["evento"])
    df_out = apply_cce_rules(df_docs, df_evt_norm, chave_doc_col=None, filter_excluded=False)
"""

from __future__ import annotations

from typing import Iterable, List, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F, types as T


# =============================================================================
# Candidatos de coluna
# =============================================================================

# Nas tabelas de eventos (Oracle)
_EVT_CHAVE_CANDS = [
    "CHAVE", "CHAVENFE", "CHAVE_NFE", "CHAVE_DOC", "CHAVE_DOCUMENTO",
    "ID_CHAVE", "CO_CHAVE", "CHV_NFE"
]
_EVT_TIPO_CANDS = [
    "TIPO_EVENTO", "TP_EVENTO", "DS_EVENTO", "DESC_EVENTO", "EVENTO", "TP_EVT", "CD_EVENTO", "COD_EVENTO"
]
_EVT_DATA_CANDS = [
    "DATA_EVENTO", "DT_EVENTO", "DH_EVENTO", "TS_EVENTO", "DATAHORA_EVENTO", "DATAHORA", "DT_HR_EVENTO"
]

# No DF de documentos, a coluna-chave pode variar:
_DOC_CHAVE_CANDS = [
    "chavenfe", "chave", "chave_nfe", "chv_nfe", "chave_documento", "id_chave"
]

# CÃ³digos oficiais mais comuns (podem variar entre docs):
#   110111: Cancelamento
#   110110: Carta de CorreÃ§Ã£o (CCE)
#   110112: InutilizaÃ§Ã£o (tratamos como exclusÃ£o)
#   110160/110140: SubstituiÃ§Ã£o / Eventos correlatos (depende do modal)
_EVT_CODES_CANCEL = {"110111"}
_EVT_CODES_CCE    = {"110110"}
_EVT_CODES_INUTIL = {"110112"}
# SubstituiÃ§Ã£o: hÃ¡ variaÃ§Ãµes por modal; aqui capturamos pelo texto + alguns cÃ³digos comuns
_EVT_CODES_SUBST  = {"110160", "110140", "240650"}  # lista aberta (ex.: CTe/BPe tÃªm variaÃ§Ãµes)


# =============================================================================
# Helpers
# =============================================================================

def _first_present(cols: Iterable[str], candidates: List[str]) -> Optional[str]:
    u = {c.upper(): c for c in cols}
    for cand in candidates:
        if cand.upper() in u:
            return u[cand.upper()]
    return None


def _is_code_in(col: F.Column, codes: set) -> F.Column:
    """
    Detecta por cÃ³digo numÃ©rico/textual em uma coluna "tipo".
    """
    v = F.upper(F.trim(col))
    # extrai somente dÃ­gitos (para o caso de "110111 - CANCELAMENTO")
    digits = F.regexp_replace(v, r"\D", "")
    return F.when(digits.isin(list(codes)), F.lit(True)).otherwise(F.lit(False))


def _is_text_like(col: F.Column, *tokens: str) -> F.Column:
    """
    Detecta por presenÃ§a de tokens textuais em qualquer ordem (case-insensitive).
    """
    v = F.upper(F.trim(col))
    expr = None
    for tok in tokens:
        cond = v.like(f"%{tok.upper()}%")
        expr = cond if expr is None else (expr | cond)
    return F.when(expr, F.lit(True)).otherwise(F.lit(False))


# =============================================================================
# NormalizaÃ§Ã£o de eventos
# =============================================================================

def normalize_eventos(df_evt_raw: DataFrame) -> DataFrame:
    """
    Retorna DF normalizado com colunas canÃ´nicas:
      [chave STRING, tipo STRING, ts_evento TIMESTAMP,
       is_cancelamento BOOLEAN, is_substituicao BOOLEAN, is_cce BOOLEAN, is_inutilizacao BOOLEAN]
    """
    cols = df_evt_raw.columns
    col_chave = _first_present(cols, _EVT_CHAVE_CANDS)
    col_tipo  = _first_present(cols, _EVT_TIPO_CANDS)
    col_data  = _first_present(cols, _EVT_DATA_CANDS)

    if not col_chave or not col_tipo:
        raise ValueError(
            f"[CCE] NÃ£o encontrei colunas mÃ­nimas na referÃªncia de eventos. "
            f"Chave em {_EVT_CHAVE_CANDS}, Tipo em {_EVT_TIPO_CANDS}. Colunas: {cols}"
        )

    # ts_evento: tenta converter a melhor coluna de data/hora; se nÃ£o houver, usa current_timestamp() como fallback
    if col_data:
        ts_expr = F.to_timestamp(F.col(col_data))
    else:
        ts_expr = F.current_timestamp()

    v = F.upper(F.trim(F.col(col_tipo)))

    is_cancel = _is_code_in(v, _EVT_CODES_CANCEL) | _is_text_like(v, "CANCEL")
    is_cce    = _is_code_in(v, _EVT_CODES_CCE)    | _is_text_like(v, "CORRECAO", "CORREÃÃO", "CCE", "CARTA")
    is_inutil = _is_code_in(v, _EVT_CODES_INUTIL) | _is_text_like(v, "INUTIL")
    is_subst  = _is_code_in(v, _EVT_CODES_SUBST)  | _is_text_like(v, "SUBSTITUI", "SUBST", "SUBSTITUIÃÃO")

    dfn = (
        df_evt_raw
        .select(
            F.trim(F.col(col_chave)).alias("chave"),
            F.col(col_tipo).alias("tipo"),
            ts_expr.alias("ts_evento"),
            is_cancel.cast("boolean").alias("is_cancelamento"),
            is_subst.cast("boolean").alias("is_substituicao"),
            is_cce.cast("boolean").alias("is_cce"),
            is_inutil.cast("boolean").alias("is_inutilizacao"),
        )
        .filter(F.col("chave").isNotNull() & (F.col("chave") != ""))
    )

    return dfn


def collapse_eventos_por_chave(df_evt_norm: DataFrame) -> DataFrame:
    """
    Consolida mÃºltiplos eventos por chave:
    - dt_cancelamento: MAX(ts_evento) onde is_cancelamento=True
    - is_cancelado:    MAX(is_cancelamento)
    - is_substituido:  MAX(is_substituicao)
    - has_cce:         MAX(is_cce)
    - has_inutil:      MAX(is_inutilizacao)
    - ts_ultimo_evento: MAX(ts_evento)
    """
    agg = (df_evt_norm
           .groupBy("chave")
           .agg(
               F.max(F.when(F.col("is_cancelamento"), F.col("ts_evento"))).alias("dt_cancelamento"),
               F.max(F.col("is_cancelamento").cast("int")).alias("is_cancelado_int"),
               F.max(F.col("is_substituicao").cast("int")).alias("is_substituido_int"),
               F.max(F.col("is_cce").cast("int")).alias("has_cce_int"),
               F.max(F.col("is_inutilizacao").cast("int")).alias("has_inutil_int"),
               F.max("ts_evento").alias("ts_ultimo_evento"),
           )
           .select(
               F.col("chave"),
               F.col("dt_cancelamento"),
               (F.col("is_cancelado_int") == 1).alias("is_cancelado"),
               (F.col("is_substituido_int") == 1).alias("is_substituido"),
               (F.col("has_cce_int") == 1).alias("has_cce"),
               (F.col("has_inutil_int") == 1).alias("has_inutil"),
               F.col("ts_ultimo_evento"),
           )
    )
    return agg


# =============================================================================
# AplicaÃ§Ã£o ao DF de documentos
# =============================================================================

def _ensure_doc_chave(df_doc: DataFrame, chave_doc_col: Optional[str]) -> Tuple[DataFrame, str]:
    """
    Garante existÃªncia de coluna 'chave_doc' trimada para join com eventos.
    """
    if chave_doc_col is None:
        chave_doc_col = next((c for c in _DOC_CHAVE_CANDS if c in df_doc.columns), None)

    if not chave_doc_col:
        # cria vazia Â sem join efetivo
        return df_doc.withColumn("chave_doc", F.lit(None).cast("string")), "chave_doc"

    return df_doc.withColumn("chave_doc", F.trim(F.col(chave_doc_col))), "chave_doc"


def apply_cce_rules(
    df_doc: DataFrame,
    df_evt_norm_or_collapsed: DataFrame,
    *,
    chave_doc_col: Optional[str] = None,
    filter_excluded: bool = False,
    add_exclusion_reason: bool = True,
    exclusion_reason_cancel: str = "Documento cancelado",
    exclusion_reason_subst: str = "Documento substituÃ­do",
    exclusion_reason_inutil: str = "Documento inutilizado"
) -> DataFrame:
    """
    Aplica as regras de eventos ao DF de documentos.

    :param df_evt_norm_or_collapsed: pode ser o DF normalizado (linha por evento)
                                     ou o DF jÃ¡ colapsado por chave (via collapse_eventos_por_chave).
    :param chave_doc_col: nome da coluna da chave no DF de documentos (auto-detecta se None).
    :param filter_excluded: remove cancelados/substituÃ­dos/inutilizados.
    :param add_exclusion_reason: escreve motivo_exclusao_evento quando excluÃ­do.
    """
    # Se nÃ£o tiver as colunas colapsadas, colapsa agora:
    cols = set(map(str.lower, df_evt_norm_or_collapsed.columns))
    need_collapse = not {"dt_cancelamento", "is_cancelado", "is_substituido", "has_cce", "has_inutil"}.issubset(cols)

    df_evt = collapse_eventos_por_chave(df_evt_norm_or_collapsed) if need_collapse else df_evt_norm_or_collapsed

    # Garante chave no documento
    dfn, chave_col = _ensure_doc_chave(df_doc, chave_doc_col)

    j = (dfn
         .join(df_evt.alias("evt"), on=[F.col(chave_col) == F.col("evt.chave")], how="left")
         .drop("evt.chave")
         .withColumn("is_cancelado",   F.coalesce(F.col("evt.is_cancelado"), F.lit(False)))
         .withColumn("is_substituido", F.coalesce(F.col("evt.is_substituido"), F.lit(False)))
         .withColumn("has_cce",        F.coalesce(F.col("evt.has_cce"), F.lit(False)))
         .withColumn("has_inutil",     F.coalesce(F.col("evt.has_inutil"), F.lit(False)))
         .withColumn("dt_cancelamento", F.col("evt.dt_cancelamento"))
         .withColumn("ts_ultimo_evento", F.col("evt.ts_ultimo_evento"))
         .drop("evt.is_cancelado", "evt.is_substituido", "evt.has_cce", "evt.has_inutil",
               "evt.dt_cancelamento", "evt.ts_ultimo_evento")
    )

    # Motivo de exclusÃ£o (opcional)
    if add_exclusion_reason:
        j = j.withColumn(
            "motivo_exclusao_evento",
            F.when(F.col("is_cancelado")   == F.lit(True), F.lit(exclusion_reason_cancel))
             .when(F.col("is_substituido") == F.lit(True), F.lit(exclusion_reason_subst))
             .when(F.col("has_inutil")     == F.lit(True), F.lit(exclusion_reason_inutil))
             .otherwise(F.lit(None).cast("string"))
        )

    # Filtragem (opcional)
    if filter_excluded:
        j = j.filter(
            (F.col("is_cancelado") == F.lit(False)) &
            (F.col("is_substituido") == F.lit(False)) &
            (F.col("has_inutil") == F.lit(False))
        )

    return j
