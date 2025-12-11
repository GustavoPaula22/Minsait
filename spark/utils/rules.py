# -*- coding: utf-8 -*-
# app/utils/rules.py
"""
Orquestração das regras de negócio:
- Carrega e normaliza referências auxiliares (Oracle): Params, CFOP, Eventos/CCE, NCM, Municípios/GEN
- Aplica as regras na ordem correta sobre DataFrames de documentos/itens
- Garante colunas obrigatórias para projeções finais (project_document/project_item)
- Expõe métricas (contagens) para auditoria

Compatível com nfe.py/bpe.py/nf3e.py/cte.py:
- Wrappers: load_params_ref, load_cfop_ref, load_ncm_ref, load_cce_ref, load_gen_ref
- RulesContext aceita df_params, df_cfop_ref, df_ncm_ref, df_eventos, df_gen, params_dict, params_bc
- apply_rules_to_documents: GEN → CCE (eventos colapsados por chave)
- apply_rules_to_items: CFOP/NCM em itens; garante valr_adicionado/codg_tipo_doc_partct_item

Observações:
- Se a referência CFOP não tiver flag, assume participante=True (compatível com IPM_CFOP_PARTICIPANTE).
- Se as regras de NCM não produzirem 'valr_adicionado', aplica fallback com 'valor_item' quando CFOP participante.
"""

from __future__ import annotations

from typing import Dict, Optional, Tuple, List, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F, types as T

from app.settings import Settings
from app.utils.io import read_oracle_ipm_auxiliares
from app.utils.params import (
    load_params_df,
    params_to_dict,
    get_param,
    broadcast_params,
)
from app.utils.gen import build_gen_context, apply_gen_enrichment, GenContext
from app.utils.cce import (
    normalize_eventos,
    collapse_eventos_por_chave,
    apply_cce_rules,
)
from app.utils.ncm import normalize_ncm_participante, apply_ncm_rules


# ============================================================
# Utilitários
# ============================================================
def _safe_count(df: Optional[DataFrame]) -> int:
    if df is None:
        return 0
    try:
        return int(df.count())
    except Exception as e:
        print(f"[rules][WARN] Falha ao contar DF: {e}")
        return -1


# ============================================================
# Suporte a CFOP (referência e extração de coluna no item)
# ============================================================
# nomes candidatos de CFOP em itens
_ITEM_CFOP_CANDS: List[str] = [
    "codg_cfop",
    "cfop",
    "cfop_item",
    "cfop_nitem",
    "cfopdoc",
    "cfo",
    "cfopera",
]

# nomes candidatos ao código do CFOP na referência
_REF_CFOP_CODE_CANDS: List[str] = [
    "codg_cfop",
    "cfop",
    "codigo",
    "codigo_cfop",
]

# nomes candidatos a um eventual flag/indicador na referência
_REF_CFOP_FLAG_CANDS: List[str] = [
    "is_participante",
    "flg_participante",
    "participante",
    "indi_participante",
]


def _first_present(cols: List[str], cands: List[str]) -> Optional[str]:
    lookup = {c.lower(): c for c in cols}
    for k in cands:
        if k.lower() in lookup:
            return lookup[k.lower()]
    return None


def normalize_cfop_participante(df_ref: DataFrame) -> DataFrame:
    """
    Normaliza DF de referência de CFOP participante.
    Se a referência NÃO tiver coluna-flag, assume-se que TODOS os CFOPs são participantes.
    Retorna colunas: 'cfop_code' (string de 3-4 dígitos) e 'is_participante' (boolean).
    """
    cols = df_ref.columns
    col_code = _first_present(cols, _REF_CFOP_CODE_CANDS)
    if not col_code:
        raise ValueError("Referência de CFOP sem coluna de código reconhecida.")

    col_flag = _first_present(cols, _REF_CFOP_FLAG_CANDS)

    out = df_ref.withColumn(
        "cfop_code",
        F.regexp_extract(F.col(col_code).cast("string"), r"(\d{3,4})", 0),
    )

    if col_flag:
        v = F.upper(F.trim(F.col(col_flag).cast("string")))
        out = out.withColumn(
            "is_participante",
            F.when(v.isin("1", "S", "SIM", "Y", "TRUE", "T"), F.lit(True))
             .when(v.isin("0", "N", "NAO", "NÃO", "FALSE", "F"), F.lit(False))
             .otherwise(F.lit(True)),  # default otimista
        )
    else:
        out = out.withColumn("is_participante", F.lit(True))

    return out.select("cfop_code", "is_participante").dropDuplicates(["cfop_code"])


# =============================================================================
# Contexto das Regras
# =============================================================================
class RulesContext:
    """
    Contexto utilizado pelas funções apply_rules_to_documents/apply_rules_to_items.
    Aceita os mesmos campos esperados por bpe.py/nf3e.py/nfe.py/cte.py via _load_refs_oracle:
      - df_params: DataFrame de parâmetros
      - df_cfop_ref: DataFrame CFOP normalizado (cfop_code, is_participante)
      - df_ncm_ref: DataFrame NCM normalizado
      - df_eventos: DataFrame de eventos COLAPSADO por chave
      - df_gen: GenContext (municípios/UF/etc.)
      - params_dict: dicionário de parâmetros
      - params_bc: broadcast do dicionário (opcional)
    """
    def __init__(
        self,
        *,
        df_params: Optional[DataFrame] = None,
        df_cfop_ref: Optional[DataFrame] = None,
        df_ncm_ref: Optional[DataFrame] = None,
        df_eventos: Optional[DataFrame] = None,
        df_gen: Optional[GenContext] = None,
        params_dict: Optional[Dict] = None,
        params_bc=None,
    ):
        self.df_params = df_params
        self.df_cfop_ref = df_cfop_ref
        self.df_ncm_ref = df_ncm_ref
        self.df_eventos = df_eventos
        self.df_gen = df_gen
        self.params_dict = params_dict or {}
        self.params_bc = params_bc


# =============================================================================
# Wrappers de carregamento (compatíveis com nfe/bpe/nf3e/cte)
# =============================================================================
def load_params_ref(spark: SparkSession, settings: Settings, *, as_dict: bool = False) -> Union[DataFrame, Dict]:
    df_params = load_params_df(spark, settings)
    if as_dict:
        return params_to_dict(df_params, coerce="auto")
    return df_params


def load_cfop_ref(spark: SparkSession, settings: Settings) -> Optional[DataFrame]:
    aux = read_oracle_ipm_auxiliares(spark, settings)
    df = aux.get("cfop_participante")
    if df is None:
        print("[rules] Aviso: referência CFOP participante ausente.")
        return None
    try:
        return normalize_cfop_participante(df)
    except Exception as e:
        print("[rules] Aviso: falha ao normalizar CFOP participante. Erro:", e)
        return None


def load_ncm_ref(spark: SparkSession, settings: Settings) -> Optional[DataFrame]:
    aux = read_oracle_ipm_auxiliares(spark, settings)
    df = aux.get("ncm_participante")
    if df is None:
        print("[rules] Aviso: referência NCM participante ausente.")
        return None
    try:
        return normalize_ncm_participante(df)
    except Exception as e:
        print("[rules] Aviso: falha ao normalizar NCM participante. Erro:", e)
        return None


def load_cce_ref(spark: SparkSession, settings: Settings) -> Optional[DataFrame]:
    """
    Retorna DF de eventos **colapsados por chave** (uma linha por documento com flags).
    """
    aux = read_oracle_ipm_auxiliares(spark, settings)
    df_evt = aux.get("evento")
    if df_evt is None:
        print("[rules] Aviso: referência EVENTOS/CCE ausente.")
        return None
    try:
        df_norm = normalize_eventos(df_evt)
        return collapse_eventos_por_chave(df_norm)
    except Exception as e:
        print("[rules] Aviso: falha ao processar EVENTOS/CCE. Erro:", e)
        return None


def load_gen_ref(spark: SparkSession, settings: Settings) -> Optional[GenContext]:
    try:
        return build_gen_context(spark, settings)
    except Exception as e:
        print("[rules] Aviso: não foi possível construir o contexto GEN. Pulando. Erro:", e)
        return None


# =============================================================================
# Construção completa do contexto (útil quando preferir 1 chamada)
# =============================================================================
def build_rules_context(
    spark: SparkSession,
    settings: Settings,
    *,
    broadcast_params_flag: bool = True,
    include_ncm_ref: bool = True,
) -> RulesContext:
    df_params = load_params_df(spark, settings)
    params_dict = params_to_dict(df_params, coerce="auto")
    params_bc = broadcast_params(spark, params_dict) if broadcast_params_flag else None

    ctx = RulesContext(
        df_params=df_params,
        df_cfop_ref=load_cfop_ref(spark, settings),
        df_ncm_ref=(load_ncm_ref(spark, settings) if include_ncm_ref else None),
        df_eventos=load_cce_ref(spark, settings),  # já colapsado
        df_gen=load_gen_ref(spark, settings),
        params_dict=params_dict,
        params_bc=params_bc,
    )
    return ctx


# =============================================================================
# Garantia de colunas obrigatórias (para projeções finais)
# =============================================================================
def ensure_business_columns(df: DataFrame, *, for_items: bool = False) -> DataFrame:
    need_cols_docs = {
        "is_cfop_participante": F.lit(False).cast("boolean"),
        "motivo_exclusao_cfop": F.lit(None).cast("string"),
        "is_cancelado": F.lit(False).cast("boolean"),
        "is_substituido": F.lit(False).cast("boolean"),
        "has_cce": F.lit(False).cast("boolean"),
        "has_inutil": F.lit(False).cast("boolean"),
        "motivo_exclusao_evento": F.lit(None).cast("string"),
        "ibge_emit": F.lit(None).cast("string"),
        "uf_emit": F.lit(None).cast("string"),
        "nome_municipio_emit": F.lit(None).cast("string"),
        "ibge_dest": F.lit(None).cast("string"),
        "uf_dest": F.lit(None).cast("string"),
        "nome_municipio_dest": F.lit(None).cast("string"),
        # Compat nfe.py (flags produtor rural, quando vindas do GEN)
        "s_prod_rural_dest": F.lit(None).cast("string"),
        "s_prod_rural_remet": F.lit(None).cast("string"),
    }

    need_cols_items = {
        "is_cfop_participante": F.lit(False).cast("boolean"),
        "motivo_exclusao_cfop": F.lit(None).cast("string"),
        "is_ncm_participante": F.lit(False).cast("boolean"),
        "motivo_exclusao_ncm": F.lit(None).cast("string"),
        "is_monofasico": F.lit(None).cast("boolean"),
        "codg_tipo_doc_partct_item": F.lit(None).cast("int"),
        "valr_adicionado": F.lit(None).cast(T.DecimalType(17, 2)),
    }

    dfn = df
    for col, default_expr in (need_cols_items.items() if for_items else need_cols_docs.items()):
        if col not in dfn.columns:
            dfn = dfn.withColumn(col, default_expr)
    return dfn


def _ensure_document_flags(df: DataFrame) -> DataFrame:
    """
    Garante que as flags genéricas de documentos existam antes do agg,
    reaproveitando colunas já presentes quando possível.

    - Para BPE, em especial: mapeia has_evt_cancel → is_cancelado, se existir.
    - Para qualquer documento: se as colunas não existirem, cria com False.
    """
    dfn = df

    # is_cancelado: tenta derivar de has_evt_cancel (0/1), senão default False
    if "is_cancelado" not in dfn.columns:
        if "has_evt_cancel" in dfn.columns:
            dfn = dfn.withColumn(
                "is_cancelado",
                F.when(F.col("has_evt_cancel").cast("int") > F.lit(0), F.lit(True)).otherwise(F.lit(False)),
            )
        else:
            dfn = dfn.withColumn("is_cancelado", F.lit(False))

    # is_substituido: por enquanto default False se não vier de regras específicas
    if "is_substituido" not in dfn.columns:
        dfn = dfn.withColumn("is_substituido", F.lit(False))

    # has_inutil: default False se não existir mapeamento específico
    if "has_inutil" not in dfn.columns:
        dfn = dfn.withColumn("has_inutil", F.lit(False))

    # has_cce: default False se não vier das regras de CCE
    if "has_cce" not in dfn.columns:
        dfn = dfn.withColumn("has_cce", F.lit(False))

    return dfn


# =============================================================================
# ITENS – helpers internos
# =============================================================================
def _ensure_cfop_column(df: DataFrame) -> DataFrame:
    """Se a coluna canônica 'cfop' não existir, tenta promovê-la a partir de candidatos/normalizada."""
    if "cfop" in df.columns:
        return df
    if "cfop_item_norm" in df.columns:
        return df.withColumn("cfop", F.col("cfop_item_norm"))
    cand = _first_present(df.columns, _ITEM_CFOP_CANDS)
    if cand and cand != "cfop":
        return df.withColumn("cfop", F.col(cand).cast("string"))
    return df


def _ensure_valor_item(df: DataFrame) -> DataFrame:
    """
    Garante a coluna VL_ITEM, replicando a lógica do legado na medida do possível.

    Regra:
    - Se já existir VL_ITEM não nula, mantém o valor existente.
    - Se VL_ITEM for nula (ou não existir), calcula a partir de:
        vprod
      + vfrete
      + vseg
      + voutro
      + vl_ipi
      + vl_icms_st
      - vdesc
      - vl_abat_nt
      + ii_vii (quando existir; caso contrário, 0).

    Qualquer coluna que não exista no DataFrame entra como 0 (lit(0)).
    """

    cols = set(df.columns)

    def c(name: str):
        # Retorna a coluna se existir; caso contrário, 0.
        return F.col(name) if name in cols else F.lit(0)

    # Cálculo padrão do valor do item
    computed = (
        c("vprod")
        + c("vfrete")
        + c("vseg")
        + c("voutro")
        + c("vl_ipi")
        + c("vl_icms_st")
        - c("vdesc")
        - c("vl_abat_nt")
        + c("ii_vii")  # se não existir no DF, vira 0 e não quebra o plano
    )

    # Se já existe VL_ITEM, só preenche quando estiver nula
    if "VL_ITEM" in cols:
        expr = F.when(F.col("VL_ITEM").isNull(), computed).otherwise(F.col("VL_ITEM"))
    else:
        expr = computed

    return df.withColumn("VL_ITEM", expr)


# =============================================================================
# DOCUMENTOS
# =============================================================================
def apply_rules_to_documents(
    df_docs: DataFrame,
    ctx: RulesContext,
    *,
    apply_gen: Optional[bool] = None,
    apply_cfop: Optional[bool] = None,  # mantido p/ compat; CFOP é tratado em ITENS
    apply_cce: Optional[bool] = None,
    filter_cfop_nonparticipants: Optional[bool] = None,  # ignorado aqui
    filter_cce_excluded: Optional[bool] = None,
) -> Tuple[DataFrame, Dict[str, int]]:
    """
    Aplica GEN → CCE (nessa ordem) sobre o DF de documentos.
    A marcação CFOP agora é tratada em ITENS e deve ser agregada no chamador.
    """
    p = ctx.params_dict
    use_gen = apply_gen if apply_gen is not None else get_param(p, "USAR_GEN", default=True, as_type="bool")
    use_cce = apply_cce if apply_cce is not None else get_param(p, "USAR_CCE", default=True, as_type="bool")
    flt_evt = (
        filter_cce_excluded
        if filter_cce_excluded is not None
        else get_param(p, "FILTRAR_EVENTOS_EXCLUIDOS", default=False, as_type="bool")
    )

    metrics: Dict[str, int] = {}
    metrics["total_docs_in"] = _safe_count(df_docs)

    cur = df_docs

    # GEN (enriquecimento de muni/UF, produtor rural etc.)
    if use_gen and ctx.df_gen is not None:
        cur = apply_gen_enrichment(cur, ctx.df_gen)
    elif use_gen and ctx.df_gen is None:
        print("[rules] Aviso: GEN solicitado, mas contexto ausente.")

    # CCE/eventos: df_eventos já deve vir COLAPSADO (uma linha por chave)
    if use_cce and ctx.df_eventos is not None:
        cur = apply_cce_rules(
            cur,
            ctx.df_eventos,
            filter_excluded=bool(flt_evt),
            add_exclusion_reason=True,
        )
    elif use_cce and ctx.df_eventos is None:
        print("[rules] Aviso: CCE solicitado, mas referência ausente.")

    # Garante que as flags básicas existam ANTES do agg
    cur = _ensure_document_flags(cur)

    # Contagens por única passagem
    agg = cur.agg(
        F.count(F.lit(1)).alias("_tot"),
        F.sum(F.col("is_cancelado").cast("int")).alias("_cancelados"),
        F.sum(F.col("is_substituido").cast("int")).alias("_substituidos"),
        F.sum(F.col("has_inutil").cast("int")).alias("_inutilizados"),
        F.sum(F.col("has_cce").cast("int")).alias("_com_cce"),
    ).collect()[0]

    metrics.update(
        {
            "cancelados": int(agg["_cancelados"]) if agg["_cancelados"] is not None else 0,
            "substituidos": int(agg["_substituidos"]) if agg["_substituidos"] is not None else 0,
            "inutilizados": int(agg["_inutilizados"]) if agg["_inutilizados"] is not None else 0,
            "com_cce": int(agg["_com_cce"]) if agg["_com_cce"] is not None else 0,
        }
    )

    # Garante o pacote completo de colunas de negócio usadas nas projeções
    cur = ensure_business_columns(cur, for_items=False)
    metrics["total_docs_out"] = _safe_count(cur)
    return cur, metrics


# =============================================================================
# ITENS
# =============================================================================
def apply_rules_to_items(
    df_itens: DataFrame,
    ctx: RulesContext,
    *,
    apply_ncm: Optional[bool] = None,
    filter_ncm_nonparticipants: Optional[bool] = None,
    apply_cfop_items: Optional[bool] = None,  # toggle
    filter_cfop_nonparticipants_items: Optional[bool] = None,  # toggle
) -> Tuple[DataFrame, Dict[str, int]]:
    """
    Aplica CFOP (em ITENS) e NCM (participante + monofásico).
    Retorna (df_out, metrics_dict).

    Garante:
      - is_cfop_participante (boolean)
      - is_ncm_participante, is_monofasico e motivo_exclusao_ncm (se referência existir)
      - cfop (string canônica)
      - valor_item (Decimal(17,2))       -> calculado se não existir
      - valr_adicionado (Decimal(17,2))  -> fallback com valor_item quando CFOP participante
      - codg_tipo_doc_partct_item (int)  -> default nulo, mantendo compatibilidade com projeção
    """
    p = ctx.params_dict
    use_ncm = apply_ncm if apply_ncm is not None else get_param(p, "USAR_NCM", default=True, as_type="bool")
    flt_ncm = (
        filter_ncm_nonparticipants
        if filter_ncm_nonparticipants is not None
        else get_param(p, "FILTRAR_NCM_NAO_PART", default=False, as_type="bool")
    )
    use_cfop_items = (
        apply_cfop_items if apply_cfop_items is not None else get_param(p, "USAR_CFOP_ITENS", default=True, as_type="bool")
    )
    flt_cfop_items = (
        filter_cfop_nonparticipants_items
        if filter_cfop_nonparticipants_items is not None
        else get_param(p, "FILTRAR_CFOP_NAO_PART_ITENS", default=False, as_type="bool")
    )

    metrics: Dict[str, int] = {}
    metrics["total_itens_in"] = _safe_count(df_itens)

    cur = df_itens

    # === CFOP em itens ===
    if use_cfop_items and ctx.df_cfop_ref is not None:
        col_cfop = _first_present(cur.columns, _ITEM_CFOP_CANDS)
        if col_cfop:
            # normaliza CFOP do item
            cfop_item = F.regexp_extract(F.col(col_cfop).cast("string"), r"(\d{3,4})", 0)
            cur = cur.withColumn("cfop_item_norm", cfop_item)

            # join com referência normalizada (que já tem is_participante)
            cur = (
                cur.join(
                    ctx.df_cfop_ref.withColumnRenamed("cfop_code", "cfop_join"),
                    cur["cfop_item_norm"] == F.col("cfop_join"),
                    "left",
                )
                .drop("cfop_join")
            )

            cur = cur.withColumn(
                "is_cfop_participante",
                F.coalesce(F.col("is_participante"), F.lit(False)),
            ).drop("is_participante")

            if flt_cfop_items:
                cur = cur.filter(F.col("is_cfop_participante") == F.lit(True))
        else:
            print("[rules] Aviso: não encontrei coluna de CFOP nos ITENS; criando is_cfop_participante=False.")
            cur = cur.withColumn("is_cfop_participante", F.lit(False))
    elif use_cfop_items and ctx.df_cfop_ref is None:
        print("[rules] Aviso: CFOP em ITENS solicitado, mas referência ausente. Pulando CFOP ITENS.")

    # === NCM em itens ===
    if use_ncm:
        if ctx.df_ncm_ref is None:
            print("[rules] Aviso: NCM solicitado, mas referência ausente. Pulando regra NCM.")
        else:
            cur = apply_ncm_rules(
                cur,
                ctx.df_ncm_ref,
                filter_non_participants=bool(flt_ncm),
                add_exclusion_reason=True,
            )

    # Garante colunas mínimas p/ itens e compatibilidade com projeção
    cur = ensure_business_columns(cur, for_items=True)
    cur = _ensure_cfop_column(cur)

    # === Garantir 'valor_item' antes do fallback de valr_adicionado ===
    cur = _ensure_valor_item(cur)

    # === Fallbacks críticos de saída ===
    # valr_adicionado: se não vier das regras de NCM, usar valor_item quando CFOP participante
    if "valr_adicionado" not in cur.columns:
        cur = cur.withColumn(
            "valr_adicionado",
            F.when(
                (F.col("is_cfop_participante") == F.lit(True)) & F.col("valor_item").isNotNull(),
                F.col("valor_item").cast(T.DecimalType(17, 2)),
            ).otherwise(F.lit(0).cast(T.DecimalType(17, 2))),
        )
    else:
        # força casting e mantém fallback onde nulo
        cur = cur.withColumn(
            "valr_adicionado",
            F.when(
                F.col("valr_adicionado").isNotNull(),
                F.col("valr_adicionado").cast(T.DecimalType(17, 2)),
            ).otherwise(
                F.when(
                    (F.col("is_cfop_participante") == F.lit(True)) & F.col("valor_item").isNotNull(),
                    F.col("valor_item").cast(T.DecimalType(17, 2)),
                ).otherwise(F.lit(0).cast(T.DecimalType(17, 2)))
            ),
        )

    # codg_tipo_doc_partct_item: manter nulo por padrão se não existir
    if "codg_tipo_doc_partct_item" not in cur.columns:
        cur = cur.withColumn("codg_tipo_doc_partct_item", F.lit(None).cast("int"))

    # Métricas (passagem única)
    agg_cols = {
        "_tot": F.count(F.lit(1)),
        "_cfop_part": F.sum(F.col("is_cfop_participante").cast("int")),
        "_ncm_part": F.sum(F.col("is_ncm_participante").cast("int")),
    }
    agg = cur.agg(*[expr.alias(name) for name, expr in agg_cols.items()]).collect()[0]

    metrics.update(
        {
            "total_itens_out": int(agg["_tot"]) if agg["_tot"] is not None else -1,
            "cfop_itens_participantes": int(agg["_cfop_part"]) if agg["_cfop_part"] is not None else 0,
            "cfop_itens_nao_participantes": (
                int(agg["_tot"]) - int(agg["_cfop_part"])
                if agg["_tot"] is not None and agg["_cfop_part"] is not None
                else 0
            ),
            "ncm_participantes": int(agg["_ncm_part"]) if agg["_ncm_part"] is not None else 0,
            "ncm_nao_participantes": (
                int(agg["_tot"]) - int(agg["_ncm_part"])
                if agg["_tot"] is not None and agg["_ncm_part"] is not None
                else 0
            ),
        }
    )

    return cur, metrics
