# -*- coding: utf-8 -*-
# app/utils/params.py
"""
ParÃÂÃÂÃÂÃÂ¢metros de processamento (IPM) vindos do Oracle.

O que este mÃÂÃÂÃÂÃÂ³dulo faz:
- LÃÂÃÂÃÂÃÂª a tabela de parÃÂÃÂÃÂÃÂ¢metros (settings.oracle.tbl_parametros) via JDBC.
- Tenta normalizar variaÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂµes comuns de nomes de coluna:
  - nome do parÃÂÃÂÃÂÃÂ¢metro:  NOME_PARAMETRO | NOME_PARAM | NOME | PARAM | NM_PARAM | NM_PARAMETRO | NOME_PARAMETRO_IPM
  - valor do parÃÂÃÂÃÂÃÂ¢metro: VALOR_PARAMETRO | VALR_PARAM | VALOR | VALR | VALUE | DS_VALOR | DESC_PARAMETRO_IPM
- Converte valores para tipos Python ÃÂÃÂÃÂÃÂºteis: bool/int/float/JSON/list
- ExpÃÂÃÂÃÂÃÂµe funÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂµes para obter como DataFrame, dict e broadcast.
- Tudo compatÃÂÃÂÃÂÃÂ­vel com execuÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ£o distribuÃÂÃÂÃÂÃÂ­da (sem pandas).

ObservaÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂµes:
- A tabela costuma ser pequena, entÃÂÃÂÃÂÃÂ£o a conversÃÂÃÂÃÂÃÂ£o para dict/broadcast ÃÂÃÂÃÂÃÂ© segura.
- Caso apareÃÂÃÂÃÂÃÂ§am novas variaÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂµes de coluna, basta acrescentar nos candidatos.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F, types as T  # T pode ser ÃÂÃÂÃÂÃÂºtil em evoluÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂµes

from app.settings import Settings
from app.utils.io import read_oracle_table


# =============================================================================
# NormalizaÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ£o de colunas
# =============================================================================

# Candidatos aceitos (inclui sufixos *_IPM usados no Oracle)
NAME_CANDIDATES = [
    "NOME_PARAMETRO", "NOME_PARAM", "NOME", "PARAM", "NM_PARAM", "NM_PARAMETRO",
    "NOME_PARAMETRO_IPM",
]
VALUE_CANDIDATES = [
    "VALOR_PARAMETRO", "VALR_PARAM", "VALOR", "VALR", "VALUE", "DS_VALOR",
    "DESC_PARAMETRO_IPM",
]


def _first_present(cols: Iterable[str], candidates: List[str]) -> Optional[str]:
    """
    Retorna o primeiro nome de coluna de 'candidates' que existe em 'cols',
    preservando o casing original de 'cols'.
    """
    by_upper = {c.upper(): c for c in cols}
    for cand in candidates:
        u = cand.upper()
        if u in by_upper:
            return by_upper[u]
    return None


def _normalize_params_df(df: DataFrame) -> DataFrame:
    """
    Padroniza o DataFrame para as colunas:
      - nome_param : STRING (uppercase, trim)
      - valor_param: STRING (trim)
    Ignora linhas sem nome vÃÂÃÂÃÂÃÂ¡lido. Em caso de duplicidade, mantÃÂÃÂÃÂÃÂ©m uma por nome,
    priorizando valores nÃÂÃÂÃÂÃÂ£o nulos/nem vazios.
    """
    orig_name = _first_present(df.columns, NAME_CANDIDATES)
    orig_value = _first_present(df.columns, VALUE_CANDIDATES)

    if not orig_name or not orig_value:
        cols_upper = [c.upper() for c in df.columns]
        raise ValueError(
            "NÃÂÃÂÃÂÃÂ£o encontrei colunas compatÃÂÃÂÃÂÃÂ­veis na tabela de parÃÂÃÂÃÂÃÂ¢metros. "
            f"Procurado nome em {NAME_CANDIDATES} e valor em {VALUE_CANDIDATES}. "
            f"Colunas: {cols_upper}"
        )

    dfn = (
        df.select(
            F.upper(F.trim(F.col(orig_name))).alias("nome_param"),
            F.trim(F.col(orig_value)).alias("valor_param"),
        )
        .filter(F.col("nome_param").isNotNull() & (F.col("nome_param") != ""))
    )

    # DeduplicaÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ£o por nome, priorizando linhas cujo valor nÃÂÃÂÃÂÃÂ£o ÃÂÃÂÃÂÃÂ© nulo/vazio.
    # Ordenamos primeiro por "ÃÂÃÂÃÂÃÂ© nulo/vazio" e depois por nome para estabilidade,
    # e entÃÂÃÂÃÂÃÂ£o aplicamos dropDuplicates(["nome_param"]) que preserva a primeira ocorrÃÂÃÂÃÂÃÂªncia.
    dfn = (
        dfn
        .withColumn(
            "is_null_val",
            F.when(
                F.col("valor_param").isNull() | (F.col("valor_param") == ""),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .orderBy(F.col("is_null_val").asc(), F.col("nome_param").asc())
        .drop("is_null_val")
        .dropDuplicates(["nome_param"])
    )

    return dfn


# =============================================================================
# Leitura
# =============================================================================

def load_params_df(
    spark: SparkSession,
    settings: Settings,
    *,
    cache: bool = True,
) -> DataFrame:
    """
    LÃÂÃÂÃÂÃÂª a tabela de parÃÂÃÂÃÂÃÂ¢metros do Oracle e retorna DataFrame normalizado:
      [nome_param STRING, valor_param STRING]
    """
    table = settings.oracle.tbl_parametros
    df_raw = read_oracle_table(spark, settings, table)
    df = _normalize_params_df(df_raw)
    if cache:
        df = df.cache()
        # materializa o cache (tabela pequena)
        _ = df.count()
    return df


# =============================================================================
# ConversÃÂÃÂÃÂÃÂ£o de tipos
# =============================================================================

def _to_bool(s: Optional[str]) -> Optional[bool]:
    if s is None:
        return None
    v = s.strip().lower()
    if v in {"1", "true", "t", "yes", "y", "sim", "s"}:
        return True
    if v in {"0", "false", "f", "no", "n", "nao", "nÃÂÃÂÃÂÃÂ£o"}:
        return False
    return None


def _parse_json_or_list(s: str) -> Optional[Any]:
    """
    Tenta interpretar como JSON (objeto, lista, nÃÂÃÂÃÂÃÂºmero) ou lista separada por vÃÂÃÂÃÂÃÂ­rgulas.
    Retorna None se nÃÂÃÂÃÂÃÂ£o aplicÃÂÃÂÃÂÃÂ¡vel.
    """
    if not s:
        return None
    st = s.strip()
    # JSON evidente
    if (st.startswith("{") and st.endswith("}")) or (st.startswith("[") and st.endswith("]")):
        try:
            return json.loads(st)
        except Exception:
            return None
    # Lista CSV simples
    if "," in st:
        parts = [p.strip() for p in st.split(",")]
        if len(parts) >= 2:
            return parts
    return None


def coerce_value(raw: Optional[str], as_type: Optional[str] = None) -> Any:
    """
    Converte string para tipo desejado.
      as_type:
        - "bool"   -> True/False (aceita 1/0/true/false/sim/nao)
        - "int"    -> int
        - "float"  -> float (aceita vÃÂÃÂÃÂÃÂ­rgula decimal)
        - "json"   -> json.loads (obj/list/num/str) se vÃÂÃÂÃÂÃÂ¡lido
        - "list"   -> lista split por vÃÂÃÂÃÂÃÂ­rgula
        - "auto"   -> tenta bool -> int -> float -> json/list -> string
        - None     -> retorna string original
    """
    if raw is None:
        return None

    s = str(raw).strip()

    if as_type == "bool":
        b = _to_bool(s)
        if b is None:
            raise ValueError(f"Valor '{s}' nÃÂÃÂÃÂÃÂ£o ÃÂÃÂÃÂÃÂ© booleano.")
        return b

    if as_type == "int":
        return int(s)

    if as_type == "float":
        return float(s.replace(",", "."))  # tolera vÃÂÃÂÃÂÃÂ­rgula decimal

    if as_type == "json":
        return json.loads(s)

    if as_type == "list":
        return [p.strip() for p in s.split(",")]

    if as_type == "auto" or as_type is None:
        # bool
        b = _to_bool(s)
        if b is not None:
            return b
        # int
        try:
            return int(s)
        except Exception:
            pass
        # float
        try:
            return float(s.replace(",", "."))
        except Exception:
            pass
        # json/list
        jl = _parse_json_or_list(s)
        if jl is not None:
            return jl
        # fallback: string
        return s

    # tipo nÃÂÃÂÃÂÃÂ£o suportado
    raise ValueError(f"Tipo de parÃÂÃÂÃÂÃÂ¢metro nÃÂÃÂÃÂÃÂ£o suportado: {as_type}")


# =============================================================================
# Acesso prÃÂÃÂÃÂÃÂ¡tico
# =============================================================================

def params_to_dict(df_params: DataFrame, *, coerce: str = "auto") -> Dict[str, Any]:
    """
    Converte o DF [nome_param, valor_param] em dict {nome_param: valor_coercido}.
    """
    rows = df_params.select("nome_param", "valor_param").collect()
    out: Dict[str, Any] = {}
    for r in rows:
        k = r["nome_param"]
        v = r["valor_param"]
        out[k] = coerce_value(v, coerce)
    return out


def get_param(
    params: Dict[str, Any],
    name: str,
    *,
    default: Any = None,
    as_type: Optional[str] = None
) -> Any:
    """
    LÃÂÃÂÃÂÃÂª um parÃÂÃÂÃÂÃÂ¢metro especÃÂÃÂÃÂÃÂ­fico, com default. Se as_type for informado,
    forÃÂÃÂÃÂÃÂ§a a conversÃÂÃÂÃÂÃÂ£o (bool/int/float/json/list/auto).
    """
    if name not in params or params[name] is None:
        return default
    if as_type:
        return coerce_value(params[name], as_type)
    return params[name]


def broadcast_params(
    spark: SparkSession,
    params_dict: Dict[str, Any]
):
    """
    Cria um broadcast dos parÃÂÃÂÃÂÃÂ¢metros para uso nas UDFs/transformaÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂµes distribuÃÂÃÂÃÂÃÂ­das.
    """
    return spark.sparkContext.broadcast(params_dict)


# =============================================================================
# Exemplo de uso integrado
# =============================================================================
# from app.settings import load_settings_from_env, build_spark
# from app.utils.params import load_params_df, params_to_dict, get_param, broadcast_params
#
# settings = load_settings_from_env()
# spark = build_spark(settings)
#
# df_params = load_params_df(spark, settings)
# pmap = params_to_dict(df_params, coerce="auto")
# bc  = broadcast_params(spark, pmap)
#
# # buscar especÃÂÃÂÃÂÃÂ­fico (com tipo)
# janela_dias = get_param(pmap, "JANELA_DIAS_PROCESSAMENTO", default=1, as_type="int")
# usar_validacao_cce = get_param(pmap, "USAR_VALIDACAO_CCE", default=True, as_type="bool")
