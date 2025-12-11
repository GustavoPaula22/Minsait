# -*- coding: utf-8 -*-
# app/utils/gen.py
"""
GEN (municÃ­pios / IBGE / UF) Â utilitÃ¡rios de enriquecimento geogrÃ¡fico.

O que este mÃ³dulo faz:
- LÃª a tabela de municÃ­pios do Oracle (settings.oracle.tbl_municipios).
- Normaliza colunas (IBGE, UF, NOME), removendo ruÃ­dos e padronizando tipos.
- Cria chaves de junÃ§Ã£o "Ã  prova de variaÃ§Ãµes" (por ex., trata IBGE numÃ©rico/strings).
- Fornece funÃ§Ãµes para aplicar join ao DF de documentos:
  * join_emitente_municipio(...)
  * join_destinatario_municipio(...)
- Fornece um "contexto GEN" com o DF de municÃ­pios cacheado/broadcastÃ¡vel.

ObservaÃ§Ãµes:
- Colunas comumente encontradas para municÃ­pios (ajustÃ¡veis nas listas abaixo):
  * CÃ³digo IBGE:  COD_IBGE | COD_MUNICIPIO | CD_IBGE | IBGE | CO_MUNICIPIO_IBGE
  * UF:           UF | SG_UF | ESTADO | UF_SIGLA
  * Nome:         NOME | NO_MUNICIPIO | NM_MUNICIPIO | MUNICIPIO | DS_NOME
- Se a sua tabela tiver variaÃ§Ãµes, basta incluir mais aliases nas listas.
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F, types as T

from app.settings import Settings
from app.utils.io import read_oracle_table


# =============================================================================
# Colunas e helpers
# =============================================================================

# Candidatos para as colunas de municÃ­pios
_CAND_IBGE = [
    "COD_IBGE", "COD_MUNICIPIO", "CD_IBGE", "IBGE", "CO_MUNICIPIO_IBGE", "ID_MUNICIPIO_IBGE"
]
_CAND_UF = [
    "UF", "SG_UF", "ESTADO", "UF_SIGLA", "SGESTADO"
]
_CAND_NOME = [
    "NOME", "NO_MUNICIPIO", "NM_MUNICIPIO", "MUNICIPIO", "DS_NOME", "NOME_MUNICIPIO"
]

def _first_present(cols: Iterable[str], candidates: List[str]) -> Optional[str]:
    upper = {c.upper(): c for c in cols}
    for cand in candidates:
        if cand.upper() in upper:
            return upper[cand.upper()]
    return None

def _to_int_str(col: F.Column) -> F.Column:
    """
    Converte para inteiro quando possÃ­vel e volta para string sem zeros Ã  esquerda.
    Ãtil para normalizar IBGE que vem "010001" vs "10001".
    """
    return F.when(F.length(F.trim(col)) > 0, F.col("__tmp_ibge_int")).otherwise(F.lit(None))

def _normalize_ibge_expr(col: F.Column) -> F.Column:
    """
    Normaliza um campo IBGE potencialmente string/nÃºmerico:
    - trim
    - remove zeros Ã  esquerda por meio de cast numÃ©rico (quando possÃ­vel)
    - retorna string (para facilitar joins textuais)
    """
    # Tentativa de cast numÃ©rico; se falhar vira null e tratamos abaixo
    as_int = F.when(F.trim(col).rlike(r"^\d+$"), F.col("cast_int")).otherwise(F.lit(None))
    return F.when(as_int.isNotNull(), F.col("cast_int").cast("string")) \
            .otherwise(F.null_literal())

def _clean_str(c: F.Column) -> F.Column:
    return F.upper(F.trim(c))


# =============================================================================
# Carregamento e normalizaÃ§Ã£o
# =============================================================================

def load_municipios_df(
    spark: SparkSession,
    settings: Settings,
    *,
    cache: bool = True
) -> DataFrame:
    """
    LÃª a tabela de municÃ­pios do Oracle e retorna DF normalizado com colunas:
      [ibge STRING, uf STRING, nome_municipio STRING]
    """
    df_raw = read_oracle_table(spark, settings, settings.oracle.tbl_municipios)

    cols = df_raw.columns
    ibge_col = _first_present(cols, _CAND_IBGE)
    uf_col = _first_present(cols, _CAND_UF)
    nome_col = _first_present(cols, _CAND_NOME)

    if not ibge_col or not uf_col or not nome_col:
        raise ValueError(
            f"[GEN] NÃ£o encontrei colunas compatÃ­veis em {settings.oracle.tbl_municipios}. "
            f"Esperava IBGE em {_CAND_IBGE}, UF em {_CAND_UF}, NOME em {_CAND_NOME}. "
            f"Colunas disponÃ­veis: {cols}"
        )

    # NormalizaÃ§Ã£o:
    # - IBGE: deixa como string sem zeros Ã  esquerda (via cast numÃ©rico controlado)
    # - UF:   upper/trim
    # - Nome: upper/trim (pode manter acentuaÃ§Ã£o; regra do negÃ³cio decide)
    df = (df_raw
          .withColumn("__ibge_raw", F.trim(F.col(ibge_col)))
          .withColumn("__ibge_int", F.when(F.col("__ibge_raw").rlike(r"^\d+$"), F.col("__ibge_raw").cast("long")).otherwise(F.lit(None)))
          .withColumn("ibge", F.when(F.col("__ibge_int").isNotNull(), F.col("__ibge_int").cast("string")).otherwise(F.col("__ibge_raw")))
          .withColumn("uf", _clean_str(F.col(uf_col)))
          .withColumn("nome_municipio", _clean_str(F.col(nome_col)))
          .select("ibge", "uf", "nome_municipio")
          .dropDuplicates(["ibge"])
    )

    if cache:
        df = df.cache()
        df.count()
    return df


# =============================================================================
# Contexto GEN (cache/broadcast)
# =============================================================================

class GenContext:
    """
    Container simples para objetos GEN.
    """
    def __init__(self, df_municipios: DataFrame):
        self.df_municipios = df_municipios

def build_gen_context(
    spark: SparkSession,
    settings: Settings
) -> GenContext:
    """
    Carrega e retorna o contexto GEN com municÃ­pios normalizados.
    """
    df_mun = load_municipios_df(spark, settings, cache=True)
    return GenContext(df_mun)


# =============================================================================
# JunÃ§Ãµes com documentos (NFe)
# =============================================================================

def _normalize_doc_ibge(df: DataFrame, colname: str, out: str) -> DataFrame:
    """
    Cria uma versÃ£o normalizada (string sem zeros Ã  esquerda) do campo IBGE do documento.
    Se a coluna nÃ£o existir, cria nula.
    """
    if colname not in df.columns:
        return df.withColumn(out, F.lit(None).cast("string"))

    return (df
            .withColumn(f"__{out}_raw", F.trim(F.col(colname)))
            .withColumn(f"__{out}_int", F.when(F.col(f"__{out}_raw").rlike(r"^\d+$"), F.col(f"__{out}_raw").cast("long")).otherwise(F.lit(None)))
            .withColumn(out, F.when(F.col(f"__{out}_int").isNotNull(), F.col(f"__{out}_int").cast("string")).otherwise(F.col(f"__{out}_raw")))
            .drop(f"__{out}_raw", f"__{out}_int")
    )


def join_emitente_municipio(
    df_doc: DataFrame,
    gen: GenContext,
    *,
    doc_ibge_col_candidates: Optional[List[str]] = None,
    suffix: str = "emit"
) -> DataFrame:
    """
    Enriquecimento por municÃ­pio do EMITENTE.
    - Localiza a primeira coluna existente entre `doc_ibge_col_candidates` (ou defaults).
    - Normaliza e faz join com df_municipios (por 'ibge').

    :param suffix: sufixo para as colunas enriquecidas: uf_{suffix}, nome_municipio_{suffix}
    """
    if doc_ibge_col_candidates is None:
        # Ajuste conforme os nomes no seu bronze/silver Kudu
        doc_ibge_col_candidates = [
            "emitente_cod_ibge",
            "emitente_cod_municipio_ibge",
            "emit_ibge",
            "ibge_emitente",
            "co_municipio_emitente",
        ]

    col_present = next((c for c in doc_ibge_col_candidates if c in df_doc.columns), None)
    if not col_present:
        # sem coluna: cria colunas vazias e retorna
        return (df_doc
                .withColumn(f"ibge_{suffix}", F.lit(None).cast("string"))
                .withColumn(f"uf_{suffix}", F.lit(None).cast("string"))
                .withColumn(f"nome_municipio_{suffix}", F.lit(None).cast("string")))

    tmp_ibge = f"ibge_{suffix}"
    dfn = _normalize_doc_ibge(df_doc, col_present, tmp_ibge)

    j = (dfn
         .join(gen.df_municipios.alias("mun"), on=[F.col(tmp_ibge) == F.col("mun.ibge")], how="left")
         .withColumnRenamed(tmp_ibge, f"ibge_{suffix}")
         .withColumn(f"uf_{suffix}", F.col("mun.uf"))
         .withColumn(f"nome_municipio_{suffix}", F.col("mun.nome_municipio"))
         .drop("mun.ibge", "mun.uf", "mun.nome_municipio")
    )
    return j


def join_destinatario_municipio(
    df_doc: DataFrame,
    gen: GenContext,
    *,
    doc_ibge_col_candidates: Optional[List[str]] = None,
    suffix: str = "dest"
) -> DataFrame:
    """
    Enriquecimento por municÃ­pio do DESTINATÃRIO.
    """
    if doc_ibge_col_candidates is None:
        doc_ibge_col_candidates = [
            "destinatario_cod_ibge",
            "destinatario_cod_municipio_ibge",
            "dest_ibge",
            "ibge_destinatario",
            "co_municipio_destinatario",
        ]

    col_present = next((c for c in doc_ibge_col_candidates if c in df_doc.columns), None)
    if not col_present:
        return (df_doc
                .withColumn(f"ibge_{suffix}", F.lit(None).cast("string"))
                .withColumn(f"uf_{suffix}", F.lit(None).cast("string"))
                .withColumn(f"nome_municipio_{suffix}", F.lit(None).cast("string")))

    tmp_ibge = f"ibge_{suffix}"
    dfn = _normalize_doc_ibge(df_doc, col_present, tmp_ibge)

    j = (dfn
         .join(gen.df_municipios.alias("mun"), on=[F.col(tmp_ibge) == F.col("mun.ibge")], how="left")
         .withColumnRenamed(tmp_ibge, f"ibge_{suffix}")
         .withColumn(f"uf_{suffix}", F.col("mun.uf"))
         .withColumn(f"nome_municipio_{suffix}", F.col("mun.nome_municipio"))
         .drop("mun.ibge", "mun.uf", "mun.nome_municipio")
    )
    return j


# =============================================================================
# Enriquecimento combinado (emitente + destinatÃ¡rio)
# =============================================================================

def apply_gen_enrichment(
    df_doc: DataFrame,
    gen: GenContext,
    *,
    emit_ibge_cols: Optional[List[str]] = None,
    dest_ibge_cols: Optional[List[str]] = None
) -> DataFrame:
    """
    Atalho que aplica os dois joins (emitente e destinatÃ¡rio) no DF de documentos.
    Retorna o DF enriquecido com:
      - ibge_emit, uf_emit, nome_municipio_emit
      - ibge_dest, uf_dest, nome_municipio_dest
    """
    d1 = join_emitente_municipio(df_doc, gen, doc_ibge_col_candidates=emit_ibge_cols, suffix="emit")
    d2 = join_destinatario_municipio(d1, gen, doc_ibge_col_candidates=dest_ibge_cols, suffix="dest")
    return d2
