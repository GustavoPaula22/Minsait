# -*- coding: utf-8 -*-
# app/utils/audit.py
"""
Auditoria padronizada do pipeline:
- Criação da tabela de auditoria (Iceberg) se não existir
- Registro de início (RUNNING)
- Atualizações parciais (contagens, colunas adicionais)
- Finalização com SUCCESS ou ERROR (MERGE por id)
Compatível com execução distribuída (sem pandas).
"""

from __future__ import annotations

import os
import socket
import time
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T

from app.settings import Settings


# =============================================================================
# Config / Helpers
# =============================================================================

def _qualify_iceberg(settings: Settings, table: str, namespace: Optional[str] = None) -> str:
    ns = namespace or settings.iceberg.namespace
    return f"{settings.iceberg.catalog}.{ns}.{table}"

def _now_ts():
    return F.current_timestamp()

def _gen_id() -> int:
    # inteiro pseudo-único com base no epoch em ms (cíclico em longos períodos, mas suficiente para auditoria)
    return int(time.time() * 1000) % 9_999_999_999

def _safe_sql_literal(v: Optional[str]) -> str:
    if v is None:
        return "NULL"
    return "'" + str(v).replace("'", "''") + "'"


# =============================================================================
# Tabela de Auditoria
# =============================================================================

def ensure_audit_table(spark: SparkSession, settings: Settings) -> str:
    """
    Garante a existência da tabela de auditoria no catálogo Iceberg.
    Retorna o nome totalmente qualificado.
    """
    full = _qualify_iceberg(settings, settings.iceberg.tbl_audit)

    if not spark.catalog.tableExists(full):
        spark.sql(f"""
            CREATE TABLE {full} (
              id_procesm_indice BIGINT,
              documento         STRING,
              data_inicio       DATE,
              data_fim          DATE,
              status            STRING,        -- RUNNING | SUCCESS | ERROR
              params_json       STRING,
              qtd_docs          BIGINT,
              qtd_itens         BIGINT,
              erro              STRING,        -- mensagem/stack resumida
              ts_inicio         TIMESTAMP,
              ts_fim            TIMESTAMP,
              app               STRING,
              host              STRING,
              spark_user        STRING,
              job_id            STRING
            )
            USING ICEBERG
        """)
    return full


# =============================================================================
# Registro de Início
# =============================================================================

def start_audit(
    spark: SparkSession,
    settings: Settings,
    *,
    documento: str,
    data_inicio: str,
    data_fim: str,
    params_json: Optional[str] = None,
    id_procesm_indice: Optional[int] = None
) -> int:
    """
    Insere uma linha de auditoria com status RUNNING e retorna o id_procesm_indice.
    data_inicio/data_fim aceitam 'YYYY-MM-DD'.
    """
    full = ensure_audit_table(spark, settings)
    audit_id = id_procesm_indice or _gen_id()

    app = spark.sparkContext.appName
    job_id = spark.sparkContext.applicationId
    host = socket.gethostname()
    spark_user = os.getenv("USER") or os.getenv("USERNAME") or "spark"

    # Criamos um DF de 1 linha (strings/dtypes simples) e inserimos via SQL
    row = spark.createDataFrame(
        [(audit_id, documento, data_inicio, data_fim, "RUNNING", params_json,
          None, None, None, None, None, app, host, spark_user, job_id)],
        schema=T.StructType([
            T.StructField("id_procesm_indice", T.LongType(), False),
            T.StructField("documento",         T.StringType(), True),
            T.StructField("data_inicio",       T.StringType(), True),  # cast no INSERT
            T.StructField("data_fim",          T.StringType(), True),
            T.StructField("status",            T.StringType(), True),
            T.StructField("params_json",       T.StringType(), True),
            T.StructField("qtd_docs",          T.LongType(), True),
            T.StructField("qtd_itens",         T.LongType(), True),
            T.StructField("erro",              T.StringType(), True),
            T.StructField("ts_inicio",         T.TimestampType(), True),
            T.StructField("ts_fim",            T.TimestampType(), True),
            T.StructField("app",               T.StringType(), True),
            T.StructField("host",              T.StringType(), True),
            T.StructField("spark_user",        T.StringType(), True),
            T.StructField("job_id",            T.StringType(), True),
        ])
    )
    row.createOrReplaceTempView("_audit_new_row")

    spark.sql(f"""
        INSERT INTO {full}
        SELECT
          id_procesm_indice,
          documento,
          TO_DATE(data_inicio) as data_inicio,
          TO_DATE(data_fim)    as data_fim,
          status,
          params_json,
          qtd_docs,
          qtd_itens,
          erro,
          current_timestamp()  as ts_inicio,
          ts_fim,
          app,
          host,
          spark_user,
          job_id
        FROM _audit_new_row
    """)

    spark.catalog.dropTempView("_audit_new_row")
    return audit_id


# =============================================================================
# Atualizações Parciais (MERGE)
# =============================================================================

def _merge_update_columns_sql(updates: Dict[str, Any]) -> str:
    """
    Constrói fragmento 'SET col = valor, ...' para MERGE.
    Aceita ints/floats/strings/None. Usa CURRENT_TIMESTAMP para ts_fim se pedirem "NOW()" (string).
    """
    sets = []
    for col, val in updates.items():
        if isinstance(val, (int, float)):
            sets.append(f"t.{col} = {val}")
        elif val is None:
            sets.append(f"t.{col} = NULL")
        elif isinstance(val, str) and val.upper() == "NOW()":
            sets.append(f"t.{col} = current_timestamp()")
        else:
            sets.append(f"t.{col} = {_safe_sql_literal(str(val))}")
    return ", ".join(sets)

def update_audit(
    spark: SparkSession,
    settings: Settings,
    *,
    id_procesm_indice: int,
    updates: Dict[str, Any]
) -> None:
    """
    Atualiza colunas arbitrárias da linha de auditoria via MERGE (por id_procesm_indice).
    Ex.: update_audit(..., updates={"qtd_docs": 123, "qtd_itens": 456})
    """
    if not updates:
        return
    full = ensure_audit_table(spark, settings)
    set_clause = _merge_update_columns_sql(updates)

    spark.sql(f"""
        MERGE INTO {full} t
        USING (SELECT {id_procesm_indice} AS id) s
        ON t.id_procesm_indice = s.id
        WHEN MATCHED THEN UPDATE SET {set_clause}
    """)


def bump_counts(
    spark: SparkSession,
    settings: Settings,
    *,
    id_procesm_indice: int,
    add_docs: Optional[int] = None,
    add_itens: Optional[int] = None
) -> None:
    """
    Incrementa contadores (qtd_docs, qtd_itens). Quando nulos, trata como 0 antes do incremento.
    """
    full = ensure_audit_table(spark, settings)
    inc_docs = 0 if add_docs is None else int(add_docs)
    inc_itens = 0 if add_itens is None else int(add_itens)

    spark.sql(f"""
        MERGE INTO {full} t
        USING (SELECT {id_procesm_indice} AS id) s
        ON t.id_procesm_indice = s.id
        WHEN MATCHED THEN UPDATE SET
          t.qtd_docs  = COALESCE(t.qtd_docs, 0) + {inc_docs},
          t.qtd_itens = COALESCE(t.qtd_itens, 0) + {inc_itens}
    """)


# =============================================================================
# Finalização
# =============================================================================

def finish_success(
    spark: SparkSession,
    settings: Settings,
    *,
    id_procesm_indice: int,
    extra_updates: Optional[Dict[str, Any]] = None
) -> None:
    """
    Marca SUCCESS e preenche ts_fim. Aceita colunas extras para atualizar no mesmo MERGE.
    """
    updates = {"status": "SUCCESS", "ts_fim": "NOW()"}
    if extra_updates:
        updates.update(extra_updates)
    update_audit(spark, settings, id_procesm_indice=id_procesm_indice, updates=updates)


def finish_error(
    spark: SparkSession,
    settings: Settings,
    *,
    id_procesm_indice: int,
    erro_msg: str,
    extra_updates: Optional[Dict[str, Any]] = None
) -> None:
    """
    Marca ERROR, grava mensagem (reduzida) e ts_fim.
    """
    msg = (erro_msg or "")[:4000]  # evita strings enormes
    updates = {"status": "ERROR", "erro": msg, "ts_fim": "NOW()"}
    if extra_updates:
        updates.update(extra_updates)
    update_audit(spark, settings, id_procesm_indice=id_procesm_indice, updates=updates)


# =============================================================================
# Atalhos de uso
# =============================================================================

def start_and_get_id(
    spark: SparkSession,
    settings: Settings,
    *,
    documento: str,
    data_inicio: str,
    data_fim: str,
    params_json: Optional[str] = None
) -> int:
    """
    Atalho para iniciar e obter o ID (útil em jobs).
    """
    return start_audit(
        spark, settings,
        documento=documento,
        data_inicio=data_inicio,
        data_fim=data_fim,
        params_json=params_json
    )


def safe_run_with_audit(
    spark: SparkSession,
    settings: Settings,
    *,
    documento: str,
    data_inicio: str,
    data_fim: str,
    params_json: Optional[str],
    fn  # callable(id_audit) -> None
) -> None:
    """
    Executa uma função sob auditoria:
    - start RUNNING
    - chama fn(id)
    - se ok: SUCCESS
    - se erro: ERROR + mensagem
    """
    audit_id = start_and_get_id(
        spark, settings,
        documento=documento,
        data_inicio=data_inicio,
        data_fim=data_fim,
        params_json=params_json
    )
    try:
        fn(audit_id)
        finish_success(spark, settings, id_procesm_indice=audit_id)
    except Exception as e:
        finish_error(spark, settings, id_procesm_indice=audit_id, erro_msg=str(e))
        raise
