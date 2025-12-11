# -*- coding: utf-8 -*-
# app/main.py
"""
CLI orchestration for the pipelines (Django -> Spark)

Implements:
  - NFE, CTE, BPE, NF3E (Kudu -> Rules -> Iceberg)
  - EFD (Kudu -> Rules -> Iceberg) com chave sintética (doc_id)
  - NFA (Oracle/IPM -> Rules -> Iceberg) [mesmas origens/regras do Django]

Examples:
  spark-submit ... app/main.py \
    --document nfe \
    --data-inicio 2024-01-01 --data-fim 2024-01-31 \
    --dt-inicio "2024-01-01 00:00:00" --dt-fim "2024-01-31 23:59:59" \
    --ts-col-docs ide_dhemi_nums --ts-col-itens ide_dhemi_nums \
    --prefer-day-partition true \
    --truncate-iceberg --no-audit --print-settings true
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, Optional, Tuple
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

from app.settings import (
    Settings,
    load_settings_from_env,
    build_spark,
    print_settings,
)

# Pipelines por documento (Kudu-based)
from app.nfe import run_nfe
from app.cte import run_cte
from app.bpe import run_bpe
from app.nf3e import run_nf3e
from app.efd import run_efd

# NFA (Oracle/IPM -> Iceberg)
from app.nfa import run_nfa


# -----------------------------------------------------------------------------
# helpers
# -----------------------------------------------------------------------------
def _parse_bool(v: str) -> bool:
    return str(v).strip().lower() in {"1", "true", "t", "y", "yes", "sim", "s"}


# defaults de coluna temporal por documento (podem ser sobrescritos por CLI)
# NFA não usa leitura por coluna temporal do Kudu → None / None
_DEFAULT_TS_COLS = {
    "nfe":  ("ide_dhemi_nums", "ide_dhemi_nums"),
    "cte":  ("ide_dhemi_nums", "ide_dhemi_nums"),
    "bpe":  ("dh_emis_nums",   "dh_emis_nums"),
    "nf3e": ("dh_emis_nums",   "dh_emis_nums"),  # NF3e ignora aqui; usa internamente
    "efd":  ("dt_doc_nums",    "dt_doc_nums"),
    "nfa":  (None, None),
}


def _mk_where_ts_auto(
    dt_inicio: Optional[str],
    dt_fim: Optional[str],
    *,
    col: str
) -> Optional[str]:
    """
    Gera WHERE que funciona se a coluna for:
      - epoch (segundos)  OU
      - yyyyMMddHHmmss (14 dígitos, numérico)
    """
    if not dt_inicio or not dt_fim:
        return None
    di = dt_inicio.replace("T", " ")
    df = dt_fim.replace("T", " ")

    di_num = f"{di[:10]} {di[11:19]}".replace("-", "").replace(" ", "").replace(":", "")
    df_num = f"{df[:10]} {df[11:19]}".replace("-", "").replace(" ", "").replace(":", "")

    where_sql = f"""
        (
          (length(cast({col} as string)) <= 10 AND
             {col} BETWEEN unix_timestamp('{di}') AND unix_timestamp('{df}')
          )
          OR
          (length(cast({col} as string)) >= 12 AND
             {col} BETWEEN CAST('{di_num}' as BIGINT) AND CAST('{df_num}' as BIGINT)
          )
        )
    """
    return " ".join(where_sql.split())


def _iceberg_tables_for_doc(settings: Settings, doc: str) -> Tuple[str, str]:
    """
    Retorna (fq_doc, fq_item) totalmente qualificados no catálogo Iceberg
    para o documento informado.
    """
    d = doc.strip().lower()
    ice = settings.iceberg

    if d == "nfe":
        return ice.nfe_document_table(), ice.nfe_item_table()
    if d == "cte":
        return ice.cte_document_table(), ice.cte_item_table()
    if d == "bpe":
        return ice.bpe_document_table(), ice.bpe_item_table()
    if d == "nf3e":
        return ice.nf3e_document_table(), ice.nf3e_item_table()
    if d == "efd":
        return ice.efd_document_table(), ice.efd_item_table()
    if d == "nfa":
        # NFA escreve nas MESMAS tabelas unificadas de IPM (documento/item) do modelo
        return ice.document_table(), ice.item_table()

    # fallback genérico (mantém compat)
    return ice.document_table(), ice.item_table()


# -----------------------------------------------------------------------------
# parser
# -----------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Executor de pipelines (Projeto Django -> Spark)")

    p.add_argument("--document", default="nfe",
                   choices=["nfe", "cte", "bpe", "nf3e", "efd", "nfa"],
                   help="Documento alvo")

    # Janela lógica/auditoria
    p.add_argument("--data-inicio", required=True, help="Data inicial AAAA-MM-DD (auditoria / janela)")
    p.add_argument("--data-fim", required=True, help="Data final AAAA-MM-DD (auditoria / janela)")

    # WHERE adicionais (fonte)
    p.add_argument("--where-docs", default=None, help="Filtro SQL para leitura de documentos (fonte)")
    p.add_argument("--where-itens", default=None, help="Filtro SQL para leitura de itens (fonte)")

    # Ajuda a gerar WHERE automático por coluna temporal
    p.add_argument("--dt-inicio", default=None, help="AAAA-MM-DD HH:MM:SS (para WHERE automático)")
    p.add_argument("--dt-fim", default=None, help="AAAA-MM-DD HH:MM:SS (para WHERE automático)")
    p.add_argument("--ts-col-docs", default=None, help="Nome da coluna temporal em DOCUMENTOS (fonte)")
    p.add_argument("--ts-col-itens", default=None, help="Nome da coluna temporal em ITENS (fonte)")

    # Estratégia de partição
    p.add_argument("--prefer-day-partition", default="false", help="true/false: executar dia a dia")

    # Exibição e auditoria
    p.add_argument("--print-settings", default="false", help="true/false: imprime settings")
    p.add_argument("--no-audit", action="store_true", help="Quando presente, desativa auditoria")

    # Operações pré-escrita Iceberg
    p.add_argument("--truncate-iceberg", action="store_true", help="TRUNCATE nas tabelas Iceberg antes de gravar")
    p.add_argument("--delete-iceberg-window", action="store_true",
                   help="DELETE somente a janela [data-inicio, data-fim] antes de gravar")

    # Evita precisar exportar variáveis de ambiente
    p.add_argument("--kudu-masters", default=None, help="Lista de masters do Kudu (se vazio, usa default interno)")
    p.add_argument("--kudu-db", default=None, help="Database do Kudu (se vazio, usa default interno)")

    # Parâmetros extras úteis para NF3e (e futuros pipelines)
    p.add_argument("--chave", default=None, help="Filtrar por uma única chave (quando suportado pelo pipeline)")
    p.add_argument("--no-write", action="store_true", help="Não grava no Iceberg (apenas lê/processa), quando suportado")
    p.add_argument("--mode", default="merge", choices=["merge", "append"], help="Modo de escrita no Iceberg (quando suportado)")

    return p


# -----------------------------------------------------------------------------
# demux documento
# -----------------------------------------------------------------------------
def run_document_pipeline(
    document: str,
    spark: SparkSession,
    settings: Settings,
    *,
    data_inicio: str,
    data_fim: str,
    where_docs: Optional[str],
    where_itens: Optional[str],
    prefer_day_partition: bool,
    audit_enabled: bool,
    # extras opcionais
    chave: Optional[str] = None,
    no_write: bool = False,
    mode: str = "merge",
    kudu_db_cli: Optional[str] = None,
    print_settings_flag: bool = False,
) -> Dict[str, int]:
    doc = (document or "").strip().lower()

    if doc == "nfe":
        return run_nfe(
            spark, settings,
            data_inicio=data_inicio,
            data_fim=data_fim,
            where_docs=where_docs,
            where_itens=where_itens,
            prefer_day_partition=prefer_day_partition,
            audit_params={"cli": True, "document": "NFE"},
            audit_enabled=audit_enabled,
        )

    if doc == "cte":
        return run_cte(
            spark, settings,
            data_inicio=data_inicio,
            data_fim=data_fim,
            where_docs=where_docs,
            where_itens=where_itens,
            prefer_day_partition=prefer_day_partition,
            audit_params={"cli": True, "document": "CTE"},
            audit_enabled=audit_enabled,
        )

    if doc == "bpe":
        return run_bpe(
            spark, settings,
            data_inicio=data_inicio,
            data_fim=data_fim,
            where_docs=where_docs,
            where_itens=where_itens,
            prefer_day_partition=prefer_day_partition,
            audit_params={"cli": True, "document": "BPE"},
            audit_enabled=audit_enabled,
        )

    if doc == "nf3e":
        # NF3e usa montagem interna de WHERE pelo período e aceita filtros adicionais via 'chave'.
        # Também aceita controlar escrita/auditoria diretamente.
        return run_nf3e(
            spark, settings,
            data_inicio=data_inicio,
            data_fim=data_fim,
            chave=chave,
            kudu_db=(kudu_db_cli or os.getenv("IPM_KUDU_DATABASE") or "kudu"),
            print_settings_flag=bool(print_settings_flag),
            audit_enabled=audit_enabled,
            write_iceberg=(not no_write),
            mode=mode,
        )

    if doc == "efd":
        # EFD ignora where_docs/where_itens; usa apenas data_inicio/data_fim.
        return run_efd(
            spark, settings,
            data_inicio=data_inicio,
            data_fim=data_fim,
        )

    if doc == "nfa":
        # NFA (Oracle/IPM -> Iceberg) executa no mesmo Spark desta orquestração.
        return run_nfa(
            spark, settings,
            data_inicio=data_inicio,
            data_fim=data_fim,
            where_docs=where_docs,
            where_itens=where_itens,  # ignorado dentro do NFA; mantido por compat
            prefer_day_partition=prefer_day_partition,
            audit_params={"cli": True, "document": "NFA"},
            audit_enabled=audit_enabled,
        )

    raise ValueError(f"Documento não suportado: {document}. Use um de: nfe | cte | bpe | nf3e | efd | nfa.")


# -----------------------------------------------------------------------------
# main
# -----------------------------------------------------------------------------
def main(argv=None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    # Compat: permitir definir Kudu via CLI (espelha envs usados no settings)
    if args.kudu_masters:
        os.environ["IPM_KUDU_MASTERS"] = args.kudu_masters
        os.environ["KUDU_MASTERS"] = args.kudu_masters  # legado
    else:
        os.environ.setdefault(
            "IPM_KUDU_MASTERS",
            "bdades01-node01.sefaz.go.gov.br:7051,bdades01-node02.sefaz.go.gov.br:7051,bdades01-node03.sefaz.go.gov.br:7051",
        )
        os.environ.setdefault(
            "KUDU_MASTERS",
            os.environ["IPM_KUDU_MASTERS"],
        )

    if args.kudu_db:
        os.environ["IPM_KUDU_DATABASE"] = args.kudu_db
        os.environ["KUDU_DB"] = args.kudu_db  # legado
    else:
        os.environ.setdefault("IPM_KUDU_DATABASE", "nfe")
        os.environ.setdefault("KUDU_DB", os.environ["IPM_KUDU_DATABASE"])

    settings = load_settings_from_env()
    spark = build_spark(settings)

    try:
        if _parse_bool(args.print_settings):
            print_settings(settings)

        prefer_day = _parse_bool(args.prefer_day_partition)

        # Resolve colunas temporais por doc (com override do usuário)
        doc_key = (args.document or "nfe").strip().lower()
        ts_docs_default, ts_itens_default = _DEFAULT_TS_COLS.get(doc_key, ("ide_dhemi_nums", "ide_dhemi_nums"))
        ts_col_docs = (args.ts_col_docs or ts_docs_default)
        ts_col_itens = (args.ts_col_itens or ts_itens_default)

        # WHERE padrão (somente para docs que usam leitura por coluna temporal)
        where_docs_ts = _mk_where_ts_auto(args.dt_inicio, args.dt_fim, col=ts_col_docs) if ts_col_docs else None
        where_itens_ts = _mk_where_ts_auto(args.dt_inicio, args.dt_fim, col=ts_col_itens) if ts_col_itens else None

        base_where_docs = args.where_docs or where_docs_ts
        base_where_itens = args.where_itens or where_itens_ts

        # Resolve tabelas Iceberg (fully qualified) para o documento selecionado
        fq_doc, fq_item = _iceberg_tables_for_doc(settings, args.document)

        # Operações pré-escrita (truncate / delete janela) — executadas uma única vez
        if args.truncate_iceberg:
            print(f"[main] TRUNCATE TABLE {fq_doc}")
            spark.sql(f"TRUNCATE TABLE {fq_doc}")
            print(f"[main] TRUNCATE TABLE {fq_item}")
            spark.sql(f"TRUNCATE TABLE {fq_item}")

        if args.delete_iceberg_window:
            di = args.data_inicio  # 'YYYY-MM-DD'
            df_ = args.data_fim    # 'YYYY-MM-DD' (fim inclusivo -> usamos < +1 dia)
            print(f"[main] DELETE janela em {fq_doc} e {fq_item}: [{di}, {df_}]")

            if doc_key == "nfa":
                # Para NFA, usamos a data do modelo IPM e filtramos pelo tipo 4
                spark.sql(f"""
                    DELETE FROM {fq_doc}
                    WHERE CODG_TIPO_DOC_PARTCT_CALC = 4
                      AND DATA_EMISSAO_DOCUMENTO >= DATE '{di}'
                      AND DATA_EMISSAO_DOCUMENTO <  DATE '{df_}' + INTERVAL 1 DAY
                """)
                spark.sql(f"""
                    DELETE FROM {fq_item}
                    WHERE CODG_TIPO_DOC_PARTCT_CALC = 4
                      AND CODG_DOCUMENTO_PARTCT_CALCULO IN (
                          SELECT CODG_DOCUMENTO_PARTCT_CALCULO
                          FROM {fq_doc}
                          WHERE CODG_TIPO_DOC_PARTCT_CALC = 4
                            AND DATA_EMISSAO_DOCUMENTO >= DATE '{di}'
                            AND DATA_EMISSAO_DOCUMENTO <  DATE '{df_}' + INTERVAL 1 DAY
                      )
                """)
            else:
                # Padrão legado (NFe etc.) — mantém comportamento atual
                spark.sql(f"""
                    DELETE FROM {fq_doc}
                    WHERE DATA_EMISSAO_NFE >= DATE '{di}'
                      AND DATA_EMISSAO_NFE <  DATE '{df_}' + INTERVAL 1 DAY
                """)
                spark.sql(f"""
                    DELETE FROM {fq_item}
                    WHERE CODG_CHAVE_ACESSO_NFE IN (
                        SELECT CODG_CHAVE_ACESSO_NFE
                        FROM {fq_doc}
                        WHERE DATA_EMISSAO_NFE >= DATE '{di}'
                          AND DATA_EMISSAO_NFE <  DATE '{df_}' + INTERVAL 1 DAY
                    )
                """)

        # Execução em janela única (default) OU dia a dia
        if not prefer_day:
            metrics = run_document_pipeline(
                args.document,
                spark,
                settings,
                data_inicio=args.data_inicio,
                data_fim=args.data_fim,
                where_docs=base_where_docs,
                where_itens=base_where_itens,
                prefer_day_partition=False,
                audit_enabled=(not args.no_audit),
                # extras
                chave=args.chave,
                no_write=args.no_write,
                mode=args.mode,
                kudu_db_cli=args.kudu_db,
                print_settings_flag=_parse_bool(args.print_settings),
            )

            print("=== Metrics ===")
            for k in sorted(metrics.keys()):
                print(f"{k}: {metrics[k]}")

        else:
            di = datetime.strptime(args.data_inicio, "%Y-%m-%d").date()
            df = datetime.strptime(args.data_fim, "%Y-%m-%d").date()

            acc: Dict[str, int] = {}
            cur = di
            while cur <= df:
                dia = cur.strftime("%Y-%m-%d")

                # WHERE diário (somente se útil para fonte; EFD/NFA ignoram)
                day_where_docs = _mk_where_ts_auto(f"{dia} 00:00:00", f"{dia} 23:59:59", col=ts_col_docs) if ts_col_docs else None
                day_where_itens = _mk_where_ts_auto(f"{dia} 00:00:00", f"{dia} 23:59:59", col=ts_col_itens) if ts_col_itens else None

                print(f"\n[main] >>> Executando {args.document.upper()} dia {dia}")
                m = run_document_pipeline(
                    args.document,
                    spark,
                    settings,
                    data_inicio=dia,
                    data_fim=dia,
                    where_docs=day_where_docs or base_where_docs,
                    where_itens=day_where_itens or base_where_itens,
                    prefer_day_partition=True,
                    audit_enabled=(not args.no_audit),
                    # extras
                    chave=args.chave,
                    no_write=args.no_write,
                    mode=args.mode,
                    kudu_db_cli=args.kudu_db,
                    print_settings_flag=_parse_bool(args.print_settings),
                )

                print(f"=== Metrics {dia} ===")
                for k in sorted(m.keys()):
                    print(f"{k}: {m[k]}")
                    try:
                        acc[k] = acc.get(k, 0) + int(m[k])
                    except Exception:
                        # mantém robustez caso alguma métrica não seja numérica
                        pass

                cur += timedelta(days=1)

            if acc:
                print("\n=== Metrics (SUMÁRIO do período) ===")
                for k in sorted(acc.keys()):
                    print(f"{k}: {acc[k]}")

    except Exception as e:
        print(f"[main] Error: {e}", file=sys.stderr)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
