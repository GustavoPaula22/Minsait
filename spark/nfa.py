# -*- coding: utf-8 -*-
# app/nfa.py
from __future__ import annotations

import argparse
import os
from typing import Dict, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from app.settings import Settings, load_settings_from_env, build_spark, print_settings
from app.utils.io import write_df, read_oracle_table
from app.utils.audit import start_and_get_id, bump_counts, finish_success, finish_error
from app.utils.rules import build_rules_context, apply_rules_to_documents, apply_rules_to_items


# =============================================================================
# Constantes / helpers
# =============================================================================
TIPO_DOC_NFA = 4  # EnumTipoDocumento.NFA no Django/IPM

# Tabelas Oracle (espelhando o Django)
DOC_TABLE = "IPM.IPM_DOCUMENTO_PARTCT_CALCULO"
HIST_TABLE = "IPM.IPM_HISTORICO_CONTRIBUINTE"
IDENT_TABLE = "NFA.NFA_IDENTIF_NOTA"
ITEM_TABLE = "NFA.NFA_ITEM_NOTA"
PARTE_ENV_TABLE = "NFA.NFA_PARTE_ENVOLVIDA"


def _ts_between(col: str, data_inicio: str, data_fim: str) -> str:
    return f"{col} >= TO_DATE('{data_inicio}','YYYY-MM-DD') AND {col} < (TO_DATE('{data_fim}','YYYY-MM-DD') + 1)"


def _align_df_to_iceberg_table(
    spark: SparkSession,
    settings: Settings,
    df: DataFrame,
    *,
    table_name: str,
    namespace: Optional[str] = None,
    fill_missing_with_null: bool = True,
) -> DataFrame:
    """
    Lê o schema físico do Iceberg e reordena/insere colunas no DF para compatibilizar a escrita.
    """
    ns = namespace or settings.iceberg.namespace
    full = f"{settings.iceberg.catalog}.{ns}.{table_name}"
    target = spark.read.format("iceberg").table(full).limit(0)
    tgt_fields = target.schema
    cols = [f.name for f in tgt_fields]
    dff = df
    if fill_missing_with_null:
        for f in tgt_fields:
            if f.name not in dff.columns:
                dff = dff.withColumn(f.name, F.lit(None).cast(f.dataType))
    # reordena: primeiro as colunas do alvo, depois as extras (se houver)
    ordered = [c for c in cols if c in dff.columns] + [c for c in dff.columns if c not in cols]
    dff = dff.select(*ordered)
    return dff


# =============================================================================
# Leitura das tabelas Oracle
# =============================================================================
def _load_base_tables(
    spark: SparkSession,
    settings: Settings,
    data_inicio: str,
    data_fim: str,
    where_docs_extra: Optional[str] = None,
) -> Dict[str, DataFrame]:
    """
    Carrega as tabelas de origem do Oracle com pushdown pelo período e tipo de documento.
    """
    base_where = f"CODG_TIPO_DOC_PARTCT_CALC = {TIPO_DOC_NFA} AND " + _ts_between("DATA_EMISSAO_DOCUMENTO", data_inicio, data_fim)
    if where_docs_extra:
        base_where = f"({base_where}) AND ({where_docs_extra})"

    doc = (
        read_oracle_table(spark, settings, DOC_TABLE, where=base_where)
        .select(
            "CODG_DOCUMENTO_PARTCT_CALCULO",
            "NUMR_INSCRICAO_SAIDA",
            "NUMR_INSCRICAO_ENTRADA",
            "CODG_MUNICIPIO_SAIDA",
            "CODG_MUNICIPIO_ENTRADA",
            "DATA_EMISSAO_DOCUMENTO",
            "NUMR_REFERENCIA",
            "CODG_TIPO_DOC_PARTCT_CALC",
        )
        .dropDuplicates(["CODG_DOCUMENTO_PARTCT_CALCULO"])
        .cache()
    )

    # Historico contribuinte (cadastro no mês/ref): join pelos pares (NUMR_INSCRICAO, NUMR_REFERENCIA)
    hist = (
        read_oracle_table(spark, settings, HIST_TABLE)
        .select(
            "NUMR_INSCRICAO_CONTRIB",
            "NUMR_REFERENCIA",
            "INDI_PRODUTOR_RURAL",
            "STAT_CADASTRO_CONTRIB",
            "TIPO_ENQDTO_FISCAL",
        )
        .dropDuplicates(["NUMR_INSCRICAO_CONTRIB", "NUMR_REFERENCIA"])
        .cache()
    )

    ident = (
        read_oracle_table(spark, settings, IDENT_TABLE)
        .select(
            "NUMR_IDENTIF_NOTA",
            "TIPO_NOTA",
            "TIPO_OPERACAO_NOTA",
            "VALR_TOTAL_NOTA",
            "STAT_NOTA",
            "CODG_CFOP",
            "CODG_MUNICIPIO_ORIGEM",
            "CODG_MUNICIPIO_DESTINO",
        )
        .cache()
    )

    item = (
        read_oracle_table(spark, settings, ITEM_TABLE)
        .select(
            "NUMR_IDENTIF_NOTA",
            "NUMR_SEQUENCIAL_ITEM",
            "CODG_PRODUTO_ANP",
            "VALR_BASE_CALCULO_ICMS",
            "VALR_ALIQUOTA_ICMS",
            "VALR_VALOR_ICMS",
            "VALR_BASE_CALCULO_PIS",
            "VALR_ALIQUOTA_PIS",
            "VALR_VALOR_PIS",
            "VALR_BASE_CALCULO_COFINS",
            "VALR_ALIQUOTA_COFINS",
            "VALR_VALOR_COFINS",
            "QUANT_ITEM",
            "VALR_UNITARIO",
            "VALR_TOTAL_ITEM",
            "INDI_APROPRIACAO",
        )
        .cache()
    )

    parte = (
        read_oracle_table(spark, settings, PARTE_ENV_TABLE)
        .select(
            "NUMR_IDENTIF_NOTA",
            "TIPO_ENVOLVIDO",
            "NUMR_INSCRICAO",
            "NUMR_CNPJ_ENVOLVIDO",
            "NUMR_CPF_ENVOLVIDO",
            "UF_ENVOLVIDO",
            "CODG_MUNICIPIO_ENVOLVIDO",
        )
        .cache()
    )

    return {"doc": doc, "hist": hist, "ident": ident, "item": item, "parte": parte}


# =============================================================================
# Projeções (NFA → IPM Iceberg)
# =============================================================================
def _mk_enquadramento(col_pr: F.Column, col_stat: F.Column, col_tipo_enq: F.Column) -> F.Column:
    """
    Replica a lógica do Django para classificar enquadramento do contribuinte:
      - Produtor Rural → 'Prod Rural'
      - STAT_CADASTRO_CONTRIB <> '1' → 'Nao Cad'
      - TIPO_ENQDTO_FISCAL ∈ {1,2} → 'NORMAL'
      - TIPO_ENQDTO_FISCAL ∈ {3,4,5} → 'SIMPLES'
      - Caso contrário → 'Nao Cad'
    """
    return (
        F.when(col_pr == F.lit("S"), F.lit("Prod Rural"))
        .otherwise(
            F.when(col_stat != F.lit("1"), F.lit("Nao Cad"))
            .otherwise(
                F.when(col_tipo_enq.isin("1", "2"), F.lit("NORMAL"))
                .when(col_tipo_enq.isin("3", "4", "5"), F.lit("SIMPLES"))
                .otherwise(F.lit("Nao Cad"))
            )
        )
    )


def _project_documents_items(
    spark: SparkSession,
    settings: Settings,
    tabs: Dict[str, DataFrame],
) -> Tuple[DataFrame, DataFrame]:
    """
    Monta DF de documentos e DF de itens com base nas tabelas da NFA.
    """
    doc = tabs["doc"].alias("Doc")
    hist = tabs["hist"].alias("Hist")
    ident = tabs["ident"].alias("Ident")
    item = tabs["item"].alias("Item")
    parte = tabs["parte"].alias("Parte")

    # Envolvidos: 2=Emitente, 3=Destinatário
    emit = (
        parte.filter(F.col("TIPO_ENVOLVIDO") == F.lit(2))
        .select(
            F.col("NUMR_IDENTIF_NOTA").alias("ID"),
            F.col("NUMR_INSCRICAO").cast(T.StringType()).alias("InscricaoEmit"),
            F.col("NUMR_CNPJ_ENVOLVIDO").alias("CnpjEmit"),
            F.col("UF_ENVOLVIDO").alias("UFEmit"),
            F.col("CODG_MUNICIPIO_ENVOLVIDO").alias("MunicEmit"),
        )
        .alias("Emit")
    )

    dest = (
        parte.filter(F.col("TIPO_ENVOLVIDO") == F.lit(3))
        .select(
            F.col("NUMR_IDENTIF_NOTA").alias("ID"),
            F.col("NUMR_INSCRICAO").cast(T.StringType()).alias("InscricaoDest"),
            F.coalesce(F.col("NUMR_CNPJ_ENVOLVIDO"), F.col("NUMR_CPF_ENVOLVIDO")).alias("CnpjCpfDest"),
            F.col("UF_ENVOLVIDO").alias("UFDest"),
            F.col("CODG_MUNICIPIO_ENVOLVIDO").alias("MunicDest"),
        )
        .alias("Dest")
    )

    # Cadastro (entrada/saída) no mês de referência do Doc
    iesai = (
        hist.select(
            F.col("NUMR_INSCRICAO_CONTRIB").alias("IE"),
            F.col("NUMR_REFERENCIA").alias("REF"),
            F.col("INDI_PRODUTOR_RURAL").alias("PR"),
            F.col("STAT_CADASTRO_CONTRIB").alias("STAT"),
            F.col("TIPO_ENQDTO_FISCAL").alias("TE"),
        ).alias("IeSai")
    )

    ieent = iesai.alias("IeEnt")

    docs = (
        doc.join(ident, F.col("Doc.CODG_DOCUMENTO_PARTCT_CALCULO") == F.col("Ident.NUMR_IDENTIF_NOTA"), "inner")
        .join(emit, F.col("Ident.NUMR_IDENTIF_NOTA") == F.col("Emit.ID"), "left")
        .join(dest, F.col("Ident.NUMR_IDENTIF_NOTA") == F.col("Dest.ID"), "left")
        .join(
            iesai,
            (F.col("Doc.NUMR_INSCRICAO_SAIDA") == F.col("IeSai.IE"))
            & (F.col("Doc.NUMR_REFERENCIA") == F.col("IeSai.REF")),
            "left",
        )
        .join(
            ieent,
            (F.col("Doc.NUMR_INSCRICAO_ENTRADA") == F.col("IeEnt.IE"))
            & (F.col("Doc.NUMR_REFERENCIA") == F.col("IeEnt.REF")),
            "left",
        )
    )

    # Campos derivados (alinhados ao SQL do Django)
    docs = (
        docs.withColumn("IeSaida", F.col("Doc.NUMR_INSCRICAO_SAIDA").cast(T.StringType()))
        .withColumn("IeEntrada", F.col("Doc.NUMR_INSCRICAO_ENTRADA").cast(T.StringType()))
        .withColumn("IeSaidaEnqdto", _mk_enquadramento(F.col("IeSai.PR"), F.col("IeSai.STAT"), F.col("IeSai.TE")))
        .withColumn("IeEntradaEnqdto", _mk_enquadramento(F.col("IeEnt.PR"), F.col("IeEnt.STAT"), F.col("IeEnt.TE")))
        .withColumn("MunicGerador", F.col("Ident.CODG_MUNICIPIO_ORIGEM"))
        .withColumn("MunicDestino", F.col("Ident.CODG_MUNICIPIO_DESTINO"))
        .withColumn("MunicSaida", F.col("Doc.CODG_MUNICIPIO_SAIDA"))
        .withColumn("MunicEntrada", F.col("Doc.CODG_MUNICIPIO_ENTRADA"))
        .withColumn("Modelo", F.lit("55"))  # mantém compatibilidade do modelo unificado
        .withColumn("NatOper", F.lit("Não informado"))
        .withColumn("numr_ref_emissao", F.date_format(F.col("Doc.DATA_EMISSAO_DOCUMENTO"), "yyyyMM"))
        .withColumn("DataEmissao", F.date_format(F.col("Doc.DATA_EMISSAO_DOCUMENTO"), "dd/MM/yyyy HH:mm:ss"))
        .withColumn("DATA_EMISSAO_DOCUMENTO", F.col("Doc.DATA_EMISSAO_DOCUMENTO").cast(T.TimestampType()))
        .withColumn("Chave", F.lit("0"))
        .withColumn(
            "TipoOperacao",
            F.when(F.col("Ident.TIPO_NOTA") == F.lit(1), F.lit("Entrada")).otherwise(F.lit("Saída")),
        )
        .withColumn(
            "DestinoOperacao",
            F.when(F.col("Ident.TIPO_OPERACAO_NOTA") == F.lit(1), F.lit("Operação interna"))
            .when(F.col("Ident.TIPO_OPERACAO_NOTA") == F.lit(2), F.lit("Operação interestadual"))
            .when(F.col("Ident.TIPO_OPERACAO_NOTA") == F.lit(3), F.lit("Operação com exterior"))
            .otherwise(F.lit("Operação não identificada")),
        )
        .withColumn(
            "Finalidade",
            F.when(F.col("Ident.TIPO_OPERACAO_NOTA").isin(1, 2, 3), F.lit("NF-e normal"))
            .when(F.col("Ident.TIPO_OPERACAO_NOTA") == F.lit(4), F.lit("NF-e complementar"))
            .when(F.col("Ident.TIPO_OPERACAO_NOTA") == F.lit(5), F.lit("Devolução de mercadoria"))
            .otherwise(F.lit("Finalidade não identificada")),
        )
        .withColumn("ValorDoc", F.col("Ident.VALR_TOTAL_NOTA").cast(T.DecimalType(17, 2)))
        .withColumn(
            "ProtocCancel",
            F.when(F.col("Ident.STAT_NOTA") == F.lit(5), F.col("Ident.NUMR_IDENTIF_NOTA")).otherwise(F.lit(0)),
        )
        .withColumn("CFOP", F.col("Ident.CODG_CFOP"))
        .withColumn("CODG_TIPO_DOC_PARTCT_CALC", F.lit(TIPO_DOC_NFA))
        .withColumn("CODG_DOCUMENTO_PARTCT_CALCULO", F.col("Doc.CODG_DOCUMENTO_PARTCT_CALCULO"))
        # partições ano/mes
        .withColumn("ano", F.year(F.col("Doc.DATA_EMISSAO_DOCUMENTO")).cast(T.IntegerType()))
        .withColumn("mes", F.month(F.col("Doc.DATA_EMISSAO_DOCUMENTO")).cast(T.IntegerType()))
    )

    # Itens (+ ano/mes herdados do documento)
    items = (
        item.join(ident, F.col("Item.NUMR_IDENTIF_NOTA") == F.col("Ident.NUMR_IDENTIF_NOTA"), "inner")
        .join(doc, F.col("Item.NUMR_IDENTIF_NOTA") == F.col("Doc.CODG_DOCUMENTO_PARTCT_CALCULO"), "inner")
        .select(
            F.col("Doc.CODG_DOCUMENTO_PARTCT_CALCULO"),
            F.col("Item.NUMR_SEQUENCIAL_ITEM").alias("ID_ITEM_NOTA_FISCAL"),
            F.col("Item.CODG_PRODUTO_ANP").alias("ANP"),
            F.col("Item.VALR_BASE_CALCULO_ICMS").alias("VALR_BASE_CALCULO_ICMS"),
            F.col("Item.VALR_ALIQUOTA_ICMS").alias("VALR_ALIQUOTA_ICMS"),
            F.col("Item.VALR_VALOR_ICMS").alias("VALR_VALOR_ICMS"),
            F.col("Item.VALR_BASE_CALCULO_PIS").alias("VALR_BASE_CALCULO_PIS"),
            F.col("Item.VALR_ALIQUOTA_PIS").alias("VALR_ALIQUOTA_PIS"),
            F.col("Item.VALR_VALOR_PIS").alias("VALR_VALOR_PIS"),
            F.col("Item.VALR_BASE_CALCULO_COFINS").alias("VALR_BASE_CALCULO_COFINS"),
            F.col("Item.VALR_ALIQUOTA_COFINS").alias("VALR_ALIQUOTA_COFINS"),
            F.col("Item.VALR_VALOR_COFINS").alias("VALR_VALOR_COFINS"),
            F.col("Item.QUANT_ITEM").alias("QUANT_ITEM"),
            F.col("Item.VALR_UNITARIO").alias("VALR_UNITARIO"),
            F.col("Item.VALR_TOTAL_ITEM").alias("VALR_TOTAL_ITEM"),
            F.col("Item.INDI_APROPRIACAO").alias("IndAprop"),
            F.lit(TIPO_DOC_NFA).alias("CODG_TIPO_DOC_PARTCT_CALC"),
            F.year(F.col("Doc.DATA_EMISSAO_DOCUMENTO")).cast(T.IntegerType()).alias("ano"),
            F.month(F.col("Doc.DATA_EMISSAO_DOCUMENTO")).cast(T.IntegerType()).alias("mes"),
        )
    )

    # Projeção final de documentos (inclui DATA_EMISSAO_DOCUMENTO para compat com deletes/partição)
    docs_out = docs.select(
        "CODG_DOCUMENTO_PARTCT_CALCULO",
        "CODG_TIPO_DOC_PARTCT_CALC",
        "IeSaida",
        "IeSaidaEnqdto",
        "IeEntrada",
        "IeEntradaEnqdto",
        "MunicGerador",
        "MunicDestino",
        "MunicSaida",
        "MunicEntrada",
        "CnpjEmit",
        "CnpjCpfDest",
        "Modelo",
        "NatOper",
        "numr_ref_emissao",
        "DataEmissao",
        "DATA_EMISSAO_DOCUMENTO",
        "Chave",
        "TipoOperacao",
        "DestinoOperacao",
        "Finalidade",
        "ValorDoc",
        "ProtocCancel",
        "CFOP",
        "ano",
        "mes",
    )

    return docs_out, items


# =============================================================================
# Função pública (para ser chamada pelo main.py diretamente)
# =============================================================================
def run_nfa(
    spark: SparkSession,
    settings: Settings,
    *,
    data_inicio: str,
    data_fim: str,
    where_docs: Optional[str],
    where_itens: Optional[str],  # não é usado aqui (itens vêm do relacionamento), mantido por compat
    prefer_day_partition: bool,
    audit_params: Optional[Dict] = None,
    audit_enabled: bool = True,
) -> Dict[str, int]:
    """
    Executa o pipeline NFA (Oracle -> regras -> Iceberg) retornando métricas.
    """
    audit = None
    if audit_enabled:
        audit = start_and_get_id(spark, "NFA")

    try:
        # 1) Carregar tabelas base (Oracle) com filtro por período e tipo
        base = _load_base_tables(spark, settings, data_inicio, data_fim, where_docs_extra=where_docs)

        # 2) Projetar documentos/itens NFA
        df_docs, df_items = _project_documents_items(spark, settings, base)

        # 3) Regras (paridade com Django)
        rules_ctx = build_rules_context(spark, settings)
        df_docs = apply_rules_to_documents(df_docs, rules_ctx, tipo_documento=TIPO_DOC_NFA)
        df_items = apply_rules_to_items(df_items, rules_ctx, tipo_documento=TIPO_DOC_NFA)

        # 4) Alinhar schemas ao Iceberg
        ns = settings.iceberg.namespace
        df_docs_out = _align_df_to_iceberg_table(
            spark, settings, df_docs, table_name=settings.iceberg.tbl_documento_partct, namespace=ns
        )
        df_items_out = _align_df_to_iceberg_table(
            spark, settings, df_items, table_name=settings.iceberg.tbl_item_documento, namespace=ns
        )

        # 5) Escrita (modos vêm do Settings.partitioning)
        write_df(
            df_docs_out,
            settings=settings,
            output_format="iceberg",
            table_name=settings.iceberg.tbl_documento_partct,
            namespace=ns,
            mode=settings.partitioning.mode_documento,
        )
        write_df(
            df_items_out,
            settings=settings,
            output_format="iceberg",
            table_name=settings.iceberg.tbl_item_documento,
            namespace=ns,
            mode=settings.partitioning.mode_item,
        )

        # Métricas
        m_docs = df_docs_out.count()
        m_items = df_items_out.count()

        if audit_enabled and audit is not None:
            bump_counts(audit, docs=m_docs, items=m_items)
            finish_success(audit, "NFA", data_inicio, data_fim)

        return {
            "documento_rows_written": int(m_docs),
            "item_rows_written": int(m_items),
            "periodo_dias": 1 if data_inicio == data_fim else -1,  # placeholder
        }

    except Exception as e:
        if audit_enabled and audit is not None:
            finish_error(audit, "NFA", str(e))
        raise


# =============================================================================
# Wrappers CLI (compat com main.py que chama nfa_cli.run(args=...))
# =============================================================================
def run(args=None):
    """
    Executa via CLI embutida (compat com main.py que delega para nfa_cli.run).
    Aceita:
      --data-inicio YYYY-MM-DD
      --data-fim    YYYY-MM-DD
      [--no-audit]
      [--namespace ...] (opcional; normalmente use o settings)
      [--modo append|overwrite] (opcional; normalmente use o settings)
      [--parquet-out <dir>] (opcional; debug)
    """
    parser = argparse.ArgumentParser(description="Pipeline NFA (Oracle/IPM -> Regras -> Iceberg)")
    parser.add_argument("--data-inicio", required=True, help="YYYY-MM-DD")
    parser.add_argument("--data-fim", required=True, help="YYYY-MM-DD")
    parser.add_argument("--no-audit", action="store_true")
    parser.add_argument("--namespace", default=None)
    parser.add_argument("--modo", default=None, choices=["append", "overwrite"])
    parser.add_argument("--parquet-out", default=None)
    ns = parser.parse_args(args=args)

    settings = load_settings_from_env()
    spark = build_spark("Pipeline NFA (IPM → Iceberg)", settings)
    print_settings(settings)

    try:
        # Executa com o mesmo core do main (reaproveitando run_nfa)
        metrics = run_nfa(
            spark,
            settings,
            data_inicio=ns.data_inicio,
            data_fim=ns.data_fim,
            where_docs=None,
            where_itens=None,
            prefer_day_partition=False,
            audit_params={"cli": True, "document": "NFA"},
            audit_enabled=(not ns.no_audit),
        )

        # (Opcional) export Parquet para depuração
        if ns.parquet_out:
            # Reconstroi dataframes já alinhados para persistir como parquet de debug
            base = _load_base_tables(spark, settings, ns.data_inicio, ns.data_fim, where_docs_extra=None)
            df_docs, df_items = _project_documents_items(spark, settings, base)
            df_docs_out = _align_df_to_iceberg_table(
                spark, settings, df_docs, table_name=settings.iceberg.tbl_documento_partct
            )
            df_items_out = _align_df_to_iceberg_table(
                spark, settings, df_items, table_name=settings.iceberg.tbl_item_documento
            )
            df_docs_out.write.mode("overwrite").parquet(os.path.join(ns.parquet_out, "nfa_docs"))
            df_items_out.write.mode("overwrite").parquet(os.path.join(ns.parquet_out, "nfa_items"))

        print("=== Metrics ===")
        for k in sorted(metrics.keys()):
            print(f"{k}: {metrics[k]}")
    finally:
        spark.stop()


# Mantém um alias explícito para execução direta deste módulo.
def _run_cli(args=None):
    return run(args=args)


if __name__ == "__main__":
    run()
