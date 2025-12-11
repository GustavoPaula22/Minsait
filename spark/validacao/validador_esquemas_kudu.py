#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, os, sys, traceback
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql import DataFrame

def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Spark + Trino: amostras de tabelas por schema")
    p.add_argument("--databases", required=True, help="Schemas do catálogo (ex: cte,bpe,nf3e,efd,nfe)")
    p.add_argument("--tables", default="", help="Opcional: lista de tabelas específicas (ex: cte_infcte,cte_evento)")
    p.add_argument("--limit", type=int, default=20, help="Linhas de amostra por tabela (default: 20)")
    p.add_argument("--catalog", default="kudu", help="Catálogo do Trino (ex: kudu)")
    p.add_argument("--host", default="databushom01.sefaz.go.gov.br", help="Host do Trino")
    p.add_argument("--port", type=int, default=8443, help="Porta do Trino (ex: 8443)")
    p.add_argument("--user", default="", help="Usuário do Trino (ou env TRINO_USER)")
    p.add_argument("--password", default="", help="Senha do Trino (ou env TRINO_PASSWORD)")
    p.add_argument("--ssl", choices=["true","false"], default="true", help="Habilita SSL no JDBC (default: true)")
    p.add_argument("--ssl-verify", choices=["NONE","CA","FULL"], default="NONE", help="SSLVerification (default: NONE)")
    p.add_argument("--out-prefix", default="/user/gustavo.pasilva/samples_trino", help="Pasta HDFS para CSVs")
    return p.parse_args(argv)

def jdbc_url(host, port, ssl, ssl_verify):
    # URL sem catálogo — usaremos nomes totalmente qualificados nas queries
    base = f"jdbc:trino://{host}:{port}"
    params = []
    if ssl == "true":
        params.append("SSL=true")
        params.append(f"SSLVerification={ssl_verify}")
    return base if not params else base + "?" + "&".join(params)

def jdbc_common_options(user, password):
    opts = {"driver": "io.trino.jdbc.TrinoDriver"}
    if user: opts["user"] = user
    if password: opts["password"] = password
    return opts

def list_tables_trino(spark, url, user, password, catalog, schema):
    print("-"*120)
    print(f"[INFO] Listando tabelas: {catalog}.{schema}")
    print("-"*120)

    dbtable = (
        "(SELECT table_name "
        " FROM system.jdbc.tables "
        f" WHERE table_cat = '{catalog}' AND table_schem = '{schema}') t"
    )
    try:
        reader = (spark.read.format("jdbc")
                  .option("url", url)
                  .option("dbtable", dbtable))
        for k,v in jdbc_common_options(user, password).items():
            reader = reader.option(k, v)
        df = reader.load()
        rows = df.collect()
        tables = [r["table_name"] for r in rows if r["table_name"]]
        for t in tables: print(f"  - {t}")
        if not tables:
            print(f"[WARN] Nenhuma tabela encontrada em {catalog}.{schema}")
        return tables
    except Exception as e:
        print(f"[ERROR] Falha ao listar tabelas de {catalog}.{schema}: {e}")
        traceback.print_exc(limit=1, file=sys.stdout)
        return []

def show_and_sample(df: DataFrame, limit: int, out_path: str):
    print("[INFO] Schema:")
    df.printSchema()
    print(f"[INFO] Amostra de até {limit} linhas:")
    df.show(limit, truncate=False)
    try:
        (df.limit(limit)
           .coalesce(1)
           .write.mode("overwrite")
           .option("header", "true")
           .csv(out_path))
        print(f"[INFO] Amostra salva em: {out_path}")
    except Exception as e:
        print(f"[ERROR] Falha ao salvar CSV em {out_path}: {e}")

def inspect_table_trino(spark, url, user, password, catalog, schema, table, limit, out_prefix):
    print("="*120)
    print(f"SCHEMA: {schema} | TABELA: {table}")
    print("="*120)

    fq = f"{catalog}.{schema}.{table}"  # totalmente qualificado
    # Usamos subselect para garantir LIMIT no lado do Trino
    dbtable = f"(SELECT * FROM {fq} LIMIT {limit}) t"
    out_path = f"{out_prefix}/{catalog}.{schema}.{table}.csv"

    try:
        print(f"[INFO] Lendo via Trino JDBC: {fq}")
        reader = (spark.read.format("jdbc")
                  .option("url", url)
                  .option("dbtable", dbtable))
        for k,v in jdbc_common_options(user, password).items():
            reader = reader.option(k, v)
        df = reader.load()
        show_and_sample(df, limit, out_path)
    except AnalysisException as ae:
        print(f"[ERROR] AnalysisException lendo {fq}: {ae}")
        traceback.print_exc(limit=1, file=sys.stdout)
    except Exception as e:
        print(f"[ERROR] Erro lendo {fq}: {e}")
        traceback.print_exc(limit=1, file=sys.stdout)

def main(argv=None):
    a = parse_args(argv)
    schemas = [s.strip() for s in a.databases.split(",") if s.strip()]
    filter_tables = [t.strip() for t in a.tables.split(",") if t.strip()]
    user = a.user or os.environ.get("TRINO_USER", "")
    password = a.password or os.environ.get("TRINO_PASSWORD", "")

    url = jdbc_url(a.host, a.port, a.ssl, a.ssl_verify)

    spark = (SparkSession.builder
             .appName("VALIDADOR_ESQUEMAS_TRINO")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    print("="*120)
    print("INICIANDO VALIDADOR (Spark + Trino)")
    print(f"Catalog: {a.catalog}  Host: {a.host}:{a.port}")
    print(f"Schemas: {', '.join(schemas)}")
    if filter_tables:
        print(f"Filtrando tabelas: {', '.join(filter_tables)}")
    print(f"Limit: {a.limit}")
    print(f"CSV output prefix (HDFS): {a.out_prefix}")
    print("="*120)

    # Tenta inicializar HDFS (opcional)
    try:
        spark._jsc.hadoopConfiguration().set("fs.defaultFS", "hdfs://tdphom")
        spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark.sparkContext._jsc.hadoopConfiguration()
        )
    except Exception:
        pass

    for schema in schemas:
        print("#"*120)
        print(f"PROCESSANDO SCHEMA: {schema}")
        print("#"*120)
        tables = list_tables_trino(spark, url, user, password, a.catalog, schema)
        if filter_tables:
            tables = [t for t in tables if t in filter_tables]
            print(f"[INFO] Aplicando filtro, permanecem: {tables}")

        for t in tables:
            inspect_table_trino(spark, url, user, password, a.catalog, schema, t, a.limit, a.out_prefix)

    print("="*120)
    print("FINALIZADO")
    print("="*120)
    spark.stop()

if __name__ == "__main__":
    main()
