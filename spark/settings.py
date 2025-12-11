# -*- coding: utf-8 -*-
# app/settings.py

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Optional, Union

from pyspark.sql import SparkSession


# =============================================================================
# Dataclasses de configuração
# =============================================================================

@dataclass
class OracleConfig:
    dsn: str
    user: str
    password: str
    fetchsize: int = 20000
    partition_column: Optional[str] = None
    num_partitions: int = 8
    lower_bound: Optional[str] = None
    upper_bound: Optional[str] = None

    # Tabelas (Django / referência)
    tbl_documento: str = "IPM_DOCUMENTO_PARTCT_CALC_IPM"
    tbl_parametros: str = "IPM_PARAMETRO_IPM"
    tbl_cfop_participante: str = "IPM_CFOP_PARTICIPANTE"
    tbl_evento: str = "IPM_CONTRIBUINTE_CCEE"
    tbl_municipios: str = "IPM_MUNICIPIO_SIAF"
    tbl_municipios_map: str = "IPM_MUNICIPIOS"  # mapeamento municípios
    tbl_ncm_participante: str = "IPM_NCM_PRODUTOR_RURAL"


@dataclass
class KuduConfig:
    """
    IMPORTANTE:
    - Os nomes abaixo já estão QUALIFICADOS com o schema correto no Kudu:
      nfe.*, cte.*, nf3e.*, bpe.*, efd.*
    - No reader, use 'impala::<schema>.<tabela>'.
    """
    masters: str
    database: str

    # ------------ NFe (schema: nfe) ------------
    nfe_ident_table: str = "nfe.nfe_b_identificacao_nota_fiscal_eletronica"
    nfe_item_table: str = "nfe.nfe_i_produtos_servicos_nfe"
    nfe_emit_table: str = "nfe.nfe_c_identificacao_emitente_nota_fiscal_eletronica"
    nfe_dest_table: str = "nfe.nfe_e_identificacao_destinatario_nota_fiscal_eletronica"
    nfe_total_table: str = "nfe.nfe_w_total_nfe"
    nfe_icms_table: str = "nfe.nfe_grupo_tributacao_icms"
    nfe_ipi_item_table: str = "nfe.nfe_o_imposto_sobre_produtos_industrializados"
    nfe_ii_item_table: str = "nfe.nfe_p_imposto_importacao"
    nfe_comb_table: str = "nfe.nfe_la_detalhamento_especifico_combustiveis"

    # ------------ CTe (schema: cte) ------------
    cte_infcte_table: str = "cte.cte_infcte"
    cte_infcteos_vprest_comp_table: str = "cte.cte_infcteos_vprest_comp"
    cte_infdoc_infnf_table: str = "cte.cte_infcte_infctenorm_infdoc_infnf"
    cte_evento_table: str = "cte.cte_evento"
    cte_evento_evccecte_infcorrecao_table: str = "cte.cte_evento_evccecte_infcorrecao"
    cte_xml_info_table: str = "cte.cte_xml_info"
    cte_infcte_autxml_table: str = "cte.cte_infcte_autxml"
    cte_infcte_vprest_comp_table: str = "cte.cte_infcte_vprest_comp"

    # ------------ BPe (schema: bpe) ------------
    bpe_ident_table: str = "bpe.bpe_b_identificacao"

    # ------------ EFD (schema: efd) ------------
    efd_c100_table: str = "efd.efd_c100"
    efd_c170_table: str = "efd.efd_c170"
    efd_e100_table: str = "efd.efd_e100"
    efd_e110_table: str = "efd.efd_e110"

    # ------------ NF3e (schema: nf3e) ------------
    nf3e_infnf3e_table: str = "nf3e.nf3e_infnf3e"
    nf3e_nfdet_det_table: str = "nf3e.nf3e_nfdet_det"
    nf3e_infevento_table: str = "nf3e.nf3e_infevento"
    nf3e_xml_info_table: str = "nf3e.nf3e_xml_info"
    nf3e_detitem_gadband_table: str = "nf3e.nf3e_nfdet_det_detitem_gadband"
    nf3e_detitem_gprocref_gproc_table: str = "nf3e.nf3e_nfdet_det_detitem_gprocref_gproc"
    nf3e_detitem_gtarif_table: str = "nf3e.nf3e_nfdet_det_detitem_gtarif"
    nf3e_gscee_gconsumidor_table: str = "nf3e.nf3e_gscee_gconsumidor"
    nf3e_gscee_gsaldocred_table: str = "nf3e.nf3e_gscee_gsaldocred"
    nf3e_gscee_gtiposaldo_gsaldocred_table: str = "nf3e.nf3e_gscee_gtiposaldo_gsaldocred"
    nf3e_ganeel_table: str = "nf3e.nf3e_ganeel"
    nf3e_ggrcontrat_table: str = "nf3e.nf3e_ggrcontrat"
    nf3e_gmed_table: str = "nf3e.nf3e_gmed"


@dataclass
class IcebergConfig:
    # Catálogo lógico usado nas queries: <catalog>.<namespace>.<tabela>
    catalog: str = "tdp_catalog"

    # Implementação do catálogo: hive | rest | hadoop
    impl: str = "hive"

    # Para impl=hive: metastore URI (ex.: thrift://metastore:9083)
    hive_uri: str = ""

    # Para impl=rest: endpoint REST (ex.: http://iceberg-rest:8181)
    rest_uri: str = ""

    # Para impl=hadoop|rest (opcional): caminho do warehouse
    warehouse: str = ""

    # Namespace lógico (esquema)
    namespace: str = "ipm"

    # ---- Campos genéricos (usados diretamente pelo código, ex.: nfe.py) ----
    tbl_documento_partct: str = "ipm_documento_partct_calc_ipm"
    tbl_item_documento: str = "ipm_item_documento"

    # Tabelas específicas por documento
    # NFe
    tbl_nfe_documento_partct: str = "ipm_documento_partct_calc_ipm"
    tbl_nfe_item_documento: str = "ipm_item_documento"
    # CTe
    tbl_cte_documento_partct: str = "ipm_documento_partct_calc_ipm"
    tbl_cte_item_documento: str = "ipm_item_documento"
    # EFD
    tbl_efd_documento_partct: str = "ipm_documento_partct_calc_ipm"
    tbl_efd_item_documento: str = "ipm_item_documento"
    # BPe
    tbl_bpe_documento_partct: str = "ipm_documento_partct_calc_ipm"
    tbl_bpe_item_documento: str = "ipm_item_documento"
    # NF3e
    tbl_nf3e_documento_partct: str = "ipm_documento_partct_calc_ipm"
    tbl_nf3e_item_documento: str = "ipm_item_documento"

    # Auditoria
    tbl_audit: str = "ipm_auditoria_processamento"

    # --------- Helpers Fully Qualified ---------
    def fq(self, table: str) -> str:
        return f"{self.catalog}.{self.namespace}.{table}"

    # NFe
    def nfe_document_table(self) -> str:
        return self.fq(self.tbl_nfe_documento_partct)
    def nfe_item_table(self) -> str:
        return self.fq(self.tbl_nfe_item_documento)

    # CTe
    def cte_document_table(self) -> str:
        return self.fq(self.tbl_cte_documento_partct)
    def cte_item_table(self) -> str:
        return self.fq(self.tbl_cte_item_documento)

    # EFD
    def efd_document_table(self) -> str:
        return self.fq(self.tbl_efd_documento_partct)
    def efd_item_table(self) -> str:
        return self.fq(self.tbl_efd_item_documento)

    # BPe
    def bpe_document_table(self) -> str:
        return self.fq(self.tbl_bpe_documento_partct)
    def bpe_item_table(self) -> str:
        return self.fq(self.tbl_bpe_item_documento)

    # NF3e
    def nf3e_document_table(self) -> str:
        return self.fq(self.tbl_nf3e_documento_partct)
    def nf3e_item_table(self) -> str:
        return self.fq(self.tbl_nf3e_item_documento)

    # Auditoria
    def audit_table(self) -> str:
        return self.fq(self.tbl_audit)

    # Defaults genéricos (backward-compat)
    def document_table(self) -> str:
        return self.fq(self.tbl_documento_partct)
    def item_table(self) -> str:
        return self.fq(self.tbl_item_documento)


@dataclass
class RuntimeConfig:
    app_name: str = "Projeto_Django_to_Spark"
    timezone: str = "America/Sao_Paulo"
    # Recursos: por padrão NÃO setamos aqui para não sobrescrever o spark-submit.
    # Ative via IPM_RUNTIME_FORCE_RESOURCES=true se quiser forçar por código.
    driver_memory: str = "2g"
    executor_memory: str = "4g"
    executor_cores: int = 2
    num_executors: int = 4
    ssl_enabled: bool = False
    force_resources: bool = False  # controla se os recursos serão aplicados via código


@dataclass
class PartitioningConfig:
    documento_partition_by: str = "ano,mes"
    item_partition_by: str = "ano,mes"
    mode_documento: str = "append"   # append/overwrite/merge (se o writer suportar)
    mode_item: str = "append"        # append/overwrite/merge


@dataclass
class Settings:
    oracle: OracleConfig
    kudu: KuduConfig
    iceberg: IcebergConfig
    runtime: RuntimeConfig
    partitioning: PartitioningConfig


# =============================================================================
# Helpers (env/DSN)
# =============================================================================

def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name, "")
    if not v:
        return default
    return str(v).strip().lower() in {"1", "true", "t", "yes", "y", "sim", "s"}


def _normalize_oracle_jdbc_url(dsn: str) -> str:
    """
    Normaliza um DSN para a forma aceita pelo Oracle JDBC.
    Aceita:
      - 'jdbc:oracle:thin:@(DESCRIPTION=...)'   (já OK)
      - '(DESCRIPTION=...)'                     (prefixa)
      - '//host:1521/service'                   (prefixa)
      - 'host:1521/service'                     (prefixa com @//)
      - 'host:1521:SID'                         (prefixa com @)
    """
    if not dsn:
        raise ValueError("DSN Oracle vazio.")
    s = dsn.strip()

    # já é JDBC completo?
    if s.lower().startswith("jdbc:oracle:"):
        return s

    # TNS descriptor
    if s.startswith("("):
        return "jdbc:oracle:thin:@" + s

    # EZCONNECT com //
    if s.startswith("//"):
        return "jdbc:oracle:thin:@" + s

    # host:port/service
    if re.match(r"^[\w\.-]+:\d+/.+$", s):
        return "jdbc:oracle:thin:@//" + s

    # host:port:SID
    if re.match(r"^[\w\.-]+:\d+:[\w$]+$", s):
        return "jdbc:oracle:thin:@" + s

    raise ValueError(f"DSN Oracle inválido: {dsn!r}")


def _build_scan_jdbc_url(
    hosts_csv: str,
    service: str,
    port: int = 1521,
    load_balance: str = "OFF",
    failover: str = "ON",
) -> str:
    """
    Gera 'jdbc:oracle:thin:@(DESCRIPTION=...)' usando múltiplos SCANs.
    Não usa SERVER=SHARED para evitar 12514 quando não há handler shared.
    """
    hosts = [h.strip() for h in hosts_csv.split(",") if h.strip()]
    if not hosts:
        raise ValueError("IPM_ORACLE_SCAN_HOSTS vazio/inválido.")
    addresses = "".join(
        f"(ADDRESS=(PROTOCOL=TCP)(HOST={h})(PORT={port}))" for h in hosts
    )
    desc = (
        f"(DESCRIPTION=(LOAD_BALANCE={load_balance})(FAILOVER={failover})"
        f"{addresses}(CONNECT_DATA=(SERVICE_NAME={service})))"
    )
    return "jdbc:oracle:thin:@" + desc


# =============================================================================
# Loader principal de Settings
# =============================================================================

def load_settings_from_env() -> Settings:
    """Carrega configurações do ambiente com defaults alinhados ao seu cluster."""

    # ---------- ORACLE (IPM) ----------
    oracle_service = os.getenv("IPM_ORACLE_SERVICE", "prodadg").strip()
    scan_hosts = os.getenv(
        "IPM_ORACLE_SCAN_HOSTS",
        "exa03vm01-scan.intra.goias.gov.br,exa04-scan01.intra.goias.gov.br",
    )

    dsn_env = os.getenv("IPM_ORACLE_DSN", "").strip()
    if dsn_env:
        dsn = _normalize_oracle_jdbc_url(dsn_env)
    else:
        dsn = _build_scan_jdbc_url(scan_hosts, oracle_service)

    oracle = OracleConfig(
        dsn=dsn,
        user=os.getenv("IPM_ORACLE_USER", "ipmuser_spark_dg"),
        password=os.getenv("IPM_ORACLE_PASSWORD", "D1G3i#K0yZt"),
        fetchsize=int(os.getenv("IPM_ORACLE_FETCHSIZE", "20000")),
        partition_column=os.getenv("IPM_ORACLE_PARTITION_COLUMN") or None,
        num_partitions=int(os.getenv("IPM_ORACLE_NUM_PARTITIONS", "8")),
        lower_bound=os.getenv("IPM_ORACLE_LOWER_BOUND") or None,
        upper_bound=os.getenv("IPM_ORACLE_UPPER_BOUND") or None,
        tbl_documento=os.getenv("IPM_ORACLE_TBL_DOCUMENTO", "IPM_DOCUMENTO_PARTCT_CALC_IPM"),
        tbl_parametros=os.getenv("IPM_ORACLE_TBL_PARAMETROS", "IPM_PARAMETRO_IPM"),
        tbl_cfop_participante=os.getenv("IPM_ORACLE_TBL_CFOP_PARTICIPANTE", "IPM_CFOP_PARTICIPANTE"),
        tbl_evento=os.getenv("IPM_ORACLE_TBL_EVENTO", "IPM_CONTRIBUINTE_CCEE"),
        tbl_municipios=os.getenv("IPM_ORACLE_TBL_MUNICIPIOS", "IPM_MUNICIPIO_SIAF"),
        tbl_municipios_map=os.getenv("IPM_ORACLE_TBL_MUNICIPIOS_MAP", "IPM_MUNICIPIOS"),
        tbl_ncm_participante=os.getenv("IPM_ORACLE_TBL_NCM_PARTICIPANTE", "IPM_NCM_PRODUTOR_RURAL"),
    )

    # ---------- KUDU ----------
    kudu = KuduConfig(
        masters=os.getenv(
            "IPM_KUDU_MASTERS",
            "bdades01-node01.sefaz.go.gov.br:7051,bdades01-node02.sefaz.go.gov.br:7051,bdades01-node03.sefaz.go.gov.br:7051",
        ),
        database=os.getenv("IPM_KUDU_DATABASE", "nfe"),

        # NFe (schema: nfe)
        nfe_ident_table=os.getenv("IPM_KUDU_NFE_IDENT_TABLE", "nfe.nfe_b_identificacao_nota_fiscal_eletronica"),
        nfe_item_table=os.getenv("IPM_KUDU_NFE_ITEM_TABLE", "nfe.nfe_i_produtos_servicos_nfe"),
        nfe_emit_table=os.getenv("IPM_KUDU_NFE_EMIT_TABLE", "nfe.nfe_c_identificacao_emitente_nota_fiscal_eletronica"),
        nfe_dest_table=os.getenv("IPM_KUDU_NFE_DEST_TABLE", "nfe.nfe_e_identificacao_destinatario_nota_fiscal_eletronica"),
        nfe_total_table=os.getenv("IPM_KUDU_NFE_TOTAL_TABLE", "nfe.nfe_w_total_nfe"),
        nfe_icms_table=os.getenv("IPM_KUDU_NFE_ICMS_TABLE", "nfe.nfe_grupo_tributacao_icms"),
        nfe_ipi_item_table=os.getenv("IPM_KUDU_NFE_IPI_ITEM_TABLE", "nfe.nfe_o_imposto_sobre_produtos_industrializados"),
        nfe_ii_item_table=os.getenv("IPM_KUDU_NFE_II_ITEM_TABLE", "nfe.nfe_p_imposto_importacao"),
        nfe_comb_table=os.getenv("IPM_KUDU_NFE_COMB_TABLE", "nfe.nfe_la_detalhamento_especifico_combustiveis"),

        # CTe (schema: cte)
        cte_infcte_table=os.getenv("IPM_KUDU_CTE_INFCTE_TABLE", "cte.cte_infcte"),
        cte_infcteos_vprest_comp_table=os.getenv("IPM_KUDU_CTE_INFCTEOS_VPREST_COMP_TABLE", "cte.cte_infcteos_vprest_comp"),
        cte_infdoc_infnf_table=os.getenv("IPM_KUDU_CTE_INFDOC_INFNF_TABLE", "cte.cte_infcte_infctenorm_infdoc_infnf"),
        cte_evento_table=os.getenv("IPM_KUDU_CTE_EVENTO_TABLE", "cte.cte_evento"),
        cte_evento_evccecte_infcorrecao_table=os.getenv("IPM_KUDU_CTE_EVENTO_EVCCECTE_INFCORRECAO_TABLE", "cte.cte_evento_evccecte_infcorrecao"),
        cte_xml_info_table=os.getenv("IPM_KUDU_CTE_XML_INFO_TABLE", "cte.cte_xml_info"),
        cte_infcte_autxml_table=os.getenv("IPM_KUDU_CTE_INFCTE_AUTXML_TABLE", "cte.cte_infcte_autxml"),
        cte_infcte_vprest_comp_table=os.getenv("IPM_KUDU_CTE_INFCTE_VPREST_COMP_TABLE", "cte.cte_infcte_vprest_comp"),

        # BPe (schema: bpe)
        bpe_ident_table=os.getenv("IPM_KUDU_BPE_IDENT_TABLE", "bpe.bpe_b_identificacao"),

        # EFD (schema: efd)
        efd_c100_table=os.getenv("IPM_KUDU_EFD_C100_TABLE", "efd.efd_c100"),
        efd_c170_table=os.getenv("IPM_KUDU_EFD_C170_TABLE", "efd.efd_c170"),
        efd_e100_table=os.getenv("IPM_KUDU_EFD_E100_TABLE", "efd.efd_e100"),
        efd_e110_table=os.getenv("IPM_KUDU_EFD_E110_TABLE", "efd.efd_e110"),

        # NF3e (schema: nf3e)
        nf3e_infnf3e_table=os.getenv("IPM_KUDU_NF3E_INFNF3E_TABLE", "nf3e.nf3e_infnf3e"),
        nf3e_nfdet_det_table=os.getenv("IPM_KUDU_NF3E_NFDET_DET_TABLE", "nf3e.nf3e_nfdet_det"),
        nf3e_infevento_table=os.getenv("IPM_KUDU_NF3E_INFEVENTO_TABLE", "nf3e.nf3e_infevento"),
        nf3e_xml_info_table=os.getenv("IPM_KUDU_NF3E_XML_INFO_TABLE", "nf3e.nf3e_xml_info"),
        nf3e_detitem_gadband_table=os.getenv("IPM_KUDU_NF3E_DETITEM_GADBAND_TABLE", "nf3e.nf3e_nfdet_det_detitem_gadband"),
        nf3e_detitem_gprocref_gproc_table=os.getenv("IPM_KUDU_NF3E_DETITEM_GPROCREF_GPROC_TABLE", "nf3e.nf3e_nfdet_det_detitem_gprocref_gproc"),
        nf3e_detitem_gtarif_table=os.getenv("IPM_KUDU_NF3E_DETITEM_GTARIF_TABLE", "nf3e.nf3e_nfdet_det_detitem_gtarif"),
        nf3e_gscee_gconsumidor_table=os.getenv("IPM_KUDU_NF3E_GSCEE_GCONSUMIDOR_TABLE", "nf3e.nf3e_gscee_gconsumidor"),
        nf3e_gscee_gsaldocred_table=os.getenv("IPM_KUDU_NF3E_GSCEE_GSALDOCRED_TABLE", "nf3e.nf3e_gscee_gsaldocred"),
        nf3e_gscee_gtiposaldo_gsaldocred_table=os.getenv("IPM_KUDU_NF3E_GSCEE_GTIPOSALDO_GSALDOCRED_TABLE", "nf3e.nf3e_gscee_gtiposaldo_gsaldocred"),
        nf3e_ganeel_table=os.getenv("IPM_KUDU_NF3E_GANEEL_TABLE", "nf3e.nf3e_ganeel"),
        nf3e_ggrcontrat_table=os.getenv("IPM_KUDU_NF3E_GGRCONTRAT_TABLE", "nf3e.nf3e_ggrcontrat"),
        nf3e_gmed_table=os.getenv("IPM_KUDU_NF3E_GMED_TABLE", "nf3e.nf3e_gmed"),
    )

    # ---------- ICEBERG ----------
    iceberg = IcebergConfig(
        catalog=os.getenv("IPM_ICEBERG_CATALOG", "tdp_catalog"),
        impl=os.getenv("IPM_ICEBERG_IMPL", "hive").lower(),
        hive_uri=os.getenv("IPM_ICEBERG_HIVE_URI", ""),
        rest_uri=os.getenv("IPM_ICEBERG_REST_URI", ""),
        warehouse=os.getenv("IPM_ICEBERG_WAREHOUSE", ""),
        namespace=os.getenv("IPM_ICEBERG_NAMESPACE", "ipm"),

        # genéricos
        tbl_documento_partct=os.getenv("IPM_ICEBERG_TBL_DOCUMENTO_PARTCT", "ipm_documento_partct_calc_ipm"),
        tbl_item_documento=os.getenv("IPM_ICEBERG_TBL_ITEM_DOCUMENTO", "ipm_item_documento"),

        # NFe
        tbl_nfe_documento_partct=os.getenv("IPM_ICEBERG_TBL_NFE_DOCUMENTO_PARTCT", "ipm_documento_partct_calc_ipm"),
        tbl_nfe_item_documento=os.getenv("IPM_ICEBERG_TBL_NFE_ITEM_DOCUMENTO", "ipm_item_documento"),

        # CTe
        tbl_cte_documento_partct=os.getenv("IPM_ICEBERG_TBL_CTE_DOCUMENTO_PARTCT", "ipm_documento_partct_calc_ipm"),
        tbl_cte_item_documento=os.getenv("IPM_ICEBERG_TBL_CTE_ITEM_DOCUMENTO", "ipm_item_documento"),

        # EFD
        tbl_efd_documento_partct=os.getenv("IPM_ICEBERG_TBL_EFD_DOCUMENTO_PARTCT", "ipm_documento_partct_calc_ipm"),
        tbl_efd_item_documento=os.getenv("IPM_ICEBERG_TBL_EFD_ITEM_DOCUMENTO", "ipm_item_documento"),

        # BPe
        tbl_bpe_documento_partct=os.getenv("IPM_ICEBERG_TBL_BPE_DOCUMENTO_PARTCT", "ipm_documento_partct_calc_ipm"),
        tbl_bpe_item_documento=os.getenv("IPM_ICEBERG_TBL_BPE_ITEM_DOCUMENTO", "ipm_item_documento"),

        # NF3e
        tbl_nf3e_documento_partct=os.getenv("IPM_ICEBERG_TBL_NF3E_DOCUMENTO_PARTCT", "ipm_documento_partct_calc_ipm"),
        tbl_nf3e_item_documento=os.getenv("IPM_ICEBERG_TBL_NF3E_ITEM_DOCUMENTO", "ipm_item_documento"),

        # Auditoria
        tbl_audit=os.getenv("IPM_ICEBERG_TBL_AUDIT", "ipm_auditoria_processamento"),
    )

    # ---------- RUNTIME ----------
    runtime = RuntimeConfig(
        app_name=os.getenv("IPM_RUNTIME_APP_NAME", "Projeto_Django_to_Spark"),
        timezone=os.getenv("IPM_RUNTIME_TIMEZONE", "America/Sao_Paulo"),
        driver_memory=os.getenv("IPM_RUNTIME_DRIVER_MEMORY", "2g"),
        executor_memory=os.getenv("IPM_RUNTIME_EXECUTOR_MEMORY", "4g"),
        executor_cores=int(os.getenv("IPM_RUNTIME_EXECUTOR_CORES", "2")),
        num_executors=int(os.getenv("IPM_RUNTIME_NUM_EXECUTORS", "4")),
        ssl_enabled=_env_bool("IPM_RUNTIME_SSL_ENABLED", False),
        force_resources=_env_bool("IPM_RUNTIME_FORCE_RESOURCES", False),
    )

    partitioning = PartitioningConfig(
        documento_partition_by=os.getenv("IPM_PART_DOCUMENTO_PARTITION_BY", "ano,mes"),
        item_partition_by=os.getenv("IPM_PART_ITEM_PARTITION_BY", "ano,mes"),
        mode_documento=os.getenv("IPM_PART_MODE_DOCUMENTO", "append"),
        mode_item=os.getenv("IPM_PART_MODE_ITEM", "append"),
    )

    return Settings(
        oracle=oracle,
        kudu=kudu,
        iceberg=iceberg,
        runtime=runtime,
        partitioning=partitioning,
    )


def load_settings() -> Settings:
    """Alias usado pelos módulos."""
    return load_settings_from_env()


# =============================================================================
# Spark
# =============================================================================

def _quiet_kudu_logs(spark: SparkSession) -> None:
    """Reduz verbosidade do conector Kudu (erros de GetTableStatistics em masters antigos)."""
    try:
        jvm = spark._jsc.jvm
        LogManager = jvm.org.apache.log4j.LogManager
        Level = jvm.org.apache.log4j.Level
        LogManager.getLogger("org.apache.kudu.client.Connection").setLevel(Level.WARN)
        LogManager.getLogger("org.apache.kudu.spark.kudu.KuduRelation").setLevel(Level.WARN)
        LogManager.getLogger("org.apache.kudu").setLevel(Level.ERROR)
        LogManager.getLogger("org.apache.kudu.client").setLevel(Level.ERROR)
        LogManager.getLogger("org.apache.kudu.client.Connection").setLevel(Level.ERROR)
        LogManager.getLogger("org.apache.kudu.spark.kudu.KuduRelation").setLevel(Level.ERROR)
    except Exception:
        pass


def build_spark(arg1: Union[Settings, str], arg2: Optional[Settings] = None) -> SparkSession:
    """
    Cria uma SparkSession com Iceberg habilitado e timezone definido.

    Assinaturas aceitas (retrocompat):
      - build_spark(settings)
      - build_spark("Nome do App", settings)

    IMPORTANTE:
    - Não define recursos (memória/cores/instances) por padrão para NÃO sobrescrever
      o que vier do spark-submit.
    - Para forçar recursos pelo código, exporte IPM_RUNTIME_FORCE_RESOURCES=true.
    """
    if isinstance(arg1, Settings):
        settings = arg1
        app_name = settings.runtime.app_name
    else:
        app_name = str(arg1)
        settings = arg2  # type: ignore
        if settings is None:
            raise ValueError("Quando usar build_spark('Nome'), forneça também o Settings.")

    builder = (
        SparkSession.builder
        .appName(app_name)
        # Iceberg
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", settings.iceberg.impl)
        .config(f"spark.sql.catalog.{settings.iceberg.catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{settings.iceberg.catalog}.type", settings.iceberg.impl)
        # Timezone
        .config("spark.sql.session.timeZone", settings.runtime.timezone)
        # Kudu via DataSource V1
        .config("spark.sql.sources.useV1SourceList", "kudu")
    )

    impl = settings.iceberg.impl
    cat = settings.iceberg.catalog

    if impl == "hive":
        if settings.iceberg.hive_uri:
            builder = builder.config(f"spark.sql.catalog.{cat}.uri", settings.iceberg.hive_uri)
        if settings.iceberg.warehouse:
            builder = builder.config(f"spark.sql.catalog.{cat}.warehouse", settings.iceberg.warehouse)

    elif impl == "rest":
        if not settings.iceberg.rest_uri:
            raise ValueError("IPM_ICEBERG_IMPL=rest mas IPM_ICEBERG_REST_URI não foi definido.")
        builder = builder.config(f"spark.sql.catalog.{cat}.uri", settings.iceberg.rest_uri)
        if settings.iceberg.warehouse:
            builder = builder.config(f"spark.sql.catalog.{cat}.warehouse", settings.iceberg.warehouse)

    elif impl == "hadoop":
        if not settings.iceberg.warehouse:
            raise ValueError("IPM_ICEBERG_IMPL=hadoop requer IPM_ICEBERG_WAREHOUSE.")
        builder = builder.config(f"spark.sql.catalog.{cat}.warehouse", settings.iceberg.warehouse)

    if settings.runtime.force_resources:
        builder = (
            builder
            .config("spark.driver.memory", settings.runtime.driver_memory)
            .config("spark.executor.memory", settings.runtime.executor_memory)
            .config("spark.executor.cores", settings.runtime.executor_cores)
            .config("spark.executor.instances", settings.runtime.num_executors)
        )

    spark = builder.getOrCreate()
    _quiet_kudu_logs(spark)
    return spark


# =============================================================================
# Util: impressão amigável das configurações
# =============================================================================

def print_settings(settings: Settings) -> None:
    oracle_dict = {
        "dsn": settings.oracle.dsn,
        "user": settings.oracle.user,
        "password": "********" if settings.oracle.password else "",
        "fetchsize": settings.oracle.fetchsize,
        "partition_column": settings.oracle.partition_column,
        "num_partitions": settings.oracle.num_partitions,
        "lower_bound": settings.oracle.lower_bound,
        "upper_bound": settings.oracle.upper_bound,
        "tbl_documento": settings.oracle.tbl_documento,
        "tbl_parametros": settings.oracle.tbl_parametros,
        "tbl_cfop_participante": settings.oracle.tbl_cfop_participante,
        "tbl_evento": settings.oracle.tbl_evento,
        "tbl_municipios": settings.oracle.tbl_municipios,
        "tbl_municipios_map": settings.oracle.tbl_municipios_map,
        "tbl_ncm_participante": settings.oracle.tbl_ncm_participante,
    }

    kudu_dict = {
        "masters": settings.kudu.masters,
        "database": settings.kudu.database,

        # NFe
        "nfe_ident_table": settings.kudu.nfe_ident_table,
        "nfe_item_table": settings.kudu.nfe_item_table,
        "nfe_emit_table": settings.kudu.nfe_emit_table,
        "nfe_dest_table": settings.kudu.nfe_dest_table,
        "nfe_total_table": settings.kudu.nfe_total_table,
        "nfe_icms_table": settings.kudu.nfe_icms_table,
        "nfe_ipi_item_table": settings.kudu.nfe_ipi_item_table,
        "nfe_ii_item_table": settings.kudu.nfe_ii_item_table,
        "nfe_comb_table": settings.kudu.nfe_comb_table,

        # CTe
        "cte_infcte_table": settings.kudu.cte_infcte_table,
        "cte_infcteos_vprest_comp_table": settings.kudu.cte_infcteos_vprest_comp_table,
        "cte_infdoc_infnf_table": settings.kudu.cte_infdoc_infnf_table,
        "cte_evento_table": settings.kudu.cte_evento_table,
        "cte_evento_evccecte_infcorrecao_table": settings.kudu.cte_evento_evccecte_infcorrecao_table,
        "cte_xml_info_table": settings.kudu.cte_xml_info_table,
        "cte_infcte_autxml_table": settings.kudu.cte_infcte_autxml_table,
        "cte_infcte_vprest_comp_table": settings.kudu.cte_infcte_vprest_comp_table,

        # BPe
        "bpe_ident_table": settings.kudu.bpe_ident_table,

        # EFD
        "efd_c100_table": settings.kudu.efd_c100_table,
        "efd_c170_table": settings.kudu.efd_c170_table,
        "efd_e100_table": settings.kudu.efd_e100_table,
        "efd_e110_table": settings.kudu.efd_e110_table,

        # NF3e
        "nf3e_infnf3e_table": settings.kudu.nf3e_infnf3e_table,
        "nf3e_nfdet_det_table": settings.kudu.nf3e_nfdet_det_table,
        "nf3e_infevento_table": settings.kudu.nf3e_infevento_table,
        "nf3e_xml_info_table": settings.kudu.nf3e_xml_info_table,
        "nf3e_detitem_gadband_table": settings.kudu.nf3e_detitem_gadband_table,
        "nf3e_detitem_gprocref_gproc_table": settings.kudu.nf3e_detitem_gprocref_gproc_table,
        "nf3e_detitem_gtarif_table": settings.kudu.nf3e_detitem_gtarif_table,
        "nf3e_gscee_gconsumidor_table": settings.kudu.nf3e_gscee_gconsumidor_table,
        "nf3e_gscee_gsaldocred_table": settings.kudu.nf3e_gscee_gsaldocred_table,
        "nf3e_gscee_gtiposaldo_gsaldocred_table": settings.kudu.nf3e_gscee_gtiposaldo_gsaldocred_table,
        "nf3e_ganeel_table": settings.kudu.nf3e_ganeel_table,
        "nf3e_ggrcontrat_table": settings.kudu.nf3e_ggrcontrat_table,
        "nf3e_gmed_table": settings.kudu.nf3e_gmed_table,
    }

    iceberg_dict = {
        "catalog": settings.iceberg.catalog,
        "impl": settings.iceberg.impl,
        "hive_uri": settings.iceberg.hive_uri,
        "rest_uri": settings.iceberg.rest_uri,
        "warehouse": settings.iceberg.warehouse,
        "namespace": settings.iceberg.namespace,

        "tbl_documento_partct": settings.iceberg.tbl_documento_partct,
        "tbl_item_documento": settings.iceberg.tbl_item_documento,

        # NFe
        "tbl_nfe_documento_partct": settings.iceberg.tbl_nfe_documento_partct,
        "tbl_nfe_item_documento": settings.iceberg.tbl_nfe_item_documento,

        # CTe
        "tbl_cte_documento_partct": settings.iceberg.tbl_cte_documento_partct,
        "tbl_cte_item_documento": settings.iceberg.tbl_cte_item_documento,

        # EFD
        "tbl_efd_documento_partct": settings.iceberg.tbl_efd_documento_partct,
        "tbl_efd_item_documento": settings.iceberg.tbl_efd_item_documento,

        # BPe
        "tbl_bpe_documento_partct": settings.iceberg.tbl_bpe_documento_partct,
        "tbl_bpe_item_documento": settings.iceberg.tbl_bpe_item_documento,

        # NF3e
        "tbl_nf3e_documento_partct": settings.iceberg.tbl_nf3e_documento_partct,
        "tbl_nf3e_item_documento": settings.iceberg.tbl_nf3e_item_documento,

        "tbl_audit": settings.iceberg.tbl_audit,
    }

    runtime_dict = {
        "app_name": settings.runtime.app_name,
        "timezone": settings.runtime.timezone,
        "driver_memory": settings.runtime.driver_memory,
        "executor_memory": settings.runtime.executor_memory,
        "executor_cores": settings.runtime.executor_cores,
        "num_executors": settings.runtime.num_executors,
        "ssl_enabled": settings.runtime.ssl_enabled,
        "force_resources": settings.runtime.force_resources,
    }

    partitioning_dict = {
        "documento_partition_by": settings.partitioning.documento_partition_by,
        "item_partition_by": settings.partitioning.item_partition_by,
        "mode_documento": settings.partitioning.mode_documento,
        "mode_item": settings.partitioning.mode_item,
    }

    print("===== SETTINGS =====")
    print(f"[oracle] -> {oracle_dict}")
    print(f"[kudu]   -> {kudu_dict}")
    print(f"[iceberg]-> {iceberg_dict}")
    print(f"[runtime]-> {runtime_dict}")
    print(f"[part]   -> {partitioning_dict}")
    print("====================")
