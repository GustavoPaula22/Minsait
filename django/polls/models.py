from django.db import models

import os


class Calendario(models.Model):
    id_dia = models.AutoField(primary_key=True, db_column='ID_DIA')
    data_completa = models.DateTimeField()
    desc_bimestre = models.CharField(max_length=30)
    desc_bimestre_ano = models.CharField(max_length=30)
    desc_dia_semana_abrev = models.CharField(max_length=30)
    desc_mes = models.CharField(max_length=30)
    desc_mes_abreviado = models.CharField(max_length=30)
    desc_mes_ano = models.CharField(max_length=30)
    desc_quadm = models.CharField(max_length=30)
    desc_quadm_ano = models.CharField(max_length=30)
    desc_semestre = models.CharField(max_length=30)
    desc_semestre_ano = models.CharField(max_length=30)
    desc_trimestre = models.CharField(max_length=30)
    desc_trimestre_ano = models.CharField(max_length=30)
    id_quinz = models.DecimalField(max_digits=4, decimal_places=0)
    nome_dia_semana = models.CharField(max_length=15)
    numr_ano = models.DecimalField(max_digits=4, decimal_places=0)
    numr_ano_bimestre = models.CharField(max_length=5)
    numr_ano_mes = models.IntegerField(db_column='NUMR_ANO_MES')
    numr_ano_mes_dia = models.DecimalField(max_digits=8, decimal_places=0)
    numr_ano_semestre = models.CharField(max_length=5)
    numr_ano_trimestre = models.DecimalField(max_digits=5, decimal_places=0)
    numr_bimestre = models.CharField(max_length=1)
    numr_dia = models.DecimalField(max_digits=2, decimal_places=0)
    numr_mes = models.DecimalField(max_digits=2, decimal_places=0)
    numr_quinz = models.DecimalField(max_digits=2, decimal_places=0)
    numr_semst = models.DecimalField(max_digits=1, decimal_places=0)
    numr_trim = models.DecimalField(max_digits=1, decimal_places=0)

    class Meta:
        db_table = os.getenv('TABELA_DATAS')
        managed = False


class DMGEN004(models.Model):
    id_mes = models.AutoField(primary_key=True)
    id_trim = models.IntegerField(null=False)
    numr_mes = models.IntegerField(null=False)
    numr_trim = models.IntegerField(null=False)
    numr_semst = models.IntegerField(null=False)
    numr_ano = models.IntegerField(null=False)
    numr_ano_mes = models.IntegerField(null=False)
    numr_bim = models.IntegerField(null=False)
    nome_mes = models.CharField(max_length=10)
    numr_ano_bimestre = models.IntegerField(null=False)
    numr_ano_semestre = models.IntegerField(null=False)
    numr_ano_trimestre = models.IntegerField(null=False)
    desc_bimestre = models.CharField(max_length=30)
    desc_bimestre_ano = models.CharField(max_length=30)
    desc_mes_abreviado = models.CharField(max_length=30)
    desc_mes_ano = models.CharField(max_length=30)
    desc_quadm = models.CharField(max_length=30)
    desc_quadm_ano = models.CharField(max_length=30)
    desc_semestre = models.CharField(max_length=30)
    desc_semestre_ano = models.CharField(max_length=30)
    desc_trimestre = models.CharField(max_length=30)
    desc_trimestre_ano = models.CharField(max_length=30)

    class Meta:
        db_table = os.getenv('TABELA_DMGEN004_ORACLE')
        managed = False


class TipoProcessamento(models.Model):
    codg_tipo_procesm_indice = models.PositiveIntegerField(primary_key=True)
    desc_tipo_procesm_indice = models.CharField(max_length=100)

    class Meta:
        db_table = os.getenv('TABELA_TIPO_PROCESM_ORACLE')
        managed = True


class Processamento(models.Model):
    id_procesm_indice = models.AutoField(primary_key=True)
    data_inicio_procesm_indice = models.DateTimeField()
    data_fim_procesm_indice = models.DateTimeField()
    stat_procesm_indice = models.CharField(max_length=1)
    desc_observacao_procesm_indice = models.CharField(max_length=500)
    codg_tipo_procesm_indice = models.DecimalField(max_digits=4, decimal_places=0)
    data_inicio_periodo_indice = models.DateTimeField()
    data_fim_periodo_indice = models.DateTimeField()

    class Meta:
        db_table = os.getenv('TABELA_PROCESSAMENTO_ORACLE')
        managed = False


class IdentConv115(models.Model):
    id_conv_115 = models.AutoField(primary_key=True)
    numr_inscricao = models.BigIntegerField(null=False)
    numr_documento_fiscal = models.BigIntegerField(null=False)
    data_emissao_documento = models.DateTimeField(null=False)
    desc_serie_documento_fiscal = models.CharField(max_length=3)
    codg_situacao_versao_arquivo = models.CharField(max_length=3)

    class Meta:
        db_table = os.getenv('TABELA_ID_CONV115_ORACLE')
        managed = False


class IdentSimples(models.Model):
    id_simples = models.AutoField(primary_key=True)
    codg_declaracao_pgdas = models.BigIntegerField(null=False)
    numr_cnpj = models.BigIntegerField(null=False)

    class Meta:
        db_table = os.getenv('TABELA_ID_SIMPES_ORACLE')
        managed = False


class IdentMei(models.Model):
    id_mei = models.AutoField(primary_key=True)
    codg_declaracao_dasnsimei = models.BigIntegerField(null=False)
    numr_cnpj = models.BigIntegerField(null=False)

    class Meta:
        db_table = os.getenv('TABELA_ID_MEI_ORACLE')
        managed = False


class Monofasico(models.Model):
    id_item_nota_fiscal = models.BigIntegerField(primary_key=True)
    perc_diferiment = models.DecimalField(max_digits=7, decimal_places=4)
    perc_reducao_ad_rem = models.DecimalField(max_digits=5, decimal_places=4)
    qtde_bc_monofasico = models.DecimalField(max_digits=15, decimal_places=4)
    qtde_bc_monofasico_retencao = models.DecimalField(max_digits=15, decimal_places=4)
    valr_ad_rem_icms = models.DecimalField(max_digits=7, decimal_places=4)
    valr_ad_rem_icms_retencao = models.DecimalField(max_digits=7, decimal_places=4)
    valr_ad_rem_icms_retido = models.DecimalField(max_digits=7, decimal_places=4)
    valr_icms_monofasico = models.DecimalField(max_digits=15, decimal_places=2)
    valr_icms_monofasico_diferido = models.DecimalField(max_digits=15, decimal_places=2)
    valr_icms_monofasico_operacao = models.DecimalField(max_digits=15, decimal_places=2)
    valr_icms_monofasico_retencao = models.DecimalField(max_digits=15, decimal_places=2)
    valr_icms_monofasico_retido = models.DecimalField(max_digits=15, decimal_places=2)

    class Meta:
        db_table = os.getenv('TABELA_ICMS_MONOFASICO_ORACLE')
        managed = False


class Municipio(models.Model):
    codg_municipio = models.AutoField(primary_key=True)
    codg_correio = models.BigIntegerField(null=False)
    data_criacao = models.DateTimeField()
    codg_serpro = models.BigIntegerField(null=False)
    numr_lei_criacao = models.BigIntegerField(null=False)
    codg_cep_generico = models.BigIntegerField(null=False)
    codg_origem_informacao = models.IntegerField(null=False)
    nome_municipio = models.CharField(max_length=60)
    data_atualiza = models.DateTimeField()
    matr_func = models.BigIntegerField(null=False)
    codg_municipio_distrito = models.IntegerField(null=False)
    codg_uf = models.CharField(max_length=2)
    codg_municipio_comarca = models.IntegerField(null=False)
    codg_unidade_oper = models.IntegerField(null=False)
    numr_simpl_municipio = models.IntegerField(null=False)
    codg_ibge = models.BigIntegerField(null=False)
    tipo_localidade = models.CharField(max_length=1)
    numr_cnpj_prefeitura = models.CharField(max_length=14)
    codg_celg = models.IntegerField(null=False)
    indi_cep_especifico = models.CharField(max_length=1)

    class Meta:
        db_table = os.getenv('TABELA_MUNICIPIO_ORACLE')
        managed = False


class Parametro(models.Model):
    class Meta:
        db_table = os.getenv('TABELA_PARAMETRO_ORACLE')
        managed = False

    id_parametro_ipm = models.AutoField(primary_key=True)
    nome_parametro_ipm = models.CharField(max_length=100)
    desc_parametro_ipm = models.CharField(max_length=500)


class TipoDocumento(models.Model):
    class Meta:
        db_table = os.getenv('TABELA_TIPO_DOCUMENTO_ORACLE')
        managed = False

    codg_tipo_doc_partct_calc = models.PositiveIntegerField(primary_key=True)
    desc_tipo_doc_partct_calc = models.CharField(max_length=50)


class MotivoExclusao(models.Model):
    class Meta:
        db_table = os.getenv('TABELA_MOTIVO_EXCLUSAO_ORACLE')
        managed = False

    codg_motivo_exclusao_calculo = models.PositiveIntegerField(primary_key=True)
    desc_motivo_exclusao_calculo = models.CharField(max_length=50)
    tipo_exclusao_calculo = models.CharField(max_length=1)


class ItemDoc(models.Model):
    codg_documento_partct_calculo = models.BigIntegerField(null=False)
    codg_item_documento = models.BigIntegerField(null=False)
    codg_tipo_doc_partct_documento = models.IntegerField(null=False)
    id_produto_ncm = models.IntegerField(null=False)
    codg_cfop = models.IntegerField(null=False)
    codg_motivo_exclusao_calculo = models.IntegerField(null=False)
    id_procesm_indice = models.IntegerField(null=False)
    valr_adicionado = models.FloatField(null=True)

    class Meta:
        db_table = os.getenv('TABELA_ITEM_DOC')
        managed = False
        unique_together = ('codg_documento_partct_calculo', 'codg_tipo_doc_partct_documento', 'codg_item_documento')


class DocPartctCalculo(models.Model):
    codg_documento_partct_calculo = models.AutoField(primary_key=True)
    valr_adicionado_operacao = models.FloatField(null=True)
    data_emissao_documento = models.DateTimeField(null=True)
    codg_tipo_doc_partct_calc = models.IntegerField(null=False)
    numr_referencia = models.IntegerField(null=True)
    id_contrib_ipm_entrada = models.BigIntegerField(null=True)
    id_contrib_ipm_saida = models.BigIntegerField(null=True)
    id_procesm_indice = models.IntegerField(null=False)
    indi_aprop = models.CharField(max_length=1)
    codg_motivo_exclusao_calculo = models.IntegerField(null=False)
    codg_municipio_entrada = models.IntegerField(null=False)
    codg_municipio_saida = models.IntegerField(null=False)

    class Meta:
        db_table = os.getenv('TABELA_DOC_PARTCT')
        managed = False


class CadastroCCE(models.Model):
    numr_inscricao = models.IntegerField(primary_key=True)
    numr_referencia = models.IntegerField(null=False)
    indi_produtor_rural = models.CharField(max_length=1)
    stat_cadastro_contrib = models.CharField(max_length=1)
    tipo_enqdto_fiscal = models.CharField(max_length=1)
    codg_municipio = models.IntegerField(null=True)
    codg_uf_inscricao = models.CharField(max_length=2)

    class Meta:
        db_table = os.getenv('TABELA_HISTORICO_CCE_ORACLE')
        managed = False


class ContribIPM(models.Model):
    id_contrib_ipm = models.AutoField(primary_key=True)
    numr_inscricao_contrib = models.BigIntegerField(null=False)
    codg_municipio = models.IntegerField(null=True)
    codg_uf = models.CharField(max_length=2)
    data_inicio_vigencia = models.DateTimeField()
    data_fim_vigencia = models.DateTimeField()
    indi_produtor_rural = models.CharField(max_length=1)
    indi_produtor_rural_exclusivo = models.CharField(max_length=1)
    stat_cadastro_contrib = models.CharField(max_length=1)
    tipo_enqdto_fiscal = models.CharField(max_length=1)
    id_procesm_indice = models.IntegerField(null=False)

    class Meta:
        db_table = os.getenv('TABELA_CONTRIBUINTE_IPM_ORACLE')
        managed = False


class CFOP(models.Model):
    codg_cfop = models.IntegerField(primary_key=True)
    data_inicio_vigencia = models.DateTimeField()
    data_fim_vigencia = models.DateTimeField()
    tipo_operacao_documento = models.IntegerField(null=True)
    indi_local_destino_operacao = models.IntegerField(null=True)
    desc_natureza_operacao = models.CharField(max_length=250)
    codg_tipo_doc_partct_calc = models.IntegerField(null=False)

    class Meta:
        db_table = os.getenv('TABELA_CFOP_ORACLE')
        managed = False


class AtivCNAE(models.Model):
    id_subclasse_cnaef = models.IntegerField(primary_key=True)
    indi_ramo_atividade = models.IntegerField(null=True)

    class Meta:
        db_table = os.getenv('TABELA_RAMO_ATIV_CNAE_ORACLE')
        managed = False


class CCEE(models.Model):
    numr_inscricao = models.IntegerField(primary_key=True)
    data_inicio_vigencia_ccee = models.DateTimeField()
    data_fim_vigencia_ccee = models.DateTimeField(null=True)
    tipo_contrib = models.CharField(max_length=1, null=True)

    class Meta:
        db_table = os.getenv('TABELA_CCEE_ORACLE')
        managed = False


class CCE_CNAE(models.Model):
    id_subclasse_cnaef = models.IntegerField(primary_key=True)
    codg_subclasse_cnaef = models.CharField(max_length=7, null=True)
    desc_subclasse_cnaef = models.CharField(max_length=257, null=True)
    indi_estab_centralz = models.CharField(max_length=1, null=True)
    indi_adjunto = models.CharField(max_length=1, null=True)
    indi_vistoria = models.CharField(max_length=1, null=True)
    indi_icms = models.CharField(max_length=1, null=True)
    stat_subclasse_cnaef = models.CharField(max_length=1, null=True)
    id_classe_cnaef = models.IntegerField(null=True)
    codg_cae = models.IntegerField(null=True)
    indi_analise_especz = models.CharField(max_length=1, null=True)
    indi_setor_produtivo = models.IntegerField(null=True)
    indi_origem_cnaef = models.CharField(max_length=1, null=True)
    indi_desenvr_paf = models.CharField(max_length=1, null=True)
    data_inicio_nfe = models.DateTimeField(null=True)
    indi_estab_unificado = models.CharField(max_length=1, null=True)
    indi_interesse_redesim = models.CharField(max_length=1, null=True)

    class Meta:
        db_table = os.getenv('TABELA_CNAE_CCE_ORACLE')
        managed = False


class CNAE_RURAL(models.Model):
    id_subclasse_cnaef = models.IntegerField(primary_key=True)
    data_inicio_vigencia = models.DateTimeField(null=False)
    data_fim_vigencia = models.DateTimeField(null=True)

    class Meta:
        db_table = os.getenv('TABELA_CNAE_RURAL_ORACLE')
        managed = False


class GEN_NCM(models.Model):
    id_produto_ncm = models.BigIntegerField(primary_key=True)
    codg_produto_ncm = models.CharField(max_length=8)
    desc_produto_ncm = models.CharField(max_length=1000)
    id_unidade_medida = models.IntegerField(null=True)
    data_inicio_vigencia_produto = models.DateTimeField()
    data_fim_vigencia_produto = models.DateTimeField()

    class Meta:
        db_table = os.getenv('TABELA_GEN_NCM_ORACLE')
        managed = False


class NCM_Rural(models.Model):
    id_produto_ncm = models.BigIntegerField(primary_key=True)
    data_inicio_vigencia = models.DateTimeField(null=False)
    data_fim_vigencia = models.DateTimeField(null=True)

    class Meta:
        db_table = os.getenv('TABELA_NCM_ORACLE')
        managed = False


class Hist_CCE(models.Model):
    id_historico_contrib = models.BigIntegerField(primary_key=True)
    numr_inscricao = models.CharField(max_length=9)
    numr_registro_juceg = models.CharField(max_length=15)
    nome_designacao_primaria = models.CharField(max_length=140)
    nome_designacao_secundaria = models.CharField(max_length=115)
    numr_cnpj_contrib = models.CharField(max_length=14)
    numr_cnpj_base_contrib = models.CharField(max_length=14)
    desc_email_contrib_ident = models.CharField(max_length=60)
    numr_ddd_telefone_contrib = models.IntegerField(null=False)
    numr_telefone_contrib_ident = models.BigIntegerField(null=False)
    desc_situacao_cadastro = models.CharField(max_length=30)
    tipo_pessoa = models.CharField(max_length=1)
    tipo_inscricao = models.CharField(max_length=1)
    desc_natureza_juridica = models.CharField(max_length=80)
    desc_faixa_porte = models.CharField(max_length=30)
    desc_tipo_enqdto = models.CharField(max_length=30)
    desc_tipo_contrib = models.CharField(max_length=253)
    desc_informat = models.CharField(max_length=30)
    desc_faixa_area_estab = models.CharField(max_length=40)
    qtde_socio = models.IntegerField(null=False)
    indi_contrib_cadastro_cce = models.CharField(max_length=1)
    indi_creden_dte = models.CharField(max_length=1)
    codg_municipio_contrib = models.BigIntegerField(null=False)
    numr_crc = models.CharField(max_length=10)
    id_cont = models.BigIntegerField(null=False)
    id_subcls = models.IntegerField(null=False)
    numr_ano_mes_dia_alteracao = models.BigIntegerField(null=False)
    tipo_alteracao = models.CharField(max_length=1)
    numr_cpf_contrib = models.CharField(max_length=11)
    valr_capital_social = models.FloatField(null=True)

    class Meta:
        db_table = os.getenv('TABELA_DM_HIST_CCE_ORACLE')
        managed = False


class Hist_CNAE(models.Model):
    id_historico_cnae_contrib = models.BigIntegerField(primary_key=True)
    numr_inscricao_contrib = models.CharField(max_length=9)
    perc_atividade_contrib = models.FloatField(null=True)
    indi_cnae_principal_contrib = models.CharField(max_length=1)
    id_contrib_cnaef = models.BigIntegerField(null=False)
    id_mes_inicio_vigencia = models.IntegerField(null=False)
    id_contrib = models.BigIntegerField(null=False)
    id_subcls = models.IntegerField(null=False)
    id_mes_fim_vigencia = models.IntegerField(null=False)

    class Meta:
        db_table = os.getenv('TABELA_DM_HIST_CNAE_ORACLE')
        managed = False


class DMRUC014(models.Model):
    id_subcls = models.IntegerField(primary_key=True)
    id_cls = models.IntegerField(null=False)
    codg_subcls = models.CharField(max_length=7)
    denom_subcls = models.CharField(max_length=253)
    numr_grp_monito = models.IntegerField(null=False)
    stat_subcls = models.CharField(max_length=1)
    id_monito_auditoria = models.IntegerField(null=False)
    id_grupo_monito = models.IntegerField(null=False)
    indi_obrigat_nfe = models.CharField(max_length=1)
    data_inicio_nfe = models.DateTimeField(null=False)

    class Meta:
        db_table = os.getenv('TABELA_DMRUC014_ORACLE')
        managed = False

# class CFOP_RURAL(models.Model):
#     codg_cfop = models.IntegerField(primary_key=True)
#     data_inicio_vigencia = models.DateTimeField(null=False)
#     data_fim_vigencia = models.DateTimeField(null=True)
#
#     class Meta:
#         db_table = os.getenv('TABELA_CFOP_RURAL_ORACLE')
#         managed = False
#
#
# class CadastroCCE(models.Model):
#     numr_inscricao = models.IntegerField(primary_key=True)
#     numr_ref_emissao = models.IntegerField(null=True)
#     codg_municipio = models.IntegerField(null=True)
#     codg_municipio_ibge = models.IntegerField(null=True)
#     indi_prod_rural = models.CharField(max_length=1)
#     tipo_enqdto = models.CharField(max_length=1)
#     situacao_cadastral = models.CharField(max_length=1)
#
#     class Meta:
#         db_table = os.getenv('TABELA_HISTORICO_CCE')
#         managed = False
#
#
# class NfeCalcNfeChave(models.Model):
#     id_nfe = models.BigIntegerField(primary_key=True)
#     ie_saida = models.BigIntegerField(null=True)
#     ie_entrada = models.BigIntegerField(null=True)
#     indi_tipo_operacao = models.TextField(null=True)
#     data_emissao = models.DateTimeField(null=True)
#     numr_ref_emissao = models.IntegerField(null=True)
#     chave_nfe = models.TextField(null=True)
#     codg_modelo_nfe = models.BigIntegerField(null=True)
#     valr_produto = models.FloatField(null=True)
#     valr_desconto = models.FloatField(null=True)
#     valr_frete = models.FloatField(null=True)
#     valr_seguro = models.FloatField(null=True)
#     valr_outros = models.FloatField(null=True)
#     valr_icms_deson = models.FloatField(null=True)
#     valr_icms_st = models.FloatField(null=True)
#     valr_imp_import = models.FloatField(null=True)
#     valr_ipi = models.FloatField(null=True)
#     valr_final_produto = models.FloatField(null=True)
#     valr_va = models.FloatField(null=True)
#     codg_municipio_entrada = models.IntegerField(null=True)
#     tipo_enqdto_entrada = models.FloatField(null=True)
#     situacao_cadastro_entrada = models.FloatField(null=True)
#     ind_prod_rural_entrada = models.BooleanField(null=True)
#     codg_municipio_saida = models.IntegerField(null=True)
#     tipo_enqdto_saida = models.FloatField(null=True)
#     situacao_cadastro_saida = models.FloatField(null=True)
#     ind_prod_rural_saida = models.BooleanField(null=True)
#
#     class Meta:
#         db_table = os.getenv('TABELA_NFE_CALC_CHAVE')
#         managed = False
#
#
#
#
# class NfeCalcNfe(models.Model):
#     id = models.AutoField(primary_key=True)
#     id_nfe = models.BigIntegerField(null=False)
#     ie_saida = models.CharField(max_length=15, null=True)
#     ie_entrada = models.CharField(max_length=15, null=True)
#     indi_tipo_operacao = models.CharField(max_length=1, null=True)
#     data_emissao = models.DateTimeField(null=True)
#     numr_ref_emissao = models.IntegerField(null=True)
#     desc_finalidade_operacao = models.CharField(max_length=50, null=True)
#     desc_destino_operacao = models.CharField(max_length=50, null=True)
#     desc_natureza_operacao = models.CharField(max_length=250, null=True)
#     chave_nfe = models.CharField(max_length=44, null=True)
#     codg_modelo_nfe = models.IntegerField(null=True)
#     numr_cnpj_remet = models.CharField(max_length=14, null=True)
#     numr_cpf_remet = models.CharField(max_length=11, null=True)
#     numr_cnpj_dest = models.CharField(max_length=14, null=True)
#     numr_cpf_dest = models.CharField(max_length=11, null=True)
#     valr_tot_nf = models.FloatField(null=True)
#     valr_tot_subst_trib = models.FloatField(null=True)
#     numr_item = models.IntegerField(null=False)
#     numr_cfop = models.IntegerField(null=True)
#     indi_cfop_particip = models.BooleanField(null=True)
#     numr_gtin = models.CharField(max_length=15, null=True)
#     numr_cest = models.CharField(max_length=15, null=True)
#     numr_ncm = models.CharField(max_length=15, null=True)
#     codg_anp = models.CharField(max_length=15, null=True)
#     valr_produto = models.FloatField(null=True)
#     valr_desconto = models.FloatField(null=True)
#     valr_frete = models.FloatField(null=True)
#     valr_seguro = models.FloatField(null=True)
#     valr_outros = models.FloatField(null=True)
#     valr_icms_deson = models.FloatField(null=True)
#     valr_icms_st = models.FloatField(null=True)
#     valr_imp_import = models.FloatField(null=True)
#     valr_ipi = models.FloatField(null=True)
#     valr_final_produto = models.FloatField(null=True)
#     valr_va = models.FloatField(null=True)
#     numr_protocolo_cancel = models.CharField(max_length=15, null=True)
#     data_cancel = models.DateTimeField(null=True)
#     stat_nota = models.TextField(null=True)
#     indi_origem = models.CharField(max_length=1, null=True)
#     data_carga = models.DateTimeField(null=True)
#     data_calc = models.DateTimeField(null=True)
#
#     class Meta:
#         db_table = os.getenv('TABELA_NFE_CALC')
#         indexes = [models.Index(fields=['data_emissao'])]
#         managed = False
#         unique_together = ('id_nfe', 'numr_item')
#
#
# class NfeMySql(models.Model):
#     id_nfe = models.BigIntegerField(primary_key=True)
#     ie_saida = models.CharField(max_length=15)
#     ie_entrada = models.CharField(max_length=15)
#     indi_tipo_operacao = models.CharField(max_length=1)
#     numr_ano_mes_dia_emissao = models.DecimalField(max_digits=8, decimal_places=0)
#     numr_ref_emissao = models.DecimalField(max_digits=6, decimal_places=0)
#     chave_nfe = models.CharField(max_length=44)
#     codg_modelo_nfe = models.DecimalField(max_digits=2, decimal_places=0)
#     valr_produto = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_desconto = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_frete = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_seguro = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_outros = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_icms_deson = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_icms_st = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_imp_import = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_ipi = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_final_produto = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_va = models.DecimalField(max_digits=15, decimal_places=2)
#     codg_municipio_entrada = models.DecimalField(max_digits=11, decimal_places=0)
#     tipo_enqdto_entrada = models.DecimalField(max_digits=1, decimal_places=0)
#     situacao_cadastro_entrada = models.DecimalField(max_digits=1, decimal_places=0)
#     ind_prod_rural_entrada = models.BooleanField()
#     codg_municipio_saida = models.DecimalField(max_digits=11, decimal_places=0)
#     tipo_enqdto_saida = models.DecimalField(max_digits=1, decimal_places=0)
#     situacao_cadastro_saida = models.DecimalField(max_digits=1, decimal_places=0)
#     ind_prod_rural_saida = models.BooleanField()
#
#     class Meta:
#         db_table = 'ipm_nfe_chave'
#         managed = False
#
#
# class Nfe(models.Model):
#     id_nfe = models.BigIntegerField(primary_key=True)
#     desc_natureza_operacao = models.CharField(max_length=60)
#     codg_modelo_nfe = models.CharField(max_length=2)
#     numr_serie_nfe = models.PositiveIntegerField()
#     data_emissao_nfe = models.DateField()
#     numr_cnpj_emissor = models.CharField(max_length=14)
#     numr_documento_fiscal = models.PositiveIntegerField()
#     data_movmt_mercadoria = models.DateField()
#     tipo_documento_fiscal = models.CharField(max_length=1)
#     codg_municipio_gerador = models.PositiveIntegerField()
#     codg_chave_acesso_nfe = models.CharField(max_length=44)
#     tipo_formato_impressao = models.CharField(max_length=1)
#     tipo_forma_emissao = models.CharField(max_length=1)
#     numr_protocolo = models.PositiveIntegerField()
#     data_inicio_procesm = models.DateField()
#     data_fim_procesm = models.DateField()
#     valr_resumo_nfe = models.CharField(max_length=40)
#     numr_protocolo_cancel = models.PositiveIntegerField()
#     numr_cpf_cnpj_dest = models.CharField(max_length=14)
#     id_versao_esquema_xml = models.PositiveIntegerField()
#     numr_inscricao = models.PositiveIntegerField()
#     id_resultado_procesm = models.PositiveIntegerField()
#     numr_recibo = models.PositiveIntegerField()
#     codg_uf = models.CharField(max_length=2)
#     valr_nota_fiscal = models.DecimalField(max_digits=17, decimal_places=2)
#     valr_icms = models.DecimalField(max_digits=17, decimal_places=2)
#     perc_aliquota_icms = models.DecimalField(max_digits=4, decimal_places=2)
#     valr_icms_substrib = models.DecimalField(max_digits=17, decimal_places=2)
#     perc_aliquota_icms_substrib = models.DecimalField(max_digits=5, decimal_places=2)
#     codg_uf_dest = models.CharField(max_length=2)
#     numr_inscricao_dest = models.PositiveIntegerField()
#     tipo_nfe = models.CharField(max_length=1)
#     valr_issqn = models.DecimalField(max_digits=17, decimal_places=2)
#     tipo_finalidade_nfe = models.PositiveIntegerField()
#     valr_base_calculo_icms = models.DecimalField(max_digits=15, decimal_places=2)
#     numr_placa_transp = models.CharField(max_length=8)
#     qtde_peso_liquido_transp = models.DecimalField(max_digits=15, decimal_places=3)
#     codg_municipio_dest = models.PositiveIntegerField()
#     codg_municipio_emissor = models.PositiveIntegerField()
#     indi_nota_exportacao = models.CharField(max_length=1)
#     stat_verif_nfe = models.PositiveIntegerField()
#     valr_total_imposto_importacao = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_total_fcp = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_total_fcp_substrib = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_total_fcp_substrib_ant = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_total_ipi_devolvido = models.DecimalField(max_digits=15, decimal_places=2)
#     numr_cpf_emissor = models.CharField(max_length=11)
#     codg_intermdr = models.CharField(max_length=1)
#
#     class Meta:
#         db_table = os.getenv('TABELA_NFE_ORIGEM')
#         managed = False
#
#
# class ItemNfe(models.Model):
#     id_nfe = models.ForeignKey('Nfe', on_delete=models.PROTECT)
#     id_nfe_recebida = models.BigIntegerField()
#     codg_item = models.CharField(max_length=60)
#     desc_item = models.CharField(max_length=120)
#     desc_unidade_comercial = models.CharField(max_length=6)
#     qtde_comercial = models.DecimalField(max_digits=19, decimal_places=4)
#     valr_unitario_comercial = models.DecimalField(max_digits=22, decimal_places=10)
#     valr_total_bruto = models.DecimalField(max_digits=25, decimal_places=2)
#     codg_unidade_tributavel = models.CharField(max_length=6)
#     qtde_tributavel = models.DecimalField(max_digits=19, decimal_places=4)
#     valr_unitario_tributavel = models.DecimalField(max_digits=22, decimal_places=10)
#     valr_frete = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_seguro = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_desconto = models.DecimalField(max_digits=15, decimal_places=2)
#     codg_produto_anp = models.BigIntegerField()
#     id_item_nota_fiscal = models.BigIntegerField()
#     codg_produto_ncm = models.BigIntegerField()
#     numr_item = models.BigIntegerField()
#     codg_cfop = models.BigIntegerField()
#     codg_ean = models.CharField(max_length=14)
#     id_situacao_tributaria_icms = models.IntegerField()
#     valr_outras_despesas = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_base_calculo_icms = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_base_calculo_subtrib = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_ipi = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_icms = models.DecimalField(max_digits=17, decimal_places=2)
#     perc_aliquota_icms = models.DecimalField(max_digits=7, decimal_places=4)
#     valr_icms_subtrib = models.DecimalField(max_digits=17, decimal_places=2)
#     perc_aliquota_icms_subtrib = models.DecimalField(max_digits=7, decimal_places=4)
#     valr_base_calculo_ipi = models.DecimalField(max_digits=15, decimal_places=2)
#     qtde_tributavel_ipi = models.DecimalField(max_digits=16, decimal_places=4)
#     valr_unitario_tributavel_ipi = models.DecimalField(max_digits=15, decimal_places=4)
#     tipo_motivo_desonera_icms = models.IntegerField()
#     valr_icms_desonera = models.DecimalField(max_digits=15, decimal_places=2)
#     perc_aliquota_ipi = models.DecimalField(max_digits=7, decimal_places=4)
#     perc_credito_simples_nacional = models.DecimalField(max_digits=7, decimal_places=4)
#     valr_icms_credito_simples = models.DecimalField(max_digits=15, decimal_places=2)
#     codg_situacao_tributaria_ipi = models.IntegerField()
#     id_cest = models.IntegerField()
#     codg_cest = models.CharField(max_length=240)
#     valr_imposto_importacao = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_icms_substrib_anterior = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_bc_icms_substrib_anterior = models.DecimalField(max_digits=15, decimal_places=2)
#     perc_aliquota_consumidor_final = models.DecimalField(max_digits=7, decimal_places=4)
#     valr_ipi_devolvido = models.DecimalField(max_digits=15, decimal_places=2)
#     codg_barra_proprio_terceiro = models.CharField(max_length=30)
#     indi_producao_escala_relevante = models.CharField(max_length=1)
#     numr_cnpj_fabricante = models.CharField(max_length=14)
#     codg_beneficio_fiscal = models.CharField(max_length=10)
#     codg_excecao_tabela_ipi = models.IntegerField()
#     codg_barra_tributavel = models.CharField(max_length=30)
#     valr_icms10_st_desonerado = models.DecimalField(max_digits=15, decimal_places=2)
#     codg_motivo_desonera_icms10_st = models.IntegerField()
#     valr_bc_retido_st_icms70 = models.DecimalField(max_digits=15, decimal_places=2)
#     perc_bc_retido_st_icms70 = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_icms70_fcp_retido_st = models.DecimalField(max_digits=15, decimal_places=2)
#     codg_modalidade_bc_icms70_st = models.IntegerField()
#     perc_margem_valor_st_icms70 = models.DecimalField(max_digits=7, decimal_places=4)
#     perc_reducao_bc_icms70_st = models.DecimalField(max_digits=7, decimal_places=4)
#     valr_base_calculo_icms70_st = models.DecimalField(max_digits=15, decimal_places=2)
#     perc_aliquota_icms70_st = models.DecimalField(max_digits=7, decimal_places=4)
#     valr_icms70_st = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_bc_fcp_st_icms70 = models.DecimalField(max_digits=15, decimal_places=2)
#     perc_fcp_retido_st_icms70 = models.DecimalField(max_digits=7, decimal_places=4)
#     valr_fcp_retido_st_icms70 = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_icms70_desonerado = models.DecimalField(max_digits=15, decimal_places=2)
#     codg_motivo_desonera_icms70 = models.IntegerField()
#     valr_icms70_st_desonerado = models.DecimalField(max_digits=15, decimal_places=2)
#     codg_motivo_desonera_icms70_st = models.BigIntegerField()
#     valr_bc_fcp_icms90 = models.DecimalField(max_digits=15, decimal_places=2)
#     perc_icms90_fcp = models.DecimalField(max_digits=7, decimal_places=4)
#     valr_icms90_fcp = models.DecimalField(max_digits=15, decimal_places=2)
#     codg_modalidade_bc_icms90_st = models.IntegerField()
#     perc_margem_valor_st_icms90 = models.DecimalField(max_digits=7, decimal_places=4)
#     perc_reducao_bc_icms90_st = models.DecimalField(max_digits=7, decimal_places=4)
#     valr_base_calculo_icms90_st = models.DecimalField(max_digits=15, decimal_places=2)
#     perc_aliquota_icms90_st = models.DecimalField(max_digits=7, decimal_places=4)
#     perc_fcp_retido_st_icms90 = models.DecimalField(max_digits=7, decimal_places=4)
#     valr_fcp_retido_st_icms90 = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_icms90_desonerado = models.DecimalField(max_digits=15, decimal_places=2)
#     codg_motivo_desonera_icms90 = models.IntegerField()
#     valr_icms90_st_desonerado = models.DecimalField(max_digits=15, decimal_places=2)
#     codg_motivo_desonera_icms90_st = models.IntegerField()
#     valr_base_calculo_pis = models.DecimalField(max_digits=15, decimal_places=2)
#     perc_aliquota_pis = models.DecimalField(max_digits=7, decimal_places=4)
#     qtde_vendida_pis = models.DecimalField(max_digits=16, decimal_places=4)
#     valr_aliquota_pis = models.DecimalField(max_digits=15, decimal_places=4)
#     valr_pis = models.DecimalField(max_digits=15, decimal_places=2)
#     indi_soma_pis_st = models.PositiveIntegerField()
#     valr_base_calculo_cofins = models.DecimalField(max_digits=15, decimal_places=2)
#     perc_aliquota_cofins = models.DecimalField(max_digits=7, decimal_places=4)
#     qtde_vendida_cofins = models.DecimalField(max_digits=16, decimal_places=4)
#     valr_aliquota_cofins = models.DecimalField(max_digits=15, decimal_places=4)
#     valr_cofins = models.DecimalField(max_digits=15, decimal_places=2)
#     indi_soma_cofins_st = models.PositiveIntegerField()
#
#     class Meta:
#         db_table = os.getenv('TABELA_NFE_ITEM_ORIGEM')
#         managed = False
#
#
# class ItemNfeMySql(models.Model):
#     id_nfe = models.ForeignKey('NfeMySql', on_delete=models.PROTECT)
#     ie_saida = models.CharField(max_length=15)
#     ie_entrada = models.CharField(max_length=15)
#     indi_tipo_operacao = models.CharField(max_length=1)
#     numr_ano_mes_dia_emissao = models.DecimalField(max_digits=8, decimal_places=0)
#     numr_ref_emissao = models.DecimalField(max_digits=6, decimal_places=0)
#     desc_finalidade_operacao = models.CharField(max_length=50)
#     desc_destino_operacao = models.CharField(max_length=50)
#     desc_natureza_operacao = models.CharField(max_length=250)
#     chave_nfe = models.CharField(max_length=44)
#     codg_modelo_nfe = models.DecimalField(max_digits=2, decimal_places=0)
#     numr_cnpj_remet = models.CharField(max_length=14)
#     numr_cpf_remet = models.CharField(max_length=11)
#     numr_cnpj_dest = models.CharField(max_length=14)
#     numr_cpf_dest = models.CharField(max_length=11)
#     valr_tot_nf = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_tot_subst_trib = models.DecimalField(max_digits=15, decimal_places=2)
#     numr_item = models.DecimalField(max_digits=3, decimal_places=0)
#     numr_cfop = models.DecimalField(max_digits=4, decimal_places=0)
#     indi_cfop_particip = models.BooleanField()
#     numr_gtin = models.CharField(max_length=15)
#     numr_cest = models.CharField(max_length=15)
#     numr_ncm = models.CharField(max_length=15)
#     codg_anp = models.CharField(max_length=15)
#     valr_produto = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_desconto = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_frete = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_seguro = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_outros = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_icms_deson = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_icms_st = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_imp_import = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_ipi = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_final_produto = models.DecimalField(max_digits=15, decimal_places=2)
#     valr_va = models.DecimalField(max_digits=15, decimal_places=2)
#     numr_protocolo_cancel = models.CharField(max_length=15)
#     data_cancel = models.DateTimeField()
#     stat_nota = models.CharField(max_length=1)
#     indi_origem = models.CharField(max_length=1)
#     data_carga = models.DateTimeField()
#     data_calc = models.DateTimeField()
#
#     class Meta:
#         db_table = 'ipm_nfe'
#         managed = False
#
#
# class Processamento(models.Model):
#     id_procesm = models.AutoField(primary_key=True)
#     data_inicio_procesm = models.DateTimeField()
#     data_fim_procesm = models.DateTimeField()
#     stat_procesm = models.CharField(max_length=1)
#     desc_observacao_procesm = models.CharField(max_length=500)
#     codg_tipo_procesm = models.DecimalField(max_digits=4, decimal_places=0)
#
#     class Meta:
#         db_table = os.getenv('TABELA_PROCESSAMENTO')
#         managed = True
