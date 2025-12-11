import zipfile

from django.http import HttpResponse

from funcoes.constantes import EnumTipoDocumento, EnumTipoProcessamento, EnumMotivoExclusao, EnumTipoExclusao
from funcoes.utilitarios import *
from negocio.CCEE import CCEEClasse
from negocio.CFOP import CFOPClasse
from negocio.CNAE import CNAEClasse
from negocio.GEN import GenClass
from negocio.NCM import NCMClasse
from negocio.Param import ParamClasse
from persistencia.Oraprd import Oraprd
from polls.models import TipoDocumento, TipoProcessamento, MotivoExclusao


def index(request):
    return HttpResponse("Informe o parâmetro de data inicio e fim de processamento no formato 'YYYYMMDD'!")


def inicializa_parametros(request):
    etapaProcess = 'Inicializa tabela de Parametros do sistema.'
    # loga_mensagem(etapaProcess)

    try:
        param = ParamClasse()
        param.carregar_parametros()
        del param

        etapaProcess += ' - Processo finalizado.'
        loga_mensagem(etapaProcess)

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)

    return HttpResponse(etapaProcess)


def atualiza_parametro(request, p_nome_param, p_valr_param):
    etapaProcess = 'Inicializa tabela de Parametros do sistema.'

    try:
        param = ParamClasse()
        param.atualizar_parametro(p_nome_param, p_valr_param)
        etapaProcess += ' - Processo finalizado.'

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)

    return HttpResponse(etapaProcess)


def cria_novo_parametro(request, p_nome_param, p_valr_param):
    etapaProcess = 'Cria novo parâmetro.'

    try:
        param = ParamClasse()
        param.cria_novo_parametro(p_nome_param, p_valr_param)
        etapaProcess += ' - Processo finalizado.'

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)

    return HttpResponse(etapaProcess)


def inicializa_tabelas(request, p_data_ref):
    etapaProcess = 'Inicializa tabelas auxiliares do sistema.'
    loga_mensagem(etapaProcess)

    try:
        # etapaProcess = carrega_arquivo_cfop(None, p_data_ref)
        # etapaProcess = carrega_arquivo_ncm(None,p_data_ref)
        # carrega_tipo_documento()
        carrega_tipo_procesm()
        # carrega_municipio_siaf()

        # param = ParamClasse()
        # param.carregar_parametros()
        # del param

        # carrega_motivo_exclusao()
        # carrega_cnae_prod_rural(None, p_data_ref)
        # etapaProcess = 'Tabelas auxiliares inicializadas.'

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)

    return HttpResponse(etapaProcess)


def carrega_arquivo_ncm(request, p_data_ref):
    etapaProcess = 'Carrega tabela de NCMs para Produtores Rurais a partir de arquivo .csv'

    try:
        d_data_inicio_vigencia = pd.to_datetime(p_data_ref, format='%Y%m%d')

        nome_arquivo = f'NCM - Produtores Rurais {p_data_ref}.csv'
        s_separador = ';'
        df = sobe_csv(nome_arquivo, s_separador)

        df['CODIGO NCM_X'] = df['CODIGO NCM'].astype(str).str.zfill(8)
        df = df.drop(columns=['CÓD DA SEÇÃO', 'Grupo da Seção', 'DESCRIÇÃO DA SEÇÃO', 'DESCRIÇÃO CÓDIGO NCM', 'CODIGO NCM'])
        df.rename(columns={'CODIGO NCM_X': 'CODIGO NCM'}, inplace=True)
        etapaProcess += f' - {len(df)} linhas lidas.'
        loga_mensagem(etapaProcess)

        db = NCMClasse()
        df_ncm = db.carga_inicial_ncm_produtores_rurais(df['CODIGO NCM'].tolist(), d_data_inicio_vigencia)
        etapaProcess += f' - {len(df_ncm)} IDs lidos.'
        loga_mensagem(etapaProcess)

        # df_sai = pd.merge(df, df_ncm, left_on=['CODIGO NCM'], right_on=['codg_produto_ncm'], how='left')
        # df_sai.name = 'df_sai'
        # baixa_csv(df_sai)

        df_ncm['data_inicio_vigencia'] = d_data_inicio_vigencia
        linhas_excluidas = db.delete_ncm_rural()
        etapaProcess += f' - {linhas_excluidas} excluídas.'

        linhas_incluidas = db.insert_ncm_rural(df_ncm)
        etapaProcess += f' - {linhas_incluidas} incluídos na tabela.'
        loga_mensagem(etapaProcess)
        del db

        etapaProcess += ' - Processo finalizado.'
        loga_mensagem(etapaProcess)

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)

    return etapaProcess


def carrega_arquivo_cfop(request, p_data_ref, p_tipo_doc):
    etapaProcess = 'Carrega tabela de CFOPs válidos para o cálculo a partir de arquivo .csv'

    try:
        nome_arquivo = 'CFOP ' + str(p_data_ref) + ' ' + str(p_tipo_doc) + '.csv'
        s_separador = '|'
        df_cfop = sobe_csv(nome_arquivo, s_separador)

        cfop = CFOPClasse()
        etapaProcess = cfop.carregar_cfops(p_data_ref, df_cfop, p_tipo_doc)
        del cfop

        etapaProcess += ' - Processo finalizado.'
        loga_mensagem(etapaProcess)

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)

    return etapaProcess


def carrega_ramo_atividade_cnae(request):
    etapaProcess = 'Carrega tabela de ramos de atividade por CNAE a partir de arquivo .csv'

    try:
        nome_arquivo = 'Ramo Ativ CNAE.csv'
        s_separador = '|'
        df_ativ_cnae = sobe_csv(nome_arquivo, s_separador)

        cnae = CNAEClasse()
        etapaProcess = cnae.carregar_ativ_cnae(df_ativ_cnae)
        del cnae

        etapaProcess += ' - Processo finalizado.'
        loga_mensagem(etapaProcess)

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)

    return etapaProcess


def carrega_arquivo_ccee(request, p_nome_arquivo, p_separador):
    etapaProcess = 'Carrega tabela de inscrições participantes da CCEE, a partir de arquivo .csv'

    try:
        ccee = CCEEClasse()
        # etapaProcess = ccee.excluir_ccee()

        df_ccee = sobe_csv(p_nome_arquivo + '.csv', p_separador)
        df_ccee['data_inicio_vigencia_ccee'] = pd.to_datetime(df_ccee['data_inicio_vigencia'], format='%d/%m/%Y %H:%M:%S')
        df_ccee['data_fim_vigencia_ccee'] = pd.to_datetime(df_ccee['data_fim_vigencia'], format='%d/%m/%Y %H:%M:%S')

        etapaProcess = ccee.carregar_ccee(df_ccee)
        del ccee

        etapaProcess += ' - Processo finalizado.'
        loga_mensagem(etapaProcess)

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)

    return etapaProcess


def carrega_cnae_prod_rural(request, p_data_ref):
    etapaProcess = 'Carrega tabela de CNAEs de produtores rurais a partir de arquivo .csv'

    try:
        nome_arquivo = 'CNAE Rural ' + str(p_data_ref) + '.csv'
        s_separador = '|'
        df = sobe_csv(nome_arquivo, s_separador)

        cnae = CNAEClasse()
        etapaProcess = cnae.carregar_cnae_rural(p_data_ref, df)
        del cnae

        etapaProcess += ' - Processo finalizado.'
        loga_mensagem(etapaProcess)

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)

    return etapaProcess


def carrega_municipio_siaf():
    etapaProcess = 'Carrega tabela de municipios do SIAF'

    try:
        nome_arquivo = 'Municipio SIAF.csv'
        s_separador = ';'
        df_siaf = sobe_csv(nome_arquivo, s_separador)

        df_siaf.drop(columns=['Unnamed: 5'], inplace=True)

        gen = GenClass()
        df_municip = gen.carrega_municipio_gen(df_siaf['COD GEN'].drop_duplicates().tolist())
        del gen

        df = pd.merge(df_siaf, df_municip, left_on=['COD GEN'], right_on=['codg_municipio'], how='left')
        df.rename(columns={'CODIGO SIAF': 'CODG_MUNICIPIO_SIAF',
                           'codg_municipio': 'CODG_MUNICIPIO'
                           }, inplace=True)

        db = Oraprd()
        linhas_inseridas = db.insert_municipio_siaf(df[['CODG_MUNICIPIO_SIAF', 'CODG_MUNICIPIO']])
        del db

        etapaProcess += f' - Processo finalizado. {linhas_inseridas} linhas inseridas.'
        loga_mensagem(etapaProcess)

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)

    return etapaProcess


def carrega_tipo_documento():
    etapaProcess = 'Carrega tabela de tipo de documentos.'

    try:
        tipo_doc = [TipoDocumento(codg_tipo_doc_partct_calc=EnumTipoDocumento.NFe.value, desc_tipo_doc_partct_calc=EnumTipoDocumento.NFe.name),
                    TipoDocumento(codg_tipo_doc_partct_calc=EnumTipoDocumento.NFCe.value, desc_tipo_doc_partct_calc=EnumTipoDocumento.NFCe.name),
                    TipoDocumento(codg_tipo_doc_partct_calc=EnumTipoDocumento.NFeRecebida.value, desc_tipo_doc_partct_calc=EnumTipoDocumento.NFeRecebida.name),
                    TipoDocumento(codg_tipo_doc_partct_calc=EnumTipoDocumento.NF3e.value, desc_tipo_doc_partct_calc=EnumTipoDocumento.NF3e.name),
                    TipoDocumento(codg_tipo_doc_partct_calc=EnumTipoDocumento.NFA.value, desc_tipo_doc_partct_calc=EnumTipoDocumento.NFA.name),
                    TipoDocumento(codg_tipo_doc_partct_calc=EnumTipoDocumento.EFD.value, desc_tipo_doc_partct_calc=EnumTipoDocumento.EFD.name),
                    TipoDocumento(codg_tipo_doc_partct_calc=EnumTipoDocumento.TelecomConv115.value, desc_tipo_doc_partct_calc=EnumTipoDocumento.TelecomConv115.name),
                    TipoDocumento(codg_tipo_doc_partct_calc=EnumTipoDocumento.BPe.value, desc_tipo_doc_partct_calc=EnumTipoDocumento.BPe.name),
                    TipoDocumento(codg_tipo_doc_partct_calc=EnumTipoDocumento.CTe.value, desc_tipo_doc_partct_calc=EnumTipoDocumento.CTe.name),
                    TipoDocumento(codg_tipo_doc_partct_calc=EnumTipoDocumento.AutoInfracao.value, desc_tipo_doc_partct_calc=EnumTipoDocumento.AutoInfracao.name),
                    TipoDocumento(codg_tipo_doc_partct_calc=EnumTipoDocumento.Simples.value,desc_tipo_doc_partct_calc=EnumTipoDocumento.Simples.name),
                    TipoDocumento(codg_tipo_doc_partct_calc=EnumTipoDocumento.MEI.value,desc_tipo_doc_partct_calc=EnumTipoDocumento.MEI.name),
                    TipoDocumento(codg_tipo_doc_partct_calc=EnumTipoDocumento.OperacoesEspeciais.value,desc_tipo_doc_partct_calc=EnumTipoDocumento.OperacoesEspeciais.name),
                    ]
        db = Oraprd()
        db.insert_tipo_docs(tipo_doc)
        del db
        return None

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        raise


def carrega_tipo_procesm():
    etapaProcess = 'Carrega tabela de tipo de processamento.'

    try:
        tipo_procesm = [TipoProcessamento(codg_tipo_procesm_indice=EnumTipoProcessamento.importacaoNFCe.value, desc_tipo_procesm_indice=EnumTipoProcessamento.importacaoNFCe.name),
                        TipoProcessamento(codg_tipo_procesm_indice=EnumTipoProcessamento.importacaoNFeRecebida.value, desc_tipo_procesm_indice=EnumTipoProcessamento.importacaoNFeRecebida.name),
                        TipoProcessamento(codg_tipo_procesm_indice=EnumTipoProcessamento.importacaoNF3e.value, desc_tipo_procesm_indice=EnumTipoProcessamento.importacaoNF3e.name),
                        TipoProcessamento(codg_tipo_procesm_indice=EnumTipoProcessamento.importacaoNFA.value, desc_tipo_procesm_indice=EnumTipoProcessamento.importacaoNFA.name),
                        TipoProcessamento(codg_tipo_procesm_indice=EnumTipoProcessamento.importacaoEFD.value, desc_tipo_procesm_indice=EnumTipoProcessamento.importacaoEFD.name),
                        TipoProcessamento(codg_tipo_procesm_indice=EnumTipoProcessamento.importacaoTelecomConv115.value, desc_tipo_procesm_indice=EnumTipoProcessamento.importacaoTelecomConv115.name),
                        TipoProcessamento(codg_tipo_procesm_indice=EnumTipoProcessamento.importacaoBPe.value, desc_tipo_procesm_indice=EnumTipoProcessamento.importacaoBPe.name),
                        TipoProcessamento(codg_tipo_procesm_indice=EnumTipoProcessamento.importacaoCTe.value, desc_tipo_procesm_indice=EnumTipoProcessamento.importacaoCTe.name),
                        TipoProcessamento(codg_tipo_procesm_indice=EnumTipoProcessamento.importacaoPAT.value, desc_tipo_procesm_indice=EnumTipoProcessamento.importacaoPAT.name),
                        TipoProcessamento(codg_tipo_procesm_indice=EnumTipoProcessamento.importacaoNFe.value, desc_tipo_procesm_indice=EnumTipoProcessamento.importacaoNFe.name),
                        TipoProcessamento(codg_tipo_procesm_indice=EnumTipoProcessamento.importacaoSimples.value, desc_tipo_procesm_indice=EnumTipoProcessamento.importacaoSimples.name),
                        TipoProcessamento(codg_tipo_procesm_indice=EnumTipoProcessamento.importacaoMEI.value, desc_tipo_procesm_indice=EnumTipoProcessamento.importacaoMEI.name),
                        TipoProcessamento(codg_tipo_procesm_indice=EnumTipoProcessamento.importacaoOpcaoSimplesSIMEI.value, desc_tipo_procesm_indice=EnumTipoProcessamento.importacaoOpcaoSimplesSIMEI.name),
                        TipoProcessamento(codg_tipo_procesm_indice=EnumTipoProcessamento.importacaoArquivoVA.value, desc_tipo_procesm_indice=EnumTipoProcessamento.importacaoArquivoVA.name),
                        TipoProcessamento(codg_tipo_procesm_indice=EnumTipoProcessamento.importacaoArquivoIndice.value, desc_tipo_procesm_indice=EnumTipoProcessamento.importacaoArquivoIndice.name),

                        TipoProcessamento(codg_tipo_procesm_indice=EnumTipoProcessamento.processaAtivoImobilizado.value, desc_tipo_procesm_indice=EnumTipoProcessamento.processaAtivoImobilizado.name),
                        TipoProcessamento(codg_tipo_procesm_indice=EnumTipoProcessamento.processaOperProdRurais.value, desc_tipo_procesm_indice=EnumTipoProcessamento.processaOperProdRurais.name),
                        TipoProcessamento(codg_tipo_procesm_indice=EnumTipoProcessamento.processaCCEE.value, desc_tipo_procesm_indice=EnumTipoProcessamento.processaCCEE.name),
                        TipoProcessamento(codg_tipo_procesm_indice=EnumTipoProcessamento.processaValorAdicionado.value, desc_tipo_procesm_indice=EnumTipoProcessamento.processaValorAdicionado.name),

                        TipoProcessamento(codg_tipo_procesm_indice=EnumTipoProcessamento.processaIndicePartct.value, desc_tipo_procesm_indice=EnumTipoProcessamento.processaIndicePartct.name),

                        TipoProcessamento(codg_tipo_procesm_indice=EnumTipoProcessamento.processaAcertoNFe.value,desc_tipo_procesm_indice=EnumTipoProcessamento.processaAcertoNFe.name),
                        TipoProcessamento(codg_tipo_procesm_indice=EnumTipoProcessamento.processaAcertoNFCe.value,desc_tipo_procesm_indice=EnumTipoProcessamento.processaAcertoNFCe.name),
                        TipoProcessamento(codg_tipo_procesm_indice=EnumTipoProcessamento.processaAcertoNFeRecebida.value,desc_tipo_procesm_indice=EnumTipoProcessamento.processaAcertoNFeRecebida.name),
                        TipoProcessamento(codg_tipo_procesm_indice=EnumTipoProcessamento.processaAcertoContrib.value,desc_tipo_procesm_indice=EnumTipoProcessamento.processaAcertoContrib.name),
                        ]
        db = Oraprd()
        db.insert_tipo_procesms(tipo_procesm)
        del db
        return None

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        raise


def carrega_motivo_exclusao(request):
    etapaProcess = 'Carrega tabela de motivos de exclusão de documentos.'

    try:
        motivos = [MotivoExclusao(codg_motivo_exclusao_calculo=EnumMotivoExclusao.CFOPNaoParticipante.value,
                                  desc_motivo_exclusao_calculo='CFOP Nao Participante do Cálculo',
                                  tipo_exclusao_calculo=EnumTipoExclusao.Item.value),
                   MotivoExclusao(codg_motivo_exclusao_calculo=EnumMotivoExclusao.CClassNaoParticipante.value,
                                  desc_motivo_exclusao_calculo='CClass Nao Participante do Cálculo',
                                  tipo_exclusao_calculo=EnumTipoExclusao.Item.value),
                   MotivoExclusao(codg_motivo_exclusao_calculo=EnumMotivoExclusao.ItemAtivoImobilizado.value,
                                  desc_motivo_exclusao_calculo='Item registrado como Ativo Imobilizado',
                                  tipo_exclusao_calculo=EnumTipoExclusao.Item.value),
                   MotivoExclusao(codg_motivo_exclusao_calculo=EnumMotivoExclusao.OperacaoDuplicada.value,
                                  desc_motivo_exclusao_calculo='Operação Duplicada',
                                  tipo_exclusao_calculo=EnumTipoExclusao.Item.value),
                   MotivoExclusao(codg_motivo_exclusao_calculo=EnumMotivoExclusao.OperacaoDuplicada.value,
                                  desc_motivo_exclusao_calculo='Operação entre Produtores Rurais Duplicada',
                                  tipo_exclusao_calculo=EnumTipoExclusao.Item.value),
                   MotivoExclusao(codg_motivo_exclusao_calculo=EnumMotivoExclusao.DocumentoCancelado.value,
                                  desc_motivo_exclusao_calculo='Documento Cancelado',
                                  tipo_exclusao_calculo=EnumTipoExclusao.Documento.value),
                   MotivoExclusao(codg_motivo_exclusao_calculo=EnumMotivoExclusao.ContribsNaoCadOuSimples.value,
                                  desc_motivo_exclusao_calculo='Contribuinte Nao Cadastrato Ou no Simples',
                                  tipo_exclusao_calculo=EnumTipoExclusao.Documento.value),
                   MotivoExclusao(codg_motivo_exclusao_calculo=EnumMotivoExclusao.DocumentoNaoContemItemParticipante.value,
                                  desc_motivo_exclusao_calculo='Documento Nao Contem Item Participante',
                                  tipo_exclusao_calculo=EnumTipoExclusao.Documento.value),
                   MotivoExclusao(codg_motivo_exclusao_calculo=EnumMotivoExclusao.DocumentoSubstituido.value,
                                  desc_motivo_exclusao_calculo='Documento Substituido',
                                  tipo_exclusao_calculo=EnumTipoExclusao.Documento.value),
                   MotivoExclusao(codg_motivo_exclusao_calculo=EnumMotivoExclusao.ValoresInconsistentes.value,
                                  desc_motivo_exclusao_calculo='Documento Com valores inconsistentes',
                                  tipo_exclusao_calculo=EnumTipoExclusao.Documento.value),
                   ]

        db = Oraprd()
        db.insert_motivos_exclusao(motivos)
        del db
        return None

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        raise


def limpa_base_dados(request, p_tabela, p_param):
    etapaProcess = 'Limpa Base de Dados'

    try:
        etapaProcess += f' - Tabela {p_tabela} - {p_param}'

        db = Oraprd()
        if p_tabela == 'IPM_DOCUMENTO_PARTCT_CALC_IPM':
            db.delete_ipm_documento_partct_calc_ipm(p_param)
        elif p_tabela == 'IPM_ITEM_DOCUMENTO':
            db.delete_ipm_item_documento(p_param)
        elif p_tabela == 'IPM_PROCESSAMENTO_INDICE':
            db.delete_ipm_processamento_indice(p_param)
        elif p_tabela == 'IPM_CONTRIBUINTE_IPM':
            db.delete_ipm_contribuinte_ipm()

        del db

        etapaProcess += f' - Processo finalizado.'
        return HttpResponse(etapaProcess)


    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        raise
