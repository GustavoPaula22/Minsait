import os
import sys
from datetime import datetime

from django.http import HttpResponse

from funcoes import utilitarios
from funcoes.constantes import EnumTipoDocumento, EnumStatusProcessamento, EnumTipoProcessamento
from funcoes.utilitarios import ultimo_dia_mes, loga_mensagem, sobe_csv, loga_mensagem_erro, converter_valor, LOCAL_TZ
from negocio.Procesm import ProcesmClasse
from negocio.ValorAdicionado import VAClass
from persistencia.Oraprd import Oraprd
from polls.models import TipoProcessamento


def calculo_valor_adicionado(request, pPeriodo):
    etapaProcess = f"request {__name__} - def calculo_valor_agregado"

    try:
        etapaProcess = f'Processa calculo dos valores adicionados para o periodo {pPeriodo}.'
        d_data_ref_ini = datetime.strptime(str(pPeriodo * 100 + 1), '%Y%m%d').date()
        d_data_ref_fim = ultimo_dia_mes(datetime.strptime(str(d_data_ref_ini), '%Y-%m-%d').date())
        d_data_ini = d_data_ref_ini
        d_data_fim = d_data_ref_fim

        neg = VAClass(d_data_ref_ini, d_data_ref_fim, d_data_ini, d_data_fim, 'Mensal')
        return HttpResponse(neg.calcula_valor_adicionado())

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        return HttpResponse(etapaProcess)


def registra_ativo_imobilizado(request, p_ano):
    etapaProcess = f"request {__name__} - def registra_ativo_imobilizado"

    try:
        etapaProcess = f'Processa registro de operações de ativo imobilizado.'
        d_data_ref_ini = datetime.strptime(str((p_ano * 100 + 1) * 100 + 1), '%Y%m%d').date()
        d_data_ref_fim = datetime.strptime(str((p_ano * 100 + 12) * 100 + 31), '%Y%m%d').date()
        d_data_ini = d_data_ref_ini
        d_data_fim = d_data_ref_fim

        neg = VAClass(d_data_ref_ini, d_data_ref_fim, d_data_ini, d_data_fim, 'Anual')
        return HttpResponse(neg.regista_ativo_imobilizado())

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        return HttpResponse(etapaProcess)


def registra_operacoes_entre_contribuintes(request, p_ano, p_arq):
    etapaProcess = f"request {__name__} - def registra_operacoes_entre_contribuintes"

    try:
        etapaProcess = f'Processa registro de operações duplicadas.'
        d_data_ref_ini = datetime.strptime(f"{p_ano}0101 000000", "%Y%m%d %H%M%S")
        d_data_ref_fim = datetime.strptime(f"{p_ano}1231 235959", "%Y%m%d %H%M%S")
        d_data_ini = d_data_ref_ini
        d_data_fim = d_data_ref_fim

        neg = VAClass(d_data_ref_ini, d_data_ref_fim, d_data_ini, d_data_fim, 'Anual')
        return HttpResponse(neg.operacoes_duplicadas(p_arq))

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        return HttpResponse(etapaProcess)


def carrega_va_externo(request, p_ano, p_tipo_doc, p_nome_arquivo, p_separador):
    etapaProcess = f'Carrega dados externos - Referencia: {p_ano} - Tipo de Documento: {p_tipo_doc} - Nome do arquivo: {p_nome_arquivo} - Separador: {p_separador}.'
    loga_mensagem(etapaProcess)

    i_ano_referencia = int(os.environ.get('ANO_REFERENCIA'))
    i_inicio_referencia = int(os.environ.get('INICIO_REFERENCIA'))
    i_fim_referencia = int(os.environ.get('FIM_REFERENCIA'))
    sData_hora_inicio = datetime.now(LOCAL_TZ).strftime('%d/%m/%Y %H:%M:%S,%f')

    try:
        etapaProcess = f'Inclui arquivo com dados de valores adicionados para o ano {p_ano}.'
        d_data_ini = datetime.strptime(str(i_inicio_referencia * 100 + 1), '%Y%m%d').date()
        d_data_fim = ultimo_dia_mes(datetime.strptime(str(i_fim_referencia * 100 + 1), '%Y%m%d').date())

        # Registra inicio do processamento #
        etapaProcess = f'Registra inicio do processamento de importação de arquivo externo do VA. Periodo de {i_inicio_referencia} a {i_fim_referencia}'
        db = ProcesmClasse()
        procesm = db.iniciar_processamento(d_data_ini, d_data_fim, EnumTipoProcessamento.importacaoArquivoVA.value)
        del db
        etapaProcess = f'{procesm.id_procesm_indice} Carga do arquivo de índices externos. Periodo {i_inicio_referencia} a {i_fim_referencia} - {sData_hora_inicio}'
        loga_mensagem(etapaProcess)

        if p_ano != i_ano_referencia:
            etapaProcess = f'Ano de referencia do arquivo difere do ano de processamento {i_ano_referencia} do sistema!'
            raise Exception(etapaProcess)

        if p_tipo_doc not in [item.value for item in EnumTipoDocumento]:
            etapaProcess = f'Tipo de documento {p_tipo_doc} não previsto!'
            raise Exception(etapaProcess)

        df = sobe_csv(p_nome_arquivo, p_separador)
        if len(df) == 0:
            etapaProcess = f'Não foi possível ler o arquivo {p_nome_arquivo}!'
            raise Exception(etapaProcess)

        # Trata as colunas do arquivo
        df.columns = df.columns.str.strip()
        colunas_esperadas = {'ano': 'int64', 'referencia': 'int64', 'tipo_documento': 'int64', 'tipo_apropriacao': 'object', 'valor_adicionado': 'float64', 'codigo_municipio': 'float64', 'num_inscricao': 'int64'}
        colunas_faltantes = [coluna for coluna in colunas_esperadas if coluna not in df.columns]
        if colunas_faltantes:
            etapaProcess = f'As colunas {colunas_faltantes} não estão presentes no arquivo {p_nome_arquivo}!'
            raise Exception(etapaProcess)

        if ~df['ano'].isin([i_ano_referencia]).all():
            etapaProcess = f'Arquivo {p_nome_arquivo} com Ano de referencia diferente do informado!'
            raise Exception(etapaProcess)

        if ~((df['referencia'] >= i_inicio_referencia) & (df['referencia'] <= i_fim_referencia)).all():
            etapaProcess = f'Arquivo {p_nome_arquivo} contem linhas com referencias diferentes do Ano informado {p_ano}!'
            raise Exception(etapaProcess)

        if (df['tipo_documento'] != p_tipo_doc).any():
            etapaProcess = f'Arquivo {p_nome_arquivo} com o Tipo de Documento diferente do informado!'
            raise Exception(etapaProcess)

        if ~df['tipo_apropriacao'].isin(['E', 'S']).all():
            etapaProcess = f'Arquivo {p_nome_arquivo} com tipo de apropriação inválido!'
            raise Exception(etapaProcess)

        # Trata valores não informados
        b_cancela = False
        valores_ausentes = df.isna().sum()
        for coluna, n_ausentes in valores_ausentes.items():
            if n_ausentes > 0:
                if coluna == 'valor_adicionado' \
                or coluna == 'codigo_municipio':
                    loga_mensagem_erro(f"Coluna '{coluna}' com valores não informados. Quantidade de linhas: {n_ausentes}")
                    b_cancela = True

                if coluna == 'num_inscricao':
                    df['num_inscricao'].fillna(0, inplace=True)

        if b_cancela:
            etapaProcess = f'Arquivo {p_nome_arquivo} com valores não informados!'
            raise Exception(etapaProcess)

        df['valor_adicionado'] = df['valor_adicionado'].str.replace('R$', '', regex=False).str.replace('$', '', regex=False)
        df['valor_adicionado'] = df['valor_adicionado'].apply(converter_valor)

        # Verifica o tipo de cada coluna
        for coluna, tipo_esperado in colunas_esperadas.items():
            tipo_real = df[coluna].dtype
            if str(tipo_real) != tipo_esperado:
                etapaProcess = f'Arquivo {p_nome_arquivo} com valores inválidos na coluna "{coluna}"!'
                raise Exception(etapaProcess)

        db = Oraprd()
        df_municipio = db.select_municipios()
        del db

        etapaProcess = "Merge entre arquivo e municípios."
        df = df.merge(df_municipio,  # Município de Entrada do GEN
                      left_on=['codigo_municipio'],
                      right_on=['codg_distrito'],
                      how='left',)

        df.drop(columns=['codg_distrito', 'nome_distrito', 'nome_municipio_x', 'codigo_municipio', ], inplace=True)
        df.rename(columns={'num_inscricao': 'numr_inscricao'
                         , 'nome_municipio_y': 'nome_municipio'
                         , 'tipo_apropriacao': 'tipo_aprop'
                         , 'valor_adicionado': 'valr_adicionado_operacao'
                         , 'referencia': 'numr_referencia'}, inplace=True)

        if (df['codg_uf'] != 'GO').any():
            etapaProcess = f'Arquivo {p_nome_arquivo} contem municípios fora de Goiás ou inválidos!'
            raise Exception(etapaProcess)

        etapaProcess = f'{procesm.id_procesm_indice} Grava índices externos - {utilitarios.formatar_com_espacos(len(df), 11)} linhas.'
        registra_processamento(procesm, etapaProcess)
        data_hora_atividade = datetime.now()

        if len(df) > 0:
            df['id_procesm_indice'] = procesm.id_procesm_indice

            neg = VAClass(d_data_ini, d_data_fim, 'Anual')
            linhas_gravadas = neg.grava_valor_adicionado(procesm, df)
            del neg

            etapaProcess = f'{procesm.id_procesm_indice} Importação de arquivo do Valores Adicionados para a referencia {p_ano} finalizado. Carregadas {len(df)} linhas.'
            procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
            procesm.stat_procesm_indice = EnumStatusProcessamento.sucesso.value
            registra_processamento(procesm, etapaProcess)
            codigo_retorno = 1

        else:
            etapaProcess = f'{procesm.id_procesm_indice} Importação de arquivo de Valores Adicionados ({p_ano}) finalizado. Não foram selecionados dados para importação.'
            procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
            procesm.stat_procesm_indice = EnumStatusProcessamento.sucessoSemDados.value
            registra_processamento(procesm, etapaProcess)
            codigo_retorno = 1

        loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_gravadas} ìndices gravados - ' + str(
            datetime.now() - data_hora_atividade))

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
        procesm.stat_procesm_indice = EnumStatusProcessamento.erro.value
        registra_processamento(procesm, etapaProcess)
        codigo_retorno = 9

    finally:
        HttpResponse(codigo_retorno)


def registra_processamento(procesm, etapa):
    etapaProcess = f"class {__name__} - def registra_processamento - {etapa}."
    # loga_mensagem(etapaProcess)

    try:
        loga_mensagem(etapa)
        procesm.desc_observacao_procesm_indice = etapa
        negProcesm = ProcesmClasse()
        negProcesm.atualizar_situacao_processamento(procesm)
        del negProcesm

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        raise
