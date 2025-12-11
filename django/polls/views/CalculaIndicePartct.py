import os
import sys
from datetime import datetime
from decimal import Decimal

import pandas as pd
from django.http import HttpResponse

from funcoes import utilitarios
from funcoes.constantes import EnumTipoIndice, EnumTipoProcessamento, EnumStatusProcessamento, EnumEtapaIndice
from funcoes.utilitarios import ultimo_dia_mes, loga_mensagem, sobe_csv, loga_mensagem_erro, converter_valor, LOCAL_TZ
from negocio.IndicePartct import IndicePartctClass
from negocio.Procesm import ProcesmClasse
from persistencia.Oraprd import Oraprd


def calculo_indice_partct(request, p_ano, p_etapa):
    etapaProcess = f"request {__name__} - def calculo_valor_agregado"

    try:
        etapaProcess = f'Processa carga de CT-es, conforme parâmetro do sistema.'
        neg = IndicePartctClass(p_ano, p_etapa)
        return HttpResponse(neg.calcula_indice_partct())
        # return HttpResponse(neg.operacoes_entre_produtores_rurais())

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        return HttpResponse(etapaProcess)


def carrega_indice_externo(request, p_ano, id_resolucao, p_etapa, p_tipo_indice, p_nome_arquivo, p_separador):
    etapaProcess = f'Carrega dados externos - Referencia: {p_ano} - Etapa: {p_etapa} - Tipo de Documento: {p_tipo_indice} - Nome do arquivo: {p_nome_arquivo} - Separador: {p_separador}.'
    loga_mensagem(etapaProcess)

    i_ano_referencia = int(os.environ.get('ANO_REFERENCIA'))
    i_inicio_referencia = int(os.environ.get('INICIO_REFERENCIA'))
    i_fim_referencia = int(os.environ.get('FIM_REFERENCIA'))
    sData_hora_inicio = datetime.now(LOCAL_TZ).strftime('%d/%m/%Y %H:%M:%S,%f')

    try:
        etapaProcess = f'Inclui arquivo com dados de índices externos para o ano {p_ano}.'
        d_data_ini = datetime.strptime(str(i_inicio_referencia * 100 + 1), '%Y%m%d').date()
        d_data_fim = ultimo_dia_mes(datetime.strptime(str(i_fim_referencia * 100 + 1), '%Y%m%d').date())

        # Registra inicio do processamento #
        etapaProcess = f'Registra inicio do processamento de importação de arquivo externo do VA. Periodo de {i_inicio_referencia} a {i_fim_referencia}'
        db = ProcesmClasse()
        procesm = db.iniciar_processamento(d_data_ini, d_data_fim, EnumTipoProcessamento.importacaoArquivoIndice.value)
        del db
        etapaProcess = f'{procesm.id_procesm_indice} Carga do arquivo de índices externos. Periodo {i_inicio_referencia} a {i_fim_referencia} - {sData_hora_inicio}'
        loga_mensagem(etapaProcess)

        # if p_ano != i_ano_referencia:
        #     etapaProcess = f'Ano de referencia do arquivo difere do ano de processamento {i_ano_referencia} do sistema!'
        #     raise Exception(etapaProcess)

        if p_etapa not in [item.value for item in EnumEtapaIndice]:
            etapaProcess = f'Etapa de cálculo {p_etapa} não prevista!'
            raise Exception(etapaProcess)

        if p_tipo_indice not in [item.value for item in EnumTipoIndice]:
            etapaProcess = f'Tipo de índice {p_tipo_indice} não previsto!'
            raise Exception(etapaProcess)

        df = sobe_csv(p_nome_arquivo, p_separador)
        if len(df) == 0:
            etapaProcess = f'Não foi possível ler o arquivo {p_nome_arquivo}!'
            raise Exception(etapaProcess)

        # Trata as colunas do arquivo
        df.columns = df.columns.str.strip()
        colunas_esperadas = {'ano': 'int64', 'etapa_indice': 'object', 'tipo_indice': 'object', 'indice_participacao': 'float64', 'codigo_municipio': 'int64'}
        colunas_faltantes = [coluna for coluna in colunas_esperadas if coluna not in df.columns]
        if colunas_faltantes:
            etapaProcess = f'As colunas {colunas_faltantes} não estão presentes no arquivo {p_nome_arquivo}!'
            raise Exception(etapaProcess)

        # if ~df['ano'].isin([i_ano_referencia]).all():
        #     etapaProcess = f'Arquivo {p_nome_arquivo} com Ano de referencia diferente do informado!'
        #     raise Exception(etapaProcess)

        df['etapa_indice'] = df['etapa_indice'].astype(object)
        if (df['etapa_indice'].astype(str) != p_etapa).any():
            etapaProcess = f'Arquivo {p_nome_arquivo} com a Etapa de Processamento diferente da informada!'
            raise Exception(etapaProcess)

        df['tipo_indice'] = df['tipo_indice'].astype(object)
        if (df['tipo_indice'].astype(str) != p_tipo_indice).any():
            etapaProcess = f'Arquivo {p_nome_arquivo} com o Tipo de Índice diferente do informado!'
            raise Exception(etapaProcess)

        # Trata valores não informados
        b_cancela = False
        valores_ausentes = df.isna().sum()
        for coluna, n_ausentes in valores_ausentes.items():
            if n_ausentes > 0:
                if coluna == 'indice_participacao' \
                or coluna == 'codigo_municipio':
                    loga_mensagem_erro(f"Coluna '{coluna}' com valores não informados. Quantidade de linhas: {n_ausentes}")
                    b_cancela = True

        if b_cancela:
            etapaProcess = f'Arquivo {p_nome_arquivo} com valores não informados!'
            raise Exception(etapaProcess)

        # Verificar se há valores duplicados na coluna codigo_municipio
        duplicados = df[df.duplicated(subset=['codigo_municipio'], keep=False)]
        if not duplicados.empty:
            etapaProcess = f'Arquivo {p_nome_arquivo} com municípios informados em mais de uma linha!'
            raise Exception(etapaProcess)

        # df['indice_participacao'] = df['indice_participacao'].apply(converter_valor)
        df['indice_participacao'] = df['indice_participacao'].str.replace(',', '.', regex=False).astype(float).round(16)
        # pd.set_option('display.float_format', '{:.16f}'.format)

        # Verifica o tipo de cada coluna
        for coluna, tipo_esperado in colunas_esperadas.items():
            tipo_real = df[coluna].dtype
            if str(tipo_real) != tipo_esperado:
                etapaProcess = f'Arquivo {p_nome_arquivo} com valores inválidos na coluna "{coluna}"!'
                raise Exception(etapaProcess)

        df['id_resolucao'] = id_resolucao
        df['indice_participacao'] = df['indice_participacao'].apply(lambda x: f"{x:.16f}")

        db = Oraprd()
        df_municipio = db.select_municipios()
        del db

        etapaProcess = "Merge entre arquivo e municípios."
        df = df.merge(df_municipio,  # Município de Entrada do GEN
                      left_on=['codigo_municipio'],
                      right_on=['codg_distrito'],
                      how='left',)

        df.drop(columns=['codg_distrito', 'nome_distrito', 'nome_municipio', 'codigo_municipio', ], inplace=True)
        df.rename(columns={'ano': 'numr_ano_ref'
                         , 'tipo_indice': 'tipo_indice_partcp'
                         , 'indice_participacao': 'indice'}, inplace=True)

        if (df['codg_uf'] != 'GO').any():
            etapaProcess = f'Arquivo {p_nome_arquivo} contem municípios fora de Goiás ou inválidos!'
            raise Exception(etapaProcess)

        etapaProcess = f'{procesm.id_procesm_indice} Grava índices externos - {utilitarios.formatar_com_espacos(len(df), 11)} linhas.'
        registra_processamento(procesm, etapaProcess)
        data_hora_atividade = datetime.now()

        if len(df) > 0:
            df['desc_motivo_geracao'] = etapaProcess
            df['id_procesm_indice'] = procesm.id_procesm_indice

            neg = IndicePartctClass(p_ano, 'D')
            linhas_gravadas = neg.grava_indice_partct(procesm, df)
            del neg

            etapaProcess = f'{procesm.id_procesm_indice} Importação de arquivo do indices externos para a referencia {p_ano} finalizado. Carregadas {len(df)} linhas.'
            procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
            procesm.stat_procesm_indice = EnumStatusProcessamento.sucesso.value
            registra_processamento(procesm, etapaProcess)
            codigo_retorno = 1

        else:
            etapaProcess = f'{procesm.id_procesm_indice} Importação de arquivo de Índices Externos ({p_ano}) finalizado. Não foram selecionados dados para importação.'
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
