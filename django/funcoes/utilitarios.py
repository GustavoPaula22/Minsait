import calendar
import locale
import os

import numpy as np
import pandas as pd
from pathlib import Path

import logzero
import psutil
import sys

import pytz
from dotenv import load_dotenv
import re

from logzero import logger

from datetime import datetime, timedelta, date

LOCAL_TZ = pytz.timezone('America/Sao_Paulo')


def __init__():
    load_dotenv(verbose=True, encoding='latin-1')
    log_directory = os.environ.get('LOG_DIR')
    #gerando arquivo de log diário
    logzero.logfile(os.path.join(Path(log_directory)) + "/loga_mensagem.log", maxBytes=1e8, backupCount=100)
    loga_mensagem(f"Iniciando configurações (IPM) - Ambiente {sys.platform}")

    if sys.platform == "linux":
        pt_locale = "pt_BR.utf8"
    elif sys.platform == "win32":
        pt_locale = "Portuguese_Brazil.1252"  # Tente com "Portuguese_Brazil.1252" ou "Portuguese_Brazil"
    else:
        pt_locale = "pt_BR"

    locale.setlocale(locale.LC_ALL, pt_locale)


def format_number(n):
    """ Retorna o numero formatado, ex: 3.123.123,99 """
    return locale.format_string("%d", n, grouping=True)


def memory_usage():
    """ Retorna a memória utilizada em pelo python em GB """
    process = psutil.Process(os.getpid())
    mem = process.memory_info()[0] / float(2 ** 30)
    return mem


def registra_tempo_processos(processo, dt_inicio, dt_fim):
    caminho = os.environ.get('LOG_DIR') + "/resultado_processamento.csv"
    # loga_mensagem(caminho)
    f = open(caminho, "a")
    linha = str(processo) + ';' + str(dt_inicio)+';' + str(dt_fim) + ';' + str(dt_fim-dt_inicio)
    f.write(linha+'\n')
    f.close()
    # loga_mensagem(f"Acabou o registra_tempo_processos... {dt_inicio} a {dt_fim}")


def loga_mensagem(msg):
    timestamp = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S %z')
    logger.info("[{}] {:.1f}Gb: {}".format(timestamp, memory_usage(), msg))


def loga_mensagem_erro(msg='', err=None):
    timestamp = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S %z')
    if msg:
        # logger.error("{:.1f}Gb: {}".format(memory_usage(), msg))
        logger.error("[{}] {:.1f}Gb: {}".format(timestamp, memory_usage(), msg))
    if err:
        logger.exception(err)


def remover_nao_numericos(valor):
    if valor in (None, "", " ", "nadda"):
        valor = '0'
    valor = str(valor)
    valor = valor.replace(' ', '')
    valor = valor.strip()

    valor = valor.replace('\W+', '')
    valor = re.sub(r'\s+', '', valor)

    valor = re.sub('[^0-9 \\\]', '', valor)
    if valor in (None, "", " ", "nadda"):
        valor = '0'
    valor = int(valor)
    valor = str(valor)
    if (len(valor) > 14):
        valor = valor[0:14]
    return valor


# Função para converter valores com separadores de milhar e decimal
def converter_valor(valor):
    if ',' in valor and '.' in valor:
        return float(valor.replace('.', '').replace(',', '.'))
    elif ',' in valor:
        return float(valor.replace(',', ''))
    else:
        return float(valor)


def baixa_csv(df):
    etapaProcess = f"class {__name__} - class baixa_csv."
    # loga_mensagem(etapaProcess)

    try:
        if sys.platform == "linux":
            diretorio = os.environ.get('STAGING_DIR_LINUX')
        else:
            diretorio = os.environ.get('STAGING_DIR_WIN')

        # Verifica se o diretório existe; caso contrário, cria-o
        if not os.path.exists(diretorio):
            os.makedirs(diretorio)  # Cria o diretório

        nome_arquivo = df.name + '.csv'

        # Caminho completo do arquivo
        caminho_completo = os.path.join(diretorio, nome_arquivo)

        # Gravar o DataFrame em um arquivo CSV no caminho completo
        df.to_csv(caminho_completo, index=False, sep='|', decimal=',')

        return None

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)


def sobe_csv(p_nome_arquivo, p_separador='|', p_low_memory=False) -> pd.DataFrame:
    etapaProcess = f"class {__name__} - class sobe_csv."
    # loga_mensagem(etapaProcess)

    try:
        df = pd.DataFrame()

        if sys.platform == "linux":
            diretorio = os.environ.get('STAGING_DIR_LINUX')
        else:
            diretorio = os.environ.get('STAGING_DIR_WIN')

        # Caminho completo do arquivo
        caminho_completo = os.path.join(diretorio, p_nome_arquivo)

        df = pd.read_csv(caminho_completo, sep=p_separador, encoding='latin-1', low_memory=p_low_memory)

        loga_mensagem(f'Arquivo {caminho_completo} carregado. Foram carregadas {len(df)} linhas.')

    except FileNotFoundError:
        etapaProcess += f" - ERRO - O arquivo '{caminho_completo}' não foi encontrado."
        loga_mensagem_erro(etapaProcess)

    except pd.errors.EmptyDataError:
        etapaProcess += f" - ERRO - O arquivo '{caminho_completo}' está vazio."
        loga_mensagem_erro(etapaProcess)

    except pd.errors.ParserError:
        etapaProcess += f" - ERRO - O arquivo '{caminho_completo}' não está no formato CSV válido."
        loga_mensagem_erro(etapaProcess)

    except PermissionError:
        etapaProcess += f" - ERRO - Permissão negada para acessar o arquivo {caminho_completo}"
        loga_mensagem_erro(etapaProcess)

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)

    return df


def formatar_com_espacos(numero, tamanho):
    import locale

    # Defina a localização para usar o formato padrão do sistema
    locale.setlocale(locale.LC_ALL, '')

    numero1 = locale.format_string("%d", numero, grouping=True)
    return f"{numero1:>{tamanho}}"


def dividir_dia_em_horas(pDdataRef, pQtdeInterval):
    # Converter a string de data inicial para um objeto datetime
    data_inicial = datetime.strptime(pDdataRef, '%Y-%m-%d %H:%M:%S')

    # Calcular a diferença de horas com base na quantidade de intervalos
    diferenca_horas = 24 / pQtdeInterval

    # Criar uma lista de datas com base nos intervalos
    datas_iniciais = [data_inicial + timedelta(hours=i * diferenca_horas) for i in range(pQtdeInterval)]

    # Adicionar 1 segundo a cada data inicial para obter as datas finais
    datas_finais = [inicio + timedelta(hours=diferenca_horas, seconds=-1) for inicio in datas_iniciais]

    # Criar um DataFrame com as datas
    return  pd.DataFrame({'dDataInicial': datas_iniciais, 'dDataFinal': datas_finais})


def primeiro_dia_mes_seguinte(p_data):
    # Verifica se o mês é dezembro
    if p_data.month == 12:
        primeiro_dia_proximo_mes = date(p_data.year + 1, 1, 1)
    else:
        primeiro_dia_proximo_mes = date(p_data.year, p_data.month + 1, 1)
    return primeiro_dia_proximo_mes


def ultimo_dia_mes(p_data):
    # Obter o ano e o mês da data fornecida
    ano = p_data.year
    mes = p_data.month

    # Obter o último dia do mês
    ultimo_dia = calendar.monthrange(ano, mes)[1]

    # Retornar a data do último dia do mês
    return date(ano, mes, ultimo_dia)


def soma_dias(p_data, p_dias, p_segundos=0):
    # Adiciona dias em uma data
    return p_data + timedelta(days=p_dias, seconds=p_segundos)


#  Função para selecionar inscrições estaduais para pesquisa no cadastro #
def seleciona_inscricoes_doc(df):
    etapaProcess = f"class {__name__} - def seleciona_inscricoes_doc."
    # loga_mensagem(etapaProcess)

    try:
        # Concatenar as colunas 'ie_entrada' e 'ie_saida e respectivas unidades federativas'
        cce = pd.DataFrame({'ie': df['ie_entrada'].tolist() + df['ie_saida'].tolist(),
                            'codg_municipio': df['codg_municipio_entrada'].tolist() + df['codg_municipio_saida'].tolist(),
                            'codg_uf': df['codg_uf_entrada'].tolist() + df['codg_uf_saida'].tolist()})

        # cce = cce.drop_duplicates()
        cce['codg_uf'] = cce['codg_uf'].fillna('GO')
        cce = cce.groupby(['ie', 'codg_uf']).agg({'codg_municipio': 'first'}).reset_index()

        # Remover valores nulos e inválidos
        cce = cce.dropna(subset=['ie'])
        cce = cce[cce['ie'] != 0]
        cce = cce[cce['ie'] != '0']

        # Converter valores para int
        cce['ie'] = cce['ie'].astype(np.int64)
        cce = cce.sort_values(by='ie').reset_index(drop=True)

        return cce

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        raise

    # cce_pesq1 = df[['ie_entrada', 'numr_ref_emissao']]
    # # Remover linhas com valores nulos na coluna 'ie_entrada'
    # cce_pesq1 = cce_pesq1.dropna(subset=['ie_entrada'])
    # # Remover linhas com valores zeros na coluna 'ie_entrada'
    # cce_pesq1 = cce_pesq1[cce_pesq1['ie_entrada'] != 0]
    # cce_pesq1 = cce_pesq1[cce_pesq1['ie_entrada'] != '0']
    # # Remover duplicatas após a remoção de nulos e zeros
    # cce_pesq1 = cce_pesq1.drop_duplicates()
    # cce_pesq1.rename(columns={'ie_entrada': 'ie'}, inplace=True)
    #
    # cce_pesq2 = df[['ie_saida', 'numr_ref_emissao']]
    # # Remover linhas com valores nulos na coluna 'ie_entrada'
    # cce_pesq2 = cce_pesq2.dropna(subset=['ie_saida'])
    # # Remover linhas com valores zeros na coluna 'ie_entrada'
    # cce_pesq2 = cce_pesq2[cce_pesq2['ie_saida'] != 0]
    # cce_pesq2 = cce_pesq2[cce_pesq2['ie_saida'] != '0']
    # # Remover duplicatas após a remoção de nulos e zeros
    # cce_pesq2 = cce_pesq2.drop_duplicates()
    # cce_pesq2.rename(columns={'ie_saida': 'ie'}, inplace=True)
    #
    # cce_pesq = pd.concat([cce_pesq1, cce_pesq2]).drop_duplicates()
    # cce_pesq = cce_pesq.sort_values(by=['ie', 'numr_ref_emissao'])
    # # cce_pesq['ie'] = cce_pesq['ie'].astype(str).str.zfill(9)
    # return cce_pesq['ie'].drop_duplicates()


def seleciona_inscricoes(df) -> pd.DataFrame:
    etapaProcess = f"class {__name__} - def seleciona_inscricoes."
    # loga_mensagem(etapaProcess)

    cce_pesq = df.sort_values(by=['numr_inscricao'])
    cce_pesq['numr_inscricao'] = cce_pesq['numr_inscricao'].astype(str)

    return cce_pesq['numr_inscricao'].drop_duplicates()


def seleciona_cnpjs(df):
    etapaProcess = f"class {__name__} - def seleciona_cnpjs."
    # loga_mensagem(etapaProcess)

    dfs = []
    for linha in df['numr_cpf_cnpj_dest'].drop_duplicates():
        if not pd.isna(linha):
            if len(linha) == 14:
                dfs.append(linha)

    if len(dfs) > 0:
        return dfs
    else:
        return []


__init__()