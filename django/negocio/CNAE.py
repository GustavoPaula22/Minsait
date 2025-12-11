import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from funcoes.constantes import inicio_referencia, fim_referencia
from funcoes.utilitarios import loga_mensagem_erro, loga_mensagem, baixa_csv, soma_dias, primeiro_dia_mes_seguinte, \
    sobe_csv
from persistencia.Oraprd import Oraprd
from persistencia.Oraprddw import OraprddwClass


class CNAEClasse:
    etapaProcess = f"class {__name__} - class CNAEClasse."
    # loga_mensagem(etapaProcess)

    def __init__(self):
        self.inicio_referencia = os.getenv('INICIO_REFERENCIA')
        self.data_inicio_referencia = datetime.strptime(str(int(self.inicio_referencia) * 100 + 1), '%Y%m%d').date()
        self.fim_referencia = os.getenv('FIM_REFERENCIA')
        self.data_fim_referencia = datetime.strptime(str(int(self.fim_referencia) * 100 + 31), '%Y%m%d').date()

    def carregar_cnae_rural(self, p_data_ref, df):
        etapaProcess = f"class {self.__class__.__name__} - def carregar_cnae_rural para a referencia {p_data_ref}."
        # loga_mensagem(etapaProcess)

        try:
            df['CODG_CNAE_RURAL_X'] = df['CODG_CNAE_RURAL'].astype(str).str.zfill(7)
            df.rename(columns={'CODG_CNAE_RURAL_X': 'CODIGO CNAE'}, inplace=True)
            etapaProcess += f' - {df.shape[0]} linhas lidas.'
            loga_mensagem(etapaProcess)

            db = Oraprd()
            df_cnae = db.select_cce_cnae(df['CODIGO CNAE'].tolist())
            etapaProcess += f' - {df_cnae.shape[0]} IDs lidos.'
            loga_mensagem(etapaProcess)

            df_cnae['data_inicio_vigencia'] = pd.to_datetime(p_data_ref, format='%Y%m%d')
            # linhas_excluidas = db.delete_ncm_rural(p_ano_ref)
            # etapaProcess += f' - {linhas_excluidas} excluídas.'

            linhas_incluidas = db.insert_cnae_rural(df_cnae)
            etapaProcess += f' - {linhas_incluidas} incluídos na tabela.'
            loga_mensagem(etapaProcess)
            del db

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def inclui_informacoes_cnae_rural(self, insc, p_ref):
        etapaProcess = f"class {self.__class__.__name__} - def inclui_informacoes_cnae_rural."
        loga_mensagem(etapaProcess)

        dfs = []
        i_inicio_ref = int(self.inicio_referencia)

        try:
            db = Oraprd()
            df_cnae_produtor = db.select_cnae_produtor()
            del db

            df_hist_cnae = self.busca_informacoes_historico_cnae(insc, p_ref)

            if len(df_hist_cnae) > 0:
                # Pesquisa alterações nos CNAEs durante o período vigente
                for mes in range(0, 13):
                    if mes != 12:
                        d_data_alteracao = pd.Timestamp(datetime.strptime(str((i_inicio_ref + mes) * 100 + 1), '%Y%m%d'))
                    else:
                        d_data_alteracao = pd.Timestamp(primeiro_dia_mes_seguinte(datetime.strptime(str((i_inicio_ref + mes - 1) * 100 + 1), '%Y%m%d')))

                    filtro = df_hist_cnae['data_alteracao'] < d_data_alteracao
                    df_cnae_vigente = df_hist_cnae[filtro].groupby(['numr_inscricao_contrib', 'codg_subcls']) \
                                                          .agg({'data_alteracao': 'last',
                                                                'indi_cnae_principal_contrib': 'first',
                                                                'perc_atividade_contrib': 'sum'}
                                                               ).reset_index()

                    # Exclui CNAEs sem percentual de atividade
                    filtro = df_cnae_vigente['perc_atividade_contrib'] > 0

                    # Faz o batimento entre os CNAEs dos contribuintes e os CNAEs de produtores rurais
                    df_cnae_vigente['codg_subcls'] = df_cnae_vigente['codg_subcls'].astype(int)
                    df_cnae_produtor['codg_cnae_rural'] = df_cnae_produtor['codg_cnae_rural'].astype(int)
                    df_cnae_merge = df_cnae_vigente[filtro].merge(df_cnae_produtor,
                                                                  left_on=['codg_subcls'],
                                                                  right_on=['codg_cnae_rural'],
                                                                  how='left')

                    df_cnae_merge.rename(columns={'codg_cnae_rural': 'produtor_rural'}, inplace=True)
                    df_cnae_merge['produtor_rural'] = df_cnae_merge['produtor_rural'].astype(object)

                    # Inscrições com todos os os CNAEs de produtor rural
                    prod_rural_exclusivo = df_cnae_merge.groupby('numr_inscricao_contrib')['produtor_rural'].apply(lambda x: x.notna().all()).reset_index()

                    # prod_rural_exclusivo = df_cnae_merge.groupby('numr_inscricao_contrib', as_index=False)['produtor_rural'].all()

                    prod_rural_exclusivo = prod_rural_exclusivo[prod_rural_exclusivo['produtor_rural'] == True]
                    prod_rural_exclusivo['indi_produtor_rural_exclusivo'] = 'S'
                    prod_rural_exclusivo.drop(columns=['produtor_rural', ], inplace=True)

                    # Inscrições com pelo menos um CNAE de produtor rural
                    prod_rural = df_cnae_merge.groupby('numr_inscricao_contrib').agg({'produtor_rural': lambda x: x.notna().any(),
                                                                                      'data_alteracao': 'max'}).reset_index()

                    prod_rural = prod_rural[prod_rural['produtor_rural'] == True]
                    prod_rural['indi_produtor_rural'] = 'S'
                    prod_rural.drop(columns=['produtor_rural', ], inplace=True)

                    prod_rural = prod_rural.merge(prod_rural_exclusivo,
                                                  on=['numr_inscricao_contrib'],
                                                  how='left')
                    prod_rural['indi_produtor_rural_exclusivo'].fillna('N', inplace=True)
                    # prod_rural['data_alteracao'] = d_data_alteracao

                    if not prod_rural.empty:
                        dfs.extend(prod_rural[['numr_inscricao_contrib'
                                             , 'data_alteracao'
                                             , 'indi_produtor_rural'
                                             , 'indi_produtor_rural_exclusivo']].to_dict(orient='records'))

                if len(dfs) > 0:
                    return pd.DataFrame(dfs).drop_duplicates()
                else:
                    return []
            else:
                return []

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def busca_informacoes_historico_cnae(self, insc, p_ref):
        etapaProcess = f"class {self.__class__.__name__} - def busca_informacoes_historico_cnae."
        # loga_mensagem(etapaProcess)

        dfs = []
        try:
            etapaProcess = f'Inicio busca históricos do CNAE - {len(insc)} linhas a pesquisar'
            db = OraprddwClass()
            df_ret = db.select_historico_cnae(insc, p_ref)
            etapaProcess += f' - {len(df_ret)} retornadas.'
            if len(df_ret) == 0:
                del db
                return []

            df_cnae = db.select_dmruc014(df_ret['id_subcls'].drop_duplicates().tolist())

            # df_cnae.name = 'df_cnae'
            # baixa_csv(df_cnae)

            if df_cnae.shape[0] > 0:
                # Concatena Retorno com códigos CNAE
                merged_df = pd.merge(df_ret, df_cnae,
                                     on=['id_subcls'],
                                     how='left')

                df_datas = db.select_dmgen004(pd.concat([df_ret['id_mes_inicio_vigencia'],
                                                         df_ret['id_mes_fim_vigencia']]).drop_duplicates().tolist())
                del db

                df_datas.name = 'df_datas'
                baixa_csv(df_datas)

                # loga_mensagem(f'Concatena Retorno com códigos CNAE. {df_cnae.shape[0]} - {df_datas.shape[0]}')

                if len(df_datas) > 0:
                    # Concatena Retorno com datas
                    merged_df = merged_df.merge(df_datas,
                                                left_on=['id_mes_inicio_vigencia'],
                                                right_on=['id_mes'],
                                                how='left')
                    merged_df.rename(columns={'numr_ano_mes': 'ref_inicio'}, inplace=True)

                    df_hist_cnae = merged_df.merge(df_datas,
                                                   left_on=['id_mes_fim_vigencia'],
                                                   right_on=['id_mes'],
                                                   how='left')
                    df_hist_cnae.rename(columns={'numr_ano_mes': 'ref_fim'}, inplace=True)

                    # Inclui coluna 'data_alteracao' com base na coluna 'ref_inicio'
                    df_hist_cnae['data_alteracao'] = pd.to_datetime(df_hist_cnae['ref_inicio']
                                                                    .replace({'0': '190001', 0: '190001'})     # Substitui '0' e 0 por '190001'
                                                                    .fillna('190001'),                         # Substitui NaN por '190001'
                                                                     format='%Y%m',                            # Especifica o formato da data original
                                                                     errors='coerce'                           # Trata erros de conversão
                                                                     ).dt.to_period('M').dt.to_timestamp('D')  # Converte para o primeiro dia do mês de referência

                    # Identificando alterações cadastrais finalizadas - Troca o sinal da coluna 'perc_atividade_contrib'
                    filtro = (df_hist_cnae['ref_fim'] != '0') & (df_hist_cnae['ref_fim'] != 0) & (~df_hist_cnae['ref_fim'].isna())
                    novas_linhas = df_hist_cnae[filtro].copy()
                    novas_linhas['perc_atividade_contrib'] = -novas_linhas['perc_atividade_contrib']

                    # Calculando o próximo dia para as novas linhas
                    novas_linhas['data_alteracao'] = (pd.to_datetime(df_hist_cnae[filtro]['ref_fim'], format='%Y%m')
                                                      .apply(lambda x: (x + pd.DateOffset(months=1)).replace(day=1)))

                    # Concatenando as novas linhas ao DataFrame original
                    df_final = pd.concat([df_hist_cnae, novas_linhas], ignore_index=True)

                    # Organiza resultado
                    df_final.drop(columns=['ref_inicio', 'ref_fim', ], inplace=True)
                    return df_final.sort_values(by=['numr_inscricao_contrib',
                                                    'codg_subcls',
                                                    'data_alteracao'])
                else:
                    return []

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def carregar_ativ_cnae(self, df) -> str:
        etapaProcess = f"class {self.__class__.__name__} - def carregar_ativ_cnae."
        # loga_mensagem(etapaProcess)

        try:
            df['CODIGO CNAE'] = df['CODIGO CNAE'].astype(str).str.zfill(7)
            etapaProcess += f' - {df.shape[0]} linhas lidas.'
            loga_mensagem(etapaProcess)

            db = Oraprd()
            df_cnae = db.select_cce_cnae(df['CODIGO CNAE'].tolist())
            etapaProcess += f' - {df_cnae.shape[0]} IDs lidos.'
            loga_mensagem(etapaProcess)

            df_sai = pd.merge(df, df_cnae, left_on=['CODIGO CNAE'], right_on=['codg_subclasse_cnaef'], how='left')
            df_sai = df_sai.drop(columns=['CODIGO CNAE', 'codg_subclasse_cnaef'])
            df_sai.rename(columns={'RAMO ATIVIDADE': 'indi_ramo_atividade'}, inplace=True)

            # df_sai.name = 'df_sai'
            # baixa_csv(df_sai)

            linhas_incluidas = db.insert_ramo_ativ_cnae(df_sai)
            etapaProcess += f' - Inclusão de CNAEs por ramo de atividade: {linhas_incluidas}'
            loga_mensagem(etapaProcess)
            del db

            return etapaProcess

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise


    # def inserir_cnae_rural(self, p_data_ref, df) -> str:
    #     etapaProcess = f"class {self.__class__.__name__} - def carregar_cnae_rural para a referencia {p_data_ref}."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         db = Oraprd()
    #         # linhas_excluidas = db.delete_cfop_partct(p_ano_ref)
    #         # etapaProcess += f' - Exclusão de CFOPs existentes: {linhas_excluidas}'
    #         # loga_mensagem(etapaProcess)
    #
    #         df['data_inicio_vigencia'] = pd.to_datetime(p_data_ref, format='%Y%m%d')
    #         linhas_incluidas = db.insert_cfop_rural(df)
    #         etapaProcess += f' - Inclusão de CFOPs: {linhas_incluidas}'
    #         loga_mensagem(etapaProcess)
    #         del db
    #
    #         return etapaProcess
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    #  Funcção para carregar informações do CNAE a partir dos dados da NF-e individual #
    # def inclui_informacoes_cnae_rural_unico(self, df):
    #     etapaProcess = f"class {self.__class__.__name__} - def inclui_informacoes_cnae_rural_unico."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         # Instancia as classes envolvidas #
    #         oraprddw = OraprddwClass()
    #         dbMySQL = Mysql()
    #
    #         # Obtem as IEs envolvidas #
    #         cce_busca = seleciona_inscricoes(df)
    #
    #         # Lista para armazenar os DataFrames temporários
    #         dfs = []
    #
    #         for item in cce_busca:
    #             if item is not None:
    #                 if len(item) == 9:
    #                     df_ret = oraprddw.select_historico_contrib(item)
    #                     if df_ret is not None:
    #                         # Pesquisa o período de referencia
    #                         mes_ref = inicio_referencia
    #                         while mes_ref <= fim_referencia:
    #                             # Pesquisa dataframe de retorno
    #                             for index, row in df_ret.iterrows():
    #                                 if int(row['ref_inicio']) <= int(mes_ref) and (
    #                                         int(row['ref_fim']) >= int(mes_ref) or int(row['ref_fim']) == 0):
    #                                     nova_linha = {'numr_inscricao': [row['numr_inscricao']],
    #                                                   'mes_ref': [mes_ref],
    #                                                   'perc_atividade': [row['perc_atividade']],
    #                                                   'indi_cnae_principal': [row['indi_cnae_principal']],
    #                                                   'codg_subcls': [row['codg_subcls']],
    #                                                   }
    #                                     dfs.append(pd.DataFrame(data=nova_linha))
    #                             mes_ref += 1
    #                     else:
    #                         loga_mensagem('Inscrição não encontrada!' + str(item))
    #                 # else:
    #                 # loga_mensagem('Inscrição de outro Estado!' + str(item))
    #             else:
    #                 loga_mensagem('Inscrição não Informada!' + str(item))
    #
    #         df_cnae = pd.concat(dfs, ignore_index=True)
    #         df_cnae['codg_subcls'] = df_cnae['codg_subcls'].astype(int)
    #
    #         # Buscar as incrições que tem CNAE de produtor rural #
    #         ######################################################
    #         df_cnae_produtor = dbMySQL.select_cnae_produtor()
    #         df_cnae_produtor['codg_cnae_rural'] = df_cnae_produtor['codg_cnae_rural'].astype(int)
    #
    #         df_cnae = df_cnae.merge(df_cnae_produtor,
    #                                 left_on=['codg_subcls'],
    #                                 right_on=['codg_cnae_rural'],
    #                                 how='left')
    #
    #         df_cnae.rename(columns={'codg_cnae_rural': 'produtor_rural'}, inplace=True)
    #         df_cnae['produtor_rural'] = df_cnae['produtor_rural'].astype(object)
    #
    #         df_cce_produtor = df_cnae[['numr_inscricao', 'mes_ref', 'produtor_rural']].drop_duplicates()
    #         df_cce_produtor = df_cce_produtor.loc[~df_cce_produtor['produtor_rural'].isna()]
    #         df_cce_produtor = df_cce_produtor.drop_duplicates(subset=['numr_inscricao', 'mes_ref'], keep='first')
    #         df_cce_produtor['produtor_rural'] = True
    #
    #         return df_cce_produtor
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    #  Funcção para carregar informações do CNAE a partir dos dados da NF-e individual #
    # def inclui_informacoes_cnae_rural_multi(self, df):
    #     etapaProcess = f"class {self.__class__.__name__} - def inclui_informacoes_cnae_rural_multi."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         # Instancia as classes envolvidas #
    #         oraprddw = OraprddwClass()
    #         dbMySQL = Mysql()
    #
    #         # Obtem as IEs envolvidas #
    #         cce_busca = seleciona_inscricoes_nfe(df)
    #
    #         # Lista para armazenar os DataFrames temporários
    #         dfs = []
    #         contador = 1
    #         inscricoes = '('
    #
    #         for item in cce_busca:
    #             if item is not None:
    #                 if len(item) == 9:
    #                     inscricoes = inscricoes + str(item) + ','
    #                     if contador >= 990:
    #                         inscricoes = inscricoes[:-1] + ')'
    #                         df_ret = oraprddw.select_historico_contrib(inscricoes)
    #                         if df_ret is not None:
    #                             # Pesquisa o período de referencia
    #                             mes_ref = inicio_referencia
    #                             while mes_ref <= fim_referencia:
    #                                 # Pesquisa dataframe de retorno
    #                                 for index, row in df_ret.iterrows():
    #                                     if int(row['ref_inicio']) <= int(mes_ref) and (
    #                                             int(row['ref_fim']) >= int(mes_ref) or int(row['ref_fim']) == 0):
    #                                         nova_linha = {'numr_inscricao': [row['numr_inscricao']],
    #                                                       'mes_ref': [mes_ref],
    #                                                       'perc_atividade': [row['perc_atividade']],
    #                                                       'indi_cnae_principal': [row['indi_cnae_principal']],
    #                                                       'codg_subcls': [row['codg_subcls']],
    #                                                       }
    #                                         dfs.append(pd.DataFrame(data=nova_linha))
    #                                 mes_ref += 1
    #                         else:
    #                             loga_mensagem('Inscrição não encontrada!' + str(inscricoes))
    #                         contador = 0
    #                         inscricoes = '('
    #                     contador += 1
    #
    #                 # else:
    #                 # loga_mensagem('Inscrição de outro Estado!', item)
    #             else:
    #                 loga_mensagem('Inscrição não Informada!' + str(item))
    #
    #         inscricoes = inscricoes[:-1] + ')'
    #         del df_ret
    #         df_ret = oraprddw.select_historico_contrib(inscricoes)
    #         if df_ret is not None:
    #             # Pesquisa o período de referencia
    #             mes_ref = inicio_referencia
    #             while mes_ref <= fim_referencia:
    #                 # Pesquisa dataframe de retorno
    #                 for index, row in df_ret.iterrows():
    #                     if int(row['ref_inicio']) <= int(mes_ref) and (int(row['ref_fim']) >= int(mes_ref) or int(row['ref_fim']) == 0):
    #                         nova_linha = {'numr_inscricao': [row['numr_inscricao']],
    #                                       'mes_ref': [mes_ref],
    #                                       'perc_atividade': [row['perc_atividade']],
    #                                       'indi_cnae_principal': [row['indi_cnae_principal']],
    #                                       'codg_subcls': [row['codg_subcls']],
    #                                       }
    #                         dfs.append(pd.DataFrame(data=nova_linha))
    #                 mes_ref += 1
    #         else:
    #             loga_mensagem('Inscrições não encontradas!' + str(inscricoes))
    #
    #         df_cnae = pd.concat(dfs, ignore_index=True)
    #         df_cnae['codg_subcls'] = df_cnae['codg_subcls'].astype(int)
    #
    #         # Buscar as incrições que tem CNAE de produtor rural #
    #         ######################################################
    #         df_cnae_produtor = dbMySQL.select_cnae_produtor()
    #         df_cnae_produtor['codg_cnae_rural'] = df_cnae_produtor['codg_cnae_rural'].astype(int)
    #
    #         df_cnae = df_cnae.merge(df_cnae_produtor,
    #                                 left_on=['codg_subcls'],
    #                                 right_on=['codg_cnae_rural'],
    #                                 how='left')
    #
    #         df_cnae.rename(columns={'codg_cnae_rural': 'produtor_rural'}, inplace=True)
    #         df_cnae['produtor_rural'] = df_cnae['produtor_rural'].astype(object)
    #
    #         df_cce_produtor = df_cnae[['numr_inscricao', 'mes_ref', 'produtor_rural']].drop_duplicates()
    #         df_cce_produtor = df_cce_produtor.loc[~df_cce_produtor['produtor_rural'].isna()]
    #         df_cce_produtor = df_cce_produtor.drop_duplicates(subset=['numr_inscricao', 'mes_ref'], keep='first')
    #         df_cce_produtor['produtor_rural'] = True
    #
    #         # Merge das informações de produtor rural com as NF-es #
    #         ########################################################
    #         df_nfe_cce = df.merge(df_cce_produtor,
    #                               left_on=['ie_entrada', 'numr_ref_emissao'],
    #                               right_on=['numr_inscricao', 'mes_ref'],
    #                               how='left')
    #         df_nfe_cce.rename(columns={'produtor_rural': 'ind_prod_rural_entrada'}, inplace=True)
    #         df_nfe_cce['ind_prod_rural_entrada'].fillna(False, inplace=True)
    #         df_nfe_cce = df_nfe_cce.drop(columns=['numr_inscricao', 'mes_ref'])
    #
    #         df_nfe_cce = df_nfe_cce.merge(df_cce_produtor,
    #                                       left_on=['ie_saida', 'numr_ref_emissao'],
    #                                       right_on=['numr_inscricao', 'mes_ref'],
    #                                       how='left')
    #         df_nfe_cce.rename(columns={'produtor_rural': 'ind_prod_rural_saida'}, inplace=True)
    #         df_nfe_cce['ind_prod_rural_saida'].fillna(False, inplace=True)
    #         df_nfe_cce = df_nfe_cce.drop(columns=['numr_inscricao', 'mes_ref'])
    #
    #         return df_nfe_cce
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    #  Função para carregar informações do CCE a partir dos dados da NF-e individual #
    # def inclui_informacoes_cnae_por_referencia(self, df, p_referencia):
    #     etapaProcess = f"class {self.__class__.__name__} - def inclui_informacoes_cnae_por_referencia - {p_referencia}"
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         oraprddw = OraprddwClass()
    #
    #         cce_busca = seleciona_inscricoes(df)
    #         etapaProcess += f" - Quantidade de inscrições: {cce_busca.shape[0]}"
    #         loga_mensagem(etapaProcess)
    #
    #         # Lista para armazenar os DataFrames temporários
    #         dfs = []
    #
    #         for item in cce_busca:
    #             if item is not None:
    #                 if len(item) == 9:
    #                     df_ret = oraprddw.select_historico_cnae(item)
    #                     if df_ret is not None:
    #                         # Pesquisa dataframe de retorno
    #                         for index, row in df_ret.iterrows():
    #                             if int(row['ref_inicio']) <= p_referencia and (
    #                                     int(row['ref_fim']) >= p_referencia or int(row['ref_fim']) == 0):
    #                                 nova_linha = {'numr_inscricao': [row['numr_inscricao_contrib']],
    #                                               'mes_ref': [p_referencia],
    #                                               'perc_atividade': [row['perc_atividade_contrib']],
    #                                               'indi_cnae_principal': [row['indi_cnae_principal_contrib']],
    #                                               'codg_subcls': [row['codg_subcls']],
    #                                               }
    #                                 dfs.append(pd.DataFrame(data=nova_linha))
    #                     else:
    #                         loga_mensagem('Inscrição não encontrada!' + str(item))
    #                 # else:
    #                 # loga_mensagem('Inscrição de outro Estado!' + str(item))
    #             else:
    #                 loga_mensagem('Inscrição não Informada!' + str(item))
    #
    #         df_cnae = pd.concat(dfs, ignore_index=True)
    #
    #         etapaProcess += f" - Quantidade de linhas do df_cnae: {df_cnae.shape[0]}"
    #         loga_mensagem(etapaProcess)
    #
    #         df_cnae['codg_subcls'] = df_cnae['codg_subcls'].astype(int)
    #
    #         # Buscar as incrições que tem CNAE de produtor rural #
    #         ######################################################
    #         dbMySQL = Mysql()
    #         df_cnae_produtor = dbMySQL.select_cnae_produtor()
    #         df_cnae_produtor['CODG_CNAE_RURAL'] = df_cnae_produtor['CODG_CNAE_RURAL'].astype(int)
    #
    #         df_cnae = df_cnae.merge(df_cnae_produtor,
    #                                 left_on=['codg_subcls'],
    #                                 right_on=['CODG_CNAE_RURAL'],
    #                                 how='left')
    #
    #         df_cnae.rename(columns={'CODG_CNAE_RURAL': 'produtor_rural'}, inplace=True)
    #         df_cnae['produtor_rural'] = df_cnae['produtor_rural'].astype(object)
    #
    #         df_cce_produtor = df_cnae[['numr_inscricao', 'mes_ref', 'produtor_rural']].drop_duplicates()
    #         df_cce_produtor = df_cce_produtor.loc[~df_cce_produtor['produtor_rural'].isna()]
    #         df_cce_produtor = df_cce_produtor.drop_duplicates(subset=['numr_inscricao', 'mes_ref'], keep='first')
    #         df_cce_produtor['produtor_rural'] = True
    #
    #         return df_cce_produtor
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    # def grava_informacoes_cnae_referencia(self, p_inscricao, p_referencia):
    #     etapaProcess = f"class {self.__class__.__name__} - def inclui_informacoes_cce_por_referencia: {p_inscricao} - {p_referencia}"
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         dbMySQL = Mysql()
    #         if p_inscricao is not None:
    #             df_ret = dbMySQL.update_cadastro_nfe_cnae(p_inscricao, p_referencia, True)
    #         else:
    #             loga_mensagem(f'Inscrição não Informada - {p_inscricao}')
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    #     return None
