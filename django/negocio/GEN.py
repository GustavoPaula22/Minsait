from funcoes.utilitarios import *
from persistencia.Oraprd import Oraprd
from persistencia.Oraprddw import OraprddwClass


class GenClass:
    etapaProcess = f"class {__name__} - class GEN."
    # loga_mensagem(etapaProcess)

    def __init__(self):
        self.inicio_referencia = os.getenv('INICIO_REFERENCIA')
        self.fim_referencia = os.getenv('FIM_REFERENCIA')

    #  Função para carregar informações de datas da DMGEN006, no período informado #
    def carrega_periodo_processamento(self, p_data_inicio, p_data_fim) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def carrega_periodo_processamento - {p_data_inicio} a {p_data_fim}."
        # loga_mensagem(etapaProcess)

        try:
            db = OraprddwClass()
            obj_retorno = db.select_dmgen006_intervalo(p_data_inicio, p_data_fim)
            del db

            datas = {'data_com_traco': [(str(item.numr_ano_mes_dia)[:4] + '-' +
                                         str(item.numr_ano_mes_dia)[4:6] + '-' +
                                         str(item.numr_ano_mes_dia)[6:]) for item in obj_retorno],
                     'data_so_numeros': [item.numr_ano_mes_dia for item in obj_retorno],
                     'data_formatada': [(str(item.numr_ano_mes_dia)[6:] + '/' +
                                         str(item.numr_ano_mes_dia)[4:6] + '/' +
                                         str(item.numr_ano_mes_dia)[:4]) for item in obj_retorno],
                     }
            return pd.DataFrame(datas)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    #  Função para carregar informações dos municipios a partir do código do IBGE #
    def carrega_municipio_ibge(self, munics) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def carrega_municipio_ibge."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_munic = db.select_municipio_ibge(munics)
            del db

            return df_munic

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    #  Função para carregar informações dos municipios a partir do código do GEN #
    def carrega_municipio_gen(self, munics) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def carrega_municipio_gen."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_munic = db.select_municipio_gen(munics)
            del db

            return df_munic

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    # def buscar_cfop_por_codigo(self, l_codigos:list):
    #     etapaProcess = f"class {self.__class__.__name__} - def buscar_cfop_por_codigo."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         db = Oraprd()
    #         resultados = db.select_cfop_ipm(str(inicio_referencia)[:4])
    #         del db
    #
    #         # # Realizar busca utilizando filter
    #         # resultados = Parametro.objects.using(self.db_alias).filter(sua_coluna__in=l_codigos)
    #
    #         # Retornar queryset de resultados
    #         return resultados
    #
    #     except Exception as err:
    #         # Tratar exceção
    #         print("Erro ao buscar por chaves:", err)
    #         raise
    #
    #  Funcção para carregar informações do CCE a partir dos dados da NF-e individual #
    # def inclui_informacoes_cce_1(self, df):
    #     etapaProcess = f"class {self.__class__.__name__} - def inclui_informacoes_cce_1"
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         oraprddw = OraprddwClass()
    #
    #         cce_busca = df[['numr_inscricao']].drop_duplicates()
    #         cce_busca.rename(columns={'numr_inscricao': 'ie'}, inplace=True)
    #
    #         # Lista para armazenar os DataFrames temporários
    #         dfs = []
    #
    #         for idx, row in cce_busca.iterrows():
    #             if row[0] != None:
    #                 if len(row[0]) == 9:
    #                     df_ret = oraprddw.select_historico_contrib(row[0])
    #                     if df_ret is not None:
    #                         # Pesquisa o período de referencia
    #                         mes_ref = self.inicio_referencia
    #                         while mes_ref <= self.fim_referencia:
    #                             # Pesquisa dataframe de retorno
    #                             for index, row in df_ret.iterrows():
    #                                 if int(row['anomesref']) < int(mes_ref):
    #                                     nova_linha = {'numr_inscricao': [row['numr_inscricao']]
    #                                         , 'mes_ref': [mes_ref]
    #                                         , 'codg_municipio_contrib': [row['codg_municipio_contrib']]
    #                                         , 'codg_tipo_enqdto': [row['codg_tipo_enqdto']]
    #                                         , 'codg_situacao_cadastro': [row['codg_situacao_cadastro']]}
    #                                     dfs.append(pd.DataFrame(data=nova_linha))
    #                                     break
    #                             mes_ref += 1
    #                     else:
    #                         loga_mensagem('Inscrição não encontrada!' + row[0])
    #                 else:
    #                     loga_mensagem('Inscrição de outro Estado!' + row[0])
    #             else:
    #                 loga_mensagem('Inscrição não Informada!' + row[0])
    #
    #         # Concatena todos os DataFrames da lista
    #         if len(dfs) > 0:
    #             df_cce = pd.concat(dfs, ignore_index=True)
    #
    #             df_cce.reset_index(inplace=True)
    #             df_cce.set_index(['numr_inscricao', 'mes_ref'], inplace=True)
    #             df_cce = df_cce.drop(columns=['index'])
    #
    #             merged_df = df.merge(df_cce,
    #                                  left_on=['numr_inscricao', 'ref_emissao'],
    #                                  right_on=['numr_inscricao', 'mes_ref'],
    #                                  how='left')
    #
    #             merged_df['codg_municipio_contrib'].fillna(0, inplace=True)
    #             merged_df['codg_municipio_contrib'] = merged_df['codg_municipio_contrib'].astype(int)
    #             merged_df['codg_tipo_enqdto'].fillna('Não cadastrado.', inplace=True)
    #             merged_df['codg_situacao_cadastro'].fillna('Não informado.', inplace=True)
    #
    #             return merged_df
    #
    #         else:
    #             return None
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    # def seleciona_inscricoes_unica(df):
    #     cce_pesq = df.sort_values(by=['ie'])
    #     cce_pesq['ie'] = cce_pesq['ie'].astype(str)
    #
    #     return cce_pesq['ie'].drop_duplicates()
    #
    #     cce_busca = Nfe.seleciona_inscricoes_nfe(df)
    #
    #     # Lista para armazenar os DataFrames temporários
    #     dfs = []
    #
    #     for item in cce_busca:
    #         if item != None:
    #             if len(item) == 9:
    #                 df_ret = Oraprddw.select_historico_contrib(item)
    #                 if df_ret is not None:
    #                     # Pesquisa o período de referencia
    #                     mes_ref = self.inicio_referencia
    #                     while mes_ref <= fim_referencia:
    #                         # Pesquisa dataframe de retorno
    #                         for index, row in df_ret.iterrows():
    #                             if int(row['anomesref']) < mes_ref:
    #                                 nova_linha = {'numr_inscricao': [row['numr_inscricao']]
    #                                     , 'mes_ref': [mes_ref]
    #                                     , 'codg_municipio_contrib': [row['codg_municipio_contrib']]
    #                                     , 'codg_tipo_enqdto': [row['codg_tipo_enqdto']]
    #                                     , 'codg_situacao_cadastro': [row['codg_situacao_cadastro']]}
    #                                 dfs.append(pd.DataFrame(data=nova_linha))
    #                                 break
    #                         mes_ref += 1
    #                 # else:
    #                 # loga_mensagem('Inscrição não encontrada!', item)
    #             # else:
    #             # loga_mensagem('Inscrição de outro Estado!', item)
    #         else:
    #             loga_mensagem('Inscrição não Informada!', item)
    #
    #     # Concatena todos os DataFrames da lista
    #     if len(dfs) > 0:
    #         df_cce = pd.concat(dfs, ignore_index=True)
    #
    #         df_cce.reset_index(inplace=True)
    #         df_cce.set_index(['numr_inscricao', 'mes_ref'], inplace=True)
    #         df_cce = df_cce.drop(columns=['index'])
    #
    #         merged_df = df.merge(df_cce,
    #                              left_on=['ie_entrada', 'numr_ref_emissao'],
    #                              right_on=['numr_inscricao', 'mes_ref'],
    #                              how='left')
    #
    #         merged_df.rename(columns={'codg_municipio_contrib': 'codg_municipio_entrada'}, inplace=True)
    #         merged_df.rename(columns={'codg_tipo_enqdto': 'tipo_enqdto_entrada'}, inplace=True)
    #         merged_df.rename(columns={'codg_situacao_cadastro': 'situacao_cadastro_entrada'}, inplace=True)
    #
    #         df = merged_df.merge(df_cce,
    #                              left_on=['ie_saida', 'numr_ref_emissao'],
    #                              right_on=['numr_inscricao', 'mes_ref'],
    #                              how='left')
    #
    #         df.rename(columns={'codg_municipio_contrib': 'codg_municipio_saida'}, inplace=True)
    #         df.rename(columns={'codg_tipo_enqdto': 'tipo_enqdto_saida'}, inplace=True)
    #         df.rename(columns={'codg_situacao_cadastro': 'situacao_cadastro_saida'}, inplace=True)
    #
    #         df['codg_municipio_entrada'].fillna(0, inplace=True)
    #         df['codg_municipio_entrada'] = df['codg_municipio_entrada'].astype(int)
    #         df['tipo_enqdto_entrada'].fillna(0, inplace=True)
    #         df['situacao_cadastro_entrada'].fillna(0, inplace=True)
    #
    #         df['codg_municipio_saida'].fillna(0, inplace=True)
    #         df['codg_municipio_saida'] = df['codg_municipio_saida'].astype(int)
    #         df['tipo_enqdto_saida'].fillna(0, inplace=True)
    #         df['situacao_cadastro_saida'].fillna(0, inplace=True)
    #
    #         return df
    #
    #     else:
    #         return None
