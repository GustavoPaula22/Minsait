from datetime import datetime

import pandas as pd

from funcoes import utilitarios
from funcoes.constantes import EnumTipoDocumento, EnumTipoProcessamento, EnumStatusProcessamento, EnumParametros, \
    EnumMotivoExclusao
from funcoes.utilitarios import LOCAL_TZ
from negocio.DocPartct import DocPartctClasse
from negocio.GEN import GenClass
from negocio.Param import ParamClasse
from negocio.Procesm import ProcesmClasse

from negocio.CCE import *
# from persistencia.Mysql import MySQL
from persistencia.Oraprodx9 import Oraprodx9
from polls.models import Processamento


class SimplesClasse:
    etapaProcess = f"class {__name__} - class SimplesClasse."
    # loga_mensagem(etapaProcess)

    negProcesm = ProcesmClasse()

    def __init__(self):
        self.codigo_retorno = 0

        self.i_inicio_referencia = int(os.getenv('INICIO_REFERENCIA'))
        self.d_data_inicio_referencia = datetime.strptime(str(int(self.i_inicio_referencia) * 100 + 1), '%Y%m%d').date()
        self.i_fim_referencia = int(os.getenv('FIM_REFERENCIA'))
        self.d_data_fim_referencia = datetime.strptime(str(int(self.i_fim_referencia) * 100 + 31), '%Y%m%d').date()

        self.s_tipo_documento = EnumTipoDocumento.Simples.value

    def carga_simples(self, df) -> str:
        etapaProcess = f"class {self.__class__.__name__} - def carga_simples. Período de {self.i_inicio_referencia} a {self.i_fim_referencia}"
        # loga_mensagem(etapaProcess)

        dData_hora_inicio = datetime.now()
        sData_hora_inicio = datetime.now(LOCAL_TZ).strftime('%d/%m/%Y %H:%M:%S,%f')

        s_tipo_procesm = EnumTipoProcessamento.importacaoSimples.value

        try:
            # Registra inicio do processamento #
            etapaProcess = f'Registra inicio do processamento de carga de dados do Simples.'
            procesm = self.negProcesm.iniciar_processamento(self.d_data_inicio_referencia, self.d_data_fim_referencia, s_tipo_procesm)
            etapaProcess = f'{procesm.id_procesm_indice} Inicio da carga dos dados do Simples Nacional. Período de {self.i_inicio_referencia} a {self.i_fim_referencia} - {sData_hora_inicio}'
            loga_mensagem(etapaProcess)

            try:
                if len(df) > 0:
                    self.trabalha_simples(procesm, df)
                    self.codigo_retorno = 0

                else:
                    etapaProcess = f'{procesm.id_procesm_indice} Carga de arquivos do Simples Nacional de {self.i_inicio_referencia} a {self.i_fim_referencia} finalizada. Não foram selecionados dados do Simples habilitadas para cálculo no período.'
                    procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                    procesm.stat_procesm_indice = EnumStatusProcessamento.sucessoSemDados.value
                    self.registra_processamento(procesm, etapaProcess)
                    self.codigo_retorno = 1

                param = ParamClasse()
                param.atualizar_parametro(EnumParametros.ultimaCargaSimples.value, self.i_fim_referencia)

            except Exception as err:
                etapaProcess += " - ERRO - " + str(err)
                loga_mensagem_erro(etapaProcess)
                procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                procesm.stat_procesm_indice = EnumStatusProcessamento.erro.value
                self.registra_processamento(procesm, etapaProcess)
                self.codigo_retorno = 9
                raise

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            self.codigo_retorno = 9
            raise

        sData_hora_final = datetime.now(LOCAL_TZ).strftime('%d/%m/%Y %H:%M:%S,%f')
        loga_mensagem(
            f'{procesm.id_procesm_indice} Fim da carga do Simples. Período de {self.i_inicio_referencia} a {self.i_fim_referencia} - {sData_hora_final} - Tempo de processamento: {(datetime.now() - dData_hora_inicio)}.')
        loga_mensagem(' ')

        return str(self.codigo_retorno)

    def trabalha_simples(self, procesm: Processamento, df):
        etapaProcess = f"class {self.__class__.__name__} - def trabalha_simples."
        # loga_mensagem(etapaProcess)

        try:
            df['codg_tipo_doc'] = self.s_tipo_documento
            df['id_processamento'] = procesm.id_procesm_indice

            # Exclui dados de processamentos anteriores #
            self.exclui_dados_anteriores(procesm)

            # Filtra linhas para trabalho
            filtro = (df['PA'] >= self.i_inicio_referencia) & (df['PA'] <= self.i_fim_referencia)
            df_simples = df[filtro].copy()

            # Monta as chaves para pesquisa do último lote
            df_simples['IdDecl'] = df_simples['IdDecl'].astype(str)
            df_simples['IdDecl'] = df_simples['IdDecl'].str.zfill(17)

            df_simples['chave'] = df_simples['IdDecl'].str[:14]

            df_simples['cnpj_base'] = df_simples['IdDecl'].str[:8]
            df_simples['cnpj_base'] = df_simples['cnpj_base'].astype(int)

            df_simples['numero_sequencial'] = df_simples['IdDecl'].str[14:]
            df_simples['numero_sequencial'] = df_simples['numero_sequencial'].astype(int)

            # Obtem o último lote
            ultimo_lote = df_simples.groupby('chave')['numero_sequencial'].max().reset_index()

            # Separa os dados do último lote
            df_simples_final = pd.merge(df_simples,
                                        ultimo_lote,
                                        left_on=['chave', 'numero_sequencial'],
                                        right_on=['chave', 'numero_sequencial'])

            # Substituir strings vazias por '0'
            df_simples_final['VlrTot'] = df_simples_final['VlrTot'].replace('', '0')

            # Remover zeros à esquerda e substituir vírgulas por pontos
            df_simples_final['VlrTot'] = df_simples_final['VlrTot'].str.lstrip('0').str.replace(',', '.')

            # Converter para float, substituindo strings inválidas por NaN
            df_simples_final['VlrTot'] = pd.to_numeric(df_simples_final['VlrTot'], errors='coerce')

            # Substituir NaN por 0 (ou outro valor, se preferir)
            df_simples_final['VlrTot'] = df_simples_final['VlrTot'].fillna(0)

            # Filtra somente os tipos de receita pertinentes
            filtro = df_simples_final['TipoAtvSN'].isin([0, 1, 2, 3, 4, 5, 6, 34, 35, 36, 37, 38, 39])

            # Totaliza o valor por CNPJ, Referencia e Município
            df_simples_tot = df_simples_final[filtro].groupby(['IdDecl', 'CNPJ', 'PA', 'CodTOM'])\
                                                     .agg({'VlrTot': 'sum', }).reset_index()

            #  Calcula 32% do valor
            df_simples_tot['VlrTot32'] = df_simples_tot['VlrTot'] * 0.32

            db = Oraprd()
            df_municipio = db.select_municipio_siaf()
            df_cnpj = db.select_inscricao_pelo_cnpj(df_simples_tot['CNPJ'].drop_duplicates())
            del db

            if len(df_municipio) > 0:
                #  Concatena as informações dos municípios
                df_simples_merge = pd.merge(df_simples_tot,
                                            df_municipio,
                                            left_on=['CodTOM'],
                                            right_on=['codg_municipio_siaf'],
                                            how='left')
            else:
                loga_mensagem_erro(f"Não foram localizados municípios na tabela!")

            if len(df_cnpj) > 0:
                #  Concatena as informações das inscrições estaduais
                df_simples_merge['cnpj'] = df_simples_merge['CNPJ'].astype(str).str.zfill(14)
                df_simples_merge = df_simples_merge.merge(df_cnpj,
                                                          left_on=['cnpj'],
                                                          right_on=['numr_cnpj'],
                                                          how='left')
            else:
                loga_mensagem_erro(f"Não foram localizados CNPJs na tabela!")

            df_simples_merge.drop(columns=['codg_municipio_siaf',
                                           'CNPJ',
                                           'cnpj',
                                           'CodTOM',
                                           'VlrTot'], inplace=True)
            df_simples_merge.rename(columns={'VlrTot32': 'VlrTot'}, inplace=True)

            db = Oraprd()     # Persistência utilizando o oracledb
            df_ident = db.insert_ident_simples(df_simples_merge['codg_declaracao_pgdas', 'numr_cnpj'])
            del db

            df_simples =  pd.merge(df_simples_merge,
                                   df_ident,
                                   on=['codg_declaracao_pgdas'],
                                   how='left')





            etapaProcess = f'{procesm.id_procesm_indice} Totaliza documentos participantes.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()
            df_cte_chave = self.totaliza_doc_participante(df)
            etapaProcess = f'{procesm.id_procesm_indice} Filtra documentos participantes. {len(df_cte_chave)} documentos participantes. ' + str(
                datetime.now() - data_hora_atividade)
            self.registra_processamento(procesm, etapaProcess)

            if len(df_cte_chave) > 0:

                # df_cte_chave.name = 'df_cte_chave'
                # baixa_csv(df_cte_chave)

                # Reordena DataFrame de NF-es consolidado #
                etapaProcess = f'{procesm.id_procesm_indice} Reordenando documentos participantes.'
                self.registra_processamento(procesm, etapaProcess)
                data_hora_atividade = datetime.now()
                df_chave = self.ordena_colunas_doc_partct(df_cte_chave)
                loga_mensagem(etapaProcess + ' Processo finalizado: ' + str(datetime.now() - data_hora_atividade))

                # Grava documentos participantes #
                etapaProcess = f'{procesm.id_procesm_indice} Gravando Documentos Participantes -' + utilitarios.formatar_com_espacos(
                    len(df_chave), 11) + ' linhas.'
                self.registra_processamento(procesm, etapaProcess)
                data_hora_atividade = datetime.now()

                db = DocPartctClasse()
                linhas_inseridas = db.grava_doc_participante(df_chave)
                del db

                loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_inseridas} linhas inseridas - ' + str(
                    datetime.now() - data_hora_atividade))

                etapaProcess = f'{procesm.id_procesm_indice} Carga do Simples de {self.i_data_inicio} a {self.i_data_fim} finalizada. Carregadas {len(df_cte_chave)} linhas.'
                procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                procesm.stat_procesm_indice = EnumStatusProcessamento.sucesso.value
                self.registra_processamento(procesm, etapaProcess)

            else:
                etapaProcess = f'{procesm.id_procesm_indice} Carga do Simples de {self.i_data_inicio} a {self.i_data_fim} finalizada. Não foram selecionos dados para cálculo do IPM no período.'
                procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                procesm.stat_procesm_indice = EnumStatusProcessamento.sucessoSemDados.value
                self.registra_processamento(procesm, etapaProcess)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def exclui_dados_anteriores(self, procesm: Processamento):
        etapaProcess = f"class {self.__class__.__name__} - def exclui_dados_anteriores. Período de {self.i_inicio_referencia} a {self.i_fim_referencia}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f'{procesm.id_procesm_indice} Exclui documentos participantes.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            db = DocPartctClasse()
            linhas_excluidas = db.exclui_doc_participante(self.s_tipo_documento, self.d_data_inicio_referencia, self.d_data_fim_referencia)
            del db

            loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_excluidas} linhas excluidas - ' + str(
                datetime.now() - data_hora_atividade))

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def totaliza_doc_participante(self, df) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def totaliza_doc_participante."
        # loga_mensagem(etapaProcess)

        try:
            filtro = df['codg_chave_acesso_anulacao'].isna()
            df_tot = df[filtro].groupby(['id_documento_cte']).agg({'data_emissao_cte': 'first',
                                                                   'numr_ref_emissao': 'first',
                                                                   'codg_municipio_entrada': 'first',
                                                                   'codg_municipio_saida': 'first',
                                                                   'codg_tipo_doc': 'first',
                                                                   'id_processamento': 'first',
                                                                   'valr_va': 'sum'}).reset_index()

            # Elimina notas sem valores #
            filtro = (df_tot['valr_va'] > 0)
            df_tot = df_tot[filtro]

            # Renomeia colunas para chamada da rotina de busca de dados do CCE #
            df_tot['numr_ref_emissao'] = df_tot['numr_ref_emissao'].astype(int)

            return df_tot

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def ordena_colunas_doc_partct(self, df):
        etapaProcess = f"class {self.__class__.__name__} - def ordena_colunas_doc_nao_partct."
        # loga_mensagem(etapaProcess)

        try:
            df.rename(columns={'id_documento_cte': 'codg_documento_partct_calculo'}, inplace=True)
            df.rename(columns={'valr_va': 'valr_adicionado_operacao'}, inplace=True)
            df.rename(columns={'data_emissao_cte': 'data_emissao_documento'}, inplace=True)
            df.rename(columns={'codg_tipo_doc': 'codg_tipo_doc_partct_calc'}, inplace=True)
            df.rename(columns={'numr_ref_emissao': 'numr_referencia'}, inplace=True)
            df.rename(columns={'ie_entrada': 'numr_inscricao_entrada'}, inplace=True)
            df.rename(columns={'ie_saida': 'numr_inscricao_saida'}, inplace=True)
            df.rename(columns={'id_processamento': 'id_procesm_indice'}, inplace=True)
            df['id_contrib_ipm_entrada'] = pd.NA
            df['id_contrib_ipm_saida'] = pd.NA
            df['codg_tipo_doc_partct_calc'] = self.s_tipo_documento
            df['indi_aprop'] = 'S'

            return df

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def registra_processamento(self, procesm, etapa):
        etapaProcess = f"class {__name__} - def registra_processamento - {etapa}."
        # loga_mensagem(etapaProcess)

        try:
            loga_mensagem(etapa)
            procesm.desc_observacao_procesm_indice = etapa
            self.negProcesm.atualizar_situacao_processamento(procesm)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    # def ordena_colunas_doc_nao_partct(self, df) -> pd.DataFrame:
    #     etapaProcess = f"class {self.__class__.__name__} - def ordena_colunas_doc_nao_partct."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         df.drop(columns=['numr_ref_emissao',
    #                          'valr_va',
    #                          ], inplace=True)
    #
    #         df.rename(columns={'id_documento_cte': 'codg_documento_nao_partct_calc'}, inplace=True)
    #         df.rename(columns={'data_emissao_cte': 'data_emissao_documento'}, inplace=True)
    #         df.rename(columns={'codg_tipo_doc': 'codg_tipo_doc_partct_calc'}, inplace=True)
    #         df.rename(columns={'id_processamento': 'id_procesm_indice'}, inplace=True)
    #         df.rename(columns={'codg_motivo_exclusao': 'codg_motivo_exclusao_calculo'}, inplace=True)
    #
    #         nova_ordem_colunas = ['codg_documento_nao_partct_calc',
    #                               'data_emissao_documento',
    #                               'codg_tipo_doc_partct_calc',
    #                               'id_procesm_indice',
    #                               'codg_motivo_exclusao_calculo',
    #                               ]
    #         return df[nova_ordem_colunas]
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    # def totaliza_doc_nao_participante(self, df) -> pd.DataFrame:
    #     etapaProcess = f"class {self.__class__.__name__} - def totaliza_doc_nao_participante."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         filtro = df['codg_chave_acesso_anulacao'].isna()
    #         df_tot = df[~filtro].groupby(['id_documento_cte']).agg({'data_emissao_cte': 'first',
    #                                                                 'numr_ref_emissao': 'first',
    #                                                                 'codg_tipo_doc': 'first',
    #                                                                 'id_processamento': 'first',
    #                                                                 'valr_va': 'sum'}).reset_index()
    #
    #         # Elimina notas sem valores #
    #         filtro = (df_tot['valr_va'] > 0)
    #         df_tot = df_tot[filtro]
    #
    #         return df_tot
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    # def carrega_incricoes_cte(self, p_periodo):
    #     etapaProcess = f"class {__name__} - def carrega_incricoes_cte. Carrega dados cadastrais para as CT-es de {p_periodo}."
    #     # loga_mensagem(etapaProcess)
    #
    #     s_tipo_procesm = EnumTipoProcessamento.importacaoCCE.value
    #
    #     dData_hora_inicio = datetime.now()
    #     sData_hora_inicio = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S,%f')
    #     etapaProcess = f'Inicio da carga das informações cadastrais das CT-es para {p_periodo} - {sData_hora_inicio}'
    #     loga_mensagem(etapaProcess)
    #
    #     try:
    #         # Registra inicio do processamento #
    #         etapaProcess = 'Registra inicio do processamento de carga de informações cadastrais das CT-es'
    #         procesm = self.negProcesm.iniciar_processamento(s_tipo_procesm)
    #
    #         # Carrega as inscrições das NF-es geradas do período informado #
    #         etapaProcess = f'{procesm.id_procesm_indice} Busca as inscrições das CT-es para processamento - {p_periodo}. '
    #         self.registra_processamento(procesm, etapaProcess)
    #         data_hora_atividade = datetime.now()
    #
    #         db = Oraprd()
    #         df_cce = db.select_inscricoes_cte(p_periodo)
    #         del db
    #
    #         loga_mensagem(etapaProcess + f' Processo finalizado. {df_cce.shape[0]} linhas selecionadas. - ' + str(
    #             datetime.now() - data_hora_atividade))
    #
    #         if df_cce.shape[0] > 0:
    #             # df_cce.name = 'df_cce'
    #             # baixa_csv(df_cce)
    #             etapaProcess = f'{procesm.id_procesm_indice} Busca informações cadastrais das inscrições - {p_periodo}. '
    #             self.registra_processamento(procesm, etapaProcess)
    #             data_hora_atividade = datetime.now()
    #
    #             cce = CCEClasse()
    #             linhas_incluidas = cce.inclui_informacoes_cce(df_cce, p_periodo)
    #             del cce
    #             loga_mensagem(etapaProcess + f' Processo finalizado. Incluidas {linhas_incluidas} linhas - ' + str(datetime.now() - data_hora_atividade))
    #
    #             etapaProcess = f'Carga das informações cadastrais das CT-es para {p_periodo} finalizado. Carregadas 0 históricos.'
    #             loga_mensagem(str(procesm.id_procesm_indice) + ' ' + etapaProcess)
    #             procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
    #             procesm.stat_procesm_indice = EnumStatusProcessamento.sucesso.value
    #             self.registra_processamento(procesm, etapaProcess)
    #
    #         else:
    #             etapaProcess = f'Carga das informações cadastrais das CT-es para {p_periodo} finalizado. Não foram selecionadas inscrições para o período.'
    #             loga_mensagem(str(procesm.id_procesm_indice) + ' ' + etapaProcess)
    #             procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
    #             procesm.stat_procesm_indice = EnumStatusProcessamento.sucessoSemDados.value
    #             self.registra_processamento(procesm, etapaProcess)
    #
    #         etapaProcess = f'Cadastro de Inscrições no histórico finalizado. Carregadas 0 Inscrições para o período {p_periodo} - {str(datetime.now() - dData_hora_inicio)}'
    #         return etapaProcess
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    #     # loga_mensagem(f'Fim do processamento de atualização do cadastro: {sData_hora_fim} - '
    #     #               f'Tempo de processamento: {(dData_hora_fim - dData_hora_inicio)}.')
    #     # loga_mensagem(' ')
    #
    # def ordena_colunas_cte(df):
    #     # Ordena o dataframe #
    #     df = df.reindex(columns=['id_cte',
    #                              'ie_saida',
    #                              'ie_entrada',
    #                              'indi_tipo_operacao',
    #                              'numr_ano_mes_dia_emissao',
    #                              'numr_ref_emissao',
    #                              'codg_municipio_origem',
    #                              'codg_municipio_destino',
    #                              'desc_finalidade_operacao',
    #                              'desc_destino_operacao',
    #                              'codg_natureza_operacao',
    #                              'indi_natur_oper_particip',
    #                              'numr_sequencial_item',
    #                              'codg_cest',
    #                              'codg_anp',
    #                              'valr_total_nota',
    #                              'valr_frete',
    #                              'valr_seguro',
    #                              'valr_outra_despesa',
    #                              'valr_ipi',
    #                              'valr_base_icms',
    #                              'valr_base_icms_substrib',
    #                              'valr_total_produto',
    #                              'valr_total_desconto',
    #                              'valr_total_icms_desonerado',
    #                              'valr_desconto_item',
    #                              'valr_icms_desonerado',
    #                              'valr_base_icms_item',
    #                              'numr_cfop',
    #                              'indi_cfop_particip',
    #                              'stat_nota',
    #                              'indi_stat_nota_particip',
    #                              'data_carga',
    #                              'data_calc'])
    #     return df
    #
    # def ordena_colunas_cte_chave(df):
    #     # Ordena o dataframe #
    #     df = df.reindex(columns=['id_cte',
    #                              'ie_saida',
    #                              'ie_entrada',
    #                              'indi_tipo_operacao',
    #                              'numr_ano_mes_dia_emissao',
    #                              'numr_ref_emissao',
    #                              'valr_frete',
    #                              'valr_seguro',
    #                              'valr_outra_despesa',
    #                              'valr_ipi',
    #                              'valr_base_icms',
    #                              'valr_base_icms_substrib',
    #                              'valr_total_produto',
    #                              'valr_total_desconto',
    #                              'valr_total_icms_desonerado',
    #                              'valr_base_icms_item',
    #                              'valr_desconto_item',
    #                              'valr_icms_desonerado',
    #                              'codg_municipio_entrada',
    #                              'tipo_enqdto_entrada',
    #                              'situacao_cadastro_entrada',
    #                              'ind_prod_rural_entrada',
    #                              'codg_municipio_saida',
    #                              'tipo_enqdto_saida',
    #                              'situacao_cadastro_saida',
    #                              'ind_prod_rural_saida'])
    #     return df
    #
    # def totaliza_documento_cte(df_cte):
    #     # Aplicando um filtro na coluna 'cfop_particip'
    #     filtro = (df_cte['indi_cfop_particip'] == True) & (df_cte['indi_natur_oper_particip'] == True) & (
    #                 df_cte['indi_stat_nota_particip'] == True)
    #
    #     # Totalizando as notas #
    #     df_cte_tot = df_cte[filtro].groupby(['id_cte']).agg({'ie_saida': 'first',
    #                                                          'ie_entrada': 'first',
    #                                                          'indi_tipo_operacao': 'first',
    #                                                          'numr_ano_mes_dia_emissao': 'first',
    #                                                          'numr_ref_emissao': 'first',
    #                                                          'valr_frete': 'first',
    #                                                          'valr_seguro': 'first',
    #                                                          'valr_outra_despesa': 'first',
    #                                                          'valr_ipi': 'first',
    #                                                          'valr_base_icms': 'first',
    #                                                          'valr_base_icms_substrib': 'first',
    #                                                          'valr_total_produto': 'first',
    #                                                          'valr_total_desconto': 'first',
    #                                                          'valr_total_icms_desonerado': 'first',
    #                                                          'valr_base_icms_item': 'sum',
    #                                                          'valr_desconto_item': 'sum',
    #                                                          'valr_icms_desonerado': 'sum'}).reset_index()
    #
    #     # Elimina notas sem valores #
    #     filtro = (df_cte_tot['valr_total_produto'] > 0)
    #     df_cte_tot = df_cte_tot[filtro]
    #
    #     if df_cte_tot.shape[0] > 0:
    #         # Renomeia colunas para chamada da rotina de busca de dados do CCE #
    #         df_cte_tot['numr_ref_emissao'] = df_cte_tot['numr_ref_emissao'].astype(int)
    #         df_cte_tot['ie_saida'] = df_cte_tot['ie_saida'].astype(object)
    #         df_cte_tot['ie_entrada'] = df_cte_tot['ie_entrada'].astype(object)
    #
    #         negCFOP = CFOPClasse()
    #         df_cfop = negCFOP.busca_cfop_nfe()
    #         del negCFOP
    #
    #         # Altera o tipo da coluna #
    #         df_cte_tot['numr_cfop'] = df_cte_tot['numr_cfop'].astype(int)
    #         df_cfop['cfop'] = df_cfop['cfop'].astype(int)
    #         df_cte_tot['indi_cfop_particip'] = df_cte_tot['numr_cfop'].isin(df_cfop['cfop'])
    #
    #         return df_cte_tot
    #     else:
    #         return None
