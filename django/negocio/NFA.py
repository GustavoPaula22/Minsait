from datetime import datetime

from funcoes import utilitarios
from funcoes.constantes import EnumTipoDocumento, EnumTipoProcessamento, EnumStatusProcessamento, EnumParametros, \
    EnumMotivoExclusao, EnumTipoExclusao
from funcoes.utilitarios import LOCAL_TZ

from negocio.ItemNaoPartct import ItemNaoPartctClasse
from negocio.DocNaoPartct import DocNaoPartctClasse
from negocio.DocPartct import DocPartctClasse

from negocio.CFOP import CFOPClasse
from negocio.GEN import GenClass
from negocio.Param import ParamClasse
from negocio.Procesm import ProcesmClasse
from negocio.CCE import *

from polls.models import Processamento


class NFAClasse:
    etapaProcess = f"class {__name__} - class NFAClasse."
    # loga_mensagem(etapaProcess)

    negProcesm = ProcesmClasse()

    def __init__(self, p_data_inicio, p_data_fim):
        self.codigo_retorno = 0
        self.d_data_inicio = p_data_inicio
        self.d_data_fim = p_data_fim
        self.s_tipo_documento = EnumTipoDocumento.NFA.value

    def carga_nfa(self) -> str:
        etapaProcess = f"class {self.__class__.__name__} - def carga_nfa. Período de {self.d_data_inicio} a {self.d_data_fim}"
        # loga_mensagem(etapaProcess)

        s_tipo_procesm = EnumTipoProcessamento.importacaoNFA.value

        dData_hora_inicio = datetime.now()
        sData_hora_inicio = datetime.now(LOCAL_TZ).strftime('%d/%m/%Y %H:%M:%S,%f')
        sData_hora_fim = self.d_data_fim.strftime('%d/%m/%Y %H:%M:%S')

        try:
            # Registra inicio do processamento #
            etapaProcess = f'Registra inicio do processamento de carga de NFAs.'
            procesm = self.negProcesm.iniciar_processamento(self.d_data_inicio, self.d_data_fim, s_tipo_procesm)
            etapaProcess = f'{procesm.id_procesm_indice} Inicio da carga de NFAs. Período de {self.d_data_inicio} a {self.d_data_fim} - {sData_hora_inicio}'
            loga_mensagem(etapaProcess)

            try:
                # Exclui dados de processamentos anteriores #
                self.exclui_dados_anteriores(procesm)

                # Carrega as NFAs geradas do período informado #
                etapaProcess = f'{procesm.id_procesm_indice} ' \
                               f'Busca NFA para processamento - {self.d_data_inicio} a {self.d_data_fim}. '
                self.registra_processamento(procesm, etapaProcess)
                data_hora_atividade = datetime.now()
                df_nfa = self.carrega_nfa()
                loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_nfa)} linhas selecionadas. - ' + str(
                    datetime.now() - data_hora_atividade))

                if len(df_nfa) > 0:
                    self.trabalha_nfa(procesm, df_nfa)
                    self.codigo_retorno = 1

                else:
                    etapaProcess = f'{procesm.id_procesm_indice} Carga de NFAs de {self.d_data_inicio} a {self.d_data_fim} finalizada. Não foram selecionadas NFAs habilitadas para cálculo no período.'
                    procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                    procesm.stat_procesm_indice = EnumStatusProcessamento.sucessoSemDados.value
                    self.registra_processamento(procesm, etapaProcess)
                    self.codigo_retorno = 1

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
            f'{procesm.id_procesm_indice} Fim da carga de NFAs. Período de {self.d_data_inicio} a {self.d_data_fim} - {sData_hora_final} - Tempo de processamento: {(datetime.now() - dData_hora_inicio)}.')
        loga_mensagem(' ')

        return str(self.codigo_retorno)

    def trabalha_nfa(self, procesm: Processamento, df):
        etapaProcess = f"class {self.__class__.__name__} - def trabalha_nfa."
        # loga_mensagem(etapaProcess)

        try:
            df['codg_tipo_doc'] = self.s_tipo_documento
            df['id_processamento'] = procesm.id_procesm_indice

            # Inclui informações do cadastro #
            df_nfa_cad = self.inclui_informacoes_cadastrais(procesm, df)

            # Inclui informações de apropriacao #
            df_nfa = self.inclui_informacoes_apropriacao(procesm, df_nfa_cad)

            # Inclui totos os motivos para exclusão do documento do cálculo do IPM #
            df_doc = self.agrega_motivos_exclusao(procesm, df_nfa)

            # Grava tabela de documentos participantes #
            linhas_gravadas = self.grava_doc_partct(procesm, df_doc)

            if linhas_gravadas > 0:
                etapaProcess = f'{procesm.id_procesm_indice} Carga de NFAs de {self.d_data_inicio} a {self.d_data_fim} finalizada. Carregadas {len(df_doc)} notas.'
                procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                procesm.stat_procesm_indice = EnumStatusProcessamento.sucesso.value
                self.registra_processamento(procesm, etapaProcess)
            else:
                etapaProcess = f'{procesm.id_procesm_indice} Carga de NFAs de {self.d_data_inicio} a {self.d_data_fim} finalizado. Não foram selecionadas NF-es para cálculo do IPM no período.'
                procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                procesm.stat_procesm_indice = EnumStatusProcessamento.sucessoSemDados.value
                self.registra_processamento(procesm, etapaProcess)

            # # Totaliza documento com as operações válidas para o cálculo
            # etapaProcess = f'{procesm.id_procesm_indice} Totaliza documentos participantes.'
            # self.registra_processamento(procesm, etapaProcess)
            # data_hora_atividade = datetime.now()
            # df_nfa_chave = self.totaliza_doc_participante(df)
            # etapaProcess = f'{procesm.id_procesm_indice} Filtra documentos participantes. {len(df_nfa_chave)} documentos participantes. ' + str(
            #     datetime.now() - data_hora_atividade)
            # self.registra_processamento(procesm, etapaProcess)
            #
            # if len(df_nfa_chave) > 0:
            #     # Inclui informações do cadastro #
            #     etapaProcess = f'{procesm.id_procesm_indice} Inclui informações do cadastro CCE.'
            #     self.registra_processamento(procesm, etapaProcess)
            #     data_hora_atividade = datetime.now()
            #     cce = CCEClasse()
            #     linhas_gravadas = cce.inclui_informacoes_cce(df_nfa_chave)
            #     del cce
            #     loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_gravadas} históricos gravados - ' + str(
            #         datetime.now() - data_hora_atividade))
            #
            #     # Reordena DataFrame de NF-es consolidado #
            #     etapaProcess = f'{procesm.id_procesm_indice} Reordenando documentos participantes.'
            #     self.registra_processamento(procesm, etapaProcess)
            #     data_hora_atividade = datetime.now()
            #     df_chave = self.ordena_colunas_doc_partct(df_nfa_chave)
            #     loga_mensagem(etapaProcess + ' Processo finalizado: ' + str(datetime.now() - data_hora_atividade))
            #
            #     # Grava documentos participantes #
            #     etapaProcess = f'{procesm.id_procesm_indice} Gravando Documentos Participantes -' + utilitarios.formatar_com_espacos(
            #         len(df_chave), 11) + ' linhas.'
            #     self.registra_processamento(procesm, etapaProcess)
            #     data_hora_atividade = datetime.now()
            #
            #     db = DocPartctClasse()
            #     linhas_inseridas = db.grava_doc_participante(df_chave)
            #     del db
            #     loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_inseridas} linhas inseridas - ' + str(
            #         datetime.now() - data_hora_atividade))
            #
            #     etapaProcess = f'{procesm.id_procesm_indice} Carga de NFAs de {self.d_data_inicio} a {self.d_data_fim} finalizada. Carregadas {len(df_nfa_chave)} notas.'
            #     procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
            #     procesm.stat_procesm_indice = EnumStatusProcessamento.sucesso.value
            #     self.registra_processamento(procesm, etapaProcess)
            #
            # else:
            #     etapaProcess = f'{procesm.id_procesm_indice} Carga de NFAs de {self.d_data_inicio} a {self.d_data_fim} finalizado. Não foram selecionadas NF-es para cálculo do IPM no período.'
            #     procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
            #     procesm.stat_procesm_indice = EnumStatusProcessamento.sucessoSemDados.value
            #     self.registra_processamento(procesm, etapaProcess)
            #
        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def inclui_informacoes_cadastrais(self, procesm, df):
        etapaProcess = f"class {self.__class__.__name__} - def inclui_informações_cadastrais."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f'{procesm.id_procesm_indice} Inclui informações do cadastro CCE.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            cce = CCEClasse()
            df_cad = cce.inclui_informacoes_cce(df, self.d_data_inicio, self.d_data_fim, procesm)
            del cce

            linhas_gravadas = len(df_cad)
            loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_gravadas} históricos gravados - ' + str(
                datetime.now() - data_hora_atividade))

            # Concatena dados do documento fiscal com os dados cadastrais da inscrição estadual de entrada
            df['ie_entrada'] = df['ie_entrada'].astype(str)
            df['ie_saida'] = df['ie_saida'].astype(str)
            df_cad['numr_inscricao_contrib'] = df_cad['numr_inscricao_contrib'].astype(str)
            df_nfe_cad = df.merge(df_cad,
                                  left_on=['ie_entrada', 'codg_uf_entrada'],
                                  right_on=['numr_inscricao_contrib', 'codg_uf'],
                                  how='left',
                                  )
            if 'data_fim_vigencia' in df_nfe_cad.columns:
                df_nfe_cad.drop(columns=['data_fim_vigencia'], inplace=True)
            df_nfe_cad.drop(columns=['numr_inscricao_contrib',
                                     'data_inicio_vigencia',
                                     ], inplace=True)

            df_nfe_cad.rename(columns={'id_contrib_ipm': 'id_contrib_ipm_entrada'}, inplace=True)
            df_nfe_cad.rename(columns={'indi_produtor_rural': 'indi_produtor_rural_entrada'}, inplace=True)
            df_nfe_cad.rename(columns={'stat_cadastro_contrib': 'stat_cadastro_contrib_entrada'}, inplace=True)
            df_nfe_cad.rename(columns={'tipo_enqdto_fiscal': 'tipo_enqdto_fiscal_entrada'}, inplace=True)
            df_nfe_cad.rename(columns={'codg_municipio': 'codg_municipio_cad_entrada'}, inplace=True)
            df_nfe_cad.rename(columns={'codg_uf': 'codg_uf_inscricao_entrada'}, inplace=True)

            # Concatena dados do documento fiscal com os dados cadastrais da inscrição estadual de saída
            df_nfe_cad = df_nfe_cad.merge(df_cad,
                                          left_on=['ie_saida', 'codg_uf_saida'],
                                          right_on=['numr_inscricao_contrib', 'codg_uf'],
                                          how='left',
                                          )
            if 'data_fim_vigencia' in df_nfe_cad.columns:
                df_nfe_cad.drop(columns=['data_fim_vigencia'], inplace=True)
            df_nfe_cad.drop(columns=['numr_inscricao_contrib',
                                     'data_inicio_vigencia',
                                     ], inplace=True)

            df_nfe_cad.rename(columns={'id_contrib_ipm': 'id_contrib_ipm_saida'}, inplace=True)
            df_nfe_cad.rename(columns={'indi_produtor_rural': 'indi_produtor_rural_saida'}, inplace=True)
            df_nfe_cad.rename(columns={'stat_cadastro_contrib': 'stat_cadastro_contrib_saida'}, inplace=True)
            df_nfe_cad.rename(columns={'tipo_enqdto_fiscal': 'tipo_enqdto_fiscal_saida'}, inplace=True)
            df_nfe_cad.rename(columns={'codg_municipio': 'codg_municipio_cad_saida'}, inplace=True)
            df_nfe_cad.rename(columns={'codg_uf': 'codg_uf_inscricao_saida'}, inplace=True)

            return df_nfe_cad

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def inclui_informacoes_apropriacao(self, procesm, df):
        etapaProcess = f"class {self.__class__.__name__} - def inclui_informacoes_apropriacao."
        # loga_mensagem(etapaProcess)

        # Não apropria = 'N'
        # Apropriação na Saída e na Entrada = 'A'
        # Apropriação na Saída = 'S'
        # Apropriação na Entrada = 'E'

        # tipo_enqdto_fiscal (3, 4, 5) = Simples ou MEI
        # stat_cadastro_contrib (1) = Ativo

        try:
            etapaProcess = f'{procesm.id_procesm_indice} Inclui informações de apropriação do documento.'

            df['indi_aprop'] = 'N'

            for indice, row in df.iterrows():

                if row['indi_produtor_rural_entrada'] == 'S':
                    s_prod_rural_dest = 'Prod'
                else:
                    s_prod_rural_dest = 'Não Prod'

                if row['stat_cadastro_contrib_entrada'] == '1':
                    s_stat_cadastro_dest = 'Ativo'
                else:
                    s_stat_cadastro_dest = 'Não Cad'

                if row['tipo_enqdto_fiscal_entrada'] in ['3', '4', '5']:
                    s_tipo_enqdto_fiscal_dest = 'Simples'
                else:
                    s_tipo_enqdto_fiscal_dest = 'Normal'

                if row['indi_produtor_rural_saida'] == 'S':
                    s_prod_rural_remet = 'Prod'
                else:
                    s_prod_rural_remet = 'Não Prod'

                if row['stat_cadastro_contrib_saida'] == '1':
                    s_stat_cadastro_remet = 'Ativo'
                else:
                    s_stat_cadastro_remet = 'Não Cad'

                if row['tipo_enqdto_fiscal_saida'] in ['3', '4', '5']:
                    s_tipo_enqdto_fiscal_remet = 'Simples'
                else:
                    s_tipo_enqdto_fiscal_remet = 'Normal'

                if   s_stat_cadastro_dest == 'Ativo':

                    if (s_tipo_enqdto_fiscal_dest == 'Normal' \
                    or  s_prod_rural_dest == 'Prod'):

                        if s_stat_cadastro_remet == 'Ativo':

                            if (s_tipo_enqdto_fiscal_remet == 'Normal' \
                            or  s_prod_rural_remet == 'Prod'):
                                df.loc[indice, 'indi_aprop'] = 'A'

                            elif s_tipo_enqdto_fiscal_remet == 'Simples':
                                df.loc[indice, 'indi_aprop'] = 'E'

                        elif s_stat_cadastro_remet == 'Não Cad':
                            df.loc[indice, 'indi_aprop'] = 'E'

                    elif s_tipo_enqdto_fiscal_dest == 'Simples':

                        if s_stat_cadastro_remet == 'Ativo':

                            if (s_tipo_enqdto_fiscal_remet == 'Normal' \
                            or  s_prod_rural_remet == 'Prod'):
                                df.loc[indice, 'indi_aprop'] = 'S'
                            elif s_tipo_enqdto_fiscal_remet == 'Simples':
                                df.loc[indice, 'indi_aprop'] = 'N'

                        elif s_stat_cadastro_remet == 'Não Cad':
                            df.loc[indice, 'indi_aprop'] = 'N'

                elif s_stat_cadastro_dest == 'Não Cad':

                    if s_stat_cadastro_remet == 'Ativo':

                        if (s_tipo_enqdto_fiscal_remet == 'Normal' \
                        or  s_prod_rural_remet == 'Prod'):
                            df.loc[indice, 'indi_aprop'] = 'S'
                        elif s_tipo_enqdto_fiscal_remet == 'Simples':
                            df.loc[indice, 'indi_aprop'] = 'N'

                    elif s_stat_cadastro_remet == 'Não Cad':
                        df.loc[indice, 'indi_aprop'] = 'N'

            return df

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def exclui_dados_anteriores(self, procesm: Processamento):
        etapaProcess = f"class {self.__class__.__name__} - def exclui_dados_anteriores. Período de {self.d_data_inicio} a {self.d_data_fim}"
        # loga_mensagem(etapaProcess)

        try:
            # Exclui dados de processamentos anteriores #
            etapaProcess = f'{procesm.id_procesm_indice} Exclui documentos participantes.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            db = DocPartctClasse()
            linhas_excluidas = db.exclui_doc_participante(self.s_tipo_documento, self.d_data_inicio, self.d_data_fim)
            del db

            loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_excluidas} linhas excluidas - ' + str(
                datetime.now() - data_hora_atividade))

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def carrega_nfa(self) -> pd.DataFrame:
        etapaProcess = f"class {__name__} - def carrega_nfa."
        # loga_mensagem(etapaProcess)

        try:
            db = OraprddwClass()
            df = db.select_nfa(int(self.d_data_inicio.strftime('%Y%m%d')), int(self.d_data_fim.strftime('%Y%m%d')))
            del db

            if len(df) > 0:
                return df
            else:
                return []

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def agrega_motivos_exclusao(self, procesm, df) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def agrega_motivos_exclusao."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f'{procesm.id_procesm_indice} Totaliza documentos participantes.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            # PATH = r'D:/SEFAZ/IPM/'
            # df = sobe_csv(PATH + "df_nfe_cad.csv", "|")

            df['tipo_exclusao'] = 0

            # Filtra notas canceladas #
            filtroCancel = df['stat_nota'] != 'NORMAL'
            df.loc[filtroCancel, 'codg_motivo_exclusao'] = EnumMotivoExclusao.DocumentoCancelado.value
            df.loc[filtroCancel, 'tipo_exclusao'] = EnumTipoExclusao.Documento.value

            # Filtra notas de contribuintes de entrada e saída não cadastrados ou do SIMPLES Nacional #
            df['stat_cadastro_contrib_entrada'] = df['stat_cadastro_contrib_entrada'].astype(str)
            df['tipo_enqdto_fiscal_entrada'] = df['tipo_enqdto_fiscal_entrada'].astype(str)
            df['stat_cadastro_contrib_saida'] = df['stat_cadastro_contrib_saida'].astype(str)
            df['tipo_enqdto_fiscal_saida'] = df['tipo_enqdto_fiscal_saida'].astype(str)

            filtroCad = ((df['stat_cadastro_contrib_entrada'] != '1') | (
                df['tipo_enqdto_fiscal_entrada'].isin(['3', '4', '5']))) \
                        & ((df['stat_cadastro_contrib_saida'] != '1') | (
                df['tipo_enqdto_fiscal_saida'].isin(['3', '4', '5'])))

            df.loc[filtroCad, 'codg_motivo_exclusao'] = EnumMotivoExclusao.ContribsNaoCadOuSimples.value
            df.loc[filtroCad, 'tipo_exclusao'] = EnumTipoExclusao.Documento.value

            loga_mensagem(etapaProcess + f' Processo finalizado. {len(df)} itens processados - ' + str(
                datetime.now() - data_hora_atividade))

            return df

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def formata_doc_participante(self, procesm, df) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def formata_doc_participante."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f'{procesm.id_procesm_indice} Formata documentos participantes. {len(df)} documentos participantes.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            df['id_contrib_ipm_saida'] = df['id_contrib_ipm_saida'].replace('0', pd.NA)
            df['id_contrib_ipm_entrada'] = df['id_contrib_ipm_entrada'].replace('0', pd.NA)

            df.rename(columns={'id_nfa': 'codg_documento_partct_calculo'}, inplace=True)
            df.rename(columns={'valr_va': 'valr_adicionado_operacao'}, inplace=True)
            df.rename(columns={'data_emissao_doc': 'data_emissao_documento'}, inplace=True)
            df.rename(columns={'codg_tipo_doc': 'codg_tipo_doc_partct_calc'}, inplace=True)
            df.rename(columns={'numr_ref_emissao': 'numr_referencia_documento'}, inplace=True)
            df.rename(columns={'id_processamento': 'id_procesm_indice'}, inplace=True)
            df.rename(columns={'codg_motivo_exclusao': 'codg_motivo_exclusao_calculo'}, inplace=True)

            df['numr_referencia_documento'] = df['numr_referencia_documento'].astype(int)
            df['data_emissao_documento'] = pd.to_datetime(df['data_emissao_documento'], format='%Y%m%d')

            colunas_doc_partct = ['codg_documento_partct_calculo',
                                  'valr_adicionado_operacao',
                                  'data_emissao_documento',
                                  'indi_aprop',
                                  'codg_tipo_doc_partct_calc',
                                  'id_procesm_indice',
                                  'id_contrib_ipm_entrada',
                                  'id_contrib_ipm_saida',
                                  'codg_motivo_exclusao_calculo',
                                  'numr_referencia_documento',
                                  'codg_municipio_saida',
                                  'codg_municipio_entrada',
                                  ]

            loga_mensagem(etapaProcess + f' Processo finalizado. {len(df)} itens gravados - ' + str(
                datetime.now() - data_hora_atividade))

            return df[colunas_doc_partct]

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def grava_doc_partct(self, procesm, df):
        etapaProcess = f"class {self.__class__.__name__} - def grava_doc_partct."
        # loga_mensagem(etapaProcess)

        linhas_gravadas = 0

        try:
            # Filtra documentos com valor maior do que zero
            # filtro = df['valr_participa'] != 0
            filtro = df['id_nfa'] != 0

            etapaProcess = f'{procesm.id_procesm_indice} Grava documentos participantes - {utilitarios.formatar_com_espacos(len(df[filtro]), 11)}' \
                               f' linhas.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            if len(df[filtro]) > 0:
                df_doc = self.formata_doc_participante(procesm, df[filtro])

                # Grava tabela de documentos não participantes #
                db = DocPartctClasse()
                linhas_gravadas = db.grava_doc_participante(df_doc)
                del db

            loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_gravadas} Docs Partct gravados - ' + str(datetime.now() - data_hora_atividade))

            return linhas_gravadas

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

    # def carrega_incricoes_nfa(self, p_periodo):
    #     etapaProcess = f"class {__name__} - def carrega_incricoes_nfa. Carrega dados cadastrais para as NFAs de {p_periodo}."
    #     # loga_mensagem(etapaProcess)
    #
    #     s_tipo_procesm = EnumTipoProcessamento.importacaoCCE.value
    #
    #     dData_hora_inicio = datetime.now()
    #     sData_hora_inicio = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S,%f')
    #     etapaProcess = f'Inicio da carga das informações cadastrais das NFAs para {p_periodo} - {sData_hora_inicio}'
    #     loga_mensagem(etapaProcess)
    #
    #     try:
    #         # Registra inicio do processamento #
    #         etapaProcess = 'Registra inicio do processamento de carga de informações cadastrais das NFAs'
    #         procesm = self.negProcesm.iniciar_processamento(s_tipo_procesm)
    #
    #         # Carrega as inscrições das NF-es geradas do período informado #
    #         etapaProcess = f'{procesm.id_procesm_indice} Busca as inscrições das NFAs para processamento - {p_periodo}. '
    #         self.registra_processamento(procesm, etapaProcess)
    #         data_hora_atividade = datetime.now()
    #
    #         db = Oraprd()
    #         df_cce = db.select_inscricoes_nfa(p_periodo)
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
    #             etapaProcess = f'Carga das informações cadastrais das NFAs para {p_periodo} finalizado. Carregadas 0 históricos.'
    #             loga_mensagem(str(procesm.id_procesm_indice) + ' ' + etapaProcess)
    #             procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
    #             procesm.stat_procesm_indice = EnumStatusProcessamento.sucesso.value
    #             self.registra_processamento(procesm, etapaProcess)
    #
    #         else:
    #             etapaProcess = f'Carga das informações cadastrais das NFAs para {p_periodo} finalizado. Não foram selecionadas inscrições para o período.'
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
    # def ordena_colunas_nfa(df):
    #     # Ordena o dataframe #
    #     df = df.reindex(columns=['id_nfa',
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
    # def ordena_colunas_nfa_chave(df):
    #     # Ordena o dataframe #
    #     df = df.reindex(columns=['id_nfa',
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
    # def totaliza_documento_nfa(df_nfa):
    #     # Aplicando um filtro na coluna 'cfop_particip'
    #     filtro = (df_nfa['indi_cfop_particip'] == True) & (df_nfa['indi_natur_oper_particip'] == True) & (
    #                 df_nfa['indi_stat_nota_particip'] == True)
    #
    #     # Totalizando as notas #
    #     df_nfa_tot = df_nfa[filtro].groupby(['id_nfa']).agg({'ie_saida': 'first',
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
    #     filtro = (df_nfa_tot['valr_total_produto'] > 0)
    #     df_nfa_tot = df_nfa_tot[filtro]
    #
    #     if df_nfa_tot.shape[0] > 0:
    #         # Renomeia colunas para chamada da rotina de busca de dados do CCE #
    #         df_nfa_tot['numr_ref_emissao'] = df_nfa_tot['numr_ref_emissao'].astype(int)
    #         df_nfa_tot['ie_saida'] = df_nfa_tot['ie_saida'].astype(object)
    #         df_nfa_tot['ie_entrada'] = df_nfa_tot['ie_entrada'].astype(object)
    #
    #         negCFOP = CFOPClasse()
    #         df_cfop = negCFOP.busca_cfop_nfe()
    #         del negCFOP
    #
    #         # Altera o tipo da coluna #
    #         df_nfa_tot['numr_cfop'] = df_nfa_tot['numr_cfop'].astype(int)
    #         df_cfop['cfop'] = df_cfop['cfop'].astype(int)
    #         df_nfa_tot['indi_cfop_particip'] = df_nfa_tot['numr_cfop'].isin(df_cfop['cfop'])
    #
    #         return df_nfa_tot
    #     else:
    #         return None
