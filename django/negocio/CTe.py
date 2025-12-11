from datetime import datetime

import pandas as pd

from funcoes import utilitarios
from funcoes.constantes import EnumTipoDocumento, EnumTipoProcessamento, EnumStatusProcessamento, EnumParametros, \
    EnumMotivoExclusao, EnumTipoExclusao
from funcoes.utilitarios import LOCAL_TZ
from negocio.CFOP import CFOPClasse
from negocio.DocNaoPartct import DocNaoPartctClasse
from negocio.DocPartct import DocPartctClasse
from negocio.GEN import GenClass
from negocio.Param import ParamClasse
from negocio.Procesm import ProcesmClasse

from negocio.CCE import *
# from persistencia.Mysql import MySQL
from persistencia.Oraprodx9 import Oraprodx9
from polls.models import Processamento


class CTeClasse:
    etapaProcess = f"class {__name__} - class CTeClasse."
    # loga_mensagem(etapaProcess)

    negProcesm = ProcesmClasse()

    def __init__(self, p_data_inicio, p_data_fim):
        self.codigo_retorno = 0
        self.d_data_inicio = p_data_inicio
        self.d_data_fim = p_data_fim
        self.s_tipo_documento = EnumTipoDocumento.CTe.value

    def carga_cte(self) -> str:
        etapaProcess = f"class {self.__class__.__name__} - def carga_cte. Período de {self.d_data_inicio} a {self.d_data_fim}"
        # loga_mensagem(etapaProcess)

        s_tipo_procesm = EnumTipoProcessamento.importacaoCTe.value

        dData_hora_inicio = datetime.now()
        sData_hora_inicio = datetime.now(LOCAL_TZ).strftime('%d/%m/%Y %H:%M:%S,%f')
        sData_hora_fim = self.d_data_fim.strftime('%d/%m/%Y %H:%M:%S')

        try:
            # Registra inicio do processamento #
            etapaProcess = f'Registra inicio do processamento de carga de CT-es.'
            procesm = self.negProcesm.iniciar_processamento(self.d_data_inicio, self.d_data_fim, s_tipo_procesm)
            etapaProcess = f'{procesm.id_procesm_indice} Inicio da carga de CT-es. Período de {self.d_data_inicio} a {self.d_data_fim} - {sData_hora_inicio}'
            loga_mensagem(etapaProcess)

            try:
                # Carrega as CT-es geradas do período informado #
                etapaProcess = f'{procesm.id_procesm_indice} ' \
                               f'Busca CT-es para processamento - {self.d_data_inicio} a {self.d_data_fim}. '
                self.registra_processamento(procesm, etapaProcess)
                data_hora_atividade = datetime.now()
                df_cte = self.carrega_cte()
                loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_cte)} linhas selecionadas. - ' + str(
                    datetime.now() - data_hora_atividade))

                if len(df_cte) > 0:
                    self.trabalha_cte(procesm, df_cte)
                    self.codigo_retorno = 1
                    if sData_hora_fim == '31/03/2023 23:59:59':
                        self.codigo_retorno = 0

                else:
                    etapaProcess = f'{procesm.id_procesm_indice} Carga de CT-es de {self.d_data_inicio} a {self.d_data_fim} finalizada. Não foram selecionados CT-es habilitadas para cálculo no período.'
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
            f'{procesm.id_procesm_indice} Fim da carga de CT-es. Período de {self.d_data_inicio} a {self.d_data_fim} - {sData_hora_final} - Tempo de processamento: {(datetime.now() - dData_hora_inicio)}.')
        loga_mensagem(' ')

        return str(self.codigo_retorno)

    def carrega_cte(self) -> pd.DataFrame:
        etapaProcess = f"class {__name__} - def carrega_cte."
        # loga_mensagem(etapaProcess)

        try:
            # PATH = r'D:/SEFAZ/IPM/'
            # # obj_retorno = sobe_csv(PATH + "obj_retorno.csv", "|")
            # obj_retorno = self.carrega_municipio_cte(sobe_csv(PATH + "Pasta2.csv", ";"))
            # filtro = obj_retorno['codg_uf_saida'] == 'GO'
            # return obj_retorno[filtro]

            obj_retorno = []

            db = Oraprd()  # Persistência utilizando o Django
            # db = oraProd.create_instance()  # Persistência utilizando o oracledb com X9
            # db = Oraprodx9()                 # Persistência utilizando o Django com X9
            df_doc_cte = db.select_doc_cte(self.d_data_inicio, self.d_data_fim)

            # df_doc_cte.name = 'df_doc_cte'
            # baixa_csv(df_doc_cte)

            if len(df_doc_cte) == 0:
                del db
                return []
            else:
                df_cte = db.select_cte(df_doc_cte['id_documento_cte'].tolist())
                del db

            if len(df_cte) == 0:
                return []
            else:
                df_retorno = pd.merge(df_doc_cte, df_cte, on=['id_documento_cte'], how='left')
                if len(df_cte) > 0:
                    obj_retorno = self.carrega_municipio_cte(df_retorno)
                else:
                    return []

            # obj_retorno.name = 'obj_retorno'
            # baixa_csv(obj_retorno)

            filtro = obj_retorno['codg_uf_saida'] == 'GO'
            return obj_retorno[filtro]

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def carrega_municipio_cte(self, df) -> pd.DataFrame:
        etapaProcess = f"class {__name__} - def carrega_municipio_cte."
        # loga_mensagem(etapaProcess)

        try:
            gen = GenClass()
            municipios = df[['codg_municipio_saida', 'codg_municipio_entrada']].stack().unique()
            df_municip = gen.carrega_municipio_ibge(municipios.tolist())
            del gen

            if len(df_municip) > 0:
                obj_retorno = df.merge(df_municip,
                                       left_on=['codg_municipio_saida'],
                                       right_on=['codg_ibge'],
                                       how='left',
                                       )
                obj_retorno.drop(columns=['codg_ibge',
                                          'codg_municipio_saida',
                                          ], inplace=True)
                obj_retorno.rename(columns={'codg_municipio': 'codg_municipio_saida'}, inplace=True)
                obj_retorno.rename(columns={'codg_uf': 'codg_uf_saida'}, inplace=True)

                obj_retorno = obj_retorno.merge(df_municip,
                                            left_on=['codg_municipio_entrada'],
                                            right_on=['codg_ibge'],
                                            how='left',
                                            )
                obj_retorno.drop(columns=['codg_ibge',
                                          'codg_municipio_entrada',
                                          ], inplace=True)
                obj_retorno.rename(columns={'codg_municipio': 'codg_municipio_entrada'}, inplace=True)
                obj_retorno.rename(columns={'codg_uf': 'codg_uf_entrada'}, inplace=True)
                return obj_retorno

            else:
                df['codg_municipio_inicio_prest'] = None
                df['codg_municipio_fim_prestacao'] = None
                return df

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def trabalha_cte(self, procesm: Processamento, df):
        etapaProcess = f"class {self.__class__.__name__} - def trabalha_cte."
        # loga_mensagem(etapaProcess)

        try:
            df['codg_tipo_doc'] = self.s_tipo_documento
            df['id_processamento'] = procesm.id_procesm_indice

            # Exclui dados de processamentos anteriores #
            self.exclui_dados_anteriores(procesm)

            df_cfop = self.inclui_cfop_participante(procesm, df)

            # Inclui totos os motivos para exclusão do documento do cálculo do IPM #
            df_doc = self.agrega_motivos_exclusao(procesm, df_cfop)

            # Grava tabela de documentos participantes #
            linhas_gravadas = self.grava_doc_partct(procesm, df_doc)

            if linhas_gravadas != len(df_doc):
                raise Exception(f"A quantidade de documentos processados ({len(df_doc)}) não corresponde à quantidade de linhas gravadas ({linhas_gravadas})!")
            else:
                db = DocPartctClasse()
                linhas_gravadas = db.verifica_linhas_gravadas(procesm.id_procesm_indice)
                del db

                if linhas_gravadas != len(df_doc):
                    raise Exception(f"A quantidade de documentos processados ({len(df_doc)}) não corresponde à quantidade de linhas no Banco de Dados ({linhas_gravadas})!")

            etapaProcess = f'Carga de CTes de {self.d_data_inicio} a {self.d_data_fim} finalizada. Carregados {linhas_gravadas} documentos.'
            loga_mensagem(str(procesm.id_procesm_indice) + ' ' + etapaProcess)

            etapaProcess = f'{procesm.id_procesm_indice} Carga de CT-es de {self.d_data_inicio} a {self.d_data_fim} finalizada. Carregadas {len(df_doc)} notas.'
            procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
            procesm.stat_procesm_indice = EnumStatusProcessamento.sucesso.value
            self.registra_processamento(procesm, etapaProcess)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def exclui_dados_anteriores(self, procesm: Processamento):
        etapaProcess = f"class {self.__class__.__name__} - def exclui_dados_anteriores. Período de {self.d_data_inicio} a {self.d_data_fim}"
        # loga_mensagem(etapaProcess)

        try:
            # etapaProcess = f'{procesm.id_procesm_indice} Exclui documentos não participantes.'
            # self.registra_processamento(procesm, etapaProcess)
            # data_hora_atividade = datetime.now()
            #
            # db = DocNaoPartctClasse()
            # linhas_excluidas = db.exclui_doc_nao_participante(self.s_tipo_documento, self.d_data_inicio, self.d_data_fim)
            # del db
            #
            # loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_excluidas} linhas excluidas - ' + str(
            #     datetime.now() - data_hora_atividade))

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

    def inclui_cfop_participante(self, procesm, df):
        etapaProcess = f"class {self.__class__.__name__} - def inclui_cfop_participante."
        # loga_mensagem(etapaProcess)

        try:
            # Busca CFOPs participantes
            etapaProcess = f'{procesm.id_procesm_indice} Busca CFOPs participantes do IPM.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            negCFOP = CFOPClasse()
            df_cfop = negCFOP.busca_cfop_nfe(self.d_data_inicio, self.d_data_fim, EnumTipoDocumento.CTe.value)
            del negCFOP

            # Altera o tipo da coluna #
            df['numr_cfop'] = df['numr_cfop'].fillna(0).astype(int)
            df_cfop['codg_cfop'] = df_cfop['codg_cfop'].astype(int)

            # Indica se o CFOP participa do cálculo
            df['indi_cfop_particip'] = df['numr_cfop'].isin(df_cfop['codg_cfop'])

            # Atribui motivo para não participar
            df['codg_motivo_exclusao'] = df['indi_cfop_particip'].map({True: None, False: EnumMotivoExclusao.CFOPNaoParticipante.value})

            loga_mensagem(etapaProcess + f' Processo finalizado. {len(df)} CFOPs participantes - ' + str(
                datetime.now() - data_hora_atividade))

            return df

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

            df['tipo_exclusao'] = 0

            # Inclui dados para gravar tabela itens com CFOP não participantes #
            filtroCFOP = df['indi_cfop_particip'] == False
            df.loc[filtroCFOP, 'tipo_exclusao'] = EnumTipoExclusao.Item.value
            # loga_mensagem(f'Quantidade de linhas com filtro de CFOP: {len(df[filtroCFOP])}')

            # Filtra CTes cancelados #
            filtroCancel = df['codg_chave_acesso_anulacao'] != '0'
            # loga_mensagem(f'Quantidade de linhas com documento cancelado: {len(df[filtroCancel])}')
            df.loc[filtroCancel, 'codg_motivo_exclusao'] = EnumMotivoExclusao.DocumentoCancelado.value
            df.loc[filtroCancel, 'tipo_exclusao'] = EnumTipoExclusao.Documento.value

            # Filtra CTes substituídos #
            filtroSubst = df['codg_chave_acesso_cte_subst'] != '0'
            # loga_mensagem(f'Quantidade de linhas com documento substituído: {len(df[filtroSubst])}')
            df.loc[filtroSubst, 'codg_motivo_exclusao'] = EnumMotivoExclusao.DocumentoSubstituido.value
            df.loc[filtroSubst, 'tipo_exclusao'] = EnumTipoExclusao.Documento.value

            # Filtra CTes com divergencia de valores #
            filtroValor = df['valr_va'] < df['valr_receber']
            # loga_mensagem(f'Quantidade de linhas com documento substituído: {len(df[filtroValor])}')
            df.loc[filtroSubst, 'codg_motivo_exclusao'] = EnumMotivoExclusao.ValoresInconsistentes.value
            df.loc[filtroSubst, 'tipo_exclusao'] = EnumTipoExclusao.Documento.value

            # Concatena todos os filtros #
            filtroItem = ~(filtroCFOP | filtroCancel | filtroSubst | filtroValor)
            df['indi_participa_calculo'] = filtroItem

            # Filtra notas com nenhum item que participa do cálculo #

            # Soma valores de va, por nota e indicação de participaçao
            df_agg = df.groupby(['id_documento_cte', 'indi_participa_calculo']).agg({'valr_va': 'sum'}).reset_index()

            # Transforma linhas em colunas
            pivot_df = pd.pivot_table(df_agg, index='id_documento_cte', columns='indi_participa_calculo', values='valr_va',
                                      fill_value=0)

            # Caso não haja itens participantes ou não participantes, cria respectivas colunas
            inclui_false = False
            if True not in pivot_df.columns:
                pivot_df[True] = 0
            if False not in pivot_df.columns:
                pivot_df[False] = 0
                inclui_false = True

            # Renomear colunas para evitar conflitos de nome, na ordem correta - False sempre vem primeiro
            if inclui_false:
                pivot_df.columns = ['valr_participa', 'valr_nao_participa']
            else:
                pivot_df.columns = ['valr_nao_participa', 'valr_participa']

            # Resetar o índice para facilitar o merge
            pivot_df = pivot_df.reset_index()

            # Merge com o DataFrame original
            df_merged = pd.merge(df, pivot_df, on='id_documento_cte', how='left')

            # Agrupa por documento fiscal
            df_cfop = df_merged.groupby(['id_documento_cte']).agg({'data_emissao_doc': 'first',
                                                                   'numr_ref_emissao': 'first',
                                                                   'codg_tipo_doc': 'first',
                                                                   'id_processamento': 'first',
                                                                   'valr_va': 'sum',
                                                                   'codg_motivo_exclusao': 'max',
                                                                   'tipo_exclusao': 'max',
                                                                   'valr_nao_participa': 'first',
                                                                   'valr_participa': 'first',
                                                                   'codg_municipio_entrada': 'first',
                                                                   'codg_municipio_saida': 'first',
                                                                   }
                                                                  ).reset_index()

            # Caso todos os itens não participem do cálculo, marca o documento sem itens.
            # Motivos que excluem o documento são desprezados
            # Aplica filtro e verifica a condição diretamente no DataFrame
            filtro = (df_cfop['valr_participa'] == 0) & (df_cfop['tipo_exclusao'] != EnumTipoExclusao.Documento.value)

            # Atualiza os valores de forma vetorizada
            df_cfop.loc[filtro, 'codg_motivo_exclusao'] = EnumMotivoExclusao.DocumentoNaoContemItemParticipante.value

            # Remove a coluna 'tipo_exclusao'
            df_cfop = df_cfop.drop(columns=['tipo_exclusao'])

            filtro = df_cfop['valr_participa'] != 0
            df_cfop.loc[filtro, 'codg_motivo_exclusao'] = 0

            loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_cfop)} itens processados - ' + str(
                datetime.now() - data_hora_atividade))

            return df_cfop

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def grava_doc_partct(self, procesm, df):
        etapaProcess = f"class {self.__class__.__name__} - def grava_doc_partct."
        # loga_mensagem(etapaProcess)

        linhas_gravadas = 0

        try:
            filtro = df['id_documento_cte'] != 0

            etapaProcess = f'{procesm.id_procesm_indice} Grava documentos participantes - {utilitarios.formatar_com_espacos(len(df[filtro]), 11)}' \
                               f' linhas.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            if len(df[filtro]) > 0:
                df_doc = self.formata_doc_participante(procesm, df[filtro])

                # Grava tabela de documentos participantes #
                db = DocPartctClasse()
                linhas_gravadas = db.grava_doc_participante(df_doc)
                del db

            loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_gravadas} Docs Partct gravados - ' + str(datetime.now() - data_hora_atividade))

            return linhas_gravadas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def formata_doc_participante(self, procesm, df) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def formata_doc_participante."
        # loga_mensagem(etapaProcess)

        try:
            df['id_contrib_ipm_saida'] = None
            df['id_contrib_ipm_entrada'] = None
            df['indi_aprop'] = 'S'

            df.rename(columns={'id_documento_cte': 'codg_documento_partct_calculo',
                               'valr_participa': 'valr_adicionado_operacao',
                               'data_emissao_doc': 'data_emissao_documento',
                               'codg_tipo_doc': 'codg_tipo_doc_partct_calc',
                               'numr_ref_emissao': 'numr_referencia_documento',
                               'id_processamento': 'id_procesm_indice',
                               'codg_motivo_exclusao': 'codg_motivo_exclusao_calculo'}, inplace=True)

            df['numr_referencia_documento'] = df['numr_referencia_documento'].astype(int)

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

            return df[colunas_doc_partct]

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
