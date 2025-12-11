from datetime import datetime

import numpy as np
import pandas as pd

from funcoes import utilitarios
from funcoes.constantes import EnumTipoDocumento, EnumTipoProcessamento, EnumStatusProcessamento, EnumParametros, \
    EnumMotivoExclusao, EnumTipoExclusao
from funcoes.utilitarios import LOCAL_TZ, loga_mensagem, loga_mensagem_erro
from negocio.CCE import CCEClasse
from negocio.CFOP import CFOPClasse
from negocio.DocNaoPartct import DocNaoPartctClasse
from negocio.DocPartct import DocPartctClasse
from negocio.ItemDoc import ItemDocClasse
from negocio.ItemNaoPartct import ItemNaoPartctClasse
from negocio.NCM import NCMClasse
from negocio.Param import ParamClasse
from negocio.Procesm import ProcesmClasse
from persistencia.OraProd import oraProd
from persistencia.Oracle import Oracle
from persistencia.Oraprd import Oraprd
from persistencia.Oraprddw import OraprddwClass
from polls.models import Processamento, IdentConv115


class Conv115Classe:
    etapaProcess = f"class {__name__} - class Conv115Classe."
    # loga_mensagem(etapaProcess)

    negProcesm = ProcesmClasse()

    def __init__(self, p_data_inicio, p_data_fim):
        self.codigo_retorno = 0
        self.d_data_inicio = p_data_inicio
        self.d_data_fim = p_data_fim
        self.s_tipo_documento = EnumTipoDocumento.TelecomConv115.value

    def carga_conv115(self) -> str:
        etapaProcess = f"class {self.__class__.__name__} - def carga_conv_115. Período de {self.d_data_inicio} a {self.d_data_fim}"
        # loga_mensagem(etapaProcess)

        s_tipo_procesm = EnumTipoProcessamento.importacaoTelecomConv115.value

        dData_hora_inicio = datetime.now()
        sData_hora_inicio = datetime.now(LOCAL_TZ).strftime('%d/%m/%Y %H:%M:%S,%f')
        sData_hora_fim = self.d_data_fim.strftime('%d/%m/%Y %H:%M:%S')

        try:
            # Registra inicio do processamento #
            etapaProcess = f'Registra inicio do processamento de carga do Convênio 115.'
            procesm = self.negProcesm.iniciar_processamento(self.d_data_inicio, self.d_data_fim, s_tipo_procesm)
            etapaProcess = f'{procesm.id_procesm_indice} Inicio da carga do Convênio 115. Período de {self.d_data_inicio} a {self.d_data_fim} - {sData_hora_inicio}'
            loga_mensagem(etapaProcess)

            try:
                # Exclui dados de processamentos anteriores #
                self.exclui_dados_anteriores(procesm, self.s_tipo_documento)

                # Carrega as informações do Convênio 115 do período informado #
                etapaProcess = f'{procesm.id_procesm_indice} ' \
                               f'Busca Conv115 para processamento - {self.d_data_inicio} a {self.d_data_fim}. '
                self.registra_processamento(procesm, etapaProcess)
                data_hora_atividade = datetime.now()
                df_115 = self.carrega_conv115()
                loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_115)} linhas selecionadas. - ' + str(
                    datetime.now() - data_hora_atividade))

                if df_115.shape[0] > 0:
                    self.trabalha_conv115(procesm, df_115)
                    self.codigo_retorno = 1
                    if sData_hora_fim == '31/03/2023 23:59:59':
                        self.codigo_retorno = 0

                else:
                    etapaProcess = f'{procesm.id_procesm_indice} Carga do Convênio 115 de {self.d_data_inicio} a {self.d_data_fim} finalizada. Não foram selecionados dados do Convênio 115 habilitadas para cálculo no período.'
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
            f'{procesm.id_procesm_indice} Fim da carga do Convênio 115. Período de {self.d_data_inicio} a {self.d_data_fim} - {sData_hora_final} - Tempo de processamento: {(datetime.now() - dData_hora_inicio)}.')
        loga_mensagem(' ')

        return str(self.codigo_retorno)

    def carrega_conv115(self) -> pd.DataFrame:
        etapaProcess = f"class {__name__} - def carrega_conv115."
        # loga_mensagem(etapaProcess)

        try:
            db = OraprddwClass()                    # Persistência utilizando o Django
            obj_retorno = db.select_conv115(self.d_data_inicio.strftime('%Y%m%d'), self.d_data_fim.strftime('%Y%m%d'))
            del db
            return obj_retorno

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def trabalha_conv115(self, procesm: Processamento, df_conv115):
        etapaProcess = f"class {self.__class__.__name__} - def trabalha_conv115."
        # loga_mensagem(etapaProcess)

        try:
            # Grava identificador do convênio 115 #
            etapaProcess = f'{procesm.id_procesm_indice} Grava identificador do Convênio 115.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()
            df_id115 = self.grava_ident_conv_115(df_conv115)
            loga_mensagem(etapaProcess + ' Processo finalizado: ' + str(datetime.now() - data_hora_atividade))

            df_id115['codg_tipo_doc'] = self.s_tipo_documento
            df_id115['id_processamento'] = procesm.id_procesm_indice
            df_id115['indi_aprop'] = 'E'
            df_id115['id_produto_ncm'] = None
            df_id115['valr_va'] = np.where((df_id115['perc_aliquota_icms'] != 0) & (df_id115['valr_base_calculo_icms'] != 0),
                                            df_id115['valr_base_calculo_icms'] + df_id115['valr_isento_nao_tributavel'],
                                            0
                                           )

            df_cfop = self.inclui_cfop_participante(procesm, df_id115)

            # Inclui informações do cadastro #
            df_cad = self.inclui_informacoes_cadastrais(procesm, df_cfop)

            # Inclui totos os motivos para exclusão do documento do cálculo do IPM #
            df_doc = self.agrega_motivos_exclusao(df_cad)

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

            if linhas_gravadas > 0:
                # Grava tabela de itens participantes #
                linhas_gravadas = self.grava_itens_doc(procesm, df_cfop)

                if linhas_gravadas != len(df_cfop):
                    raise Exception(
                        f"A quantidade de itens processados ({len(df_cfop)}) não corresponde à quantidade de linhas gravadas ({linhas_gravadas})!")
                else:
                    db = ItemDocClasse()
                    linhas_gravadas = db.verifica_linhas_gravadas(procesm.id_procesm_indice)
                    del db

                    if linhas_gravadas != len(df_cfop):
                        raise Exception(
                            f"A quantidade de itens processados ({len(df_cfop)}) não corresponde à quantidade de linhas no Banco de Dados ({linhas_gravadas})!")

                etapaProcess = f'{procesm.id_procesm_indice} Carga do Convênio 115 de {self.d_data_inicio} a {self.d_data_fim} finalizada. Carregadas {len(df_id115)} notas.'
                procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                procesm.stat_procesm_indice = EnumStatusProcessamento.sucesso.value
                self.registra_processamento(procesm, etapaProcess)

            else:
                etapaProcess = f'{procesm.id_procesm_indice} Carga do Convênio 115 de {self.d_data_inicio} a {self.d_data_fim} finalizado. Não foram selecionadas documentos para cálculo do IPM no período.'
                procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                procesm.stat_procesm_indice = EnumStatusProcessamento.sucessoSemDados.value
                self.registra_processamento(procesm, etapaProcess)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def exclui_dados_anteriores(self, procesm: Processamento, p_tipo_documento):
        etapaProcess = f"class {self.__class__.__name__} - def exclui_dados_anteriores. Período de {self.d_data_inicio} a {self.d_data_fim}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f'{procesm.id_procesm_indice} Exclui Itens participantes.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            db = ItemDocClasse()
            linhas_excluidas = db.exclui_item_documento(p_tipo_documento, self.d_data_inicio, self.d_data_fim)
            del db

            loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_excluidas} linhas excluidas - ' + str(
                datetime.now() - data_hora_atividade))

            etapaProcess = f'{procesm.id_procesm_indice} Exclui documentos participantes.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            db = DocPartctClasse()
            linhas_excluidas = db.exclui_doc_participante(p_tipo_documento, self.d_data_inicio, self.d_data_fim)
            del db

            loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_excluidas} linhas excluidas - ' + str(
                datetime.now() - data_hora_atividade))


            etapaProcess = f'{procesm.id_procesm_indice} Exclui ids dos documentos participantes do CONV115.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            db = Oraprd()
            linhas_excluidas = db.delete_ident_conv115(self.d_data_inicio, self.d_data_fim)
            del db

            loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_excluidas} linhas excluidas - ' + str(
            datetime.now() - data_hora_atividade))

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def formata_doc_participante(self, df) -> pd.DataFrame:
        etapaProcess = f"class {__name__} - def formata_doc_participante."
        # loga_mensagem(etapaProcess)

        try:
            df = df.drop(columns=['valr_va',
                                  'valr_nao_participa',
                                  ]
                         )

            df.rename(columns={'id_conv_115': 'codg_documento_partct_calculo'}, inplace=True)
            df.rename(columns={'valr_participa': 'valr_adicionado_operacao'}, inplace=True)
            df.rename(columns={'codg_tipo_doc': 'codg_tipo_doc_partct_calc'}, inplace=True)
            df.rename(columns={'numr_ref_emissao': 'numr_referencia_documento'}, inplace=True)
            df.rename(columns={'id_processamento': 'id_procesm_indice'}, inplace=True)
            df.rename(columns={'codg_motivo_exclusao': 'codg_motivo_exclusao_calculo'}, inplace=True)

            df['numr_referencia_documento'] = df['numr_referencia_documento'].astype(int)

            return df

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def grava_ident_conv_115(self, df) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def grava_ident_conv_115."
        # loga_mensagem(etapaProcess)

        ids_conv115 = []

        try:
            df.rename(columns={'ie_saida': 'numr_inscricao',
                               'indi_situacao_versao_arquivo': 'codg_situacao_versao_arquivo'}, inplace=True)

            df_ident = df[['numr_inscricao', 'numr_documento_fiscal', 'data_emissao_documento', 'desc_serie_documento_fiscal', 'codg_situacao_versao_arquivo']].drop_duplicates()
            df_ident.reset_index(drop=True, inplace=True)

            # db = oraProd.create_instance()     # Persistência utilizando o oracledb
            db = Oraprd()
            df_ident = db.insert_id_conv115(df_ident)
            del db

            df.rename(columns={'numr_inscricao': 'ie_saida'}, inplace=True)
            df_ident.rename(columns={'numr_inscricao': 'ie_saida'}, inplace=True)

            return pd.merge(df, df_ident, on=['ie_saida',
                                              'data_emissao_documento',
                                              'numr_documento_fiscal',
                                              'desc_serie_documento_fiscal',
                                              'codg_situacao_versao_arquivo'],
                            how='left')

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def grava_doc_partct(self, procesm, df):
        etapaProcess = f"class {self.__class__.__name__} - def grava_doc_partct."
        # loga_mensagem(etapaProcess)

        linhas_gravadas = 0

        try:
            filtro = df['id_conv_115'] != 0

            etapaProcess = f'{procesm.id_procesm_indice} Grava documentos participantes - {utilitarios.formatar_com_espacos(len(df[filtro]), 11)}' \
                               f' linhas.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            if len(df[filtro]) > 0:
                df_doc = self.formata_doc_participante(df[filtro])

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
        etapaProcess = f"class {__name__} - def registra_processamento - {etapa}"
        # loga_mensagem(etapaProcess)

        try:
            loga_mensagem(etapa)
            procesm.desc_observacao_procesm_indice = etapa
            self.negProcesm.atualizar_situacao_processamento(procesm)
            return None

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def inclui_informacoes_cadastrais(self, procesm, df):
        etapaProcess = f"class {self.__class__.__name__} - def inclui_informacoes_cadastrais."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f'{procesm.id_procesm_indice} Inclui informações do cadastro CCE para documentos dO Conv115.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            cce = CCEClasse()
            df_cad = cce.inclui_informacoes_cce(df, self.d_data_inicio, self.d_data_fim, procesm)
            del cce

            loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_cad)} contribuintes retornados - ' + str(
                datetime.now() - data_hora_atividade))

            # Concatena dados do documento fiscal com os dados cadastrais da inscrição estadual de entrada
            etapaProcess = f'{procesm.id_procesm_indice} Concatena dados do documento fiscal com os dados cadastrais da inscrição estadual de entrada.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            df['ie_entrada'] = df['ie_entrada'].astype(str)
            df['ie_saida'] = df['ie_saida'].astype(str)
            df_cad['numr_inscricao_contrib'] = df_cad['numr_inscricao_contrib'].astype(str)
            df['codg_uf_entrada'].fillna('GO', inplace=True)
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

            df_nfe_cad.rename(columns={'id_contrib_ipm': 'id_contrib_ipm_entrada',
                                       'indi_produtor_rural': 'indi_produtor_rural_entrada',
                                       'stat_cadastro_contrib': 'stat_cadastro_contrib_entrada',
                                       'tipo_enqdto_fiscal': 'tipo_enqdto_fiscal_entrada',
                                       'codg_municipio': 'codg_municipio_cad_entrada',
                                       'codg_uf': 'codg_uf_inscricao_entrada'}, inplace=True)

            loga_mensagem(etapaProcess + f' Processo finalizado - ' + str(datetime.now() - data_hora_atividade))

            # Concatena dados do documento fiscal com os dados cadastrais da inscrição estadual de saída
            etapaProcess = f'{procesm.id_procesm_indice} Concatena dados do documento fiscal com os dados cadastrais da inscrição estadual de saída.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            df['codg_uf_saida'].fillna('GO', inplace=True)
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

            loga_mensagem(etapaProcess + f' Processo finalizado - ' + str(datetime.now() - data_hora_atividade))

            # df_nfe_cad.name = 'df_nfe_cad'
            # baixa_csv(df_nfe_cad)

            return df_nfe_cad

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
            df_cfop = negCFOP.busca_cfop_nfe(self.d_data_inicio, self.d_data_fim, self.s_tipo_documento)
            del negCFOP

            # Altera o tipo da coluna #
            df['numr_cfop'] = df['numr_cfop'].astype(int)
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

    def agrega_motivos_exclusao(self, df) -> pd.DataFrame:
        etapaProcess = f"class {__name__} - def agrega_motivos_exclusao."
        # loga_mensagem(etapaProcess)

        try:
            df['tipo_exclusao'] = 0

            # Inclui dados para gravar tabela itens com CFOP não participantes #
            filtroCFOP = df['indi_cfop_particip'] == False
            df.loc[filtroCFOP, 'tipo_exclusao'] = EnumTipoExclusao.Item.value
            # loga_mensagem(f'Quantidade de linhas com filtro de CFOP: {len(df[filtroCFOP])}')

            # Filtra notas canceladas #
            filtroCancel = df['indi_situacao_documento'] == 'S'
            df.loc[filtroCancel, 'codg_motivo_exclusao'] = EnumMotivoExclusao.DocumentoCancelado.value

            # Concatena todos os filtros #
            filtroItem = ~(filtroCancel | filtroCFOP)
            df['indi_participa_calculo'] = filtroItem

            # Filtra notas com nenhum item que participa do cálculo #

            # Soma valores de va, por nota e indicação de participante do cálculo
            df_agg = df.groupby(['id_conv_115', 'indi_participa_calculo']).agg({'valr_va': 'sum'}).reset_index()

            # Transforma linhas em colunas
            pivot_df = pd.pivot_table(df_agg, index='id_conv_115', columns='indi_participa_calculo', values='valr_va',
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
            df_merged = pd.merge(df, pivot_df, on='id_conv_115', how='left')

            # Agrupa por documento fiscal
            df_cfop = df_merged.groupby(['id_conv_115']).agg({'data_emissao_documento': 'first',
                                                         'numr_ref_emissao': 'first',
                                                         'id_contrib_ipm_saida': 'first',
                                                         'id_contrib_ipm_entrada': 'first',
                                                         'codg_tipo_doc': 'first',
                                                         'id_processamento': 'first',
                                                         'valr_va': 'sum',
                                                         'codg_motivo_exclusao': 'max',
                                                         'tipo_exclusao': 'max',
                                                         'valr_nao_participa': 'first',
                                                         'valr_participa': 'first',
                                                         'codg_municipio_entrada': 'first',
                                                         'codg_municipio_saida': 'first',
                                                         'indi_aprop': 'first',
                                                         }
                                                        ).reset_index()

            # Caso todos os itens não participem do cálculo, marca o documento sem itens.
            # Motivos que excluem o documento são desprezados
            filtro = df_cfop['valr_participa'] == 0
            if len(df_cfop[filtro]) > 0:
                for indice, row in df_cfop[filtro].iterrows():
                    if row['tipo_exclusao'] != EnumTipoExclusao.Documento.value:
                        df_cfop.loc[
                            indice, 'codg_motivo_exclusao'] = EnumMotivoExclusao.DocumentoNaoContemItemParticipante.value

            df_cfop = df_cfop.drop(columns=['tipo_exclusao'])
            return df_cfop

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def formata_itens_doc(self, procesm, df) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def ordena_itens_doc."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f'{procesm.id_procesm_indice} Formata itens de documentos. {len(df)} itens participantes.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            df = df.replace({np.nan: None})

            df.rename(columns={'id_conv_115': 'CODG_DOCUMENTO_PARTCT_CALCULO',
                               'numr_ordem_item': 'CODG_ITEM_DOCUMENTO',
                               'codg_tipo_doc': 'CODG_TIPO_DOC_PARTCT_DOCUMENTO',
                               'id_processamento': 'ID_PROCESM_INDICE',
                               'codg_motivo_exclusao': 'CODG_MOTIVO_EXCLUSAO_CALCULO',
                               'numr_cfop': 'CODG_CFOP',
                               'id_produto_ncm': 'ID_PRODUTO_NCM',
                               'valr_va': 'VALR_ADICIONADO'}, inplace=True)

            colunas_item_doc = ['CODG_DOCUMENTO_PARTCT_CALCULO',
                                'CODG_ITEM_DOCUMENTO',
                                'CODG_TIPO_DOC_PARTCT_DOCUMENTO',
                                'ID_PROCESM_INDICE',
                                'CODG_MOTIVO_EXCLUSAO_CALCULO',
                                'CODG_CFOP',
                                'ID_PRODUTO_NCM',
                                'VALR_ADICIONADO',
                                ]

            loga_mensagem(etapaProcess + f' Processo finalizado. {len(df)} itens gravados - ' + str(
                datetime.now() - data_hora_atividade))

            return df[colunas_item_doc]

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def grava_itens_doc(self, procesm, df):
        etapaProcess = f"class {self.__class__.__name__} - def grava_itens_doc."
        # loga_mensagem(etapaProcess)

        try:
            # Grava tabela de itens participantes #
            etapaProcess = f'{procesm.id_procesm_indice} Gravando Itens participantes - {utilitarios.formatar_com_espacos(len(df), 11)} linhas.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            df_item = self.formata_itens_doc(procesm, df)

            db = ItemDocClasse()
            linhas_gravadas = db.grava_item_doc(df_item)
            del db

            loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_gravadas} itens gravados - ' + str(datetime.now() - data_hora_atividade))

            return linhas_gravadas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

