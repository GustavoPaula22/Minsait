import pandas as pd

from funcoes import utilitarios
from funcoes.constantes import EnumTipoProcessamento, EnumStatusProcessamento, EnumParametros, EnumTipoDocumento, \
    EnumMotivoExclusao, EnumTipoExclusao
from negocio.CCE import CCEClasse
from negocio.CFOP import CFOPClasse
from negocio.DocPartct import DocPartctClasse
from negocio.GEN import GenClass
from negocio.ItemDoc import ItemDocClasse
from negocio.NCM import NCMClasse
from negocio.Param import ParamClasse
from negocio.Procesm import ProcesmClasse
from persistencia.OraProd import oraProd
from persistencia.Oraprd import *
from persistencia.Oraprodx9 import Oraprodx9
# from persistencia.Trino import Trino


class NFeClasse:
    etapaProcess = f"class {__name__} - class NFeClasse."

    # loga_mensagem(etapaProcess)

    def __init__(self, p_data_inicio, p_data_fim):
        if p_data_inicio is None \
                or p_data_fim is None:
            etapaProcess = f"class {self.__class__.__name__} - def carga_nfe.__init__. Período de processamento {self.d_data_inicio} a {self.d_data_fim} inválido!"
            loga_mensagem_erro(etapaProcess)
            raise ValueError(etapaProcess)

        self.codigo_retorno = 0
        self.d_data_inicio = p_data_inicio
        self.d_data_fim = p_data_fim
        self.i_ref = self.d_data_inicio.year * 100 + self.d_data_inicio.month
        self.inicio_referencia = os.getenv('INICIO_REFERENCIA')
        self.fim_referencia = os.getenv('FIM_REFERENCIA')

    def carga_nfe(self, p_tipo_nfe) -> str:
        etapaProcess = f"class {self.__class__.__name__} - def carga_nfe({p_tipo_nfe}). Período de {self.d_data_inicio} a {self.d_data_fim}."
        # loga_mensagem(etapaProcess)

        if p_tipo_nfe == EnumTipoDocumento.NFe.value:
            s_tipo_procesm = EnumTipoProcessamento.importacaoNFe.value
        elif p_tipo_nfe == EnumTipoDocumento.NFCe.value:
            s_tipo_procesm = EnumTipoProcessamento.importacaoNFCe.value
        elif p_tipo_nfe == EnumTipoDocumento.NFeRecebida.value:
            s_tipo_procesm = EnumTipoProcessamento.importacaoNFeRecebida.value

        dData_hora_inicio = datetime.now()
        sData_hora_inicio = datetime.now(LOCAL_TZ).strftime('%d/%m/%Y %H:%M:%S,%f')
        sData_hora_fim = self.d_data_fim.strftime('%d/%m/%Y %H:%M:%S')

        try:
            # Registra inicio do processamento #
            etapaProcess = f'Registra inicio do processamento de carga de NF-es({p_tipo_nfe}).'
            db = ProcesmClasse()
            procesm = db.iniciar_processamento(self.d_data_inicio, self.d_data_fim, s_tipo_procesm)
            del db
            etapaProcess = f'{procesm.id_procesm_indice} Inicio da carga de NF-e({p_tipo_nfe}). Período de {self.d_data_inicio} a {self.d_data_fim} - {sData_hora_inicio}'
            loga_mensagem(etapaProcess)

            df_nfe = []
            try:
                # Carrega os NF-es recebidas do período informado #
                etapaProcess = f'{procesm.id_procesm_indice} ' \
                               f'Busca NF-e({p_tipo_nfe}) para processamento - {self.d_data_inicio} a {self.d_data_fim}. '
                self.registra_processamento(procesm, etapaProcess)
                data_hora_atividade = datetime.now()
                if p_tipo_nfe == EnumTipoDocumento.NFe.value:
                    df_nfe = self.carrega_nfe_gerada()
                elif p_tipo_nfe == EnumTipoDocumento.NFCe.value:
                    df_nfe = self.carrega_nfce_gerada()
                elif p_tipo_nfe == EnumTipoDocumento.NFeRecebida.value:
                    df_nfe = self.carrega_nfe_recebida()

                loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_nfe)} linhas selecionadas. - ' + str(
                    datetime.now() - data_hora_atividade))

                # PATH = r'D:/SEFAZ/IPM/'
                # df_nfe = sobe_csv(PATH + "df_nfe.csv", "|")
                #
                if len(df_nfe) > 0:

                    # df_nfe.name = 'df_nfe'
                    # baixa_csv(df_nfe)

                    df_nfe['codg_tipo_doc'] = p_tipo_nfe
                    self.trabalha_nfe(procesm, df_nfe, p_tipo_nfe)
                    self.codigo_retorno = 1

                else:
                    etapaProcess = f'{procesm.id_procesm_indice} Carga de NF-es({p_tipo_nfe}) de {self.d_data_inicio} a {self.d_data_fim} finalizada. Não foram selecionadas NF-es com CFOPs habilitados para cálculo no período.'
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
            f'{procesm.id_procesm_indice} Fim da carga de NF-e({p_tipo_nfe}). Período de {self.d_data_inicio} a {self.d_data_fim} - {sData_hora_final} - Tempo de processamento: {(datetime.now() - dData_hora_inicio)}.')
        loga_mensagem(' ')

        return str(self.codigo_retorno)

    def trabalha_nfe(self, procesm: Processamento, df, p_tipo_nfe):
        etapaProcess = f"class {self.__class__.__name__} - def trabalha_nfe."
        # loga_mensagem(etapaProcess)

        try:
            # Exclui dados de processamentos anteriores #
            self.exclui_dados_anteriores(procesm, p_tipo_nfe)

            df['codg_anp'] = df['codg_anp'].astype(int)
            df['id_processamento'] = procesm.id_procesm_indice

            df_cfop = self.inclui_cfop_participante(procesm, df, p_tipo_nfe)

            # Inclui informações do cadastro #
            df_cad = self.inclui_informacoes_cadastrais(procesm, df_cfop)

            # Inclui informações de apropriacao #
            df_nfe_cad = self.inclui_informacoes_apropriacao(procesm, df_cad)

            # Inclui totos os motivos para exclusão do documento do cálculo do IPM #
            df_doc = self.agrega_motivos_exclusao(procesm, df_nfe_cad)

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

                etapaProcess = f'{procesm.id_procesm_indice} Carga de NF-es({p_tipo_nfe}) de {self.d_data_inicio} a {self.d_data_fim} finalizada. Carregadas {len(df_doc)} notas e {len(df_cfop)} itens.'
                procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                procesm.stat_procesm_indice = EnumStatusProcessamento.sucesso.value
                self.registra_processamento(procesm, etapaProcess)
            else:
                etapaProcess = f'{procesm.id_procesm_indice} Carga de NF-es({p_tipo_nfe}) de {self.d_data_inicio} a {self.d_data_fim} finalizado. Não foram selecionadas NF-es para cálculo do IPM no período.'
                procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                procesm.stat_procesm_indice = EnumStatusProcessamento.sucessoSemDados.value
                self.registra_processamento(procesm, etapaProcess)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def inclui_informacoes_cadastrais(self, procesm, df):
        etapaProcess = f"class {self.__class__.__name__} - def inclui_informações_cadastrais."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f'{procesm.id_procesm_indice} Inclui informações do cadastro CCE para documentos da NF-e.'
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

            # Inicializa a coluna como 'N'
            df['indi_aprop'] = 'N'

            # Mapeamento de valores
            df['s_prod_rural_dest'] = np.where(df['indi_produtor_rural_entrada'] == 'S', 'Prod', 'Não Prod')
            df['s_stat_cadastro_dest'] = np.where(df['stat_cadastro_contrib_entrada'] == '1', 'Ativo', 'Não Cad')
            df['s_tipo_enqdto_fiscal_dest'] = np.where(df['tipo_enqdto_fiscal_entrada'].isin(['3', '4', '5']), 'Simples', 'Normal')

            df['s_prod_rural_remet'] = np.where(df['indi_produtor_rural_saida'] == 'S', 'Prod', 'Não Prod')
            df['s_stat_cadastro_remet'] = np.where(df['stat_cadastro_contrib_saida'] == '1', 'Ativo', 'Não Cad')
            df['s_tipo_enqdto_fiscal_remet'] = np.where(df['tipo_enqdto_fiscal_saida'].isin(['3', '4', '5']), 'Simples', 'Normal')

            # Condições para atribuição do 'indi_aprop'
            condicoes = [
                (df['s_stat_cadastro_dest'] == 'Ativo') &
                ((df['s_tipo_enqdto_fiscal_dest'] == 'Normal') | (df['s_prod_rural_dest'] == 'Prod')) &
                (df['s_stat_cadastro_remet'] == 'Ativo') &
                ((df['s_tipo_enqdto_fiscal_remet'] == 'Normal') | (df['s_prod_rural_remet'] == 'Prod')),

                (df['s_stat_cadastro_dest'] == 'Ativo') &
                ((df['s_tipo_enqdto_fiscal_dest'] == 'Normal') | (df['s_prod_rural_dest'] == 'Prod')) &
                (df['s_stat_cadastro_remet'] == 'Ativo') &
                (df['s_tipo_enqdto_fiscal_remet'] == 'Simples'),

                (df['s_stat_cadastro_dest'] == 'Ativo') &
                ((df['s_tipo_enqdto_fiscal_dest'] == 'Normal') | (df['s_prod_rural_dest'] == 'Prod')) &
                (df['s_stat_cadastro_remet'] == 'Não Cad'),

                (df['s_stat_cadastro_dest'] == 'Ativo') &
                (df['s_tipo_enqdto_fiscal_dest'] == 'Simples') &
                (df['s_stat_cadastro_remet'] == 'Ativo') &
                ((df['s_tipo_enqdto_fiscal_remet'] == 'Normal') | (df['s_prod_rural_remet'] == 'Prod')),

                (df['s_stat_cadastro_dest'] == 'Ativo') &
                (df['s_tipo_enqdto_fiscal_dest'] == 'Simples') &
                (df['s_stat_cadastro_remet'] == 'Ativo') &
                (df['s_tipo_enqdto_fiscal_remet'] == 'Simples'),

                (df['s_stat_cadastro_dest'] == 'Ativo') &
                (df['s_tipo_enqdto_fiscal_dest'] == 'Simples') &
                (df['s_stat_cadastro_remet'] == 'Não Cad'),

                (df['s_stat_cadastro_dest'] == 'Não Cad') &
                (df['s_stat_cadastro_remet'] == 'Ativo') &
                ((df['s_tipo_enqdto_fiscal_remet'] == 'Normal') | (df['s_prod_rural_remet'] == 'Prod')),

                (df['s_stat_cadastro_dest'] == 'Não Cad') &
                (df['s_stat_cadastro_remet'] == 'Ativo') &
                (df['s_tipo_enqdto_fiscal_remet'] == 'Simples'),

                (df['s_stat_cadastro_dest'] == 'Não Cad') &
                (df['s_stat_cadastro_remet'] == 'Não Cad')
            ]

            valores = ['A', 'E', 'E', 'S', 'N', 'N', 'S', 'N', 'N']

            df['indi_aprop'] = np.select(condicoes, valores, default='N')

            return df

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def inclui_cfop_participante(self, procesm, df, p_tipo_nfe):
        etapaProcess = f"class {self.__class__.__name__} - def inclui_cfop_participante."
        # loga_mensagem(etapaProcess)

        try:
            # Busca CFOPs participantes
            etapaProcess = f'{procesm.id_procesm_indice} Busca CFOPs participantes do IPM.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            negCFOP = CFOPClasse()
            df_cfop = negCFOP.busca_cfop_nfe(self.d_data_inicio, self.d_data_fim, p_tipo_nfe)
            del negCFOP

            # df_cfop.name = 'df_cfop'
            # baixa_csv(df_cfop)
            #
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

    def exclui_dados_anteriores(self, procesm: Processamento, p_tipo_documento):
        etapaProcess = f"class {self.__class__.__name__} - def exclui_dados_anteriores. Período de {self.d_data_inicio} a {self.d_data_fim}"
        # loga_mensagem(etapaProcess)

        try:
            # Exclui dados de processamentos anteriores #
            etapaProcess = f'{procesm.id_procesm_indice} Exclui Itens participantes.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            db = ItemDocClasse()
            linhas_excluidas = db.exclui_item_documento_etapas(p_tipo_documento, self.d_data_inicio, self.d_data_fim)
            del db

            loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_excluidas} linhas excluidas - ' + str(
                datetime.now() - data_hora_atividade))

            # etapaProcess = f'{procesm.id_procesm_indice} Exclui documentos participantes.'
            # self.registra_processamento(procesm, etapaProcess)
            # data_hora_atividade = datetime.now()
            #
            # db = DocNaoPartctClasse()
            # linhas_excluidas = db.exclui_doc_nao_participante(p_tipo_documento, self.d_data_inicio, self.d_data_fim)
            # del db
            #
            # loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_excluidas} linhas excluidas - ' + str(
            #     datetime.now() - data_hora_atividade))

            etapaProcess = f'{procesm.id_procesm_indice} Exclui documentos participantes.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            db = DocPartctClasse()
            linhas_excluidas = db.exclui_doc_participante(p_tipo_documento, self.d_data_inicio, self.d_data_fim)
            del db

            loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_excluidas} linhas excluidas - ' + str(
                datetime.now() - data_hora_atividade))

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def carrega_nfe_gerada(self) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def carrega_nfe_gerada - Periodo: {self.d_data_inicio} a {self.d_data_fim}."
        # loga_mensagem(etapaProcess)

        df = []

        try:
            db = Oraprodx9()  # Persistência utilizando o Django
            # db = Oraprd()  # Persistência utilizando o Django
            # db = oraProd.create_instance()  # Persistência utilizando o oracledb
            data_hora_atividade = datetime.now()
            df = db.select_nfe_gerada(self.d_data_inicio, self.d_data_fim)
            del db
            loga_mensagem(etapaProcess + f' Processo finalizado. {len(df)} linhas selecionadas. - ' + str(datetime.now() - data_hora_atividade))

            if len(df) > 0:
                data_hora_atividade = datetime.now()
                etapaProcess = "Leitura da tabela de municípios"
                municipios = df[['codg_municipio_entrada', 'codg_municipio_saida']].stack().unique()  # Concatena municípios de entrada e saída
                df_municipio = self.carrega_municipio_nfe(municipios)
                # loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_municipio)} linhas selecionadas. - ' + str(datetime.now() - data_hora_atividade))

                if len(df_municipio) > 0:
                    etapaProcess = "Merge entre NF-es e municípios - codg_municipio_entrada"
                    data_hora_atividade = datetime.now()
                    # df['codg_municipio_entrada'] = df['codg_municipio_entrada'].astype(int)
                    df = df.merge(df_municipio,  # Município de Entrada do GEN
                                  left_on=['codg_municipio_entrada'],
                                  right_on=['codg_ibge'],
                                  how='left',
                                  )
                    df.drop(columns=['codg_ibge',
                                     'codg_municipio_entrada',
                                     ], inplace=True
                            )
                    df.rename(columns={'codg_municipio': 'codg_municipio_entrada'}, inplace=True)
                    df.rename(columns={'codg_uf': 'codg_uf_inform_entrada'}, inplace=True)
                    # loga_mensagem(etapaProcess + f' Processo finalizado - ' + str(datetime.now() - data_hora_atividade))

                    etapaProcess = "Merge entre NF-es e municípios - codg_municipio_saida"
                    data_hora_atividade = datetime.now()
                    # df['codg_municipio_saida'] = df['codg_municipio_saida'].astype(int)
                    df = df.merge(df_municipio,  # Município de Saída do GEN
                                  left_on=['codg_municipio_saida'],
                                  right_on=['codg_ibge'],
                                  how='left',
                                  )
                    df.drop(columns=['codg_ibge',
                                     'codg_municipio_saida',
                                     ], inplace=True
                            )
                    df.rename(columns={'codg_municipio': 'codg_municipio_saida'}, inplace=True)
                    df.rename(columns={'codg_uf': 'codg_uf_inform_saida'}, inplace=True)
                    # loga_mensagem(etapaProcess + f' Processo finalizado - ' + str(datetime.now() - data_hora_atividade))

                    etapaProcess = "Substitui a UF no documento pela UF do Município (se o município for informado)"
                    data_hora_atividade = datetime.now()
                    # Substitui a UF no documento pela UF do Município (se o município for informado)
                    df['codg_uf_entrada'] = np.where(df['codg_municipio_entrada'].notna(), df['codg_uf_inform_entrada'],
                                                     df['codg_uf_entrada'])
                    df['codg_uf_saida'] = np.where(df['codg_municipio_saida'].notna(), df['codg_uf_inform_saida'],
                                                   df['codg_uf_saida'])

                    df.drop(columns=['codg_uf_inform_entrada',
                                     'codg_uf_inform_saida',
                                     ], inplace=True)
                    # loga_mensagem(etapaProcess + f' Processo finalizado - ' + str(datetime.now() - data_hora_atividade))

                else:
                    df['codg_municipio_entrada'] = None
                    df['codg_municipio_saida'] = None

                return df
                # return df_retorno
            else:
                return []

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def carrega_nfce_gerada(self) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def carrega_nfce_gerada - Periodo: {self.d_data_inicio} a {self.d_data_fim}."
        # loga_mensagem(etapaProcess)

        df = []

        try:
            db = Oraprodx9()
            # db = Oraprd()
            # db = oraProd.create_instance()

            data_hora_atividade = datetime.now()
            df = db.select_nfce_gerada(self.d_data_inicio, self.d_data_fim)
            del db
            loga_mensagem(etapaProcess + f' Processo finalizado. {len(df)} linhas selecionadas. - ' + str(datetime.now() - data_hora_atividade))

            if len(df) > 0:
                data_hora_atividade = datetime.now()
                etapaProcess = "Leitura da tabela de municípios"
                municipios = df[['codg_municipio_entrada', 'codg_municipio_saida']].stack().unique()  # Concatena municípios de entrada e saída
                df_municipio = self.carrega_municipio_nfe(municipios)
                # loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_municipio)} linhas selecionadas. - ' + str(datetime.now() - data_hora_atividade))

                if len(df_municipio) > 0:
                    etapaProcess = "Merge entre NF-es e municípios - codg_municipio_entrada"
                    data_hora_atividade = datetime.now()

                    df['codg_municipio_entrada'] = pd.to_numeric(df['codg_municipio_entrada'], errors='coerce')
                    df_municipio['codg_ibge'] = pd.to_numeric(df_municipio['codg_ibge'], errors='coerce')

                    df = df.merge(df_municipio,  # Município de Entrada do GEN
                                  left_on=['codg_municipio_entrada'],
                                  right_on=['codg_ibge'],
                                  how='left',
                                  )
                    df.drop(columns=['codg_ibge',
                                     'codg_municipio_entrada',
                                     ], inplace=True
                            )
                    df.rename(columns={'codg_municipio': 'codg_municipio_entrada'}, inplace=True)
                    df.rename(columns={'codg_uf': 'codg_uf_inform_entrada'}, inplace=True)
                    # loga_mensagem(etapaProcess + f' Processo finalizado - ' + str(datetime.now() - data_hora_atividade))

                    etapaProcess = "Merge entre NF-es e municípios - codg_municipio_saida"
                    data_hora_atividade = datetime.now()

                    df['codg_municipio_saida'] = pd.to_numeric(df['codg_municipio_saida'], errors='coerce')

                    df = df.merge(df_municipio,  # Município de Saída do GEN
                                  left_on=['codg_municipio_saida'],
                                  right_on=['codg_ibge'],
                                  how='left',
                                  )
                    df.drop(columns=['codg_ibge',
                                     'codg_municipio_saida',
                                     ], inplace=True
                            )
                    df.rename(columns={'codg_municipio': 'codg_municipio_saida'}, inplace=True)
                    df.rename(columns={'codg_uf': 'codg_uf_inform_saida'}, inplace=True)
                    # loga_mensagem(etapaProcess + f' Processo finalizado - ' + str(datetime.now() - data_hora_atividade))

                    etapaProcess = "Substitui a UF no documento pela UF do Município (se o município for informado)"
                    data_hora_atividade = datetime.now()
                    # Substitui a UF no documento pela UF do Município (se o município for informado)
                    df['codg_uf_entrada'] = np.where(df['codg_municipio_entrada'].notna(), df['codg_uf_inform_entrada'],
                                                     df['codg_uf_entrada'])
                    df['codg_uf_saida'] = np.where(df['codg_municipio_saida'].notna(), df['codg_uf_inform_saida'],
                                                   df['codg_uf_saida'])

                    df.drop(columns=['codg_uf_inform_entrada',
                                     'codg_uf_inform_saida',
                                     ], inplace=True)
                    # loga_mensagem(etapaProcess + f' Processo finalizado - ' + str(datetime.now() - data_hora_atividade))

                else:
                    df['codg_municipio_entrada'] = None
                    df['codg_municipio_saida'] = None

                return df
                # return df_retorno
            else:
                return []

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def carrega_nfe_recebida(self) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def carrega_nfe_recebida - Periodo: {self.d_data_inicio} a {self.d_data_fim}."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            # db = Oraprodx9()
            # db = oraProd.create_instance()  # Persistência utilizando o oracledb
            df = db.select_nfe_recebida(self.d_data_inicio, self.d_data_fim)
            del db

            if len(df) > 0:
                data_hora_atividade = datetime.now()
                etapaProcess = "Leitura da tabela de municípios"
                municipios = df[['codg_municipio_entrada', 'codg_municipio_saida']].stack().unique()  # Concatena municípios de entrada e saída
                df_municipio = self.carrega_municipio_nfe(municipios)
                loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_municipio)} linhas selecionadas. - ' + str(datetime.now() - data_hora_atividade))

                if len(df_municipio) > 0:
                    etapaProcess = "Merge entre NF-es e municípios - codg_municipio_entrada"
                    data_hora_atividade = datetime.now()
                    # df['codg_municipio_entrada'] = df['codg_municipio_entrada'].astype(int)
                    df = df.merge(df_municipio,  # Município de Entrada do GEN
                                  left_on=['codg_municipio_entrada'],
                                  right_on=['codg_ibge'],
                                  how='left',
                                  )
                    df.drop(columns=['codg_ibge',
                                     'codg_municipio_entrada',
                                     ], inplace=True
                            )
                    df.rename(columns={'codg_municipio': 'codg_municipio_entrada'}, inplace=True)
                    df.rename(columns={'codg_uf': 'codg_uf_inform_entrada'}, inplace=True)
                    # loga_mensagem(etapaProcess + f' Processo finalizado - ' + str(datetime.now() - data_hora_atividade))

                    etapaProcess = "Merge entre NF-es e municípios - codg_municipio_saida"
                    data_hora_atividade = datetime.now()
                    # df['codg_municipio_saida'] = df['codg_municipio_saida'].astype(int)
                    df = df.merge(df_municipio,  # Município de Saída do GEN
                                  left_on=['codg_municipio_saida'],
                                  right_on=['codg_ibge'],
                                  how='left',
                                  )
                    df.drop(columns=['codg_ibge',
                                     'codg_municipio_saida',
                                     ], inplace=True
                            )
                    df.rename(columns={'codg_municipio': 'codg_municipio_saida'}, inplace=True)
                    df.rename(columns={'codg_uf': 'codg_uf_inform_saida'}, inplace=True)
                    # loga_mensagem(etapaProcess + f' Processo finalizado - ' + str(datetime.now() - data_hora_atividade))

                    etapaProcess = "Substitui a UF no documento pela UF do Município (se o município for informado)"
                    data_hora_atividade = datetime.now()
                    # Substitui a UF no documento pela UF do Município (se o município for informado)
                    df['codg_uf_entrada'] = np.where(df['codg_municipio_entrada'].notna(), df['codg_uf_inform_entrada'],
                                                     df['codg_uf_entrada'])
                    df['codg_uf_saida'] = np.where(df['codg_municipio_saida'].notna(), df['codg_uf_inform_saida'],
                                                   df['codg_uf_saida'])

                    df.drop(columns=['codg_uf_inform_entrada',
                                     'codg_uf_inform_saida',
                                     ], inplace=True)
                    # loga_mensagem(etapaProcess + f' Processo finalizado - ' + str(datetime.now() - data_hora_atividade))

                else:
                    df['codg_municipio_entrada'] = None
                    df['codg_municipio_saida'] = None

                return df
                # return df_retorno
            else:
                return []

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def le_nfe_simples(self) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def le_nfe_simples - Periodo: {self.d_data_inicio} a {self.d_data_fim}."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprodx9()
            df = db.select_nfe_simples(self.d_data_inicio, self.d_data_fim)
            del db

            if len(df) > 0:
                data_hora_atividade = datetime.now()
                etapaProcess = "Leitura da tabela de municípios"
                municipios = df[['codg_municipio_entrada', 'codg_municipio_saida']].stack().unique()  # Concatena municípios de entrada e saída
                df_municipio = self.carrega_municipio_nfe(municipios)
                loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_municipio)} linhas selecionadas. - ' + str(datetime.now() - data_hora_atividade))

                if len(df_municipio) > 0:
                    etapaProcess = "Merge entre NF-es e municípios - codg_municipio_entrada"
                    data_hora_atividade = datetime.now()
                    # df['codg_municipio_entrada'] = df['codg_municipio_entrada'].astype(int)
                    df = df.merge(df_municipio,  # Município de Entrada do GEN
                                  left_on=['codg_municipio_entrada'],
                                  right_on=['codg_ibge'],
                                  how='left',
                                  )
                    df.drop(columns=['codg_ibge',
                                     'codg_municipio_entrada',
                                     ], inplace=True
                            )
                    df.rename(columns={'codg_municipio': 'codg_municipio_entrada'}, inplace=True)
                    df.rename(columns={'codg_uf': 'codg_uf_inform_entrada'}, inplace=True)
                    # loga_mensagem(etapaProcess + f' Processo finalizado - ' + str(datetime.now() - data_hora_atividade))

                    etapaProcess = "Merge entre NF-es e municípios - codg_municipio_saida"
                    data_hora_atividade = datetime.now()
                    # df['codg_municipio_saida'] = df['codg_municipio_saida'].astype(int)
                    df = df.merge(df_municipio,  # Município de Saída do GEN
                                  left_on=['codg_municipio_saida'],
                                  right_on=['codg_ibge'],
                                  how='left',
                                  )
                    df.drop(columns=['codg_ibge',
                                     'codg_municipio_saida',
                                     ], inplace=True
                            )
                    df.rename(columns={'codg_municipio': 'codg_municipio_saida'}, inplace=True)
                    df.rename(columns={'codg_uf': 'codg_uf_inform_saida'}, inplace=True)
                    # loga_mensagem(etapaProcess + f' Processo finalizado - ' + str(datetime.now() - data_hora_atividade))

                    etapaProcess = "Substitui a UF no documento pela UF do Município (se o município for informado)"
                    data_hora_atividade = datetime.now()
                    # Substitui a UF no documento pela UF do Município (se o município for informado)
                    df['codg_uf_entrada'] = np.where(df['codg_municipio_entrada'].notna(), df['codg_uf_inform_entrada'],
                                                     df['codg_uf_entrada'])
                    df['codg_uf_saida'] = np.where(df['codg_municipio_saida'].notna(), df['codg_uf_inform_saida'],
                                                   df['codg_uf_saida'])

                    df.drop(columns=['codg_uf_inform_entrada',
                                     'codg_uf_inform_saida',
                                     ], inplace=True)
                    # loga_mensagem(etapaProcess + f' Processo finalizado - ' + str(datetime.now() - data_hora_atividade))

                else:
                    df['codg_municipio_entrada'] = None
                    df['codg_municipio_saida'] = None

                return df
                # return df_retorno
            else:
                return []

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def carrega_municipio_nfe(self, df) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def carrega_municipio_nfe."
        loga_mensagem(etapaProcess)

        try:
            gen = GenClass()
            df_municip = gen.carrega_municipio_ibge(df.tolist())
            del gen

            return df_municip

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

            # Filtra notas canceladas #
            filtroCancel = df['numr_protocolo_cancel'] != 0
            # loga_mensagem(f'Quantidade de linhas com documento cancelado: {len(df[filtroCancel])}')
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

            # loga_mensagem(f'Quantidade de linhas com filtro de cadastro: {len(df[filtroCad])}')
            df.loc[filtroCad, 'codg_motivo_exclusao'] = EnumMotivoExclusao.ContribsNaoCadOuSimples.value
            df.loc[filtroCad, 'tipo_exclusao'] = EnumTipoExclusao.Documento.value

            # Filtros para teste
            # filtroCad = df['id_nfe'] == 386199175
            # df.loc[filtroCad, 'codg_motivo_exclusao'] = EnumMotivoExclusao.ContribsNaoCadOuSimples.value
            # df.loc[filtroCad, 'tipo_exclusao'] = EnumTipoExclusao.Documento.value

            # Concatena todos os filtros #
            filtroItem = ~(filtroCFOP | filtroCancel | filtroCad)
            df['indi_participa_calculo'] = filtroItem
            # loga_mensagem(f'Quantidade de linhas com filtro de CFOP: {len(df[~filtroItem])}')

            # Filtra notas com nenhum item que participa do cálculo #

            # Soma valores de va, por nota e indicação de participaçao
            df_agg = df.groupby(['id_nfe', 'indi_participa_calculo']).agg({'valr_va': 'sum'}).reset_index()

            # Transforma linhas em colunas
            pivot_df = pd.pivot_table(df_agg, index='id_nfe', columns='indi_participa_calculo', values='valr_va',
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
            df_merged = pd.merge(df, pivot_df, on='id_nfe', how='left')

            # Agrupa por documento fiscal
            df_cfop = df_merged.groupby(['id_nfe']).agg({'data_emissao_doc': 'first',
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

            # df_cfop.name = 'agrega_motivos_exclusao_cfop'
            # baixa_csv(df_cfop)

            return df_cfop

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def formata_itens_doc(self, procesm, df) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def ordena_itens_doc."
        # loga_mensagem(etapaProcess)

        try:
            # Busca ID dos NCMs
            db = NCMClasse()
            df['numr_ncm'] = df['numr_ncm'].astype(str).str.zfill(8)
            df_ncm = db.busca_ncm_doc_fiscal(df['numr_ncm'].drop_duplicates().tolist(), self.d_data_inicio, self.d_data_fim)

            # Inclui Id do NCM
            df_merge = df.merge(df_ncm,
                                left_on=['numr_ncm'],
                                right_on=['codg_produto_ncm'],
                                how='left')

            df_merge.rename(columns={'id_nfe': 'codg_documento_partct_calculo',
                                     'id_item_nota_fiscal': 'codg_item_documento',
                                     'codg_tipo_doc': 'codg_tipo_doc_partct_calc',
                                     'id_processamento': 'id_procesm_indice',
                                     'codg_motivo_exclusao': 'codg_motivo_exclusao_calculo',
                                     'numr_cfop': 'codg_cfop',
                                     'valr_va': 'valr_adicionado'}, inplace=True)

            df_merge.rename(columns={'codg_documento_partct_calculo': 'CODG_DOCUMENTO_PARTCT_CALCULO',
                                     'codg_item_documento': 'CODG_ITEM_DOCUMENTO',
                                     'codg_tipo_doc_partct_calc': 'CODG_TIPO_DOC_PARTCT_DOCUMENTO',
                                     'id_procesm_indice': 'ID_PROCESM_INDICE',
                                     'codg_motivo_exclusao_calculo': 'CODG_MOTIVO_EXCLUSAO_CALCULO',
                                     'codg_cfop': 'CODG_CFOP',
                                     'id_produto_ncm': 'ID_PRODUTO_NCM',
                                     'valr_adicionado': 'VALR_ADICIONADO'}, inplace=True)

            cols_com_nan = ['CODG_MOTIVO_EXCLUSAO_CALCULO']
            df_merge[cols_com_nan] = df_merge[cols_com_nan].replace({np.nan: None})

            df_merge['ID_PRODUTO_NCM'] = df_merge['ID_PRODUTO_NCM'].astype('Int64')

            colunas_item_doc = ['CODG_DOCUMENTO_PARTCT_CALCULO',
                                'CODG_ITEM_DOCUMENTO',
                                'CODG_TIPO_DOC_PARTCT_DOCUMENTO',
                                'ID_PROCESM_INDICE',
                                'CODG_MOTIVO_EXCLUSAO_CALCULO',
                                'CODG_CFOP',
                                'ID_PRODUTO_NCM',
                                'VALR_ADICIONADO',
                                ]

            return df_merge[colunas_item_doc]

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

    def formata_doc_participante(self, procesm, df) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def formata_doc_participante."
        # loga_mensagem(etapaProcess)

        try:
            df['id_contrib_ipm_saida'] = df['id_contrib_ipm_saida'].replace('0', pd.NA)
            df['id_contrib_ipm_entrada'] = df['id_contrib_ipm_entrada'].replace('0', pd.NA)

            df.rename(columns={'id_nfe': 'codg_documento_partct_calculo',
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

    def grava_doc_partct(self, procesm, df):
        etapaProcess = f"class {self.__class__.__name__} - def grava_doc_partct."
        # loga_mensagem(etapaProcess)

        linhas_gravadas = 0

        try:
            # Filtra documentos com valor maior do que zero
            # filtro = df['valr_participa'] != 0
            filtro = df['id_nfe'] != 0

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

    def altera_doc_partct(self, procesm, df):
        etapaProcess = f"class {self.__class__.__name__} - def altera_doc_partct."
        # loga_mensagem(etapaProcess)

        linhas_gravadas = 0

        try:
            # Filtra documentos com valor maior do que zero
            # filtro = df['valr_participa'] != 0
            filtro = df['id_nfe'] != 0

            etapaProcess = f'{procesm.id_procesm_indice} Altera documentos participantes - {utilitarios.formatar_com_espacos(len(df[filtro]), 11)}' \
                               f' linhas.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            if len(df[filtro]) > 0:
                df_doc = self.formata_doc_participante(procesm, df[filtro])

                # Grava tabela de documentos participantes #
                db = DocPartctClasse()
                linhas_gravadas = db.altera_doc_partct_cad(df_doc)
                del db

            # loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_gravadas} Docs Partct gravados - ' + str(datetime.now() - data_hora_atividade))

            return linhas_gravadas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def acerto_nfe_cfop(self, p_tipo_nfe) -> str:
        etapaProcess = f"class {self.__class__.__name__} - def acerto_nfe_cfop({p_tipo_nfe}). Período de {self.d_data_inicio} a {self.d_data_fim}."
        # loga_mensagem(etapaProcess)

        if p_tipo_nfe == EnumTipoDocumento.NFe.value:
            s_tipo_procesm = EnumTipoProcessamento.processaAcertoNFe.value
        elif p_tipo_nfe == EnumTipoDocumento.NFCe.value:
            s_tipo_procesm = EnumTipoProcessamento.processaAcertoNFCe.value
        elif p_tipo_nfe == EnumTipoDocumento.NFeRecebida.value:
            s_tipo_procesm = EnumTipoProcessamento.processaAcertoNFeRecebida.value

        dData_hora_inicio = datetime.now()
        sData_hora_inicio = datetime.now(LOCAL_TZ).strftime('%d/%m/%Y %H:%M:%S,%f')
        sData_hora_fim = self.d_data_fim.strftime('%d/%m/%Y %H:%M:%S')

        try:
            # Registra inicio do processamento #
            etapaProcess = f'Registra inicio do processamento de Acerto de CFOPs({p_tipo_nfe}).'
            db = ProcesmClasse()
            procesm = db.iniciar_processamento(self.d_data_inicio, self.d_data_fim, s_tipo_procesm)
            del db
            etapaProcess = f'{procesm.id_procesm_indice} Inicio da carga dos Documentos Fiscais({p_tipo_nfe}). Período de {self.d_data_inicio} a {self.d_data_fim} - {sData_hora_inicio}'
            loga_mensagem(etapaProcess)

            df_nfe = []
            try:
                # Limpa o Status de motivos de exclusão dos documentos fiscais recebidos do período informado #
                etapaProcess = f'{procesm.id_procesm_indice} ' \
                               f'Limpa motivo exclusão dos Docs Fiscais({p_tipo_nfe}) para processamento - {self.d_data_inicio} a {self.d_data_fim}. '
                self.registra_processamento(procesm, etapaProcess)
                data_hora_atividade = datetime.now()

                db = DocPartctClasse()
                linhas_atualizadas = db.limpa_motivo_excl_sem_item_partct(p_tipo_nfe, self.d_data_inicio, self.d_data_fim)
                del db

                loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_atualizadas} linhas selecionadas. - ' + str(datetime.now() - data_hora_atividade))

                # Carrega os NF-es recebidas do período informado #
                etapaProcess = f'{procesm.id_procesm_indice} ' \
                               f'Busca Docs Fiscais({p_tipo_nfe}) para processamento - {self.d_data_inicio} a {self.d_data_fim}. '
                self.registra_processamento(procesm, etapaProcess)
                data_hora_atividade = datetime.now()

                db = DocPartctClasse()
                df_doc_partct = db.carrega_doc_partct_acerto_cfop(p_tipo_nfe, self.d_data_inicio, self.d_data_fim)
                del db

                loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_doc_partct)} linhas selecionadas. - ' + str(datetime.now() - data_hora_atividade))

                if len(df_doc_partct) > 0:
                    db = ItemDocClasse()
                    df_item_doc = db.carrega_item_doc(list(set(df_doc_partct['codg_documento_partct_calculo'].tolist())), p_tipo_nfe)
                    del db

                    loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_doc_partct)} linhas selecionadas. - ' + str(datetime.now() - data_hora_atividade))

                    df_cfop = self.inclui_cfop_participante(procesm, df_item_doc, p_tipo_nfe)

                    df_merge = df_doc_partct.merge(df_cfop,
                                                   on=['codg_documento_partct_calculo'],
                                                   how='left',
                                                   )

                    df_merge.rename(columns={'codg_motivo_exclusao_calculo_x': 'codg_motivo_exclusao_calculo_doc'}, inplace=True)
                    df_merge.rename(columns={'codg_motivo_exclusao_calculo_y': 'codg_motivo_exclusao_calculo_item'}, inplace=True)

                    etapaProcess = f'{procesm.id_procesm_indice} Atualiza CFOPs válidos ({p_tipo_nfe}) para processamento - {self.d_data_inicio} a {self.d_data_fim}. '
                    self.registra_processamento(procesm, etapaProcess)
                    data_hora_atividade = datetime.now()

                    df = self.agrega_motivos_exclusao_acerto(df_merge)

                    loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_doc_partct)} linhas selecionadas. - ' + str(datetime.now() - data_hora_atividade))

                    colunas_doc = ['codg_documento_partct_calculo',
                                   'codg_tipo_doc_partct_calc',
                                   'valr_adicionado_operacao',
                                   'codg_motivo_exclusao_calculo_doc']

                    etapaProcess = f'{procesm.id_procesm_indice} Atualiza Documentos ({p_tipo_nfe}) para processamento - {self.d_data_inicio} a {self.d_data_fim}. '
                    self.registra_processamento(procesm, etapaProcess)
                    data_hora_atividade = datetime.now()

                    db = DocPartctClasse()
                    filtro = df['codg_motivo_exclusao_calculo_doc'] > 0
                    i_docs_alterados = db.altera_doc_partct(df.loc[filtro, colunas_doc])
                    del db

                    loga_mensagem(etapaProcess + f' Processo finalizado. {i_docs_alterados} linhas selecionadas. - ' + str(datetime.now() - data_hora_atividade))

                    df_cfop.drop(columns=['codg_motivo_exclusao_calculo'], inplace=True)
                    df_cfop.rename(columns={'codg_motivo_exclusao': 'codg_motivo_exclusao_calculo'}, inplace=True)

                    colunas_item = ['codg_documento_partct_calculo',
                                    'codg_item_documento',
                                    'codg_tipo_doc_partct_documento',
                                    'codg_motivo_exclusao_calculo']

                    etapaProcess = f'{procesm.id_procesm_indice} Atualiza Itens dos Documentos ({p_tipo_nfe}) para processamento - {self.d_data_inicio} a {self.d_data_fim}. '
                    self.registra_processamento(procesm, etapaProcess)
                    data_hora_atividade = datetime.now()

                    db = ItemDocClasse()
                    filtro = ~pd.isna(df_cfop['codg_motivo_exclusao_calculo'])
                    i_itens_alterados = db.altera_item_partct(df_cfop.loc[filtro, colunas_item])
                    del db

                    loga_mensagem(etapaProcess + f' Processo finalizado. {len(df)} linhas selecionadas. - ' + str(datetime.now() - data_hora_atividade))

                    etapaProcess = f'{procesm.id_procesm_indice} Acerto de CFOPs({p_tipo_nfe}) de {self.d_data_inicio} a {self.d_data_fim} finalizado. {i_docs_alterados} documentos e {i_itens_alterados} itens alterados.'
                    procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                    procesm.stat_procesm_indice = EnumStatusProcessamento.sucesso.value
                    self.registra_processamento(procesm, etapaProcess)

                    self.codigo_retorno = 1

                else:
                    etapaProcess = f'{procesm.id_procesm_indice} Carga de Docs Fiscais ({p_tipo_nfe}) de {self.d_data_inicio} a {self.d_data_fim} finalizada. Não foram selecionados documentos para acerto de CFOP.'
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
        loga_mensagem(f'{procesm.id_procesm_indice} Fim da carga dos Documentos Fiscais({p_tipo_nfe}). Período de {self.d_data_inicio} a {self.d_data_fim} - {sData_hora_final} - Tempo de processamento: {(datetime.now() - dData_hora_inicio)}.')
        loga_mensagem(' ')

        return str(self.codigo_retorno)

    def acerto_nfe_cadastro(self, p_tipo_doc) -> str:
        etapaProcess = f"class {self.__class__.__name__} - def acerto_nfe_cadastro({p_tipo_doc})."
        # loga_mensagem(etapaProcess)

        if p_tipo_doc == EnumTipoDocumento.NFe.value:
            s_tipo_procesm = EnumTipoProcessamento.processaAcertoNFe.value
        elif p_tipo_doc == EnumTipoDocumento.NFCe.value:
            s_tipo_procesm = EnumTipoProcessamento.processaAcertoNFCe.value
        elif p_tipo_doc == EnumTipoDocumento.NFeRecebida.value:
            s_tipo_procesm = EnumTipoProcessamento.processaAcertoNFeRecebida.value

        dData_hora_inicio = datetime.now()
        sData_hora_inicio = datetime.now(LOCAL_TZ).strftime('%d/%m/%Y %H:%M:%S,%f')
        sData_hora_fim = self.d_data_fim.strftime('%d/%m/%Y %H:%M:%S')

        try:
            # Registra inicio do processamento #
            etapaProcess = f'Registra inicio do processamento de Acerto de NFes por erro de cadastro.'
            db = ProcesmClasse()
            procesm = db.iniciar_processamento(self.d_data_inicio, self.d_data_fim, s_tipo_procesm)
            del db
            etapaProcess = f'{procesm.id_procesm_indice} Inicio do processamento do acerto. Identificação de casos com erro - {sData_hora_inicio}'
            loga_mensagem(etapaProcess)

            df_nfe = []
            try:
                # Levantamento das inscrições que sofreram alteração #
                # Foi criada tabela IPM_CONTRIBUINTE_IPM_NOVO, que foi carregada com o novo cadastro de contribuintes #
                etapaProcess = f'{procesm.id_procesm_indice} - Busca contribuintes com alterações de cadastro.'
                self.registra_processamento(procesm, etapaProcess)
                data_hora_atividade = datetime.now()

                # db = Oraprd()
                # qtde_linhas = db.update_contrib_alterados(procesm.id_procesm_indice)
                # qtde_linhas += db.insert_contrib_novos(procesm.id_procesm_indice)
                # qtde_linhas += db.delete_contrib_excedentes()

                # df_alt = db.select_contrib_alterados()
                # if len(df_alt) > 0:
                #     filtro = df_alt['data_inicio_vigencia']>=datetime.strptime('20230101', '%Y%m%d')
                #     s_contrib = df_alt.loc[filtro, 'numr_inscricao_contrib'].drop_duplicates()
                #     s_contrib = df_alt['numr_inscricao_contrib'].drop_duplicates()
                #     qtde_linhas = db.update_contrib_alterados(procesm.id_procesm_indice)
                #     loga_mensagem(etapaProcess + f' Alteração de linhas existentes. {qtde_linhas} linhas atualizadas. - ' + str(datetime.now() - data_hora_atividade))
                # else:
                #     s_contrib = pd.Series()
                #
                # df_incl = db.select_contrib_novos()
                # if len(df_incl) > 0:
                #     filtro = df_incl['data_inicio_vigencia']>=datetime.strptime('20230101', '%Y%m%d')
                #     s_contrib = pd.concat([s_contrib, df_incl.loc[filtro, 'numr_inscricao_contrib']], ignore_index=True).drop_duplicates()
                #     s_contrib = pd.concat([s_contrib, df_incl['numr_inscricao_contrib']], ignore_index=True).drop_duplicates()
                #     qtde_linhas = db.insert_contrib_novos(procesm.id_procesm_indice)
                #     loga_mensagem(etapaProcess + f' Inclusão de linhas. {qtde_linhas} linhas inseridas. - ' + str(datetime.now() - data_hora_atividade))
                #
                # df_excl = db.select_contrib_excedentes()
                # if len(df_excl) > 0:
                #     filtro = df_excl['data_inicio_vigencia'] >= datetime.strptime('20230101', '%Y%m%d')
                #     s_contrib = pd.concat([s_contrib, df_excl.loc[filtro, 'numr_inscricao_contrib']],ignore_index=True).drop_duplicates()
                #     s_contrib = pd.concat([s_contrib, df_excl['numr_inscricao_contrib']],ignore_index=True).drop_duplicates()
                #     qtde_linhas = db.delete_contrib_excedentes()
                #     loga_mensagem(etapaProcess + f' Exclusão de linhas. {qtde_linhas} linhas excluidas. - ' + str(datetime.now() - data_hora_atividade))

                # df_inscr = s_contrib.to_frame()
                # df_inscr.name = 'df_inscr'
                # baixa_csv(df_inscr)

                # del db
                #
                # loga_mensagem(etapaProcess + f' Processo finalizado - ' + str(datetime.now() - data_hora_atividade))

                # nome_arquivo = 'df_inscr.csv'
                # s_separador = ';'
                # df_inscr = sobe_csv(nome_arquivo, s_separador)
                # #
                # db = Oraprd()
                # data_hora_atividade = datetime.now()
                # i_qtde_delete = db.delete_contrib_novo()
                # loga_mensagem(f' delete_contrib_novo - Processo finalizado - {i_qtde_delete} linhas excluídas.' + str(datetime.now() - data_hora_atividade))
                # #
                # data_hora_atividade = datetime.now()
                # i_qtde_insert = db.insert_contrib_nv(df_inscr)
                # loga_mensagem(f' insert_contrib_nv - Processo finalizado - {i_qtde_insert} linhas inseridas.' + str(datetime.now() - data_hora_atividade))
                # del db

                # Carrega os NF-es recebidas do período informado #
                etapaProcess = f'{procesm.id_procesm_indice} ' \
                               f'Busca Docs Fiscais para processamento - {self.d_data_inicio} a {self.d_data_fim}. '
                self.registra_processamento(procesm, etapaProcess)
                data_hora_atividade = datetime.now()

                # tamanho_lote = 1000
                # db = DocPartctClasse()
                # for lote in np.array_split(df_inscr['numr_inscricao_contrib'], len(df_inscr) // tamanho_lote + 1):
                #     df_doc_partct = db.carrega_doc_partct_acerto_cad(lote.tolist(), p_tipo_doc, self.d_data_inicio, self.d_data_fim)

                db = DocPartctClasse()
                df_doc_partct = db.carrega_doc_partct_acerto_cad(p_tipo_doc, self.d_data_inicio, self.d_data_fim)
                if len(df_doc_partct) > 0:
                    municipios = df_doc_partct[['codg_municipio_entrada', 'codg_municipio_saida']].stack().unique()  # Concatena municípios de entrada e saída
                    df_municipio = self.carrega_municipio_nfe(municipios)
                    if len(df_municipio) > 0:
                        df_compl = df_doc_partct.merge(df_municipio, left_on=['codg_municipio_entrada'], right_on=['codg_ibge'],how='left',)
                        df_compl.drop(columns=['codg_ibge', 'codg_municipio_entrada',], inplace=True)
                        df_compl.rename(columns={'codg_municipio': 'codg_municipio_entrada', 'codg_uf': 'codg_uf_entrada'}, inplace=True)

                        df_compl = df_compl.merge(df_municipio, left_on=['codg_municipio_saida'], right_on=['codg_ibge'], how='left',)
                        df_compl.drop(columns=['codg_ibge', 'codg_municipio_saida',], inplace=True)
                        df_compl.rename(columns={'codg_municipio': 'codg_municipio_saida', 'codg_uf': 'codg_uf_saida'}, inplace=True)

                        if len(df_compl) > 0:
                            df_compl['data_emissao_trunc'] = df_compl['data_emissao_documento'].dt.date
                            df_compl['numr_protocolo_cancel'] = df_compl['numr_protocolo_cancel'].astype(int)
                            df_compl['id_processamento'] = procesm.id_procesm_indice

                            for (data_emi, tipo_doc), df in df_compl.groupby(['data_emissao_trunc', 'codg_tipo_doc']):
                                loga_mensagem(' ')
                                loga_mensagem(f'Processa inscrições com alteração cadastral. Data de emissão dos documentos - {data_emi} - Tipo Doc - {tipo_doc} - Qdade {len(df)}')
                                self.d_data_inicio = data_emi
                                self.d_data_fim = data_emi
                                df_cfop = self.inclui_cfop_participante(procesm, df, tipo_doc)

                                # Inclui informações do cadastro #
                                df_cad = self.inclui_informacoes_cadastrais(procesm, df)

                                # Inclui informações de apropriacao #
                                df_nfe_cad = self.inclui_informacoes_apropriacao(procesm, df_cad)

                                # df_nfe_cad.name = 'df_nfe_aprop'
                                # baixa_csv(df_nfe_cad)

                                # Inclui totos os motivos para exclusão do documento do cálculo do IPM #
                                df_nfe_cad.rename(columns={'data_emissao_documento': 'data_emissao_doc'}, inplace=True)
                                df_doc = self.agrega_motivos_exclusao(procesm, df_nfe_cad)

                                # df_doc.name = 'df_nfe_motivos'
                                # baixa_csv(df_doc)

                                # Grava tabela de documentos participantes #
                                linhas_alteradas = self.altera_doc_partct(procesm, df_doc)

                                loga_mensagem(
                                    etapaProcess + f' Processo finalizado. {len(df_doc_partct)} contribuintes selecionadas. - ' + str(
                                        datetime.now() - data_hora_atividade))

                else:
                    df_compl = pd.DataFrame()

                del db

                # df_compl.name = 'df_doc_partct'
                # baixa_csv(df_compl)

                loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_compl)} linhas selecionadas. - ' + str(datetime.now() - data_hora_atividade))

                etapaProcess = f'{procesm.id_procesm_indice} Acerto do cadastro({p_tipo_doc}) de {self.d_data_inicio} a {self.d_data_fim} finalizado.'
                procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                procesm.stat_procesm_indice = EnumStatusProcessamento.sucesso.value
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
            f'{procesm.id_procesm_indice} Fim da carga dos Documentos Fiscais({p_tipo_doc}). Período de {self.d_data_inicio} a {self.d_data_fim} - {sData_hora_final} - Tempo de processamento: {(datetime.now() - dData_hora_inicio)}.')
        loga_mensagem(' ')

        return str(self.codigo_retorno)

    def agrega_motivos_exclusao_acerto(self, df) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def agrega_motivos_exclusao_acerto."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f'Totaliza documentos participantes.'

            df['tipo_exclusao'] = 0

            # Inclui dados para gravar tabela itens com CFOP não participantes #
            filtroCFOP = df['indi_cfop_particip'] == False
            df.loc[filtroCFOP, 'tipo_exclusao'] = 1  # EnumTipoExclusao.Item.value

            # Concatena todos os filtros #
            filtroItem = ~(filtroCFOP)
            df['indi_participa_calculo'] = filtroItem

            # Filtra notas com nenhum item que participa do cálculo #

            # Soma valores de va, por nota e indicação de participaçao
            df_agg = df.groupby(['codg_documento_partct_calculo', 'indi_participa_calculo']).agg(
                {'valr_adicionado': 'sum'}).reset_index()

            # Transforma linhas em colunas
            pivot_df = pd.pivot_table(df_agg, index='codg_documento_partct_calculo', columns='indi_participa_calculo',
                                      values='valr_adicionado',
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
            df_merged = pd.merge(df, pivot_df, on='codg_documento_partct_calculo', how='left')

            # Agrupa por documento fiscal
            df_cfop = df_merged.groupby(['codg_documento_partct_calculo']).agg({'codg_tipo_doc_partct_calc': 'first',
                                                                                'valr_adicionado': 'sum',
                                                                                'codg_motivo_exclusao_calculo_doc': 'max',
                                                                                'tipo_exclusao': 'max',
                                                                                'valr_nao_participa': 'first',
                                                                                'valr_participa': 'first',
                                                                                }
                                                                               ).reset_index()

            # Caso todos os itens não participem do cálculo, marca o documento sem itens.
            # Motivos que excluem o documento são desprezados
            filtro = df_cfop['valr_participa'] == 0
            if len(df_cfop[filtro]) > 0:
                for indice, row in df_cfop[filtro].iterrows():
                    if row['tipo_exclusao'] != 2:  # EnumTipoExclusao.Documento.value:
                        df_cfop.loc[
                            indice, 'codg_motivo_exclusao_calculo_doc'] = 12  # EnumMotivoExclusao.DocumentoNaoContemItemParticipante.value

            df_cfop = df_cfop.drop(columns=['tipo_exclusao'])

            filtro = df_cfop['valr_participa'] != 0
            df_cfop.loc[filtro, 'codg_motivo_exclusao_calculo_doc'] = 0

            df_cfop.rename(columns={'valr_participa': 'valr_adicionado_operacao'}, inplace=True)

            loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_cfop)} itens processados')

            return df_cfop

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            print(etapaProcess)
            raise

    def cancela_nfe(self, df):
        etapaProcess = f"class {self.__class__.__name__} - def cancela_nfe."
        # loga_mensagem(etapaProcess)

        linhas_gravadas = 0

        try:
            # Criar df_nfe
            df_chave_nfe = df[df['ChaveAcessoNFe'].str.startswith('52')].copy()
            df_chave_nfe_receb = df[~df['ChaveAcessoNFe'].str.startswith('52')].copy()

            db = Oraprd()
            df_nfe = db.select_nfe_gerada_pela_chave(df_chave_nfe['ChaveAcessoNFe'].tolist())
            if len(df_nfe) > 0:
                filtro = df_nfe['NUMR_PROTOCOLO_CANCEL'].isna()
                df_nfe['valr_adicionado_operacao'] = 0
                df_nfe['codg_tipo_doc_partct_calc'] = EnumTipoDocumento.NFe.value
                linhas_atualizadas = db.update_doc_partct_valr_excl(df_nfe[filtro])
                loga_mensagem(f'Processo de cancelamento de documentos fiscais (NFe) finalizado - {linhas_atualizadas} NF-es canceladas')
            else:
                loga_mensagem(f'Processo de cancelamento de documentos fiscais (NFe) finalizado - Nenhuma NF-e cancelada')

            df_nfe_receb = db.select_nfe_receb_pela_chave(df_chave_nfe_receb['ChaveAcessoNFe'].tolist())
            if len(df_nfe_receb) > 0:
                filtro = df_nfe_receb['NUMR_PROTOCOLO_CANCEL'].isna()
                df_nfe_receb['valr_adicionado_operacao'] = 0
                df_nfe_receb['codg_tipo_doc_partct_calc'] = EnumTipoDocumento.NFeRecebida.value
                linhas_atualizadas = db.update_doc_partct_valr_excl(df_nfe_receb[filtro])
                loga_mensagem(f'Processo de cancelamento de documentos fiscais (NFe Recebida) finalizado - {linhas_atualizadas} NF-es canceladas')
            else:
                loga_mensagem(f'Processo de cancelamento de documentos fiscais (NFe Recebida) finalizado - Nenhuma NF-e Recebida cancelada')

            del db

            return None

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
            db = ProcesmClasse()
            db.atualizar_situacao_processamento(procesm)
            del db

            return None

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise
