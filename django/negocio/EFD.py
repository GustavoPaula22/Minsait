from decimal import Decimal

from funcoes import utilitarios
from funcoes.constantes import EnumTipoDocumento, EnumTipoProcessamento, EnumStatusProcessamento, EnumParametros, \
    EnumMotivoExclusao, EnumTipoExclusao
from funcoes.utilitarios import *
from negocio.CFOP import CFOPClasse
from negocio.DocPartct import DocPartctClasse
from negocio.ItemDoc import ItemDocClasse
from negocio.Param import ParamClasse
from negocio.Procesm import ProcesmClasse

from negocio.CCE import *
from persistencia.Oraprodx9 import Oraprodx9
from persistencia.Oraprd import Oraprd
from polls.models import Processamento


class EFDClasse:
    etapaProcess = f"class {__name__} - class EFDClasse."
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
        self.s_tipo_documento = EnumTipoDocumento.EFD.value

    def carga_efd(self) -> str:
        etapaProcess = f"class {self.__class__.__name__} - def carga_efd. Período de {self.d_data_inicio} a {self.d_data_fim}"
        # loga_mensagem(etapaProcess)

        dData_hora_inicio = datetime.now()
        sData_hora_inicio = datetime.now(LOCAL_TZ).strftime('%d/%m/%Y %H:%M:%S,%f')
        sData_hora_fim = self.d_data_fim.strftime('%d/%m/%Y %H:%M:%S')

        try:
            # Registra inicio do processamento #
            etapaProcess = f'Registra inicio do processamento de carga de EFDs.'
            db = ProcesmClasse()
            procesm = db.iniciar_processamento(self.d_data_inicio, self.d_data_fim, self.s_tipo_documento)
            del db
            etapaProcess = f'{procesm.id_procesm_indice} Inicio da carga de EFDs. Período de {self.d_data_inicio} a {self.d_data_fim} - {sData_hora_inicio}'
            loga_mensagem(etapaProcess)

            try:
                # Carrega as EFDs geradas do período informado #
                etapaProcess = f'{procesm.id_procesm_indice} - Busca EFD para processamento - {self.d_data_inicio} a {self.d_data_fim}. '
                self.registra_processamento(procesm, etapaProcess)
                data_hora_atividade = datetime.now()
                df_efd = self.carrega_efd()

                loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_efd)} linhas selecionadas. - ' + str(
                    datetime.now() - data_hora_atividade))

                if len(df_efd) > 0:
                    self.trabalha_efd(procesm, df_efd)
                    self.codigo_retorno = 1
                    if self.d_data_fim >= datetime.strptime('2024-05-13 23:59:59', '%Y-%m-%d %H:%M:%S'):
                        self.codigo_retorno = 0
                else:
                    etapaProcess = f'{procesm.id_procesm_indice} Carga de EFDs de {self.d_data_inicio} a {self.d_data_fim} finalizada. Não foram selecionadas EFDs com CFOPs habilitados para cálculo no período.'
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
            f'{procesm.id_procesm_indice} Fim da carga de EFDs. Período de {self.d_data_inicio} a {self.d_data_fim} - {sData_hora_final} - Tempo de processamento: {(datetime.now() - dData_hora_inicio)}.')
        loga_mensagem(' ')

        return str(self.codigo_retorno)

    def trabalha_efd(self, procesm: Processamento, df):
        etapaProcess = f"class {self.__class__.__name__} - def trabalha_efd."
        # loga_mensagem(etapaProcess)

        try:
            # Exclui dados de processamentos anteriores #
            self.exclui_dados_anteriores(procesm, df[['ie_entrada', 'numr_ref_arquivo']].drop_duplicates())

            df['id_processamento'] = procesm.id_procesm_indice
            df['codg_motivo_exclusao'] = pd.NA
            df['id_produto_ncm'] = pd.NA
            df['indi_aprop'] = 'N'

            df['codg_tipo_doc'] = self.s_tipo_documento
            df['ie_entrada'] = df['ie_entrada'].astype(object)
            df['ie_saida'] = df['ie_saida'].astype(object)

            df['numr_ref_emissao'] = df['numr_ref_arquivo']

            df_cfop = self.inclui_cfop_participante(procesm, df)

            # Inclui informações do cadastro #
            df_cad = self.inclui_informacoes_cadastrais(procesm, df_cfop)

            # Inclui totos os motivos para exclusão do documento do cálculo do IPM #
            df_doc = self.agrega_motivos_exclusao(procesm, df_cad)

            # Grava tabela de documentos participantes #
            linhas_gravadas = self.grava_doc_partct(procesm, df_doc)

            if linhas_gravadas > 0:
                # Grava tabela de itens participantes #
                linhas_inseridas = self.grava_itens_doc(procesm, df_cfop)

                etapaProcess = f'{procesm.id_procesm_indice} Carga de EFDs de {self.d_data_inicio} a {self.d_data_fim} finalizada. Carregadas {linhas_gravadas} notas e {linhas_inseridas} itens.'
                procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                procesm.stat_procesm_indice = EnumStatusProcessamento.sucesso.value
                self.registra_processamento(procesm, etapaProcess)

                etapaProcess = f'Carga de EFDs de {self.d_data_inicio} a {self.d_data_fim} finalizada. Carregadas {len(df)} notas.'
                loga_mensagem(str(procesm.id_procesm_indice) + ' ' + etapaProcess)

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

        # try:
        #     etapaProcess = f'{procesm.id_procesm_indice} Inclui informações do cadastro CCE, para documentos da EFD'
        #     self.registra_processamento(procesm, etapaProcess)
        #     data_hora_atividade = datetime.now()
        #
        #     # O processamento dos contribuintes da EFD é feito por referencia do arquivo
        #     # Como a leitura dos documentos fiscais é feito pela data de entrega da EFD, existem várias datas de emissão dos documentos.
        #     # O valor da coluna NUMR_REFERENCIA_DOCUMENTO será a referencia o arquivo processado.
        #     # Esta atribuição é necessária para a exclusão dos dados armazenados em caso de retificaçãoes
        #
        #     dfs = []
        #     df_cads = []
        #     n_referencia = 0
        #     i = 0
        #     linhas_gravadas = 0
        #
        #     #  Monta inscrições para inclusão no cadastro
        #     df_cce = pd.DataFrame({'numr_ref_emissao': df['numr_ref_emissao'].tolist() + df['numr_ref_emissao'].tolist(),
        #                            'ie_entrada': df['ie_entrada'].tolist() + df['ie_saida'].tolist(),
        #                            'codg_municipio_entrada': df['codg_municipio_entrada'].tolist() + df['codg_municipio_saida'].tolist(),
        #                            'codg_uf_entrada': df['codg_uf_entrada'].tolist() + df['codg_uf_saida'].tolist(),})
        #
        #     df_cce = df_cce.drop_duplicates()
        #     df_cce['ie_saida'] = '0'
        #     df_cce['codg_municipio_saida'] = pd.NA
        #     df_cce['codg_uf_saida'] = pd.NA
        #     df_cce = df_cce.sort_values(by=['numr_ref_emissao', 'ie_entrada', 'codg_uf_entrada'])
        #     df_cce.dropna(subset=['numr_ref_emissao'], inplace=True)
        #
        #     cce = CCEClasse()
        #     for idx, linha in df_cce.iterrows():
        #         if n_referencia != linha['numr_ref_emissao']:
        #             if n_referencia != 0:
        #                 df_cads.append(cce.inclui_informacoes_cce(pd.DataFrame(dfs), self.d_data_inicio, self.d_data_fim, procesm))
        #
        #                 dfs = []
        #                 i = 0
        #
        #             n_referencia = linha['numr_ref_emissao']
        #
        #         nova_linha = {'ie_entrada': linha['ie_entrada'],
        #                       'ie_saida': linha['ie_saida'],
        #                       'codg_municipio_entrada': linha['codg_municipio_entrada'],
        #                       'codg_municipio_saida': linha['codg_municipio_saida'],
        #                       'codg_uf_entrada': linha['codg_uf_entrada'],
        #                       'codg_uf_saida': linha['codg_uf_saida'],
        #                       'numr_ref_emissao': linha['numr_ref_emissao'],
        #                       }
        #         dfs.append(nova_linha)
        #
        #     if len(dfs) > 0:
        #         df_cads.append(cce.inclui_informacoes_cce(pd.DataFrame(dfs), self.d_data_inicio, self.d_data_fim, procesm))
        #
        #     if len(df_cads) > 0:
        #         df_cads = [df.dropna(axis=1, how='all') for df in df_cads]
        #         df_concat = pd.concat(df_cads, axis=0, ignore_index=True).drop_duplicates()
        #         linhas_gravadas = len(df_concat)
        #         df_cad = self.concatena_dados_cadastrais(df, df_concat)
        #
        #     del cce
        #
        #     loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_gravadas} históricos gravados - ' + str(
        #         datetime.now() - data_hora_atividade))
        #
        #     return df_cad
        #
        # except Exception as err:
        #     etapaProcess += " - ERRO - " + str(err)
        #     loga_mensagem_erro(etapaProcess)
        #     raise

    def inclui_cfop_participante(self, procesm, df):
        etapaProcess = f"class {self.__class__.__name__} - def inclui_cfop_participante."
        # loga_mensagem(etapaProcess)

        try:
            # Busca CFOPs participantes
            etapaProcess = f'{procesm.id_procesm_indice} Busca CFOPs participantes do IPM.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            negCFOP = CFOPClasse()
            df_cfop = negCFOP.busca_cfop_nfe(self.d_data_inicio, self.d_data_fim, EnumTipoDocumento.EFD.value)
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

            # Concatena todos os filtros #
            filtroItem = ~(filtroCFOP)
            df['indi_participa_calculo'] = filtroItem

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

            return df_cfop

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def carrega_efd(self) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def carrega_efd - {self.d_data_inicio} a {self.d_data_fim}"
        # loga_mensagem(etapaProcess)

        try:
            # PATH = r'D:/SEFAZ/IPM/'
            # obj_retorno = sobe_csv(PATH + "df_efd.csv", "|")
            # filtro = obj_retorno['id_nfe'].isna()
            # return obj_retorno[~filtro]

            obj_retorno = []

            db = Oraprd()
            # db = Oraprodx9()
            df_arq = db.select_arq_efd(self.d_data_inicio, self.d_data_fim)

            # df_arq.name = 'df_arq'
            # baixa_csv(df_arq)

            if len(df_arq) == 0:
                del db
                return []
            else:
                dfs = []
                for idx, row in df_arq.iterrows():
                    obj_edf = db.select_efd(row['id_arquivo'])
                    dfs.append(obj_edf)

                del db

                df_efd = pd.concat(dfs, ignore_index=True)

                if len(df_efd) == 0:
                    return []
                else:
                    obj_retorno = pd.merge(df_arq, df_efd, on=['id_arquivo'], how='left')

                    obj_retorno.name = 'obj_retorno'
                    baixa_csv(obj_retorno)

            filtro = obj_retorno['id_nfe'].isna()

            obj_retorno.name = 'df_efd'
            baixa_csv(obj_retorno)

            return obj_retorno[~filtro]

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    # def agrega_itens(self, procesm, df) -> pd.DataFrame:
    #     etapaProcess = f"class {self.__class__.__name__} - def agrega_itens."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f'{procesm.id_procesm_indice} Totaliza documentos participantes.'
    #         self.registra_processamento(procesm, etapaProcess)
    #         data_hora_atividade = datetime.now()
    #
    #         # Agrupa por documento fiscal
    #         df_chave = df.groupby(['id_nfe']).agg({'data_emissao_doc': 'first',
    #                                                'numr_ref_arquivo': 'first',
    #                                                'id_contrib_ipm_saida': 'first',
    #                                                'id_contrib_ipm_entrada': 'first',
    #                                                'codg_tipo_doc': 'first',
    #                                                'id_processamento': 'first',
    #                                                'valr_va': 'sum',
    #                                                'codg_motivo_exclusao': 'max',
    #                                                'codg_municipio_entrada': 'first',
    #                                                'codg_municipio_saida': 'first',
    #                                                'indi_aprop': 'first',
    #                                                }
    #                                               ).reset_index()
    #
    #         loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_chave)} doscumentos gerados - ' + str(
    #             datetime.now() - data_hora_atividade))
    #
    #         return df_chave
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    def exclui_dados_anteriores(self, procesm: Processamento, df):
        etapaProcess = f"class {self.__class__.__name__} - def exclui_dados_anteriores. Período de {self.d_data_inicio} a {self.d_data_fim}"
        # loga_mensagem(etapaProcess)

        try:
            # Como a leitura dos documentos fiscais é feito pela data de entrega da EFD, existem várias datas de emissão dos documentos.
            # O valor da coluna NUMR_REFERENCIA_DOCUMENTO será a referencia o arquivo processado.
            # Esta atribuição é necessária para a exclusão dos dados armazenados em caso de retificaçãoes
            #
            # A exclusão de dados de processamentos anteriores para a EFD é feita pelo número da inscrição de entrada e a referencia do movimento.

            etapaProcess = f'{procesm.id_procesm_indice} Exclui itens dos documentos participantes.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            db = ItemDocClasse()
            linhas_excluidas = db.exclui_item_documento_efd(df)
            del db

            loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_excluidas} linhas excluidas - ' + str(datetime.now() - data_hora_atividade))

            etapaProcess = f'{procesm.id_procesm_indice} Exclui documentos participantes.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            db = DocPartctClasse()
            linhas_excluidas = db.exclui_doc_participante_efd(df)
            del db

            loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_excluidas} linhas excluidas - ' + str(datetime.now() - data_hora_atividade))

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)

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
            filtro = df['id_nfe'] != 0

            etapaProcess = f'{procesm.id_procesm_indice} Grava documentos participantes - {utilitarios.formatar_com_espacos(len(df[filtro]), 11)}' \
                               f' linhas.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            if len(df[filtro]) > 0:
                df_doc = self.formata_doc_participante(procesm, df[filtro])

                # Grava tabela de documentos participantes #
                db = DocPartctClasse()
                linhas_gravadas = db.grava_doc_participante_efd(df_doc)
                del db

            loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_gravadas} Docs Partct gravados - ' + str(datetime.now() - data_hora_atividade))

            return linhas_gravadas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def formata_itens_doc(self, procesm, df) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def ordena_itens_doc."
        # loga_mensagem(etapaProcess)

        try:
            df.rename(columns={'id_nfe': 'codg_documento_partct_calculo',
                               'id_item_nota_fiscal': 'codg_item_documento',
                               'codg_tipo_doc': 'codg_tipo_doc_partct_documento',
                               'id_processamento': 'id_procesm_indice',
                               'codg_motivo_exclusao': 'codg_motivo_exclusao_calculo',
                               'numr_cfop': 'codg_cfop',
                               'id_produto_ncm': 'id_produto_ncm',
                               'valr_va': 'valr_adicionado'}, inplace=True)

            cols_com_nan = ['codg_motivo_exclusao_calculo']
            df[cols_com_nan] = df[cols_com_nan].replace({np.nan: None})

            df['ID_PRODUTO_NCM'] = df['id_produto_ncm'].astype('Int64')

            colunas_item_doc = ['codg_documento_partct_calculo',
                                'codg_item_documento',
                                'codg_tipo_doc_partct_documento',
                                'id_procesm_indice',
                                'codg_motivo_exclusao_calculo',
                                'codg_cfop',
                                'id_produto_ncm',
                                'valr_adicionado',
                                ]

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
            linhas_gravadas = db.grava_item_doc_efd(df_item)
            del db

            loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_gravadas} itens gravados - ' + str(datetime.now() - data_hora_atividade))

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
            db = ProcesmClasse()
            db.atualizar_situacao_processamento(procesm)
            del db

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def concatena_dados_cadastrais(self, df_doc, df_cad):
        etapaProcess = f"class {__name__} - def concatena_dados_cadastrais."
        # loga_mensagem(etapaProcess)

        try:
            loga_mensagem(f"concatena_dados_cadastrais - Qdade de linhas do df_cad - {len(df_doc)} - {len(df_cad)}")

            # Concatena dados do documento fiscal com os dados cadastrais da inscrição estadual de entrada
            df_doc['ie_entrada'] = df_doc['ie_entrada'].astype(str)
            df_cad['numr_inscricao_contrib'] = df_cad['numr_inscricao_contrib'].astype(str)
            df_doc['codg_uf_entrada'].fillna('GO', inplace=True)
            df_nfe_cad = df_doc.merge(df_cad,
                                  left_on=['ie_entrada', 'codg_uf_entrada'],
                                  right_on=['numr_inscricao_contrib', 'codg_uf'],
                                  how='left',
                                  )

            loga_mensagem(f"concatena_dados_cadastrais - Qdade de linhas do df_nfe_cad - {len(df_nfe_cad)}")
            df_doc.name = 'df_doc'
            baixa_csv(df_doc)
            df_nfe_cad.name = 'df_nfe_cad'
            baixa_csv(df_nfe_cad)

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
            df_nfe_cad['ie_saida'] = df_nfe_cad['ie_saida'].astype(str)
            df_nfe_cad['ie_saida'] = df_nfe_cad['ie_saida'].str.replace('.0', '', regex=False)
            df_nfe_cad['codg_uf_saida'].fillna('GO', inplace=True)
            df_nfe_cad1 = df_nfe_cad.merge(df_cad,
                                           left_on=['ie_saida', 'codg_uf_saida'],
                                           right_on=['numr_inscricao_contrib', 'codg_uf'],
                                           how='left',
                                           )
            if 'data_fim_vigencia' in df_nfe_cad1.columns:
                df_nfe_cad1.drop(columns=['data_fim_vigencia'], inplace=True)

            df_nfe_cad1.drop(columns=['numr_inscricao_contrib',
                                      'data_inicio_vigencia',
                                      ], inplace=True)

            df_nfe_cad1.rename(columns={'id_contrib_ipm': 'id_contrib_ipm_saida'}, inplace=True)
            df_nfe_cad1.rename(columns={'indi_produtor_rural': 'indi_produtor_rural_saida'}, inplace=True)
            df_nfe_cad1.rename(columns={'stat_cadastro_contrib': 'stat_cadastro_contrib_saida'}, inplace=True)
            df_nfe_cad1.rename(columns={'tipo_enqdto_fiscal': 'tipo_enqdto_fiscal_saida'}, inplace=True)
            df_nfe_cad1.rename(columns={'codg_municipio': 'codg_municipio_cad_saida'}, inplace=True)
            df_nfe_cad1.rename(columns={'codg_uf': 'codg_uf_inscricao_saida'}, inplace=True)

            loga_mensagem(f"concatena_dados_cadastrais - Qdade de linhas do df_nfe_cad - {len(df_nfe_cad1)}")
            df_nfe_cad1.name = 'df_nfe_cad1'
            baixa_csv(df_nfe_cad1)

            return df_nfe_cad1

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    # def carrega_incricoes_efd(self, p_periodo):
    #     etapaProcess = f"class {__name__} - def carrega_incricoes_efd. Carrega dados cadastrais para as EFDs de {p_periodo}."
    #     # loga_mensagem(etapaProcess)
    #
    #     s_tipo_procesm = EnumTipoProcessamento.importacaoCCE.value
    #
    #     dData_hora_inicio = datetime.now()
    #     sData_hora_inicio = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S,%f')
    #     etapaProcess = f'Inicio da carga das informações cadastrais das EFDs para {p_periodo} - {sData_hora_inicio}'
    #     loga_mensagem(etapaProcess)
    #
    #     try:
    #         # Registra inicio do processamento #
    #         etapaProcess = 'Registra inicio do processamento de carga de informações cadastrais das EFDs'
    #         procesm = self.negProcesm.iniciar_processamento(s_tipo_procesm)
    #
    #         # Exclui dados de processamentos anteriores #
    #         # etapaProcess = f'{procesm.id_procesm_indice} Exclui histórico de contribuintes para {p_periodo}.'
    #         # self.registra_processamento(procesm, etapaProcess)
    #         # data_hora_atividade = datetime.now()
    #         # cce = CCEClasse()
    #         # linhas_excluidas = cce.exclui_historico_cce(p_periodo)
    #         # loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_excluidas} linhas excluidas - ' + str(datetime.now() - data_hora_atividade))
    #
    #         # Carrega as inscrições das EFDs geradas do período informado #
    #         etapaProcess = f'{procesm.id_procesm_indice} Busca as inscrições das EFDs para processamento - {p_periodo}. '
    #         self.registra_processamento(procesm, etapaProcess)
    #         data_hora_atividade = datetime.now()
    #
    #         db = Oraprd()
    #         df_cce = db.select_inscricoes_efd(p_periodo)
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
    #             linhas_incluidas = cce.inclui_informacoes_cce(df_cce)
    #             del cce
    #             loga_mensagem(etapaProcess + f' Processo finalizado. Incluidas {linhas_incluidas} linhas - ' + str(datetime.now() - data_hora_atividade))
    #
    #             etapaProcess = f'Carga das informações cadastrais das EFDs para {p_periodo} finalizado. Carregadas 0 históricos.'
    #             loga_mensagem(str(procesm.id_procesm_indice) + ' ' + etapaProcess)
    #             procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
    #             procesm.stat_procesm_indice = EnumStatusProcessamento.sucesso.value
    #             self.registra_processamento(procesm, etapaProcess)
    #
    #         else:
    #             etapaProcess = f'Carga das informações cadastrais das EFDs para {p_periodo} finalizado. Não foram selecionadas inscrições para o período.'
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
