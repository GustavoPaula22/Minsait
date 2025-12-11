from datetime import datetime

import pandas as pd
import numpy as np

from funcoes.constantes import EnumTipoDocumento, EnumTipoProcessamento, EnumStatusProcessamento, EnumParametros, \
    EnumMotivoExclusao, EnumTipoExclusao
from funcoes.utilitarios import LOCAL_TZ, loga_mensagem, loga_mensagem_erro, baixa_csv

from negocio.ItemDoc import ItemDocClasse

from negocio.DocPartct import DocPartctClasse
from negocio.GEN import GenClass
from negocio.Param import ParamClasse
from negocio.Procesm import ProcesmClasse
from negocio.CCE import CCEClasse
from negocio.CFOP import CFOPClasse

from persistencia.Trino import Trino
from persistencia.Oraprd import Oraprd

from polls.models import Processamento
from funcoes import utilitarios


class NF3eClasse:
    etapaProcess = f"class {__name__} - class NF3elasse."
    # loga_mensagem(etapaProcess)

    negProcesm = ProcesmClasse()

    def __init__(self, p_data_inicio, p_data_fim, p_df_nf3e_evnt):
        self.codigo_retorno = 0
        self.d_data_inicio = p_data_inicio
        self.d_data_fim = p_data_fim
        self.s_tipo_documento = EnumTipoDocumento.NF3e.value
        self.df_nf3e_evnt = p_df_nf3e_evnt

    def carga_nf3e_eventos_cancelamento(self) -> pd.DataFrame:
        etapaProcess = f"class {__name__} - def carrega_nf3e_infeventos."
        # loga_mensagem(etapaProcess)

        try:
            db = Trino('nf3e')
            return db.get_nf3e_eventos_cancelamento(p_data_inicio=self.d_data_inicio)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def carga_nf3e(self) -> str:
        etapaProcess = f"class {self.__class__.__name__} - def carga_nf3e. Período de {self.d_data_inicio} a {self.d_data_fim}"
        # loga_mensagem(etapaProcess)

        s_tipo_procesm = EnumTipoProcessamento.importacaoNF3e.value

        dData_hora_inicio = datetime.now()
        sData_hora_inicio = datetime.now(LOCAL_TZ).strftime('%d/%m/%Y %H:%M:%S,%f')
        sData_hora_fim = self.d_data_fim.strftime('%d/%m/%Y %H:%M:%S')

        try:
            # Registra inicio do processamento #
            etapaProcess = f'Registra inicio do processamento de carga de NF3-e.'
            procesm = self.negProcesm.iniciar_processamento(self.d_data_inicio, self.d_data_fim, s_tipo_procesm)
            etapaProcess = f'{procesm.id_procesm_indice} Inicio da carga de NF3-e. Período de {self.d_data_inicio} a {self.d_data_fim} - {sData_hora_inicio}'
            loga_mensagem(etapaProcess)

            try:
                # Carrega as NF3-es geradas do período informado #
                etapaProcess = f'{procesm.id_procesm_indice} ' \
                               f'Busca NF3-e para processamento - {self.d_data_inicio} a {self.d_data_fim}. '
                self.registra_processamento(procesm, etapaProcess)
                data_hora_atividade = datetime.now()
                df_nf3e = self.carrega_nf3e()
                loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_nf3e)} linhas selecionadas. - ' + str(
                    datetime.now() - data_hora_atividade))

                if len(df_nf3e) > 0:
                    self.trabalha_nf3e(procesm, df_nf3e)
                    self.codigo_retorno = 1

                else:
                    etapaProcess = f'{procesm.id_procesm_indice} Carga de NF3-es de {self.d_data_inicio} a {self.d_data_fim} finalizada. Não foram selecionadas NF3-es habilitadas para cálculo no período.'
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
            f'{procesm.id_procesm_indice} Fim da carga de NF3-e. Período de {self.d_data_inicio} a {self.d_data_fim} - {sData_hora_final} - Tempo de processamento: {(datetime.now() - dData_hora_inicio)}.')
        loga_mensagem(' ')

        return str(self.codigo_retorno)

    def carrega_nf3e(self) -> pd.DataFrame:
        etapaProcess = f"class {__name__} - def carrega_nf3e."
        # loga_mensagem(etapaProcess)

        try:
            db = Trino('nf3e')
            df_nf3e = db.get_nf3e_infnf3e(p_data_inicio=self.d_data_inicio, p_data_fim=self.d_data_fim)
            del db

            if len(df_nf3e) > 0:
                df_nf3e['codg_municipio_saida'] = 0
                df_nf3e['desc_natureza_operacao'] = None
                df_nf3e['indi_tipo_operacao'] = 'S'
                df_nf3e['desc_destino_operacao'] = 'Operação interna'
                df_nf3e['desc_finalidade_operacao'] = 'NF3-e normal'
                df_nf3e['numr_gtin'] = 0
                df_nf3e['numr_cest'] = 0
                df_nf3e['numr_ncm'] = 0
                df_nf3e['qdade_itens'] = 0
                df_nf3e['codg_anp'] = 0
                df_nf3e['codg_tipo_doc'] = self.s_tipo_documento
                df_nf3e.rename(columns={'data_emissao_nf3e': 'data_emissao_doc'}, inplace=True)
                df_nf3e['numr_ref_emissao'] = pd.to_datetime(df_nf3e['data_emissao_doc']).dt.strftime("%Y%m")
                # Lookup gen_municipio
                df_lkp_mun = self.carrega_municipio_nf3e(df_nf3e)

                # Lookup nf3e eventos cancelamento
                df_retorno = df_lkp_mun.merge(self.df_nf3e_evnt,
                                              left_on=['id_nf3e'],
                                              right_on=['chnf3e'],
                                              how='left',
                                              )
                df_retorno.drop(columns=['chnf3e'], inplace=True)

                return df_retorno
            else:
                return []

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def carrega_municipio_nf3e(self, df) -> pd.DataFrame:
        etapaProcess = f"class {__name__} - def carrega_municipio_nf3e."
        # loga_mensagem(etapaProcess)

        try:
            gen = GenClass()
            municipios = df[['codg_municipio_entrada',
                             'codg_municipio_saida']].stack().unique()  # Concatena municípios de entrada e saída
            df_municip = gen.carrega_municipio_ibge(municipios.tolist())
            del gen

            if len(df_municip) > 0:
                # Converte os atributos em Int para efetuar join
                df['codg_municipio_entrada'] = df['codg_municipio_entrada'].fillna(0).astype(int)
                df['codg_municipio_saida'] = df['codg_municipio_saida'].fillna(0).astype(int)
                df_municip['codg_ibge'] = df_municip['codg_ibge'].fillna(0).astype(int)

                obj_retorno = df.merge(df_municip,  # Município de Entrada do GEN
                                       left_on=['codg_municipio_entrada'],
                                       right_on=['codg_ibge'],
                                       how='left',
                                       )
                obj_retorno.drop(columns=['codg_ibge',
                                          'codg_municipio_entrada',
                                          ], inplace=True)
                obj_retorno.rename(columns={'codg_municipio': 'codg_municipio_entrada'}, inplace=True)
                obj_retorno.rename(columns={'codg_uf': 'codg_uf_inform_entrada'}, inplace=True)

                obj_retorno = obj_retorno.merge(df_municip,  # Município de Saída do GEN
                                                left_on=['codg_municipio_saida'],
                                                right_on=['codg_ibge'],
                                                how='left',
                                                )
                obj_retorno.drop(columns=['codg_ibge',
                                          'codg_municipio_saida',
                                          ], inplace=True)

                obj_retorno.rename(columns={'codg_municipio': 'codg_municipio_saida',
                                            'codg_uf': 'codg_uf_inform_saida'}, inplace=True)

                # Substitui a UF no documento pela UF do Município (se o município for informado)
                # for idx, row in obj_retorno.iterrows():
                #     if not pd.isna(row.codg_municipio_entrada):
                #         obj_retorno.loc[idx, 'codg_uf_entrada'] = row.codg_uf_inform_entrada

                #     if not pd.isna(row.codg_municipio_saida):
                #         obj_retorno.loc[idx, 'codg_uf_saida'] = row.codg_uf_inform_saida

                # Substituir valores com base nas condições de forma vetorizada
                obj_retorno.loc[~obj_retorno['codg_municipio_entrada'].isna(), 'codg_uf_entrada'] = obj_retorno[
                    'codg_uf_inform_entrada']
                obj_retorno.loc[~obj_retorno['codg_municipio_saida'].isna(), 'codg_uf_saida'] = obj_retorno[
                    'codg_uf_inform_saida']

                obj_retorno.drop(columns=['codg_uf_inform_entrada',
                                          'codg_uf_inform_saida',
                                          ], inplace=True)

                return obj_retorno

            else:
                df['codg_municipio_entrada'] = None
                return df

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def trabalha_nf3e(self, procesm: Processamento, df):
        etapaProcess = f"class {self.__class__.__name__} - def trabalha_nf3e."
        # loga_mensagem(etapaProcess)

        try:
            # Exclui dados de processamentos anteriores #
            self.exclui_dados_anteriores(procesm, self.s_tipo_documento)

            df['codg_anp'] = df['codg_anp'].astype(int)
            df['id_processamento'] = procesm.id_procesm_indice

            df_cfop = self.inclui_cfop_participante(procesm, df)

            df_cfop = self.inclui_cclass_participante(procesm, df_cfop)

            # Inclui informações do cadastro #
            df_cad = self.inclui_informacoes_cadastrais(procesm, df_cfop)

            # Inclui informações de apropriacao #
            df_nf3e_cad = self.inclui_informacoes_apropriacao(procesm, df_cad)

            # Inclui motivos para exclusão do documento do cálculo do IPM
            df_doc = self.agrega_motivos_exclusao(procesm, df_nf3e_cad)

            # Grava tabela de itens participantes (IPM_ITEM_DOCUMENTO) #
            linhas_gravadas, df_conversao_nf3e = self.grava_doc_partct(procesm, df_doc)

            if linhas_gravadas > 0:
                # df_cfop.name = 'df_cfop'
                # baixa_csv(df_cfop)

                # Grava tabela de itens participantes #
                self.grava_itens_doc(procesm, df_cfop, df_conversao_nf3e)

                etapaProcess = f'{procesm.id_procesm_indice} Carga de NF3e de {self.d_data_inicio} a {self.d_data_fim} finalizada. Carregadas {len(df_doc)} notas.'
                procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                procesm.stat_procesm_indice = EnumStatusProcessamento.sucesso.value
                self.registra_processamento(procesm, etapaProcess)
            else:
                etapaProcess = f'{procesm.id_procesm_indice} Carga de NF3e de {self.d_data_inicio} a {self.d_data_fim} finalizado. Não foram selecionadas NF3e para cálculo do IPM no período.'
                procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                procesm.stat_procesm_indice = EnumStatusProcessamento.sucessoSemDados.value
                self.registra_processamento(procesm, etapaProcess)

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

            # Inclui dados para gravar tabela itens com CCLASS não participantes #
            filtroCCLASS = df['indi_cclass_particip'] == False
            df.loc[filtroCCLASS, 'tipo_exclusao'] = EnumTipoExclusao.Item.value

            # Multiplica por -1 quando a condição tipo_operacao_cclass == 'S'
            df.loc[df['tipo_operacao_cclass'] == 'S', 'valr_va'] *= -1

            # Multiplica por -1 quando ind_devolucao == '1'
            df.loc[df['ind_devolucao'] == '1', 'valr_va'] *= -1

            # Filtra notas canceladas #
            filtroCancel = df['dhevento'].notna()
            df.loc[filtroCancel, 'codg_motivo_exclusao'] = EnumMotivoExclusao.DocumentoCancelado.value
            df.loc[filtroCancel, 'tipo_exclusao'] = EnumTipoExclusao.Documento.value

            # Concatena todos os filtros #
            filtroItem = ~(filtroCFOP | filtroCCLASS | filtroCancel)
            df['indi_participa_calculo'] = filtroItem

            # Filtra notas com nenhum item que participa do cálculo #

            # Soma valores de va, por nota e indicação de participaçao
            df_agg = df.groupby(['id_nf3e', 'indi_participa_calculo']).agg({'valr_va': 'sum'}).reset_index()

            # Transforma linhas em colunas
            pivot_df = pd.pivot_table(df_agg, index='id_nf3e', columns='indi_participa_calculo', values='valr_va', fill_value=0)

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
            df_merged = pd.merge(df, pivot_df, on='id_nf3e', how='left')

            # Agrupa por documento fiscal
            df_cfop = df_merged.groupby(['id_nf3e']).agg({'data_emissao_doc': 'first',  # ?
                                                          'numr_ref_emissao': 'first',
                                                          'id_contrib_ipm_saida': 'first',
                                                          'id_contrib_ipm_entrada': 'first',
                                                          'codg_tipo_doc': 'first',
                                                          'id_processamento': 'first',
                                                          'valr_nf3e': 'first',
                                                          'valr_va': 'sum',
                                                          'codg_motivo_exclusao': 'max',
                                                          'tipo_exclusao': 'max',
                                                          'valr_nao_participa': 'first',
                                                          'valr_participa': 'first',
                                                          'codg_municipio_entrada': 'first',
                                                          'codg_municipio_cad_saida': 'first',
                                                          'indi_aprop': 'first',
                                                          'tipo_classe_consumo': 'first'
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
            df_cfop.rename(columns={'codg_municipio_cad_saida': 'codg_municipio_saida'}, inplace=True)

            filtro = df_cfop['valr_participa'] != 0
            df_cfop.loc[filtro, 'codg_motivo_exclusao'] = 0

            loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_cfop)} itens processados - ' + str(
                datetime.now() - data_hora_atividade))

            return df_cfop

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def inclui_cclass_participante(self, procesm, df):
        etapaProcess = f"class {self.__class__.__name__} - def inclui_cclass_participante."
        loga_mensagem(etapaProcess)

        try:
            etapaProcess = f'{procesm.id_procesm_indice} Busca CClass participantes do IPM.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            db = Oraprd()
            df_cclass = db.select_cclass_nf3e(self.d_data_inicio, self.d_data_fim)

            # Indica se o CCLASS participa do cálculo
            df['indi_cclass_particip'] = df['item_prod_cclass'].isin(df_cclass['codg_item_nf3e'])

            # Atribui motivo para não participar
            valor_constante = EnumMotivoExclusao.CClassNaoParticipante.value

            df.loc[df['codg_motivo_exclusao'].isna() & (
                ~df['indi_cclass_particip']), 'codg_motivo_exclusao'] = valor_constante

            # Realizar o merge para incluir o valor de tipo_operacao no df
            df = pd.merge(df, df_cclass[['codg_item_nf3e', 'tipo_operacao_cclass']],
                          left_on='item_prod_cclass',
                          right_on='codg_item_nf3e',
                          how='left'
                          )

            # Remover a coluna de chave adicional, se não for mais necessária
            df.drop(columns=['codg_item_nf3e'], inplace=True)

            loga_mensagem(etapaProcess + f' Processo finalizado. {len(df)} CClass participantes - ' + str(
                datetime.now() - data_hora_atividade))

            return df

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def inclui_informacoes_cadastrais(self, procesm, df):
        etapaProcess = f"class {self.__class__.__name__} - def inclui_informações_cadastrais."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f'{procesm.id_procesm_indice} Inclui informações do cadastro CCE para documentos da NF3e.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            cce = CCEClasse()
            df['codg_uf_saida'] = df['codg_uf_saida'].fillna('GO')
            df_cad = cce.inclui_informacoes_cce(df, self.d_data_inicio, self.d_data_fim, procesm)
            del cce

            linhas_gravadas = len(df_cad)
            loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_gravadas} históricos gravados - ' + str(
                datetime.now() - data_hora_atividade))

            # Concatena dados do documento fiscal com os dados cadastrais da inscrição estadual de entrada
            etapaProcess = f'{procesm.id_procesm_indice} Concatena dados do documento fiscal com os dados cadastrais da inscrição estadual de entrada.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            df['ie_entrada'] = df['ie_entrada'].astype(str)
            df['ie_saida'] = df['ie_saida'].astype(str)
            df_cad['numr_inscricao_contrib'] = df_cad['numr_inscricao_contrib'].astype(str)
            df['codg_uf_entrada'] = df['codg_uf_entrada'].fillna('GO')
            df_nf3e_cad = df.merge(df_cad,
                                   left_on=['ie_entrada', 'codg_uf_entrada'],
                                   right_on=['numr_inscricao_contrib', 'codg_uf'],
                                   how='left',
                                   )

            if 'data_fim_vigencia' in df_nf3e_cad.columns:
                df_nf3e_cad.drop(columns=['data_fim_vigencia'], inplace=True)

            df_nf3e_cad.drop(columns=['numr_inscricao_contrib',
                                      'data_inicio_vigencia',
                                      'operacao',
                                      ], inplace=True)

            df_nf3e_cad.rename(columns={'id_contrib_ipm': 'id_contrib_ipm_entrada',
                                        'indi_produtor_rural': 'indi_produtor_rural_entrada',
                                        'indi_produtor_rural_exclusivo': 'indi_produtor_rural_exclusivo_entrada',
                                        'stat_cadastro_contrib': 'stat_cadastro_contrib_entrada',
                                        'tipo_enqdto_fiscal': 'tipo_enqdto_fiscal_entrada',
                                        'codg_municipio': 'codg_municipio_cad_entrada',
                                        'codg_uf': 'codg_uf_inscricao_entrada'}, inplace=True)

            loga_mensagem(etapaProcess + f' Processo finalizado - ' + str(datetime.now() - data_hora_atividade))

            # Concatena dados do documento fiscal com os dados cadastrais da inscrição estadual de saída
            etapaProcess = f'{procesm.id_procesm_indice} Concatena dados do documento fiscal com os dados cadastrais da inscrição estadual de saída.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            df_nf3e_cad = df_nf3e_cad.merge(df_cad,
                                            left_on=['ie_saida', 'codg_uf_saida'],
                                            right_on=['numr_inscricao_contrib', 'codg_uf'],
                                            how='left',
                                            )

            if 'data_fim_vigencia' in df_nf3e_cad.columns:
                df_nf3e_cad.drop(columns=['data_fim_vigencia'], inplace=True)

            df_nf3e_cad.drop(columns=['numr_inscricao_contrib',
                                      'data_inicio_vigencia',
                                      'operacao',
                                      ], inplace=True)

            df_nf3e_cad.rename(columns={'id_contrib_ipm': 'id_contrib_ipm_saida',
                                        'indi_produtor_rural': 'indi_produtor_rural_saida',
                                        'indi_produtor_rural_exclusivo': 'indi_produtor_rural_exclusivo_saida',
                                        'stat_cadastro_contrib': 'stat_cadastro_contrib_saida',
                                        'tipo_enqdto_fiscal': 'tipo_enqdto_fiscal_saida',
                                        'codg_municipio': 'codg_municipio_cad_saida',
                                        'codg_uf': 'codg_uf_inscricao_saida'}, inplace=True)

            loga_mensagem(etapaProcess + f' Processo finalizado - ' + str(datetime.now() - data_hora_atividade))

            df_nf3e_cad.name = 'df_nf3e_cad'
            baixa_csv(df_nf3e_cad)

            return df_nf3e_cad

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

            # Definindo as colunas calculadas de forma vetorizada
            df['s_prod_rural_dest'] = np.where(df['indi_produtor_rural_entrada'] == 'S', 'Prod', 'Não Prod')
            df['s_stat_cadastro_dest'] = np.where(df['stat_cadastro_contrib_entrada'] == '1', 'Ativo', 'Não Cad')
            df['s_tipo_enqdto_fiscal_dest'] = np.where(df['tipo_enqdto_fiscal_entrada'].isin(['3', '4', '5']), 'Simples', 'Normal')

            df['s_prod_rural_remet'] = np.where(df['indi_produtor_rural_saida'] == 'S', 'Prod', 'Não Prod')
            df['s_stat_cadastro_remet'] = np.where(df['stat_cadastro_contrib_saida'] == '1', 'Ativo', 'Não Cad')
            df['s_tipo_enqdto_fiscal_remet'] = np.where(df['tipo_enqdto_fiscal_saida'].isin(['3', '4', '5']), 'Simples', 'Normal')

            # Aplicando a lógica das condições
            df['indi_aprop'] = np.select([(df['s_stat_cadastro_dest'] == 'Ativo')
                                       & ((df['s_tipo_enqdto_fiscal_dest'] == 'Normal') | (df['s_prod_rural_dest'] == 'Prod'))
                                       &  (df['s_stat_cadastro_remet'] == 'Ativo')
                                       & ((df['s_tipo_enqdto_fiscal_remet'] == 'Normal') | (df['s_prod_rural_remet'] == 'Prod'))
                                     ,    (df['s_stat_cadastro_dest'] == 'Ativo')
                                       & ((df['s_tipo_enqdto_fiscal_dest'] == 'Normal') | (df['s_prod_rural_dest'] == 'Prod'))
                                       &  (df['s_stat_cadastro_remet'] == 'Ativo')
                                       &  (df['s_tipo_enqdto_fiscal_remet'] == 'Simples')
                                     ,    (df['s_stat_cadastro_dest'] == 'Ativo')
                                       & ((df['s_tipo_enqdto_fiscal_dest'] == 'Normal') | (df['s_prod_rural_dest'] == 'Prod'))
                                       &  (df['s_stat_cadastro_remet'] == 'Não Cad')
                                     ,    (df['s_stat_cadastro_dest'] == 'Ativo')
                                       &  (df['s_tipo_enqdto_fiscal_dest'] == 'Simples')
                                       &  (df['s_stat_cadastro_remet'] == 'Ativo')
                                       & ((df['s_tipo_enqdto_fiscal_remet'] == 'Normal') | (df['s_prod_rural_remet'] == 'Prod'))
                                     ,    (df['s_stat_cadastro_dest'] == 'Ativo')
                                       &  (df['s_tipo_enqdto_fiscal_dest'] == 'Simples')
                                       &  (df['s_stat_cadastro_remet'] == 'Ativo')
                                       &  (df['s_tipo_enqdto_fiscal_remet'] == 'Simples')
                                     ,    (df['s_stat_cadastro_dest'] == 'Ativo')
                                       &  (df['s_tipo_enqdto_fiscal_dest'] == 'Simples')
                                       &  (df['s_stat_cadastro_remet'] == 'Não Cad')
                                     ,    (df['s_stat_cadastro_dest'] == 'Não Cad')
                                       &  (df['s_stat_cadastro_remet'] == 'Ativo')
                                       & ((df['s_tipo_enqdto_fiscal_remet'] == 'Normal') | (df['s_prod_rural_remet'] == 'Prod'))
                                     ,    (df['s_stat_cadastro_dest'] == 'Não Cad')
                                       &  (df['s_stat_cadastro_remet'] == 'Ativo')
                                       &  (df['s_tipo_enqdto_fiscal_remet'] == 'Simples')
                                     ,    (df['s_stat_cadastro_dest'] == 'Não Cad')
                                       &  (df['s_stat_cadastro_remet'] == 'Não Cad')
                                          ],
                                         ['A', 'E', 'E', 'S', 'N', 'N', 'S', 'N', 'N'], 'N')
            df.drop(columns=['s_prod_rural_dest',
                             's_stat_cadastro_dest',
                             's_tipo_enqdto_fiscal_dest',
                             's_prod_rural_remet',
                             's_stat_cadastro_remet',
                             's_tipo_enqdto_fiscal_remet',
                             ], inplace=True)

            return df

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

            # Remover as linhas onde há valores NaN
            df['numr_cfop'] = df['numr_cfop'].fillna(0)
            df_cfop = df_cfop.dropna(subset=['codg_cfop'])

            # Altera o tipo da coluna #
            df['numr_cfop'] = df['numr_cfop'].astype(int)
            df_cfop['codg_cfop'] = df_cfop['codg_cfop'].astype(int)

            # Indica se o CFOP participa do cálculo
            df['indi_cfop_particip'] = df['numr_cfop'].isin(df_cfop['codg_cfop'])

            # Atribui motivo para não participar
            df['codg_motivo_exclusao'] = df['indi_cfop_particip'].map(
                {True: None, False: EnumMotivoExclusao.CFOPNaoParticipante.value})

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

    def formata_itens_doc(self, procesm, df) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def ordena_itens_doc."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f'{procesm.id_procesm_indice} Formata itens de documentos. {len(df)} itens participantes.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            df.rename(columns={'id_nf3e': 'CODG_DOCUMENTO_PARTCT_CALCULO'}, inplace=True)
            df.rename(columns={'id_item_nota_fiscal': 'CODG_ITEM_DOCUMENTO'}, inplace=True)
            df.rename(columns={'codg_tipo_doc': 'CODG_TIPO_DOC_PARTCT_DOCUMENTO'}, inplace=True)
            df.rename(columns={'id_processamento': 'ID_PROCESM_INDICE'}, inplace=True)
            df.rename(columns={'codg_motivo_exclusao': 'CODG_MOTIVO_EXCLUSAO_CALCULO'}, inplace=True)
            df.rename(columns={'numr_cfop': 'CODG_CFOP'}, inplace=True)
            df.rename(columns={'valr_va': 'VALR_ADICIONADO'}, inplace=True)

            df['ID_PRODUTO_NCM'] = None

            try:
                motivo = df['CODG_MOTIVO_EXCLUSAO_CALCULO']
                if motivo is not None:
                    df['CODG_MOTIVO_EXCLUSAO_CALCULO'] = int(motivo)

            except (ValueError, TypeError):
                df['CODG_MOTIVO_EXCLUSAO_CALCULO'] = None

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

    def grava_itens_doc(self, procesm, df, df_conversao_nf3e):
        etapaProcess = f"class {self.__class__.__name__} - def grava_itens_doc."
        # loga_mensagem(etapaProcess)

        try:
            # Grava tabela de itens participantes #
            etapaProcess = f'{procesm.id_procesm_indice} Gravando Itens participantes - {utilitarios.formatar_com_espacos(len(df), 11)} linhas.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            df_item = self.formata_itens_doc(procesm, df)

            # df_item.name = 'df_item_doc'
            # baixa_csv(df_item)

            df_item = df_item.merge(df_conversao_nf3e,
                                    left_on=['CODG_DOCUMENTO_PARTCT_CALCULO'],
                                    right_on=['CODG_CHAVE_ACESSO_NF3E'],
                                    how='left')

            df_item.drop(columns=['CODG_CHAVE_ACESSO_NF3E', 'CODG_DOCUMENTO_PARTCT_CALCULO'], inplace=True)
            df_item.rename(columns={'ID_CONVERSAO_NF3E': 'CODG_DOCUMENTO_PARTCT_CALCULO'}, inplace=True)

            db = ItemDocClasse()
            linhas_gravadas = db.grava_item_doc(df_item)
            del db

            loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_gravadas} itens gravados - ' + str(
                datetime.now() - data_hora_atividade))

            return None

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

            df['valr_participa'] = np.minimum(df['valr_participa'], df['valr_nf3e'])

            df.rename(columns={'id_nf3e': 'codg_documento_partct_calculo'}, inplace=True)
            df.rename(columns={'valr_participa': 'valr_adicionado_operacao'}, inplace=True)
            df.rename(columns={'data_emissao_doc': 'data_emissao_documento'}, inplace=True)
            df.rename(columns={'codg_tipo_doc': 'codg_tipo_doc_partct_calc'}, inplace=True)
            df.rename(columns={'numr_ref_emissao': 'numr_referencia_documento'}, inplace=True)
            df.rename(columns={'id_processamento': 'id_procesm_indice'}, inplace=True)
            df.rename(columns={'codg_motivo_exclusao': 'codg_motivo_exclusao_calculo'}, inplace=True)

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
                                  'tipo_classe_consumo'
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
        df_conversao_nf3e = []

        try:
            # Filtra documentos com valor maior do que zero
            filtro = df['id_nf3e'] != 0

            etapaProcess = f'{procesm.id_procesm_indice} Grava documentos participantes - {utilitarios.formatar_com_espacos(len(df[filtro]), 11)}' \
                           f' linhas.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            if len(df[filtro]) > 0:
                df_doc = self.formata_doc_participante(procesm, df[filtro])

                # df_doc.name = 'df_doc'
                # baixa_csv(df_doc)

                # Grava tabela IPM_CONVERSAO_NF3E
                df_conversao_nf3e = self.grava_doc_conversao_nf3e(procesm, df_doc)

                df_merge = df_doc.merge(df_conversao_nf3e,
                                        left_on=['codg_documento_partct_calculo'],
                                        right_on=['CODG_CHAVE_ACESSO_NF3E'],
                                        how='left')

                df_merge.drop(columns=['CODG_CHAVE_ACESSO_NF3E', 'codg_documento_partct_calculo'], inplace=True)
                df_merge.rename(columns={'ID_CONVERSAO_NF3E': 'codg_documento_partct_calculo'}, inplace=True)

                # Grava tabela de documentos não participantes #
                db = DocPartctClasse()
                linhas_gravadas = db.grava_doc_participante(df_merge)
                del db

            loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_gravadas} Docs Partct gravados - ' + str(
                datetime.now() - data_hora_atividade))

            return linhas_gravadas, df_conversao_nf3e

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def grava_doc_conversao_nf3e(self, procesm, df):
        etapaProcess = f"class {self.__class__.__name__} - def grava_doc_conversao_nf3e."
        # loga_mensagem(etapaProcess)

        linhas_gravadas = 0

        try:
            etapaProcess = f'{procesm.id_procesm_indice} Grava documentos conversao nf3e - {utilitarios.formatar_com_espacos(len(df), 11)}' \
                           f' linhas.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            # Grava tabela IPM_CONVERSAO_NF3E
            db = Oraprd()  # Persistência utilizando o Django
            # db = oraProd.create_instance()     # Persistência utilizando o oracledb
            linhas_incluidas = db.insert_doc_conversao_nf3e(df)

            loga_mensagem(
                etapaProcess + f' Processo finalizado. {linhas_gravadas} Docs Conversao NF3e gravados - ' + str(
                    datetime.now() - data_hora_atividade))

            del db

            return linhas_incluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def acerto_classe_consumo(self):
        etapaProcess = f"class {self.__class__.__name__} - def acerto_classe_consumo."
        # loga_mensagem(etapaProcess)

        linhas_alteradas = 0

        try:
            etapaProcess = f'Atualiza Classe de Consumo da tabela IPM_CONVERSAO_NF3E - {self.d_data_inicio} a {self.d_data_fim}'
            loga_mensagem(etapaProcess)
            data_hora_atividade = datetime.now()

            db = Trino('nf3e')
            df_nf3e = db.get_nf3e_classe_consumo(p_data_inicio=self.d_data_inicio, p_data_fim=self.d_data_fim)
            del db

            if len(df_nf3e) > 0:
                db = Oraprd()
                linhas_alteradas = db.update_classe_cons_conversao_nf3e(df_nf3e)
                del db

            loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_alteradas} Docs Conversao NF3e gravados - ' + str(datetime.now() - data_hora_atividade))

            return linhas_alteradas

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

