import os
from datetime import datetime, timedelta, date

import numpy as np
import pandas as pd

from funcoes.constantes import EnumTipoProcessamento, EnumStatusProcessamento
from funcoes.utilitarios import seleciona_inscricoes_doc, loga_mensagem_erro, loga_mensagem, baixa_csv, \
    sobe_csv, primeiro_dia_mes_seguinte, ultimo_dia_mes, LOCAL_TZ, soma_dias
from negocio.CNAE import CNAEClasse
from negocio.Procesm import ProcesmClasse
from negocio.SSN import SSNClasse
from persistencia.OraProd import oraProd
from persistencia.Oraprd import Oraprd
from persistencia.Oraprddw import OraprddwClass
from persistencia.Oraprodx9 import Oraprodx9
from polls.models import ContribIPM, Processamento


class CCEClasse:
    etapaProcess = f"class {__name__} - class CCEClasse."
    # loga_mensagem(etapaProcess)

    def __init__(self):
        self.inicio_referencia = os.getenv('INICIO_REFERENCIA')
        self.data_inicio_referencia = datetime.strptime(str(int(self.inicio_referencia) * 100 + 1), '%Y%m%d').date()
        self.fim_referencia = os.getenv('FIM_REFERENCIA')
        self.data_fim_referencia = datetime.strptime(str(int(self.fim_referencia) * 100 + 31), '%Y%m%d').date()

    def exclui_historico_cce(self, p_periodo):
        etapaProcess = f"class {self.__class__.__name__} - def exclui_historico_cce."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            linhas_excluidas = db.delete_historico_contrib(p_periodo)
            del db
            return linhas_excluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def codifica_situacao_cadastro_df(self, s_sit_cad):
        etapaProcess = f"class {self.__class__.__name__} - def codifica_situacao_cadastro_df."
        # loga_mensagem(etapaProcess)

        codificacao = {'Ativo': '1', 'Suspenso': '2', 'Baixado': '3', 'Paralisado': '4', 'Cassado': '5', 'Anulado': '6'}
        return s_sit_cad.map(codificacao).fillna('0')

    def codifica_situacao_cadastro(self, desc_sit_cad) -> int:
        etapaProcess = f"class {self.__class__.__name__} - def codifica_situacao_cadastro."
        # loga_mensagem(etapaProcess)

        if pd.notna(desc_sit_cad):
            if desc_sit_cad == 'Ativo':
                return '1'
            elif desc_sit_cad == 'Suspenso':
                return '2'
            elif desc_sit_cad == 'Baixado':
                return '3'
            elif desc_sit_cad == 'Paralisado':
                return '4'
            elif desc_sit_cad == 'Cassado':
                return '5'
            elif desc_sit_cad == 'Anulado':
                return '6'
            else:
                return '0'
        else:
            return '0'

    def codifica_tipo_enqdto_df(self, s_tipo_enqdto) -> int:
        etapaProcess = f"class {self.__class__.__name__} - def codifica_tipo_enqdto_df."
        # loga_mensagem(etapaProcess)

        codificacao = {'Microempresa': '1', 'Normal': '2', 'Micro EPP/Simples Naciona': '3', 'Simples Nacional/SIMEI': '4', 'Simples Nacional/Normal': '5'}
        return s_tipo_enqdto.map(codificacao).fillna('2')

    def codifica_tipo_enqdto(self, tipo_enqdto) -> int:
        etapaProcess = f"class {self.__class__.__name__} - def codifica_situacao_cadastro."
        # loga_mensagem(etapaProcess)

        if pd.notna(tipo_enqdto):
            if tipo_enqdto == 'Microempresa':
                return '1'
            elif tipo_enqdto == 'Normal':
                return '2'
            elif tipo_enqdto == 'Micro EPP/Simples Naciona':
                return '3'
            elif tipo_enqdto == 'Simples Nacional/SIMEI':
                return '4'
            elif tipo_enqdto == 'Simples Nacional/Normal':
                return '5'
            else:
                return '0'
        else:
            return '0'

    def inclui_informacoes_cce(self, df_doc, p_data_inicio, p_data_fim, procesm:Processamento):
        etapaProcess = f"class {self.__class__.__name__} - def inclui_informacoes_cce. Carrega informações do CCE a partir dos dados dos documentos fiscais."
        # loga_mensagem(etapaProcess)

        df_inclusao = pd.DataFrame()

        try:
            i_ref = p_data_inicio.year * 100 + p_data_inicio.month
            # df_doc.name = 'df_doc'
            # baixa_csv(df_doc)

            df_inscr = seleciona_inscricoes_doc(df_doc)

            # df_inscr.name = 'df_inscr_pesq'
            # baixa_csv(df_inscr)

            etapaProcess = f'{procesm.id_procesm_indice} Pesquisa no cadastro de contribuintes do IPM - {len(df_inscr)} inscrições.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            # Busca inscrições já cadastradas
            db = Oraprd()
            df_inscr_cad = db.select_contribuinte_ipm(df_inscr, p_data_inicio, p_data_fim)
            del db

            loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_inscr_cad)} contribuintes retornados - ' + str(datetime.now() - data_hora_atividade))

            # cce_cnpj = seleciona_cnpjs(df_nfe)
            # df_inscr = db.select_inscricao_cce(cce_cnpj)

            # df_inscr_cad.name = 'df_inscr_cad'
            # baixa_csv(df_inscr_cad)

            # Mescla as inscrições dos documentos com as do cadastro
            df = pd.DataFrame(df_inscr)
            df['ie'] = df['ie'].astype(str)
            df['id_contrib_ipm'] = pd.NA
            df['operacao'] = pd.NA

            if len(df_inscr_cad) > 0:
                # loga_mensagem(f'Selecionadas {len(df_inscr_cad)} inscrições cadastradas.')
                df_inscr_cad['numr_inscricao_contrib'] = df_inscr_cad['numr_inscricao_contrib'].astype(str)
                df_inscr_cad['operacao'] = 'cad ok'
                df = pd.merge(df, df_inscr_cad,
                              left_on=['ie'],
                              right_on=['numr_inscricao_contrib'],
                              how='left'
                              )
                # Substitui o município informado no documento pelo município do cadastro
                df.loc[df['codg_municipio_y'].notna(), 'codg_municipio_x'] = df['codg_municipio_y'].astype(float)
                df.drop(columns=['codg_municipio_y'], inplace=True)
                df.rename(columns={'codg_municipio_x': 'codg_municipio'}, inplace=True)

                df.drop(columns=['id_contrib_ipm_x'], inplace=True)
                df.rename(columns={'id_contrib_ipm_y': 'id_contrib_ipm'}, inplace=True)

                df.drop(columns=['operacao_x'], inplace=True)
                df.rename(columns={'operacao_y': 'operacao'}, inplace=True)

                df.drop(columns=['codg_uf_y'], inplace=True)
                df.rename(columns={'codg_uf_x': 'codg_uf'}, inplace=True)

                df['numr_referencia'] = i_ref
            else:
                df['id_contrib_ipm'] = pd.NA
                df['data_inicio_vigencia'] = self.data_inicio_referencia
                df['data_fim_vigencia'] = pd.NA
                df['stat_cadastro_contrib'] = '0'
                df['tipo_enqdto_fiscal'] = '0'
                df['indi_produtor_rural'] = 'N'
                df['indi_produtor_rural_exclusivo'] = 'N'

            df['numr_inscricao_contrib'] = df['ie']

            # Busca dados históricos do cadastro de contribuintes no log de auditoria
            df_hist_cce = []

            # Filtra contribuintes com Uf = 'GO' ou vazio e que ainda não estão cadastrados (id_contrib_ipm nulo)
            filtro = ((pd.isna(df['codg_uf'])) | (df['codg_uf'] == 'GO')) & pd.isna(df['id_contrib_ipm'])
            if len(df[filtro]) > 0:
                # df_hist_cce = self.busca_historico_ruc(df[filtro], p_data_inicio, p_data_fim)
                df_hist_cce = self.busca_historico_cce(df[filtro])
                loga_mensagem(f'Retornaram {len(df_hist_cce)} linhas da rotina busca_historico_cce.')

                if len(df_hist_cce) > 0:
                    # loga_mensagem(f'Concatena as inscrições do cadastro com as encontradas no histórico. {len(df)} NF-es com {len(df_hist_cce)} históricos.')
                    df_hist_cce['numr_inscricao_contrib'] = df_hist_cce['numr_inscricao_contrib'].astype(str)
                    df_hist_cce['stat_cadastro_contrib'] = df_hist_cce['stat_cadastro_contrib'].astype(str)
                    df_hist_cce['tipo_enqdto_fiscal'] = df_hist_cce['tipo_enqdto_fiscal'].astype(str)

                    df_hist_cce['operacao'] = 'inclusao'

                    df = pd.merge(df, df_hist_cce,
                                  left_on=['ie'],
                                  right_on=['numr_inscricao_contrib'],
                                  how='left'
                                  )

                    if 'numr_inscricao_contrib_x' in df.columns:
                        df.loc[df['numr_inscricao_contrib_y'].notna(), 'numr_inscricao_contrib_x'] = df['numr_inscricao_contrib_y']
                        df.drop(columns=['numr_inscricao_contrib_y'], inplace=True)
                        df.rename(columns={'numr_inscricao_contrib_x': 'numr_inscricao_contrib'}, inplace=True)

                        df.loc[df['data_fim_vigencia_y'].notna(), 'data_fim_vigencia_x'] = df['data_fim_vigencia_y']
                        df.drop(columns=['data_fim_vigencia_y'], inplace=True)
                        df.rename(columns={'data_fim_vigencia_x': 'data_fim_vigencia'}, inplace=True)

                        df.loc[df['data_inicio_vigencia_y'].notna(), 'data_inicio_vigencia_x'] = df['data_inicio_vigencia_y']
                        df.drop(columns=['data_inicio_vigencia_y'], inplace=True)
                        df.rename(columns={'data_inicio_vigencia_x': 'data_inicio_vigencia'}, inplace=True)

                        df.loc[df['tipo_enqdto_fiscal_y'].notna(), 'tipo_enqdto_fiscal_x'] = df['tipo_enqdto_fiscal_y']
                        df.drop(columns=['tipo_enqdto_fiscal_y'], inplace=True)
                        df.rename(columns={'tipo_enqdto_fiscal_x': 'tipo_enqdto_fiscal'}, inplace=True)

                        df.loc[df['stat_cadastro_contrib_y'].notna(), 'stat_cadastro_contrib_x'] = df['stat_cadastro_contrib_y']
                        df.drop(columns=['stat_cadastro_contrib_y'], inplace=True)
                        df.rename(columns={'stat_cadastro_contrib_x': 'stat_cadastro_contrib'}, inplace=True)

                        df.loc[df['indi_produtor_rural_y'].notna(), 'indi_produtor_rural_x'] = df['indi_produtor_rural_y']
                        df.drop(columns=['indi_produtor_rural_y'], inplace=True)
                        df.rename(columns={'indi_produtor_rural_x': 'indi_produtor_rural'}, inplace=True)

                        df.loc[df['indi_produtor_rural_exclusivo_y'].notna(), 'indi_produtor_rural_exclusivo_x'] = df['indi_produtor_rural_exclusivo_y']
                        df.drop(columns=['indi_produtor_rural_exclusivo_y'], inplace=True)
                        df.rename(columns={'indi_produtor_rural_exclusivo_x': 'indi_produtor_rural_exclusivo'}, inplace=True)

                    df.loc[df['operacao_y'].notna(), 'operacao_x'] = df['operacao_y']
                    df.drop(columns=['operacao_y'], inplace=True)
                    df.rename(columns={'operacao_x': 'operacao'}, inplace=True)

                    df.drop(columns=['codg_uf_y'], inplace=True)
                    df.rename(columns={'codg_uf_x': 'codg_uf'}, inplace=True)

                    # Move o municipio do documento para o do cadastro, caso for nulo
                    df['codg_municipio_y'] = df['codg_municipio_y'].where(~pd.isna(df['codg_municipio_y']), df['codg_municipio_x'])

                    df.drop(columns=['codg_municipio_x'], inplace=True)
                    df.rename(columns={'codg_municipio_y': 'codg_municipio'}, inplace=True)

                    # df_hist_cce.name = 'df_hist_cce'
                    # baixa_csv(df_hist_cce)

            # df.name = 'df_cce'
            # baixa_csv(df)

            filtro = pd.isna(df['operacao'])
            if len(df[filtro]) > 0:
                df.loc[filtro, 'operacao'] = 'inclusao'
                df.loc[filtro, 'data_inicio_vigencia'] = self.data_inicio_referencia
                df.loc[filtro, 'stat_cadastro_contrib'] = '0'
                df.loc[filtro, 'tipo_enqdto_fiscal'] = '0'
                df.loc[filtro, 'indi_produtor_rural'] = 'N'
                df.loc[filtro, 'indi_produtor_rural_exclusivo'] = 'N'

                # loga_mensagem(f'Resultado da concatenação: {len(df)} linhas a incluir no histórico. Processa as informações pertinentes.')

            # df.name = 'df_receb'
            # baixa_csv(df)

            filtro = df['operacao'] == 'inclusao'
            if len(df[filtro]) > 0:
                df.loc[filtro, 'id_procesm_indice'] = procesm.id_procesm_indice

                # df.name = 'df_inclusao'
                # baixa_csv(df)

                db = Oraprd()  # Persistência utilizando o Django
                df_novos_contribs = db.insert_contrib_ipm(df[filtro])
                del db

                obj_retorno = pd.concat([df_inscr_cad, df_novos_contribs], axis=0).reset_index(drop=True)

                # Retorna apenas dados cadastrais referentes ao período dos documentos
                if isinstance(p_data_inicio, date):
                    p_data_inicio = pd.to_datetime(p_data_inicio)

                if p_data_fim is not None:
                    if isinstance(p_data_fim, date):
                        p_data_fim = pd.to_datetime(p_data_fim)

                filtro = (((pd.to_datetime(obj_retorno['data_inicio_vigencia']) <= p_data_inicio) & (pd.to_datetime(obj_retorno['data_fim_vigencia']) >= p_data_fim)) |
                          ((pd.to_datetime(obj_retorno['data_inicio_vigencia']) <= p_data_inicio) & (obj_retorno['data_fim_vigencia'].isna()))
                          )

                obj_retorno.name = 'df_inclusao'
                baixa_csv(obj_retorno)

                return obj_retorno[filtro]

            else:
                return df_inscr_cad

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def inclui_informacoes_cce_periodo(self, p_data_inicio, p_data_fim, p_ref):
        etapaProcess = f"class {self.__class__.__name__} - def inclui_informacoes_cce_periodo."
        # loga_mensagem(etapaProcess)

        try:
            dData_hora_inicio = datetime.now()
            sData_hora_inicio = datetime.now(LOCAL_TZ).strftime('%d/%m/%Y %H:%M:%S,%f')

            # Registra processamento
            db = ProcesmClasse()
            s_tipo_procesm = EnumTipoProcessamento.processaAcertoContrib.value
            procesm = db.iniciar_processamento(p_data_inicio, p_data_fim, s_tipo_procesm)
            del db
            etapaProcess = f'{procesm.id_procesm_indice} Inicio da atualização cadastral dos contribuintes. Período de {p_data_inicio} a {p_data_fim} - {sData_hora_inicio}'
            loga_mensagem(etapaProcess)

            # Busca inscrições cadastradas
            etapaProcess = f'{procesm.id_procesm_indice} - Busca inscrições cadastradas - {p_data_inicio} a {p_data_fim}. '
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            db = Oraprd()
            df_inscr_cad_completo = db.select_contribuinte_ipm_cadastrados_go(p_data_inicio, p_data_fim)
            df_inscr_cad = df_inscr_cad_completo[['numr_inscricao_contrib', 'codg_uf']].drop_duplicates()
            df_inscr_cad.rename(columns={'numr_inscricao_contrib': 'numr_inscricao'}, inplace=True)

            loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_inscr_cad)} linhas selecionadas. - ' + str(datetime.now() - data_hora_atividade))
            del db

            # df_inscr_cad.name = 'df_inscr_cad'
            # baixa_csv(df_inscr_cad)

            # Busca históricos das inscrições cadastradas
            etapaProcess = f'{procesm.id_procesm_indice} - Busca históricos das inscrições cadastradas. '
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            db = OraprddwClass()
            df_hist = db.select_historico_contrib_periodo(df_inscr_cad['numr_inscricao'].tolist(), int(p_data_inicio.strftime('%Y%m%d')), int(p_data_fim.strftime('%Y%m%d')))
            df_hist['data_alteracao'] = pd.to_datetime(df_hist['numr_ano_mes_dia_alteracao'], format='%Y%m%d')
            del db

            loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_hist)} linhas selecionadas - ' + str(datetime.now() - data_hora_atividade))

            df_hist.name = 'df_hist'
            baixa_csv(df_hist)

            # Busca históricos das CNAEs das inscrições com alteração cadastral
            etapaProcess = f'{procesm.id_procesm_indice} - Busca históricos das CNAEs das inscrições com alteração cadastral - {p_data_inicio} a {p_data_fim}. '
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            cnae = CNAEClasse()
            df_rural = cnae.inclui_informacoes_cnae_rural(df_inscr_cad['numr_inscricao'].tolist(), p_ref)
            del cnae

            if len(df_rural) > 0:
                df_rural.name = 'df_rural'
                baixa_csv(df_rural)

            loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_rural)} linhas selecionadas. - ' + str(datetime.now() - data_hora_atividade))

            df_ssn = self.busca_historico_ssn(df_inscr_cad, self.data_inicio_referencia, self.data_fim_referencia)

            if len(df_ssn) > 0:
                df_ssn.name = 'df_ssn'
                baixa_csv(df_ssn)

            # Cruza as informações entre cadastro e histórico
            if len(df_inscr_cad) > 0:
                df_inscr_cad['numr_inscricao'] = df_inscr_cad['numr_inscricao'].astype(str)
                df_hist['numr_inscricao'] = df_hist['numr_inscricao'].astype(str)
                df_hist.rename(columns={'codg_municipio_contrib': 'codg_municipio'}, inplace=True)
                if len(df_rural) > 0:
                    df_rural.rename(columns={'numr_inscricao_contrib': 'numr_inscricao'}, inplace=True)
                    df_rural['numr_inscricao'] = df_rural['numr_inscricao'].astype(str)

                if len(df_ssn) > 0:
                    df_ssn['numr_inscricao'] = df_ssn['numr_inscricao'].astype(str)

                df_merged = pd.merge(df_inscr_cad, df_hist, on=['numr_inscricao'], how='outer')

                df_merged.name = 'df_merged_hist'
                baixa_csv(df_merged)

                if len(df_rural) > 0:
                    df_merged = pd.merge(df_merged, df_rural, on=['numr_inscricao', 'data_alteracao'], how='outer')

                    df_merged.name = 'df_merged_rural'
                    baixa_csv(df_merged)

                else:
                    df_merged['indi_produtor_rural'] = pd.NA
                    df_merged['indi_produtor_rural_exclusivo'] = pd.NA

                if len(df_ssn) > 0:
                    df_merged = pd.merge(df_merged, df_ssn, on=['numr_inscricao', 'data_alteracao'], how='outer')

                    df_merged.name = 'df_merged_ssn'
                    baixa_csv(df_merged)

                else:
                    df_merged['desc_tipo_enqdto'] = 'Normal'

                df_merged = df_merged.sort_values(by=['numr_inscricao', 'data_alteracao'])

                cols_preencher = ['codg_municipio', 'indi_produtor_rural', 'indi_produtor_rural_exclusivo', 'desc_tipo_enqdto', 'desc_situacao_cadastro']
                df_merged[cols_preencher] = df_merged.groupby('numr_inscricao')[cols_preencher].fillna(method='ffill')

                df_merged.name = 'df_merged_preench'
                baixa_csv(df_merged)

                df_merged['codg_uf'].fillna('GO', inplace=True)
                df_merged['codg_municipio'].fillna(999999999, inplace=True)
                df_merged['data_alteracao'].fillna(p_data_inicio, inplace=True)
                df_merged['indi_produtor_rural'].fillna('N', inplace=True)
                df_merged['indi_produtor_rural_exclusivo'].fillna('N', inplace=True)
                df_merged['tipo_enqdto_fiscal'] = self.codifica_tipo_enqdto_df(df_merged['desc_tipo_enqdto'])
                df_merged['stat_cadastro_contrib'] = self.codifica_situacao_cadastro_df(df_merged['desc_situacao_cadastro'])

                df_merged['numr_inscricao'] = df_merged['numr_inscricao'].astype(str)
                df_merged['data_alteracao'] = pd.to_datetime(df_merged['data_alteracao'])
                df_merged.drop(columns=['desc_situacao_cadastro'
                                      , 'numr_ano_mes_dia_alteracao'
                                      , 'desc_tipo_enqdto'], inplace=True)

                # Criar um indicador de mudança comparando com a linha anterior para cada 'numr_inscricao'
                cols_cadastro = ['codg_municipio', 'stat_cadastro_contrib', 'tipo_enqdto_fiscal', 'indi_produtor_rural', 'indi_produtor_rural_exclusivo']
                df_merged['alteracao'] = df_merged[cols_cadastro].ne(df_merged.groupby('numr_inscricao')[cols_cadastro].shift()).any(axis=1)

                df_merged.name = 'df_merged_indic'
                baixa_csv(df_merged)

                filtro = df_merged['alteracao']
                df_merged['data_fim_vigencia'] = df_merged[filtro].groupby('numr_inscricao')['data_alteracao'].shift(-1)
                df_merged['data_fim_vigencia'] = df_merged['data_fim_vigencia'] - pd.Timedelta(seconds=1)

                df_merged.rename(columns={'numr_inscricao': 'numr_inscricao_contrib'
                                        , 'data_alteracao': 'data_inicio_vigencia'}, inplace=True)
                df_merged['id_procesm_indice'] = procesm.id_procesm_indice

                etapaProcess = f'{procesm.id_procesm_indice} - Atualiza cadastro de contribuintes do IPM - {p_data_inicio} a {p_data_fim}. '
                data_hora_atividade = datetime.now()

                df_merged.name = 'df_merged_insert'
                baixa_csv(df_merged)

                db = Oraprd()  # Persistência utilizando o Django
                db.insert_contrib_ipm(df_merged[filtro])
                del db

                loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_merged[filtro])} linhas atualizadas. - ' + str(datetime.now() - data_hora_atividade))

                etapaProcess = f'{procesm.id_procesm_indice} Atualização cadastral dos contribuintes de {p_data_inicio} a {p_data_fim} finalizada. Inseridos {len(df_merged)} atualizações.'
                procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                procesm.stat_procesm_indice = EnumStatusProcessamento.sucesso.value
                self.registra_processamento(procesm, etapaProcess)

            else:
                etapaProcess = f'{procesm.id_procesm_indice} Atualização cadastral dos contribuintes de {p_data_inicio} a {p_data_fim} finalizada. Não foram localizadas alterações.'
                procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                procesm.stat_procesm_indice = EnumStatusProcessamento.sucesso.value
                self.registra_processamento(procesm, etapaProcess)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

        sData_hora_final = datetime.now(LOCAL_TZ).strftime('%d/%m/%Y %H:%M:%S,%f')
        loga_mensagem(f'{etapaProcess} - {sData_hora_final} - Tempo de processamento: {(datetime.now() - dData_hora_inicio)}.')

        return 0

    def inclui_informacoes_cce_log(self, p_data_inicio, p_data_fim, p_ref):
        etapaProcess = f"class {self.__class__.__name__} - def inclui_informacoes_cce_log."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            data_hora_atividade = datetime.now()
            i_qtde_delete = db.delete_contrib_novo()
            loga_mensagem(f' delete_contrib_novo - Processo finalizado - {i_qtde_delete} linhas excluídas.' + str(datetime.now() - data_hora_atividade))
            del db

            s_data_ref = '2025-04-30'
            d_data_ref = datetime.strptime(s_data_ref, '%Y-%m-%d')

            dData_hora_inicio = datetime.now()
            sData_hora_inicio = datetime.now(LOCAL_TZ).strftime('%d/%m/%Y %H:%M:%S,%f')

            # Registra processamento
            db = ProcesmClasse()
            s_tipo_procesm = EnumTipoProcessamento.processaAcertoContrib.value
            procesm = db.iniciar_processamento(p_data_inicio, p_data_fim, s_tipo_procesm)
            del db
            etapaProcess = f'{procesm.id_procesm_indice} Inicio da atualização cadastral dos contribuintes. Período de {p_data_inicio} a {p_data_fim} - {sData_hora_inicio}'
            loga_mensagem(etapaProcess)

            # Busca inscrições cadastradas
            etapaProcess = f'{procesm.id_procesm_indice} - Busca inscrições cadastradas - {p_data_inicio} a {p_data_fim}. '
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            db = Oraprd()
            df_inscr_cad = db.select_contribuinte_ipm_cadastrados_go(p_data_inicio, p_data_fim)
            loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_inscr_cad)} linhas selecionadas - ' + str(datetime.now() - data_hora_atividade))
            del db

            # df_inscr_cad = sobe_csv("df_contrib.csv", "|")

            # df_inscr_cad = pd.DataFrame({'numr_inscricao_contrib': [108599345], 'codg_uf': ['GO']})
            # df_inscr_cad = pd.DataFrame({'numr_inscricao_contrib': [104164409, 103473130, 106535552, 107111144], 'codg_uf': ['GO', 'GO', 'GO', 'GO']})

            # Busca históricos das inscrições cadastradas
            etapaProcess = f'{procesm.id_procesm_indice} - Busca históricos das inscrições cadastradas - {p_data_inicio} a {p_data_fim}. '
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            df_hist = self.busca_historico_cce(df_inscr_cad)
            df_hist['id_procesm_indice'] = procesm.id_procesm_indice

            db = Oraprd()
            db.insert_contrib_ipm(df_hist)
            del db

            """        
            s_chave_contrib_ant = None
            lista_excl_cnae = []
            lista_incl_cnae = []
            b_alterou_cnae = False
            b_alterou_munic = False
            i_munic_munic = 0
            i_munic_lograd = 0
            lista_resultados = []

            max_lote = 1000
            l_inscrs = df_inscr_cad['numr_inscricao_contrib'].tolist()
            for i in range(0, len(l_inscrs), max_lote):
                inscrs = l_inscrs[i:i + max_lote]
                lista_inscrs = ', '.join(map(str, inscrs))

                db = Oraprd()
                df_contrib = db.select_cadastro_contrib(lista_inscrs, s_data_ref)
                # loga_mensagem(f'Retornaram {len(df_contrib)} contribuintes do cadastro atual.')

                df_contrib.name = 'df_contrib'
                baixa_csv(df_contrib)

                df_cnae = db.select_cnae_contrib(lista_inscrs)
                # loga_mensagem(f'Retornaram {len(df_cnae)} cnaes dos contribuintes do cadastro atual.')

                df_cnae_produtor = db.select_cnae_produtor()
                # loga_mensagem(f'Retornaram {len(df_cnae_produtor)} cnaes de produtores rurais.')

                del db

                # Convertendo colunas para os tipos corretos
                df_contrib['numr_inscricao_contrib'] = df_contrib['numr_inscricao_contrib'].astype(int)
                df_contrib['stat_cadastro_contrib'] = df_contrib['stat_cadastro_contrib'].astype(int)

                # Merge de CNAEs (feito uma vez antes do loop principal)
                if ~df_cnae.empty and len(df_cnae) > 0:
                    df_cnae_merge = df_cnae.merge(df_cnae_produtor,
                                                  left_on='codg_subclasse_cnaef',
                                                  right_on='codg_cnae_rural',
                                                  how='left')
                    # Determina se cada inscrição possui apenas CNAEs rurais ou pelo menos um
                    df_cnae_agrupado = df_cnae_merge.groupby('numr_inscricao') \
                                                    .agg(b_prod_rural_exclusivo=('codg_cnae_rural', lambda x: x.notna().all()),b_prod_rural=('codg_cnae_rural', lambda x: x.notna().any())) \
                                                    .reset_index()

                    # Adicionando colunas categóricas no DataFrame original
                    df_cnae_agrupado['s_prod_rural_exclusivo'] = df_cnae_agrupado['b_prod_rural_exclusivo'].map({True: 'S', False: 'N'})
                    df_cnae_agrupado['s_prod_rural'] = df_cnae_agrupado['b_prod_rural'].map({True: 'S', False: 'N'})
                    df_cnae_agrupado = df_cnae_agrupado.drop(columns=['b_prod_rural_exclusivo', 'b_prod_rural'])

                    # Fazendo merge com df_contrib para adicionar os status
                    df_cnae_agrupado.rename(columns={'numr_inscricao': 'numr_inscricao_contrib'}, inplace=True)
                    df_contrib = df_contrib.merge(df_cnae_agrupado, on='numr_inscricao_contrib', how='left')
                else:
                    df_contrib['s_prod_rural'] = 'N'
                    df_contrib['s_prod_rural_exclusivo'] = 'N'

                db = SSNClasse()
                df_simples = db.busca_historico_ssn(lista_inscrs)
                # loga_mensagem(f'Retornaram {len(df_simples)} historicos do Simples Nacional.')
                del db

                db = Oraprd()
                df_hist = db.select_historico_contrib(lista_inscrs)
                # loga_mensagem(f'Retornaram {len(df_hist)} historicos do LOG de auditoria.')
                del db

                df_hist['numr_inscricao'] = df_hist['numr_inscricao'].astype(int)

                if len(df_simples) > 0:
                    df_simples['numr_inscricao'] = df_simples['numr_inscricao'].astype(int)
                    df_hist = pd.concat([df_hist, df_simples], ignore_index=True)

                df_hist = df_hist.sort_values(by=['numr_inscricao', 'data_hora_transacao'], ascending=[True, False])

                # loga_mensagem(f'Processando {len(df_hist)} historicos do LOG de auditoria.')

                df_hist.name = 'df_hist'
                baixa_csv(df_hist)

                # Criar um DataFrame para armazenar os resultados
                dfs = []

                # Iteração sobre df_contrib
                for rowC in df_contrib.itertuples(index=False):
                    i_numr_inscricao = rowC.numr_inscricao_contrib
                    i_stat_cadastro_contrib = rowC.stat_cadastro_contrib
                    i_tipo_enqdto_fiscal = rowC.tipo_enqdto_fiscal
                    i_codg_municipio = rowC.codg_municipio
                    s_prod_rural = 'N' if pd.isna(rowC.s_prod_rural) else rowC.s_prod_rural
                    s_prod_rural_exclusivo = 'N' if pd.isna(rowC.s_prod_rural_exclusivo) else rowC.s_prod_rural_exclusivo
                    d_data_alteracao = pd.to_datetime(p_ref)
                    # loga_mensagem(f'1 Inscrição: {i_numr_inscricao} - Município: {i_codg_municipio} - Alt Municipio: {b_alterou_munic}')

                    lista_resultados = []

                    db = Oraprd()
                    filtro = df_hist['numr_inscricao'] == i_numr_inscricao
                    filtro_cnae = df_cnae['numr_inscricao'] == i_numr_inscricao
                    for rowH in df_hist[filtro].itertuples(index=False):
                        s_chave_contrib = f"{rowH.numr_inscricao}-{rowH.data_hora_transacao}"
                        d_data_alteracao = rowH.data_hora_transacao
                        # loga_mensagem(f'2 Inscrição: {i_numr_inscricao} - Município: {i_codg_municipio} - Data Alteração: {d_data_alteracao} - Alt Municipio: {b_alterou_munic}')

                        if s_chave_contrib_ant != s_chave_contrib:
                            # loga_mensagem(f'Quebrou a chave - {s_chave_contrib_ant} != {s_chave_contrib}')
                            # Excluir CNAEs
                            if lista_excl_cnae:
                                df_excl_cnae = pd.DataFrame(lista_excl_cnae)
                                df_cnae = df_cnae[~df_cnae.set_index(['numr_inscricao', 'id_contrib_cnaef']).index.isin(
                                    df_excl_cnae.set_index(['numr_inscricao', 'id_contrib_cnaef']).index)]
                                b_alterou_cnae = True
                                lista_excl_cnae = []

                            # Incluir CNAEs
                            if lista_incl_cnae:
                                df_lista_cnae = pd.DataFrame(lista_incl_cnae)
                                df_incl_cnae = db.select_cce_cnae(df_lista_cnae['id_subclasse_cnaef'].tolist())
                                df_incl_cnae['numr_inscricao'] = i_numr_inscricao
                                df_cnae = pd.concat([df_cnae, df_incl_cnae], ignore_index=True)
                                b_alterou_cnae = True
                                lista_incl_cnae = []

                            if b_alterou_cnae:
                                filtro_cnae = df_cnae['numr_inscricao'] == i_numr_inscricao
                                if len(df_cnae[filtro_cnae]) > 0:
                                    df_cnae_merge = df_cnae[filtro_cnae].merge(df_cnae_produtor
                                                                , left_on='codg_subclasse_cnaef'
                                                                , right_on='codg_cnae_rural'
                                                                , how='left')
                                    b_prod_rural_exclusivo = df_cnae_merge['codg_cnae_rural'].notna().all()
                                    b_prod_rural = df_cnae_merge['codg_cnae_rural'].notna().any()
                                    s_prod_rural_exclusivo = 'S' if b_prod_rural_exclusivo else 'N'
                                    s_prod_rural = 'S' if b_prod_rural else 'N'
                                else:
                                    s_prod_rural_exclusivo = 'N'
                                    s_prod_rural = 'N'

                                b_alterou_cnae = False

                            if b_alterou_munic:
                                b_alterou_munic = False
                                # loga_mensagem(f'Municipios: i_codg_municipio:{i_codg_municipio} - i_munic_munic:{i_munic_munic} - i_munic_lograd:{i_munic_lograd} - b_alterou_munic:{b_alterou_munic}')
                                if i_codg_municipio != i_munic_munic and i_codg_municipio != i_munic_lograd:
                                    i_codg_municipio = i_munic_munic if i_munic_munic != 0 else i_munic_lograd if i_munic_lograd != 0 else 0
                                    # loga_mensagem(f'Trocou o municipio - {i_codg_municipio}')

                            nova_linha = {'numr_inscricao_contrib': i_numr_inscricao,
                                          'stat_cadastro_contrib': i_stat_cadastro_contrib,
                                          'codg_municipio': i_codg_municipio,
                                          'codg_uf': 'GO',
                                          'tipo_enqdto_fiscal': i_tipo_enqdto_fiscal,
                                          'indi_produtor_rural': s_prod_rural,
                                          'indi_produtor_rural_exclusivo': s_prod_rural_exclusivo,
                                          'data_inicio_vigencia': d_data_alteracao,
                                          'data_fim_vigencia': pd.NaT,
                                          }
                            lista_resultados.append(nova_linha)
                            # loga_mensagem(f'1--> {nova_linha}')

                            s_chave_contrib_ant = s_chave_contrib

                        # Testes e atualizações conforme mudanças no histórico
                        if rowH.nome_tabela == 'CCE_CONTRIBUINTE' and rowH.nome_coluna == 'INDI_SITUACAO_CADASTRAL':
                            i_stat_cadastro_contrib = int(rowH.valr_anterior_coluna)
                            # loga_mensagem('Alterou Situação cadastral')

                        if rowH.nome_tabela == 'GEN_ENDERECO' and rowH.nome_coluna == 'CODG_MUNICIPIO':
                            i_munic_munic = int(rowH.valr_anterior_coluna) if pd.notna(rowH.valr_anterior_coluna) else 0
                            b_alterou_munic = True
                            # loga_mensagem(f'Alterou Municipio: {rowH.valr_anterior_coluna}-{rowH.valr_subst_coluna}')

                        if rowH.nome_tabela == 'GEN_ENDERECO' and rowH.nome_coluna == 'CODG_LOGRADOURO':
                            # i_munic_lograd = 0
                            # b_alterou_munic = True
                            # if pd.notna(rowH.valr_anterior_coluna):
                            #     df_munic = db.select_logradouro_municipio(rowH.valr_anterior_coluna)
                            #     if df_munic is not None and not df_munic.empty:
                            #         i_munic_lograd = df_munic['codg_municipio'].iloc[0]

                            df_munic = db.select_logradouro_municipio(rowH.valr_anterior_coluna) if pd.notna(rowH.valr_anterior_coluna) else None
                            # i_munic_lograd = df_munic['codg_municipio'].iloc[0] if df_munic is not None else 0
                            i_munic_lograd = df_munic['codg_municipio'].iloc[0] if isinstance(df_munic, pd.DataFrame) and not df_munic.empty else 0
                            b_alterou_munic = True
                            # loga_mensagem(f'Alterou Logradouro: {rowH.valr_anterior_coluna}-{rowH.valr_subst_coluna}')

                        if rowH.nome_tabela == 'CCE_CONTRIB_CNAEF' and rowH.nome_coluna == 'ID_CONTRIB_CNAEF' and rowH.tipo_transacao == 'I':
                            lista_excl_cnae.append(
                                {'numr_inscricao': i_numr_inscricao, 'id_contrib_cnaef': int(rowH.valr_subst_coluna)})
                            # loga_mensagem(f'excluiu cnaef: {rowH.valr_subst_coluna}-{rowH.valr_subst_coluna}')

                        if rowH.nome_tabela == 'CCE_CONTRIB_CNAEF' and rowH.nome_coluna == 'ID_SUBCLASSE_CNAEF' and rowH.tipo_transacao == 'D':
                            lista_incl_cnae.append(
                                {'numr_inscricao': i_numr_inscricao, 'id_subclasse_cnaef': int(rowH.valr_anterior_coluna)})
                            # loga_mensagem(f'Incluiu cnaef: {rowH.valr_subst_coluna}-{rowH.valr_subst_coluna}')

                        if rowH.nome_tabela == 'IPM_OPCAO_CONTRIB_SIMPL_SIMEI' and rowH.nome_coluna == 'OPCAO_SIMPLES':
                            i_tipo_enqdto_fiscal = rowH.valr_anterior_coluna
                            # loga_mensagem('Alterou Simples')

                    # loga_mensagem(f'Acabaram os históricos da Inscrição')

                    # Excluir CNAEs
                    if lista_excl_cnae:
                        df_excl_cnae = pd.DataFrame(lista_excl_cnae)
                        df_cnae = df_cnae[~df_cnae.set_index(['numr_inscricao', 'id_contrib_cnaef']).index.isin(
                            df_excl_cnae.set_index(['numr_inscricao', 'id_contrib_cnaef']).index)]
                        b_alterou_cnae = True
                        lista_excl_cnae = []

                    # Incluir CNAEs
                    if lista_incl_cnae:
                        df_lista_cnae = pd.DataFrame(lista_incl_cnae)
                        df_incl_cnae = db.select_cce_cnae(df_lista_cnae['id_subclasse_cnaef'].tolist())
                        df_incl_cnae['numr_inscricao'] = i_numr_inscricao
                        df_cnae = pd.concat([df_cnae, df_incl_cnae], ignore_index=True)
                        b_alterou_cnae = True
                        lista_incl_cnae = []

                    if b_alterou_cnae:
                        filtro_cnae = df_cnae['numr_inscricao'] == i_numr_inscricao
                        if len(df_cnae[filtro_cnae]) > 0:
                            # loga_mensagem('Ainda tem cnae')
                            df_cnae_merge = df_cnae[filtro_cnae].merge(df_cnae_produtor
                                                          , left_on='codg_subclasse_cnaef'
                                                          , right_on='codg_cnae_rural'
                                                          , how='left')
                            b_prod_rural_exclusivo = df_cnae_merge['codg_cnae_rural'].notna().all()
                            b_prod_rural = df_cnae_merge['codg_cnae_rural'].notna().any()
                            s_prod_rural_exclusivo = 'S' if b_prod_rural_exclusivo else 'N'
                            s_prod_rural = 'S' if b_prod_rural else 'N'
                            # loga_mensagem(f's_prod_rural_exclusivo: {s_prod_rural_exclusivo} - s_prod_rural:{s_prod_rural} - df_cnae_merge: {df_cnae_merge}')
                        else:
                            # loga_mensagem('Sem cnae')
                            s_prod_rural_exclusivo = 'N'
                            s_prod_rural = 'N'

                        b_alterou_cnae = False

                    if b_alterou_munic:
                        b_alterou_munic = False
                        # loga_mensagem(f'2 Municipios: i_codg_municipio:{i_codg_municipio} - i_munic_munic:{i_munic_munic} - i_munic_lograd:{i_munic_lograd} - b_alterou_munic:{b_alterou_munic}')
                        if i_codg_municipio != i_munic_munic and i_codg_municipio != i_munic_lograd:
                            i_codg_municipio = i_munic_munic if i_munic_munic != 0 else i_munic_lograd if i_munic_lograd != 0 else 0
                            # loga_mensagem(f'2 Trocou o municipio - {i_codg_municipio}')

                    nova_linha = {'numr_inscricao_contrib': i_numr_inscricao,
                                  'stat_cadastro_contrib': i_stat_cadastro_contrib,
                                  'codg_municipio': i_codg_municipio,
                                  'codg_uf': 'GO',
                                  'tipo_enqdto_fiscal': i_tipo_enqdto_fiscal,
                                  'indi_produtor_rural': s_prod_rural,
                                  'indi_produtor_rural_exclusivo': s_prod_rural_exclusivo,
                                  'data_inicio_vigencia': d_data_alteracao,
                                  'data_fim_vigencia': pd.NaT,
                                  }
                    lista_resultados.append(nova_linha)
                    # loga_mensagem(f'2--> {nova_linha}')

                    del db

                    if len(lista_resultados) > 0:
                        df_ret = pd.DataFrame(lista_resultados)

                        # df_ret.name = f'df{i_numr_inscricao}'
                        # baixa_csv(df_ret)

                        # Retirando linhas duplicadas do histórico
                        df = df_ret.drop_duplicates(subset=['numr_inscricao_contrib', 'data_inicio_vigencia'], keep='first')

                        # Ordenando por contribuinte e data
                        df = df.sort_values(by=['numr_inscricao_contrib', 'data_inicio_vigencia']).reset_index(drop=True)
                        df['data_inicio_vigencia'] = df['data_inicio_vigencia'].dt.floor('D')  # Removendo horas

                        # Criando colunas auxiliares com os valores do dia anterior
                        df['stat_cadastro_contrib_anterior'] = df.groupby('numr_inscricao_contrib')[
                            'stat_cadastro_contrib'].shift(1)
                        df['codg_municipio_anterior'] = df.groupby('numr_inscricao_contrib')['codg_municipio'].shift(1)
                        df['tipo_enqdto_fiscal_anterior'] = df.groupby('numr_inscricao_contrib')[
                            'tipo_enqdto_fiscal'].shift(1)
                        df['indi_produtor_rural_anterior'] = df.groupby('numr_inscricao_contrib')[
                            'indi_produtor_rural'].shift(1)
                        df['indi_produtor_rural_exclusivo_anterior'] = df.groupby('numr_inscricao_contrib')[
                            'indi_produtor_rural_exclusivo'].shift(1)

                        # Identificando alterações em relação ao dia anterior
                        df['alterou_stat_cadastro'] = df['stat_cadastro_contrib'] != df['stat_cadastro_contrib_anterior']
                        df['alterou_municipio'] = df['codg_municipio'] != df['codg_municipio_anterior']
                        df['alterou_tipo_enqdto'] = df['tipo_enqdto_fiscal'] != df['tipo_enqdto_fiscal_anterior']
                        df['alterou_produtor_rural'] = df['indi_produtor_rural'] != df['indi_produtor_rural_anterior']
                        df['alterou_produtor_rural_exclusivo'] = df['indi_produtor_rural_exclusivo'] != df[
                            'indi_produtor_rural_exclusivo_anterior']

                        # Criando máscara para manter apenas linhas que tiveram pelo menos uma alteração
                        df['mantem_linha'] = df[['alterou_stat_cadastro', 'alterou_municipio', 'alterou_tipo_enqdto',
                                                 'alterou_produtor_rural', 'alterou_produtor_rural_exclusivo']].any(axis=1)

                        # Removendo colunas auxiliares
                        df = df.drop(columns=[
                            'stat_cadastro_contrib_anterior', 'codg_municipio_anterior', 'tipo_enqdto_fiscal_anterior',
                            'indi_produtor_rural_anterior', 'indi_produtor_rural_exclusivo_anterior',
                            'alterou_stat_cadastro', 'alterou_municipio', 'alterou_tipo_enqdto',
                            'alterou_produtor_rural', 'alterou_produtor_rural_exclusivo'
                        ])

                        # Consolidando todas as alterações do mesmo dia em uma única linha
                        df_consolidado = df[df['mantem_linha']].drop(columns=['mantem_linha'])

                        # Ajustando data_fim_vigencia para o dia anterior à próxima alteração
                        df_consolidado['data_fim_vigencia'] = df_consolidado.groupby('numr_inscricao_contrib')['data_inicio_vigencia'].shift(-1) - pd.Timedelta(seconds=1)

                        # Garantindo que não haja valores inconsistentes
                        df_consolidado = df_consolidado[df_consolidado['data_fim_vigencia'] != df_consolidado['data_inicio_vigencia']]

                        # Troca valor nulo da data de vigência final para valor de data máximo
                        df_consolidado['data_fim_vigencia'] = df_consolidado['data_fim_vigencia'].fillna(pd.Timestamp.max)

                        # Elimida linhas com a mesda data de inico
                        obj_retorno = df_consolidado.groupby(['numr_inscricao_contrib', 'data_inicio_vigencia'],as_index=False).last()

                        # Retorna o valor nulo para a data de vigência final
                        obj_retorno.loc[obj_retorno['data_fim_vigencia'] == pd.Timestamp.max, 'data_fim_vigencia'] = pd.NaT

                    obj_retorno['id_procesm_indice'] = procesm.id_procesm_indice

                    obj_retorno.name = 'obj_retorno'
                    baixa_csv(obj_retorno)

            """

            loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_hist)} linhas selecionadas. - ' + str(datetime.now() - data_hora_atividade))

            # etapaProcess = f'{procesm.id_procesm_indice} Atualização cadastral dos contribuintes de {p_data_inicio} a {p_data_fim} finalizada. Inseridos {len(obj_retorno)} atualizações.'
            procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
            procesm.stat_procesm_indice = EnumStatusProcessamento.sucesso.value
            self.registra_processamento(procesm, etapaProcess)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

        sData_hora_final = datetime.now(LOCAL_TZ).strftime('%d/%m/%Y %H:%M:%S,%f')
        loga_mensagem(f'{etapaProcess} - {sData_hora_final} - Tempo de processamento: {(datetime.now() - dData_hora_inicio)}.')

        return 0

    def inclui_informacoes_historico(self, df, p_data_ref, procesm):
        etapaProcess = f"class {self.__class__.__name__} - def inclui_informacoes_historico."
        # loga_mensagem(etapaProcess)

        try:
            s_data_ref = str(p_data_ref)
            s_chave_contrib_ant = None
            lista_excl_cnae = []
            lista_incl_cnae = []
            b_alterou_cnae = False
            b_alterou_munic = False
            i_munic_munic = 0
            i_munic_lograd = 0
            lista_resultados = []
            df_historico = pd.DataFrame()

            max_lote = 1000
            filtro = df['codg_uf'] == 'GO'
            l_inscrs = df[filtro, 'numr_inscricao_contrib'].tolist()
            for i in range(0, len(l_inscrs), max_lote):
                inscrs = l_inscrs[i:i + max_lote]
                lista_inscrs = ', '.join(map(str, inscrs))

                db = Oraprd()
                df_contrib = db.select_cadastro_contrib(lista_inscrs)
                # loga_mensagem(f'Retornaram {len(df_contrib)} contribuintes do cadastro atual.')

                df_cnae = db.select_cnae_contrib(lista_inscrs)
                # loga_mensagem(f'Retornaram {len(df_cnae)} cnaes dos contribuintes do cadastro atual.')

                df_cnae_produtor = db.select_cnae_produtor()
                # loga_mensagem(f'Retornaram {len(df_cnae_produtor)} cnaes de produtores rurais.')

                del db

                # Convertendo colunas para os tipos corretos
                df_contrib['numr_inscricao_contrib'] = df_contrib['numr_inscricao_contrib'].astype(int)
                df_contrib['stat_cadastro_contrib'] = df_contrib['stat_cadastro_contrib'].astype(int)

                # Merge de CNAEs (feito uma vez antes do loop principal)
                if ~df_cnae.empty and len(df_cnae) > 0:
                    df_cnae_merge = df_cnae.merge(df_cnae_produtor,
                                                  left_on='codg_subclasse_cnaef',
                                                  right_on='codg_cnae_rural',
                                                  how='left')
                    # Determina se cada inscrição possui apenas CNAEs rurais ou pelo menos um
                    df_cnae_agrupado = df_cnae_merge.groupby('numr_inscricao') \
                                                    .agg(b_prod_rural_exclusivo=('codg_cnae_rural', lambda x: x.notna().all()),b_prod_rural=('codg_cnae_rural', lambda x: x.notna().any())) \
                                                    .reset_index()

                    # Adicionando colunas categóricas no DataFrame original
                    df_cnae_agrupado['s_prod_rural_exclusivo'] = df_cnae_agrupado['b_prod_rural_exclusivo'].map({True: 'S', False: 'N'})
                    df_cnae_agrupado['s_prod_rural'] = df_cnae_agrupado['b_prod_rural'].map({True: 'S', False: 'N'})
                    df_cnae_agrupado = df_cnae_agrupado.drop(columns=['b_prod_rural_exclusivo', 'b_prod_rural'])

                    # Fazendo merge com df_contrib para adicionar os status
                    df_cnae_agrupado.rename(columns={'numr_inscricao': 'numr_inscricao_contrib'}, inplace=True)
                    df_contrib = df_contrib.merge(df_cnae_agrupado, on='numr_inscricao_contrib', how='left')
                else:
                    df_contrib['s_prod_rural'] = 'N'
                    df_contrib['s_prod_rural_exclusivo'] = 'N'

                db = SSNClasse()
                df_simples = db.busca_historico_ssn(lista_inscrs)
                # loga_mensagem(f'Retornaram {len(df_simples)} historicos do Simples Nacional.')
                del db

                db = Oraprd()
                df_hist = db.select_historico_contrib(lista_inscrs)
                # loga_mensagem(f'Retornaram {len(df_hist)} historicos do LOG de auditoria.')
                del db

                df_hist['numr_inscricao'] = df_hist['numr_inscricao'].astype(int)

                if len(df_simples) > 0:
                    df_simples['numr_inscricao'] = df_simples['numr_inscricao'].astype(int)
                    df_hist = pd.concat([df_hist, df_simples], ignore_index=True)

                df_hist = df_hist.sort_values(by=['numr_inscricao', 'data_hora_transacao'], ascending=[True, False])

                # loga_mensagem(f'Processando {len(df_hist)} historicos do LOG de auditoria.')

                # Criar um DataFrame para armazenar os resultados
                dfs = []

                # Iteração sobre df_contrib
                for rowC in df_contrib.itertuples(index=False):
                    i_numr_inscricao = rowC.numr_inscricao_contrib
                    i_stat_cadastro_contrib = rowC.stat_cadastro_contrib
                    i_tipo_enqdto_fiscal = rowC.tipo_enqdto_fiscal
                    i_codg_municipio = rowC.codg_municipio
                    s_prod_rural = 'N' if pd.isna(rowC.s_prod_rural) else rowC.s_prod_rural
                    s_prod_rural_exclusivo = 'N' if pd.isna(rowC.s_prod_rural_exclusivo) else rowC.s_prod_rural_exclusivo
                    d_data_alteracao = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                    # loga_mensagem(f'1 Inscrição: {i_numr_inscricao} - Município: {i_codg_municipio} - Alt Municipio: {b_alterou_munic}')

                    lista_resultados = []

                    db = Oraprd()
                    filtro = df_hist['numr_inscricao'] == i_numr_inscricao
                    filtro_cnae = df_cnae['numr_inscricao'] == i_numr_inscricao
                    for rowH in df_hist[filtro].itertuples(index=False):
                        s_chave_contrib = f"{rowH.numr_inscricao}-{rowH.data_hora_transacao}"
                        d_data_alteracao = rowH.data_hora_transacao
                        # loga_mensagem(f'2 Inscrição: {i_numr_inscricao} - Município: {i_codg_municipio} - Data Alteração: {d_data_alteracao} - Alt Municipio: {b_alterou_munic}')

                        if s_chave_contrib_ant != s_chave_contrib:
                            # loga_mensagem(f'Quebrou a chave - {s_chave_contrib_ant} != {s_chave_contrib}')
                            # Excluir CNAEs
                            if lista_excl_cnae:
                                df_excl_cnae = pd.DataFrame(lista_excl_cnae)
                                df_cnae = df_cnae[~df_cnae.set_index(['numr_inscricao', 'id_contrib_cnaef']).index.isin(
                                    df_excl_cnae.set_index(['numr_inscricao', 'id_contrib_cnaef']).index)]
                                b_alterou_cnae = True
                                lista_excl_cnae = []

                            # Incluir CNAEs
                            if lista_incl_cnae:
                                df_lista_cnae = pd.DataFrame(lista_incl_cnae)
                                df_incl_cnae = db.select_cce_cnae(df_lista_cnae['id_subclasse_cnaef'].tolist())
                                df_incl_cnae['numr_inscricao'] = i_numr_inscricao
                                df_cnae = pd.concat([df_cnae, df_incl_cnae], ignore_index=True)
                                b_alterou_cnae = True
                                lista_incl_cnae = []

                            if b_alterou_cnae:
                                filtro_cnae = df_cnae['numr_inscricao'] == i_numr_inscricao
                                if len(df_cnae[filtro_cnae]) > 0:
                                    df_cnae_merge = df_cnae[filtro_cnae].merge(df_cnae_produtor
                                                                , left_on='codg_subclasse_cnaef'
                                                                , right_on='codg_cnae_rural'
                                                                , how='left')
                                    b_prod_rural_exclusivo = df_cnae_merge['codg_cnae_rural'].notna().all()
                                    b_prod_rural = df_cnae_merge['codg_cnae_rural'].notna().any()
                                    s_prod_rural_exclusivo = 'S' if b_prod_rural_exclusivo else 'N'
                                    s_prod_rural = 'S' if b_prod_rural else 'N'
                                else:
                                    s_prod_rural_exclusivo = 'N'
                                    s_prod_rural = 'N'

                                b_alterou_cnae = False

                            if b_alterou_munic:
                                b_alterou_munic = False
                                # loga_mensagem(f'Municipios: i_codg_municipio:{i_codg_municipio} - i_munic_munic:{i_munic_munic} - i_munic_lograd:{i_munic_lograd} - b_alterou_munic:{b_alterou_munic}')
                                if i_codg_municipio != i_munic_munic and i_codg_municipio != i_munic_lograd:
                                    i_codg_municipio = i_munic_munic if i_munic_munic != 0 else i_munic_lograd if i_munic_lograd != 0 else 0
                                    # loga_mensagem(f'Trocou o municipio - {i_codg_municipio}')

                            nova_linha = {'numr_inscricao_contrib': i_numr_inscricao,
                                          'stat_cadastro_contrib': i_stat_cadastro_contrib,
                                          'codg_municipio': i_codg_municipio,
                                          'codg_uf': 'GO',
                                          'tipo_enqdto_fiscal': i_tipo_enqdto_fiscal,
                                          'indi_produtor_rural': s_prod_rural,
                                          'indi_produtor_rural_exclusivo': s_prod_rural_exclusivo,
                                          'data_inicio_vigencia': d_data_alteracao,
                                          'data_fim_vigencia': pd.NaT,
                                          }
                            lista_resultados.append(nova_linha)
                            # loga_mensagem(f'1--> {nova_linha}')

                            s_chave_contrib_ant = s_chave_contrib

                        # Testes e atualizações conforme mudanças no histórico
                        if rowH.nome_tabela == 'CCE_CONTRIBUINTE' and rowH.nome_coluna == 'INDI_SITUACAO_CADASTRAL':
                            i_stat_cadastro_contrib = int(rowH.valr_anterior_coluna)
                            # loga_mensagem('Alterou Situação cadastral')

                        if rowH.nome_tabela == 'GEN_ENDERECO' and rowH.nome_coluna == 'CODG_MUNICIPIO':
                            i_munic_munic = int(rowH.valr_anterior_coluna) if pd.notna(rowH.valr_anterior_coluna) else 0
                            b_alterou_munic = True
                            # loga_mensagem(f'Alterou Municipio: {rowH.valr_anterior_coluna}-{rowH.valr_subst_coluna}')

                        if rowH.nome_tabela == 'GEN_ENDERECO' and rowH.nome_coluna == 'CODG_LOGRADOURO':
                            # i_munic_lograd = 0
                            # b_alterou_munic = True
                            # if pd.notna(rowH.valr_anterior_coluna):
                            #     df_munic = db.select_logradouro_municipio(rowH.valr_anterior_coluna)
                            #     if df_munic is not None and not df_munic.empty:
                            #         i_munic_lograd = df_munic['codg_municipio'].iloc[0]

                            df_munic = db.select_logradouro_municipio(rowH.valr_anterior_coluna) if pd.notna(rowH.valr_anterior_coluna) else None
                            # i_munic_lograd = df_munic['codg_municipio'].iloc[0] if df_munic is not None else 0
                            i_munic_lograd = df_munic['codg_municipio'].iloc[0] if isinstance(df_munic, pd.DataFrame) and not df_munic.empty else 0
                            b_alterou_munic = True
                            # loga_mensagem(f'Alterou Logradouro: {rowH.valr_anterior_coluna}-{rowH.valr_subst_coluna}')

                        if rowH.nome_tabela == 'CCE_CONTRIB_CNAEF' and rowH.nome_coluna == 'ID_CONTRIB_CNAEF' and rowH.tipo_transacao == 'I':
                            lista_excl_cnae.append(
                                {'numr_inscricao': i_numr_inscricao, 'id_contrib_cnaef': int(rowH.valr_subst_coluna)})
                            # loga_mensagem(f'excluiu cnaef: {rowH.valr_subst_coluna}-{rowH.valr_subst_coluna}')

                        if rowH.nome_tabela == 'CCE_CONTRIB_CNAEF' and rowH.nome_coluna == 'ID_SUBCLASSE_CNAEF' and rowH.tipo_transacao == 'D':
                            lista_incl_cnae.append(
                                {'numr_inscricao': i_numr_inscricao, 'id_subclasse_cnaef': int(rowH.valr_anterior_coluna)})
                            # loga_mensagem(f'Incluiu cnaef: {rowH.valr_subst_coluna}-{rowH.valr_subst_coluna}')

                        if rowH.nome_tabela == 'IPM_OPCAO_CONTRIB_SIMPL_SIMEI' and rowH.nome_coluna == 'OPCAO_SIMPLES':
                            i_tipo_enqdto_fiscal = rowH.valr_anterior_coluna
                            # loga_mensagem('Alterou Simples')

                    # loga_mensagem(f'Acabaram os históricos da Inscrição.')

                    # Excluir CNAEs
                    if lista_excl_cnae:
                        df_excl_cnae = pd.DataFrame(lista_excl_cnae)
                        df_cnae = df_cnae[~df_cnae.set_index(['numr_inscricao', 'id_contrib_cnaef']).index.isin(
                            df_excl_cnae.set_index(['numr_inscricao', 'id_contrib_cnaef']).index)]
                        b_alterou_cnae = True
                        lista_excl_cnae = []

                    # Incluir CNAEs
                    if lista_incl_cnae:
                        df_lista_cnae = pd.DataFrame(lista_incl_cnae)
                        df_incl_cnae = db.select_cce_cnae(df_lista_cnae['id_subclasse_cnaef'].tolist())
                        df_incl_cnae['numr_inscricao'] = i_numr_inscricao
                        df_cnae = pd.concat([df_cnae, df_incl_cnae], ignore_index=True)
                        b_alterou_cnae = True
                        lista_incl_cnae = []

                    if b_alterou_cnae:
                        filtro_cnae = df_cnae['numr_inscricao'] == i_numr_inscricao
                        if len(df_cnae[filtro_cnae]) > 0:
                            # loga_mensagem('Ainda tem cnae')
                            df_cnae_merge = df_cnae[filtro_cnae].merge(df_cnae_produtor
                                                          , left_on='codg_subclasse_cnaef'
                                                          , right_on='codg_cnae_rural'
                                                          , how='left')
                            b_prod_rural_exclusivo = df_cnae_merge['codg_cnae_rural'].notna().all()
                            b_prod_rural = df_cnae_merge['codg_cnae_rural'].notna().any()
                            s_prod_rural_exclusivo = 'S' if b_prod_rural_exclusivo else 'N'
                            s_prod_rural = 'S' if b_prod_rural else 'N'
                            # loga_mensagem(f's_prod_rural_exclusivo: {s_prod_rural_exclusivo} - s_prod_rural:{s_prod_rural} - df_cnae_merge: {df_cnae_merge}')
                        else:
                            # loga_mensagem('Sem cnae')
                            s_prod_rural_exclusivo = 'N'
                            s_prod_rural = 'N'

                        b_alterou_cnae = False

                    if b_alterou_munic:
                        b_alterou_munic = False
                        # loga_mensagem(f'2 Municipios: i_codg_municipio:{i_codg_municipio} - i_munic_munic:{i_munic_munic} - i_munic_lograd:{i_munic_lograd} - b_alterou_munic:{b_alterou_munic}')
                        if i_codg_municipio != i_munic_munic and i_codg_municipio != i_munic_lograd:
                            i_codg_municipio = i_munic_munic if i_munic_munic != 0 else i_munic_lograd if i_munic_lograd != 0 else 0
                            # loga_mensagem(f'2 Trocou o municipio - {i_codg_municipio}')

                    nova_linha = {'numr_inscricao_contrib': i_numr_inscricao,
                                  'stat_cadastro_contrib': i_stat_cadastro_contrib,
                                  'codg_municipio': i_codg_municipio,
                                  'codg_uf': 'GO',
                                  'tipo_enqdto_fiscal': i_tipo_enqdto_fiscal,
                                  'indi_produtor_rural': s_prod_rural,
                                  'indi_produtor_rural_exclusivo': s_prod_rural_exclusivo,
                                  'data_inicio_vigencia': d_data_alteracao,
                                  'data_fim_vigencia': pd.NaT,
                                  }
                    lista_resultados.append(nova_linha)
                    # loga_mensagem(f'2--> {nova_linha}')

                    del db

                    if len(lista_resultados) > 0:
                        df_ret = pd.DataFrame(lista_resultados)

                        # Retirando linhas duplicadas do histórico
                        df = df_ret.drop_duplicates(subset=['numr_inscricao_contrib', 'data_inicio_vigencia'], keep='first')

                        # Ordenando por contribuinte e data
                        df = df.sort_values(by=['numr_inscricao_contrib', 'data_inicio_vigencia']).reset_index(drop=True)
                        df['data_inicio_vigencia'] = df['data_inicio_vigencia'].dt.floor('D')  # Removendo horas

                        # Criando colunas auxiliares com os valores do dia anterior
                        df['stat_cadastro_contrib_anterior'] = df.groupby('numr_inscricao_contrib')[
                            'stat_cadastro_contrib'].shift(1)
                        df['codg_municipio_anterior'] = df.groupby('numr_inscricao_contrib')['codg_municipio'].shift(1)
                        df['tipo_enqdto_fiscal_anterior'] = df.groupby('numr_inscricao_contrib')[
                            'tipo_enqdto_fiscal'].shift(1)
                        df['indi_produtor_rural_anterior'] = df.groupby('numr_inscricao_contrib')[
                            'indi_produtor_rural'].shift(1)
                        df['indi_produtor_rural_exclusivo_anterior'] = df.groupby('numr_inscricao_contrib')[
                            'indi_produtor_rural_exclusivo'].shift(1)

                        # Identificando alterações em relação ao dia anterior
                        df['alterou_stat_cadastro'] = df['stat_cadastro_contrib'] != df['stat_cadastro_contrib_anterior']
                        df['alterou_municipio'] = df['codg_municipio'] != df['codg_municipio_anterior']
                        df['alterou_tipo_enqdto'] = df['tipo_enqdto_fiscal'] != df['tipo_enqdto_fiscal_anterior']
                        df['alterou_produtor_rural'] = df['indi_produtor_rural'] != df['indi_produtor_rural_anterior']
                        df['alterou_produtor_rural_exclusivo'] = df['indi_produtor_rural_exclusivo'] != df[
                            'indi_produtor_rural_exclusivo_anterior']

                        # Criando máscara para manter apenas linhas que tiveram pelo menos uma alteração
                        df['mantem_linha'] = df[['alterou_stat_cadastro', 'alterou_municipio', 'alterou_tipo_enqdto',
                                                 'alterou_produtor_rural', 'alterou_produtor_rural_exclusivo']].any(axis=1)

                        # Removendo colunas auxiliares
                        df = df.drop(columns=[
                            'stat_cadastro_contrib_anterior', 'codg_municipio_anterior', 'tipo_enqdto_fiscal_anterior',
                            'indi_produtor_rural_anterior', 'indi_produtor_rural_exclusivo_anterior',
                            'alterou_stat_cadastro', 'alterou_municipio', 'alterou_tipo_enqdto',
                            'alterou_produtor_rural', 'alterou_produtor_rural_exclusivo'
                        ])

                        # Consolidando todas as alterações do mesmo dia em uma única linha
                        df_consolidado = df[df['mantem_linha']].drop(columns=['mantem_linha'])

                        # Ajustando data_fim_vigencia para o dia anterior à próxima alteração
                        df_consolidado['data_fim_vigencia'] = df_consolidado.groupby('numr_inscricao_contrib')['data_inicio_vigencia'].shift(-1) - pd.Timedelta(seconds=1)

                        # Garantindo que não haja valores inconsistentes
                        df_consolidado = df_consolidado[df_consolidado['data_fim_vigencia'] != df_consolidado['data_inicio_vigencia']]

                        # Troca valor nulo da data de vigência final para valor de data máximo
                        df_consolidado['data_fim_vigencia'] = df_consolidado['data_fim_vigencia'].fillna(pd.Timestamp.max)

                        # Elimida linhas com a mesda data de inico
                        obj_retorno = df_consolidado.groupby(['numr_inscricao_contrib', 'data_inicio_vigencia'], as_index=False).last()

                        # Retorna o valor nulo para a data de vigência final
                        obj_retorno.loc[obj_retorno['data_fim_vigencia'] == pd.Timestamp.max, 'data_fim_vigencia'] = pd.NaT

                    obj_retorno['id_procesm_indice'] = procesm.id_procesm_indice

                    db = Oraprd()
                    df_historico = db.insert_contrib_ipm(obj_retorno)
                    del db

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

        finally:
            return df_historico

    def busca_historico_cce(self, df) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def busca_historico_cce - {len(df)} Inscrições."
        loga_mensagem(etapaProcess)

        # df.name = 'df_busca_historico_cce'
        # baixa_csv(df)

        s_chave_contrib_ant = None
        lista_excl_cnae = []
        lista_incl_cnae = []
        b_alterou_cnae = False
        b_alterou_munic = False
        i_munic_munic = 0
        i_munic_lograd = 0
        max_lote = 1000
        obj_retorno = pd.DataFrame()

        try:
            l_inscrs = df['numr_inscricao_contrib'].tolist()
            for i in range(0, len(l_inscrs), max_lote):
                inscrs = l_inscrs[i:i + max_lote]
                lista_inscrs = ', '.join(map(str, inscrs))

                db = Oraprd()
                df_contrib = db.select_cadastro_contrib(lista_inscrs)
                # loga_mensagem(f'Retornaram {len(df_contrib)} contribuintes do cadastro atual.')
                if len(df_contrib) > 0:
                    # df_contrib.name = 'df_contrib'
                    # baixa_csv(df_contrib)

                    df_cnae = db.select_cnae_contrib(lista_inscrs)
                    # loga_mensagem(f'Retornaram {len(df_cnae)} cnaes dos contribuintes do cadastro atual.')

                    df_cnae_produtor = db.select_cnae_produtor()
                    # loga_mensagem(f'Retornaram {len(df_cnae_produtor)} cnaes de produtores rurais.')

                    del db

                    # Convertendo colunas para os tipos corretos
                    df_contrib['numr_inscricao_contrib'] = df_contrib['numr_inscricao_contrib'].astype(int)
                    df_contrib['stat_cadastro_contrib'] = df_contrib['stat_cadastro_contrib'].astype(int)

                    # Merge de CNAEs (feito uma vez antes do loop principal)
                    if ~df_cnae.empty and len(df_cnae) > 0:
                        df_cnae_merge = df_cnae.merge(df_cnae_produtor,
                                                      left_on='codg_subclasse_cnaef',
                                                      right_on='codg_cnae_rural',
                                                      how='left')
                        # Determina se cada inscrição possui apenas CNAEs rurais ou pelo menos um
                        df_cnae_agrupado = df_cnae_merge.groupby('numr_inscricao') \
                                                        .agg(b_prod_rural_exclusivo=('codg_cnae_rural', lambda x: x.notna().all()),b_prod_rural=('codg_cnae_rural', lambda x: x.notna().any())) \
                                                        .reset_index()

                        # Adicionando colunas categóricas no DataFrame original
                        df_cnae_agrupado['s_prod_rural_exclusivo'] = df_cnae_agrupado['b_prod_rural_exclusivo'].map({True: 'S', False: 'N'})
                        df_cnae_agrupado['s_prod_rural'] = df_cnae_agrupado['b_prod_rural'].map({True: 'S', False: 'N'})
                        df_cnae_agrupado = df_cnae_agrupado.drop(columns=['b_prod_rural_exclusivo', 'b_prod_rural'])

                        # Fazendo merge com df_contrib para adicionar os status
                        df_cnae_agrupado.rename(columns={'numr_inscricao': 'numr_inscricao_contrib'}, inplace=True)
                        df_contrib = df_contrib.merge(df_cnae_agrupado, on='numr_inscricao_contrib', how='left')
                    else:
                        df_contrib['s_prod_rural'] = 'N'
                        df_contrib['s_prod_rural_exclusivo'] = 'N'
                else:
                    df_contrib['stat_cadastro_contrib'] = '0'
                    df_contrib['tipo_enqdto_fiscal'] = '0'
                    df_contrib['s_prod_rural'] = 'N'
                    df_contrib['s_prod_rural_exclusivo'] = 'N'

                db = SSNClasse()
                df_simples = db.busca_historico_ssn(lista_inscrs)
                # loga_mensagem(f'Retornaram {len(df_simples)} historicos do Simples Nacional.')
                del db

                # df_simples.name = 'df_simples'
                # baixa_csv(df_simples)

                db = Oraprodx9()
                df_hist = db.select_historico_contrib(lista_inscrs)
                # loga_mensagem(f'Retornaram {len(df_hist)} historicos do LOG de auditoria.')
                del db

                # df_hist.name = 'df_histant'
                # baixa_csv(df_hist)

                if len(df_hist) > 0:
                    df_hist['numr_inscricao'] = df_hist['numr_inscricao'].astype(int)

                if len(df_simples) > 0:
                    df_simples['numr_inscricao'] = df_simples['numr_inscricao'].astype(int)
                    df_hist = pd.concat([df_hist, df_simples], ignore_index=True)

                if len(df_hist) > 0:
                    df_hist = df_hist.sort_values(by=['numr_inscricao', 'data_hora_transacao'], ascending=[True, False])

                loga_mensagem(f'Processando {len(df_hist)} historicos do LOG de auditoria.')

                # df_hist.name = 'df_hist'
                # baixa_csv(df_hist)

                # Iteração sobre df_contrib
                for rowC in df_contrib.itertuples(index=False):
                    i_numr_inscricao = rowC.numr_inscricao_contrib
                    i_stat_cadastro_contrib = rowC.stat_cadastro_contrib
                    i_tipo_enqdto_fiscal = rowC.tipo_enqdto_fiscal
                    i_codg_municipio = rowC.codg_municipio
                    s_prod_rural = 'N' if pd.isna(rowC.s_prod_rural) else rowC.s_prod_rural
                    s_prod_rural_exclusivo = 'N' if pd.isna(rowC.s_prod_rural_exclusivo) else rowC.s_prod_rural_exclusivo
                    d_data_alteracao = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                    # loga_mensagem(f'1 Inscrição: {i_numr_inscricao} - Município: {i_codg_municipio} - Alt Municipio: {b_alterou_munic}')

                    lista_resultados = []

                    db = Oraprd()
                    if len(df_cnae) > 0:
                        filtro_cnae = df_cnae['numr_inscricao'] == i_numr_inscricao

                    if len(df_hist) > 0:
                        filtro = df_hist['numr_inscricao'] == i_numr_inscricao
                        for rowH in df_hist[filtro].itertuples(index=False):
                            s_chave_contrib = f"{rowH.numr_inscricao}-{rowH.data_hora_transacao}"
                            d_data_alteracao = rowH.data_hora_transacao
                            # loga_mensagem(f'2 Inscrição: {i_numr_inscricao} - Município: {i_codg_municipio} - Data Alteração: {d_data_alteracao} - Alt Municipio: {b_alterou_munic}')

                            if s_chave_contrib_ant != s_chave_contrib:
                                # loga_mensagem(f'Quebrou a chave - {s_chave_contrib_ant} != {s_chave_contrib}')
                                # Excluir CNAEs
                                if lista_excl_cnae:
                                    df_excl_cnae = pd.DataFrame(lista_excl_cnae)
                                    df_cnae = df_cnae[~df_cnae.set_index(['numr_inscricao', 'id_contrib_cnaef']).index.isin(
                                        df_excl_cnae.set_index(['numr_inscricao', 'id_contrib_cnaef']).index)]
                                    b_alterou_cnae = True
                                    lista_excl_cnae = []

                                # Incluir CNAEs
                                if lista_incl_cnae:
                                    df_lista_cnae = pd.DataFrame(lista_incl_cnae)
                                    df_incl_cnae = db.select_cce_cnae(df_lista_cnae['id_subclasse_cnaef'].tolist())
                                    df_incl_cnae['numr_inscricao'] = i_numr_inscricao
                                    df_cnae = pd.concat([df_cnae, df_incl_cnae], ignore_index=True)
                                    b_alterou_cnae = True
                                    lista_incl_cnae = []

                                if b_alterou_cnae:
                                    filtro_cnae = df_cnae['numr_inscricao'] == i_numr_inscricao
                                    if len(df_cnae[filtro_cnae]) > 0:
                                        df_cnae_merge = df_cnae[filtro_cnae].merge(df_cnae_produtor
                                                                    , left_on='codg_subclasse_cnaef'
                                                                    , right_on='codg_cnae_rural'
                                                                    , how='left')
                                        b_prod_rural_exclusivo = df_cnae_merge['codg_cnae_rural'].notna().all()
                                        b_prod_rural = df_cnae_merge['codg_cnae_rural'].notna().any()
                                        s_prod_rural_exclusivo = 'S' if b_prod_rural_exclusivo else 'N'
                                        s_prod_rural = 'S' if b_prod_rural else 'N'
                                    else:
                                        s_prod_rural_exclusivo = 'N'
                                        s_prod_rural = 'N'

                                    b_alterou_cnae = False

                                if b_alterou_munic:
                                    b_alterou_munic = False
                                    # loga_mensagem(f'Municipios: i_codg_municipio:{i_codg_municipio} - i_munic_munic:{i_munic_munic} - i_munic_lograd:{i_munic_lograd} - b_alterou_munic:{b_alterou_munic}')
                                    if i_codg_municipio != i_munic_munic and i_codg_municipio != i_munic_lograd:
                                        i_codg_municipio = i_munic_munic if i_munic_munic != 0 else i_munic_lograd if i_munic_lograd != 0 else 0
                                        # loga_mensagem(f'Trocou o municipio - {i_codg_municipio}')

                                nova_linha = {'numr_inscricao_contrib': i_numr_inscricao,
                                              'stat_cadastro_contrib': i_stat_cadastro_contrib,
                                              'codg_municipio': i_codg_municipio,
                                              'codg_uf': 'GO',
                                              'tipo_enqdto_fiscal': i_tipo_enqdto_fiscal,
                                              'indi_produtor_rural': s_prod_rural,
                                              'indi_produtor_rural_exclusivo': s_prod_rural_exclusivo,
                                              'data_inicio_vigencia': d_data_alteracao,
                                              'data_fim_vigencia': pd.NaT,
                                              }
                                lista_resultados.append(nova_linha)
                                # loga_mensagem(f'1--> {nova_linha}')

                                s_chave_contrib_ant = s_chave_contrib

                            # Testes e atualizações conforme mudanças no histórico
                            if rowH.nome_tabela == 'CCE_CONTRIBUINTE' and rowH.nome_coluna == 'INDI_SITUACAO_CADASTRAL':
                                i_stat_cadastro_contrib = int(rowH.valr_anterior_coluna)
                                # loga_mensagem('Alterou Situação cadastral')

                            if rowH.nome_tabela == 'GEN_ENDERECO' and rowH.nome_coluna == 'CODG_MUNICIPIO':
                                i_munic_munic = int(rowH.valr_anterior_coluna) if pd.notna(rowH.valr_anterior_coluna) else 0
                                b_alterou_munic = True
                                # loga_mensagem(f'Alterou Municipio: {rowH.valr_anterior_coluna}-{rowH.valr_subst_coluna}')

                            if rowH.nome_tabela == 'GEN_ENDERECO' and rowH.nome_coluna == 'CODG_LOGRADOURO':
                                # i_munic_lograd = 0
                                # b_alterou_munic = True
                                # if pd.notna(rowH.valr_anterior_coluna):
                                #     df_munic = db.select_logradouro_municipio(rowH.valr_anterior_coluna)
                                #     if df_munic is not None and not df_munic.empty:
                                #         i_munic_lograd = df_munic['codg_municipio'].iloc[0]

                                df_munic = db.select_logradouro_municipio(rowH.valr_anterior_coluna) if pd.notna(rowH.valr_anterior_coluna) else None
                                # i_munic_lograd = df_munic['codg_municipio'].iloc[0] if df_munic is not None else 0
                                i_munic_lograd = df_munic['codg_municipio'].iloc[0] if isinstance(df_munic, pd.DataFrame) and not df_munic.empty else 0
                                b_alterou_munic = True
                                # loga_mensagem(f'Alterou Logradouro: {rowH.valr_anterior_coluna}-{rowH.valr_subst_coluna}')

                            if rowH.nome_tabela == 'CCE_CONTRIB_CNAEF' and rowH.nome_coluna == 'ID_CONTRIB_CNAEF' and rowH.tipo_transacao == 'I':
                                lista_excl_cnae.append(
                                    {'numr_inscricao': i_numr_inscricao, 'id_contrib_cnaef': int(rowH.valr_subst_coluna)})
                                # loga_mensagem(f'excluiu cnaef: {rowH.valr_subst_coluna}-{rowH.valr_subst_coluna}')

                            if rowH.nome_tabela == 'CCE_CONTRIB_CNAEF' and rowH.nome_coluna == 'ID_SUBCLASSE_CNAEF' and rowH.tipo_transacao == 'D':
                                lista_incl_cnae.append(
                                    {'numr_inscricao': i_numr_inscricao, 'id_subclasse_cnaef': int(rowH.valr_anterior_coluna)})
                                # loga_mensagem(f'Incluiu cnaef: {rowH.valr_subst_coluna}-{rowH.valr_subst_coluna}')

                            if rowH.nome_tabela == 'IPM_OPCAO_CONTRIB_SIMPL_SIMEI' and rowH.nome_coluna == 'OPCAO_SIMPLES':
                                i_tipo_enqdto_fiscal = rowH.valr_anterior_coluna
                                # loga_mensagem('Alterou Simples')

                    # loga_mensagem(f'Acabaram os históricos da Inscrição')

                    # Excluir CNAEs
                    if lista_excl_cnae:
                        df_excl_cnae = pd.DataFrame(lista_excl_cnae)
                        df_cnae = df_cnae[~df_cnae.set_index(['numr_inscricao', 'id_contrib_cnaef']).index.isin(
                            df_excl_cnae.set_index(['numr_inscricao', 'id_contrib_cnaef']).index)]
                        b_alterou_cnae = True
                        lista_excl_cnae = []

                    # Incluir CNAEs
                    if lista_incl_cnae:
                        df_lista_cnae = pd.DataFrame(lista_incl_cnae)
                        df_incl_cnae = db.select_cce_cnae(df_lista_cnae['id_subclasse_cnaef'].tolist())
                        df_incl_cnae['numr_inscricao'] = i_numr_inscricao
                        df_cnae = pd.concat([df_cnae, df_incl_cnae], ignore_index=True)
                        b_alterou_cnae = True
                        lista_incl_cnae = []

                    if b_alterou_cnae:
                        filtro_cnae = df_cnae['numr_inscricao'] == i_numr_inscricao
                        if len(df_cnae[filtro_cnae]) > 0:
                            # loga_mensagem('Ainda tem cnae')
                            df_cnae_merge = df_cnae[filtro_cnae].merge(df_cnae_produtor
                                                          , left_on='codg_subclasse_cnaef'
                                                          , right_on='codg_cnae_rural'
                                                          , how='left')
                            b_prod_rural_exclusivo = df_cnae_merge['codg_cnae_rural'].notna().all()
                            b_prod_rural = df_cnae_merge['codg_cnae_rural'].notna().any()
                            s_prod_rural_exclusivo = 'S' if b_prod_rural_exclusivo else 'N'
                            s_prod_rural = 'S' if b_prod_rural else 'N'
                            # loga_mensagem(f's_prod_rural_exclusivo: {s_prod_rural_exclusivo} - s_prod_rural:{s_prod_rural} - df_cnae_merge: {df_cnae_merge}')
                        else:
                            # loga_mensagem('Sem cnae')
                            s_prod_rural_exclusivo = 'N'
                            s_prod_rural = 'N'

                        b_alterou_cnae = False

                    if b_alterou_munic:
                        b_alterou_munic = False
                        # loga_mensagem(f'2 Municipios: i_codg_municipio:{i_codg_municipio} - i_munic_munic:{i_munic_munic} - i_munic_lograd:{i_munic_lograd} - b_alterou_munic:{b_alterou_munic}')
                        if i_codg_municipio != i_munic_munic and i_codg_municipio != i_munic_lograd:
                            i_codg_municipio = i_munic_munic if i_munic_munic != 0 else i_munic_lograd if i_munic_lograd != 0 else 0
                            # loga_mensagem(f'2 Trocou o municipio - {i_codg_municipio}')

                    nova_linha = {'numr_inscricao_contrib': i_numr_inscricao,
                                  'stat_cadastro_contrib': i_stat_cadastro_contrib,
                                  'codg_municipio': i_codg_municipio,
                                  'codg_uf': 'GO',
                                  'tipo_enqdto_fiscal': i_tipo_enqdto_fiscal,
                                  'indi_produtor_rural': s_prod_rural,
                                  'indi_produtor_rural_exclusivo': s_prod_rural_exclusivo,
                                  'data_inicio_vigencia': d_data_alteracao,
                                  'data_fim_vigencia': pd.NaT,
                                  }
                    lista_resultados.append(nova_linha)
                    # loga_mensagem(f'2--> {nova_linha}')

                    del db

                    if len(lista_resultados) > 0:
                        df_ret = pd.DataFrame(lista_resultados)

                        # df_ret.name = 'df{i_numr_inscricao}'
                        # baixa_csv(df_ret)

                        # Retirando linhas duplicadas do histórico
                        df = df_ret.drop_duplicates(subset=['numr_inscricao_contrib', 'data_inicio_vigencia'], keep='first')

                        # Ordenando por contribuinte e data
                        df = df.sort_values(by=['numr_inscricao_contrib', 'data_inicio_vigencia']).reset_index(drop=True)
                        df['data_inicio_vigencia'] = pd.to_datetime(df['data_inicio_vigencia'], errors='coerce')
                        df['data_inicio_vigencia'] = df['data_inicio_vigencia'].dt.floor('D')  # Removendo horas

                        # Criando colunas auxiliares com os valores do dia anterior
                        df['stat_cadastro_contrib_anterior'] = df.groupby('numr_inscricao_contrib')[
                            'stat_cadastro_contrib'].shift(1)
                        df['codg_municipio_anterior'] = df.groupby('numr_inscricao_contrib')['codg_municipio'].shift(1)
                        df['tipo_enqdto_fiscal_anterior'] = df.groupby('numr_inscricao_contrib')[
                            'tipo_enqdto_fiscal'].shift(1)
                        df['indi_produtor_rural_anterior'] = df.groupby('numr_inscricao_contrib')[
                            'indi_produtor_rural'].shift(1)
                        df['indi_produtor_rural_exclusivo_anterior'] = df.groupby('numr_inscricao_contrib')[
                            'indi_produtor_rural_exclusivo'].shift(1)

                        # Identificando alterações em relação ao dia anterior
                        df['alterou_stat_cadastro'] = df['stat_cadastro_contrib'] != df['stat_cadastro_contrib_anterior']
                        df['alterou_municipio'] = df['codg_municipio'] != df['codg_municipio_anterior']
                        df['alterou_tipo_enqdto'] = df['tipo_enqdto_fiscal'] != df['tipo_enqdto_fiscal_anterior']
                        df['alterou_produtor_rural'] = df['indi_produtor_rural'] != df['indi_produtor_rural_anterior']
                        df['alterou_produtor_rural_exclusivo'] = df['indi_produtor_rural_exclusivo'] != df[
                            'indi_produtor_rural_exclusivo_anterior']

                        # Criando máscara para manter apenas linhas que tiveram pelo menos uma alteração
                        df['mantem_linha'] = df[['alterou_stat_cadastro', 'alterou_municipio', 'alterou_tipo_enqdto',
                                                 'alterou_produtor_rural', 'alterou_produtor_rural_exclusivo']].any(axis=1)

                        # Removendo colunas auxiliares
                        df = df.drop(columns=[
                            'stat_cadastro_contrib_anterior', 'codg_municipio_anterior', 'tipo_enqdto_fiscal_anterior',
                            'indi_produtor_rural_anterior', 'indi_produtor_rural_exclusivo_anterior',
                            'alterou_stat_cadastro', 'alterou_municipio', 'alterou_tipo_enqdto',
                            'alterou_produtor_rural', 'alterou_produtor_rural_exclusivo'
                        ])

                        # Consolidando todas as alterações do mesmo dia em uma única linha
                        df_consolidado = df[df['mantem_linha']].drop(columns=['mantem_linha'])

                        # Ajustando data_fim_vigencia para o dia anterior à próxima alteração
                        df_consolidado['data_fim_vigencia'] = df_consolidado.groupby('numr_inscricao_contrib')['data_inicio_vigencia'].shift(-1) - pd.Timedelta(seconds=1)

                        # Garantindo que não haja valores inconsistentes
                        df_consolidado = df_consolidado[df_consolidado['data_fim_vigencia'] != df_consolidado['data_inicio_vigencia']]

                        # Troca valor nulo da data de vigência final para valor de data máximo
                        df_consolidado['data_fim_vigencia'] = df_consolidado['data_fim_vigencia'].fillna(pd.Timestamp.max)

                        # Elimida linhas com a mesda data de inico
                        df_final = df_consolidado.groupby(['numr_inscricao_contrib', 'data_inicio_vigencia'],as_index=False).last()

                        # Retorna o valor nulo para a data de vigência final
                        df_final.loc[df_final['data_fim_vigencia'] == pd.Timestamp.max, 'data_fim_vigencia'] = pd.NaT

                    obj_retorno = pd.concat([obj_retorno, df_final], axis=0).reset_index(drop=True)

            # obj_retorno.name = 'obj_retorno'
            # baixa_csv(obj_retorno)

            return obj_retorno

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def busca_historico_ruc(self, df, p_data_inicio, p_data_fim) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def busca_historico_ruc."
        loga_mensagem(etapaProcess)

        df_hist_cce = []
        df_hist = []
        df_hist_trab = []

        try:
            i_ref = p_data_inicio.year * 100 + p_data_inicio.month

            db = OraprddwClass()
            df_hist = db.select_historico_contrib(list(set(df['ie'].tolist())))
            # loga_mensagem(f'Busca históricos do RUC - {len(df)} linhas a pesquisar. - {len(df_hist)} retornadas.')
            del db

            if len(df_hist) == 0:
                return []

            df_hist['mes_ref'] = i_ref
            df_hist['codg_uf'] = 'GO'
            df_hist.rename(columns={'codg_municipio_contrib': 'codg_municipio'}, inplace=True)
            df_hist['data_alteracao'] = df_hist['numr_ano_mes_dia_alteracao'].apply(lambda x: pd.Timestamp(datetime.strptime(str(x), '%Y%m%d')))

            df_cnae = self.busca_historico_cnae(df_hist, i_ref)

            df_ssn = self.busca_historico_ssn(df_hist, self.data_inicio_referencia, self.data_fim_referencia)

            df_hist_cnae = self.agrega_historico_cnae_ssn(df_hist, df_cnae, df_ssn)

            df_hist_periodo = self.busca_alteracoes_cad_periodo(df_hist_cnae, self.data_inicio_referencia, self.data_fim_referencia)

            df_cad = self.busca_alteracoes_cad(df_hist_periodo)

            df_cad.name = 'df_cad'
            baixa_csv(df_cad)

            return df_cad

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def busca_alteracoes_cad(self, df) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def busca_alteracoes_cad."
        # loga_mensagem(etapaProcess)

        # Inicializa variáveis de trabalho
        i_inscricao_contrib = 0
        d_data_cad_ant = 0
        d_data_cad_atu = 0
        d_data_fim = pd.NaT
        i_codg_municipio_atu = 0
        i_codg_municipio_ant = 0
        s_codg_uf_atu = 0
        s_codg_uf_ant = 0
        s_tipo_enqdto_fiscal_atu = '0'
        s_tipo_enqdto_fiscal_ant = '0'
        s_stat_cadastro_contrib_atu = '0'
        s_stat_cadastro_contrib_ant = '0'
        s_indi_produtor_rural_atu = ''
        s_indi_produtor_rural_ant = ''
        s_indi_produtor_rural_exclusivo_atu = ''
        s_indi_produtor_rural_exclusivo_ant = ''
        b_1a_inclusao = True
        i = 0
        dfs = []

        try:
            df.name = 'busca_alteracoes_cad'
            baixa_csv(df)

            # PATH = r'D:/SEFAZ/IPM/'
            # df_hist_cnae = sobe_csv(PATH + "df_hist_cnae.csv", "|")

            for index, row in df.iterrows():
                if pd.isna(row['codg_municipio']):
                    i_codg_municipio_atu = 999999999
                else:
                    i_codg_municipio_atu = row['codg_municipio']

                s_codg_uf_atu = row['codg_uf']
                s_tipo_enqdto_fiscal_atu = self.codifica_tipo_enqdto(row['desc_tipo_enqdto'])
                s_stat_cadastro_contrib_atu = self.codifica_situacao_cadastro(row['desc_situacao_cadastro'])
                s_indi_produtor_rural_atu = row['indi_produtor_rural']
                s_indi_produtor_rural_exclusivo_atu = row['indi_produtor_rural_exclusivo']
                d_data_cad_atu = row['data_alteracao']

                if i_inscricao_contrib == int(row['numr_inscricao_contrib']):
                    # Verifica se houve alteração cadastral
                    if i_codg_municipio_atu != i_codg_municipio_ant \
                    or s_stat_cadastro_contrib_atu != s_stat_cadastro_contrib_ant \
                    or s_tipo_enqdto_fiscal_atu != s_tipo_enqdto_fiscal_ant \
                    or s_indi_produtor_rural_atu != s_indi_produtor_rural_ant \
                    or s_indi_produtor_rural_exclusivo_atu != s_indi_produtor_rural_exclusivo_ant:
                        if b_1a_inclusao:
                            d_data_fim = pd.NaT

                        nova_linha = {'numr_inscricao_contrib': i_inscricao_contrib,
                                      'data_inicio_vigencia': d_data_cad_ant,
                                      'data_fim_vigencia': d_data_fim,
                                      'codg_municipio': i_codg_municipio_ant,
                                      'codg_uf': s_codg_uf_ant,
                                      'tipo_enqdto_fiscal': s_tipo_enqdto_fiscal_ant,
                                      'stat_cadastro_contrib': s_stat_cadastro_contrib_ant,
                                      'indi_produtor_rural': s_indi_produtor_rural_ant,
                                      'indi_produtor_rural_exclusivo': s_indi_produtor_rural_exclusivo_ant,
                                      }

                        dfs.append(pd.DataFrame(data=nova_linha, index=[i]))
                        i += 1
                        b_1a_inclusao = False
                        d_data_fim = pd.Timestamp(d_data_cad_ant) + timedelta(seconds=-1)

                else:
                    if i_inscricao_contrib != 0:
                        nova_linha = {'numr_inscricao_contrib': i_inscricao_contrib,
                                      'data_inicio_vigencia': d_data_cad_ant,
                                      'data_fim_vigencia': d_data_fim,
                                      'codg_municipio': i_codg_municipio_ant,
                                      'codg_uf': s_codg_uf_ant,
                                      'tipo_enqdto_fiscal': s_tipo_enqdto_fiscal_ant,
                                      'stat_cadastro_contrib': s_stat_cadastro_contrib_ant,
                                      'indi_produtor_rural': s_indi_produtor_rural_ant,
                                      'indi_produtor_rural_exclusivo': s_indi_produtor_rural_exclusivo_ant,
                                      }
                        dfs.append(pd.DataFrame(data=nova_linha, index=[i]))
                        i += 1
                        b_1a_inclusao = True
                        d_data_fim = pd.NaT

                i_codg_municipio_ant = i_codg_municipio_atu
                s_codg_uf_ant = s_codg_uf_atu
                s_tipo_enqdto_fiscal_ant = s_tipo_enqdto_fiscal_atu
                s_stat_cadastro_contrib_ant = s_stat_cadastro_contrib_atu
                s_indi_produtor_rural_ant = s_indi_produtor_rural_atu
                s_indi_produtor_rural_exclusivo_ant = s_indi_produtor_rural_exclusivo_atu
                d_data_cad_ant = d_data_cad_atu

                i_inscricao_contrib = int(row['numr_inscricao_contrib'])

            # Trata última ocorrência do dataframe, que está em memória
            if i_inscricao_contrib != 0:
                if b_1a_inclusao:
                    d_data_fim = pd.NA

                nova_linha = {'numr_inscricao_contrib': i_inscricao_contrib,
                              'data_inicio_vigencia': d_data_cad_ant,
                              'data_fim_vigencia': d_data_fim,
                              'codg_municipio': i_codg_municipio_ant,
                              'codg_uf': s_codg_uf_ant,
                              'tipo_enqdto_fiscal': s_tipo_enqdto_fiscal_ant,
                              'stat_cadastro_contrib': s_stat_cadastro_contrib_ant,
                              'indi_produtor_rural': s_indi_produtor_rural_ant,
                              'indi_produtor_rural_exclusivo': s_indi_produtor_rural_exclusivo_ant,
                              }
                dfs.append(pd.DataFrame(data=nova_linha, index=[i]))

            # Concatena todos os DataFrames da lista
            if len(dfs) > 0:
                return pd.concat(dfs, ignore_index=True)
            else:
                return []

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def busca_alteracoes_cad_periodo(self, df, p_data_inicio, p_data_fim) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def busca_alteracoes_cad_periodo."
        # loga_mensagem(etapaProcess)

        # Inicializa variáveis de trabalho
        i_inscricao_contrib = 0
        d_data_cad = 0
        i = 0
        dfs = []
        b_inscr = True

        try:
            df_classif = df.sort_values(by=['numr_inscricao_contrib', 'data_alteracao'], ascending=[True, False])

            df_classif.name = 'busca_alteracoes_cad_periodo'
            baixa_csv(df_classif)

            # PATH = r'D:/SEFAZ/IPM/'
            # df_hist_cnae = sobe_csv(PATH + "df_hist_cnae.csv", "|")

            for index, row in df_classif.iterrows():
                if i_inscricao_contrib == int(row['numr_inscricao_contrib']):
                    if b_inscr:
                        if d_data_cad <= p_data_fim:
                            dfs.append(pd.DataFrame(data=nova_linha, index=[i]))
                            i += 1
                            if d_data_cad < p_data_inicio:
                                b_inscr = False
                else:
                    if i_inscricao_contrib != 0:
                        if b_inscr:
                            dfs.append(pd.DataFrame(data=nova_linha, index=[i]))
                            i += 1

                        b_inscr = True

                i_inscricao_contrib = int(row['numr_inscricao_contrib'])
                d_data_cad = row['data_alteracao'].date()

                nova_linha = {'numr_inscricao_contrib': row.numr_inscricao_contrib,
                              'data_alteracao': d_data_cad,
                              'codg_municipio': row.codg_municipio,
                              'codg_uf': row.codg_uf,
                              'desc_tipo_enqdto': row.desc_tipo_enqdto,
                              'desc_situacao_cadastro': row.desc_situacao_cadastro,
                              'indi_produtor_rural': row.indi_produtor_rural,
                              'indi_produtor_rural_exclusivo': row.indi_produtor_rural_exclusivo,
                              }

            # Trata última ocorrência do dataframe, que está em memória
            if b_inscr:
                dfs.append(pd.DataFrame(data=nova_linha, index=[i]))

            # Concatena todos os DataFrames da lista
            if len(dfs) > 0:
                df = pd.concat(dfs, ignore_index=True)
                return pd.DataFrame(df)
            else:
                return []

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def busca_historico_cnae(self, df, p_ref) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def busca_historico_cnae."
        loga_mensagem(etapaProcess)

        try:
            cnae = CNAEClasse()
            loga_mensagem(f'Inicio busca históricos do CNAE - {df.shape[0]} linhas a pesquisar.')
            df_rural = cnae.inclui_informacoes_cnae_rural(list(set(df['numr_inscricao'].tolist())), p_ref)
            loga_mensagem(f'Fim busca históricos do CNAE - {len(df)} linhas a pesquisar. - {len(df_rural)} retornadas.')
            del cnae

            etapaProcess += " - Processo finalizado."
            return df_rural

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def busca_historico_ssn(self, df, p_data_inicio, p_data_fim) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def busca_historico_ssn."
        loga_mensagem(etapaProcess)

        try:
            ssn = SSNClasse()
            loga_mensagem(f'Inicio busca históricos do SSN - {df.shape[0]} linhas a pesquisar.')
            df_ssn = ssn.busca_contrib_ssn_periodo(list(set(df['numr_inscricao'].tolist())), p_data_inicio, p_data_fim)
            loga_mensagem(f'Fim busca históricos do CNAE - {len(df)} linhas a pesquisar. - {len(df_ssn)} retornadas.')
            del ssn

            etapaProcess += " - Processo finalizado."
            return df_ssn

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def agrega_historico_cnae_ssn(self, df_cad, df_cnae, df_ssn):
        etapaProcess = f"class {self.__class__.__name__} - def agrega_historico_cnae_ssn."
        loga_mensagem(etapaProcess)

        try:
            df_cad.name = 'agrega_historico_e_cnae'
            baixa_csv(df_cad)

            loga_mensagem(f'df_cad - {len(df_cad)}, df_cnae - {len(df_cnae)}, df_ssn - {len(df_ssn)}')

            # Mesclar o cadastro com cnae
            if len(df_cnae) > 0:
                df_cnae.rename(columns={'numr_inscricao_contrib': 'numr_inscricao'}, inplace=True)
                df_merge = pd.merge(df_cad, df_cnae,
                                    left_on=['numr_inscricao', 'data_alteracao'],
                                    right_on=['numr_inscricao', 'data_alteracao'],
                                    how='outer')
            else:
                df_merge = df_cad.copy()
                df_merge['indi_produtor_rural'] = 'N'
                df_merge['indi_produtor_rural_exclusivo'] = 'N'

            # Mesclar o resultado com o enquadramento do Simples
            if len(df_ssn) > 0:
                # df_ssn['numr_inscricao'] = df_ssn['numr_inscricao'].astype(object)
                df_merge = pd.merge(df_merge, df_ssn,
                                    left_on=['numr_inscricao', 'data_alteracao'],
                                    right_on=['numr_inscricao', 'data_alteracao'],
                                    how='outer')
            else:
                df_merge['desc_tipo_enqdto'] = 'Normal'

            df_merge.name = 'df_merge'
            baixa_csv(df_merge)

            # Classifica os dados em ordem crescente de data de alteração
            df_classif = df_merge.sort_values(by=['numr_inscricao', 'data_alteracao'],
                                              ascending=[True, True]).copy().reset_index(drop=True)

            # Aplica o forward fill (ffill) por grupo de numr_inscricao
            df_classif[['codg_municipio',
                        'desc_situacao_cadastro',
                        'desc_tipo_enqdto',
                        'mes_ref',
                        'codg_uf',
                        'indi_produtor_rural',
                        'indi_produtor_rural_exclusivo']] = (df_classif.groupby('numr_inscricao')[['codg_municipio',
                                                                                                   'desc_situacao_cadastro',
                                                                                                   'desc_tipo_enqdto',
                                                                                                   'mes_ref',
                                                                                                   'codg_uf',
                                                                                                   'indi_produtor_rural',
                                                                                                   'indi_produtor_rural_exclusivo']].ffill())

            # Ajusta valores para 'indi_produtor_rural' e 'indi_produtor_rural_exclusivo'
            df_classif['indi_produtor_rural'].fillna('N', inplace=True)
            df_classif['indi_produtor_rural_exclusivo'].fillna('N', inplace=True)
            df_classif['desc_tipo_enqdto'].fillna('Normal', inplace=True)

            # Ajuste final de nomenclatura
            df_classif.rename(columns={'numr_inscricao': 'numr_inscricao_contrib'}, inplace=True)

            # # Move os dados dos CNAEs para o RUC
            # i_inscricao = 0
            # s_indi_produtor_rural = ' '
            # s_indi_produtor_rural_exclusivo = ' '
            #
            # # Move os dados do RUC para os CNAEs
            # i_inscricao = 0
            # i_mes_ref = 0
            # i_codg_municipio = 0
            # s_desc_situacao_cadastro = ''
            # s_codg_uf = ''
            # s_desc_tipo_enqdto = None
            #
            # # Classifica os dados em ordem crescente de data de alteração
            # df_classif = df_merge.sort_values(by=['numr_inscricao', 'data_alteracao'], ascending=[True, True]).copy().reset_index(drop=True)
            #
            # for index, row in df_classif.iterrows():
            #     if i_inscricao == int(row['numr_inscricao']):
            #         if pd.isna(row.codg_municipio):
            #             df_classif.loc[index, 'codg_municipio'] = i_codg_municipio
            #         if pd.isna(row.desc_situacao_cadastro):
            #             df_classif.loc[index, 'desc_situacao_cadastro'] = s_desc_situacao_cadastro
            #         if pd.isna(row.desc_tipo_enqdto) or row.desc_tipo_enqdto == None:
            #             df_classif.loc[index, 'desc_tipo_enqdto'] = s_desc_tipo_enqdto
            #         if pd.isna(row.mes_ref):
            #             df_classif.loc[index, 'mes_ref'] = i_mes_ref
            #         if pd.isna(row.codg_uf):
            #             df_classif.loc[index, 'codg_uf'] = s_codg_uf
            #         if pd.isna(row.indi_produtor_rural):
            #             df_classif.loc[index, 'indi_produtor_rural'] = s_indi_produtor_rural
            #         if pd.isna(row.indi_produtor_rural):
            #             df_classif.loc[index, 'indi_produtor_rural_exclusivo'] = s_indi_produtor_rural_exclusivo
            #     else:
            #         if i_inscricao != 0:
            #             if pd.isna(row.codg_municipio):
            #                 df_classif.loc[index, 'codg_municipio'] = i_codg_municipio
            #             if pd.isna(row.desc_situacao_cadastro):
            #                 df_classif.loc[index, 'desc_situacao_cadastro'] = s_desc_situacao_cadastro
            #             if pd.isna(row.desc_tipo_enqdto) or row.desc_tipo_enqdto == None:
            #                 df_classif.loc[index, 'desc_tipo_enqdto'] = s_desc_tipo_enqdto
            #             if pd.isna(row.mes_ref):
            #                 df_classif.loc[index, 'mes_ref'] = i_mes_ref
            #             if pd.isna(row.codg_uf):
            #                 df_classif.loc[index, 'codg_uf'] = s_codg_uf
            #             if pd.isna(row.indi_produtor_rural):
            #                 df_classif.loc[index, 'indi_produtor_rural'] = s_indi_produtor_rural
            #             if pd.isna(row.indi_produtor_rural):
            #                 df_classif.loc[index, 'indi_produtor_rural_exclusivo'] = s_indi_produtor_rural_exclusivo
            #
            #     i_inscricao = int(row['numr_inscricao'])
            #
            #     if not pd.isna(row['codg_municipio']):
            #         i_codg_municipio = row['codg_municipio']
            #     if not pd.isna(row['desc_situacao_cadastro']):
            #         s_desc_situacao_cadastro = row['desc_situacao_cadastro']
            #     if not (pd.isna(row['desc_tipo_enqdto']) or row.desc_tipo_enqdto == None):
            #         s_desc_tipo_enqdto = row['desc_tipo_enqdto']
            #     if not pd.isna(row['mes_ref']):
            #         s_codg_uf = row['mes_ref']
            #     if not pd.isna(row['codg_uf']):
            #         s_codg_uf = row['codg_uf']
            #     if not pd.isna(row['indi_produtor_rural']):
            #         s_indi_produtor_rural = row['indi_produtor_rural']
            #     if not pd.isna(row['indi_produtor_rural_exclusivo']):
            #         s_indi_produtor_rural_exclusivo = row['indi_produtor_rural_exclusivo']
            #
            #     if index == 0:
            #         df_classif.loc[index, 'codg_municipio'] = i_codg_municipio
            #         df_classif.loc[index, 'desc_situacao_cadastro'] = s_desc_situacao_cadastro
            #         df_classif.loc[index, 'desc_tipo_enqdto'] = s_desc_tipo_enqdto
            #         df_classif.loc[index, 'mes_ref'] = i_mes_ref
            #         df_classif.loc[index, 'codg_uf'] = s_codg_uf
            #         df_classif.loc[index, 'indi_produtor_rural'] = s_indi_produtor_rural
            #         df_classif.loc[index, 'indi_produtor_rural_exclusivo'] = s_indi_produtor_rural_exclusivo
            #
            # df_classif.loc[index, 'codg_municipio'] = i_codg_municipio
            # df_classif.loc[index, 'desc_situacao_cadastro'] = s_desc_situacao_cadastro
            # df_classif.loc[index, 'desc_tipo_enqdto'] = s_desc_tipo_enqdto
            # df_classif.loc[index, 'mes_ref'] = i_mes_ref
            # df_classif.loc[index, 'codg_uf'] = s_codg_uf
            # df_classif.loc[index, 'indi_produtor_rural'] = s_indi_produtor_rural
            # df_classif.loc[index, 'indi_produtor_rural_exclusivo'] = s_indi_produtor_rural_exclusivo
            #
            # df_classif['indi_produtor_rural'].replace({' ': 'N'}, inplace=True)
            # df_classif['indi_produtor_rural_exclusivo'].replace({' ': 'N'}, inplace=True)
            #
            # df_classif.rename(columns={'numr_inscricao': 'numr_inscricao_contrib'}, inplace=True)
            #
            # return df_classif

            return df_classif

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def registra_processamento(self, procesm, etapa):
        etapaProcess = f"class {self.__class__.__name__} - def registra_processamento - {etapa}"
        # loga_mensagem(etapaProcess)

        try:
            loga_mensagem(etapa)
            db = ProcesmClasse()
            procesm.desc_observacao_procesm_indice = etapa
            db.atualizar_situacao_processamento(procesm)
            del db

            return None

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    #  Função para carregar informações do CCE a partir dos dados da NF-e individual #
    # def inclui_informacoes_cce_por_referencia(self, pInscricao, pReferencia):
    #     etapaProcess = f"class {self.__class__.__name__} - def inclui_informacoes_cce_por_referencia: {pInscricao} - {pReferencia}"
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         linhasAtualizadas = 0
    #         if pInscricao != None:
    #             dbOraprddw = OraprddwClass()
    #             df_ret = dbOraprddw.select_historico_contrib_referencia(pInscricao, pReferencia)
    #             del dbOraprddw
    #
    #             dbMySQL = Mysql()
    #             if df_ret.shape[0] > 0:
    #                 linhasAtualizadas = dbMySQL.insert_cadastro_cce(pReferencia,
    #                                                                 df_ret['numr_inscricao'].iloc[0],
    #                                                                 df_ret['codg_municipio_contrib'].iloc[0],
    #                                                                 df_ret['codg_tipo_enqdto'].iloc[0],
    #                                                                 df_ret['codg_situacao_cadastro'].iloc[0],
    #                                                                 0)
    #             else:
    #                 linhasAtualizadas = dbMySQL.update_cadastro_cce(pReferencia, pInscricao, 999999, 0, 0, 0)
    #             del dbMySQL
    #         else:
    #             loga_mensagem('Inscrição não Informada - ' + pInscricao)
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    #     return linhasAtualizadas
    #
    #  Funcção para carregar informações do CCE a partir dos dados da NF-e individual #
    # def inclui_informacoes_cce_1(self, df):
    #     etapaProcess = f"class {self.__class__.__name__} - def inclui_informacoes_cce_1."
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
    #                         mes_ref = inicio_referencia
    #                         while mes_ref <= fim_referencia:
    #                             # Pesquisa dataframe de retorno
    #                             for index, row in df_ret.iterrows():
    #                                 if int(row['anomesref']) < mes_ref:
    #                                     nova_linha = {'numr_inscricao': [row['numr_inscricao']],
    #                                                   'mes_ref': [mes_ref],
    #                                                   'codg_municipio_contrib': [row['codg_municipio_contrib']],
    #                                                   'codg_tipo_enqdto': [row['codg_tipo_enqdto']],
    #                                                   'codg_situacao_cadastro': [row['codg_situacao_cadastro']]}
    #                                     dfs.append(pd.DataFrame(data=nova_linha))
    #                                     break
    #                             mes_ref += 1
    #                     else:
    #                         loga_mensagem('Inscrição não encontrada - ' + row[0])
    #                 else:
    #                     loga_mensagem('Inscrição de outro Estado - ' + row[0])
    #             else:
    #                 loga_mensagem('Inscrição não Informada - ' + row[0])
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
    # def seleciona_inscricoes_unica(self, df):
    #     etapaProcess = f"class {self.__class__.__name__} - def seleciona_inscricoes_unica."
    #     # loga_mensagem(etapaProcess)
    #
    #     cce_pesq = df.sort_values(by=['ie'])
    #     cce_pesq['ie'] = cce_pesq['ie'].astype(str)
    #
    #     return cce_pesq['ie'].drop_duplicates()
