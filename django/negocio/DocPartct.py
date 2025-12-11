import os
from datetime import datetime, timedelta

import pandas as pd

from funcoes.constantes import EnumTipoDocumento
from funcoes.utilitarios import loga_mensagem, loga_mensagem_erro, dividir_dia_em_horas, baixa_csv
from negocio.GEN import GenClass
from persistencia.OraProd import oraProd
from persistencia.Oraprd import Oraprd
from persistencia.Oraprodx9 import Oraprodx9


class DocPartctClasse:
    etapaProcess = f"class {__name__} - class DocPartctClasse."
    # loga_mensagem(etapaProcess)

    def __init__(self):
        var = None

    def grava_doc_participante(self, df):
        etapaProcess = f"class {self.__class__.__name__} - def grava_doc_participante."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()                    # Persistência utilizando o Django
            # db = oraProd.create_instance()     # Persistência utilizando o oracledb
            linhas_incluidas = db.insert_doc_partct(df)
            del db
            return linhas_incluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def grava_doc_participante_efd(self, df):
        etapaProcess = f"class {self.__class__.__name__} - def grava_doc_participante_efd."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()                    # Persistência utilizando o Django
            # db = oraProd.create_instance()  # Persistência utilizando o oracledb
            linhas_incluidas = db.insert_doc_partct_efd(df)
            del db
            return linhas_incluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def exclui_doc_participante(self, p_tipo_documento, p_data_inicio, p_data_fim) -> int:
        etapaProcess = f"class {self.__class__.__name__} - def exclui_doc_participante - Tipo de Documento: {p_tipo_documento} - Periodo: {p_data_inicio} a {p_data_fim}."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            linhas_excluidas = db.delete_doc_partct(p_data_inicio, p_data_fim, p_tipo_documento)
            del db
            return linhas_excluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def exclui_doc_participante_efd(self, df):
        etapaProcess = f"class {self.__class__.__name__} - def exclui_doc_participante_efd"
        # loga_mensagem(etapaProcess)

        linhas_excluidas = 0
        i_tipo_doc_efd = EnumTipoDocumento.EFD.value

        try:
            # A exclusão dos dados de processamentos anteriores é feito separando todas as inscrições por referencia
            # A rotina de exclusão recebe uma lista de inscrições da mesma referencia

            refs = df['numr_ref_arquivo'].drop_duplicates()

            db = Oraprd()
            for ref in refs:
                filtro = (df['numr_ref_arquivo'] == ref)
                inscrs = df.loc[filtro, 'ie_entrada'].drop_duplicates().tolist()
                linhas_excluidas += db.delete_doc_partct_efd(inscrs, ref, i_tipo_doc_efd)

            del db
            return linhas_excluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def lista_doc_partct_chave(self, p_inicio_referencia, p_fim_referencia, p_tipo_documento, p_chave):
        etapaProcess = f"class {self.__class__.__name__} - def lista_doc_partct_chave."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_docs = db.select_doc_partct(p_inicio_referencia, p_fim_referencia, p_tipo_documento, 'ChaveEletr', None, None, None, None, p_chave)

            if len(df_docs) > 0:
                df_municip = self.carrega_municipio_nfe(df_docs)
                df_mono = self.carrega_icms_nomofasico(df_municip)
                return df_mono.to_dict('records')

            del db

            return []

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def lista_doc_partct_inscricao(self, p_inicio_referencia, p_fim_referencia, p_tipo_documento, p_ie_entrada, p_ie_saida):
        etapaProcess = f"class {self.__class__.__name__} - def lista_doc_partct_inscricao - {p_ie_entrada} - {p_ie_saida}"
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_docs = db.select_doc_partct(p_inicio_referencia, p_fim_referencia, p_tipo_documento, 'Inscricao', None, None, p_ie_entrada, p_ie_saida, None)

            if len(df_docs) > 0:
                df_municip = self.carrega_municipio_nfe(df_docs)
                df_mono = self.carrega_icms_nomofasico(df_municip)
                del db
                return df_mono.to_dict('records')
            else:
                del db
                return []

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def lista_doc_partct_periodo(self, p_inicio_referencia, p_fim_referencia, p_tipo_documento, p_data_inicio, p_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def lista_doc_partct_periodo."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_docs = db.select_doc_partct(p_inicio_referencia, p_fim_referencia, p_tipo_documento, 'Periodo', p_data_inicio, p_data_fim, None, None, None)

            if len(df_docs) > 0:
                df_municip = self.carrega_municipio_nfe(df_docs)
                df_mono = self.carrega_icms_nomofasico(df_municip)

                return df_mono.to_dict('records')

            del db

            return []

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def lista_doc_partct(self, p_inicio_referencia, p_fim_referencia, p_tipo_documento, p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida):
        etapaProcess = f"class {self.__class__.__name__} - def lista_doc_partct."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_docs = db.select_doc_partct(p_inicio_referencia, p_fim_referencia, p_tipo_documento, 'Periodo/Inscricao', p_inicio_referencia, p_fim_referencia, p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida, None)

            if len(df_docs) > 0:
                df_municip = self.carrega_municipio_nfe(df_docs)
                df_mono = self.carrega_icms_nomofasico(df_municip)
                return df_mono.to_dict('records')

            del db

            return []

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def lista_nfe_simples(self, p_tipo_consulta, p_data_inicio, p_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def lista_nfe_simples - Periodo de {p_data_inicio} a {p_data_fim} - Tipo {p_tipo_consulta}."
        loga_mensagem(etapaProcess)

        dfs = []
        i_qdade_process = 12

        try:
            d_data_inicio = datetime.strptime(p_data_inicio, '%Y%m%d').date()
            d_data_fim = datetime.strptime(p_data_fim, '%Y%m%d').date()

            db = Oraprd()
            d_data_atual = d_data_inicio

            while d_data_atual <= d_data_fim:
                etapaProcess = f"Processando a data {d_data_atual}"
                loga_mensagem(etapaProcess)
                df_data_proc = dividir_dia_em_horas(d_data_atual.strftime('%Y-%m-%d %H:%M:%S'), i_qdade_process)

                if df_data_proc is not None:
                    for x, hora in df_data_proc.iterrows():
                        df = db.select_nfe_simples(p_tipo_consulta, hora.dDataInicial, hora.dDataFinal)
                        dfs.append(df)

                    d_data_atual += timedelta(days=1)

                else:
                    etapaProcess = f'Período para carga de NF-e Recebida com problemas. Período informado: {p_data_inicio} a {p_data_fim}.'
                    loga_mensagem_erro(etapaProcess)
                    return etapaProcess

            del db

            df_resultado = pd.concat(dfs, ignore_index=True)
            etapaProcess += f' Processo finalizado. Geradas {len(df_resultado)} linhas.'
            loga_mensagem(etapaProcess)

            if len(df_resultado) > 0:
                df_resultado.name = 'df_resultado'
                baixa_csv(df_resultado)

            return etapaProcess

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def carrega_doc_partct_acerto_cfop(self, p_tipo_documento, p_data_inicio, p_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def carrega_doc_partct_acerto_cfop."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_docs = db.select_doc_partct_acerto_cfop(p_tipo_documento, p_data_inicio, p_data_fim)
            del db

            if len(df_docs) > 0:
                return df_docs
            else:
                return []

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def carrega_doc_partct_acerto_cad(self, p_tipo_doc, p_data_inicio, p_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def carrega_doc_partct_acerto_cad."
        loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_docs = db.select_doc_partct_acerto_cad(p_tipo_doc, p_data_inicio, p_data_fim)
            del db

            return df_docs

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def carrega_doc_partct_codg(self, p_tipo_documento, p_data_inicio, p_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def carrega_doc_partct_codg."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_docs = db.select_doc_partct_codg(p_tipo_documento, p_data_inicio, p_data_fim)
            del db

            if len(df_docs) > 0:
                return df_docs
            else:
                return []

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def altera_doc_partct(self, df):
        etapaProcess = f"class {self.__class__.__name__} - def altera_doc_partct."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            linhas_alteradas = db.update_doc_partct_valr_excl(df)
            del db

            return linhas_alteradas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def altera_doc_partct_cad(self, df):
        etapaProcess = f"class {self.__class__.__name__} - def altera_doc_partct_cad."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            colunas_doc = ['codg_documento_partct_calculo',
                           'codg_tipo_doc_partct_calc',
                           'valr_adicionado_operacao',
                           'codg_motivo_exclusao_calculo',
                           'indi_aprop',
                           'id_procesm_indice',
                           'id_contrib_ipm_saida',
                           'id_contrib_ipm_entrada']

            linhas_alteradas = db.update_doc_partct_cad(df[colunas_doc])
            del db

            return linhas_alteradas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def limpa_motivo_excl_sem_item_partct(self, p_tipo_documento, p_data_inicio, p_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def limpa_motivo_excl_sem_item_partct. Tipo Doc - {p_tipo_documento} - Periodo de {p_data_inicio} a {p_data_fim}."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            linhas_alteradas = db.update_doc_partct_limpa_excl_item_n_partct(p_tipo_documento, p_data_inicio, p_data_fim)
            del db

            return linhas_alteradas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def limpa_motivo_excl_doc(self, p_docs, p_tipo_documento):
        etapaProcess = f"class {self.__class__.__name__} - def limpa_motivo_excl_doc. Tipo Doc - {p_tipo_documento}"
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            linhas_alteradas = db.update_doc_partct_limpa_excl(p_docs, p_tipo_documento)
            del db

            return linhas_alteradas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def carrega_icms_nomofasico(self, df):
        etapaProcess = f"class {__name__} - def carrega_icms_nomofasico."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            itens = df[['id_item_nota_fiscal']].stack().unique()  # Concatena identificadores de itens de nota
            df_mono = db.select_icms_monofasico(itens.tolist())
            del db

            if len(df_mono) > 0:
                obj_retorno = df.merge(df_mono,
                                       left_on=['id_item_nota_fiscal'],
                                       right_on=['id_item_nota_fiscal'],
                                       how='left',
                                       )
                obj_retorno.drop(columns=['id_item_nota_fiscal'], inplace=True)
                obj_retorno.rename(columns={'valr_icms_monofasico_retido': 'valmonofasico'}, inplace=True)

                return obj_retorno

            else:
                df.drop(columns=['id_item_nota_fiscal'], inplace=True)
                df['valmonofasico'] = 0.0
                return df

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def carrega_municipio_nfe(self, df) -> pd.DataFrame:
        etapaProcess = f"class {__name__} - def carrega_municipio_nfe."
        # loga_mensagem(etapaProcess)

        try:
            gen = GenClass()
            municipios = df[['municgerador', 'municdestino']].stack().unique()  # Concatena municípios de gerador e destino
            df_municip = gen.carrega_municipio_ibge(municipios.tolist())
            del gen

            if len(df_municip) > 0:
                df_municip['codg_ibge'] = df_municip['codg_ibge'].astype(object)
                obj_retorno = df.merge(df_municip,  # Município de Entrada do GEN
                                       left_on=['municgerador'],
                                       right_on=['codg_ibge'],
                                       how='left',
                                       )
                obj_retorno.drop(columns=['codg_ibge',
                                          'municgerador',
                                          ], inplace=True)
                obj_retorno.rename(columns={'codg_municipio': 'municgerador'}, inplace=True)

                obj_retorno = obj_retorno.merge(df_municip,  # Município de Saída do GEN
                                                left_on=['municdestino'],
                                                right_on=['codg_ibge'],
                                                how='left',
                                                )
                obj_retorno.drop(columns=['codg_ibge',
                                          'municdestino',
                                          ], inplace=True)
                obj_retorno.rename(columns={'codg_municipio': 'municdestino'}, inplace=True)
                return obj_retorno

            else:
                df['municgerador'] = None
                df['municdestino'] = None
                return df

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def verifica_linhas_gravadas(self, p_id_procesm):
        etapaProcess = f"class {__name__} - def verifica_linhas_gravadas."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            linhas_gravadas = db.select_doc_partct_id_procesm(p_id_procesm)
            del db

            loga_mensagem(f"Retornadas {linhas_gravadas} da pesquisa")

            return linhas_gravadas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

