import os

import pandas as pd

from funcoes.constantes import EnumTipoDocumento, EnumMotivoExclusao
from funcoes.utilitarios import loga_mensagem, loga_mensagem_erro, baixa_csv
from negocio.DocPartct import DocPartctClasse
from negocio.GEN import GenClass
from persistencia.OraProd import oraProd
from persistencia.Oracle import Oracle
from persistencia.Oraprd import Oraprd


class ItemDocClasse:
    etapaProcess = f"class {__name__} - class ItemDocClasse."
    # loga_mensagem(etapaProcess)

    def __init__(self):
        var = None

    def grava_item_doc(self, df) -> int:
        etapaProcess = f"class {self.__class__.__name__} - def grava_item_doc."
        # loga_mensagem(etapaProcess)

        try:
            # # db = Oraprd()  # Persistência utilizando o Django
            # db = oraProd.create_instance()  # Persistência utilizando o oracledb
            # linhas_incluidas = db.insert_item_doc(df)
            # del db
            # return linhas_incluidas

            db = Oracle()
            linhas_incluidas = db.insert_item_documento(df)
            del db
            return linhas_incluidas

        except Exception as err:
            df.name = 'df_item'
            baixa_csv(df)
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def grava_item_doc_efd(self, df) -> int:
        etapaProcess = f"class {self.__class__.__name__} - def grava_item_doc_efd."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()  # Persistência utilizando o Django
            # db = oraProd.create_instance()  # Persistência utilizando o oracledb
            linhas_incluidas = db.insert_item_doc_efd(df)
            del db
            return linhas_incluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def exclui_item_documento(self, p_tipo_documento, p_data_inicio, p_data_fim) -> int:
        etapaProcess = f"class {self.__class__.__name__} - def exclui_item_documento - Tipo de Documento: {p_tipo_documento} - Periodo: {p_data_inicio} a {p_data_fim}."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            linhas_excluidas = db.delete_item_doc(p_data_inicio, p_data_fim, p_tipo_documento)
            del db

            return linhas_excluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def exclui_item_documento_etapas(self, p_tipo_documento, p_data_inicio, p_data_fim) -> int:
        etapaProcess = f"class {self.__class__.__name__} - def exclui_item_documento_etapas - Tipo de Documento: {p_tipo_documento} - Periodo: {p_data_inicio} a {p_data_fim}."
        # loga_mensagem(etapaProcess)

        linhas_excluidas = 0

        try:
            db = DocPartctClasse()
            df_doc_partct = db.carrega_doc_partct_codg(p_tipo_documento, p_data_inicio, p_data_fim)
            del db

            if len(df_doc_partct) > 0:
                db = Oraprd()
                linhas_excluidas = db.delete_item_doc_docs(p_tipo_documento, list(set(df_doc_partct['codg_documento_partct_calculo'].tolist())))
                del db

            return linhas_excluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def exclui_item_documento_efd(self, df) -> int:
        etapaProcess = f"class {self.__class__.__name__} - def exclui_item_documento_efd."
        # loga_mensagem(etapaProcess)

        linhas_excluidas = 0
        i_tipo_doc_efd = EnumTipoDocumento.EFD.value

        try:
            refs = df['numr_ref_arquivo'].drop_duplicates()

            db = Oraprd()
            for ref in refs:
                filtro = (df['numr_ref_arquivo'] == ref)
                inscrs = df.loc[filtro, 'ie_entrada'].drop_duplicates().tolist()
                linhas_excluidas += db.delete_item_doc_efd(inscrs, ref, i_tipo_doc_efd)

            del db
            return linhas_excluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def lista_item_doc_chave(self, p_inicio_referencia, p_fim_referencia, p_tipo_documento, p_chave):
        etapaProcess = f"class {self.__class__.__name__} - def lista_item_doc_chave."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_itens = db.select_item_doc(p_inicio_referencia, p_fim_referencia, p_tipo_documento, 'ChaveEletr', None, None, None, None, p_chave)
            del db

            return df_itens.to_dict('records')

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def lista_item_doc_inscricao(self, p_inicio_referencia, p_fim_referencia, p_tipo_documento, p_ie_entrada, p_ie_saida):
        etapaProcess = f"class {self.__class__.__name__} - def lista_doc_nao_partct_inscricao - {p_tipo_documento} - {p_ie_entrada} - {p_ie_saida}"
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_itens = db.select_item_doc(p_inicio_referencia, p_fim_referencia, p_tipo_documento, 'Inscricao', None, None, p_ie_entrada, p_ie_saida, None)
            return df_itens.to_dict('records')

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def lista_item_doc_periodo(self, p_inicio_referencia, p_fim_referencia, p_tipo_documento, p_data_inicio, p_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def lista_item_doc_periodo."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_itens = db.select_item_doc(p_inicio_referencia, p_fim_referencia, p_tipo_documento, 'Periodo', p_data_inicio, p_data_fim, None, None, None)
            del db

            return df_itens.to_dict('records')

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def lista_item_doc(self, p_inicio_referencia, p_fim_referencia, p_tipo_documento, p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida):
        etapaProcess = f"class {self.__class__.__name__} - def lista_item_doc."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_itens = db.select_item_doc(p_inicio_referencia, p_fim_referencia, p_tipo_documento, 'Periodo/Inscricao', p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida, None)
            del db

            return df_itens.to_dict('records')

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def carrega_item_doc(self, p_docs, p_tipo_doc):
        etapaProcess = f"class {self.__class__.__name__} - def carrega_item_doc."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_itens = db.select_item_doc_acerto(p_docs, p_tipo_doc)
            del db

            if len(df_itens) > 0:
                return df_itens
            else:
                return []

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def carrega_item_excluido_nf(self, p_codigo_exclusao, p_data_inicio, p_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def carrega_item_excluido."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_itens = db.select_item_excluido_nf(p_codigo_exclusao, p_data_inicio, p_data_fim)
            del db

            return df_itens

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def altera_item_partct(self, df):
        etapaProcess = f"class {self.__class__.__name__} - def altera_item_partct."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            linhas_alteradas = db.update_item_doc_valr_excl(df)
            del db

            return linhas_alteradas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def limpa_motivo_excl_item(self, p_itens, p_tipo_documento):
        etapaProcess = f"class {self.__class__.__name__} - def limpa_motivo_excl_item. Tipo Doc - {p_tipo_documento}"
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            linhas_alteradas = db.update_item_partct_limpa_excl(p_itens, p_tipo_documento)
            del db

            return linhas_alteradas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def verifica_linhas_gravadas(self, p_id_procesm):
        etapaProcess = f"class {__name__} - def verifica_linhas_gravadas."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            linhas_gravadas = db.select_item_doc_id_procesm(p_id_procesm)
            del db

            return linhas_gravadas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise
