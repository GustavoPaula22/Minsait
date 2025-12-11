import os

import pandas as pd

from funcoes.constantes import EnumTipoDocumento
from funcoes.utilitarios import loga_mensagem, loga_mensagem_erro
from negocio.GEN import GenClass
from persistencia.OraProd import oraProd
from persistencia.Oraprd import Oraprd


class DocNaoPartctClasse:
    etapaProcess = f"class {__name__} - class DocNaoPartctClasse."
    # loga_mensagem(etapaProcess)

    def __init__(self):
        self.inicio_referencia = os.getenv('INICIO_REFERENCIA')
        self.fim_referencia = os.getenv('FIM_REFERENCIA')

    def grava_doc_nao_participante(self, df) -> int:
        etapaProcess = f"class {self.__class__.__name__} - def grava_doc_nao_participante."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()  # Persistência utilizando o Django
            # db = oraProd.create_instance()  # Persistência utilizando o oracledb
            linhas_incluidas = db.insert_doc_nao_partct(df)
            del db
            return linhas_incluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def exclui_doc_nao_participante(self, p_tipo_documento, p_data_inicio, p_data_fim) -> int:
        etapaProcess = f"class {self.__class__.__name__} - def exclui_doc_nao_participante - Tipo de Documento: {p_tipo_documento} - Periodo: {p_data_inicio} a {p_data_fim}."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            linhas_excluidas = db.delete_doc_nao_partct(p_data_inicio, p_data_fim, p_tipo_documento)
            del db
            return linhas_excluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def lista_doc_nao_partct_chave(self, p_inicio_referencia, p_fim_referencia, p_tipo_documento, p_chave):
        etapaProcess = f"class {self.__class__.__name__} - def lista_doc_nao_partct_chave."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_docs = db.select_doc_nao_partct(p_inicio_referencia, p_fim_referencia, p_tipo_documento, 'ChaveEletr', None, None, None, None, p_chave)
            del db

            return df_docs.to_dict('records')

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def lista_doc_nao_partct_inscricao(self, p_inicio_referencia, p_fim_referencia, p_tipo_documento, p_ie_entrada, p_ie_saida):
        etapaProcess = f"class {self.__class__.__name__} - def lista_doc_nao_partct_inscricao - {p_tipo_documento} - {p_ie_entrada} - {p_ie_saida}"
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_docs = db.select_doc_nao_partct(p_inicio_referencia, p_fim_referencia, p_tipo_documento, 'Inscricao', None, None, p_ie_entrada, p_ie_saida, None)
            return df_docs.to_dict('records')

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def lista_doc_nao_partct_periodo(self, p_inicio_referencia, p_fim_referencia, p_tipo_documento, p_data_inicio, p_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def lista_doc_nao_partct_periodo."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_docs = db.select_doc_nao_partct(p_inicio_referencia, p_fim_referencia, p_tipo_documento, 'Periodo', p_data_inicio, p_data_fim, None, None, None)
            del db

            return df_docs.to_dict('records')

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def lista_doc_nao_partct(self, p_inicio_referencia, p_fim_referencia, p_tipo_documento, p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida):
        etapaProcess = f"class {self.__class__.__name__} - def lista_doc_nao_partct."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_docs = db.select_doc_nao_partct(p_inicio_referencia, p_fim_referencia, p_tipo_documento, 'Periodo/Inscricao', p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida, None)
            del db

            return df_docs.to_dict('records')

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

