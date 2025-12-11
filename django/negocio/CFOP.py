import pandas as pd

from funcoes.utilitarios import loga_mensagem, loga_mensagem_erro
from persistencia.Oraprd import Oraprd


class CFOPClasse:
    etapaProcess = f"class {__name__} - class CFOPClasse."
    # loga_mensagem(etapaProcess)

    def __init__(self):
        var = None

    #  Função para carregar CFOP nas informações da NF-e  #
    def busca_cfop_nfe(self, p_data_inicio, p_data_fim, p_tipo_documento):
        etapaProcess = f"class {self.__class__.__name__} - def busca_cfop_nfe para o período de {p_data_inicio} a {p_data_fim}."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_cfop = db.select_cfop_ipm(p_data_inicio, p_data_fim, p_tipo_documento)
            del db
            return df_cfop

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def carregar_cfops(self, p_data_ref, df, p_tipo_doc) -> str:
        etapaProcess = f"class {self.__class__.__name__} - def carregar_cfops para a referencia {p_data_ref}."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            # linhas_excluidas = db.delete_cfop_partct(p_ano_ref)
            # etapaProcess += f' - Exclusão de CFOPs existentes: {linhas_excluidas}'
            # loga_mensagem(etapaProcess)

            df['data_inicio_vigencia'] = pd.to_datetime(p_data_ref, format='%Y%m%d')
            df['codg_tipo_doc_partct_calc'] = p_tipo_doc

            linhas_incluidas = db.insert_cfop_partct(df)
            etapaProcess += f' - Inclusão de CFOPs: {linhas_incluidas}'
            loga_mensagem(etapaProcess)
            del db

            return etapaProcess

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def acerto_cfop(self, p_ano_ref, p_tipo_doc) -> str:
        etapaProcess = f"class {self.__class__.__name__} - def acerto_cfop."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            linhas_excluidas = db.delete_cfop_partct_acerto(p_ano_ref, p_tipo_doc)
            etapaProcess += f' - Exclusão de CFOPs existentes: {linhas_excluidas}'
            loga_mensagem(etapaProcess)

            return linhas_excluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise
