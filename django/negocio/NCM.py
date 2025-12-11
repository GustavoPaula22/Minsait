import pandas as pd

from funcoes.utilitarios import loga_mensagem, loga_mensagem_erro
from persistencia.Oraprd import Oraprd


class NCMClasse:
    etapaProcess = f"class {__name__} - class NCMClasse."
    # loga_mensagem(etapaProcess)

    def __init__(self):
        var = None

    def busca_ncm_produtores_rurais(self, p_data_inicio, p_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def busca_ncm_produtores_rurais para o período de {p_data_inicio} a {p_data_fim}."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_ncm_rural = db.select_ncm_prod_rural(p_data_inicio, p_data_fim)
            df_ncm = db.select_gen_ncm_id(df_ncm_rural['id_produto_ncm'].tolist())
            del db
            return df_ncm

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def busca_ncm_doc_fiscal(self, ncms, p_data_inicio, p_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def busca_ncm_produtores_rurais para o período de {p_data_inicio} a {p_data_fim}."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_ncm = db.select_gen_ncm_cod(ncms, p_data_inicio, p_data_fim)
            del db
            return df_ncm

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def carga_inicial_ncm_produtores_rurais(self, ncms, p_data_ref):
        etapaProcess = f"class {self.__class__.__name__} - def carga_inicial_ncm_produtores_rurais. Carga inicial da tabela de NCMs para Produtores Rurais a partir de arquivo .csv"
        # loga_mensagem(etapaProcess)

        try:
            # Lê a tabela GEN_PRODUTO_NCM para obter o ID do NCM
            db = Oraprd()
            df_ncm = db.select_gen_ncm_cod(ncms, p_data_ref, None)
            etapaProcess += f' - {len(df_ncm)} IDs lidos.'
            loga_mensagem(etapaProcess)

            # Exclui toda a tabela de NCMs de produtores rurais
            linhas_excluidas = db.delete_ncm_rural()
            etapaProcess += f' - {linhas_excluidas} excluídas.'

            # Inclui a tabela de NCMs de produtores rurais
            df_ncm['data_inicio_vigencia'] = p_data_ref
            linhas_incluidas = db.insert_ncm_rural(df_ncm)
            etapaProcess += f' - {linhas_incluidas} incluídos na tabela.'
            loga_mensagem(etapaProcess)
            del db

            etapaProcess += ' - Processo finalizado.'
            loga_mensagem(etapaProcess)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)

        return etapaProcess


