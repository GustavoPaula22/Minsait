import pandas as pd
from funcoes.constantes import EnumParametros
# from persistencia.Mysql import *
from funcoes.utilitarios import loga_mensagem_erro
from persistencia.Oraprd import Oraprd
from polls.models import Parametro


class ParamClasse:
    etapaProcess = f"class {__name__} - class class ParamClasse."
    # loga_mensagem(etapaProcess)

    def __init__(self):
        etapaProcess = f"class {__name__} - class class ParamClasse.__init__."
        self.valr_parametro = None
        self.nome_parametro = None

        try:
            self.db = Oraprd()

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def busca_valor_parametro(self, p_nome_param):
        etapaProcess = f"class {self.__class__.__name__} - def busca_valor_parametro - {p_nome_param}"
        # loga_mensagem(etapaProcess)

        try:
            parametro = self.db.select_parametro(p_nome_param)
            return parametro[0].desc_parametro_ipm

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def busca_parametro(self, nome_parametro) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def busca_parametro - {nome_parametro}"
        # loga_mensagem(etapaProcess)

        try:
            obj_retorno = self.db.select_parametro(nome_parametro)
            return obj_retorno

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def atualizar_parametro(self, p_nome_param, p_valr_param):
        etapaProcess = f"class {self.__class__.__name__} - def atualizar_parametro: {p_nome_param} - {p_valr_param}"
        # loga_mensagem(etapaProcess)

        try:
            self.db.update_parametro(p_nome_param, p_valr_param)
            etapaProcess += f" - Alterado o valor do parametro {p_nome_param} com o valor = {p_valr_param}"
            # loga_mensagem(etapaProcess)

            return None

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def cria_novo_parametro(self, p_nome_param, p_valr_param):
        etapaProcess = f"class {self.__class__.__name__} - def cria_novo_parametro: {p_nome_param} - {p_valr_param}"
        # loga_mensagem(etapaProcess)

        try:
            self.db.cria_novo_parametro(p_nome_param, p_valr_param)
            etapaProcess += f" - Criado novo parametro {p_nome_param} com o valor = {p_valr_param}"
            # loga_mensagem(etapaProcess)

            return None

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def carregar_parametros(self):
        etapaProcess = f"class {self.__class__.__name__} - def carregar_parametros"
        # loga_mensagem(etapaProcess)

        try:
            parametros = [Parametro(nome_parametro_ipm=EnumParametros.atrasoProcessNFe.value, desc_parametro_ipm='60'),
                          Parametro(nome_parametro_ipm=EnumParametros.periodProcessNFe.value, desc_parametro_ipm='60'),

                          Parametro(nome_parametro_ipm=EnumParametros.ultimaCargaBPe.value, desc_parametro_ipm='2025-11-17 23:59:59'),
                          Parametro(nome_parametro_ipm=EnumParametros.ultimaCargaConv115.value, desc_parametro_ipm='2023-12-31 23:59:59'),
                          Parametro(nome_parametro_ipm=EnumParametros.ultimaCargaCTe.value, desc_parametro_ipm='2024-02-05 23:59:59'),
                          Parametro(nome_parametro_ipm=EnumParametros.ultimaCargaEFD.value, desc_parametro_ipm='2024-01-31 23:59:59'),
                          Parametro(nome_parametro_ipm=EnumParametros.ultimaCargaMEI.value,desc_parametro_ipm='2022-12-31 23:59:59'),
                          Parametro(nome_parametro_ipm=EnumParametros.ultimaCargaNFA.value, desc_parametro_ipm='2024-01-22 23:59:59'),
                          Parametro(nome_parametro_ipm=EnumParametros.ultimaCargaNFCe.value, desc_parametro_ipm='2023-12-23 23:59:59'),
                          Parametro(nome_parametro_ipm=EnumParametros.ultimaCargaNFe.value, desc_parametro_ipm='2024-01-22 23:59:59'),
                          Parametro(nome_parametro_ipm=EnumParametros.ultimaCargaNFeRecebida.value, desc_parametro_ipm='2023-12-31 23:59:59'),
                          Parametro(nome_parametro_ipm=EnumParametros.ultimaCargaNF3e.value, desc_parametro_ipm='2024-02-08 23:59:59'),
                          Parametro(nome_parametro_ipm=EnumParametros.ultimaCargaAutoInfracao.value, desc_parametro_ipm='2022-12-31 23:59:59'),
                          Parametro(nome_parametro_ipm=EnumParametros.ultimaCargaSimples.value,desc_parametro_ipm='2022-12-31 23:59:59'),
                          Parametro(nome_parametro_ipm=EnumParametros.ultimaCargaMEI.value,desc_parametro_ipm='2022-12-31 23:59:59'),
                          Parametro(nome_parametro_ipm=EnumParametros.ultimaCargaOpcaoSimplesSIMEI.value,desc_parametro_ipm='2025-02-12 23:59:59'),

                          Parametro(nome_parametro_ipm=EnumParametros.ultimoAcertoCFOPNFA.value,desc_parametro_ipm='2022-12-31 23:59:59'),
                          Parametro(nome_parametro_ipm=EnumParametros.ultimoAcertoCFOPNFCe.value,desc_parametro_ipm='2022-12-31 23:59:59'),
                          Parametro(nome_parametro_ipm=EnumParametros.ultimoAcertoCFOPNFe.value,desc_parametro_ipm='2027-09-15 23:59:59'),
                          Parametro(nome_parametro_ipm=EnumParametros.ultimoAcertoCFOPNFeRecebida.value,desc_parametro_ipm='2024-01-01 23:59:59'),

                          Parametro(nome_parametro_ipm=EnumParametros.ultimoAcertoCadNFA.value,desc_parametro_ipm='2022-12-31 23:59:59'),
                          Parametro(nome_parametro_ipm=EnumParametros.ultimoAcertoCadNFCe.value,desc_parametro_ipm='2022-12-31 23:59:59'),
                          Parametro(nome_parametro_ipm=EnumParametros.ultimoAcertoCadNFe.value,desc_parametro_ipm='2022-12-31 23:59:59'),
                          Parametro(nome_parametro_ipm=EnumParametros.ultimoAcertoCadNFeRecebida.value,desc_parametro_ipm='2023-12-31 23:59:59'),

                          Parametro(nome_parametro_ipm=EnumParametros.ultimoProcessVA.value,desc_parametro_ipm='202312'),
            ]
            # Parametro(nome_parametro_ipm=EnumParametros.ultimoProcessNFe.value,desc_parametro_ipm='2022-12-31 23:59:59'),

            self.db.insert_parametros(parametros)
            return None

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def lista_parametros_cadastrados(self):
        etapaProcess = f"class {self.__class__.__name__} - def lista_parametros_cadastrados"
        # loga_mensagem(etapaProcess)

        try:
            obj_retorno = self.db.select_parametros()
            return obj_retorno

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise


    # def buscar_ncm(self, ncms):
    #     try:
    #         resultados = Parametro.objects.using(self.db_alias).filter(sua_coluna__in=lista_de_chaves)
    #
    #         # Retornar queryset de resultados
    #         return resultados
    #
    #     except Exception as err:
    #         # Tratar exceção
    #         print("Erro ao buscar por chaves:", err)
    #         raise
