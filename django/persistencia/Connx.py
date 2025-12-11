import os
import jaydebeapi

from dotenv import load_dotenv

from funcoes.utilitarios import loga_mensagem, loga_mensagem_erro

load_dotenv()

class Connx:
    __conn = None

    def get_connection(self):
        etapaProcess = f"class {self.__class__.__name__} - def get_connection."
        # loga_mensagem(etapaProcess)

        if self.__conn:
            return self.__conn

        loga_mensagem('Abrindo conexão com o Connx/ADABAS.')

        try:
            connx_driver = os.getenv('DB_CONNX_DRIVER')
            connx_jar = os.getenv('DB_CONNX_JAR')
            connx_host = os.getenv('DB_CONNX_HOST')
            connx_port = os.getenv('DB_CONNX_PORT')
            connx_database = os.getenv('DB_CONNX_DATABASE')
            connx_user = os.getenv('DB_CONNX_USER')
            connx_pass = os.getenv('DB_CONNX_PASS')
            url_conn = f'jdbc:connx:GATEWAY={connx_host};port={connx_port};DD={connx_database}'

            self.__conn = jaydebeapi.connect(connx_driver, url_conn, [connx_user, connx_pass], connx_jar)
            return self.__conn

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def get_lista_simples(self, data_ref_ini, data_ref_fim):
        etapaProcess = f"class {self.__class__.__name__} - def get_lista_simples."
        # loga_mensagem(etapaProcess)

        query = f'''
                    SELECT DATA_EFEITO_INI
                         , DATA_EFEITO_FIM
                         , NUMR_CNPJ_BASE
                         , DATA_ATUALIZACAO
                         , NUMR_OPCAO
                         , (TIPO_REGISTRO = 5 AND DATA_EFEITO_FIM IS NULL) AS INDI_VALIDADE_INFORMACAO
                      FROM SSN_SIMPLES_PERIODOS simples
                     WHERE DATA_ATUALIZACAO >= {data_ref_ini} AND DATA_ATUALIZACAO < {data_ref_fim};
                 '''
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            return cursor.fetchall()

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def get_lista_simei(self, data_ref_ini, data_ref_fim):
        etapaProcess = f"class {self.__class__.__name__} - def get_lista_simei."
        # loga_mensagem(etapaProcess)

        query = f'''
                    SELECT DATA_EFEITO_INI_SIMEI 
                         , DATA_EFEITO_FIM_SIMEI 
                         , NUMR_CNPJ_BASE_SIMEI 
                         , DATA_ATUALIZACAO_SIMEI 
                         , NUMR_OPCAO_SIMEI
                         , (TIPO_REGISTRO_SIMEI = 5 AND DATA_EFEITO_FIM_SIMEI IS NULL) AS INDI_VALIDADE_INFORMACAO
                      FROM SSN_SIMEI_PERIODOS simei
                     WHERE DATA_ATUALIZACAO_SIMEI >= {data_ref_ini} AND DATA_ATUALIZACAO_SIMEI < {data_ref_fim};
                 '''

        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            return cursor.fetchall()

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise
