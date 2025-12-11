import os

import warnings
from urllib3.exceptions import InsecureRequestWarning

from dotenv import load_dotenv
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from trino.exceptions import TrinoQueryError
import pandas as pd

from funcoes.utilitarios import loga_mensagem_erro

warnings.simplefilter("ignore", InsecureRequestWarning)

load_dotenv(override=True)


class Trino:
    __conn = None

    def __init__(self, schema, catalog='kudu') -> None:
        self.__conn = self.get_connection(schema, catalog)

    def get_connection(self, schema_name, catalog='kudu'):
        etapaProcess = f"class {self.__class__.__name__} - def _execute - {schema_name} - {catalog}"
        # loga_mensagem(etapaProcess)

        assert schema_name

        try:
            if self.__conn:
                self.__conn.close()
                self.__conn = None

            host = os.getenv('DB_TRINO_HOST')
            port = os.getenv('DB_TRINO_PORT')
            username = os.getenv('DB_TRINO_USER')
            password = os.getenv('DB_TRINO_PASS')

            self.__conn = connect(
                host=f'{host}:{port}',
                auth=BasicAuthentication(username, password),
                catalog=catalog,
                schema=schema_name,
                http_scheme='https',
                verify=False
            )
            return self.__conn

        except Exception as err:
            loga_mensagem_erro(etapaProcess, err)
            raise err

    def _execute(self, sql) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def _execute - {sql}"
        # loga_mensagem(etapaProcess)

        try:
            conn = self.__conn
            cursor = conn.cursor()
            cursor.execute(sql)
            colunas = [col[0].lower() for col in cursor.description]
            dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]

            cursor.close()
            return pd.DataFrame(dados)

        except Exception as err:
            loga_mensagem_erro(etapaProcess, err)
            raise err

    def get_nf3e_infnf3e(self, p_data_inicio, p_data_fim) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def get_nf3e_infnf3e - {p_data_inicio} a {p_data_fim}"

        sql = f"""SELECT nf3e.chnf3e                                     AS id_nf3e
                       , det.nitem                                       AS id_item_nota_fiscal 
                       , nf3e.dest_ie                                    AS ie_entrada
                       , nf3e.emit_ie                                    AS ie_saida
                       , nf3e.ide_cmunfg                                 AS codg_municipio_entrada 
                       , COALESCE(nf3e.dest_cnpj, nf3e.dest_cpf)         AS nusmr_cpf_cnpj_dest 
                       , nf3e.ide_mod                                    AS codg_modelo_nf3e 
                       , CAST(nf3e.ide_dhemi AS DATE)                    AS data_emissao_nf3e
                       , nf3e.total_vnf                                  AS valr_nf3e
                       , det.nitem                                       AS numr_item 
                       , det.detitem_prod_cfop                           AS numr_cfop
                       , CAST(det.detitem_prod_cclass as INT)            AS item_prod_cclass
                       , nf3e.acessante_tpclasse                         AS tipo_classe_consumo
                       , det.detitem_prod_inddevolucao                   AS ind_devolucao
                       , det.detitem_prod_vprod                          AS valr_va
                    FROM nf3e_infnf3e nf3e
                          LEFT JOIN nf3e_nfdet_det det ON nf3e.chnf3e = det.chnf3e
                   WHERE 1=1
                     AND nf3e.ide_dhemi BETWEEN CAST('{p_data_inicio}' AS TIMESTAMP) 
                                            AND CAST('{p_data_fim}' AS TIMESTAMP)"""
        try:
            return self._execute(sql=sql)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)

    def get_nf3e_classe_consumo(self, p_data_inicio, p_data_fim) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def get_nf3e_classe_consumo - {p_data_inicio} a {p_data_fim}"
        # loga_mensagem(etapaProcess)

        sql = f"""SELECT nf3e.chnf3e                                     AS id_nf3e
                       , nf3e.acessante_tpclasse                         AS tipo_classe_consumo
                    FROM nf3e_infnf3e nf3e
                   WHERE 1=1
                     AND nf3e.ide_dhemi BETWEEN CAST('{p_data_inicio}' AS TIMESTAMP) 
                                            AND CAST('{p_data_fim}' AS TIMESTAMP)"""
        try:
            return self._execute(sql=sql)

        except Exception as err:
            etapaProcess += " - ERRO - "
            loga_mensagem_erro(etapaProcess, err)

    def get_nf3e_eventos_cancelamento(self, p_data_inicio) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def get_nf3e_eventos_cancelamento >= {p_data_inicio}"
        # loga_mensagem(etapaProcess)

        sql = f"""SELECT evnt.chnf3e,
                         evnt.dhevento
                    FROM nf3e_infevento evnt
                   WHERE 1=1
                     AND evnt.dhevento >= CAST('{p_data_inicio}' AS TIMESTAMP)
                     AND evnt.tpevento IN ('110111', '240140')
                    """
        try:
            return self._execute(sql=sql)

        except Exception as err:
            etapaProcess += " - ERRO - "
            loga_mensagem_erro(etapaProcess, err)
            raise
