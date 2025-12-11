import os
import sys
import time
from datetime import datetime
from decimal import Decimal

import cx_Oracle
import pandas as pd
import sqlalchemy
import oracledb

from sqlalchemy.orm import Session

from funcoes.utilitarios import loga_mensagem_erro, baixa_csv, loga_mensagem


class oraProd:
    __url_database = None
    __root_directory: str = None
    __page_size = 10
    __session: Session = None
    __connection_oppened = False
    __env_username = None
    __env_password = None
    __env_host = None
    __env_database = None
    __env_driver = None
    __env_oracle_client = None

    etapaProcess = f"class {__name__} - class OraProd."
    # loga_mensagem(etapaProcess)

    try:
        cx_Oracle.coerce_to_unicode = True
        if sys.platform == "win32":
            cx_Oracle.init_oracle_client(lib_dir=os.environ.get("DB_ORACLE_CLIENT"))

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        raise

    @staticmethod
    def create_instance():
        etapaProcess = f"class {__class__.__name__} - class create_instance"
        # loga_mensagem(etapaProcess)

        # Cria objeto usando os parâmetros de conexão configurados individualmente
        env_username = os.environ.get('DB_ORACLE_USERNAME')
        env_password = os.environ.get('DB_ORACLE_PASSWORD')
        env_host = os.environ.get('DB_ORACLE_HOST')
        env_database = os.environ.get('DB_ORACLE_DATABASE')
        env_driver = os.environ.get('DB_ORACLE_DRIVER')
        env_porta = os.environ.get('DB_ORACLE_PORT')
        env_client = os.environ.get('DB_ORACLE_CLIENT')

        # env_username = os.environ.get('DB_ORAHOM_USERNAME')
        # env_password = os.environ.get('DB_ORAHOM_PASSWORD')
        # env_host = os.environ.get('DB_ORAHOM_HOST')
        # env_database = os.environ.get('DB_ORAHOM_DATABASE')
        # env_driver = os.environ.get('DB_ORAHOM_DRIVER')
        # env_porta = os.environ.get('DB_ORAHOM_PORT')
        # env_client = os.environ.get('DB_ORAHOM_CLIENT')

        return oraProd(username=env_username,
                       password=env_password,
                       host=env_host,
                       drivername=env_driver,
                       database=env_database,
                       porta=env_porta,
                       )

    def __init__(self,
                 drivername: str = None,
                 username: str = None,
                 password: str = None,
                 host: str = None,
                 database: str = None,
                 porta: str = None
                 ) -> None:
        super().__init__()

        if drivername is not None \
                and username is not None \
                and password is not None \
                and host is not None \
                and database is not None:
            self.__url_database = sqlalchemy.engine.url.URL(drivername=drivername,
                                                            username=username,
                                                            password=password,
                                                            host=host,
                                                            database=database,
                                                            port=porta,
                                                            query=None)
            self.__env_username = username
            self.__env_password = password
            self.__env_host = host
            self.__env_database = database
            self.__env_drivername = drivername
            self.__env_porta = porta
        else:
            raise Exception("""É necessário informar os dados para a conexão com o banco de dados. Parâmetros 
                               "drivername", "username", "password", "host" e "database"
                            """)

    def create_connection(self):
        etapaProcess = f"class {self.__class__.__name__} - def create_connection."
        # loga_mensagem(etapaProcess)

        dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={self.__env_host})(PORT={self.__env_porta}))(CONNECT_DATA=(SERVICE_NAME={self.__env_database})(SERVER=DEDICATED)))"
        oracledb.init_oracle_client(lib_dir=self.__env_oracle_client)
        return oracledb.connect(user=self.__env_username,
                                password=self.__env_password,
                                dsn=dsn)

    def insert_doc_partct(self, df):
        etapaProcess = f"class {self.__class__.__name__} - def insert_doc_partct."

        sql = """INSERT INTO IPM_DOCUMENTO_PARTCT_CALC_IPM (CODG_DOCUMENTO_PARTCT_CALCULO,
                                                            VALR_ADICIONADO_OPERACAO,
                                                            DATA_EMISSAO_DOCUMENTO,
                                                            INDI_APROP,
                                                            CODG_TIPO_DOC_PARTCT_CALC,
                                                            ID_PROCESM_INDICE,
                                                            ID_CONTRIB_IPM_ENTRADA,
                                                            ID_CONTRIB_IPM_SAIDA,
                                                            CODG_MOTIVO_EXCLUSAO_CALCULO,
                                                            NUMR_REFERENCIA_DOCUMENTO,
                                                            CODG_MUNICIPIO_ENTRADA,
                                                            CODG_MUNICIPIO_SAIDA
                                                           )
                                                   VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12)"""

        try:
            lista_colunas = []
            conexao = self.create_connection()
            cursor = conexao.cursor()

            for linha in df.itertuples(index=False, name='Pandas'):
                id_contrib_ipm_entrada = None if pd.isna(linha.id_contrib_ipm_entrada) \
                                              or linha.id_contrib_ipm_entrada in [0, 'None'] \
                                              else linha.id_contrib_ipm_entrada
                id_contrib_ipm_saida = None if pd.isna(linha.id_contrib_ipm_saida) \
                                            or linha.id_contrib_ipm_saida in [0, 'None'] \
                                            else linha.id_contrib_ipm_saida

                codg_municipio_entrada = None if pd.isna(linha.codg_municipio_entrada) \
                                              or linha.codg_municipio_entrada in [0, 'None'] \
                                              else linha.codg_municipio_entrada
                codg_municipio_saida = None if pd.isna(linha.codg_municipio_saida) \
                                            or linha.codg_municipio_saida in [0, 'None'] \
                                            else linha.codg_municipio_saida
                codg_motivo_exclusao_calculo = None if pd.isna(linha.codg_motivo_exclusao_calculo) \
                                                    or linha.codg_motivo_exclusao_calculo in [0, 'None'] \
                                                    else linha.codg_motivo_exclusao_calculo

                colunas = (
                    linha.codg_documento_partct_calculo,
                    linha.valr_adicionado_operacao,
                    linha.data_emissao_documento,
                    linha.indi_aprop,
                    linha.codg_tipo_doc_partct_calc,
                    linha.id_procesm_indice,
                    id_contrib_ipm_entrada,
                    id_contrib_ipm_saida,
                    codg_motivo_exclusao_calculo,
                    linha.numr_referencia_documento,
                    codg_municipio_entrada,
                    codg_municipio_saida,
                )
                lista_colunas.append(colunas)

            cursor.executemany(sql, lista_colunas)
            conexao.commit()

            return len(lista_colunas)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def insert_item_doc(self, df, chunk_size=10000):
        etapaProcess = f"class {self.__class__.__name__} - def insert_item_doc."
        etapaProcess = f"Grava dados na tabela IPM_ITEM_DOCUMENTO"
        lista_colunas = []
        total_inserido = 0

        sql = f"""Insert Into IPM_ITEM_DOCUMENTO (CODG_ITEM_DOCUMENTO
                                               ,  VALR_ADICIONADO
                                               ,  ID_PROCESM_INDICE
                                               ,  ID_PRODUTO_NCM
                                               ,  CODG_MOTIVO_EXCLUSAO_CALCULO
                                               ,  CODG_DOCUMENTO_PARTCT_CALCULO
                                               ,  CODG_TIPO_DOC_PARTCT_DOCUMENTO
                                               ,  CODG_CFOP)
                                          VALUES (:1, :2, :3, :4, :5, :6, :7, :8)"""

        try:
            conexao = self.create_connection()
            cursor = conexao.cursor()

            for linha in df.itertuples(index=False, name='Pandas'):
                # Garantir conversão correta para valores compatíveis
                i_codg_motivo_exclusao = None if pd.isna(linha.codg_motivo_exclusao_calculo) \
                                              or linha.codg_motivo_exclusao_calculo in [0, 'None'] \
                                              else str(int(linha.codg_motivo_exclusao_calculo))

                i_id_ncm = None if pd.isna(linha.id_produto_ncm) \
                                or linha.id_produto_ncm in [0, 'None'] \
                                else str(int(linha.id_produto_ncm))

                i_codg_cfop = None if pd.isna(linha.codg_cfop) \
                                   or linha.codg_cfop in [0, 'None'] \
                                   else str(int(linha.codg_cfop))

                # Conversão para float quando Decimal
                f_valr_adicionado = float(linha.valr_adicionado) if isinstance(linha.valr_adicionado, Decimal) \
                                                                 else linha.valr_adicionado

                # Criando a tupla com valores convertidos
                colunas = (
                    int(linha.codg_item_documento),
                    f_valr_adicionado,
                    linha.id_procesm_indice,
                    i_id_ncm,
                    i_codg_motivo_exclusao,
                    linha.codg_documento_partct_calculo,
                    linha.codg_tipo_doc_partct_calc,
                    i_codg_cfop
                )
                lista_colunas.append(colunas)

                if len(lista_colunas) == chunk_size:
                    cursor.executemany(sql, lista_colunas)
                    conexao.commit()
                    total_inserido += len(lista_colunas)
                    lista_colunas = []

            # Inserção final para o lote restante
            if lista_colunas:
                cursor.executemany(sql, lista_colunas)
                conexao.commit()
                total_inserido += len(lista_colunas)

        except Exception as err:
            df.name = 'item_doc'
            baixa_csv(df)
            etapaProcess += f" - ERRO - {self.__class__.__name__} - IPM_ITEM_DOCUMENTO - {err}"
            loga_mensagem_erro(etapaProcess)
            raise

        return total_inserido

    def insert_id_conv115(self, df, chunk_size=10000):
        etapaProcess = f"class {self.__class__.__name__} - def insert_id_conv115."
        # loga_mensagem(etapaProcess)

        etapaProcess = f"Grava dados na tabela IPM_CONVERSAO_CONV_115"

        lista_colunas = []
        lista_ids = []  # Lista para armazenar os IDs gerados

        sql_insert = f"""INSERT INTO IPM_CONVERSAO_CONV_115 (NUMR_INSCRICAO
                                                          ,  NUMR_DOCUMENTO_FISCAL
                                                          ,  DATA_EMISSAO_DOCUMENTO
                                                          ,  DESC_SERIE_DOCUMENTO_FISCAL
                                                          ,  CODG_SITUACAO_VERSAO_ARQUIVO)
                                                     VALUES (:1, :2, :3, :4, :5)"""

        try:
            conexao = self.create_connection()
            cursor = conexao.cursor()

            # Obtém o maior ID antes da inserção
            cursor.execute("SELECT MAX(ID_CONV_115) FROM IPM_CONVERSAO_CONV_115")
            max_id_conv115 = cursor.fetchone()[0] or 0

            # Insere os dados em blocos para eficiência
            for linha in df.itertuples(index=False, name='Pandas'):
                colunas = (linha.numr_inscricao,
                           linha.numr_documento_fiscal,
                           linha.data_emissao_documento,
                           linha.desc_serie_documento_fiscal,
                           linha.codg_situacao_versao_arquivo
                           )
                lista_colunas.append(colunas)

                if len(lista_colunas) == chunk_size:
                    cursor.executemany(sql_insert, lista_colunas)
                    conexao.commit()
                    lista_colunas = []

            # Insere os dados restantes
            if lista_colunas:
                cursor.executemany(sql_insert, lista_colunas)
                conexao.commit()

            # Busca os novos IDs gerados após o maior ID anterior à inserção
            sql = f"""SELECT ID_CONV_115
                        FROM IPM_CONVERSAO_CONV_115 
                       WHERE ID_CONV_115 > :id_conv115 
                       ORDER BY ID_CONV_115"""
            cursor.execute(sql, {'id_conv115': max_id_conv115})
            lista_ids = [row[0] for row in cursor.fetchall()]

            df_ids = pd.DataFrame(lista_ids, columns=['ID_CONV_115'])
            df['id_conv_115'] = df_ids['ID_CONV_115']
            return df

        except Exception as err:
            etapaProcess += f" - ERRO - {self.__class__.__name__} - IPM_CONVERSAO_CONV_115 - {err}"
            loga_mensagem_erro(etapaProcess)
            raise

    def insert_historico_contrib(self, df=None):
        etapaProcess = f"class {self.__class__.__name__} - def insert_historico_contrib."
        # loga_mensagem(etapaProcess)

        etapaProcess = f"Grava dados na tabela IPM_HISTORICO_CONTRIBUINTE"

        try:
            lista_colunas = []
            lista_sem_municipo = []
            lista_com_municipo = []
            conexao = self.create_connection()
            cursor = conexao.cursor()

            # for linha in df.itertuples(index=False, name='Pandas'):
            #     if pd.isna(linha.codg_municipio)\
            #        or linha.codg_municipio == 0\
            #        or linha.codg_municipio == '0':
            #         codg_municipio = None
            #     else:
            #         codg_municipio = linha.codg_municipio
            #
            #     colunas = (linha.numr_inscricao,
            #                linha.numr_referencia,
            #                linha.indi_produtor_rural,
            #                linha.stat_cadastro_contrib,
            #                linha.tipo_enqdto_fiscal,
            #                codg_municipio,
            #                )
            #     lista_colunas.append(colunas)

            for linha in df.itertuples(index=False, name='Pandas'):
                if pd.isna(linha.codg_municipio)\
                   or linha.codg_municipio == 0\
                   or linha.codg_municipio == '0':
                    colunas = (linha.numr_inscricao,
                               linha.numr_referencia,
                               linha.indi_produtor_rural,
                               linha.stat_cadastro_contrib,
                               linha.tipo_enqdto_fiscal,
                               )
                    lista_sem_municipo.append(colunas)

                else:
                    colunas = (linha.numr_inscricao,
                               linha.numr_referencia,
                               linha.indi_produtor_rural,
                               linha.stat_cadastro_contrib,
                               linha.tipo_enqdto_fiscal,
                               linha.codg_municipio,
                               )
                    lista_com_municipo.append(colunas)

            if len(lista_com_municipo) > 0:
                cursor.executemany("""
                                    INSERT INTO IPM_HISTORICO_CONTRIBUINTE (NUMR_INSCRICAO,
                                                                            NUMR_REFERENCIA,
                                                                            INDI_PRODUTOR_RURAL,
                                                                            STAT_CADASTRO_CONTRIB,
                                                                            TIPO_ENQDTO_FISCAL,
                                                                            CODG_MUNICIPIO)
                                                                    VALUES (:1, :2, :3, :4, :5, :6)""", lista_com_municipo)
            if len(lista_sem_municipo) > 0:
                cursor.executemany("""
                                    INSERT INTO IPM_HISTORICO_CONTRIBUINTE (NUMR_INSCRICAO,
                                                                            NUMR_REFERENCIA,
                                                                            INDI_PRODUTOR_RURAL,
                                                                            STAT_CADASTRO_CONTRIB,
                                                                            TIPO_ENQDTO_FISCAL)
                                                                    VALUES (:1, :2, :3, :4, :5)""", lista_sem_municipo)
            conexao.commit()

        except Exception as err:
            etapaProcess += f" - ERRO - {self.__class__.__name__} - IPM_HISTORICO_CONTRIBUINTE - {err}"
            loga_mensagem_erro(etapaProcess)
            raise

    def select_nfe_gerada(self, p_data_inicio=str, p_data_fim=str, chunk_size=10000) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def select_nfe_gerada."
        # loga_mensagem(etapaProcess)

        sql = f"""SELECT Ident.ID_NFE                                                    As id_nfe
                       , Item.ID_ITEM_NOTA_FISCAL                                        As id_item_nota_fiscal
                       , CAST(
                         CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                              THEN Ident.NUMR_INSCRICAO
                              ELSE Ident.NUMR_INSCRICAO_DEST        END As VARCHAR(14))  As ie_entrada
                       , CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                              THEN Ident.CODG_MUNICIPIO_GERADOR
                              ELSE Ident.CODG_MUNICIPIO_DEST        END                  As codg_municipio_entrada
                       , CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                              THEN Ident.CODG_UF
                              ELSE Ident.CODG_UF_DEST               END                  As codg_uf_entrada
                       , CAST(
                         CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                              THEN Ident.NUMR_INSCRICAO_DEST
                              ELSE Ident.NUMR_INSCRICAO             END As VARCHAR(14))  As ie_saida
                       , CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                              THEN Ident.CODG_MUNICIPIO_DEST
                              ELSE Ident.CODG_MUNICIPIO_GERADOR     END                  As codg_municipio_saida
                       , CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                              THEN Ident.CODG_UF_DEST
                              ELSE Ident.CODG_UF                    END                  As codg_uf_saida
                       , Ident.NUMR_CPF_CNPJ_DEST                                        As numr_cpf_cnpj_dest
                       , Ident.DESC_NATUREZA_OPERACAO                                    As desc_natureza_operacao
                       , Ident.CODG_MODELO_NFE                                           As codg_modelo_nfe
                       , TO_NUMBER(TO_CHAR(DATA_EMISSAO_NFE, 'YYYYMM'))                  As numr_ref_emissao
                       , DATA_EMISSAO_NFE                                                As data_emissao_doc
                       , CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                              THEN 'E' 
                              ELSE 'S' END                                               As indi_tipo_operacao
                       , CASE Ident.INDI_NOTA_EXPORTACAO
                              WHEN 'S' THEN 'Operação com exterior'
                              ELSE          'Operação interna'      END                  As desc_destino_operacao
                       , CASE Ident.TIPO_FINALIDADE_NFE 
                              WHEN 1 THEN 'NF-e normal'
                              WHEN 2 THEN 'NF-e complementar'
                              WHEN 3 THEN 'NF-e de ajuste'
                              WHEN 4 THEN 'Devolução de mercadoria'
                              ELSE        'Finalidade não identificada' END              As desc_finalidade_operacao
                       , Item.NUMR_ITEM                                                  As numr_item
                       , Item.CODG_CFOP                                                  As numr_cfop
                       , Item.CODG_EAN                                                   As numr_gtin
                       , Item.CODG_CEST                                                  As numr_cest
                       , Item.CODG_PRODUTO_NCM                                           As numr_ncm
                       , Item.QTDE_COMERCIAL                                             As qdade_itens
                       , COALESCE(Item.CODG_PRODUTO_ANP, 0)                              As codg_anp
                       , COALESCE(Ident.VALR_NOTA_FISCAL, 0)                             As valr_nfe
                       , CASE WHEN SUBSTR(Item.CODG_PRODUTO_ANP, 1, 2) IN ('32', '42', '82')
                              THEN COALESCE(Item.VALR_TOTAL_BRUTO, 0)
                                 - COALESCE(Item.VALR_DESCONTO, 0)
                                 - COALESCE(Item.VALR_ICMS_DESONERA, 0) 
                                 + COALESCE(Item.VALR_ICMS_SUBTRIB, 0)
                                 + COALESCE((Select Mono.VALR_ICMS_MONOFASICO_RETENCAO
                                               from NFE.NFE_ICMS_MONOFASICO Mono
                                              Where Item.ID_ITEM_NOTA_FISCAL = Mono.ID_ITEM_NOTA_FISCAL) , 0)
                                 + COALESCE(Item.VALR_FRETE, 0)
                                 + COALESCE(Item.VALR_SEGURO, 0)
                                 + COALESCE(Item.VALR_OUTRAS_DESPESAS, 0)
                                 + COALESCE(Item.VALR_IPI, 0)
                                 + COALESCE(Item.VALR_IMPOSTO_IMPORTACAO, 0)
                              ELSE COALESCE(Item.VALR_TOTAL_BRUTO, 0)
                                 - COALESCE(Item.VALR_DESCONTO, 0)
                                 - COALESCE(Item.VALR_ICMS_DESONERA, 0) 
                                 + COALESCE(Item.VALR_FRETE, 0)
                                 + COALESCE(Item.VALR_SEGURO, 0)
                                 + COALESCE(Item.VALR_OUTRAS_DESPESAS, 0)
                                 + COALESCE(Item.VALR_IPI, 0)
                                 + COALESCE(Item.VALR_IMPOSTO_IMPORTACAO, 0) END         As valr_va
                       , NVL(NUMR_PROTOCOLO_CANCEL, 0)                                   As numr_protocolo_cancel
                    FROM NFE_IDENTIFICACAO                Ident 
                          INNER JOIN NFE_ITEM_NOTA_FISCAL Item  ON Ident.ID_NFE             = Item.ID_NFE
                   WHERE 1=1
                     AND Ident.CODG_MODELO_NFE = '55'
--                   AND ((Ident.NUMR_INSCRICAO      IN (101651899, 103160310, 102347239, 106593129)
--                    Or   Ident.NUMR_INSCRICAO_DEST IN (101651899, 103160310, 102347239, 106593129))
--                    Or  (Ident.NUMR_CPF_CNPJ_DEST  IN ('03387396000160', '02916265008306', '33000167002155', '15689716001510')
--                    Or   Ident.NUMR_CNPJ_EMISSOR   IN ('03387396000160', '02916265008306', '33000167002155', '15689716001510')))
                     AND Ident.ID_RESULTADO_PROCESM   = 321         -- Nota Denegada = 390
                     AND Ident.DATA_EMISSAO_NFE BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                    AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')"""

        try:
            etapaProcess = f"Executa query de busca de NF-e geradas: "
            data_hora_atividade = datetime.now()
            conexao = self.create_connection()
            cursor = conexao.cursor()
            cursor.execute(sql)
            # loga_mensagem(etapaProcess + f' Processo finalizado. ' + str(datetime.now() - data_hora_atividade))

            # etapaProcess = f"Executa fetch do cursor. "
            data_hora_atividade = datetime.now()
            dados = []
            while True:
                chunk = cursor.fetchmany(chunk_size)
                if not chunk:
                    break
                dados.extend(chunk)
            # loga_mensagem(etapaProcess + f' Processo finalizado. ' + str(datetime.now() - data_hora_atividade))

            etapaProcess = f"Converte resultado para um dataframe. "
            data_hora_atividade = datetime.now()
            colunas = [col[0].lower() for col in cursor.description]
            df = pd.DataFrame(dados, columns=colunas)
            # loga_mensagem(etapaProcess + f' Processo finalizado. {len(df)} lidos' + str(datetime.now() - data_hora_atividade))

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

        finally:
            cursor.close()
            conexao.close()

        return df

    def select_nfce_gerada(self, p_data_inicio=str, p_data_fim=str, chunk_size=10000) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def select_nfce_gerada."
        loga_mensagem(etapaProcess)

        sql = f"""SELECT Ident.ID_NFE                                                    As id_nfe
                       , Item.ID_ITEM_NOTA_FISCAL                                        As id_item_nota_fiscal
                       , CAST(
                         CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                              THEN Ident.NUMR_INSCRICAO
                              ELSE Ident.NUMR_INSCRICAO_DEST        END As VARCHAR(14))  As ie_entrada
                       , CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                              THEN Ident.CODG_MUNICIPIO_GERADOR
                              ELSE Ident.CODG_MUNICIPIO_DEST        END                  As codg_municipio_entrada
                       , CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                              THEN Ident.CODG_UF
                              ELSE Ident.CODG_UF_DEST               END                  As codg_uf_entrada
                       , CAST(
                         CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                              THEN Ident.NUMR_INSCRICAO_DEST
                              ELSE Ident.NUMR_INSCRICAO             END As VARCHAR(14))  As ie_saida
                       , CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                              THEN Ident.CODG_MUNICIPIO_DEST
                              ELSE Ident.CODG_MUNICIPIO_GERADOR     END                  As codg_municipio_saida
                       , CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                              THEN Ident.CODG_UF_DEST
                              ELSE Ident.CODG_UF                    END                  As codg_uf_saida
                       , Ident.NUMR_CPF_CNPJ_DEST                                        As numr_cpf_cnpj_dest
                       , Ident.DESC_NATUREZA_OPERACAO                                    As desc_natureza_operacao
                       , Ident.CODG_MODELO_NFE                                           As codg_modelo_nfe
                       , TO_NUMBER(TO_CHAR(DATA_EMISSAO_NFE, 'YYYYMM'))                  As numr_ref_emissao
                       , DATA_EMISSAO_NFE                                                As data_emissao_doc
                       , CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                              THEN 'E' 
                              ELSE 'S' END                                               As indi_tipo_operacao
                       , CASE Ident.INDI_NOTA_EXPORTACAO
                              WHEN 'S' THEN 'Operação com exterior'
                              ELSE          'Operação interna'      END                  As desc_destino_operacao
                       , CASE Ident.TIPO_FINALIDADE_NFE 
                              WHEN 1 THEN 'NF-e normal'
                              WHEN 2 THEN 'NF-e complementar'
                              WHEN 3 THEN 'NF-e de ajuste'
                              WHEN 4 THEN 'Devolução de mercadoria'
                              ELSE        'Finalidade não identificada' END              As desc_finalidade_operacao
                       , Item.NUMR_ITEM                                                  As numr_item
                       , Item.CODG_CFOP                                                  As numr_cfop
                       , Item.CODG_EAN                                                   As numr_gtin
                       , Item.CODG_CEST                                                  As numr_cest
                       , Item.CODG_PRODUTO_NCM                                           As numr_ncm
                       , Item.QTDE_COMERCIAL                                             As qdade_itens
                       , COALESCE(Item.CODG_PRODUTO_ANP, 0)                              As codg_anp
                       , COALESCE(Ident.VALR_NOTA_FISCAL, 0)                             As valr_nfe
                       , CASE WHEN SUBSTR(Item.CODG_PRODUTO_ANP, 1, 2) IN ('32', '42', '82')
                              THEN COALESCE(Item.VALR_TOTAL_BRUTO, 0)
                                 - COALESCE(Item.VALR_DESCONTO, 0)
                                 - COALESCE(Item.VALR_ICMS_DESONERA, 0) 
                                 + COALESCE(Item.VALR_ICMS_SUBTRIB, 0)
                                 + COALESCE(Item.VALR_FRETE, 0)
                                 + COALESCE(Item.VALR_SEGURO, 0)
                                 + COALESCE(Item.VALR_OUTRAS_DESPESAS, 0)
                                 + COALESCE(Item.VALR_IPI, 0)
                                 + COALESCE(Item.VALR_IMPOSTO_IMPORTACAO, 0)
                              ELSE COALESCE(Item.VALR_TOTAL_BRUTO, 0)
                                 - COALESCE(Item.VALR_DESCONTO, 0)
                                 - COALESCE(Item.VALR_ICMS_DESONERA, 0) 
                                 + COALESCE(Item.VALR_FRETE, 0)
                                 + COALESCE(Item.VALR_SEGURO, 0)
                                 + COALESCE(Item.VALR_OUTRAS_DESPESAS, 0)
                                 + COALESCE(Item.VALR_IPI, 0)
                                 + COALESCE(Item.VALR_IMPOSTO_IMPORTACAO, 0) END         As valr_va
                       , NVL(NUMR_PROTOCOLO_CANCEL, 0)                                   As numr_protocolo_cancel
                    FROM NFE_IDENTIFICACAO                Ident 
                          INNER JOIN NFE_ITEM_NOTA_FISCAL Item  ON Ident.ID_NFE             = Item.ID_NFE
                   WHERE 1=1
                     AND Ident.CODG_MODELO_NFE = '65'
--                   AND ((Ident.NUMR_INSCRICAO      IN (101651899, 103160310, 102347239, 106593129)
--                    Or   Ident.NUMR_INSCRICAO_DEST IN (101651899, 103160310, 102347239, 106593129))
--                    Or  (Ident.NUMR_CPF_CNPJ_DEST  IN ('03387396000160', '02916265008306', '33000167002155', '15689716001510')
--                    Or   Ident.NUMR_CNPJ_EMISSOR   IN ('03387396000160', '02916265008306', '33000167002155', '15689716001510')))
                     AND Ident.ID_RESULTADO_PROCESM   = 321         -- Nota Denegada = 390
                     AND Ident.DATA_EMISSAO_NFE BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                    AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')"""

        try:
            etapaProcess = f"Executa query de busca de NFC-e geradas: "
            data_hora_atividade = datetime.now()
            conexao = self.create_connection()
            cursor = conexao.cursor()
            cursor.execute(sql)
            loga_mensagem(etapaProcess + f' Processo finalizado. ' + str(datetime.now() - data_hora_atividade))

            # etapaProcess = f"Executa fetch do cursor. "
            data_hora_atividade = datetime.now()
            dados = []
            while True:
                chunk = cursor.fetchmany(chunk_size)
                if not chunk:
                    break
                dados.extend(chunk)
            loga_mensagem(etapaProcess + f' Processo finalizado. ' + str(datetime.now() - data_hora_atividade))

            etapaProcess = f"Converte resultado para um dataframe. "
            data_hora_atividade = datetime.now()
            colunas = [col[0].lower() for col in cursor.description]
            df = pd.DataFrame(dados, columns=colunas)
            # loga_mensagem(etapaProcess + f' Processo finalizado. {len(df)} lidos' + str(datetime.now() - data_hora_atividade))

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

        finally:
            cursor.close()
            conexao.close()

        return df

    def select_nfe_recebida(self, p_data_inicio=str, p_data_fim=str) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - class select_nfe_recebida - {p_data_inicio} a {p_data_fim}"
        # loga_mensagem(etapaProcess)

        sql = f"""SELECT Ident.ID_NFE_RECEBIDA                                                           As id_nfe
                                   , Item.ID_ITEM_NOTA_FISCAL                                            As id_item_nota_fiscal
                                   , CAST(
                                     CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                                          THEN Ident.NUMR_INSCRICAO_EMITENTE
                                          ELSE Ident.NUMR_INSCRICAO_DEST            END As VARCHAR(14))  As ie_entrada
                                   , CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                                          THEN Ident.CODG_MUNICIPIO_EMITENTE
                                          ELSE Ident.CODG_MUNICIPIO_DEST              END                As codg_municipio_entrada
                                   , CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                                          THEN Ident.CODG_UF_EMITENTE
                                          ELSE Ident.CODG_UF_DEST                     END                As codg_uf_entrada
                                   , CAST(
                                     CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                                          THEN Ident.NUMR_INSCRICAO_DEST
                                          ELSE Ident.NUMR_INSCRICAO_EMITENTE        END As VARCHAR(14))  As ie_saida
                                   , CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                                          THEN Ident.CODG_MUNICIPIO_DEST
                                          ELSE Ident.CODG_MUNICIPIO_EMITENTE          END                As codg_municipio_saida
                                   , CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                                          THEN Ident.CODG_UF_DEST
                                          ELSE Ident.CODG_UF_EMITENTE                 END                As codg_uf_saida
                                   , COALESCE(Ident.NUMR_CNPJ_DEST, NUMR_CPF_DEST)                       As numr_cpf_cnpj_dest
                                   , Null                                                                As desc_natureza_operacao
                                   , Ident.CODG_MODELO_NFE                                               As codg_modelo_nfe
                                   , TO_NUMBER(TO_CHAR(DATA_EMISSAO_NFE, 'YYYYMM'))                      As numr_ref_emissao
                                   , DATA_EMISSAO_NFE                                                    As data_emissao_doc
                                   , CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                                          THEN 'E' 
                                          ELSE 'S' END                                                   As indi_tipo_operacao
                                   , 'Operação interna'                                                  As desc_destino_operacao
                                   , 'NF-e normal'                                                       As desc_finalidade_operacao
                                   , Item.NUMR_ITEM                                                      As numr_item
                                   , Item.CODG_CFOP                                                      As numr_cfop
                                   , Item.CODG_EAN                                                       As numr_gtin
                                   , Item.CODG_CEST                                                      As numr_cest
                                   , Item.CODG_PRODUTO_NCM                                               As numr_ncm
                                   , Item.QTDE_COMERCIAL                                                 As qdade_itens
                                   , COALESCE(Item.CODG_PRODUTO_ANP, 0)                                  As codg_anp
                                   , COALESCE(Ident.VALR_NOTA_FISCAL, 0)                                 As valr_nfe
                                   , CASE WHEN SUBSTR(Item.CODG_PRODUTO_ANP, 1, 2) IN ('32', '42', '82')
                                          THEN COALESCE(Item.VALR_TOTAL_BRUTO, 0)
                                             - COALESCE(Item.VALR_DESCONTO, 0)
                                             - COALESCE(Item.VALR_ICMS_DESONERA, 0) 
                                             + COALESCE(Item.VALR_ICMS_SUBTRIB, 0)
--                                             + COALESCE((Select Mono.VALR_ICMS_MONOFASICO_RETENCAO
--                                                           from NFE.NFE_ICMS_MONOFASICO Mono
--                                                          Where Item.ID_ITEM_NOTA_FISCAL = Mono.ID_ITEM_NOTA_FISCAL) , 0)
                                             + COALESCE(Item.VALR_FRETE, 0)
                                             + COALESCE(Item.VALR_SEGURO, 0)
                                             + COALESCE(Item.VALR_OUTRAS_DESPESAS, 0)
                                             + COALESCE(Item.VALR_IPI, 0)
                                             + COALESCE(Item.VALR_IMPOSTO_IMPORTACAO, 0)
                                          ELSE COALESCE(Item.VALR_TOTAL_BRUTO, 0)
                                             - COALESCE(Item.VALR_DESCONTO, 0)
                                             - COALESCE(Item.VALR_ICMS_DESONERA, 0) 
                                             + COALESCE(Item.VALR_FRETE, 0)
                                             + COALESCE(Item.VALR_SEGURO, 0)
                                             + COALESCE(Item.VALR_OUTRAS_DESPESAS, 0)
                                             + COALESCE(Item.VALR_IPI, 0)
                                             + COALESCE(Item.VALR_IMPOSTO_IMPORTACAO, 0) END             As valr_va
                                   , NVL(NUMR_PROTOCOLO_CANCEL, 0)                                       As numr_protocolo_cancel
                                FROM NFE_IDENTIFICACAO_RECEBIDA       Ident 
                                      INNER JOIN NFE_ITEM_NOTA_FISCAL Item  ON Ident.ID_NFE_RECEBIDA    = Item.ID_NFE_RECEBIDA
                               WHERE 1=1
        --                         AND ((Ident.NUMR_INSCRICAO_EMITENTE IN (101651899, 103160310, 102347239, 106593129)
        --                          Or   Ident.NUMR_INSCRICAO_DEST     IN (101651899, 103160310, 102347239, 106593129))
        --                          Or  (Ident.NUMR_CNPJ_DEST          IN ('03387396000160', '02916265008306', '33000167002155', '15689716001510')
        --                          Or   Ident.NUMR_CNPJ               IN ('03387396000160', '02916265008306', '33000167002155', '15689716001510')))
                                 AND Ident.DATA_EMISSAO_NFE BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                                AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')"""

        try:
            etapaProcess = f"Executa query de busca de NF-e recebidas: {sql}"
            conexao = self.create_connection()
            cursor = conexao.cursor()

            df = pd.DataFrame(cursor.execute(sql))
            if df.size > 0:
                df.columns = ['id_nfe',
                              'id_item_nota_fiscal',
                              'ie_entrada',
                              'codg_municipio_entrada',
                              'codg_uf_entrada',
                              'ie_saida',
                              'codg_municipio_saida',
                              'codg_uf_saida',
                              'numr_cpf_cnpj_dest',
                              'desc_natureza_operacao',
                              'codg_modelo_nfe',
                              'numr_ref_emissao',
                              'data_emissao_doc',
                              'indi_tipo_operacao',
                              'desc_destino_operacao',
                              'desc_finalidade_operacao',
                              'numr_item',
                              'numr_cfop',
                              'numr_gtin',
                              'numr_cest',
                              'numr_ncm',
                              'qdade_itens',
                              'codg_anp',
                              'valr_nfe',
                              'valr_va',
                              'numr_protocolo_cancel',
                              ]

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

        return df

