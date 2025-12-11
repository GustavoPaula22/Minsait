import math
import os
import cx_Oracle
import numpy as np
import pandas as pd

from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Date, text, Numeric, bindparam, \
    BigInteger
from sqlalchemy.engine import Connection, Engine

from funcoes.utilitarios import loga_mensagem, loga_mensagem_erro

load_dotenv()

class Oracle:
    def __init__(self) -> None:
        # Carregando client Oracle
        oracle_lib_dir = os.getenv('DB_ORACLE_CLIENT')
        try:
            # Esse código lança um erro se já tiver sido chamado antes.
            # Não tem como testar se já foi chamado antes, por isso o try/except/pass.
            cx_Oracle.init_oracle_client(lib_dir=oracle_lib_dir)
        except:
            pass

        # Criando 'pool' de conexões
        username = os.getenv('DB_ORACLE_USERNAME')
        password = os.getenv('DB_ORACLE_PASSWORD')
        host = os.getenv('DB_ORACLE_HOST')
        port = os.getenv('DB_ORACLE_PORT')
        service_name = os.getenv('DB_ORACLE_DATABASE')
        
        dsn = f'(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = {host})(PORT = {port})) (CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = {service_name})))'
        conn_str = f'oracle://{username}:{password}@{dsn}'

        self.engine = create_engine(conn_str, echo=False)

    def get_connection(self) -> Connection:
        return self.engine.connect()

    def get_engine(self) -> Engine:
        return self.engine

    def insert_id_conv115(self, df, chunksize=50000):
        etapaProcess = f"class {self.__class__.__name__} - def insert_id_conv115."
        # loga_mensagem(etapaProcess)

        try:
            # Cria uma conexão com o banco
            conn = self.get_engine().connect()

            # Transforma para um formato de 'lista de dicionários'
            values = df.to_dict('records')

            # Cria uma representação da tabela do banco
            metadata = MetaData()
            table = Table('IPM_CONVERSAO_CONV_115', metadata,
                          Column('ID_CONV_115', Integer, primary_key=True, autoincrement=True),
                          Column('numr_inscricao', Numeric(15, 0)),
                          Column('numr_documento_fiscal', Numeric(15, 0)),
                          Column('data_emissao_documento', Date),
                          Column('desc_serie_documento_fiscal', String(3)),
                          Column('codg_situacao_versao_arquivo', String(3)),
                          schema='IPM')

            # Grava as linhas no banco
            i, j, x = 0, chunksize, len(values)
            list_ids = []
            while i < x:
                loga_mensagem(f'Gravando registros de {i} até {min(j, x)}.')
                stmt = table.insert().returning(table.c.ID_CONV_115)
                result = conn.execute(stmt, values[i:j])
                # Recupera os ids autoincrement gerados para um dataframe
                ids_step = result.fetchall()
                ids_step = pd.DataFrame(ids_step, columns=['id_conv_115'], dtype=int)
                list_ids.append(ids_step)
                i += chunksize
                j += chunksize

                conn.commit()

            # Transoforma os ids recuperados em um único dataframe
            ids = pd.concat(list_ids)
            ids = ids.reset_index()

            # Adiciona os ids ao dataframe original
            df['id_conv_115'] = ids['id_conv_115']

            conn.commit()

            return df

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def get_inscricoes(self, cnpjs):
        assert cnpjs.any()
        assert len(cnpjs) <= 1000

        # Converte a lista de CNPJs para uma string separada por vírgula
        cnpjs_text_list = "','".join(cnpjs)
        # Cada CNPJ precisa estar entre aspas para não afetar o desempenho
        cnpjs_text_list = "'" + cnpjs_text_list + "'"

        query = text(f'''SELECT NUMR_INSCRICAO, NUMR_CNPJ_BASE
                           FROM CCE_CONTRIB_PESSOA_JURIDICA
                          WHERE NUMR_CNPJ_BASE IN ({cnpjs_text_list})
                      ''')

        with self.get_connection() as conn:
            self.get_connection()
            result = conn.execute(query).all()
        return result

    def get_table_opcao_simples_simei(self):
        meta = MetaData()
        table = Table('IPM_OPCAO_CONTRIB_SIMPL_SIMEI', meta,
                      Column('NUMR_INSCRICAO_SIMPLES', Integer),
                      Column('DATA_INICIO_OPCAO_SIMPLES', Date),
                      Column('DATA_FIM_OPCAO_SIMPLES', Date),
                      Column('NUMR_CNPJ_BASE_SIMPLES', String(8)),
                      Column('INDI_SIMPLES_SIMEI', String(1)),
                      Column('DATA_ATUALIZ', Date),
                      Column('NUMR_OPCAO', Integer),
                      Column('INDI_VALIDADE_INFORMACAO', String(1)),
                      schema='IPM')
        return table

    def inserir_opcao_simples_simei(self, df, chunksize):
        etapaProcess = f"class {self.__class__.__name__} - def inserir_opcao_simples_simei."
        # loga_mensagem(etapaProcess)

        try:
            values = df.to_dict('records')

            # Pegando referência da representação da tabela
            table = self.get_table_opcao_simples_simei()

            # Insere registros
            with self.get_connection() as conn:
                stmt = table.insert().prefix_with('/*+ ignore_row_on_dupkey_index(IPM_OPCAO_CONTRIB_SIMPL_SIMEI, IPMCONTSIM_PK) */')
                conn.execute(stmt, values)
                conn.commit()

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def alterar_opcao_simples_simei(self, df):
        etapaProcess = f"class {self.__class__.__name__} - def alterar_opcao_simples_simei."
        # loga_mensagem(etapaProcess)

        df = df.rename(columns={'NUMR_OPCAO': 'NUMR_OPCAO_REF',
                                'NUMR_CNPJ_BASE_SIMPLES': 'NUMR_CNPJ_BASE_SIMPLES_REF',
                                'INDI_SIMPLES_SIMEI': 'INDI_SIMPLES_SIMEI_REF',
                                'NUMR_INSCRICAO_SIMPLES': 'NUMR_INSCRICAO_SIMPLES_REF'})
        values = df.to_dict('records')

        # Pegando referência da representação da tabela
        table = self.get_table_opcao_simples_simei()
        
        # Altera registros
        try:
            with self.get_connection() as conn:
                stmt = (
                    table.update()
                        .where(table.c.NUMR_OPCAO==bindparam('NUMR_OPCAO_REF'))
                        .where(table.c.NUMR_CNPJ_BASE_SIMPLES==bindparam('NUMR_CNPJ_BASE_SIMPLES_REF'))
                        .where(table.c.INDI_SIMPLES_SIMEI==bindparam('INDI_SIMPLES_SIMEI_REF'))
                        .where(table.c.NUMR_INSCRICAO_SIMPLES==bindparam('NUMR_INSCRICAO_SIMPLES_REF'))
                )
                conn.execute(stmt, values)
                conn.commit()

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def get_table_item_documento(self):
        meta = MetaData()
        table = Table('IPM_ITEM_DOCUMENTO', meta,
                      Column('CODG_ITEM_DOCUMENTO', BigInteger),
                      Column('VALR_ADICIONADO', Numeric),
                      Column('ID_PROCESM_INDICE', Integer),
                      Column('ID_PRODUTO_NCM', Integer),
                      Column('CODG_MOTIVO_EXCLUSAO_CALCULO', Integer),
                      Column('CODG_DOCUMENTO_PARTCT_CALCULO', BigInteger),
                      Column('CODG_TIPO_DOC_PARTCT_DOCUMENTO', Integer),
                      Column('CODG_CFOP', Integer),
                      schema='IPM')
        return table

    # def insert_item_documento(self, df):
    #     etapaProcess = f"class {self.__class__.__name__} - def insert_item_documento."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         values = df.to_dict('records')
    #
    #         # Pegando referência da representação da tabela
    #         table = self.get_table_item_documento()
    #
    #         with self.get_connection() as conn:
    #             stmt = table.insert()
    #             resultado = conn.execute(stmt, values)
    #             conn.commit()
    #
    #         return resultado.rowcount
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise

    def insert_item_documento(self, df, batch_size=1000):
        etapaProcess = f"class {self.__class__.__name__} - def insert_item_documento."

        try:
            table = self.get_table_item_documento()

            with self.get_connection() as conn:
                stmt = table.insert()
                total_inserted = 0

                for start in range(0, len(df), batch_size):
                    end = start + batch_size
                    df_chunk = df.iloc[start:end]
                    values = df_chunk.to_dict('records')

                    try:
                        resultado = conn.execute(stmt, values)
                        conn.commit()
                        total_inserted += resultado.rowcount

                    except Exception as e:
                        loga_mensagem_erro(f"Erro ao inserir o lote de linhas {start} a {end}: {e}")
                        loga_mensagem_erro("Entrando em modo linha-a-linha para diagnosticar...")

                        for idx, row in df_chunk.iterrows():
                            try:
                                conn.execute(stmt, [row.to_dict()])
                                conn.commit()
                                total_inserted += 1
                            except Exception as err_linha:
                                loga_mensagem_erro(f"Erro na linha {idx}:{row.to_dict()} {err_linha}")

                return total_inserted

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise
