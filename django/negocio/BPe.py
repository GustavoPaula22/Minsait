import time
import pandas as pd
import numpy as np

from datetime import datetime, timezone
from sqlalchemy import MetaData, Table, Column, String, Integer, Double, Date, text

from persistencia.Trino import Trino
from persistencia.Oracle import Oracle
from funcoes.constantes import EnumTipoDocumento, EnumMotivoExclusao, EnumTipoProcessamento, EnumStatusProcessamento, \
    EnumParametros
from negocio.Procesm import ProcesmClasse
from negocio.Param import ParamClasse
from funcoes.utilitarios import loga_mensagem, loga_mensagem_erro


class BPe:
    etapaProcess = f"class {__name__} - class BPe."
    # loga_mensagem(etapaProcess)

    def __init__(self) -> None:
        loga_mensagem('Instanciando classe BPeTrino.')

        # Cria conexão com o Trino
        trino = Trino('bpe')
        self._trino_conn = trino.get_connection('bpe')

        # Cria conexão com o Oracle
        oracle = Oracle()
        self._oracle_engine = oracle.get_engine()

        # Log do processamento
        self.negProcesm = ProcesmClasse()

    def carga_bpe(self, data_ini, data_fim, step=100000) -> str:
        etapaProcess = f"class {self.__class__.__name__} - def carga_bpe. Período de {data_ini} a {data_fim}"
        # loga_mensagem(etapaProcess)

        try:
            assert data_ini
            assert data_fim
            assert step

            self._tempo_ini = time.time()

            self._data_ini_str = data_ini.strftime('%Y-%m-%dT%H:%M:%S+00:00')
            self._data_fim_str = data_fim.strftime('%Y-%m-%dT%H:%M:%S+00:00')
            self._step = step

            # Loga na base o processamento
            tipo_processamento = EnumTipoProcessamento.importacaoBPe.value
            self.processamento = self.negProcesm.iniciar_processamento(data_ini, data_fim, tipo_processamento)

            loga_mensagem(f'Período de carga de {self._data_ini_str} até {self._data_fim_str}.')
            loga_mensagem(f'Step de carga de {self._step} registros por vez.')

            # Atualiza o log na base de processamento
            self.processamento.desc_observacao_procesm_indice = f'Carga do BPe INICIADA para o período de {self._data_ini_str} até {self._data_fim_str}.'
            self.negProcesm.atualizar_situacao_processamento(self.processamento)

            # Etapa de exclusão dos dados anteriores
            self._exclui_dados_anteriores()
            loga_mensagem(f'Exclusão finalizada em {time.time() - self._tempo_ini} segundos.')

            # Atualiza o log na base de processamento
            self.processamento.desc_observacao_procesm_indice = 'Finalizada etapa de exclusão. Iniciando processamento dos documentos.'
            self.negProcesm.atualizar_situacao_processamento(self.processamento)

            # Etapa de processamento dos documentos. Busca na base de origem (Trino) os documentos no período.
            self._processa_documentos()
            loga_mensagem(f'Processamento finalizado em {time.time() - self._tempo_ini} segundos.')

            # Atualiza o log na base de processamento
            self.processamento.desc_observacao_procesm_indice = 'Finalizada etapa de processamento. Iniciando gravação dos documentos.'
            self.negProcesm.atualizar_situacao_processamento(self.processamento)

            # Etapa de gravação dos documentos.
            # Só executa essa etapa se o processamento retornar algum documento.
            if len(self._df_documentos) > 0:
                self._grava_documentos()
                loga_mensagem(f'Gravação finalizada em {time.time() - self._tempo_ini} segundos.')
            else:
                loga_mensagem('Sem documentos para gravar.')

            # Atualiza o log na base de processamento
            self.processamento.desc_observacao_procesm_indice = 'Finalizada etapa de gravação. Finalizando carga.'
            self.negProcesm.atualizar_situacao_processamento(self.processamento)

            param = ParamClasse()
            param.atualizar_parametro(EnumParametros.ultimaCargaBPe.value, data_fim)

            # Atualiza o log na base de processamento
            self.processamento.desc_observacao_procesm_indice = f'Carga do BPe finalizada com sucesso para o período de {self._data_ini_str} até {self._data_fim_str}.'
            if len(self._df_documentos) > 0:
                self.processamento.stat_procesm_indice = EnumStatusProcessamento.sucesso.value
            else:
                self.processamento.stat_procesm_indice = EnumStatusProcessamento.sucessoSemDados.value
            self.negProcesm.atualizar_situacao_processamento(self.processamento)

            self.codigo_retorno = 0

        except Exception as err:
            etapaProcess += " - ERRO - "
            loga_mensagem_erro(etapaProcess, err)

            # Atualiza o log na base de processamento
            self.processamento.desc_observacao_procesm_indice = f' - ERRO - {err}'
            self.processamento.stat_procesm_indice = EnumStatusProcessamento.erro.value
            self.negProcesm.atualizar_situacao_processamento(self.processamento)
            self.codigo_retorno = 9
            raise

        loga_mensagem(f'Carga BPe finalizada em {time.time() - self._tempo_ini} segundos.')
        return str(self.codigo_retorno)

    def _exclui_dados_anteriores(self):
        etapaProcess = f"class {self.__class__.__name__} - def _exclui_dados_anteriores."
        # loga_mensagem(etapaProcess)

        # Cria uma conexão com o banco
        try:
            conn = self._oracle_engine.connect()

            # Pega o intervalo de data adequado
            data_ini = self._data_ini_str.split('+')[0]
            data_fim = self._data_fim_str.split('+')[0]

            # Deleta os registros da tabela IPM_CONVERSAO_BPE
            loga_mensagem('Exclui Dados data tabela IPM_CONVERSAO_BPE.')
            sql = text(f'''
                           DELETE FROM IPM.IPM_CONVERSAO_BPE
                            WHERE ID_CONVERSAO_BPE in (SELECT CODG_DOCUMENTO_PARTCT_CALCULO
                                                         FROM IPM.IPM_DOCUMENTO_PARTCT_CALC_IPM
                                                        WHERE CODG_TIPO_DOC_PARTCT_CALC IN ({EnumTipoDocumento.BPe.value}) 
                                                          AND DATA_EMISSAO_DOCUMENTO BETWEEN to_date('{data_ini}', 'YYYY-MM-DD"T"HH24:MI:SS') 
                                                                                         AND to_date('{data_fim}', 'YYYY-MM-DD"T"HH24:MI:SS')
                                                       )''')

            conn.execute(sql)

            # Deleta os registros da tabela IPM_DOCUMENTO_PARTCT_CALC_IPM
            loga_mensagem('Exclui Dados data tabela IPM_DOCUMENTO_PARTCT_CALC_IPM.')
            sql = text(f'''
                           DELETE FROM IPM.IPM_DOCUMENTO_PARTCT_CALC_IPM
                            WHERE CODG_TIPO_DOC_PARTCT_CALC IN ({EnumTipoDocumento.BPe.value})
                              AND DATA_EMISSAO_DOCUMENTO BETWEEN to_date('{data_ini}', 'YYYY-MM-DD"T"HH24:MI:SS')
                                                             AND to_date('{data_fim}', 'YYYY-MM-DD"T"HH24:MI:SS')''')

            conn.execute(sql)

            conn.commit()

        except Exception as err:
            etapaProcess += " - ERRO - "
            loga_mensagem_erro(etapaProcess, err)
            raise

    def _processa_documentos(self):
        etapaProcess = f"class {self.__class__.__name__} - def _processa_documentos."
        # loga_mensagem(etapaProcess)

        sql = f'''
                  SELECT bpe.ide_dhemi_nums, --data emissão nums
                  	     bpe.infpassagem_dhemb_nums, --data embarque nums
                  	     bpe.id, --chave acesso
                  	     bpe.ide_cmunini, --municipio inicio
                  	     bpe.infvalorbpe_vpgto, --valor
                         {EnumTipoDocumento.BPe.value} AS tipo_documento,
                         evento.motivo_exclusao,
                         'S' AS tipo_apropriacao
                      FROM kudu.bpe.bpe_infbpe bpe LEFT JOIN (SELECT evento.chave, 
		                                                             {EnumMotivoExclusao.DocumentoCancelado.value} as motivo_exclusao
		                                                        FROM kudu.bpe.bpe_evento AS evento
		                                                       WHERE evento.tpevento IN ('110111', '240140', '110115') --cancelamento, autorizado substituição de BP-e ou não embarque
		                                                       GROUP BY evento.chave
		                                                      ) evento ON bpe.id = evento.chave 
                     WHERE bpe.ide_dhemi_nums BETWEEN to_unixtime(from_iso8601_timestamp('{self._data_ini_str}')) 
                                                  AND to_unixtime(from_iso8601_timestamp('{self._data_fim_str}'))
               '''

        col_names = ['data_emissao', 'data_embarque', 'chave', 'municipio_inicio', 'valor', 'tipo_documento',
                     'motivo_exclusao', 'tipo_apropriacao']

        try:
            cur = self._trino_conn.cursor()
            cur.execute(sql)
            df_documentos = pd.DataFrame(cur.fetchall(), columns=col_names)
            cur.close()

            df_documentos['data_emissao'] = df_documentos['data_emissao'].apply(
                lambda x: datetime.fromtimestamp(x, tz=timezone.utc))
            df_documentos['data_embarque'] = df_documentos['data_embarque'].apply(
                lambda x: datetime.fromtimestamp(x, tz=timezone.utc).strftime('%Y%m'))

            df_documentos = df_documentos.astype({
                'valor': np.float64,
                'tipo_documento': 'Int32',
                'motivo_exclusao': 'Int32',
                'municipio_inicio': int,
            })

            loga_mensagem(f'Número de Documentos: {len(df_documentos)}')
            self._df_documentos = df_documentos

        except Exception as err:
            etapaProcess += " - ERRO - "
            loga_mensagem_erro(etapaProcess, err)
            raise

    def _grava_documentos(self):
        etapaProcess = f"class {self.__class__.__name__} - def _grava_documentos."
        # loga_mensagem(etapaProcess)

        try:
            # Cria uma conexão com o banco
            conn = self._oracle_engine.connect()

            # Seleciona somente a 'coluna' com as chaves
            df = self._df_documentos[['chave']]

            # Renomeia com o nome do campo na tabela
            df = df.rename(columns={
                'chave': 'CODG_CHAVE_ACESSO_BPE'
            })

            # Transforma para um formato de 'lista de dicionários'
            values = df.to_dict('records')

            # Cria uma representação da tabela do banco
            meta = MetaData()
            table = Table('IPM_CONVERSAO_BPE', meta,
                          Column('ID_CONVERSAO_BPE', Integer, primary_key=True, autoincrement=True),
                          Column('CODG_CHAVE_ACESSO_BPE', String(44)),
                          schema='IPM')

            # Grava os registros no banco
            loga_mensagem('Gravando na tabela IPM_CONVERSAO_BPE.')
            i, j, x = 0, self._step, len(values)
            list_ids = []
            while i < x:
                loga_mensagem(f'Gravando registros de {i} até {min(j, x)}.')
                stmt = table.insert().returning(table.c.ID_CONVERSAO_BPE)
                result = conn.execute(stmt, values[i:j])

                # Recupera os ids autoincrement gerados para um dataframe
                ids_step = result.fetchall()
                ids_step = pd.DataFrame(ids_step, columns=['id_conversao_bpe'], dtype=int)
                list_ids.append(ids_step)

                i += self._step
                j += self._step

            # Transoforma os ids recuperados em um único dataframe
            ids = pd.concat(list_ids)
            ids = ids.reset_index()

            # Adiciona os ids ao dataframe original
            self._df_documentos['id_bpe'] = ids['id_conversao_bpe']

            # Substitui id município do IBGE para o Estadual
            loga_mensagem('Substituindo IDs dos Municípios.')
            lista_municipios = self._df_documentos['municipio_inicio'].unique()
            lista_municipios = [x.item() for x in lista_municipios]

            # Cria uma representação da tabela do banco
            meta = MetaData()
            table = Table('GEN_MUNICIPIO', meta,
                          Column('CODG_MUNICIPIO', Integer),
                          Column('CODG_IBGE', Integer),
                          schema='GEN')

            # Recupera lista de ids da tabela GEN_MUNICIPIO
            stmt = table.select().where(table.c.CODG_IBGE.in_(lista_municipios))
            result = conn.execute(stmt, values[i:j]).fetchall()
            df_municipios = pd.DataFrame(result, columns=['CODG_MUNICIPIO_SAIDA', 'municipio_inicio'], dtype=int)

            # Adiciona os novos ids ao dataframe
            self._df_documentos = pd.merge(self._df_documentos, df_municipios, on='municipio_inicio')

            df = self._df_documentos
            # Adiciona o ID do processamento
            df['id_processamento'] = self.processamento.id_procesm_indice

            # Renomeia com o nome do campo na tabela
            df = df.rename(columns={
                'id_bpe': 'CODG_DOCUMENTO_PARTCT_CALCULO',
                'valor': 'VALR_ADICIONADO_OPERACAO',
                'data_emissao': 'DATA_EMISSAO_DOCUMENTO',
                'tipo_apropriacao': 'INDI_APROP',
                'tipo_documento': 'CODG_TIPO_DOC_PARTCT_CALC',
                'id_processamento': 'ID_PROCESM_INDICE',
                'motivo_exclusao': 'CODG_MOTIVO_EXCLUSAO_CALCULO',
                'data_embarque': 'NUMR_REFERENCIA_DOCUMENTO',
                # 'CODG_MUNICIPIO_SAIDA': 'CODG_MUNICIPIO_SAIDA',
            })

            # Transforma para um formato de 'lista de dicionários'
            values = df.to_dict('records')

            # Cria uma representação da tabela do banco
            meta = MetaData()
            table = Table('IPM_DOCUMENTO_PARTCT_CALC_IPM', meta,
                          Column('CODG_DOCUMENTO_PARTCT_CALCULO', Integer),
                          Column('VALR_ADICIONADO_OPERACAO', Double),
                          Column('DATA_EMISSAO_DOCUMENTO', Date),
                          Column('INDI_APROP', String(1)),
                          Column('CODG_TIPO_DOC_PARTCT_CALC', Integer),
                          Column('ID_PROCESM_INDICE', Integer),
                          Column('CODG_MOTIVO_EXCLUSAO_CALCULO', Integer),
                          Column('NUMR_REFERENCIA_DOCUMENTO', Integer),
                          Column('CODG_MUNICIPIO_SAIDA', Integer),
                          schema='IPM')

            # Grava os registros no banco
            loga_mensagem('Gravando na tabela IPM_DOCUMENTO_PARTCT_CALC_IPM.')
            i, j, x = 0, self._step, len(values)
            while i < x:
                loga_mensagem(f'Gravando registros de {i} até {min(j, x)}.')
                stmt = table.insert()
                result = conn.execute(stmt, values[i:j])

                i += self._step
                j += self._step

            conn.commit()

        except Exception as err:
            etapaProcess += " - ERRO - "
            loga_mensagem_erro(etapaProcess, err)
            raise
