import pandas as pd

from datetime import date, datetime, timedelta, tzinfo

from persistencia.Connx import Connx
from persistencia.Oracle import Oracle
from funcoes.constantes import EnumTipoProcessamento, EnumStatusProcessamento
from funcoes.utilitarios import loga_mensagem, loga_mensagem_erro, LOCAL_TZ, baixa_csv
from negocio.Procesm import ProcesmClasse


class OpcaoSimplesSIMEI:
    def __init__(self):
        loga_mensagem('Instanciando classe Opção Simples-SIMEI')
        self._connx = Connx()
        self._oracle = Oracle()

        # Log do processamento
        self.negProcesm = ProcesmClasse()

    # Realiza a carga dos dados referentes às tabelas Simples e SIMEI do banco Adabas/Connx 
    # onde data te alteração seja maior ou igual a data_referencia_ini e menor que data_referencia_fim.
    def carga_simples_smei(self, data_referencia_ini: date, data_referencia_fim: date):
        etapaProcess = f"class {self.__class__.__name__} - def carga_simples_smei."
        # loga_mensagem(etapaProcess)

        assert data_referencia_ini
        assert data_referencia_fim

        try:
            # Loga na base o processamento
            tipo_processamento = EnumTipoProcessamento.importacaoOpcaoSimplesSIMEI.value
            # Uma vez que a data fim de referência é não inclusiva, preciso subtrair 1 da data antes de salvar o 
            # processamento para ficar de acordo com o padrão de datas do resto do sistema.
            data_referencia_fim_proc = data_referencia_fim
            if data_referencia_fim > data_referencia_ini:
                data_referencia_fim_proc = data_referencia_fim + timedelta(days=-1)
            self.processamento = self.negProcesm.iniciar_processamento(data_referencia_ini, data_referencia_fim_proc, tipo_processamento)

            # Transforma datas de referência para string no formato YYYYMMDD
            data_ref_ini = data_referencia_ini.strftime('%Y%m%d')
            data_ref_fim = data_referencia_fim.strftime('%Y%m%d')
            column_names = ['DATA_INICIO_OPCAO_SIMPLES', 'DATA_FIM_OPCAO_SIMPLES', 'NUMR_CNPJ_BASE_SIMPLES', 'DATA_ATUALIZ', 'NUMR_OPCAO', 'INDI_VALIDADE_INFORMACAO']

            loga_mensagem(f'Iniciando carga Opção Simples-SIMEI. Data de Referência: de {data_ref_ini} (Inclusive) até {data_ref_fim} (Not Inclusive).')

            # Atualiza o log na base de processamento
            self.processamento.desc_observacao_procesm_indice = f'Buscando informações do Simples na base Adabas/Connx.'
            self.negProcesm.atualizar_situacao_processamento(self.processamento)

            # Pega todas as atualizações do Simples
            loga_mensagem('Listando as informações do Simples.')
            df_simples = pd.DataFrame(self._connx.get_lista_simples(data_ref_ini, data_ref_fim), columns=column_names)
            df_simples['INDI_SIMPLES_SIMEI'] = 1

            # Atualiza o log na base de processamento
            self.processamento.desc_observacao_procesm_indice = f'Buscando informações do SIMEI na base Adabas/Connx.'
            self.negProcesm.atualizar_situacao_processamento(self.processamento)

            # Pega todas as atualizações do SIMEI
            loga_mensagem('Listando as informações do SIMEI.')
            df_simei = pd.DataFrame(self._connx.get_lista_simei(data_ref_ini, data_ref_fim), columns=column_names)
            df_simei['INDI_SIMPLES_SIMEI'] = 2

            loga_mensagem('Juntando os dataframes Simples e SIMEI.')
            # Junta as informações do Simples e SIMEI em um único dataframe
            df = pd.concat([df_simples, df_simei], ignore_index=True)

            # Atualiza o log na base de processamento
            self.processamento.desc_observacao_procesm_indice = f'Buscando informações de Inscrição Estadual no CCE.'
            self.negProcesm.atualizar_situacao_processamento(self.processamento)

            # Pega todas as informações de inscrições baseado nos CNPJs
            loga_mensagem('Buscando as informações de Inscrição Estadual.')

            # Converte os CNPJs pra string e preenche com zeros a esquerda
            df['NUMR_CNPJ_BASE_SIMPLES'] = df['NUMR_CNPJ_BASE_SIMPLES'].apply(lambda cnpj : str(cnpj).zfill(8))
            # Pega uma lista de CPNJs únicos
            cnpjs = df['NUMR_CNPJ_BASE_SIMPLES'].unique()
            inscricoes = []

            # Realiza a consulta das Inscrições em chunks
            size = len(cnpjs)
            step = 1000
            loga_mensagem(f'Total de CNPJs únicos: {size}.')
            for start in range(0, size, step):
                end = start + step
                end = min(end, size)

                # loga_mensagem(f'Buscando inscrições para os CNPJs de {start + 1} à {end}.')
                inscricoes_chunk = self._oracle.get_inscricoes(cnpjs[start:end])
                for i in range(0, len(inscricoes_chunk)):
                    inscricoes.append(inscricoes_chunk[i])

            # Atualiza o log na base de processamento
            self.processamento.desc_observacao_procesm_indice = f'Juntando informações de Inscrição Estadual com os dados do Adabas.'
            self.negProcesm.atualizar_situacao_processamento(self.processamento)

            loga_mensagem('Finalizando ajustes no conjunto de dados para inclusão no banco.')
            # Converte as inscrições retornadas do banco para um dataframe
            df_inscricoes = pd.DataFrame(inscricoes, columns=['NUMR_INSCRICAO_SIMPLES','NUMR_CNPJ_BASE_SIMPLES'])

            # Junta os dois dataframes, adicionando a coluna com o número de inscrição
            # O 'right_join' serve para já eliminar os registros que não possuem inscrição estadual

            df_inscricoes.name = 'df_inscricoes'
            baixa_csv(df_inscricoes)

            df.name = 'df'
            baixa_csv(df)

            df = df.join(df_inscricoes.set_index('NUMR_CNPJ_BASE_SIMPLES'), on='NUMR_CNPJ_BASE_SIMPLES', how='left')

            df.name = 'df_join'
            baixa_csv(df)

            # Altera o valor da coluna INDI_VALIDADE_INFORMACAO para atender a regra de Validade das Informações
            # If TIPO_REGISTRO = 5 AND DATA_EFEITO_FIM IS NULL, return 'N', else return 'S'
            df['INDI_VALIDADE_INFORMACAO'] = df['INDI_VALIDADE_INFORMACAO'].map(lambda i : 'N' if int(i) == 1 else 'S')

            df['NUMR_INSCRICAO_SIMPLES'] = df['NUMR_INSCRICAO_SIMPLES'].fillna(0)

            # Seta datas com valor '0' como NAs e converte para data
            df['DATA_FIM_OPCAO_SIMPLES'] = df['DATA_FIM_OPCAO_SIMPLES'].map(lambda d : pd.NA if d == 0 else converte_datas(d))

            # Converte campos do tipo data
            df['DATA_INICIO_OPCAO_SIMPLES'] = df['DATA_INICIO_OPCAO_SIMPLES'].map(converte_datas)
            df['DATA_ATUALIZ'] = df['DATA_ATUALIZ'].map(converte_datas)

            size = len(df)
            loga_mensagem(f'Total de registros a serem salvos: {size}.')
            # Só executa a etapa de salvar os dados se houver dados a salvar.
            if size > 0:
                # Atualiza o log na base de processamento
                self.processamento.desc_observacao_procesm_indice = f'Salvando dados no banco.'
                self.negProcesm.atualizar_situacao_processamento(self.processamento)

                # Realiza essa etapa em chunks
                step = 10000
                for start in range(0, size, step):
                    end = start + step
                    end = min(end, size)
                    df_temp = df.iloc[start:end]

                    # Inclui os dados que não existem ainda...
                    # (Tenta incluir todos os registros ignorando os que já possuem NUMR_OPCAO repetido)
                    # loga_mensagem(f'Incluindo registros de {start + 1} à {end}.')
                    self._oracle.inserir_opcao_simples_simei(df_temp, step)
                    
                    # ... e depois atualiza todos os registros no banco.
                    # loga_mensagem(f'Alterando registros de {start + 1} à {end}.')
                    self._oracle.alterar_opcao_simples_simei(df_temp)
                
                self.processamento.stat_procesm_indice = EnumStatusProcessamento.sucesso.value
            else:
                loga_mensagem('Não há dados a salvar.')
                self.processamento.stat_procesm_indice = EnumStatusProcessamento.sucessoSemDados.value
            
            self.processamento.desc_observacao_procesm_indice = f'Carga finalizada. Registros do Simples: {len(df_simples)}; Registros do SIMEI {len(df_simei)}; Registros Gravados {len(df)}.'
            self.processamento.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
            self.negProcesm.atualizar_situacao_processamento(self.processamento)

            loga_mensagem('Carga de Opção Simples-SIMEI finalizada.')

            return 0
        except Exception as err:
            loga_mensagem_erro('Erro durante carga de Opção Simples-SIMEI.', err)
            # Atualiza o log na base de processamento
            self.processamento.stat_procesm_indice = EnumStatusProcessamento.erro.value
            self.processamento.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
            self.negProcesm.atualizar_situacao_processamento(self.processamento)
            return 9

# Caso o dado que venha do banco esteja inconsistente,
# retorna uma data fixa válida em 1900-01-01
def converte_datas(data_val):
    try:
        return datetime.strptime(f'{data_val}', '%Y%m%d').date()
    except ValueError:
        loga_mensagem_erro(f'AVISO: Data "{data_val}" é inválida. Setando data padrão (1900-01-01).')
        return date(1900, 1, 1)
