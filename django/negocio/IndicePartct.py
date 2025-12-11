from datetime import datetime

import pandas as pd

from funcoes import utilitarios
from funcoes.constantes import EnumTipoProcessamento, EnumStatusProcessamento, EnumTipoIndice
from funcoes.utilitarios import LOCAL_TZ, ultimo_dia_mes, loga_mensagem, loga_mensagem_erro
from negocio.Procesm import ProcesmClasse
from persistencia.Oraprd import Oraprd
from polls.models import Processamento


class IndicePartctClass:
    etapaProcess = f"class {__name__} - class IndicePartct."
    # loga_mensagem(etapaProcess)

    negProcesm = ProcesmClasse()

    def __init__(self, p_ano_ref, p_etapa_indice):
        etapaProcess = f"class {self.__class__.__name__} - def IndicePartctClass. Referencia {p_ano_ref} - Tipo do Índice - {p_etapa_indice}"

        self.codigo_retorno = 0
        self.i_ano_ref = p_ano_ref
        self.s_etapa_indice = p_etapa_indice


    def calcula_indice_partct(self) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def calcula_indice_partct - Referencia {self.i_ano_ref} - Tipo do Índice - {self.s_etapa_indice}"
        # loga_mensagem(etapaProcess)

        df = []
        dData_hora_inicio = datetime.now()
        sData_hora_inicio = datetime.now(LOCAL_TZ).strftime('%d/%m/%Y %H:%M:%S,%f')

        d_data_inicio = datetime.strptime(str(self.i_ano_ref * 10000 + 101), '%Y%m%d').date()
        d_data_fim = datetime.strptime(str(self.i_ano_ref * 10000 + 1231), '%Y%m%d').date()

        try:
            # Registra inicio do processamento #
            etapaProcess = f'Registra inicio do processamento de cálculo do índice de participação. Referencia {self.i_ano_ref} - Tipo do Índice - {self.s_etapa_indice}'
            db = ProcesmClasse()
            procesm = db.iniciar_processamento(d_data_inicio, d_data_fim, EnumTipoProcessamento.processaIndicePartct.value)
            del db
            etapaProcess = f'{procesm.id_procesm_indice} Inicio do cálculo. Referencia {self.i_ano_ref} - {self.s_etapa_indice} - {sData_hora_inicio}'
            loga_mensagem(etapaProcess)

            df_adic = []
            try:
                # Carrega os NF-es recebidas do período informado #
                data_hora_atividade = datetime.now()
                df_indice = self.carrega_documentos_calculo(procesm)
                df_indice['numr_ano_ref'] = self.i_ano_ref
                df_indice['id_procesm_indice'] = procesm.id_procesm_indice
                df_indice['indi_etapa_indice_partcp'] = self.s_etapa_indice
                df_indice['tipo_indice_partcp'] = EnumTipoIndice.CemPorCento.value
                df_indice['desc_motivo_geracao'] = 'Geração do ìndice 100%'

                loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_indice)} linhas selecionadas. - ' + str(datetime.now() - data_hora_atividade))

                if len(df_indice) > 0:
                    # Exclui dados de processamentos anteriores #
                    self.exclui_dados_anteriores_indice(procesm)

                    # Grava tabela de valores adicionados #
                    linhas_gravadas = self.grava_indice_partct(procesm, df_indice)

                    etapaProcess = f'{procesm.id_procesm_indice} Cálculo do valor adicionado para a referencia {self.i_ano_ref} finalizado. Carregadas {len(df_adic)} linhas.'
                    procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                    procesm.stat_procesm_indice = EnumStatusProcessamento.sucesso.value
                    self.registra_processamento(procesm, etapaProcess)
                    self.codigo_retorno = 1

                else:
                    etapaProcess = f'{procesm.id_procesm_indice} Cálculo do Valor Adicionado ({self.i_ano_ref}) finalizado. Não foram selecionados documentos para cálculo na referencia.'
                    procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                    procesm.stat_procesm_indice = EnumStatusProcessamento.sucessoSemDados.value
                    self.registra_processamento(procesm, etapaProcess)
                    self.codigo_retorno = 1

                # param = ParamClasse()
                # param.atualizar_parametro(EnumParametros.ultimoProcessIndice.value, self.i_ano_ref)

            except Exception as err:
                etapaProcess += " - ERRO - " + str(err)
                loga_mensagem_erro(etapaProcess)
                procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                procesm.stat_procesm_indice = EnumStatusProcessamento.erro.value
                self.registra_processamento(procesm, etapaProcess)
                self.codigo_retorno = 9
                raise

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            self.codigo_retorno = 9
            raise

        sData_hora_final = datetime.now(LOCAL_TZ).strftime('%d/%m/%Y %H:%M:%S,%f')
        loga_mensagem(
            f'{procesm.id_procesm_indice} Fim do Cálculo do Índice de Participação - {self.i_ano_ref} - {sData_hora_final} - Tempo de processamento: {(datetime.now() - dData_hora_inicio)}.')
        loga_mensagem(' ')

        return str(self.codigo_retorno)

    def carrega_documentos_calculo(self, procesm) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def carrega_documentos_calculo - Referencia {self.i_ano_ref}"
        # loga_mensagem(etapaProcess)

        df = []

        try:
            etapaProcess = f'{procesm.id_procesm_indice} ' f'Busca documentos para processamento - Referencia {self.i_ano_ref}'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            db = Oraprd()  # Persistência utilizando o Django
            df = db.select_docs_calculo_indice(self.i_ano_ref)
            del db

            if len(df) > 0:
                return df
            else:
                return []

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def grava_indice_partct(self, procesm, df):
        etapaProcess = f"class {self.__class__.__name__} - def grava_indice_partct."
        # loga_mensagem(etapaProcess)

        linhas_gravadas = 0

        try:
            etapaProcess = f'{procesm.id_procesm_indice} Grava Índice Partct - {utilitarios.formatar_com_espacos(len(df), 11)} linhas.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            if len(df) > 0:
                db = Oraprd()
                linhas_gravadas = db.insert_indice_partct(df)
                del db

            loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_gravadas} Docs Partct gravados - ' + str(datetime.now() - data_hora_atividade))

            return linhas_gravadas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def exclui_dados_anteriores_indice(self, procesm: Processamento):
        etapaProcess = f"class {self.__class__.__name__} - def exclui_dados_anteriores_va. Referencia {self.i_ano_ref}"
        # loga_mensagem(etapaProcess)

        try:
            # Exclui dados de processamentos anteriores #
            etapaProcess = f'{procesm.id_procesm_indice} Exclui Valores Adicionados - Ref: {self.i_ano_ref}'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            db = Oraprd()
            linhas_excluidas = db.delete_indice_partct(self.i_ano_ref, EnumTipoIndice.CemPorCento.value, self.s_etapa_indice)
            del db

            loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_excluidas} linhas excluidas - ' + str(
                datetime.now() - data_hora_atividade))

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def registra_processamento(self, procesm, etapa):
        etapaProcess = f"class {__name__} - def registra_processamento - {etapa}."
        # loga_mensagem(etapaProcess)

        try:
            loga_mensagem(etapa)
            procesm.desc_observacao_procesm_indice = etapa
            self.negProcesm.atualizar_situacao_processamento(procesm)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise



