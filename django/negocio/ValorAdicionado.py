import os
from datetime import datetime

import pandas as pd

from funcoes import utilitarios
from funcoes.constantes import EnumTipoProcessamento, EnumStatusProcessamento, EnumParametros, EnumMotivoExclusao
from funcoes.utilitarios import *
from negocio.DocPartct import DocPartctClasse
from negocio.GEN import GenClass
from negocio.ItemDoc import ItemDocClasse
from negocio.Param import ParamClasse
from negocio.Procesm import ProcesmClasse
from persistencia.Oraprd import Oraprd
from persistencia.Oraprodx9 import Oraprodx9
from polls.models import Processamento


class VAClass:
    etapaProcess = f"class {__name__} - class ValorAdicionado."
    # loga_mensagem(etapaProcess)

    negProcesm = ProcesmClasse()

    def __init__(self, p_data_ref_inicio, p_data_ref_fim, p_data_inicio, p_data_fim, p_periodicidade):
        etapaProcess = f"class {self.__class__.__name__} - def VAClass. Referencia {p_data_inicio} a {p_data_fim}"

        self.codigo_retorno = 0
        self.d_data_ref_inicio = p_data_ref_inicio
        self.d_data_ref_fim = p_data_ref_fim
        self.d_data_inicio = p_data_inicio
        self.d_data_fim = p_data_fim
        self.i_ref_inicio = p_data_inicio.year * 100 + p_data_inicio.month
        self.i_ref_fim = p_data_fim.year * 100 + p_data_fim.month

        try:
            # if pd.to_datetime(p_data_inicio.dt.strftime("%Y%m") <> pd.to_datetime(p_data_fim.dt.strftime("%Y%m"):
            if p_periodicidade == 'Mensal':
                if p_data_inicio.year != p_data_fim.year \
                or p_data_inicio.month != p_data_fim.month:
                    etapaProcess += f" - Período inválido!"
                    loga_mensagem_erro(etapaProcess)
                    raise 9
            elif p_periodicidade == 'Anual':
                if p_data_inicio.year != p_data_fim.year:
                    etapaProcess += f" - Período inválido!"
                    loga_mensagem_erro(etapaProcess)
                    raise 9

            self.i_mes_ref = p_data_inicio.year * 100 + p_data_inicio.month

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def processa_arquivo_valor_adicionado(self, df):
        etapaProcess = f"class {self.__class__.__name__} - def processa_arquivo_valor_adicionado - Periodo {self.d_data_inicio} a {self.d_data_fim}"
        # loga_mensagem(etapaProcess)

        df = []
        dData_hora_inicio = datetime.now()
        sData_hora_inicio = datetime.now(LOCAL_TZ).strftime('%d/%m/%Y %H:%M:%S,%f')

        try:
            # Registra inicio do processamento #
            etapaProcess = f'Registra inicio do processamento de arquivo externo de valores adicionados.'
            db = ProcesmClasse()
            procesm = db.iniciar_processamento(self.d_data_inicio, self.d_data_fim, EnumTipoProcessamento.importacaoArquivoVA.value)
            del db
            etapaProcess = f'{procesm.id_procesm_indice} Inicio da carga de arquivo externo de VA. Periodo {self.d_data_inicio} a {self.d_data_fim} - {sData_hora_inicio}'
            loga_mensagem(etapaProcess)

            df['id_procesm_indice'] = procesm.id_procesm_indice
            df.rename(columns={'tipo_documento': 'codg_tipo_doc_partct_calc',
                               'referencia': 'refe_valor_adicionado',
                               'tipo_apropriacao': 'tipo_aprop_valor_adicionado',
                               'valor_adicionado': 'valr_adicionado',
                               'codigo_municipio': 'codg_municipio',
                               'num_inscricao': 'numr_inscricao',}, inplace=True)

            linhas_gravadas = self.grava_valor_adicionado(procesm, df)

            if len(df) > 0:
                return df
            else:
                return []

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def calcula_valor_adicionado(self) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def carga_dados_calculo - Periodo {self.d_data_inicio} a {self.d_data_fim}"
        # loga_mensagem(etapaProcess)

        df = []
        dData_hora_inicio = datetime.now()
        sData_hora_inicio = datetime.now(LOCAL_TZ).strftime('%d/%m/%Y %H:%M:%S,%f')

        try:
            # Registra inicio do processamento #
            etapaProcess = f'Registra inicio do processamento de cálculo do valor adicionado. Periodo de {self.d_data_inicio} a {self.d_data_fim}'
            db = ProcesmClasse()
            procesm = db.iniciar_processamento(self.d_data_inicio, self.d_data_fim, EnumTipoProcessamento.processaValorAdicionado.value)
            del db
            etapaProcess = f'{procesm.id_procesm_indice} Inicio da carga do cálculo do valor adicionado. Periodo {self.d_data_inicio} a {self.d_data_fim} - {sData_hora_inicio}'
            loga_mensagem(etapaProcess)

            df_adic = []
            try:
                # Carrega os documentos recebidos do período informado #
                data_hora_atividade = datetime.now()
                df_adic = self.carrega_documentos_calculo(procesm)
                df_adic['id_procesm_indice'] = procesm.id_procesm_indice

                loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_adic)} linhas selecionadas. - ' + str(datetime.now() - data_hora_atividade))

                if len(df_adic) > 0:
                    # Exclui dados de processamentos anteriores #
                    self.exclui_dados_anteriores_va(procesm)

                    # Grava tabela de valores adicionados #
                    linhas_gravadas = self.grava_valor_adicionado(procesm, df_adic)

                    etapaProcess = f'{procesm.id_procesm_indice} Cálculo do valor adicionado para a referencia {self.i_mes_ref} finalizado. Carregadas {len(df_adic)} linhas.'
                    procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                    procesm.stat_procesm_indice = EnumStatusProcessamento.sucesso.value
                    self.registra_processamento(procesm, etapaProcess)
                    self.codigo_retorno = 1

                else:
                    etapaProcess = f'{procesm.id_procesm_indice} Cálculo do Valor Adicionado ({self.i_mes_ref}) finalizado. Não foram selecionados documentos para cálculo na referencia.'
                    procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                    procesm.stat_procesm_indice = EnumStatusProcessamento.sucessoSemDados.value
                    self.registra_processamento(procesm, etapaProcess)
                    self.codigo_retorno = 1

                param = ParamClasse()
                param.atualizar_parametro(EnumParametros.ultimoProcessVA.value, self.i_mes_ref)

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
            f'{procesm.id_procesm_indice} Fim do Cálculo do Valor Adicionado. Periodo {self.d_data_inicio} a {self.d_data_fim} - {sData_hora_final} - Tempo de processamento: {(datetime.now() - dData_hora_inicio)}.')
        loga_mensagem(' ')

        return str(self.codigo_retorno)

    def carrega_documentos_calculo(self, procesm) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def carrega_documentos_calculo - Periodo {self.d_data_inicio} a {self.d_data_fim}"
        # loga_mensagem(etapaProcess)

        df = []

        try:
            etapaProcess = f'{procesm.id_procesm_indice} ' f'Busca documentos para processamento - Periodo {self.d_data_inicio} a {self.d_data_fim}'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            db = Oraprd()  # Persistência utilizando o Django
            df = db.select_docs_calculo_va(self.d_data_inicio, self.d_data_fim)
            del db

            if len(df) > 0:
                return df
            else:
                return []

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def grava_valor_adicionado(self, procesm, df):
        etapaProcess = f"class {self.__class__.__name__} - def grava_valor_adicionado."
        # loga_mensagem(etapaProcess)

        linhas_gravadas = 0

        try:
            etapaProcess = f'{procesm.id_procesm_indice} Grava Índice Partct - {utilitarios.formatar_com_espacos(len(df), 11)} linhas.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            if len(df) > 0:
                db = Oraprd()
                linhas_gravadas = db.insert_valor_adicionado(df)
                del db

            loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_gravadas} Docs Partct gravados - ' + str(datetime.now() - data_hora_atividade))

            return linhas_gravadas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def exclui_dados_anteriores_va(self, procesm: Processamento):
        etapaProcess = f"class {self.__class__.__name__} - def exclui_dados_anteriores_va. Referencia {self.i_mes_ref}"
        # loga_mensagem(etapaProcess)

        try:
            # Exclui dados de processamentos anteriores #
            etapaProcess = f'{procesm.id_procesm_indice} Exclui Valores Adicionados - Ref: {self.i_mes_ref}'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            db = Oraprd()
            linhas_excluidas = db.delete_valor_adicionado(self.i_mes_ref)
            del db

            loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_excluidas} linhas excluidas - ' + str(
                datetime.now() - data_hora_atividade))

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def calcula_valor_adicionado_ant(self) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def calcula_valor_adicionado - {self.inicio_referencia} a {self.fim_referencia}."
        # loga_mensagem(etapaProcess)

        try:
            df_imob = self.carrega_ativo_imobilizado()

            df_imob.name = 'df_imob'
            baixa_csv(df_imob)

            df_opostas = self.operacoes_duplicadas()

            df_opostas_dup = df_opostas.merge(df_opostas,
                                              left_on=['numr_inscricao_entrada', 'numr_inscricao_saida'],
                                              right_on=['numr_inscricao_saida', 'numr_inscricao_entrada'],
                                              suffixes=('_entrada', '_saida')
                                              )

            # Ordenar as colunas 'numr_inscricao_entrada' e 'numr_inscricao_saida' para tratar a inversão
            df_opostas_dup['ie_pair'] = df_opostas_dup.apply(
                lambda row: tuple(sorted([row['numr_inscricao_entrada_entrada'], row['numr_inscricao_saida_entrada']])), axis=1)

            # Eliminar duplicatas baseadas nos pares ordenados
            df_opostas = df_opostas_dup.drop_duplicates(subset='ie_pair').drop(columns='ie_pair').reset_index(drop=True)

            df_opostas.drop(columns=['numr_inscricao_entrada_saida', 'numr_inscricao_saida_saida'], inplace=True)
            df_opostas.rename(columns={'numr_inscricao_entrada_entrada': 'numr_inscricao_entrada'}, inplace=True)
            df_opostas.rename(columns={'numr_inscricao_saida_entrada': 'numr_inscricao_saida'}, inplace=True)

            loga_mensagem(etapaProcess)

            df_opostas.name = 'df_opostas'
            baixa_csv(df_opostas)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def operacoes_duplicadas(self, p_arq) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def operacoes_duplicadas."
        # loga_mensagem(etapaProcess)

        s_tipo_procesm = EnumTipoProcessamento.processaOperProdRurais.value

        dData_hora_inicio = datetime.now()
        sData_hora_inicio = datetime.now(LOCAL_TZ).strftime('%d/%m/%Y %H:%M:%S,%f')

        try:
            # Registra inicio do processamento #
            etapaProcess = f'Registra inicio do processamento de exclusão de operações duplicadas entre contribuintes.'
            procesm = self.negProcesm.iniciar_processamento(self.d_data_inicio, self.d_data_fim, s_tipo_procesm)
            etapaProcess = f'{procesm.id_procesm_indice} Inicio da exclusão das operações duplicadas. Período de {self.d_data_inicio} a {self.d_data_fim} - {sData_hora_inicio}'
            loga_mensagem(etapaProcess)

            # Busca operações entre produtores rurais
            etapaProcess = f'{procesm.id_procesm_indice} Busca operações entre contribuintes. '
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            # df_contrib = self.carrega_operacoes_entre_contribuintes()
            df_contrib = [0]
            loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_contrib)} linhas selecionadas. - ' + str(datetime.now() - data_hora_atividade))

            if len(df_contrib) > 0:
                # Resume contribuintes que fizeram transações entre si
                etapaProcess = f'{procesm.id_procesm_indice} Resume contribuintes que fizeram transações entre si. '
                self.registra_processamento(procesm, etapaProcess)
                data_hora_atividade = datetime.now()

                # transacoes_bi_direcionais = self.processar_em_lotes(df_contrib)
                # transacoes_bi_direcionais.name = 'df_direc'
                # baixa_csv(transacoes_bi_direcionais)

                # transacoes_bi_direcionais = sobe_csv('df_direc.csv')

                # loga_mensagem(etapaProcess + f' Processo finalizado. {len(transacoes_bi_direcionais)} operações selecionadas. - ' + str(datetime.now() - data_hora_atividade))

                # Percorre dataframe com pares, buscando documentos envolvidos
                resultados = []

                # # Resume contribuintes que fizeram transações entre si
                # etapaProcess = f'{procesm.id_procesm_indice} Identifica operações opostas entre contribuintes. '
                # self.registra_processamento(procesm, etapaProcess)
                # data_hora_atividade = datetime.now()
                #
                # # Criar dicionário: (IeSai, UfIeSai) → [(IeEnt, UfIeEnt), (..), ...]
                # p_inscrs = (transacoes_bi_direcionais.drop_duplicates(subset=["IeSai", "UfIeSai", "IeEnt", "UfIeEnt"])
                #                                      .groupby(["IeSai", "UfIeSai"])[["IeEnt", "UfIeEnt"]]
                #                                      .apply(lambda g: list(zip(g["IeEnt"], g["UfIeEnt"])))
                #                                      .to_dict()
                #             )
                #
                # loga_mensagem(etapaProcess + f' Processo finalizado. {len(p_inscrs)} pares de contribuintes selecionados. - ' + str(datetime.now() - data_hora_atividade))

                etapaProcess = f'{procesm.id_procesm_indice} Busca documentos fiscais com operações opostas.'
                self.registra_processamento(procesm, etapaProcess)
                data_hora_atividade = datetime.now()

                p_inscrs = []
                linhas_docs_alteradas, linhas_itens_alteradas = self.carrega_docs_oper_entre_contribuintes(p_inscrs, procesm, p_arq)

                loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_docs_alteradas} documentos selecionados. - ' + str(datetime.now() - data_hora_atividade))

                etapaProcess = f'{procesm.id_procesm_indice} Operações opostas registradas. {linhas_docs_alteradas} documentos atualizados - {linhas_itens_alteradas} itens atualizados.'
                procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                procesm.stat_procesm_indice = EnumStatusProcessamento.sucesso.value
                self.registra_processamento(procesm, etapaProcess)

            return linhas_docs_alteradas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def processar_em_lotes(self, df, bucket_size=1_500_000):
        etapaProcess = f"class {self.__class__.__name__} - def processar_em_lotes."
        loga_mensagem(etapaProcess)
        resultados = []

        for start in range(0, len(df), bucket_size):
            end = start + bucket_size
            bucket = df.iloc[start:end].copy()

            # Conversões de tipos para economizar memória
            bucket['ufieent'] = bucket['ufieent'].astype('category')
            bucket['ufiesai'] = bucket['ufiesai'].astype('category')
            bucket['ieent'] = pd.to_numeric(bucket['ieent'], downcast='integer')
            bucket['iesai'] = pd.to_numeric(bucket['iesai'], downcast='integer')

            # Cria os pares
            bucket['IeSai'] = list(zip(bucket['iesai'], bucket['ufiesai']))
            bucket['IeEnt'] = list(zip(bucket['ieent'], bucket['ufieent']))

            # Ordena os pares
            bucket['contribA'] = bucket[['IeSai', 'IeEnt']].min(axis=1)
            bucket['contribB'] = bucket[['IeSai', 'IeEnt']].max(axis=1)

            # Identifica sentido
            bucket['sentido'] = (bucket['IeSai'] == bucket['contribA']).map({True: 'a->b', False: 'b->a'})

            # Contagem
            contagens = (bucket.groupby(['contribA', 'contribB', 'sentido']).size().reset_index(name='contagem'))

            resultados.append(contagens)

            # Libera memória do bucket
            del bucket

        # Consolida resultados dos buckets
        consolidados = pd.concat(resultados, ignore_index=True)

        # Reagrupar para consolidar contagens entre buckets
        consolidados = (
            consolidados.groupby(['contribA', 'contribB', 'sentido'])['contagem']
            .sum()
            .reset_index()
        )

        # Pivotar
        contagens_pivot = consolidados.pivot(
            index=['contribA', 'contribB'],
            columns='sentido',
            values='contagem'
        ).reset_index()

        contagens_pivot = contagens_pivot.fillna(0)

        # Filtra bidirecionais
        contagens_pivot['sentidos'] = (contagens_pivot['a->b'] > 0) & (contagens_pivot['b->a'] > 0)
        transacoes_bi_direcionais = contagens_pivot[contagens_pivot['sentidos']].copy()

        # Divide contribA e contribB
        transacoes_bi_direcionais['IeSai'] = transacoes_bi_direcionais['contribA'].str[0]
        transacoes_bi_direcionais['UfIeSai'] = transacoes_bi_direcionais['contribA'].str[1]
        transacoes_bi_direcionais['IeEnt'] = transacoes_bi_direcionais['contribB'].str[0]
        transacoes_bi_direcionais['UfIeEnt'] = transacoes_bi_direcionais['contribB'].str[1]

        return transacoes_bi_direcionais

    def carrega_operacoes_entre_contribuintes(self) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def carrega_operacoes_entre_contribuintes - {self.d_data_inicio} a {self.d_data_fim}."
        # loga_mensagem(etapaProcess)

        s_tipo_procesm = EnumTipoProcessamento.processaOperProdRurais.value

        dData_hora_inicio = datetime.now()
        sData_hora_inicio = datetime.now(LOCAL_TZ).strftime('%d/%m/%Y %H:%M:%S,%f')

        linhas_alteradas = 0

        try:
            # Registra inicio do processamento #
            etapaProcess = f'Registra inicio do processamento de Operações Opostas.'
            procesm = self.negProcesm.iniciar_processamento(self.d_data_inicio, self.d_data_fim, s_tipo_procesm)
            etapaProcess = f'{procesm.id_procesm_indice} Inicio do registro das operações opostas. Período de {self.d_data_inicio} a {self.d_data_fim} - {sData_hora_inicio}'
            loga_mensagem(etapaProcess)

            # Exclui indicador de operações opostas
            etapaProcess = f'{procesm.id_procesm_indice} Exclui operações opostas. '
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()
            # self.exclui_dados_operacoes_entre_contribuintes(self.d_data_inicio, self.d_data_fim, procesm)

            # Busca operações de ativo imobilizado
            etapaProcess = f'{procesm.id_procesm_indice} Busca operações opostas. '
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            gen = GenClass()
            df_data_proc = gen.carrega_periodo_processamento(self.d_data_inicio.strftime("%Y%m%d"), self.d_data_fim.strftime("%Y%m%d"))
            del gen

            total_datas = len(df_data_proc)

            if df_data_proc is not None:
                db = Oraprodx9()
                resultados = []

                for idx, row in df_data_proc.iterrows():
                    df_hora_proc = dividir_dia_em_horas(row.data_com_traco + ' 00:00:00', 1)

                    for x, hora in df_hora_proc.iterrows():
                        loga_mensagem(f"{etapaProcess} - Progresso: {idx + 1}/{total_datas} linhas ({(idx + 1) / total_datas:.0%})")

                        df_opostas_dup = db.select_operacoes_contribuintes(hora.dDataInicial, hora.dDataFinal).drop_duplicates()
                        if len(df_opostas_dup) > 0:
                            resultados.append(df_opostas_dup)

                if len(resultados) > 0:
                    df_resultado = pd.concat(resultados, ignore_index=True)
                    df_resultado.name = 'df_resultado'
                    baixa_csv(df_resultado)
                else:
                    df_resultado = pd.DataFrame()

                del db
                return df_resultado

            else:
                etapaProcess = f'Período para acerto do cadastro com problemas. Período informado: {self.d_data_inicio} a {self.d_data_fim}.'
                loga_mensagem_erro(etapaProcess)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def carrega_docs_oper_entre_contribuintes(self, p_inscrs, procesm, p_arq):
        etapaProcess = f"class {self.__class__.__name__} - def carrega_docs_oper_entre_contribuintes - {self.d_data_inicio} a {self.d_data_fim}."
        # loga_mensagem(etapaProcess)

        try:
            resultados = []
            batch_size = 1
            qdade_documentos_tot = 0
            qdade_itens_tot = 0

            # p_inscrs_list = [(ie_sai, uf_sai, ie_ent, uf_ent)
            #                  for (ie_sai, uf_sai), lista_ent in p_inscrs.items()
            #                  for (ie_ent, uf_ent) in lista_ent
            #                  ]

            df = sobe_csv(f'p_inscrs_list{p_arq}.csv', ',')
            p_inscrs_list = list(df.itertuples(index=False, name=None))

            qdade_documentos = 0
            for i in range(0, len(p_inscrs_list), batch_size):
                l_inscrs = p_inscrs_list[i:i + batch_size]
                loga_mensagem(f'{procesm.id_procesm_indice} {etapaProcess} Processando o lote de inscrições {i}. Inscrição inicial: {l_inscrs[0]} - Inscrição final: {l_inscrs[-1]}')

                db = Oraprodx9()
                df_docs = db.select_operacoes_opostas(self.i_ref_inicio, self.i_ref_fim, l_inscrs)
                del db
                # df_docs = sobe_csv('df_docs.csv')

                loga_mensagem(f'{procesm.id_procesm_indice} Retornou da leitura dos documentos fiscais - Lidos {len(df_docs)} documentos.')
                if not df_docs.empty:
                    qdade_documentos, qdade_itens = self.processa_documentos_entre_contribuintes(df_docs, procesm)
                    qdade_documentos_tot =+ qdade_documentos
                    qdade_itens_tot = + qdade_itens

            return qdade_documentos_tot, qdade_itens_tot

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def processa_documentos_entre_contribuintes(self, docs, procesm):
        etapaProcess = f"class {self.__class__.__name__} - def processa_documentos_entre_contribuintes."
        # loga_mensagem(etapaProcess)

        linhas_docs_alteradas = 0
        linhas_itens_alteradas = 0

        try:
            etapaProcess = f'{procesm.id_procesm_indice} Processa os documentos fiscais com operações opostas.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            df_oposto = docs.groupby(['numr_inscricao_entrada',
                                      'codg_uf_entrada',
                                      'numr_inscricao_saida',
                                      'codg_uf_saida',
                                      'codg_cfop',
                                      'codg_produto_ncm']) \
                            .agg({'valr_adicionado': 'sum',
                                  'valr_total_bruto': 'sum',}).reset_index()

            # df_docs.name = 'df_docs'
            # baixa_csv(df_docs)

            # df_oposto.name = 'df_oposto'
            # baixa_csv(df_oposto)

            mapa_cfop = {5101: 1101,
                         5102: 1102,
                         5116: 1116,
                         5201: 1201,
                         5202: 1202,
                         5410: 1410,
                         6101: 2101,
                         6102: 2102,
                         6116: 2116,
                         6201: 2201,
                         6202: 2202,
                         6410: 2410, }

            # Criar mapa inverso e juntar os dois
            mapa_inverso = {v: k for k, v in mapa_cfop.items()}
            mapa_completo = {**mapa_cfop, **mapa_inverso}

            df_oposto['codg_cfop_corresp'] = df_oposto['codg_cfop'].map(mapa_completo)
            filtro = df_oposto['codg_cfop_corresp'].isna()

            df_opostas_dup = df_oposto.merge(df_oposto[~filtro],
                                             left_on=['numr_inscricao_entrada', 'codg_uf_entrada',
                                                      'numr_inscricao_saida', 'codg_uf_saida',
                                                      'codg_cfop', 'codg_produto_ncm'],
                                             right_on=['numr_inscricao_saida', 'codg_uf_saida',
                                                       'numr_inscricao_entrada', 'codg_uf_entrada',
                                                       'codg_cfop_corresp', 'codg_produto_ncm'],
                                             suffixes=('_1', '_2')
                                             )

            doc_selecionados = 0

            if len(df_opostas_dup) > 0:
                # Exclui e renomeia colunas
                df_opostas_dup.drop(columns={'codg_cfop_corresp_1', 'codg_cfop_corresp_2'}, inplace=True)

                # Identifica operações a serem excluídas
                df_opostas_dup['cfop_selecionado'] = np.where(
                    df_opostas_dup['valr_total_bruto_1'] >= df_opostas_dup['valr_total_bruto_2'],
                    df_opostas_dup['codg_cfop_2'],
                    df_opostas_dup['codg_cfop_1']
                    )

                # Agrupar por combinações únicas de parâmetros para evitar consultas duplicadas
                filtro = df_opostas_dup['cfop_selecionado'] == df_opostas_dup['codg_cfop_1']
                grupos = df_opostas_dup[filtro].groupby(['numr_inscricao_saida_1',
                                                         'codg_uf_saida_1',
                                                         'numr_inscricao_entrada_1',
                                                         'codg_uf_entrada_1',
                                                         'codg_produto_ncm',
                                                         'cfop_selecionado'
                                                         ]).size().reset_index()

                # Preparar lotes para atualização
                df_alt = pd.merge(docs,
                                  grupos,
                                  left_on=['numr_inscricao_entrada', 'codg_uf_entrada', 'numr_inscricao_saida',
                                           'codg_uf_saida', 'codg_cfop', 'codg_produto_ncm'],
                                  right_on=['numr_inscricao_entrada_1', 'codg_uf_entrada_1',
                                            'numr_inscricao_saida_1', 'codg_uf_saida_1', 'cfop_selecionado',
                                            'codg_produto_ncm'],
                                  how='left')

                # Somatório para apurar valor do documento
                df_alt['valr_doc'] = df_alt.groupby(['codg_documento_partct_calculo', 'codg_tipo_doc_partct_calc'])[
                    'valr_total_bruto'].transform('sum')

                # Criar uma coluna temporária com o valor a ser somado (0 quando numr_inscricao_saida_1 não for nulo)
                df_alt['valr_para_soma'] = df_alt['valr_total_bruto'].where(df_alt['numr_inscricao_saida_1'].isna(), 0)

                # Somatório para apurar valor do documento participante
                df_alt['total_doc'] = df_alt.groupby(['codg_documento_partct_calculo', 'codg_tipo_doc_partct_calc'])[
                    'valr_para_soma'].transform('sum')

                # Remover a coluna temporária
                df_alt = df_alt.drop('valr_para_soma', axis=1)

                # Documentos para alteração do valor adicionado
                filtro = df_alt['valr_doc'] != df_alt['total_doc']
                df_alt_doc = df_alt.loc[filtro, ['codg_documento_partct_calculo', 'codg_tipo_doc_partct_calc',
                                                 'total_doc']].drop_duplicates().copy()
                df_alt_doc['codg_motivo_exclusao_calculo'] = np.where(df_alt_doc['total_doc'] == 0, EnumMotivoExclusao.DocumentoNaoContemItemParticipante.value, None)
                df_alt_doc.rename(columns={'total_doc': 'valr_adicionado_operacao'}, inplace=True)

                # Itens para exclusão por operações opostas
                filtro = ~df_alt['numr_inscricao_saida_1'].isna()
                df_alt_item = df_alt.loc[
                    filtro, ['codg_documento_partct_calculo', 'codg_tipo_doc_partct_calc', 'codg_item_documento']]
                df_alt_item.rename(columns={'codg_tipo_doc_partct_calc': 'codg_tipo_doc_partct_documento'},
                                   inplace=True)

                if len(df_alt_doc) > 0:
                    doc_selecionados = len(df_alt_doc)
                    df_alt_doc.name = 'df_alt_doc'
                    baixa_csv(df_alt_doc)

                    df_alt_item.name = 'df_alt_item'
                    baixa_csv(df_alt_item)

            loga_mensagem(etapaProcess + f' Processo finalizado. {doc_selecionados} documentos selecionados. - ' + str(
                datetime.now() - data_hora_atividade))

            if doc_selecionados > 0:
                # Limpa indicadores de exclusão de documentos fiscais por operações opostas
                etapaProcess = f'{procesm.id_procesm_indice} Limpa indicadores de exclusão de documentos fiscais por operações opostas. '
                self.registra_processamento(procesm, etapaProcess)
                data_hora_atividade = datetime.now()

                # db = Oraprd()
                # db.delete_operacoes_duplicadas(self.d_data_inicio, self.d_data_fim)
                # del db

                loga_mensagem(etapaProcess + f' Processo finalizado - ' + str(datetime.now() - data_hora_atividade))

                etapaProcess = f'{procesm.id_procesm_indice} Atualiza documentos fiscais para serem retirados do cálculo. '
                self.registra_processamento(procesm, etapaProcess)
                data_hora_atividade = datetime.now()

                db = Oraprd()
                df_tipo = pd.DataFrame(df_alt_item['codg_tipo_doc_partct_documento'].drop_duplicates())
                for idx, row in df_tipo.iterrows():
                    filtroItem = df_alt_item['codg_tipo_doc_partct_documento'] == row.codg_tipo_doc_partct_documento
                    linhas_itens_alteradas += db.update_itens_operacoes_duplicadas(df_alt_item.loc[filtroItem,
                    'codg_item_documento'].tolist(),
                                                                                   row.codg_tipo_doc_partct_documento)

                    filtroDoc = df_alt_doc['codg_tipo_doc_partct_calc'] == row.codg_tipo_doc_partct_documento
                    linhas_docs_alteradas += db.update_docs_operacoes_duplicadas(df_alt_doc[filtroDoc], row.codg_tipo_doc_partct_documento)

                loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_docs_alteradas} documentos atualizados - {linhas_itens_alteradas} itens atualizados - ' + str(datetime.now() - data_hora_atividade))

            return linhas_docs_alteradas, linhas_itens_alteradas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise


    def regista_ativo_imobilizado(self):
        etapaProcess = f"class {self.__class__.__name__} - def carrega_ativo_imobilizado."
        loga_mensagem(etapaProcess)

        s_tipo_procesm = EnumTipoProcessamento.processaAtivoImobilizado.value

        dData_hora_inicio = datetime.now()
        sData_hora_inicio = datetime.now(LOCAL_TZ).strftime('%d/%m/%Y %H:%M:%S,%f')

        linhas_alteradas = 0

        try:
            # Registra inicio do processamento #
            etapaProcess = f'Registra inicio do processamento de ativos imobilizados.'
            procesm = self.negProcesm.iniciar_processamento(self.d_data_inicio, self.d_data_fim, s_tipo_procesm)
            etapaProcess = f'{procesm.id_procesm_indice} Inicio do registro das operações de ativos imobilizados. Período de {self.d_data_inicio} a {self.d_data_fim} - {sData_hora_inicio}'
            loga_mensagem(etapaProcess)

            # Busca operações de ativo imobilizado
            etapaProcess = f'{procesm.id_procesm_indice} Busca operações de ativos imobilizados. '
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            gen = GenClass()
            df_data_proc = gen.carrega_periodo_processamento(self.d_data_inicio.strftime("%Y%m%d"), self.d_data_fim.strftime("%Y%m%d"))
            del gen

            total_datas = len(df_data_proc)

            if df_data_proc is not None:
                db = Oraprd()
                for idx, row in df_data_proc.iterrows():
                    df_hora_proc = dividir_dia_em_horas(row.data_com_traco + ' 00:00:00', 1)
                    for x, hora in df_hora_proc.iterrows():
                        loga_mensagem(f"{etapaProcess} - Progresso: {idx + 1}/{total_datas} linhas ({(idx + 1) / total_datas:.0%})")

                        # self.exclui_dados_ativo_imobilizado(hora.dDataInicial, hora.dDataFinal, procesm)

                        df_ativ = self.carrega_operacoes_ativo_imob(self.d_data_ref_inicio, self.d_data_ref_fim, hora.dDataInicial, hora.dDataFinal)
                        if len(df_ativ) > 0:
                            df_ativ.name = 'df_ativ'
                            baixa_csv(df_ativ)

                            # Grava tabela de valores adicionados #
                            linhas_gravadas = self.inclui_dados_ativo_imobilizado(procesm, df_ativ)

                del db
            else:
                etapaProcess = f'Período para acerto do cadastro com problemas. Período informado: {self.d_data_inicio} a {self.d_data_fim}.'
                loga_mensagem_erro(etapaProcess)

            loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_ativ)} linhas selecionadas. - ' + str(datetime.now() - data_hora_atividade))

                #
                # if len(df_ativ) > 0:
                #     # Grava tabela de valores adicionados #
                #     linhas_gravadas = self.inclui_dados_ativo_imobilizado(procesm, df_ativ)
                #
                #     etapaProcess = f'{procesm.id_procesm_indice} Cálculo do valor adicionado para a referencia {self.i_mes_ref} finalizado. Carregadas {len(df_adic)} linhas.'
                #     procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                #     procesm.stat_procesm_indice = EnumStatusProcessamento.sucesso.value
                #     self.registra_processamento(procesm, etapaProcess)
                #     self.codigo_retorno = 1
                #
                # else:
                #     etapaProcess = f'{procesm.id_procesm_indice} Cálculo do Valor Adicionado ({self.i_mes_ref}) finalizado. Não foram selecionados documentos para cálculo na referencia.'
                #     procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                #     procesm.stat_procesm_indice = EnumStatusProcessamento.sucessoSemDados.value
                #     self.registra_processamento(procesm, etapaProcess)
                #     self.codigo_retorno = 1
                #
                # param = ParamClasse()
                # param.atualizar_parametro(EnumParametros.ultimoProcessVA.value, self.i_mes_ref)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            procesm.data_fim_procesm_indice = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
            procesm.stat_procesm_indice = EnumStatusProcessamento.erro.value
            self.registra_processamento(procesm, etapaProcess)
            self.codigo_retorno = 9
            raise

    def carrega_operacoes_ativo_imob(self, p_data_ref_inicio, p_data_ref_fim, p_data_inicio, p_data_fim) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def carrega_operacoes_ativo_imob - {p_data_inicio} a {p_data_fim}."
        loga_mensagem(etapaProcess)

        try:
            data_hora_atividade = datetime.now()

            df_retorno = pd.DataFrame()
            db = Oraprd()
            # db = Oraprodx9()
            df_contrib = db.select_ativo_imobilizado(p_data_ref_inicio, p_data_ref_fim, p_data_inicio, p_data_fim)
            del db

            loga_mensagem(etapaProcess + f' Processo finalizado. {len(df_contrib)} linhas selecionados - ' + str(datetime.now() - data_hora_atividade))

            return df_contrib.drop_duplicates()

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def inclui_dados_ativo_imobilizado(self, procesm: Processamento, df):
        etapaProcess = f"class {self.__class__.__name__} - def inclui_dados_ativo_imobilizado."
        loga_mensagem(etapaProcess)

        # A indicação de Ativo imobilizado será feito pela ocorrencia da nota fiscal na EFD, independentemente dos valores dos itens

        itens_alterados = 0
        docs_alterados = 0

        try:
            etapaProcess = f'{procesm.id_procesm_indice} Inclui motivo de exclusão para documentos participantes.'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            # Totaliza o valor adicionado por documento
            df['valr_adicionado'] = (df['valr_adicionado'].replace({None: 0, 'nan': 0})
                                                          .astype(str)
                                                          .str.replace('.', '',regex=False)
                                                          .str.replace(',', '.', regex=False)
                                                          .astype(float))

            df_nfe = df.loc[df['tipo'] == 'NFE']
            df_nfe = df_nfe.rename(columns={"numr_inscricao_entrada": "numr_inscricao_entrada_nfe",
                                            "codg_item_documento": "codg_item_documento_nfe",
                                            "valr_adicionado": "valr_adicionado_nfe"
                                            })
            df_efd = df.loc[df['tipo'] == 'EFD']
            df_efd = df_efd.rename(columns={"numr_inscricao_entrada": "numr_inscricao_entrada_efd",
                                            "codg_item_documento": "codg_item_documento_efd",
                                            "valr_adicionado": "valr_adicionado_efd"
                                            })

            df_nfe['rn'] = df_nfe.sort_values('codg_item_documento_nfe').groupby(['codg_documento_partct_calculo_nfe',
                                                                                  'codg_tipo_doc_partct_calc_nfe',
                                                                                  'codg_documento_partct_calculo_efd',
                                                                                  'codg_tipo_doc_partct_calc_efd']).cumcount() + 1

            df_efd['rn'] = df_efd.sort_values('codg_item_documento_efd').groupby(['codg_documento_partct_calculo_nfe',
                                                                                  'codg_tipo_doc_partct_calc_nfe',
                                                                                  'codg_documento_partct_calculo_efd',
                                                                                  'codg_tipo_doc_partct_calc_efd']).cumcount() + 1

            df_novo = pd.merge(df_nfe, df_efd,on=['codg_documento_partct_calculo_nfe',
                                                  'codg_tipo_doc_partct_calc_nfe',
                                                  'codg_documento_partct_calculo_efd',
                                                  'codg_tipo_doc_partct_calc_efd',
                                                  'rn'],
                                              how='outer')

            cols_int = ['codg_item_documento_nfe', 'codg_item_documento_efd']
            df_novo[cols_int] = df_novo[cols_int].astype('Int64')

            df_novo = df_novo.drop(columns=['rn'])
            df_novo = df_novo[["codg_documento_partct_calculo_nfe",
                               "codg_tipo_doc_partct_calc_nfe",
                               "codg_documento_partct_calculo_efd",
                               "codg_tipo_doc_partct_calc_efd",
                               "numr_inscricao_entrada_efd",
                               "codg_item_documento_efd",
                               "valr_adicionado_efd",
                               "numr_inscricao_entrada_nfe",
                               "codg_item_documento_nfe",
                               "valr_adicionado_nfe"
                               ]]

            filtro = ~pd.isna(df_novo['valr_adicionado_nfe'])
            df_novo['valr_adicionado_operacao_nfe'] = df_novo[filtro].groupby(['codg_documento_partct_calculo_nfe',
                                                                               'codg_tipo_doc_partct_calc_nfe'])['valr_adicionado_nfe'].transform('sum')
            filtro = ~pd.isna(df_novo['valr_adicionado_efd'])
            df_novo['valr_adicionado_operacao_efd'] = df_novo[filtro].groupby(['codg_documento_partct_calculo_efd',
                                                                               'codg_tipo_doc_partct_calc_efd'])['valr_adicionado_efd'].transform('sum')

            filtro = (~pd.isna(df_novo['valr_adicionado_operacao_nfe'])
                   & (~pd.isna(df_novo['valr_adicionado_operacao_efd'])))
            df_docs = df_novo[filtro][['codg_documento_partct_calculo_nfe', 'codg_tipo_doc_partct_calc_nfe', 'valr_adicionado_operacao_nfe', 'valr_adicionado_operacao_efd']].drop_duplicates()

            linhas_doc_alteradas = 0
            linhas_itens_alteradas = 0
            if len(df_docs) > 0:
                df_docs['diferenca_nfe_efd'] = df_novo['valr_adicionado_operacao_nfe'] - df_novo['valr_adicionado_operacao_efd']

                df_docs['codg_motivo_exclusao_calculo'] = np.where(df_docs['diferenca_nfe_efd'] == 0, 12, 0)

                # Serão excluídos todos os itens da NFe
                df_docs['codg_motivo_exclusao_calculo'] = 12
                df_docs['valr_adicionado_operacao'] = 0

                df_docs.rename(columns={'codg_documento_partct_calculo_nfe': 'codg_documento_partct_calculo',
                                        'codg_tipo_doc_partct_calc_nfe': 'codg_tipo_doc_partct_calc'}, inplace=True)
                df_docs.drop(columns=['valr_adicionado_operacao_nfe', 'valr_adicionado_operacao_efd', 'diferenca_nfe_efd'], inplace=True)

                db = Oraprd()
                linhas_doc_alteradas = db.update_doc_partct_valr_excl(df_docs)

                df_itens = df_novo[filtro][['codg_documento_partct_calculo_nfe', 'codg_tipo_doc_partct_calc_nfe', 'codg_item_documento_nfe']].drop_duplicates()
                df_itens.rename(columns={'codg_documento_partct_calculo_nfe': 'codg_documento_partct_calculo',
                                         'codg_tipo_doc_partct_calc_nfe': 'codg_tipo_doc_partct_calc',
                                         'codg_item_documento_nfe': 'codg_item_documento'}, inplace=True)

                if len(df_itens) > 0:
                    linhas_itens_alteradas = db.update_item_excl_ativo(df_itens)
                del db

            loga_mensagem(etapaProcess + f' Processo finalizado. {linhas_doc_alteradas} documentos alterados - {linhas_itens_alteradas} itens alterados' + str(
                datetime.now() - data_hora_atividade))

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def exclui_dados_operacoes_entre_contribuintes(self, p_data_inicio, p_data_fim, procesm: Processamento):
        etapaProcess = f"class {self.__class__.__name__} - def exclui_dados_operacoes_entre_contribuintes. Período de {p_data_inicio} a {p_data_fim}"
        loga_mensagem(etapaProcess)

        itens_alterados = 0
        docs_alterados = 0

        try:
            etapaProcess = f'{procesm.id_procesm_indice} Exclui indicador de exclusao de operacoes opostas do calculo - {p_data_inicio} a {p_data_fim}'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            db_item = ItemDocClasse()
            df_itens = db_item.carrega_item_excluido_nf(EnumMotivoExclusao.OperacaoDuplicada.value, p_data_inicio, p_data_fim)

            if len(df_itens) > 0:
                for i_tipo_doc, group in df_itens.groupby('codg_tipo_doc_partct_calc'):
                    l_lista_itens = list(zip(group['codg_documento_partct_calculo'], group['codg_item_documento']))
                    itens_alterados += db_item.limpa_motivo_excl_item(l_lista_itens, i_tipo_doc)

                db_doc = DocPartctClasse()
                df_filtrado = df_itens[df_itens['codg_motivo_exclusao_calculo'] == EnumMotivoExclusao.DocumentoNaoContemItemParticipante.value]
                for i_tipo_doc, group in df_filtrado.groupby('codg_tipo_doc_partct_calc'):
                    l_lista_docs = group['codg_documento_partct_calculo'].unique().tolist()
                    docs_alterados += db_doc.limpa_motivo_excl_doc(l_lista_docs, i_tipo_doc)
                del db_doc

            del db_item

            loga_mensagem(etapaProcess + f' Processo finalizado. {itens_alterados} linhas excluidas - ' + str(
                datetime.now() - data_hora_atividade))

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def exclui_dados_ativo_imobilizado(self, p_data_inicio, p_data_fim, procesm: Processamento):
        etapaProcess = f"class {self.__class__.__name__} - def exclui_dados_ativo_imobilizado. Período de {p_data_inicio} a {p_data_fim}"
        loga_mensagem(etapaProcess)

        itens_alterados = 0
        docs_alterados = 0

        try:
            etapaProcess = f'{procesm.id_procesm_indice} Exclui indicador de exclusao de operacoes de ativos imobilizados - {p_data_inicio} a {p_data_fim}'
            self.registra_processamento(procesm, etapaProcess)
            data_hora_atividade = datetime.now()

            db_item = ItemDocClasse()
            df_itens = db_item.carrega_item_excluido_nf(EnumMotivoExclusao.ItemAtivoImobilizado.value, p_data_inicio, p_data_fim)

            if len(df_itens) > 0:
                for i_tipo_doc, group in df_itens.groupby('codg_tipo_doc_partct_calc'):
                    l_lista_itens = list(zip(group['codg_documento_partct_calculo'], group['codg_item_documento']))
                    itens_alterados += db_item.limpa_motivo_excl_item(l_lista_itens, i_tipo_doc)

                db_doc = DocPartctClasse()
                df_filtrado = df_itens[df_itens['codg_motivo_exclusao_calculo'] == EnumMotivoExclusao.DocumentoNaoContemItemParticipante.value]
                for i_tipo_doc, group in df_filtrado.groupby('codg_tipo_doc_partct_calc'):
                    l_lista_docs = group['codg_documento_partct_calculo'].unique().tolist()
                    docs_alterados += db_doc.limpa_motivo_excl_doc(l_lista_docs, i_tipo_doc)
                del db_doc

            del db_item

            loga_mensagem(etapaProcess + f' Processo finalizado. {itens_alterados} linhas excluidas - ' + str(
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

    def lista_valor_adicionado(self, p_inicio_referencia, p_fim_referencia, p_tipo_documento, p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida):
        etapaProcess = f"class {self.__class__.__name__} - def lista_doc_nao_partct."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_docs = db.select_valor_adicionado(p_inicio_referencia, p_fim_referencia, p_tipo_documento, 'Periodo/Inscricao', p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida, None)
            del db

            return df_docs.to_dict('records')

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    # def lista_valor_adicionado_tipo_doc(self, p_inicio_referencia, p_fim_referencia, p_tipo_documento):
    #     etapaProcess = f"class {self.__class__.__name__} - def lista_valor_adicionado_tipo_doc - {p_inicio_referencia} - {p_fim_referencia} - {p_tipo_documento}."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         db = Oraprd()
    #         df_docs = db.select_valor_adicionado(p_inicio_referencia, p_fim_referencia, p_tipo_documento, 'ChaveEletr', None, None, None, None, p_chave)
    #         del db
    #
    #         return df_docs.to_dict('records')
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise

    # def lista_valor_adicionado_inscricao(self, p_inicio_referencia, p_fim_referencia, p_inscricao):
    #     etapaProcess = f"class {self.__class__.__name__} - def lista_valor_adicionado_inscricao - {p_inicio_referencia} - {p_fim_referencia} - {p_inscricao}."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         db = Oraprd()
    #         df_docs = db.select_valor_adicionado(p_inicio_referencia, p_fim_referencia, p_tipo_documento, 'Inscricao', None, None, p_ie_entrada, p_ie_saida, None)
    #         return df_docs.to_dict('records')
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise

    # def lista_valor_adicionado_periodo(self, p_inicio_referencia, p_fim_referencia):
    #     etapaProcess = f"class {self.__class__.__name__} - def lista_valor_adicionado_periodo- {p_inicio_referencia} - {p_fim_referencia}."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         db = Oraprd()
    #         df_docs = db.select_valor_adicionado(p_inicio_referencia, p_fim_referencia, p_tipo_documento, 'Periodo', p_data_inicio, p_data_fim, None, None, None)
    #         del db
    #
    #         return df_docs.to_dict('records')
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
