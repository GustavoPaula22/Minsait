from typing import Any, Optional

from funcoes.utilitarios import *
from polls.models import Calendario, Hist_CCE, Hist_CNAE, DMRUC014, DMGEN004
from django.db import connections

import pandas as pd

class OraprddwClass:
    etapaProcess = f"class {__name__} - class oraprddw."
    # loga_mensagem(etapaProcess)

    db_alias = 'oracle_dw'

    def __init__(self):
        var = None

    def select_nfa(self, p_data_inicio, p_data_fim) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def select_nfa - {p_data_inicio} a {str}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca de informações da NFA no período de {p_data_inicio} a {p_data_fim}"
            sql = f"""Select FT.NUMR_NF_AVULSA                                                        As id_nfa
                           , CAST(
                             CASE WHEN FT.CODG_TIPO_OPERACAO = 3 
                                  THEN CASE WHEN IeSai.ID_CONTRIB            = 999999999
                                            THEN CASE WHEN IeSai.NUMR_INSCRICAO_INFORMADO = 0
                                                      THEN Null
                                                      ELSE IeSai.NUMR_INSCRICAO_INFORMADO     END
                                            ELSE RUCSai.NUMR_INSC                             END
                                  ELSE CASE WHEN IeEnt.ID_CONTRIB            = 999999999
                                            THEN CASE WHEN IeEnt.NUMR_INSCRICAO_INFORMADO = 0
                                                      THEN Null
                                                      ELSE IeEnt.NUMR_INSCRICAO_INFORMADO     END
                                            ELSE RUCEnt.NUMR_INSC                             END END As VARCHAR(14)) As ie_entrada
                           , CASE WHEN FT.CODG_TIPO_OPERACAO = 3 
                                  THEN CASE FT.CODG_MUNICIPIO_REMETENTE
                                            WHEN 999999999 THEN Null
                                            WHEN    216100 THEN 175400
                                            ELSE                FT.CODG_MUNICIPIO_REMETENTE   END
                                  ELSE CASE FT.CODG_MUNICIPIO_DEST
                                            WHEN 999999999 THEN Null
                                            WHEN    216100 THEN 175400
                                            ELSE               FT.CODG_MUNICIPIO_DEST         END END                 As codg_municipio_entrada
                           , CASE WHEN FT.CODG_TIPO_OPERACAO = 3 
                                  THEN CASE WHEN FT.CODG_MUNICIPIO_REMETENTE = 999999999
                                            THEN '99'
                                            ELSE MunSai.CODG_UF                               END
                                  ELSE CASE WHEN FT.CODG_MUNICIPIO_DEST      = 999999999
                                            THEN Null
                                            ELSE MunEnt.CODG_UF                               END END                 As codg_uf_entrada
                           , CAST(
                             CASE WHEN FT.CODG_TIPO_OPERACAO = 3 
                                  THEN CASE WHEN IeEnt.ID_CONTRIB            = 999999999
                                            THEN CASE WHEN IeEnt.NUMR_INSCRICAO_INFORMADO = 0
                                                      THEN Null
                                                      ELSE IeEnt.NUMR_INSCRICAO_INFORMADO     END
                                            ELSE RUCEnt.NUMR_INSC                             END
                                  ELSE CASE WHEN IeSai.ID_CONTRIB            = 999999999
                                            THEN CASE WHEN IeSai.NUMR_INSCRICAO_INFORMADO = 0
                                                      THEN Null
                                                      ELSE IeSai.NUMR_INSCRICAO_INFORMADO     END
                                            ELSE RUCSai.NUMR_INSC                             END END As VARCHAR(14)) As ie_saida
                         , CASE WHEN FT.CODG_TIPO_OPERACAO = 3 
                                  THEN CASE FT.CODG_MUNICIPIO_DEST
                                            WHEN 999999999 THEN Null
                                            WHEN    216100 THEN 175400
                                            ELSE                FT.CODG_MUNICIPIO_DEST        END
                                  ELSE CASE FT.CODG_MUNICIPIO_REMETENTE
                                            WHEN 999999999 THEN Null
                                            WHEN    216100 THEN 175400
                                            ELSE FT.CODG_MUNICIPIO_REMETENTE                  END END                 As codg_municipio_saida
                         , CASE WHEN FT.CODG_TIPO_OPERACAO = 3 
                                  THEN CASE WHEN FT.CODG_MUNICIPIO_DEST      = 999999999
                                            THEN '99'
                                            ELSE MunEnt.CODG_UF                               END
                                  ELSE CASE WHEN FT.CODG_MUNICIPIO_REMETENTE = 999999999
                                            THEN Null
                                            ELSE MunSai.CODG_UF                               END END                 As codg_uf_saida
                           , 0                                                                                        As numr_cpf_cnpj_dest
                           , Natur.DESC_NATUREZA_OPERACAO                                                             As desc_natureza_operacao
                           , Null                                                                                     As codg_modelo_nfe
                           , DtEmi.NUMR_ANO_MES                                                                       As numr_ref_emissao
                           , DtEmi.NUMR_ANO_MES_DIA                                                                   As data_emissao_doc
                           , CASE WHEN FT.CODG_TIPO_OPERACAO = 3 
                                  THEN 'E' 
                                  ELSE 'S'                                                    END                     As indi_tipo_operacao
                           , Null                                                                                     As desc_destino_operacao
                           , Null                                                                                     As desc_finalidade_operacao
                           , FT.VALR_TOTAL_PRODUTO_NF                                                                 As valr_va
                           , FT.STAT_NOTA                                                                             As stat_nota
                        From FT_SCR_MOV_ESTOC_PROD_PRIMA_NF FT
                              Left  Join DM_SCR_CONTRIBUINTE_NFA  IeSai  On FT.ID_CONTRIB_NFA_REMETENTE = IeSai.ID_CONTRIB_NFA
                              Left  Join VWRUC002                 RUCSai On IeSai.ID_CONTRIB            = RUCSai.ID_CONTRIB
                              Left  Join DMGEN011                 MunSai On FT.CODG_MUNICIPIO_REMETENTE = MunSai.CODG_MUN
                              Left  Join DM_SCR_CONTRIBUINTE_NFA  IeEnt  On FT.ID_CONTRIB_NFA_DEST      = IeEnt.ID_CONTRIB_NFA
                              Left  Join VWRUC002                 RUCEnt On IeEnt.ID_CONTRIB            = RUCEnt.ID_CONTRIB
                              Left  Join DMGEN011                 MunEnt On FT.CODG_MUNICIPIO_DEST      = MunEnt.CODG_MUN
                              Inner Join DMGEN006                 DtEmi  On FT.ID_DIA                   = DtEmi.ID_DIA
                              Inner Join DM_SCR_NATUREZA_OPERACAO Natur  On FT.ID_NATUREZA_OPERACAO     = Natur.ID_NATUREZA_OPERACAO
                       Where 1=1
                         And FT.NUMR_NF_AVULSA      Is Not Null
                         And FT.STAT_NOTA            = 'NORMAL'
                         And Natur.CODG_NATUREZA_OPERACAO IN (101, 102, 103, 104, 111, 112, 113, 201, 202, 203, 204, 301, 302, 303, 401, 402, 403, 501, 502, 503)
                         And DtEmi.NUMR_ANO_MES_DIA BETWEEN {p_data_inicio} AND {p_data_fim}"""

            connections[self.db_alias].close()

            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                colunas = [col[0].lower() for col in cursor.description]
                dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]

            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_dmgen006_intervalo(self, pAnoMesDiaIni: int, pAnoMesDiaFim: int) -> Optional[Any]:
        etapaProcess = f"class {self.__class__.__name__} - def select_dmgen006_intervalo - {pAnoMesDiaIni} a {pAnoMesDiaFim}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca de informações de datas no período de {pAnoMesDiaIni} a {pAnoMesDiaFim}"
            objeto = Calendario.objects.using(self.db_alias).filter(numr_ano_mes_dia__range=(pAnoMesDiaIni, pAnoMesDiaFim)).order_by('numr_ano_mes_dia')
            if objeto.exists():
                etapaProcess += f' - Retornadas {objeto.count()} linhas.'
                # loga_mensagem(etapaProcess)
                return objeto

            else:
                etapaProcess += f' - Não retornou nenhuma data!'
                loga_mensagem(etapaProcess)
                return None

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_dmgen004(self, ids):
        etapaProcess = f"class {self.__class__.__name__} - def select_dmgen004."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca de informações de meses."
            dados = DMGEN004.objects.using(self.db_alias).filter(id_mes__in=ids).values('id_mes', 'numr_ano_mes')
            if dados.exists():
                etapaProcess += f' - Retornadas {dados.count()} linhas.'
                # loga_mensagem(etapaProcess)
                return pd.DataFrame(dados)

            else:
                etapaProcess += f' - Não retornou nenhuma data!'
                loga_mensagem(etapaProcess)
                return None

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_historico_contrib(self, inscrs):
        etapaProcess = f"class {self.__class__.__name__} - def select_historico_contrib."
        # loga_mensagem(etapaProcess)

        try:
            tamanho_lote = 20000
            lotes = [inscrs[i:i + tamanho_lote] for i in range(0, len(inscrs), tamanho_lote)]

            dfs = []

            # Itere sobre cada lote e execute a consulta ao banco de dados
            for lote in lotes:
                dados = Hist_CCE.objects.using(self.db_alias)\
                                        .filter(numr_inscricao__in=lote)\
                                        .values('numr_inscricao',
                                                'codg_municipio_contrib',
                                                'desc_situacao_cadastro',
                                                'desc_tipo_enqdto',
                                                'numr_ano_mes_dia_alteracao',
                                                )
                df = pd.DataFrame(dados)
                dfs.append(df)

            # Concatene todos os DataFrames em um único DataFrame
            df_resultado = pd.concat(dfs, ignore_index=True)
            if len(df_resultado) > 0:
                return df_resultado
            else:
                return []

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_historico_contrib_periodo(self, inscrs, p_data_inicio, p_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def select_historico_contrib_periodo."
        # loga_mensagem(etapaProcess)

        try:
            tamanho_lote = 20000
            lotes = [inscrs[i:i + tamanho_lote] for i in range(0, len(inscrs), tamanho_lote)]

            dfs = []

            # Itere sobre cada lote e execute a consulta ao banco de dados
            # .filter(numr_ano_mes_dia_alteracao__gte=p_data_inicio, numr_ano_mes_dia_alteracao__lte=p_data_fim)\
            for lote in lotes:
                dados = Hist_CCE.objects.using(self.db_alias)\
                                        .filter(numr_inscricao__in=lote)\
                                        .values('numr_inscricao',
                                                'codg_municipio_contrib',
                                                'desc_situacao_cadastro',
                                                'numr_ano_mes_dia_alteracao',
                                                )
                df = pd.DataFrame(dados)
                dfs.append(df)

            # Concatene todos os DataFrames em um único DataFrame
            df_resultado = pd.concat(dfs, ignore_index=True)
            if len(df_resultado) > 0:
                return df_resultado
            else:
                return []

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_historico_cnae(self, inscrs, p_inicio_ref):
        etapaProcess = f"class {self.__class__.__name__} - def select_historico_cnae"
        # loga_mensagem(etapaProcess)

        try:
            tamanho_lote = 20000
            lotes = [inscrs[i:i + tamanho_lote] for i in range(0, len(inscrs), tamanho_lote)]
            dfs = []

            # Itere sobre cada lote e execute a consulta ao banco de dados
            for lote in lotes:
                dados = Hist_CNAE.objects\
                                 .using(self.db_alias)\
                                 .filter(numr_inscricao_contrib__in=lote)\
                                 .values('numr_inscricao_contrib',
                                         'perc_atividade_contrib',
                                         'indi_cnae_principal_contrib',
                                         'id_mes_inicio_vigencia',
                                         'id_mes_fim_vigencia',
                                         'id_subcls',
                                         )
                df = pd.DataFrame(dados)
                dfs.append(df)

            # Concatene todos os DataFrames em um único DataFrame
            df_resultado = pd.concat(dfs, ignore_index=True)
            return df_resultado

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_dmruc014(self, ids):
        etapaProcess = f"class {self.__class__.__name__} - def select_dmruc014"
        # loga_mensagem(etapaProcess)

        try:
            dados = DMRUC014.objects\
                            .using(self.db_alias)\
                            .filter(id_subcls__in=ids)\
                            .values('id_subcls',
                                    'codg_subcls',
                                    )
            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_conv115(self, p_data_inicio=str, p_data_fim=str) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - class select_conv115 - {p_data_inicio} a {p_data_fim}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca de informações do Conv 115 no período de {p_data_inicio} a {p_data_fim}"
            sql = f"""Select DM.NUMR_INSCRICAO_EMITENTE                          As IE_SAIDA
                           , DM.NUMR_CNPJ_EMITENTE                               As CNPJ_IE_SAIDA
                           , MunSai.CODG_MUN                                     As CODG_MUNICIPIO_SAIDA
                           , MunSai.CODG_UF                                      As CODG_UF_SAIDA
                           , Case When Dest.NUMR_INSCRICAO_DEST = 99999999999999
                                  Then Null 
                                  Else Dest.NUMR_INSCRICAO_DEST   End            As IE_ENTRADA
                           , FT.NUMR_CNPJ_CPF_DEST
                           , NVL(MunEnt.CODG_MUN, 0)                             As CODG_MUNICIPIO_ENTRADA
                           , MunEnt.CODG_UF                                      As CODG_UF_ENTRADA
                           , FT.TIPO_PESSOA_TOMADOR                              As INDI_TIPO_PESSOA
                           , DM.CODG_MODELO_DOCUMENTO_FISCAL
                           , DM.DESC_SERIE_DOCUMENTO_FISCAL
                           , FT.NUMR_DOCUMENTO_FISCAL
                           , FT.INDI_SITUACAO_VERSAO_ARQUIVO
                           , FT.INDI_SITUACAO_DOCUMENTO                          As INDI_SITUACAO_DOCUMENTO
                           , TO_DATE(DM.NUMR_ANO_MES_DIA_EMISSAO, 'YYYYMMDD')    As DATA_EMISSAO_DOCUMENTO
                           , SUBSTR(DM.NUMR_ANO_MES_DIA_EMISSAO, 1, 6)           As NUMR_REF_EMISSAO
                           , FT.NUMR_ORDEM_ITEM
                           , FT.NUMR_CFOP
                           , FT.PERC_ALIQUOTA_ICMS
                           , FT.VALR_BASE_CALCULO_ICMS
                           , FT.VALR_ISENTO_NAO_TRIBUTAVEL
                        From DM_GCI_DOCUMENTO_FISCAL_TELEC            DM
                              Inner Join FT_GCI_ITEM_DOC_FISCAL_TELEC FT     On (DM.NUMR_CNPJ_EMITENTE           = FT.NUMR_CNPJ_EMITENTE
                                                                             And DM.NUMR_ANO_MES_DIA_EMISSAO     = FT.NUMR_ANO_MES_DIA_EMISSAO
                                                                             And DM.DESC_SERIE_DOCUMENTO_FISCAL  = FT.DESC_SERIE_DOCUMENTO_FISCAL
                                                                             And DM.NUMR_DOCUMENTO_FISCAL        = FT.NUMR_DOCUMENTO_FISCAL
                                                                             And DM.INDI_SITUACAO_VERSAO_ARQUIVO = FT.INDI_SITUACAO_VERSAO_ARQUIVO)
                              Inner Join DM_GCI_DESTINATARIO_TELEC    Dest   On (FT.NUMR_CNPJ_CPF_DEST           = Dest.NUMR_CNPJ_CPF_DEST
                                                                             And FT.NUMR_INSCRICAO_DEST          = Dest.NUMR_INSCRICAO_DEST
                                                                             And FT.CODG_DEST                    = Dest.CODG_DEST
                                                                             And FT.NUMR_TERMINAL_CONTA          = Dest.NUMR_TERMINAL_CONTA
                                                                             And FT.NOME_RAZAO_SOCIAL_DEST       = Dest.NOME_RAZAO_SOCIAL_DEST
                                                                             And FT.CODG_UF_DEST                 = Dest.CODG_UF_DEST)
                              Inner Join DMGEN011                     MunEnt On  Dest.CODG_MUNICIPIO_IBGE        = MunEnt.CODG_MUN_IBGE
                              Inner Join DMRUC001                     Ruc    On  DM.NUMR_INSCRICAO_EMITENTE      = Ruc.NUMR_INSC
                              Inner Join DMGEN011                     MunSai On  Ruc.CODG_MUN                    = MunSai.CODG_MUN
                       Where 1=1
                         And MunEnt.CODG_SERPRO               Is Not Null
                         And FT.VALR_BASE_CALCULO_ICMS        <> 0
                         And DM.NUMR_ANO_MES_DIA_EMISSAO BETWEEN {p_data_inicio} And {p_data_fim}"""

            connections[self.db_alias].close()

            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                colunas = [col[0].lower() for col in cursor.description]
                dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]

            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    # def select_historico_cnae(self, pInscricao):
    #     etapaProcess = f"class {self.__class__.__name__} - def select_historico_cnae - {pInscricao}"
    #     # loga_mensagem(etapaProcess)
    #
    #     sql = f"""Select CnaeHist.NUMR_INSCRICAO_CONTRIB
    #                    , CnaeHist.PERC_ATIVIDADE_CONTRIB
    #                    , CnaeHist.INDI_CNAE_PRINCIPAL_CONTRIB
    #                    , MesIni.NUMR_ANO_MES          As Ref_Inicio
    #                    , MesFim.NUMR_ANO_MES          As Ref_Fim
    #                    , Cnae.CODG_SUBCLS
    #                 From DM_RUC_HISTORICO_CNAE_CONTRIB CnaeHist
    #                       Inner Join DMGEN004          MesIni   On CnaeHist.ID_MES_INICIO_VIGENCIA = MesIni.ID_MES
    #                       Inner Join DMGEN004          MesFim   On CnaeHist.ID_MES_FIM_VIGENCIA    = MesFim.ID_MES
    #                       Inner Join DMRUC014          Cnae     On CnaeHist.ID_SUBCLS              = Cnae.ID_SUBCLS
    #                Where 1=1
    #                  And  NUMR_INSCRICAO_CONTRIB  = {pInscricao}
    #                Order By Ref_Inicio                  Desc
    #                       , Ref_Fim                     Desc
    #                       , INDI_CNAE_PRINCIPAL_CONTRIB Desc
    #                       , PERC_ATIVIDADE_CONTRIB      Desc"""
    #
    #     try:
    #         etapaProcess = f"Executa query de busca de informações do histórico do CCE: {sql}"
    #
    #         connections[self.db_alias].close()
    #
    #         with connections[self.db_alias].cursor() as cursor:
    #             cursor.execute(sql)
    #             colunas = [col[0].lower() for col in cursor.description]
    #             dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]
    #
    #         return pd.DataFrame(dados)
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise

    # Carrega as informações históricas do cadastro (CCE), pelo contribuinte #
    # def select_historico_contrib(self, pInscricao):
    #     etapaProcess = f"class {self.__class__.__name__} - def select_historico_contrib - {pInscricao}"
    #     # loga_mensagem(etapaProcess)
    #
    #     sql = f"""Select NUMR_INSCRICAO
    #                , NUMR_CNPJ_CONTRIB
    #                , CODG_MUNICIPIO_CONTRIB
    #                , Case DESC_SITUACAO_CADASTRO
    #                     When 'Ativo'      Then 1
    #                     When 'Suspenso'   Then 2
    #                     When 'Baixado'    Then 3
    #                     When 'Paralisado' Then 4
    #                     When 'Cassado'    Then 5
    #                     When 'Anulado'    Then 6
    #                     Else                   0                END AS CODG_SITUACAO_CADASTRO
    #                , Case DESC_TIPO_ENQDTO
    #                     When 'Microempresa'              THEN 1
    #                     When 'Normal'                    THEN 2
    #                     When 'Micro EPP/Simples Naciona' THEN 3
    #                     When 'Simples Nacional/SIMEI'	 THEN 4
    #                     When 'Simples Nacional/Normal'	 THEN 5
    #                     Else                                  0 END AS CODG_TIPO_ENQDTO
    #                , NUMR_ANO_MES_DIA_ALTERACAO
    #                , SUBSTR(NUMR_ANO_MES_DIA_ALTERACAO, 1, 6)       As AnoMesRef
    #             From DM_RUC_HISTORICO_CONTRIB
    #            Where 1=1
    #              And NUMR_INSCRICAO = {pInscricao}
    #            Order By NUMR_ANO_MES_DIA_ALTERACAO Desc"""
    #
    #     try:
    #         etapaProcess = f"Executa query de busca de informações do histórico do CCE: {sql}"
    #
    #         connections[self.db_alias].close()
    #
    #         with connections[self.db_alias].cursor() as cursor:
    #             cursor.execute(sql)
    #             colunas = [col[0].lower() for col in cursor.description]
    #             dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]
    #
    #         return pd.DataFrame(dados)
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    # def select_historico_contrib(self, pInscricao, pReferencia):
    #     etapaProcess = f"class {self.__class__.__name__} - def select_historico_contrib - {pInscricao} na referencia {pReferencia}"
    #     # loga_mensagem(etapaProcess)
    #
    #     sql = f"""Select NUMR_INSCRICAO
    #                , NUMR_CNPJ_CONTRIB
    #                , CODG_MUNICIPIO_CONTRIB
    #                , Case DESC_SITUACAO_CADASTRO
    #                     When 'Ativo'      Then 1
    #                     When 'Suspenso'   Then 2
    #                     When 'Baixado'    Then 3
    #                     When 'Paralisado' Then 4
    #                     When 'Cassado'    Then 5
    #                     When 'Anulado'    Then 6
    #                     Else                   0                END AS CODG_SITUACAO_CADASTRO
    #                , Case DESC_TIPO_ENQDTO
    #                     When 'Microempresa'              THEN 1
    #                     When 'Normal'                    THEN 2
    #                     When 'Micro EPP/Simples Naciona' THEN 3
    #                     When 'Simples Nacional/SIMEI'	 THEN 4
    #                     When 'Simples Nacional/Normal'	 THEN 5
    #                     Else                                  0 END AS CODG_TIPO_ENQDTO
    #                , NUMR_ANO_MES_DIA_ALTERACAO
    #                , SUBSTR(NUMR_ANO_MES_DIA_ALTERACAO, 1, 6)       As AnoMesRef
    #             From DM_RUC_HISTORICO_CONTRIB
    #            Where 1=1
    #              And NUMR_INSCRICAO = '{pInscricao}'
    #              And SUBSTR(NUMR_ANO_MES_DIA_ALTERACAO, 1, 6) <= '{pReferencia}'
    #            Order By NUMR_ANO_MES_DIA_ALTERACAO Desc
    #            Fetch First 1 Rows Only"""
    #
    #     try:
    #         etapaProcess = f"Executa query de busca de informações do histórico do CCE: {sql}"
    #
    #         connections[self.db_alias].close()
    #
    #         with connections[self.db_alias].cursor() as cursor:
    #             cursor.execute(sql)
    #             colunas = [col[0].lower() for col in cursor.description]
    #             dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]
    #
    #         return pd.DataFrame(dados)
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    # def select_historico_contrib_referencia(self, pInscricao, pReferencia):
    #     etapaProcess = f"class {self.__class__.__name__} - def select_historico_contrib - {pInscricao} na referencia {pReferencia}"
    #     # loga_mensagem(etapaProcess)
    #
    #     sql = f"""Select NUMR_INSCRICAO
    #                , NUMR_CNPJ_CONTRIB
    #                , CODG_MUNICIPIO_CONTRIB
    #                , Case DESC_SITUACAO_CADASTRO
    #                     When 'Ativo'      Then 1
    #                     When 'Suspenso'   Then 2
    #                     When 'Baixado'    Then 3
    #                     When 'Paralisado' Then 4
    #                     When 'Cassado'    Then 5
    #                     When 'Anulado'    Then 6
    #                     Else                   0                END AS CODG_SITUACAO_CADASTRO
    #                , Case DESC_TIPO_ENQDTO
    #                     When 'Microempresa'              THEN 1
    #                     When 'Normal'                    THEN 2
    #                     When 'Micro EPP/Simples Naciona' THEN 3
    #                     When 'Simples Nacional/SIMEI'	 THEN 4
    #                     When 'Simples Nacional/Normal'	 THEN 5
    #                     Else                                  0 END AS CODG_TIPO_ENQDTO
    #                , NUMR_ANO_MES_DIA_ALTERACAO
    #                , SUBSTR(NUMR_ANO_MES_DIA_ALTERACAO, 1, 6)       As AnoMesRef
    #             From DM_RUC_HISTORICO_CONTRIB
    #            Where 1=1
    #              And NUMR_INSCRICAO = '{pInscricao}'
    #              And SUBSTR(NUMR_ANO_MES_DIA_ALTERACAO, 1, 6) <= '{pReferencia}'
    #            Order By NUMR_ANO_MES_DIA_ALTERACAO Desc
    #            Fetch First 1 Rows Only"""
    #
    #     try:
    #         etapaProcess = f"Executa query de busca de informações do histórico do CCE: {sql}"
    #
    #         connections[self.db_alias].close()
    #
    #         with connections[self.db_alias].cursor() as cursor:
    #             cursor.execute(sql)
    #             colunas = [col[0].lower() for col in cursor.description]
    #             dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]
    #
    #         return pd.DataFrame(dados)
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #