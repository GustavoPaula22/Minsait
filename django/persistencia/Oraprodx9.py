import time

import cx_Oracle
from django.db import connections
from django.db.models import Q
from funcoes.constantes import inicio_referencia, fim_referencia, EnumTipoDocumento
from funcoes.utilitarios import *
from polls.models import Parametro, CFOP, CadastroCCE, GEN_NCM, CCE_CNAE


class Oraprodx9:
    etapaProcess = f"class {__name__} - class Oraprodx9."
    # loga_mensagem(etapaProcess)

    db_alias = 'oracle_x9'

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
                       WHERE Ident.CODG_MODELO_NFE = '55'
    --                   AND ((Ident.NUMR_INSCRICAO      IN (101651899, 103160310, 102347239, 106593129)
    --                    Or   Ident.NUMR_INSCRICAO_DEST IN (101651899, 103160310, 102347239, 106593129))
    --                    Or  (Ident.NUMR_CPF_CNPJ_DEST  IN ('03387396000160', '02916265008306', '33000167002155', '15689716001510')
    --                    Or   Ident.NUMR_CNPJ_EMISSOR   IN ('03387396000160', '02916265008306', '33000167002155', '15689716001510')))
                         AND Ident.ID_RESULTADO_PROCESM   = 321         -- Nota Denegada = 390
                         AND Ident.DATA_EMISSAO_NFE BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                        AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')"""

        try:
            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                colunas = [col[0].lower() for col in cursor.description]
                dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]

            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

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
                   WHERE Ident.CODG_MODELO_NFE = '65'
--                   AND ((Ident.NUMR_INSCRICAO      IN (101651899, 103160310, 102347239, 106593129)
--                    Or   Ident.NUMR_INSCRICAO_DEST IN (101651899, 103160310, 102347239, 106593129))
--                    Or  (Ident.NUMR_CPF_CNPJ_DEST  IN ('03387396000160', '02916265008306', '33000167002155', '15689716001510')
--                    Or   Ident.NUMR_CNPJ_EMISSOR   IN ('03387396000160', '02916265008306', '33000167002155', '15689716001510')))
                     AND Ident.ID_RESULTADO_PROCESM   = 321         -- Nota Denegada = 390
                     AND Ident.DATA_EMISSAO_NFE BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                    AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')"""

        try:
            with connections[self.db_alias].cursor() as cursor:
                etapaProcess = f"Executa query de busca de NFC-e geradas: "
                data_hora_atividade = datetime.now()
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

                colunas = [col[0].lower() for col in cursor.description]

            connections[self.db_alias].close()

            loga_mensagem(etapaProcess + f' Processo finalizado. ' + str(datetime.now() - data_hora_atividade))

            etapaProcess = f"Converte resultado para um dataframe. "
            data_hora_atividade = datetime.now()
            df = pd.DataFrame(dados, columns=colunas)
            # loga_mensagem(etapaProcess + f' Processo finalizado. {len(df)} lidos' + str(datetime.now() - data_hora_atividade))

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

        return df

    def select_nfe_recebida(self, p_data_inicio=str, p_data_fim=str) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - class select_nfe_recebida - {p_data_inicio} a {p_data_fim}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca de informações da NFe no período de {p_data_inicio} a {p_data_fim}"
            sql = f"""With NFeReceb As (SELECT Ident.ID_NFE_RECEBIDA                                               As id_nfe
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
                                             , Ident.CODG_MODELO_NFE                                               As codg_modelo_nfe
                                             , TO_NUMBER(TO_CHAR(DATA_EMISSAO_NFE, 'YYYYMM'))                      As numr_ref_emissao
                                             , DATA_EMISSAO_NFE                                                    As data_emissao_doc
                                             , CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                                                    THEN 'E' 
                                                    ELSE 'S' END                                                   As indi_tipo_operacao
                                             , COALESCE(Ident.VALR_NOTA_FISCAL, 0)                                 As valr_nfe
                                             , NVL(NUMR_PROTOCOLO_CANCEL, 0)                                       As numr_protocolo_cancel
                                          FROM NFE_IDENTIFICACAO_RECEBIDA       Ident 
                                         WHERE Ident.DATA_EMISSAO_NFE BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                                          AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS'))
--                                           AND ((Ident.NUMR_INSCRICAO_EMITENTE IN (101651899, 103160310, 102347239, 106593129)
--                                            Or   Ident.NUMR_INSCRICAO_DEST     IN (101651899, 103160310, 102347239, 106593129))
--                                            Or  (Ident.NUMR_CNPJ_DEST          IN ('03387396000160', '02916265008306', '33000167002155', '15689716001510')
--                                            Or   Ident.NUMR_CNPJ               IN ('03387396000160', '02916265008306', '33000167002155', '15689716001510')))
                         , Final    As (SELECT Ident.id_nfe
                                             , Item.ID_ITEM_NOTA_FISCAL                                            As id_item_nota_fiscal
                                             , Ident.ie_entrada
                                             , Ident.codg_municipio_entrada
                                             , Ident.codg_uf_entrada
                                             , Ident.ie_saida
                                             , Ident.codg_municipio_saida
                                             , Ident.codg_uf_saida
                                             , Ident.numr_cpf_cnpj_dest
                                             , Null                                                                As desc_natureza_operacao
                                             , Ident.codg_modelo_nfe
                                             , Ident.numr_ref_emissao
                                             , Ident.data_emissao_doc
                                             , Ident.indi_tipo_operacao
                                             , 'Operação interna'                                                  As desc_destino_operacao
                                             , 'NF-e normal'                                                       As desc_finalidade_operacao
                                             , Item.NUMR_ITEM                                                      As numr_item
                                             , Item.CODG_CFOP                                                      As numr_cfop
                                             , Item.CODG_EAN                                                       As numr_gtin
                                             , Item.CODG_CEST                                                      As numr_cest
                                             , Item.CODG_PRODUTO_NCM                                               As numr_ncm
                                             , Item.QTDE_COMERCIAL                                                 As qdade_itens
                                             , COALESCE(Item.CODG_PRODUTO_ANP, 0)                                  As codg_anp
                                             , Ident.valr_nfe
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
                                                       + COALESCE(Item.VALR_IMPOSTO_IMPORTACAO, 0) END             As valr_va
                                             , Ident.numr_protocolo_cancel
                                          FROM NFeReceb                         Ident 
                                                INNER JOIN NFE_ITEM_NOTA_FISCAL Item  ON Ident.ID_NFE = Item.ID_NFE_RECEBIDA
                                         WHERE 1=1)
                      Select * From Final"""

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

    def select_arq_efd(self, p_data_inicio=str, p_data_fim=str) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - class select_arq_efd - {p_data_inicio} a {p_data_fim}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca de identificadores de arquivos da EFD no período de {p_data_inicio} a {p_data_fim}"
            sql = f"""Select Arq.ID_ARQUIVO                                              As id_arquivo
                           , Arq.REFE_ARQUIVO                                            As numr_ref_arquivo
                           , Arq.NUMR_INSCRICAO                                          As ie_entrada
                           , Null                                                        As codg_municipio_entrada
                           , 'GO'                                                        As codg_uf_entrada
                           , Arq.NUMR_CNPJ                                               As numr_cnpj
                           , Arq.DATA_ENTREGA_ARQUIVO                                    As data_entrega_arquivo
                        From EFD_ARQUIVO                               Arq
                       Where Arq.STAT_PROCESM_ARQUIVO            = 'S'
                         And Arq.TIPO_MODELO_ARQUIVO             = 'E'
                         And Arq.REFE_ARQUIVO              Between {inicio_referencia} And {fim_referencia}
                         And Arq.DATA_ENTREGA_ARQUIVO      Between TO_DATE('{p_data_inicio}', 'yyyy/mm/dd hh24:mi:ss') 
                                                               And TO_DATE('{p_data_fim}',    'yyyy/mm/dd hh24:mi:ss')
--                         And (Arq.NUMR_INSCRICAO               IN (101651899, 103160310, 102347239, 106593129)
--                          Or (Arq.NUMR_CNPJ                    IN ('03387396000160', '02916265008306', '33000167002155', '15689716001510')))
                         And Arq.DATA_ENTREGA_ARQUIVO In (Select Max(Arq1.DATA_ENTREGA_ARQUIVO)
                                                            From EFD_ARQUIVO Arq1
                                                           Where Arq1.NUMR_INSCRICAO             = Arq.NUMR_INSCRICAO
                                                             And Arq1.REFE_ARQUIVO               = Arq.REFE_ARQUIVO
                                                             And Arq1.REFE_ARQUIVO         Between {inicio_referencia} And {fim_referencia}
                                                             And Arq1.STAT_PROCESM_ARQUIVO       = 'S'
                                                             And Arq1.TIPO_MODELO_ARQUIVO        = 'E')"""

            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                colunas = [col[0].lower() for col in cursor.description]
                dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]

            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_efd(self, p_id_arquivo) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - class select_efd - {p_id_arquivo}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca de informações da EFD pelo ID {p_id_arquivo}"
            # sql = f"""Select NFe.ID_NOTA_FISCAL                                          As id_nfe
            #                , Arq.NUMR_INSCRICAO                                          As ie_entrada
            #                , NVL(Partct.NUMR_INSCRICAO, PartExt.NUMR_INSCRICAO_ESTADUAL) As ie_saida
            #                , Arq.NUMR_CNPJ
            #                , Arq.REFE_ARQUIVO                                            As numr_ref_emissao
            #                , NFe.DATA_EMISSAO                                            As data_emissao_nfe
            #                , SUM(Item.VALR_TOTAL)                                        As valr_total
            #                , SUM(Item.VALR_DESCONTO)                                     As valr_desconto
            #                , SUM(Item.VALR_IPI)                                          As valr_ipi
            #             From EFD_ARQUIVO                               Arq
            #                   Inner Join EFD_NOTA_FISCAL               NFe     On Arq.ID_ARQUIVO           = NFe.ID_ARQUIVO
            #                   Inner Join EFD_ITEM_NOTA_FISCAL          Item    On NFe.ID_NOTA_FISCAL       = Item.ID_NOTA_FISCAL
            #                   Left  Join EFD_CONTRIBUINTE_PARTICIPANTE Partct  On NFe.ID_CONTRIB_PARTCT    = Partct.ID_CONTRIB_PARTCT
            #                   Left  Join EFD_PARTICIPANTE_EXTERNO      PartExt On Partct.ID_PARTCT_EXTERNO = PartExt.ID_PARTCT_EXTERNO
            #            Where Arq.STAT_PROCESM_ARQUIVO            = 'S'
            #              And Arq.TIPO_MODELO_ARQUIVO             = 'E'
            #              And Arq.REFE_ARQUIVO              Between {inicio_referencia} And {fim_referencia}
            #              And NFe.CODG_CHAVE_ACESSO_NFE          Is Not Null
            #              And Arq.DATA_ENTREGA_ARQUIVO      Between TO_DATE('{p_data_inicio}', 'yyyy/mm/dd hh24:mi:ss')
            #                                                    And TO_DATE('{p_data_fim}',    'yyyy/mm/dd hh24:mi:ss')
            #              And NFe.TIPO_OPERACAO                   = 0                                                               -- NFe de Entrada --
            #              And NFe.CODG_SITUACAO_DOCUMENTO_FISCAL In (0, 1, 6, 7, 8)                                                 -- Situação do Documento Fiscal (NF-e) --
            #              And Item.CODG_CFOP                     In (1406, 1407, 1551, 1556, 2406, 2407, 2551, 2556, 3551, 3556)    -- CFOPs de ativo imobilizado e materiais para uso e consumo --
            #              And Arq.DATA_ENTREGA_ARQUIVO In (Select Max(Arq1.DATA_ENTREGA_ARQUIVO)
            #                                                 From EFD_ARQUIVO Arq1
            #                                                Where Arq1.NUMR_INSCRICAO             = Arq.NUMR_INSCRICAO
            #                                                  And Arq1.REFE_ARQUIVO               = Arq.REFE_ARQUIVO
            #                                                  And Arq1.REFE_ARQUIVO         Between {inicio_referencia} And {fim_referencia}
            #                                                  And Arq1.STAT_PROCESM_ARQUIVO       = 'S'
            #                                                  And Arq1.TIPO_MODELO_ARQUIVO        = 'E')
            #              --And Arq.ID_ARQUIVO In (Select Max(Arq1.ID_ARQUIVO)
            #              --                         From EFD_ARQUIVO Arq1
            #              --                        Where Arq1.NUMR_INSCRICAO             = Arq.NUMR_INSCRICAO
            #              --                          And Arq1.REFE_ARQUIVO               = Arq.REFE_ARQUIVO
            #              --                          And Arq1.REFE_ARQUIVO         Between {inicio_referencia} And {fim_referencia}
            #              --                          And Arq1.STAT_PROCESM_ARQUIVO       = 'S')
            #            Group By NFe.ID_NOTA_FISCAL
            #                   , Arq.NUMR_INSCRICAO
            #                   , NVL(Partct.NUMR_INSCRICAO, PartExt.NUMR_INSCRICAO_ESTADUAL)
            #                   , Arq.NUMR_CNPJ
            #                   , Arq.REFE_ARQUIVO
            #                   , NFe.DATA_EMISSAO"""

            sql = f"""Select NFe.ID_ARQUIVO                                              As id_arquivo
                           , NFe.ID_NOTA_FISCAL                                          As id_nfe
                           , Item.ID_ITEM_NOTA_FISCAL
                           , CAST(NVL(Partct.NUMR_INSCRICAO, TO_NUMBER(TRIM(PartExt.NUMR_INSCRICAO_ESTADUAL))) As varchar(14)) As ie_saida
                           , NVL(PartExt.CODG_MUNICIPIO, null)                           As codg_municipio_saida
                           , NVL(CODG_UF, 'GO')                                          As codg_uf_saida
                           , TO_NUMBER(TO_CHAR(NFe.DATA_EMISSAO, 'YYYYMM'))              As numr_ref_emissao
                           , NFe.DATA_EMISSAO                                            As data_emissao_doc
                           , Item.CODG_CFOP
                           , Item.VALR_TOTAL - Item.VALR_DESCONTO - Item.VALR_IPI        As valr_va
                        From EFD_NOTA_FISCAL                           NFe
                              Inner Join EFD_ITEM_NOTA_FISCAL          Item    On NFe.ID_NOTA_FISCAL       = Item.ID_NOTA_FISCAL
                              Left  Join EFD_CONTRIBUINTE_PARTICIPANTE Partct  On NFe.ID_CONTRIB_PARTCT    = Partct.ID_CONTRIB_PARTCT
                              Left  Join EFD_PARTICIPANTE_EXTERNO      PartExt On Partct.ID_PARTCT_EXTERNO = PartExt.ID_PARTCT_EXTERNO 
                              Left  Join GEN_MUNICIPIO                 Munic   On PartExt.CODG_MUNICIPIO   = Munic.CODG_MUNICIPIO       
                       Where NFe.CODG_CHAVE_ACESSO_NFE          Is Not Null 
                         And NFe.TIPO_OPERACAO                   = 0                                                               -- NFe de Entrada --
                         And NFe.CODG_SITUACAO_DOCUMENTO_FISCAL In (0, 1, 6, 7, 8)                                                 -- Situação do Documento Fiscal (NF-e) --
                         And Item.CODG_CFOP                     In (1406, 1407, 1551, 1556, 2406, 2407, 2551, 2556, 3551, 3556)    -- CFOPs de ativo imobilizado e materiais para uso e consumo --
                         And NFe.ID_ARQUIVO                      = {p_id_arquivo}
                         And NFe.DATA_EMISSAO                    > TO_DATE('2022-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')"""

            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                colunas = [col[0].lower() for col in cursor.description]
                dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]

            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_nfa(self, p_data_inicio=str, p_data_fim=str) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - class select_nfa - {p_data_inicio} a {p_data_fim}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca de informações da NFA no período de {p_data_inicio} a {p_data_fim}"
            sql = f"""SELECT Ident.NUMR_IDENTIF_NOTA                                       As id_nfa
                           , Item.ID_PRODUTO_SERVICO                                       As id_item_nota_fiscal
                           , (Select NUMR_INSCRICAO 
                                From NFA.NFA_PARTE_ENVOLVIDA Envol
                               Where Envol.TIPO_ENVOLVIDO = 3
                                 And Ident.NUMR_IDENTIF_NOTA = Envol.NUMR_IDENTIF_NOTA)    As ie_entrada
                           , (Select NUMR_INSCRICAO
                                From NFA.NFA_PARTE_ENVOLVIDA Envol
                               Where Envol.TIPO_ENVOLVIDO = 2
                                 And Ident.NUMR_IDENTIF_NOTA = Envol.NUMR_IDENTIF_NOTA)    As ie_saida
                           , CODG_MUNICIPIO_DESTINO                                        As codg_municipio_entrada
                           , CODG_MUNICIPIO_ORIGEM                                         As codg_municipio_saida
                           , (Select COALESCE(NUMR_CNPJ_ENVOLVIDO, NUMR_CPF_ENVOLVIDO)
                                From NFA.NFA_PARTE_ENVOLVIDA Envol
                               Where Envol.TIPO_ENVOLVIDO = 3
                                 And Ident.NUMR_IDENTIF_NOTA = Envol.NUMR_IDENTIF_NOTA)    As numr_cpf_cnpj_dest
                           , Ident.CODG_NATUREZA_OPERACAO                                  As desc_natureza_operacao
                           , '55'                                                          As codg_modelo_nfa
                           , TO_NUMBER(TO_CHAR(Ident.DATA_HORA_EMISSAO, 'yyyymm'))         As numr_ref_emissao
                           , Ident.DATA_HORA_EMISSAO                                       As data_emissao_nfe
                           , CASE Ident.TIPO_NOTA                
                                  WHEN 1 THEN 'E'
                                  WHEN 2 THEN 'S'
                                  ELSE        'D'                                    END   As indi_tipo_operacao
                           , CASE Ident.TIPO_OPERACAO_NOTA
                                  WHEN 1 THEN 'Operação interna'
                                  WHEN 2 THEN 'Operação interestadual'
                                  WHEN 3 THEN 'Operação com exterior'
                                  ELSE        'Operação não identificada'            END As desc_destino_operacao
                           , CASE Ident.TIPO_OPERACAO_NOTA
                                  WHEN 1 THEN 'NF-e normal'
                                  WHEN 2 THEN 'NF-e normal'
                                  WHEN 3 THEN 'NF-e normal'
                                  WHEN 4 THEN 'NF-e complementar'
                                  WHEN 5 THEN 'Devolução de mercadoria'
                                  ELSE        'Finalidade não identificada'          END As desc_finalidade_operacao
                           , Item.NUMR_SEQUENCIAL_ITEM                                   As numr_item
                           , 0                                                           As numr_cfop
                           , 0                                                           As numr_gtin
                           , Item.CODG_CEST                                              As numr_cest
                           , 0                                                           As numr_ncm
                           , Item.QTDE_ITEM                                              As qdade_itens
                           , COALESCE(Item.CODG_PRODUTO_ANP, 0)                          As codg_anp
                           , COALESCE(Ident.VALR_TOTAL_PRODUTO, 0)
                           - COALESCE(Ident.VALR_TOTAL_DESCONTO, 0)
                           + COALESCE(Ident.VALR_FRETE, 0)
                           + COALESCE(Ident.VALR_SEGURO, 0)
                           + COALESCE(Ident.VALR_OUTRA_DESPESA, 0)
                           + COALESCE(Ident.VALR_IPI, 0)
                           - COALESCE(Ident.VALR_TOTAL_ICMS_DESONERADO, 0) 
                           + COALESCE(Ident.VALR_ICMS_SUBSTRIB, 0)                       As valr_va
                           , Case When Ident.STAT_NOTA = 5
                                  Then Ident.NUMR_IDENTIF_NOTA
                                  Else 0                       End                       As numr_protocolo_cancel
                        FROM NFA.NFA_IDENTIF_NOTA                Ident
                              INNER JOIN NFA.NFA_ITEM_NOTA       Item    On   Ident.NUMR_IDENTIF_NOTA = Item.NUMR_IDENTIF_NOTA
                       WHERE Ident.CODG_NATUREZA_OPERACAO IN (101, 102, 103, 104, 111, 112, 113, 201, 202, 203, 204, 301, 302, 303, 401, 402, 403, 501, 502, 503)
                         And Ident.STAT_NOTA              In (4, 5)
                         And Ident.DATA_HORA_EMISSAO BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                         AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')"""

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

    def select_doc_cte(self, p_data_inicio=str, p_data_fim=str) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - select_doc_cte - {p_data_inicio} a {p_data_fim}"
        loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca de informações da CT-e no período de {p_data_inicio} a {p_data_fim}"
            sql = f"""SELECT Ident.ID_DOCUMENTO_CTE
                           , Ident.CODG_MUNICIPIO_FIM_PRESTACAO        AS codg_municipio_entrada
                           , Ident.CODG_MUNICIPIO_INICIO_PREST         AS codg_municipio_saida
                           , TO_CHAR(Ident.DATA_EMISSAO_CTE, 'YYYYMM') As numr_ref_emissao
                           , Ident.DATA_EMISSAO_CTE
                           , Ident.CODG_MODELO_FISCAL_CTE
                           , Ident.TIPO_CTE
                           , Ident.CODG_CFOP
                           , Ident.TIPO_MODAL_CTE
                           , Ident.CODG_MUNICIPIO_INICIO_PREST
                           , Ident.CODG_MUNICIPIO_FIM_PRESTACAO
                           , NatOper.ID_NATUREZA_OPERACAO_CTE
                           , NatOper.DESC_NATUREZA_OPERACAO_CTE 
                        From CTR_IDENTIFICACAO_CTE                    Ident
                              INNER JOIN CTR_NATUREZA_OPERACAO_CTE    NatOper  ON Ident.ID_NATUREZA_OPERACAO_CTE   = NatOper.ID_NATUREZA_OPERACAO_CTE
                       WHERE Ident.DATA_EMISSAO_CTE BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                        AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')"""

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

    def select_cte(self, p_lista_id_doc) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - class select_cte"
        loga_mensagem(etapaProcess)

        max_lote = 1000
        dfs = []

        try:
            for i in range(0, len(p_lista_id_doc), max_lote):
                batch_ids = p_lista_id_doc[i:i + max_lote]
                lista_id_docs = ', '.join(map(str, batch_ids))

                etapaProcess = f"Executa query de busca de informações da CT-e para uma lista de ids de documentos."
                # sql = f"""WITH CTe AS (SELECT Ident.ID_DOCUMENTO_CTE
                #                             , Ident.CODG_MUNICIPIO_FIM_PRESTACAO AS codg_municipio_entrada
                #                             , Ident.CODG_MUNICIPIO_INICIO_PREST  AS codg_municipio_saida
                #                             , Ident.DATA_EMISSAO_CTE
                #                             , Ident.CODG_MODELO_FISCAL_CTE
                #                             , Ident.TIPO_CTE
                #                             , Ident.CODG_CFOP
                #                             , Ident.TIPO_MODAL_CTE
                #                             , Ident.CODG_MUNICIPIO_INICIO_PREST
                #                             , Ident.CODG_MUNICIPIO_FIM_PRESTACAO
                #                             , Doc.DATA_RECEB_CTE
                #                             , Doc.CODG_STATUS_CTR
                #                             , NVL (Val.VALR_TOTAL_SERVICO, 0)    AS valr_total_servico
                #                             , NatOper.ID_NATUREZA_OPERACAO_CTE
                #                             , NatOper.DESC_NATUREZA_OPERACAO_CTE
                #                             , Anul.CODG_CHAVE_ACESSO_ANULACAO
                #                         From CTR_IDENTIFICACAO_CTE                   Ident
                #                               INNER JOIN CTR_DOCUMENTO_CTE           Doc      ON Ident.ID_DOCUMENTO_CTE          = Doc.ID_DOCUMENTO_CTE
                #                               INNER JOIN CTR_VALOR_PRESTACAO_SERVICO Val      ON Ident.ID_DOCUMENTO_CTE          = Val.ID_DOCUMENTO_CTE
                #                               INNER JOIN CTR_NATUREZA_OPERACAO_CTE   NatOper  ON Ident.ID_NATUREZA_OPERACAO_CTE  = NatOper.ID_NATUREZA_OPERACAO_CTE
                #                               LEFT  JOIN CTR_ANULACAO_VALOR          Anul     ON Ident.ID_DOCUMENTO_CTE          = Anul.ID_DOCUMENTO_CTE
                #                        WHERE 1=1
                #           --               AND Doc.CODG_CHAVE_ACESSO_CTE In ('52230287689402003149570040000637821010957733')
                #                          AND Doc.data_receb_cte BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                #                                                     AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS') )
                #              , Entr AS (SELECT PEnvolv.ID_DOCUMENTO_CTE
                #                              , NVL(Partict.NUMR_INSCRICAO, PExt.NUMR_INSCRICAO_ESTADUAL)                         AS ie_entrada
                #                              , NVL(NVL(PExt.NUMR_CNPJ_PARTCT_EXTERNO_CTE, PExt.NUMR_CPF_PARTCT_EXTERNO_CTE), 0)  AS numr_cpf_cnpj_dest
                #                           FROM CTe
                #                                 INNER JOIN CTR_PARTE_ENVOLVIDA_CTE      PEnvolv ON CTe.ID_DOCUMENTO_CTE           = PEnvolv.ID_DOCUMENTO_CTE
                #                                 INNER JOIN CTR_CONTRIBUINTE_PARTCT_CTE  Partict ON Partict.ID_CONTRIB_PARTCT      = PEnvolv.ID_CONTRIB_PARTCT
                #                                 LEFT  JOIN CTR_PARTICIPANTE_EXTERNO_CTE PExt    ON Partict.ID_PARTCT_EXTERNO_CTE  = PExt.ID_PARTCT_EXTERNO_CTE
                #                          WHERE PEnvolv.TIPO_PARTE_ENVOLVIDA_CTE = 3)
                #              , Said AS (SELECT PEnvolv.ID_DOCUMENTO_CTE
                #                              , NVL(Partict.NUMR_INSCRICAO, PExt.NUMR_INSCRICAO_ESTADUAL)                         AS ie_saida
                #                              , NVL(NVL(PExt.NUMR_CNPJ_PARTCT_EXTERNO_CTE, PExt.NUMR_CPF_PARTCT_EXTERNO_CTE), 0)  AS numr_cpf_cnpj_saida
                #                           FROM CTe
                #                                 INNER JOIN CTR_PARTE_ENVOLVIDA_CTE      PEnvolv ON CTe.ID_DOCUMENTO_CTE           = PEnvolv.ID_DOCUMENTO_CTE
                #                                 INNER JOIN CTR_CONTRIBUINTE_PARTCT_CTE  Partict ON Partict.ID_CONTRIB_PARTCT      = PEnvolv.ID_CONTRIB_PARTCT
                #                                 LEFT  JOIN CTR_PARTICIPANTE_EXTERNO_CTE PExt    ON Partict.ID_PARTCT_EXTERNO_CTE  = PExt.ID_PARTCT_EXTERNO_CTE
                #                          WHERE PEnvolv.TIPO_PARTE_ENVOLVIDA_CTE = 0)
                #           SELECT cte.id_documento_cte
                #                , ie_entrada
                #                , ie_saida
                #                , codg_municipio_entrada
                #                , codg_municipio_saida
                #                , numr_cpf_cnpj_saida
                #                , numr_cpf_cnpj_dest
                #                , ID_NATUREZA_OPERACAO_CTE
                #                , DESC_NATUREZA_OPERACAO_CTE
                #                , codg_modelo_fiscal_cte
                #                , TO_CHAR(data_emissao_cte, 'YYYYMM') As numr_ref_emissao
                #                , data_emissao_cte
                #                , TIPO_CTE
                #                , CODG_CFOP
                #                , CODG_STATUS_CTR
                #                , valr_total_servico
                #                , codg_chave_acesso_anulacao
                #             FROM CTe
                #                   INNER JOIN Entr ON CTe.id_documento_cte = Entr.id_documento_cte
                #                   INNER JOIN Said ON CTe.id_documento_cte = Said.id_documento_cte"""

                sql = f"""SELECT Doc.ID_DOCUMENTO_CTE
                               , Doc.CODG_STATUS_CTR
                               , NVL (Val.VALR_TOTAL_SERVICO, 0)                                                       AS valr_va
                               , Anul.CODG_CHAVE_ACESSO_ANULACAO
                            From CTR_DOCUMENTO_CTE                        Doc
                                  INNER JOIN CTR_VALOR_PRESTACAO_SERVICO  Val      ON Doc.ID_DOCUMENTO_CTE             = Val.ID_DOCUMENTO_CTE
                                  LEFT  JOIN CTR_ANULACAO_VALOR           Anul     ON Doc.ID_DOCUMENTO_CTE             = Anul.ID_DOCUMENTO_CTE
                           WHERE Doc.ID_DOCUMENTO_CTE IN ({lista_id_docs})"""

                connections[self.db_alias].close()

                with connections[self.db_alias].cursor() as cursor:
                    cursor.execute(sql)
                    colunas = [col[0].lower() for col in cursor.description]
                    dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]
                    dfs.extend(dados)

            return pd.DataFrame(dfs)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_parametros(self) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def select_parametros"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca dos parâmetros do sistema IPM."
            obj_retorno = Parametro.objects.using(self.db_alias).values('nome_parametro_ipm', 'desc_parametro_ipm').all()
            df_ret = pd.DataFrame(list(obj_retorno))

            return df_ret

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_parametro(self, p_nome_param):
        etapaProcess = f"class {self.__class__.__name__} - def select_parametro - {p_nome_param}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca do valor do parâmetro {p_nome_param}."
            dados = Parametro.objects.using(self.db_alias).filter(nome_parametro_ipm=p_nome_param)

            # Crie instâncias do modelo usando os resultados do QuerySet
            parametros = [Parametro(nome_parametro_ipm=item.nome_parametro_ipm, desc_parametro_ipm=item.desc_parametro_ipm) for item in dados]
            return parametros

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_cfop_ipm(self, p_data_inicio, p_data_fim) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def select_cfop_ipm"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess += f"Executa query de busca da relação de CFOPs para o cálculo do IPM no periodo de {p_data_inicio} a {p_data_fim}."
            dados = CFOP.objects.using(self.db_alias)\
                        .filter(Q(data_inicio_vigencia__lte=p_data_inicio, data_fim_vigencia__gte=p_data_fim)
                                |
                                Q(data_inicio_vigencia__lte=p_data_inicio, data_fim_vigencia__isnull=True)
                                ).values('codg_cfop')
            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_gen_ncm(self, ncms):
        etapaProcess = f"class {self.__class__.__name__} - def select_gen_ncm."
        # loga_mensagem(etapaProcess)

        try:
            # Realizar busca utilizando filter
            dados = GEN_NCM.objects.using(self.db_alias).filter(codg_produto_ncm__in=ncms).values('id_produto_ncm')
            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_cce_cnae(self, cnaes):
        etapaProcess = f"class {self.__class__.__name__} - def select_cce_cnae."
        # loga_mensagem(etapaProcess)

        try:
            tamanho_lote = 20000
            lotes = [cnaes[i:i + tamanho_lote] for i in range(0, len(cnaes), tamanho_lote)]

            dfs = []

            # Itere sobre cada lote e execute a consulta ao banco de dados
            for lote in lotes:
                dados = CCE_CNAE.objects.using(self.db_alias)\
                                .filter(codg_subclasse_cnaef__in=lote)\
                                .values('id_subclasse_cnaef')
                df = pd.DataFrame(dados)
                dfs.append(df)

            # Concatene todos os DataFrames em um único DataFrame
            df_resultado = pd.concat(dfs, ignore_index=True)

            return df_resultado

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_cnae_produtor(self) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def select_cnae_produtor"
        # loga_mensagem(etapaProcess)

        try:
            sql = f"""Select CODG_SUBCLASSE_CNAEF As CODG_CNAE_RURAL
                        From CCE_SUBCLASSE_CNAE_FISCAL           CNAE
                              Inner Join IPM_CNAE_PRODUTOR_RURAL Rural On CNAe.ID_SUBCLASSE_CNAEF = Rural.ID_SUBCLASSE_CNAEF
                   """

            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                colunas = [col[0].lower() for col in cursor.description]
                dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]

            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)

    def select_historico_contribuinte_ipm(self, inscr, ref):
        etapaProcess = f"class {self.__class__.__name__} - def select_historico_contribuinte_ipm."
        # loga_mensagem(etapaProcess)

        try:
            tamanho_lote = 20000
            lotes = [inscr[i:i + tamanho_lote] for i in range(0, len(inscr), tamanho_lote)]

            dfs = []

            # Itere sobre cada lote e execute a consulta ao banco de dados
            for lote in lotes:
                dados = CadastroCCE.objects.using(self.db_alias)\
                                   .filter(numr_inscricao__in=lote,
                                           numr_referencia=ref)\
                                   .values('numr_inscricao')

                df = pd.DataFrame(dados)
                dfs.append(df)

            # Concatene todos os DataFrames em um único DataFrame
            df_resultado = pd.concat(dfs, ignore_index=True)

            return df_resultado

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_inscricoes_nfe(self, p_periodo=int, p_emit=str):
        etapaProcess = f"class {self.__class__.__name__} - def select_inscricoes_nfe para o periodo de {p_periodo}."
        # loga_mensagem(etapaProcess)

        try:
            if p_emit == 'E':
                sql = f"""Select Distinct NUMR_INSCRICAO 
                            From NFE_IDENTIFICACAO
                           Where NUMR_INSCRICAO         Is Not Null
                             And NUMR_INSCRICAO          > 0
                             And LENGTH(NUMR_INSCRICAO) <= 9
                             And TO_CHAR(NFE_IDENTIFICACAO.DATA_EMISSAO_NFE, 'YYYYMM') = '{p_periodo}'
                             --And NUMR_INSCRICAO In (100276890, 100267904)
                       """
            else:
                sql = f"""Select Distinct NUMR_INSCRICAO_DEST As NUMR_INSCRICAO
                            From NFE_IDENTIFICACAO
                           Where NUMR_INSCRICAO_DEST         Is Not Null
                             And NUMR_INSCRICAO_DEST          > 0
                             And LENGTH(NUMR_INSCRICAO_DEST) <= 9
                             And TO_CHAR(NFE_IDENTIFICACAO.DATA_EMISSAO_NFE, 'YYYYMM') = '{p_periodo}'
                             --And NUMR_INSCRICAO_DEST In (100276890, 100267904)
                       """
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

    def select_inscricoes_nfe_recebida(self, p_periodo=int, p_emit=str):
        etapaProcess = f"class {self.__class__.__name__} - def select_inscricoes_nfe_recebida para o periodo de {p_periodo}."
        # loga_mensagem(etapaProcess)

        try:
            if p_emit == 'E':
                sql = f"""Select Distinct NUMR_INSCRICAO_EMITENTE As NUMR_INSCRICAO 
                            From NFE_IDENTIFICACAO_RECEBIDA
                           Where NUMR_INSCRICAO_EMITENTE         Is Not Null
                             And NUMR_INSCRICAO_EMITENTE          > 0
                             And LENGTH(NUMR_INSCRICAO_EMITENTE) <= 9
                             And TO_CHAR(DATA_EMISSAO_NFE, 'YYYYMM') = '{p_periodo}'
                             --And NUMR_INSCRICAO In (100276890, 100267904)
                       """
            else:
                sql = f"""Select Distinct NUMR_INSCRICAO_DEST As NUMR_INSCRICAO
                            From NFE_IDENTIFICACAO_RECEBIDA
                           Where NUMR_INSCRICAO_DEST         Is Not Null
                             And NUMR_INSCRICAO_DEST          > 0
                             And LENGTH(NUMR_INSCRICAO_DEST) <= 9
                             And TO_CHAR(DATA_EMISSAO_NFE, 'YYYYMM') = '{p_periodo}'
                             --And NUMR_INSCRICAO_DEST In (100276890, 100267904)
                       """
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

    def select_inscricoes_efd(self, p_periodo=int):
        etapaProcess = f"class {self.__class__.__name__} - def select_inscricoes_efd para o periodo de {p_periodo}."
        # loga_mensagem(etapaProcess)

        try:
            sql = f"""Select Distinct Arq.NUMR_INSCRICAO
                        From EFD_ARQUIVO                      Arq
                              Inner Join EFD_NOTA_FISCAL      NFe   On Arq.ID_ARQUIVO     = NFe.ID_ARQUIVO
                              Inner Join EFD_ITEM_NOTA_FISCAL Item  On NFe.ID_NOTA_FISCAL = Item.ID_NOTA_FISCAL
                       Where NFe.CODG_CHAVE_ACESSO_NFE          Is Not Null 
                         And Arq.REFE_ARQUIVO                    = {p_periodo}
                         And NFe.TIPO_OPERACAO                   = 0                                                               -- NFe de Entrada --
                         And NFe.CODG_SITUACAO_DOCUMENTO_FISCAL In (0, 1, 6, 7, 8)                                                 -- Situação do Documento Fiscal (NF-e) --
                         And Item.CODG_CFOP                     In (1406, 1407, 1551, 1556, 2406, 2407, 2551, 2556, 3551, 3556)    -- CFOPs de ativo imobilizado e materiais para uso e consumo --
                         And Arq.ID_ARQUIVO In (Select Max(Arq1.ID_ARQUIVO)
                                                  From EFD_ARQUIVO Arq1
                                                 Where Arq1.NUMR_INSCRICAO       = Arq.NUMR_INSCRICAO
                                                   And Arq1.REFE_ARQUIVO         = Arq.REFE_ARQUIVO
                                                   And Arq1.REFE_ARQUIVO         = {p_periodo}
                                                   And Arq1.STAT_PROCESM_ARQUIVO = 'S')
                   """
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

    def select_inscricoes_nfa(self, p_periodo=int):
        etapaProcess = f"class {self.__class__.__name__} - def select_inscricoes_nfa para o periodo de {p_periodo}."
        # loga_mensagem(etapaProcess)

        try:
            sql = f"""SELECT (Select NUMR_INSCRICAO 
                                From NFA.NFA_PARTE_ENVOLVIDA Envol
                               Where Envol.TIPO_ENVOLVIDO = 3
                                 And Ident.NUMR_IDENTIF_NOTA = Envol.NUMR_IDENTIF_NOTA)    As numr_inscricao
                        FROM NFA.NFA_IDENTIF_NOTA                Ident
                              INNER JOIN NFA.NFA_ITEM_NOTA       Item    On   Ident.NUMR_IDENTIF_NOTA = Item.NUMR_IDENTIF_NOTA
                       WHERE Ident.CODG_NATUREZA_OPERACAO IN (101, 102, 103, 104, 111, 112, 113, 201, 202, 203, 204, 301, 302, 303, 401, 402, 403, 501, 502, 503)
                         And Ident.STAT_NOTA              In (4, 5)
                         And TO_NUMBER(TO_CHAR(Ident.DATA_HORA_EMISSAO, 'yyyymm')) = {p_periodo}
                      Union 
                      SELECT (Select NUMR_INSCRICAO 
                                From NFA.NFA_PARTE_ENVOLVIDA Envol
                               Where Envol.TIPO_ENVOLVIDO = 2
                                 And Ident.NUMR_IDENTIF_NOTA = Envol.NUMR_IDENTIF_NOTA)    As numr_inscricao
                        FROM NFA.NFA_IDENTIF_NOTA                Ident
                              INNER JOIN NFA.NFA_ITEM_NOTA       Item    On   Ident.NUMR_IDENTIF_NOTA = Item.NUMR_IDENTIF_NOTA
                       WHERE Ident.CODG_NATUREZA_OPERACAO IN (101, 102, 103, 104, 111, 112, 113, 201, 202, 203, 204, 301, 302, 303, 401, 402, 403, 501, 502, 503)
                         And Ident.STAT_NOTA              In (4, 5)
                         And TO_NUMBER(TO_CHAR(Ident.DATA_HORA_EMISSAO, 'yyyymm')) = {p_periodo}
                       """
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

    def select_cadastro_contrib(self, p_inscrs):
        etapaProcess = f"class {self.__class__.__name__} - def select_cadastro_contrib"
        # loga_mensagem(etapaProcess)

        dfs = []

        try:
            etapaProcess = f"Executa query de busca os históricos de alterações cadastrais dos contribuintes."
            sql = f"""With Municip As (Select Case When Municip.CODG_MUNICIPIO Is Null
                                              Then Distrit.CODG_MUNICIPIO
                                              Else Municip.CODG_MUNICIPIO              End              As CODG_MUNICIPIO
                                            , Case When Municip.NOME_MUNICIPIO Is Null
                                                   Then Distrit.NOME_MUNICIPIO
                                                   Else Municip.NOME_MUNICIPIO         End              As NOME_MUNICIPIO
                                            , Distrit.CODG_MUNICIPIO                                    As CODG_DISTRITO
                                            , Distrit.NOME_MUNICIPIO                                    As NOME_DISTRITO
                                            , Distrit.CODG_UF                                           As CODG_UF
                                         From GEN_MUNICIPIO             Distrit
                                               Left  Join GEN_MUNICIPIO Municip On Distrit.CODG_MUNICIPIO_DISTRITO  = Municip.CODG_MUNICIPIO
                                         Union
                                         Select 0, 'Não informado', 0, 'Não informado', '99' From Dual)

                      Select Contrib.NUMR_INSCRICAO                                  As NUMR_INSCRICAO_CONTRIB
                           , INDI_SITUACAO_CADASTRAL                                 As STAT_CADASTRO_CONTRIB
                           , Case When Simples.INDI_SIMPLES_SIMEI Is Null
                                  Then 2
                                  Else 3                            End              As TIPO_ENQDTO_FISCAL
                           , NVL(MunLog.CODG_DISTRITO, NVL(MunEnd.CODG_DISTRITO, 0)) As CODG_MUNICIPIO
                        From CCE_CONTRIBUINTE                          Contrib
                              Left  Join CCE_ESTAB_CONTRIBUINTE        Estab    On   Contrib.NUMR_INSCRICAO             = Estab.NUMR_INSCRICAO
                              Left  Join GEN_ENDERECO                  Ender    On   Estab.ID_ENDERECO                  = Ender.ID_ENDERECO
                              Left  Join GEN_LOGRADOURO                Logr     On   Ender.CODG_LOGRADOURO              = Logr.CODG_LOGRADOURO
                              Left  Join Municip                       MunLog   On   Logr.CODG_MUNICIPIO                = MunLog.CODG_DISTRITO
                              Left  Join Municip                       MunEnd   On   Ender.CODG_MUNICIPIO               = MunEnd.CODG_DISTRITO
                              Left  Join IPM_OPCAO_CONTRIB_SIMPL_SIMEI Simples  On  (Contrib.NUMR_INSCRICAO             = Simples.NUMR_INSCRICAO_SIMPLES
                                                                                And  Simples.INDI_SIMPLES_SIMEI         = '1'
                                                                                And  Simples.INDI_VALIDADE_INFORMACAO   = 'S'
                                                                                And (Simples.DATA_INICIO_OPCAO_SIMPLES <= SYSDATE
                                                                                And (Simples.DATA_FIM_OPCAO_SIMPLES    >= SYSDATE
                                                                                Or  Simples.DATA_FIM_OPCAO_SIMPLES    Is Null)))
                       Where Contrib.NUMR_INSCRICAO In ({p_inscrs})
--                       Where Contrib.NUMR_INSCRICAO In (Select Distinct NUMR_INSCRICAO_CONTRIB
--                                                          From IPM_CONTRIBUINTE_IPM
--                                                         Where CODG_UF = 'GO'
--                                                           And NUMR_INSCRICAO_CONTRIB > 0)
                         And Estab.INDI_CENTRALIZ = 'S'
                       Order By Contrib.NUMR_INSCRICAO"""

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

    def select_historico_contrib(self, inscrs):
        etapaProcess = f"class {self.__class__.__name__} - def select_historico_contrib"
        # loga_mensagem(etapaProcess)

        max_lote = 1000
        dfs = []

        try:
            etapaProcess = f"Executa query de busca os históricos de alterações cadastrais dos contribuintes."
            sql = f"""With Pessoa   As (Select TO_CHAR(ID_PESSOA)   As ID_PESSOA
                                          From CCE_CONTRIBUINTE CCE
                                         Where CCE.NUMR_INSCRICAO In ({inscrs})
                                         Union
                                        Select VALR_ANTERIOR_COLUNA As ID_PESSOA
                                          From CCE_HISTORICO Hist
                                           Where Hist.NOME_TABELA   = 'CCE_CONTRIBUINTE'
                                             And Hist.NOME_COLUNA   = 'ID_PESSOA'
                                             And Hist.ID_HISTORICO In ({inscrs})
                                        Union
                                        Select TO_CHAR(ID_PESSOA)   As ID_PESSOA
                                          From CCE_PREPOSTO PrePosto
                                         Where PrePosto.NUMR_INSCRICAO In ({inscrs})  
                                        Union
                                        Select VALR_ANTERIOR_COLUNA As ID_PESSOA
                                          From CCE_HISTORICO Hist
                                           Where NOME_TABELA        = 'CCE_PREPOSTO'
                                             And NOME_COLUNA        = 'ID_PESSOA'
                                             And Hist.ID_HISTORICO In ({inscrs}))
                         , HistCCE As (Select CH.NOME_TABELA            As NOME_TABELA,
                                                  CH.NOME_COLUNA            As NOME_COLUNA,
                                                  CH.DATA_HORA_TRANSACAO    As DATA_HORA_TRANSACAO,
                                                  CH.TIPO_TRANSACAO         As TIPO_TRANSACAO,
                                                  CH.VALR_SUBST_COLUNA      As VALR_SUBST_COLUNA,
                                                  CH.VALR_ANTERIOR_COLUNA   As VALR_ANTERIOR_COLUNA,
                                                  CH.ID_HISTORICO           As ID_HISTORICO,
                                                  CH.MATR_FUNC              As MATR_FUNC,
                                                  CH.NOME_USUARIO_TRANSACAO As NOME_USUARIO_TRANSACAO,
                                                  CH.NUMR_INSCRICAO         As NUMR_INSCRICAO,
                                                  CH.ID_SOLICT              As ID_SOLICT,
                                                  CH.INDI_CORRECAO          As INDI_CORRECAO
                                             From CCE_HISTORICO_UNIFICADO CH
                                            Where  CH.NUMR_INSCRICAO In ({inscrs})
                                              And (NOME_TABELA           In ('CCE_CONTRIBUINTE'
                                                                          ,  'CCE_ESTAB_CONTRIBUINTE'
                                                                          ,  'CCE_TELEFONE_ESTAB'
                                                                          ,  'CCE_PRODUTOR_EXTRATOR'
                                                                          ,  'CCE_MUNICIPIO_ATUACAO'
                                                                          ,  'CCE_CONTRIB_PESSOA_JURIDICA'
                                                                          ,  'CCE_PREPOSTO'
                                                                          ,  'CCE_CONTRIB_CNAEF'
                                                                          ,  'CCE_FORMA_ATUA_CONTRIB'
                                                                          ,  'CCE_CONTRIB_TIPO_UNIDADE_AUX')
                                               Or (NOME_TABELA            =  'CCE_SOLICITACAO'
                                              And  NOME_COLUNA            =  'DATA_HOMOLOG')))
                             , HistGEN As (Select CH2.NOME_TABELA            As NOME_TABELA,
                                                  CH2.NOME_COLUNA            As NOME_COLUNA,
                                                  CH2.DATA_HORA_TRANSACAO    As DATA_HORA_TRANSACAO,
                                                  CH2.TIPO_TRANSACAO         As TIPO_TRANSACAO,
                                                  CH2.VALR_SUBST_COLUNA      As VALR_SUBST_COLUNA,
                                                  CH2.VALR_ANTERIOR_COLUNA   As VALR_ANTERIOR_COLUNA,
                                                  CH2.ID_HISTORICO           As ID_HISTORICO,
                                                  CH2.MATR_FUNC              As MATR_FUNC,
                                                  CH2.NOME_USUARIO_TRANSACAO As NOME_USUARIO_TRANSACAO,
                                                  CH2.NUMR_INSCRICAO         As NUMR_INSCRICAO,
                                                  CH2.ID_SOLICT              As ID_SOLICT,
                                                  CH2.INDI_CORRECAO          As INDI_CORRECAO
                                             From GEN_HISTORICO CH2
                                            Where ID_SOLICT               Is NOT NULL
                                              And NOME_TABELA            In ('GEN_PESSOA_JURIDICA'
                                                                          ,  'GEN_PESSOA_FISICA'
                                                                          ,  'GEN_ENDERECO'
                                                                          ,  'GEN_SITE_PESSOA'
                                                                          ,  'GEN_TELEFONE_PESSOA'
                                                                          ,  'GEN_EMAIL_PESSOA')
                                              And NOME_COLUNA             <> 'INDI_HOMOLOG_CADASTRO'
                                              And INDI_CORRECAO            = 'N'
                                              And CH2.NUMR_INSCRICAO In ({inscrs}))
                             , Hist    As (Select NOME_TABELA            As NOME_TABELA,
                                                  NOME_COLUNA            As NOME_COLUNA,
                                                  DATA_HORA_TRANSACAO    As DATA_HORA_TRANSACAO,
                                                  TIPO_TRANSACAO         As TIPO_TRANSACAO,
                                                  VALR_SUBST_COLUNA      As VALR_SUBST_COLUNA,
                                                  VALR_ANTERIOR_COLUNA   As VALR_ANTERIOR_COLUNA,
                                                  ID_HISTORICO           As ID_HISTORICO,
                                                  MATR_FUNC              As MATR_FUNC,
                                                  NOME_USUARIO_TRANSACAO As NOME_USUARIO_TRANSACAO,
                                                  NUMR_INSCRICAO         As NUMR_INSCRICAO,
                                                  ID_SOLICT              As ID_SOLICT,
                                                  INDI_CORRECAO          As INDI_CORRECAO
                                             From HistGEN     
                                            Where ((NOME_TABELA           In ('GEN_PESSOA_JURIDICA'
                                                                           ,  'GEN_PESSOA_FISICA')
                                              And   ID_HISTORICO          In (Select ID_PESSOA From Pessoa))

                                               Or  (NOME_TABELA            = 'GEN_ENDERECO'
                                              And ((ID_HISTORICO          In (Select ID_ENDERECO
                                                                                From CCE_ESTAB_CONTRIBUINTE Estab
                                                                               Where Estab.NUMR_INSCRICAO In ({inscrs})))
                                               Or  (ID_HISTORICO          In (Select ID_ENDERECO
                                                                                From GEN_PESSOA_ENDERECO
                                                                               Where ID_PESSOA In (Select ID_PESSOA From Pessoa))))))
                                           Union
                                           Select NOME_TABELA            As NOME_TABELA,
                                                  NOME_COLUNA            As NOME_COLUNA,
                                                  DATA_HORA_TRANSACAO    As DATA_HORA_TRANSACAO,
                                                  TIPO_TRANSACAO         As TIPO_TRANSACAO,
                                                  VALR_SUBST_COLUNA      As VALR_SUBST_COLUNA,
                                                  VALR_ANTERIOR_COLUNA   As VALR_ANTERIOR_COLUNA,
                                                  ID_HISTORICO           As ID_HISTORICO,
                                                  MATR_FUNC              As MATR_FUNC,
                                                  NOME_USUARIO_TRANSACAO As NOME_USUARIO_TRANSACAO,
                                                  NUMR_INSCRICAO         As NUMR_INSCRICAO,
                                                  ID_SOLICT              As ID_SOLICT,
                                                  INDI_CORRECAO          As INDI_CORRECAO
                                             From HistCCE     
                                            Where 1=1)
                      Select *
                        From Hist
                       Where 1=1"""

            # --                                         Where CODG_UF                = 'GO'
            # --                                           And NUMR_INSCRICAO_CONTRIB > 0)

            # --                                              And CH.INDI_CORRECAO        = 'N'
            # --                                              And CH.ID_SOLICT           Is NOT NULL

            # --                                               Or  (NOME_TABELA            = 'GEN_SITE_PESSOA'
            # --                                              And   ID_HISTORICO          In (Select ID_SITE_PESSOA
            # --                                                                                From GEN_SITE_PESSOA
            # --                                                                               Where ID_PESSOA In (Select ID_PESSOA From Pessoa)))
            #
            # --                                               Or  (NOME_TABELA            = 'GEN_TELEFONE_PESSOA'
            # --                                              And   ID_HISTORICO          In (Select ID_TELEFONE
            # --                                                                                From GEN_TELEFONE_PESSOA
            # --                                                                               Where ID_PESSOA In (Select ID_PESSOA From Pessoa)))
            #
            # --                                               Or  (NOME_TABELA            = 'GEN_EMAIL_PESSOA'
            # --                                              And   ID_HISTORICO          In (Select ID_EMAIL_PESSOA
            # --                                                                                From GEN_EMAIL_PESSOA
            # --                                                                               Where ID_PESSOA In (Select ID_PESSOA From Pessoa))))

            connections[self.db_alias].close()
            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                colunas = [col[0].lower() for col in cursor.description]
                dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]
                dfs.extend(dados)

            return pd.DataFrame(dfs)

        except Exception as err:
            loga_mensagem_erro(sql)
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_cnae_contrib(self, p_inscrs) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def select_cnae_contrib."
        # loga_mensagem(etapaProcess)

        etapaProcess = "Executa query de busca os CNAEs das inscrições estaduais."
        dtypes = {'numr_inscricao': 'int64',
                  'indi_principal': 'str',
                  'perc_atividade_contrib': 'str',
                  'codg_subclasse_cnaef': 'int32',
                  'id_contrib_cnaef': 'int32',
                  'id_subclasse_cnaef': 'int32',
                  }

        try:
            sql = f"""Select CAST(Contrib.NUMR_INSCRICAO As Integer)       As NUMR_INSCRICAO
                           , ContribCNAE.INDI_PRINCIPAL
                           , ContribCNAE.PERC_ATIVIDADE_CONTRIB
                           , CAST(SubCNAE.CODG_SUBCLASSE_CNAEF As Integer) As CODG_SUBCLASSE_CNAEF
                           , CAST(ContribCNAE.ID_CONTRIB_CNAEF As Integer) As ID_CONTRIB_CNAEF
                           , CAST(SubCNAE.ID_SUBCLASSE_CNAEF   As Integer) As ID_SUBCLASSE_CNAEF
                        From CCE_CONTRIBUINTE                      Contrib
                              Inner Join CCE_CONTRIB_CNAEF         ContribCNAE On Contrib.NUMR_INSCRICAO         = ContribCNAE.NUMR_INSCRICAO
                              Inner Join CCE_SUBCLASSE_CNAE_FISCAL SubCNAE     On ContribCNAE.ID_SUBCLASSE_CNAEF = SubCNAE.ID_SUBCLASSE_CNAEF
                       Where Contrib.NUMR_INSCRICAO In ({p_inscrs})
--                       Where Contrib.NUMR_INSCRICAO In (Select Distinct NUMR_INSCRICAO_CONTRIB
--                                                          From IPM_CONTRIBUINTE_IPM
--                                                         Where CODG_UF                = 'GO'
--                                                           And NUMR_INSCRICAO_CONTRIB > 0)
                       Order By NUMR_INSCRICAO"""

            connections[self.db_alias].close()

            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                colunas = [col[0].lower() for col in cursor.description]
                dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]

            return pd.DataFrame(dados)

        except Exception as err:
            loga_mensagem_erro(sql)
            etapaProcess += " - ERRO - " + str(err)
            print(etapaProcess)

    def select_ativo_imobilizado(self, p_data_inicio, p_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def select_ativo_imobilizado - {p_data_inicio} a {p_data_fim}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca de informações de documentos para o ativo Imobilizado - {p_data_inicio} a {p_data_fim}"
            sql = f"""With EFD     As (Select EFD.CODG_CHAVE_ACESSO_NFE
                                            , Doc.CODG_DOCUMENTO_PARTCT_CALCULO
                                         From IPM_DOCUMENTO_PARTCT_CALC_IPM    Doc
                                               Inner Join EFD_NOTA_FISCAL      EFD     On  Doc.CODG_DOCUMENTO_PARTCT_CALCULO = EFD.ID_NOTA_FISCAL
                                        Where Doc.CODG_MOTIVO_EXCLUSAO_CALCULO   Is Null
                                          And Doc.CODG_TIPO_DOC_PARTCT_CALC       = 5
                                          And Doc.DATA_EMISSAO_DOCUMENTO Between TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                                             And TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')
                                          And EFD.CODG_CHAVE_ACESSO_NFE IN ('35240161067161001835550010005422121000096484')
--                                          And EFD.CODG_CHAVE_ACESSO_NFE IN ('31230161490561002901550100018577611898351348', '52230129921718000100550010000247531001614954', '52230137391539000129550010001044341551104434',
--                                                                            '52230112145475000902550490001828431898122390', '52230105448784000139550020003085751004462744', '52230102200608000195550050000962771131616475',
--                                                                            '52230129416475000145550010000022741055122740', '52230161490561008608550100007905251985555456', '52230161490561008608550100007902981612028698',
--                                                                            '52230129921718000100550010000247771001616983', '52230161295473002444550000004930231817622882', '52230129044977000192550010000002931000293117')
)
                         , NFe     As (Select NFe.ID_NFE                         AS CODG_DOCUMENTO_PARTCT_CALCULO_NFE
                                            , EFD.CODG_DOCUMENTO_PARTCT_CALCULO  AS CODG_DOCUMENTO_PARTCT_CALCULO_EFD
                                            , CASE WHEN NFe.CODG_MODELO_NFE = 55
                                                   THEN 10
                                                   ELSE 1  END                   AS CODG_TIPO_DOC_PARTCT_CALC
                                         From NFE_IDENTIFICACAO NFe
                                               Inner Join       EFD On  NFe.CODG_CHAVE_ACESSO_NFE = EFD.CODG_CHAVE_ACESSO_NFE
                                        Where 1=1
                                       Union
                                       Select NFe.ID_NFE_RECEBIDA               AS CODG_DOCUMENTO_PARTCT_CALCULO_NFE
                                            , EFD.CODG_DOCUMENTO_PARTCT_CALCULO AS CODG_DOCUMENTO_PARTCT_CALCULO_EFD
                                            , 2                                 AS CODG_TIPO_DOC_PARTCT_CALC
                                         From NFE_IDENTIFICACAO_RECEBIDA NFe
                                               Inner Join                EFD On  NFe.CODG_CHAVE_ACESSO_NFE = EFD.CODG_CHAVE_ACESSO_NFE
                                        Where 1=1)
                         , ItemEFD As (Select NFe.CODG_DOCUMENTO_PARTCT_CALCULO_NFE
                                            , Doc.CODG_DOCUMENTO_PARTCT_CALCULO
                                            , Doc.CODG_TIPO_DOC_PARTCT_CALC
                                            , Item.CODG_ITEM_DOCUMENTO
                                            , IeEnt.NUMR_INSCRICAO_CONTRIB      As NUMR_INSCRICAO_ENTRADA
                                            , Doc.VALR_ADICIONADO_OPERACAO
                                            , Item.VALR_ADICIONADO
                                            , ItemEFD.NUMR_ITEM
--                                            , ItemEFD.QTDE_ITEM
--                                            , ItemEFD.INFO_COMPL
--                                            , ItemEFD.VALR_TOTAL
                                        From NFe NFe
                                              Inner Join IPM_DOCUMENTO_PARTCT_CALC_IPM Doc     ON  Doc.CODG_DOCUMENTO_PARTCT_CALCULO = NFe.CODG_DOCUMENTO_PARTCT_CALCULO_EFD
                                                                                               AND Doc.CODG_TIPO_DOC_PARTCT_CALC     = 5
                                              Inner Join IPM_ITEM_DOCUMENTO            Item    On  Doc.CODG_DOCUMENTO_PARTCT_CALCULO = Item.CODG_DOCUMENTO_PARTCT_CALCULO
                                                                                               AND Doc.CODG_TIPO_DOC_PARTCT_CALC     = Item.CODG_TIPO_DOC_PARTCT_DOCUMENTO
                                              Inner Join IPM_CONTRIBUINTE_IPM          IeEnt   ON  Doc.ID_CONTRIB_IPM_ENTRADA        = IeEnt.ID_CONTRIB_IPM
                                              Inner Join EFD_NOTA_FISCAL               EFD     ON  Doc.CODG_DOCUMENTO_PARTCT_CALCULO = EFD.ID_NOTA_FISCAL
                                              Inner Join EFD_ITEM_NOTA_FISCAL          ItemEFD On  Item.CODG_ITEM_DOCUMENTO          = ItemEFD.ID_ITEM_NOTA_FISCAL
                                       Where Item.CODG_MOTIVO_EXCLUSAO_CALCULO   Is Null
--                                         And ItemEFD.NUMR_ITEM < 4
)
                         , ItemNFe As (Select NFe.CODG_DOCUMENTO_PARTCT_CALCULO_EFD
                                            , Doc.CODG_DOCUMENTO_PARTCT_CALCULO
                                            , Doc.CODG_TIPO_DOC_PARTCT_CALC
                                            , Item.CODG_ITEM_DOCUMENTO
                                            , IeEnt.NUMR_INSCRICAO_CONTRIB      As NUMR_INSCRICAO_ENTRADA
                                            , Doc.VALR_ADICIONADO_OPERACAO
                                            , Item.VALR_ADICIONADO
                                            , ItemNFe.NUMR_ITEM
                                            , ItemNFe.QTDE_COMERCIAL
                                            , ItemNFe.VALR_TOTAL_BRUTO
                                         From NFe 
                                               Inner Join IPM_DOCUMENTO_PARTCT_CALC_IPM Doc     ON  Doc.CODG_DOCUMENTO_PARTCT_CALCULO  = NFe.CODG_DOCUMENTO_PARTCT_CALCULO_NFE
                                                                                                AND Doc.CODG_TIPO_DOC_PARTCT_CALC      = NFe.CODG_TIPO_DOC_PARTCT_CALC
                                               Inner Join IPM_ITEM_DOCUMENTO            Item    ON  Doc.CODG_DOCUMENTO_PARTCT_CALCULO  = Item.CODG_DOCUMENTO_PARTCT_CALCULO
                                                                                                AND Doc.CODG_TIPO_DOC_PARTCT_CALC      = Item.CODG_TIPO_DOC_PARTCT_DOCUMENTO
                                               Inner Join IPM_CONTRIBUINTE_IPM          IeEnt   ON  Doc.ID_CONTRIB_IPM_ENTRADA         = IeEnt.ID_CONTRIB_IPM
                                               Inner Join NFE_IDENTIFICACAO             NFeGer  ON  Doc.CODG_DOCUMENTO_PARTCT_CALCULO  = NFeGer.ID_NFE
                                                                                                AND NFe.CODG_TIPO_DOC_PARTCT_CALC     In (1, 10) 
                                               Inner Join NFE_ITEM_NOTA_FISCAL          ItemNFe ON  Item.CODG_ITEM_DOCUMENTO           = ItemNFe.ID_ITEM_NOTA_FISCAL
                                        Where Item.CODG_MOTIVO_EXCLUSAO_CALCULO   Is Null
                                          And IeEnt.CODG_UF                       = 'GO'
                                       Union
                                       Select NFe.CODG_DOCUMENTO_PARTCT_CALCULO_EFD
                                            , Doc.CODG_DOCUMENTO_PARTCT_CALCULO
                                            , Doc.CODG_TIPO_DOC_PARTCT_CALC
                                            , Item.CODG_ITEM_DOCUMENTO
                                            , IeEnt.NUMR_INSCRICAO_CONTRIB      As NUMR_INSCRICAO_ENTRADA
                                            , Doc.VALR_ADICIONADO_OPERACAO
                                            , Item.VALR_ADICIONADO
                                            , ItemNFe.NUMR_ITEM
                                            , ItemNFe.QTDE_COMERCIAL
                                            , ItemNFe.VALR_TOTAL_BRUTO
                                         From NFe 
                                               Inner Join IPM_DOCUMENTO_PARTCT_CALC_IPM Doc     ON  Doc.CODG_DOCUMENTO_PARTCT_CALCULO  = NFe.CODG_DOCUMENTO_PARTCT_CALCULO_NFE
                                                                                                AND Doc.CODG_TIPO_DOC_PARTCT_CALC      = NFe.CODG_TIPO_DOC_PARTCT_CALC
                                               Inner Join IPM_ITEM_DOCUMENTO            Item    ON  Doc.CODG_DOCUMENTO_PARTCT_CALCULO  = Item.CODG_DOCUMENTO_PARTCT_CALCULO
                                                                                                AND Doc.CODG_TIPO_DOC_PARTCT_CALC      = Item.CODG_TIPO_DOC_PARTCT_DOCUMENTO
                                               Inner Join IPM_CONTRIBUINTE_IPM          IeEnt   ON  Doc.ID_CONTRIB_IPM_ENTRADA         = IeEnt.ID_CONTRIB_IPM
                                               Inner Join NFE_IDENTIFICACAO_RECEBIDA    NFeRec  ON  Doc.CODG_DOCUMENTO_PARTCT_CALCULO  = NFeRec.ID_NFE_RECEBIDA
                                                                                                AND Doc.CODG_TIPO_DOC_PARTCT_CALC      = 2
                                               Inner Join NFE_ITEM_NOTA_FISCAL          ItemNFe ON  Item.CODG_ITEM_DOCUMENTO           = ItemNFe.ID_ITEM_NOTA_FISCAL
                                       Where Item.CODG_MOTIVO_EXCLUSAO_CALCULO  Is Null
                                         And IeEnt.CODG_UF                       = 'GO')
                         , Ativo  As (Select NFe.CODG_DOCUMENTO_PARTCT_CALCULO_EFD
                                           , NFe.CODG_DOCUMENTO_PARTCT_CALCULO_NFE
                                           , NFe.CODG_TIPO_DOC_PARTCT_CALC
                                           , ItemNFe.CODG_ITEM_DOCUMENTO
                                           , ItemNFe.NUMR_INSCRICAO_ENTRADA
                                           , ItemNFe.VALR_ADICIONADO_OPERACAO
                                           , ItemNFe.VALR_ADICIONADO                 AS VALR_ADICIONADO_ItemNFe
                                           , ItemEFD.VALR_ADICIONADO                 AS VALR_ADICIONADO_ItemEFD
--                                           , ItemEFD.VALR_TOTAL
                                           , ItemEFD.NUMR_ITEM                       AS NUMR_ITEM_ItemEFD
                                           , ItemNFe.NUMR_ITEM                       AS NUMR_ITEM_ItemNFe
                                        From NFe
                                              INNER JOIN ItemNFe ON  NFe.CODG_DOCUMENTO_PARTCT_CALCULO_NFE = ItemNFe.CODG_DOCUMENTO_PARTCT_CALCULO
                                              LEFT  JOIN ItemEFD ON  NFe.CODG_DOCUMENTO_PARTCT_CALCULO_EFD = ItemEFD.CODG_DOCUMENTO_PARTCT_CALCULO
                                                                 AND ItemNFe.NUMR_INSCRICAO_ENTRADA        = ItemEFD.NUMR_INSCRICAO_ENTRADA
                                                                 AND ItemNFe.NUMR_ITEM                     = ItemEFD.NUMR_ITEM)
                      Select CODG_DOCUMENTO_PARTCT_CALCULO_NFE
                           , CODG_ITEM_DOCUMENTO
                           , CODG_TIPO_DOC_PARTCT_CALC
                           , NUMR_INSCRICAO_ENTRADA
                           , NUMR_ITEM_ItemNFe  
                           , VALR_ADICIONADO_OPERACAO
--                           , VALR_TOTAL AS Valor_EFD
                           , VALR_ADICIONADO_ItemNFe
                           , VALR_ADICIONADO_ItemEFD
                        From Ativo"""

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

    def select_operacoes_contribuintes(self, p_data_inicio, p_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def select_operacoes_contribuintes - {p_data_inicio} - {p_data_fim}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca de informações de transações entre Contribuintes - {p_data_inicio} - {p_data_fim}"
            sql = f"""
                      Select IeEnt.NUMR_INSCRICAO_CONTRIB As IeEnt
                           , IeEnt.CODG_UF                As UfIeEnt
                           , IeSai.NUMR_INSCRICAO_CONTRIB As IeSai
                           , IeSai.CODG_UF                As UfIeSai
                        From IPM_DOCUMENTO_PARTCT_CALC_IPM    Doc
                              Inner Join IPM_CONTRIBUINTE_IPM IeEnt ON Doc.ID_CONTRIB_IPM_ENTRADA = IeEnt.ID_CONTRIB_IPM
                              Inner Join IPM_CONTRIBUINTE_IPM IeSai ON Doc.ID_CONTRIB_IPM_SAIDA   = IeSai.ID_CONTRIB_IPM
                       Where Doc.CODG_MOTIVO_EXCLUSAO_CALCULO      Is Null
                         And Doc.CODG_TIPO_DOC_PARTCT_CALC         In ({EnumTipoDocumento.NFe.value}, {EnumTipoDocumento.NFeRecebida.value}, {EnumTipoDocumento.NFA.value})
                         And Doc.DATA_EMISSAO_DOCUMENTO       Between TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                                  And TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')
--                         And ((IeEnt.NUMR_INSCRICAO_CONTRIB IN (101651899, 103160310, 102347239, 106593129) And IeEnt.CODG_UF = 'GO')
--                          Or  (IeSai.NUMR_INSCRICAO_CONTRIB IN (101651899, 103160310, 102347239, 106593129) And IeSai.CODG_UF = 'GO'))
                   """

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

    def select_operacoes_opostas(self, p_inicio_referencia, p_fim_referencia, p_inscrs):
        etapaProcess = f"class {self.__class__.__name__} - def select_operacoes_opostas - {p_inicio_referencia} - {p_fim_referencia}"
        # loga_mensagem(etapaProcess)

        etapaProcess = f"Executa query de busca de informações de transações entre contribuintes para o periodo de {p_inicio_referencia} a {p_fim_referencia}"

        def _executa_lote(l_inscrs):
            sql_parts = []
            params = {}

            for idx, (ie_sai, uf_sai, ie_ent, uf_ent) in enumerate(l_inscrs):
                sql_parts.append(
                    f"""SELECT :ie_sai{idx} AS ie_sai,
                               :uf_sai{idx} AS uf_sai,
                               :ie_ent{idx} AS ie_ent,
                               :uf_ent{idx} AS uf_ent
                          FROM dual"""
                )
                params[f"ie_sai{idx}"] = str(ie_sai)
                params[f"uf_sai{idx}"] = str(uf_sai)
                params[f"ie_ent{idx}"] = str(ie_ent)
                params[f"uf_ent{idx}"] = str(uf_ent)

            sql_params = " UNION ALL ".join(sql_parts)

            sql = f"""With Params As ({sql_params})
                          , Doc As (Select Doc.CODG_DOCUMENTO_PARTCT_CALCULO
                                         , Doc.CODG_TIPO_DOC_PARTCT_CALC
                                         , IeEnt.NUMR_INSCRICAO_CONTRIB       As NUMR_INSCRICAO_ENTRADA
                                         , IeEnt.CODG_UF                      As CODG_UF_ENTRADA
                                         , IeSai.NUMR_INSCRICAO_CONTRIB       As NUMR_INSCRICAO_SAIDA
                                         , IeSai.CODG_UF                      As CODG_UF_SAIDA
                                      From IPM_DOCUMENTO_PARTCT_CALC_IPM             Doc
                                            left  Join IPM_CONTRIBUINTE_IPM          IeEnt On    Doc.ID_CONTRIB_IPM_ENTRADA   = IeEnt.ID_CONTRIB_IPM
                                            left  Join IPM_CONTRIBUINTE_IPM          IeSai On    Doc.ID_CONTRIB_IPM_SAIDA     = IeSai.ID_CONTRIB_IPM
                                            Inner Join Params                        p     On  ((IeSai.NUMR_INSCRICAO_CONTRIB = p.ie_sai AND IeSai.CODG_UF = p.uf_sai)
                                                                                           AND  (IeEnt.NUMR_INSCRICAO_CONTRIB = p.ie_ent AND IeEnt.CODG_UF = p.uf_ent))
                                                                                            OR ((IeSai.NUMR_INSCRICAO_CONTRIB = p.ie_ent AND IeSai.CODG_UF = p.uf_ent)
                                                                                           AND (IeEnt.NUMR_INSCRICAO_CONTRIB  = p.ie_sai AND IeEnt.CODG_UF = p.uf_sai))
                                     Where Doc.CODG_MOTIVO_EXCLUSAO_CALCULO       Is Null
                                       And Doc.CODG_TIPO_DOC_PARTCT_CALC          In ({EnumTipoDocumento.NFe.value}, {EnumTipoDocumento.NFeRecebida.value}, {EnumTipoDocumento.NFA.value})
                                       And Doc.NUMR_REFERENCIA_DOCUMENTO     Between  {p_inicio_referencia} And {p_fim_referencia})
                          , NFe As (Select Doc.CODG_DOCUMENTO_PARTCT_CALCULO
                                         , Doc.CODG_TIPO_DOC_PARTCT_CALC
                                         , Doc.NUMR_INSCRICAO_ENTRADA
                                         , Doc.CODG_UF_ENTRADA
                                         , Doc.NUMR_INSCRICAO_SAIDA
                                         , Doc.CODG_UF_SAIDA
                                         , SUBSTR(ItemNFe.CODG_PRODUTO_NCM, 1, 4) As CODG_PRODUTO_NCM
                                         , ItemNFe.ID_ITEM_NOTA_FISCAL            As CODG_ITEM_DOCUMENTO
                                         , ItemNFe.CODG_CFOP
                                         , ItemNFe.VALR_TOTAL_BRUTO
                                         , ItemIPM.VALR_ADICIONADO
                                      From Doc
                                                Inner Join IPM_ITEM_DOCUMENTO            ItemIPM  ON  Doc.CODG_DOCUMENTO_PARTCT_CALCULO  = ItemIPM.CODG_DOCUMENTO_PARTCT_CALCULO
                                                Inner Join NFE_ITEM_NOTA_FISCAL          ItemNFe  ON  Doc.CODG_DOCUMENTO_PARTCT_CALCULO  = ItemNFe.ID_NFE
                                                                                                  And ItemIPM.CODG_ITEM_DOCUMENTO        = ItemNFe.ID_ITEM_NOTA_FISCAL
                                     Where Doc.CODG_TIPO_DOC_PARTCT_CALC        In ({EnumTipoDocumento.NFe.value}, {EnumTipoDocumento.NFA.value})
                                       And ItemIPM.CODG_CFOP In (5101, 1101, 5102, 1102, 5116, 1116, 5201, 1201, 5202, 1202, 5410, 1410, 6101, 2101, 6102, 2102, 6116, 2116, 6201, 2201, 6202, 2202, 6410, 2410)
                                       And ItemIPM.CODG_MOTIVO_EXCLUSAO_CALCULO Is Null)

                          , NFeR As (Select Doc.CODG_DOCUMENTO_PARTCT_CALCULO
                                              , Doc.CODG_TIPO_DOC_PARTCT_CALC
                                              , Doc.NUMR_INSCRICAO_ENTRADA
                                              , Doc.CODG_UF_ENTRADA
                                              , Doc.NUMR_INSCRICAO_SAIDA
                                              , Doc.CODG_UF_SAIDA
                                              , SUBSTR(ItemNFeR.CODG_PRODUTO_NCM, 1, 4) As CODG_PRODUTO_NCM
                                              , ItemNFeR.ID_ITEM_NOTA_FISCAL            As CODG_ITEM_DOCUMENTO
                                              , ItemNFeR.CODG_CFOP
                                              , ItemNFeR.VALR_TOTAL_BRUTO
                                              , ItemIPM.VALR_ADICIONADO
                                           From Doc
                                                Inner Join IPM_ITEM_DOCUMENTO            ItemIPM  ON  Doc.CODG_DOCUMENTO_PARTCT_CALCULO  = ItemIPM.CODG_DOCUMENTO_PARTCT_CALCULO
                                                Inner Join NFE_ITEM_NOTA_FISCAL          ItemNFeR ON  Doc.CODG_DOCUMENTO_PARTCT_CALCULO  = ItemNFeR.ID_NFE_RECEBIDA
                                                                                                  And ItemIPM.CODG_ITEM_DOCUMENTO        = ItemNFeR.ID_ITEM_NOTA_FISCAL
                                      Where Doc.CODG_TIPO_DOC_PARTCT_CALC = {EnumTipoDocumento.NFeRecebida.value}
                                        And ItemIPM.CODG_CFOP In (5101, 1101, 5102, 1102, 5116, 1116, 5201, 1201, 5202, 1202, 5410, 1410, 6101, 2101, 6102, 2102, 6116, 2116, 6201, 2201, 6202, 2202, 6410, 2410)
                                        And ItemIPM.CODG_MOTIVO_EXCLUSAO_CALCULO Is Null)

                          Select NUMR_INSCRICAO_ENTRADA
                               , CODG_UF_ENTRADA
                               , NUMR_INSCRICAO_SAIDA
                               , CODG_UF_SAIDA
                               , CODG_DOCUMENTO_PARTCT_CALCULO
                               , CODG_TIPO_DOC_PARTCT_CALC
                               , CODG_ITEM_DOCUMENTO
                               , CODG_CFOP
                               , CODG_PRODUTO_NCM
                               , VALR_TOTAL_BRUTO
                               , VALR_ADICIONADO
                            From (Select * From NFe
                                  Union All
                                  Select * From NFeR)
                          """

            with connections[self.db_alias].cursor() as cursor:
                    cursor.execute(sql, params)
                    colunas = [col[0].lower() for col in cursor.description]
                    dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]

            connections[self.db_alias].close()

            return pd.DataFrame(dados)

        try:
            batch_size = 1000
            resultados = []

            for i in range(0, len(p_inscrs), batch_size):
                l_inscrs = p_inscrs[i:i + batch_size]

                while True:
                    try:
                        df = _executa_lote(l_inscrs)
                        if not df.empty:
                            resultados.append(df)
                        break  # sucesso, sai do while

                    except cx_Oracle.DatabaseError as err:
                        loga_mensagem_erro(err)
                        error_obj, = err.args
                        if "ORA-04088" in str(error_obj):
                            # reduz o tamanho do lote
                            if len(l_inscrs) == 1:
                                # já está no menor possível, não adianta continuar
                                raise
                            novo_tamanho = max(1, len(l_inscrs) // 2)
                            loga_mensagem_erro(
                                f"Lote de {len(l_inscrs)} falhou com ORA-04088, tentando com {novo_tamanho}")
                            # redivide o lote em dois pedaços menores
                            for j in range(0, len(l_inscrs), novo_tamanho):
                                sub_lote = l_inscrs[j:j + novo_tamanho]
                                df_sub = _executa_lote(sub_lote)
                                if not df_sub.empty:
                                    resultados.append(df_sub)
                            break  # já tratou o lote quebrando
                        else:
                            raise  # se for outro erro, não trata aqui

            if resultados:
                return pd.concat(resultados, ignore_index=True).drop_duplicates()
            else:
                return pd.DataFrame()

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise


