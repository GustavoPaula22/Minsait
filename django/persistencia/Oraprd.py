import time

import pandas as pd
from django.db import transaction, connections, IntegrityError
from django.db.models import Q
from funcoes.constantes import inicio_referencia, fim_referencia, EnumTipoDocumento, EnumMotivoExclusao
from funcoes.utilitarios import *
from polls.models import DocPartctCalculo, Processamento, \
    Parametro, CFOP, CadastroCCE, GEN_NCM, NCM_Rural, TipoDocumento, TipoProcessamento, MotivoExclusao, \
    CNAE_RURAL, CCE_CNAE, IdentConv115, Municipio, CCEE, AtivCNAE, Monofasico, ContribIPM, models, ItemDoc, \
    IdentSimples, IdentMei


def monta_sql_doc_partct(p_inicio_referencia, p_fim_referencia, p_tipo_documento, p_escopo, p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida, p_chave):
    etapaProcess = f"class {__name__} - def monta_sql_doc_partct."
    # loga_mensagem(etapaProcess)

    try:
        if p_tipo_documento == str(EnumTipoDocumento.NFe.value):
            sql = f"""Select 'Partct'                                             As TipoDado
                           , 'NF-e Gerada'                                        As TipoDoc
                           , Ident.NUMR_INSCRICAO                                 As InscricaoEmit
                           , Ident.NUMR_INSCRICAO_DEST                            As InscricaoDest
                           , IeSai.NUMR_INSCRICAO_CONTRIB                         As IeSaida
                           , CASE WHEN IeSai.INDI_PRODUTOR_RURAL = 'S'
                                  THEN 'Prod Rural'
                                  ELSE CASE WHEN IeSai.STAT_CADASTRO_CONTRIB <> '1'
                                            THEN 'Nao Cad'
                                            ELSE CASE IeSai.TIPO_ENQDTO_FISCAL
                                                    WHEN '1' THEN 'NORMAL'
                                                    WHEN '2' THEN 'NORMAL'
                                                    WHEN '3' THEN 'SIMPLES'
                                                    WHEN '4' THEN 'SIMPLES'
                                                    WHEN '5' THEN 'SIMPLES'
                                                    ELSE        'Nao Cad'
                                                    END END END                   As IeSaidaEnqdto
                           , IeEnt.NUMR_INSCRICAO_CONTRIB                         As IeEntrada
                           , CASE WHEN IeEnt.INDI_PRODUTOR_RURAL = 'S'
                                  THEN 'Prod Rural'
                                  ELSE CASE WHEN IeEnt.STAT_CADASTRO_CONTRIB <> '1'
                                            THEN 'Nao Cad'
                                            ELSE CASE IeEnt.TIPO_ENQDTO_FISCAL
                                                    WHEN '1' THEN 'NORMAL'
                                                    WHEN '2' THEN 'NORMAL'
                                                    WHEN '3' THEN 'SIMPLES'
                                                    WHEN '4' THEN 'SIMPLES'
                                                    WHEN '5' THEN 'SIMPLES'
                                                    ELSE        'Nao Cad'
                             END       END       END                              As IeEntradaEnqdto
                           , Ident.CODG_MUNICIPIO_GERADOR                         As MunicGerador
                           , Ident.CODG_MUNICIPIO_DEST                            As MunicDestino
                           , Doc.CODG_MUNICIPIO_SAIDA                             As MunicSaida
                           , Doc.CODG_MUNICIPIO_ENTRADA                           As MunicEntrada
                           , Ident.NUMR_CNPJ_EMISSOR                              As CnpjEmit
                           , Ident.NUMR_CPF_CNPJ_DEST                             As CnpjCpfDest
                           , Ident.CODG_MODELO_NFE                                As Modelo
                           , Ident.DESC_NATUREZA_OPERACAO                         As NatOper
                           , TO_CHAR(Doc.DATA_EMISSAO_DOCUMENTO, 'YYYYMM')        As numr_ref_emissao
                           , TO_CHAR(Doc.DATA_EMISSAO_DOCUMENTO, 'dd/mm/yyyy hh24:mi:ss') As DataEmissao
                           , Ident.CODG_CHAVE_ACESSO_NFE                          As Chave
                           , CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                                  THEN 'Entrada' 
                                  ELSE 'Saída'                              END   As TipoOperacao
                           , CASE Ident.INDI_NOTA_EXPORTACAO
                                  WHEN 'S' THEN 'Operação com exterior'
                                  ELSE          'Operação interna'          END   As DestinoOperacao
                           , CASE Ident.TIPO_FINALIDADE_NFE 
                                  WHEN 1 THEN 'NF-e normal'
                                  WHEN 2 THEN 'NF-e complementar'
                                  WHEN 3 THEN 'NF-e de ajuste'
                                  WHEN 4 THEN 'Devolução de mercadoria'
                                  ELSE        'Finalidade não identificada' END   As Finalidade
                           , Ident.VALR_NOTA_FISCAL                               As ValorDoc
                           , Ident.NUMR_PROTOCOLO_CANCEL                          As ProtocCancel
                           , Item.CODG_PRODUTO_ANP                                As ANP
                           , Item.CODG_PRODUTO_NCM                                As NCM
                           , Item.CODG_CFOP                                       As CFOP
                           , Item.DESC_ITEM                                       As Item
                           , Item.VALR_TOTAL_BRUTO                                As ValBruto
                           , Item.VALR_DESCONTO                                   As ValDesc
                           , COALESCE(Item.VALR_ICMS_DESONERA, 0.0)               As ValIcmsDeson
                           , Item.VALR_ICMS_SUBTRIB                               As ValSubsTrib
                           , Item.VALR_FRETE                                      As ValFrete
                           , Item.VALR_SEGURO                                     As ValSeguro
                           , Item.VALR_OUTRAS_DESPESAS                            As ValOutros
                           , Item.VALR_IPI                                        As ValIPI
                           , Item.VALR_IMPOSTO_IMPORTACAO                         As ValII
                           , Doc.VALR_ADICIONADO_OPERACAO                         As ValVA
                           , CASE Doc.INDI_APROP
                                  WHEN 'E' THEN 'Entrada'
                                  WHEN 'S' THEN 'Saída'
                                  WHEN 'A' THEN 'Entrada e Saída'
                                  ELSE          'Não apropria'              END   As IndAprop
                           , Item.ID_ITEM_NOTA_FISCAL
                        FROM IPM_DOCUMENTO_PARTCT_CALC_IPM          Doc
                              LEFT  JOIN IPM_CONTRIBUINTE_IPM       IeEnt   ON Doc.ID_CONTRIB_IPM_ENTRADA         = IeEnt.ID_CONTRIB_IPM
                              LEFT  JOIN IPM_CONTRIBUINTE_IPM       IeSai   ON Doc.ID_CONTRIB_IPM_SAIDA           = IeSai.ID_CONTRIB_IPM
                              INNER JOIN NFE_IDENTIFICACAO          Ident   ON  Doc.CODG_DOCUMENTO_PARTCT_CALCULO = Ident.ID_NFE
                              INNER JOIN NFE_ITEM_NOTA_FISCAL       Item    ON  Doc.CODG_DOCUMENTO_PARTCT_CALCULO = Item.ID_NFE
                       Where CODG_TIPO_DOC_PARTCT_CALC = {EnumTipoDocumento.NFe.value}"""
        elif p_tipo_documento == str(EnumTipoDocumento.NFeRecebida.value):
            sql = f"""Select 'Partct'                                             As TipoDado
                           , 'NF-e Gerada'                                        As TipoDoc
                           , Ident.NUMR_INSCRICAO                                 As InscricaoEmit
                           , Ident.NUMR_INSCRICAO_DEST                            As InscricaoDest
                           , IeSai.NUMR_INSCRICAO_CONTRIB                         As IeSaida
                           , CASE WHEN IeSai.INDI_PRODUTOR_RURAL = 'S'
                                  THEN 'Prod Rural'
                                  ELSE CASE WHEN IeSai.STAT_CADASTRO_CONTRIB <> '1'
                                            THEN 'Nao Cad'
                                            ELSE CASE IeSai.TIPO_ENQDTO_FISCAL
                                                    WHEN '1' THEN 'NORMAL'
                                                    WHEN '2' THEN 'NORMAL'
                                                    WHEN '3' THEN 'SIMPLES'
                                                    WHEN '4' THEN 'SIMPLES'
                                                    WHEN '5' THEN 'SIMPLES'
                                                    ELSE        'Nao Cad'
                                                    END END END                   As IeSaidaEnqdto
                           , IeEnt.NUMR_INSCRICAO_CONTRIB                         As IeEntrada
                           , CASE WHEN IeEnt.INDI_PRODUTOR_RURAL = 'S'
                                  THEN 'Prod Rural'
                                  ELSE CASE WHEN IeEnt.STAT_CADASTRO_CONTRIB <> '1'
                                            THEN 'Nao Cad'
                                            ELSE CASE IeEnt.TIPO_ENQDTO_FISCAL
                                                    WHEN '1' THEN 'NORMAL'
                                                    WHEN '2' THEN 'NORMAL'
                                                    WHEN '3' THEN 'SIMPLES'
                                                    WHEN '4' THEN 'SIMPLES'
                                                    WHEN '5' THEN 'SIMPLES'
                                                    ELSE        'Nao Cad'
                             END       END       END                              As IeEntradaEnqdto
                           , Ident.CODG_MUNICIPIO_GERADOR                         As MunicGerador
                           , Ident.CODG_MUNICIPIO_DEST                            As MunicDestino
                           , Doc.CODG_MUNICIPIO_SAIDA                             As MunicSaida
                           , Doc.CODG_MUNICIPIO_ENTRADA                           As MunicEntrada
                           , Ident.NUMR_CNPJ_EMISSOR                              As CnpjEmit
                           , Ident.NUMR_CPF_CNPJ_DEST                             As CnpjCpfDest
                           , Ident.CODG_MODELO_NFE                                As Modelo
                           , Ident.DESC_NATUREZA_OPERACAO                         As NatOper
                           , TO_CHAR(Doc.DATA_EMISSAO_DOCUMENTO, 'YYYYMM')        As numr_ref_emissao
                           , TO_CHAR(Doc.DATA_EMISSAO_DOCUMENTO, 'dd/mm/yyyy hh24:mi:ss') As DataEmissao
                           , Ident.CODG_CHAVE_ACESSO_NFE                          As Chave
                           , CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                                  THEN 'Entrada' 
                                  ELSE 'Saída'                              END   As TipoOperacao
                           , CASE Ident.INDI_NOTA_EXPORTACAO
                                  WHEN 'S' THEN 'Operação com exterior'
                                  ELSE          'Operação interna'          END   As DestinoOperacao
                           , CASE Ident.TIPO_FINALIDADE_NFE 
                                  WHEN 1 THEN 'NF-e normal'
                                  WHEN 2 THEN 'NF-e complementar'
                                  WHEN 3 THEN 'NF-e de ajuste'
                                  WHEN 4 THEN 'Devolução de mercadoria'
                                  ELSE        'Finalidade não identificada' END   As Finalidade
                           , Ident.VALR_NOTA_FISCAL                               As ValorDoc
                           , Ident.NUMR_PROTOCOLO_CANCEL                          As ProtocCancel
                           , Item.CODG_PRODUTO_ANP                                As ANP
                           , Item.CODG_PRODUTO_NCM                                As NCM
                           , Item.CODG_CFOP                                       As CFOP
                           , Item.DESC_ITEM                                       As Item
                           , Item.VALR_TOTAL_BRUTO                                As ValBruto
                           , Item.VALR_DESCONTO                                   As ValDesc
                           , COALESCE(Item.VALR_ICMS_DESONERA, 0.0)               As ValIcmsDeson
                           , Item.VALR_ICMS_SUBTRIB                               As ValSubsTrib
                           , Item.VALR_FRETE                                      As ValFrete
                           , Item.VALR_SEGURO                                     As ValSeguro
                           , Item.VALR_OUTRAS_DESPESAS                            As ValOutros
                           , Item.VALR_IPI                                        As ValIPI
                           , Item.VALR_IMPOSTO_IMPORTACAO                         As ValII
                           , Doc.VALR_ADICIONADO_OPERACAO                         As ValVA
                           , CASE Doc.INDI_APROP
                                  WHEN 'E' THEN 'Entrada'
                                  WHEN 'S' THEN 'Saída'
                                  WHEN 'A' THEN 'Entrada e Saída'
                                  ELSE          'Não apropria'              END   As IndAprop
                           , Item.ID_ITEM_NOTA_FISCAL
                        FROM IPM_DOCUMENTO_PARTCT_CALC_IPM          Doc
                              LEFT  JOIN IPM_CONTRIBUINTE_IPM       IeEnt   ON Doc.ID_CONTRIB_IPM_ENTRADA         = IeEnt.ID_CONTRIB_IPM
                              LEFT  JOIN IPM_CONTRIBUINTE_IPM       IeSai   ON Doc.ID_CONTRIB_IPM_SAIDA           = IeSai.ID_CONTRIB_IPM
                              INNER JOIN NFE_IDENTIFICACAO_RECEBIDA Ident   ON  Doc.CODG_DOCUMENTO_PARTCT_CALCULO = Ident.ID_NFE_RECEBIDA
                              INNER JOIN NFE_ITEM_NOTA_FISCAL       Item    ON  Doc.CODG_DOCUMENTO_PARTCT_CALCULO = Item.ID_NFE_RECEBIDA
                       WHERE CODG_TIPO_DOC_PARTCT_CALC = {EnumTipoDocumento.NFeRecebida.value}"""
        elif p_tipo_documento == str(EnumTipoDocumento.EFD.value):
            sql = f"""Select 'Partct'                                                                     As TipoDado
                           , 'EFD'                                                                        As TipoDoc
                           , Arq.NUMR_INSCRICAO                                                           As InscricaoDest
                           , NVL(Partct.NUMR_INSCRICAO, TO_NUMBER(TRIM(PartExt.NUMR_INSCRICAO_ESTADUAL))) As InscricaoEmit
                           , Doc.NUMR_INSCRICAO_SAIDA                                                     As IeSaida
                           , CASE WHEN IeSai.INDI_PRODUTOR_RURAL = 'S'
                                  THEN 'Prod Rural'
                                  ELSE CASE WHEN IeSai.STAT_CADASTRO_CONTRIB <> '1'
                                            THEN 'Nao Cad'
                                            ELSE CASE IeSai.TIPO_ENQDTO_FISCAL
                                                    WHEN '1' THEN 'NORMAL'
                                                    WHEN '2' THEN 'NORMAL'
                                                    WHEN '3' THEN 'SIMPLES'
                                                    WHEN '4' THEN 'SIMPLES'
                                                    WHEN '5' THEN 'SIMPLES'
                                                    ELSE        'Nao Cad'
                                                    END END END                                           As IeSaidaEnqdto
                           , Doc.NUMR_INSCRICAO_ENTRADA                                                   As IeEntrada
                           , CASE WHEN IeEnt.INDI_PRODUTOR_RURAL = 'S'
                                  THEN 'Prod Rural'
                                  ELSE CASE WHEN IeEnt.STAT_CADASTRO_CONTRIB <> '1'
                                            THEN 'Nao Cad'
                                            ELSE CASE IeEnt.TIPO_ENQDTO_FISCAL
                                                    WHEN '1' THEN 'NORMAL'
                                                    WHEN '2' THEN 'NORMAL'
                                                    WHEN '3' THEN 'SIMPLES'
                                                    WHEN '4' THEN 'SIMPLES'
                                                    WHEN '5' THEN 'SIMPLES'
                                                    ELSE        'Nao Cad'
                             END       END       END                                                      As IeEntradaEnqdto
                           , 0                                                                            As MunicGerador
                           , 0                                                                            As MunicDestino
                           , NVL(Doc.CODG_MUNICIPIO_SAIDA, 0)                                             As MunicSaida
                           , NVL(Doc.CODG_MUNICIPIO_ENTRADA, 0)                                           As MunicEntrada
                           , Ident.CODG_DOCUMENTO_FISCAL_ICMS                                             As Modelo
                           , TO_CHAR(Doc.DATA_EMISSAO_DOCUMENTO, 'YYYYMM')                                As numr_ref_emissao
                           , TO_CHAR(Doc.DATA_EMISSAO_DOCUMENTO, 'dd/mm/yyyy hh24:mi:ss')                 As DataEmissao
                           , Ident.CODG_CHAVE_ACESSO_NFE                                                  As Chave
                           , CASE WHEN Ident.TIPO_OPERACAO = 0 
                                  THEN 'Entrada' 
                                  ELSE 'Saída'                              END                           As TipoOperacao
                           , Ident.VALR_NOTA_FISCAL                                                       As ValorDoc
                           , NVL(NFe.NUMR_PROTOCOLO_CANCEL, NFeRec.NUMR_PROTOCOLO_CANCEL)                 As ProtocCancel
                           , Item.CODG_CFOP                                                               As CFOP
                           , Item.VALR_TOTAL                                                              As ValTotal
                           , Item.VALR_DESCONTO                                                           As ValDesc
                           , Item.VALR_ICMS_SUBSTRIB                                                      As ValSubsTrib
                           , Item.VALR_IPI                                                                As ValIPI
                           , Doc.VALR_ADICIONADO_OPERACAO                                                 As ValVA
                           , CASE Doc.INDI_APROP
                                  WHEN 'E' THEN 'Entrada'
                                  WHEN 'S' THEN 'Saída'
                                  WHEN 'A' THEN 'Entrada e Saída'
                                  ELSE          'Não apropria'              END                           As IndAprop
                           , Item.ID_ITEM_NOTA_FISCAL
                        FROM IPM_DOCUMENTO_PARTCT_CALCULO              Doc
                              LEFT  JOIN IPM_HISTORICO_CONTRIBUINTE    IeEnt   ON  (Doc.NUMR_INSCRICAO_ENTRADA                           = IeEnt.NUMR_INSCRICAO
                                                                               AND  TO_NUMBER(TO_CHAR(DATA_EMISSAO_DOCUMENTO, 'YYYYMM')) = IeEnt.NUMR_REFERENCIA)
                              LEFT  JOIN IPM_HISTORICO_CONTRIBUINTE    IeSai   ON  (Doc.NUMR_INSCRICAO_SAIDA                             = IeSai.NUMR_INSCRICAO
                                                                               AND  TO_NUMBER(TO_CHAR(DATA_EMISSAO_DOCUMENTO, 'YYYYMM')) = IeSai.NUMR_REFERENCIA)
                              LEFT  JOIN EFD_NOTA_FISCAL               Ident   ON   Doc.CODG_DOCUMENTO_PARTCT_CALCULO                    = Ident.ID_NOTA_FISCAL
                              LEFT  JOIN EFD_ITEM_NOTA_FISCAL          Item    ON   Ident.ID_NOTA_FISCAL                                 = Item.ID_NOTA_FISCAL
                              INNER JOIN EFD_ARQUIVO                   Arq     ON   Ident.ID_ARQUIVO                                     = Arq.ID_ARQUIVO
                              LEFT  JOIN EFD_CONTRIBUINTE_PARTICIPANTE Partct  ON   Ident.ID_CONTRIB_PARTCT                              = Partct.ID_CONTRIB_PARTCT
                              LEFT  JOIN EFD_PARTICIPANTE_EXTERNO      PartExt ON   Partct.ID_PARTCT_EXTERNO                             = PartExt.ID_PARTCT_EXTERNO        
                              LEFT  JOIN NFE_IDENTIFICACAO             NFe     ON   Ident.ID_NFE                                         = NFe.ID_NFE
                              LEFT  JOIN NFE_IDENTIFICACAO_RECEBIDA    NFeRec  ON   Ident.ID_NFE_RECEBIDA                                = NFeRec.ID_NFE_RECEBIDA
                       Where CODG_TIPO_DOC_PARTCT_CALC = {EnumTipoDocumento.EFD.value}"""
        elif p_tipo_documento == str(EnumTipoDocumento.NFA.value):
            sql = f"""Select 'Partct'                                                       As TipoDado
                           , 'NFA'                                                        As TipoDoc
                           , CAST(Emit.NUMR_INSCRICAO As varchar(14))                     As InscricaoEmit
                           , CAST(Dest.NUMR_INSCRICAO As varchar(14))                     As InscricaoDest
                           , Doc.NUMR_INSCRICAO_SAIDA                                     As IeSaida
                           , CASE WHEN IeSai.INDI_PRODUTOR_RURAL = 'S'
                                  THEN 'Prod Rural'
                                  ELSE CASE WHEN IeSai.STAT_CADASTRO_CONTRIB <> '1'
                                            THEN 'Nao Cad'
                                            ELSE CASE IeSai.TIPO_ENQDTO_FISCAL
                                                    WHEN '1' THEN 'NORMAL'
                                                    WHEN '2' THEN 'NORMAL'
                                                    WHEN '3' THEN 'SIMPLES'
                                                    WHEN '4' THEN 'SIMPLES'
                                                    WHEN '5' THEN 'SIMPLES'
                                                    ELSE        'Nao Cad'
                                                    END END END                           As IeSaidaEnqdto
                           , Doc.NUMR_INSCRICAO_ENTRADA                                   As IeEntrada
                           , CASE WHEN IeEnt.INDI_PRODUTOR_RURAL = 'S'
                                  THEN 'Prod Rural'
                                  ELSE CASE WHEN IeEnt.STAT_CADASTRO_CONTRIB <> '1'
                                            THEN 'Nao Cad'
                                            ELSE CASE IeEnt.TIPO_ENQDTO_FISCAL
                                                    WHEN '1' THEN 'NORMAL'
                                                    WHEN '2' THEN 'NORMAL'
                                                    WHEN '3' THEN 'SIMPLES'
                                                    WHEN '4' THEN 'SIMPLES'
                                                    WHEN '5' THEN 'SIMPLES'
                                                    ELSE        'Nao Cad'
                             END       END       END                                      As IeEntradaEnqdto
                           , Ident.CODG_MUNICIPIO_ORIGEM                                  As MunicGerador
                           , Ident.CODG_MUNICIPIO_DESTINO                                 As MunicDestino
                           , Doc.CODG_MUNICIPIO_SAIDA                                     As MunicSaida
                           , Doc.CODG_MUNICIPIO_ENTRADA                                   As MunicEntrada
                           , Emit.NUMR_CNPJ_ENVOLVIDO                                     As CnpjEmit
                           , NVL(Dest.NUMR_CNPJ_ENVOLVIDO, Dest.NUMR_CPF_ENVOLVIDO)       As CnpjCpfDest
                           , '55'                                                         As Modelo
                           , 'Não informado'                                              As NatOper
                           , TO_CHAR(Doc.DATA_EMISSAO_DOCUMENTO, 'YYYYMM')                As numr_ref_emissao
                           , TO_CHAR(Doc.DATA_EMISSAO_DOCUMENTO, 'dd/mm/yyyy hh24:mi:ss') As DataEmissao
                           , '0'                                                          As Chave
                           , CASE WHEN Ident.TIPO_NOTA = 1
                                  THEN 'Entrada' 
                                  ELSE 'Saída'                                      END   As TipoOperacao
                           , CASE Ident.TIPO_OPERACAO_NOTA
                                  WHEN 1 THEN 'Operação interna'
                                  WHEN 2 THEN 'Operação interestadual'
                                  WHEN 3 THEN 'Operação com exterior'
                                  ELSE        'Operação não identificada'           END   As DestinoOperacao
                           , CASE Ident.TIPO_OPERACAO_NOTA
                                  WHEN 1 THEN 'NF-e normal'
                                  WHEN 2 THEN 'NF-e normal'
                                  WHEN 3 THEN 'NF-e normal'
                                  WHEN 4 THEN 'NF-e complementar'
                                  WHEN 5 THEN 'Devolução de mercadoria'
                                  ELSE        'Finalidade não identificada'         END   As Finalidade
                           , Ident.VALR_TOTAL_NOTA                                        As ValorDoc
                           , Case When Ident.STAT_NOTA = 5
                                  Then Ident.NUMR_IDENTIF_NOTA
                                  Else 0                       End                        As ProtocCancel
                           , Item.CODG_PRODUTO_ANP                                        As ANP
                           , 0                                                            As NCM
                           , Ident.CODG_CFOP                                              As CFOP
                           , Item.DESC_PRODUTO                                            As Item
                           , Item.VALR_UNITARIO_VENDA
                           * Item.QTDE_ITEM                                               As ValBruto
                           , Item.VALR_DESCONTO_ITEM
                           * Item.QTDE_ITEM                                               As ValDesc
                           , COALESCE(Item.VALR_ICMS_DESONERADO, 0.0)                     As ValIcmsDeson
                           , Ident.VALR_ICMS_SUBSTRIB                                     As ValSubsTrib
                           , Ident.VALR_FRETE                                             As ValFrete
                           , Ident.VALR_SEGURO                                            As ValSeguro
                           , Ident.VALR_OUTRA_DESPESA                                     As ValOutros
                           , Ident.VALR_IPI                                               As ValIPI
                           , 0                                                            As ValII
                           , Doc.VALR_ADICIONADO_OPERACAO                                 As ValVA
                           , CASE Doc.INDI_APROP
                                  WHEN 'E' THEN 'Entrada'
                                  WHEN 'S' THEN 'Saída'
                                  WHEN 'A' THEN 'Entrada e Saída'
                                  ELSE          'Não apropria'                      END   As IndAprop
                           , Item.NUMR_SEQUENCIAL_ITEM                                    As ID_ITEM_NOTA_FISCAL

                        FROM IPM_DOCUMENTO_PARTCT_CALCULO           Doc
                              LEFT  JOIN IPM_HISTORICO_CONTRIBUINTE IeEnt   ON (Doc.NUMR_INSCRICAO_ENTRADA        = IeEnt.NUMR_INSCRICAO
                                                                            AND Doc.NUMR_REFERENCIA               = IeEnt.NUMR_REFERENCIA)
                              LEFT  JOIN IPM_HISTORICO_CONTRIBUINTE IeSai   ON (Doc.NUMR_INSCRICAO_SAIDA          = IeSai.NUMR_INSCRICAO
                                                                            AND Doc.NUMR_REFERENCIA               = IeSai.NUMR_REFERENCIA)
                              INNER JOIN NFA.NFA_IDENTIF_NOTA       Ident   ON  Doc.CODG_DOCUMENTO_PARTCT_CALCULO = Ident.NUMR_IDENTIF_NOTA
                              INNER JOIN NFA.NFA_ITEM_NOTA          Item    ON  Doc.CODG_DOCUMENTO_PARTCT_CALCULO = Item.NUMR_IDENTIF_NOTA
                              LEFT  JOIN NFA.NFA_PARTE_ENVOLVIDA    Emit    ON (Ident.NUMR_IDENTIF_NOTA           = Emit.NUMR_IDENTIF_NOTA
                                                                            AND Emit.TIPO_ENVOLVIDO               = 2)
                              LEFT  JOIN NFA.NFA_PARTE_ENVOLVIDA    Dest    ON (Ident.NUMR_IDENTIF_NOTA           = Dest.NUMR_IDENTIF_NOTA
                                                                            AND Dest.TIPO_ENVOLVIDO               = 3)
                       Where CODG_TIPO_DOC_PARTCT_CALC = {EnumTipoDocumento.NFA.value}"""
        else:
            return None
        sql += f"""      And TO_NUMBER(TO_CHAR(Doc.DATA_EMISSAO_DOCUMENTO, 'YYYYMM')) BETWEEN {p_inicio_referencia} AND {p_fim_referencia}"""
        sql += f"""    --AND Ident.ID_NFE = 5648376134"""
        if p_escopo == 'ChaveEletr':
            sql += f"""  AND Ident.CODG_CHAVE_ACESSO_NFE = '{p_chave}'"""
        elif p_escopo == 'Periodo':
            if p_data_inicio == 'None' or p_data_fim == 'None':
                if p_data_inicio != 'None':
                    sql += f"""
                         AND Doc.DATA_EMISSAO_DOCUMENTO = TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')"""
                else:
                    sql += f"""
                         AND Doc.DATA_EMISSAO_DOCUMENTO = TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')"""
            else:
                sql += f"""
                         AND Doc.DATA_EMISSAO_DOCUMENTO BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                            AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')"""
        elif p_escopo == 'Inscricao':
            if len(p_ie_entrada) > 0 and p_ie_entrada != ' ':
                sql += f"""
                         AND Doc.NUMR_INSCRICAO_ENTRADA = {p_ie_entrada}"""
            if len(p_ie_saida) > 0 and p_ie_saida != ' ':
                sql += f"""
                         AND Doc.NUMR_INSCRICAO_SAIDA   = {p_ie_saida}"""
        elif p_escopo == 'Periodo/Inscricao':
            if p_data_inicio == 'None' or p_data_fim == 'None':
                if p_data_inicio != 'None':
                    sql += f"""
                         AND Doc.DATA_EMISSAO_DOCUMENTO = TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')"""
                else:
                    sql += f"""
                         AND Doc.DATA_EMISSAO_DOCUMENTO = TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')"""
            else:
                sql += f"""
                         AND Doc.DATA_EMISSAO_DOCUMENTO BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                            AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')"""
            if len(p_ie_entrada) > 0 and p_ie_entrada != ' ':
                sql += f"""
                        AND Doc.NUMR_INSCRICAO_ENTRADA = {p_ie_entrada}"""
            if len(p_ie_saida) > 0 and p_ie_saida != ' ':
                sql += f"""
                        AND Doc.NUMR_INSCRICAO_SAIDA   = {p_ie_saida}"""
        else:
            return None

        return sql

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        raise

def monta_sql_doc_nao_partct(p_inicio_referencia, p_fim_referencia, p_tipo_documento, p_escopo, p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida, p_chave):
    etapaProcess = f"class {__name__} - def monta_sql_doc_nao_partct."
    # loga_mensagem(etapaProcess)

    try:
        if p_tipo_documento == str(EnumTipoDocumento.NFe.value):
            sql = f"""Select 'Nao Partct'                                         As TipoDado
                           , 'NF-e Gerada'                                        As TipoDoc
                           , Mot.DESC_MOTIVO_EXCLUSAO_CALCULO                     As MotivoExclusao
                           , Ident.NUMR_INSCRICAO                                 As InscricaoEmit
                           , Ident.NUMR_INSCRICAO_DEST                            As InscricaoDest
                           , Ident.CODG_MUNICIPIO_GERADOR                         As MunicGerador
                           , Ident.CODG_MUNICIPIO_DEST                            As MunicDestino
                           , Ident.NUMR_CNPJ_EMISSOR                              As CnpjEmit
                           , Ident.NUMR_CPF_CNPJ_DEST                             As CnpjCpfDest
                           , Ident.CODG_MODELO_NFE                                As Modelo
                           , Ident.DESC_NATUREZA_OPERACAO                         As NatOper
                           , TO_CHAR(NDoc.DATA_EMISSAO_DOCUMENTO, 'YYYYMM')       As numr_ref_emissao
                           , TO_CHAR(NDoc.DATA_EMISSAO_DOCUMENTO, 'dd/mm/yyyy hh24:mi:ss') As DataEmissao
                           , Ident.CODG_CHAVE_ACESSO_NFE                          As Chave
                           , CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                                  THEN 'Entrada' 
                                  ELSE 'Saída'                              END   As TipoOperacao
                           , CASE Ident.INDI_NOTA_EXPORTACAO
                                  WHEN 'S' THEN 'Operação com exterior'
                                  ELSE          'Operação interna'          END   As DestinoOperacao
                           , CASE Ident.TIPO_FINALIDADE_NFE 
                                  WHEN 1 THEN 'NF-e normal'
                                  WHEN 2 THEN 'NF-e complementar'
                                  WHEN 3 THEN 'NF-e de ajuste'
                                  WHEN 4 THEN 'Devolução de mercadoria'
                                  ELSE        'Finalidade não identificada' END   As Finalidade
                           , Ident.VALR_NOTA_FISCAL                               As ValorDoc
                           , Ident.NUMR_PROTOCOLO_CANCEL                          As ProtocCancel
                        FROM IPM_DOCUMENTO_NAO_PARTCT_CALC           NDoc
                              INNER JOIN NFE_IDENTIFICACAO           Ident   ON  NDoc.CODG_DOCUMENTO_NAO_PARTCT_CALC = Ident.ID_NFE
                              INNER JOIN IPM_MOTIVO_EXCLUSAO_CALCULO Mot     ON  NDoc.CODG_MOTIVO_EXCLUSAO_CALCULO   = Mot.CODG_MOTIVO_EXCLUSAO_CALCULO
                       Where CODG_TIPO_DOC_PARTCT_CALC = 1"""
        elif p_tipo_documento == str(EnumTipoDocumento.NFeRecebida.value):
            sql = f"""Select 'Nao Partct'                                         As TipoDado
                           , 'NF-e Recebida'                                      As TipoDoc
                           , Mot.DESC_MOTIVO_EXCLUSAO_CALCULO                     As MotivoExclusao
                           , Ident.NUMR_INSCRICAO_EMITENTE                        As InscricaoEmit
                           , Ident.NUMR_INSCRICAO_DEST                            As InscricaoDest
                           , Ident.CODG_MUNICIPIO_EMITENTE                        As MunicGerador
                           , Ident.CODG_MUNICIPIO_DEST                            As MunicDestino
                           , Ident.NUMR_CNPJ                                      As CnpjEmit
                           , COALESCE(Ident.NUMR_CNPJ_DEST, NUMR_CPF_DEST)        As CnpjCpfDest
                           , Ident.CODG_MODELO_NFE                                As Modelo
                           , 'Não informado'                                      As NatOper
                           , TO_CHAR(NDoc.DATA_EMISSAO_DOCUMENTO, 'YYYYMM')       As numr_ref_emissao
                           , TO_CHAR(NDoc.DATA_EMISSAO_DOCUMENTO, 'dd/mm/yyyy hh24:mi:ss') As DataEmissao
                           , Ident.CODG_CHAVE_ACESSO_NFE                          As Chave
                           , CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                                  THEN 'Entrada' 
                                  ELSE 'Saída'                              END   As TipoOperacao
                           , 'Operação interna'                                   As DestinoOperacao
                           , CASE Ident.CODG_FINALIDADE_NFE 
                                  WHEN 1 THEN 'NF-e normal'
                                  WHEN 2 THEN 'NF-e complementar'
                                  WHEN 3 THEN 'NF-e de ajuste'
                                  WHEN 4 THEN 'Devolução de mercadoria'
                                  ELSE        'Finalidade não identificada' END   As Finalidade
                           , Ident.VALR_NOTA_FISCAL                               As ValorDoc
                           , Ident.NUMR_PROTOCOLO_CANCEL                          As ProtocCancel
                        FROM IPM_DOCUMENTO_NAO_PARTCT_CALC           NDoc
                              INNER JOIN NFE_IDENTIFICACAO_RECEBIDA  Ident   ON  NDoc.CODG_DOCUMENTO_NAO_PARTCT_CALC = Ident.ID_NFE_RECEBIDA
                              INNER JOIN IPM_MOTIVO_EXCLUSAO_CALCULO Mot     ON  NDoc.CODG_MOTIVO_EXCLUSAO_CALCULO   = Mot.CODG_MOTIVO_EXCLUSAO_CALCULO
                       Where CODG_TIPO_DOC_PARTCT_CALC = 2"""
        elif p_tipo_documento == str(EnumTipoDocumento.EFD.value):
            return None
        else:
            return None

        sql += f"""      And TO_NUMBER(TO_CHAR(Doc.DATA_EMISSAO_DOCUMENTO, 'YYYYMM')) BETWEEN {p_inicio_referencia} AND {p_fim_referencia}"""
        sql += f"""    --AND Ident.ID_NFE = 5988069096"""
        if p_escopo == 'ChaveEletr':
            sql += f"""  AND Ident.CODG_CHAVE_ACESSO_NFE = '{p_chave}'"""
        elif p_escopo == 'Periodo':
            if p_data_inicio == 'None' or p_data_fim == 'None':
                if p_data_inicio != 'None':
                    sql += f"""
                         AND NDoc.DATA_EMISSAO_DOCUMENTO = TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')"""
                else:
                    sql += f"""
                         AND NDoc.DATA_EMISSAO_DOCUMENTO = TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')"""
            else:
                sql += f"""
                         AND NDoc.DATA_EMISSAO_DOCUMENTO BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                             AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')"""
        elif p_escopo == 'Inscricao':
            if len(p_ie_entrada) > 0 and p_ie_entrada != ' ':
                    sql += f"""
                         AND Ident.NUMR_INSCRICAO_DEST = {p_ie_entrada}"""
            if len(p_ie_saida) > 0 and p_ie_saida != ' ':
                if p_tipo_documento == str(EnumTipoDocumento.NFe.value):
                    sql += f"""
                         AND Ident.NUMR_INSCRICAO          = {p_ie_saida}"""
                elif p_tipo_documento == str(EnumTipoDocumento.NFeRecebida.value):
                    sql += f"""
                         AND Ident.NUMR_INSCRICAO_EMITENTE = {p_ie_saida}"""
        elif p_escopo == 'Periodo/Inscricao':
            if p_data_inicio == 'None' or p_data_fim == 'None':
                if p_data_inicio != 'None':
                    sql += f"""
                         AND NDoc.DATA_EMISSAO_DOCUMENTO = TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')"""
                else:
                    sql += f"""
                         AND NDoc.DATA_EMISSAO_DOCUMENTO = TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')"""
            else:
                sql += f"""
                         AND NDoc.DATA_EMISSAO_DOCUMENTO BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                             AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')"""
            if len(p_ie_entrada) > 0 and p_ie_entrada != ' ':
                sql += f"""
                         AND Ident.NUMR_INSCRICAO_DEST  = {p_ie_entrada}"""
            if len(p_ie_saida) > 0 and p_ie_saida != ' ':
                if p_tipo_documento == str(EnumTipoDocumento.NFe.value):
                    sql += f"""
                         AND Ident.NUMR_INSCRICAO       = {p_ie_saida}"""
                elif p_tipo_documento == str(EnumTipoDocumento.NFeRecebida.value):
                    sql += f"""
                         AND Ident.NUMR_INSCRICAO_EMITENTE = {p_ie_saida}"""
        else:
            return None

        return sql

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        raise

def monta_sql_item_nao_partct(p_inicio_referencia, p_fim_referencia, p_tipo_documento, p_escopo, p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida, p_chave):
    etapaProcess = f"class {__name__} - def monta_sql_item_nao_partct."
    # loga_mensagem(etapaProcess)

    try:
        if p_tipo_documento == str(EnumTipoDocumento.NFe.value):
            sql = f"""Select 'Item Nao Partct'                                    As TipoDado
                           , 'NF-e Gerada'                                        As TipoDoc
                           , Mot.DESC_MOTIVO_EXCLUSAO_CALCULO                     As MotivoExclusao
                           , Ident.NUMR_INSCRICAO                                 As InscricaoEmit
                           , Ident.NUMR_INSCRICAO_DEST                            As InscricaoDest
                           , Ident.CODG_MUNICIPIO_GERADOR                         As MunicGerador
                           , Ident.CODG_MUNICIPIO_DEST                            As MunicDestino
                           , Ident.NUMR_CNPJ_EMISSOR                              As CnpjEmit
                           , Ident.NUMR_CPF_CNPJ_DEST                             As CnpjCpfDest
                           , Ident.CODG_MODELO_NFE                                As Modelo
                           , Ident.DESC_NATUREZA_OPERACAO                         As NatOper
                           , TO_CHAR(NItem.DATA_EMISSAO_DOCUMENTO, 'YYYYMM')      As numr_ref_emissao
                           , TO_CHAR(NItem.DATA_EMISSAO_DOCUMENTO, 'dd/mm/yyyy hh24:mi:ss') As DataEmissao
                           , Ident.CODG_CHAVE_ACESSO_NFE                          As Chave
                           , CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                                  THEN 'Entrada' 
                                  ELSE 'Saída'                              END   As TipoOperacao
                           , CASE Ident.INDI_NOTA_EXPORTACAO
                                  WHEN 'S' THEN 'Operação com exterior'
                                  ELSE          'Operação interna'          END   As DestinoOperacao
                           , CASE Ident.TIPO_FINALIDADE_NFE 
                                  WHEN 1 THEN 'NF-e normal'
                                  WHEN 2 THEN 'NF-e complementar'
                                  WHEN 3 THEN 'NF-e de ajuste'
                                  WHEN 4 THEN 'Devolução de mercadoria'
                                  ELSE        'Finalidade não identificada' END   As Finalidade
                           , Ident.VALR_NOTA_FISCAL                               As ValorDoc
                           , Ident.NUMR_PROTOCOLO_CANCEL                          As ProtocCancel
                           , Item.CODG_PRODUTO_ANP                                As ANP
                           , Item.CODG_PRODUTO_NCM                                As NCM
                           , Item.CODG_CFOP                                       As CFOP
                           , Item.DESC_ITEM                                       As Item
                           , Item.VALR_TOTAL_BRUTO                                As ValBruto
                           , Item.VALR_DESCONTO                                   As ValDesc
                           , COALESCE(Item.VALR_ICMS_DESONERA, 0.0)               As ValIcmsDeson
                           , Item.VALR_ICMS_SUBTRIB                               As ValSubsTrib
                           , Item.VALR_FRETE                                      As ValFrete
                           , Item.VALR_SEGURO                                     As ValSeguro
                           , Item.VALR_OUTRAS_DESPESAS                            As ValOutros
                           , Item.VALR_IPI                                        As ValIPI
                           , Item.VALR_IMPOSTO_IMPORTACAO                         As ValII
                        FROM IPM_ITEM_DOC_NAO_PARTCT_CALC            NItem
                              INNER JOIN NFE_ITEM_NOTA_FISCAL        Item    ON  NItem.CODG_ITEM_DOC_NAO_PARTCT_CALC = Item.ID_ITEM_NOTA_FISCAL
                              INNER JOIN NFE_IDENTIFICACAO           Ident   ON  Item.ID_NFE                         = Ident.ID_NFE
                              INNER JOIN IPM_MOTIVO_EXCLUSAO_CALCULO Mot     ON  NItem.CODG_MOTIVO_EXCLUSAO_CALCULO  = Mot.CODG_MOTIVO_EXCLUSAO_CALCULO
                       Where CODG_TIPO_DOC_PARTCT_CALC = 1"""
        elif p_tipo_documento == str(EnumTipoDocumento.NFeRecebida.value):
            sql = f"""Select 'Item Nao Partct'                                    As TipoDado
                           , 'NF-e Recebida'                                      As TipoDoc
                           , Mot.DESC_MOTIVO_EXCLUSAO_CALCULO                     As MotivoExclusao
                           , Ident.NUMR_INSCRICAO_EMITENTE                        As InscricaoEmit
                           , Ident.NUMR_INSCRICAO_DEST                            As InscricaoDest
                           , Ident.CODG_MUNICIPIO_EMITENTE                        As MunicGerador
                           , Ident.CODG_MUNICIPIO_DEST                            As MunicDestino
                           , Ident.NUMR_CNPJ                                      As CnpjEmit
                           , COALESCE(Ident.NUMR_CNPJ_DEST, NUMR_CPF_DEST)        As CnpjCpfDest
                           , Ident.CODG_MODELO_NFE                                As Modelo
                           , 'Não informado'                                      As NatOper
                           , TO_CHAR(NItem.DATA_EMISSAO_DOCUMENTO, 'YYYYMM')      As numr_ref_emissao
                           , TO_CHAR(NItem.DATA_EMISSAO_DOCUMENTO, 'dd/mm/yyyy hh24:mi:ss') As DataEmissao
                           , Ident.CODG_CHAVE_ACESSO_NFE                          As Chave
                           , CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0 
                                  THEN 'Entrada' 
                                  ELSE 'Saída'                              END   As TipoOperacao
                           , 'Operação interna'                                   As DestinoOperacao
                           , CASE Ident.CODG_FINALIDADE_NFE 
                                  WHEN 1 THEN 'NF-e normal'
                                  WHEN 2 THEN 'NF-e complementar'
                                  WHEN 3 THEN 'NF-e de ajuste'
                                  WHEN 4 THEN 'Devolução de mercadoria'
                                  ELSE        'Finalidade não identificada' END   As Finalidade
                           , Ident.VALR_NOTA_FISCAL                               As ValorDoc
                           , Ident.NUMR_PROTOCOLO_CANCEL                          As ProtocCancel
                           , Item.CODG_PRODUTO_ANP                                As ANP
                           , Item.CODG_PRODUTO_NCM                                As NCM
                           , Item.CODG_CFOP                                       As CFOP
                           , Item.DESC_ITEM                                       As Item
                           , Item.VALR_TOTAL_BRUTO                                As ValBruto
                           , Item.VALR_DESCONTO                                   As ValDesc
                           , COALESCE(Item.VALR_ICMS_DESONERA, 0.0)               As ValIcmsDeson
                           , Item.VALR_ICMS_SUBTRIB                               As ValSubsTrib
                           , Item.VALR_FRETE                                      As ValFrete
                           , Item.VALR_SEGURO                                     As ValSeguro
                           , Item.VALR_OUTRAS_DESPESAS                            As ValOutros
                           , Item.VALR_IPI                                        As ValIPI
                           , Item.VALR_IMPOSTO_IMPORTACAO                         As ValII
                        FROM IPM_ITEM_DOC_NAO_PARTCT_CALC            NItem
                              INNER JOIN NFE_ITEM_NOTA_FISCAL        Item    ON  NItem.CODG_ITEM_DOC_NAO_PARTCT_CALC = Item.ID_ITEM_NOTA_FISCAL
                              INNER JOIN NFE_IDENTIFICACAO_RECEBIDA  Ident   ON  Item.ID_NFE_RECEBIDA                = Ident.ID_NFE_RECEBIDA
                              INNER JOIN IPM_MOTIVO_EXCLUSAO_CALCULO Mot     ON  NItem.CODG_MOTIVO_EXCLUSAO_CALCULO  = Mot.CODG_MOTIVO_EXCLUSAO_CALCULO
                       Where CODG_TIPO_DOC_PARTCT_CALC = 2"""
        elif p_tipo_documento == str(EnumTipoDocumento.EFD.value):
            return None
        else:
            return None

        sql += f"""      And TO_NUMBER(TO_CHAR(Doc.DATA_EMISSAO_DOCUMENTO, 'YYYYMM')) BETWEEN {p_inicio_referencia} AND {p_fim_referencia}"""
        sql += f"""    --AND Item.ID_ITEM_NOTA_FISCAL = 34595931421"""
        if p_escopo == 'ChaveEletr':
            sql += f"""  AND Ident.CODG_CHAVE_ACESSO_NFE = '{p_chave}'"""
        elif p_escopo == 'Periodo':
            if p_data_inicio == 'None' or p_data_fim == 'None':
                if p_data_inicio != 'None':
                    sql += f"""
                         AND NItem.DATA_EMISSAO_DOCUMENTO = TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')"""
                else:
                    sql += f"""
                         AND NItem.DATA_EMISSAO_DOCUMENTO = TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')"""
            else:
                sql += f"""
                         AND NItem.DATA_EMISSAO_DOCUMENTO BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                              AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')"""
        elif p_escopo == 'Inscricao':
            if len(p_ie_entrada) > 0 and p_ie_entrada != ' ':
                    sql += f"""
                         AND Ident.NUMR_INSCRICAO_DEST = {p_ie_entrada}"""
            if len(p_ie_saida) > 0 and p_ie_saida != ' ':
                if p_tipo_documento == str(EnumTipoDocumento.NFe.value):
                    sql += f"""
                         AND Ident.NUMR_INSCRICAO          = {p_ie_saida}"""
                elif p_tipo_documento == str(EnumTipoDocumento.NFeRecebida.value):
                    sql += f"""
                         AND Ident.NUMR_INSCRICAO_EMITENTE = {p_ie_saida}"""
        elif p_escopo == 'Periodo/Inscricao':
            if p_data_inicio == 'None' or p_data_fim == 'None':
                if p_data_inicio != 'None':
                    sql += f"""
                         AND NDoc.DATA_EMISSAO_DOCUMENTO = TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')"""
                else:
                    sql += f"""
                         AND NDoc.DATA_EMISSAO_DOCUMENTO = TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')"""
            else:
                sql += f"""
                         AND NDoc.DATA_EMISSAO_DOCUMENTO BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                             AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')"""
            if len(p_ie_entrada) > 0 and p_ie_entrada != ' ':
                sql += f"""
                         AND Ident.NUMR_INSCRICAO_DEST  = {p_ie_entrada}"""
            if len(p_ie_saida) > 0 and p_ie_saida != ' ':
                if p_tipo_documento == str(EnumTipoDocumento.NFe.value):
                    sql += f"""
                         AND Ident.NUMR_INSCRICAO       = {p_ie_saida}"""
                elif p_tipo_documento == str(EnumTipoDocumento.NFeRecebida.value):
                    sql += f"""
                         AND Ident.NUMR_INSCRICAO_EMITENTE = {p_ie_saida}"""
        else:
            return None

        return sql

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        raise

class Oraprd():
    etapaProcess = f"class {__name__} - class Oraprd."
    # loga_mensagem(etapaProcess)

    db_alias = 'oracle'

    def select_nfe_gerada(self, p_data_inicio=str, p_data_fim=str) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - class select_nfe_gerada - {p_data_inicio} a {p_data_fim}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca de informações da NFe no período de {p_data_inicio} a {p_data_fim}"
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
                       WHERE Ident.CODG_MODELO_NFE        = '55'
                         AND Ident.ID_RESULTADO_PROCESM   = 321         -- Nota Denegada = 390     
--                         AND ((Ident.NUMR_INSCRICAO      IN (101651899, 103160310, 102347239, 106593129)
--                          Or   Ident.NUMR_INSCRICAO_DEST IN (101651899, 103160310, 102347239, 106593129))
--                          Or  (Ident.NUMR_CPF_CNPJ_DEST  IN ('03387396000160', '02916265008306', '33000167002155', '15689716001510')
--                          Or   Ident.NUMR_CNPJ_EMISSOR   IN ('03387396000160', '02916265008306', '33000167002155', '15689716001510')))
                         AND Ident.DATA_EMISSAO_NFE BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                        AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')"""

            connections[self.db_alias].close()

            # Medindo o tempo de execução da consulta
            start_time = time.time()

            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                colunas = [col[0].lower() for col in cursor.description]
                dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]

            end_time = time.time()
            loga_mensagem(f"{etapaProcess} - Tempo de execução Oraprod = {end_time - start_time}")

            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_nfe_gerada_pela_chave(self, p_lista_chave_acesso) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - class select_nfe_gerada_pela_chave"
        # loga_mensagem(etapaProcess)

        max_lote = 1000
        dfs = []

        try:
            for i in range(0, len(p_lista_chave_acesso), max_lote):
                batch_ids = p_lista_chave_acesso[i:i + max_lote]
                lista_chaves = ", ".join([f"'{str(x)}'" for x in batch_ids])

                etapaProcess = f"Executa query de busca de informações da NF-e para uma lista de chaves de acesso."
                sql = f"""Select ID_NFE  As codg_documento_partct_calculo
                               , NUMR_PROTOCOLO_CANCEL
                            From NFE_IDENTIFICACAO
                           Where CODG_CHAVE_ACESSO_NFE In ({lista_chaves})"""

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

    def select_nfe_receb_pela_chave(self, p_lista_chave_acesso) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - class select_nfe_receb_pela_chave"
        # loga_mensagem(etapaProcess)

        max_lote = 1000
        dfs = []

        try:
            for i in range(0, len(p_lista_chave_acesso), max_lote):
                batch_ids = p_lista_chave_acesso[i:i + max_lote]
                lista_chaves = ", ".join([f"'{str(x)}'" for x in batch_ids])

                etapaProcess = f"Executa query de busca de informações da NF-e para uma lista de chaves de acesso."
                sql = f"""Select ID_NFE_RECEBIDA As codg_documento_partct_calculo
                               , NUMR_PROTOCOLO_CANCEL
                            From NFE_IDENTIFICACAO_RECEBIDA
                           Where CODG_CHAVE_ACESSO_NFE In ({lista_chaves})"""

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

    def select_nfe_recebida(self, p_data_inicio=str, p_data_fim=str) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - class select_nfe_recebida - {p_data_inicio} a {p_data_fim}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca de informações da NFe no período de {p_data_inicio} a {p_data_fim}"
            sql = f"""SELECT Ident.ID_NFE_RECEBIDA                                               As id_nfe
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
                           , NVL(NUMR_PROTOCOLO_CANCEL, 0)                                       As numr_protocolo_cancel
                        FROM NFE_IDENTIFICACAO_RECEBIDA       Ident 
                              INNER JOIN NFE_ITEM_NOTA_FISCAL Item  ON Ident.ID_NFE_RECEBIDA    = Item.ID_NFE_RECEBIDA
                       WHERE Ident.DATA_EMISSAO_NFE BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                        AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')
--                         AND ((Ident.NUMR_INSCRICAO_EMITENTE IN (101651899, 103160310, 102347239, 106593129)
--                          Or   Ident.NUMR_INSCRICAO_DEST     IN (101651899, 103160310, 102347239, 106593129))
--                          Or  (Ident.NUMR_CNPJ_DEST          IN ('03387396000160', '02916265008306', '33000167002155', '15689716001510')
--                          Or   Ident.NUMR_CNPJ               IN ('03387396000160', '02916265008306', '33000167002155', '15689716001510')))
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

    def select_icms_monofasico(self, itens):
        etapaProcess = f"class {self.__class__.__name__} - def select_icms_monofasico."
        # loga_mensagem(etapaProcess)

        try:
            tamanho_lote = 20000
            lotes = [itens[i:i + tamanho_lote] for i in range(0, len(itens), tamanho_lote)]
            dfs = []

            # Itere sobre cada lote e execute a consulta ao banco de dados
            for lote in lotes:
                dados = Monofasico.objects\
                                  .using(self.db_alias)\
                                  .filter(id_item_nota_fiscal__in=lote)\
                                  .values('id_item_nota_fiscal',
                                          'valr_icms_monofasico_retido',
                                          )
                df = pd.DataFrame(dados)
                dfs.append(df)

            # Concatene todos os DataFrames em um único DataFrame
            if len(dfs) > 0:
                df_resultado = pd.concat(dfs, ignore_index=True)
                return df_resultado
            else:
                return []

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
                                                             And Arq1.DATA_ENTREGA_ARQUIVO Between TO_DATE('{p_data_inicio}', 'yyyy/mm/dd hh24:mi:ss') 
                                                                                               And TO_DATE('{p_data_fim}',    'yyyy/mm/dd hh24:mi:ss')
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
            #            Where 1=1
            #              And Arq.STAT_PROCESM_ARQUIVO            = 'S'
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
                           , CAST(NVL(Partct.NUMR_INSCRICAO, TO_NUMBER(TRIM(PartExt.NUMR_INSCRICAO_ESTADUAL) DEFAULT 0 ON CONVERSION ERROR)) As varchar(14)) As ie_saida
                           , NVL(PartExt.CODG_MUNICIPIO, null)                           As codg_municipio_saida
                           , NVL(CODG_UF, 'GO')                                          As codg_uf_saida
                           , TO_NUMBER(TO_CHAR(NFe.DATA_EMISSAO, 'YYYYMM'))              As numr_ref_emissao
                           , NFe.DATA_EMISSAO                                            As data_emissao_doc
                           , Item.CODG_CFOP                                              As numr_cfop
                           , Item.VALR_TOTAL - Item.VALR_DESCONTO - Item.VALR_IPI        As valr_va
                        From EFD_NOTA_FISCAL                           NFe
                              Inner Join EFD_ITEM_NOTA_FISCAL          Item    On NFe.ID_NOTA_FISCAL       = Item.ID_NOTA_FISCAL
                              Left  Join EFD_CONTRIBUINTE_PARTICIPANTE Partct  On NFe.ID_CONTRIB_PARTCT    = Partct.ID_CONTRIB_PARTCT
                              Left  Join EFD_PARTICIPANTE_EXTERNO      PartExt On Partct.ID_PARTCT_EXTERNO = PartExt.ID_PARTCT_EXTERNO 
                              Left  Join GEN_MUNICIPIO                 Munic   On PartExt.CODG_MUNICIPIO   = Munic.CODG_MUNICIPIO       
                       Where NFe.CODG_CHAVE_ACESSO_NFE          Is Not Null 
                         And NFe.TIPO_OPERACAO                   = 0                                                               -- NFe de Entrada --
                         And NFe.CODG_SITUACAO_DOCUMENTO_FISCAL In (0, 1, 6, 7, 8)                                                 -- Situação do Documento Fiscal (NF-e) --
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

    def select_doc_cte(self, p_data_inicio=str, p_data_fim=str) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - select_doc_cte - {p_data_inicio} a {p_data_fim}"
        loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca de informações da CT-e no período de {p_data_inicio} a {p_data_fim}"
            sql = f"""SELECT Ident.ID_DOCUMENTO_CTE
                           , Ident.CODG_MUNICIPIO_FIM_PRESTACAO        AS codg_municipio_entrada
                           , Ident.CODG_MUNICIPIO_INICIO_PREST         AS codg_municipio_saida
                           , TO_CHAR(Ident.DATA_EMISSAO_CTE, 'YYYYMM') As numr_ref_emissao
                           , Ident.DATA_EMISSAO_CTE                    As data_emissao_doc
                           , Ident.CODG_MODELO_FISCAL_CTE
                           , Ident.TIPO_CTE
                           , Ident.CODG_CFOP                           As numr_cfop
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
                sql = f"""SELECT Doc.ID_DOCUMENTO_CTE
                               , Doc.CODG_STATUS_CTR
                               , NVL (Val.VALR_TOTAL_SERVICO, 0)                                                       AS valr_va
                               , NVL (Val.VALR_RECEBER, 0)                                                             AS valr_receber
                               , NVL(Anul.CODG_CHAVE_ACESSO_ANULACAO, 0)                                               AS codg_chave_acesso_anulacao
                               , NVL(Subst.CODG_CHAVE_ACESSO_CTE_SUBST, 0)                                             AS codg_chave_acesso_cte_subst

                            From CTR_DOCUMENTO_CTE                        Doc
                                  INNER JOIN CTR_VALOR_PRESTACAO_SERVICO  Val      ON Doc.ID_DOCUMENTO_CTE             = Val.ID_DOCUMENTO_CTE
                                  LEFT  JOIN CTR_ANULACAO_VALOR           Anul     ON Doc.ID_DOCUMENTO_CTE             = Anul.ID_DOCUMENTO_CTE
                                  LEFT  JOIN CTR_INFO_SUBSTIT             Subst    ON Doc.CODG_CHAVE_ACESSO_CTE        = Subst.CODG_CHAVE_ACESSO_CTE_SUBST

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

    def insert_ident_conv115(self, p_conv115=None) -> IdentConv115:
        etapaProcess = f"class {self.__class__.__name__} - def insert_ident_conv115."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Insere dados na tabela IPM_CONVERSAO_CONV_115"

            sql = """INSERT INTO IPM_CONVERSAO_CONV_115 (NUMR_INSCRICAO
                                                       , NUMR_DOCUMENTO_FISCAL
                                                       , DATA_EMISSAO_DOCUMENTO
                                                       , DESC_SERIE_DOCUMENTO_FISCAL
                                                       , CODG_SITUACAO_VERSAO_ARQUIVO)
                                                 VALUES (:numr_inscricao,
                                                         :numr_documento_fiscal,
                                                         :data_emissao_documento,
                                                         :desc_serie_documento_fiscal,
                                                         :codg_situacao_versao_arquivo)         
                                              RETURNING ID_CONV_115 INTO :id"""

            with connections[self.db_alias].cursor() as cursor:
                id_var = cursor.var(int)
                cursor.execute(sql, {'numr_inscricao': p_conv115.numr_inscricao,
                                     'numr_documento_fiscal': p_conv115.numr_documento_fiscal,
                                     'data_emissao_documento': p_conv115.data_emissao_documento,
                                     'desc_serie_documento_fiscal': p_conv115.desc_serie_documento_fiscal,
                                     'codg_situacao_versao_arquivo': p_conv115.codg_situacao_versao_arquivo,
                                     'id': id_var
                                     })
                id_conv_115 = id_var.getvalue()[0]

            return id_conv_115

        except IntegrityError as err:
            linha = IdentConv115.objects.using(self.db_alias) \
                                .filter(numr_inscricao=p_conv115.numr_inscricao,
                                        numr_documento_fiscal=p_conv115.numr_documento_fiscal,
                                        data_emissao_documento=p_conv115.data_emissao_documento,
                                        desc_serie_documento_fiscal=p_conv115.desc_serie_documento_fiscal,
                                        codg_situacao_versao_arquivo=p_conv115.codg_situacao_versao_arquivo,
                                        ).first()

            if linha:
                return linha.id_conv_115
            else:
                # Caso ocorra um erro diferente ou o registro não exista, relança a exceção
                raise

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def delete_ident_conv115(self, p_data_inicio, p_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def delete_ident_conv115 - {p_data_inicio} a {p_data_fim}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Exclui dados na tabela IPM_CONVERSAO_CONV_115 para o período fornecido."
            linhas_excluidas = 0

            with connections[self.db_alias].cursor() as cursor:
                sql = f"""Delete From IPM_CONVERSAO_CONV_115
                           Where DATA_EMISSAO_DOCUMENTO BETWEEN :p_data_inicio AND :p_data_fim"""

                cursor.execute(sql, {'p_data_inicio': p_data_inicio, 'p_data_fim': p_data_fim})
                linhas_excluidas += cursor.rowcount

            return linhas_excluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_ident_conv115(self, idconv115=None) -> IdentConv115:
        etapaProcess = f"class {self.__class__.__name__} - def select_ident_conv115"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca do valor do id do convênio 115."
            dados = IdentConv115.objects.using(self.db_alias).filter(numr_inscricao=idconv115.numr_inscricao,
                                                                     refe_apuracao_icms=idconv115.refe_apuracao_icms,
                                                                     numr_documento_fiscal=idconv115.numr_documento_fiscal,
                                                                     )
            obj_retorno = [IdentConv115(id_conv_115=item.id_conv_115) for item in dados]

            return obj_retorno[0]

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def insert_doc_conversao_simples(self, df):
        etapaProcess = f"class {self.__class__.__name__} - def insert_doc_conversao_simples."

        batch_size = 1000
        lookup_batch_size = 1000
        results = []

        # SQL para o MERGE sem RETURNING
        sql_insert = """
                        Merge Into IPM_CONVERSAO_SIMPLES Dest
                        Using (Select :IdDecl As CODG_DECLARACAO_PGDAS
                                    , :numr_cnpj       As NUMR_CNPJ
                                    , :numr_inscricao  As NUMR_INSCRICAO
                                    , :codg_municipio  As CODG_MUNICIPIO
                                 From DUAL) src
                           On (Dest.CODG_DECLARACAO_PGDAS = src.CODG_DECLARACAO_PGDAS
                           And Dest.NUMR_INSCRICAO        = src.NUMR_INSCRICAO
                           And Dest.CODG_MUNICIPIO        = src.CODG_MUNICIPIO)
                                 When Not Matched
                                      Then Insert (Dest.CODG_DECLARACAO_PGDAS,
                                                   Dest.NUMR_CNPJ,
                                                   Dest.NUMR_INSCRICAO,
                                                   Dest.CODG_MUNICIPIO)
                                                   Values (src.CODG_DECLARACAO_PGDAS,
                                                           src.NUMR_CNPJ,
                                                           src.NUMR_INSCRICAO,
                                                           src.CODG_MUNICIPIO)
                     """

        try:
            with connections[self.db_alias].cursor() as cursor:
                # Dividir o DataFrame em lotes para inserção
                for i in range(0, len(df), batch_size):
                    batch_df = df.iloc[i:i + batch_size]

                    # Preparar os parâmetros para inserção
                    params = [{'IdDecl': row['IdDecl'],
                               'numr_cnpj': str(row['numr_cnpj']) if pd.notna(row['numr_cnpj']) else '00000000000000',
                               'numr_inscricao': int(row['numr_inscricao']) if pd.notna(row['numr_inscricao']) else 0,
                               'codg_municipio': int(row['codg_municipio']) if pd.notna(row['codg_municipio']) else 0} for _, row in batch_df.iterrows()]

                    cursor.executemany(sql_insert, params)

            connections[self.db_alias].close()

            with connections[self.db_alias].cursor() as cursor:
                # Fazer o lookup em batches
                for i in range(0, len(df), lookup_batch_size):
                    lookup_df = df.iloc[i:i + lookup_batch_size]
                    codg_list = lookup_df['IdDecl'].tolist()

                    # Construir a consulta dinamicamente usando o operador IN
                    placeholders = ','.join([f':IdDecl{i}' for i in range(len(codg_list))])
                    sql_lookup = f"""
                                     SELECT ID_CONVERSAO_SIMPLES
                                          , CODG_DECLARACAO_PGDAS
                                          , NUMR_INSCRICAO
                                          , CODG_MUNICIPIO
                                       FROM IPM_CONVERSAO_SIMPLES
                                      WHERE CODG_DECLARACAO_PGDAS IN ({placeholders})
                                  """

                    lookup_params = {f'IdDecl{i}': codg for i, codg in enumerate(codg_list)}

                    # Executar a consulta de lookup para obter IDs
                    cursor.execute(sql_lookup, lookup_params)
                    results.extend(cursor.fetchall())

                loga_mensagem('Lookup finalizado')
            connections[self.db_alias].close()

            # Converter os resultados em DataFrame
            result_df = pd.DataFrame(results, columns=['ID_CONVERSAO_SIMPLES', 'CODG_DECLARACAO_PGDAS', 'NUMR_INSCRICAO', 'CODG_MUNICIPIO'])
            return result_df

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def insert_ident_simples(self, p_simples=None) -> IdentSimples:
        etapaProcess = f"class {self.__class__.__name__} - def insert_ident_simples."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Insere dados na tabela IPM_CONVERSAO_SIMPLES"

            sql = """INSERT INTO IPM_CONVERSAO_SIMPLES (CODG_DECLARACAO_PGDAS
                                                     ,  NUMR_CNPJ
                                                VALUES (:codg_declaracao_pgdas,
                                                        :numr_cnpj)         
                                              RETURNING ID_CONV_SIMPLES INTO :id"""

            with connections[self.db_alias].cursor() as cursor:
                id_var = cursor.var(int)
                cursor.execute(sql, {'codg_declaracao_pgdas': p_simples.codg_declaracao_pgdas,
                                     'numr_cnpj': p_simples.numr_cnpj,
                                     'id': id_var
                                     })
                id_simples = id_var.getvalue()[0]

            return id_simples

        except IntegrityError as err:
            linha = IdentSimples.objects.using(self.db_alias) \
                                .filter(codg_declaracao_pgdas=p_simples.codg_declaracao_pgdas,
                                        numr_cnpj=p_simples.numr_cnpj,
                                        ).first()

            if linha:
                return linha.id_simples
            else:
                # Caso ocorra um erro diferente ou o registro não exista, relança a exceção
                raise

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def delete_ident_simples(self):
        etapaProcess = f"class {self.__class__.__name__} - def delete_ident_simples"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Exclui dados na tabela IPM_CONVERSAO_SIMPLES."
            linhas_excluidas = 0

            with connections[self.db_alias].cursor() as cursor:
                sql = f"""Delete From IPM_CONVERSAO_SIMPLES
                           Where 1=1"""

                cursor.execute(sql, {})
                linhas_excluidas += cursor.rowcount

            return linhas_excluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_ident_simples(self, p_id_simples=None) -> IdentSimples:
        etapaProcess = f"class {self.__class__.__name__} - def select_ident_simples"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca do valor do id do Simples Nacional."
            dados = IdentSimples.objects.using(self.db_alias).filter(codg_declaracao_pgdas=p_id_simples.codg_declaracao_pgdas,
                                                                     )
            obj_retorno = [IdentSimples(id_simples=item.id_simples) for item in dados]

            return obj_retorno[0]

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def insert_ident_mei(self, p_mei=None) -> IdentMei:
        etapaProcess = f"class {self.__class__.__name__} - def insert_ident_mei."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Insere dados na tabela IPM_CONVERSAO_MEI"

            sql = """INSERT INTO IPM_CONVERSAO_MEI (CODG_DECLARACAO_DASNSIMEI
                                                 ,  NUMR_CNPJ
                                            VALUES (:codg_declaracao_dasnsimei,
                                                    :numr_cnpj)         
                                         RETURNING ID_CONV_MEI INTO :id"""

            with connections[self.db_alias].cursor() as cursor:
                id_var = cursor.var(int)
                cursor.execute(sql, {'codg_declaracao_pgdas': p_mei.codg_declaracao_dasnsimei,
                                     'numr_cnpj': p_mei.numr_cnpj,
                                     'id': id_var
                                     })
                id_mei = id_var.getvalue()[0]

            return id_mei

        except IntegrityError as err:
            linha = IdentMei.objects.using(self.db_alias) \
                            .filter(codg_declaracao_dasnsimei=p_mei.codg_declaracao_dasnsimei,
                                    numr_cnpj=p_mei.numr_cnpj,
                                    ).first()

            if linha:
                return linha.id_mei
            else:
                # Caso ocorra um erro diferente ou o registro não exista, relança a exceção
                raise

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def delete_ident_mei(self):
        etapaProcess = f"class {self.__class__.__name__} - def delete_ident_mei"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Exclui dados na tabela IPM_CONVERSAO_MEI."
            linhas_excluidas = 0

            with connections[self.db_alias].cursor() as cursor:
                sql = f"""Delete From IPM_CONVERSAO_MEI
                           Where 1=1"""

                cursor.execute(sql, {})
                linhas_excluidas += cursor.rowcount

            return linhas_excluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_ident_mei(self, p_id_mei=None) -> IdentMei:
        etapaProcess = f"class {self.__class__.__name__} - def select_ident_mei"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca do valor do id do MEI."
            dados = IdentMei.objects.using(self.db_alias).filter(codg_declaracao_dasnsimei=p_id_mei.codg_declaracao_dasnsimei,
                                                                 )
            obj_retorno = [IdentMei(id_mei=item.id_mei) for item in dados]

            return obj_retorno[0]

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def insert_item_doc(self, df, chunk_size=100000):
        etapaProcess = f"class {self.__class__.__name__} - def insert_item_doc."
        # loga_mensagem(etapaProcess)

        with connections[self.db_alias].cursor() as cursor:
            lote = []
            total_inserido = 0

            sql = f"""Insert Into IPM_ITEM_DOCUMENTO (CODG_ITEM_DOCUMENTO
                                                   ,  VALR_ADICIONADO
                                                   ,  ID_PROCESM_INDICE
                                                   ,  ID_PRODUTO_NCM
                                                   ,  CODG_MOTIVO_EXCLUSAO_CALCULO
                                                   ,  CODG_DOCUMENTO_PARTCT_CALCULO
                                                   ,  CODG_TIPO_DOC_PARTCT_DOCUMENTO
                                                   ,  CODG_CFOP)
                                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

            for i, row in df.iterrows():
                i_codg_motivo_exclusao = None if pd.isna(row['codg_motivo_exclusao_calculo']) \
                                              or row['codg_motivo_exclusao_calculo'] in [0, 'None'] \
                                              else int(row['codg_motivo_exclusao_calculo'])

                i_id_ncm = None if pd.isna(row['id_produto_ncm']) \
                                or row['id_produto_ncm'] in [0, 'None'] \
                                else int(row['id_produto_ncm'])

                i_codg_cfop = None if pd.isna(row['codg_cfop']) \
                                   or row['codg_cfop'] in [0, 'None'] \
                                   else int(row['codg_cfop'])

                lote.append((row['codg_item_documento'],
                             row['valr_adicionado'],
                             row['id_procesm_indice'],
                             i_id_ncm,
                             i_codg_motivo_exclusao,
                             row['codg_documento_partct_calculo'],
                             row['codg_tipo_doc_partct_calc'],
                             i_codg_cfop)
                            )

                if len(lote) == chunk_size:
                    cursor.executemany(sql, lote)
                    connections[self.db_alias].commit()
                    total_inserido += len(lote)
                    lote = []

            # Insere os dados restantes que não completaram um lote
            if lote:
                cursor.executemany(sql, lote)
                connections[self.db_alias].commit()
                total_inserido += len(lote)

        return total_inserido

    def delete_item_doc(self, p_data_inicio, p_data_fim, p_tipo_documento):
        etapaProcess = f"class {self.__class__.__name__} - def delete_item_doc - Tipo de documento: {p_tipo_documento}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Exclui dados na tabela IPM_ITEM_DOCUMENTO para o período fornecido."
            linhas_excluidas = 0

            with connections[self.db_alias].cursor() as cursor:
                sql = f"""Delete From IPM_ITEM_DOCUMENTO    Item
                           Where Item.CODG_TIPO_DOC_PARTCT_DOCUMENTO  = :p_tipo_doc
                             And Item.CODG_DOCUMENTO_PARTCT_CALCULO  In (Select CODG_DOCUMENTO_PARTCT_CALCULO
                                                                           From IPM_DOCUMENTO_PARTCT_CALC_IPM Doc
                                                                          Where Doc.DATA_EMISSAO_DOCUMENTO    Between :p_data_inicio AND :p_data_fim
                                                                            And Doc.CODG_TIPO_DOC_PARTCT_CALC       = :p_tipo_doc)"""

                cursor.execute(sql, {'p_tipo_doc': p_tipo_documento, 'p_data_inicio': p_data_inicio, 'p_data_fim': p_data_fim})
                linhas_excluidas += cursor.rowcount

            return linhas_excluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def delete_item_doc_docs(self, p_tipo_documento, docs):
        etapaProcess = f"class {self.__class__.__name__} - def delete_item_doc_docs - Tipo de documento: {p_tipo_documento} - {len(docs)} documentos."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Exclui dados na tabela IPM_ITEM_DOCUMENTO para os documentos fornecidos - {len(docs)} documentos."
            linhas_excluidas = 0

            max_lote = 1000
            dfs = []

            for i in range(0, len(docs), max_lote):
                batch_ids = docs[i:i + max_lote]
                lista_docs = ', '.join(map(str, batch_ids))

                sql = f"""Delete From IPM_ITEM_DOCUMENTO    Item
                           Where Item.CODG_TIPO_DOC_PARTCT_DOCUMENTO  = {p_tipo_documento}
                             And Item.CODG_DOCUMENTO_PARTCT_CALCULO  In ({lista_docs})"""

                connections[self.db_alias].close()
                with connections[self.db_alias].cursor() as cursor:
                    cursor.execute(sql)
                    linhas_excluidas += cursor.rowcount

            return linhas_excluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def insert_item_doc_efd(self, df, chunk_size=10000):
        etapaProcess = f"class {self.__class__.__name__} - def insert_item_doc_efd."
        # loga_mensagem(etapaProcess)

        lote = []
        linhas_inseridas = 0

        sql = f"""Insert Into IPM_ITEM_DOCUMENTO (CODG_ITEM_DOCUMENTO
                                               ,  VALR_ADICIONADO
                                               ,  ID_PROCESM_INDICE
                                               ,  ID_PRODUTO_NCM
                                               ,  CODG_MOTIVO_EXCLUSAO_CALCULO
                                               ,  CODG_DOCUMENTO_PARTCT_CALCULO
                                               ,  CODG_TIPO_DOC_PARTCT_DOCUMENTO
                                               ,  CODG_CFOP)
                                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

        try:
            with connections[self.db_alias].cursor() as cursor:
                for i, row in df.iterrows():
                    i_codg_motivo_exclusao = None if pd.isna(row['codg_motivo_exclusao_calculo']) \
                                                  or row['codg_motivo_exclusao_calculo'] in [0, 'None'] \
                                                  else int(row['codg_motivo_exclusao_calculo'])

                    i_id_ncm = None if pd.isna(row['id_produto_ncm']) \
                                    or row['id_produto_ncm'] in [0, 'None'] \
                                    else int(row['id_produto_ncm'])

                    i_codg_cfop = None if pd.isna(row['codg_cfop']) \
                                       or row['codg_cfop'] in [0, 'None'] \
                                       else int(row['codg_cfop'])

                    lote.append((row['codg_item_documento'],
                                 row['valr_adicionado'],
                                 row['id_procesm_indice'],
                                 i_id_ncm,
                                 i_codg_motivo_exclusao,
                                 row['codg_documento_partct_calculo'],
                                 row['codg_tipo_doc_partct_documento'],
                                 i_codg_cfop))

                    if len(lote) == chunk_size:
                        cursor.executemany(sql, lote)
                        connections[self.db_alias].commit()
                        linhas_inseridas += len(lote)
                        lote = []

                if lote:
                    # Insere os dados restantes que não completaram um lote
                    cursor.executemany(sql, lote)
                    connections[self.db_alias].commit()
                    linhas_inseridas += len(lote)

            return linhas_inseridas

        except Exception as err:
            df.name = 'item_doc'
            baixa_csv(df)
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def delete_item_doc_efd(self, p_inscricoes, p_referencia, p_tipo_documento):
        etapaProcess = f"class {self.__class__.__name__} - def delete_item_doc_efd"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"exclui dados na tabela IPM_ITEM_DOCUMENTO para a inscrições da EFD na referencia {p_referencia}."
            linhas_excluidas = 0

            with connections[self.db_alias].cursor() as cursor:
                for i_inscricao in p_inscricoes:
                    sql = f"""Delete From IPM_ITEM_DOCUMENTO    Item
                                    Where Item.CODG_TIPO_DOC_PARTCT_DOCUMENTO  = %s
                                      And Item.CODG_DOCUMENTO_PARTCT_CALCULO  In (Select CODG_DOCUMENTO_PARTCT_CALCULO
                                                                                    From IPM_DOCUMENTO_PARTCT_CALC_IPM    Doc
                                                                                          Inner Join IPM_CONTRIBUINTE_IPM Contrib On Doc.ID_CONTRIB_IPM_ENTRADA = Contrib.ID_CONTRIB_IPM
                                                                                   Where Doc.CODG_TIPO_DOC_PARTCT_CALC  = %s
                                                                                     And Doc.NUMR_REFERENCIA_DOCUMENTO  = %s
                                                                                     And Contrib.NUMR_INSCRICAO_CONTRIB = %s)"""
                    cursor.execute(sql, [p_tipo_documento, p_tipo_documento, p_referencia, i_inscricao])
                    linhas_excluidas += cursor.rowcount

            return linhas_excluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def insert_doc_conversao_nf3e(self, df):
        etapaProcess = f"class {self.__class__.__name__} - def insert_and_return_ids."
        batch_size = 1000
        lookup_batch_size = 1000
        results = []

        # SQL para o MERGE sem RETURNING
        sql_insert = """
            MERGE INTO IPM_CONVERSAO_NF3E dest
            USING (SELECT :codg_documento      AS CODG_CHAVE_ACESSO_NF3E
                        , :tipo_classe_consumo As TIPO_CLASSE_CONSUMO
                     FROM DUAL) src   
               ON (dest.CODG_CHAVE_ACESSO_NF3E = src.CODG_CHAVE_ACESSO_NF3E)
             When Not Matched
                  Then Insert (CODG_CHAVE_ACESSO_NF3E,
                               TIPO_CLASSE_CONSUMO)
                               Values (src.CODG_CHAVE_ACESSO_NF3E,
                                       src.TIPO_CLASSE_CONSUMO)
        """

        try:
            with connections[self.db_alias].cursor() as cursor:
                # Dividir o DataFrame em lotes para inserção
                for i in range(0, len(df), batch_size):
                    batch_df = df.iloc[i:i + batch_size]

                    # Preparar os parâmetros para inserção
                    params = [{'codg_documento': row['codg_documento_partct_calculo'],
                               'tipo_classe_consumo': row['tipo_classe_consumo']} for _, row in
                              batch_df.iterrows()]
                    cursor.executemany(sql_insert, params)
            connections[self.db_alias].close()

            with connections[self.db_alias].cursor() as cursor:
                # Fazer o lookup em batches
                for i in range(0, len(df), lookup_batch_size):
                    lookup_df = df.iloc[i:i + lookup_batch_size]
                    codg_list = lookup_df['codg_documento_partct_calculo'].tolist()

                    # Construir a consulta dinamicamente usando o operador IN
                    placeholders = ','.join([f':codg_documento{i}' for i in range(len(codg_list))])
                    sql_lookup = f"""
                        SELECT CODG_CHAVE_ACESSO_NF3E, ID_CONVERSAO_NF3E 
                        FROM IPM_CONVERSAO_NF3E 
                        WHERE CODG_CHAVE_ACESSO_NF3E IN ({placeholders})
                    """

                    lookup_params = {f'codg_documento{i}': codg for i, codg in enumerate(codg_list)}

                    # Executar a consulta de lookup para obter IDs
                    cursor.execute(sql_lookup, lookup_params)
                    results.extend(cursor.fetchall())

                loga_mensagem('Lookup finalizado')
            connections[self.db_alias].close()

            # Converter os resultados em DataFrame
            result_df = pd.DataFrame(results, columns=['CODG_CHAVE_ACESSO_NF3E', 'ID_CONVERSAO_NF3E'])
            return result_df

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def update_classe_cons_conversao_nf3e(self, df):
        etapaProcess = f"class {self.__class__.__name__} - def update_classe_cons_conversao_nf3e."

        batch_size = 1000
        lookup_batch_size = 1000
        results = []

        # SQL para o MERGE sem RETURNING
        sql_insert = """
            Merge Into IPM_CONVERSAO_NF3E Dest
            Using (Select :codg_documento      As CODG_CHAVE_ACESSO_NF3E
                        , :tipo_classe_consumo As TIPO_CLASSE_CONSUMO
                     From DUAL) src
               On (Dest.CODG_CHAVE_ACESSO_NF3E = src.CODG_CHAVE_ACESSO_NF3E)
                     When Matched
                          Then Update Set Dest.TIPO_CLASSE_CONSUMO = src.TIPO_CLASSE_CONSUMO
                     When Not Matched
                          Then Insert (Dest.CODG_CHAVE_ACESSO_NF3E,
                                       Dest.TIPO_CLASSE_CONSUMO)
                                       Values (src.CODG_CHAVE_ACESSO_NF3E,
                                               src.TIPO_CLASSE_CONSUMO)
                     """

        # sql_insert = """Select * From IPM_CONVERSAO_NF3E Dest"""

        try:
            with connections[self.db_alias].cursor() as cursor:
                # Dividir o DataFrame em lotes para inserção
                for i in range(0, len(df), batch_size):
                    batch_df = df.iloc[i:i + batch_size]

                    # Preparar os parâmetros para inserção
                    params = [{'codg_documento': row['id_nf3e'],
                               'tipo_classe_consumo': row['tipo_classe_consumo']}
                              for _, row in batch_df.iterrows()]

                    cursor.executemany(sql_insert, params)

                    i_linhas = cursor.rowcount

            connections[self.db_alias].close()
            return i_linhas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def insert_doc_partct(self, df, chunk_size=10000):
        etapaProcess = f"class {self.__class__.__name__} - def insert_doc_partct."
        # loga_mensagem(etapaProcess)

        lote = []
        linhas_inseridas = 0

        etapaProcess = f"Grava dados na tabela IPM_DOCUMENTO_PARTCT_CALC_IPM"
        sql = """INSERT INTO IPM_DOCUMENTO_PARTCT_CALC_IPM (CODG_DOCUMENTO_PARTCT_CALCULO,
                                                            VALR_ADICIONADO_OPERACAO,
                                                            DATA_EMISSAO_DOCUMENTO,
                                                            CODG_TIPO_DOC_PARTCT_CALC,
                                                            NUMR_REFERENCIA_DOCUMENTO,
                                                            ID_CONTRIB_IPM_ENTRADA,
                                                            ID_CONTRIB_IPM_SAIDA,
                                                            ID_PROCESM_INDICE,
                                                            CODG_MOTIVO_EXCLUSAO_CALCULO,
                                                            CODG_MUNICIPIO_ENTRADA,
                                                            CODG_MUNICIPIO_SAIDA,
                                                            INDI_APROP)
                                                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        try:
            with connections[self.db_alias].cursor() as cursor:
                for i, row in df.iterrows():

                    # Preparando os valores para inserção
                    id_ie_entrada = None if pd.isna(row['id_contrib_ipm_entrada']) \
                                         or row['id_contrib_ipm_entrada'] in [0, 'None'] \
                                         else int(row['id_contrib_ipm_entrada'])
                    id_ie_saida = None if pd.isna(row['id_contrib_ipm_saida']) \
                                       or row['id_contrib_ipm_saida'] in [0, 'None'] \
                                       else int(row['id_contrib_ipm_saida'])

                    codg_municipio_entrada = None if pd.isna(row['codg_municipio_entrada']) \
                                                  or row['codg_municipio_entrada'] in [0, 'None'] \
                                                  else int(row['codg_municipio_entrada'])
                    codg_municipio_saida = None if pd.isna(row['codg_municipio_saida']) \
                                                or row['codg_municipio_saida'] in [0, 'None'] \
                                                else int(row['codg_municipio_saida'])

                    codg_motivo_exclusao_calculo = None if pd.isna(row['codg_motivo_exclusao_calculo']) \
                                                        or row['codg_motivo_exclusao_calculo'] in [0, 'None'] \
                                                        else int(row['codg_motivo_exclusao_calculo'])

                    lote.append((row['codg_documento_partct_calculo'],
                                 row['valr_adicionado_operacao'],
                                 row['data_emissao_documento'],
                                 row['codg_tipo_doc_partct_calc'],
                                 row['numr_referencia_documento'],
                                 id_ie_entrada,
                                 id_ie_saida,
                                 row['id_procesm_indice'],
                                 codg_motivo_exclusao_calculo,
                                 codg_municipio_entrada,
                                 codg_municipio_saida,
                                 row['indi_aprop']
                                 ))

                    if len(lote) == chunk_size:
                        cursor.executemany(sql, lote)
                        connections[self.db_alias].commit()
                        linhas_inseridas += len(lote)
                        lote = []

                # Insere os dados restantes que não completaram um lote
                if lote:
                    cursor.executemany(sql, lote)
                    connections[self.db_alias].commit()
                    linhas_inseridas += len(lote)

            return linhas_inseridas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            loga_mensagem_erro(sql)
            raise

    def delete_doc_partct_efd(self, p_inscricoes, p_referencia, p_tipo_documento):
        etapaProcess = f"class {self.__class__.__name__} - def delete_doc_partct_efd"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"exclui dados na tabela IPM_DOCUMENTO_PARTCT_CALC_IPM para a inscrições da EFD na referencia {p_referencia}."
            linhas_excluidas = 0

            with connections[self.db_alias].cursor() as cursor:
                for i_inscricao in p_inscricoes:
                    sql = f"""Delete From IPM_DOCUMENTO_PARTCT_CALC_IPM Doc
                                        Where Doc.CODG_TIPO_DOC_PARTCT_CALC      = %s
                                          And Doc.CODG_DOCUMENTO_PARTCT_CALCULO In (Select CODG_DOCUMENTO_PARTCT_CALCULO
                                                                                      From IPM_DOCUMENTO_PARTCT_CALC_IPM    Doc1
                                                                                            Inner Join IPM_CONTRIBUINTE_IPM Contrib On Doc1.ID_CONTRIB_IPM_ENTRADA = Contrib.ID_CONTRIB_IPM
                                                                                     Where Doc1.CODG_TIPO_DOC_PARTCT_CALC  = %s
                                                                                       And Doc1.NUMR_REFERENCIA_DOCUMENTO  = %s
                                                                                       And Contrib.NUMR_INSCRICAO_CONTRIB = %s)"""
                    cursor.execute(sql, [p_tipo_documento, p_tipo_documento, p_referencia, i_inscricao])
                    linhas_excluidas += cursor.rowcount

            return linhas_excluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def insert_doc_partct_efd(self, df, chunk_size=10000):
        etapaProcess = f"class {self.__class__.__name__} - def insert_doc_partct."
        # loga_mensagem(etapaProcess)

        lote = []
        linhas_inseridas = 0

        sql = """INSERT INTO IPM_DOCUMENTO_PARTCT_CALC_IPM (CODG_DOCUMENTO_PARTCT_CALCULO,
                                                            VALR_ADICIONADO_OPERACAO,
                                                            DATA_EMISSAO_DOCUMENTO,
                                                            CODG_TIPO_DOC_PARTCT_CALC,
                                                            NUMR_REFERENCIA_DOCUMENTO,
                                                            ID_CONTRIB_IPM_ENTRADA,
                                                            ID_CONTRIB_IPM_SAIDA,
                                                            ID_PROCESM_INDICE,
                                                            CODG_MUNICIPIO_ENTRADA,
                                                            CODG_MUNICIPIO_SAIDA,
                                                            INDI_APROP)
                                                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        etapaProcess = f"Grava dados na tabela IPM_DOCUMENTO_PARTCT_CALC_IPM para o EFD"

        try:
            with connections[self.db_alias].cursor() as cursor:

                for i, row in df.iterrows():
                    id_ie_entrada = None if pd.isna(row['id_contrib_ipm_entrada']) \
                                         or row['id_contrib_ipm_entrada'] in [0, 'None'] \
                                         else int(row['id_contrib_ipm_entrada'])

                    id_ie_saida = None if pd.isna(row['id_contrib_ipm_saida']) \
                                       or row['id_contrib_ipm_saida'] in [0, 'None'] \
                                       else int(row['id_contrib_ipm_saida'])

                    codg_municipio_entrada = None if pd.isna(row['codg_municipio_entrada']) \
                                                  or row['codg_municipio_entrada'] in [0, 'None'] \
                                                  else int(row['codg_municipio_entrada'])

                    codg_municipio_saida = None if pd.isna(row['codg_municipio_saida']) \
                                                or row['codg_municipio_saida'] in [0, 'None'] \
                                                else int(row['codg_municipio_saida'])

                    lote.append((row['codg_documento_partct_calculo'],
                                 row['valr_adicionado_operacao'],
                                 row['data_emissao_documento'],
                                 row['codg_tipo_doc_partct_calc'],
                                 row['numr_referencia_documento'],
                                 id_ie_entrada,
                                 id_ie_saida,
                                 row['id_procesm_indice'],
                                 codg_municipio_entrada,
                                 codg_municipio_saida,
                                 row['indi_aprop']
                                 ))

                    if len(lote) == chunk_size:
                        cursor.executemany(sql, lote)
                        connections[self.db_alias].commit()
                        linhas_inseridas += len(lote)
                        lote = []

                # Insere os dados restantes que não completaram um lote
                if lote:
                    cursor.executemany(sql, lote)
                    connections[self.db_alias].commit()
                    linhas_inseridas += len(lote)

            return linhas_inseridas

        except Exception as err:
            etapaProcess += f" - ERRO - {i} - {err}"
            loga_mensagem_erro(etapaProcess)
            loga_mensagem_erro(sql)
            raise

    def delete_doc_partct(self, p_data_inicio, p_data_fim, p_tipo_documento):
        etapaProcess = f"class {self.__class__.__name__} - def delete_doc_partct"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"exclui dados na tabela IPM_DOCUMENTO_PARTCT_CALC_IPM, para o tipo de documento {p_tipo_documento} e período {p_data_inicio} a {p_data_fim}."
            with connections[self.db_alias].cursor() as cursor:
                sql = f"""Delete From IPM_DOCUMENTO_PARTCT_CALC_IPM    Doc
                                    Where Doc.DATA_EMISSAO_DOCUMENTO    BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                                            AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS') 
                                      And Doc.CODG_TIPO_DOC_PARTCT_CALC       = {p_tipo_documento}"""
                cursor.execute(sql)
                linhas_excluidas = cursor.rowcount

            return linhas_excluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_id_doc_partct(self, p_data_inicio, p_data_fim, p_tipo_documento):
        etapaProcess = f"class {self.__class__.__name__} - def select_id_doc_partct - {p_data_inicio} - {p_data_fim} - {p_tipo_documento}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca do identificador do Doc PartCt - {p_data_inicio} - {p_data_fim} - {p_tipo_documento}"
            sql = f"""Select CODG_DOCUMENTO_PARTCT_CALCULO
                        FROM IPM_DOCUMENTO_PARTCT_CALC_IPM          Doc
                       Where CODG_TIPO_DOC_PARTCT_CALC = {p_tipo_documento}
                         And Doc.DATA_EMISSAO_DOCUMENTO BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
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

    def select_doc_partct_id_procesm(self, p_id_procesm):
        etapaProcess = f"class {self.__class__.__name__} - def select_doc_partct_id_procesm - {p_id_procesm}"
        # loga_mensagem(etapaProcess)

        linhas_retornadas = 0

        try:
            etapaProcess = f"Executa query para dados estatísticos de IPM_DOCUMENTO_PARTCT_CALC_IPM - ID_PROCESM: {p_id_procesm}"

            sql = f"""Select COUNT(*)
                        FROM IPM_DOCUMENTO_PARTCT_CALC_IPM          Doc
                       Where ID_PROCESM_INDICE = {p_id_procesm}"""

            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                result = cursor.fetchone()  # Obtém o resultado da contagem
                linhas_retornadas = result[0] if result else 0

            return linhas_retornadas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_doc_partct(self, p_inicio_referencia, p_fim_referencia, p_tipo_documento, p_escopo, p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida, p_chave):
        etapaProcess = f"class {self.__class__.__name__} - def select_doc_partct - {p_tipo_documento} - {p_escopo}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca de informações da Doc PartCt - {p_escopo} - {p_data_inicio} - {p_data_fim} - {p_ie_entrada} - {p_ie_saida} - {p_chave}"
            sql = monta_sql_doc_partct(p_inicio_referencia, p_fim_referencia, p_tipo_documento, p_escopo, p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida, p_chave)

            if sql is None:
                return []
            else:
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

    def select_doc_partct_acerto_cfop(self, p_tipo_documento, p_data_inicio, p_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def select_doc_partct_acerto_cfop - {p_tipo_documento}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca de informações da Doc PartCt - {p_data_inicio} - {p_data_fim} - {p_tipo_documento}"
            sql = f"""Select Doc.CODG_DOCUMENTO_PARTCT_CALCULO
                           , Doc.VALR_ADICIONADO_OPERACAO
                           , Doc.CODG_TIPO_DOC_PARTCT_CALC
                           , Doc.CODG_MOTIVO_EXCLUSAO_CALCULO
                           , Doc.INDI_APROP
                        From IPM_DOCUMENTO_PARTCT_CALC_IPM  Doc
                       Where  1=1
                         And (Doc.CODG_MOTIVO_EXCLUSAO_CALCULO  Not In ({EnumMotivoExclusao.DocumentoCancelado.value}, {EnumMotivoExclusao.ContribsNaoCadOuSimples.value})
                          Or  Doc.CODG_MOTIVO_EXCLUSAO_CALCULO      Is Null)
                         And Doc.CODG_TIPO_DOC_PARTCT_CALC           = {p_tipo_documento}
                         And DATA_EMISSAO_DOCUMENTO            Between TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                                   And TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')"""

            if sql is None:
                return []
            else:
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

    def select_doc_partct_acerto_cad(self, p_tipo_doc, p_data_inicio, p_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def select_doc_partct_acerto_cad."
        loga_mensagem(etapaProcess)

        dfs = []

        try:
            # sql = f"""With NFe As (
            #                        SELECT /*+PARALLEL(20)*/NFe.ID_NFE                                       As id_nfe
            #                             , CAST(CASE WHEN NFe.TIPO_DOCUMENTO_FISCAL = 0
            #                                         THEN NFe.NUMR_INSCRICAO
            #                                         ELSE NFe.NUMR_INSCRICAO_DEST       END  As VARCHAR(14)) As ie_entrada
            #                             , CASE WHEN NFe.TIPO_DOCUMENTO_FISCAL = 0
            #                                    THEN NFe.CODG_MUNICIPIO_GERADOR
            #                                    ELSE NFe.CODG_MUNICIPIO_DEST            END                  As codg_municipio_entrada
            #                             , CAST(CASE WHEN NFe.TIPO_DOCUMENTO_FISCAL = 0
            #                                         THEN NFe.NUMR_INSCRICAO_DEST
            #                                         ELSE NFe.NUMR_INSCRICAO            END  As VARCHAR(14)) As ie_saida
            #                             , CASE WHEN NFe.TIPO_DOCUMENTO_FISCAL = 0
            #                                    THEN NFe.CODG_MUNICIPIO_DEST
            #                                    ELSE NFe.CODG_MUNICIPIO_GERADOR         END                  As codg_municipio_saida
            #                             , NVL(NFe.NUMR_PROTOCOLO_CANCEL, 0)                                 As numr_protocolo_cancel
            #                          FROM NFE_IDENTIFICACAO NFe
            #                         WHERE  1=1
            #                           AND  NFe.DATA_EMISSAO_NFE BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
            #                                                         AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')
            #                           AND  NFe.CODG_MODELO_NFE        = 55
            #                           AND (NFe.NUMR_INSCRICAO                        IN  (Select NUMR_INSCRICAO_CONTRIB From IPM_CONTRIBUINTE_IPM_NOVO)
            #                            OR  NFe.NUMR_INSCRICAO_DEST                   IN  (Select NUMR_INSCRICAO_CONTRIB From IPM_CONTRIBUINTE_IPM_NOVO))
            #                       )
            #              , Doc As (
            #                        SELECT NFe.id_nfe
            #                             , NFe.ie_entrada
            #                             , NFe.codg_municipio_entrada
            #                             , NFe.ie_saida
            #                             , NFe.codg_municipio_saida
            #                             , Doc.CODG_TIPO_DOC_PARTCT_CALC                                     As codg_tipo_doc
            #                             , Doc.NUMR_REFERENCIA_DOCUMENTO                                     As numr_ref_emissao
            #                             , Doc.DATA_EMISSAO_DOCUMENTO                                        As data_emissao_documento
            #                             , Doc.INDI_APROP
            #                             , Doc.VALR_ADICIONADO_OPERACAO
            #                             , Item.CODG_ITEM_DOCUMENTO
            #                             , Item.VALR_ADICIONADO                                              As valr_va
            #                             , Item.CODG_MOTIVO_EXCLUSAO_CALCULO
            #                             , Item.CODG_CFOP                                                    As numr_cfop
            #                             , NFe.numr_protocolo_cancel
            #                          FROM NFe
            #                                INNER JOIN IPM_DOCUMENTO_PARTCT_CALC_IPM Doc  On  NFe.ID_NFE                        = Doc.CODG_DOCUMENTO_PARTCT_CALCULO
            #                                INNER JOIN IPM_ITEM_DOCUMENTO            Item ON  Doc.CODG_DOCUMENTO_PARTCT_CALCULO = Item.CODG_DOCUMENTO_PARTCT_CALCULO
            #                                                                              AND Doc.CODG_TIPO_DOC_PARTCT_CALC     = Item.CODG_TIPO_DOC_PARTCT_DOCUMENTO
            #                         WHERE  1=1
            #                           AND  Doc.CODG_TIPO_DOC_PARTCT_CALC              = {p_tipo_doc}
            #                       )
            #           Select * From Doc"""
            #
            if p_tipo_doc in (1, 10):
                sql = f"""WITH Doc As (
                                       SELECT Doc.CODG_DOCUMENTO_PARTCT_CALCULO
                                            , Doc.CODG_TIPO_DOC_PARTCT_CALC                                     As codg_tipo_doc
                                            , Doc.NUMR_REFERENCIA_DOCUMENTO                                     As numr_ref_emissao
                                            , Doc.DATA_EMISSAO_DOCUMENTO                                        As data_emissao_documento
                                            , NVL(IeEnt.NUMR_INSCRICAO_CONTRIB, 0)                              As IE_ENTRADA_DOC
                                            , NVL(IeSai.NUMR_INSCRICAO_CONTRIB, 0)                              As IE_SAIDA_DOC
                                            , Doc.INDI_APROP
                                            , Doc.VALR_ADICIONADO_OPERACAO
                                            , Item.CODG_ITEM_DOCUMENTO
                                            , Item.VALR_ADICIONADO                                              As valr_va
                                            , Item.CODG_MOTIVO_EXCLUSAO_CALCULO
                                            , Item.CODG_CFOP                                                    As numr_cfop
                                         FROM IPM_DOCUMENTO_PARTCT_CALC_IPM             Doc
                                               INNER JOIN IPM_ITEM_DOCUMENTO            Item  ON  Doc.CODG_DOCUMENTO_PARTCT_CALCULO = Item.CODG_DOCUMENTO_PARTCT_CALCULO
                                                                                              AND Doc.CODG_TIPO_DOC_PARTCT_CALC     = Item.CODG_TIPO_DOC_PARTCT_DOCUMENTO
                                               LEFT  JOIN IPM_CONTRIBUINTE_IPM          IeEnt ON  Doc.ID_CONTRIB_IPM_ENTRADA        = IeEnt.ID_CONTRIB_IPM
                                               LEFT  JOIN IPM_CONTRIBUINTE_IPM          IeSai ON  Doc.ID_CONTRIB_IPM_SAIDA          = IeSai.ID_CONTRIB_IPM
                                        WHERE  1=1
                                          AND  Doc.CODG_TIPO_DOC_PARTCT_CALC              = {p_tipo_doc}
                                          AND  Doc.DATA_EMISSAO_DOCUMENTO BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                                              AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS'))
                             , NFe As (
                                       SELECT /*+PARALLEL(20)*/NFe.ID_NFE                                       As id_nfe
                                            , CAST(CASE WHEN NFe.TIPO_DOCUMENTO_FISCAL = 0 
                                                        THEN NFe.NUMR_INSCRICAO
                                                        ELSE NFe.NUMR_INSCRICAO_DEST       END  As VARCHAR(14)) As ie_entrada
                                            , CASE WHEN NFe.TIPO_DOCUMENTO_FISCAL = 0 
                                                   THEN NFe.CODG_MUNICIPIO_GERADOR
                                                   ELSE NFe.CODG_MUNICIPIO_DEST            END                  As codg_municipio_entrada
                                            , CAST(CASE WHEN NFe.TIPO_DOCUMENTO_FISCAL = 0 
                                                        THEN NFe.NUMR_INSCRICAO_DEST
                                                        ELSE NFe.NUMR_INSCRICAO            END  As VARCHAR(14)) As ie_saida
                                            , CASE WHEN NFe.TIPO_DOCUMENTO_FISCAL = 0 
                                                   THEN NFe.CODG_MUNICIPIO_DEST
                                                   ELSE NFe.CODG_MUNICIPIO_GERADOR         END                  As codg_municipio_saida
                                            , Doc.codg_tipo_doc
                                            , Doc.numr_ref_emissao
                                            , Doc.DATA_EMISSAO_DOCUMENTO
                                            , Doc.INDI_APROP
                                            , Doc.VALR_ADICIONADO_OPERACAO
                                            , Doc.CODG_ITEM_DOCUMENTO
                                            , Doc.valr_va
                                            , Doc.CODG_MOTIVO_EXCLUSAO_CALCULO
                                            , Doc.NUMR_CFOP
                                            , NVL(NFe.NUMR_PROTOCOLO_CANCEL, 0)                                 As numr_protocolo_cancel
                                         FROM NFE_IDENTIFICACAO NFe
                                               INNER JOIN       Doc ON NFe.ID_NFE = Doc.CODG_DOCUMENTO_PARTCT_CALCULO
                                        WHERE  1=1
                                          AND (Doc.IE_ENTRADA_DOC <> NVL((CASE WHEN NFe.TIPO_DOCUMENTO_FISCAL = 0 THEN NFe.NUMR_INSCRICAO ELSE NFe.NUMR_INSCRICAO_DEST END), 0)
                                           OR  Doc.IE_SAIDA_DOC   <> NVL((CASE WHEN NFe.TIPO_DOCUMENTO_FISCAL = 0 THEN NFe.NUMR_INSCRICAO_DEST ELSE NFe.NUMR_INSCRICAO END), 0)))
                          Select * From NFe"""
            elif p_tipo_doc == 2:
                sql = f"""WITH Doc As (
                                       SELECT Doc.CODG_DOCUMENTO_PARTCT_CALCULO
                                            , Doc.CODG_TIPO_DOC_PARTCT_CALC                                     As codg_tipo_doc
                                            , Doc.NUMR_REFERENCIA_DOCUMENTO                                     As numr_ref_emissao
                                            , Doc.DATA_EMISSAO_DOCUMENTO                                        As data_emissao_documento
                                            , NVL(IeEnt.NUMR_INSCRICAO_CONTRIB, 0)                              As IE_ENTRADA_DOC
                                            , NVL(IeSai.NUMR_INSCRICAO_CONTRIB, 0)                              As IE_SAIDA_DOC
                                            , Doc.INDI_APROP
                                            , Doc.VALR_ADICIONADO_OPERACAO
                                            , Item.CODG_ITEM_DOCUMENTO
                                            , Item.VALR_ADICIONADO                                              As valr_va
                                            , Item.CODG_MOTIVO_EXCLUSAO_CALCULO
                                            , Item.CODG_CFOP                                                    As numr_cfop
                                         FROM IPM_DOCUMENTO_PARTCT_CALC_IPM             Doc
                                               INNER JOIN IPM_ITEM_DOCUMENTO            Item  ON  Doc.CODG_DOCUMENTO_PARTCT_CALCULO = Item.CODG_DOCUMENTO_PARTCT_CALCULO
                                                                                              AND Doc.CODG_TIPO_DOC_PARTCT_CALC     = Item.CODG_TIPO_DOC_PARTCT_DOCUMENTO
                                               LEFT  JOIN IPM_CONTRIBUINTE_IPM          IeEnt ON  Doc.ID_CONTRIB_IPM_ENTRADA        = IeEnt.ID_CONTRIB_IPM
                                               LEFT  JOIN IPM_CONTRIBUINTE_IPM          IeSai ON  Doc.ID_CONTRIB_IPM_SAIDA          = IeSai.ID_CONTRIB_IPM
                                        WHERE  1=1
                                          AND  Doc.CODG_TIPO_DOC_PARTCT_CALC              = {p_tipo_doc}
                                          AND  Doc.DATA_EMISSAO_DOCUMENTO BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                                              AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS'))
                             , NFe As (
                                       SELECT /*+PARALLEL(20)*/NFe.ID_NFE_RECEBIDA                          As id_nfe
                                        , CAST(CASE WHEN NFe.TIPO_DOCUMENTO_FISCAL = 0 
                                                    THEN NFe.NUMR_INSCRICAO_EMITENTE
                                                    ELSE NFe.NUMR_INSCRICAO_DEST       END  As VARCHAR(14)) As ie_entrada
                                        , CASE WHEN NFe.TIPO_DOCUMENTO_FISCAL = 0 
                                               THEN NFe.CODG_MUNICIPIO_EMITENTE
                                               ELSE NFe.CODG_MUNICIPIO_DEST            END                  As codg_municipio_entrada
                                        , CAST(CASE WHEN NFe.TIPO_DOCUMENTO_FISCAL = 0 
                                                    THEN NFe.NUMR_INSCRICAO_DEST
                                                    ELSE NFe.NUMR_INSCRICAO_EMITENTE   END  As VARCHAR(14)) As ie_saida
                                        , CASE WHEN NFe.TIPO_DOCUMENTO_FISCAL = 0 
                                               THEN NFe.CODG_MUNICIPIO_DEST
                                               ELSE NFe.CODG_MUNICIPIO_EMITENTE        END                  As codg_municipio_saida
                                            , Doc.codg_tipo_doc
                                            , Doc.numr_ref_emissao
                                            , Doc.DATA_EMISSAO_DOCUMENTO
                                            , Doc.INDI_APROP
                                            , Doc.VALR_ADICIONADO_OPERACAO
                                            , Doc.CODG_ITEM_DOCUMENTO
                                            , Doc.valr_va
                                            , Doc.CODG_MOTIVO_EXCLUSAO_CALCULO
                                            , Doc.NUMR_CFOP
                                            , NVL(NFe.NUMR_PROTOCOLO_CANCEL, 0)                                 As numr_protocolo_cancel
                                         FROM NFE_IDENTIFICACAO_RECEBIDA NFe
                                               INNER JOIN       Doc ON NFe.ID_NFE_RECEBIDA = Doc.CODG_DOCUMENTO_PARTCT_CALCULO
                                        WHERE  1=1
                                          AND (Doc.IE_ENTRADA_DOC <> NVL((CASE WHEN NFe.TIPO_DOCUMENTO_FISCAL = 0 THEN NFe.NUMR_INSCRICAO_EMITENTE ELSE NFe.NUMR_INSCRICAO_DEST END), 0)
                                           OR  Doc.IE_SAIDA_DOC   <> NVL((CASE WHEN NFe.TIPO_DOCUMENTO_FISCAL = 0 THEN NFe.NUMR_INSCRICAO_DEST     ELSE NFe.NUMR_INSCRICAO_EMITENTE END), 0)))
                              Select * From NFe"""

            etapaProcess = f"Executa query de busca de informações das NF-es: {sql}"

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

    def select_doc_partct_codg(self, p_tipo_documento, p_data_inicio, p_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def select_doc_partct_codg - {p_tipo_documento}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca de informações da Doc PartCt - {p_data_inicio} - {p_data_fim} - {p_tipo_documento}"
            sql = f"""Select Doc.CODG_DOCUMENTO_PARTCT_CALCULO
                           , Doc.VALR_ADICIONADO_OPERACAO
                           , Doc.CODG_TIPO_DOC_PARTCT_CALC
                           , Doc.CODG_MOTIVO_EXCLUSAO_CALCULO
                           , Doc.INDI_APROP
                        From IPM_DOCUMENTO_PARTCT_CALC_IPM  Doc
                       Where  1=1
                         And Doc.CODG_TIPO_DOC_PARTCT_CALC           = {p_tipo_documento}
                         And DATA_EMISSAO_DOCUMENTO            Between TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                                   And TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')"""

            if sql is None:
                return []
            else:
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

    def select_item_doc_acerto(self, p_docs, p_tipo_doc):
        etapaProcess = f"class {self.__class__.__name__} - def select_item_doc_acerto - {len(p_docs)} documentos."
        # loga_mensagem(etapaProcess)

        max_lote = 1000
        dfs = []

        try:
            for i in range(0, len(p_docs), max_lote):
                batch_ids = p_docs[i:i + max_lote]
                lista_docs = ', '.join(map(str, batch_ids))

                sql = f"""Select Item.CODG_DOCUMENTO_PARTCT_CALCULO
                               , Item.CODG_ITEM_DOCUMENTO
                               , Item.CODG_TIPO_DOC_PARTCT_DOCUMENTO
                               , Item.VALR_ADICIONADO
                               , Item.CODG_CFOP                       As numr_cfop
                               , Item.CODG_MOTIVO_EXCLUSAO_CALCULO
                            From IPM_ITEM_DOCUMENTO Item
                           Where 1=1
                             And Item.CODG_TIPO_DOC_PARTCT_DOCUMENTO  = {p_tipo_doc} 
                             And Item.CODG_DOCUMENTO_PARTCT_CALCULO  In ({lista_docs})"""

                etapaProcess = f"Executa query de busca de informações dos itens da NF-e: {sql}"

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

    def select_item_doc_id_procesm(self, p_id_procesm):
        etapaProcess = f"class {self.__class__.__name__} - def select_item_doc_id_procesm - {p_id_procesm}"
        # loga_mensagem(etapaProcess)

        linhas_retornadas = 0

        try:
            etapaProcess = f"Executa query para dados estatísticos de IPM_ITEM_DOCUMENTO - ID_PROCESM: {p_id_procesm}"

            sql = f"""Select COUNT(*)
                        FROM IPM_ITEM_DOCUMENTO          Doc
                       Where ID_PROCESM_INDICE = {p_id_procesm}"""

            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                result = cursor.fetchone()  # Obtém o resultado da contagem
                linhas_retornadas = result[0] if result else 0

            return linhas_retornadas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_item_excluido_nf(self, p_cod_exclusao, p_data_inicio, p_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def select_item_excluido_nf - Periodo de {p_data_inicio} a {p_data_fim}"
        # loga_mensagem(etapaProcess)

        dfs = []

        try:
            sql = f"""Select Doc.CODG_DOCUMENTO_PARTCT_CALCULO
                           , Item.CODG_ITEM_DOCUMENTO
                           , Doc.CODG_TIPO_DOC_PARTCT_CALC
                           , Doc.CODG_MOTIVO_EXCLUSAO_CALCULO
                        From IPM_DOCUMENTO_PARTCT_CALC_IPM    Doc
                              Inner Join IPM_ITEM_DOCUMENTO   Item  On Doc.CODG_DOCUMENTO_PARTCT_CALCULO = Item.CODG_DOCUMENTO_PARTCT_CALCULO
                       Where Item.CODG_MOTIVO_EXCLUSAO_CALCULO  =  {p_cod_exclusao}
                         And Doc.CODG_TIPO_DOC_PARTCT_CALC     In ({EnumTipoDocumento.NFe.value}
                                                                 , {EnumTipoDocumento.NFeRecebida.value}
                                                                 , {EnumTipoDocumento.NFCe.value}
                                                                 , {EnumTipoDocumento.NFA.value})
                         And Doc.DATA_EMISSAO_DOCUMENTO BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                            AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')"""

            etapaProcess = f"Executa query de busca de informações de ativo imobilizadoos itens da NF-e: {sql}"

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

    def select_ativo_imobilizado(self, p_data_ref_inicio, p_data_ref_fim, p_data_inicio, p_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def select_ativo_imobilizado - {p_data_inicio} a {p_data_fim}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca de informações de documentos para o ativo Imobilizado - {p_data_inicio} a {p_data_fim}"
            sql = f"""With EFD     As (Select EFD.CODG_CHAVE_ACESSO_NFE
                                            , Doc.CODG_DOCUMENTO_PARTCT_CALCULO As CODG_DOCUMENTO_PARTCT_CALCULO_EFD
                                            , 5                                 As CODG_TIPO_DOC_PARTCT_CALC_EFD
                                            , Doc.ID_CONTRIB_IPM_ENTRADA        As ID_CONTRIB_IPM_ENTRADA_EFD
                                         From IPM_DOCUMENTO_PARTCT_CALC_IPM    Doc
                                               Inner Join EFD_NOTA_FISCAL      EFD     On  Doc.CODG_DOCUMENTO_PARTCT_CALCULO = EFD.ID_NOTA_FISCAL
                                        Where Doc.CODG_MOTIVO_EXCLUSAO_CALCULO   Is Null
                                          And Doc.CODG_TIPO_DOC_PARTCT_CALC       = 5
                                          And Doc.DATA_EMISSAO_DOCUMENTO Between TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                                             And TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')
--                                          And EFD.CODG_CHAVE_ACESSO_NFE IN ('52230129044977000192550010000002931000293117')
)
                         , ItemEFD As (Select EFD.CODG_DOCUMENTO_PARTCT_CALCULO_EFD
                                            , EFD.CODG_TIPO_DOC_PARTCT_CALC_EFD
                                            , Item.CODG_ITEM_DOCUMENTO              As CODG_ITEM_DOCUMENTO_EFD
                                            , IeEnt.NUMR_INSCRICAO_CONTRIB          As NUMR_INSCRICAO_ENTRADA_EFD
                                            , Item.VALR_ADICIONADO                  As VALR_ADICIONADO_EFD
                                        From EFD 
                                              Inner Join IPM_ITEM_DOCUMENTO            Item    On  EFD.CODG_DOCUMENTO_PARTCT_CALCULO_EFD = Item.CODG_DOCUMENTO_PARTCT_CALCULO
                                                                                               AND EFD.CODG_TIPO_DOC_PARTCT_CALC_EFD     = Item.CODG_TIPO_DOC_PARTCT_DOCUMENTO
                                              Inner Join IPM_CONTRIBUINTE_IPM          IeEnt   ON  EFD.ID_CONTRIB_IPM_ENTRADA_EFD        = IeEnt.ID_CONTRIB_IPM
                                       Where Item.CODG_MOTIVO_EXCLUSAO_CALCULO   Is Null)
                         , NFe     As (Select NFe.ID_NFE                         AS CODG_DOCUMENTO_PARTCT_CALCULO_NFE
                                            , CASE WHEN NFe.CODG_MODELO_NFE = 55
                                                   THEN 10
                                                   ELSE 1  END                   AS CODG_TIPO_DOC_PARTCT_CALC_NFE
                                            , EFD.CODG_DOCUMENTO_PARTCT_CALCULO_EFD
                                            , EFD.CODG_TIPO_DOC_PARTCT_CALC_EFD
                                         From NFE_IDENTIFICACAO NFe
                                               Inner Join       EFD On  NFe.CODG_CHAVE_ACESSO_NFE = EFD.CODG_CHAVE_ACESSO_NFE
                                          And NFe.DATA_EMISSAO_NFE Between TO_DATE('{p_data_ref_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                                       And TO_DATE('{p_data_ref_fim}', 'YYYY-MM-DD HH24:MI:SS')
                                       Union
                                       Select NFe.ID_NFE_RECEBIDA               AS CODG_DOCUMENTO_PARTCT_CALCULO_NFE
                                            , 2                                 AS CODG_TIPO_DOC_PARTCT_CALC_NFE
                                            , EFD.ID_CONTRIB_IPM_ENTRADA_EFD
                                            , EFD.CODG_DOCUMENTO_PARTCT_CALCULO_EFD
                                         From NFE_IDENTIFICACAO_RECEBIDA NFe
                                               Inner Join                EFD On  NFe.CODG_CHAVE_ACESSO_NFE = EFD.CODG_CHAVE_ACESSO_NFE
                                          And NFe.DATA_EMISSAO_NFE Between TO_DATE('{p_data_ref_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                                       And TO_DATE('{p_data_ref_fim}', 'YYYY-MM-DD HH24:MI:SS'))
                         , ItemNFe As (Select NFe.CODG_DOCUMENTO_PARTCT_CALCULO_NFE
                                            , NFe.CODG_TIPO_DOC_PARTCT_CALC_NFE
                                            , Item.CODG_ITEM_DOCUMENTO              As CODG_ITEM_DOCUMENTO_NFE
                                            , IeEnt.NUMR_INSCRICAO_CONTRIB          As NUMR_INSCRICAO_ENTRADA_NFE
                                            , Item.VALR_ADICIONADO                  As VALR_ADICIONADO_NFE
                                         From NFe 
                                               Inner Join IPM_DOCUMENTO_PARTCT_CALC_IPM Doc     ON  NFe.CODG_DOCUMENTO_PARTCT_CALCULO_NFE = Doc.CODG_DOCUMENTO_PARTCT_CALCULO
                                                                                                AND NFe.CODG_TIPO_DOC_PARTCT_CALC_NFE     = Doc.CODG_TIPO_DOC_PARTCT_CALC
                                               Inner Join IPM_ITEM_DOCUMENTO            Item    ON  Doc.CODG_DOCUMENTO_PARTCT_CALCULO     = Item.CODG_DOCUMENTO_PARTCT_CALCULO
                                                                                                AND Doc.CODG_TIPO_DOC_PARTCT_CALC         = Item.CODG_TIPO_DOC_PARTCT_DOCUMENTO
                                               Inner Join IPM_CONTRIBUINTE_IPM          IeEnt   ON  Doc.ID_CONTRIB_IPM_ENTRADA            = IeEnt.ID_CONTRIB_IPM
                                        Where Item.CODG_MOTIVO_EXCLUSAO_CALCULO   Is Null
                                          And IeEnt.CODG_UF                       = 'GO')
                      Select 'NFE' As TIPO
                           , NFe.CODG_DOCUMENTO_PARTCT_CALCULO_NFE
                           , NFe.CODG_TIPO_DOC_PARTCT_CALC_NFE
                           , NFe.CODG_DOCUMENTO_PARTCT_CALCULO_EFD
                           , NFe.CODG_TIPO_DOC_PARTCT_CALC_EFD
                           , ItemNFe.NUMR_INSCRICAO_ENTRADA_NFE     As NUMR_INSCRICAO_ENTRADA
                           , ItemNFe.CODG_ITEM_DOCUMENTO_NFE        As CODG_ITEM_DOCUMENTO
                           , ItemNFe.VALR_ADICIONADO_NFE            As VALR_ADICIONADO
                        From NFe
                              Inner Join ItemNFe On  NFe.CODG_DOCUMENTO_PARTCT_CALCULO_NFE = ItemNFe.CODG_DOCUMENTO_PARTCT_CALCULO_NFE
                                                 And NFe.CODG_TIPO_DOC_PARTCT_CALC_NFE     = ItemNFe.CODG_TIPO_DOC_PARTCT_CALC_NFE 
                      Union                  
                      Select 'EFD' As TIPO
                           , NFe.CODG_DOCUMENTO_PARTCT_CALCULO_NFE
                           , NFe.CODG_TIPO_DOC_PARTCT_CALC_NFE
                           , NFe.CODG_DOCUMENTO_PARTCT_CALCULO_EFD
                           , NFe.CODG_TIPO_DOC_PARTCT_CALC_EFD
                           , ItemEFD.NUMR_INSCRICAO_ENTRADA_EFD     As NUMR_INSCRICAO_ENTRADA
                           , ItemEFD.CODG_ITEM_DOCUMENTO_EFD        As CODG_ITEM_DOCUMENTO
                           , ItemEFD.VALR_ADICIONADO_EFD            As VALR_ADICIONADO
                        From NFe
                              Inner Join ItemEFD On  NFe.CODG_DOCUMENTO_PARTCT_CALCULO_EFD = ItemEFD.CODG_DOCUMENTO_PARTCT_CALCULO_EFD
                                                 And NFe.CODG_TIPO_DOC_PARTCT_CALC_EFD     = ItemEFD.CODG_TIPO_DOC_PARTCT_CALC_EFD
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
            loga_mensagem_erro(sql)
            raise

    def update_doc_partct_limpa_excl_item_n_partct(self, p_tipo_documento, p_data_inicio, p_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def update_doc_partct_limpa_excl_item_n_partct - {p_tipo_documento} - {p_data_inicio} - {p_data_fim}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa limpeza do indicador de exclusão (sem item participante) de documentos fiscais - {p_tipo_documento} - {p_data_inicio} - {p_data_fim}"
            sql = f"""UPDATE IPM_DOCUMENTO_PARTCT_CALC_IPM
                         SET CODG_MOTIVO_EXCLUSAO_CALCULO = Null
                       Where  1=1
                         And CODG_MOTIVO_EXCLUSAO_CALCULO       = {EnumMotivoExclusao.DocumentoNaoContemItemParticipante.value}
                         And CODG_TIPO_DOC_PARTCT_CALC          = {p_tipo_documento}
                         And DATA_EMISSAO_DOCUMENTO       Between TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                              And TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')"""

            connections[self.db_alias].close()

            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)

            return cursor.rowcount

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def update_doc_partct_limpa_excl(self, p_docs, p_tipo_documento):
        etapaProcess = f"class {self.__class__.__name__} - def update_doc_partct_limpa_excl - {p_tipo_documento}"
        # loga_mensagem(etapaProcess)

        linhas_alteradas = 0
        max_lote = 1000
        dfs = []

        try:
            etapaProcess = f"Executa limpeza do indicador de exclusão dos documentos - {p_tipo_documento}"

            for i in range(0, len(p_docs), max_lote):
                batch_ids = p_docs[i:i + max_lote]
                lista_docs = ', '.join(map(str, batch_ids))

                sql = f"""UPDATE IPM_DOCUMENTO_PARTCT_CALC_IPM
                             SET CODG_MOTIVO_EXCLUSAO_CALCULO = Null
                           Where  1=1
                            And CODG_TIPO_DOC_PARTCT_CALC       = {p_tipo_documento} 
                            AND CODG_MOTIVO_EXCLUSAO_CALCULO   IS NOT NULL
                            AND CODG_DOCUMENTO_PARTCT_CALCULO  IN ({lista_docs})"""

                connections[self.db_alias].close()
                with connections[self.db_alias].cursor() as cursor:
                    cursor.execute(sql)
                    linhas_alteradas += cursor.rowcount

            return linhas_alteradas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def update_item_partct_limpa_excl(self, p_itens, p_tipo_documento):
        etapaProcess = f"class {self.__class__.__name__} - def update_item_partct_limpa_excl - {p_tipo_documento}"
        # loga_mensagem(etapaProcess)

        linhas_alteradas = 0
        max_lote = 1000
        dfs = []

        try:
            etapaProcess = f"Executa limpeza do indicador de exclusão dos itens dos documentos - {p_tipo_documento}"

            for i in range(0, len(p_itens), max_lote):
                batch_chave = p_itens[i:i + max_lote]  # Lista de tuplas (codg_documento, codg_item)
                valores_tupla = ', '.join(f"('{doc}', {item})" for doc, item in batch_chave
                )

                sql = f"""UPDATE IPM_ITEM_DOCUMENTO
                            SET CODG_MOTIVO_EXCLUSAO_CALCULO = NULL
                          WHERE CODG_TIPO_DOC_PARTCT_DOCUMENTO  = {p_tipo_documento} 
                            AND CODG_MOTIVO_EXCLUSAO_CALCULO   IS NOT NULL
                            AND (CODG_DOCUMENTO_PARTCT_CALCULO, CODG_ITEM_DOCUMENTO) IN ({valores_tupla})
                       """

                connections[self.db_alias].close()
                with connections[self.db_alias].cursor() as cursor:
                    cursor.execute(sql)
                    linhas_alteradas += cursor.rowcount

            return linhas_alteradas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def update_doc_partct_valr_excl(self, df, chunk_size=10000):
        etapaProcess = f"class {self.__class__.__name__} - def update_doc_partct_valr_excl"
        loga_mensagem(etapaProcess)

        try:
            with connections[self.db_alias].cursor() as cursor:
                lote = []
                total_atualizado = 0

                sql = """UPDATE IPM_DOCUMENTO_PARTCT_CALC_IPM
                            SET VALR_ADICIONADO_OPERACAO     = %s,
                                CODG_MOTIVO_EXCLUSAO_CALCULO = %s
                          WHERE CODG_DOCUMENTO_PARTCT_CALCULO = %s
                            AND CODG_TIPO_DOC_PARTCT_CALC     = %s"""

                for i, row in df.iterrows():
                    i_motivo_exclusao = (None if pd.isna(row['codg_motivo_exclusao_calculo']) or
                                                 row['codg_motivo_exclusao_calculo'] in [0, 'None']
                                         else int(row['codg_motivo_exclusao_calculo']))

                    lote.append((row['valr_adicionado_operacao'],
                                 i_motivo_exclusao,
                                 row['codg_documento_partct_calculo'],
                                 row['codg_tipo_doc_partct_calc'],
                                 ))

                    if len(lote) == chunk_size:
                        cursor.executemany(sql, lote)
                        connections[self.db_alias].commit()
                        total_atualizado += len(lote)
                        lote = []

                # Insere os dados restantes que não completaram um lote
                if lote:
                    cursor.executemany(sql, lote)
                    connections[self.db_alias].commit()
                    total_atualizado += len(lote)

            return total_atualizado

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def update_item_excl_ativo(self, df, chunk_size=10000):
        etapaProcess = f"class {self.__class__.__name__} - def update_item_excl_ativo"
        loga_mensagem(etapaProcess)

        try:
            df.name = 'df_itens'
            baixa_csv(df)

            with connections[self.db_alias].cursor() as cursor:
                lote = []
                total_atualizado = 0

                sql = """
                    UPDATE IPM_ITEM_DOCUMENTO
                       SET CODG_MOTIVO_EXCLUSAO_CALCULO = :motivo
                     WHERE CODG_DOCUMENTO_PARTCT_CALCULO  = :doc
                       AND CODG_ITEM_DOCUMENTO            = :item
                       AND CODG_TIPO_DOC_PARTCT_DOCUMENTO = :tipo
                """

                for _, row in df.iterrows():
                    lote.append({
                        "motivo": EnumMotivoExclusao.ItemAtivoImobilizado.value,
                        "doc": row["codg_documento_partct_calculo"],
                        "item": row["codg_item_documento"],
                        "tipo": row["codg_tipo_doc_partct_calc"],
                    })

                    if len(lote) == chunk_size:
                        cursor.executemany(sql, lote)
                        connections[self.db_alias].commit()
                        total_atualizado += len(lote)
                        lote = []

                # Insere os dados restantes
                if lote:
                    cursor.executemany(sql, lote)
                    connections[self.db_alias].commit()
                    total_atualizado += len(lote)

            return total_atualizado

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def update_doc_partct_cad(self, df, chunk_size=10000):
        etapaProcess = f"class {self.__class__.__name__} - def update_doc_partct_cad"
        # loga_mensagem(etapaProcess)

        try:
            with connections[self.db_alias].cursor() as cursor:
                lote = []
                total_atualizado = 0

                sql = """UPDATE IPM_DOCUMENTO_PARTCT_CALC_IPM
                            SET VALR_ADICIONADO_OPERACAO     = %s
                              , CODG_MOTIVO_EXCLUSAO_CALCULO = %s
                              , INDI_APROP                   = %s
                              , ID_PROCESM_INDICE            = %s 
                              , ID_CONTRIB_IPM_ENTRADA       = %s
                              , ID_CONTRIB_IPM_SAIDA         = %s
                          WHERE CODG_DOCUMENTO_PARTCT_CALCULO = %s
                            AND CODG_TIPO_DOC_PARTCT_CALC     = %s"""

                for i, row in df.iterrows():
                    i_id_contrib_entrada = (None if pd.isna(row['id_contrib_ipm_entrada'])
                                                 or row['id_contrib_ipm_entrada'] in [0, 'None']
                                                 else int(row['id_contrib_ipm_entrada']))

                    i_id_contrib_saida = (None if pd.isna(row['id_contrib_ipm_saida'])
                                               or row['id_contrib_ipm_saida'] in [0, 'None']
                                               else int(row['id_contrib_ipm_saida']))

                    i_motivo_exclusao = (None if pd.isna(row['codg_motivo_exclusao_calculo']) or
                                                 row['codg_motivo_exclusao_calculo'] in [0, 'None']
                                         else int(row['codg_motivo_exclusao_calculo']))

                    lote.append((row['valr_adicionado_operacao'],
                                 i_motivo_exclusao,
                                 row['indi_aprop'],
                                 row['id_procesm_indice'],
                                 i_id_contrib_entrada,
                                 i_id_contrib_saida,
                                 row['codg_documento_partct_calculo'],
                                 row['codg_tipo_doc_partct_calc'],
                                 ))

                    if len(lote) == chunk_size:
                        cursor.executemany(sql, lote)
                        connections[self.db_alias].commit()
                        total_atualizado += len(lote)
                        lote = []

                # Insere os dados restantes que não completaram um lote
                if lote:
                    cursor.executemany(sql, lote)
                    connections[self.db_alias].commit()
                    total_atualizado += len(lote)

            return total_atualizado

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def update_item_doc_valr_excl(self, df, chunk_size=10000):
        etapaProcess = f"class {self.__class__.__name__} - def update_item_doc_valr_excl"
        # loga_mensagem(etapaProcess)

        try:
            with connections[self.db_alias].cursor() as cursor:
                lote = []
                total_atualizado = 0

                sql = """UPDATE IPM_ITEM_DOCUMENTO
                            SET CODG_MOTIVO_EXCLUSAO_CALCULO = %s
                          WHERE CODG_DOCUMENTO_PARTCT_CALCULO = %s
                            AND CODG_ITEM_DOCUMENTO           = %s
                            AND CODG_DOCUMENTO_PARTCT_CALCULO = %s"""

                for i, row in df.iterrows():
                    # i_motivo_exclusao = (None if pd.isna(row['codg_motivo_exclusao_calculo'])
                    #                           or row['codg_motivo_exclusao_calculo'] in [0, 'None']
                    #                           else int(row['codg_motivo_exclusao_calculo']))

                    lote.append((int(row['codg_motivo_exclusao_calculo']),
                                 int(row['codg_documento_partct_calculo']),
                                 int(row['codg_item_documento']),
                                 int(row['codg_tipo_doc_partct_documento']),
                                 ))

                    if len(lote) == chunk_size:
                        cursor.executemany(sql, lote)
                        connections[self.db_alias].commit()
                        total_atualizado += len(lote)
                        lote = []

                # Insere os dados restantes que não completaram um lote
                if lote:
                    cursor.executemany(sql, lote)
                    connections[self.db_alias].commit()
                    total_atualizado += len(lote)

            return total_atualizado

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    # def select_operacoes_opostas(self, p_inicio_referencia, p_fim_referencia, p_list_inscricao1, p_list_uf_inscricao1, p_list_inscricao2, p_list_uf_inscricao2):
    def select_operacoes_opostas(self, p_inicio_referencia, p_fim_referencia, p_inscrs):
        etapaProcess = f"class {self.__class__.__name__} - def select_operacoes_duplicadas - {p_inicio_referencia} - {p_fim_referencia}"
        # loga_mensagem(etapaProcess)

        etapaProcess = f"Executa query de busca de informações de transações entre contribuintes para o periodo de {p_inicio_referencia} a {p_fim_referencia}"

        def build_union_block(batch):
            sql_parts = []
            params = {}

            for idx, (ie_sai, uf_sai, ie_ent, uf_ent) in enumerate(batch):
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

            return " UNION ALL ".join(sql_parts), params

        try:
            resultados = []
            batch_size = 1000
            p_inscrs_list = [(ie_sai, uf_sai, ie_ent, uf_ent)
                             for (ie_sai, uf_sai), lista_ent in p_inscrs.items()
                             for (ie_ent, uf_ent) in lista_ent
                             ]

            for i in range(0, len(p_inscrs_list), batch_size):
                batch = p_inscrs_list[i:i + batch_size]
                union_sql, params = build_union_block(batch)

                sql = f"""With Params As ({union_sql})
                      , Doc As (Select Doc.CODG_DOCUMENTO_PARTCT_CALCULO
                                     , Doc.CODG_TIPO_DOC_PARTCT_CALC
                                     , IeEnt.NUMR_INSCRICAO_CONTRIB       As NUMR_INSCRICAO_ENTRADA
                                     , IeSai.NUMR_INSCRICAO_CONTRIB       As NUMR_INSCRICAO_SAIDA
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
        --                           And (((IeSai.NUMR_INSCRICAO_CONTRIB = :ie_sai And IeSai.CODG_UF = :uf_sai)
        --                           And   (IeEnt.NUMR_INSCRICAO_CONTRIB = :ie_ent And IeEnt.CODG_UF = :uf_ent))
        --                            Or  ((IeSai.NUMR_INSCRICAO_CONTRIB = :ie_ent And IeSai.CODG_UF = :uf_ent)
        --                           And   (IeEnt.NUMR_INSCRICAO_CONTRIB = :ie_sai And IeEnt.CODG_UF = :uf_sai))))                     
        --                     And (((IeSai.NUMR_INSCRICAO_CONTRIB = :ie_sai1 And IeSai.CODG_UF = :uf_sai1)
        --                     And   (IeEnt.NUMR_INSCRICAO_CONTRIB = :ie_ent1 And IeEnt.CODG_UF = :uf_ent1))
        --                      Or  ((IeSai.NUMR_INSCRICAO_CONTRIB = :ie_ent2 And IeSai.CODG_UF = :uf_ent2)
        --                     And   (IeEnt.NUMR_INSCRICAO_CONTRIB = :ie_sai2 And IeEnt.CODG_UF = :uf_sai2)))                     
                      , NFe As (Select Doc.CODG_DOCUMENTO_PARTCT_CALCULO
                                     , Doc.CODG_TIPO_DOC_PARTCT_CALC
                                     , Doc.NUMR_INSCRICAO_ENTRADA
                                     , Doc. NUMR_INSCRICAO_SAIDA
                                     , SUBSTR(ItemNFe.CODG_PRODUTO_NCM, 1, 4) As CODG_PRODUTO_NCM
                                     , ItemNFe.VALR_TOTAL_BRUTO
                                     , ItemNFe.ID_ITEM_NOTA_FISCAL As CODG_ITEM_DOCUMENTO_NFE
                                     , ItemNFe.ID_ITEM_NOTA_FISCAL
                                     , ItemNFe.CODG_CFOP
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
                                          , Doc. NUMR_INSCRICAO_SAIDA
                                          , SUBSTR(ItemNFeR.CODG_PRODUTO_NCM, 1, 4) As CODG_PRODUTO_NCM
                                          , ItemNFeR.VALR_TOTAL_BRUTO
                                          , ItemNFeR.ID_ITEM_NOTA_FISCAL As CODG_ITEM_DOCUMENTO_NFE
                                          , ItemNFeR.ID_ITEM_NOTA_FISCAL
                                          , ItemNFeR.CODG_CFOP
                                          , ItemIPM.VALR_ADICIONADO
                                       From Doc
                                            Inner Join IPM_ITEM_DOCUMENTO            ItemIPM  ON  Doc.CODG_DOCUMENTO_PARTCT_CALCULO  = ItemIPM.CODG_DOCUMENTO_PARTCT_CALCULO
                                            Inner Join NFE_ITEM_NOTA_FISCAL          ItemNFeR ON  Doc.CODG_DOCUMENTO_PARTCT_CALCULO  = ItemNFeR.ID_NFE_RECEBIDA
                                                                                              And ItemIPM.CODG_ITEM_DOCUMENTO        = ItemNFeR.ID_ITEM_NOTA_FISCAL
                                  Where Doc.CODG_TIPO_DOC_PARTCT_CALC = {EnumTipoDocumento.NFeRecebida.value}
                                    And ItemIPM.CODG_CFOP In (5101, 1101, 5102, 1102, 5116, 1116, 5201, 1201, 5202, 1202, 5410, 1410, 6101, 2101, 6102, 2102, 6116, 2116, 6201, 2201, 6202, 2202, 6410, 2410)
                                    And ItemIPM.CODG_MOTIVO_EXCLUSAO_CALCULO Is Null)
    
                      Select NUMR_INSCRICAO_ENTRADA
                           , NUMR_INSCRICAO_SAIDA
                           , CODG_CFOP
                           , CODG_PRODUTO_NCM
                           , SUM(VALR_ADICIONADO)               As valr_adicionado_item
                           , SUM(VALR_TOTAL_BRUTO)              As valr_total_item
                        From (Select * From NFe
                              Union
                              Select * From NFeR)
                       Group By NUMR_INSCRICAO_ENTRADA
                           , NUMR_INSCRICAO_SAIDA
                           , CODG_CFOP
                           , CODG_PRODUTO_NCM
                      """

            connections[self.db_alias].close()

            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql, params)
                colunas = [col[0].lower() for col in cursor.description]
                dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]

            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            loga_mensagem_erro(sql)
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

    def delete_operacoes_duplicadas(self, p_inicio_referencia, p_fim_referencia):
        etapaProcess = f"class {self.__class__.__name__} - def delete_operacoes_duplicadas - {p_inicio_referencia} - {p_fim_referencia}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa limpeza do indicador de exclusão de operações entre produtores rurais - {p_inicio_referencia} - {p_fim_referencia}"
            sql = f"""UPDATE IPM_ITEM_DOCUMENTO
                         SET CODG_MOTIVO_EXCLUSAO_CALCULO    = Null
                       WHERE CODG_MOTIVO_EXCLUSAO_CALCULO    = {EnumMotivoExclusao.OperacaoDuplicada.value}
                         AND CODG_TIPO_DOC_PARTCT_DOCUMENTO IN ({EnumTipoDocumento.NFe.value}, {EnumTipoDocumento.NFeRecebida.value}, {EnumTipoDocumento.NFA.value})
                         AND CODG_DOCUMENTO_PARTCT_CALCULO  IN (SELECT CODG_DOCUMENTO_PARTCT_CALCULO
                                                                  FROM IPM_DOCUMENTO_PARTCT_CALC_IPM 
                                                                 WHERE NUMR_REFERENCIA_DOCUMENTO BETWEEN {p_inicio_referencia} AND {p_fim_referencia})"""

            connections[self.db_alias].close()

            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)

            return None

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            loga_mensagem_erro(sql)
            raise

    def update_docs_operacoes_duplicadas(self, docs, p_codg_tipo_documento):
        etapaProcess = f"class {self.__class__.__name__} - def update_docs_operacoes_duplicadas - {p_codg_tipo_documento} - {len(docs)} para atualização."
        # loga_mensagem(etapaProcess)

        try:
            documentos_ids = docs['codg_documento_partct_calculo'].tolist()
            # docs['codg_motivo_exclusao_calculo'] = docs['codg_motivo_exclusao_calculo'].apply(lambda x: None if pd.isna(x) else x)

            linhas_alteradas = 0
            tamanho_lote = 20000
            dfs = []

            lotes = [documentos_ids[i:i + tamanho_lote] for i in range(0, len(documentos_ids), tamanho_lote)]
            valores_dict = dict(zip(docs['codg_documento_partct_calculo'], docs['valr_adicionado_operacao']))
            motivos_dict = dict(zip(docs['codg_documento_partct_calculo'], docs['codg_motivo_exclusao_calculo']))


            for i, lote in enumerate(lotes):
                loga_mensagem(f"Processando lote {i + 1}/{len(lotes)} com {len(lote)} documentos")

                objetos = DocPartctCalculo.objects.using(self.db_alias) \
                                          .filter(codg_documento_partct_calculo__in=lote) \
                                          .filter(codg_tipo_doc_partct_calc=p_codg_tipo_documento) \
                                          .only('codg_documento_partct_calculo', 'codg_motivo_exclusao_calculo', 'valr_adicionado_operacao')

                objetos_atualizados = []

                for obj in objetos:
                    obj.valr_adicionado_operacao = valores_dict.get(obj.codg_documento_partct_calculo, 0)
                    obj.codg_motivo_exclusao_calculo = motivos_dict.get(obj.codg_documento_partct_calculo, 0)
                    objetos_atualizados.append(obj)

                if objetos_atualizados:
                    DocPartctCalculo.objects.using(self.db_alias).bulk_update(objetos_atualizados,
                                                                              ['codg_motivo_exclusao_calculo', 'valr_adicionado_operacao'],
                                                                              batch_size=500
                                                                              )
                    linhas_alteradas += len(objetos_atualizados)

                # Bulk update do lote
                DocPartctCalculo.objects.using(self.db_alias).bulk_update(objetos, ['codg_motivo_exclusao_calculo', 'valr_adicionado_operacao'], batch_size=1000)

                linhas_alteradas += len(objetos)

            loga_mensagem(f"Total de documentos atualizados: {linhas_alteradas}")
            return linhas_alteradas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def update_itens_operacoes_duplicadas(self, itens, p_codg_tipo_documento):
        etapaProcess = f"class {self.__class__.__name__} - def update_itens_operacoes_duplicadas - {p_codg_tipo_documento} - {len(itens)} para atualização."
        # loga_mensagem(etapaProcess)

        try:
            linhas_alteradas = 0
            tamanho_lote = 20000
            lotes = [itens[i:i + tamanho_lote] for i in range(0, len(itens), tamanho_lote)]
            dfs = []

            for lote in lotes:
                dados = ItemDoc.objects.using(self.db_alias)\
                               .filter(codg_item_documento__in=lote)\
                               .filter(codg_tipo_doc_partct_documento=p_codg_tipo_documento) \
                               .update(codg_motivo_exclusao_calculo=EnumMotivoExclusao.OperacaoDuplicada.value)
                linhas_alteradas += dados

            return linhas_alteradas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_municipio_ibge(self, munics):
        etapaProcess = f"class {self.__class__.__name__} - def select_municipio_ibge."
        # loga_mensagem(etapaProcess)

        try:
            tamanho_lote = 20000
            lotes = [munics[i:i + tamanho_lote] for i in range(0, len(munics), tamanho_lote)]
            dfs = []

            # Itere sobre cada lote e execute a consulta ao banco de dados
            for lote in lotes:
                dados = Municipio.objects\
                                 .using(self.db_alias)\
                                 .filter(codg_ibge__in=lote)\
                                 .values('codg_municipio',
                                         'codg_ibge',
                                         'codg_uf',
                                         )
                df = pd.DataFrame(dados)
                dfs.append(df)

            # Concatene todos os DataFrames em um único DataFrame
            if len(dfs) > 0:
                df_resultado = pd.concat(dfs, ignore_index=True)
                return df_resultado
            else:
                return []

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_municipio_gen(self, munics):
        etapaProcess = f"class {self.__class__.__name__} - def select_municipio_gen."
        # loga_mensagem(etapaProcess)

        try:
            tamanho_lote = 20000
            lotes = [munics[i:i + tamanho_lote] for i in range(0, len(munics), tamanho_lote)]
            dfs = []

            # Itere sobre cada lote e execute a consulta ao banco de dados
            for lote in lotes:
                dados = Municipio.objects\
                                 .using(self.db_alias)\
                                 .filter(codg_municipio__in=lote)\
                                 .values('codg_municipio',
                                         )
                df = pd.DataFrame(dados)
                dfs.append(df)

            # Concatene todos os DataFrames em um único DataFrame
            if len(dfs) > 0:
                df_resultado = pd.concat(dfs, ignore_index=True)
                return df_resultado
            else:
                return []

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def insert_processamento(self, procesm=None) -> Processamento:
        etapaProcess = f"class {self.__class__.__name__} - def insert_processamento."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Insere novo processamento"
            # procesm.objects.save(using=self.db_alias, force_insert=True, force_update=False)
            # return procesm.objects.using(self.db_alias).last()

            data_inicio_procesm_indice = procesm.data_inicio_procesm_indice
            stat_procesm_indice = procesm.stat_procesm_indice
            desc_observacao_procesm_indice = procesm.desc_observacao_procesm_indice
            codg_tipo_procesm_indice = procesm.codg_tipo_procesm_indice
            data_inicio_periodo_indice = procesm.data_inicio_periodo_indice
            data_fim_periodo_indice = procesm.data_fim_periodo_indice

            novo_processamento = Processamento.objects.using(self.db_alias).create(data_inicio_procesm_indice=data_inicio_procesm_indice,
                                                                                   stat_procesm_indice=stat_procesm_indice,
                                                                                   desc_observacao_procesm_indice=desc_observacao_procesm_indice,
                                                                                   codg_tipo_procesm_indice=codg_tipo_procesm_indice,
                                                                                   data_inicio_periodo_indice=data_inicio_periodo_indice,
                                                                                   data_fim_periodo_indice=data_fim_periodo_indice,
                                                                                   )
            return novo_processamento

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def update_processamento(self, procesm=Processamento) -> int:
        etapaProcess = f"class {self.__class__.__name__} - def update_processamento - ID_PROCESM_INDICE: {procesm.id_procesm_indice}."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Atualiza dados do processamento"
            # return procesm.save(using=self.db_alias, force_update=True, force_insert=False)

            return Processamento.objects.using(self.db_alias)\
                                        .filter(id_procesm_indice=procesm.id_procesm_indice)\
                                        .update(data_fim_procesm_indice=procesm.data_fim_procesm_indice,
                                                stat_procesm_indice=procesm.stat_procesm_indice,
                                                desc_observacao_procesm_indice=procesm.desc_observacao_procesm_indice,
                                                )

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def update_parametro(self, p_nome_parametro, p_valor_parametro):
        etapaProcess = f"class {self.__class__.__name__} - def update_parametro."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Atualiza parametro {p_nome_parametro} do sistema: {p_valor_parametro}."
            Parametro.objects.using(self.db_alias).filter(nome_parametro_ipm=p_nome_parametro).update(desc_parametro_ipm=p_valor_parametro)
            return None

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def cria_novo_parametro(self, p_nome_parametro, p_valor_parametro):
        etapaProcess = f"class {self.__class__.__name__} - def cria_novo_parametro."

        try:
            etapaProcess = f"Atualiza parametro {p_nome_parametro} do sistema: {p_valor_parametro}."

            # Busca o parâmetro existente
            parametro = Parametro.objects.using(self.db_alias).filter(nome_parametro_ipm=p_nome_parametro).first()

            if parametro:
                parametro.desc_parametro_ipm = p_valor_parametro
                parametro.save(using=self.db_alias)  # Atualiza no banco
            else:
                # Se não existir, cria um novo parâmetro
                Parametro.objects.using(self.db_alias).create(
                    nome_parametro_ipm=p_nome_parametro,
                    desc_parametro_ipm=p_valor_parametro
                )

            return None

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_parametros(self) -> list:
        etapaProcess = f"class {self.__class__.__name__} - def select_parametros"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca dos parâmetros do sistema IPM."
            obj_retorno = Parametro.objects.using(self.db_alias).values('nome_parametro_ipm', 'desc_parametro_ipm').all()
            return list(obj_retorno)

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

    def delete_cfop_partct(self, p_ano_ref):
        etapaProcess = f"class {self.__class__.__name__} - def delete_cfop_partct - {p_ano_ref}."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"exclui dados na tabela IPM_CFOP_PARTICIPANTE para o ano de {p_ano_ref}."
            obj_retorno = CFOP.objects.using(self.db_alias).filter(numr_ano_referencia=p_ano_ref)
            linhas_excluidas = obj_retorno.count()
            obj_retorno.delete()
            return linhas_excluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def delete_cfop_partct_acerto(self, p_ano_ref, p_tipo_doc):
        etapaProcess = f"class {self.__class__.__name__} - def delete_cfop_partct_acerto - {p_ano_ref}."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"exclui dados na tabela IPM_CFOP_PARTICIPANTE para o ano de {p_ano_ref} e tipo de documento {p_tipo_doc}."
            obj_retorno = CFOP.objects.using(self.db_alias).filter(data_inicio_vigencia=p_ano_ref) \
                                                           .filter(codg_tipo_doc_partct_calc=p_tipo_doc)
            linhas_excluidas = obj_retorno.count()
            obj_retorno.delete()
            return linhas_excluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def insert_cfop_partct(self, df):
        etapaProcess = f"class {self.__class__.__name__} - def insert_doc_partct."
        # loga_mensagem(etapaProcess)

        qdade_incluidas = 0

        try:
            etapaProcess = f"Grava dados na tabela IPM_CFOP_PARTICIPANTE"
            records = df.to_dict(orient='records')

            with transaction.atomic():
                for record in records:
                    try:
                        CFOP.objects.using(self.db_alias).create(**record)
                        qdade_incluidas += 1

                    except IntegrityError as e:
                        loga_mensagem_erro(etapaProcess + f" - ERRO - CFOP {record['codg_cfop']} com erro de integridade. Verificar!")
                        continue

            return qdade_incluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def delete_ramo_ativ_cnae(self):
        etapaProcess = f"class {self.__class__.__name__} - def delete_ramo_ativ_cnae."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"exclui dados na tabela IPM_RAMO_ATIVIDADE_CNAE."
            obj_retorno = AtivCNAE.objects.using(self.db_alias)
            linhas_excluidas = obj_retorno.count()
            obj_retorno.delete()
            return linhas_excluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def insert_ramo_ativ_cnae(self, df):
        etapaProcess = f"class {self.__class__.__name__} - def insert_ramo_ativ_cnae."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Grava dados na tabela IPM_RAMO_ATIVIDADE_CNAE"
            records = df.to_dict(orient='records')
            with transaction.atomic():
                obj_retorno = AtivCNAE.objects.using(self.db_alias).bulk_create([
                    AtivCNAE(**record) for record in records
                ], batch_size=1000)

            return len(obj_retorno)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def delete_inscricoes_ccee(self):
        etapaProcess = f"class {self.__class__.__name__} - def delete_inscricoes_ccee."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"exclui todas as linhas da tabela IPM_CONTRIBUINTE_CCEE."
            obj_retorno = CCEE.objects.using(self.db_alias)
            linhas_excluidas = obj_retorno.count()
            obj_retorno.delete()
            return linhas_excluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def insert_inscricoes_ccee(self, df):
        etapaProcess = f"class {self.__class__.__name__} - def insert_inscricoes_ccee."
        # loga_mensagem(etapaProcess)

        qdade_erro = 0

        try:
            etapaProcess = f"Grava dados na tabela IPM_CONTRIBUINTE_CCEE"

            for i, row in df.iterrows():
                if row['tipo_operacao'] == 'I':
                    if pd.isna(row['data_fim_vigencia_ccee']):
                        sql = f"""Insert Into IPM_CONTRIBUINTE_CCEE (NUMR_INSCRICAO,
                                                                     DATA_INICIO_VIGENCIA_CCEE,
                                                                     TIPO_CONTRIB)
                                         Values ({row['numr_inscricao']},
                                                TO_DATE('{row['data_inicio_vigencia_ccee']}', 'DD/MM/YYYY HH24:MI:SS'),
                                                '{row['tipo_contrib']}'
                                                )"""
                    else:
                        sql = f"""Insert Into IPM_CONTRIBUINTE_CCEE (NUMR_INSCRICAO,
                                                                     DATA_INICIO_VIGENCIA_CCEE,
                                                                     DATA_FIM_VIGENCIA_CCEE,
                                                                     TIPO_CONTRIB)
                                         Values ({row['numr_inscricao']},
                                                TO_DATE('{row['data_inicio_vigencia_ccee']}', 'DD/MM/YYYY HH24:MI:SS'),
                                                TO_DATE('{row['data_fim_vigencia_ccee']}', 'DD/MM/YYYY HH24:MI:SS'),
                                                '{row['tipo_contrib']}'
                                                )"""
                else:
                    sql = f"""Update IPM_CONTRIBUINTE_CCEE
                                     Set DATA_FIM_VIGENCIA_CCEE = TO_DATE('{row['data_fim_vigencia_ccee']}', 'DD/MM/YYYY HH24:MI:SS')
                                     Where NUMR_INSCRICAO            = {row['numr_inscricao']}
                                       And DATA_INICIO_VIGENCIA_CCEE = TO_DATE('{row['data_inicio_vigencia_ccee']}', 'DD/MM/YYYY HH24:MI:SS')"""

                try:
                    with connections[self.db_alias].cursor() as cursor:
                        cursor.execute(sql)

                except IntegrityError as e:
                    loga_mensagem_erro(etapaProcess + f" - ERRO - Inscrição {row['numr_inscricao']} com erro de integridade. Verificar!")
                    qdade_erro += 1
                    continue

            return qdade_erro

            # with transaction.atomic():
            #     for index, row in df.iterrows():
            #         CCEE.objects\
            #             .using(self.db_alias)\
            #             .update_or_create(defaults=row.to_dict(),
            #                               **row.to_dict()
            #                               )
            # return None

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_inscricoes_ccee_periodo(self, p_data_inicio, p_data_fim) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def select_inscricoes_ccee_periodo - Periodo de {p_data_inicio} a {p_data_fim}."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess += f"Executa query de busca da relação de inscrições vigentes no periodo de {p_data_inicio} a {p_data_fim}."
            dados = CCEE.objects.using(self.db_alias)\
                        .filter(Q(data_inicio_vigencia__lte=p_data_inicio, data_fim_vigencia__gte=p_data_fim)
                                |
                                Q(data_inicio_vigencia__lte=p_data_inicio, data_fim_vigencia__isnull=True)
                                ).values('numr_inscricao')
            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_inscricoes_ccee(self, inscrs) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def select_inscricoes_ccee."
        # loga_mensagem(etapaProcess)

        etapaProcess += f"Executa query de busca da relação de inscrições com cadastro no CCEE."

        try:
            tamanho_lote = 20000
            lotes = [inscrs[i:i + tamanho_lote] for i in range(0, len(inscrs), tamanho_lote)]
            dfs = []

            for lote in lotes:
                dados = CCEE.objects.using(self.db_alias)\
                            .filter(numr_inscricao__in=lote)\
                            .values('numr_inscricao', 'data_inicio_vigencia_ccee', 'data_fim_vigencia_ccee', 'tipo_contrib')

                df = pd.DataFrame(dados)
                dfs.append(df)

            return pd.concat(dfs, ignore_index=True)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def insert_cnae_rural(self, df):
        etapaProcess = f"class {self.__class__.__name__} - def insert_cnae_rural."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Grava dados na tabela IPM_CNAE_PRODUTOR_RURAL"
            records = df.to_dict(orient='records')
            with transaction.atomic():
                obj_retorno = CNAE_RURAL.objects.using(self.db_alias).bulk_create([
                    CNAE_RURAL(**record) for record in records
                ], batch_size=1000)

            return len(obj_retorno)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_cfop_ipm(self, p_data_inicio, p_data_fim, p_tipo_documento) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def select_cfop_ipm"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess += f"Executa query de busca da relação de CFOPs para o cálculo do IPM no periodo de {p_data_inicio} a {p_data_fim}."
            dados = CFOP.objects.using(self.db_alias) \
                        .filter(codg_tipo_doc_partct_calc=p_tipo_documento) \
                        .filter(Q(data_inicio_vigencia__lte=p_data_inicio, data_fim_vigencia__gte=p_data_fim)
                                |
                                Q(data_inicio_vigencia__lte=p_data_inicio, data_fim_vigencia__isnull=True)
                                ).values('codg_cfop')
            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_cclass_nf3e(self, p_data_inicio, p_data_fim) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def select_cclass_nf3e"
        loga_mensagem(etapaProcess)

        try:
            sql = f"""SELECT CODG_ITEM_NF3E, DESC_ITEM_NF3E, TIPO_OPERACAO as tipo_operacao_cclass
                        From IPM_ITEM_NF3E_PARTCT
                       WHERE TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS') >= DATA_INICIO_VIVENCIA
                         AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS') <= NVL(DATA_FIM_VIGENCIA, SYSDATE)
                   """

            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                colunas = [col[0].lower() for col in cursor.description]
                dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]

            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)

    def select_contrib_ssn(self, inscrs) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def select_contrib_ssn "
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess += f"Executa query de busca o enquadramento do contribuinte no Simples Nacional."
            sql = f"""Select CAST(NUMR_INSCRICAO_SIMPLES As VARCHAR(9)) As NUMR_INSCRICAO  
                           , DATA_INICIO_OPCAO_SIMPLES As DATA_INICIO_OPCAO
                           , DATA_FIM_OPCAO_SIMPLES    As DATA_FINAL_OPCAO
                           , NUMR_OPCAO                As NUMR_OPCAO
                        From IPM_OPCAO_CONTRIB_SIMPL_SIMEI
                       Where INDI_SIMPLES_SIMEI = '1'
                         And INDI_VALIDADE_INFORMACAO = 'S'
                         And NUMR_INSCRICAO_SIMPLES In ({inscrs})
--                         And NUMR_INSCRICAO_SIMPLES In (Select Distinct NUMR_INSCRICAO_CONTRIB
--                                                          From IPM_CONTRIBUINTE_IPM
--                                                         Where CODG_UF = 'GO'
--                                                           And NUMR_INSCRICAO_CONTRIB > 0)
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

    def select_contrib_ssn_periodo(self, inscrs, p_data_inicio, p_data_fim) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def select_contrib_ssn_periodo "
        # loga_mensagem(etapaProcess)

        max_lote = 1000
        dfs = []

        try:
            for i in range(0, len(inscrs), max_lote):
                batch_ids = inscrs[i:i + max_lote]
                lista_inscrs = ', '.join(map(str, batch_ids))

                etapaProcess += f"Executa query de busca o enquadramento do contribuinte no Simples Nacional no periodo de {p_data_inicio} a {p_data_fim}."
                sql = f"""Select CAST(NUMR_INSCRICAO_SIMPLES As VARCHAR(9)) As NUMR_INSCRICAO  
                               , DATA_INICIO_OPCAO_SIMPLES As DATA_INICIO_OPCAO
                               , DATA_FIM_OPCAO_SIMPLES    As DATA_FINAL_OPCAO
                               , NUMR_OPCAO                As NUMR_OPCAO
                            From IPM_OPCAO_CONTRIB_SIMPL_SIMEI
                           Where INDI_SIMPLES_SIMEI = '1'
                             And (DATA_INICIO_OPCAO_SIMPLES <= TO_DATE('{p_data_fim} 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
                             And (DATA_FIM_OPCAO_SIMPLES    >= TO_DATE('{p_data_inicio} 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
                              Or  DATA_FIM_OPCAO_SIMPLES IS NULL))
                              And INDI_VALIDADE_INFORMACAO = 'S'
                             And NUMR_INSCRICAO_SIMPLES In ({lista_inscrs})"""

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

    def insert_contrib_ipm(self, df_contrib):
        etapaProcess = f"class {self.__class__.__name__} - def insert_contrib_ipm."
        # loga_mensagem(etapaProcess)

        try:
            # df_contrib.name = 'df_contrib'
            # baixa_csv(df_contrib)

            etapaProcess = f"Grava dados na tabela IPM_CONTRIBUINTE_IPM"
            contribs = []

            with transaction.atomic():
                for index, row in df_contrib.iterrows():
                    codg_municipio = None if pd.isna(row.codg_municipio) \
                                          or row.codg_municipio == 0 \
                                          or row.codg_municipio == 999999999 \
                                          else row.codg_municipio
                    data_fim_vigencia = None if pd.isna(row.data_fim_vigencia) else row.data_fim_vigencia

                    try:
                        contrib, created = ContribIPM.objects.using(self.db_alias)\
                                                             .get_or_create(numr_inscricao_contrib=row.numr_inscricao_contrib,
                                                                            codg_municipio=codg_municipio,
                                                                            codg_uf=row.codg_uf,
                                                                            data_inicio_vigencia=row.data_inicio_vigencia,
                                                                            data_fim_vigencia=data_fim_vigencia,
                                                                            indi_produtor_rural=row.indi_produtor_rural,
                                                                            indi_produtor_rural_exclusivo=row.indi_produtor_rural_exclusivo,
                                                                            stat_cadastro_contrib=row.stat_cadastro_contrib,
                                                                            tipo_enqdto_fiscal=row.tipo_enqdto_fiscal,
                                                                            id_procesm_indice=row.id_procesm_indice,
                                                                            )
                        contribs.append(contrib)

                    except IntegrityError as e:
                        erro = f"{row} - {e}"
                        loga_mensagem_erro(erro)
                        continue

            data = [{'id_contrib_ipm': contrib.id_contrib_ipm,
                     'numr_inscricao_contrib': contrib.numr_inscricao_contrib,
                     'codg_municipio': contrib.codg_municipio,
                     'codg_uf': contrib.codg_uf,
                     'data_inicio_vigencia': contrib.data_inicio_vigencia,
                     'data_fim_vigencia': contrib.data_fim_vigencia,
                     'indi_produtor_rural': contrib.indi_produtor_rural,
                     'indi_produtor_rural_exclusivo': contrib.indi_produtor_rural_exclusivo,
                     'stat_cadastro_contrib': contrib.stat_cadastro_contrib,
                     'tipo_enqdto_fiscal': contrib.tipo_enqdto_fiscal,
                     'id_procesm_indice': contrib.id_procesm_indice
                     }
                    for contrib in contribs
                    ]

            return pd.DataFrame(data)

        except Exception as err:
            etapaProcess += f" - ERRO - {self.__class__.__name__} - IPM_CONTRIBUINTE_IPM - {err}"
            loga_mensagem_erro(etapaProcess)
            raise

    def update_contrib_ipm_fim_vigencia(self, df_contrib):
        etapaProcess = f"class {self.__class__.__name__} - def update_contrib_ipm_fim_vigencia."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Atualiza data vim de vigencia na tabela IPM_CONTRIBUINTE_IPM"
            contribs = []

            with transaction.atomic():
                for index, row in df_contrib.iterrows():
                    ContribIPM.objects.using(self.db_alias) \
                        .filter(id_contrib_ipm=row.id_contrib_ipm) \
                        .update(data_fim_vigencia=row.data_fim_vigencia,
                                )

            return None

        except Exception as err:
            etapaProcess += f" - ERRO - {self.__class__.__name__} - IPM_CONTRIBUINTE_IPM - {err}"
            loga_mensagem_erro(etapaProcess)
            raise

    def delete_historico_contrib(self, p_periodo):
        etapaProcess = f"class {self.__class__.__name__} - def delete_historico_contrib para o período {p_periodo}."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Exclui dados na tabela IPM_HISTORICO_CONTRIBUINTE"
            obj_retorno = CadastroCCE.objects.using(self.db_alias).filter(numr_referencia=p_periodo)
            linhas_excluidas = obj_retorno.count()
            obj_retorno.delete()
            return linhas_excluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_ncm_prod_rural(self, p_data_inicio, p_data_fim) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def select_ncm_prod_rural"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess += f"Executa query de busca da relação de NCMs que caracterizam um Produtor Rural - {p_data_inicio} a {p_data_fim}."
            dados = NCM_Rural.objects.using(self.db_alias)\
                             .filter(Q(data_inicio_vigencia__lte=p_data_inicio, data_fim_vigencia__gte=p_data_fim) |
                                     Q(data_inicio_vigencia__lte=p_data_inicio, data_fim_vigencia__isnull=True))\
                             .values('id_produto_ncm')
            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_gen_ncm_id(self, ncms):
        etapaProcess = f"class {self.__class__.__name__} - def select_gen_ncm_id."
        # loga_mensagem(etapaProcess)

        try:
            # Realizar busca utilizando filter
            dados = GEN_NCM.objects.using(self.db_alias)\
                                   .filter(id_produto_ncm__in=ncms)\
                                   .values(all)
            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_gen_ncm_cod(self, ncms, p_data_inicio, p_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def select_gen_ncm_cod."
        # loga_mensagem(etapaProcess)

        try:
            tamanho_lote = 20000
            lotes = [ncms[i:i + tamanho_lote] for i in range(0, len(ncms), tamanho_lote)]

            dfs = []

            # Itere sobre cada lote e execute a consulta ao banco de dados
            for lote in lotes:
                # Realizar busca utilizando filter
                dados = GEN_NCM.objects.using(self.db_alias)\
                                       .filter(codg_produto_ncm__in=lote) \
                                       .filter(Q(data_inicio_vigencia_produto__lte=p_data_inicio, data_fim_vigencia_produto__gte=p_data_fim) |
                                               Q(data_inicio_vigencia_produto__lte=p_data_inicio, data_fim_vigencia_produto__isnull=True)) \
                                       .values('id_produto_ncm', 'codg_produto_ncm')
                df = pd.DataFrame(dados)
                dfs.append(df)

            # Concatene todos os DataFrames em um único DataFrame
            df_resultado = pd.concat(dfs, ignore_index=True)

            return df_resultado

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def delete_ncm_rural(self):
        etapaProcess = f"class {self.__class__.__name__} - def delete_ncm_rural."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Exclui dados na tabela IPM_NCM_PRODUTOR_RURAL"
            obj_retorno = NCM_Rural.objects.using(self.db_alias)
            linhas_excluidas = obj_retorno.count()
            obj_retorno.delete()
            return linhas_excluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def insert_ncm_rural(self, df):
        etapaProcess = f"class {self.__class__.__name__} - def insert_ncm_rural."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Grava dados na tabela IPM_NCM_PRODUTOR_RURAL"
            records = df.to_dict(orient='records')
            with transaction.atomic():
                obj_retorno = NCM_Rural.objects.using(self.db_alias).bulk_create([
                    NCM_Rural(**record) for record in records
                ], batch_size=50000)

            return len(obj_retorno)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def insert_parametros(self, parametros):
        etapaProcess = f"class {self.__class__.__name__} - def insert_parametros."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Carga inicial da tabela."

            for parametro in parametros:
                Parametro.objects.using(self.db_alias).update_or_create(
                    nome_parametro_ipm=parametro.nome_parametro_ipm,
                    defaults={'desc_parametro_ipm': parametro.desc_parametro_ipm
                              }
                )
            return None

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def insert_tipo_docs(self, tipo_doc):
        etapaProcess = f"class {self.__class__.__name__} - def insert_tipo_docs."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Carga inicial da tabela de tipos de documentos - IPM_TIPO_DOCUMENTO_PARTCT_CALC."

            for item in tipo_doc:
                TipoDocumento.objects.using(self.db_alias).update_or_create(
                    codg_tipo_doc_partct_calc=item.codg_tipo_doc_partct_calc,
                    defaults={'desc_tipo_doc_partct_calc': item.desc_tipo_doc_partct_calc
                              }
                )
            return None

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def insert_tipo_procesms(self, tipo_procesm):
        etapaProcess = f"class {self.__class__.__name__} - def insert_tipo_procesms."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Carga inicial da tabela de tipos de processamentos - IPM_TIPO_PROCESSAMENTO_INDICE."

            for item in tipo_procesm:
                TipoProcessamento.objects.using(self.db_alias).update_or_create(
                    codg_tipo_procesm_indice=item.codg_tipo_procesm_indice,
                    defaults={'desc_tipo_procesm_indice': item.desc_tipo_procesm_indice
                              }
                )
            return None

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def insert_motivos_exclusao(self, motivo_exclusao):
        etapaProcess = f"class {self.__class__.__name__} - def insert_motivos_exclusao."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Carga inicial da tabela de motivos de exclusão de documentos do cálculo - IPM_MOTIVO_EXCLUSAO_CALCULO."

            for item in motivo_exclusao:
                MotivoExclusao.objects.using(self.db_alias).update_or_create(
                    codg_motivo_exclusao_calculo=item.codg_motivo_exclusao_calculo,
                    defaults={'desc_motivo_exclusao_calculo': item.desc_motivo_exclusao_calculo,
                              'tipo_exclusao_calculo': item.tipo_exclusao_calculo,
                              }
                )
            return None

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
                                .values('id_subclasse_cnaef', 'codg_subclasse_cnaef')
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

        dtypes = {'codg_cnae_rural': 'int32'}

        try:
            sql = f"""Select CODG_SUBCLASSE_CNAEF As CODG_CNAE_RURAL
                        From CCE_SUBCLASSE_CNAE_FISCAL           CNAE
                              Inner Join IPM_CNAE_PRODUTOR_RURAL Rural On CNAe.ID_SUBCLASSE_CNAEF = Rural.ID_SUBCLASSE_CNAEF
                   """

            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                colunas = [col[0].lower() for col in cursor.description]
                dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]

            # Verifica se há dados antes de criar o DataFrame
            if dados:
                df = pd.DataFrame(dados)
                df = df.astype(dtypes)
                return df
            else:
                return None

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)

    def select_contribuinte_ipm(self, df_inscr, p_data_inicio, p_data_fim, batch_size=500):
        etapaProcess = f"class {self.__class__.__name__} - def select_contribuinte_ipm."
        # loga_mensagem(etapaProcess)

        try:
            # Converter DataFrame para lista de tuplas (mais eficiente)
            tupla_inscr = list(zip(df_inscr['ie'].tolist(),
                                   df_inscr['codg_uf'].fillna('GO').tolist()))

            dfs = []

            # Processar em lotes para evitar queries muito grandes
            for i in range(0, len(tupla_inscr), batch_size):
                batch = tupla_inscr[i:i + batch_size]

                # Construir condições Q
                queries = [Q(numr_inscricao_contrib=ie, codg_uf=uf) for ie, uf in batch]
                combined_query = queries.pop()
                for q in queries:
                    combined_query |= q

                # Condições de data
                date_condition = (Q(data_inicio_vigencia__lte=p_data_inicio) &
                                 (Q(data_fim_vigencia__gte=p_data_fim) |
                                  Q(data_fim_vigencia__isnull=True)))

                # Consulta final
                final_query = combined_query & date_condition

                # Executar consulta e armazenar resultados
                dados = ContribIPM.objects.using(self.db_alias) \
                                  .filter(final_query) \
                                  .values('id_contrib_ipm',
                                          'numr_inscricao_contrib',
                                          'codg_municipio',
                                          'codg_uf',
                                          'data_inicio_vigencia',
                                          'data_fim_vigencia',
                                          'stat_cadastro_contrib',
                                          'tipo_enqdto_fiscal',
                                          'indi_produtor_rural',
                                          'indi_produtor_rural_exclusivo',
                                          )

                df_batch = pd.DataFrame.from_records(dados)
                if not df_batch.empty:
                    dfs.append(df_batch)

            # Consolidar todos os resultados
            if dfs:
                obj_retorno = pd.concat(dfs, ignore_index=True)
                if 'data_fim_vigencia' not in obj_retorno.columns:
                    obj_retorno['data_fim_vigencia'] = pd.NaT
                return obj_retorno

            return pd.DataFrame()

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

        # try:
        #     dfs = []
        #
        #     # Itere sobre cada linha do DataFrame e execute a consulta ao banco de dados
        #     for index, row in df_inscr.iterrows():
        #         numr_inscricao = row['ie']
        #         codg_uf = row['codg_uf']
        #         if codg_uf == None:
        #             codg_uf = 'GO'
        #
        #         dados = ContribIPM.objects.using(self.db_alias) \
        #             .filter(numr_inscricao_contrib=numr_inscricao,
        #                     codg_uf=codg_uf) \
        #             .filter(Q(data_inicio_vigencia__lte=p_data_inicio, data_fim_vigencia__gte=p_data_fim)
        #                     |
        #                     Q(data_inicio_vigencia__lte=p_data_inicio, data_fim_vigencia__isnull=True)
        #                     ) \
        #             .values('id_contrib_ipm',
        #                     'numr_inscricao_contrib',
        #                     'codg_municipio',
        #                     'codg_uf',
        #                     'data_inicio_vigencia',
        #                     'data_fim_vigencia',
        #                     'stat_cadastro_contrib',
        #                     'tipo_enqdto_fiscal',
        #                     'indi_produtor_rural',
        #                     'indi_produtor_rural_exclusivo',
        #                     )
        #
        #         df_lote = pd.DataFrame(dados)
        #         dfs.append(df_lote)
        #
        #     # Concatene todos os DataFrames em um único DataFrame
        #     if len(dfs) > 0:
        #         dfs = [df.dropna(axis=1, how='all') for df in dfs]
        #         obj_retorno = pd.concat(dfs, ignore_index=True)
        #
        #         if 'data_fim_vigencia' not in obj_retorno.columns:
        #             obj_retorno['data_fim_vigencia'] = pd.NaT
        #
        #         return obj_retorno
        #     else:
        #         return []
        #
        # except Exception as err:
        #     etapaProcess += " - ERRO - " + str(err)
        #     loga_mensagem_erro(etapaProcess)
        #     raise

    def select_contribuinte_ipm_cadastrados_go(self, p_data_inicio, p_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def select_contribuinte_ipm_cadastrados."
        # loga_mensagem(etapaProcess)

        dtypes = {'numr_inscricao_contrib': 'int32'
                , 'codg_uf': 'str'}

        try:
            sql = f"""Select Distinct NUMR_INSCRICAO_CONTRIB 
                           , CODG_UF
                        From IPM_CONTRIBUINTE_IPM
                       Where NUMR_INSCRICAO_CONTRIB > 0
--                         And NUMR_INSCRICAO_CONTRIB IN (114276951,114277010,114277311)
                         And CODG_UF                = 'GO'
                       Order By NUMR_INSCRICAO_CONTRIB"""

            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                colunas = [col[0].lower() for col in cursor.description]
                dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]

            # Verifica se há dados antes de criar o DataFrame
            if dados:
                df = pd.DataFrame(dados)
                df = df.astype(dtypes)
                return df
            else:
                return None
                # return pd.DataFrame([{'numr_inscricao_contrib': 102350760, 'codg_uf': 'GO'}])

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)



        try:
            dfs = []

            # dados = ContribIPM.objects.using(self.db_alias)\
            #                   .filter(codg_uf='GO')\
            #                   .filter(numr_inscricao_contrib=106432907) \
            #                   .filter(data_fim_vigencia__isnull=True)\
            #                   .all()
            dados = ContribIPM.objects.using(self.db_alias) \
                              .filter(codg_uf='GO') \
                              .filter(models.Q(data_inicio_vigencia__gte=p_data_inicio) \
                                    | models.Q(data_fim_vigencia__gte=p_data_fim) \
                                    | models.Q(data_fim_vigencia__isnull=True))

            # data = [{'id_contrib_ipm': contrib.id_contrib_ipm,
            #          'numr_inscricao_contrib': contrib.numr_inscricao_contrib,
            #          'codg_municipio': contrib.codg_municipio,
            #          'codg_uf': contrib.codg_uf,
            #          'data_inicio_vigencia': contrib.data_inicio_vigencia,
            #          'data_fim_vigencia': contrib.data_fim_vigencia,
            #          'indi_produtor_rural': contrib.indi_produtor_rural,
            #          'stat_cadastro_contrib': contrib.stat_cadastro_contrib,
            #          'tipo_enqdto_fiscal': contrib.tipo_enqdto_fiscal
            #          }
            #         for contrib in dados
            #         ]
            data = [{'numr_inscricao_contrib': contrib.numr_inscricao_contrib,
                     'codg_uf': contrib.codg_uf} for contrib in dados]

            return pd.DataFrame(data)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_contrib_alterados(self) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def select_contrib_alterados."
        # loga_mensagem(etapaProcess)

        try:
            sql = f"""SELECT t1.NUMR_INSCRICAO_CONTRIB
                           , t1.DATA_INICIO_VIGENCIA
                        FROM IPM_CONTRIBUINTE_IPM t1
                              JOIN IPM_CONTRIBUINTE_IPM_NOVO t2 ON  t2.NUMR_INSCRICAO_CONTRIB = t1.NUMR_INSCRICAO_CONTRIB
                                                                AND t2.DATA_INICIO_VIGENCIA   = t1.DATA_INICIO_VIGENCIA
                       WHERE  1 = 1
                         AND  t1.CODG_UF                                            = 'GO'
--                         AND  t1.NUMR_INSCRICAO_CONTRIB IN (100995047, 107315580)
                         AND  NVL(t1.CODG_UF, 'X')                                  = NVL(t2.CODG_UF, 'X')
                         AND (NVL(t1.INDI_PRODUTOR_RURAL, 'X')                     <> NVL(t2.INDI_PRODUTOR_RURAL, 'X')
                          OR  NVL(t1.INDI_PRODUTOR_RURAL_EXCLUSIVO, 'X')           <> NVL(t2.INDI_PRODUTOR_RURAL_EXCLUSIVO, 'X')
                          OR  NVL(t1.DATA_FIM_VIGENCIA, TO_DATE(010101, 'YYMMDD')) <> NVL(t2.DATA_FIM_VIGENCIA, TO_DATE(010101, 'YYMMDD'))
                          OR  NVL(t1.STAT_CADASTRO_CONTRIB, 'X')                   <> NVL(t2.STAT_CADASTRO_CONTRIB, 'X')
                          OR  NVL(t1.TIPO_ENQDTO_FISCAL, 'X')                      <> NVL(t2.TIPO_ENQDTO_FISCAL, 'X')
                          OR  NVL(t1.CODG_MUNICIPIO, 0)                            <> NVL(t2.CODG_MUNICIPIO, 0))"""

            etapaProcess = f"Executa query de busca de informações do cadastro: {sql}"

            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                colunas = [col[0].lower() for col in cursor.description]
                dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]

            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            print(etapaProcess)

    def update_contrib_alterados(self, id_procesm) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def update_contrib_alterados."
        # loga_mensagem(etapaProcess)

        try:
            sql = f"""UPDATE IPM_CONTRIBUINTE_IPM t1
                         SET (DATA_FIM_VIGENCIA
                           ,  INDI_PRODUTOR_RURAL
                           ,  INDI_PRODUTOR_RURAL_EXCLUSIVO
                           ,  STAT_CADASTRO_CONTRIB
                           ,  TIPO_ENQDTO_FISCAL
                           ,  CODG_MUNICIPIO
                           ,  ID_PROCESM_INDICE) = (SELECT t2.DATA_FIM_VIGENCIA
                                                         , t2.INDI_PRODUTOR_RURAL
                                                         , t2.INDI_PRODUTOR_RURAL_EXCLUSIVO
                                                         , t2.STAT_CADASTRO_CONTRIB
                                                         , t2.TIPO_ENQDTO_FISCAL
                                                         , t2.CODG_MUNICIPIO
                                                         , {id_procesm}
                                                      FROM IPM_CONTRIBUINTE_IPM_NOVO t2
                                                     WHERE t1.NUMR_INSCRICAO_CONTRIB = t2.NUMR_INSCRICAO_CONTRIB
                                                       AND t1.DATA_INICIO_VIGENCIA   = t2.DATA_INICIO_VIGENCIA
                                                       AND t1.CODG_UF                = 'GO')
                       WHERE EXISTS (SELECT 1
                                       FROM IPM_CONTRIBUINTE_IPM_NOVO t2
                                      WHERE  t1.NUMR_INSCRICAO_CONTRIB = t2.NUMR_INSCRICAO_CONTRIB
--                                        AND  t1.NUMR_INSCRICAO_CONTRIB IN (108505286, 115225994, 109615670, 114667136)
                                        AND  t1.CODG_UF                                            = 'GO'
                                        AND  t1.DATA_INICIO_VIGENCIA                               = t2.DATA_INICIO_VIGENCIA
                                        AND  NVL(t1.CODG_UF, 'X')                                  = NVL(t2.CODG_UF, 'X')
                                        AND (NVL(t1.INDI_PRODUTOR_RURAL, 'X')                     <> NVL(t2.INDI_PRODUTOR_RURAL, 'X')
                                         OR  NVL(t1.INDI_PRODUTOR_RURAL_EXCLUSIVO, 'X')           <> NVL(t2.INDI_PRODUTOR_RURAL_EXCLUSIVO, 'X')
                                         OR  NVL(t1.DATA_FIM_VIGENCIA, TO_DATE(010101, 'YYMMDD')) <> NVL(t2.DATA_FIM_VIGENCIA, TO_DATE(010101, 'YYMMDD'))
                                         OR  NVL(t1.STAT_CADASTRO_CONTRIB, 'X')                   <> NVL(t2.STAT_CADASTRO_CONTRIB, 'X')
                                         OR  NVL(t1.TIPO_ENQDTO_FISCAL, 'X')                      <> NVL(t2.TIPO_ENQDTO_FISCAL, 'X')
                                         OR  NVL(t1.CODG_MUNICIPIO, 0)                            <> NVL(t2.CODG_MUNICIPIO, 0)))"""

            etapaProcess = f"Executa query de atualização de dados dos contribuintes: {sql}"

            connections[self.db_alias].close()
            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)

            return cursor.rowcount

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            print(etapaProcess)

    def select_contrib_novos(self) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def select_contrib_novos."
        # loga_mensagem(etapaProcess)

        try:
            sql = f"""SELECT t2.NUMR_INSCRICAO_CONTRIB
                           , t2.DATA_INICIO_VIGENCIA
                        FROM IPM_CONTRIBUINTE_IPM_NOVO t2
                              LEFT JOIN IPM_CONTRIBUINTE_IPM t1 ON  t1.NUMR_INSCRICAO_CONTRIB = t2.NUMR_INSCRICAO_CONTRIB
                                                                AND t1.DATA_INICIO_VIGENCIA   = t2.DATA_INICIO_VIGENCIA
                                                                And t1.CODG_UF                = t2.CODG_UF
                       WHERE t2.CODG_UF                 = 'GO'
--                         AND t2.NUMR_INSCRICAO_CONTRIB IN (100995047, 107315580)
                         AND t1.NUMR_INSCRICAO_CONTRIB IS NULL"""

            etapaProcess = f"Executa query de busca de informações do cadastro: {sql}"

            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                colunas = [col[0].lower() for col in cursor.description]
                dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]

            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            print(etapaProcess)

    def insert_contrib_novos(self, id_procesm) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def insert_contrib_novos."
        # loga_mensagem(etapaProcess)

        try:
            sql = f"""INSERT INTO IPM_CONTRIBUINTE_IPM (NUMR_INSCRICAO_CONTRIB
                                                     ,  CODG_UF
                                                     ,  DATA_INICIO_VIGENCIA
                                                     ,  DATA_FIM_VIGENCIA
                                                     ,  INDI_PRODUTOR_RURAL
                                                     ,  INDI_PRODUTOR_RURAL_EXCLUSIVO
                                                     ,  STAT_CADASTRO_CONTRIB
                                                     ,  TIPO_ENQDTO_FISCAL
                                                     ,  CODG_MUNICIPIO
                                                     ,  ID_PROCESM_INDICE)
                      SELECT t2.NUMR_INSCRICAO_CONTRIB
                           , t2.CODG_UF
                           , t2.DATA_INICIO_VIGENCIA
                           , t2.DATA_FIM_VIGENCIA
                           , t2.INDI_PRODUTOR_RURAL
                           , t2.INDI_PRODUTOR_RURAL_EXCLUSIVO
                           , t2.STAT_CADASTRO_CONTRIB
                           , t2.TIPO_ENQDTO_FISCAL
                           , t2.CODG_MUNICIPIO
                           , {id_procesm}     
                        FROM IPM_CONTRIBUINTE_IPM_NOVO t2
                              LEFT JOIN IPM_CONTRIBUINTE_IPM t1 ON  t1.NUMR_INSCRICAO_CONTRIB = t2.NUMR_INSCRICAO_CONTRIB
                                                                AND t1.DATA_INICIO_VIGENCIA   = t2.DATA_INICIO_VIGENCIA
                                                                And t1.CODG_UF                = t2.CODG_UF
                       WHERE t2.CODG_UF                 = 'GO'
--                         AND t2.NUMR_INSCRICAO_CONTRIB IN (108505286, 115225994, 109615670, 114667136)
                         AND t1.NUMR_INSCRICAO_CONTRIB IS NULL"""

            etapaProcess = f"Executa query de inclusão de contribuintes no cadastro: {sql}"

            connections[self.db_alias].close()
            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)

            return cursor.rowcount

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            print(etapaProcess)

    def select_contrib_excedentes(self) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def select_contrib_excedentes."
        # loga_mensagem(etapaProcess)

        try:
            sql = f"""SELECT t1.NUMR_INSCRICAO_CONTRIB
                           , t1.DATA_INICIO_VIGENCIA
                        FROM IPM_CONTRIBUINTE_IPM t1
                       WHERE t1.CODG_UF = 'GO'
--                         AND t1.NUMR_INSCRICAO_CONTRIB IN (100995047, 107315580)
                         AND NOT EXISTS (SELECT 1 
                                           FROM IPM_CONTRIBUINTE_IPM_NOVO t2
                                          WHERE t1.NUMR_INSCRICAO_CONTRIB = t2.NUMR_INSCRICAO_CONTRIB
                                            AND t1.DATA_INICIO_VIGENCIA   = t2.DATA_INICIO_VIGENCIA
                                            AND t1.CODG_UF                = t2.CODG_UF)"""

            etapaProcess = f"Executa query de busca de informações do cadastro: {sql}"

            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                colunas = [col[0].lower() for col in cursor.description]
                dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]

            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            print(etapaProcess)

    def delete_contrib_excedentes(self) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def delete_contrib_excedentes."
        # loga_mensagem(etapaProcess)

        try:
            sql = f"""DELETE FROM IPM_CONTRIBUINTE_IPM t1
                       WHERE t1.CODG_UF = 'GO'
--                         AND t1.NUMR_INSCRICAO_CONTRIB IN (108505286, 115225994, 109615670, 114667136)
                         AND NOT EXISTS (SELECT 1 
                                           FROM IPM_CONTRIBUINTE_IPM_NOVO t2
                                          WHERE t1.NUMR_INSCRICAO_CONTRIB = t2.NUMR_INSCRICAO_CONTRIB
                                            AND t1.DATA_INICIO_VIGENCIA   = t2.DATA_INICIO_VIGENCIA
                                            AND t1.CODG_UF                = t2.CODG_UF)"""

            etapaProcess = f"Executa query de exclusão de contribuintes no cadastro: {sql}"

            connections[self.db_alias].close()
            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)

            return cursor.rowcount

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            print(etapaProcess)

    def delete_contrib_novo(self) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def delete_contrib_novo."
        # loga_mensagem(etapaProcess)

        try:
            sql = f"""DELETE FROM IPM_CONTRIBUINTE_IPM_NOVO t1
                       WHERE NUMR_INSCRICAO_CONTRIB <= 110000000"""

            sql1 = f"""DELETE FROM IPM_CONTRIBUINTE_IPM_NOVO t1
                       WHERE NUMR_INSCRICAO_CONTRIB > 110000000"""

            etapaProcess = f"Executa query de exclusão de contribuintes no cadastro: {sql}"

            connections[self.db_alias].close()
            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                cursor.execute(sql1)

            return cursor.rowcount

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            print(etapaProcess)

    def insert_contrib_nv(self, df) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def insert_contrib_nv."
        # loga_mensagem(etapaProcess)

        d_data_inicio_vigencia = datetime.strptime(str(20230101), '%Y%m%d').date()

        with connections[self.db_alias].cursor() as cursor:
            lote = []
            total_inserido = 0

            sql = f"""Insert Into IPM_CONTRIBUINTE_IPM_NOVO (DATA_INICIO_VIGENCIA
                                                           , DATA_FIM_VIGENCIA
                                                           , INDI_PRODUTOR_RURAL
                                                           , STAT_CADASTRO_CONTRIB
                                                           , TIPO_ENQDTO_FISCAL
                                                           , NUMR_INSCRICAO_CONTRIB
                                                           , INDI_PRODUTOR_RURAL_EXCLUSIVO
                                                           , CODG_UF
                                                           , CODG_MUNICIPIO
                                                           , ID_PROCESM_INDICE)
                                                      Values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            for i, row in df.iterrows():
                lote.append((d_data_inicio_vigencia, None, 'N', '1', '1', row['numr_inscricao_contrib'], 'N', 'GO', None, 1))

                if len(lote) == 10000:
                    cursor.executemany(sql, lote)
                    connections[self.db_alias].commit()
                    total_inserido += len(lote)
                    lote = []

            # Insere os dados restantes que não completaram um lote
            if lote:
                cursor.executemany(sql, lote)
                connections[self.db_alias].commit()
                total_inserido += len(lote)

        return total_inserido

    def select_hist_cce(self, p_inscricao) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def select_hist_cce - {p_inscricao}."
        # loga_mensagem(etapaProcess)

        etapaProcess = "Executa query de busca do histórico do cadastro do Contribuinte."

        try:
            sql = f"""With Pessoa   As (Select TO_CHAR(ID_PESSOA)   As ID_PESSOA
                                          From CCE_CONTRIBUINTE
                                         Where NUMR_INSCRICAO = {p_inscricao}
                                        Union
                                        Select VALR_ANTERIOR_COLUNA As ID_PESSOA
                                          From CCE_HISTORICO
                                         Where NOME_TABELA = 'CCE_CONTRIBUINTE'
                                           And NOME_COLUNA   = 'ID_PESSOA'
                                           And ID_HISTORICO  = {p_inscricao}
                                        Union
                                        Select TO_CHAR(ID_PESSOA)   As ID_PESSOA
                                          From CCE_PREPOSTO
                                         Where NUMR_INSCRICAO = {p_inscricao}
                                        Union
                                        Select VALR_ANTERIOR_COLUNA As ID_PESSOA
                                          From CCE_HISTORICO
                                         Where NOME_TABELA = 'CCE_PREPOSTO'
                                           And NOME_COLUNA   = 'ID_PESSOA'
                                           And ID_HISTORICO  = {p_inscricao})  
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
                                        Where CH.NUMR_INSCRICAO       = {p_inscricao}
                                          And CH.INDI_CORRECAO        = 'N'
                                          And CH.ID_SOLICT           Is NOT NULL
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
                                        Where NUMR_INSCRICAO           = {p_inscricao}
                                          And ID_SOLICT               Is NOT NULL
                                          And NOME_TABELA            In ('GEN_PESSOA_JURIDICA'
                                                                      ,  'GEN_PESSOA_FISICA'
                                                                      ,  'GEN_ENDERECO'
                                                                      ,  'GEN_SITE_PESSOA'
                                                                      ,  'GEN_TELEFONE_PESSOA'
                                                                      ,  'GEN_EMAIL_PESSOA')
                                          And NOME_COLUNA             <> 'INDI_HOMOLOG_CADASTRO'
                                          And INDI_CORRECAO            = 'N')

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
                                        Where (
                                               (NOME_TABELA           In ('GEN_PESSOA_JURIDICA'
                                                                       ,  'GEN_PESSOA_FISICA')
                                          And   ID_HISTORICO          In (Select ID_PESSOA From Pessoa))

                                           Or  (NOME_TABELA            = 'GEN_ENDERECO'
                                          And ((ID_HISTORICO          In (Select ID_ENDERECO
                                                                            From CCE_ESTAB_CONTRIBUINTE
                                                                           Where NUMR_INSCRICAO = {p_inscricao}))
                                           Or  (ID_HISTORICO          In (Select ID_ENDERECO
                                                                            From GEN_PESSOA_ENDERECO
                                                                           Where ID_PESSOA In (Select ID_PESSOA From Pessoa)))))

                                           Or  (NOME_TABELA            = 'GEN_SITE_PESSOA'
                                          And   ID_HISTORICO          In (Select ID_SITE_PESSOA
                                                                            From GEN_SITE_PESSOA
                                                                           Where ID_PESSOA In (Select ID_PESSOA From Pessoa))) 

                                           Or  (NOME_TABELA            = 'GEN_TELEFONE_PESSOA'
                                          And   ID_HISTORICO          In (Select ID_TELEFONE
                                                                            From GEN_TELEFONE_PESSOA
                                                                           Where ID_PESSOA In (Select ID_PESSOA From Pessoa))) 

                                           Or  (NOME_TABELA            = 'GEN_EMAIL_PESSOA'
                                          And   ID_HISTORICO          In (Select ID_EMAIL_PESSOA
                                                                            From GEN_EMAIL_PESSOA
                                                                           Where ID_PESSOA In (Select ID_PESSOA From Pessoa))))

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

            connections[self.db_alias].close()
            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                colunas = [col[0].lower() for col in cursor.description]
                dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]

            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            print(etapaProcess)

    def delete_base_dados(self):
        etapaProcess = f"class {self.__class__.__name__} - def delete_base_dados."
        loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de exclusão da tabela IPM_DOCUMENTO_PARTCT_CALC_IPM."
            loga_mensagem(etapaProcess)
            sql = f"""Delete From IPM_DOCUMENTO_PARTCT_CALC_IPM
                       Where 1=1"""
                         # And CODG_TIPO_DOC_PARTCT_CALC = 4"""
                         # And DATA_EMISSAO_DOCUMENTO       > TO_DATE('2023-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')"""
                         # And DATA_EMISSAO_DOCUMENTO BETWEEN TO_DATE('2023-06-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
                         #                                AND TO_DATE('2023-06-30 23:59:59', 'YYYY-MM-DD HH24:MI:SS')"""
            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)

            etapaProcess = f"Executa query de exclusão da tabela IPM_ITEM_DOCUMENTO."
            loga_mensagem(etapaProcess)
            sql = f"""Delete From IPM_ITEM_DOCUMENTO
                       Where 1=1"""
                         # And CODG_TIPO_DOC_PARTCT_CALC = 4"""
                         # And DATA_EMISSAO_DOCUMENTO       > TO_DATE('2023-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')"""
                         # And DATA_EMISSAO_DOCUMENTO BETWEEN TO_DATE('2023-06-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
                         #                                AND TO_DATE('2023-06-30 23:59:59', 'YYYY-MM-DD HH24:MI:SS')"""
            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)

            etapaProcess = f"Executa query de exclusão da tabela IPM_PROCESSAMENTO_INDICE."
            loga_mensagem(etapaProcess)
            sql = f"""Delete From IPM_PROCESSAMENTO_INDICE
                       Where 1=1"""
                         # And CODG_TIPO_PROCESM_INDICE = 4"""
            #              And DATA_INICIO_PERIODO_INDICE >= TO_DATE('2024-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')"""
            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)

            loga_mensagem('Limpeza concluída!')
            return None

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def delete_ipm_documento_partct_calc_ipm(self, p_tipo_doc):
        etapaProcess = f"class {self.__class__.__name__} - def delete_ipm_documento_partct_calc_ipm."
        loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de exclusão da tabela IPM_DOCUMENTO_PARTCT_CALC_IPM - {p_tipo_doc}"
            loga_mensagem(etapaProcess)
            sql = f"""Delete From IPM_DOCUMENTO_PARTCT_CALC_IPM
                       Where CODG_TIPO_DOC_PARTCT_CALC = {p_tipo_doc}"""
                         # And DATA_EMISSAO_DOCUMENTO       > TO_DATE('2023-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')"""
                         # And DATA_EMISSAO_DOCUMENTO BETWEEN TO_DATE('2023-06-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
                         #                                AND TO_DATE('2023-06-30 23:59:59', 'YYYY-MM-DD HH24:MI:SS')"""
            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def delete_ipm_item_documento(self, p_tipo_doc):
        etapaProcess = f"class {self.__class__.__name__} - def delete_ipm_item_documento."
        loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de exclusão da tabela IPM_ITEM_DOCUMENTO - {p_tipo_doc}"
            loga_mensagem(etapaProcess)
            sql = f"""Delete From IPM_ITEM_DOCUMENTO
                       Where CODG_TIPO_DOC_PARTCT_DOCUMENTO = {p_tipo_doc}"""
            # And DATA_EMISSAO_DOCUMENTO       > TO_DATE('2023-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')"""
            # And DATA_EMISSAO_DOCUMENTO BETWEEN TO_DATE('2023-06-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
            #                                AND TO_DATE('2023-06-30 23:59:59', 'YYYY-MM-DD HH24:MI:SS')"""
            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def delete_ipm_contribuinte_ipm(self):
        etapaProcess = f"class {self.__class__.__name__} - def delete_ipm_contribuinte_ipm."
        loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de exclusão da tabela IPM_CONTRIBUINTE_IPM."
            loga_mensagem(etapaProcess)
            sql = f"""Delete From IPM_CONTRIBUINTE_IPM
                       Where 1=1"""
                         # And DATA_INICIO_VIGENCIA = TO_DATE('13/02/2023', 'dd/mm/yyyy')"""
            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def delete_ipm_processamento_indice(self, p_tipo_procesm):
        etapaProcess = f"class {self.__class__.__name__} - def delete_ipm_processamento_indice."
        loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de exclusão da tabela IPM_PROCESSAMENTO_INDICE - {p_tipo_procesm}"
            loga_mensagem(etapaProcess)
            sql = f"""Delete From IPM_PROCESSAMENTO_INDICE
                       Where CODG_TIPO_PROCESM_INDICE = {p_tipo_procesm}"""
            #              And DATA_INICIO_PERIODO_INDICE >= TO_DATE('2024-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')"""
            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_municipio_siaf(self):
        etapaProcess = f"class {self.__class__.__name__} - def select_municipio_siaf"
        loga_mensagem(etapaProcess)

        try:
            sql = f"""Select CODG_MUNICIPIO_SIAF
                           , CODG_MUNICIPIO
                        From IPM_MUNICIPIO_SIAF"""

            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                colunas = [col[0].lower() for col in cursor.description]
                dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]

            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess, err)
            raise

    def insert_municipio_siaf(self, df):
        etapaProcess = f"class {self.__class__.__name__} - def insert_municipio_siaf."
        # loga_mensagem(etapaProcess)

        lote = []
        total_inserido = 0
        etapaProcess = f"Insere IPM_MUNICIPIO_SIAF"

        try:
            sql = """INSERT INTO IPM_MUNICIPIO_SIAF (CODG_MUNICIPIO_SIAF
                                                   , CODG_MUNICIPIO)
                                             VALUES (%s, %s)"""

            with connections[self.db_alias].cursor() as cursor:
                for i, row in df.iterrows():
                    lote.append((row['CODG_MUNICIPIO_SIAF'],
                                 row['CODG_MUNICIPIO'],
                                ))

                cursor.executemany(sql, lote)
                connections[self.db_alias].commit()
                total_inserido += len(lote)

            return total_inserido

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess, err)
            raise

    def select_docs_calculo_va(self, d_data_inicio, d_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def select_docs_calculo_va"
        loga_mensagem(etapaProcess)

        try:
            sql = f"""Select 'S'                                                     As Tipo_Aprop
                           , IESai.NUMR_INSCRICAO_CONTRIB                            As Numr_Inscricao
                           , NVL(IESai.CODG_MUNICIPIO, Ident.CODG_MUNICIPIO_SAIDA)   As Codg_Municipio
                           , Ident.NUMR_REFERENCIA_DOCUMENTO                         As Numr_Referencia
                           , Ident.CODG_TIPO_DOC_PARTCT_CALC                         As Tipo_Documento
                           , SUM(Ident.VALR_ADICIONADO_OPERACAO)                     As Valr_Adicionado_Operacao
                        From IPM_DOCUMENTO_PARTCT_CALC_IPM    Ident
                              Left  Join IPM_CONTRIBUINTE_IPM IESai  On Ident.ID_CONTRIB_IPM_SAIDA = IESai.ID_CONTRIB_IPM
                              Left  Join GEN_MUNICIPIO        MunDoc On Ident.CODG_MUNICIPIO_SAIDA = MunDoc.CODG_MUNICIPIO
                       Where Ident.CODG_MOTIVO_EXCLUSAO_CALCULO      Is Null
                         And Ident.INDI_APROP                        In ('A', 'S')
                         And NVL(IESai.CODG_UF, MunDoc.CODG_UF)       = 'GO'
                         AND Ident.DATA_EMISSAO_DOCUMENTO       Between TO_DATE('{d_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                                    And TO_DATE('{d_data_fim}', 'YYYY-MM-DD HH24:MI:SS')
                       Group By IESai.NUMR_INSCRICAO_CONTRIB
                              , NVL(IESai.CODG_MUNICIPIO, Ident.CODG_MUNICIPIO_SAIDA)
                              , Ident.NUMR_REFERENCIA_DOCUMENTO
                              , Ident.CODG_TIPO_DOC_PARTCT_CALC
                      Union
                      Select 'E'                                                     As Tipo_Aprop
                           , IEEnt.NUMR_INSCRICAO_CONTRIB                            As Numr_Inscricao
                           , NVL(IEEnt.CODG_MUNICIPIO, Ident.CODG_MUNICIPIO_ENTRADA) As Codg_Municipio
                           , Ident.NUMR_REFERENCIA_DOCUMENTO                         As Numr_Referencia
                           , Ident.CODG_TIPO_DOC_PARTCT_CALC                         As Tipo_Documento
                           , SUM(Ident.VALR_ADICIONADO_OPERACAO)                     As Valr_Adicionado_Operacao
                        From IPM_DOCUMENTO_PARTCT_CALC_IPM    Ident
                              Left  Join IPM_CONTRIBUINTE_IPM IEEnt  On Ident.ID_CONTRIB_IPM_ENTRADA = IEEnt.ID_CONTRIB_IPM
                              Left  Join GEN_MUNICIPIO        MunDoc On Ident.CODG_MUNICIPIO_ENTRADA = MunDoc.CODG_MUNICIPIO
                       Where Ident.CODG_MOTIVO_EXCLUSAO_CALCULO      Is Null
                         And Ident.INDI_APROP                        In ('A', 'E')
                         And NVL(IEEnt.CODG_UF, MunDoc.CODG_UF)       = 'GO'
                         AND Ident.DATA_EMISSAO_DOCUMENTO       Between TO_DATE('{d_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                                    And TO_DATE('{d_data_fim}', 'YYYY-MM-DD HH24:MI:SS')
                       Group By IEEnt.NUMR_INSCRICAO_CONTRIB
                              , NVL(IEEnt.CODG_MUNICIPIO, Ident.CODG_MUNICIPIO_ENTRADA)
                              , Ident.NUMR_REFERENCIA_DOCUMENTO
                              , Ident.CODG_TIPO_DOC_PARTCT_CALC
                       Order By Numr_Inscricao
                              , Tipo_Documento
                              , Tipo_Aprop"""

            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                colunas = [col[0].lower() for col in cursor.description]
                dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]

            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess, err)
            raise

    def select_docs_calculo_indice(self, p_ano):
        etapaProcess = f"class {self.__class__.__name__} - def select_docs_calculo_indice - {p_ano}"
        loga_mensagem(etapaProcess)

        try:
            sql = f"""WITH VAMun  AS (SELECT CODG_MUNICIPIO
                                           , SUM(VALOR_VA)  AS VA_MUNICIPIO
                                        FROM (SELECT CODG_MUNICIPIO
                                                   , NUMR_INSCRICAO
                                                   , CASE WHEN SUM(VALR_SAIDA) - SUM(VALR_ENTRADA) < 0
                                                          THEN 0
                                                          ELSE SUM(VALR_SAIDA) - SUM(VALR_ENTRADA) END AS VALOR_VA
                                                FROM (SELECT NVL(Munic.CODG_MUNICIPIO_DISTRITO, VA.CODG_MUNICIPIO) AS CODG_MUNICIPIO
                                                           , NUMR_INSCRICAO
                                                           , CASE WHEN TIPO_APROP_VALOR_ADICIONADO = 'E'
                                                                  THEN VALR_ADICIONADO
                                                                  ELSE 0                        END      AS VALR_ENTRADA
                                                           , CASE WHEN TIPO_APROP_VALOR_ADICIONADO = 'S'
                                                                  THEN VALR_ADICIONADO
                                                                  ELSE 0                        END      AS VALR_SAIDA
                                                        FROM IPM_VALOR_ADICIONADO      VA
                                                              INNER JOIN GEN_MUNICIPIO Munic ON VA.CODG_MUNICIPIO = Munic.CODG_MUNICIPIO
                                                       WHERE SUBSTR(REFE_VALOR_ADICIONADO, 1,4) = '{p_ano}'
                                                         AND Munic.CODG_UF                      = 'GO')
                                               GROUP BY CODG_MUNICIPIO
                                                      , NUMR_INSCRICAO)
                                       GROUP BY CODG_MUNICIPIO)
                         , VAEst  AS (SELECT SUM(VA_MUNICIPIO) AS VA_ESTADO
                                        FROM VAMun)
                         , Perc   AS (SELECT CODG_MUNICIPIO
                                           , VA_MUNICIPIO
                                           , VA_ESTADO
                                           , TRUNC((VA_MUNICIPIO / VA_ESTADO), 18) * 100 AS INDICE
                                        FROM VAMun
                                           , VAEst)
                      SELECT * FROM Perc"""


            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                colunas = [col[0].lower() for col in cursor.description]
                dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]

            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess, err)
            raise

    def insert_valor_adicionado(self, df, chunk_size=100000):
        etapaProcess = f"class {self.__class__.__name__} - def insert_valor_adicionado."
        # loga_mensagem(etapaProcess)

        try:
            with connections[self.db_alias].cursor() as cursor:
                lote = []
                total_inserido = 0

                sql = f"""Insert Into IPM_VALOR_ADICIONADO (CODG_TIPO_DOC_PARTCT_CALC
                                                         ,  REFE_VALOR_ADICIONADO
                                                         ,  TIPO_APROP_VALOR_ADICIONADO
                                                         ,  VALR_ADICIONADO
                                                         ,  ID_PROCESM_INDICE
                                                         ,  NUMR_INSCRICAO
                                                         ,  CODG_MUNICIPIO)
                                                  VALUES (%s, %s, %s, %s, %s, %s, %s)"""

                for i, row in df.iterrows():
                    i_inscricao = 0 if pd.isna(row['numr_inscricao']) \
                                    or row['numr_inscricao'] in [0, 'None'] \
                                    else int(row['numr_inscricao'])

                    i_codg_municipio = 0 if pd.isna(row['codg_municipio']) \
                                       or row['codg_municipio'] in [0, 'None'] \
                                       else int(row['codg_municipio'])

                    lote.append((row['tipo_documento'],
                                 row['numr_referencia'],
                                 row['tipo_aprop'],
                                 row['valr_adicionado_operacao'],
                                 row['id_procesm_indice'],
                                 i_inscricao,
                                 i_codg_municipio,
                                 ))

                    if len(lote) == chunk_size:
                        cursor.executemany(sql, lote)
                        connections[self.db_alias].commit()
                        total_inserido += len(lote)
                        lote = []

                # Insere os dados restantes que não completaram um lote
                if lote:
                    cursor.executemany(sql, lote)
                    connections[self.db_alias].commit()
                    total_inserido += len(lote)

            return total_inserido

        except Exception as err:
            etapaProcess += f" - ERRO - {self.__class__.__name__} - IPM_VALOR_ADICIONADO - {err}"
            loga_mensagem_erro(etapaProcess)
            raise

    def delete_valor_adicionado(self, p_mes_ref):
        etapaProcess = f"class {self.__class__.__name__} - def delete_valor_adicionado - {p_mes_ref}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de exclusão da tabela IPM_VALOR_ADICIONADO para a referencia {p_mes_ref}."
            sql = f"""Delete From IPM_VALOR_ADICIONADO
                       Where REFE_VALOR_ADICIONADO = {p_mes_ref}"""

            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def insert_indice_partct(self, df, chunk_size=100000):
        etapaProcess = f"class {self.__class__.__name__} - def insert_indice_partct."
        # loga_mensagem(etapaProcess)

        try:
            with connections[self.db_alias].cursor() as cursor:
                lote = []
                total_inserido = 0

                sql = f"""Insert Into IPM_INDICE_PARTICIPACAO (NUMR_ANO_REF
                                                            ,  CODG_MUNICIPIO
                                                            ,  ID_PROCESM_INDICE
                                                            ,  INDI_ETAPA_INDICE_PARTCP
                                                            ,  TIPO_INDICE_PARTCP
                                                            ,  VALR_INDICE_PARTCP
                                                            ,  DESC_MOTIVO_GERACAO
                                                            , ID_RESOLUCAO_PARTCP_MUNICIPIO)
                                                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

                for i, row in df.iterrows():
                    lote.append((row['numr_ano_ref'],
                                 row['codg_municipio'],
                                 row['id_procesm_indice'],
                                 row['etapa_indice'],
                                 row['tipo_indice_partcp'],
                                 row['indice'],
                                 row['desc_motivo_geracao'],
                                 row['id_resolucao'],
                                 ))

                    if len(lote) == chunk_size:
                        cursor.executemany(sql, lote)
                        connections[self.db_alias].commit()
                        total_inserido += len(lote)
                        lote = []

                # Insere os dados restantes que não completaram um lote
                if lote:
                    cursor.executemany(sql, lote)
                    connections[self.db_alias].commit()
                    total_inserido += len(lote)

            return total_inserido

        except Exception as err:
            etapaProcess += f" - ERRO - {self.__class__.__name__} - IPM_INDICE_PARTICIPACAO - {err}"
            loga_mensagem_erro(etapaProcess)
            raise

    def delete_indice_partct(self, p_ano, p_tipo_indice, p_etapa_indice):
        etapaProcess = f"class {self.__class__.__name__} - def delete_indice_partct - {p_ano} - {p_etapa_indice} - {p_tipo_indice}."
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de exclusão da tabela IPM_INDICE_PARTICIPACAO para Ano {p_ano} Etapa {p_etapa_indice} Tipo {p_tipo_indice}."
            sql = f"""Delete From IPM_INDICE_PARTICIPACAO
                       Where NUMR_ANO_REF             = {p_ano}
                         And INDI_ETAPA_INDICE_PARTCP = '{p_etapa_indice}'
                         And TIPO_INDICE_PARTCP       = '{p_tipo_indice}'"""

            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_doc_nao_partct(self, p_inicio_referencia, p_fim_referencia, p_tipo_documento, p_escopo, p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida, p_chave):
        etapaProcess = f"class {self.__class__.__name__} - def select_doc_nao_partct_nfe_chave - {p_chave}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"Executa query de busca de informações da Doc ão PartCt - {p_escopo} - {p_data_inicio} - {p_data_fim} - {p_ie_entrada} - {p_ie_saida} - {p_chave}"
            sql = monta_sql_doc_nao_partct(p_inicio_referencia, p_fim_referencia, p_tipo_documento, p_escopo, p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida, p_chave)

            if sql is None:
                return []
            else:
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
                                            Where  1 = 1 
                                              And  CH.NUMR_INSCRICAO In ({inscrs})
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

    def select_municipios(self) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def select_municipios."
        loga_mensagem(etapaProcess)

        etapaProcess = "Executa query de busca os municípios."
        try:
            sql = f"""Select Case When Municip.CODG_MUNICIPIO Is Null
                                  Then Distrit.CODG_MUNICIPIO
                                  Else Municip.CODG_MUNICIPIO         End   As CODG_MUNICIPIO
                           , Case When Municip.NOME_MUNICIPIO Is Null
                                  Then Distrit.NOME_MUNICIPIO
                                  Else Municip.NOME_MUNICIPIO         End   As NOME_MUNICIPIO
                           , Distrit.CODG_MUNICIPIO                         As CODG_DISTRITO
                           , Distrit.NOME_MUNICIPIO                         As NOME_DISTRITO
                           , Distrit.CODG_UF                                As CODG_UF
                        From GEN_MUNICIPIO             Distrit
                              Left  Join GEN_MUNICIPIO Municip On Distrit.CODG_MUNICIPIO_DISTRITO  = Municip.CODG_MUNICIPIO"""

            connections[self.db_alias].close()
            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                colunas = [col[0].lower() for col in cursor.description]
                dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]

            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            print(etapaProcess)

    def select_logradouro_municipio(self, p_cod_lograd) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def select_logradouro_municipio."
        # loga_mensagem(etapaProcess)

        etapaProcess = "Executa query de busca os municípios pelos logradouros."
        try:
            sql = f"""Select CODG_MUNICIPIO
                        From GEN_LOGRADOURO
                       Where CODG_LOGRADOURO = {p_cod_lograd}"""

            connections[self.db_alias].close()
            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                colunas = [col[0].lower() for col in cursor.description]
                dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]

            # Verifica se há dados antes de criar o DataFrame
            if dados:
                df = pd.DataFrame(dados)
                return df
            else:
                loga_mensagem(f'Não achou GEN_LOGRADOURO para CODG_LOGRADOURO = {p_cod_lograd}')
                return None

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            print(etapaProcess)

    def insert_item_doc_nao_partct(self, df):
        etapaProcess = f"class {self.__class__.__name__} - def insert_item_doc_nao_partct."
        # loga_mensagem(etapaProcess)

        try:
            # etapaProcess = f"Grava dados na tabela IPM_ITEM_DOCTO_NAO_PARTC_CALC"
            # records = df.to_dict(orient='records')
            # with transaction.atomic():
            #     # Use bulk_create para inserir os registros no banco de dados
            #     ItemNaoPartctCalculo.objects.using(self.db_alias).bulk_create([
            #         ItemNaoPartctCalculo(**record) for record in records
            #     ], batch_size=50000)
            #
            # return None

            for i, row in df.iterrows():
                sql = f"""Insert Into IPM_ITEM_DOC_NAO_PARTCT_CALC
                              Values ({row['codg_item_doc_nao_partct_calc']},
                                     '{row['data_emissao_documento']}',
                                     '{row['codg_tipo_doc_partct_calc']}',
                                     '{row['id_procesm_indice']}',
                                     '{row['codg_motivo_exclusao_calculo']}'
                                      )
                           """
                with connections[self.db_alias].cursor() as cursor:
                    cursor.execute(sql)

            return df.shape[0]

        except Exception as err:
            df.name = 'item_doc_nao_partct'
            baixa_csv(df)
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def delete_item_doc_nao_partct(self, p_data_inicio, p_data_fim, p_tipo_documento):
        etapaProcess = f"class {self.__class__.__name__} - def delete_item_doc_nao_partct - Tipo de documento: {p_tipo_documento} - Periodo: {p_data_inicio} a {p_data_fim}"
        # loga_mensagem(etapaProcess)

        try:
            etapaProcess = f"exclui dados na tabela IPM_ITEM_DOCTO_NAO_PARTC_CALC para o período de {p_data_inicio} a {p_data_fim}."
            obj_retorno = ItemNaoPartctCalculo.objects.using(self.db_alias)\
                                              .filter(Q(data_emissao_documento__gte=p_data_inicio) &
                                                      Q(data_emissao_documento__lte=p_data_fim) &
                                                      Q(codg_tipo_doc_partct_calc=p_tipo_documento))
            linhas_excluidas = obj_retorno.count()
            obj_retorno.delete()
            return linhas_excluidas

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def select_inscricao_pelo_cnpj(self, cnpjs):
        etapaProcess = f"class {self.__class__.__name__} - def select_inscricao_pelo_cnpj."
        # loga_mensagem(etapaProcess)

        max_lote = 1000
        dfs = []

        try:
            for i in range(0, len(cnpjs), max_lote):
                batch_ids = cnpjs[i:i + max_lote]
                lista_cnpjs = ', '.join(map(str, batch_ids))

                etapaProcess += f"Executa consulta que busca a Inscrição Estadual pelo CNPJ do contribuinte."
                sql = f"""Select Contrib.NUMR_INSCRICAO
                               , PJ.NUMR_CNPJ
                            From CCE_CONTRIBUINTE                Contrib
                                  Inner Join GEN_PESSOA_JURIDICA PJ      On Contrib.ID_PESSOA = PJ.ID_PESSOA
                           Where PJ.NUMR_CNPJ In ({lista_cnpjs})"""

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

    def select_nfe_simples(self, p_tipo_consulta, p_data_inicio=str, p_data_fim=str) -> pd.DataFrame:
            etapaProcess = f"class {self.__class__.__name__} - class select_nfe_simples - {p_data_inicio} a {p_data_fim}"
            # loga_mensagem(etapaProcess)

            try:
                etapaProcess = f"Executa query de busca de informações da NFe de contribuintes do Simples Nacional no período de {p_data_inicio} a {p_data_fim}"

                if p_tipo_consulta == '1':
                    sql = f"""    SELECT 'NFe.3.1.g'
                                       , CASE WHEN NFe.TIPO_DOCUMENTO_FISCAL = 0 THEN 'Entrada' ELSE 'Saída' END AS Tipo_Nota 
                                       , NFe.CODG_CHAVE_ACESSO_NFE
                                       , Doc.NUMR_REFERENCIA_DOCUMENTO                              AS NUMR_REFERENCIA
                                       , NFe.NUMR_CNPJ_EMISSOR                                      AS CNPJ
                                       , NFe.NUMR_INSCRICAO                                         AS IE_NFe
                                       , Munic.CODG_MUNICIPIO                                       AS CODG_MUNICIPIO_NFe
                                       , NVL(IE.NUMR_INSCRICAO_CONTRIB, 0)                          AS IE_CAD
                                       , NVL(IE.CODG_MUNICIPIO, 0)                                  AS CODG_MUNICIPIO_CAD
                                       , NVL(Simples.NUMR_INSCRICAO_SIMPLES, 0)                     As IE_Simples 
                                       , SUM(Item.VALR_ADICIONADO)                                  AS VALR_ADICIONADO
                                    FROM IPM_DOCUMENTO_PARTCT_CALC_IPM             Doc 
                                          Inner Join IPM_ITEM_DOCUMENTO            Item    ON   Doc.CODG_DOCUMENTO_PARTCT_CALCULO     = Item.CODG_DOCUMENTO_PARTCT_CALCULO
                                                                                           AND  Doc.CODG_TIPO_DOC_PARTCT_CALC         = Item.CODG_TIPO_DOC_PARTCT_DOCUMENTO
                                          Inner Join NFE_IDENTIFICACAO             NFe     ON   Doc.CODG_DOCUMENTO_PARTCT_CALCULO     = NFe.ID_NFE
                                          INNER JOIN GEN_MUNICIPIO                 Munic   ON   NFe.CODG_MUNICIPIO_EMISSOR            = Munic.CODG_IBGE
                                          Left  Join IPM_OPCAO_CONTRIB_SIMPL_SIMEI Simples ON   SUBSTR(NFe.NUMR_CNPJ_EMISSOR, 1, 8)   = Simples.NUMR_CNPJ_BASE_SIMPLES
                                                                                           AND  Simples.INDI_VALIDADE_INFORMACAO      = 'S'
                                                                                           AND  NFe.DATA_EMISSAO_NFE                 >= Simples.DATA_INICIO_OPCAO_SIMPLES
                                                                                           AND (NFe.DATA_EMISSAO_NFE                 <= Simples.DATA_FIM_OPCAO_SIMPLES
                                                                                           OR   Simples.DATA_FIM_OPCAO_SIMPLES       IS NULL) 
                                          Left  Join IPM_CONTRIBUINTE_IPM          IE      ON   NFe.NUMR_INSCRICAO                    = IE.NUMR_INSCRICAO_CONTRIB
                                                                                           AND  NFe.DATA_EMISSAO_NFE                 >= IE.DATA_INICIO_VIGENCIA
                                                                                           AND (NFe.DATA_EMISSAO_NFE                 <= IE.DATA_FIM_VIGENCIA
                                                                                           OR   IE.DATA_FIM_VIGENCIA                 IS NULL) 
                                   WHERE NVL(Simples.NUMR_CNPJ_BASE_SIMPLES, 0) > 0         -- Elimina quem não está do cadastro do Simples
                                     AND  NFe.NUMR_CNPJ_EMISSOR             IS NOT NULL
                                     AND  Doc.CODG_TIPO_DOC_PARTCT_CALC     IN (1, 10)
                                     AND (Doc.CODG_MOTIVO_EXCLUSAO_CALCULO IS NULL
                                      OR  Doc.CODG_MOTIVO_EXCLUSAO_CALCULO <> 10)
                                     AND  Item.CODG_MOTIVO_EXCLUSAO_CALCULO IS NULL
                                     AND  NFe.TIPO_DOCUMENTO_FISCAL          = 1
                                     AND  NVL(IE.TIPO_ENQDTO_FISCAL, 3)     >= 3 
                                     AND (IE.CODG_MUNICIPIO    = 53100
                                      OR  Munic.CODG_MUNICIPIO = 53100)
                                     AND Doc.NUMR_REFERENCIA_DOCUMENTO      Between 202301 AND 202312
                                     AND Item.CODG_CFOP IN (5101,5102,5103,5104,5105,5106,5109,5110,5111,5112,5113,5114,5115,5116,5117,5118,5119,5120,5122,
                                                            5123,5129,5251,5252,5253,5254,5255,5256,5257,5258,5401,5402,5403,5405,5651,5652,5653,5654,5655,5656,5667,6101,6102,6103,
                                                            6104,6105,6106,6107,6108,6109,6110,6111,6112,6113,6114,6115,6116,6117,6118,6119,6120,6122,6123,6129,6251,6252,6253,6254,
                                                            6255,6256,6257,6258,6401,6402,6403,6404,6651,6652,6653,6654,6655,6656,6667,7101,7102,7105,7106,7127,7129,7651,7654,7667)
                                     AND NFe.DATA_EMISSAO_NFE BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                                  AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')
                                   GROUP BY CASE WHEN NFe.TIPO_DOCUMENTO_FISCAL = 0 THEN 'Entrada' ELSE 'Saída' END
                                          , NFe.CODG_CHAVE_ACESSO_NFE
                                          , Doc.NUMR_REFERENCIA_DOCUMENTO
                                          , NFe.NUMR_CNPJ_EMISSOR
                                          , NFe.NUMR_INSCRICAO
                                          , Munic.CODG_MUNICIPIO
                                          , IE.NUMR_INSCRICAO_CONTRIB
                                          , IE.CODG_MUNICIPIO
                                          , Simples.NUMR_INSCRICAO_SIMPLES"""
                elif p_tipo_consulta == '2':
                    sql = f"""SELECT 'NFe.3.2.g'
                                   , CASE WHEN NFe.TIPO_DOCUMENTO_FISCAL = 0 THEN 'Entrada' ELSE 'Saída' END AS Tipo_Nota 
                                   , NFe.CODG_CHAVE_ACESSO_NFE
                                   , Doc.NUMR_REFERENCIA_DOCUMENTO                              AS NUMR_REFERENCIA
                                   , NFe.NUMR_CPF_CNPJ_DEST                                     AS CNPJ
                                   , NVL(NFe.NUMR_INSCRICAO_DEST, 0)                            AS IE_NFE
                                   , Munic.CODG_MUNICIPIO                                       AS CODG_MUNICIPIO_NFe
                                   , NVL(IE.NUMR_INSCRICAO_CONTRIB, 0)                          AS IE_CAD
                                   , NVL(IE.CODG_MUNICIPIO, 0)                                  AS CODG_MUNICIPIO_CAD
                              --     , Case When NVL(IE.TIPO_ENQDTO_FISCAL, 0)     >= 3
                              --            Then 'S'
                              --            Else 'N' End                                          As Indi_Simples_CAD 
                                   , NVL(Simples.NUMR_INSCRICAO_SIMPLES, 0)                     As IE_Simples 
                              --     , Case When NVL(Simples.NUMR_CNPJ_BASE_SIMPLES, 0) > 0
                              --            Then 'S'
                              --            Else 'N' End                                          As INDI_SIMPLES_SIMPLES
                              --     , (Item.VALR_ADICIONADO)                                  AS VALR_ADICIONADO
                                   , SUM(Item.VALR_ADICIONADO)                                  AS VALR_ADICIONADO
                                FROM IPM_DOCUMENTO_PARTCT_CALC_IPM             Doc 
                                      Inner Join IPM_ITEM_DOCUMENTO            Item    ON   Doc.CODG_DOCUMENTO_PARTCT_CALCULO     = Item.CODG_DOCUMENTO_PARTCT_CALCULO
                                                                                       AND  Doc.CODG_TIPO_DOC_PARTCT_CALC         = Item.CODG_TIPO_DOC_PARTCT_DOCUMENTO
                                      Inner Join NFE_IDENTIFICACAO             NFe     ON   Doc.CODG_DOCUMENTO_PARTCT_CALCULO     = NFe.ID_NFE
                                      INNER JOIN GEN_MUNICIPIO                 Munic   ON   NFe.CODG_MUNICIPIO_DEST               = Munic.CODG_IBGE
                                      Left  Join IPM_OPCAO_CONTRIB_SIMPL_SIMEI Simples ON   SUBSTR(NFe.NUMR_CPF_CNPJ_DEST, 1, 8)  = Simples.NUMR_CNPJ_BASE_SIMPLES
                                                                                       AND  Simples.INDI_VALIDADE_INFORMACAO      = 'S'
                                                                                       AND  NFe.DATA_EMISSAO_NFE                 >= Simples.DATA_INICIO_OPCAO_SIMPLES
                                                                                       AND (NFe.DATA_EMISSAO_NFE                 <= Simples.DATA_FIM_OPCAO_SIMPLES
                                                                                       OR   Simples.DATA_FIM_OPCAO_SIMPLES       IS NULL) 
                                      Left  Join IPM_CONTRIBUINTE_IPM          IE      ON   NFe.NUMR_INSCRICAO_DEST               = IE.NUMR_INSCRICAO_CONTRIB
                                                                                       AND  NFe.DATA_EMISSAO_NFE                 >= IE.DATA_INICIO_VIGENCIA
                                                                                       AND (NFe.DATA_EMISSAO_NFE                 <= IE.DATA_FIM_VIGENCIA
                                                                                       OR   IE.DATA_FIM_VIGENCIA                 IS NULL) 
                               WHERE NVL(Simples.NUMR_CNPJ_BASE_SIMPLES, 0) > 0         -- Elimina quem não está do cadastro do Simples
                              --   AND ROWNUM < 11
                              --   AND NFe.CODG_CHAVE_ACESSO_NFE In ('52240703647755001576550010000186601706758705') -- '52230102606820000157550010002315291003569059', '52230728402652000171550010000003001000023352')
                                 AND LENGTH(NFe.NUMR_CPF_CNPJ_DEST)     > 11            -- Elimina CPF
                                 AND Doc.CODG_TIPO_DOC_PARTCT_CALC     IN (1, 10)       -- Elimina NFe Recebida
                                 AND (Doc.CODG_MOTIVO_EXCLUSAO_CALCULO IS NULL          -- Elimina documento excluído 
                                  OR  Doc.CODG_MOTIVO_EXCLUSAO_CALCULO <> 10)
                                 AND Item.CODG_MOTIVO_EXCLUSAO_CALCULO IS NULL          -- Elimina item excluído
                                 AND NFe.TIPO_DOCUMENTO_FISCAL          = 0             -- Elimina NF-e Saída
                                 AND NVL(IE.TIPO_ENQDTO_FISCAL, 3)     >= 3             -- Elimina contribuinte que não é do Simples dentro do cadastro do sistema IPM
                                 AND (IE.CODG_MUNICIPIO                 = 53100         -- Elimina contribuinte que não está no município de estudo
                                  OR  Munic.CODG_MUNICIPIO              = 53100)
                                 AND Doc.NUMR_REFERENCIA_DOCUMENTO      Between 202401 AND 202412
                                 AND Item.CODG_CFOP IN (1101,1102,1111,1113,1116,1117,1118,1120,1121,1122,1251,1252,1253,
                                                        1254,1255,1256,1257,1401,1403,1651,1652,1653,2101,2102,2111,2113,2116,2117,2118,2120,2121,
                                                        2122,2251,2252,2253,2254,2255,2256,2257,2401,2403,2651,2652,2653,3101,3102,3127,3129,3652)
                                 AND NFe.DATA_EMISSAO_NFE BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
                                                              AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')
                               GROUP BY CASE WHEN NFe.TIPO_DOCUMENTO_FISCAL = 0 THEN 'Entrada' ELSE 'Saída' END
                                   , NFe.CODG_CHAVE_ACESSO_NFE
                                   , Doc.NUMR_REFERENCIA_DOCUMENTO
                                   , NFe.NUMR_CPF_CNPJ_DEST
                                   , NFe.NUMR_INSCRICAO_DEST
                                   , Munic.CODG_MUNICIPIO
                                   , IE.NUMR_INSCRICAO_CONTRIB
                                   , IE.CODG_MUNICIPIO
                                   , Simples.NUMR_INSCRICAO_SIMPLES"""
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

    def insert_id_conv115(self, df, chunk_size=10000):
        etapaProcess = f"class {self.__class__.__name__} - def insert_id_conv115."
        # loga_mensagem(etapaProcess)

        etapaProcess = f"Grava dados na tabela IPM_CONVERSAO_CONV_115"

        lista_colunas = []
        lista_ids = []  # Lista para armazenar os IDs gerados
        total_inserido = 0

        sql_insert = f"""INSERT INTO IPM_CONVERSAO_CONV_115 (NUMR_INSCRICAO
                                                          ,  NUMR_DOCUMENTO_FISCAL
                                                          ,  DATA_EMISSAO_DOCUMENTO
                                                          ,  DESC_SERIE_DOCUMENTO_FISCAL
                                                          ,  CODG_SITUACAO_VERSAO_ARQUIVO)
                                                     VALUES (%s, %s, %s, %s, %s)"""

        try:
            # Obtém o maior ID antes da inserção
            with connections[self.db_alias].cursor() as cursor:
                cursor.execute("SELECT MAX(ID_CONV_115) FROM IPM_CONVERSAO_CONV_115")
                result = cursor.fetchone()  # Obtém o resultado da contagem
                max_id_conv115 = result[0] if result else 0

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
                        connections[self.db_alias].commit()
                        total_inserido += len(lista_colunas)
                        lista_colunas = []

                # Insere os dados restantes
                if lista_colunas:
                    cursor.executemany(sql_insert, lista_colunas)
                    connections[self.db_alias].commit()

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




    # def delete_item_doc(self, p_lista_id_doc, p_tipo_documento):
    #     etapaProcess = f"class {self.__class__.__name__} - def delete_item_doc - Tipo de documento: {p_tipo_documento}"
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"exclui dados na tabela IPM_ITEM_DOCUMENTO para uma relação de documentos fornecidos."
    #
    #         linhas_excluidas = 0
    #         tamanho_lote = 20000
    #         lotes = [p_lista_id_doc[i:i + tamanho_lote] for i in range(0, len(p_lista_id_doc), tamanho_lote)]
    #         dfs = []
    #
    #         # Itere sobre cada lote e execute a consulta ao banco de dados
    #         for lote in lotes:
    #             obj_retorno = ItemDoc.objects.using(self.db_alias)\
    #                                          .filter(Q(codg_documento_partct_calculo__in=lote) &
    #                                                  Q(codg_tipo_doc_partct_documento=p_tipo_documento))
    #             linhas_excluidas += obj_retorno.count()
    #             obj_retorno.delete()
    #
    #         return linhas_excluidas
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    # def insert_doc_partct(self, df):
    #     etapaProcess = f"class {self.__class__.__name__} - def insert_doc_partct."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"Grava dados na tabela IPM_DOCUMENTO_PARTCT_CALCULO"
    #         tamanho_lote = 10000
    #         numr_lotes = math.ceil(len(df) / tamanho_lote)
    #
    #         for i in range(numr_lotes):
    #             batch_df = df.iloc[i * tamanho_lote: (i + 1) * tamanho_lote]
    #
    #             records = batch_df.to_dict(orient='records')
    #             with transaction.atomic():
    #                 obj_retorno = DocPartctCalculo.objects.using(self.db_alias).bulk_create([
    #                     DocPartctCalculo(**record) for record in records
    #                 ], batch_size=10000)
    #
    #         # Retornar o número total de registros inseridos
    #         return len(obj_retorno)
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    # def insert_historico_contrib(self, df_cce=None):
    #     etapaProcess = f"class {self.__class__.__name__} - def insert_historico_contrib."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"Grava dados na tabela IPM_HISTORICO_CONTRIBUINTE"
    #         records = df_cce.to_dict(orient='records')
    #         with transaction.atomic():
    #             # Use bulk_create para inserir os registros no banco de dados
    #             CadastroCCEOracle.objects.using(self.db_alias).bulk_create([
    #                 CadastroCCEOracle(**record) for record in records
    #             ], batch_size=10000)
    #
    #         return None
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    #
    # def select_nfe_item_periodo_raw(self, pAnoMesDiaIni: Optional[datetime] = None,
    #                                       pAnoMesDiaFim: Optional[datetime] = None) -> Any:
    #     etapaProcess = f"class {self.__class__.__name__} - class select_nfe_item_periodo_raw - {pAnoMesDiaIni} a {pAnoMesDiaFim}"
    #     # loga_mensagem(etapaProcess)
    #
    #     db_alias = 'oraprd'
    #
    #     try:
    #         etapaProcess = f"Executa query de busca de informações da NFe no período de {pAnoMesDiaIni} a {pAnoMesDiaFim}"
    #         sql = f"""SELECT Ident.ID_NFE                                           AS ID_NFE
    #                        , Ident.CODG_CHAVE_ACESSO_NFE                            AS CHAVE_NFE
    #                        , Ident.NUMR_CNPJ_EMISSOR                                AS NUMR_CNPJ_REMET
    #                        , Ident.NUMR_CPF_EMISSOR                                 AS NUMR_CPF_REMET
    #                        , Ident.NUMR_CPF_CNPJ_DEST                               AS NUMR_CNPJ_DEST
    #                        , '0'                                                    AS NUMR_CPF_DEST
    #                        , Ident.DESC_NATUREZA_OPERACAO                           AS DESC_NATUREZA_OPERACAO
    #                        , Ident.CODG_MODELO_NFE                                  AS CODG_MODELO_NFE
    #                        , TO_NUMBER(TO_CHAR(Ident.DATA_EMISSAO_NFE, 'yyyymmdd')) AS NUMR_ANO_MES_DIA_EMISSAO
    #                        , TO_NUMBER(TO_CHAR(DATA_EMISSAO_NFE, 'YYYYMM'))         AS NUMR_REF_EMISSAO
    #                        , CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0
    #                               THEN 'E'
    #                               ELSE 'S' END                                      AS INDI_TIPO_OPERACAO
    #                        , CASE Ident.INDI_NOTA_EXPORTACAO
    #                               WHEN 'S' THEN 'Operação com exterior'
    #                               ELSE          'Operação interna'  END             AS DESC_DESTINO_OPERACAO
    #                        , CASE Ident.TIPO_FINALIDADE_NFE
    #                               WHEN 1 THEN 'NF-e normal'
    #                               WHEN 2 THEN 'NF-e complementar'
    #                               WHEN 3 THEN 'NF-e de ajuste'
    #                               WHEN 4 THEN 'Devolução de mercadoria'
    #                               ELSE        'Finalidade não identificada' END     AS DESC_FINALIDADE_OPERACAO
    #                        , TO_CHAR(Ident.NUMR_INSCRICAO)                          AS IE_SAIDA
    #                        , TO_CHAR(Ident.NUMR_INSCRICAO_DEST)                     AS IE_ENTRADA
    #                        , Ident.VALR_NOTA_FISCAL                                 AS VALR_TOT_NF
    #                        , Ident.VALR_ICMS_SUBSTRIB                               AS VALR_TOT_SUBST_TRIB
    #                        , Item.NUMR_ITEM                                         AS NUMR_ITEM
    #                        , Item.CODG_CFOP                                         AS NUMR_CFOP
    #                        , Item.CODG_EAN                                          AS NUMR_GTIN
    #                        , Item.CODG_CEST                                         AS NUMR_CEST
    #                        , Item.CODG_PRODUTO_NCM                                  AS NUMR_NCM
    #                        , COALESCE(Item.CODG_PRODUTO_ANP, 0)                     AS CODG_ANP
    #                        , COALESCE(Item.VALR_TOTAL_BRUTO, 0)                     AS VALR_PRODUTO
    #                        , COALESCE(Item.VALR_DESCONTO, 0)                        AS VALR_DESCONTO
    #                        , COALESCE(Item.VALR_FRETE, 0)                           AS VALR_FRETE
    #                        , COALESCE(Item.VALR_SEGURO, 0)                          AS VALR_SEGURO
    #                        , COALESCE(Item.VALR_OUTRAS_DESPESAS, 0)                 AS VALR_OUTROS
    #                        , COALESCE(Item.VALR_ICMS_DESONERA, 0)                   AS VALR_ICMS_DESON
    #                        , COALESCE(Item.VALR_ICMS_SUBTRIB, 0)                    AS VALR_ICMS_ST
    #                        , COALESCE(Item.VALR_IMPOSTO_IMPORTACAO, 0)              AS VALR_IMP_IMPORT
    #                        , COALESCE(Item.VALR_IPI, 0)                             AS VALR_IPI
    #                        , COALESCE(Item.VALR_TOTAL_BRUTO, 0)
    #                        - COALESCE(Item.VALR_DESCONTO, 0)
    #                        + COALESCE(Item.VALR_FRETE, 0)
    #                        + COALESCE(Item.VALR_SEGURO, 0)
    #                        + COALESCE(Item.VALR_OUTRAS_DESPESAS, 0)
    #                        + COALESCE(Item.VALR_IPI, 0)                             AS VALR_FINAL_PRODUTO
    #                        , COALESCE(Item.VALR_TOTAL_BRUTO, 0)
    #                        - COALESCE(Item.VALR_DESCONTO, 0)
    #                        + COALESCE(Item.VALR_FRETE, 0)
    #                        + COALESCE(Item.VALR_SEGURO, 0)
    #                        + COALESCE(Item.VALR_OUTRAS_DESPESAS, 0)
    #                        + COALESCE(Item.VALR_IPI, 0)
    #                        - COALESCE(Item.VALR_ICMS_DESONERA, 0)
    #                        + COALESCE(Item.VALR_ICMS_SUBTRIB, 0)
    #                        + COALESCE(Item.VALR_IMPOSTO_IMPORTACAO, 0)              AS VALR_VA
    #                        , NVL(NUMR_PROTOCOLO_CANCEL, '0')                        AS NUMR_PROTOCOLO_CANCEL
    #                        , Null                                                   AS DATA_CANCEL
    #                        , 'A'                                                    AS STAT_NOTA
    #                        , 'E'                                                    AS INDI_ORIGEM
    #                        , SYSDATE                                                AS DATA_CARGA
    #                     FROM NFE_IDENTIFICACAO                Ident
    #                           INNER JOIN NFE_ITEM_NOTA_FISCAL Item  ON Ident.ID_NFE = Item.ID_NFE
    #                    WHERE 1=1
    #                      --AND Ident.CODG_CHAVE_ACESSO_NFE = '52230101540533000129650020032520390000821559'
    #                      AND Ident.DATA_EMISSAO_NFE BETWEEN TO_DATE('{pAnoMesDiaIni}', 'YYYY-MM-DD HH24:MI:SS')
    #                                                     AND TO_DATE('{pAnoMesDiaFim}', 'YYYY-MM-DD HH24:MI:SS')"""
    #
    #         # Usa RawQuerySet para executar a consulta
    #         obj_retorno = Nfe.objects.using(db_alias).raw(sql)
    #
    #         # Recupera as colunas do modelo (NFe)
    #         colunas = [campo.name for campo in Nfe._meta.fields]
    #
    #         # Carrega manualmente o DataFrame
    #         dados = [dict(zip(colunas, [getattr(result, campo) for campo in colunas])) for result in obj_retorno]
    #         df_ret = pd.DataFrame(dados)
    #
    #         if df_ret.shape[0] > 0:
    #             etapaProcess += f' - Retornou {df_ret.shape[0]} linhas.'
    #             loga_mensagem(etapaProcess)
    #             return df_ret
    #
    #         else:
    #             etapaProcess += f' - Não retornou nenhuma Nfe!'
    #             loga_mensagem(etapaProcess)
    #             return None
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    # def select_nfe_item_periodo(self, pAnoMesDiaIni: Optional[datetime] = None, pAnoMesDiaFim: Optional[datetime] = None) -> Optional[Any]:
    #     etapaProcess = f"class {self.__class__.__name__} - class select_nfe_item_periodo - {pAnoMesDiaIni} a {pAnoMesDiaFim}"
    #     # loga_mensagem(etapaProcess)
    #
    #     db_alias = 'oraprd'
    #
    #     try:
    #         etapaProcess = f"Executa query de busca de informações da NFe no período de {pAnoMesDiaIni} a {pAnoMesDiaFim}"
    #         objeto_nfe = Nfe.objects.using(db_alias).filter(data_emissao_nfe__range=(pAnoMesDiaIni, pAnoMesDiaFim))
    #
    #         if objeto_nfe.exists():
    #             etapaProcess += f' - Retornadas {objeto_nfe.count()} linhas.'
    #             loga_mensagem(etapaProcess)
    #
    #             # Agora você pode acessar os objetos relacionados usando o nome do campo da chave estrangeira
    #             etapaProcess = f"Executa query de busca de informações dos Itens da NFe no período de {pAnoMesDiaIni} a {pAnoMesDiaFim}"
    #             objeto_item_nfe = ItemNfe.objects.using('oraprd').filter(id_nfe=objeto_nfe)
    #
    #             if objeto_item_nfe.exists():
    #                 etapaProcess += f' - Retornadas {objeto_item_nfe.count()} linhas.'
    #                 loga_mensagem(etapaProcess)
    #                 return objeto_nfe
    #             else:
    #                 etapaProcess += f' - Não retornou nenhum item de Nfe!'
    #                 loga_mensagem(etapaProcess)
    #                 return None
    #
    #         else:
    #             etapaProcess += f' - Não retornou nenhuma Nfe!'
    #             loga_mensagem(etapaProcess)
    #             return None
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    # def select_nfe_periodo_mysql(pAnoMesDiaIni: Optional[datetime] = None, pAnoMesDiaFim: Optional[datetime] = None) -> Optional[Any]:
    #     etapaProcess = f"class {__class__.__name__} - class select_nfe_periodo_mysql - {pAnoMesDiaIni} a {pAnoMesDiaFim}"
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"Executa query de busca de informações da NFe no período de {pAnoMesDiaIni} a {pAnoMesDiaFim}"
    #         #objeto_nfe = Nfe.objects.using('oraprd').filter(data_emissao_nfe__range=(p_data_inicio, p_data_fim))
    #         objeto_nfe = Nfe.objects.using('oraprd').filter(data_emissao_nfe=(pAnoMesDiaIni))
    #
    #         if objeto_nfe.exists():
    #             etapaProcess += f' - Retornadas {objeto_nfe.count()} linhas.'
    #             loga_mensagem(etapaProcess)
    #
    #             return objeto_nfe
    #
    #         else:
    #             etapaProcess += f' - Não retornou nenhuma Nfe!'
    #             loga_mensagem(etapaProcess)
    #             return None
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    # def select_item_nfe(self, objeto_nfe: Optional[Any] = None) -> Optional[Any]:
    #     etapaProcess = f"class {self.__class__.__name__} - class select_item_nfe"
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"Executa query de busca de informações dos Itens da NFe"
    #         objeto_item_nfe = ItemNfe.objects.using('oraprd').filter(id_nfe=objeto_nfe)
    #
    #         if objeto_item_nfe.exists():
    #             etapaProcess += f' - Retornadas {objeto_item_nfe.count()} linhas.'
    #             loga_mensagem(etapaProcess)
    #             return objeto_nfe
    #
    #         else:
    #             etapaProcess += f' - Não retornou nenhum item de Nfe!'
    #             loga_mensagem(etapaProcess)
    #             return None
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    # def select_nfe_item_periodo_conn(self, p_data_inicio=str, p_data_fim=str) -> pd.DataFrame:
    #     etapaProcess = f"class {self.__class__.__name__} - class select_nfe_item_periodo_conn - {p_data_inicio} a {p_data_fim}"
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"Executa query de busca de informações da NFe no período de {p_data_inicio} a {p_data_fim}"
    #         sql = f"""SELECT Ident.ID_NFE                                           AS ID_NFE
    #                        , Ident.CODG_CHAVE_ACESSO_NFE                            AS CHAVE_NFE
    #                        , Ident.NUMR_CNPJ_EMISSOR                                AS NUMR_CNPJ_REMET
    #                        , Ident.NUMR_CPF_EMISSOR                                 AS NUMR_CPF_REMET
    #                        , Ident.NUMR_CPF_CNPJ_DEST                               AS NUMR_CNPJ_DEST
    #                        , '0'                                                    AS NUMR_CPF_DEST
    #                        , Ident.DESC_NATUREZA_OPERACAO                           AS DESC_NATUREZA_OPERACAO
    #                        , Ident.CODG_MODELO_NFE                                  AS CODG_MODELO_NFE
    #                        , Ident.DATA_EMISSAO_NFE                                 AS DATA_EMISSAO
    #                        , TO_NUMBER(TO_CHAR(DATA_EMISSAO_NFE, 'YYYYMM'))         AS NUMR_REF_EMISSAO
    #                        , CASE WHEN Ident.TIPO_DOCUMENTO_FISCAL = 0
    #                               THEN 'E'
    #                               ELSE 'S' END                                      AS INDI_TIPO_OPERACAO
    #                        , CASE Ident.INDI_NOTA_EXPORTACAO
    #                               WHEN 'S' THEN 'Operação com exterior'
    #                               ELSE          'Operação interna'  END             AS DESC_DESTINO_OPERACAO
    #                        , CASE Ident.TIPO_FINALIDADE_NFE
    #                               WHEN 1 THEN 'NF-e normal'
    #                               WHEN 2 THEN 'NF-e complementar'
    #                               WHEN 3 THEN 'NF-e de ajuste'
    #                               WHEN 4 THEN 'Devolução de mercadoria'
    #                               ELSE        'Finalidade não identificada' END     AS DESC_FINALIDADE_OPERACAO
    #                        , TO_CHAR(Ident.NUMR_INSCRICAO)                          AS IE_SAIDA
    #                        , TO_CHAR(Ident.NUMR_INSCRICAO_DEST)                     AS IE_ENTRADA
    #                        , Ident.VALR_NOTA_FISCAL                                 AS VALR_TOT_NF
    #                        , Ident.VALR_ICMS_SUBSTRIB                               AS VALR_TOT_SUBST_TRIB
    #                        , Item.NUMR_ITEM                                         AS NUMR_ITEM
    #                        , Item.CODG_CFOP                                         AS NUMR_CFOP
    #                        , Item.CODG_EAN                                          AS NUMR_GTIN
    #                        , Item.CODG_CEST                                         AS NUMR_CEST
    #                        , Item.CODG_PRODUTO_NCM                                  AS NUMR_NCM
    #                        , COALESCE(Item.CODG_PRODUTO_ANP, 0)                     AS CODG_ANP
    #                        , COALESCE(Item.VALR_TOTAL_BRUTO, 0)                     AS VALR_PRODUTO
    #                        , COALESCE(Item.VALR_DESCONTO, 0)                        AS VALR_DESCONTO
    #                        , COALESCE(Item.VALR_FRETE, 0)                           AS VALR_FRETE
    #                        , COALESCE(Item.VALR_SEGURO, 0)                          AS VALR_SEGURO
    #                        , COALESCE(Item.VALR_OUTRAS_DESPESAS, 0)                 AS VALR_OUTROS
    #                        , COALESCE(Item.VALR_ICMS_DESONERA, 0)                   AS VALR_ICMS_DESON
    #                        , COALESCE(Item.VALR_ICMS_SUBTRIB, 0)                    AS VALR_ICMS_ST
    #                        , COALESCE(Item.VALR_IMPOSTO_IMPORTACAO, 0)              AS VALR_IMP_IMPORT
    #                        , COALESCE(Item.VALR_IPI, 0)                             AS VALR_IPI
    #                        , COALESCE(Item.VALR_TOTAL_BRUTO, 0)
    #                        - COALESCE(Item.VALR_DESCONTO, 0)
    #                        + COALESCE(Item.VALR_FRETE, 0)
    #                        + COALESCE(Item.VALR_SEGURO, 0)
    #                        + COALESCE(Item.VALR_OUTRAS_DESPESAS, 0)
    #                        + COALESCE(Item.VALR_IPI, 0)                             AS VALR_FINAL_PRODUTO
    #                        , COALESCE(Item.VALR_TOTAL_BRUTO, 0)
    #                        - COALESCE(Item.VALR_DESCONTO, 0)
    #                        + COALESCE(Item.VALR_FRETE, 0)
    #                        + COALESCE(Item.VALR_SEGURO, 0)
    #                        + COALESCE(Item.VALR_OUTRAS_DESPESAS, 0)
    #                        + COALESCE(Item.VALR_IPI, 0)
    #                        - COALESCE(Item.VALR_ICMS_DESONERA, 0)
    #                        + COALESCE(Item.VALR_ICMS_SUBTRIB, 0)
    #                        + COALESCE(Item.VALR_IMPOSTO_IMPORTACAO, 0)              AS VALR_VA
    #                        , NVL(NUMR_PROTOCOLO_CANCEL, '0')                        AS NUMR_PROTOCOLO_CANCEL
    #                        , Null                                                   AS DATA_CANCEL
    #                        , 'A'                                                    AS STAT_NOTA
    #                        , 'E'                                                    AS INDI_ORIGEM
    #                        , SYSDATE                                                AS DATA_CARGA
    #                     FROM NFE_IDENTIFICACAO                Ident
    #                           INNER JOIN NFE_ITEM_NOTA_FISCAL Item  ON Ident.ID_NFE = Item.ID_NFE
    #                    WHERE 1=1
    #                      --AND Ident.CODG_CHAVE_ACESSO_NFE = '52230101540533000129650020032520399790821559'
    #                      AND Ident.DATA_EMISSAO_NFE BETWEEN TO_DATE('{p_data_inicio}', 'YYYY-MM-DD HH24:MI:SS')
    #                                                     AND TO_DATE('{p_data_fim}', 'YYYY-MM-DD HH24:MI:SS')"""
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
    #
    # def insert_parametros(self, parametros):
    #     etapaProcess = f"class {self.__class__.__name__} - def insert_parametros."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"Carga inicial da tabela."
    #         Parametro.objects.bulk_create(parametros)
    #         return None
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    # def select_inscricoes_nfe(self, p_periodo=int, p_emit=str):
    #     etapaProcess = f"class {self.__class__.__name__} - def select_inscricoes_nfe para o periodo de {p_periodo}."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         if p_emit == 'E':
    #             sql = f"""Select Distinct NUMR_INSCRICAO
    #                         From NFE_IDENTIFICACAO
    #                        Where NUMR_INSCRICAO         Is Not Null
    #                          And NUMR_INSCRICAO          > 0
    #                          And LENGTH(NUMR_INSCRICAO) <= 9
    #                          And TO_CHAR(NFE_IDENTIFICACAO.DATA_EMISSAO_NFE, 'YYYYMM') = '{p_periodo}'
    #                          --And NUMR_INSCRICAO In (100276890, 100267904)
    #                    """
    #         else:
    #             sql = f"""Select Distinct NUMR_INSCRICAO_DEST As NUMR_INSCRICAO
    #                         From NFE_IDENTIFICACAO
    #                        Where NUMR_INSCRICAO_DEST         Is Not Null
    #                          And NUMR_INSCRICAO_DEST          > 0
    #                          And LENGTH(NUMR_INSCRICAO_DEST) <= 9
    #                          And TO_CHAR(NFE_IDENTIFICACAO.DATA_EMISSAO_NFE, 'YYYYMM') = '{p_periodo}'
    #                          --And NUMR_INSCRICAO_DEST In (100276890, 100267904)
    #                    """
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
    # def select_inscricoes_nfe_recebida(self, p_periodo=int, p_emit=str):
    #     etapaProcess = f"class {self.__class__.__name__} - def select_inscricoes_nfe_recebida para o periodo de {p_periodo}."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         if p_emit == 'E':
    #             sql = f"""Select Distinct NUMR_INSCRICAO_EMITENTE As NUMR_INSCRICAO
    #                         From NFE_IDENTIFICACAO_RECEBIDA
    #                        Where NUMR_INSCRICAO_EMITENTE         Is Not Null
    #                          And NUMR_INSCRICAO_EMITENTE          > 0
    #                          And LENGTH(NUMR_INSCRICAO_EMITENTE) <= 9
    #                          And TO_CHAR(DATA_EMISSAO_NFE, 'YYYYMM') = '{p_periodo}'
    #                          --And NUMR_INSCRICAO In (100276890, 100267904)
    #                    """
    #         else:
    #             sql = f"""Select Distinct NUMR_INSCRICAO_DEST As NUMR_INSCRICAO
    #                         From NFE_IDENTIFICACAO_RECEBIDA
    #                        Where NUMR_INSCRICAO_DEST         Is Not Null
    #                          And NUMR_INSCRICAO_DEST          > 0
    #                          And LENGTH(NUMR_INSCRICAO_DEST) <= 9
    #                          And TO_CHAR(DATA_EMISSAO_NFE, 'YYYYMM') = '{p_periodo}'
    #                          --And NUMR_INSCRICAO_DEST In (100276890, 100267904)
    #                    """
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
    # def select_inscricoes_efd(self, p_periodo=int):
    #     etapaProcess = f"class {self.__class__.__name__} - def select_inscricoes_efd para o periodo de {p_periodo}."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         sql = f"""Select Distinct Arq.NUMR_INSCRICAO
    #                     From EFD_ARQUIVO                      Arq
    #                           Inner Join EFD_NOTA_FISCAL      NFe   On Arq.ID_ARQUIVO     = NFe.ID_ARQUIVO
    #                           Inner Join EFD_ITEM_NOTA_FISCAL Item  On NFe.ID_NOTA_FISCAL = Item.ID_NOTA_FISCAL
    #                    Where 1=1
    #                      And NFe.CODG_CHAVE_ACESSO_NFE          Is Not Null
    #                      And Arq.REFE_ARQUIVO                    = {p_periodo}
    #                      And NFe.TIPO_OPERACAO                   = 0                                                               -- NFe de Entrada --
    #                      And NFe.CODG_SITUACAO_DOCUMENTO_FISCAL In (0, 1, 6, 7, 8)                                                 -- Situação do Documento Fiscal (NF-e) --
    #                      And Item.CODG_CFOP                     In (1406, 1407, 1551, 1556, 2406, 2407, 2551, 2556, 3551, 3556)    -- CFOPs de ativo imobilizado e materiais para uso e consumo --
    #                      And Arq.ID_ARQUIVO In (Select Max(Arq1.ID_ARQUIVO)
    #                                               From EFD_ARQUIVO Arq1
    #                                              Where Arq1.NUMR_INSCRICAO       = Arq.NUMR_INSCRICAO
    #                                                And Arq1.REFE_ARQUIVO         = Arq.REFE_ARQUIVO
    #                                                And Arq1.REFE_ARQUIVO         = {p_periodo}
    #                                                And Arq1.STAT_PROCESM_ARQUIVO = 'S')
    #                """
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
    # def select_inscricoes_nfa(self, p_periodo=int):
    #     etapaProcess = f"class {self.__class__.__name__} - def select_inscricoes_nfa para o periodo de {p_periodo}."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         sql = f"""SELECT (Select NUMR_INSCRICAO
    #                             From NFA.NFA_PARTE_ENVOLVIDA Envol
    #                            Where Envol.TIPO_ENVOLVIDO = 3
    #                              And Ident.NUMR_IDENTIF_NOTA = Envol.NUMR_IDENTIF_NOTA)    As numr_inscricao
    #                     FROM NFA.NFA_IDENTIF_NOTA                Ident
    #                           INNER JOIN NFA.NFA_ITEM_NOTA       Item    On   Ident.NUMR_IDENTIF_NOTA = Item.NUMR_IDENTIF_NOTA
    #                    WHERE 1=1
    #                      And Ident.CODG_NATUREZA_OPERACAO IN (101, 102, 103, 104, 111, 112, 113, 201, 202, 203, 204, 301, 302, 303, 401, 402, 403, 501, 502, 503)
    #                      And Ident.STAT_NOTA              In (4, 5)
    #                      And TO_NUMBER(TO_CHAR(Ident.DATA_HORA_EMISSAO, 'yyyymm')) = {p_periodo}
    #                   Union
    #                   SELECT (Select NUMR_INSCRICAO
    #                             From NFA.NFA_PARTE_ENVOLVIDA Envol
    #                            Where Envol.TIPO_ENVOLVIDO = 2
    #                              And Ident.NUMR_IDENTIF_NOTA = Envol.NUMR_IDENTIF_NOTA)    As numr_inscricao
    #                     FROM NFA.NFA_IDENTIF_NOTA                Ident
    #                           INNER JOIN NFA.NFA_ITEM_NOTA       Item    On   Ident.NUMR_IDENTIF_NOTA = Item.NUMR_IDENTIF_NOTA
    #                    WHERE 1=1
    #                      And Ident.CODG_NATUREZA_OPERACAO IN (101, 102, 103, 104, 111, 112, 113, 201, 202, 203, 204, 301, 302, 303, 401, 402, 403, 501, 502, 503)
    #                      And Ident.STAT_NOTA              In (4, 5)
    #                      And TO_NUMBER(TO_CHAR(Ident.DATA_HORA_EMISSAO, 'yyyymm')) = {p_periodo}
    #                    """
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
    #
'''
all(): Retorna todos os objetos na tabela.
objetos = MeuModelo.objects.all()

filter(**kwargs): Retorna um conjunto de objetos que correspondem aos parâmetros fornecidos.
resultados = MeuModelo.objects.filter(nome='algum_nome')

get(**kwargs): Retorna um único objeto que corresponde aos parâmetros fornecidos.
               Se nenhum objeto ou mais de um objeto for encontrado, levanta uma exceção DoesNotExist ou MultipleObjectsReturned.
resultado = MeuModelo.objects.get(nome='algum_nome')

exclude(**kwargs): Retorna um conjunto de objetos que não correspondem aos parâmetros fornecidos.
resultados = MeuModelo.objects.exclude(nome='algum_nome')

first(): Retorna o primeiro objeto na tabela.
primeiro_objeto = MeuModelo.objects.first()

last(): Retorna o último objeto na tabela.
ultimo_objeto = MeuModelo.objects.last()

values(*fields): Retorna um QuerySet que contém dicionários para cada objeto, onde cada dicionário contém os valores dos campos especificados.
valores = MeuModelo.objects.values('campo1', 'campo2')


values_list(*fields, flat=False): Similar a values(), mas retorna uma lista de tuplas.
valores_lista = MeuModelo.objects.values_list('campo1', 'campo2')

count(): Retorna o número de objetos na tabela que correspondem à condição.
total = MeuModelo.objects.count()

exists(): Retorna True se pelo menos um objeto que atende à condição existir na tabela, senão, retorna False.
existe = MeuModelo.objects.filter(nome='algum_nome').exists()

save(force_insert=False, force_update=False, using=None, update_fields=None): Salva ou atualiza o objeto no banco de dados.
    force_insert: Se True, força a criação de um novo registro, mesmo que um objeto com a mesma chave primária já exista.
    force_update: Se True, força a atualização de um registro existente.
    using: Especifica o alias do banco de dados a ser usado.
    update_fields: Lista de campos a serem atualizados.
meu_objeto = MeuModelo(campo1='valor1', campo2='valor2')
meu_objeto.save()

create(**kwargs): Cria e salva um novo objeto em uma única chamada.
MeuModelo.objects.create(campo1='valor1', campo2='valor2')

update(**kwargs): Atualiza todos os objetos que atendem à condição fornecida.
MeuModelo.objects.filter(campo1='valor_antigo').update(campo1='valor_novo')

delete(): Exclui o objeto do banco de dados.
meu_objeto = MeuModelo.objects.get(campo1='valor_antigo')
meu_objeto.delete()

bulk_create(objs, batch_size=None, ignore_conflicts=False): Cria vários objetos em uma única chamada eficiente.
objetos = [MeuModelo(campo1='valor1'), MeuModelo(campo1='valor2')]
MeuModelo.objects.bulk_create(objetos)

bulk_update(objs, fields, batch_size=None): Atualiza vários objetos em uma única chamada eficiente.
objetos = MeuModelo.objects.filter(campo1='valor_antigo')
for obj in objetos:
    obj.campo1 = 'valor_novo'
MeuModelo.objects.bulk_update(objetos, ['campo1'])

'''