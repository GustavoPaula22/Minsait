import pandas as pd
from django.db import connections

from funcoes.utilitarios import loga_mensagem_erro


class MySQL:
    etapaProcess = f"class {__name__} - class Mysql."
    # loga_mensagem(etapaProcess)

    db_alias = 'default'

    def select_nf3e(self, p_data_inicio=str, p_data_fim=str) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def select_nf3e - {p_data_inicio} a {p_data_fim}"
        # loga_mensagem(etapaProcess)

        sql = f"""SELECT nf3e.id_documento                                  AS id_nf3e
                       , det.id_item_documento                              AS id_item_nota_fiscal
                       , CAST(nf3e.dest_ie AS INT)                          AS ie_entrada
                       , CAST(nf3e.emit_ie AS INT)                          AS ie_saida
                       , NVL(ide_cmunfg, 0)                                 AS codg_municipio_entrada
                       , NVL(nf3e.dest_cnpj, nf3e.dest_cpf)                 AS numr_cpf_cnpj_dest
                       , nf3e.ide_mod                                       AS codg_modelo_nf3e
                       , CONCAT(SUBSTR(NVL(ide_dhemi, 0), 1, 4),
                                SUBSTR(NVL(ide_dhemi, 0), 6, 2))            As numr_ref_emissao
                       , CAST(ide_dhemi AS DATE)                            AS data_emissao_nf3e
                       , det.nitem                                          As numr_item
                       , NVL(det.detitem_prod_cfop, 0)                      AS numr_cfop
                       , NVL(det.detitem_prod_vprod, 0)
                       - det.detitem_prod_xprod                             As valr_va
                       , SUBSTR(NVL(even.dhevento, 0), 1, 10)               AS data_cancelamento
                    FROM nf3e_infnf3e               nf3e
                          INNER JOIN nf3e_nfdet_det det  On nf3e.chnf3e = det.chnf3e
                          LEFT  JOIN nf3e_evento    even On nf3e.chnf3e = even.chnf3e
                   WHERE ide_dhemi BETWEEN STR_TO_DATE('{p_data_inicio}', '%Y-%m-%d %H:%i:%s')
                                       AND STR_TO_DATE('{p_data_fim}', '%Y-%m-%d %H:%i:%s')"""

        try:
            etapaProcess = f"Executa query da tabela NF3-e: {sql}"
            connections[self.db_alias].close()

            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                colunas = [col[0].lower() for col in cursor.description]
                dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]
            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)

    def select_bpe(self, p_data_inicio=str, p_data_fim=str) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def select_bpe - {p_data_inicio} a {p_data_fim}"
        # loga_mensagem(etapaProcess)

        sql = f"""SELECT bpe.id_documento                                    AS id_bpe
                       , CAST(emit_ie AS INTEGER)                            AS ie_saida
                       , bpe.ide_cmunini                                     AS codg_municipio_saida
                       , bpe.ide_cmunfim                                     AS codg_municipio_entrada
                       , 63                                                  AS codg_modelo_bpe
                       , CONCAT(SUBSTR(NVL(bpe.infpassagem_dhemb, 0), 1, 4),
                                SUBSTR(NVL(bpe.infpassagem_dhemb, 0), 6, 2)) AS numr_ref_emissao
                       , CAST(bpe.infpassagem_dhemb AS DATE)                 AS data_emissao_bpe
                       , bpe.infvalorbpe_vpgto                               AS valr_va
                       , evento.tpevento                                     AS codigo_evento
                    FROM bpe_infbpe bpe
                      left  Join bpe_evento evento On  bpe.id = evento.chave
                   Where 1=1
                     And SUBSTRING(bpe.infpassagem_dhemb, 1, 19) BETWEEN STR_TO_DATE('{p_data_inicio}', '%Y-%m-%d %H:%i:%s')
                                                                     AND STR_TO_DATE('{p_data_fim}', '%Y-%m-%d %H:%i:%s')"""

        try:
            etapaProcess = f"Executa query da tabela BP-e: {sql}"
            connections[self.db_alias].close()

            with connections[self.db_alias].cursor() as cursor:
                cursor.execute(sql)
                colunas = [col[0].lower() for col in cursor.description]
                dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]
            return pd.DataFrame(dados)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)

    # def update_parametro(self, nomeParametro, valorParametro):
    #     etapaProcess = f"class {self.__class__.__name__} - def update_parametro."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"Atualiza parametro {nomeParametro} do sistema: {valorParametro}."
    #         Parametro.objects.using(self.db_alias).filter(nome_parametro=nomeParametro).update(valr_parametro=valorParametro)
    #         return None
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    # def select_parametros(self) -> pd.DataFrame:
    #     etapaProcess = f"class {self.__class__.__name__} - def select_parametros"
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"Executa query de busca dos parâmetros do sistema IPM."
    #         obj_retorno = Parametro.objects.using(self.db_alias).values('nome_parametro', 'valr_parametro').all()
    #         df_ret = pd.DataFrame(list(obj_retorno))
    #
    #         return df_ret
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    # def select_parametro(self, pNomeParam) -> Parametro:
    #     etapaProcess = f"class {self.__class__.__name__} - def select_parametro - {pNomeParam}"
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"Executa query de busca do valor do parâmetro {pNomeParam}."
    #         dados = Parametro.objects.using(self.db_alias).filter(nome_parametro=pNomeParam)
    #
    #         # Crie instâncias do modelo usando os resultados do QuerySet
    #         parametros = [Parametro(nome_parametro=item.nome_parametro, valr_parametro=item.valr_parametro) for item in dados]
    #         return parametros
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
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
    # def delete_nfe_calc_nfe(self, pDataInicio, pDataFim):
    #     etapaProcess = f"class {self.__class__.__name__} - def delete_nfe_calc_nfe - {pDataInicio} a {pDataFim}"
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"Executa query de exclusão dos dados da tabela nfe_calc_nfe"
    #         obj_retorno = NfeCalcNfe.objects.using(self.db_alias).filter(Q(data_emissao__gte=pDataInicio)
    #                                                               & Q(data_emissao__lte=pDataFim))
    #         obj_retorno.delete()
    #
    #         return None
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    # def insert_nfe_calc_nfe(self, df_nfe):
    #     etapaProcess = f"class {self.__class__.__name__} - def insert_nfe_calc_nfe."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"Grava dados na tabela nfe_calc_nfe"
    #         records = df_nfe.to_dict(orient='records')
    #         with transaction.atomic():
    #             # Use bulk_create para inserir os registros no banco de dados
    #             NfeCalcNfe.objects.using(self.db_alias).bulk_create([
    #                 NfeCalcNfe(**record) for record in records
    #             ], batch_size=50000)
    #
    #         return None
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    # def update_nfe_calc_nfe(self, pDataInicio, pDataFim, pDataCalculo):
    #     etapaProcess = f"class {self.__class__.__name__} - def update_nfe_calc_nfe."
    #     # loga_mensagem(etapaProcess)
    #
    #     dataAtual = pDataCalculo
    #
    #     try:
    #         etapaProcess = f"Atualiza a data de cálculo na tabela nfe_calc_nfe"
    #         obj_retorno = NfeCalcNfe.objects.using(self.db_alias).filter(Q(data_emissao__gte=pDataInicio)
    #                                                               & Q(data_emissao__lte=pDataFim)
    #                                                               & Q(indi_cfop_particip=True)
    #                                                               & Q(numr_protocolo_cancel='0')).update(data_calc=dataAtual)
    #         return None
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    # def delete_nfe_calc_nfe_chave(self, pDataInicio, pDataFim):
    #     etapaProcess = f"class {self.__class__.__name__} - def delete_nfe_calc_nfe_chave - {pDataInicio} a {pDataFim}"
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"Executa query de exclusão dos dados da tabela nfe_calc_nfe_chave"
    #         obj_retorno = NfeCalcNfeChave.objects.using(self.db_alias).filter(Q(data_emissao__gte=pDataInicio)
    #                                                                    & Q(data_emissao__lte=pDataFim))
    #         obj_retorno.delete()
    #
    #         return None
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    # def insert_nfe_calc_nfe_chave(self, df_nfe_calc=None):
    #     etapaProcess = f"class {self.__class__.__name__} - def insert_nfe_calc_nfe_chave."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"Grava dados na tabela nfe_calc_nfe_chave"
    #         records = df_nfe_calc.to_dict(orient='records')
    #         with transaction.atomic():
    #             # Use bulk_create para inserir os registros no banco de dados
    #             NfeCalcNfeChave.objects.using(self.db_alias).bulk_create([
    #                 NfeCalcNfeChave(**record) for record in records
    #             ], batch_size=50000)
    #
    #         return None
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    # def select_inscricoes_periodo_nfe(self, p_inscricao, pPeriodo=int, p_qdade=int):
    #     etapaProcess = f"class {self.__class__.__name__} - def select_inscricoes_periodo_nfe para o periodo de {pPeriodo}, a partir da inscrição {p_inscricao}."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"Busca inscrições do período de {pPeriodo})."
    #         sql = f"""Select ie_saida As numr_inscricao
    #                     From nfe_calc_nfe_chave
    #                    Where ie_saida             Is Not NULL
    #                      And codg_municipio_saida  = 0
    #                      And LENGTH(ie_saida)      = 9
    #                      And numr_ref_emissao        = {pPeriodo}
    #                    Limit 300"""
    #
    #                   # Order By numr_inscricao
    #                   # Limit {p_qdade}
    #                   # And ie_entrada              > {p_inscricao}
    #
    #                   # Union
    #         sql = f"""Select ie_entrada As numr_inscricao
    #                     From nfe_calc_nfe_chave
    #                    Where ie_entrada             Is Not NULL
    #                      And ie_entrada              = '100194443'
    #                      And codg_municipio_entrada  > 0
    #                      And LENGTH(ie_entrada)      = 9
    #                      And numr_ref_emissao        = 202202
    #                    Order By numr_inscricao
    #                    Limit 100"""
    #
    #         sql = f"""Select 100194443 as numr_inscricao
    #                    Union
    #                   Select 100194443 as numr_inscricao
    #                    Order By numr_inscricao"""
    #
    #         # sql = 'Select 101488564 as numr_inscricao'
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
    # def select_inscricoes_periodo(self, p_periodo=int):
    #     etapaProcess = f"class {self.__class__.__name__} - def select_inscricoes_periodo para o periodo de {p_periodo}."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"Busca todas as inscrições do período de {p_periodo})."
    #         sql = f"""Select ie_saida         As numr_inscricao
    #                     From nfe_calc_nfe_chave
    #                    Where 1=1
    #                      And ie_saida         Is Not NULL
    #                      And ie_saida            <> '0'
    #                      And LENGTH(ie_saida)     = 9
    #                      And numr_ref_emissao     = {p_periodo}
    #                   Union
    #                   Select ie_entrada       As numr_inscricao
    #                     From nfe_calc_nfe_chave
    #                    Where 1=1
    #                      And ie_entrada       Is Not NULL
    #                      And ie_entrada          <> '0'
    #                      And LENGTH(ie_entrada)   = 9
    #                      And numr_ref_emissao     = {p_periodo}
    #                   Order By numr_inscricao"""
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
    # def insert_cadastro_cce(self, df_cce=None):
    #     etapaProcess = f"class {self.__class__.__name__} - def insert_cadastro_cce."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"Grava dados na tabela ipm_historico_cce"
    #         records = df_cce.to_dict(orient='records')
    #         with transaction.atomic():
    #             # Use bulk_create para inserir os registros no banco de dados
    #             CadastroCCE.objects.using(self.db_alias).bulk_create([
    #                 CadastroCCE(**record) for record in records
    #             ], batch_size=10000)
    #
    #         return None
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    # def update_cadastro_cce(self, pReferencia=int, pInscricao=str, pMunicipIBGE=int, pEnquadr=int, pSituacaoCad=int, pProdRural=int):
    #     etapaProcess = f"class {self.__class__.__name__} - def update_cadastro_cce para inscrição: {pInscricao}: {pMunicipIBGE} - {pEnquadr} - {pSituacaoCad} - {pProdRural}"
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"Atualiza dados cadastrais da inscrição estadual ({pInscricao})."
    #         obj_retorno = CadastroCCE.objects.using(self.db_alias).filter(Q(numr_inscricao=pInscricao)
    #                                                                     & Q(numr_ref_emissao=pReferencia)).update(codg_municipio_ibge=pMunicipIBGE,
    #                                                                                                               tipo_enqdto=pEnquadr,
    #                                                                                                               situacao_cadastral=pSituacaoCad,
    #                                                                                                               indi_prod_rural=pProdRural)
    #         return obj_retorno
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    # def update_cadastro_nfe_cce(self, pReferencia=int, pInscricao=str, pMunicip=int, pEnquadr=int, pSituacaoCad=int):
    #     etapaProcess = f"class {self.__class__.__name__} - def update_cadastro_nfe_cce para inscrição: {pInscricao}: {pMunicip} - {pEnquadr} - {pSituacaoCad}"
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"Atualiza dados cadastrais da inscrição estadual ({pInscricao})."
    #         obj_retorno = NfeCalcNfeChave.objects.using(self.db_alias).filter(Q(ie_entrada=pInscricao)
    #                                                                    & Q(numr_ref_emissao=pReferencia)).update(codg_municipio_entrada=pMunicip,
    #                                                                                                              tipo_enqdto_entrada=pEnquadr,
    #                                                                                                              situacao_cadastro_entrada=pSituacaoCad)
    #         obj_retorno += NfeCalcNfeChave.objects.using(self.db_alias).filter(Q(ie_saida=pInscricao)
    #                                                                     & Q(numr_ref_emissao=pReferencia)).update(codg_municipio_saida=pMunicip,
    #                                                                                                               tipo_enqdto_saida=pEnquadr,
    #                                                                                                               situacao_cadastro_saida=pSituacaoCad)
    #         return obj_retorno
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    # def update_cadastro_nfe_cnae(self, pInscricao=str, pReferencia=int, p_produtor_rural=int):
    #     etapaProcess = f"class {self.__class__.__name__} - def update_cadastro_nfe_cnae para inscrição: {pInscricao}: {pReferencia} - {p_produtor_rural}"
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"Atualiza dados cadastrais da inscrição estadual ({pInscricao})."
    #         obj_retorno = NfeCalcNfeChave.objects.using(self.db_alias).filter(Q(ie_entrada=pInscricao)
    #                                                                    & Q(numr_ref_emissao=pReferencia)).update(ind_prod_rural_entrada=p_produtor_rural)
    #         obj_retorno += NfeCalcNfeChave.objects.using(self.db_alias).filter(Q(ie_saida=pInscricao)
    #                                                                     & Q(numr_ref_emissao=pReferencia)).update(ind_prod_rural_saida=p_produtor_rural)
    #         return None
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    # def select_cfop_ipm(self) -> pd.DataFrame:
    #     etapaProcess = f"class {self.__class__.__name__} - def select_cfop_ipm"
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"Executa query de busca relação de CFOPs que participam do IPM."
    #         obj_retorno = CFOP.objects.using(self.db_alias).values('cfop').all()
    #         df_ret = pd.DataFrame(list(obj_retorno))
    #
    #         return df_ret
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise

    # def select_cnae_produtor(self) -> pd.DataFrame:
    #     etapaProcess = f"class {self.__class__.__name__} - def select_cnae_produtor"
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"Executa query de busca dos CNAEs de produtor rural"
    #         obj_retorno = CNAE.objects.using(self.db_alias).values('CODG_CNAE_RURAL').all()
    #         df_ret = pd.DataFrame(list(obj_retorno))
    #
    #         return df_ret
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #
    # def insert_processamento(self, processamento=Processamento) -> Processamento:
    #     etapaProcess = f"class {self.__class__.__name__} - def insert_processamento."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"Insere novo processamento"
    #         processamento.save(using=self.db_alias, force_insert=True, force_update=False)
    #         return Processamento.objects.using(self.db_alias).last()
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise
    #
    # def update_processamento(self, processamento=Processamento):
    #     etapaProcess = f"class {self.__class__.__name__} - def update_processamento."
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"Atualiza dados do processamento"
    #         processamento.save(using=self.db_alias, force_update=True, force_insert=False)
    #         return None
    #
    #         # sql = f'''Update MCC_PROCESM_MELHOR_COMPRA
    #         #              Set DESC_OBSERVACAO_PROCESM  = '{procesm.desc_observacao_procesm}'
    #         #            Where TIPO_MELHOR_COMPRA       = '{procesm.tipo_melhor_compra}'
    #         #              And ID_PROCESM_MELHOR_COMPRA = {procesm.id_procesm_melhor_compra}'''
    #
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #         raise







    # # Busca dados processados das tabelas #
    # def select_doc_processado(self, pTabela=str, pDataRef=str) -> pd.DataFrame:
    #     etapaProcess = f"class {self.__class__.__name__} - def select_doc_processado - {pDataRef}"
    #     # loga_mensagem(etapaProcess)
    #
    #     sql = f"""SELECT *
    #                 FROM {pTabela}
    #                WHERE numr_ano_mes_dia_emissao = '{pDataRef}'"""
    #
    #     try:
    #         etapaProcess = f"Executa query da tabela {pTabela} de documentos processados: {sql}"
    #         df = sessao.execute_sql_consulta(sql, params={})
    #         return df
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #
    # # Atualiza a data do cálculo da tabela do Conv-115 #
    # def update_data_calculo_conv115(self, pDataRef=str, pDataCalculo=str):
    #     etapaProcess = f"class {self.__class__.__name__} - def update_data_calculo_conv115"
    #     # loga_mensagem(etapaProcess)
    #
    #     sql = f"""Update nfe_calc_115_energia
    #                  Set data_calc = '{pDataCalculo}'
    #                Where 1=1
    #                  And cfop_particip            Is True
    #                  And indi_situacao_documento  <> 'S'
    #                  And numr_ano_mes_dia_emissao  = '{pDataRef}'"""
    #
    #     try:
    #         etapaProcess = f"Executa query de atualização da data de cálculo do Conv115: {sql}"
    #         self.Session.execute(sql)
    #         self.Session.commit()
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #
    # # Atualiza a data do cálculo da tabela de NFAs #
    # def update_data_calculo_nfa(self, pDataRef=str, pDataCalculo=str):
    #     etapaProcess = f"class {self.__class__.__name__} - def update_data_calculo_nfa"
    #     # loga_mensagem(etapaProcess)
    #
    #     sql = f"""Update nfe_calc_nfa
    #                  Set data_calc = '{pDataCalculo}'
    #                Where 1=1
    #                  And indi_cfop_particip       Is True
    #                  And indi_natur_oper_particip Is True
    #                  And indi_stat_nota_particip  Is True
    #                  And numr_ano_mes_dia_emissao = '{pDataRef}'"""
    #
    #     try:
    #         etapaProcess = f"Executa query de atualização da data de cálculo da NFAs: {sql}"
    #         self.sessionMySQL.execute(sql)
    #         self.sessionMySQL.commit()
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #
    # # Atualiza a data do cálculo da tabela da NF-e Receb #
    # def update_data_calculo_nfe_receb(self, pDataRef=str, pDataCalculo=str):
    #     etapaProcess = f"class {self.__class__.__name__} - def update_data_calculo_nfe_receb"
    #     # loga_mensagem(etapaProcess)
    #
    #     sql = f"""Update nfe_calc_nfe_receb
    #                  Set data_calc = '{pDataCalculo}'
    #                Where 1=1
    #                  And indi_cfop_particip    Is True
    #                  And numr_protocolo_cancel Is Null
    #                  And numr_ano_mes_dia_emissao = '{pDataRef}'"""
    #
    #     try:
    #         etapaProcess = f"Executa query de atualização da data de cálculo da NF3-e Receb: {sql}"
    #         self.sessionMySQL.execute(sql)
    #         self.sessionMySQL.commit()
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #
    # # Atualiza a data do cálculo da tabela do NF3-e #
    # def update_data_calculo_nf3e(self, pDataRef=str, pDataCalculo=str):
    #     etapaProcess = f"class {self.__class__.__name__} - def update_data_calculo_nf3e"
    #     # loga_mensagem(etapaProcess)
    #
    #     sql = f"""Update nfe_calc_nf3e
    #                  Set data_calc = '{pDataCalculo}'
    #                Where 1=1
    #                  And cfop_particip            Is True
    #                  And data_cancelamento         = '0'
    #                  And numr_ano_mes_dia_emissao  = '{pDataRef}'"""
    #
    #     try:
    #         etapaProcess = f"Executa query de atualização da data de cálculo da NF3-e: {sql}"
    #         self.sessionMySQL.execute(sql)
    #         self.sessionMySQL.commit()
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #
    # # Atualiza a data do cálculo dos CT-es #
    # def update_data_calculo_cte(self, pDataRef=str, pDataCalculo=str):
    #     etapaProcess = f"class {self.__class__.__name__} - def update_data_calculo_cte"
    #     # loga_mensagem(etapaProcess)
    #
    #     sql = f"""Update nfe_calc_cte
    #                  Set data_calc = '{pDataCalculo}'
    #                Where 1=1
    #                  And indi_calcelamento Is False
    #                  And data_emissao        = '{pDataRef}'"""
    #
    #     try:
    #         etapaProcess = f"Executa query de atualização da data de cálculo dos CT-es: {sql}"
    #         self.sessionMySQL.execute(sql)
    #         self.sessionMySQL.commit()
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #
    # # Atualiza a data do cálculo dos BP-es #
    # def update_data_calculo_bpe(self, pDataRef=str, pDataCalculo=str):
    #     etapaProcess = f"class {self.__class__.__name__} - def update_data_calculo_bpe"
    #     # loga_mensagem(etapaProcess)
    #
    #     sql = f"""Update nfe_calc_bpe
    #                  Set data_calc = '{pDataCalculo}'
    #                Where 1=1
    #                  And bpe.codg_evento Is Null
    #                  And SUBSTRING(bpe.data_embarque, 1, 19) Between CONCAT('{pDataRef}', 'T00:00:00')
    #                                                              And CONCAT('{pDataRef}', 'T23:59:59')"""
    #
    #     try:
    #         etapaProcess = f"Executa query de atualização da data de cálculo dos BP-es: {sql}"
    #         self.sessionMySQL.execute(sql)
    #         self.sessionMySQL.commit()
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #
    # # Totaliza os CT-es por municipio #
    # def select_cte_total_municipio(self, pDataRef) -> pd.DataFrame:
    #     etapaProcess = f"class {self.__class__.__name__} - def select_cte_total_municipio"
    #     # loga_mensagem(etapaProcess)
    #
    #     sql = f"""Select cte.codg_munic_inicio
    #                    , SUM(cte.valr_total_prest_servico)   As valr_cte
    #                 From nfe_calc_cte cte
    #                Where 1=1
    #                  And cte.indi_calcelamento Is False
    #                  And cte.data_emissao       = {pDataRef}
    #                Group By cte.codg_munic_inicio"""
    #
    #     try:
    #         etapaProcess = f"Executa query de totalização de CT-es por municípios: {sql}"
    #         df = self.sessionMySQL.execute(sql)
    #         return df
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #
    # # Totaliza os BP-es por municipio #
    # def select_bpe_total_municipio(self, pDataRef) -> pd.DataFrame:
    #     etapaProcess = f"class {self.__class__.__name__} - def select_bpe_total_municipio"
    #     # loga_mensagem(etapaProcess)
    #
    #     sql = f"""Select bpe.codg_munic_inicio
    #                    , SUM(bpe.valr_bilhete)   As valr_bilhete
    #                    , SUM(bpe.valr_desconto)  As valr_desconto
    #                    , SUM(bpe.valr_pagamento) As valr_pagamento
    #                 From nfe_calc_bpe bpe
    #                Where 1=1
    #                  And bpe.codg_evento Is Null
    #                  And SUBSTRING(bpe.data_embarque, 1, 19) Between CONCAT('{pDataRef}', 'T00:00:00')
    #                                                              And CONCAT('{pDataRef}', 'T23:59:59')
    #                Group By bpe.codg_munic_inicio"""
    #
    #     try:
    #         etapaProcess = f"Executa query de totalização de BP-es por municípios: {sql}"
    #         df = self.sessionMySQL.execute(sql)
    #         return df
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #
    # # Grava tabela de NF-e #
    # def insert_nfe(self, df) -> pd.DataFrame:
    #     etapaProcess = f"class {self.__class__.__name__} - def insert_nfe"
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"Grava dados na tabela de NF-e."
    #         nome_tabela_insert = 'nfe_calc_nfe'
    #         df.to_sql(con=self.engine, name=nome_tabela_insert, if_exists='append', index=None, chunksize=50000)
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #
    # # Grava tabela de NF-e #
    # def insert_nfe_chave(self, df) -> pd.DataFrame:
    #     etapaProcess = f"class {self.__class__.__name__} - def insert_nfe"
    #     # loga_mensagem(etapaProcess)
    #
    #     try:
    #         etapaProcess = f"Grava dados na tabela de NF-e."
    #         nome_tabela_insert = 'nfe_calc_nfe_chave'
    #         df.to_sql(con=self.engine, name=nome_tabela_insert, if_exists='append', index=None, chunksize=50000)
    #
    #     except Exception as err:
    #         etapaProcess += " - ERRO - " + str(err)
    #         loga_mensagem_erro(etapaProcess)
    #

    # def __init__(self):
    #     var = None
    #
    # def criaSessao() -> Session:
    #     # Carregando variaveis do .env_diego
    #     load_dotenv()
    #
    #     # Cria objeto usando os parâmetros de conexão configurados individualmente
    #     env_database = os.environ.get('DB_MYSQL_DATABASE')
    #     env_driver = os.environ.get('DB_MYSQL_DRIVER')
    #     env_host = os.environ.get('DB_MYSQL_HOST')
    #     env_username = os.environ.get('DB_MYSQL_USERNAME')
    #     env_password = os.environ.get('DB_MYSQL_PASSWORD')
    #
    #     return Mysql(drivername=env_driver,
    #                  username=env_username,
    #                  password=env_password,
    #                  host=env_host,
    #                  database=env_database)


'''
    def create_engine(self):
        etapaProcess = f"class {self.__class__.__name__} - def create_engine."
        # loga_mensagem(etapaProcess)

        try:
            database_url = f"mysql://{self.__env_username}:{self.__env_password}@{self.__env_host}/{self.__env_database}?charset=utf8"
            engineMySQL = sqlalchemy.create_engine(database_url)
            # connectionMySQL = engineMySQL.connect()

            # Cria uma fábrica de sessões
            SessionMySQL = sessionmaker(bind=engineMySQL)

            # Crie uma sessão
            sessionMySQL = SessionMySQL()

            return sessionMySQL

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise


    def closeSession(self):
        self.session.close()

    def commit(self):
        etapaProcess = f"class {self.__class__.__name__} - def commit."
        # loga_mensagem(etapaProcess)

        try:
            if self.session.transaction:
                self.session.commit()
                # loga_mensagem("Transação commitada com sucesso!")

        except Exception as err:
            etapaProcess += " - Exception - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

        except exc.SQLAlchemyError as err:
            etapaProcess += " - SQLAlchemyError - " + str(err)
            self.session.rollback()
            raise

    def rollback(self):
        etapaProcess = f"class {self.__class__.__name__} - def rollback."
        # loga_mensagem(etapaProcess)

        self.session.rollback()

    def inserir_pessoa(self, nome, idade):
        etapaProcess = f"class {self.__class__.__name__} - def inserir_pessoa."
        # loga_mensagem(etapaProcess)

        try:
            nova_pessoa = Pessoa(nome=nome, idade=idade)
            self.session.add(nova_pessoa)
            loga_mensagem("Pessoa inserida com sucesso!")

        except Exception as err:
            etapaProcess += " - Exception - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

        except exc.SQLAlchemyError as err:
            etapaProcess += " - SQLAlchemyError - " + str(err)
            loga_mensagem_erro(etapaProcess)
            self.rollback()

    def execute_sql_consulta(self, sql:str, params:str) -> pd.DataFrame:
        etapaProcess = f"class {self.__class__.__name__} - def execute_sql_consulta."
        # loga_mensagem(etapaProcess)

        try:
            session = self.create_engine()
            df = session.execute(sql, params=params)
            session.close()

            return df

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def execute_sql(self, sql:str) -> Session:
        etapaProcess = f"class {self.__class__.__name__} - def execute_sql."
        # loga_mensagem(etapaProcess)

        try:
            session = self.create_engine()
            session.execute(sql)
            return session

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def commit_operacao(sessionMySQL=Session):
        etapaProcess = f"class {self.__class__.__name__} - def commit_operacao."
        # loga_mensagem(etapaProcess)

        try:
            sessionMySQL.commit()
            return None

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def consultar_pessoas(self):
        try:
            # Consulta todas as pessoas na tabela
            pessoas = self.session.query(Pessoa).all()
            for pessoa in pessoas:
                loga_mensagem(f"ID: {pessoa.id}, Nome: {pessoa.nome}, Idade: {pessoa.idade}")
        except exc.SQLAlchemyError as e:
            loga_mensagem(f"Erro ao consultar pessoas: {e}")
        finally:
            # Fecha a sessão
            self.session.close()
'''