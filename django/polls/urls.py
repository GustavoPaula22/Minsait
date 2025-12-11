from django.urls import path

from .views import CargaNfe, CalculaVA, CargaTabsAuxiliares, CargaEFD, CargaNFA, CargaNF3e, CargaConv115, CargaBPe, \
    CargaCTe, ListaParam, ListaDocPartct, ProcessaAcerto, CargaSimples, CargaOpcaoSimplesSIMEI, CalculaIndicePartct

urlpatterns = [
    path("CargaNFe", CargaNfe.carga_nfe_gerada, name="index"),
    path("CargaNFe/<int:p_data_inicio>/<int:p_data_fim>/<int:p_qtde_procesm>/", CargaNfe.carga_nfe_gerada_data, name="detail"),
    path("CargaNFeHora/<int:p_data_ref>/<str:p_hora_ini>/<str:p_hora_fim>/", CargaNfe.carga_nfe_gerada_hora, name="detail"),

    path("CargaNFCe", CargaNfe.carga_nfce_gerada, name="index"),
    path("CargaNFCe/<int:p_data_inicio>/<int:p_data_fim>/<int:p_qtde_procesm>/", CargaNfe.carga_nfce_gerada_data, name="detail"),
    path("CargaNFCeHora/<int:p_data_ref>/<str:p_hora_ini>/<str:p_hora_fim>/", CargaNfe.carga_nfce_gerada_hora, name="detail"),

    path("CargaNFeRecebida", CargaNfe.carga_nfe_recebida, name="index"),
    path("CargaNFeRecebida/<int:p_data_inicio>/<int:p_data_fim>/<int:p_qtde_procesm>/", CargaNfe.carga_nfe_recebida_data, name="detail"),
    path("CargaNFeRecebidaHora/<int:p_data_ref>/<str:p_hora_ini>/<str:p_hora_fim>/", CargaNfe.carga_nfe_recebida_hora, name="detail"),

    path("CargaNFes/<int:p_data_inicio>/<int:p_data_fim>/<int:p_qtde_procesm>/", CargaNfe.carga_nfes_data,name="detail"),

    path("CargaEFD", CargaEFD.carga_efd, name="index"),
    path("CargaEFD/<int:p_data_inicio>/<int:p_data_fim>/", CargaEFD.carga_efd_data, name="detail"),

    path("CargaNFA", CargaNFA.carga_nfa, name="index"),
    path("CargaNFA/<int:p_data_inicio>/<int:p_data_fim>/", CargaNFA.carga_nfa_data, name="detail"),

    path("CargaBPe", CargaBPe.carga_bpe, name="index"),
    path("CargaBPe/<int:p_data_inicio>/<int:p_data_fim>/", CargaBPe.carga_bpe_data, name="detail"),

    path("CargaCTe", CargaCTe.carga_cte, name="index"),
    path("CargaCTe/<int:p_data_inicio>/<int:p_data_fim>/", CargaCTe.carga_cte_data, name="detail"),

    path("CargaNF3e", CargaNF3e.carga_nf3e, name="index"),
    path("CargaNF3e/<int:p_data_inicio>/<int:p_data_fim>/", CargaNF3e.carga_nf3e_data, name="detail"),

    path("CargaConv115", CargaConv115.carga_conv115, name="index"),
    path("CargaConv115/<int:p_data_inicio>/<int:p_data_fim>/", CargaConv115.carga_conv115_data, name="detail"),

    path("CargaSimples", CargaSimples.carga_simples, name="index"),
    path("CargaSimples/<str:p_arquivo>/", CargaSimples.carga_simples_arquivo, name="detail"),

    path("CargaSimplesADABAS", CargaOpcaoSimplesSIMEI.carga_opcao_simples_simei, name="index"),

    path("CargaCalcelamentoNFe/<int:p_ano>/<str:p_nome_arquivo>/<str:p_separador>/", CargaNfe.cancela_nfe, name="index"),

    path("InicializaParametros", CargaTabsAuxiliares.inicializa_parametros, name="index"),
    path("AtualizaParametro/<str:p_nome_param>/<str:p_valr_param>/", CargaTabsAuxiliares.atualiza_parametro, name="detail"),
    path("CriaNovoParametro/<str:p_nome_param>/<str:p_valr_param>/", CargaTabsAuxiliares.cria_novo_parametro, name="detail"),
    path("CargaCCEE/<str:p_nome_arquivo>/<str:p_separador>/", CargaTabsAuxiliares.carrega_arquivo_ccee, name="detail"),
    path("CargaNCMRural/<int:p_data_ref>/", CargaTabsAuxiliares.carrega_arquivo_ncm, name="detail"),
    path("CargaRamoAtivCNAE", CargaTabsAuxiliares.carrega_ramo_atividade_cnae, name="detail"),
    path("CargaMotivoExclusao", CargaTabsAuxiliares.carrega_motivo_exclusao, name="index"),

    path("AcertaCFOP/<int:p_tipo_doc>/", ProcessaAcerto.acerta_cfop, name="index"),
    path("AcertaCFOP/<int:p_data_inicio>/<int:p_data_fim>/<int:p_tipo_doc>/", ProcessaAcerto.acerta_cfop_data,name="index"),
    path("CargaCFOP/<int:p_data_ref>/<str:p_tipo_doc>/", CargaTabsAuxiliares.carrega_arquivo_cfop, name="detail"),
    path("ExcluiCFOP/", ProcessaAcerto.exclui_cfop, name="index"),

    path("AcertaCadastro/<int:p_tipo_doc>/", ProcessaAcerto.acerta_cadastro, name="index"),
    path("AcertaCadastro/<int:p_data_inicio>/<int:p_data_fim>/<int:p_tipo_doc>/", ProcessaAcerto.acerta_cadastro_data, name="index"),

    path("AcertaClasseConsumo/<int:p_data_inicio>/<int:p_data_fim>/", ProcessaAcerto.acerta_classe_consumo,name="index"),

    path("AcertoContrib", ProcessaAcerto.carrega_historico_contrib, name="index"),

    path("InicializaBD/<int:p_data_ref>/", CargaTabsAuxiliares.inicializa_tabelas, name="detail"),
    path("LimpaTabela/<str:p_tabela>/<str:p_param>/", CargaTabsAuxiliares.limpa_base_dados, name="detail"),

    path("ListaParametros/", ListaParam.lista_parametros_cadastrados, name="index"),

    path('MenuDocs/', ListaDocPartct.menu_consulta_documentos, name='menu_consulta'),
    path('processa_consulta/', ListaDocPartct.processa_consulta_documentos, name='processa_consulta'),

    path('ListaDocsPartctPeriodo/<str:p_tipo_doc>/<str:p_data_inicio>/<str:p_data_fim>/', ListaDocPartct.lista_documentos_partct_periodo, name='lista_documentos_partct_periodo'),
    path("ListaDocsPartctInscricao/<str:p_tipo_doc>/<str:p_ie_entrada>/<str:p_ie_saida>/", ListaDocPartct.lista_documentos_partct_ie, name='lista_documentos_partct_ie'),
    path("ListaDocsPartctPeriodoInscricao/<str:p_tipo_doc>/<str:p_data_inicio>/<str:p_data_fim>/<str:p_ie_entrada>/<str:p_ie_saida>/", ListaDocPartct.lista_documentos_partct_periodo_ie, name='lista_documentos_partct_periodo_ie'),
    path("ListaDocsPartctChave/<str:p_tipo_doc>/<str:p_chave>/", ListaDocPartct.lista_documentos_partct_chave, name='lista_documentos_partct_chave'),

    path('ListaNFeEstudoSimples/<str:p_tipo>/<str:p_data_inicio>/<str:p_data_fim>/', ListaDocPartct.lista_nfe_simples, name='lista_nfe_simples_estudo'),

    path('ListaDocsNaoPartctPeriodo/<str:p_tipo_doc>/<str:p_data_inicio>/<str:p_data_fim>/', ListaDocPartct.lista_documentos_nao_partct_periodo, name='lista_documentos_nao_partct_periodo'),
    path("ListaDocsNaoPartctInscricao/<str:p_tipo_doc>/<str:p_ie_entrada>/<str:p_ie_saida>/", ListaDocPartct.lista_documentos_nao_partct_ie, name='lista_documentos_nao_partct_ie'),
    path("ListaDocsNaoPartctPeriodoInscricao/<str:p_tipo_doc>/<str:p_data_inicio>/<str:p_data_fim>/<str:p_ie_entrada>/<str:p_ie_saida>/", ListaDocPartct.lista_documentos_nao_partct_periodo_ie, name='lista_documentos_nao_partct_periodo_ie'),
    path("ListaDocsNaoPartctChave/<str:p_tipo_doc>/<str:p_chave>/", ListaDocPartct.lista_documentos_nao_partct_chave, name='lista_documentos_nao_partct_chave'),

    path('ListaItensNaoPartctPeriodo/<str:p_tipo_doc>/<str:p_data_inicio>/<str:p_data_fim>/', ListaDocPartct.lista_itens_nao_partct_periodo, name='lista_itens_nao_partct_periodo'),
    path("ListaItensNaoPartctInscricao/<str:p_tipo_doc>/<str:p_ie_entrada>/<str:p_ie_saida>/", ListaDocPartct.lista_itens_nao_partct_ie, name='lista_itens_nao_partct_ie'),
    path("ListaItensNaoPartctPeriodoInscricao/<str:p_tipo_doc>/<str:p_data_inicio>/<str:p_data_fim>/<str:p_ie_entrada>/<str:p_ie_saida>/", ListaDocPartct.lista_itens_nao_partct_periodo_ie, name='lista_itens_nao_partct_periodo_ie'),
    path("ListaItensNaoPartctChave/<str:p_tipo_doc>/<str:p_chave>/", ListaDocPartct.lista_itens_nao_partct_chave, name='lista_itens_nao_partct_chave'),

    path("baixa_documentos_partct/", ListaDocPartct.baixa_documentos_partct, name='download_csv'),

    # path("AtualizaCCENFe/<int:p_periodo>/", CargaNfe.carga_cce_nfe, name="detail"),
    # path("AtualizaCCEEFD/<int:p_periodo>/", CargaEFD.carga_cce_efd, name="detail"),
    # path("atualizaCNAE/<int:pPeriodo>/", CargaNfe.atualizaCNAE, name="detail"),

    path("CargaArquivoVA/<int:p_ano>/<int:p_tipo_doc>/<str:p_nome_arquivo>/<str:p_separador>/", CalculaVA.carrega_va_externo,name='carrega_documento_valor_adicionado'),
    path("RegistraAtivoImobilizado/<int:p_ano>/", CalculaVA.registra_ativo_imobilizado,name='registra_registra_ativo_imobilizado'),
    path("RegistraOperacoesDuplicadas/<int:p_ano>/<int:p_arq>/", CalculaVA.registra_operacoes_entre_contribuintes,name='registra_operacoes_entre_contribuintes'),
    path("CalculaVA/<int:pPeriodo>/", CalculaVA.calculo_valor_adicionado, name='calculo_valor_adicionado'),

    path("CargaArquivoIndice/<int:p_ano>/<int:id_resolucao>/<str:p_etapa>/<str:p_tipo_indice>/<str:p_nome_arquivo>/<str:p_separador>/", CalculaIndicePartct.carrega_indice_externo,name='carrega_documento_valor_adicionado'),
    path("CalculaIndice/<int:p_ano>/<str:p_etapa>/", CalculaIndicePartct.calculo_indice_partct, name='calculo_valor_adicionado'),

]

