from datetime import datetime, timedelta

from django.http import HttpResponse

from funcoes.constantes import EnumParametros, EnumTipoDocumento
from funcoes.utilitarios import loga_mensagem_erro, dividir_dia_em_horas
from negocio.CCE import CCEClasse
from negocio.CFOP import CFOPClasse
from negocio.GEN import GenClass
from negocio.NF3e import NF3eClasse
from negocio.NFe import NFeClasse
from negocio.Param import ParamClasse


def processa_acerto_command():
    # return exclui_cfop_command()
    return acerta_cadastro_command(2)


def acerta_cadastro(request, p_tipo_doc):
    return HttpResponse(acerta_cadastro_command(p_tipo_doc))


def exclui_cfop(request):
    return HttpResponse(exclui_cfop_command())


def acerta_cadastro_data(request, p_data_inicio, p_data_fim, p_tipo_doc):
    return HttpResponse(acerta_cadastro_periodo(p_data_inicio, p_data_fim, p_tipo_doc))


def acerta_cfop(request, p_tipo_doc):
    return HttpResponse(acerta_cfop_command(p_tipo_doc))


def acerta_cfop_data(request, p_data_inicio, p_data_fim, p_tipo_doc):
    return HttpResponse(acerta_cfop_periodo(p_data_inicio, p_data_fim, p_tipo_doc))


def acerta_cfop_command(p_tipo_doc):
    etapaProcess = f'Processa acerto do indicador de participação dos CFOPs.'

    try:
        if p_tipo_doc == EnumTipoDocumento.NFe.value:
            s_tipo_process = EnumParametros.ultimoAcertoCFOPNFe.value
        elif p_tipo_doc == EnumTipoDocumento.NFCe.value:
            s_tipo_process = EnumParametros.ultimoAcertoCFOPNFCe.value
        elif p_tipo_doc == EnumTipoDocumento.NFeRecebida.value:
            s_tipo_process = EnumParametros.ultimoAcertoCFOPNFeRecebida.value
        else:
            raise RuntimeError("Tipo de documento não previsto.")

        param = ParamClasse()
        ultimaDataProcess = param.busca_valor_parametro(s_tipo_process)
        del param

        proxDataProcess = datetime.strptime(ultimaDataProcess, '%Y-%m-%d %H:%M:%S')
        proxDataProcess += timedelta(seconds=1)
        df_data_proc = dividir_dia_em_horas(proxDataProcess.strftime('%Y-%m-%d %H:%M:%S'), 1)

        data_variavel = df_data_proc.loc[0, 'dDataInicial']
        p_data_inicio = data_variavel.strftime('%Y%m%d')

        data_variavel = df_data_proc.loc[0, 'dDataFinal']
        p_data_fim = data_variavel.strftime('%Y%m%d')

        obj_retorno = acerta_cfop_periodo(p_data_inicio, p_data_fim, p_tipo_doc)

        param = ParamClasse()
        param.atualizar_parametro(s_tipo_process, data_variavel)
        del param

        return obj_retorno

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)


def exclui_cfop_command():
    etapaProcess = f'Exclui CFOPs de participação.'

    try:
        cfop = CFOPClasse()
        etapaProcess = cfop.acerto_cfop('2024-01-01 00:00', 5)
        del cfop

        return etapaProcess

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)


def acerta_cfop_periodo(p_data_inicio, p_data_fim, p_tipo_doc):
    etapaProcess = f"class {__name__} - def acerta_cfop_periodo - {p_data_inicio} a {p_data_fim} - Tipo de documento: {p_tipo_doc}"
    # loga_mensagem(etapaProcess)

    etapaProcess = f'Atualiza o indicador de CFOP participante - {p_data_inicio} a {p_data_fim} - Tipo de Documento {p_tipo_doc}'
    i_qdade_process = 0
    i_tipo_process = 0

    if p_tipo_doc == EnumTipoDocumento.NFe.value:
        i_qdade_process = 1
    elif p_tipo_doc == EnumTipoDocumento.NFCe.value:
        i_qdade_process = 3
    elif p_tipo_doc == EnumTipoDocumento.NFeRecebida.value:
        i_qdade_process = 1
    else:
        raise RuntimeError("Tipo de documento não previsto.")

    try:
        gen = GenClass()
        df_data_proc = gen.carrega_periodo_processamento(p_data_inicio, p_data_fim)

        if df_data_proc is not None:
            for idx, row in df_data_proc.iterrows():
                df_hora_proc = dividir_dia_em_horas(row.data_com_traco + ' 00:00:00', i_qdade_process)
                for x, hora in df_hora_proc.iterrows():
                    nfe = NFeClasse(hora.dDataInicial, hora.dDataFinal)
                    etapaProcess = nfe.acerto_nfe_cfop(p_tipo_doc)
                    del nfe
        else:
            etapaProcess = f'Período para carga de NF-e Recebida com problemas. Período informado: {p_data_inicio} a {p_data_fim}.'
            loga_mensagem_erro(etapaProcess)

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)

    return etapaProcess


def acerta_cadastro_command(p_tipo_doc):
    etapaProcess = f"class {__name__} - def acerta_cadastro_command - Tipo de documento: {p_tipo_doc}"
    # loga_mensagem(etapaProcess)

    try:
        if p_tipo_doc == EnumTipoDocumento.NFA.value:
            s_tipo_process = EnumParametros.ultimoAcertoCadNFA.value
        elif p_tipo_doc == EnumTipoDocumento.NFe.value:
            s_tipo_process = EnumParametros.ultimoAcertoCadNFe.value
        elif p_tipo_doc == EnumTipoDocumento.NFCe.value:
            s_tipo_process = EnumParametros.ultimoAcertoCadNFCe.value
        elif p_tipo_doc == EnumTipoDocumento.NFeRecebida.value:
            s_tipo_process = EnumParametros.ultimoAcertoCadNFeRecebida.value
        else:
            raise RuntimeError("Tipo de documento não previsto.")

        param = ParamClasse()
        ultimaDataProcess = param.busca_valor_parametro(s_tipo_process)
        del param

        proxDataProcess = datetime.strptime(ultimaDataProcess, '%Y-%m-%d %H:%M:%S')
        proxDataProcess += timedelta(seconds=1)
        df_data_proc = dividir_dia_em_horas(proxDataProcess.strftime('%Y-%m-%d %H:%M:%S'), 1)

        data_variavel = df_data_proc.loc[0, 'dDataInicial']
        p_data_inicio = data_variavel.strftime('%Y%m%d')

        data_variavel = df_data_proc.loc[0, 'dDataFinal']
        p_data_fim = data_variavel.strftime('%Y%m%d')

        obj_retorno = acerta_cadastro_periodo(p_data_inicio, p_data_fim, p_tipo_doc)

        param = ParamClasse()
        param.atualizar_parametro(s_tipo_process, data_variavel)
        del param

        return obj_retorno

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)


def acerta_cadastro_periodo(p_data_inicio, p_data_fim, p_tipo_doc):
    etapaProcess = f"class {__name__} - def acerta_cadastro_periodo - {p_data_inicio} a {p_data_fim} - Tipo de documento: {p_tipo_doc}"
    # loga_mensagem(etapaProcess)

    etapaProcess = f'Atualiza o cadastro dos participantes do documento fiscal - {p_data_inicio} a {p_data_fim} - Tipo de Documento {p_tipo_doc}'

    if p_tipo_doc == EnumTipoDocumento.NFA.value:
        i_qdade_process = 1
    elif p_tipo_doc == EnumTipoDocumento.NFe.value:
        i_qdade_process = 1
    elif p_tipo_doc == EnumTipoDocumento.NFCe.value:
        i_qdade_process = 3
    elif p_tipo_doc == EnumTipoDocumento.NFeRecebida.value:
        i_qdade_process = 1
    else:
        raise RuntimeError("Tipo de documento não previsto.")

    try:
        # nfe = NFeClasse(datetime.strptime('20230101', '%Y%m%d'), datetime.strptime('20231231', '%Y%m%d'))
        # etapaProcess = nfe.acerto_nfe_cadastro(p_tipo_doc)
        # del nfe

        gen = GenClass()
        df_data_proc = gen.carrega_periodo_processamento(p_data_inicio, p_data_fim)

        if df_data_proc is not None:
            for idx, row in df_data_proc.iterrows():
                df_hora_proc = dividir_dia_em_horas(row.data_com_traco + ' 00:00:00', i_qdade_process)
                for x, hora in df_hora_proc.iterrows():
                    nfe = NFeClasse(hora.dDataInicial, hora.dDataFinal)
                    etapaProcess = nfe.acerto_nfe_cadastro(p_tipo_doc)
                    del nfe
        else:
            etapaProcess = f'Período para acerto do cadastro com problemas. Período informado: {p_data_inicio} a {p_data_fim}.'
            loga_mensagem_erro(etapaProcess)

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)

    return etapaProcess


def acerta_classe_consumo(request, p_data_inicio, p_data_fim):
    etapaProcess = f'Processa acerto do indicador de classe de consumo da tabela IPM_CONVERSAO_NF3E.'

    try:
        gen = GenClass()

        df_data_proc = gen.carrega_periodo_processamento(p_data_inicio, p_data_fim)

        if df_data_proc is not None:
            for idx, row in df_data_proc.iterrows():
                df_hora_proc = dividir_dia_em_horas(row.data_com_traco + ' 00:00:00', 1)
                for x, hora in df_hora_proc.iterrows():
                    nf3e = NF3eClasse(hora.dDataInicial, hora.dDataFinal, [])
                    etapaProcess = nf3e.acerto_classe_consumo()
                    del nf3e
        else:
            etapaProcess = f'Período para carga de NF-e Recebida com problemas. Período informado: {p_data_inicio} a {p_data_fim}.'
            loga_mensagem_erro(etapaProcess)

        return None

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)


def carrega_historico_contrib(request):
    etapaProcess = 'Atualiza a base de dados de cadastro de contribuintes.'

    try:
        cce = CCEClasse()
        etapaProcess = cce.inclui_informacoes_cce_log(datetime.strptime('20230101', '%Y%m%d').date(), datetime.strptime('20241231', '%Y%m%d').date(), 202302)
        del cce

        return None

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        raise

