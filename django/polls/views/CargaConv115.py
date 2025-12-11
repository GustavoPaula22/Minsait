from django.http import HttpResponse
from funcoes.utilitarios import *
from funcoes.constantes import EnumTipoDocumento, EnumParametros
from negocio.Conv115 import Conv115Classe
from negocio.NF3e import NF3eClasse
from negocio.Param import ParamClasse
from negocio.GEN import GenClass


def carga_conv115(request):
    etapaProcess = f'Processa carga do Convênio 115, conforme parâmetro do sistema.'
    return carga_conv115_param()


def carga_conv115_data(request, p_data_inicio, p_data_fim):
    etapaProcess = f'Processa carga do Convênio 115, conforme parâmetro de datas informado.'
    return carga_conv115_periodo(p_data_inicio, p_data_fim)


def carga_conv115_param() -> str:
    etapaProcess = f"class {__name__} - def carga_conv115_param"
    # loga_mensagem(etapaProcess)

    try:
        param = ParamClasse()
        ultimaDataProcess = param.busca_valor_parametro(EnumParametros.ultimaCargaConv115.value)
        proxDataProcess = datetime.strptime(ultimaDataProcess, '%Y-%m-%d %H:%M:%S')
        proxDataProcess += timedelta(seconds=1)

        df_hora_proc = dividir_dia_em_horas(proxDataProcess.strftime('%Y-%m-%d %H:%M:%S'), 1)
        for x, hora in df_hora_proc.iterrows():
            conv115 = Conv115Classe(hora.dDataInicial, hora.dDataFinal)
            etapaProcess = conv115.carga_conv115()
            del conv115

        param = ParamClasse()
        param.atualizar_parametro(EnumParametros.ultimaCargaConv115.value, hora.dDataFinal)
        del param

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        etapaProcess = 9

    return etapaProcess


def carga_conv115_periodo(p_data_inicio, p_data_fim):
    etapaProcess = f"class {__name__} - def carga_conv115_periodo - {p_data_inicio} a {p_data_fim}"
    # loga_mensagem(etapaProcess)

    etapaProcess = f'Busca datas de processamento - {p_data_inicio} a {p_data_fim}'

    p_qtde_procesm = 1
    if p_qtde_procesm > 0 & p_qtde_procesm < 25:
        try:
            gen = GenClass()
            df_data_proc = gen.carrega_periodo_processamento(p_data_inicio, p_data_fim)

            if df_data_proc is not None:
                for idx, row in df_data_proc.iterrows():
                    df_hora_proc = dividir_dia_em_horas(row.data_com_traco + ' 00:00:00', p_qtde_procesm)
                    for x, hora in df_hora_proc.iterrows():
                        conv115 = Conv115Classe(hora.dDataInicial, hora.dDataFinal)
                        etapaProcess = conv115.carga_conv115()
                        del conv115
            else:
                etapaProcess = f'Período para carga do Convênio 115 com problemas. Período informado: {p_data_inicio} a {p_data_fim}.'
                loga_mensagem_erro(etapaProcess)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
    else:
        etapaProcess = 'Quantidade de processamentos no dia deve estar entre 1 e 24!'

    return HttpResponse(etapaProcess)
