from django.http import HttpResponse
from funcoes.utilitarios import *
from funcoes.constantes import EnumTipoDocumento, EnumParametros
from negocio.CTe import CTeClasse
from negocio.Param import ParamClasse
from negocio.GEN import GenClass


def carga_cte(request):
    etapaProcess = f'Processa carga de CT-es, conforme parâmetro do sistema.'
    return carga_cte_param()


def carga_cte_data(request, p_data_inicio, p_data_fim):
    etapaProcess = f'Processa carga de CT-es, conforme parâmetro de datas informado.'
    return carga_cte_periodo(p_data_inicio, p_data_fim)


def carga_cte_param():
    return HttpResponse(carga_cte_command())


def carga_cte_command() -> str:
    etapaProcess = f"class {__name__} - def carga_cte_command"
    # loga_mensagem(etapaProcess)

    try:
        param = ParamClasse()
        ultimaDataProcess = param.busca_valor_parametro(EnumParametros.ultimaCargaCTe.value)
        proxDataProcess = datetime.strptime(ultimaDataProcess, '%Y-%m-%d %H:%M:%S')
        proxDataProcess += timedelta(seconds=1)

        df_hora_proc = dividir_dia_em_horas(proxDataProcess.strftime('%Y-%m-%d %H:%M:%S'), 1)
        for x, hora in df_hora_proc.iterrows():
            cte = CTeClasse(hora.dDataInicial, hora.dDataFinal)
            etapaProcess = cte.carga_cte()
            del cte

        param = ParamClasse()
        param.atualizar_parametro(EnumParametros.ultimaCargaCTe.value, hora.dDataFinal)
        del param

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        etapaProcess = 9

    return etapaProcess


def carga_cte_periodo(p_data_inicio, p_data_fim):
    etapaProcess = f"class {__name__} - def carga_cte_periodo - {p_data_inicio} a {p_data_fim}"
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
                        cte = CTeClasse(hora.dDataInicial, hora.dDataFinal)
                        etapaProcess = cte.carga_cte()
                        del cte
            else:
                etapaProcess = f'Período para carga de CT-es com problemas. Período informado: {p_data_inicio} a {p_data_fim}.'
                loga_mensagem_erro(etapaProcess)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
    else:
        etapaProcess = 'Quantidade de processamentos no dia deve estar entre 1 e 24!'

    return HttpResponse(etapaProcess)
