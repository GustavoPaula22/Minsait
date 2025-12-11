from funcoes.constantes import EnumParametros
from funcoes.utilitarios import *
from negocio.EFD import EFDClasse
from negocio.GEN import GenClass
from negocio.Param import ParamClasse


def carga_efd(request):
    etapaProcess = f'Processa carga de NF-e geradas, conforme parâmetro do sistema.'
    return carga_efd_param()


def carga_efd_data(request, p_data_inicio, p_data_fim):
    etapaProcess = f'Processa carga de EFDs, conforme parâmetro de datas informado.'
    return carga_efd_periodo(p_data_inicio, p_data_fim)


def carga_efd_param():
    etapaProcess = f'Processa carga de EFD conforme parâmetro do sistema.'

    p_qtde_procesm = 1

    try:
        param = ParamClasse()

        ultimaDataProcess = param.busca_valor_parametro(EnumParametros.ultimaCargaEFD.value)
        proxDataProcess = datetime.strptime(ultimaDataProcess, '%Y-%m-%d %H:%M:%S')
        proxDataProcess += timedelta(seconds=1)

        df_hora_proc = dividir_dia_em_horas(proxDataProcess.strftime('%Y-%m-%d %H:%M:%S'), p_qtde_procesm)
        for x, hora in df_hora_proc.iterrows():
            efd = EFDClasse(hora.dDataInicial, hora.dDataFinal)
            etapaProcess = efd.carga_efd()
            del efd

        param = ParamClasse()
        param.atualizar_parametro(EnumParametros.ultimaCargaEFD.value, hora.dDataFinal)
        del param

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        etapaProcess = 9

    return etapaProcess


def carga_efd_periodo(p_data_inicio, p_data_fim):
    etapaProcess = f'Carrega informações do EFD, no período de {p_data_inicio} a {p_data_fim}.'

    p_qtde_procesm = 1

    try:
        gen = GenClass()
        df_data_proc = gen.carrega_periodo_processamento(p_data_inicio, p_data_fim)

        if df_data_proc is not None:
            for idx, row in df_data_proc.iterrows():
                df_hora_proc = dividir_dia_em_horas(row.data_com_traco + ' 00:00:00', p_qtde_procesm)
                for x, hora in df_hora_proc.iterrows():
                    efd = EFDClasse(hora.dDataInicial, hora.dDataFinal)
                    etapaProcess = efd.carga_efd()
                    del efd
        else:
            etapaProcess = f'Período para carga de EFDs com problemas. Período informado: {p_data_inicio} a {p_data_fim}.'
            loga_mensagem_erro(etapaProcess)

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        etapaProcess = 9

    return etapaProcess
