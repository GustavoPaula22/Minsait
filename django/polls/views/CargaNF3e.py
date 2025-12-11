from django.http import HttpResponse
from funcoes.utilitarios import *
from funcoes.constantes import EnumTipoDocumento, EnumParametros
from negocio.NF3e import NF3eClasse
from negocio.Param import ParamClasse
from negocio.GEN import GenClass


def carga_nf3e(request):
    etapaProcess = f'Processa carga de NF3-es, conforme parâmetro do sistema.'
    return carga_nf3e_param()


def carga_nf3e_data(request, p_data_inicio, p_data_fim):
    etapaProcess = f'Processa carga de NF3-es, conforme parâmetro de datas informado.'
    return carga_nf3e_periodo(p_data_inicio, p_data_fim)


def carga_nf3e_param() -> str:
    etapaProcess = f"class {__name__} - def carga_nf3e_param"
    # loga_mensagem(etapaProcess)

    try:
        param = ParamClasse()
        ultimaDataProcess = param.busca_valor_parametro(EnumParametros.ultimaCargaNF3e.value)
        proxDataProcess = datetime.strptime(ultimaDataProcess, '%Y-%m-%d %H:%M:%S')
        proxDataProcess += timedelta(seconds=1)

        df_hora_proc = dividir_dia_em_horas(proxDataProcess.strftime('%Y-%m-%d %H:%M:%S'), 1)

        # Consulta os eventos a partir da primeira data do processamento
        # Este DF será utilizado para todos os intervalos de data
        nf3e = NF3eClasse(p_data_inicio=df_hora_proc['dDataInicial'].iloc[0],
                          p_data_fim=df_hora_proc['dDataFinal'].iloc[0],
                          p_df_nf3e_evnt=None)
        df_nf3e_evnt = nf3e.carga_nf3e_eventos_cancelamento()

        for x, hora in df_hora_proc.iterrows():
            nf3e = NF3eClasse(p_data_inicio=hora.dDataInicial,
                              p_data_fim=hora.dDataFinal,
                              p_df_nf3e_evnt=df_nf3e_evnt)

            etapaProcess = nf3e.carga_nf3e()
            del nf3e

        param = ParamClasse()
        param.atualizar_parametro(EnumParametros.ultimaCargaNF3e.value, hora.dDataFinal)
        del param

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        etapaProcess = 9

    return etapaProcess


def carga_nf3e_periodo(p_data_inicio, p_data_fim):
    etapaProcess = f"class {__name__} - def carga_nf3e_periodo - {p_data_inicio} a {p_data_fim}"
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

                    # Consulta os eventos a partir da primeira data do processamento
                    # Este DF será utilizado para todos os intervalos de data
                    nf3e = NF3eClasse(p_data_inicio=df_hora_proc['dDataInicial'].iloc[0],
                                      p_data_fim=df_hora_proc['dDataFinal'].iloc[0],
                                      p_df_nf3e_evnt=None)
                    df_nf3e_evnt = nf3e.carga_nf3e_eventos_cancelamento()

                    for x, hora in df_hora_proc.iterrows():
                        nf3e = NF3eClasse(p_data_inicio=hora.dDataInicial,
                                          p_data_fim=hora.dDataFinal,
                                          p_df_nf3e_evnt=df_nf3e_evnt)
                        etapaProcess = nf3e.carga_nf3e()
                        del nf3e
            else:
                etapaProcess = f'Período para carga de NF3-es com problemas. Período informado: {p_data_inicio} a {p_data_fim}.'
                loga_mensagem_erro(etapaProcess)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
    else:
        etapaProcess = 'Quantidade de processamentos no dia deve estar entre 1 e 24!'

    return HttpResponse(etapaProcess)
