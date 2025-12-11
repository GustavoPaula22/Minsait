from django.http import HttpResponse
from funcoes.utilitarios import *
from funcoes.constantes import EnumTipoDocumento, EnumParametros
from negocio.BPe import BPe
from negocio.Param import ParamClasse
from negocio.GEN import GenClass

def carga_bpe(request):
    etapaProcess = f'Processa carga de BP-es, conforme parâmetro do sistema.'
    return carga_bpe_param()

def carga_bpe_data(request, p_data_inicio, p_data_fim):
    etapaProcess = f'Processa carga de BP-es, conforme parâmetro de datas informado.'
    return carga_bpe_periodo(p_data_inicio, p_data_fim)

def carga_bpe_param():
    return HttpResponse(carga_bpe_command())

def carga_bpe_command():
    etapaProcess = f"class {__name__} - def carga_bpe_param"
    # loga_mensagem(etapaProcess)

    try:
        param = ParamClasse()
        ultimaDataProcess = param.busca_valor_parametro(EnumParametros.ultimaCargaBPe.value)
        proxDataProcess = datetime.strptime(ultimaDataProcess, '%Y-%m-%d %H:%M:%S')
        proxDataProcess += timedelta(seconds=1)

        df_hora_proc = dividir_dia_em_horas(proxDataProcess.strftime('%Y-%m-%d %H:%M:%S'), 1)
        for x, hora in df_hora_proc.iterrows():
            bpe = BPe()
            etapaProcess = bpe.carga_bpe(hora.dDataInicial, hora.dDataFinal)
            del bpe

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)

    return etapaProcess

def carga_bpe_periodo(p_data_inicio, p_data_fim):
    etapaProcess = f"class {__name__} - def carga_bpe_periodo - {p_data_inicio} a {p_data_fim}"
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
                        bpe = BPe()
                        etapaProcess = bpe.carga_bpe(hora.dDataInicial, hora.dDataFinal)
                        del bpe
            else:
                etapaProcess = f'Período para carga de BP-es com problemas. Período informado: {p_data_inicio} a {p_data_fim}.'
                loga_mensagem_erro(etapaProcess)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
    else:
        etapaProcess = 'Quantidade de processamentos no dia deve estar entre 1 e 24!'

    return HttpResponse(etapaProcess)
