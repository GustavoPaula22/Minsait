from django.http import HttpResponse
from funcoes.utilitarios import *
from funcoes.constantes import EnumParametros, EnumTipoDocumento
from negocio.NFe import NFeClasse
from negocio.Param import ParamClasse
from negocio.GEN import GenClass
from persistencia.Oraprd import Oraprd


def carga_nfe_gerada(request):
    etapaProcess = f'Processa carga de NF-e geradas, conforme parâmetro do sistema.'
    return carga_nfe_param(EnumTipoDocumento.NFe.value)


def carga_nfe_gerada_data(request, p_data_inicio, p_data_fim, p_qtde_procesm):
    etapaProcess = f'Processa carga de NF-e geradas, conforme parâmetro de datas informado.'
    return carga_nfe_data(EnumTipoDocumento.NFe.value, p_data_inicio, p_data_fim, p_qtde_procesm)


def carga_nfe_gerada_hora(request, p_data_ref, p_hora_ini, p_hora_fim):
    etapaProcess = f'Processa carga de NF-e geradas, conforme parâmetro de horas informado.'
    return carga_nfe_hora(EnumTipoDocumento.NFe.value, p_data_ref, p_hora_ini, p_hora_fim)


def carga_nfe_recebida(request):
    etapaProcess = f'Processa carga de NF-e recebidas, conforme parâmetro do sistema.'
    return carga_nfe_param(EnumTipoDocumento.NFeRecebida.value)


def carga_nfe_recebida_data(request, p_data_inicio, p_data_fim, p_qtde_procesm):
    etapaProcess = f'Processa carga de NF-e recebidas, conforme parâmetro de datas informado.'
    return carga_nfe_data(EnumTipoDocumento.NFeRecebida.value, p_data_inicio, p_data_fim, p_qtde_procesm)


def carga_nfe_recebida_hora(request, p_data_ref, p_hora_ini, p_hora_fim):
    etapaProcess = f'Processa carga de NF-e recebidas, conforme parâmetro de horas informado.'
    return carga_nfe_hora(EnumTipoDocumento.NFeRecebida.value, p_data_ref, p_hora_ini, p_hora_fim)


def carga_nfce_gerada(request):
    etapaProcess = f'Processa carga de NF-e geradas, conforme parâmetro do sistema.'
    return carga_nfe_param(EnumTipoDocumento.NFCe.value)


def carga_nfce_gerada_data(request, p_data_inicio, p_data_fim, p_qtde_procesm):
    etapaProcess = f'Processa carga de NF-e geradas, conforme parâmetro de datas informado.'
    return carga_nfe_data(EnumTipoDocumento.NFCe.value, p_data_inicio, p_data_fim, p_qtde_procesm)


def carga_nfce_gerada_hora(request, p_data_ref, p_hora_ini, p_hora_fim):
    etapaProcess = f'Processa carga de NF-e geradas, conforme parâmetro de horas informado.'
    return carga_nfe_hora(EnumTipoDocumento.NFCe.value, p_data_ref, p_hora_ini, p_hora_fim)


def carga_nfes_data(request, p_data_inicio, p_data_fim, p_qtde_procesm):
    etapaProcess = f'Processa carga de NF-e geradas, conforme parâmetro de datas informado.'
    return carga_nfe_data(3, p_data_inicio, p_data_fim, p_qtde_procesm)


def carga_nfe_param(p_tipo_nfe) -> str:
    etapaProcess = f"class {__name__} - def carga_nfe_param - {p_tipo_nfe}"
    # loga_mensagem(etapaProcess)

    i_periodicidade = 1

    try:
        param = ParamClasse()
        if p_tipo_nfe == EnumTipoDocumento.NFe.value:
            ultimaDataProcess = param.busca_valor_parametro(EnumParametros.ultimaCargaNFe.value)
        elif p_tipo_nfe == EnumTipoDocumento.NFCe.value:
            ultimaDataProcess = param.busca_valor_parametro(EnumParametros.ultimaCargaNFCe.value)
            i_periodicidade = 12
        else:
            ultimaDataProcess = param.busca_valor_parametro(EnumParametros.ultimaCargaNFeRecebida.value)
            i_periodicidade = 2

        proxDataProcess = datetime.strptime(ultimaDataProcess, '%Y-%m-%d %H:%M:%S')
        proxDataProcess += timedelta(seconds=1)

        df_hora_proc = dividir_dia_em_horas(proxDataProcess.strftime('%Y-%m-%d %H:%M:%S'), i_periodicidade)
        for x, hora in df_hora_proc.iterrows():
            verifica_param_datas(hora.dDataInicial, hora.dDataFinal)
            nfe = NFeClasse(hora.dDataInicial, hora.dDataFinal)
            etapaProcess = nfe.carga_nfe(p_tipo_nfe)
            del nfe

        param = ParamClasse()
        if p_tipo_nfe == EnumTipoDocumento.NFe.value:
            param.atualizar_parametro(EnumParametros.ultimaCargaNFe.value, hora.dDataFinal)
        elif p_tipo_nfe == EnumTipoDocumento.NFCe.value:
            param.atualizar_parametro(EnumParametros.ultimaCargaNFCe.value, hora.dDataFinal)
        elif p_tipo_nfe == EnumTipoDocumento.NFeRecebida.value:
            param.atualizar_parametro(EnumParametros.ultimaCargaNFeRecebida.value, hora.dDataFinal)
        del param

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        etapaProcess = 9

    return etapaProcess


def carga_nfe_data(p_tipo_nfe, p_data_inicio, p_data_fim, p_qtde_procesm):
    etapaProcess = f"class {__name__} - def carga_nfe_data - {p_data_inicio} a {p_data_fim}"
    # loga_mensagem(etapaProcess)

    etapaProcess = f'Busca datas de processamento - {p_data_inicio} a {p_data_fim}'

    if p_qtde_procesm > 0 & p_qtde_procesm < 25:
        try:
            gen = GenClass()
            df_data_proc = gen.carrega_periodo_processamento(p_data_inicio, p_data_fim)

            if df_data_proc is not None:
                for idx, row in df_data_proc.iterrows():
                    df_hora_proc = dividir_dia_em_horas(row.data_com_traco + ' 00:00:00', p_qtde_procesm)
                    verifica_param_datas(df_hora_proc['dDataInicial'][0], df_hora_proc['dDataFinal'][0])
                    for x, hora in df_hora_proc.iterrows():
                        nfe = NFeClasse(hora.dDataInicial, hora.dDataFinal)
                        if p_tipo_nfe == 3:
                            etapaProcess = nfe.carga_nfe(10)
                            if etapaProcess == '1':
                                etapaProcess = nfe.carga_nfe(1)
                                if etapaProcess == '1':
                                    etapaProcess = nfe.carga_nfe(2)
                        else:
                            etapaProcess = nfe.carga_nfe(p_tipo_nfe)

                        del nfe
            else:
                etapaProcess = f'Período para carga de NF-e com problemas. Período informado: {p_data_inicio} a {p_data_fim}.'
                loga_mensagem_erro(etapaProcess)

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
    else:
        etapaProcess = 'Quantidade de processamentos no dia deve estar entre 1 e 24!'

    return etapaProcess


def carga_nfe_hora(p_tipo_nfe, p_data_ref, p_hora_ini, p_hora_fim):
    etapaProcess = f'Processa carga_nfe_hora - Intervalo: {p_data_ref} de {p_hora_ini} a {p_hora_fim}'

    try:
        sHoraInicio = ' ' + p_hora_ini
        sHoraFim = ' ' + p_hora_fim
        dDataIni = datetime.strptime(str(p_data_ref) + sHoraInicio, '%Y%m%d %H:%M:%S')
        dDataFim = datetime.strptime(str(p_data_ref) + sHoraFim, '%Y%m%d %H:%M:%S')

        verifica_param_datas(dDataIni, dDataFim)
        nfe = NFeClasse(dDataIni, dDataFim)
        nfe.carga_nfe(p_tipo_nfe)

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)

    return HttpResponse(etapaProcess)


def verifica_param_datas(p_data_ini, p_data_fim):
    d_data_ini = datetime.strptime(str(p_data_ini)[:10], '%Y-%m-%d').date()
    d_data_fim = datetime.strptime(str(p_data_fim)[:10], '%Y-%m-%d').date()

    # Verifica se o intervalo das datas de parâmetro estão dentro do mesmo mês (Implica na busca do cadastro do contribuinte)
    if d_data_ini.year != d_data_fim.year \
    or d_data_ini.month != d_data_fim.month:
        etapaProcess = f"Período de {p_data_ini} a {p_data_fim} inválido!"
        loga_mensagem_erro(etapaProcess)
        raise 9


def cancela_nfe(request, p_ano, p_nome_arquivo, p_separador):
    etapaProcess = 'Carrega relação de NFes para cancelamento, a partir de arquivo .csv'
    loga_mensagem(etapaProcess)

    try:
        df_cancel = sobe_csv(p_nome_arquivo + '.csv', p_separador).drop_duplicates()

        i_ano_referencia = int(os.environ.get('ANO_REFERENCIA'))
        i_inicio_referencia = int(os.environ.get('INICIO_REFERENCIA'))
        i_fim_referencia = int(os.environ.get('FIM_REFERENCIA'))
        sData_hora_inicio = datetime.now(LOCAL_TZ).strftime('%d/%m/%Y %H:%M:%S,%f')
        d_data_ini = datetime.strptime(str(i_inicio_referencia * 100 + 1), '%Y%m%d').date()
        d_data_fim = ultimo_dia_mes(datetime.strptime(str(i_fim_referencia * 100 + 1), '%Y%m%d').date())

        if p_ano != i_ano_referencia:
            etapaProcess = f'Ano de referencia do arquivo difere do ano de processamento {i_ano_referencia} do sistema!'
            raise Exception(etapaProcess)

        nfe = NFeClasse(d_data_ini, d_data_fim)
        nfe.cancela_nfe(df_cancel)
        del nfe

        etapaProcess += ' - Processo finalizado.'
        loga_mensagem(etapaProcess)

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)

    return HttpResponse(etapaProcess)


