from datetime import date, timedelta

from funcoes.constantes import EnumParametros
from funcoes.utilitarios import loga_mensagem_erro
from funcoes.utilitarios import loga_mensagem
from negocio.Param import ParamClasse
from negocio.OpcaoSimplesSIMEI import OpcaoSimplesSIMEI


def carga_opcao_simples_simei(request):
    etapaProcess = f'Processa carga de todos os arquivos do Simples Nacional contidos no diretório.'
    return carga_opcao_simples_simei_command()


def carga_opcao_simples_simei_command():
    try:
        param = ParamClasse()
        ultimaDataProcess = param.busca_valor_parametro(EnumParametros.ultimaCargaOpcaoSimplesSIMEI.value)
        loga_mensagem(f'Data obtida no parametro ultimaCargaOpcaoSimplesSIMEI: {ultimaDataProcess}')
        # Executa o processamento desde a última data processada até 30 dias. 
        # Caso data fim seja maior que a data de hoje, usa a data de hoje como referência fim.
        dataIni = date.fromisoformat(ultimaDataProcess)
        dataFim = dataIni + timedelta(days=30)
        dataFim = dataFim if dataFim <= date.today() else date.today()

        opcaoSimplesSIMEI = OpcaoSimplesSIMEI()
        resultado_exec = opcaoSimplesSIMEI.carga_simples_smei(dataIni, dataFim)

        if resultado_exec == 0:
            param.atualizar_parametro(EnumParametros.ultimaCargaOpcaoSimplesSIMEI.value, dataFim)

            if dataFim < date.today():
                # Se a carga executou corretamente, mas ainda tem algo a executar, retorna 0....
                return "1"
            else:
                # Se a carga executou corretamente, e não tem mais dias a processar, retorna 0....
                return "0"

    except Exception as err:
        msg = f"class {__name__} - def carga_opcao_simples_simei_command - ERRO - " + str(err)
        loga_mensagem_erro(msg, err)
    
    # Se deu algum erro, retorna 9
    return "9"
