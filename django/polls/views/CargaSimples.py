from funcoes.utilitarios import *
from negocio.Simples import SimplesClasse


def carga_simples(request):
    etapaProcess = f'Processa carga de todos os arquivos do Simples Nacional contidos no diretório.'
    return carga_simples_command()


def carga_simples_arquivo(request, p_endereco_arquivo):
    etapaProcess = f'Processa carga do arquivos {p_endereco_arquivo} do Simples Nacional.'
    return carga_simples_command(p_endereco_arquivo)


def carga_simples_command(p_arquivo=None) -> str:
    etapaProcess = f"class {__name__} - def carga_simples_command"
    # loga_mensagem(etapaProcess)

    try:
        simples = SimplesClasse()
        etapaProcess = simples.carga_simples(p_arquivo)
        del simples

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        etapaProcess = 9

    return etapaProcess

