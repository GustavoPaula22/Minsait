from django.shortcuts import render

from funcoes.utilitarios import loga_mensagem_erro, loga_mensagem
from negocio.Param import ParamClasse


def lista_parametros_cadastrados(request):
    etapaProcess = 'Lista Parâmetros Cadastrados na Tabela Oracle.'
    # loga_mensagem(etapaProcess)

    try:
        param = ParamClasse()
        parametros = param.lista_parametros_cadastrados()
        del param

        return render(request, 'lista_parametros.html', {'parametros': parametros})

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
