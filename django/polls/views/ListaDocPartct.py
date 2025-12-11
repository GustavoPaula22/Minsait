import csv
import os
from datetime import datetime

from django.shortcuts import render, redirect
from django.urls import reverse
from django.http import HttpResponse

from funcoes.constantes import EnumTipoDocumento
from funcoes.utilitarios import loga_mensagem_erro, loga_mensagem, dividir_dia_em_horas, baixa_csv
from negocio.DocNaoPartct import DocNaoPartctClasse
from negocio.DocPartct import DocPartctClasse
from negocio.ItemNaoPartct import ItemNaoPartctClasse

p_inicio_referencia = os.getenv('INICIO_REFERENCIA')
p_fim_referencia = os.getenv('FIM_REFERENCIA')


def menu_consulta_documentos(request):
    return render(request, 'Menu.html')


def lista_documentos_partct_periodo_ie(request, p_tipo_doc, p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida):
    etapaProcess = f'Lista os documentos que participam do calculo por Período e Inscrição - {p_data_inicio} a {p_data_fim} - {p_ie_entrada} - {p_ie_saida}'
    # loga_mensagem(etapaProcess)

    try:
        doc = DocPartctClasse()
        docs = doc.lista_doc_partct(p_inicio_referencia, p_fim_referencia, p_tipo_doc, p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida)
        del doc

        titulo = monta_titulo_relatorio('DocPartct', p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida, ' ')

        return render(request, 'lista_doc_partct.html', {'documentos': docs, 'titulo': titulo})

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        return HttpResponse(f"Erro ao processar a solicitação: {err}", status=500)


def lista_documentos_partct_periodo(request, p_tipo_doc, p_data_inicio, p_data_fim):
    etapaProcess = f'Lista os documentos que participam do calculo no periodo de {p_data_inicio} a {p_data_fim}.'
    # loga_mensagem(etapaProcess)

    try:
        doc = DocPartctClasse()
        docs = doc.lista_doc_partct_periodo(p_inicio_referencia, p_fim_referencia, p_tipo_doc, p_data_inicio, p_data_fim)
        del doc

        titulo = monta_titulo_relatorio('DocPartct', p_data_inicio, p_data_fim, ' ', ' ', ' ')

        return render(request, 'lista_doc_partct.html', {'documentos': docs, 'titulo': titulo})

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        return HttpResponse(f"Erro ao processar a solicitação: {err}", status=500)


def lista_documentos_partct_ie(request, p_tipo_doc, p_ie_entrada, p_ie_saida):
    etapaProcess = f'Lista os documentos que participam do calculo por Inscrição - {p_ie_entrada} - {p_ie_saida}'
    # loga_mensagem(etapaProcess)

    try:
        doc = DocPartctClasse()
        docs = doc.lista_doc_partct_inscricao(p_inicio_referencia, p_fim_referencia, p_tipo_doc, p_ie_entrada, p_ie_saida)
        del doc

        titulo = monta_titulo_relatorio('DocPartct', ' ', ' ', p_ie_entrada, p_ie_saida, ' ')

        return render(request, 'lista_doc_partct.html', {'documentos': docs, 'titulo': titulo})

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        return HttpResponse(f"Erro ao processar a solicitação: {err}", status=500)


def lista_documentos_partct_chave(request, p_tipo_doc, p_chave):
    etapaProcess = f'Lista os documentos que participam do calculo por Chave Eletrônica - {p_chave}'
    # loga_mensagem(etapaProcess)

    try:
        doc = DocPartctClasse()
        docs = doc.lista_doc_partct_chave(p_inicio_referencia, p_fim_referencia, p_tipo_doc, p_chave)
        del doc

        titulo = monta_titulo_relatorio('DocPartct', ' ', ' ', ' ', ' ', p_chave)

        return render(request, 'lista_doc_partct.html', {'documentos': docs, 'titulo': titulo})

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        return HttpResponse(f"Erro ao processar a solicitação: {err}", status=500)


def lista_documentos_nao_partct_periodo_ie(request, p_tipo_doc, p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida):
    etapaProcess = f'Lista os documentos que não participam do calculo por Período e Inscrição - {p_data_inicio} a {p_data_fim} - {p_ie_entrada} - {p_ie_saida}'
    # loga_mensagem(etapaProcess)

    try:
        doc = DocNaoPartctClasse()
        docs = doc.lista_doc_nao_partct(p_inicio_referencia, p_fim_referencia, p_tipo_doc, p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida)
        del doc

        titulo = monta_titulo_relatorio('DocNaoPartct', p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida, ' ')

        return render(request, 'lista_doc_nao_partct.html', {'documentos': docs, 'titulo': titulo})

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        return HttpResponse(f"Erro ao processar a solicitação: {err}", status=500)


def lista_documentos_nao_partct_periodo(request, p_tipo_doc, p_data_inicio, p_data_fim):
    etapaProcess = f'Lista os documentos que não participam do calculo no periodo de {p_data_inicio} a {p_data_fim}.'
    # loga_mensagem(etapaProcess)

    try:
        doc = DocNaoPartctClasse()
        docs = doc.lista_doc_nao_partct_periodo(p_inicio_referencia, p_fim_referencia, p_tipo_doc, p_data_inicio, p_data_fim)
        del doc

        titulo = monta_titulo_relatorio('DocNaoPartct', p_data_inicio, p_data_fim, ' ', ' ', ' ')

        return render(request, 'lista_doc_nao_partct.html', {'documentos': docs, 'titulo': titulo})

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        return HttpResponse(f"Erro ao processar a solicitação: {err}", status=500)


def lista_documentos_nao_partct_ie(request, p_tipo_doc, p_ie_entrada, p_ie_saida):
    etapaProcess = f'Lista os documentos que não participam do calculo por Inscrição - {p_ie_entrada} - {p_ie_saida}'
    # loga_mensagem(etapaProcess)

    try:
        doc = DocNaoPartctClasse()
        docs = doc.lista_doc_nao_partct_inscricao(p_inicio_referencia, p_fim_referencia, p_tipo_doc, p_ie_entrada, p_ie_saida)
        del doc

        titulo = monta_titulo_relatorio('DocNaoPartct', ' ', ' ', p_ie_entrada, p_ie_saida, ' ')

        return render(request, 'lista_doc_nao_partct.html', {'documentos': docs, 'titulo': titulo})

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        return HttpResponse(f"Erro ao processar a solicitação: {err}", status=500)


def lista_documentos_nao_partct_chave(request, p_tipo_doc, p_chave):
    etapaProcess = f'Lista os documentos que não participam do calculo por Chave Eletrônica - {p_chave}'
    # loga_mensagem(etapaProcess)

    try:
        doc = DocNaoPartctClasse()
        docs = doc.lista_doc_nao_partct_chave(p_inicio_referencia, p_fim_referencia, p_tipo_doc, p_chave)
        del doc

        titulo = monta_titulo_relatorio('DocNaoPartct', ' ', ' ', ' ', ' ', p_chave)

        return render(request, 'lista_doc_nao_partct.html', {'documentos': docs, 'titulo': titulo})

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        return HttpResponse(f"Erro ao processar a solicitação: {err}", status=500)


def lista_itens_nao_partct_periodo_ie(request, p_tipo_doc, p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida):
    etapaProcess = f'Lista os itens que não participam do calculo por Período e Inscrição - {p_data_inicio} a {p_data_fim} - {p_ie_entrada} - {p_ie_saida}'
    # loga_mensagem(etapaProcess)

    try:
        doc = ItemNaoPartctClasse()
        docs = doc.lista_item_nao_partct(p_inicio_referencia, p_fim_referencia, p_tipo_doc, p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida)
        del doc

        titulo = monta_titulo_relatorio('ItemNaoPartct', p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida, ' ')

        return render(request, 'lista_item_nao_partct.html', {'documentos': docs, 'titulo': titulo})

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        return HttpResponse(f"Erro ao processar a solicitação: {err}", status=500)


def lista_itens_nao_partct_periodo(request, p_tipo_doc, p_data_inicio, p_data_fim):
    etapaProcess = f'Lista os itens que não participam do calculo no periodo de {p_data_inicio} a {p_data_fim}.'
    # loga_mensagem(etapaProcess)

    try:
        doc = ItemNaoPartctClasse()
        docs = doc.lista_item_nao_partct_periodo(p_inicio_referencia, p_fim_referencia, p_tipo_doc, p_data_inicio, p_data_fim)
        del doc

        titulo = monta_titulo_relatorio('ItemNaoPartct', p_data_inicio, p_data_fim, ' ', ' ', ' ')

        return render(request, 'lista_item_nao_partct.html', {'documentos': docs, 'titulo': titulo})

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        return HttpResponse(f"Erro ao processar a solicitação: {err}", status=500)


def lista_itens_nao_partct_ie(request, p_tipo_doc, p_ie_entrada, p_ie_saida):
    etapaProcess = f'Lista os itens que não participam do calculo por Inscrição - {p_ie_entrada} - {p_ie_saida}'
    # loga_mensagem(etapaProcess)

    try:
        doc = ItemNaoPartctClasse()
        docs = doc.lista_item_nao_partct_inscricao(p_inicio_referencia, p_fim_referencia, p_tipo_doc, p_ie_entrada, p_ie_saida)
        del doc

        titulo = monta_titulo_relatorio('ItemNaoPartct', ' ', ' ', p_ie_entrada, p_ie_saida, ' ')

        return render(request, 'lista_item_nao_partct.html', {'documentos': docs, 'titulo': titulo})

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        return HttpResponse(f"Erro ao processar a solicitação: {err}", status=500)


def lista_itens_nao_partct_chave(request, p_tipo_doc, p_chave):
    etapaProcess = f'Lista os itens que não participam do calculo por Chave Eletrônica - {p_chave}'
    # loga_mensagem(etapaProcess)

    try:
        doc = ItemNaoPartctClasse()
        docs = doc.lista_item_nao_partct_chave(p_inicio_referencia, p_fim_referencia, p_tipo_doc, p_chave)
        del doc

        titulo = monta_titulo_relatorio('ItemNaoPartct', ' ', ' ', ' ', ' ', p_chave)

        return render(request, 'lista_item_nao_partct.html', {'documentos': docs, 'titulo': titulo})

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        return HttpResponse(f"Erro ao processar a solicitação: {err}", status=500)


def lista_nfe_simples(request, p_tipo, p_data_inicio, p_data_fim):
    return HttpResponse(lista_nfe_simples_periodo(p_tipo, p_data_inicio, p_data_fim))


def lista_nfe_simples_periodo(p_tipo, p_data_inicio, p_data_fim):
    etapaProcess = f'Lista as nf-es para estudo do Simples Nacional no periodo de {p_data_inicio} a {p_data_fim} - Tipo da consulta - {p_tipo}'
    loga_mensagem(etapaProcess)

    try:
        doc = DocPartctClasse()
        docs = doc.lista_nfe_simples(p_tipo, p_data_inicio, p_data_fim)
        del doc

        titulo = 'Relação de NF-es para estudo do Simples Nacional gerada.'

        return titulo

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        return HttpResponse(f"Erro ao processar a solicitação: {err}", status=500)


def monta_titulo_relatorio(p_tipo_consulta, p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida, p_chave):
    etapaProcess = f'Monta o Título do relatório solicitado. - {p_data_inicio} a {p_data_fim} - {p_ie_entrada} - {p_ie_saida} - {p_chave}'
    # loga_mensagem(etapaProcess)

    try:
        if p_tipo_consulta == 'DocPartct':
            titulo = "Relação de Documentos Participantes do Cálculo do IPM"
        elif p_tipo_consulta == 'DocNaoPartct':
            titulo = "Relação de Documentos Não Participantes do Cálculo do IPM"
        elif p_tipo_consulta == 'ItemNaoPartct':
            titulo = "Relação de Itens Não Participantes do Cálculo do IPM"
        else:
            titulo = "Relação de NÃO IDENTIFICADA do Cálculo do IPM"

        if p_data_inicio != ' ' or p_data_fim != ' ':
            if p_data_inicio == ' ' or p_data_fim == ' ':
                if p_data_inicio != ' ':
                    dData_hora_inicio = datetime.strptime(p_data_inicio, '%Y-%m-%d %H:%M:%S')
                    sData_hora_inicio = dData_hora_inicio.strftime('%d/%m/%Y %H:%M:%S')
                else:
                    dData_hora_inicio = datetime.strptime(p_data_fim, '%Y-%m-%d %H:%M:%S')
                    sData_hora_inicio = dData_hora_inicio.strftime('%d/%m/%Y %H:%M:%S')
                titulo += f" - Data: {sData_hora_inicio}"
            else:
                dData_hora_inicio = datetime.strptime(p_data_inicio, '%Y-%m-%d %H:%M:%S')
                sData_hora_inicio = dData_hora_inicio.strftime('%d/%m/%Y %H:%M:%S')
                dData_hora_fim = datetime.strptime(p_data_fim, '%Y-%m-%d %H:%M:%S')
                sData_hora_fim = dData_hora_fim.strftime('%d/%m/%Y %H:%M:%S')
                titulo += f" - Período de {sData_hora_inicio} a {sData_hora_fim}"

        if p_ie_entrada != ' ':
            titulo += f" - Inscrição de Entrada: {p_ie_entrada}"

        if p_ie_saida != ' ':
            titulo += f" - Inscrição de Saída: {p_ie_saida}"

        if p_chave != ' ':
            titulo += f" - Chave Eletrônica - {p_chave}"

        return titulo

    except Exception as err:
        etapaProcess += " - ERRO - " + str(err)
        loga_mensagem_erro(etapaProcess)
        return HttpResponse(f"Erro ao processar a solicitação: {err}", status=500)


def processa_consulta_documentos(request):
    etapaProcess = f'Recebe parâmetros para consulta a Documentos Participantes do Cálculo do IPM.'
    loga_mensagem(etapaProcess)

    try:
        tipo_documento = request.GET.get('tipo_documento')
        tipo_consulta = request.GET.get('tipo_consulta')

        data_inicio = request.GET.get('data_inicio')
        if len(data_inicio) > 0:
            if len(data_inicio) == 19:
                data_formatada = datetime.strptime(data_inicio, '%Y-%m-%dT%H:%M:%S')
            else:
                data_formatada = datetime.strptime(data_inicio, '%Y-%m-%dT%H:%M')
            data_inicio = data_formatada
        else:
            data_inicio = None

        data_fim = request.GET.get('data_fim')
        if len(data_fim) > 0:
            if len(data_fim) == 19:
                data_formatada = datetime.strptime(data_fim, '%Y-%m-%dT%H:%M:%S')
            else:
                data_formatada = datetime.strptime(data_fim, '%Y-%m-%dT%H:%M')
            data_fim = data_formatada
        else:
            data_fim = None

        p_ie_entrada = request.GET.get('ie_entrada')
        if not p_ie_entrada:
            p_ie_entrada = ' '
        p_ie_saida = request.GET.get('ie_saida')
        if not p_ie_saida:
            p_ie_saida = ' '
        chave_eletr = request.GET.get('chave_eletr')

        retorno = consiste_filtro_pesquisa(data_inicio, data_fim, p_ie_entrada, p_ie_saida, chave_eletr)

        if retorno[:4] == 'ERRO':
            return HttpResponse(retorno, status=400)
        elif retorno[:2] == 'OK':
            if tipo_documento == 'NFeGerada':
                p_tipo_doc = EnumTipoDocumento.NFe.value
            elif tipo_documento == 'NFeRecebida':
                p_tipo_doc = EnumTipoDocumento.NFeRecebida.value
            elif tipo_documento == 'NF3e':
                p_tipo_doc = EnumTipoDocumento.NF3e.value
            elif tipo_documento == 'NFA':
                p_tipo_doc = EnumTipoDocumento.NFA.value
            elif tipo_documento == 'EFD':
                p_tipo_doc = EnumTipoDocumento.EFD.value
            elif tipo_documento == 'Conv115':
                p_tipo_doc = EnumTipoDocumento.TelecomConv115.value
            elif tipo_documento == 'BPe':
                p_tipo_doc = EnumTipoDocumento.BPe.value
            elif tipo_documento == 'CTe':
                p_tipo_doc = EnumTipoDocumento.CTe.value
            elif tipo_documento == 'PAT':
                p_tipo_doc = EnumTipoDocumento.PAT.value
            else:
                p_tipo_doc = 0

            if tipo_consulta == 'DocPartct':
                if retorno[7:] == 'chave':
                    url = reverse('lista_documentos_partct_chave', args=[p_tipo_doc, chave_eletr])
                elif retorno[7:] == 'periodo_ie':
                    url = reverse('lista_documentos_partct_periodo_ie', args=[p_tipo_doc, data_inicio, data_fim, p_ie_entrada, p_ie_saida])
                elif retorno[7:] == 'periodo':
                    url = reverse('lista_documentos_partct_periodo', args=[p_tipo_doc, data_inicio, data_fim])
                elif retorno[7:] == 'ie':
                    url = reverse('lista_documentos_partct_ie', args=[p_tipo_doc, p_ie_entrada, p_ie_saida])

            elif tipo_consulta == 'DocNaoPartct':
                if retorno[7:] == 'chave':
                    url = reverse('lista_documentos_nao_partct_chave', args=[p_tipo_doc, chave_eletr])
                elif retorno[7:] == 'periodo_ie':
                    url = reverse('lista_documentos_nao_partct_periodo_ie', args=[p_tipo_doc, data_inicio, data_fim, p_ie_entrada, p_ie_saida])
                elif retorno[7:] == 'periodo':
                    url = reverse('lista_documentos_nao_partct_periodo', args=[p_tipo_doc, data_inicio, data_fim])
                elif retorno[7:] == 'ie':
                    url = reverse('lista_documentos_nao_partct_ie', args=[p_tipo_doc, p_ie_entrada, p_ie_saida])

            elif tipo_consulta == 'ItemNaoPartct':
                if retorno[7:] == 'chave':
                    url = reverse('lista_itens_nao_partct_chave', args=[p_tipo_doc, chave_eletr])
                elif retorno[7:] == 'periodo_ie':
                    url = reverse('lista_itens_nao_partct_periodo_ie', args=[p_tipo_doc, data_inicio, data_fim, p_ie_entrada, p_ie_saida])
                elif retorno[7:] == 'periodo':
                    url = reverse('lista_itens_nao_partct_periodo', args=[p_tipo_doc, data_inicio, data_fim])
                elif retorno[7:] == 'ie':
                    url = reverse('lista_itens_nao_partct_ie', args=[p_tipo_doc, p_ie_entrada, p_ie_saida])
            else:
                return HttpResponse("Erro ao processar o tipo da consulta.", status=400)

            return redirect(url)

    except Exception as e:
        return HttpResponse(f"Erro ao processar a solicitação: {e}", status=500)


def baixa_documentos_partct(request):

    try:
        tipo_documento = request.GET.get('tipo_documento')
        tipo_consulta = request.GET.get('tipo_consulta')

        data_inicio = request.GET.get('data_inicio')
        if len(data_inicio) > 0:
            if len(data_inicio) == 19:
                data_formatada = datetime.strptime(data_inicio, '%Y-%m-%dT%H:%M:%S')
            else:
                data_formatada = datetime.strptime(data_inicio, '%Y-%m-%dT%H:%M')
            data_inicio = data_formatada
        else:
            data_inicio = None

        data_fim = request.GET.get('data_fim')
        if len(data_fim) > 0:
            if len(data_fim) == 19:
                data_formatada = datetime.strptime(data_fim, '%Y-%m-%dT%H:%M:%S')
            else:
                data_formatada = datetime.strptime(data_fim, '%Y-%m-%dT%H:%M')
            data_fim = data_formatada
        else:
            data_fim = None

        p_ie_entrada = request.GET.get('ie_entrada')
        if not p_ie_entrada:
            p_ie_entrada = ' '
        p_ie_saida = request.GET.get('ie_saida')
        if not p_ie_saida:
            p_ie_saida = ' '
        chave_eletr = request.GET.get('chave_eletr')

        retorno = consiste_filtro_pesquisa(data_inicio, data_fim, p_ie_entrada, p_ie_saida, chave_eletr)

        if retorno[:4] == 'ERRO':
            return HttpResponse(retorno, status=400)
        elif retorno[:2] == 'OK':
            if tipo_documento == 'NFeGerada':
                p_tipo_doc = EnumTipoDocumento.NFe.value
            elif tipo_documento == 'NFeRecebida':
                p_tipo_doc = EnumTipoDocumento.NFeRecebida.value
            elif tipo_documento == 'NF3e':
                p_tipo_doc = EnumTipoDocumento.NF3e.value
            elif tipo_documento == 'NFA':
                p_tipo_doc = EnumTipoDocumento.NFA.value
            elif tipo_documento == 'EFD':
                p_tipo_doc = EnumTipoDocumento.EFD.value
            elif tipo_documento == 'Conv115':
                p_tipo_doc = EnumTipoDocumento.TelecomConv115.value
            elif tipo_documento == 'BPe':
                p_tipo_doc = EnumTipoDocumento.BPe.value
            elif tipo_documento == 'CTe':
                p_tipo_doc = EnumTipoDocumento.CTe.value
            elif tipo_documento == 'PAT':
                p_tipo_doc = EnumTipoDocumento.PAT.value
            else:
                p_tipo_doc = 0

            if tipo_consulta == 'DocPartct':
                doc = DocPartctClasse()

                if retorno[7:] == 'chave':
                    docs = doc.lista_doc_partct_chave(p_inicio_referencia, p_fim_referencia, p_tipo_doc, chave_eletr)
                    nome_csv = f"DocPartct_{p_tipo_doc}_Chave_{chave_eletr}.csv"

                elif retorno[7:] == 'periodo_ie':
                    docs = doc.lista_doc_partct(p_inicio_referencia, p_fim_referencia, p_tipo_doc, data_inicio, data_fim, p_ie_entrada, p_ie_saida)
                    nome_csv = f"DocPartct_{p_tipo_doc}_Periodo_{data_inicio}_{data_fim}_IeEnt_{p_ie_entrada}_IeSai_{p_ie_saida}.csv"

                elif retorno[7:] == 'periodo':
                    docs = doc.lista_doc_partct_periodo(p_inicio_referencia, p_fim_referencia, p_tipo_doc, data_inicio, data_fim)
                    nome_csv = f"DocPartct_{p_tipo_doc}_Periodo_{data_inicio}_{data_fim}.csv"

                elif retorno[7:] == 'ie':
                    docs = doc.lista_doc_partct_inscricao(p_inicio_referencia, p_fim_referencia, p_tipo_doc, p_ie_entrada, p_ie_saida)
                    nome_csv = f"DocPartct_{p_tipo_doc}_IeEnt_{p_ie_entrada}_IeSai_{p_ie_saida}.csv"

                else:
                    docs = None

            elif tipo_consulta == 'DocNaoPartct':
                doc = DocNaoPartctClasse()

                if retorno[7:] == 'chave':
                    docs = doc.lista_doc_nao_partct_chave(p_inicio_referencia, p_fim_referencia, p_tipo_doc, chave_eletr)
                    nome_csv = f"DocNaoPartct_{p_tipo_doc}_Chave_{chave_eletr}.csv"

                elif retorno[7:] == 'periodo_ie':
                    docs = doc.lista_doc_nao_partct(p_inicio_referencia, p_fim_referencia, p_tipo_doc, data_inicio, data_fim, p_ie_entrada, p_ie_saida)
                    nome_csv = f"DocNaoPartct_{p_tipo_doc}_Periodo_{data_inicio}_{data_fim}_IeEnt_{p_ie_entrada}_IeSai_{p_ie_saida}.csv"

                elif retorno[7:] == 'periodo':
                    docs = doc.lista_doc_nao_partct_periodo(p_inicio_referencia, p_fim_referencia, p_tipo_doc, data_inicio, data_fim)
                    nome_csv = f"DocNaoPartct_{p_tipo_doc}_Periodo_{data_inicio}_{data_fim}.csv"

                elif retorno[7:] == 'ie':
                    docs = doc.lista_doc_nao_partct_inscricao(p_inicio_referencia, p_fim_referencia, p_tipo_doc, p_ie_entrada, p_ie_saida)
                    nome_csv = f"DocNaoPartct_{p_tipo_doc}_IeEnt_{p_ie_entrada}_IeSai_{p_ie_saida}.csv"

                else:
                    docs = None

            elif tipo_consulta == 'ItemNaoPartct':
                doc = ItemNaoPartctClasse()

                if retorno[7:] == 'chave':
                    docs = doc.lista_item_nao_partct_chave(p_inicio_referencia, p_fim_referencia, p_tipo_doc, chave_eletr)
                    nome_csv = f"ItemNaoPartct_{p_tipo_doc}_Chave_{chave_eletr}.csv"

                elif retorno[7:] == 'periodo_ie':
                    docs = doc.lista_item_nao_partct(p_inicio_referencia, p_fim_referencia, p_tipo_doc, data_inicio, data_fim, p_ie_entrada, p_ie_saida)
                    nome_csv = f"ItemNaoPartct_{p_tipo_doc}_Periodo_{data_inicio}_{data_fim}_IeEnt_{p_ie_entrada}_IeSai_{p_ie_saida}.csv"

                elif retorno[7:] == 'periodo':
                    docs = doc.lista_item_nao_partct_periodo(p_inicio_referencia, p_fim_referencia, p_tipo_doc, data_inicio, data_fim)
                    nome_csv = f"ItemNaoPartct_{p_tipo_doc}_Periodo_{data_inicio}_{data_fim}.csv"

                elif retorno[7:] == 'ie':
                    docs = doc.lista_item_nao_partct_inscricao(p_inicio_referencia, p_fim_referencia, p_tipo_doc, p_ie_entrada, p_ie_saida)
                    nome_csv = f"ItemNaoPartct_{p_tipo_doc}_IeEnt_{p_ie_entrada}_IeSai_{p_ie_saida}.csv"

                else:
                    docs = None

            else:
                return HttpResponse("Erro ao processar o tipo da consulta.", status=400)

        del doc

        # Crie um CSV na memória
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = f'attachment; filename="{nome_csv}"'

        if docs:
            # Supondo que docs seja uma lista de dicionários
            fieldnames = docs[0].keys()
            writer = csv.DictWriter(response, fieldnames=fieldnames, delimiter='|')
            writer.writeheader()
            for doc in docs:
                writer.writerow(doc)

        return response

    except Exception as e:
        return HttpResponse(f"Erro ao processar a solicitação: {e}", status=500)


def consiste_filtro_pesquisa(p_data_inicio, p_data_fim, p_ie_entrada, p_ie_saida, p_chave_eletr):
    etapaProcess = f'Consiste os filtros para pesquisa à base de dados.'
    loga_mensagem(etapaProcess)

    try:
        if len(p_chave_eletr) == 44:
            return "OK   - chave"
        elif len(p_chave_eletr) > 0:
            return "ERRO - O parâmetro 'Chave Eletrônica' não foi informado corretamente. Verifique!"

        tem_inscricao = False
        if len(p_ie_entrada) == 9:
            tem_inscricao = True
        elif len(p_ie_entrada) > 0 and p_ie_entrada != ' ':
            return "ERRO - O parâmetro 'Inscrição Entrada' não foi informado corretamente. Verifique!"

        if len(p_ie_saida) == 9:
            tem_inscricao = True
        elif len(p_ie_saida) > 0 and p_ie_saida != ' ':
            return "ERRO - O parâmetro 'Inscrição Saída' não foi informado corretamente. Verifique!"

        tem_data = False
        if p_data_inicio or p_data_fim:
            tem_data = True
            if p_data_inicio and p_data_fim:
                if p_data_inicio > p_data_fim:
                    return "ERRO - A data de inicio do período é maior que a data final do período. Verifique!"

        if tem_data and tem_inscricao:
            return "OK   - periodo_ie"
        elif tem_data and not tem_inscricao:
            return "OK   - periodo"
        elif not tem_data and tem_inscricao:
            return "OK   - ie"
        else:
            return "ERRO - Ao menos 1 parâmetro de consulta deve ser informado. Verifique!"

    except Exception as e:
        return f"Erro ao processar a solicitação: {e}"
