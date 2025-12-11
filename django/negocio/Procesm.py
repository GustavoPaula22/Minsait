from datetime import datetime
from funcoes.constantes import EnumStatusProcessamento
from funcoes.utilitarios import loga_mensagem_erro, loga_mensagem
from ipm.settings import LOCAL_TZ
from persistencia.Oraprd import Oraprd
from polls.models import Processamento


class ProcesmClasse:
    etapaProcess = f"class {__name__} - class ProcesmClasse."
    # loga_mensagem(etapaProcess)

    def __init__(self):
        self.db = Oraprd()

    def iniciar_processamento(self, p_data_inicio, p_data_fim, tipo_procesm=int) -> Processamento:
        etapaProcess = f"class {self.__class__.__name__} - def iniciar_processamento:" + str(tipo_procesm)
        # loga_mensagem(etapaProcess)

        try:
            procesm = Processamento(data_inicio_procesm_indice=datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S'),
                                    stat_procesm_indice=EnumStatusProcessamento.iniciado.value,
                                    desc_observacao_procesm_indice='Processamento iniciado.',
                                    codg_tipo_procesm_indice=tipo_procesm,
                                    data_inicio_periodo_indice=p_data_inicio,
                                    data_fim_periodo_indice=p_data_fim,
                                    )

            novo_procesm = self.db.insert_processamento(procesm)
            etapaProcess += f" - ID_PROCESM_INDICE={novo_procesm.id_procesm_indice}"

            return novo_procesm

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def atualizar_situacao_processamento(self, procesm: Processamento):
        etapaProcess = f"class {self.__class__.__name__} - atualizar_situacao_processamento: {procesm.id_procesm_indice}."
        # loga_mensagem(etapaProcess)

        try:
            linhasAtualiz = self.db.update_processamento(procesm)
            etapaProcess += f" - Alterado a situação do processamento com ID = {procesm.id_procesm_indice} - {linhasAtualiz} atualizadas."
            # loga_mensagem(etapaProcess)

            return None

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise
