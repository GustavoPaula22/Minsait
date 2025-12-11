# importacaoNFE_ipm.py
from django.core.management.base import BaseCommand
import polls.views.CargaNfe as cargaNfe

class Command(BaseCommand):
    help = 'Descrição do comando'

    def handle(self, *args, **kwargs):
        # Chame a função index() aqui
        result = cargaNfe.index()
        print(result)
