from django.core.management import BaseCommand
from polls.views.CargaOpcaoSimplesSIMEI import carga_opcao_simples_simei_command


class Command(BaseCommand):
    def handle(self, *args, **options):
        return carga_opcao_simples_simei_command()
