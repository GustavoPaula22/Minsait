from django.core.management import BaseCommand
from polls.views.CargaNfe import carga_nfe_param


class Command(BaseCommand):
    def handle(self, *args, **options):
        return carga_nfe_param(10)
