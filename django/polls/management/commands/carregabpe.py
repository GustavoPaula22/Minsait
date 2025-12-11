from django.core.management import BaseCommand
from polls.views.CargaBPe import carga_bpe_command


class Command(BaseCommand):
    def handle(self, *args, **options):
        return carga_bpe_command()
