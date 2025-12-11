from django.core.management import BaseCommand
from polls.views.CargaCTe import carga_cte_command


class Command(BaseCommand):
    def handle(self, *args, **options):
        return carga_cte_command()
