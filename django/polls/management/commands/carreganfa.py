from django.core.management import BaseCommand
from polls.views.CargaNFA import carga_nfa_param


class Command(BaseCommand):
    def handle(self, *args, **options):
        return carga_nfa_param()
