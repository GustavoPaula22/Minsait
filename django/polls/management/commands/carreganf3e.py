from django.core.management import BaseCommand
from polls.views.CargaNF3e import carga_nf3e_param


class Command(BaseCommand):
    def handle(self, *args, **options):
        return carga_nf3e_param()
