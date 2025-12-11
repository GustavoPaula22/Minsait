from django.core.management import BaseCommand
from polls.views.CargaEFD import carga_efd_param


class Command(BaseCommand):
    def handle(self, *args, **options):
        return carga_efd_param()
