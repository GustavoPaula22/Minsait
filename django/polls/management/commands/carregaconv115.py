from django.core.management import BaseCommand

from polls.views.CargaConv115 import carga_conv115_param


class Command(BaseCommand):
    def handle(self, *args, **options):
        return carga_conv115_param()
