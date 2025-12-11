from django.core.management import BaseCommand
from polls.views.ProcessaAcerto import processa_acerto_command


class Command(BaseCommand):
    def handle(self, *args, **options):
        return processa_acerto_command()
