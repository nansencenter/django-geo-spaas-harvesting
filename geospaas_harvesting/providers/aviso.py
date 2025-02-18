"""Code for searching AVISO data (https://tds.aviso.altimetry.fr/thredds)"""
from .base import Provider, TimeFilterMixin
from ..arguments import StringArgument
from ..crawlers import ThreddsCrawler


class AVISOProvider(TimeFilterMixin, Provider):
    """Provider for AVISO's Thredds"""

    type = 'aviso'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = 'https://tds.aviso.altimetry.fr/thredds'
        self.search_parameters_parser.add_arguments([
            StringArgument('directory', required=True),
            StringArgument('include'),
        ])

    def _make_crawler(self, parameters):
        return ThreddsCrawler(
            '/'.join((self.url, parameters['directory'].lstrip('/'))),
            time_range=(parameters['start_time'], parameters['end_time']),
            include=parameters.get('include'),
            max_threads=30,
            username=self.username,
            password=self.password,
        )
