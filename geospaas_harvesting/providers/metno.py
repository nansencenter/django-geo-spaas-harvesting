"""Code for searching MET NO data (https://thredds.met.no/thredds)"""
from .base import Provider, TimeFilterMixin
from ..arguments import StringArgument
from ..crawlers import ThreddsCrawler


class METNOProvider(TimeFilterMixin, Provider):
    """Provider for MET NO's Thredds"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = 'https://thredds.met.no/thredds'
        self.search_parameters_parser.add_arguments([
            StringArgument('directory', required=True),
            StringArgument('include'),
        ])

    def _make_crawler(self, parameters):
        return ThreddsCrawler(
            '/'.join((self.url, parameters['directory'])),
            time_range=(parameters['start_time'], parameters['end_time']),
            include=parameters.get('include'),
            max_threads=30
        )
