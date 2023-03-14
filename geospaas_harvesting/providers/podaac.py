"""Code for searching PO.DAAC data (https://opendap.jpl.nasa.gov/opendap)"""
from .base import Provider, TimeFilterMixin
from ..arguments import StringArgument
from ..crawlers import OpenDAPCrawler


class PODAACProvider(TimeFilterMixin, Provider):
    """Provider for PODAAC's OpenDAP"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = 'https://opendap.jpl.nasa.gov/opendap'
        self.search_parameters_parser.add_arguments([
            StringArgument('directory', required=True),
            StringArgument('include', default=r'\.nc$'),
        ])

    def _make_crawler(self, parameters):
        return OpenDAPCrawler(
            '/'.join((self.url, parameters['directory'])),
            time_range=(parameters['start_time'], parameters['end_time']),
            include=parameters['include'],
            max_threads=30
        )
