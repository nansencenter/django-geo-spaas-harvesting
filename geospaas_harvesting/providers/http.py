"""Code for searching FTP repositories"""
from urllib.parse import urljoin

from .base import Provider, TimeFilterMixin
from ..arguments import PathArgument, StringArgument
from ..crawlers import HTMLDirectoryCrawler


class HTTPProvider(TimeFilterMixin, Provider):
    """Generic HTTP directory provider"""

    type = 'http'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.search_parameters_parser.add_arguments([
            StringArgument('url', required=True),
            StringArgument('include', default='.'),
        ])

    def _make_crawler(self, parameters):
        return HTMLDirectoryCrawler(
            parameters['url'],
            time_range=(parameters['start_time'], parameters['end_time']),
            username=self.username,
            password=self.password,
            include=parameters['include'])
