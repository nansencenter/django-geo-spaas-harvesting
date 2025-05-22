"""Code for searching FTP repositories"""
from urllib.parse import urljoin

from .base import Provider, TimeFilterMixin
from ..arguments import PathArgument, StringArgument
from ..crawlers import FTPCrawler


class FTPProvider(TimeFilterMixin, Provider):
    """Generic FTP provider"""

    type = 'ftp'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.search_parameters_parser.add_arguments([
            StringArgument('server', required=True),
            PathArgument('directory', default='/'),
            StringArgument('include', default='.'),
        ])

    def make_crawler(self, parameters):
        return FTPCrawler(
            urljoin(parameters['server'], parameters['directory']),
            time_range=(parameters['start_time'], parameters['end_time']),
            username=parameters['username'],
            password=parameters['password'],
            include=parameters['include'],
        )
