"""Code for searching JAXA GPortal data (https://gportal.jaxa.jp/)"""
from urllib.parse import urljoin

from .base import Provider, TimeFilterMixin
from ..arguments import PathArgument, StringArgument
from ..crawlers import FTPCrawler


class GPortalProvider(TimeFilterMixin, Provider):
    """Provider for JAXA GPortal FTP server"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = "ftp://ftp.gportal.jaxa.jp"
        self.search_parameters_parser.add_arguments([
            PathArgument('directory', valid_options=(
                '/standard/GCOM-W/GCOM-W.AMSR2/L3.SST_25/3'
            )),
            StringArgument('include', default=r'\.h5$'),
        ])

    def _make_crawler(self, parameters):
        return FTPCrawler(
            urljoin(self.url, parameters['directory']),
            time_range=(parameters['start_time'], parameters['end_time']),
            username=self.username,
            password=self.password,
            include=parameters['include'],
        )
