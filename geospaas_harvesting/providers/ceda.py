"""Code for searching JAXA GPortal data (https://archive.ceda.ac.uk/)"""
from urllib.parse import urljoin

from .base import Provider, TimeFilterMixin
from ..arguments import PathArgument, StringArgument
from ..crawlers import FTPCrawler


class CEDAProvider(TimeFilterMixin, Provider):
    """Provider for CEDA FTP server"""

    type = 'ceda'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = "ftp://anon-ftp.ceda.ac.uk"
        self.search_parameters_parser.add_arguments([
            PathArgument('directory', valid_options=(
                '/neodc/esacci/sst/data/CDR_v2/Climatology/L4/v2.1',
            )),
            StringArgument('include', default=r'\.nc$'),
        ])

    def _make_crawler(self, parameters):
        return FTPCrawler(
            urljoin(self.url, parameters['directory']),
            time_range=(parameters['start_time'], parameters['end_time']),
            username=self.username,
            password=self.password,
            include=parameters['include'],
        )
