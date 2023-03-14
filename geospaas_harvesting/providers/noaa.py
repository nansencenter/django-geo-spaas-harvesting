"""Code for searching NOAA data (https://www.noaa.gov/)"""
from urllib.parse import urljoin

from .base import Provider, TimeFilterMixin
from ..arguments import ChoiceArgument, PathArgument, StringArgument
from ..crawlers import FTPCrawler


class NOAAProvider(TimeFilterMixin, Provider):
    """Provider for NOAA FTP servers"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = "ftp://{server}.ncep.noaa.gov"
        self.search_parameters_parser.add_arguments([
            ChoiceArgument('server', valid_options=('ftp.opc', 'ftpprd'), default='ftp.opc'),
            PathArgument('directory', valid_options=(
                '/grids/operational/GLOBALHYCOM/Navy',
                '/pub/data/nccf/com/rtofs/prod',
            )),
            StringArgument('include', default=r'\.nc(\.gz)?$'),
        ])

    def _make_crawler(self, parameters):
        url = self.url.format(server=parameters['server'])
        return FTPCrawler(
            urljoin(url, parameters['directory']),
            time_range=(parameters['start_time'], parameters['end_time']),
            include=parameters['include'],
        )
