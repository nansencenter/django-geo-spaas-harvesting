"""Code for searching CMEMS data (https://marine.copernicus.eu/)"""
from urllib.parse import urljoin

from .base import Provider, TimeFilterMixin
from ..arguments import  ChoiceArgument, PathArgument, StringArgument
from ..crawlers import FTPCrawler


class CMEMSFTPProvider(TimeFilterMixin, Provider):
    """Provider for CMEMS' FTP servers"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = "ftp://{server}.cmems-du.eu"
        self.search_parameters_parser.add_arguments([
            ChoiceArgument('server', valid_options=('nrt', 'my'), default='nrt'),
            PathArgument('directory', valid_options=(
                '/Core/GLOBAL_ANALYSIS_FORECAST_PHY_001_024',
                '/Core/MULTIOBS_GLO_PHY_NRT_015_003',
                '/Core/SEALEVEL_GLO_PHY_L4_NRT_OBSERVATIONS_008_046',
                '/Core/IBI_ANALYSISFORECAST_PHY_005_001',
                '/Core/MEDSEA_ANALYSISFORECAST_PHY_006_013',
                '/Core/ARCTIC_ANALYSIS_FORECAST_PHYS_002_001_a',
                '/Core/ARCTIC_MULTIYEAR_PHY_002_003',
                '/Core/ARCTIC_ANALYSISFORECAST_BGC_002_004',
                '/Core/ARCTIC_ANALYSISFORECAST_PHY_002_001',
            )),
            StringArgument('include', default=r'\.nc$'),
        ])

    def _make_crawler(self, parameters):
        url = self.url.format(server=parameters['server'])
        return FTPCrawler(
            urljoin(url, parameters['directory']),
            time_range=(parameters['start_time'], parameters['end_time']),
            username=self.username,
            password=self.password,
            include=parameters['include'],
        )
