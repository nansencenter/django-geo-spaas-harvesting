# pylint: disable=protected-access
"""Test for the Copernicus Scihub provider and crawler"""
import json
import os.path
import unittest
import unittest.mock as mock
from datetime import datetime, timezone

import requests
from shapely.geometry.polygon import Point

from geospaas_harvesting.crawlers import DatasetInfo
from geospaas_harvesting.providers.copernicus_scihub import (CopernicusScihubCrawler,
                                                             CopernicusScihubProvider)


class CopernicusScihubProviderTestCase(unittest.TestCase):
    """Tests for the CopernicusScihubProvider class"""

    def test_make_crawler(self):
        """Test creating a crawler from parameters"""
        parameters = {
            'start_time': datetime(2023, 1, 1, tzinfo=timezone.utc),
            'end_time': datetime(2023, 1, 2, tzinfo=timezone.utc),
            'platformname': 'platform',
            'collection': 'collection',
            'filename': 'filename',
        }

        provider = CopernicusScihubProvider(
            name='test_scihub',
            username='user',
            password='pass')

        with mock.patch.object(provider, '_replace_location') as mock_replace_location, \
                mock.patch.object(provider, '_replace_level') as mock_replace_level:
            crawler = provider._make_crawler(parameters)

        self.assertEqual(
            crawler,
            CopernicusScihubCrawler(
                'https://apihub.copernicus.eu/apihub/search',
                time_range=(datetime(2023, 1, 1, tzinfo=timezone.utc),
                            datetime(2023, 1, 2, tzinfo=timezone.utc)),
                username='user',
                password='pass',
                search_terms={
                    'platformname': 'platform',
                    'collection': 'collection',
                    'filename': 'filename',
                }
            )
        )
        mock_replace_location.assert_called_once_with(parameters)
        mock_replace_level.assert_called_once_with(parameters)

    def test_replace_location(self):
        """Test replacing the location parameter with a
        Scihub-compatible format
        """
        parameters = {'location': Point(1, 2)}
        CopernicusScihubProvider()._replace_location(parameters)
        self.assertDictEqual(parameters, {'footprint': '"intersects(POINT (1 2))"'})

    def test_replace_level_without_raw_query(self):
        """Test replacing the level parameter with a
        Scihub-compatible format without existing raw query
        """
        parameters = {'level': 'L1'}
        CopernicusScihubProvider()._replace_level(parameters)
        self.assertDictEqual(parameters, {'raw_query': 'L1'})

    def test_replace_level_with_raw_query(self):
        """Test replacing the level parameter with a
        Scihub-compatible format with an existing raw query
        """
        parameters = {'level': 'L1', 'raw_query': 'foo AND bar'}
        CopernicusScihubProvider()._replace_level(parameters)
        self.assertDictEqual(parameters, {'raw_query': 'foo AND bar AND L1'})


class CopernicusOpenSearchAPICrawlerTestCase(unittest.TestCase):
    """Tests for the Copernicus OpenSearch API crawler"""

    fixtures = [os.path.join(os.path.dirname(os.path.dirname(__file__)), "fixtures", "harvest")]
    TEST_DATA = {
        'full': {
            'url': "https://scihub.copernicus.eu/apihub/odata/v1/full"
                   "?$format=json&$expand=Attributes",
            'file_path': "data/copernicus_opensearch/full.json"}
    }

    def request_side_effect(self, method, url, **kwargs):  # pylint: disable=unused-argument
        """Side effect function used to mock calls to requests.get().text"""
        if method != 'GET':
            return None
        data_file_relative_path = None
        for test_data in self.TEST_DATA.values():
            if url == test_data['url']:
                data_file_relative_path = test_data['file_path']

        response = requests.Response()

        if data_file_relative_path:
            # Open data file as binary stream so it can be used to mock a requests response
            data_file = open(os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                data_file_relative_path), 'rb')
            # Store opened files so they can be closed when the test is finished
            self.opened_files.append(data_file)

            response.status_code = 200
            response.raw = data_file
        else:
            response.status_code = 404
            raise requests.exceptions.HTTPError()

        return response

    def setUp(self):
        self.base_url = 'https://scihub.copernicus.eu/dhus/search'
        self.search_terms = {
            'raw_query': '(platformname:Sentinel-1 OR platformname:Sentinel-3) AND NOT L0'
        }
        self.page_size = 2
        self.crawler = CopernicusScihubCrawler(
            url=self.base_url,
            search_terms=self.search_terms.copy(),
            username='user',
            password='pass',
            page_size=self.page_size,
            initial_offset=0)

        # Mock requests.get()
        self.patcher_request = mock.patch('geospaas_harvesting.utils.http_request')
        self.mock_request = self.patcher_request.start()
        self.mock_request.side_effect = self.request_side_effect
        self.opened_files = []

    def tearDown(self):
        self.patcher_request.stop()
        # Close any files opened during the test
        for opened_file in self.opened_files:
            opened_file.close()

    def test_equality(self):
        """Test the equality operator between crawlers"""
        self.assertEqual(CopernicusScihubCrawler('http://foo'),
                         CopernicusScihubCrawler('http://foo'))
        self.assertEqual(
            CopernicusScihubCrawler('http://foo',
                                    username='user', password='pass', search_terms={'bar': 'baz'}),
            CopernicusScihubCrawler('http://foo',
                                    username='user', password='pass', search_terms={'bar': 'baz'}))
        self.assertNotEqual(CopernicusScihubCrawler('http://foo'),
                            CopernicusScihubCrawler('http://bar'))
        self.assertNotEqual(
            CopernicusScihubCrawler('http://foo',
                                    username='user2', password='pass', search_terms={'bar': 'baz'}),
            CopernicusScihubCrawler('http://foo',
                                    username='user', password='pass', search_terms={'bar': 'baz'}))
        self.assertNotEqual(
            CopernicusScihubCrawler('http://foo',
                                    username='user', password='pass2', search_terms={'bar': 'baz'}),
            CopernicusScihubCrawler('http://foo',
                                    username='user', password='pass', search_terms={'bar': 'baz'}))
        self.assertNotEqual(
            CopernicusScihubCrawler('http://foo',
                                    username='user', password='pass', search_terms={'bar': 'baz'}),
            CopernicusScihubCrawler('http://foo',
                                    username='user', password='pass', search_terms={'bar': 'qux'}))

    def test_increment_offset(self):
        """The offset should be incremented by the page size"""
        self.assertEqual(self.crawler.page_offset, 0)
        self.crawler.increment_offset()
        self.assertEqual(self.crawler.page_offset, self.page_size)

    def test_build_parameters_with_standard_time_range(self):
        """Build the request parameters with a time range composed of two datetime objects"""
        request_parameters = self.crawler._build_request_parameters(
            search_terms=self.search_terms.copy(), username='user', password='pass',
            page_size=self.page_size, time_range=(
                datetime(2020, 2, 10, tzinfo=timezone.utc),
                datetime(2020, 2, 11, tzinfo=timezone.utc)))

        self.assertDictEqual(request_parameters, {
            'params': {
                'q': f"({self.search_terms['raw_query']}) AND " +
                     "(beginposition:[1000-01-01T00:00:00Z TO 2020-02-11T00:00:00Z] AND "
                     "endposition:[2020-02-10T00:00:00Z TO NOW])",
                'start': 0,
                'rows': self.page_size,
                'orderby': 'ingestiondate asc'
            },
            'auth': ('user', 'pass')
        })

    def test_build_parameters_with_time_range_without_lower_limit(self):
        """Build the request parameters with a time range in which the first element is None"""
        request_parameters = self.crawler._build_request_parameters(
            search_terms=self.search_terms.copy(), username='user', password='pass',
            page_size=self.page_size, time_range=(None, datetime(2020, 2, 11, tzinfo=timezone.utc)))
        self.assertEqual(request_parameters['params']['q'],
                         f"({self.search_terms['raw_query']}) AND " +
                         "(beginposition:[1000-01-01T00:00:00Z TO 2020-02-11T00:00:00Z])")

    def test_build_parameters_with_time_range_without_upper_limit(self):
        """Build the request parameters with a time range in which the second element is None"""
        request_parameters = self.crawler._build_request_parameters(
            search_terms=self.search_terms.copy(), username='user', password='pass',
            page_size=self.page_size, time_range=(datetime(2020, 2, 10, tzinfo=timezone.utc), None))
        self.assertEqual(request_parameters['params']['q'],
                         f"({self.search_terms['raw_query']}) AND " +
                         "(endposition:[2020-02-10T00:00:00Z TO NOW])")

    def test_build_parameters_without_time_range(self):
        """
        Build the request parameters with a time range in which the both elements are None
        The result is equivalent to a search without a time condition.
        """
        request_parameters = self.crawler._build_request_parameters(
            search_terms=self.search_terms.copy(), username='user', password='pass',
            page_size=self.page_size, time_range=(None, None))

        self.assertEqual(request_parameters['params']['q'], f"({self.search_terms['raw_query']})")

    def test_get_datasets_info(self):
        """_get_datasets_info() should extract download URLs from a response page"""
        data_file_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'data/copernicus_opensearch/page1.xml')

        with open(data_file_path, 'r') as f_h:
            page = f_h.read()

        self.crawler._get_datasets_info(page)
        self.assertListEqual(self.crawler._results, [
            DatasetInfo("https://scihub.copernicus.eu/dhus/odata/v1/"
                        "Products('87ddb795-dab4-4985-85f4-c390c9cdd65b')/$value"),
            DatasetInfo("https://scihub.copernicus.eu/dhus/odata/v1/"
                        "Products('d023819a-60d3-4b5e-bb81-645294d73b5b')/$value")
        ])

    def test_build_metadata_url(self):
        """Test that the metadata URL is correctly built from the dataset URL"""
        test_url = 'http://scihub.copernicus.eu/dataset/$value'
        expected_result = 'http://scihub.copernicus.eu/dataset?$format=json&$expand=Attributes'

        self.assertEqual(self.crawler._build_metadata_url(test_url), expected_result)

    def test_get_raw_metadata(self):
        """Test that the raw metadata is correctly fetched"""
        raw_metadata = self.crawler._get_raw_metadata(
            'https://scihub.copernicus.eu/apihub/odata/v1/full/$value')
        test_file_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), self.TEST_DATA['full']['file_path'])

        with open(test_file_path, 'rb') as test_file_handler:
            self.assertDictEqual(json.load(test_file_handler), raw_metadata)

    def test_error_on_inexistent_metadata_page(self):
        """An exception must be raised in case the metadata URL points
        to nothing
        """
        with self.assertRaises(requests.HTTPError):
            self.crawler._get_raw_metadata('http://nothing/$value')

    def test_log_on_invalid_dataset_url(self):
        """An exception must be raised in case the dataset URL does not
        match the ingester's regex
        """
        with self.assertRaises(ValueError):
            self.crawler._get_raw_metadata('')

    def test_get_normalized_attributes(self):
        """Test that the correct attributes are extracted from Sentinel-SAFE JSON metadata"""
        with mock.patch(
                'geospaas_harvesting.crawlers.MetadataHandler.get_parameters') as mock_get_params:
            _ = self.crawler.get_normalized_attributes(
                DatasetInfo('https://scihub.copernicus.eu/apihub/odata/v1/full/$value'))
        mock_get_params.assert_called_with({
            'Acquisition Type': 'NOMINAL',
            'Carrier rocket': 'Soyuz',
            'Cycle number': '195',
            'Date': '2020-03-18T06:23:05.976Z',
            'Filename': 'S1A_IW_GRDH_1SDV_20200318T062305_20200318T062330_031726_03A899_F558.SAFE',
            'Footprint':
                '<gml:Polygon srsName="http://www.opengis.net/gml/srs/epsg.xml#4326" '
                'xmlns:gml="http://www.opengis.net/gml">\n   <gml:outerBoundaryIs>\n      '
                '<gml:LinearRing>\n         <gml:coordinates>50.983601,-0.694377 51.396446,'
                '-4.436811 52.891499,-4.065843 52.476219,-0.197663 50.983601,-0.694377'
                '</gml:coordinates>\n      </gml:LinearRing>\n   </gml:outerBoundaryIs>\n'
                '</gml:Polygon>',
            'Format': 'SAFE',
            'Identifier': 'S1A_IW_GRDH_1SDV_20200318T062305_20200318T062330_031726_03A899_F558',
            'Ingestion Date': '2020-03-18T09:29:16.539Z',
            'Instrument': 'SAR-C',
            'Instrument abbreviation': 'SAR-C SAR',
            'Instrument description':
                '<a target="_blank" '
                'href="https://sentinel.esa.int/web/sentinel/missions/sentinel-1">'
                'https://sentinel.esa.int/web/sentinel/missions/sentinel-1</a>',
            'Instrument description text':
                'The SAR Antenna Subsystem (SAS) is developed and build by AstriumGmbH. It is a '
                'large foldable planar phased array antenna, which isformed by a centre panel and '
                'two antenna side wings. In deployedconfiguration the antenna has an overall '
                'aperture of 12.3 x 0.84 m.The antenna provides a fast electronic scanning '
                'capability inazimuth and elevation and is based on low loss and highly '
                'stablewaveguide radiators build in carbon fibre technology, which arealready '
                'successfully used by the TerraSAR-X radar imaging mission.The SAR Electronic '
                'Subsystem (SES) is developed and build byAstrium Ltd. It provides all radar '
                'control, IF/ RF signalgeneration and receive data handling functions for the '
                'SARInstrument. The fully redundant SES is based on a channelisedarchitecture '
                'with one transmit and two receive chains, providing amodular approach to the '
                'generation and reception of wide-bandsignals and the handling of '
                'multi-polarisation modes. One keyfeature is the implementation of the Flexible '
                'Dynamic BlockAdaptive Quantisation (FD-BAQ) data compression concept, whichallows '
                'an efficient use of on-board storage resources and minimisesdownlink times.',
            'Instrument mode': 'IW',
            'Instrument name': 'Synthetic Aperture Radar (C-band)',
            'Instrument swath': 'IW',
            'JTS footprint':
                'MULTIPOLYGON (((-0.694377 50.983601, -0.197663 52.476219, -4.065843 52.891499, '
                '-4.436811 51.396446, -0.694377 50.983601)))',
            'Launch date': 'April 3rd, 2014',
            'Mission datatake id': '239769',
            'Mission type': 'Earth observation',
            'Mode': 'IW',
            'NSSDC identifier': '2014-016A',
            'Operator': 'European Space Agency',
            'Orbit number (start)': '31726',
            'Orbit number (stop)': '31726',
            'Pass direction': 'DESCENDING',
            'Phase identifier': '1',
            'Polarisation': 'VV VH',
            'Product class': 'S',
            'Product class description': 'SAR Standard L1 Product',
            'Product composition': 'Slice',
            'Product level': 'L1',
            'Product type': 'GRD',
            'Relative orbit (start)': '154',
            'Relative orbit (stop)': '154',
            'Resolution': 'High',
            'Satellite': 'Sentinel-1',
            'Satellite description':
                '<a target="_blank" '
                'href="https://sentinel.esa.int/web/sentinel/missions/sentinel-1">'
                'https://sentinel.esa.int/web/sentinel/missions/sentinel-1</a>',
            'Satellite name': 'Sentinel-1',
            'Satellite number': 'A',
            'Sensing start': '2020-03-18T06:23:05.976Z',
            'Sensing stop': '2020-03-18T06:23:30.975Z',
            'Size': '1.65 GB',
            'Slice number': '14',
            'Start relative orbit number': '154',
            'Status': 'ARCHIVED',
            'Stop relative orbit number': '154',
            'Timeliness Category': 'Fast-24h',
            'url': 'https://scihub.copernicus.eu/apihub/odata/v1/full/$value'
        })
