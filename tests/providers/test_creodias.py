# pylint: disable=protected-access
"""Test for the Creodias provider and crawler"""
import json
import os.path
import unittest
import unittest.mock as mock
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import geospaas.catalog.managers as catalog_managers
import requests
from shapely.geometry.polygon import Polygon

import geospaas_harvesting.arguments as arguments
import geospaas_harvesting.providers.creodias as provider_creodias
from geospaas_harvesting.crawlers import DatasetInfo


class CreodiasProviderTestCase(unittest.TestCase):
    """Tests for CreodiasProvider"""

    def test_make_crawler(self):
        """Test creating a crawler from parameters"""
        provider = provider_creodias.CreodiasProvider(name='test', username='user', password='pass')
        parameters = {
            'collection': 'SENTINEL-1',
            'location': Polygon(((1, 2), (2, 3), (3, 4), (1, 2))),
            'start_time': datetime(2023, 1, 1, tzinfo=timezone.utc),
            'end_time': datetime(2023, 1, 2, tzinfo=timezone.utc),
            'foo': 'bar',
        }
        crawler = provider._make_crawler(parameters)
        self.assertEqual(
            crawler,
            provider_creodias.CreodiasEOFinderCrawler(
                'https://datahub.creodias.eu/resto/api/collections/SENTINEL-1/search.json',
                search_terms={
                    'foo': 'bar',
                    'geometry': 'POLYGON ((1 2, 2 3, 3 4, 1 2))',
                },
                time_range=(datetime(2023, 1, 1, tzinfo=timezone.utc),
                            datetime(2023, 1, 2, tzinfo=timezone.utc)),
                username='user',
                password='pass'))

    def test_collections(self):
        """Test creating a list of collections from the Creodias API response"""
        with open(os.path.join(os.path.dirname(os.path.dirname(__file__)),
                  'data/creodias_eofinder/collections.json'), 'rb') as collection_file:
            response = requests.Response()
            response.raw = collection_file
            response.status_code = 200
            with mock.patch('geospaas_harvesting.utils.http_request',
                            return_value=response) as mock_http_request:
                provider = provider_creodias.CreodiasProvider(
                    name='test', username='user', password='pass')
                collections = provider.collections

        mock_http_request.assert_called_with('GET', 'https://datahub.creodias.eu/stac/collections')
        self.assertListEqual(collections, ['SENTINEL-1'])


class CollectionArgumentTestCase(unittest.TestCase):
    """Tests for CollectionArgument"""

    def test_parse(self):
        """Test parsing a collection"""
        collections = ['SENTINEL-1']
        collection_argument = provider_creodias.CollectionArgument('collection',
                                                                   valid_options=collections)
        with mock.patch.object(collection_argument,
                               '_get_collection_parameters') as mock_get_collection_parameters:
            self.assertEqual(collection_argument.parse('SENTINEL-1'), 'SENTINEL-1')
        mock_get_collection_parameters.assert_called_once_with('SENTINEL-1')

    def test_get_collection_parameters(self):
        """Test populating children arguments"""
        collections = ['SENTINEL-1']
        collection_argument = provider_creodias.CollectionArgument('collection',
                                                                   valid_options=collections)
        with open(os.path.join(os.path.dirname(os.path.dirname(__file__)),
                'data/creodias_eofinder/s1_describe.xml'), 'rb') as collection_file, \
             mock.patch('geospaas_harvesting.utils.http_request') as mock_http_request:
            mock_http_request.return_value.raw = collection_file
            collection_argument._get_collection_parameters('SENTINEL-1')

        self.assertEqual(
            collection_argument.children,
            [
                arguments.IntegerArgument('lon', min_value=-180, max_value=180,
                                          description='lon description'),
                arguments.ChoiceArgument('platform', valid_options=['S1A']),
                arguments.StringArgument('swath'),
            ]
        )

    def test_str(self):
        """Test string representation"""
        self.assertEqual(
            str(provider_creodias.CollectionArgument('collection',
                                                     required=False,
                                                     valid_options=['SENTINEL-1'])),
            "collection, type=multiple choices, not required, valid options=['SENTINEL-1']")


class MakeOpenSearchArgumentTestCase(unittest.TestCase):
    """Tests for the CollectionArgument._make_argument() method"""

    def setUp(self):
        self.namespaces = {'parameters': 'http://baz/'}
        self.collection = provider_creodias.CollectionArgument(
            'collection',
            valid_options=['SENTINEL-1'])

    def test_make_argument_choice(self):
        """Test creating a choice argument object from an OpenSearch
        parameter
        """
        choice_param = ET.Element('{http://baz/}Parameter',
                                  name='productType',
                                  value='{eo:productType}')
        choice_param.append(ET.Element('{http://baz/}Option', value='GRD'))
        self.assertEqual(
            self.collection._make_argument(choice_param, self.namespaces),
            arguments.ChoiceArgument('productType', valid_options=['GRD']))

    def test_make_argument_string(self):
        """Test creating a string argument object from an OpenSearch
        parameter
        """
        self.assertEqual(
            self.collection._make_argument(
                ET.Element('{http://baz/}Parameter', name='foo', value='bar'), self.namespaces),
            arguments.StringArgument('foo'))
        self.assertEqual(
            self.collection._make_argument(
                ET.Element('{http://baz/}Parameter', name='foo', pattern='.*'), self.namespaces),
            arguments.StringArgument('foo', regex='.*'))
        self.assertEqual(
            self.collection._make_argument(
                ET.Element('{http://baz/}Parameter', name='foo', pattern='.*', title='bar'),
                self.namespaces),
            arguments.StringArgument('foo', regex='.*', description='bar'))

    def test_make_argument_integer(self):
        """Test creating an integer argument object from an OpenSearch
        parameter
        """
        self.assertEqual(
            self.collection._make_argument(
                ET.Element('{http://baz/}Parameter', name='foo', minInclusive=-10),
                self.namespaces),
            arguments.IntegerArgument('foo', min_value=-10))
        self.assertEqual(
            self.collection._make_argument(
                ET.Element('{http://baz/}Parameter', name='foo', minExclusive=-10),
                self.namespaces),
            arguments.IntegerArgument('foo', min_value=-9))
        self.assertEqual(
            self.collection._make_argument(
                ET.Element('{http://baz/}Parameter', name='foo', maxInclusive=10),
                self.namespaces),
            arguments.IntegerArgument('foo', max_value=10))
        self.assertEqual(
            self.collection._make_argument(
                ET.Element('{http://baz/}Parameter', name='foo', maxExclusive=10),
                self.namespaces),
            arguments.IntegerArgument('foo', max_value=9))
        self.assertEqual(
            self.collection._make_argument(
                ET.Element('{http://baz/}Parameter', name='foo', minInclusive=-10, maxInclusive=10),
                self.namespaces),
            arguments.IntegerArgument('foo', min_value=-10, max_value=10))
        self.assertEqual(
            self.collection._make_argument(
                ET.Element('{http://baz/}Parameter', name='foo', minExclusive=-10, maxInclusive=10),
                self.namespaces),
            arguments.IntegerArgument('foo', min_value=-9, max_value=10))
        self.assertEqual(
            self.collection._make_argument(
                ET.Element('{http://baz/}Parameter', name='foo', minExclusive=-10, maxExclusive=10),
                self.namespaces),
            arguments.IntegerArgument('foo', min_value=-9, max_value=9))
        self.assertEqual(
            self.collection._make_argument(
                ET.Element('{http://baz/}Parameter', name='foo', minInclusive=-10, maxExclusive=10),
                self.namespaces),
            arguments.IntegerArgument('foo', min_value=-10, max_value=9))

    def test_make_argument_error(self):
        """Test error cases for _get_collection_parameters()"""
        with self.assertRaises(ValueError):
            self.collection._make_argument(
                ET.Element('{http://baz/}Parameter', {'foo': 'bar'}),
                namespaces=self.namespaces)


class CreodiasEOFinderCrawlerTestCase(unittest.TestCase):
    """Tests for CreodiasEOFinderCrawler"""
    SEARCH_TERMS = {'param1': 'value1', 'param2': 'value2'}

    def setUp(self):
        self.crawler = provider_creodias.CreodiasEOFinderCrawler('foo', self.SEARCH_TERMS)

    def test_build_request_parameters_no_argument(self):
        """Test building the request parameters without specifying any argument"""
        self.assertDictEqual(self.crawler._build_request_parameters(), {
            'params': {
                'maxRecords': 100,
                'page': 1,
                'sortOrder': 'ascending',
                'sortParam': 'published'
            }
        })

    def test_build_request_parameters_no_time_range(self):
        """Test building the request parameters without time range"""
        self.assertDictEqual(self.crawler._build_request_parameters(self.SEARCH_TERMS), {
            'params': {
                'param1': 'value1',
                'param2': 'value2',
                'maxRecords': 100,
                'page': 1,
                'sortOrder': 'ascending',
                'sortParam': 'published'
            }
        })

    def test_build_request_parameters_with_time_range(self):
        """Test building the request parameters without time range"""
        time_range = (
            datetime(2020, 2, 1, tzinfo=timezone.utc),
            datetime(2020, 2, 2, tzinfo=timezone.utc)
        )

        self.assertDictEqual(
            self.crawler._build_request_parameters(self.SEARCH_TERMS, time_range), {
                'params': {
                    'param1': 'value1',
                    'param2': 'value2',
                    'maxRecords': 100,
                    'page': 1,
                    'sortOrder': 'ascending',
                    'sortParam': 'published',
                    'startDate': '2020-02-01T00:00:00Z',
                    'completionDate': '2020-02-02T00:00:00Z'
                }
            }
        )

    def test_build_request_parameters_with_time_range_start_only(self):
        """Test building the request parameters without time range"""
        time_range = (datetime(2020, 2, 1, tzinfo=timezone.utc), None)

        self.assertDictEqual(
            self.crawler._build_request_parameters(self.SEARCH_TERMS, time_range), {
                'params': {
                    'param1': 'value1',
                    'param2': 'value2',
                    'maxRecords': 100,
                    'page': 1,
                    'sortOrder': 'ascending',
                    'sortParam': 'published',
                    'startDate': '2020-02-01T00:00:00Z'
                }
            })

    def test_build_request_parameters_with_time_range_end_only(self):
        """Test building the request parameters without time range"""
        time_range = (None, datetime(2020, 2, 2, tzinfo=timezone.utc))

        self.assertDictEqual(
            self.crawler._build_request_parameters(self.SEARCH_TERMS, time_range), {
                'params': {
                    'param1': 'value1',
                    'param2': 'value2',
                    'maxRecords': 100,
                    'page': 1,
                    'sortOrder': 'ascending',
                    'sortParam': 'published',
                    'completionDate': '2020-02-02T00:00:00Z'
                }
            })

    def test_get_datasets_info(self):
        """_get_datasets_info() should extract datasets information from a response page"""
        data_file_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), 'data/creodias_eofinder/result_page.json')

        with open(data_file_path, 'r', encoding='utf-8') as f_h:
            page = f_h.read()

        expected_entry = json.loads(page)['features'][0]

        expected_result_metadata = expected_entry['properties'].copy()
        expected_result_metadata['geometry'] = json.dumps(expected_entry['geometry'])
        expected_result = DatasetInfo(
            'https://zipper.creodias.eu/download/c6ff8061-df12-53b7-8dd8-fb834b998f5b',
            expected_result_metadata)

        self.crawler._get_datasets_info(page)
        self.assertEqual(self.crawler._results[0], expected_result)

    def test_get_normalized_attributes(self):
        """Test the right metadata is added when normalizing"""
        crawler = provider_creodias.CreodiasEOFinderCrawler('https://foo')
        dataset_info = DatasetInfo('https://foo/bar', {'baz': 'qux'})
        with mock.patch('geospaas_harvesting.crawlers.MetadataHandler.get_parameters',
                        side_effect=lambda d: d) as mock_get_params:
            self.assertDictEqual(
                crawler.get_normalized_attributes(dataset_info),
                {
                    'baz': 'qux',
                    'url': 'https://foo/bar',
                    'geospaas_service': catalog_managers.HTTP_SERVICE,
                    'geospaas_service_name': catalog_managers.HTTP_SERVICE_NAME,
                })
        mock_get_params.assert_called_once_with(dataset_info.metadata)
