# pylint: disable=protected-access
"""Tests for EarthData CMR provider and crawler"""
import json
import os.path
import unittest
import unittest.mock as mock
from datetime import datetime, timezone

from shapely.geometry import LineString, Point, Polygon, MultiPoint

import geospaas.catalog.managers as catalog_managers
import geospaas_harvesting.providers.earthdata_cmr as provider_earthdata_cmr
from geospaas_harvesting.crawlers import DatasetInfo


class EarthDataCMRProviderTestCase(unittest.TestCase):
    """Tests for EarthDataCMRProvider"""

    def test_make_crawler(self):
        """Test creating a crawler from parameters"""
        provider = provider_earthdata_cmr.EarthDataCMRProvider(
            name='test', username='user', password='pass')
        parameters = {
            'location': Polygon(((1, 2), (2, 3), (3, 4), (1, 2))),
            'start_time': datetime(2023, 1, 1, tzinfo=timezone.utc),
            'end_time': datetime(2023, 1, 2, tzinfo=timezone.utc),
            'foo': 'bar',
        }
        crawler = provider._make_crawler(parameters)
        self.assertEqual(
            crawler,
            provider_earthdata_cmr.EarthDataCMRCrawler(
                'https://cmr.earthdata.nasa.gov/search/granules.umm_json',
                search_terms={
                    'foo': 'bar',
                    'polygon': '1.0,2.0,2.0,3.0,3.0,4.0,1.0,2.0',
                },
                time_range=(datetime(2023, 1, 1, tzinfo=timezone.utc),
                            datetime(2023, 1, 2, tzinfo=timezone.utc)),
                username='user',
                password='pass'))

    def test_make_spatial_parameter(self):
        """Test converting a location geometry to the supported format
        """
        provider = provider_earthdata_cmr.EarthDataCMRProvider(name='test')
        self.assertEqual(
            provider._make_spatial_parameter(Polygon(((1, 2), (2, 3), (3, 4), (1, 2)))),
            {'polygon': '1.0,2.0,2.0,3.0,3.0,4.0,1.0,2.0'})
        self.assertEqual(
            provider._make_spatial_parameter(LineString(((1, 2), (3,4)))),
            {'line': '1.0,2.0,3.0,4.0'})
        self.assertEqual(
            provider._make_spatial_parameter(Point((1, 2))),
            {'point': '1.0,2.0'})
        with self.assertRaises(ValueError):
            provider._make_spatial_parameter(MultiPoint(((1, 2), (3, 4))))


class EarthdataCMRCrawlerTestCase(unittest.TestCase):
    """Tests for EarthdataCMRCrawler"""
    SEARCH_TERMS = {'param1': 'value1', 'param2': 'value2'}

    def setUp(self):
        self.crawler = provider_earthdata_cmr.EarthDataCMRCrawler('foo', self.SEARCH_TERMS)

    def test_build_request_parameters_no_argument(self):
        """Test building the request parameters without specifying any argument"""
        self.assertDictEqual(self.crawler._build_request_parameters(), {
            'params': {
                'page_size': 100,
                'page_num': 1,
                'sort_key': '+start_date',
            }
        })

    def test_build_request_parameters_no_time_range(self):
        """Test building the request parameters without time range"""
        self.assertDictEqual(self.crawler._build_request_parameters(self.SEARCH_TERMS), {
            'params': {
                'param1': 'value1',
                'param2': 'value2',
                'page_size': 100,
                'page_num': 1,
                'sort_key': '+start_date'
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
                    'page_size': 100,
                    'page_num': 1,
                    'sort_key': '+start_date',
                    'temporal': '2020-02-01T00:00:00+00:00,2020-02-02T00:00:00+00:00'
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
                    'page_size': 100,
                    'page_num': 1,
                    'sort_key': '+start_date',
                    'temporal': '2020-02-01T00:00:00+00:00,'
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
                    'page_size': 100,
                    'page_num': 1,
                    'sort_key': '+start_date',
                    'temporal': ',2020-02-02T00:00:00+00:00'
                }
            })

    def test_get_datasets_info(self):
        """_get_datasets_info() should extract datasets information
        from a response page
        """
        data_file_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'data/earthdata_cmr/result_page.json')

        with open(data_file_path, 'r') as f_h:
            page = f_h.read()

        expected_entry = DatasetInfo(
            'https://oceandata.sci.gsfc.nasa.gov/cmr/getfile/V2012002205400.L2_SNPP_OC.nc',
            metadata=json.loads(page)['items'][0])

        self.crawler._get_datasets_info(page)
        self.assertEqual(self.crawler._results[0], expected_entry)

    def test_get_normalized_attributes(self):
        """Test the right metadata is added when normalizing"""
        crawler = provider_earthdata_cmr.EarthDataCMRCrawler('https://foo')
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
