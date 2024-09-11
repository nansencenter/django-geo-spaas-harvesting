"""Tests for ERDDAP providers"""
import unittest
from datetime import datetime

import shapely.wkt

from geospaas_harvesting.crawlers import ERDDAPTableCrawler
from geospaas_harvesting.providers.erddap import ERDDAPTableProvider


class ERDDAPTableProviderTest(unittest.TestCase):
    """Tests for the ERDDAPTableProvider"""

    def setUp(self):
        self.provider = ERDDAPTableProvider(
            url='https://foo.json',
            id_attrs=['id'],
            longitude_attr='lon',
            latitude_attr='lat',
            time_attr='time',
            position_qc_attr='pos_qc',
            time_qc_attr='time_qc',
            valid_qc_codes=['1', '2'],
            variables=['bar', 'baz'])

    def test_make_crawler(self):
        """Test creating a crawler from parameters"""
        time_range = (datetime(2024, 1, 1), datetime(2024, 1, 2))
        location = shapely.wkt.loads('POLYGON((1 2,2 3,3 4,1 2))')
        self.assertEqual(
            self.provider._make_crawler({
                'start_time': time_range[0],
                'end_time': time_range[1],
                'location':  location,
                'search_terms': ['platform_number="123456"']}),
            ERDDAPTableCrawler(
                'https://foo.json',
                ['id'],
                longitude_attr='lon',
                latitude_attr='lat',
                time_attr='time',
                position_qc_attr='pos_qc',
                time_qc_attr='time_qc',
                valid_qc_codes=['1', '2'],
                variables=['bar', 'baz'],
                search_terms=[
                    'platform_number="123456"',
                    *self.provider._make_spatial_condition(location),
                    *self.provider._make_temporal_condition(time_range)
                ]))

    def test_make_spatial_condition(self):
        """Test converting a shapely geometry into a bounding box
        spatial condition
        """
        self.assertListEqual(
            self.provider._make_spatial_condition(shapely.wkt.loads('POLYGON((1 2,2 3,3 4,1 2))')),
            ['lon>=1.0', 'lon<=3.0', 'lat>=2.0', 'lat<=4.0'])
        self.assertListEqual(self.provider._make_spatial_condition(''), [])

    def test_make_temporal_condition(self):
        """Test making 2 datetimes into ERDDAP table API condition"""
        self.assertListEqual(
            self.provider._make_temporal_condition((datetime(2024, 1, 1), datetime(2024, 1, 2))),
            ['time>=2024-01-01T00:00:00Z', 'time<=2024-01-02T00:00:00Z'])
        self.assertListEqual(
            self.provider._make_temporal_condition((None, datetime(2024, 1, 2))),
            ['time<=2024-01-02T00:00:00Z'])
        self.assertListEqual(
            self.provider._make_temporal_condition((datetime(2024, 1, 1), None)),
            ['time>=2024-01-01T00:00:00Z'])
        self.assertListEqual(self.provider._make_temporal_condition((None, None)), [])
