# pylint: disable=protected-access
"""Tests for the NOAA provider"""
import unittest
import unittest.mock as mock
from datetime import datetime, timezone

import geospaas_harvesting.crawlers as crawlers
from geospaas_harvesting.providers.noaa import NOAAProvider


class NOAAProviderTestCase(unittest.TestCase):
    """Tests for NOAAProvider"""

    def test_make_crawler(self):
        """Test creating a crawler from parameters"""
        provider = NOAAProvider(name='test', username='user', password='pass')
        parameters = {
            'start_time': datetime(2023, 1, 1, tzinfo=timezone.utc),
            'end_time': datetime(2023, 1, 2, tzinfo=timezone.utc),
            'server': 'ftp.opc',
            'directory': '/grids/operational/GLOBALHYCOM/Navy/foo',
            'include': '.*'
        }
        with mock.patch('ftplib.FTP'):
            self.assertEqual(
                provider._make_crawler(parameters),
                crawlers.FTPCrawler(
                    'ftp://ftp.opc.ncep.noaa.gov/grids/operational/GLOBALHYCOM/Navy/foo',
                    include='.*',
                    time_range=(datetime(2023, 1, 1, tzinfo=timezone.utc),
                                datetime(2023, 1, 2, tzinfo=timezone.utc))))
