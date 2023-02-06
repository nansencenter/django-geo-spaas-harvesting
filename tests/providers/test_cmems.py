# pylint: disable=protected-access
"""Tests for the CMEMS provider"""
import unittest
import unittest.mock as mock
from datetime import datetime, timezone

import geospaas_harvesting.crawlers as crawlers
from geospaas_harvesting.providers.cmems import CMEMSFTPProvider


class CMEMSFTPProviderTestCase(unittest.TestCase):
    """Tests for CMEMSFTPProvider"""

    def test_make_crawler(self):
        """Test creating a crawler from parameters"""
        provider = CMEMSFTPProvider(name='test', username='user', password='pass')
        parameters = {
            'start_time': datetime(2023, 1, 1, tzinfo=timezone.utc),
            'end_time': datetime(2023, 1, 2, tzinfo=timezone.utc),
            'server': 'nrt',
            'directory': '/Core/MULTIOBS_GLO_PHY_NRT_015_003/foo',
            'include': '.*'
        }
        with mock.patch('ftplib.FTP'):
            self.assertEqual(
                provider._make_crawler(parameters),
                crawlers.FTPCrawler(
                    'ftp://nrt.cmems-du.eu/Core/MULTIOBS_GLO_PHY_NRT_015_003/foo',
                    include='.*',
                    time_range=(datetime(2023, 1, 1, tzinfo=timezone.utc),
                                datetime(2023, 1, 2, tzinfo=timezone.utc)),
                    username='user',
                    password='pass'))
