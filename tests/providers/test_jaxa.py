# pylint: disable=protected-access
"""Tests for the JAXA GPortal provider"""
import unittest
import unittest.mock as mock
from datetime import datetime, timezone

import geospaas_harvesting.crawlers as crawlers
from geospaas_harvesting.providers.jaxa import GPortalProvider


class GPortalProviderTestCase(unittest.TestCase):
    """Tests for GPortalProvider"""

    def test_make_crawler(self):
        """Test creating a crawler from parameters"""
        provider = GPortalProvider(name='test', username='user', password='pass')
        parameters = {
            'start_time': datetime(2023, 1, 1, tzinfo=timezone.utc),
            'end_time': datetime(2023, 1, 2, tzinfo=timezone.utc),
            'directory': '/standard/GCOM-W/GCOM-W.AMSR2/L3.SST_25/3/foo',
            'include': '.*'
        }
        with mock.patch('ftplib.FTP'):
            self.assertEqual(
                provider._make_crawler(parameters),
                crawlers.FTPCrawler(
                    'ftp://ftp.gportal.jaxa.jp/standard/GCOM-W/GCOM-W.AMSR2/L3.SST_25/3/foo',
                    include='.*',
                    time_range=(datetime(2023, 1, 1, tzinfo=timezone.utc),
                                datetime(2023, 1, 2, tzinfo=timezone.utc)),
                    username='user',
                    password='pass'))
