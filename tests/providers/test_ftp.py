# pylint: disable=protected-access
"""Tests for the generic FTP provider"""
import unittest
import unittest.mock as mock
from datetime import datetime, timezone

import geospaas_harvesting.crawlers as crawlers
from geospaas_harvesting.providers.ftp import FTPProvider


class FTPProviderTestCase(unittest.TestCase):
    """Tests for FTPProvider"""

    def test_make_crawler(self):
        """Test creating a crawler from parameters"""
        provider = FTPProvider(name='test', username='user', password='pass')
        parameters = {
            'start_time': datetime(2023, 1, 1, tzinfo=timezone.utc),
            'end_time': datetime(2023, 1, 2, tzinfo=timezone.utc),
            'server': 'ftp://foo',
            'directory': '/bar',
            'include': '.*'
        }
        with mock.patch('ftplib.FTP'):
            self.assertEqual(
                provider._make_crawler(parameters),
                crawlers.FTPCrawler(
                    'ftp://foo/bar',
                    include='.*',
                    time_range=(datetime(2023, 1, 1, tzinfo=timezone.utc),
                                datetime(2023, 1, 2, tzinfo=timezone.utc)),
                    username='user',
                    password='pass'))
