# pylint: disable=protected-access
"""Tests for the generic FTP provider"""
import unittest
import unittest.mock as mock
from datetime import datetime, timezone

import geospaas_harvesting.crawlers as crawlers
from geospaas_harvesting.providers.http import HTTPProvider


class HTTPProviderTestCase(unittest.TestCase):
    """Tests for HTTPProvider"""

    def test_make_crawler(self):
        """Test creating a crawler from parameters"""
        provider = HTTPProvider(name='test', username='user', password='pass')
        parameters = {
            'start_time': datetime(2023, 1, 1, tzinfo=timezone.utc),
            'end_time': datetime(2023, 1, 2, tzinfo=timezone.utc),
            'url': 'http://foo/bar',
            'include': '.*'
        }
        with mock.patch('ftplib.FTP'):
            self.assertEqual(
                provider._make_crawler(parameters),
                crawlers.HTMLDirectoryCrawler(
                    'http://foo/bar',
                    include='.*',
                    time_range=(datetime(2023, 1, 1, tzinfo=timezone.utc),
                                datetime(2023, 1, 2, tzinfo=timezone.utc)),
                    username='user',
                    password='pass'))
