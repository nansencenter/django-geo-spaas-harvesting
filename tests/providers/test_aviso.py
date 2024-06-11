# pylint: disable=protected-access
"""Tests for the AVISO provider"""
import unittest
import unittest.mock as mock
from datetime import datetime, timezone

import geospaas_harvesting.crawlers as crawlers
from geospaas_harvesting.providers.aviso import AVISOProvider


class AVISOProviderTestCase(unittest.TestCase):
    """Tests for AVISOProvider"""

    def test_make_crawler(self):
        """Test creating a crawler from parameters"""
        provider = AVISOProvider(name='test', username='user', password='pass')
        parameters = {
            'start_time': datetime(2023, 1, 1, tzinfo=timezone.utc),
            'end_time': datetime(2023, 1, 2, tzinfo=timezone.utc),
            'directory': 'foo',
            'include': '.*'
        }
        self.assertEqual(
            provider._make_crawler(parameters),
            crawlers.ThreddsCrawler(
                'https://tds.aviso.altimetry.fr/thredds/foo',
                include='.*',
                time_range=(datetime(2023, 1, 1, tzinfo=timezone.utc),
                            datetime(2023, 1, 2, tzinfo=timezone.utc)),
                username='user', password='pass'))
