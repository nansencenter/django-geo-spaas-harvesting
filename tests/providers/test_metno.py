# pylint: disable=protected-access
"""Tests for the METNO provider"""
import unittest
import unittest.mock as mock
from datetime import datetime, timezone

import geospaas_harvesting.crawlers as crawlers
from geospaas_harvesting.providers.metno import METNOProvider


class METNOProviderTestCase(unittest.TestCase):
    """Tests for METNOProvider"""

    def test_make_crawler(self):
        """Test creating a crawler from parameters"""
        provider = METNOProvider(name='test')
        parameters = {
            'start_time': datetime(2023, 1, 1, tzinfo=timezone.utc),
            'end_time': datetime(2023, 1, 2, tzinfo=timezone.utc),
            'directory': 'foo',
            'include': '.*'
        }
        self.assertEqual(
            provider._make_crawler(parameters),
            crawlers.ThreddsCrawler(
                'https://thredds.met.no/thredds/foo',
                include='.*',
                time_range=(datetime(2023, 1, 1, tzinfo=timezone.utc),
                            datetime(2023, 1, 2, tzinfo=timezone.utc))))
