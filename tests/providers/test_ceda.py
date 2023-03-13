# pylint: disable=protected-access
"""Tests for the CEDA provider"""
import unittest
import unittest.mock as mock
from datetime import datetime, timezone

import geospaas_harvesting.crawlers as crawlers
from geospaas_harvesting.providers.ceda import CEDAProvider


class CEDAProviderTestCase(unittest.TestCase):
    """Tests for CEDAProvider"""

    def test_make_crawler(self):
        """Test creating a crawler from parameters"""
        provider = CEDAProvider(name='test', username='user', password='pass')
        parameters = {
            'start_time': datetime(2023, 1, 1, tzinfo=timezone.utc),
            'end_time': datetime(2023, 1, 2, tzinfo=timezone.utc),
            'directory': '/neodc/esacci/sst/data/CDR_v2/Climatology/L4/v2.1/foo',
            'include': '.*'
        }
        with mock.patch('ftplib.FTP'):
            self.assertEqual(
                provider._make_crawler(parameters),
                crawlers.FTPCrawler(
                    'ftp://anon-ftp.ceda.ac.uk/neodc/esacci/sst'
                    '/data/CDR_v2/Climatology/L4/v2.1/foo',
                    include='.*',
                    time_range=(datetime(2023, 1, 1, tzinfo=timezone.utc),
                                datetime(2023, 1, 2, tzinfo=timezone.utc)),
                    username='user',
                    password='pass'))
