# pylint: disable=protected-access
"""Tests for the CMEMS provider"""
import unittest
from datetime import datetime, timezone

from geospaas_harvesting.providers.cmems import CMEMSProvider, CMEMSCrawler


class CMEMSProviderTestCase(unittest.TestCase):
    """Tests for CMEMSProvider"""

    def test_make_crawler(self):
        """Test creating a crawler from parameters"""
        provider = CMEMSProvider(name='test', username='user', password='pass')
        parameters = {
            'start_time': datetime(2023, 1, 1, tzinfo=timezone.utc),
            'end_time': datetime(2023, 1, 2, tzinfo=timezone.utc),
            'product_id': 'SEALEVEL_GLO_PHY_L3_NRT_008_044',
            'dataset_ids': [
                'cmems_obs-sl_glo_phy-ssh_nrt_al-l3-duacs_PT1S',
                'cmems_obs-sl_glo_phy-ssh_nrt_al-l3-duacs_PT1S',
            ],
        }
        self.assertEqual(
            provider._make_crawler(parameters),
            CMEMSCrawler(
                'SEALEVEL_GLO_PHY_L3_NRT_008_044',
                [
                    'cmems_obs-sl_glo_phy-ssh_nrt_al-l3-duacs_PT1S',
                    'cmems_obs-sl_glo_phy-ssh_nrt_al-l3-duacs_PT1S',
                ],
                time_range=(datetime(2023, 1, 1, tzinfo=timezone.utc),
                            datetime(2023, 1, 2, tzinfo=timezone.utc)),
                username='user',
                password='pass'))
