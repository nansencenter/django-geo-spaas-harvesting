# pylint: disable=protected-access
"""Tests for the CMEMS provider"""
import unittest
import unittest.mock as mock
from datetime import datetime, timezone
from pathlib import Path

from geospaas_harvesting.crawlers import DatasetInfo
from geospaas_harvesting.providers.cmems import CMEMSProvider, CMEMSCrawler, CMEMSMetadataNormalizer


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


class CMEMSCrawlerTestCase(unittest.TestCase):
    """Tests for CMEMSCrawler"""

    def test_instanciation(self):
        """Test creating a crawler"""
        crawler = CMEMSCrawler(
                'product', ['dataset_1', 'dataset_2'],
                (datetime(2024, 9, 1), datetime(2024, 9, 2)),
                'user', 'pass')
        self.assertEqual(crawler.cmems_product_id, 'product')
        self.assertEqual(crawler.cmems_dataset_ids, ['dataset_1', 'dataset_2'])
        self.assertEqual(crawler.time_range, (datetime(2024, 9, 1), datetime(2024, 9, 2)))
        self.assertEqual(crawler.username, 'user')
        self.assertEqual(crawler.password, 'pass')

    def test_make_filter(self):
        """Test making a regular expression matching a time range
        """
        mock_crawler = mock.Mock()

        mock_crawler.time_range = (datetime(2024, 9, 1),
                                   datetime(2024, 9, 2))
        self.assertEqual(
             CMEMSCrawler.make_filter(mock_crawler),
             '.*_((2024(09(01|02))))_.*')

        mock_crawler.time_range = (datetime(2024, 9, 1),
                                   datetime(2024, 10, 15))
        self.assertEqual(
             CMEMSCrawler.make_filter(mock_crawler),
             '.*_((2024(09(01|02|03|04|05|06|07|08|09|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24'
             '|25|26|27|28|29|30)|10(01|02|03|04|05|06|07|08|09|10|11|12|13|14|15))))_.*')

        mock_crawler.time_range = (datetime(2024, 11, 1),
                                   datetime(2025, 1, 1))
        self.assertEqual(
             CMEMSCrawler.make_filter(mock_crawler),
             '.*_((202412[0-3][0-9])|(2024(11(01|02|03|04|05|06|07|08|09|10|11|12|13|14|15|16|17|'
             '18|19|20|21|22|23|24|25|26|27|28|29|30)))|(2025(01(01))))_.*')

        mock_crawler.time_range = (datetime(2023, 12, 30),
                                   datetime(2024, 1, 2))
        self.assertEqual(
             CMEMSCrawler.make_filter(mock_crawler),
             '.*_((2023(12(30|31)))|(2024(01(01|02))))_.*')

        mock_crawler.time_range = (datetime(2023, 12, 30),
                                   datetime(2025, 1, 2))
        self.assertEqual(
             CMEMSCrawler.make_filter(mock_crawler),
             '.*_((2023(12(30|31)))|(2024[0-9]{4})|(2025(01(01|02))))_.*')

        mock_crawler.time_range = (None, None)
        self.assertIsNone(CMEMSCrawler.make_filter(mock_crawler))

    def test_find_dict_in_list(self):
        """Test finding a dictionary in a list according to a criterion
        """
        dicts = [{'a': 1, 'b':2}, {'a': 3, 'b':2}, {'a': 1, 'b':4}]
        self.assertDictEqual(
            CMEMSCrawler._find_dict_in_list(dicts, 'a', 1),
            {'a': 1, 'b':2})

    def test_find_dict_in_list_error(self):
        """An exception must be raised when no matching dict is found
        """
        dicts = [{'a': 1, 'b':2}, {'a': 3, 'b':2}, {'a': 1, 'b':4}]
        with self.assertRaises(RuntimeError):
            CMEMSCrawler._find_dict_in_list(dicts, 'a', 4),

    def test_set_initial_state(self):
        """Test setting the initial state of the crawler"""
        crawler = CMEMSCrawler(
                'product', ['dataset_1', 'dataset_2'],
                (datetime(2024, 9, 1), datetime(2024, 9, 2)),
                'user', 'pass')
        self.assertEqual(crawler._product_info, None)
        self.assertEqual(crawler._tmpdir, None)
        self.assertEqual(crawler._dataset_lists, None)
        self.assertEqual(crawler._normalizer, None)

        with mock.patch('tempfile.TemporaryDirectory') as mock_tmpdir, \
             mock.patch('pathlib.Path.exists', side_effect=(True, False)), \
             mock.patch('pathlib.Path.unlink') as mock_unlink, \
             mock.patch('copernicusmarine.describe') as mock_describe, \
             mock.patch('copernicusmarine.get'):
            mock_tmpdir.return_value.name = '/tmp'
            product_info = {
                'product_id': 'product',
                'datasets': [{'dataset_id': 'dataset_1'},
                                {'dataset_id': 'dataset_2'}]
            }
            mock_describe.return_value = {
                'products': [
                    {
                        'product_id': 'foo',
                        'datasets': [{'dataset_id': 'bar'}]
                     },
                    {
                        'product_id': 'product',
                        'datasets': [{'dataset_id': 'dataset_1'},
                                     {'dataset_id': 'dataset_2'}]
                    },
                ]
            }
            crawler.set_initial_state()
            self.assertEqual(crawler._tmpdir, mock_tmpdir.return_value)
            self.assertDictEqual(crawler._product_info, product_info)
            self.assertIsInstance(crawler._normalizer, CMEMSMetadataNormalizer)
            self.assertDictEqual(crawler._dataset_lists, {
                'dataset_1': Path('/tmp', 'dataset_1.txt'),
                'dataset_2': Path('/tmp', 'dataset_2.txt'),
            })
            mock_unlink.assert_called_once()

    def test_get_cmems_dataset_properties(self):
        """Test getting metadata given a dataset ID"""
        crawler = CMEMSCrawler('product', ['dataset_1', 'dataset_2'])
        crawler._product_info = {
            'product_id': 'product',
            'datasets': [
                {'dataset_id': 'dataset_1'},
                {
                    'dataset_id': 'dataset_2',
                    'dataset_name': 'Dataset 2',
                    'versions': [{
                        'parts': [{
                            'services': [
                                {'service_type': {'service_name': 'foo','short_name': 'f'}},
                                {
                                    'service_format': None,
                                    'service_type': {
                                        'service_name': 'original-files',
                                        'short_name': 'files'
                                    },
                                    'uri': 'https://foo/bar',
                                    'variables': [{'short_name': 'var1'}, {'short_name': 'var2'}]
                                }
                            ]
                        }]
                    }]
                }
            ]
        }
        self.assertTupleEqual(
            crawler._get_cmems_dataset_properties('dataset_2'),
            ('Dataset 2', [{'short_name': 'var1'}, {'short_name': 'var2'}]))


    def test_crawl(self):
        """Test crawling through dataset files"""
        crawler = CMEMSCrawler('product', ['dataset_1'])
        crawler._dataset_lists = {
            'dataset_1': Path('/tmp', 'dataset_1.txt'),
        }
        file_contents = "{}\n{}".format(
            's3://mdl-native-07/native/product/dataset_1/2024/08/file_1.nc',
            's3://mdl-native-07/native/product/dataset_2/2024/09/file_2.nc')
        mock_open = mock.mock_open(read_data=file_contents)
        with mock.patch('builtins.open', mock_open), \
             mock.patch.object(crawler, '_get_cmems_dataset_properties') as mock_get_properties:
            mock_get_properties.return_value = ('d1f1', ('var1', 'var2'))
            self.assertListEqual(
                list(crawler.crawl()),
                [
                    DatasetInfo(
                        url='https://s3.waw3-1.cloudferro.com/mdl-native-07/native/product/dataset_1/2024/08/file_1.nc',
                        metadata={
                            'cmems_dataset_name': 'd1f1',
                            'variables': ('var1', 'var2'),
                        }
                    ),
                    DatasetInfo(
                        url='https://s3.waw3-1.cloudferro.com/mdl-native-07/native/product/dataset_2/2024/09/file_2.nc',
                        metadata={
                            'cmems_dataset_name': 'd1f1',
                            'variables': ('var1', 'var2'),
                        }
                    ),
                ]
            )

    def test_get_normalized_attributes(self):
        """Test normalizing the attributes"""
        crawler = CMEMSCrawler('product', ['dataset_1'])
        dataset_info = mock.Mock()
        with mock.patch.object(crawler, '_normalizer') as mock_normalizer:
            crawler.get_normalized_attributes(dataset_info)
            mock_normalizer.get_normalized_attributes.assert_called_with(dataset_info)

