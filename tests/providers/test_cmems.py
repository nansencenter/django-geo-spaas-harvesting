# pylint: disable=protected-access
"""Tests for the CMEMS provider"""
import unittest
import unittest.mock as mock
from datetime import datetime, timezone
from pathlib import Path

from geospaas_harvesting.crawlers import DatasetInfo
from geospaas_harvesting.providers.cmems import CMEMSProvider, CMEMSCrawler, CMEMSMetadataNormalizer
from geospaas_harvesting.providers.metadata_utils import MetadataNormalizationError


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


class CMEMSMetadataNormalizerTestCase(unittest.TestCase):
    """Tests for the CMEMSMetadataNormalizer base class"""

    def setUp(self):
        self.normalizer = CMEMSMetadataNormalizer({
            "title": "GLOBAL OCEAN ALONG-TRACK L3 SEA SURFACE HEIGHTS NRT",
            "product_id": "SEALEVEL_GLO_PHY_L3_NRT_008_044",
            "thumbnail_url": "https://foo/SEALEVEL_GLO_PHY_L3_NRT_008_044.jpg",
            "description": "SEALEVEL_GLO_PHY_L3_NRT_008_044 description",
            "digital_object_identifier": "id123",
            "sources": [
                "Satellite observations"
            ],
            "processing_level": "Level 3",
            "production_center": "some production center",
            "keywords": [
                "global-ocean",
                "arctic-ocean",
                "level-3"
            ],
            "datasets": []
        })

    def test_get_normalized_attributes(self):
        """Test getting normalized attributes from dataset metadata"""
        dataset_info = DatasetInfo('foo')
        mock_normalizer = mock.MagicMock()
        mock_normalizer.get_time_coverage.return_value = (mock.Mock(), mock.Mock())
        mock_normalizer.get_source.return_value = (mock.Mock(), mock.Mock())
        mock_normalizer.get_service.return_value = (mock.Mock(), mock.Mock())
        expected_result = {
            'entry_title': mock_normalizer._product_info.__getitem__.return_value,
            'entry_id': mock_normalizer.get_entry_id.return_value,
            'summary': mock_normalizer.get_summary.return_value,
            'time_coverage_start': mock_normalizer.get_time_coverage.return_value[0],
            'time_coverage_end': mock_normalizer.get_time_coverage.return_value[1],
            'platform': mock_normalizer.get_source.return_value[0],
            'instrument': mock_normalizer.get_source.return_value[1],
            'location_geometry': mock_normalizer.get_location_geometry.return_value,
            'provider': mock_normalizer.get_provider.return_value,
            'iso_topic_category': mock_normalizer.get_iso_topic_category.return_value,
            'gcmd_location': mock_normalizer.get_gcmd_location.return_value,
            'dataset_parameters': mock_normalizer.get_dataset_parameters.return_value,
            'geospaas_service_name': mock_normalizer.get_service.return_value[1],
            'geospaas_service': mock_normalizer.get_service.return_value[0],
        }
        self.assertDictEqual(
            CMEMSMetadataNormalizer.get_normalized_attributes(mock_normalizer, DatasetInfo('foo')),
            expected_result)

    def test_entry_id(self):
        """Test extracting the entry_id from a URL"""
        self.assertEqual(
            self.normalizer.get_entry_id(DatasetInfo(url='https://foo/bar/baz123.nc')), 'baz123')
        self.assertEqual(
            self.normalizer.get_entry_id(DatasetInfo(url='https://foo/bar/baz123.h5')), 'baz123')
        self.assertEqual(
            self.normalizer.get_entry_id(DatasetInfo(url='https://foo/bar/baz123.nc.gz')), 'baz123')
        self.assertEqual(
            self.normalizer.get_entry_id(DatasetInfo(url='https://foo/bar/baz123.h5.gz')), 'baz123')

    def test_entry_id_error(self):
        """a MetadataNormalizationError must be raised when an entry_id cannot be found"""
        # wrong file format
        with self.assertRaises(MetadataNormalizationError):
            self.normalizer.get_entry_id(DatasetInfo(url='ftp://foo/bar.txt'))
        # no url attribute
        with self.assertRaises(MetadataNormalizationError):
            self.normalizer.get_entry_id(DatasetInfo(url=None))

    def test_summary(self):
        """Test getting the summary"""
        self.assertEqual(
            self.normalizer.get_summary(DatasetInfo('foo', {'cmems_dataset_name': 'dataset_1'})),
            'Description: SEALEVEL_GLO_PHY_L3_NRT_008_044 description;Processing level: Level 3;'
            'Product: SEALEVEL_GLO_PHY_L3_NRT_008_044;Dataset ID: dataset_1')

    def test_time_coverage(self):
        """Test the time coverage retrieval"""
        self.assertTupleEqual(
            self.normalizer.get_time_coverage('nrt_global_allsat_phy_l4_20240603_20240609'),
            (datetime(2024, 6, 2, 12, tzinfo=timezone.utc),
             datetime(2024, 6, 3, 12, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage('dataset-uv-nrt-daily_20200301T0000Z_P20200307T0000'),
            (datetime(2020, 3, 1, tzinfo=timezone.utc),
             datetime(2020, 3, 2, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage('dataset-uv-nrt-monthly_202004T0000Z_P20200506T0000'),
            (datetime(2020, 4, 1, tzinfo=timezone.utc),
             datetime(2020, 5, 1, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage('dataset-uv-nrt-hourly_20200906T0000Z_P20200912T0000'),
            (datetime(2020, 9, 6, tzinfo=timezone.utc),
             datetime(2020, 9, 7, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage('mercatorpsy4v3r1_gl12_mean_20160303_R20160316'),
            (datetime(2016, 3, 3, tzinfo=timezone.utc),
             datetime(2016, 3, 4, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage(
                'mercatorpsy4v3r1_gl12_thetao_20200404_18h_R20200405'),
            (datetime(2020, 4, 4, 18, tzinfo=timezone.utc),
             datetime(2020, 4, 4, 18, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage(
                'mercatorpsy4v3r1_gl12_uovo_20200403_06h_R20200404'),
            (datetime(2020, 4, 3, 6, tzinfo=timezone.utc),
             datetime(2020, 4, 3, 6, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage('SMOC_20190515_R20190516'),
            (datetime(2019, 5, 15, tzinfo=timezone.utc),
             datetime(2019, 5, 16, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage('mercatorpsy4v3r1_gl12_hrly_20200511_R20200520'),
            (datetime(2020, 5, 11, tzinfo=timezone.utc),
             datetime(2020, 5, 12, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage('mercatorpsy4v3r1_gl12_mean_201807'),
            (datetime(2018, 7, 1, tzinfo=timezone.utc),
             datetime(2018, 8, 1, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage(
                '20200601_d-CMCC--RFVL-MFSeas6-MEDATL-b20210101_an-sv07.00'),
            (datetime(2020, 6, 1, tzinfo=timezone.utc),
             datetime(2020, 6, 2, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage(
                '20210601_h-CMCC--RFVL-MFSeas6-MEDATL-b20210615_an-sv07.00'),
            (datetime(2021, 6, 1, tzinfo=timezone.utc),
             datetime(2021, 6, 2, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage(
                '20200502_qm-CMCC--RFVL-MFSeas6-MEDATL-b20210101_an-sv07.00'),
            (datetime(2020, 5, 2, tzinfo=timezone.utc),
             datetime(2020, 5, 3, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage(
                '20210601_hts-CMCC--RFVL-MFSeas6-MEDATL-b20210615_an-sv07.00'),
            (datetime(2021, 6, 1, tzinfo=timezone.utc),
             datetime(2021, 6, 2, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage(
                '20210601_m-CMCC--RFVL-MFSeas6-MEDATL-b20210713_an-sv07.00'),
            (datetime(2021, 6, 1, tzinfo=timezone.utc),
             datetime(2021, 6, 2, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage(
                'CMEMS_v5r1_IBI_PHY_NRT_PdE_15minav_20201212_20201212_R20201221_AN04'),
            (datetime(2020, 12, 12, tzinfo=timezone.utc),
             datetime(2020, 12, 13, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage(
                'CMEMS_v5r1_IBI_PHY_NRT_PdE_01dav_20210503_20210503_R20210510_AN06'),
            (datetime(2021, 5, 3, tzinfo=timezone.utc),
             datetime(2021, 5, 4, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage(
                'CMEMS_v5r1_IBI_PHY_NRT_PdE_01hav_20191112_20191112_R20191113_AN07'),
            (datetime(2019, 11, 12, tzinfo=timezone.utc),
             datetime(2019, 11, 13, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage(
                'CMEMS_v5r1_IBI_PHY_NRT_PdE_01hav3D_20210815_20210815_R20210816_HC01'),
            (datetime(2021, 8, 15, tzinfo=timezone.utc),
             datetime(2021, 8, 16, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage(
                'CMEMS_v5r1_IBI_PHY_NRT_PdE_01mav_20191001_20191031_R20191031_AN01'),
            (datetime(2019, 10, 1, tzinfo=timezone.utc),
             datetime(2019, 11, 1, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage('20210204_dm-12km-NERSC-MODEL-TOPAZ4B-ARC-RAN.fv2.0'),
            (datetime(2021, 2, 4, tzinfo=timezone.utc),
             datetime(2021, 2, 5, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage('19910115_mm-12km-NERSC-MODEL-TOPAZ4B-ARC-RAN.fv2.0'),
            (datetime(1991, 1, 1, tzinfo=timezone.utc),
             datetime(1991, 2, 1, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage('19910101_ym-12km-NERSC-MODEL-TOPAZ4B-ARC-RAN.fv2.0'),
            (datetime(1991, 1, 1, tzinfo=timezone.utc),
             datetime(1992, 1, 1, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage(
                '20180104_dm-metno-MODEL-topaz4-ARC-b20180108-fv02.0'),
            (datetime(2018, 1, 4, tzinfo=timezone.utc),
             datetime(2018, 1, 5, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage(
                '20180102_hr-metno-MODEL-topaz4-ARC-b20180102-fv02.0'),
            (datetime(2018, 1, 2, tzinfo=timezone.utc),
             datetime(2018, 1, 3, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage(
                '20220929_dm-metno-MODEL-topaz5-ARC-b20220922-fv02.0'),
            (datetime(2022, 9, 29, tzinfo=timezone.utc),
             datetime(2022, 9, 30, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage(
                '20211130_hr-metno-MODEL-topaz5-ARC-b20211130-fv02.0'),
            (datetime(2021, 11, 30, tzinfo=timezone.utc),
             datetime(2021, 12, 1, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage(
                '20211031_dm-metno-MODEL-topaz5_ecosmo-ARC-b20211028-fv02.0'),
            (datetime(2021, 10, 31, tzinfo=timezone.utc),
             datetime(2021, 11, 1, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage(
                '202011_mm-metno-MODEL-topaz5_ecosmo-ARC-fv02.0'),
            (datetime(2020, 11, 1, tzinfo=timezone.utc),
             datetime(2020, 12, 1, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage('mfwamglocep_2021020200_R20210203'),
            (datetime(2021, 2, 2, tzinfo=timezone.utc),
             datetime(2021, 2, 3, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage('mercatorbiomer4v2r1_global_mean_20230103'),
            (datetime(2023, 1, 3, tzinfo=timezone.utc),
             datetime(2023, 1, 4, tzinfo=timezone.utc)))
        self.assertTupleEqual(
            self.normalizer.get_time_coverage('mercatorbiomer4v2r1_global_mean_202104'),
            (datetime(2021, 4, 1, tzinfo=timezone.utc),
             datetime(2021, 5, 1, tzinfo=timezone.utc)))

    def test_time_coverage_error(self):
        """An exception should be raised when time coverage retrieval
        fails
        """
        with self.assertRaises(RuntimeError):
            self.normalizer.get_time_coverage('foo')

    def test_search_source(self):
        """Test retrieval of platform or instrument from a Pythesint
        vocabulary
        """
        with mock.patch('pythesint.vocabulary.Vocabulary.fuzzy_search') as mock_fuzzy_search:
            mock_fuzzy_search.side_effect = ([], ['foo', 'bar'], ['baz'])
            self.assertEqual(
                self.normalizer._search_source('gcmd_instrument', ('s1', 's2', 's3')),
                'foo')

    def test_search_source_error(self):
        """An exception must be raised when no source is found
        """
        with mock.patch('pythesint.vocabulary.Vocabulary.fuzzy_search') as mock_fuzzy_search:
            mock_fuzzy_search.side_effect = ([None])
            with self.assertRaises(MetadataNormalizationError):
                self.normalizer._search_source('gcmd_instrument', ('s1',))

    def test_get_source(self):
        """Test retrieval of instrument and platform"""
        with mock.patch.object(self.normalizer, '_search_source') as mock_search_source, \
             mock.patch('pythesint.get_gcmd_instrument'):
            platform = mock.MagicMock()
            instrument = mock.Mock()
            mock_search_source.side_effect = (platform, instrument)
            self.assertTupleEqual(
                self.normalizer.get_source(
                    DatasetInfo('foo', {'cmems_dataset_name': 'dataset_1'})),
                (platform, instrument))
            self.assertListEqual(
                mock_search_source.call_args_list,
                [mock.call('gcmd_platform', ('dataset_1', 'Satellite observations')),
                 mock.call('gcmd_instrument', ('dataset_1', 'Satellite observations'))])

    def test_get_source_model(self):
        """Test retrieval of instrument and platform when the platform
        is a model
        """
        with mock.patch.object(self.normalizer, '_search_source') as mock_search_source, \
             mock.patch('pythesint.get_gcmd_instrument') as mock_get_gcmd_instrument:
            mock_search_source.return_value.__getitem__.return_value = 'Models'
            self.assertTupleEqual(
                self.normalizer.get_source(
                    DatasetInfo('foo', {'cmems_dataset_name': 'dataset_1'})),
                (mock_search_source.return_value, mock_get_gcmd_instrument.return_value))
            mock_search_source.assert_called_once_with('gcmd_platform',
                                                       ('dataset_1', 'Satellite observations'))
            mock_get_gcmd_instrument.assert_called_once_with('Computer')

    def test_get_location_geometry(self):
        """Test getting the location"""
        self.assertEqual(
            self.normalizer.get_location_geometry(DatasetInfo('foo', {
                'variables': [{'bbox': [-180, -78, 180, 81]}]
            })),
            'POLYGON((-180 -78,180 -78,180 81,-180 81,-180 -78))')

    def test_get_provider(self):
        """Test getting the provider"""
        with mock.patch('pythesint.get_gcmd_provider') as mock_get_gcmd_method:
            self.assertEqual(
                self.normalizer.get_provider(mock.Mock()),
                mock_get_gcmd_method.return_value)

    def test_get_iso_topic_category(self):
        """The iso topic category should be retrived from pythesint
        """
        with mock.patch('pythesint.get_iso19115_topic_category') as mock_get_category:
            self.assertEqual(
                self.normalizer.get_iso_topic_category(DatasetInfo('foo')),
                mock_get_category.return_value)

    def test_get_gcmd_location(self):
        """The GCMD location should be retrived from pythesint
        """
        with mock.patch('pythesint.get_gcmd_location') as mock_get_location:
            self.assertEqual(
                self.normalizer.get_gcmd_location(DatasetInfo('foo')),
                mock_get_location.return_value)

    def test_get_dataset_parameters(self):
        """Test retrieval of variable names"""
        vocabularies = {'cf_standard_name': mock.Mock()}
        with mock.patch('geospaas_harvesting.providers.metadata_utils'
                        '.get_cf_or_wkv_standard_name') as mock_get_cf_wkv, \
             mock.patch('pythesint.vocabularies', vocabularies):

            mock_get_cf_wkv.side_effect = ('variable_1', IndexError, IndexError)
            vocabularies['cf_standard_name'].fuzzy_search.side_effect = (
                IndexError, ['variable_3', 'varrriable_3'])

            self.assertListEqual(
                self.normalizer.get_dataset_parameters(DatasetInfo('foo', {
                    'variables': ({'standard_name': 'var1'},
                                  {'standard_name': 'var2'},
                                  {'standard_name': 'var3'})
                })),
                ['variable_1', 'variable_3'])

    def test_get_service(self):
        """Test retrieval of the type of repository where the data is
        hosted
        """
        self.assertTupleEqual(
            self.normalizer.get_service(DatasetInfo('foo')),
            ('HTTPServer', 'http'))
