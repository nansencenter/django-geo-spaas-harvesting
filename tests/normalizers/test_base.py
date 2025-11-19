"""Tests for the base GeoSPaaS normalizer"""

import unittest
import unittest.mock as mock

import geospaas_harvesting.normalizers as normalizers
from geospaas_harvesting.crawlers.base import DatasetInfo


class GeoSPaaSMetadataNormalizerTestCase(unittest.TestCase):
    """Tests for GeoSPaaSMetadataNormalizer"""

    def setUp(self):
        self.normalizer = normalizers.base.MetadataNormalizer()

    def test_get_entry_title(self):
        """get_entry_title() should return an empty string"""
        self.assertEqual(self.normalizer.get_entry_title(DatasetInfo('')), '')

    def test_get_entry_id(self):
        """get_entry_id() should be raise a NotImplementedError"""
        with self.assertRaises(NotImplementedError):
            self.normalizer.get_entry_id(DatasetInfo(''))

    def test_summary(self):
        """get_summary() should return an empty string
        """
        self.assertEqual(self.normalizer.get_summary(DatasetInfo('')), '')

    def test_get_time_coverage_start(self):
        """get_time_coverage_start() should be raise a
        NotImplementedError
        """
        with self.assertRaises(NotImplementedError):
            self.normalizer.get_time_coverage_start(DatasetInfo(''))

    def test_get_time_coverage_end(self):
        """get_time_coverage_end() should be raise a
        NotImplementedError
        """
        with self.assertRaises(NotImplementedError):
            self.normalizer.get_time_coverage_end(DatasetInfo(''))

    def test_get_keywords(self):
        """get_keywords() should be return and empty list"""
        self.assertListEqual(self.normalizer.get_keywords(DatasetInfo('')), [])

    def test_get_dataset_parameters(self):
        """Test getting parameters from the 'raw_dataset_parameters'
        attribute
        """
        with mock.patch('geospaas_harvesting.normalizers.utils.create_parameter_list'
                        ) as mock_create_parameter_list:
            mock_create_parameter_list.return_value = ['foo', 'bar']
            self.assertCountEqual(
                self.normalizer.get_dataset_parameters(
                    DatasetInfo('', {'raw_dataset_parameters': ['baz', 'qux']})),
                ['foo', 'bar'])

    def test_get_dataset_parameters_no_raw_parameters(self):
        """get_dataset_parameters() should return an empty string when
        'raw_dataset_parameters' is not present in the raw metadata
        """
        self.assertListEqual(self.normalizer.get_dataset_parameters(DatasetInfo('')), [])

    def test_normalize(self):
        """Test that the normalize method returns the right attributes
        """

        class TestNormalizer(normalizers.base.MetadataNormalizer):
            """Normalizer class inheriting from
            GeoSPaaSMetadataNormalizer for testing purposes
            """

            def get_entry_title(self, raw_metadata):
                """Get the entry title from the raw metadata"""
                return 'entry_title'

            def get_entry_id(self, raw_metadata):
                """Get the entry ID from the raw metadata"""
                return 'entry_id'

            def get_summary(self, raw_metadata):
                """Get the summary from the raw metadata"""
                return 'summary'

            def get_time_coverage_start(self, raw_metadata):
                """Get the start of the time coverage from the raw metadata"""
                return 'time_coverage_start'

            def get_time_coverage_end(self, raw_metadata):
                """Get the end of the time coverage from the raw metadata"""
                return 'time_coverage_end'

            def get_keywords(self, dataset_info):
                return [{'kind': 'gcmd_instrument', 'data': {'Short_Name': 'instrument'}}]

            def get_location_geometry(self, raw_metadata):
                """Get the location geometry (in WKT or GeoJSON) from the raw
                metadata
                """
                return 'location_geometry'

            def get_dataset_parameters(self, raw_metadata):
                """Get the dataset parameters, if any, from the raw metadata"""
                return ['dataset_parameters']

            def get_tags(self, dataset_info):
                return [{'name': 'collection', 'value': 'test'}]

        self.assertTupleEqual(
            TestNormalizer().normalize(DatasetInfo('https://foo')),
            (
                {
                    'entry_title': 'entry_title',
                    'entry_id': 'entry_id',
                    'summary': 'summary',
                    'location': 'location_geometry',
                    'time_coverage_start': 'time_coverage_start',
                    'time_coverage_end': 'time_coverage_end',
                },
                'https://foo',
                [{'kind': 'gcmd_instrument', 'data': {'Short_Name': 'instrument'}}],
                ['dataset_parameters'],
                [{'name': 'collection', 'value': 'test'}],
            )
        )
