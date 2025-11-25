"""Tests for the RawMetadataNormalizer"""
import unittest
import unittest.mock as mock
from datetime import datetime, timezone

import shapely.geometry

import geospaas_harvesting.normalizers as normalizers
from geospaas_harvesting.crawlers.base import DatasetInfo
from geospaas_harvesting.normalizers.errors import MetadataNormalizationError


class RawMetadataNormalizerTestCase(unittest.TestCase):
    """Tests for the raw normalizer"""
    def setUp(self):
        self.normalizer = normalizers.raw.RawMetadataNormalizer()

    def test_get_entry_id(self):
        """Test getting the entry ID from the URL"""
        self.assertEqual(
            self.normalizer.get_entry_id(DatasetInfo(url='https://foo/bar/baz.nc')),
            'baz')
        self.assertEqual(
            self.normalizer.get_entry_id(DatasetInfo(url='https://foo/bar/baz.nc.gz')),
            'baz')
        self.assertEqual(
            self.normalizer.get_entry_id(DatasetInfo(url='https://foo/bar/baz.h5')),
            'baz')
        self.assertEqual(
            self.normalizer.get_entry_id(DatasetInfo(url='https://foo/bar/baz.h5.gz')),
            'baz')
        self.assertIsNone(self.normalizer.get_entry_id(DatasetInfo(url='https://foo')))

    def test_get_time_coverage_start(self):
        """Test getting the start of the time coverage"""
        self.assertIsNone(self.normalizer.get_time_coverage_start(DatasetInfo('')))

    def test_get_time_coverage_end(self):
        """Test getting the end of the time coverage"""
        self.assertIsNone(self.normalizer.get_time_coverage_end(DatasetInfo('')))

    def test_get_location_geometry(self):
        """Test getting the geometry"""
        self.assertIsNone(self.normalizer.get_location_geometry(DatasetInfo('')))

    def test_get_tags(self):
        """Test getting the tags keyword arguments"""
        self.assertEqual(
            self.normalizer.get_tags(DatasetInfo('', {'foo': 'bar', 'baz': 'qux'})),
            [{'name': 'foo', 'value': 'bar'}, {'name': 'baz', 'value': 'qux'}])
