"""Test suite for ingesters"""
#pylint: disable=protected-access

import logging
import os
import unittest
import unittest.mock as mock
import xml.etree.ElementTree as ET
from datetime import datetime

import django.test
import requests
from dateutil.tz import tzlocal
from django.contrib.gis.geos.geometry import GEOSGeometry
from geospaas.catalog.models import Dataset

import geospaas_harvesting.ingesters as ingesters


class BaseIngesterTestCase(unittest.TestCase):
    """Test the base ingester class"""

    def test_ingest_must_be_implemented(self):
        """An error must be raised if the ingest() method is not implemented"""
        base_ingester = ingesters.Ingester()
        with self.assertRaises(NotImplementedError), self.assertLogs(ingesters.LOGGER):
            base_ingester.ingest([])


class DDXIngesterTestCase(django.test.TestCase):
    """Test the DDXIngester"""

    TEST_DATA = {
        'full_ddx': {
            'url': "https://test-opendap.com/full_dataset.nc.ddx",
            'file_path': "data/opendap_full_ddx.xml"},
        'full_ddx_2': {
            'url': "https://test-opendap2.com/full_dataset.nc.ddx",
            'file_path': "data/opendap_full_ddx.xml"},
        'short_ddx': {
            'url': "https://test-opendap.com/short_dataset.nc.ddx",
            'file_path': "data/opendap_short_ddx.xml"},
        'no_ns_ddx': {
            'url': "https://test-opendap.com/no_ns_dataset.nc.ddx",
            'file_path': "data/opendap_ddx_no_ns.xml"},
    }

    def requests_get_side_effect(self, url, **kwargs): # pylint: disable=unused-argument
        """Side effect function used to mock calls to requests.get().text"""
        data_file_relative_path = None
        for test_data in self.TEST_DATA.values():
            if url == test_data['url']:
                data_file_relative_path = test_data['file_path']

        response = requests.Response()

        if data_file_relative_path:
            # Open data file as binary stream so it can be used to mock a requests response
            data_file = open(os.path.join(os.path.dirname(__file__), data_file_relative_path), 'rb')
            # Store opened files so they can be closed when the test is finished
            self.opened_files.append(data_file)

            response.status_code = 200
            response.raw = data_file
        else:
            response.status_code = 404

        return response

    def setUp(self):
        # Mock requests.get()
        self.patcher_requests_get = mock.patch('geospaas_harvesting.ingesters.requests.get')
        self.mock_requests_get = self.patcher_requests_get.start()
        self.mock_requests_get.side_effect = self.requests_get_side_effect
        self.opened_files = []

    def tearDown(self):
        self.patcher_requests_get.stop()
        # Close any files opened during the test
        for opened_file in self.opened_files:
            opened_file.close()

    def test_get_xml_namespace(self):
        """Get xml namespace from the test data DDX file"""
        test_file_path = os.path.join(
            os.path.dirname(__file__),
            self.TEST_DATA['short_ddx']['file_path'])

        with open(test_file_path, 'rb') as test_file:
            root = ET.parse(test_file).getroot()

        ingester = ingesters.DDXIngester()

        self.assertEqual(ingester._get_xml_namespace(root), 'http://xml.opendap.org/ns/DAP/3.2#')

    def test_logging_if_no_namespace(self):
        """A warning must be logged if no namespace has been found, and an empty string returned"""
        test_file_path = os.path.join(
            os.path.dirname(__file__),
            self.TEST_DATA['no_ns_ddx']['file_path'])

        with open(test_file_path, 'rb') as test_file:
            root = ET.parse(test_file).getroot()

        ingester = ingesters.DDXIngester()

        with self.assertLogs(ingesters.LOGGER, level=logging.WARNING):
            namespace = ingester._get_xml_namespace(root)

        self.assertEqual(namespace, '')

    def test_extract_global_attributes(self):
        """Get nc_global attributes from the test data DDX file"""
        test_file_path = os.path.join(
            os.path.dirname(__file__),
            self.TEST_DATA['short_ddx']['file_path'])

        with open(test_file_path, 'rb') as test_file:
            root = ET.parse(test_file).getroot()

        ingester = ingesters.DDXIngester()
        self.assertDictEqual(
            ingester._extract_global_attributes(root),
            {
                'Conventions': 'CF-1.7, ACDD-1.3',
                'title': 'VIIRS L2P Sea Surface Skin Temperature'
            }
        )

    def test_ingest_ddx(self):
        """Ingest a DDX file"""
        initial_datasets_count = Dataset.objects.count()

        ingester = ingesters.DDXIngester()
        with self.assertLogs(ingesters.LOGGER):
            ingester.ingest([self.TEST_DATA['full_ddx']['url']])

        self.assertEqual(Dataset.objects.count(), initial_datasets_count + 1)
        inserted_dataset = Dataset.objects.latest('id')

        self.assertEqual(inserted_dataset.entry_title, 'VIIRS L2P Sea Surface Skin Temperature')
        self.assertEqual(inserted_dataset.summary, '''Sea surface temperature (SST) retrievals produced at the NASA OBPG for the Visible Infrared Imaging
                Radiometer Suite (VIIRS) sensor on the Suomi National Polar-Orbiting Partnership (Suomi NPP) platform.
                These have been reformatted to GHRSST GDS version 2 Level 2P specifications by the JPL PO.DAAC. VIIRS
                SST algorithms developed by the University of Miami, RSMAS''')
        self.assertEqual(inserted_dataset.time_coverage_start, datetime(
            year=2020, month=1, day=1, hour=0, minute=0, second=1, tzinfo=tzlocal()))
        self.assertEqual(inserted_dataset.time_coverage_end, datetime(
            year=2020, month=1, day=1, hour=0, minute=5, second=59, tzinfo=tzlocal()))

        self.assertEqual(inserted_dataset.source.instrument.short_name, 'VIIRS')
        self.assertEqual(inserted_dataset.source.instrument.long_name,
                         'Visible-Infrared Imager-Radiometer Suite')
        self.assertEqual(inserted_dataset.source.instrument.category,
                         'Earth Remote Sensing Instruments')
        self.assertEqual(inserted_dataset.source.instrument.subtype,
                         'Imaging Spectrometers/Radiometers')
        self.assertEqual(inserted_dataset.source.instrument.instrument_class,
                         'Passive Remote Sensing')

        self.assertEqual(inserted_dataset.source.platform.short_name, 'SUOMI-NPP')
        self.assertEqual(inserted_dataset.source.platform.long_name,
                         'Suomi National Polar-orbiting Partnership')
        self.assertEqual(inserted_dataset.source.platform.category, 'Earth Observation Satellites')
        self.assertEqual(inserted_dataset.source.platform.series_entity,
                         'Joint Polar Satellite System (JPSS)')

        self.assertEqual(inserted_dataset.geographic_location.geometry, GEOSGeometry(
            'POLYGON((' +
            '-175.084000 -15.3505001,' +
            '-142.755005 -15.3505001,' +
            '-142.755005 9.47472000,' +
            '-175.084000 9.47472000,' +
            '-175.084000 -15.3505001))',
            srid=4326
        ))
        self.assertEqual(inserted_dataset.data_center.short_name, 'The GHRSST Project Office')
        self.assertEqual(inserted_dataset.data_center.long_name, 'The GHRSST Project Office')
        self.assertEqual(inserted_dataset.data_center.data_center_url, 'http://www.ghrsst.org')

        self.assertEqual(inserted_dataset.ISO_topic_category.name, 'Oceans')

        self.assertEqual(inserted_dataset.gcmd_location.category, 'VERTICAL LOCATION')
        self.assertEqual(inserted_dataset.gcmd_location.type, 'SEA SURFACE')

    def test_ingest_same_url_twice(self):
        """The same URL must not be ingested twice"""
        initial_datasets_count = Dataset.objects.count()

        ingester = ingesters.DDXIngester()
        with self.assertLogs(ingesters.LOGGER):
            ingester.ingest([self.TEST_DATA['full_ddx']['url']])
        self.assertEqual(Dataset.objects.count(), initial_datasets_count + 1)

        with self.assertLogs(ingesters.LOGGER, level=logging.INFO) as logger_cm:
            ingester.ingest([self.TEST_DATA['full_ddx']['url']])

        self.assertTrue(logger_cm.records[0].msg.endswith('is already present in the database.'))
        self.assertEqual(Dataset.objects.count(), initial_datasets_count + 1)

    def test_ingest_dataset_twice_different_urls(self):
        """The same dataset must not be ingested twice even if it is present at different URLs"""
        initial_datasets_count = Dataset.objects.count()

        ingester = ingesters.DDXIngester()
        with self.assertLogs(ingesters.LOGGER):
            ingester.ingest([self.TEST_DATA['full_ddx']['url']])
        self.assertEqual(Dataset.objects.count(), initial_datasets_count + 1)

        with self.assertLogs(ingesters.LOGGER, level=logging.INFO) as logger_cm:
            ingester.ingest([self.TEST_DATA['full_ddx_2']['url']])

        self.assertTrue(logger_cm.records[0].msg.endswith('already exists in the database.'))
        self.assertEqual(Dataset.objects.count(), initial_datasets_count + 1)

    def test_logging_on_normalization_error(self):
        """An error must be logged if the normalization failed"""

        ingester = ingesters.DDXIngester()

        with self.assertLogs(ingesters.LOGGER, level=logging.ERROR) as logger_cm:
            ingester.ingest([self.TEST_DATA['short_ddx']['url']])
        self.assertTrue(logger_cm.records[0].message.startswith(
            f"Ingestion of the dataset at '{self.TEST_DATA['short_ddx']['url']}' failed:"))


class NansatIngesterTestCase(django.test.TestCase):
    """Test the NansatIngester"""

    def test_ingest_netcdf_with_nansat(self):
        """Test the ingestion of a netcdf file using nansat"""
        initial_datasets_count = Dataset.objects.count()

        ingester = ingesters.NansatIngester()
        with self.assertLogs(ingesters.LOGGER):
            ingester.ingest([os.path.join(os.path.dirname(__file__), 'data/arc_metno_dataset.nc')])

        self.assertEqual(Dataset.objects.count(), initial_datasets_count + 1)
        inserted_dataset = Dataset.objects.latest('id')

        self.assertEqual(inserted_dataset.entry_title, 'NONE')
        self.assertEqual(inserted_dataset.summary, 'NONE')
        self.assertEqual(inserted_dataset.time_coverage_start, datetime(
            year=2017, month=5, day=18, hour=0, minute=0, second=0, tzinfo=tzlocal()))
        self.assertEqual(inserted_dataset.time_coverage_end, datetime(
            year=2017, month=5, day=27, hour=0, minute=0, second=0, tzinfo=tzlocal()))

        self.assertEqual(inserted_dataset.source.instrument.short_name, 'Computer')
        self.assertEqual(inserted_dataset.source.instrument.long_name, 'Computer')
        self.assertEqual(inserted_dataset.source.instrument.category,
                         'In Situ/Laboratory Instruments')
        self.assertEqual(inserted_dataset.source.instrument.subtype, '')
        self.assertEqual(inserted_dataset.source.instrument.instrument_class, 'Data Analysis')

        self.assertEqual(inserted_dataset.source.platform.short_name, 'MODELS')
        self.assertEqual(inserted_dataset.source.platform.long_name, '')
        self.assertEqual(inserted_dataset.source.platform.category, 'Models/Analyses')
        self.assertEqual(inserted_dataset.source.platform.series_entity, '')

        expected_geometry = GEOSGeometry(
            'POLYGON ((20.704223629342 89.99989256067724, 24.995696372329 89.99987077812315, ' +
            '28.03728216024454 89.99984848089234, 30.2939221300575 89.99982586658082, ' +
            '32.02978515699697 89.99980304437932, 33.40416281560302 89.99978007899939, ' +
            '34.5181331485164 89.99975701103389, 35.43865303831961 89.99973386715732, ' +
            '36.21171468995888 89.99971066558552, 36.86989764584402 89.99968741916564, ' +
            '37.60881652342161 89.99965636990923, 37.60881652342161 89.99965636990923, ' +
            '33.5815529218009 89.99965234415896, 29.66530387656485 89.99964664479596, ' +
            '25.89035287122399 89.99963935115623, 22.28000271854232 89.99963055765173, ' +
            '18.850402039079 89.99962036849348, 15.61098853367965 89.99960889274531, ' +
            '12.5653483799073 89.99959624009551, 9.712286628119696 89.99958251753553, ' +
            '7.046939023377033 89.99956782697018, 4.128626239815715 89.99954934720112, ' +
            '4.128626239815715 89.99954934720112, 1.379076461628385 89.99957255048969, ' +
            '-0.883102001486741 89.99958924373496, -3.332737430412262 89.99960524311808, ' +
            '-5.983994159666929 89.99962046088012, -8.849993050812204 89.99963479929851, ' +
            '-11.94175268863764 89.99964815085281, -15.26682844732823 89.99966039912611, ' +
            '-18.82767840387169 89.99967142071603, -22.61986494804042 89.99968108841456, ' +
            '-26.63030830092556 89.99968927582337, -26.63030830092556 89.99968927582337, ' +
            '-24.77514056883192 89.99971674199658, -22.90124981623723 89.99973970559563, ' +
            '-20.66768923166103 89.99976233715211, -17.96913974015701 89.9997845320129, ' +
            '-14.66023223730137 89.99980614012912, -10.53918372862823 89.99982694157781, ' +
            '-5.328234781979837 89.99984660783163, 1.34000778089971 89.99986464317887, ' +
            '9.898259944152521 89.99988030809297, 20.704223629342 89.99989256067724))',
            srid=4326
        )

        # This fails, which is why string representations are compared. Any explanation is welcome.
        # self.assertTrue(inserted_dataset.geographic_location.geometry.equals(expected_geometry))
        self.assertEqual(str(inserted_dataset.geographic_location.geometry), str(expected_geometry))

        self.assertEqual(inserted_dataset.data_center.short_name, 'NERSC')
        self.assertEqual(inserted_dataset.data_center.long_name,
                         'Nansen Environmental and Remote Sensing Centre')
        self.assertEqual(inserted_dataset.data_center.data_center_url,
                         'http://www.nersc.no/main/index2.php')

        self.assertEqual(inserted_dataset.ISO_topic_category.name, 'Oceans')

        self.assertEqual(inserted_dataset.gcmd_location.category, 'VERTICAL LOCATION')
        self.assertEqual(inserted_dataset.gcmd_location.type, 'SEA SURFACE')

    def test_ingest_same_url_twice(self):
        """The same URL must not be ingested twice"""
        initial_datasets_count = Dataset.objects.count()

        ingester = ingesters.NansatIngester()
        with self.assertLogs(ingesters.LOGGER):
            ingester.ingest([os.path.join(os.path.dirname(__file__), 'data/arc_metno_dataset.nc')])

        self.assertEqual(Dataset.objects.count(), initial_datasets_count + 1)

        with self.assertLogs(ingesters.LOGGER, level=logging.INFO) as logger_cm:
            ingester.ingest([os.path.join(os.path.dirname(__file__), 'data/arc_metno_dataset.nc')])

        self.assertTrue(logger_cm.records[0].msg.endswith('is already present in the database.'))
        self.assertEqual(Dataset.objects.count(), initial_datasets_count + 1)

    #TODO: make this work
    # def test_ingest_dataset_twice_different_urls(self):
    #     """The same dataset must not be ingested twice even if it is present at different URLs"""
    #     initial_datasets_count = Dataset.objects.count()

    #     ingester = ingesters.NansatIngester()
    #     ingester.ingest([os.path.join(os.path.dirname(__file__), 'data/arc_metno_dataset.nc')])
    #     self.assertEqual(Dataset.objects.count(), initial_datasets_count + 1)

    #     with self.assertLogs(ingesters.LOGGER, level=logging.INFO) as logger_cm:
    #         ingester.ingest([
    #             os.path.join(os.path.dirname(__file__), 'data/arc_metno_dataset_2.nc')])

    #     self.assertTrue(logger_cm.records[0].msg.endswith('already exists in the database.'))
    #     self.assertEqual(Dataset.objects.count(), initial_datasets_count + 1)
