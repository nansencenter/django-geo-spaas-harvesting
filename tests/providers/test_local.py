"""Tests for local crawlers and providers"""
import unittest
import unittest.mock as mock
from collections import OrderedDict
from datetime import datetime, timezone

from django.contrib.gis.geos import GEOSGeometry
from geospaas.catalog.managers import (FILE_SERVICE_NAME,
                                       LOCAL_FILE_SERVICE,
                                       DAP_SERVICE_NAME,
                                       OPENDAP_SERVICE)

import geospaas_harvesting.providers.local as provider_local
from geospaas_harvesting.crawlers import DatasetInfo


class NansatCrawlerTestCase(unittest.TestCase):
    """Tests for NansatCrawler"""

    def setUp(self):
        self.patcher_get_metadata = mock.patch('geospaas_harvesting.providers.local.Nansat')
        self.mock_get_metadata = self.patcher_get_metadata.start()

        self.mock_get_metadata.return_value.get_border_wkt.return_value = (
            'POLYGON((24.88 68.08,22.46 68.71,19.96 69.31,17.39 69.87,24.88 68.08))')

    def tearDown(self):
        self.patcher_get_metadata.stop()

    def test_normalize_netcdf_attributes_with_nansat(self):
        """Test the ingestion of a netcdf file using nansat"""
        self.mock_get_metadata.return_value.get_metadata.side_effect = [
            {'bulletin_type': 'Forecast', 'Conventions': 'CF-1.4', 'field_date': '2017-05-29',
             'field_type': 'Files based on file type nersc_daily',
             'filename': '/vsimem/343PBWM116.vrt', 'Forecast_range': '10 days',
             'history': '20170521:Created by program hyc2proj, version V0.3',
             'institution': 'MET Norway, Henrik Mohns plass 1, N-0313 Oslo, Norway',
             'instrument':
             '{"Category": "In Situ/Laboratory Instruments", "Class": "Data Analysis", '
             '"Type": "Environmental Modeling", "Subtype": "", "Short_Name": "Computer", '
             '"Long_Name": "Computer"}',
             'platform':
             '{"Category": "Models/Analyses", "Series_Entity": "", "Short_Name": "MODELS", '
             '"Long_Name": ""}',
             'references': 'http://marine.copernicus.eu', 'source': 'NERSC-HYCOM model fields',
             'time_coverage_end': '2017-05-27T00:00:00', 'time_coverage_start':
             '2017-05-18T00:00:00',
             'title':
             'Arctic Ocean Physics Analysis and Forecast, 12.5km daily mean '
             '(dataset-topaz4-arc-myoceanv2-be)',
             'dataset_parameters': '["surface_backwards_scattering_coefficient_of_radar_wave"]'}]
        crawler = provider_local.NansatCrawler('/foo')
        normalized_attributes = crawler.get_normalized_attributes(DatasetInfo(''))
        self.assertEqual(normalized_attributes['entry_title'], 'NONE')
        self.assertEqual(normalized_attributes['summary'], 'NONE')
        self.assertEqual(normalized_attributes['time_coverage_start'], datetime(
            year=2017, month=5, day=18, hour=0, minute=0, second=0, tzinfo=timezone.utc))
        self.assertEqual(normalized_attributes['time_coverage_end'], datetime(
            year=2017, month=5, day=27, hour=0, minute=0, second=0, tzinfo=timezone.utc))

        self.assertEqual(normalized_attributes['instrument']['Short_Name'], 'Computer')
        self.assertEqual(normalized_attributes['instrument']['Long_Name'], 'Computer')
        self.assertEqual(normalized_attributes['instrument']['Category'],
                         'In Situ/Laboratory Instruments')
        self.assertEqual(normalized_attributes['instrument']['Subtype'], '')
        self.assertEqual(normalized_attributes['instrument']['Class'], 'Data Analysis')

        self.assertEqual(normalized_attributes['platform']['Short_Name'], 'MODELS')
        self.assertEqual(normalized_attributes['platform']['Long_Name'], '')
        self.assertEqual(normalized_attributes['platform']['Category'], 'Models/Analyses')
        self.assertEqual(normalized_attributes['platform']['Series_Entity'], '')

        expected_geometry = GEOSGeometry((
            'POLYGON((24.88 68.08,22.46 68.71,19.96 69.31,17.39 69.87,24.88 68.08))'), srid=4326)

        # This fails, which is why string representations are compared. Any explanation is welcome.
        # self.assertTrue(normalized_attributes['location_geometry'].equals(expected_geometry))
        self.assertEqual(str(normalized_attributes['location_geometry']), str(expected_geometry))

        self.assertEqual(normalized_attributes['provider']['Short_Name'], 'NERSC')
        self.assertEqual(normalized_attributes['provider']['Long_Name'],
                         'Nansen Environmental and Remote Sensing Centre')
        self.assertEqual(normalized_attributes['provider']['Data_Center_URL'],
                         'http://www.nersc.no/main/index2.php')

        self.assertEqual(
            normalized_attributes['iso_topic_category']['iso_topic_category'], 'Oceans')

        self.assertEqual(
            normalized_attributes['gcmd_location']['Location_Category'], 'VERTICAL LOCATION')
        self.assertEqual(normalized_attributes['gcmd_location']['Location_Type'], 'SEA SURFACE')
        self.assertEqual(
            normalized_attributes['dataset_parameters'],
            [
                OrderedDict(
                    [('standard_name', 'surface_backwards_scattering_coefficient_of_radar_wave'),
                     ('canonical_units', '1'),
                     ('definition',
                         'The scattering/absorption/attenuation coefficient is assumed to be an '
                         'integral over all wavelengths, unless a coordinate of '
                         'radiation_wavelength is included to specify the wavelength. Scattering of'
                         ' radiation is its deflection from its incident path without loss of '
                         'energy. Backwards scattering refers to the sum of scattering into all '
                         'backward angles i.e. scattering_angle exceeding pi/2 radians. A '
                         'scattering_angle should not be specified with this quantity.')
                     ])
            ])

    # TODO: make this work
    # def test_ingest_dataset_twice_different_urls(self):
    #     """The same dataset must not be ingested twice even if it is present at different URLs"""
    #     initial_datasets_count = Dataset.objects.count()

    #     ingester = ingesters.NansatIngester()
    #     ingester.ingest([os.path.join(os.path.dirname(__file__), 'data/nansat/arc_metno_dataset.nc')])
    #     self.assertEqual(Dataset.objects.count(), initial_datasets_count + 1)

    #     with self.assertLogs(ingester.LOGGER, level=logging.INFO) as logger_cm:
    #         ingester.ingest([
    #             os.path.join(os.path.dirname(__file__), 'data/nansat/arc_metno_dataset_2.nc')])

    #     self.assertTrue(logger_cm.records[0].msg.endswith('already exists in the database.'))
    #     self.assertEqual(Dataset.objects.count(), initial_datasets_count + 1)

    def test_exception_handling_of_bad_development_of_mappers(self):
        """Test the exception handling of bad development of 'dataset_parameters' of metadata.
        ANY mapper should return a python list as 'dataset_parameters' of metadata."""
        self.mock_get_metadata.return_value.get_metadata.side_effect = [
            {
            'time_coverage_end': '2017-05-27T00:00:00', 'time_coverage_start':
                '2017-05-18T00:00:00',
                'platform':
                '{"Category": "Models/Analyses", "Series_Entity": "", "Short_Name": "MODELS", '
                '"Long_Name": ""}',
                'instrument':
                '{"Category": "In Situ/Laboratory Instruments", "Class": "Data Analysis", '
                '"Type": "Environmental Modeling", "Subtype": "", "Short_Name": "Computer", '
                '"Long_Name": "Computer"}',
                'dataset_parameters': "{}"}]
        crawler = provider_local.NansatCrawler('/foo')
        with self.assertRaises(TypeError) as err:
            normalized_attributes = crawler.get_normalized_attributes(DatasetInfo(''))
        self.assertEqual(
            err.exception.args[0],
            "Can't ingest '': the 'dataset_parameters' section of the metadata returned by nansat "
            "is not a JSON list")

    def test_no_dataset_parameters(self):
        """If no "dataset_parameters" attribute is present in the
        nansat metadata, normalized_attributes['dataset_parameters']
        should be set to an empty list
        """
        self.mock_get_metadata.return_value.get_metadata.return_value = {
            'time_coverage_end': '2017-05-27T00:00:00',
            'time_coverage_start': '2017-05-18T00:00:00',
            'platform':
                '{"Category": "Models/Analyses", "Series_Entity": "", "Short_Name": "MODELS", '
                '"Long_Name": ""}',
            'instrument':
                '{"Category": "In Situ/Laboratory Instruments", "Class": "Data Analysis", '
                '"Type": "Environmental Modeling", "Subtype": "", "Short_Name": "Computer", '
                '"Long_Name": "Computer"}'
        }
        crawler = provider_local.NansatCrawler('/foo')
        self.assertListEqual(
            crawler.get_normalized_attributes(DatasetInfo(''))['dataset_parameters'],
            [])

    def test_usage_of_nansat_crawler_with_http_protocol_in_the_OPENDAP_cases(self):
        """LOCALHarvester(which uses NansatCrawler) can be used for `OPENDAP provided` files """
        crawler = provider_local.NansatCrawler('/foo')
        self.mock_get_metadata.return_value.get_metadata.side_effect = [{
            'time_coverage_end': '2017-05-27T00:00:00', 'time_coverage_start':
                '2017-05-18T00:00:00',
                'platform':
                '{"Category": "Models/Analyses", "Series_Entity": "", "Short_Name": "MODELS", '
                '"Long_Name": ""}',
                'instrument':
                '{"Category": "In Situ/Laboratory Instruments", "Class": "Data Analysis", '
                '"Type": "Environmental Modeling", "Subtype": "", "Short_Name": "Computer", '
                '"Long_Name": "Computer"}',
        }]
        normalized_attributes = crawler.get_normalized_attributes(DatasetInfo('http://'))
        self.assertEqual(normalized_attributes['geospaas_service_name'], DAP_SERVICE_NAME)
        self.assertEqual(normalized_attributes['geospaas_service'], OPENDAP_SERVICE)

    def test_usage_of_nansat_crawler_with_local_file(self):
        """LOCALHarvester(which uses NansatCrawler) can be used for local files """
        crawler = provider_local.NansatCrawler('/foo')
        self.mock_get_metadata.return_value.get_metadata.side_effect = [{
            'time_coverage_end': '2017-05-27T00:00:00', 'time_coverage_start':
                '2017-05-18T00:00:00',
                'platform':
                '{"Category": "Models/Analyses", "Series_Entity": "", "Short_Name": "MODELS", '
                '"Long_Name": ""}',
                'instrument':
                '{"Category": "In Situ/Laboratory Instruments", "Class": "Data Analysis", '
                '"Type": "Environmental Modeling", "Subtype": "", "Short_Name": "Computer", '
                '"Long_Name": "Computer"}',
        }]
        normalized_attributes = crawler.get_normalized_attributes(DatasetInfo('/src/blabla'))
        self.assertEqual(normalized_attributes['geospaas_service_name'], FILE_SERVICE_NAME)
        self.assertEqual(normalized_attributes['geospaas_service'], LOCAL_FILE_SERVICE)


    def test_exception_handling_of_bad_inputting_of_nansat_crawler_with_ftp_protocol(self):
        """LOCALHarvester(which uses NansatCrawler) is only for local file addresses"""
        crawler = provider_local.NansatCrawler('/foo')
        self.mock_get_metadata.return_value.get_metadata.side_effect = ['']
        with self.assertRaises(ValueError) as err:
            normalized_attributes = crawler.get_normalized_attributes(DatasetInfo('ftp://'))
        self.assertEqual(
            err.exception.args[0],
            "Can't ingest 'ftp://': nansat can't open remote ftp files")

    def test_reprojection_based_on_gcps(self):
        """Nansat crawler should reproject if there is any GC point in the metadata"""
        self.mock_get_metadata.return_value.vrt.dataset.GetGCPs.return_value = True
        self.mock_get_metadata.return_value.get_metadata.side_effect = [{
            'time_coverage_end': '2017-05-27T00:00:00', 'time_coverage_start':
                '2017-05-18T00:00:00',
                'platform':
                '{"Category": "Models/Analyses", "Series_Entity": "", "Short_Name": "MODELS", '
                '"Long_Name": ""}',
                'instrument':
                '{"Category": "In Situ/Laboratory Instruments", "Class": "Data Analysis", '
                '"Type": "Environmental Modeling", "Subtype": "", "Short_Name": "Computer", '
                '"Long_Name": "Computer"}',
        }]
        crawler = provider_local.NansatCrawler('/foo')
        normalized_attributes = crawler.get_normalized_attributes(DatasetInfo(''))
        self.mock_get_metadata.return_value.reproject_gcps.assert_called_once()
