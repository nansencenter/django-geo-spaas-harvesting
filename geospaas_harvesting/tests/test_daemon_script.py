"""Tests for the daemon script"""
#pylint: disable=protected-access

import logging
import os
import sys
import unittest
import unittest.mock as mock

import geospaas_harvesting.harvest as harvest
import geospaas_harvesting.harvesters as harvesters

CONFIGURATION_PATH = os.path.join(os.path.dirname(__file__), 'data', 'configuration_files')
CONFIGURATION_FILES = {
    'ok': os.path.join(CONFIGURATION_PATH, 'harvest_ok.yml'),
    'empty': os.path.join(CONFIGURATION_PATH, 'harvest_empty.yml'),
    'no_harvesters_section': os.path.join(CONFIGURATION_PATH,
                                          'harvest_no_harvesters_section.yml'),
    'no_harvesters': os.path.join(CONFIGURATION_PATH, 'harvest_no_harvesters.yml'),
    'no_class': os.path.join(CONFIGURATION_PATH, 'harvest_no_class.yml'),
    'no_urls': os.path.join(CONFIGURATION_PATH, 'harvest_no_urls.yml')
}


class ConfigurationTestCase(unittest.TestCase):
    """Test the configuration manager"""

    def test_loading_valid_conf(self):
        """Correct configuration file parsing"""
        configuration = harvest.Configuration(CONFIGURATION_FILES['ok'])
        self.assertDictEqual(
            configuration._data,
            {
                'harvesters': {
                    'test': {
                        'class': 'TestHarvester',
                        'urls': ['https://random1.url', 'https://random2.url']
                    }
                }
            }
        )

    def test_loading_empty_conf(self):
        """An exception must be raised if the configuration file is empty"""
        with self.assertRaises(AssertionError, msg='Configuration data is empty'):
            _ = harvest.Configuration(CONFIGURATION_FILES['empty'])

    def test_loading_conf_without_harvester_section(self):
        """
        An exception must be raised if the configuration does not contain a 'harvesters' section
        """
        with self.assertRaises(AssertionError, msg='Invalid top-level keys'):
            _ = harvest.Configuration(CONFIGURATION_FILES['no_harvesters_section'])

    def test_loading_conf_without_harvesters(self):
        """
        An exception must be raised if the 'harvesters' section is empty
        """
        with self.assertRaises(AssertionError, msg='No harvesters are configured'):
            _ = harvest.Configuration(CONFIGURATION_FILES['no_harvesters'])

    def test_loading_conf_without_class(self):
        """
        An exception must be raised if a harvester configuration does not contain 'class'
        """
        message_regex = "^Harvester configuration must contain the following keys: .*$"
        with self.assertRaisesRegex(AssertionError, message_regex):
            _ = harvest.Configuration(CONFIGURATION_FILES['no_class'])

    def test_loading_conf_without_urls(self):
        """
        An exception must be raised if a harvester configuration does not contain 'urls'
        """
        message_regex = "^Harvester configuration must contain the following keys: .*$"
        with self.assertRaisesRegex(AssertionError, message_regex):
            _ = harvest.Configuration(CONFIGURATION_FILES['no_urls'])

    def test_inexistent_config_file(self):
        """
        An exception must be raised if an attempt is made to load the configuration from an empty
        file, and an error message must be logged
        """
        conf_file_path = '/this_file_should_not_exist'
        with self.assertRaises(AssertionError, msg='Configuration data is empty'), self.assertLogs(
                logging.getLogger(harvest.LOGGER_NAME), level=logging.ERROR) as logs_cm:
            _ = harvest.Configuration(conf_file_path)
        self.assertEqual(len(logs_cm.records), 1)
        self.assertEqual(logs_cm.records[0].exc_info[0], FileNotFoundError)

    def test_get_config_path_from_cli(self):
        """Must be able to retrieve the configuration file path from the CLI"""
        conf_file_path = CONFIGURATION_FILES['ok']

        # Test short argument
        with mock.patch.object(sys, 'argv', ['harvest.py', '-c', conf_file_path]):
            configuration = harvest.Configuration()
        self.assertEqual(configuration._path, conf_file_path)

        # Test long argument
        with mock.patch.object(sys, 'argv', ['harvest.py', '--config', conf_file_path]):
            configuration = harvest.Configuration()
        self.assertEqual(configuration._path, conf_file_path)

    @mock.patch.object(sys, 'argv', ['harvest.py'])
    @mock.patch.object(
        harvest.Configuration, 'DEFAULT_CONFIGURATION_PATH', CONFIGURATION_FILES['ok'])
    def test_default_config_path(self):
        """
        If the file path cannot be obtained from the CLI or constructor arguments, the default value
        must be used
        """
        configuration = harvest.Configuration()
        self.assertEqual(configuration._path, CONFIGURATION_FILES['ok'])

    def test_subscriptable(self):
        """Configuration objects must be subscriptable"""
        configuration = harvest.Configuration(CONFIGURATION_FILES['ok'])
        self.assertTrue(callable(getattr(configuration, '__getitem__')))
        self.assertIsNotNone(configuration['harvesters'])

    def test_length(self):
        """the __len__ method must be correctly implemented"""
        configuration = harvest.Configuration(CONFIGURATION_FILES['ok'])
        self.assertTrue(callable(getattr(configuration, '__len__')))
        self.assertEqual(len(configuration), 1)

class MainTestCase(unittest.TestCase):
    """Test the main() function in the daemon script"""
    #TODO: complete this

    class TestHarvester(harvesters.Harvester):
        """Dummy Harvester used for tests"""
        def harvest(self):
            pass

    @mock.patch.object(sys, 'argv', ['harvest.py'])
    @mock.patch.object(
        harvest.Configuration, 'DEFAULT_CONFIGURATION_PATH', CONFIGURATION_FILES['empty'])
    def test_error_on_invalid_configuration(self):
        """
        Configuration validation errors are caught and logged, then the program exists with a
        non-zero code
        """
        with self.assertLogs(
                logging.getLogger(harvest.LOGGER_NAME),
                level=logging.ERROR), self.assertRaises(SystemExit) as system_exit_cm:
            harvest.main()
        self.assertGreater(system_exit_cm.exception.code, 0)
