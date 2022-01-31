"""Tests for the daemon script"""

import logging
import os
import os.path
import signal
import sys
import tempfile
import unittest
import unittest.mock as mock


import geospaas_harvesting.harvest as harvest
import geospaas_harvesting.harvesters as harvesters
import geospaas_harvesting.harvest as harvest
import tests.stubs as stubs

CONFIGURATION_PATH = os.path.join(os.path.dirname(__file__), 'data', 'configuration_files')
CONFIGURATION_FILES = {
    'ok': os.path.join(CONFIGURATION_PATH, 'harvest_ok.yml'),
    'ok_pass': os.path.join(CONFIGURATION_PATH, 'harvest_ok_password.yml'),
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
        with self.assertLogs(harvest.LOGGER):
            configuration = harvest.Configuration(CONFIGURATION_FILES['ok'])
        self.assertDictEqual(
            configuration._data,
            {
                'harvesters': {
                    'test': {
                        'class': 'TestHarvester',
                        'urls': ['https://random1.url', 'https://random2.url']
                    }
                },
                'update_vocabularies': True,
                'update_pythesint': True
            }
        )

    def test_loading_valid_conf_with_env_var_inside_it(self):
        """Correct configuration file parsing with changing the 'password name' into 'password value' """
        os.environ["test_password"]="password_value"
        os.environ["test_sth_like_password"]="sth_like_password_value"
        with self.assertLogs(harvest.LOGGER):
            configuration = harvest.Configuration(CONFIGURATION_FILES['ok_pass'])
        self.assertDictEqual(
            configuration._data,
            {
                'harvesters': {
                    'test': {
                        'class': 'TestHarvester',
                        'urls': ['https://random1.url', 'https://random2.url'],
                        'password': 'password_value',
                        'sth_like_password': 'sth_like_password_value'
                    },
                },
                'update_vocabularies': True,
                'update_pythesint': True
            }
        )

    def test_loading_empty_conf(self):
        """An exception must be raised if the configuration file is empty"""
        with self.assertRaises(AssertionError, msg='Configuration data is empty'):
            with self.assertLogs(harvest.LOGGER):
                _ = harvest.Configuration(CONFIGURATION_FILES['empty'])

    def test_loading_conf_without_harvester_section(self):
        """
        An exception must be raised if the configuration does not contain a 'harvesters' section
        """
        with self.assertRaises(AssertionError, msg='Invalid top-level keys'):
            with self.assertLogs(harvest.LOGGER):
                _ = harvest.Configuration(CONFIGURATION_FILES['no_harvesters_section'])

    def test_loading_conf_without_harvesters(self):
        """
        An exception must be raised if the 'harvesters' section is empty
        """
        with self.assertRaises(AssertionError, msg='No harvesters are configured'):
            with self.assertLogs(harvest.LOGGER):
                _ = harvest.Configuration(CONFIGURATION_FILES['no_harvesters'])

    def test_loading_conf_without_class(self):
        """
        An exception must be raised if a harvester configuration does not contain 'class'
        """
        message_regex = "^Harvester configuration must contain the following keys: .*$"
        with self.assertRaisesRegex(AssertionError, message_regex):
            with self.assertLogs(harvest.LOGGER):
                _ = harvest.Configuration(CONFIGURATION_FILES['no_class'])

    def test_inexistent_config_file(self):
        """
        An exception must be raised if an attempt is made to load the configuration from an empty
        file, and an error message must be logged
        """
        conf_file_path = '/this_file_should_not_exist'
        with self.assertRaises(AssertionError, msg='Configuration data is empty'):
            with self.assertLogs(harvest.LOGGER, level=logging.ERROR) as logs_cm:
                _ = harvest.Configuration(conf_file_path)
        self.assertEqual(len(logs_cm.records), 1)
        self.assertEqual(logs_cm.records[0].exc_info[0], FileNotFoundError)

    def test_get_config_path_from_cli(self):
        """Must be able to retrieve the configuration file path from the CLI"""
        conf_file_path = CONFIGURATION_FILES['ok']

        # Test short argument
        with mock.patch.object(sys, 'argv', [harvest.__file__, '-c', conf_file_path]):
            with self.assertLogs(harvest.LOGGER):
                configuration = harvest.Configuration()
        self.assertEqual(configuration._path, conf_file_path)

        # Test long argument
        with mock.patch.object(sys, 'argv', [harvest.__file__, '--config', conf_file_path]):
            with self.assertLogs(harvest.LOGGER):
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
        with self.assertLogs(harvest.LOGGER):
            configuration = harvest.Configuration()
        self.assertEqual(configuration._path, CONFIGURATION_FILES['ok'])

    def test_subscriptable(self):
        """Configuration objects must be subscriptable"""
        with self.assertLogs(harvest.LOGGER):
            configuration = harvest.Configuration(CONFIGURATION_FILES['ok'])
        self.assertTrue(callable(getattr(configuration, '__getitem__')))
        self.assertIsNotNone(configuration['harvesters'])

    def test_length(self):
        """the __len__ method must be correctly implemented"""
        with self.assertLogs(harvest.LOGGER):
            configuration = harvest.Configuration(CONFIGURATION_FILES['ok'])
        self.assertTrue(callable(getattr(configuration, '__len__')))
        self.assertEqual(len(configuration), 3)

    def test_iterable(self):
        """Configuration objects must be iterable"""
        with self.assertLogs(harvest.LOGGER):
            configuration = harvest.Configuration(CONFIGURATION_FILES['ok'])
        self.assertTrue(callable(getattr(configuration, '__iter__')))
        config_iterator = iter(configuration)
        self.assertEqual(next(config_iterator), 'update_vocabularies')
        self.assertEqual(next(config_iterator), 'update_pythesint')
        self.assertEqual(next(config_iterator), 'harvesters')
        with self.assertRaises(StopIteration):
            next(config_iterator)


class MainTestCase(unittest.TestCase):
    """Test the main() function in the daemon script"""

    @mock.patch.object(sys, 'argv', ['harvest.py'])
    @mock.patch.object(
        harvest.Configuration, 'DEFAULT_CONFIGURATION_PATH', CONFIGURATION_FILES['empty'])
    def test_error_on_invalid_configuration(self):
        """
        Configuration validation errors are caught and logged, then the program exists with a
        non-zero code
        """
        with self.assertLogs(harvest.LOGGER, level=logging.ERROR):
            with self.assertRaises(SystemExit) as system_exit_cm:
                harvest.main()
        self.assertGreater(system_exit_cm.exception.code, 0)

    def test_raise_keyboard_error(self):
        """The raise_keyboard_interrupt function must raise a KeyboardInterrupt exception"""
        with self.assertRaises(KeyboardInterrupt):
            harvest.raise_keyboard_interrupt()

    def test_init_worker(self):
        """Workers must ignore SIGINT"""
        harvest.init_worker()
        self.assertEqual(signal.getsignal(signal.SIGINT), signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_DFL)

    def test_refresh_vocabularies(self):
        """The database vocabulary objects must be updated
        only if the corresponding setting is True
        """
        config = {'update_vocabularies': True}
        with mock.patch('django.core.management.call_command') as mock_call_command:
            with self.assertLogs(logger= harvest.LOGGER, level=logging.INFO):
                harvest.refresh_vocabularies(config)
            mock_call_command.assert_called_with('update_vocabularies', force=False, versions=None)

        config = {'update_vocabularies': False}
        with mock.patch('django.core.management.call_command') as mock_call_command:
            harvest.refresh_vocabularies(config)
            mock_call_command.assert_not_called()

    def test_refresh_vocabularies_and_update_pythesint(self):
        """The database vocabulary objects must be updated and
        pythesint's data refreshed only if the corresponding settings
        are both True
        """
        # Refresh the database but not the pythesint files
        config = {'update_vocabularies': True, 'update_pythesint': False}
        with mock.patch('django.core.management.call_command') as mock_call_command:
            with self.assertLogs(logger=harvest.LOGGER, level=logging.INFO):
                harvest.refresh_vocabularies(config)
            mock_call_command.assert_called_with('update_vocabularies', force=False, versions=None)

        # Refresh both the database and the pythesint files
        config = {'update_vocabularies': True, 'update_pythesint': True}
        with mock.patch('django.core.management.call_command') as mock_call_command:
            with self.assertLogs(logger=harvest.LOGGER, level=logging.INFO):
                harvest.refresh_vocabularies(config)
            mock_call_command.assert_called_with('update_vocabularies', force=True, versions=None)

        # Refresh both the database and the pythesint files,
        # specifying versions
        config = {
            'update_vocabularies': True,
            'update_pythesint': True,
            'pythesint_versions': {'gcmd_instrument': '9.1.5'}
        }
        with mock.patch('django.core.management.call_command') as mock_call_command:
            with self.assertLogs(logger=harvest.LOGGER, level=logging.INFO):
                harvest.refresh_vocabularies(config)
            mock_call_command.assert_called_with(
                'update_vocabularies',
                force=True,
                versions={'gcmd_instrument': '9.1.5'})

    #TODO
    # def test_interrupt_main_process(self):
    #     """Main process terminates workers and exits on interruption"""
