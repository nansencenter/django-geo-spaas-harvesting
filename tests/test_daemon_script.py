"""Tests for the daemon script"""
#pylint: disable=protected-access

import logging
import os
import os.path
import sys
import unittest
import unittest.mock as mock

import geospaas_harvesting.ingesters as ingesters
import geospaas_harvesting.harvesters as harvesters
from .stubs import StubExceptionHarvesterList, StubInterruptHarvesterList

os.environ.setdefault('GEOSPAAS_PERSISTENCE_DIR', os.path.join('/', 'tmp', 'harvesting_tests'))
import geospaas_harvesting.harvest as harvest

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
                }
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
        self.assertEqual(len(configuration), 1)

class MainTestCase(unittest.TestCase):
    """Test the main() function in the daemon script"""

    class TestHarvester(harvesters.Harvester):
        """Dummy Harvester used for tests"""
        def _create_crawlers(self):
            pass
        def _create_ingester(self):
            pass
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
        with self.assertLogs(harvest.LOGGER, level=logging.ERROR):
            with self.assertRaises(SystemExit) as system_exit_cm:
                harvest.main()
        self.assertGreater(system_exit_cm.exception.code, 0)

    def test_raise_keyboard_error(self):
        """The raise_keyboard_interrupt function must raise a KeyboardInterrupt exception"""
        with self.assertRaises(KeyboardInterrupt):
            harvest.raise_keyboard_interrupt()

    def test_log_on_harvest_value_error(self):
        """An error must be logged if a ValueError is raised in a Harvester.harvest() call"""
        harvester_list_patcher = mock.patch.object(
            harvesters, 'HarvesterList', StubInterruptHarvesterList)
        configuration_patcher = mock.patch('geospaas_harvesting.harvest.Configuration')
        harvest_patcher = mock.patch.object(harvesters.Harvester, 'harvest', side_effect=ValueError)

        with harvester_list_patcher, configuration_patcher, harvest_patcher:
            with self.assertRaises(ValueError):
                with self.assertLogs(harvest.LOGGER, level=logging.ERROR):
                    harvest.main()


class PersistenceTestCase(unittest.TestCase):
    """Test the persistence of the harvesters"""
    # These tests are inelegant, it might be necessary to rewrite the daemon script in a more easily
    # testable form

    def setUp(self):
        try:
            os.mkdir(harvest.PERSISTENCE_DIR)
        except FileExistsError:
            pass

        self.conf_patcher = mock.patch('geospaas_harvesting.harvest.Configuration')
        self.conf_patcher.start()

    def tearDown(self):
        self.conf_patcher.stop()
        try:
            os.remove(harvest.PERSISTENCE_FILE)
        except FileNotFoundError:
            pass

    def test_log_on_dump_error(self):
        """
        An error must be logged if the persistence file is a directory or is in a non existing
        directory
        """
        with self.assertLogs(harvest.LOGGER, level=logging.ERROR):
            harvest.dump('some_object', '/this/path/does/not/exist')

        with self.assertLogs(harvest.LOGGER, level=logging.ERROR):
            harvest.dump('some_object', os.path.dirname(__file__))

        with mock.patch.object(harvest, 'open', side_effect=KeyError):
            with self.assertLogs(harvest.LOGGER, level=logging.ERROR):
                harvest.dump('some_object', '/this/path/does/not/exist')

    def test_log_on_load_error(self):
        """
        An error must be logged if the persistence file is a directory or is in a non existing
        directory
        """
        with self.assertLogs(harvest.LOGGER, level=logging.ERROR):
            _ = harvest.load('/this/path/does/not/exist')
        with self.assertLogs(harvest.LOGGER, level=logging.ERROR):
            _ = harvest.load(os.path.dirname(__file__))

    def test_dump_on_keyboard_interrupt(self):
        """The harvesters state is dumped when a KeyboardInterrupt exception is raised"""

        harvester_list_patcher = mock.patch.object(
            harvesters, 'HarvesterList', StubInterruptHarvesterList)

        assert_daemon_logs = self.assertLogs(harvest.LOGGER)
        assert_ingester_logs = self.assertLogs(ingesters.LOGGER)

        with harvester_list_patcher, self.assertRaises(SystemExit):
            with assert_daemon_logs, assert_ingester_logs:
                harvest.main()

        self.assertTrue(os.path.exists(harvest.PERSISTENCE_FILE))


    def test_dump_on_exception(self):
        """The harvesters state is dumped when any other exception is raised"""

        harvester_list_patcher = mock.patch.object(
            harvesters, 'HarvesterList', StubExceptionHarvesterList)

        assert_daemon_logs = self.assertLogs(harvest.LOGGER_NAME)
        assert_ingester_logs = self.assertLogs(ingesters.LOGGER)

        with harvester_list_patcher, self.assertRaises(IndexError):
            with assert_daemon_logs, assert_ingester_logs:
                harvest.main()

        self.assertTrue(os.path.exists(harvest.PERSISTENCE_FILE))

    def test_state_file_removed_after_loading(self):
        """The persistence file is removed after deserialization"""
        harvester_interrupt_list_patcher = mock.patch.object(
            harvesters, 'HarvesterList', StubInterruptHarvesterList)

        assert_exit = self.assertRaises(SystemExit)
        assert_daemon_logs = self.assertLogs(harvest.LOGGER_NAME)
        assert_ingester_logs = self.assertLogs(ingesters.LOGGER)

        with harvester_interrupt_list_patcher, assert_exit:
            with assert_daemon_logs, assert_ingester_logs:
                harvest.main()
            self.assertTrue(os.path.exists(harvest.PERSISTENCE_FILE))

        with assert_daemon_logs, assert_ingester_logs:
            harvest.main()
        self.assertFalse(os.path.exists(harvest.PERSISTENCE_FILE))

    def test_resume_with_correct_url(self):
        """The correct harvester is used after deserialization"""
        harvester_interrupt_list_patcher = mock.patch.object(
            harvesters, 'HarvesterList', StubInterruptHarvesterList)

        assert_exit = self.assertRaises(SystemExit)
        assert_daemon_logs = self.assertLogs(harvest.LOGGER_NAME)
        assert_ingester_logs = self.assertLogs(ingesters.LOGGER)

        with harvester_interrupt_list_patcher, assert_exit, assert_daemon_logs:
            with assert_ingester_logs as ingester_logs_cm:
                harvest.main()
        self.assertEqual(ingester_logs_cm.records[0].message, "https://random1.url/ressource_1")

        with assert_daemon_logs, assert_ingester_logs as ingester_logs_cm:
            harvest.main()
        self.assertEqual(ingester_logs_cm.records[0].message, "https://random1.url/ressource_3")

    # TODO: test persistence for each crawler and harvester
