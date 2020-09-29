"""Tests for the daemon script"""

import logging
import os
import os.path
import signal
import sys
import tempfile
import unittest
import unittest.mock as mock

import django

import geospaas_harvesting.harvest as harvest
import geospaas_harvesting.harvesters as harvesters
import geospaas_harvesting.harvest as harvest
import tests.stubs as stubs
from geospaas.vocabularies.models import Parameter

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
                'poll_interval': 0.1
            }
        )

    def test_loading_valid_conf_with_password(self):
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
                'poll_interval': 0.1
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
        self.assertEqual(len(configuration), 2)

    def test_iterable(self):
        """Configuration objects must be iterable"""
        with self.assertLogs(harvest.LOGGER):
            configuration = harvest.Configuration(CONFIGURATION_FILES['ok'])
        self.assertTrue(callable(getattr(configuration, '__iter__')))
        config_iterator = iter(configuration)
        self.assertEqual(next(config_iterator), 'harvesters')
        self.assertEqual(next(config_iterator), 'poll_interval')
        with self.assertRaises(StopIteration):
            next(config_iterator)


class TemporaryPersistenceDirTestCase(unittest.TestCase):
    """
    Base class for test cases which need to put persistence files in a temporary directory.
    Child classes should use super().setUp() and/or super().tearDown()
    if they need extra pre/post processing.
    """
    def setUp(self):
        self.temp_directory = tempfile.TemporaryDirectory()
        mock.patch('geospaas_harvesting.harvest.PERSISTENCE_DIR',
                   self.temp_directory.name).start()
        self.addCleanup(mock.patch.stopall)

    def tearDown(self):
        self.temp_directory.cleanup()


class MainTestCase(TemporaryPersistenceDirTestCase):
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

    @mock.patch.object(sys, 'argv', ['harvest.py'])
    @mock.patch.object(
        harvest.Configuration, 'DEFAULT_CONFIGURATION_PATH', CONFIGURATION_FILES['ok'])
    @mock.patch('geospaas.vocabularies.management.commands.update_vocabularies.Command')
    def test_main_ends_on_workers_exceptions(self, mock_update_vocab):
        """The main process must end if all workers finished with exceptions"""
        setattr(harvesters, 'TestHarvester', stubs.StubExceptionHarvester)
        with self.assertLogs(harvest.LOGGER_NAME, level=logging.ERROR) as daemon_logs_cm:
            harvest.main()
        self.assertEqual(daemon_logs_cm.records[0].message,
                         "All harvester processes encountered errors")

    #TODO
    # def test_interrupt_main_process(self):
    #     """Main process terminates workers and exits on interruption"""


class PersistenceTestCase(TemporaryPersistenceDirTestCase):
    """Test the persistence of the harvesters"""
    # These tests are inelegant, it might be necessary to rewrite the daemon script in a more easily
    # testable form

    def setUp(self):
        super().setUp()
        self.config_mock = mock.patch('geospaas_harvesting.harvest.Configuration').start()
        self.patcher_param_count = mock.patch.object(Parameter.objects, 'count')
        self.mock_param_count = self.patcher_param_count.start()
        self.mock_param_count.return_value = 2

    def test_get_persistence_files(self):
        """Test that the persistence files are correctly retrieved and sorted"""
        with mock.patch('os.listdir') as mock_listdir:
            mock_listdir.return_value = [
                '2020-04-16-08-43-53-112204_podaac',
                '2020-04-16-08-44-02-201730_copernicus_sentinel',
                '2020-04-16-08-43-58-756088_copernicus_sentinel',
                '2020-04-16-08-43-49-606514_podaac'
            ]
            self.assertListEqual(harvest.get_persistence_files(), [
                '2020-04-16-08-44-02-201730_copernicus_sentinel',
                '2020-04-16-08-43-58-756088_copernicus_sentinel',
                '2020-04-16-08-43-53-112204_podaac',
                '2020-04-16-08-43-49-606514_podaac'
            ])

    def test_get_persistence_files_inexistent_directory(self):
        """
        The get_persistence_files() function must return an empty list in case the directory does
        not exist
        """
        with mock.patch.object(harvest, 'PERSISTENCE_DIR', '/path/does/not/exist'):
            self.assertListEqual(harvest.get_persistence_files(), [])

    def test_get_last_persistence_file_name(self):
        """
        The get_harvester_file_name(harvester_name) function must return the file with the most
        recent timestamp and which matches the harvester's name.
        """
        with mock.patch('os.listdir') as mock_listdir:
            mock_listdir.return_value = [
                '2020-04-16-08-43-49-606514_podaac',
                '2020-04-16-08-43-53-112204_podaac',
                '2020-04-16-08-43-58-756088_copernicus_sentinel',
                '2020-04-16-08-44-02-201730_copernicus_sentinel'
            ]
            self.assertEqual(harvest.get_harvester_file_name('podaac'),
                             '2020-04-16-08-43-53-112204_podaac')

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
        setattr(harvesters, 'StubInterruptHarvester', stubs.StubInterruptHarvester)
        assert_daemon_logs = self.assertLogs(harvest.LOGGER)
        assert_ingester_logs = self.assertLogs(stubs.StubExceptionIngester.LOGGER)

        with self.assertRaises(SystemExit), assert_daemon_logs, assert_ingester_logs:
            harvest.launch_harvest('stub_interrupt_harvester', {
                'class': 'StubInterruptHarvester',
                'urls': ['https://random1.url']
            })

        created_persistence_files = os.listdir(harvest.PERSISTENCE_DIR)
        self.assertEqual(len(created_persistence_files), 1)
        created_file_path = os.path.join(harvest.PERSISTENCE_DIR, created_persistence_files[0])
        self.assertTrue(os.path.getsize(created_file_path) > 0)

    def test_dump_on_exception(self):
        """The harvesters state is dumped when any other exception is raised"""
        setattr(harvesters, 'StubExceptionHarvester', stubs.StubExceptionHarvester)
        assert_daemon_logs = self.assertLogs(harvest.LOGGER_NAME)
        assert_ingester_logs = self.assertLogs(stubs.StubExceptionIngester.LOGGER)

        with self.assertRaises(ZeroDivisionError), assert_daemon_logs, assert_ingester_logs:
            harvest.launch_harvest('stub_exception_harvester', {
                'class': 'StubExceptionHarvester',
                'urls': ['https://random1.url']
            })

        created_persistence_files = os.listdir(harvest.PERSISTENCE_DIR)
        self.assertEqual(len(created_persistence_files), 1)
        created_file_path = os.path.join(harvest.PERSISTENCE_DIR, created_persistence_files[0])
        self.assertTrue(os.path.getsize(created_file_path) > 0)

    def test_no_dump_on_keyboard_interrupt_if_disabled(self):
        """
        The harvesters state should not be dumped when a KeyboardInterrupt exception is raised
        if "dump_on_interruption" is set to False in the configuration file
        """
        self.config_mock.return_value._data = {
            'dump_on_interruption': False
        }

        setattr(harvesters, 'StubInterruptHarvester', stubs.StubInterruptHarvester)
        assert_daemon_logs = self.assertLogs(harvest.LOGGER)
        assert_ingester_logs = self.assertLogs(stubs.StubExceptionIngester.LOGGER)

        with self.assertRaises(SystemExit), assert_daemon_logs, assert_ingester_logs:
            with mock.patch('geospaas_harvesting.harvest.dump_with_timestamp') as mock_dump:
                harvest.launch_harvest('stub_interrupt_harvester', {
                    'class': 'StubInterruptHarvester',
                    'urls': ['https://random1.url']
                })
            mock_dump.assert_not_called()

    def test_no_dump_on_exception_if_disabled(self):
        """
        The harvesters state should not be dumped when an unexpected exception is raised
        if "dump_on_interruption" is set to False in the configuration file
        """
        self.config_mock.return_value._data = {
            'dump_on_interruption': False
        }

        setattr(harvesters, 'StubExceptionHarvester', stubs.StubExceptionHarvester)
        assert_daemon_logs = self.assertLogs(harvest.LOGGER_NAME)
        assert_ingester_logs = self.assertLogs(stubs.StubExceptionIngester.LOGGER)

        with self.assertRaises(ZeroDivisionError), assert_daemon_logs, assert_ingester_logs:
            with mock.patch('geospaas_harvesting.harvest.dump_with_timestamp') as mock_dump:
                harvest.launch_harvest('stub_exception_harvester', {
                    'class': 'StubExceptionHarvester',
                    'urls': ['https://random1.url']
                })
            mock_dump.assert_not_called()

    def test_no_loading_if_persistence_disabled(self):
        """load_or_create_harvester() should not try to load if the load_dumped argument is False"""
        with mock.patch('geospaas_harvesting.harvest.create_harvester') as mock_create:
            with mock.patch('geospaas_harvesting.harvest.load_last_dumped_harvester') as mock_load:
                with self.assertLogs(harvest.LOGGER, logging.INFO):
                    harvest.load_or_create_harvester('Harvester', {}, False)
        mock_load.assert_not_called()
        mock_create.assert_called_with({})

    def test_load_last_dumped_harvester(self):
        """Test that the last dumped harvester is correctly loaded"""
        original_harvester = harvesters.PODAACHarvester(urls=['url1', 'url2'])
        with self.assertLogs(harvest.LOGGER):
            harvest.dump_with_timestamp(original_harvester, 'test_harvester')
            loaded_harvester = harvest.load_last_dumped_harvester('test_harvester')
        self.assertDictEqual(original_harvester.config, loaded_harvester.config)
        self.assertEqual(len(os.listdir(harvest.PERSISTENCE_DIR)), 0)

    def test_resume_with_correct_url(self):
        """The correct url is ingested after deserialization"""
        setattr(harvesters, 'StubExceptionHarvester', stubs.StubExceptionHarvester)

        assert_exit = self.assertRaises(ZeroDivisionError)
        assert_daemon_logs = self.assertLogs(harvest.LOGGER_NAME)
        assert_ingester_logs = self.assertLogs(stubs.StubExceptionIngester.LOGGER)

        with assert_exit, assert_daemon_logs, assert_ingester_logs as ingester_logs_cm:
            harvest.launch_harvest('stub_harvester', {
                'class': 'StubExceptionHarvester',
                'urls': ['https://random1.url']
            })
        self.assertEqual(ingester_logs_cm.records[0].message, "https://random1.url/ressource_1")

        with assert_daemon_logs, assert_ingester_logs as ingester_logs_cm:
            harvest.launch_harvest('stub_harvester', {
                'class': 'StubExceptionHarvester',
                'urls': ['https://random1.url']
            })
        self.assertEqual(ingester_logs_cm.records[0].message, "https://random1.url/ressource_3")

    # TODO: test persistence for each crawler and harvester
