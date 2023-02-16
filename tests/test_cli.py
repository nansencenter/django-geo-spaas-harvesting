"""Tests for the CLI"""
import argparse
import io
import logging
import unittest
import unittest.mock as mock
import signal

import geospaas_harvesting.cli as cli


class CLITestCase(unittest.TestCase):
    """Tests for the CLI"""

    def test_init_worker(self):
        """Workers must ignore SIGINT"""
        cli.init_worker()
        self.assertEqual(signal.getsignal(signal.SIGINT), signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_DFL)

    def test_refresh_vocabularies(self):
        """The database vocabulary objects must be updated
        only if the corresponding setting is True
        """
        config = mock.Mock(update_vocabularies=True,
                           update_pythesint=False,
                           pythesint_versions=None)
        with mock.patch('django.core.management.call_command') as mock_call_command:
            with self.assertLogs(logger=cli.logger, level=logging.INFO):
                cli.refresh_vocabularies(config)
            mock_call_command.assert_called_with('update_vocabularies', force=False, versions=None)

    def test_no_refresh_vocabularies(self):
        """The vocabularies should not be refreshed if the setting is
        False
        """
        config = mock.Mock(update_vocabularies=False)
        with mock.patch('django.core.management.call_command') as mock_call_command, \
             mock.patch('geospaas.vocabularies.models.Parameter.objects.count', return_value=2):
            cli.refresh_vocabularies(config)
            mock_call_command.assert_not_called()

    def test_refresh_vocabularies_parameters_error(self):
        """Test that an exception is raised if no refresh is requested
        and there are no Parameters in the database
        """
        config = mock.Mock(update_vocabularies=False)
        with mock.patch('geospaas.vocabularies.models.Parameter.objects.count', return_value=0):
            with self.assertRaises(RuntimeError):
                cli.refresh_vocabularies(config)

    def test_refresh_vocabularies_and_update_pythesint(self):
        """The database vocabulary objects must be updated and
        pythesint's data refreshed only if the corresponding settings
        are both True
        """
        # Refresh the database but not the pythesint files
        config = mock.Mock(update_vocabularies=True,
                           update_pythesint=False,
                           pythesint_versions=None)
        with mock.patch('django.core.management.call_command') as mock_call_command:
            with self.assertLogs(logger=cli.logger, level=logging.INFO):
                cli.refresh_vocabularies(config)
            mock_call_command.assert_called_with('update_vocabularies', force=False, versions=None)

        # Refresh both the database and the pythesint files
        config = mock.Mock(update_vocabularies=True,
                           update_pythesint=True,
                           pythesint_versions=None)
        with mock.patch('django.core.management.call_command') as mock_call_command:
            with self.assertLogs(logger=cli.logger, level=logging.INFO):
                cli.refresh_vocabularies(config)
            mock_call_command.assert_called_with('update_vocabularies', force=True, versions=None)

        # Refresh both the database and the pythesint files,
        # specifying versions
        config = mock.Mock(update_vocabularies=True,
                           update_pythesint=True,
                           pythesint_versions={'gcmd_instrument': '9.1.5'})
        with mock.patch('django.core.management.call_command') as mock_call_command:
            with self.assertLogs(logger=cli.logger, level=logging.INFO):
                cli.refresh_vocabularies(config)
            mock_call_command.assert_called_with(
                'update_vocabularies',
                force=True,
                versions={'gcmd_instrument': '9.1.5'})

    def test_save_results(self):
        """Test the the save() method of SearchResults objects is
        called in separate processes
        """
        searches_results = [mock.Mock(), mock.Mock()]
        error_future = mock.Mock()
        error_future.exception.return_value = RuntimeError
        futures = [mock.Mock(), error_future]

        with mock.patch('concurrent.futures.ProcessPoolExecutor.submit',
                        side_effect=futures) as mock_submit, \
             mock.patch('concurrent.futures.as_completed', side_effect=iter) as mock_as_completed, \
             self.assertLogs(cli.logger, level=logging.ERROR):
            cli.save_results(searches_results)

        mock_submit.assert_has_calls([
            mock.call(search_results.save) for search_results in searches_results
        ])
        mock_as_completed.assert_called_with(futures)

    def test_save_results_keyboard_interrupt(self):
        """Test KeyboardIbnterrupt handling"""
        with mock.patch('concurrent.futures.ProcessPoolExecutor') as mock_executor_builder:
            mock_executor = mock_executor_builder.return_value.__enter__.return_value
            mock_executor.submit.side_effect = KeyboardInterrupt
            with self.assertRaises(KeyboardInterrupt):
                cli.save_results([mock.Mock()])
        mock_executor.shutdown.assert_called()

    def test_print_providers(self):
        """Test printing providers help texts"""
        buffer = io.StringIO()
        with mock.patch('sys.stdout', buffer), \
             mock.patch('geospaas_harvesting.cli.ProvidersConfiguration') as mock_config:
            mock_config.from_file.return_value.providers = {'foo': 'bar'}
            cli.print_providers(argparse.Namespace(config_path=''))
        self.assertEqual(buffer.getvalue(), 'Available providers:\nbar\n')

    def test_harvest(self):
        """Check that the necessary functions are called"""
        with mock.patch('geospaas_harvesting.cli.ProvidersConfiguration'), \
             mock.patch('geospaas_harvesting.cli.SearchConfiguration'), \
             mock.patch('geospaas_harvesting.cli.refresh_vocabularies') as mock_refresh_vocs, \
             mock.patch('geospaas_harvesting.cli.save_results') as mock_save_results, \
             mock.patch('geospaas_harvesting.cli.retry_ingest') as mock_retry_ingest:
            cli.harvest(mock.Mock())

            mock_refresh_vocs.assert_called()
            mock_save_results.assert_called()
            mock_retry_ingest.assert_called()

    def test_make_arg_parser(self):
        """Test making the CLI arguments parser"""
        arg_parser = cli.make_arg_parser()
        parsed_args = arg_parser.parse_args((
            '-c', '/foo/config.yml',
            'harvest',
            '-s', '/foo/search.yml'))
        self.assertEqual(
            parsed_args,
            argparse.Namespace(
                config_path='/foo/config.yml',
                search_path='/foo/search.yml',
                func=cli.harvest))

    def test_main(self):
        """Test the right function is called in accord with the CLI
        arguments
        """
        with mock.patch('sys.argv', ['test_cli.py', 'harvest']), \
             mock.patch('geospaas_harvesting.cli.harvest') as mock_harvest:
            cli.main()
            mock_harvest.assert_called()
