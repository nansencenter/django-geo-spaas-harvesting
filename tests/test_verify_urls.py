""" Test the verification code """
import argparse
import io
import logging
import os
import os.path
import textwrap
import unittest
import unittest.mock as mock

import requests.auth
import requests_oauthlib

import geospaas_harvesting.verify_urls as verify_urls


class VerifyURLsTestCase(unittest.TestCase):
    """Test the URLs verification module"""

    def test_main_check(self):
        """The correct actions should be launched depending on the CLI
        arguments
        """
        args = mock.Mock()
        args.action = 'check'
        with mock.patch('geospaas_harvesting.verify_urls.parse_cli_arguments', return_value=args), \
                mock.patch('geospaas_harvesting.verify_urls.read_config'), \
                mock.patch('geospaas_harvesting.verify_urls.check_providers') as mock_check, \
                mock.patch('geospaas_harvesting.verify_urls.delete_stale_urls') as mock_delete:

            with self.assertLogs(verify_urls.logger):
                verify_urls.main()
            mock_check.assert_called_once()
            mock_delete.assert_not_called()

    def test_main_delete(self):
        args = mock.Mock()
        args.action = 'delete-stale'
        with mock.patch('geospaas_harvesting.verify_urls.parse_cli_arguments', return_value=args), \
                mock.patch('geospaas_harvesting.verify_urls.read_config'), \
                mock.patch('geospaas_harvesting.verify_urls.check_providers') as mock_check, \
                mock.patch('geospaas_harvesting.verify_urls.delete_stale_urls') as mock_delete:

            with self.assertLogs(verify_urls.logger):
                verify_urls.main()
            mock_check.assert_not_called()
            mock_delete.assert_called_once()

    def test_parse_cli_arguments_check(self):
        """Test CLI arguments parsing for the check action"""
        with mock.patch('sys.argv',
                        ['verify_urls.py', '-p', '/foo.yml', 'check', '-o', '/bar']):
            self.assertEqual(
                verify_urls.parse_cli_arguments(),
                argparse.Namespace(
                    providers_conf='/foo.yml', action='check', output_directory='/bar'))

    def test_parse_cli_arguments_check_defaults(self):
        """Test CLI arguments parsing for the check action with default
        values
        """
        default_provider_conf = os.path.join(os.path.dirname(verify_urls.__file__), 'check.yml')
        with mock.patch('sys.argv', ['verify_urls.py', 'check']):
            self.assertEqual(
                verify_urls.parse_cli_arguments(),
                argparse.Namespace(
                    providers_conf=default_provider_conf, action='check', output_directory='.'))

    def test_parse_cli_arguments_delete(self):
        """Test CLI arguments parsing for the delete action"""
        with mock.patch('sys.argv',
                        ['verify_urls.py', '-p', '/foo.yml', 'delete-stale', '/bar/baz.txt']):
            self.assertEqual(
                verify_urls.parse_cli_arguments(),
                argparse.Namespace(
                    providers_conf='/foo.yml',
                    action='delete-stale',
                    urls_file='/bar/baz.txt',
                    force=False))

    def test_parse_cli_arguments_delete_force(self):
        """Test CLI arguments parsing for the delete action with the force option"""
        with mock.patch('sys.argv',
                        ['verify_urls.py', '-p', '/foo.yml', 'delete-stale', '/bar/baz.txt', '-f']):
            self.assertEqual(
                verify_urls.parse_cli_arguments(),
                argparse.Namespace(
                    providers_conf='/foo.yml',
                    action='delete-stale',
                    urls_file='/bar/baz.txt',
                    force=True))

    def test_parse_cli_arguments_no_action(self):
        """An error should be raised if no action is specified in the
        CLI arguments
        """
        buffer = io.StringIO()
        with mock.patch('sys.argv', ['verify_urls.py']), \
                mock.patch('sys.stderr', buffer), \
                self.assertRaises(SystemExit):
            verify_urls.parse_cli_arguments()
            self.assertIn(
                'the following arguments are required: action',
                buffer.getvalue())

    def test_parse_cli_arguments_check_wrong_arg(self):
        """An error should be raised if the wrong argument is provided
        to the check action
        """
        buffer = io.StringIO()
        with mock.patch('sys.argv', ['verify_urls.py', 'check', '-f']), \
                mock.patch('sys.stderr', buffer), \
                self.assertRaises(SystemExit):
            verify_urls.parse_cli_arguments()
            self.assertIn('unrecognized arguments: -f', buffer.getvalue())

    def test_parse_cli_arguments_delete_wrong_arg(self):
        """An error should be raised if the wrong argument is provided
        to the delete action
        """
        buffer = io.StringIO()
        with mock.patch('sys.argv', ['verify_urls.py', 'delete-stale', '-o', '/bar']), \
                mock.patch('sys.stderr', buffer), \
                self.assertRaises(SystemExit):
            verify_urls.parse_cli_arguments()
            self.assertIn('unrecognized arguments: -o', buffer.getvalue())

    def test_delete_stale_urls(self):
        """404 URLs should be deleted unless the force option is used
        """
        provider = {'url': 'https://foo', 'auth': ('username', 'password')}
        file_contents = '404 12 https://foo/bar\n500 13 https://foo/baz'
        check_url_results = (
            (False, 404, 12, 'https://foo/bar'),
            (False, 500, 13, 'https://foo/baz')
        )

        dataset_uris = {12: 'https://foo/bar', 13: 'https://foo/baz'}
        mock_manager = mock.Mock()
        mock_manager.get.side_effect = lambda id: mock.Mock(uri=dataset_uris[id])

        with mock.patch('geospaas_harvesting.verify_urls.find_provider', return_value=provider), \
             mock.patch('geospaas_harvesting.verify_urls.DatasetURI.objects', mock_manager):

            buffer = io.StringIO(file_contents)
            with mock.patch('geospaas_harvesting.verify_urls.open', return_value=buffer), \
                    mock.patch('geospaas_harvesting.verify_urls.check_url',
                                side_effect=check_url_results), \
                    mock.patch('geospaas_harvesting.verify_urls.remove_dataset_uri',
                                return_value=True) as mock_remove:
                # force == False, only the URL that returns 404 must be
                # deleted
                self.assertEqual(verify_urls.delete_stale_urls('', {}, force=False), (1, 1))
                self.assertListEqual(
                    [args[0][0].uri for args in mock_remove.call_args_list],
                    ['https://foo/bar'])

            buffer = io.StringIO(file_contents)
            with mock.patch('geospaas_harvesting.verify_urls.open', return_value=buffer), \
                    mock.patch('geospaas_harvesting.verify_urls.check_url',
                                side_effect=check_url_results), \
                    mock.patch('geospaas_harvesting.verify_urls.remove_dataset_uri',
                            return_value=True) as mock_remove:
                # force == True, both URLs must be deleted
                self.assertEqual(verify_urls.delete_stale_urls('', {}, force=True), (2, 2))
                self.assertListEqual(
                    [args[0][0].uri for args in mock_remove.call_args_list],
                    ['https://foo/bar', 'https://foo/baz'])

    def test_remove_dataset_uri(self):
        """The URI should be removed, as well as the corresponding
        dataset if it does not have anymore URIs
        """
        dataset_uri = mock.Mock()

        # simulate empty queryset
        dataset_uri.dataset.dataseturi_set.all.return_value = []
        self.assertTrue(verify_urls.remove_dataset_uri(dataset_uri))
        dataset_uri.delete.assert_called_once_with()
        dataset_uri.dataset.delete.assert_called_once_with()

        dataset_uri.reset_mock()

        # simulate queryset with one element
        dataset_uri.dataset.dataseturi_set.all.return_value = [mock.Mock()]
        self.assertFalse(verify_urls.remove_dataset_uri(dataset_uri))
        dataset_uri.delete.assert_called_once_with()
        dataset_uri.dataset.delete.assert_not_called()

    def test_find_provider(self):
        """Should return the right provider given a URL"""
        scihub_attributes = {
            'url': 'https://scihub.copernicus.eu/',
            'auth': ('scihub_user', 'scihub_pass'),
            'throttle': 0
        }
        podaac_attributes = {
            'url': 'https://opendap.jpl.nasa.gov/opendap/',
            'auth': ('podaac_user', 'podaac_pass'),
            'throttle': 0
        }

        providers = {
            'scihub': scihub_attributes,
            'podaac': podaac_attributes
        }
        self.assertIsNone(verify_urls.find_provider('https://foo', providers))
        self.assertDictEqual(
            verify_urls.find_provider('https://scihub.copernicus.eu/foo', providers),
            scihub_attributes)
        self.assertDictEqual(
            verify_urls.find_provider('https://opendap.jpl.nasa.gov/opendap/foo', providers),
            podaac_attributes)

    def test_check_providers(self):
        """Should run URL checks for each provider in a separate
        process. If an exception is raised in one of the sub-processes,
        check_providers() should return False and the traceback of the
        exception should be logged
        """
        scihub_attributes = {
            'url': 'https://scihub.copernicus.eu/',
            'auth': ('scihub_user', 'scihub_pass'),
            'throttle': 0
        }
        podaac_attributes = {
            'url': 'https://opendap.jpl.nasa.gov/opendap/',
            'auth': ('podaac_user', 'podaac_pass'),
            'throttle': 0
        }

        providers = {
            'scihub': scihub_attributes,
            'podaac': podaac_attributes
        }

        with mock.patch('concurrent.futures.ProcessPoolExecutor') as mock_pool, \
                mock.patch('geospaas_harvesting.verify_urls.datetime') as mock_datetime, \
                mock.patch('geospaas_harvesting.verify_urls.check_provider_urls') as mock_check, \
                mock.patch('concurrent.futures.as_completed', iter):
            mock_executor = mock_pool.return_value.__enter__.return_value
            mock_datetime.now.return_value.strftime.return_value = 'time'
            self.assertTrue(verify_urls.check_providers('foo', providers))
            mock_executor.submit.assert_has_calls((
                mock.call(
                    mock_check,
                    os.path.join('foo', 'scihub_stale_urls_time.txt'),
                    scihub_attributes['url'],
                    scihub_attributes['auth'],
                    scihub_attributes['throttle']),
                mock.call(
                    mock_check,
                    os.path.join('foo', 'podaac_stale_urls_time.txt'),
                    podaac_attributes['url'],
                    podaac_attributes['auth'],
                    podaac_attributes['throttle'])
            ), any_order=True)
            self.assertEqual(len(mock_executor.submit.call_args_list), 2)

            mock_executor.submit.return_value.result.side_effect = AttributeError
            with self.assertLogs(verify_urls.logger, level=logging.ERROR):
                self.assertFalse(verify_urls.check_providers('foo', providers))

    def test_check_provider_urls(self):
        """Should check all the URLs for one provider"""
        mock_lock = mock.Mock()
        with mock.patch('geospaas_harvesting.verify_urls.Lock', return_value=mock_lock), \
                mock.patch(
                    'geospaas_harvesting.verify_urls.BoundedThreadPoolExecutor') as mock_pool, \
                mock.patch('geospaas_harvesting.verify_urls.DatasetURI.objects') as mock_manager, \
                mock.patch('geospaas_harvesting.verify_urls.write_stale_url') as mock_write:
            mock_executor = mock_pool.return_value.__enter__.return_value
            mock_dataset_uri = mock.Mock()
            mock_auth = mock.Mock()
            mock_manager.filter.return_value.iterator.return_value = [mock_dataset_uri]

            # call without throttle: 50 workers
            with self.assertLogs(verify_urls.logger, level=logging.INFO):
                verify_urls.check_provider_urls('output.txt', 'https://foo/', mock_auth)

            mock_executor.submit.assert_called_once_with(
                mock_write, mock_lock, 'output.txt', mock_dataset_uri, mock_auth, throttle=0)
            mock_pool.assert_called_once_with(max_workers=50, queue_limit=2000)

            mock_pool.reset_mock()

            # call with throttle: 1 worker
            with self.assertLogs(verify_urls.logger, level=logging.INFO):
                verify_urls.check_provider_urls('output.txt', 'https://foo/', mock_auth, throttle=1)
            mock_executor.submit.assert_called_once_with(
                mock_write, mock_lock, 'output.txt', mock_dataset_uri, mock_auth, throttle=1)
            mock_pool.assert_called_once_with(max_workers=1, queue_limit=2000)

    def test_write_stale_url_valid(self):
        """Should not write anything to the output file if the URL is
        valid
        """
        mock_lock = mock.MagicMock()
        with mock.patch('geospaas_harvesting.verify_urls.check_url',
                        return_value=(True, 200, 1, 'https://foo')), \
                 mock.patch('geospaas_harvesting.verify_urls.open') as mock_open:
            verify_urls.write_stale_url(mock_lock, 'output.txt', mock.Mock(), mock.Mock())
            mock_open.assert_not_called()

    def test_write_stale_url_invalid(self):
        """Should write the URL info to the output file if the URL is
        invalid
        """
        with mock.patch('geospaas_harvesting.verify_urls.check_url',
                        return_value=(False, 404, 1, 'https://foo')), \
                mock.patch('geospaas_harvesting.verify_urls.open') as mock_open:
            mock_file = mock.MagicMock()
            mock_open.return_value.__enter__.return_value = mock_file
            verify_urls.write_stale_url(mock.MagicMock(), 'output.txt', mock.Mock(), mock.Mock())
            mock_file.write.assert_called_once_with(f"404 1 https://foo{os.linesep}")

    def test_check_url_200(self):
        """Should send a HEAD request to the URL and return whether the
        URL is valid or not.
        """
        mock_dataset_uri = mock.Mock(id=1, uri='https://foo')
        mock_response = mock.MagicMock(status_code=200, headers={})
        with mock.patch('geospaas_harvesting.utils.http_request',
                        return_value=mock_response) as mock_request:
            self.assertTupleEqual(
                verify_urls.check_url(mock_dataset_uri, mock.Mock()),
                (True, 200, 1, 'https://foo')
            )

    def test_check_url_404(self):
        """Should send a HEAD request to the URL and return whether the
        URL is valid or not.
        """
        mock_dataset_uri = mock.Mock(id=1, uri='https://foo')
        mock_response = mock.MagicMock(status_code=404, headers={})
        with mock.patch('geospaas_harvesting.utils.http_request',
                        return_value=mock_response) as mock_request:
            self.assertTupleEqual(
                verify_urls.check_url(mock_dataset_uri, mock.Mock()),
                (False, 404, 1, 'https://foo'))
            mock_request.assert_called_once()

    def test_check_url_429_no_header(self):
        """When an error 429 occurs, the URL should ne retried after a
        delay
        """
        mock_dataset_uri = mock.Mock(id=1, uri='https://foo')
        mock_responses = (
            mock.MagicMock(status_code=429, headers={}),
            mock.MagicMock(status_code=404, headers={})
        )
        with mock.patch('geospaas_harvesting.utils.http_request',
                        side_effect=mock_responses) as mock_request, \
                mock.patch('time.sleep') as mock_sleep:

            with self.assertLogs(verify_urls.logger, level=logging.WARNING):
                self.assertTupleEqual(
                    verify_urls.check_url(mock_dataset_uri, mock.Mock()),
                    (False, 404, 1, 'https://foo'))

            self.assertEqual(mock_request.call_count, 2)
            mock_sleep.assert_has_calls((mock.call(60), mock.call(0)))

    def test_check_url_429_retry_after_header(self):
        """When an error 429 occurs, the URL should ne retried after a
        delay
        """
        mock_dataset_uri = mock.Mock(id=1, uri='https://foo')
        mock_responses = (
            mock.MagicMock(status_code=429, headers={'Retry-After': 2}),
            mock.MagicMock(status_code=200, headers={})
        )
        with mock.patch('geospaas_harvesting.utils.http_request',
                        side_effect=mock_responses) as mock_request, \
                mock.patch('time.sleep') as mock_sleep:

            with self.assertLogs(verify_urls.logger, level=logging.WARNING):
                self.assertTupleEqual(
                    verify_urls.check_url(mock_dataset_uri, mock.Mock(), throttle=1),
                    (True, 200, 1, 'https://foo'))

            self.assertEqual(mock_request.call_count, 2)
            mock_sleep.assert_has_calls((mock.call(2), mock.call(1)))

    def test_check_url_429_too_many_retries(self):
        """When there are too many retries, an exception should be
        raised
        """
        mock_dataset_uri = mock.Mock(id=1, uri='https://foo')
        mock_responses = (
            mock.MagicMock(status_code=429, headers={}),
            mock.MagicMock(status_code=200, headers={})
        )
        with mock.patch('geospaas_harvesting.utils.http_request',
                        side_effect=mock_responses) as mock_request:

            with self.assertRaises(verify_urls.TooManyRequests):
                verify_urls.check_url(mock_dataset_uri, mock.Mock(), tries=1)
            mock_request.assert_called_once()

    def test_read_config(self):
        """Should read the provider configuration from a YAML file"""
        config = textwrap.dedent('''---
        podaac:
          url: 'https://opendap.jpl.nasa.gov/opendap/'
        scihub:
          url: 'https://scihub.copernicus.eu/'
          username: !ENV 'COPERNICUS_OPEN_HUB_USERNAME'
          password: !ENV 'COPERNICUS_OPEN_HUB_PASSWORD'
        creodias:
          url: 'https://zipper.creodias.eu/'
          username: !ENV 'CREODIAS_USERNAME'
          password: !ENV 'CREODIAS_PASSWORD'
          token_url: 'https://auth.creodias.eu/auth/realms/DIAS/protocol/openid-connect/token'
          client_id: 'CLOUDFERRO_PUBLIC'
          throttle: 1
          ''')
        environment = {
            'COPERNICUS_OPEN_HUB_USERNAME': 'copernicus_user',
            'COPERNICUS_OPEN_HUB_PASSWORD': 'copernicus_password',
            'CREODIAS_USERNAME': 'creodias_user',
            'CREODIAS_PASSWORD': 'creodias_password',
        }
        # we check that get_auth() is called with the right arguments
        # by replacing its output by its arguments
        with mock.patch('geospaas_harvesting.verify_urls.open', mock.mock_open(read_data=config)), \
                mock.patch('geospaas_harvesting.verify_urls.get_auth',
                           side_effect=lambda args: args), \
                mock.patch('os.environ', environment):
            providers = verify_urls.read_config('foo.yml')

        self.assertDictEqual(providers, {
            'podaac': {
                'url': 'https://opendap.jpl.nasa.gov/opendap/',
                'auth': {'url': 'https://opendap.jpl.nasa.gov/opendap/'},
                'throttle': 0
            },
            'scihub': {
                'url': 'https://scihub.copernicus.eu/',
                'auth': {
                    'url': 'https://scihub.copernicus.eu/',
                    'username': 'copernicus_user',
                    'password': 'copernicus_password'
                },
                'throttle': 0
            },
            'creodias': {
                'url': 'https://zipper.creodias.eu/',
                'auth': {
                    'url': 'https://zipper.creodias.eu/',
                    'username': 'creodias_user',
                    'password': 'creodias_password',
                    'token_url': 'https://auth.creodias.eu/auth/realms/DIAS/protocol/'
                                 'openid-connect/token',
                    'client_id': 'CLOUDFERRO_PUBLIC',
                    'throttle': 1
                },
                'throttle': 1
            },
        })

    def test_get_auth_oauth2(self):
        """Should return the right authentication object based on the
        provider attributes
        """
        # oauth2
        mock_oauth2 = mock.Mock()
        with mock.patch('geospaas_harvesting.verify_urls.build_oauth2',
                        return_value=mock_oauth2) as mock_build_oauth2:
            self.assertEqual(
                verify_urls.get_auth({
                    'username': 'user',
                    'password': 'pass',
                    'token_url': 'https://foo',
                    'client_id': 'CLIENT'
                }),
                mock_oauth2)
            mock_build_oauth2.assert_called_once_with('user', 'pass', 'https://foo', 'CLIENT')

    def test_get_auth_basic(self):
        """Should return the right authentication object based on the
        provider attributes
        """
        # basic HTTP auth
        self.assertEqual(
            verify_urls.get_auth({'username': 'user', 'password': 'pass'}),
            requests.auth.HTTPBasicAuth('user', 'pass'))

    def test_get_auth_no_auth(self):
        """Should return the right authentication object based on the
        provider attributes
        """
        # no authentication
        self.assertIsNone(verify_urls.get_auth({}))

    def test_build_oauth2(self):
        """Should return an OAuth2 object usable by `requests`"""
        with mock.patch('requests_oauthlib.OAuth2Session') as mock_oauth2_session:
            self.assertIsInstance(
                verify_urls.build_oauth2('user', 'pass', 'https://foo', 'CLIENT'),
                requests_oauthlib.OAuth2)
            mock_oauth2_session.return_value.fetch_token.assert_called_with(
                token_url='https://foo',
                username='user',
                password='pass',
                client_id='CLIENT'
            )

    def test_bounded_thread_pool_executor_init(self):
        """The executor should have a semaphore attribute with an
        initial value equal to the provided queue limit + the number of
        workers
        """
        pool_executor = verify_urls.BoundedThreadPoolExecutor(max_workers=1, queue_limit=1)
        self.assertIsInstance(pool_executor.semaphore, verify_urls.BoundedSemaphore)
        self.assertEqual(pool_executor.semaphore._initial_value, 2)

    def test_bounded_thread_pool_executor_submit(self):
        """This executor should stop adding jobs to its internal queue
        when it hits the limit
        """
        # check that the semaphore is acquired and released in a
        # normal case
        with verify_urls.BoundedThreadPoolExecutor(max_workers=1, queue_limit=1) as bounded_pool:
            bounded_pool.semaphore = mock.Mock()
            bounded_pool.submit(lambda x: x, 1,)
        bounded_pool.semaphore.acquire.assert_called()
        bounded_pool.semaphore.release.assert_called()

        # check that the semaphore is acquired and released when
        # submit() raises an exception
        with mock.patch('concurrent.futures.ThreadPoolExecutor.submit', side_effect=ValueError):
            with verify_urls.BoundedThreadPoolExecutor(
                    max_workers=1, queue_limit=1) as bounded_pool:
                bounded_pool.semaphore = mock.Mock()
                with self.assertRaises(ValueError):
                    bounded_pool.submit(lambda x: x, 1,)
        bounded_pool.semaphore.acquire.assert_called()
        bounded_pool.semaphore.release.assert_called()
