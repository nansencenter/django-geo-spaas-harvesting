# pylint: disable=protected-access
"""Tests for the config module"""
import logging
import unittest
import unittest.mock as mock
from datetime import datetime, timezone as tz
from pathlib import Path

import geospaas_harvesting.config as config
import geospaas_harvesting.providers.base as providers_base
import geospaas_harvesting.providers.podaac as providers_podaac
import geospaas_harvesting.providers.cmems as providers_cmems
import geospaas_harvesting.providers.resto as providers_resto

from . import TEST_FILES_PATH


class ConfigurationTestCase(unittest.TestCase):
    """Tests for the Configuration class"""

    class TestConfiguration(config.Configuration):
        """Concrete configuration class used for testing"""
        config_arguments_parser = mock.Mock(parse=lambda d: d)

    def test_parse_config(self):
        """Test parsing configuration"""
        configuration = self.TestConfiguration()
        configuration._parse_config({'foo': 'bar', 'baz': 'qux'})
        self.assertEqual(configuration.foo, 'bar')
        self.assertEqual(configuration.baz, 'qux')

    def test_from_dict(self):
        """Test creating a configuration object from a dictionary"""
        configuration = self.TestConfiguration.from_dict({'foo': 'bar'})
        self.assertEqual(configuration.foo, 'bar')

    def test_from_file(self):
        """Test creating a configuration object from a YAML file"""
        configuration = self.TestConfiguration.from_file(TEST_FILES_PATH / 'sample.yml')
        self.assertEqual(configuration.foo, 'bar')


class ProvidersArgumentTestCase(unittest.TestCase):
    """Tests for the ProvidersArgument class"""

    def test_parse(self):
        """Test parsing a providers argument"""
        providers_arg = {
            'podaac': {'type': 'podaac'},
            'cmems': {'type': 'cmems', 'username': 'user', 'password': 'pass'}
        }
        parsed_providers = config.ProvidersArgument('providers').parse(providers_arg)
        self.assertDictEqual(
            parsed_providers,
            {
                'podaac': providers_podaac.PODAACProvider(name='podaac'),
                'cmems': providers_cmems.CMEMSProvider(
                    name='cmems', username='user', password='pass'),
            })

    def test_parse_error(self):
        """Test error handling when parsing wrong configuration"""
        with self.assertLogs(config.logger, level=logging.ERROR):
            _ = config.ProvidersArgument('providers').parse({'foo': {}})


class ProvidersConfigurationTestCase(unittest.TestCase):
    """Tests for the ProvidersConfiguration class"""

    def test_parse_providers_config(self):
        """Test parsing a providers configuration file"""
        with mock.patch('geospaas_harvesting.utils.http_request'):
            providers_config = config.ProvidersConfiguration.from_file(
                TEST_FILES_PATH / 'providers_config.yml')
            self.assertFalse(providers_config.update_vocabularies)
            self.assertFalse(providers_config.update_vocabularies)
            self.assertDictEqual(providers_config.pythesint_versions, {'gcmd_instrument': '9.1.5'})
            self.assertDictEqual(
                providers_config.providers,
                {'creodias': providers_resto.RestoProvider(name='creodias',
                                                           url='https://datahub.creodias.eu',)})


class SearchConfigurationTestCase(unittest.TestCase):
    """Tests for the SearchConfiguration class"""

    def setUp(self):
        self.mock_http_request = mock.patch('geospaas_harvesting.utils.http_request').start()
        self.addCleanup(mock.patch.stopall)
        self.providers_config = config.ProvidersConfiguration.from_file(
            TEST_FILES_PATH / 'providers_config.yml')
        self.search_config = config.SearchConfiguration \
            .from_file(TEST_FILES_PATH / 'search_config.yml') \
            .with_providers(self.providers_config.providers)
        # self.mock_request = mock.patch('requests.request').start()

    def test_create_search_configuration(self):
        """Test making a SearchConfiguration object from files"""

        self.assertDictEqual(self.search_config.common, {})
        self.assertListEqual(self.search_config.searches, [{
            'provider_name': 'creodias',
            'collection': 'SENTINEL-3',
            'processingLevel': '2',
            'start_time': '2023-01-01',
            'end_time': '2023-01-02',
        }])
        self.assertEqual(self.search_config.providers, self.providers_config.providers)

    def test_with_provider_error(self):
        """An exception must be raised if the argument to
        with_providers() is not a ProvidersConfiguration object
        """
        with self.assertRaises(ValueError):
            config.SearchConfiguration().with_providers('foo')

    def test_create_provider_searches(self):
        """Test starting searches from a SearchConfiguration object
        """
        with mock.patch('geospaas_harvesting.providers.resto.RestoProvider.search') as mock_search:
            self.assertListEqual(
                self.search_config.create_provider_searches(),
                [mock_search.return_value])
        mock_search.assert_called_once_with(
            collection='SENTINEL-3',
            processingLevel='2',
            start_time='2023-01-01',
            end_time='2023-01-02')
