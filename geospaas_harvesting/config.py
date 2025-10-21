"""Configuration management"""
import importlib
import logging
import pkgutil

import geospaas_harvesting
from .arguments import ArgumentParser, BooleanArgument, DictArgument, ListArgument
from .providers import Provider
from .utils import read_yaml_file


logger = logging.getLogger(__name__)


class NoProviderFoundError(Exception):
    """No provider class was found"""


class Configuration():
    """Base class for configuration objects"""

    def _parse_config(self, config_dict):
        """Parse a config dictionary and set the keys as properties of
        the current Configuration object
        """
        for name, value in self.config_arguments_parser.parse(config_dict).items():
            setattr(self, name, value)

    @classmethod
    def from_dict(cls, config_dict):
        """Creates a configuration object from a dictionary"""
        config = cls()
        config._parse_config(config_dict)
        return config

    @classmethod
    def from_file(cls, config_path):
        """Creates a configuration object from a YAML file"""
        return cls.from_dict(read_yaml_file(config_path))


class ProvidersArgument(DictArgument):
    """This argument is a dict of providers in the format:
    {
        'provider_name1':
            'type': 'type1'
        'provider_name2:
            'type': 'type2'
            'username': 'user1'
            'password': 'pass123'
    }
    """

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)

    def parse(self, value):
        """Go through the list of provider settings and create the
        providers
        """
        _providers = []
        providers_dict = super().parse(value)
        for provider_name, provider_settings in providers_dict.items():
            _providers.append(Provider(
                name=provider_name,
                crawler_name=provider_settings['crawler']['name'],
                normalizer_name=provider_settings.get('normalizer', {}).get('name', 'raw'),
                config={
                    'crawler': provider_settings['crawler'].get('defaults', {}),
                    'normalizer': provider_settings.get('normalizer', {}).get('config', {}),
                    'ingester': provider_settings.get('ingester', {}).get('config', {}),
                }
            ))
        return _providers

class GeneralConfiguration(Configuration):
    """Configuration manager for general harvesting settings"""
    def __init__(self):
        self.config_arguments_parser = ArgumentParser([
            BooleanArgument('update_vocabularies', default=True),
            BooleanArgument('update_pythesint', default=True),
            DictArgument('pythesint_versions', default=None),
            ProvidersArgument('default_providers', required=True)
        ])


class ProvidersConfiguration(Configuration):
    """Configuration manager for providers"""
    def __init__(self):
        self.config_arguments_parser = ArgumentParser([
            ProvidersArgument('providers', required=True)
        ])


class SearchConfiguration(Configuration):
    """Configuration manager used to parse search parameters"""
    def __init__(self):
        self.config_arguments_parser = ArgumentParser([
            DictArgument('common'),
            ListArgument('searches')
        ])

    def create_provider_searches(self):
        """Creates a SearchResults object for each of the provider
        specific searches
        """
        searches = []
        for search in self.searches:  # pylint: disable=no-member
            provider_name = search.pop('provider_name')
            search_terms = self.common.copy()  # pylint: disable=no-member
            search_terms.update(search)
            searches.append(Provider.objects.get(name=provider_name).search(**search_terms))
        return searches
