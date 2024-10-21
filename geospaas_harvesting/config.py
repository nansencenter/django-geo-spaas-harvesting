"""Configuration management"""
import importlib
import logging
import pkgutil

import geospaas_harvesting
from .arguments import ArgumentParser, BooleanArgument, DictArgument, ListArgument
from .providers.base import Provider
from .utils import read_yaml_file


def import_provider_modules():
    """Import provider classes from core modules and plugins"""
    imported = []
    for base_module in [geospaas_harvesting, *geospaas_harvesting.discovered_plugins.values()]:
        for _, name, ispkg in pkgutil.iter_modules(base_module.__path__):
            if name == 'providers':
                providers = importlib.import_module(f"{base_module.__name__}.{name}")
                imported.append(providers)
                if ispkg:
                    for _, provider_name, _ in pkgutil.iter_modules(providers.__path__):
                        imported.append(
                            importlib.import_module(f"{providers.__name__}.{provider_name}"))
    return imported


import_provider_modules()
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
    provider_classes = Provider.__subclasses__()

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)

    def _find_provider(self, provider_type):
        """Try to find a provider matching the `provider_type` in the
        Provider subclasses
        """
        for provider_class in self.provider_classes:
            if provider_class.type == provider_type:
                return provider_class
        raise NoProviderFoundError(f"No provider found of type {provider_type}")

    def parse(self, value):
        """Go through the list of provider settings and create the
        providers
        """
        _providers = {}
        providers_dict = super().parse(value)
        for provider_name, provider_settings in providers_dict.items():
            try:
                _providers[provider_name] = (
                    self._find_provider(provider_settings['type'])(
                        name=provider_name,
                        **provider_settings,
                    ))
            except KeyError as error:
                logger.error('Missing setting for provider: %s', error.args[0])
            except NoProviderFoundError as error:
                logger.error(error.args[0])
        return _providers

class ProvidersConfiguration(Configuration):
    """Configuration manager for providers"""

    def __init__(self):
        self.config_arguments_parser = ArgumentParser([
            BooleanArgument('update_vocabularies', default=True),
            BooleanArgument('update_pythesint', default=True),
            DictArgument('pythesint_versions', default=None),
            ProvidersArgument('providers', required=True)
        ])


class SearchConfiguration(Configuration):
    """Configuration manager used to parse search parameters"""

    def __init__(self):
        self.providers = None
        common_argument_parser = Provider().search_parameters_parser
        self.config_arguments_parser = ArgumentParser([
            DictArgument(
                'common', argument_parser=common_argument_parser),
            ListArgument('searches')
        ])

    def with_providers(self, providers):
        """Adds a dict of providers to the current object.
        Needs to be called before the create_provider_searches() method
        """
        if isinstance(providers, dict):
            self.providers = providers
        else:
            raise ValueError("Need a dictionary")
        return self

    def create_provider_searches(self):
        """Creates a SearchResults object for each of the provider
        specific searches
        """
        searches = []
        for provider_search in self.searches:  # pylint: disable=no-member
            provider_name = provider_search.pop('provider_name')
            search_terms = self.common.copy()  # pylint: disable=no-member
            search_terms.update(provider_search)
            searches.append(self.providers[provider_name].search(**search_terms))
        return searches
