"""Configuration management"""
import logging

import geospaas_harvesting.providers as providers  # pylint: disable=wrong-import-position
from .arguments import ArgumentParser, BooleanArgument, DictArgument, ListArgument
from .utils import read_yaml_file


logger = logging.getLogger(__name__)


class Configuration():
    """Base class for configuration objects"""

    def __init__(self):
        """Define the argument parser for the configuration object"""
        raise NotImplementedError()

    def _parse_config(self, config_dict):
        """Parse a config dictionary and set the keys as properties of
        the current Configuration object
        """
        for name, value in self.config_arguments_parser.parse(config_dict).items():
            setattr(self, name, value)

    @classmethod
    def from_file(cls, config_path):
        """Creates a configuration object from a YAML file"""
        config = cls()
        config._parse_config(read_yaml_file(config_path))
        return config


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
    provider_types = {
        'ceda': providers.ceda.CEDAProvider,
        'cmems_ftp': providers.cmems.CMEMSFTPProvider,
        'copernicus_scihub': providers.copernicus_scihub.CopernicusScihubProvider,
        'creodias': providers.creodias.CreodiasProvider,
        'earthdata_cmr': providers.earthdata_cmr.EarthDataCMRProvider,
        'ftp': providers.ftp.FTPProvider,
        'gportal_ftp': providers.jaxa.GPortalProvider,
        'netcdf': providers.local.NetCDFProvider,
        'nansat': providers.local.NansatProvider,
        'metno': providers.metno.METNOProvider,
        'noaa': providers.noaa.NOAAProvider,
        'podaac': providers.podaac.PODAACProvider,
    }
    valid_settings = {'type', 'username', 'password'}

    def parse(self, value):
        """Go through the list of provider settings and create the
        providers
        """
        _providers = {}
        providers_dict = super().parse(value)
        for provider_name, provider_settings in providers_dict.items():
            try:
                _providers[provider_name] = (
                    self.provider_types[provider_settings['type']](
                        name=provider_name,
                        **provider_settings,
                    ))
            except KeyError as error:
                logger.error('Missing setting for provider: %s', error.args[0])
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
        common_argument_parser = providers.base.Provider().search_parameters_parser
        self.config_arguments_parser = ArgumentParser([
            DictArgument(
                'common', argument_parser=common_argument_parser),
            ListArgument('provider_specific')
        ])

    def with_providers(self, providers):
        """Adds a dict of providers to the current object.
        Needs to be called before the start_searches() method
        """
        try:
            self.providers = providers
        except IndexError as error:
            raise ValueError(
                "Expecting a dict of providers as returned "
                "by a ProvidersConfiguration object") from error
        return self

    def start_searches(self):
        """Starts a search for each of the provider specific searches
        """
        searches = []
        for provider_search in self.provider_specific:  # pylint: disable=no-member
            provider_name = provider_search.pop('provider_name')
            search_terms = self.common.copy()  # pylint: disable=no-member
            search_terms.update(provider_search)
            searches.append(self.providers[provider_name].search(**search_terms))
        return searches
