"""Configuration management"""
import logging

import geospaas_harvesting.providers.aviso as providers_aviso
import geospaas_harvesting.providers.base as providers_base
import geospaas_harvesting.providers.ceda as providers_ceda
import geospaas_harvesting.providers.cmems as providers_cmems
import geospaas_harvesting.providers.copernicus_scihub as providers_copernicus_scihub
import geospaas_harvesting.providers.earthdata_cmr as providers_earthdata_cmr
import geospaas_harvesting.providers.erddap as providers_erddap
import geospaas_harvesting.providers.ftp as providers_ftp
import geospaas_harvesting.providers.http as providers_http
import geospaas_harvesting.providers.jaxa as providers_jaxa
import geospaas_harvesting.providers.local as providers_local
import geospaas_harvesting.providers.metno as providers_metno
import geospaas_harvesting.providers.noaa as providers_noaa
import geospaas_harvesting.providers.podaac as providers_podaac
import geospaas_harvesting.providers.resto as providers_resto
from .arguments import ArgumentParser, BooleanArgument, DictArgument, ListArgument
from .utils import read_yaml_file


logger = logging.getLogger(__name__)


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
    provider_types = {
        'aviso': providers_aviso.AVISOProvider,
        'ceda': providers_ceda.CEDAProvider,
        'cmems_ftp': providers_cmems.CMEMSFTPProvider,
        'copernicus_scihub': providers_copernicus_scihub.CopernicusScihubProvider,
        'earthdata_cmr': providers_earthdata_cmr.EarthDataCMRProvider,
        'ftp': providers_ftp.FTPProvider,
        'gportal_ftp': providers_jaxa.GPortalProvider,
        'http': providers_http.HTTPProvider,
        'metno': providers_metno.METNOProvider,
        'nansat': providers_local.NansatProvider,
        'netcdf': providers_local.NetCDFProvider,
        'noaa': providers_noaa.NOAAProvider,
        'podaac': providers_podaac.PODAACProvider,
        'resto': providers_resto.RestoProvider,
        'tabledap': providers_erddap.ERDDAPTableProvider,
    }

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
        common_argument_parser = providers_base.Provider().search_parameters_parser
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
