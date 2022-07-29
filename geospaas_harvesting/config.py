"""Configuration management"""
import logging

import geospaas_harvesting.providers as providers  # pylint: disable=wrong-import-position
from .arguments import ArgumentParser, BooleanArgument, DictArgument
from .utils import read_yaml_file


logger = logging.getLogger(__name__)


class Configuration():
    """General configuration"""

    def __init__(self, config_dict):
        config_arguments_parser = ArgumentParser([
            BooleanArgument('update_vocabularies', default=True),
            BooleanArgument('update_pythesint', default=True),
            DictArgument('pythesint_versions', default=None),
            ProvidersArgument('providers', required=True)
        ])
        for name, value in config_arguments_parser.parse(config_dict).items():
            setattr(self, name, value)

    @classmethod
    def from_file(cls, config_path):
        """Creates a configuration object from a YAML file"""
        return cls(read_yaml_file(config_path))


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
