"""Daemon script for GeoSPaaS data harvesting"""

import argparse
import logging
import os.path
import sys
import yaml

import django

# Load Django settings to be able to interact with the database
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'geospaas_harvesting.settings')
django.setup()
import geospaas_harvesting.harvesters as harvesters # pylint: disable=wrong-import-position

LOGGER_NAME = 'geospaas_harvesting.daemon'
LOGGER = logging.getLogger(LOGGER_NAME)
LOGGER.addHandler(logging.NullHandler())


class Configuration():
    """Manages harvesting configuration"""

    DEFAULT_CONFIGURATION_PATH = os.path.join(os.path.dirname(__file__), 'harvest.yml')
    TOP_LEVEL_KEYS = set(['harvesters'])
    HARVESTER_KEYS = set(['class', 'urls'])

    def __init__(self, config_path=None):
        self._cli_args = self._get_cli_arguments()
        self._path = config_path or self._cli_args.config_path
        self._data = None
        self._load_configuration()
        self._validate()

    def __getitem__(self, key):
        return self._data[key]

    def __len__(self):
        return len(self._data)

    def _validate(self):
        """Validates that the configuration data is correct"""
        assert self._data, 'Configuration data is empty'
        assert self.TOP_LEVEL_KEYS.issuperset(self._data.keys()), 'Invalid top-level keys'
        assert self._data['harvesters'], 'No harvesters are configured'
        for name, config in self._data['harvesters'].items():
            assert self.HARVESTER_KEYS == set(config.keys()), (
                "Harvester configuration must contain the following keys: " +
                ', '.join(self.HARVESTER_KEYS))

            assert isinstance(config['class'], str), (
                f"In '{name}' section: 'class' must be a string")

            assert isinstance(config['urls'], list), (f"In '{name}' section: 'urls' must be a list")

            for url in config['urls']:
                assert isinstance(url, str), (
                    f"In '{name}.urls' section: each URL must be a string")

    def _get_cli_arguments(self):
        """Parse CLI arguments"""
        # Parse the arguments only when this module was directly executed
        if sys.argv[0] == __file__:
            arg_parser = argparse.ArgumentParser(
                description='Harvests data for the GeoSPaaS catalog')
            arg_parser.add_argument('-c', '--config',
                                    dest='config_path',
                                    default=self.DEFAULT_CONFIGURATION_PATH,
                                    help='Path to the configuration file')
            arguments = arg_parser.parse_args()
        else:
            arguments = argparse.Namespace()
            setattr(arguments, 'config_path', self.DEFAULT_CONFIGURATION_PATH)

        return arguments

    def _load_configuration(self):
        """Loads the harvesting configuration from a file"""
        try:
            with open(self._path, 'rb') as config_stream:
                self._data = yaml.safe_load(config_stream)
        except FileNotFoundError as error:
            LOGGER.exception('Configuration file not found', exc_info=error)


def main():
    """Loads harvesting configuration and runs each harvester in turn"""
    #TODO: add a way to resume when stopped
    # Load the configuration
    try:
        config = Configuration()
    except AssertionError:
        LOGGER.error('Invalid configuration', exc_info=True)
        exit(1)

    # Build a list of harvester instances
    harvesters_list = harvesters.HarvesterList(config['harvesters'])

    # Infinite loop
    try:
        for harvester in harvesters_list:
            harvester.harvest()
    except ValueError:
        LOGGER.error("Could not iterate over harvesters list, please check the configuration file",
                     exc_info=True)


if __name__ == '__main__':
    main()
