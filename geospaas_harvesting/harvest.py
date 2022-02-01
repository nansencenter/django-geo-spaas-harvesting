"""Daemon script for GeoSPaaS data harvesting"""

import geospaas_harvesting.recovery as recovery
import argparse
import collections.abc
import logging
import multiprocessing
import os
import os.path
import signal
import sys

import django
import django.db
import django.core.management
import yaml
# Load Django settings to be able to interact with the database
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'geospaas_harvesting.settings')
django.setup()
import geospaas_harvesting.harvesters as harvesters  # pylint: disable=wrong-import-position


LOGGER_NAME = 'geospaas_harvesting.daemon'
LOGGER = logging.getLogger(LOGGER_NAME)
LOGGER.addHandler(logging.NullHandler())


class Configuration(collections.abc.Mapping):
    """Manages harvesting configuration"""

    DEFAULT_CONFIGURATION_PATH = os.path.join(os.path.dirname(__file__), 'harvest.yml')
    TOP_LEVEL_KEYS = set([
        'harvesters',
        'update_vocabularies',
        'update_pythesint',
        'pythesint_versions'
    ])
    HARVESTER_CLASS_KEY = 'class'

    def __init__(self, config_path=None):
        self._cli_args = self._get_cli_arguments()
        self._path = config_path or self._cli_args.config_path
        self._data = None
        self._load_configuration()
        self._validate()

    def __getitem__(self, key):
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def _validate(self):
        """Validates that the configuration data is correct"""
        assert self._data, 'Configuration data is empty'
        assert self.TOP_LEVEL_KEYS.issuperset(self._data.keys()), 'Invalid top-level keys'
        assert self._data['harvesters'], 'No harvesters are configured'
        for name, config in self._data['harvesters'].items():
            assert self.HARVESTER_CLASS_KEY in config.keys(), (
                "Harvester configuration must contain the following keys: " +
                ', '.join(self.HARVESTER_CLASS_KEY))
            assert isinstance(config['class'], str), (
                f"In '{name}' section: 'class' must be a string")

    class EnvTag(yaml.YAMLObject):
        """class for reading the tags of yml file for finding the value of environment variables"""
        yaml_tag = u'!ENV'

        @classmethod
        def from_yaml(cls, loader, node):
            return os.getenv(node.value)

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
        LOGGER.info("Loading configuration from '%s'", self._path)
        yaml.SafeLoader.add_constructor('!ENV', self.EnvTag.from_yaml)
        try:
            with open(self._path, 'rb') as config_stream:
                self._data = yaml.safe_load(config_stream)
        except FileNotFoundError as error:
            LOGGER.exception('Configuration file not found', exc_info=error)


def raise_keyboard_interrupt(*args):
    """Raises a KeyboardInterrupt exception, to be used for signals handling"""
    raise KeyboardInterrupt


def create_harvester(harvester_config):
    """Instantiate a harvester"""
    harvester_class = getattr(harvesters, harvester_config['class'])
    return (harvester_class(**{
        key: value
        for (key, value) in harvester_config.items() if key != 'class'
    }))


def launch_harvest(harvester_name, harvester_config):
    """Launch the harvest operation and process errors. Meant to be run in a separate process"""
    # Force the creation of a new database connection for each new process
    django.db.connection.close()

    try:
        harvester = create_harvester(harvester_config)
        harvester.harvest()
    except Exception:  # pylint: disable=broad-except
        LOGGER.error("An unexpected error occurred", exc_info=True)
        raise
    LOGGER.info("%s finished harvesting", harvester_name)


def init_worker():
    """Initialization function for child processes. Defines signals handling."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def refresh_vocabularies(config):
    """Update the Vocabulary objects in the database if the
    `update_vocabularies` settings is True.
    If the `update_pythesint` setting is also True,
    the local pythesint data is also updated.
    """
    if config.get('update_vocabularies', True):
        LOGGER.info('Updating vocabularies...')
        django.core.management.call_command(
            'update_vocabularies',
            force=config.get('update_pythesint', False),
            versions=config.get('pythesint_versions', None)
        )


def main():
    """Loads harvesting configuration and runs each harvester in its own process"""
    signal.signal(signal.SIGTERM, raise_keyboard_interrupt)

    try:
        config = Configuration()
    except AssertionError:
        LOGGER.error('Invalid configuration', exc_info=True)
        sys.exit(1)

    refresh_vocabularies(config)

    LOGGER.info('Finished updating vocabularies')
    processes_number = len(config['harvesters'])
    try:
        with multiprocessing.Pool(processes_number, initializer=init_worker) as pool:
            results = {}
            # Loop over the harvesters defined in the configuration file
            for harvester_name, harvester_config in config['harvesters'].items():
                # Start a new process
                results[harvester_name] = pool.apply_async(
                    launch_harvest, (
                        harvester_name,
                        harvester_config
                    )
                )
            pool.close()
            pool.join()
    except KeyboardInterrupt:
        pool.terminate()
        pool.join()
        sys.exit(1)

    # Retry to ingest datasets for which the ingestion failed
    recovery.retry_ingest()


if __name__ == '__main__':
    main()
