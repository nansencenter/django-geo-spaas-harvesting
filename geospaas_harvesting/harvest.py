"""Daemon script for GeoSPaaS data harvesting"""

import argparse
import logging
import os
import os.path
import pickle
import signal
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

PERSISTENCE_DIR = os.getenv('GEOSPAAS_PERSISTENCE_DIR', os.path.join('/', 'var', 'run', 'geospaas'))
PERSISTENCE_FILE = os.path.join(PERSISTENCE_DIR, 'harvesters_state')


class Configuration():
    """Manages harvesting configuration"""

    DEFAULT_CONFIGURATION_PATH = os.path.join(os.path.dirname(__file__), 'harvest.yml')
    TOP_LEVEL_KEYS = set(['harvesters'])
    HARVESTER_CLASS_KEY = 'class'

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
            assert self.HARVESTER_CLASS_KEY in config.keys(), (
                "Harvester configuration must contain the following keys: " +
                ', '.join(self.HARVESTER_CLASS_KEY))

            assert isinstance(config['class'], str), (
                f"In '{name}' section: 'class' must be a string")

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
        try:
            with open(self._path, 'rb') as config_stream:
                self._data = yaml.safe_load(config_stream)
        except FileNotFoundError as error:
            LOGGER.exception('Configuration file not found', exc_info=error)


def raise_keyboard_interrupt(*args):
    """Raises a KeyboardInterrupt exception, to be used for signals handling"""
    raise KeyboardInterrupt

def dump(obj, path):
    """Convenience function to serialize objects"""
    try:
        with open(path, 'wb') as persistence_file_handler:
            pickle.dump(obj, persistence_file_handler)
    except (FileNotFoundError, IsADirectoryError):
        LOGGER.error("Could not dump %s to %s", str(obj), path, exc_info=True)
    except Exception: # pylint: disable=broad-except
        LOGGER.error("An unexpected error occurred while dumping %s to %s",
                     str(obj), path, exc_info=True)

def load(path):
    """Convenience function to deserialize objects"""
    try:
        with open(path, 'rb') as persistence_file_handler:
            return pickle.load(persistence_file_handler)
    except (FileNotFoundError, IsADirectoryError, TypeError):
        LOGGER.error("Could not load from %s", path, exc_info=True)

def main():
    """Loads harvesting configuration and runs each harvester in turn"""

    signal.signal(signal.SIGTERM, raise_keyboard_interrupt)

    # Deserialize the last known state if possible, otherwise initialize harvesters from
    # configuration
    if os.path.exists(PERSISTENCE_FILE):
        LOGGER.info("Loading saved state")
        (current_harvester, harvesters_iterator) = load(PERSISTENCE_FILE)
        os.remove(PERSISTENCE_FILE)
    else:
        try:
            config = Configuration()
        except AssertionError:
            LOGGER.error('Invalid configuration', exc_info=True)
            sys.exit(1)
        harvesters_iterator = iter(harvesters.HarvesterList(config['harvesters']))
        current_harvester = next(harvesters_iterator)

    # Infinite loop
    while True:
        try:
            current_harvester.harvest()
        except ValueError:
            LOGGER.error(
                "Could not iterate over harvesters list, please check the configuration file",
                exc_info=True)
            raise
        except KeyboardInterrupt:
            LOGGER.error("The process was killed", exc_info=True)
            LOGGER.info("Dumping current state")
            dump((current_harvester, harvesters_iterator), PERSISTENCE_FILE)
            sys.exit(1)
        except Exception:  # pylint: disable=broad-except
            LOGGER.error("An unexpected error occurred", exc_info=True)
            LOGGER.info("Dumping current state")
            dump((current_harvester, harvesters_iterator), PERSISTENCE_FILE)
            raise
        else:
            try:
                current_harvester = next(harvesters_iterator)
            except StopIteration:
                LOGGER.warning('The loop over the harvesters ended, it is not supposed to happen')
                break


if __name__ == '__main__':
    main()
