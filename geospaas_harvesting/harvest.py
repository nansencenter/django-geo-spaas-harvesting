"""Daemon script for GeoSPaaS data harvesting"""

import argparse
import collections.abc
import logging
import multiprocessing
import os
import os.path
import pickle
import signal
import sys
import time
from datetime import datetime
import django
import yaml
# Load Django settings to be able to interact with the database
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'geospaas_harvesting.settings')
django.setup()
import geospaas_harvesting.harvesters as harvesters # pylint: disable=wrong-import-position

from geospaas.vocabularies.management.commands import update_vocabularies


LOGGER_NAME = 'geospaas_harvesting.daemon'
LOGGER = logging.getLogger(LOGGER_NAME)
LOGGER.addHandler(logging.NullHandler())

PERSISTENCE_DIR = os.getenv('GEOSPAAS_PERSISTENCE_DIR', os.path.join('/', 'var', 'run', 'geospaas'))


class Configuration(collections.abc.Mapping):
    """Manages harvesting configuration"""

    DEFAULT_CONFIGURATION_PATH = os.path.join(os.path.dirname(__file__), 'harvest.yml')
    TOP_LEVEL_KEYS = set(['harvesters', 'poll_interval', 'endless', 'dump_on_interruption'])
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


def dump_with_timestamp(obj, suffix=None):
    """Dump an object in a file named with a timestamp and the object's class name"""
    LOGGER.info("Dumping current state of %s", str(obj))
    dump(obj, os.path.join(PERSISTENCE_DIR, (
        datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S-%f') + '_' +
        suffix or obj.__class__.__name__
    )))


def load(path):
    """Convenience function to deserialize objects"""
    try:
        with open(path, 'rb') as persistence_file_handler:
            return pickle.load(persistence_file_handler)
    except (FileNotFoundError, IsADirectoryError, TypeError):
        LOGGER.error("Could not load from %s", path, exc_info=True)


def get_persistence_files():
    """Returns a list containing the names of all persistence files"""
    files = []
    try:
        files = sorted(os.listdir(PERSISTENCE_DIR), key=lambda s: s.split('_')[0], reverse=True)
    except FileNotFoundError:
        pass
    return files


def get_harvester_file_name(harvester_name):
    """Returns the name of the most recent file matching the harvester's name"""
    harvester_file = None
    for file_name in get_persistence_files():
        if file_name.endswith(harvester_name):
            harvester_file = file_name
            break
    return harvester_file


def load_last_dumped_harvester(harvester_name):
    """
    Returns a harvester unpickled from a persistence file, based on `harvester_name`.
    """
    harvester = None
    harvester_file_name = get_harvester_file_name(harvester_name)
    if harvester_file_name:
        harvester_file_path = os.path.join(PERSISTENCE_DIR, harvester_file_name)
        LOGGER.info("Loading harvester from '%s'", harvester_file_path)
        harvester = load(harvester_file_path)
        os.remove(harvester_file_path)
    return harvester


def create_harvester(harvester_config):
    """Instantiate a harvester"""
    harvester_class = getattr(harvesters, harvester_config['class'])
    return (harvester_class(**{
        key: value
        for (key, value) in harvester_config.items() if key != 'class'
    }))


def load_or_create_harvester(harvester_name, harvester_config, load_dumped=True):
    """
    Try to load the last dumped harvester. If no havester was found, instantiate one using the
    configuration
    """
    if load_dumped:
        LOGGER.info('Trying to load last dumped harvester')
        harvester = load_last_dumped_harvester(harvester_name)
    else:
        harvester = None

    if not harvester:
        LOGGER.info('Instantiating a new harvester')
        harvester = create_harvester(harvester_config)

    return harvester


def launch_harvest(harvester_name, harvester_config, dump_on_interruption=True):
    """Launch the harvest operation and process errors. Meant to be run in a separate process"""
    try:
        harvester = load_or_create_harvester(harvester_name, harvester_config, dump_on_interruption)
        harvester.harvest()
    except KeyboardInterrupt:
        LOGGER.error("The process was killed", exc_info=True)
        if dump_on_interruption:
            dump_with_timestamp(harvester, f"{harvester_name}")
        sys.exit(1)
    except Exception:  # pylint: disable=broad-except
        LOGGER.error("An unexpected error occurred", exc_info=True)
        if dump_on_interruption:
            dump_with_timestamp(harvester, f"{harvester_name}")
        raise
    LOGGER.info("%s finished harvesting", harvester_name)


def init_worker():
    """Initialization function for child processes. Defines signals handling."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    # signal.signal(signal.SIGTERM, raise_keyboard_interrupt)


def main():
    """Loads harvesting configuration and runs each harvester in its own process"""
    signal.signal(signal.SIGTERM, raise_keyboard_interrupt)

    try:
        config = Configuration()
    except AssertionError:
        LOGGER.error('Invalid configuration', exc_info=True)
        sys.exit(1)
    LOGGER.info('Updating vocabularies...')

    #update_vocabularies.Command().handle() #updating the vocabulary with this command

    LOGGER.info('Finished updating vocabularies')
    processes_number = len(config['harvesters'])
    try:
        with multiprocessing.Pool(processes_number, initializer=init_worker) as pool:
            results = {}
            # While all the processes do not end in error, launch the harvesters and check
            # regularly if they are finished.
            while not (
                    results and all([r.ready() and not r.successful() for r in results.values()])):
                # Loop over the harvesters defined in the configuration file
                for harvester_name, harvester_config in config['harvesters'].items():
                    #If the harvester has not been launched yet or has finished executing, launch it
                    # (again). Otherwise it is executing, so do nothing
                    previous_result = results.get(harvester_name, None)
                    if (previous_result and previous_result.ready() and previous_result.successful()
                            or not previous_result):
                        #Start a new process
                        results[harvester_name] = pool.apply_async(
                            launch_harvest, (
                                harvester_name,
                                harvester_config,
                                config.get('dump_on_interruption', True)
                            )
                        )
                if not config.get('endless', False):
                    break
                time.sleep(config.get('poll_interval', 600))
            LOGGER.error("All harvester processes encountered errors")
            pool.close()
            pool.join()
    except KeyboardInterrupt:
        pool.terminate()
        pool.join()
        sys.exit(1)


if __name__ == '__main__':
    main()
