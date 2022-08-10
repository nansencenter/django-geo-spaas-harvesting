# pylint: disable=wrong-import-position
# pylint: disable=no-member
"""CLI for interacting with geospaas_harvesting"""
# TODO: manage URL checks and recovery
import argparse
import concurrent.futures
import logging
import os
import signal
from pathlib import Path

import django
# Load Django settings to be able to interact with the database
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'geospaas_harvesting.settings')
django.setup()

from geospaas.catalog.models import Parameter
from .config import ProvidersConfiguration, SearchConfiguration
from .recovery import retry_ingest
from .utils import read_yaml_file


logger = logging.getLogger(__name__)

package_dir = Path(__file__).parent
default_configuration_path = package_dir / 'config.yml'
# look in the folder from which the command is executed
default_search_path = Path() / 'search.yml'


def init_worker():
    """Initialization function for child processes. Defines signals handling."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def refresh_vocabularies(config):
    """Update the Vocabulary objects in the database if the
    `update_vocabularies` settings is True.
    If the `update_pythesint` setting is also True,
    the local pythesint data is also updated.
    """
    if config.update_vocabularies:
        logger.info('Updating vocabularies...')
        django.core.management.call_command(
            'update_vocabularies',
            force=config.update_pythesint,
            versions=config.pythesint_versions)
        logger.info('Finished updating vocabularies')
    # safety check in order to prevent harvesting process with an empty
    # list of parameters
    elif Parameter.objects.count() < 1:
        raise RuntimeError((
            "Parameters must be updated (with the 'update_vocabularies' command "
            "of django-geospaas) before the harvesting process"
        ))


def save_results(searches_results):
    """Ingests the results of each search in a separate process"""
    with concurrent.futures.ProcessPoolExecutor(initializer=init_worker) as executor:
        try:
            futures = []
            for search_results in searches_results:
                futures.append(executor.submit(search_results.save))

            for future in concurrent.futures.as_completed(futures):
                exception = future.exception()
                if exception:
                    logger.error(
                        "An exception happened during harvesting process",
                        exc_info=exception)
        except KeyboardInterrupt:
            executor.shutdown(wait=False)
            raise


def harvest(cli_arguments):
    """Reads the configuration files and harvests the searched data.
    If errors occur during the ingestion process (like the provider
    website being temporarily unavailable), it is retried at the end.
    """
    config = ProvidersConfiguration.from_file(cli_arguments.config_path)
    search_config = SearchConfiguration.from_file(cli_arguments.search_path) \
                                       .with_providers(config.providers)
    searches_results = search_config.start_searches()

    refresh_vocabularies(config)
    save_results(searches_results)

    # Retry to ingest datasets for which the ingestion failed
    retry_ingest()


def make_arg_parser():
    """Creates a parser for the CLI arguments"""
    arg_parser = argparse.ArgumentParser(
        description='CLI for searching and harvesting data for the GeoSPaaS catalog')
    subparsers = arg_parser.add_subparsers()

    harvest_parser = subparsers.add_parser('harvest',
                                           help='Harvest data directly into the database')
    harvest_parser.add_argument('-c', '--config',
                                dest='config_path',
                                default=default_configuration_path,
                                help='Path to the configuration file')
    harvest_parser.add_argument('-s', '--search',
                                dest='search_path',
                                default=default_search_path,
                                help='Path to the file containing the search parameters')
    harvest_parser.set_defaults(func=harvest)
    return arg_parser


def main():
    """Parse the CLI arguments and call the function corresponding to
    the invoked subcommand
    """
    arg_parser = make_arg_parser()
    cli_arguments = arg_parser.parse_args()
    try:
        cli_arguments.func(cli_arguments)
    except AttributeError:
        arg_parser.print_help()


if __name__ == '__main__':  # pragma: no cover
    main()
