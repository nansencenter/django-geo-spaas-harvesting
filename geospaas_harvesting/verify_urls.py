"""URLs verification module"""
import argparse
import concurrent.futures
import logging
import os
import time
from contextlib import closing
from datetime import datetime
from threading import BoundedSemaphore, Lock

import django
import oauthlib.oauth2
import requests
import requests.auth
import requests_oauthlib
import yaml

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'geospaas_harvesting.settings')
django.setup()
from geospaas.catalog.models import DatasetURI

import geospaas_harvesting.utils as utils

logger = logging.getLogger('geospaas_harvesting.verify_urls')


class TooManyRequests(Exception):
    """Exception raised when HTTP error 429 is repeatedly received from
    a provider
    """


class BoundedThreadPoolExecutor(concurrent.futures.ThreadPoolExecutor):
    """A ThreadPoolExecutor which has a limit on the number of jobs
    which can be submitted at once
    """

    def __init__(self, *args, queue_limit=10000, **kwargs):
        self.semaphore = BoundedSemaphore(kwargs.get('max_workers', 0) + queue_limit)
        super().__init__(*args, **kwargs)

    def submit(self, fn, *args, **kwargs):
        self.semaphore.acquire()
        try:
            future = super().submit(fn, *args, **kwargs)
        except:
            # if anything goes, wrong, we need to release the semaphore
            self.semaphore.release()
            raise
        else:
            # release the semaphore once the thread ends
            future.add_done_callback(lambda x: self.semaphore.release())
            return future


def build_oauth2(username, password, token_url, client_id):
    """Creates an OAuth2 object usable by requests.get()"""
    client = oauthlib.oauth2.LegacyApplicationClient(client_id=client_id)
    token = requests_oauthlib.OAuth2Session(client=client).fetch_token(
        token_url=token_url,
        username=username,
        password=password,
        client_id=client_id,
    )
    return requests_oauthlib.OAuth2(client_id=client_id, client=client, token=token)


def get_auth(attributes):
    """Returns the right authentication object based on the provided
    attributes
    """
    if set(('username', 'password', 'token_url', 'client_id')).issubset(attributes):
        return build_oauth2(
            attributes['username'], attributes['password'],
            attributes['token_url'], attributes['client_id'],
        )
    elif set(('username', 'password')).issubset(attributes):
        return requests.auth.HTTPBasicAuth(attributes['username'], attributes['password'])
    else:
        return None


def read_config(config_path):
    """Reads the configuration file and builds a dictionary of
    providers
    """
    yaml.SafeLoader.add_constructor('!ENV', lambda loader, node: os.getenv(node.value))
    with open(config_path, 'r') as config_file:
        config = yaml.safe_load(config_file)

    providers = {}
    for provider, attributes in config.items():
        providers[provider] = {
            'url': attributes['url'],
            'auth': get_auth(attributes),
            'throttle': attributes.get('throttle', 0)
        }

    return providers


def check_url(dataset_uri, auth, throttle=0, tries=5):
    """Sends an HTTP HEAD request to the URL and returns whether it is
    valid or not
    """
    while tries:
        tries -= 1
        with closing(utils.http_request(
                'HEAD', dataset_uri.uri, allow_redirects=True, auth=auth)) as response:
            status_code = response.status_code
            headers = response.headers

        logger.debug("%d %s", status_code, dataset_uri.uri)

        # Too Many Requests: wait and retry
        if status_code == 429:
            if tries <= 0:
                raise TooManyRequests(dataset_uri)
            else:
                logger.warning(
                    "Error 429 received from '%s'; retries left: %d", dataset_uri, tries)
                time.sleep(headers.get('Retry-After', 60))
        # other errors: return False
        elif status_code < 200 or status_code > 299:
            tries = 0
            is_valid = False
        # no error: return True
        else:
            tries = 0
            is_valid = True

    time.sleep(throttle)
    return (is_valid, status_code, dataset_uri.id, dataset_uri.uri)


def write_stale_url(lock, file_name, dataset_uri, auth, throttle=0, tries=5):
    """Check the `dataset_uri` and write it to the output file if it is not valid"""
    is_valid, status_code, dataset_uri_id, url = check_url(
        dataset_uri, auth, throttle=throttle, tries=tries)

    if not is_valid:
        with lock, open(file_name, 'a') as file_handle:
            file_handle.write(f"{status_code} {dataset_uri_id} {url}{os.linesep}")


def check_provider_urls(file_name, url_prefix, auth, throttle=0):
    """Check the URLs for one provider"""
    logger.info("Starting to check %s URLs", url_prefix)
    lock = Lock()
    max_workers = 1 if throttle else 50
    with BoundedThreadPoolExecutor(max_workers=max_workers, queue_limit=2000) as thread_executor:
        for dataset_uri in DatasetURI.objects.filter(uri__startswith=url_prefix).iterator():
            thread_executor.submit(
                write_stale_url, lock, file_name, dataset_uri, auth, throttle=throttle)
    logger.info("Finished checking %s URLs", url_prefix)


def check_providers(output_directory, providers):
    """Check the URLs for each provider in a separate process"""
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = {}
        for provider, attributes in providers.items():
            results_file_name = os.path.join(
                output_directory,
                f"{provider}_stale_urls_{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}.txt"
            )
            futures[executor.submit(
                check_provider_urls,
                results_file_name,
                attributes['url'],
                attributes['auth'],
                attributes['throttle']
            )] = attributes['url']

        success = True
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception:
                success = False
                logger.error("An error occurred while checking '%s'",
                             futures[future], exc_info=True)
        return success


def find_provider(url, providers):
    """Find which provider a given URL comes from"""
    for provider in providers.values():
        if url.startswith(provider['url']):
            return provider


def remove_dataset_uri(dataset_uri):
    """Remove a DatasetURI and the corresponding Dataset, if it has no
    URIs anymore
    """
    logger.debug("Removing dataset URI %d, %s", dataset_uri.id, dataset_uri.uri)
    dataset = dataset_uri.dataset
    dataset_uri.delete()

    remove_dataset = not dataset.dataseturi_set.all()  # .all() is needed to refresh the queryset
    if remove_dataset:
        logger.debug("Removing dataset %d", dataset.id)
        dataset.delete()

    return remove_dataset


def delete_stale_urls(urls_file_path, providers, force=False):
    """Re-check the URLs contained in a file issued from the checking
    step, then remove them.
    """
    deleted_uris_count = 0
    deleted_datasets_count = 0
    with open(urls_file_path, 'r') as urls_file:
        for line in urls_file:
            _, dataset_uri_id, _ = line.split()
            dataset_uri = DatasetURI.objects.get(id=dataset_uri_id)
            provider = find_provider(dataset_uri.uri, providers)
            is_valid, status_code, *_ = check_url(dataset_uri, provider['auth'])
            if not is_valid and (status_code == 404 or force):
                deleted_uris_count += 1
                if remove_dataset_uri(dataset_uri):
                    deleted_datasets_count += 1
    return (deleted_uris_count, deleted_datasets_count)


def parse_cli_arguments():
    """Parse the arguments given on the command line"""
    parser = argparse.ArgumentParser(description='Check and cleanup dataset URIs.')
    parser.add_argument(
        '-p', '--providers-conf',
        default=os.path.join(os.path.dirname(__file__), 'check.yml'),
        help='Path to the providers configuration file.')

    sub_parsers = parser.add_subparsers(dest='action', required=True)

    check_parser = sub_parsers.add_parser(
        'check',
        help=('Check all URIs present in the database. Write the stale URIs to files in the '
              'output directory (one file per provider).'))
    check_parser.add_argument(
        '-o', '--output-directory',
        default='.',
        help='Path to the directory where the output files will be created')

    delete_parser = sub_parsers.add_parser(
        'delete-stale',
        help=('Delete the stale URLs present in the file given as argument. The file should have '
              'the same structure as one obtained by running this script with the --check option'
              'By default, only URLs which return an HTTP error 404 are deleted. To override this '
              'behaviour and remove URLs which return any kind of error, use the --force option.'))
    delete_parser.add_argument(
        'urls_file',
        help='Path to the file containing the URls to delete.')
    delete_parser.add_argument(
        '-f', '--force',
        action='store_true',
        help='Remove URLs which return any error, not just 404')

    return parser.parse_args()


def main():
    """Runs one process per provider, which checks the URLs for this
    provider
    """
    args = parse_cli_arguments()
    providers = read_config(args.providers_conf)

    if args.action == 'check':
        if check_providers(args.output_directory, providers):
            logger.info("Finished checking all URLs")
    elif args.action == 'delete-stale':
        deleted_counts = delete_stale_urls(args.urls_file, providers)
        logger.info("Deleted %d URIs and %d datasets", *deleted_counts)


if __name__ == '__main__':
    main()
