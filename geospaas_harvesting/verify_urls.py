""" Verification module. """
import concurrent.futures
import logging
import os
import sys
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

logging.basicConfig(level=logging.INFO)


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


def check_url(lock, file_name, dataset_uri, auth, throttle=0):
    """Sends an HTTP HEAD request to the URL and writes the URL to the
    output file if it fails
    """
    with closing(requests.head(dataset_uri.uri, allow_redirects=True, auth=auth)) as response:
        logging.debug("%d %s", response.status_code, dataset_uri.uri)
        if response.status_code < 200 or response.status_code > 299:
            with lock:
                with open(file_name, 'a') as file_handle:
                    file_handle.write(
                        f"{response.status_code} {dataset_uri.id} {dataset_uri.uri}{os.linesep}")
    time.sleep(throttle)


def check_provider_urls(file_name, url_prefix, auth, throttle=0):
    """Check the URLs for one provider"""
    logging.info("Starting to check %s URLs", url_prefix)
    lock = Lock()
    max_workers = 1 if throttle else 50
    with BoundedThreadPoolExecutor(max_workers=max_workers, queue_limit=2000) as thread_executor:
        for dataset_uri in DatasetURI.objects.filter(uri__startswith=url_prefix).iterator():
            thread_executor.submit(check_url, lock, file_name, dataset_uri, auth, throttle=throttle)
    logging.info("Finished checking %s URLs", url_prefix)


def main():
    """Runs one process per provider, which checks the URLs for this
    provider
    """
    try:
        dir_name = sys.argv[1]
    except IndexError:
        dir_name = '.'

    providers = read_config(os.path.join(os.path.dirname(__file__), 'check.yml'))

    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = {}
        for provider, attributes in providers.items():
            results_file_name = os.path.join(
                dir_name,
                f"{provider}_stale_urls_{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}.txt"
            )
            futures[executor.submit(
                check_provider_urls,
                results_file_name,
                attributes['url'],
                attributes['auth'],
                attributes['throttle']
            )] = attributes['url']

        error = False
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception:
                error = True
                logging.error("An error occurred while checking '%s'",
                              futures[future], exc_info=True)
    if error:
        sys.exit(1)
    else:
        logging.info("Finished checking all URLs")


if __name__ == '__main__':
    main()
