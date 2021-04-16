""" Verification module. """
import concurrent.futures
import functools
import logging
import os
import sys
import time
from datetime import datetime
from threading import Lock

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


def throttle(period):
    """Decorator builder. Returns a decorator which prevents a function
    from being called more than once every `period` seconds
    """
    def decorator_throttle(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if wrapper.last_call:
                time_since_last_call = time.monotonic() - wrapper.last_call
                if time_since_last_call < period:
                    time.sleep(period - time_since_last_call)
            wrapper.last_call = time.monotonic()
            return func(*args, **kwargs)
        wrapper.last_call = 0
        return wrapper
    return decorator_throttle


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
    """Reads the configuration file and build a providers dictionary
    """
    yaml.SafeLoader.add_constructor('!ENV', lambda loader, node: os.getenv(node.value))
    with open(config_path, 'r') as config_file:
        config = yaml.safe_load(config_file)

    providers = {}
    for provider, attributes in config.items():

        throttle_period = attributes.get('throttle')
        checker_function = throttle(throttle_period)(check_url) if throttle_period else check_url

        providers[provider] = {
            'url': attributes['url'],
            'auth': get_auth(attributes),
            'checker': checker_function
        }

    return providers


def check_url(lock, file_name, dataset_uri, auth):
    """Sends an HTTP HEAD request to the URL and writes the URL to the
    output file if it fails
    """
    response = requests.head(dataset_uri.uri, allow_redirects=True, auth=auth)
    logging.debug("%d %s", response.status_code, dataset_uri.uri)
    if response.status_code < 200 or response.status_code > 299:
        with lock:
            with open(file_name, 'a') as file_handle:
                file_handle.write(
                    f"{response.status_code} {dataset_uri.id} {dataset_uri.uri} {os.linesep}"
                    f"{response.text} {os.linesep}{os.linesep}"
                )


def check_provider_urls(file_name, checker, url_prefix, auth):
    """Check the URLs for one provider"""
    logging.info("Starting to check %s URLs", url_prefix)
    lock = Lock()
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as thread_executor:
        for dataset_uri in DatasetURI.objects.filter(uri__startswith=url_prefix).iterator():
            thread_executor.submit(checker, lock, file_name, dataset_uri, auth)
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
        for provider, attributes in providers.items():
            results_file_name = os.path.join(
                dir_name,
                f"{provider}_stale_urls_{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}.txt"
            )
            executor.submit(
                check_provider_urls,
                results_file_name,
                attributes['checker'],
                attributes['url'],
                attributes['auth']
            )

    logging.info("Finished checking all URLs")


if __name__ == '__main__':
    main()
