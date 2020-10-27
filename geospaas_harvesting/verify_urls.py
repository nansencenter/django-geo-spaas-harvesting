""" Verification module. """
from geospaas.catalog.models import DatasetURI
import concurrent.futures
import logging
import os
import sys
from datetime import datetime
from threading import Lock

import django
import requests
import requests.auth

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'geospaas_harvesting.settings')
django.setup()


logging.basicConfig(level=logging.INFO)

providers = {
    'scihub': (
        'https://scihub.copernicus.eu',
        requests.auth.HTTPBasicAuth('topvoys', os.environ.get('COPERNICUS_OPEN_HUB_PASSWORD'))
    ),
    'podaac': ('https://opendap.jpl.nasa.gov/opendap/', None)
}


def check_url(lock, file_name, dataset_uri, auth):
    """"""
    response = requests.head(dataset_uri.uri, allow_redirects=True, auth=auth)
    logging.debug("%d %s", response.status_code, dataset_uri.uri)
    if response.status_code < 200 or response.status_code > 299:
        with lock:
            with open(file_name, 'w') as file_handle:
                file_handle.write(f"{response.status_code} {dataset_uri.uri} {os.linesep}")


def check_provider_urls(file_name, url_prefix, auth):
    """"""
    logging.info("Starting to check %s URLs", url_prefix)
    lock = Lock()
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as thread_executor:
        # with open(file_name, 'w') as file_handle:
        for dataset_uri in DatasetURI.objects.filter(uri__startswith=url_prefix).iterator():
            thread_executor.submit(check_url, lock, file_name, dataset_uri, auth)
    logging.info("Finished checking %s URLs", url_prefix)


def main():
    """"""
    try:
        dir_name = sys.argv[1]
    except IndexError:
        dir_name = '.'

    with concurrent.futures.ProcessPoolExecutor() as executor:
        for file_prefix, (url_prefix, auth) in providers.items():
            file_name = os.path.join(
                dir_name,
                f"{file_prefix}_stale_urls_{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}.txt"
            )
            executor.submit(check_provider_urls, file_name, url_prefix, auth)

    logging.info("Finished checking all URLs")


if __name__ == '__main__':
    main()
