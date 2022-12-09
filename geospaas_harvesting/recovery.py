"""Module dedicated to retrying failed ingestions"""
import logging
import os
import pickle
import time
from pathlib import Path

import django
import django.conf
import requests
# Load Django settings to be able to interact with the database
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'geospaas_harvesting.settings')
if not django.conf.settings.configured:
    django.setup()

import geospaas_harvesting.crawlers as crawlers  # pylint: disable=wrong-import-position
import geospaas_harvesting.ingesters as ingesters  # pylint: disable=wrong-import-position


logger = logging.getLogger('geospaas_harvesting.recovery')


def ingest_file(file_path):
    """Ingest the contents of a pickle file. The file should contain an
    ingester object followed by an arbitrary number of 2-tuples
    containing the dataset information required by the ingester and the
    error which happened when trying the first ingestion.
    """
    logger.info("Getting failed ingestions from %s", file_path)
    ingester = ingesters.Ingester()
    with open(file_path, 'rb') as pickle_file:
        dataset_infos = []
        while True:
            try:
                dataset_info, error = pickle.load(pickle_file)
                if (isinstance(error, requests.ConnectionError) or
                        isinstance(error, requests.Timeout) or
                        (isinstance(error, requests.HTTPError) and
                        error.response.status_code >= 500 and
                        error.response.status_code <= 599)):
                    dataset_infos.append(dataset_info)
                else:
                    logger.warning("%s error, won't retry", error.__class__.__name__)
            except EOFError:
                break
        if dataset_infos:
            logger.info("Ingesting datasets from %s", file_path)
            ingester.ingest(dataset_infos)
        else:
            logger.info("Nothing to ingest in %s", file_path)
        file_path.unlink()


def retry_ingest():
    """Ingest the contents of all files contained in the failed
    ingestions directory. Some new files might be created if the
    ingestion fails again. In that case, the new files are retried
    after waiting for a while. Maximum 5 tries.
    """
    base_path = Path(crawlers.CrawlerIterator.FAILED_INGESTIONS_PATH)
    glob_pattern = f'*{crawlers.CrawlerIterator.RECOVERY_SUFFIX}'
    wait_time = 60  # seconds
    recovery_attempted = False

    for _ in range(5):  # try maximum 5 times, i.e. wait in total 31 minutes
        recovery_files = base_path.glob(glob_pattern)
        for file_path in recovery_files:
            recovery_attempted = True
            try:
                ingest_file(file_path)
            except Exception:  # pylint: disable=broad-except
                # do not interrupt recovery process in case of error for one file
                logger.error("Did not manage to ingest %s", file_path, exc_info=True)

        if tuple(base_path.glob(glob_pattern)):
            logger.warning("There were errors while ingesting previous failures. "
                           "Will attempt to ingest again in %d seconds.", wait_time)
            time.sleep(wait_time)
            wait_time *= 2
        else:
            break

    if tuple(base_path.glob(glob_pattern)):
        logger.error("There are still errors. Stopping.")
    elif recovery_attempted:
        logger.info("All failed datasets have been successfully ingested.")


def main():
    """Call recovery functions. For now only ingestion recovery is
    implemented
    """
    retry_ingest()


if __name__ == '__main__':  # pragma: no cover
    main()
