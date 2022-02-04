"""Module dedicated to retrying failed ingestions"""
import logging
import os
import pickle
import time
from pathlib import Path

import django
import requests
# Load Django settings to be able to interact with the database
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'geospaas_harvesting.settings')
django.setup()
import geospaas_harvesting.ingesters as ingesters  # pylint: disable=wrong-import-position


logger = logging.getLogger(__name__)


def ingest_file(file_path):
    """Ingest the contents of a pickle file. The file should contain an
    ingester object followed by an arbitrary number of 2-tuples
    containing the dataset information required by the ingester and the
    error which happened when trying the first ingestion.
    """
    logger.info("Ingesting datasets from %s", file_path)
    with open(file_path, 'rb') as pickle_file:
        ingester = pickle.load(pickle_file)
        dataset_infos = []
        while True:
            try:
                dataset_info, error = pickle.load(pickle_file)
                if (isinstance(error, requests.ConnectionError) or
                        isinstance(error, requests.Timeout) or
                        isinstance(error, requests.HTTPError and
                        error.response.status_code >= 500 and
                        error.response.status_code <= 599)):
                    dataset_infos.append(dataset_info)
            except EOFError:
                break
    ingester.ingest(dataset_infos)
    file_path.unlink()


def retry_ingest():
    """Ingest the contents of all files contained in the failed
    ingestions directory. Some new files might be created if the
    ingestion fails again. In that case, the new files are retried
    after waiting for a while. Maximum 5 tries.
    """
    base_path = Path(ingesters.Ingester.FAILED_INGESTIONS_PATH)
    glob_pattern = f'*{ingesters.Ingester.RECOVERY_SUFFIX}'
    wait_time = 60  # seconds

    for _ in range(5):  # try maximum 5 times, i.e. wait in total 31 minutes
        for file_path in base_path.glob(glob_pattern):
            ingest_file(file_path)

        if tuple(base_path.glob(glob_pattern)):
            logger.warning("There were errors while ingesting previous failures. "
                           "Will attempt to ingest again in %d seconds.", wait_time)
            time.sleep(wait_time)
            wait_time *= 2
        else:
            break

    if tuple(base_path.glob(glob_pattern)):
        logger.error("There are still errors. Stopping.")
    else:
        logger.info("All failed datasets have been successfully ingested.")


def main():
    """Call recovery functions. For now only ingestion recovery is
    implemented
    """
    retry_ingest()


if __name__ == '__main__':  # pragma: no cover
    main()
