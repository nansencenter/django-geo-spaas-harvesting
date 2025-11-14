"""This module contains the code necessary to write the metadata of
discovered datasets in the GeoSPaaS catalog database.
"""
import concurrent.futures
import logging
from enum import Enum

import django.db.transaction
from django.contrib.gis.geos import GEOSGeometry
import django.db.transaction

from geospaas.catalog.models import Dataset, DatasetURI, Tag


logging.getLogger(__name__).addHandler(logging.NullHandler())


class OperationStatus(Enum):
    NOOP = 0
    CREATED = 1
    UPDATED = 2
    REMOVED = 3


class Ingester():
    """Takes care of ingesting the output of a crawler to the database
    """

    logger = logging.getLogger(__name__ + '.Ingester')

    def __init__(self, max_db_threads=1, update=False):
        if not isinstance(max_db_threads, int):
            raise TypeError
        self.max_db_threads = max_db_threads
        self.update = update

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(max_db_threads={self.max_db_threads}, update={self.update})"
        )

    def _ingest_dataset(self, to_ingest):
        """Writes a dataset to the database based on its attributes and
        URL. The input should be a DatasetInfo object.
        """
        dataset_kwargs, url, keywords, parameters, tags = to_ingest
        dataset_status = dataset_uri_status = OperationStatus.NOOP

        with django.db.transaction.atomic():
            if self.update:
                operation = Dataset.objects.update_or_create
            else:
                operation = Dataset.objects.get_or_create

            try:
                dataset, dataset_created = operation(
                    entry_id=dataset_kwargs['entry_id'],
                    defaults=dataset_kwargs)
            except KeyError:
                dataset, dataset_created = operation(**dataset_kwargs)

            if dataset_created:
                dataset_status = OperationStatus.CREATED
            elif self.update:
                dataset_status = OperationStatus.UPDATED

            dataset_uri, uri_created = DatasetURI.objects.get_or_create(uri=url, dataset=dataset)

            if uri_created:
                dataset_uri_status = OperationStatus.CREATED

            # add many-to-many relationships if the dataset was created
            # or update is True
            if dataset_status == OperationStatus.CREATED or self.update:
                for keyword in keywords:
                    self.logger.debug("Adding keyword %s to dataset %s", keyword, dataset)
                    dataset.keywords.add(keyword)
                for parameter in parameters:
                    self.logger.debug("Adding parameter %s to dataset %s", parameter, dataset)
                    dataset.parameters.add(parameter)
                for tag_kwargs in tags:
                    tag, _ = Tag.objects.get_or_create(**tag_kwargs)
                    self.logger.debug("Adding tag %s to dataset %s", tag, dataset)
                    dataset.tags.add(tag)

        return (dataset_uri.uri, dataset.entry_id, dataset_status, dataset_uri_status)

    def ingest(self, elements_to_ingest):
        """Iterates over an iterator of Tuple[Dataset,DatasetURI] and
        writes the datasets to the database.
        If a KeyboardInterrupt exception occurs (which might mean that
        a SIGINT or SIGTERM was received by the process), all scheduled
        threads are cancelled. We wait for the currently running
        threads to finish before exiting.
        """
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_db_threads) as executor:
            try:
                futures = []
                for to_ingest in elements_to_ingest:
                    futures.append(executor.submit(self._ingest_dataset, to_ingest))
                for future in concurrent.futures.as_completed(futures):
                    try:
                        url, dataset_entry_id, dataset_status, dataset_uri_status = future.result()
                        if dataset_status == OperationStatus.CREATED:
                            self.logger.info("Successfully created dataset '%s' from url: '%s'",
                                             dataset_entry_id, url)
                            if dataset_uri_status == OperationStatus.NOOP:
                                # This should only happen if a database problem
                                # occurred in _ingest_dataset(), because the
                                # presence of the URI in the database is checked
                                # before attempting to ingest.
                                self.logger.error(
                                    "The Dataset URI '%s' was not created for dataset '%s'",
                                    url, dataset_entry_id)
                        elif dataset_status == OperationStatus.UPDATED:
                            self.logger.info("Sucessfully updated dataset '%s' from url: '%s'",
                                             dataset_entry_id, url)
                        elif dataset_status == OperationStatus.NOOP:
                            if dataset_uri_status == OperationStatus.CREATED:
                                self.logger.info("Dataset URI '%s' added to existing dataset '%s'",
                                                 url, dataset_entry_id)
                            elif dataset_uri_status == OperationStatus.NOOP:
                                self.logger.info("Dataset '%s' with URI '%s' already exists",
                                                 dataset_entry_id, url)

                    except Exception as error:  # pylint: disable=broad-except
                        self.logger.error("Error during ingestion: %s", str(error), exc_info=True)
                    finally:
                        futures.remove(future)  # avoid keeping finished futures in memory
            except KeyboardInterrupt:
                for future in reversed(futures):
                    future.cancel()
                self.logger.debug(
                    'Cancelled future ingestion threads, '
                    'waiting for the running threads to finish')
                raise
