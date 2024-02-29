"""This module contains the code necessary to write the metadata of
discovered datasets in the GeoSPaaS catalog database.
"""
import concurrent.futures
import logging
from enum import Enum

from django.contrib.gis.geos import GEOSGeometry

from geospaas.catalog.models import (Dataset, DatasetURI, GeographicLocation,
                                     Source)
from geospaas.vocabularies.models import (DataCenter, Instrument,
                                          ISOTopicCategory, Location, Parameter, Platform)


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

    @staticmethod
    def _uri_exists(uri):
        """Checks if the given URI already exists in the database"""
        return DatasetURI.objects.filter(uri=uri).exists()

    @staticmethod
    def _prepare_dataset_attributes(normalized_attributes):
        """Prepares the attributes needed to instantiate a Dataset"""
        # Create the objects with which the dataset has relationships
        # (or get them if they already exist)
        data_center, _ = DataCenter.objects.get_or_create(
            normalized_attributes.get('provider'))

        location_geometry = normalized_attributes.get('location_geometry')
        geographic_location, _ = GeographicLocation.objects.get_or_create(
            geometry=GEOSGeometry(location_geometry))

        location, _ = Location.objects.get_or_create(normalized_attributes.get('gcmd_location'))

        iso_topic_category, _ = ISOTopicCategory.objects.get_or_create(
            normalized_attributes.get('iso_topic_category'))

        platform, _ = Platform.objects.get_or_create(normalized_attributes.get('platform'))

        instrument, _ = Instrument.objects.get_or_create(
            normalized_attributes.get('instrument'))

        source, _ = Source.objects.get_or_create(
            platform=platform,
            instrument=instrument,
            specs=normalized_attributes.get('specs', ''))
        dataset_parameters_list = normalized_attributes.get('dataset_parameters')

        attributes = {
            'entry_title': normalized_attributes['entry_title'],
            'entry_id': normalized_attributes['entry_id'],
            'summary': normalized_attributes['summary'],
            'time_coverage_start': normalized_attributes['time_coverage_start'],
            'time_coverage_end': normalized_attributes['time_coverage_end'],
            'data_center': data_center,
            'geographic_location': geographic_location,
            'gcmd_location': location,
            'ISO_topic_category': iso_topic_category,
            'source': source,
        }
        return attributes, dataset_parameters_list

    @classmethod
    def _create_dataset(cls, normalized_attributes):
        """Create a Dataset object in the database"""
        dataset_attributes, dataset_parameters_list = (
            cls._prepare_dataset_attributes(normalized_attributes))
        dataset = Dataset.objects.create(**dataset_attributes)
        cls._add_dataset_parameters(dataset, dataset_parameters_list)
        return (dataset, OperationStatus.CREATED)

    @classmethod
    def _update_dataset(cls, dataset, normalized_attributes):
        """Update an existing Dataset object in the database"""
        dataset_attributes, dataset_parameters_list = (
            cls._prepare_dataset_attributes(normalized_attributes))
        Dataset.objects.filter(id=dataset.id).update(**dataset_attributes)
        cls._add_dataset_parameters(dataset, dataset_parameters_list)
        return OperationStatus.UPDATED

    @staticmethod
    def _add_dataset_parameters(dataset, parameters_list):
        """Add parameters to a dataset"""
        for dataset_parameter_info in parameters_list:
            standard_name = dataset_parameter_info.get('standard_name', None)
            short_name = dataset_parameter_info.get('short_name', None)
            units = dataset_parameter_info.get('units', None)
            if standard_name in ['latitude', 'longitude', None]:
                continue
            params = Parameter.objects.filter(standard_name=standard_name)
            if params.count() > 1 and short_name is not None:
                params = params.filter(short_name=short_name)
            if params.count() > 1 and units is not None:
                params = params.filter(units=units)
            if params.count() >= 1:
                dataset.parameters.add(params[0])

    @staticmethod
    def _prepare_dataset_uri_attributes(dataset, uri, normalized_attributes):
        """Prepares the attributes needed to instantiate a DatasetURI
        """
        # Extract service information
        service = normalized_attributes.get('geospaas_service', 'UNKNOWN')
        service_name = normalized_attributes.get('geospaas_service_name', 'UNKNOWN')
        return {
            'dataset': dataset,
            'uri': uri,
            'name': service_name,
            'service': service
        }

    def _create_dataset_uri(self, dataset, uri, normalized_attributes):
        """Create a DatasetURI object in the database"""
        uri_attributes = self._prepare_dataset_uri_attributes(
            dataset, uri, normalized_attributes)
        DatasetURI.objects.create(**uri_attributes)
        return OperationStatus.CREATED

    def _ingest_dataset(self, dataset_info):
        """Writes a dataset to the database based on its attributes and
        URL. The input should be a DatasetInfo object.
        """
        url = dataset_info.url
        normalized_attributes = dataset_info.metadata

        dataset_status = dataset_uri_status = OperationStatus.NOOP

        try:
            dataset_uri = DatasetURI.objects.get(uri=url)
        except DatasetURI.DoesNotExist:
            dataset_uri = None

        try:
            dataset = Dataset.objects.get(entry_id=normalized_attributes['entry_id'])
        except Dataset.DoesNotExist:
            dataset = None

        if dataset is None:
            dataset, dataset_status = self._create_dataset(normalized_attributes)
        else:
            if self.update:
                dataset_status = self._update_dataset(dataset, normalized_attributes)

        if dataset_uri is None:
            dataset_uri_status = self._create_dataset_uri(dataset, url, normalized_attributes)

        return (url, dataset.entry_id, dataset_status, dataset_uri_status)

    def ingest(self, datasets_to_ingest):
        """Iterates over a crawler and writes the datasets to the
        database.
        If a KeyboardInterrupt exception occurs (which might mean that
        a SIGINT or SIGTERM was received by the process), all scheduled
        threads are cancelled. We wait for the currently running
        threads to finish before exiting.
        """
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_db_threads) as executor:
            try:
                futures = []
                for dataset_info in datasets_to_ingest:
                    futures.append(executor.submit(self._ingest_dataset,
                                                   dataset_info))
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
                                self.logger.error("The Dataset URI '%s' was not created.", url)
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
