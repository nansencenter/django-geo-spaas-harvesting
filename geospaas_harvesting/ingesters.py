"""This module contains the code necessary to write the metadata of
discovered datasets in the GeoSPaaS catalog database.
"""
import concurrent.futures
import logging

from django.contrib.gis.geos import GEOSGeometry

from geospaas.catalog.models import (Dataset, DatasetURI, GeographicLocation,
                                     Source)
from geospaas.vocabularies.models import (DataCenter, Instrument,
                                          ISOTopicCategory, Location, Parameter, Platform)


logging.getLogger(__name__).addHandler(logging.NullHandler())


class Ingester():
    """Takes care of ingesting the output of a crawler to the database
    """

    logger = logging.getLogger(__name__ + '.Ingester')

    def __init__(self, max_db_threads=1):
        if not isinstance(max_db_threads, int):
            raise TypeError
        self.max_db_threads = max_db_threads

    @staticmethod
    def _uri_exists(uri):
        """Checks if the given URI already exists in the database"""
        return DatasetURI.objects.filter(uri=uri).exists()

    def _ingest_dataset(self, dataset_info):
        """Writes a dataset to the database based on its attributes and
        URL. The input should be a DatasetInfo object.
        """
        url = dataset_info.url
        normalized_attributes = dataset_info.metadata

        created_dataset = created_dataset_uri = False

        if self._uri_exists(url):
            self.logger.info(
                "'%s' is already present in the database", url)
            return (url, created_dataset, created_dataset_uri)

        # Extract service information
        service = normalized_attributes.pop('geospaas_service', 'UNKNOWN')
        service_name = normalized_attributes.pop('geospaas_service_name', 'UNKNOWN')

        # Create the objects with which the dataset has relationships
        # (or get them if they already exist)
        data_center, _ = DataCenter.objects.get_or_create(
            normalized_attributes.pop('provider'))

        location_geometry = normalized_attributes.pop('location_geometry')
        geographic_location, _ = GeographicLocation.objects.get_or_create(
            geometry=GEOSGeometry(location_geometry))

        location, _ = Location.objects.get_or_create(normalized_attributes.pop('gcmd_location'))

        iso_topic_category, _ = ISOTopicCategory.objects.get_or_create(
            normalized_attributes.pop('iso_topic_category'))

        platform, _ = Platform.objects.get_or_create(normalized_attributes.pop('platform'))

        instrument, _ = Instrument.objects.get_or_create(
            normalized_attributes.pop('instrument'))

        source, _ = Source.objects.get_or_create(
            platform=platform,
            instrument=instrument,
            specs=normalized_attributes.pop('specs', ''))
        dataset_parameters_list = normalized_attributes.pop('dataset_parameters')
        # Create Dataset in the database. The normalized_attributes dict contains the
        # "basic parameter", which are not objects in the database.
        # The objects we just created in the database are passed separately.
        dataset, created_dataset = Dataset.objects.get_or_create(
            **normalized_attributes,
            data_center=data_center,
            geographic_location=geographic_location,
            gcmd_location=location,
            ISO_topic_category=iso_topic_category,
            source=source)

        # Create the URI for the created Dataset in the database
        _, created_dataset_uri = DatasetURI.objects.get_or_create(
            name=service_name,
            service=service,
            uri=url,
            dataset=dataset)

        for dataset_parameter_info in dataset_parameters_list:
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

        return (url, created_dataset, created_dataset_uri)

    def ingest(self, datasets_to_ingest, *args, **kwargs):
        """Iterates over a crawler and writes the datasets to the
        database. Database access can be parallelized, although it is
        usually not necessary. The bottleneck when harvesting is
        generally the crawling or metadata normalization step.
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
                    futures.append(executor.submit(self._ingest_dataset, dataset_info))

                for future in concurrent.futures.as_completed(futures):
                    try:
                        url, created_dataset, created_dataset_uri = future.result()
                        if created_dataset:
                            self.logger.info("Successfully created dataset from url: '%s'", url)
                            if not created_dataset_uri:
                                # This should only happen if a database problem
                                # occurred in _ingest_dataset(), because the
                                # presence of the URI in the database is checked
                                # before attempting to ingest.
                                self.logger.warning("The Dataset URI '%s' was not created.", url)
                        elif created_dataset_uri:
                            self.logger.info("Dataset URI '%s' added to existing dataset", url)
                    except Exception:  # pylint: disable=broad-except
                        self.logger.error("Error during ingestion", exc_info=True)
                    finally:
                        futures.remove(future)  # avoid keeping finished futures in memory
            except KeyboardInterrupt:
                for future in reversed(futures):
                    future.cancel()
                self.logger.debug(
                    'Cancelled future ingestion threads, '
                    'waiting for the running threads to finish')
                raise
