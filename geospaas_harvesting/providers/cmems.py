"""Code for searching CMEMS data (https://marine.copernicus.eu/)"""
import calendar
import logging
import re
import tempfile
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
from urllib.parse import urljoin, urlparse

import copernicusmarine
import pythesint
from copernicusmarine.catalogue_parser.catalogue_parser import MARINE_DATA_STORE_STAC_BASE_URL

import geospaas_harvesting.normalizers.utils as providers_utils

from .base import Provider, TimeFilterMixin
from ..arguments import (ArgumentParser,
                         ChoiceArgument,
                         DatetimeArgument,
                         PathArgument,
                         StringArgument,
                         ListArgument,
                         SequenceArgument,)
from ..crawlers import Crawler, DatasetInfo, FTPCrawler


class CMEMSProvider(Provider):
    """Provider for CMEMS using the copernicusmarine package"""

    type = 'cmems'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.search_parameters_parser.add_arguments([
            StringArgument('product_id', required=True),
            ListArgument('dataset_ids', default=None),
        ])

    def make_crawler(self, parameters):
        return CMEMSCrawler(
            cmems_product_id=parameters['product_id'],
            cmems_dataset_ids=parameters['dataset_ids'],
            time_range=(parameters['start_time'], parameters['end_time']),
            username=parameters['username'],
            password=parameters['password'],
        )


class CMEMSCrawler(Crawler):
    """Crawler which accesses CMEMS products through the
    copernicusmarine toolbox
    """
    argument_parser = ArgumentParser([
        *Crawler.argument_parser.arguments,
        StringArgument('cmems_product_id', required=True),
        SequenceArgument('cmems_dataset_ids', contents_type=StringArgument, required=True),
        SequenceArgument('time_range',
                         contents_type=DatetimeArgument,
                         length=2,
                         default=(None, None)),
        StringArgument('username', default=None),
        StringArgument('password', default=None),
    ])
    S3_BASE_URL = '://'.join(urlparse(MARINE_DATA_STORE_STAC_BASE_URL)[0:2])

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.cmems_product_id = kwargs['cmems_product_id']
        self.cmems_dataset_ids = kwargs['cmems_dataset_ids']
        self.time_range = kwargs['time_range']
        self.username = kwargs['username']
        self.password = kwargs['password']
        # initialized in self.set_initial_state()
        self._product_info = None
        self._tmpdir = None
        self._dataset_lists = None
        self._normalizer = None

    def __eq__(self, other):
        return (
            self.cmems_product_id == other.cmems_product_id and
            self.cmems_dataset_ids == other.cmems_dataset_ids and
            self.time_range == other.time_range and
            self.username == other.username and
            self.password == other.password)

    def make_filter(self):
        """Create a regular expression based on a time range.
        Granularity: 1 day
        """
        first_date = self.time_range[0]
        last_date = self.time_range[1]

        if first_date is None and last_date is None:
            return None

        years = list(range(first_date.year, last_date.year + 1))
        years_regex = []
        for year in years:
            # small optimisation to match whole years without going
            # down to the day-by-day level
            if year > first_date.year and year < last_date.year:
                years_regex.append(f"({year}[0-9]{{4}})")
                continue

            if year == first_date.year:
                first_month = first_date.month
            else:
                first_month = 1

            if year == last_date.year:
                last_month = last_date.month
            else:
                last_month = 12

            months_regex = []
            for month in list(range(first_month, last_month + 1)):
                # small optimisation to match whole months without going
                # down to the day-by-day level
                if (first_date.year != last_date.year and
                        ((year == first_date.year and month > first_date.month)
                         or (year == last_date.year and month < last_date.month))):
                    years_regex.append(f"({year}{month:02d}[0-3][0-9])")
                    continue

                if year == first_date.year and month == first_month:
                    first_day = first_date.day
                else:
                    first_day = 1

                if year == last_date.year and month == last_month:
                    last_day = last_date.day
                else:
                    last_day = calendar.monthrange(year, month)[1]

                days_regex = '|'.join((f"{day:02d}" for day in range(first_day, last_day + 1)))
                months_regex.append(f"{month:02d}({days_regex})")

            years_regex.append(f"({year}({'|'.join(months_regex)}))")
            full_regex = '|'.join(years_regex)

        return f"^(.*_({full_regex})_.*)|({full_regex}.*)$"

    @staticmethod
    def _find_dict_in_list(dicts_list, key, value):
        """Find a dictionary whose `key` equals `value`"""
        for d in dicts_list:
            if d[key] == value:
                return d
        raise RuntimeError(f"Could not find dict with {key}={value} in {dicts_list}")

    def set_initial_state(self):
        """Download lists of dataset files
        """
        self._tmpdir = tempfile.TemporaryDirectory()
        self._dataset_lists = {}
        raw_product_info = copernicusmarine.describe(
            include_description=True,
            include_datasets=True,
            include_keywords=True,
            include_versions=False,
            contains=[self.cmems_product_id])

        self._product_info = self._find_dict_in_list(raw_product_info['products'],
                                                    'product_id', self.cmems_product_id)

        for cmems_dataset in self._product_info['datasets']:
            dataset_id = cmems_dataset['dataset_id']
            if self.cmems_dataset_ids is None or dataset_id in self.cmems_dataset_ids:
                list_file = Path(self._tmpdir.name, f"{dataset_id}.txt")
                if list_file.exists():
                    list_file.unlink()
                copernicusmarine.get(dataset_id=dataset_id,
                                     create_file_list=str(list_file),
                                     regex=self.make_filter(),
                                     username=self.username,
                                     password=self.password)
                self._dataset_lists[dataset_id] = list_file

    def _get_cmems_dataset_properties(self, cmems_dataset_id):
        """Get relevant dataset properties from the product info"""
        raw_dataset_properties = self._find_dict_in_list(self._product_info['datasets'],
                                                         'dataset_id', cmems_dataset_id)

        name = raw_dataset_properties['dataset_name']
        variables = []
        for part in raw_dataset_properties['versions'][0]['parts']:
            service = self._find_dict_in_list(
                part['services'], 'service_type', {
                    'service_name': 'original-files', 'short_name': 'files'})
            for v in service['variables']:
                variables.append(v)

        return (name, variables)


    def crawl(self):
        """Generator which crawls through a dataset repository and yields
        DatasetInfo objects
        """
        self.set_initial_state()
        for dataset_id, dataset_list_file in self._dataset_lists.items():
            name, variables = self._get_cmems_dataset_properties(dataset_id)
            with open(dataset_list_file, 'r') as current_file:
                for line in current_file:
                    yield DatasetInfo(
                        url=line.replace('s3://', f"{self.S3_BASE_URL}/").rstrip(),
                        metadata={
                            'cmems_dataset_name': name,
                            'variables': variables,
                            'product_info': self._product_info,
                        })
