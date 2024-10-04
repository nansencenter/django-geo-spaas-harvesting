"""Code for searching CMEMS data (https://marine.copernicus.eu/)"""
import calendar
import re
import tempfile
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
from urllib.parse import urljoin, urlparse

import copernicusmarine
import pythesint
from copernicusmarine.catalogue_parser.catalogue_parser import MARINE_DATA_STORE_STAC_BASE_URL

import geospaas_harvesting.providers.metadata_utils as providers_utils
from geospaas.catalog.managers import HTTP_SERVICE, HTTP_SERVICE_NAME
from .base import Provider, TimeFilterMixin
from ..arguments import  ChoiceArgument, PathArgument, StringArgument, ListArgument
from ..crawlers import Crawler, DatasetInfo, FTPCrawler


class CMEMSProvider(Provider):
    """Provider for CMEMS using the copernicusmarine package"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.search_parameters_parser.add_arguments([
            StringArgument('product_id', required=True),
            ListArgument('dataset_ids', default=None),
        ])

    def _make_crawler(self, parameters):
        return CMEMSCrawler(
            cmems_product_id=parameters['product_id'],
            cmems_dataset_ids=parameters['dataset_ids'],
            time_range=(parameters['start_time'], parameters['end_time']),
            username=self.username,
            password=self.password,
        )


class CMEMSCrawler(Crawler):
    """Crawler which accesses CMEMS products through the
    copernicusmarine toolbox
    """
    S3_BASE_URL = '://'.join(urlparse(MARINE_DATA_STORE_STAC_BASE_URL)[0:2])

    def __init__(self, cmems_product_id, cmems_dataset_ids, time_range=(None, None),
                 username=None, password=None, max_threads=1):
        super().__init__(max_threads)
        self.cmems_product_id = cmems_product_id
        self.cmems_dataset_ids = cmems_dataset_ids
        self.time_range = time_range
        self.username = username
        self.password = password
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

        return f".*_({'|'.join(years_regex)})_.*"

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

        self._normalizer = CMEMSMetadataNormalizer(self._product_info)

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
        for dataset_id, dataset_list_file in self._dataset_lists.items():
            name, variables = self._get_cmems_dataset_properties(dataset_id)
            with open(dataset_list_file, 'r') as current_file:
                for line in current_file:
                    yield DatasetInfo(
                        url=line.replace('s3://', f"{self.S3_BASE_URL}/").rstrip(),
                        metadata={
                            'cmems_dataset_name': name,
                            'variables': variables,
                        })

    def get_normalized_attributes(self, dataset_info, **kwargs):
        """Use normalizer to get normalized attributes"""
        return self._normalizer.get_normalized_attributes(dataset_info, **kwargs)


class CMEMSMetadataNormalizer():
    """Normalizer for CMEMS datasets"""

    def __init__(self, product_info):
        self._product_info = product_info

    def get_normalized_attributes(self, dataset_info, **kwargs):
        """Returns attributes which can be used to instantiate a
        GeoSPaaS dataset
        """
        entry_id = self.get_entry_id(dataset_info)
        time_coverage = self.get_time_coverage(entry_id)
        platform, instrument = self.get_source(dataset_info)
        service, service_name = self.get_service(dataset_info)

        return {
            'entry_title': self._product_info['title'],
            'entry_id': entry_id,
            'summary': self.get_summary(dataset_info),
            'time_coverage_start': time_coverage[0],
            'time_coverage_end': time_coverage[1],
            'platform': platform,
            'instrument': instrument,
            'location_geometry': self.get_location_geometry(dataset_info),
            'provider': self.get_provider(dataset_info),
            'iso_topic_category': self.get_iso_topic_category(dataset_info),
            'gcmd_location': self.get_gcmd_location(dataset_info),
            'dataset_parameters': self.get_dataset_parameters(dataset_info),
            'geospaas_service_name': service_name,
            'geospaas_service': service,
        }

    @providers_utils.raises((AttributeError, TypeError))
    def get_entry_id(self, dataset_info):
        """Extract entry_id from URL"""
        return providers_utils.NC_H5_FILENAME_MATCHER.search(dataset_info.url).group(1)

    def get_summary(self, dataset_info):
        """Build a summary from metadata fields"""
        return providers_utils.dict_to_string({
            providers_utils.SUMMARY_FIELDS['description']: self._product_info['description'],
            providers_utils.SUMMARY_FIELDS['processing_level']: (
                self._product_info['processing_level']),
            providers_utils.SUMMARY_FIELDS['product']: self._product_info['product_id'],
            'Dataset ID': dataset_info.metadata['cmems_dataset_name'],
        })

    def get_time_coverage(self, entry_id):
        """Get the time coverage from the file name"""
        dataset_patterns = (
            # dataset-specific time coverage
            (
                re.compile(rf'^nrt_global_allsat_phy_l4_{providers_utils.YEARMONTHDAY_REGEX}_'),
                lambda time: (time - relativedelta(hours=12), time + relativedelta(hours=12))
            ),
            (
                re.compile(rf'^dataset-uv-nrt-monthly_{providers_utils.YEARMONTH_REGEX}T'),
                lambda time: (time, time + relativedelta(months=1))
            ),
            (
                re.compile(rf'^mercatorpsy4v3r1_gl12_mean_{providers_utils.YEARMONTH_REGEX}'),
                lambda time: (time, time + relativedelta(months=1))
            ),
            (
                re.compile(
                    r'^mercatorpsy4v3r1_gl12_(thetao|so|uovo)_' +
                    providers_utils.YEARMONTHDAY_REGEX +
                    r'_(?P<hour>\d{2})h_R'),
                lambda time: (time, time)
            ),
            (
                re.compile(rf'{providers_utils.YEARMONTHDAY_REGEX}_m-.*\.nc$'),
                lambda time: (time, time + relativedelta(months=1))
            ),
            (
                re.compile(
                    rf'^CMEMS_v5r1_IBI_PHY_NRT_PdE_01mav_{providers_utils.YEARMONTHDAY_REGEX}_.*$'),
                lambda time: (time, time + relativedelta(months=1))
            ),
            (
                re.compile(rf"/{providers_utils.YEARMONTHDAY_REGEX}" +
                           r"_mm-12km-NERSC-MODEL-TOPAZ4B-ARC-RAN.*"),
                lambda time: (
                    datetime(time.year, time.month, 1, tzinfo=time.tzinfo),
                    datetime(time.year, time.month, 1, tzinfo=time.tzinfo) + relativedelta(months=1)
                )
            ),
            (
                re.compile(rf"^{providers_utils.YEARMONTHDAY_REGEX}" +
                           r"_ym-12km-NERSC-MODEL-TOPAZ4B-ARC-RAN.*"),
                lambda time: (time, time + relativedelta(years=1))
            ),
            (
                re.compile(rf"^{providers_utils.YEARMONTH_REGEX}" +
                           r"_mm-metno-MODEL-topaz5_ecosmo-ARC-.*"),
                lambda time: (time, time + relativedelta(months=1))
            ),
            (
                re.compile(rf'^mfwamglocep_{providers_utils.YEARMONTHDAY_REGEX}00_R[0-9]{8}'),
                lambda time: (time, time + relativedelta(hours=24))
            ),
            (
                re.compile(rf'^mercatorbiomer4v2r1_global_mean_{providers_utils.YEARMONTH_REGEX}$'),
                lambda time: (time, time + relativedelta(months=1))
            ),
            # generic 1 day coverage
            (
                re.compile(rf'(^|[-_.:]){providers_utils.YEARMONTHDAY_REGEX}[-_.:T]'),
                lambda time: (time, time + relativedelta(days=1))
            ),
            # generic 1 month coverage
            (
                re.compile(rf'(^|[-_.:]){providers_utils.YEARMONTH_REGEX}[-_.:T]'),
                lambda time: (time, time + relativedelta(months=1))
            ),
        )
        for regex, make_time_coverage in dataset_patterns:
            match = regex.search(entry_id)
            if match:
                return make_time_coverage(providers_utils.create_datetime(**match.groupdict()))
        raise RuntimeError("Could not get time coverage")

    @staticmethod
    def _search_source(vocabulary_name, search_strings):
        """Look for platform or instrument in a pythesint vocabulary.
        Try each search string until a result is found.
        """
        source = None
        for search_string in search_strings:
            sources = pythesint.vocabularies[vocabulary_name].fuzzy_search(search_string)
            if sources:
                source = sources[0]
                break
        if source is None:
            raise RuntimeError(f"could not find source in {vocabulary_name}")
        return source

    def get_source(self, dataset_info):
        """Get the platform and instrument"""
        search_strings = (
            dataset_info.metadata['cmems_dataset_name'],
            *self._product_info['sources'],
        )
        platform = self._search_source('gcmd_platform', search_strings)
        if platform['Category'] == 'Models':
            instrument = pythesint.get_gcmd_instrument('Computer')
        else:
            instrument = self._search_source('gcmd_instrument', search_strings)
        return (platform, instrument)

    def get_location_geometry(self, dataset_info):
        """Get the spatial coverage of the dataset"""
        bbox = dataset_info.metadata['variables'][0]['bbox']
        return providers_utils.wkt_polygon_from_wgs84_limits(bbox[3], bbox[1], bbox[2], bbox[0])

    def get_provider(self, dataset_info):
        """Get the data provider"""
        return pythesint.get_gcmd_provider('CMEMS')

    def get_iso_topic_category(self, dataset_info):
        """Get ISO 19115 topic category"""
        return pythesint.get_iso19115_topic_category('Oceans')

    def get_gcmd_location(self, dataset_info):
        """Get the GCMD location"""
        return pythesint.get_gcmd_location('OCEAN')

    def get_dataset_parameters(self, dataset_info):
        """Get a list of normalized dataset variables"""
        variables = []
        variable_dict = None
        for variable in dataset_info.metadata['variables']:
            try:
                variable_dict = providers_utils.get_cf_or_wkv_standard_name(
                    variable['standard_name'])
            except IndexError:
                try:
                    variable_dict = pythesint.vocabularies['cf_standard_name'].fuzzy_search(
                            variable['standard_name'])[0]
                except IndexError:
                    continue
            if variable_dict not in variables:
                variables.append(variable_dict)
        return variables

    def get_service(self, dataset_info):
        """Get the type of the repository where the data is hosted
        """
        return (HTTP_SERVICE, HTTP_SERVICE_NAME)
