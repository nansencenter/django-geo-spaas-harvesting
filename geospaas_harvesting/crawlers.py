"""
A set of crawlers used to explore data provider interfaces and get resources URLs. Each crawler
should inherit from the Crawler class and implement the abstract methods defined in Crawler.
"""
import calendar
import concurrent.futures
import ftplib
import functools
import io
import logging
import os
import os.path
import pickle
import queue
import re
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

import requests

import geospaas_harvesting.utils as utils
import geospaas.catalog.managers as catalog_managers

from metanorm.handlers import MetadataHandler
from metanorm.normalizers.geospaas import GeoSPaaSMetadataNormalizer


logging.getLogger(__name__).addHandler(logging.NullHandler())


class Stop():
    """Class used in normalizing queues to signal that processing
    should stop
    """


class DatasetInfo():
    """Class used to store dataset information coming from crawled repositories
    url is a string, metadata is a dict
    """
    def __init__(self, url, metadata=None):
        self.url = url
        self.metadata = metadata

    def __eq__(self, other):
        return self.url == other.url and self.metadata == other.metadata


class Crawler():
    """Base Crawler class"""

    logger = logging.getLogger(__name__ + '.Crawler')

    def __init__(self, max_threads=1):
        self._metadata_handler = MetadataHandler(GeoSPaaSMetadataNormalizer)
        self.max_threads = max_threads

    # ------------- crawl ------------
    def __iter__(self):
        return CrawlerIterator(self, self.crawl(), max_threads=self.max_threads)

    def crawl(self):
        """Generator which crawls through a dataset repository and yields
        DatasetInfo objects
        """
        raise NotImplementedError()

    def set_initial_state(self):
        """
        This method should set the crawler's attributes which are used for iterating in their
        initial state. Child classes must implement this method so that the crawler can be reused
        """
        raise NotImplementedError()

    @classmethod
    def _http_get(cls, url, request_parameters=None):
        """Returns the contents of a Web page as a string"""
        cls.logger.debug("Getting page: '%s'", url)

        max_tries = 5
        wait_time = 30
        for try_index in range(max_tries):
            try:
                response = utils.http_request('GET', url, **request_parameters or {})
                response.raise_for_status()
                return response.text
            except (requests.ConnectionError, requests.HTTPError, requests.Timeout) as error:
                # retry only for connection errors and HTTP errors 5**
                if (isinstance(error, requests.HTTPError) and
                        (error.response.status_code < 500 or error.response.status_code > 599)):
                    cls.logger.error('Could not get page', exc_info=True)
                    return None
                cls.logger.warning('Error while sending request to %s, %d retries left',
                                   url, max_tries - try_index - 1, exc_info=True)
            except requests.exceptions.RequestException:
                # don't retry
                cls.logger.error('Could not get page', exc_info=True)
                return None

            time.sleep(wait_time)
            wait_time *= 2

        cls.logger.error('Max retries reached for %s', url)
        return None

    # --------- get metadata ---------
    def get_normalized_attributes(self, dataset_info, **kwargs):
        """
        Returns a dictionary of normalized attribute which characterize a Dataset. It should
        contain the following extra entries: `geospaas_service` and `geospaas_service_name`, which
        should respectively contain the `service` and `service_name` values necessary to create a
        DatasetURI object.
        """
        raise NotImplementedError()

    @staticmethod
    def add_url(url, raw_attributes):
        """Utility method to add the dataset's URL to the raw attributes in case it is not there"""
        if 'url' not in raw_attributes:
            raw_attributes['url'] = url


class CrawlerIterator():
    """Iterator for crawlers which returns DatasetInfo objects
    """
    logger = logging.getLogger(__name__ + '.CrawlerIterator')
    QUEUE_SIZE = 500
    FAILED_INGESTIONS_PATH = os.getenv(
        'GEOSPAAS_FAILED_INGESTIONS_DIR',
        os.path.join('/', 'var', 'run', 'geospaas'))
    MAX_FAILED = 500000  # max number of failed objects per recovery file
    RECOVERY_SUFFIX = 'failed_ingestions.pickle'

    def __init__(self, crawler, dataset_infos, max_threads=1):
        """Initializes the iterator and creates a managing thread which
        will in turn spawn normalization threads
        """
        self.crawler = crawler
        self.dataset_infos = dataset_infos
        self.max_threads = max_threads

        self._results = queue.Queue(self.QUEUE_SIZE)
        self._failed = queue.Queue(self.QUEUE_SIZE)

        self.main_thread = threading.current_thread()
        self.manager_thread = threading.Thread(target=self._start_normalizing, daemon=True)
        self.manager_thread.start()

    def __del__(self):
        """Make sure the managing thread is done before destroying the
        iterator
        """
        if self.main_thread == threading.current_thread():
            self.manager_thread.join()

    def __iter__(self):
        return self

    def __next__(self):
        """Gets the next result from the _results queue"""
        next_result = self._results.get()
        if next_result is Stop:
            raise StopIteration()
        else:
            return next_result

    def _pickle_list_elements(self, list_to_pickle, pickle_path):
        """Pickle all the elements in the list, then empty it"""
        self.logger.info("Dumping items to %s", pickle_path)
        with open(pickle_path, 'ab') as pickle_file:
            for element_to_pickle in list_to_pickle:
                pickle.dump(element_to_pickle, pickle_file)
        list_to_pickle.clear()

    def _start_normalizing(self, **kwargs):
        """Iterate over the DatasetInfo objects obtained from the crawler and
        normalize the attributes. Normalizing happens in separate threads to
        parallelize the I/Os.
        """
        # Launch thread which checks the size of the failed ingestions
        # queue and dumps it to disk when necessary
        failed_queue_thread = threading.Thread(target=self._thread_manage_failed_normalizing)
        failed_queue_thread.start()

        try:
            # Launch normalizing threads
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.max_threads,
                    thread_name_prefix=self.__class__.__name__) as executor:
                futures = []
                for dataset_info in self.dataset_infos:
                    futures.append(executor.submit(
                        self._thread_get_normalized_attributes,
                        dataset_info,
                        **kwargs
                    ))
        except KeyboardInterrupt:
            self.logger.info('Normalizing thread received stopping signal')
            for future in reversed(futures):
                future.cancel()
            self.logger.info(
                'Cancelled future normalizing threads')
        finally:
            self.logger.debug("Normalizing threads are done")
            self._results.put(Stop)
            self.logger.debug('Stopping failed queue watcher thread')
            self._failed.put(Stop)
            failed_queue_thread.join()

            # raise exceptions from threads
            for future in concurrent.futures.as_completed(futures):
                exception = future.exception()
                if exception:
                    self.logger.error(
                        "Exception happened during thread",
                        exc_info=exception)

    def _thread_get_normalized_attributes(self, dataset_info, **kwargs):
        """
        Gets the attributes needed to insert a dataset into the
        database from its URL, and puts a dictionary containing these
        attributes in the results queue.
        If an error occurs while retrieving the attributes, the dataset
        info and the exception are put in the _failed queue for
        processing by the dedicated thread.
        This method is meant to be run in a thread.
        """
        self.logger.debug("Getting metadata for '%s'", dataset_info.url)
        try:
            normalized_attributes = self.crawler.get_normalized_attributes(dataset_info, **kwargs)
        except Exception as error:  # pylint: disable=broad-except
            self.logger.error("Could not get metadata for '%s'", dataset_info.url, exc_info=True)
            self._failed.put((dataset_info, error), block=True)
        else:
            self._results.put(DatasetInfo(dataset_info.url, normalized_attributes))

    def _thread_manage_failed_normalizing(self):
        """Watches the `_failed` queue and put the incoming failed
        elements in a list. When the list reaches its maximum size or
        when None is received, dump the contents of the list to a file.
        This method is meant to be run in a thread.
        """
        self.logger.debug("Starting failure management thread")
        class_name = self.__class__.__name__.lower()
        date = datetime.now().strftime('%Y-%m-%dT%H-%M-%S-%f')
        pickle_path = os.path.join(self.FAILED_INGESTIONS_PATH,
                                   f'{class_name}_{date}_{self.RECOVERY_SUFFIX}')

        os.makedirs(self.FAILED_INGESTIONS_PATH, exist_ok=True)

        failed_ingestions = []
        while True:
            element = self._failed.get()

            if element is Stop:
                self.logger.debug("Stopping failure management thread")
                if failed_ingestions:
                    self._pickle_list_elements(failed_ingestions, pickle_path)
                self._failed.task_done()
                break

            failed_ingestions.append(element)
            if len(failed_ingestions) >= self.MAX_FAILED:
                self._pickle_list_elements(failed_ingestions, pickle_path)
            self._failed.task_done()


class LinkExtractor(HTMLParser):
    """
    HTML parser which extracts links from an HTML page
    """

    logger = logging.getLogger(__name__ + '.LinkExtractor')

    def __init__(self):
        """Constructor with extra attribute definition"""
        super().__init__()
        self._links = []

    def error(self, message):
        """Error behavior"""
        self.logger.error(message)

    def feed(self, data):
        """Reset links lists when new data is fed to the parser"""
        self._links = []
        super().feed(data)

    @property
    def links(self):
        """Getter for the links attribute"""
        return self._links

    def handle_starttag(self, tag, attrs):
        """Extracts links from the HTML data"""
        if tag == 'a':
            for attr in attrs:
                if attr[0] == 'href':
                    self._links.append(attr[1])


class DirectoryCrawler(Crawler):
    """Parent class for crawlers used on repositories which expose a directory-like structure"""
    EXCLUDE = None

    YEAR_PATTERN = r'y?(?P<year>\d{4})'
    MONTH_PATTERN = r'm?(?P<month>1[0-2]|0[1-9])'
    DAY_OF_MONTH_PATTERN = r'(?P<day>3[0-1]|[1-2]\d|0[1-9])'
    DAY_OF_YEAR_PATTERN = (r'(?P<day>36[0-6]|3[0-5]\d|[1-2]\d\d|0[1-9]\d|00[1-9])')

    YEAR_MATCHER = re.compile(f'^.*/{YEAR_PATTERN}(/.*)?$')
    MONTH_MATCHER = re.compile(f'^.*/{YEAR_PATTERN}/?{MONTH_PATTERN}(/.*)?$')
    DAY_OF_MONTH_MATCHER = re.compile(
        f'^.*/{YEAR_PATTERN}/?{MONTH_PATTERN}/?{DAY_OF_MONTH_PATTERN}(/.*)?$')
    DAY_OF_YEAR_MATCHER = re.compile(f'^.*/{YEAR_PATTERN}/{DAY_OF_YEAR_PATTERN}(/.*)?$')

    def __init__(self, root_url, time_range=(None, None), include=None, max_threads=1):
        """
        `root_url` is the URL of the data repository to explore.
        `time_range` is a 2-tuple of datetime.datetime objects defining the time range
        of the datasets returned by the crawler.
        `include` is a regular expression string used to filter the crawler's output.
        Only URLs matching it are returned.
        """
        super().__init__(max_threads)
        self.root_url = urlparse(root_url)
        self.time_range = time_range
        self.include = re.compile(include) if include else None
        self.set_initial_state()

    def __eq__(self, other):
        return (
            self.root_url == other.root_url and
            self.time_range == other.time_range and
            self.include == other.include)

    @property
    def base_url(self):
        """Get the root URL without the path"""
        return f"{self.root_url.scheme}://{self.root_url.netloc}"

    # ------------- crawl ------------
    def set_initial_state(self):
        """
        The `_urls` attribute contains URLs to the resources which
        will be returned by the crawler.
        The `_to_process` attribute contains URLs to pages which
        need to be searched for resources.
        """
        self._results = []
        self._to_process = [self.root_url.path.rstrip('/')]

    def crawl(self):
        while True:
            try:
                # Return all resource URLs from the previously processed folder
                yield self._results.pop()
            except IndexError:
                # If no more URLs from the previously processed folder are available,
                # process the next one
                try:
                    self._process_folder(self._to_process.pop())
                except IndexError:
                    break

    @classmethod
    def _folder_coverage(cls, folder_path, time_zone=timezone.utc):
        """
        Find out if the folder has date info in its path.
        The maximum resolution is one day.
        For now, it supports the following structures:
          - .../yyyy/...
          - .../yyyy/mm/...
          - .../yyyymm/...
          - .../yyyy/mm/dd/...
          - .../yyyymmdd/...
          - .../yyyy/ddd/... (day of year)
        It will need to be updated to support new structures.
        """
        folder_coverage_start = folder_coverage_stop = None

        match_day = cls.DAY_OF_MONTH_MATCHER.search(folder_path)
        if match_day:
            folder_coverage_start = datetime(
                int(match_day.group('year')),
                int(match_day.group('month')),
                int(match_day.group('day')),
                tzinfo=time_zone)
            folder_coverage_stop = folder_coverage_start + timedelta(days=1)
            return (folder_coverage_start, folder_coverage_stop)

        match_day_of_year = cls.DAY_OF_YEAR_MATCHER.search(folder_path)
        if match_day_of_year:
            offset = timedelta(int(match_day_of_year.group('day')) - 1)
            folder_coverage_start = datetime(
                int(match_day_of_year.group('year')), 1, 1,
                tzinfo=time_zone) + offset
            folder_coverage_stop = folder_coverage_start + timedelta(days=1)
            return (folder_coverage_start, folder_coverage_stop)

        match_month = cls.MONTH_MATCHER.search(folder_path)
        if match_month:
            last_day_of_month = calendar.monthrange(
                int(match_month.group('year')), int(match_month.group('month')))[1]
            folder_coverage_start = datetime(
                int(match_month.group('year')),
                int(match_month.group('month')),
                1,
                tzinfo=time_zone)
            folder_coverage_stop = datetime(
                int(match_month.group('year')),
                int(match_month.group('month')),
                last_day_of_month,
                tzinfo=time_zone) + timedelta(days=1)
            return (folder_coverage_start, folder_coverage_stop)

        match_year = cls.YEAR_MATCHER.search(folder_path)
        if match_year:
            folder_coverage_start = datetime(int(match_year.group('year')), 1, 1,
                                             tzinfo=time_zone)
            folder_coverage_stop = datetime(int(match_year.group('year')) + 1, 1, 1,
                                            tzinfo=time_zone)
            return (folder_coverage_start, folder_coverage_stop)

        return (folder_coverage_start, folder_coverage_stop)

    def _intersects_time_range(self, start_time=None, stop_time=None):
        """
        Return True if either of these conditions is met:
          - a time coverage was extracted from the folder's path or a timestamp from the dataset's
            name, and this time coverage intersects with the Crawler's time range
          - no time range was defined when instantiating the crawler
          - no time coverage was extracted from the folder's url or dataset's name
        """
        return ((not start_time or not self.time_range[1] or start_time <= self.time_range[1]) and
                (not stop_time or not self.time_range[0] or stop_time >= self.time_range[0]))

    def _list_folder_contents(self, folder_path):
        """Lists the contents of a folder. Should return absolute paths"""
        raise NotImplementedError()

    def _is_folder(self, path):
        """Returns True if path points to a folder"""
        raise NotImplementedError()

    def get_download_url(self, path):
        """Get the download URL from a path in the repository
        """
        return urljoin(self.base_url, path)

    def _add_url_to_return(self, path):
        """
        Add a URL to the list of URLs returned by the crawler after
        checking that it fits inside the crawler's time range.
        """
        download_url = self.get_download_url(path)
        if download_url is not None:
            dataset_info = DatasetInfo(download_url)
            if dataset_info not in self._results:
                self.logger.debug("Adding '%s' to the list of resources.", dataset_info)
                self._results.append(dataset_info)

    def _add_folder_to_process(self, path):
        """Add a folder to the list of folder which will be explored later"""
        if self._intersects_time_range(*self._folder_coverage(path)):
            if path not in self._to_process:
                self.logger.debug("Adding '%s' to the list of pages to process.", path)
                self._to_process.append(path)

    def _process_folder(self, folder_path):
        """
        Get the contents of a folder and feed the _urls (based on includes) and _to_process
        attributes
        """
        self.logger.debug("Looking for resources in '%s'...", folder_path)
        for path in self._list_folder_contents(folder_path):
            # deselect paths which contains any of the excludes strings
            if self.EXCLUDE and self.EXCLUDE.search(path):
                continue
            if self._is_folder(path):
                self._add_folder_to_process(path)
            # select paths which are matched based on input config file
            if self.include and self.include.search(path):
                self._add_url_to_return(path)

    # --------- get metadata ---------
    def get_normalized_attributes(self, dataset_info, **kwargs):
        raise NotImplementedError()


class LocalDirectoryCrawler(DirectoryCrawler):
    """Crawl through the contents of a local folder"""

    logger = logging.getLogger(__name__ + '.LocalDirectoryCrawler')

    # ------------- crawl ------------
    def _list_folder_contents(self, folder_path):
        if self._is_folder(folder_path):
            return [os.path.join(folder_path, file_path) for file_path in os.listdir(folder_path)]
        else:
            # if the given path points to a file, just return it
            return [folder_path]

    def _is_folder(self, path):
        return os.path.isdir(path)

    # --------- get metadata ---------
    def get_normalized_attributes(self, dataset_info, **kwargs):
        raise NotImplementedError()


class HTMLDirectoryCrawler(DirectoryCrawler):
    """Implementation of WebDirectoryCrawler for repositories exposed as HTML pages."""

    logger = logging.getLogger(__name__ + '.HTMLDirectoryCrawler')

    FOLDERS_SUFFIXES = None

    # ------------- crawl ------------
    @staticmethod
    def _strip_folder_page(folder_path):
        """
        Remove the index page of a folder path.
        For example: /foo/bar/contents.html becomes /foo/bar.
        """
        return re.sub(r'/\w+\.html?$', r'', folder_path)

    def _is_folder(self, path):
        return path.endswith(self.FOLDERS_SUFFIXES)

    @classmethod
    def _get_links(cls, html):
        """Returns the list of links contained in an HTML page, passed as a string"""
        parser = LinkExtractor()
        cls.logger.debug("Parsing HTML data.")
        parser.feed(html)
        return parser.links

    @staticmethod
    def _prepend_parent_path(parent_path, paths):
        """
        Prepend the parent_path to each path contained in paths,
        except if the path already starts with the parent_path.
        """
        result = []
        if not parent_path.endswith('/'):
            parent_path += '/'
        for path in paths:
            if path.startswith(parent_path):
                result.append(path)
            else:
                result.append(urljoin(parent_path, path))
        return result

    def _list_folder_contents(self, folder_path):
        html = self._http_get(f"{self.base_url}{folder_path}")
        stripped_folder_path = self._strip_folder_page(folder_path)
        return self._prepend_parent_path(stripped_folder_path, self._get_links(html))

    # --------- get metadata ---------
    def get_normalized_attributes(self, dataset_info, **kwargs):
        raise NotImplementedError()


class OpenDAPCrawler(HTMLDirectoryCrawler):
    """
    Crawler for harvesting the data of OpenDAP
    """
    logger = logging.getLogger(__name__ + '.OpenDAPCrawler')
    FOLDERS_SUFFIXES = ('/contents.html',)
    EXCLUDE = re.compile(r'\?')
    GLOBAL_ATTRIBUTES_NAME = 'NC_GLOBAL'
    NAMESPACE_REGEX = r'^\{(\S+)\}Dataset$'

    # --------- get metadata ---------
    def _get_xml_namespace(self, root):
        """Try to get the namespace for the XML tag in the document from the root tag"""
        try:
            namespace_prefix = re.match(self.NAMESPACE_REGEX, root.tag)[1]  # first matched group
        except TypeError:
            namespace_prefix = ''
            self.logger.warning('Could not find XML namespace while reading DDX metadata')
        return namespace_prefix

    def _extract_attributes(self, root):
        """
        Extracts the global or specific attributes of a dataset or specific ones from a DDX document

        "x_path_global" is pointing to the 'NC_GLOBAL' part of response of the DDX document to
        obtain general information.
        "x_path_specific" is used to extract the dataset parameter names from the DDX document.
        """
        self.logger.debug("Getting the dataset's global attributes.")
        namespaces = {'default': self._get_xml_namespace(root)}
        extracted_attributes = {}
        x_path_global = "./default:Attribute[@name='NC_GLOBAL']/default:Attribute"
        x_path_specific = "./default:Grid/default:Attribute[@name='standard_name']"
        # finding the global metadata
        for attribute in root.findall(x_path_global, namespaces):
            extracted_attributes[attribute.get('name')] = attribute.find(
                "./default:value", namespaces).text
        # finding the parameters of the dataset that are declared in
        # the online source (specific metadata)
        # The specific ones are stored in 'raw_dataset_parameters' part of
        # the returned dictionary("extracted_attributes")
        extracted_attributes['raw_dataset_parameters'] = list()
        for attribute in root.findall(x_path_specific, namespaces):
            extracted_attributes['raw_dataset_parameters'].append(
                attribute.find("./default:value", namespaces).text)
        # removing the "latitude" and "longitude" from
        # the 'raw_dataset_parameters' part of the dictionary
        if 'latitude' in extracted_attributes['raw_dataset_parameters']:
            extracted_attributes['raw_dataset_parameters'].remove('latitude')
        if 'longitude' in extracted_attributes['raw_dataset_parameters']:
            extracted_attributes['raw_dataset_parameters'].remove('longitude')
        return extracted_attributes

    @classmethod
    def get_ddx_url(cls, url):
        """
        Converts the downloadable link into the link for reading meta data. In all cases,
        this method results in a url that ends with '.ddx' which will be used in further steps
        of ingestion.
        """
        if url.endswith('.ddx'):
            return url
        elif url.endswith('.dods'):
            return url[:-4]+'ddx'
        else:
            return url + '.ddx'

    def get_normalized_attributes(self, dataset_info, **kwargs):
        """Get normalized metadata from the DDX info of the dataset located at
        the provided URL
        """
        ddx_url = self.get_ddx_url(dataset_info.url)
        # Get the metadata from the dataset as an XML tree
        stream = io.BytesIO(utils.http_request('GET', ddx_url, stream=True).content)
        # Get all the global attributes of the Dataset into a dictionary
        extracted_attributes = self._extract_attributes(
            ET.parse(stream).getroot())
        # add the URL to the attributes passed to metanorm
        self.add_url(dataset_info.url, extracted_attributes)
        # Get the parameters needed to create a geospaas catalog dataset from the global attributes
        normalized_attributes = self._metadata_handler.get_parameters(extracted_attributes)
        normalized_attributes['geospaas_service'] = catalog_managers.OPENDAP_SERVICE
        normalized_attributes['geospaas_service_name'] = catalog_managers.DAP_SERVICE_NAME

        return normalized_attributes


class ThreddsCrawler(OpenDAPCrawler):
    """
    Crawler for harvesting the data which are provided by Thredds
    """
    logger = logging.getLogger(__name__ + '.ThreddsCrawler')
    FOLDERS_SUFFIXES = ('/catalog.html',)
    FILES_SUFFIXES = ('.nc',)
    EXCLUDE = re.compile(r'/thredds/catalog.html$')
    url_matcher = re.compile(r'^(.*)/(fileServer)/(.*)$')

    # --------- get metadata ---------
    @classmethod
    def get_ddx_url(cls, url):
        url_match = cls.url_matcher.match(url)
        if url_match:
            return f"{url_match[1]}/dodsC/{url_match[3]}.ddx"
        else:
            raise ValueError(f"{url} is not a Thredds HTTPServer URL")

    def get_download_url(self, path):
        result = None
        links = self._get_links(self._http_get(urljoin(self.base_url, path)))
        for link in links:
            if "fileServer" in link and link.endswith(self.FILES_SUFFIXES):
                result = f"{self.base_url}{link}"
                break
        return result


class FTPCrawler(DirectoryCrawler):
    """
    Crawler which returns the search results of an FTP, given the URL and search
    terms
    """
    logger = logging.getLogger(__name__ + '.FTPCrawler')

    def __init__(self, root_url, time_range=(None, None), include=None,
                 username='anonymous', password='anonymous', max_threads=1):

        if not root_url.startswith('ftp://'):
            raise ValueError("The root url must start with 'ftp://'")

        self.username = username
        self.password = password
        self.ftp = None

        super().__init__(root_url, time_range, include, max_threads=1)

    def __getstate__(self):
        """Method used to pickle the crawler"""
        state = self.__dict__
        if isinstance(state['ftp'], ftplib.FTP):
            state['ftp'] = None
        return state

    def __setstate__(self, state):
        """Method used to unpickle the crawler"""
        self.__dict__.update(state)
        self.connect()

    # ------------- crawl ------------
    def set_initial_state(self):
        """
        The `_urls` attribute contains URLs to the resources which will be returned by the crawler.
        The `_to_process` attribute contains URLs to pages which need to be searched for resources.
        """
        self._results = []
        self._to_process = [self.root_url.path or '/']
        self.connect()

    def connect(self):
        """Creates an FTP connection and logs in"""
        self.ftp = ftplib.FTP(self.root_url.netloc, user=self.username, passwd=self.password)
        try:
            self.ftp.login(self.username, self.password)
        except ftplib.error_perm as err_content:
            # these errors happen when we try to log in twice, so they can be ignored
            if not (err_content.args[0].startswith('503') or err_content.args[0].startswith('230')):
                raise

    class Decorators():
        """Decorators for the FTPCrawler"""
        @staticmethod
        def retry_on_timeout(tries=2):
            """Wrapper around the retry decorator which
            enables to pass the number of tries"""
            def decorator_retry(method):
                """Decorator which re-creates the FTP connection
                if a timeout error occurs"""
                @functools.wraps(method)
                def wrapper_reconnect(crawler_instance, *args, **kwargs):
                    """Try to execute the decorated method.
                    If a FTP 421 error or a ConnectionError
                    (a network issue) occurs, re-create the connection
                    """
                    countdown = tries
                    last_error = None
                    while countdown > 0:
                        try:
                            return method(crawler_instance, *args, **kwargs)
                        except (ftplib.error_temp, ConnectionError) as error:
                            last_error = error
                            if isinstance(error, ftplib.error_temp) and '421' not in error.args[0]:
                                raise
                            else:
                                crawler_instance.logger.info("Re-initializing the FTP connection")
                                crawler_instance.connect()
                            countdown -= 1
                    if last_error:
                        raise last_error
                return wrapper_reconnect
            return decorator_retry

    @Decorators.retry_on_timeout(tries=5)
    def _list_folder_contents(self, folder_path):
        return self.ftp.nlst(folder_path)

    @Decorators.retry_on_timeout(tries=5)
    def _is_folder(self, path):
        """Determine if path is a folder by trying to change the working directory to path."""
        try:
            self.ftp.cwd(path)
        except ftplib.error_perm:
            return False
        else:
            return True

    # --------- get metadata ---------
    def get_normalized_attributes(self, dataset_info, **kwargs):
        """Gets dataset attributes using ftp"""
        raw_attributes = {}
        self.add_url(dataset_info.url, raw_attributes)
        normalized_attributes = self._metadata_handler.get_parameters(raw_attributes)
        # TODO: add FTP_SERVICE_NAME and FTP_SERVICE in django-geo-spaas
        normalized_attributes['geospaas_service_name'] = 'ftp'
        normalized_attributes['geospaas_service'] = 'ftp'
        return normalized_attributes


class HTTPPaginatedAPICrawler(Crawler):
    """Base class for crawlers used on repositories exposing a paginated API over HTTP"""

    PAGE_OFFSET_NAME = ''
    PAGE_SIZE_NAME = ''
    MIN_OFFSET = 0

    def __init__(self, url, search_terms=None, time_range=(None, None),
                 username=None, password=None,
                 page_size=100, initial_offset=None, max_threads=1):
        super().__init__(max_threads)
        self.url = url
        self._results = None
        self.initial_offset = initial_offset or self.MIN_OFFSET
        self.request_parameters = self._build_request_parameters(
            search_terms, time_range, username, password, page_size)
        self.set_initial_state()

    def __eq__(self, other):
        return (
            self.url == other.url and
            self.initial_offset == other.initial_offset and
            self.request_parameters == other.request_parameters
        )

    # ------------- crawl ------------
    def set_initial_state(self):
        self.page_offset = self.initial_offset
        self._results = []

    @property
    def page_size(self):
        """Getter for the page size"""
        return self.request_parameters['params'][self.PAGE_SIZE_NAME]

    @property
    def page_offset(self):
        """Getter for the page offset"""
        return self.request_parameters['params'][self.PAGE_OFFSET_NAME]

    @page_offset.setter
    def page_offset(self, offset):
        """Setter for the page offset"""
        self.request_parameters['params'][self.PAGE_OFFSET_NAME] = offset

    def increment_offset(self):
        self.page_offset += 1

    def _build_request_parameters(self, search_terms=None, time_range=(None, None),
                                  username=None, password=None, page_size=100):
        """Build a dict containing the parameters used to query the API.
        This dict will be unpacked to provide the arguments to `requests.get()`.
        """
        return {
            'params': {
                self.PAGE_OFFSET_NAME: self.initial_offset,
                self.PAGE_SIZE_NAME: page_size,
            }
        }

    def crawl(self):
        while True:
            try:
                # Return all resource URLs from the previously processed page
                yield self._results.pop()
            except IndexError:
                # If no more URLs from the previously processed page are available,
                # process the next one
                if not self._get_datasets_info(self._get_next_page()):
                    self.logger.debug("No more entries found at '%s' matching '%s'",
                                    self.url, self.request_parameters['params'])
                    break

    def _get_next_page(self):
        """Get the next page of search results"""
        self.logger.debug("Looking for resources at '%s', matching '%s'",
                         self.url, self.request_parameters['params'])
        current_page = self._http_get(self.url, self.request_parameters)
        self.increment_offset()
        return current_page

    def _get_datasets_info(self, page):
        """Get dataset information from the current page and add it
        to self._results. It should be a DatasetInfo object.
        Returns True if information was found, False otherwise"""
        raise NotImplementedError()

    # --------- get metadata ---------
    def get_normalized_attributes(self, dataset_info, **kwargs):
        raise NotImplementedError()
