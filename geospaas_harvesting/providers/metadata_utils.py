"""Utility functions for metadata normalizing"""

import importlib
import functools
import pkgutil
import re
import sys
from collections import OrderedDict
from datetime import datetime, timedelta

import pythesint as pti
import shapely.geometry
import shapely.ops
import shapely.wkt
from dateutil.tz import tzutc

from .errors import MetadataNormalizationError


######################## Class manipulation utilities ########################

def get_all_subclasses(base_class):
    """Recursively get all subclasses of `base_class`.
    Returns a set to ensure uniqueness
    """
    subclasses = set()
    for subclass in base_class.__subclasses__():
        subclasses.add(subclass)
        subclasses = subclasses.union(get_all_subclasses(subclass))
    return subclasses


def export_subclasses(package__all__, package_name, package_dir, base_class):
    """Append `base_class` and all of its subclasses declared in
    modules in `package_dir` to `all`. This is meant to be used in
    __init__.py files to make normalizer classes easily importable.
    """
    package__all__.append(base_class.__name__)

    # Import the modules in the package
    for (_, name, _) in pkgutil.iter_modules([package_dir]):
        importlib.import_module('.' + name, package_name)

    # Make the base_class subclasses available
    # in the 'package' namespace
    for cls in get_all_subclasses(base_class):
        setattr(sys.modules[package_name], cls.__name__, cls)
        package__all__.append(cls.__name__)


######################## Pythesint utilities ########################

# Field names commonly used in the 'summary' attribute
SUMMARY_FIELDS = {
    'description': 'Description',
    'processing_level': 'Processing level',
    'product': 'Product',
}


# Key: valid pythesint search keyword
# Value: iterable of aliases
PYTHESINT_KEYWORD_TRANSLATION = {
    # instruments
    'OLCI': ('OL',),
    'SLSTR': ('SL',),
    # platforms
    'METEOSAT-10': ('MSG3',),
    'METEOSAT-11': ('MSG4',),
    'METEOSAT-8': ('MSG1',),
    'METEOSAT-9': ('MSG2',),
    'METOP-B': ('METOP_B',),
    'Sentinel-1A': ('S1A',),
    'Sentinel-1B': ('S1B',),
    'Sentinel-2A': ('S2A',),
    'Sentinel-2B': ('S2B',),
    'Sentinel-3A': ('S3A',),
    'Sentinel-3B': ('S3B',),
    'argo-float': ('Argo float',),
    # providers
    'ESA/EO': ('ESA',),
    'OB.DAAC': ('OB_DAAC',),
    'NASA/JPL/PODAAC': ('POCLOUD',),
    'C-SAR': ('SAR-C', 'SAR-C SAR'),
    'EUMETSAT/OSISAF': ('EUMETSAT OSI SAF',),
    'NSIDC': ('NSIDC_ECS',),
}

def translate_pythesint_keyword(translation_dict, alias):
    """Get a valid pythesint search keyword from known aliases"""
    for valid_keyword, aliases in translation_dict.items():
        if alias in aliases:
            return valid_keyword
    return alias

# TODO: rework the utils for provider so that they are
# consistent with other GCMD fields
def get_gcmd_provider(potential_provider_attributes, additional_keywords=None):
    """
    Get a GCMD provider from a name and/or URL, otherwise return None
    """
    provider = None
    for attribute in potential_provider_attributes:
        provider = gcmd_search('provider', attribute, additional_keywords)
        if provider:
            break
    return provider


def get_gcmd_platform(platform_name, additional_keywords=None):
    """
    Gets a GCMD platform from a platform name, otherwise generate a GCMD platform-like data
    structure
    """
    gcmd_platform = gcmd_search('platform', platform_name, additional_keywords)

    if not gcmd_platform:  # TODO: find a better way to manage the fallback value
        gcmd_platform = OrderedDict([
            ('Category', UNKNOWN),
            ('Series_Entity', UNKNOWN),
            ('Short_Name', platform_name[:100]),
            ('Long_Name', platform_name[:250])
        ])

    return gcmd_platform


def get_gcmd_instrument(instrument_name, additional_keywords=None):
    """
    Gets a GCMD instrument from an instrument name, otherwise generate a GCMD instrument-like data
    structure.
    """
    gcmd_instrument = gcmd_search('instrument', instrument_name, additional_keywords)

    if not gcmd_instrument:
        gcmd_instrument = OrderedDict([
            ('Category', UNKNOWN),
            ('Class', UNKNOWN),
            ('Type', UNKNOWN),
            ('Subtype', UNKNOWN),
            ('Short_Name', instrument_name[:60]),
            ('Long_Name', instrument_name[:200])
        ])

    return gcmd_instrument


def gcmd_search(vocabulary_name, keyword, additional_keywords=None):
    """
    Search for GCMD objects using the provided vocabulary name and keywords.
    Returns None if nothing was found.
    """
    pti_search_method = getattr(pti, f"search_gcmd_{vocabulary_name}_list")
    pti_get_method = getattr(pti, f"get_gcmd_{vocabulary_name}")

    translated_keyword = translate_pythesint_keyword(PYTHESINT_KEYWORD_TRANSLATION, keyword)

    gcmd_object = None
    # Try to search for the object name
    matching_objects = pti_search_method(translated_keyword)
    matching_objects_length = len(matching_objects)

    if matching_objects_length == 1:
        gcmd_object = matching_objects[0]
    # If more than one is found, look for the additional keywords
    # in the search results to narrow it down
    elif matching_objects_length > 1 and additional_keywords:
        restricted_search = restrict_gcmd_search(matching_objects, additional_keywords)
        restricted_search_length = len(restricted_search)
        if restricted_search_length == 1:
            gcmd_object = restricted_search[0]

    if not gcmd_object:
        # If the additional keywords did not manage to narrow down the search enough, or if no
        # additional keyword was provided, try the strict `get_` method from pythesint
        try:
            gcmd_object = pti_get_method(translated_keyword)
        except IndexError:
            pass

    return gcmd_object


def restrict_gcmd_search(gcmd_objects, keywords):
    """Restricts a list of GCMD objects using a list of keywords to search"""
    restricted_search = gcmd_objects.copy()
    restricted_search_length = len(restricted_search)

    for keyword in keywords:
        keyword_search = [
            gcmd_object for gcmd_object in restricted_search
            if keyword.lower() in str(gcmd_object).lower()
        ]
        keyword_search_length = len(keyword_search)
        if keyword_search_length > 0 and keyword_search_length < restricted_search_length:
            restricted_search = keyword_search
            restricted_search_length = keyword_search_length

    return restricted_search


def get_cf_or_wkv_standard_name(keyword):
    """return the values of a dataset parameter in a standard way from the
    standards that are defined in the pti package based on the keyword that has been passed to it.
    For example, it returns something like:

    'standard_name':'sea_ice_area_fraction'
    'canonical_units':'1'
    'description':"X_area_fraction"

    as the result_values.
    """
    try:
        result_values = pti.get_cf_standard_name(keyword)
    except IndexError:
        result_values = pti.get_wkv_variable(keyword)
    return result_values


######################## Time utilities ########################

YEARMONTH_REGEX = r'(?P<year>\d{4})(?P<month>\d{2})'
YEARMONTHDAY_REGEX = YEARMONTH_REGEX + r'(?P<day>\d{2})'

def create_datetime(year, month=1, day=1, day_of_year=None, hour=0, minute=0, second=0):
    """Returns a datetime object using the provided arguments.
    Possible argument combinations are:
      - year, month, day(, hour, minute, second)
      - year, day_of_year(, hour, minute, second)
    """
    year = int(year)
    hour = int(hour)
    minute = int(minute)
    second = int(second)

    if day_of_year:
        day_of_year = int(day_of_year)
        first_day = datetime(year, 1, 1, hour, minute, second).replace(tzinfo=tzutc())
        return first_day + timedelta(days=day_of_year-1)
    else:
        month = int(month)
        day = int(day)
        return datetime(year, month, day, hour, minute, second).replace(tzinfo=tzutc())

def find_time_coverage(time_patterns, url):
    """Find the time coverage based on the 'url' raw attribute.
    Returns a 2-tuple containing the start and end time,
    or a 2-tuple containing None if no time coverage was found.

    This method uses the `time_patterns` dictionary.
    This dictionary has the following structure:
    time_patterns = [
        (
            compiled_regex,
            datetime_creation_function,
            time_coverage_function
        ),
        (...)
    ]
    Where:
        - "url_prefix" is the prefix matched against the 'url' raw
        attribute

        - "compiled_regex" is a compiled regular expresion used to
        extract the time information from the URL. It should
        contain named groups which will be given as arguments
        to the datetime_creation_function

        - "datetime_creation_function" is a function which creates
        a datetime object from the information extracted using
        the regex.

        - "time_coverage_function" is a function which takes the
        datetime object returned by datetime_creation_function
        and returns the time coverage as a 2-tuple
    """
    for matcher, get_time, get_coverage in time_patterns:
        match = matcher.search(url)
        if match:
            file_time = get_time(**match.groupdict())
            return (get_coverage(file_time)[0], get_coverage(file_time)[1])
    raise MetadataNormalizationError(f"Could not extract the time coverage from {url}")

######################## Spatial utilities ########################


def wkt_polygon_from_wgs84_limits(north, south, east, west):
    """
    Returns a WKT string representation of a simple boundary box delimited by its northernmost
    latitude, southernmost latitude, easternmost longitude and westernmost longitude
    """
    return f"POLYGON(({west} {south},{east} {south},{east} {north},{west} {north},{west} {south}))"


def translate_west_coordinates(multipolygon):
    """Translate west coordinates from [-180, 0[ to [180, 360[
    Should be used on a shapely multipolygon
    """

    def translate_point(point):
        return (point[0] + 360, point[1]) if point[0] < 0 else point

    def translate_ring(ring):
        return shapely.geometry.LinearRing(translate_point(point) for point in ring.coords)

    new_polygons = []
    for polygon in multipolygon.geoms:

        exterior_ring = translate_ring(polygon.exterior)

        new_interior_rings = []
        for ring in polygon.interiors:
            new_interior_rings.append(translate_ring(ring))

        new_polygons.append(shapely.geometry.Polygon(exterior_ring, new_interior_rings))

    return shapely.geometry.MultiPolygon(new_polygons)


def restore_west_coordinates(multipolygon):
    """Translate west coordinates back from [180, 360[ to [-180, 0[
    Should be used on a shapely multipolygon split along the IDL
    """

    def restore_point(point, is_east):
        """Restores a point's longitude to the [-180, 0[ range. If the
        current polygon is on the west side of the IDL, points located
        on the IDL which have a longitude of 180 are translated to
        -180.
        """
        if point[0] > 180 or (point[0] == 180 and not is_east):
            lon = point[0] - 360
        else:
            lon = point[0]
        return (lon, point[1])

    def restore_ring(ring):
        return shapely.geometry.LinearRing(restore_point(point, is_east) for point in ring.coords)

    new_polygons = []
    for polygon in multipolygon.geoms:
        # Determine if this polygon is on the east or west side of the
        # IDL. It has been split already, so it is either east or west.
        # We find the first point which is not on the IDL and check
        # whether it is east or west.
        for point in polygon.exterior.coords:
            if point[0] == 180:
                continue
            else:
                # we deal with translated coordinates, so west
                # coordinates are in [180, 360[
                is_east = point[0] < 180
                break

        exterior_ring = restore_ring(polygon.exterior)
        interior_rings = []
        for ring in polygon.interiors:
            interior_rings.append(restore_ring(ring))
        new_polygons.append(shapely.geometry.Polygon(exterior_ring, interior_rings))

    return shapely.geometry.MultiPolygon(new_polygons)


def split_multipolygon_along_idl(multipolygon):
    """Split multipolygons which cross the international dateline to
    avoid undesired side effects
    """
    # if the multipolygon has global coverage, return it as is
    if shapely.wkt.loads(WORLD_WIDE_COVERAGE_WKT).difference(multipolygon).is_empty:
        return multipolygon

    # translate the longitude of west points from  the range [-180, 0[
    # to [180, 360[. This makes it easy to split the multipolygon along
    # the IDL
    translated_geometry = translate_west_coordinates(multipolygon)

    # split the multipolygon along the IDL
    line = shapely.geometry.LineString(((180, 90), (180, -90)))
    split_geometry = shapely.ops.split(translated_geometry, line)

    # restore the longitude of west points to [-180, 0[ and return
    # the result
    return restore_west_coordinates(split_geometry)


######################## Other utilities ########################

UNKNOWN = 'Unknown'
NC_H5_FILENAME_MATCHER = re.compile(r"([^/]+)\.(nc|h5)(\.gz)?$")
WORLD_WIDE_COVERAGE_WKT = 'POLYGON((-180 -90, -180 90, 180 90, 180 -90, -180 -90))'


def dict_to_string(dictionary):
    """Returns a string representation of the dictionary argument.
    The following dictionary:
    {'key1': 'value1', 'key2': 'value2'}
    Will be represented as:
    "key1: value1;key2: value2"
    """
    string = ''
    for key, value in dictionary.items():
        string += f"{key}: {value};"
    return string.rstrip(';')


def raises(exceptions):
    """Decorator for methods which get an attribute from metadata.
    Makes it possible to declare which exception(s) are thrown when the
    raw metadata does not have the expected structure.
    `exceptions` can be an exception class or a tuple of exception
    classes. If any of these exceptions is raised by the method,
    a MetadataNormalizationError with a (hopefully) clear error message
    is raised from this exception.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, raw_metadata):
            try:
                return func(self, raw_metadata)
            except exceptions as error:
                raise MetadataNormalizationError(
                    f"{func.__name__} was unable to process the following metadata: {raw_metadata}"
                ) from error
        return wrapper
    return decorator


def create_parameter_list(parameters):
    """Converts a list of standard names into a list of Pythesint dicts
    """
    return [get_cf_or_wkv_standard_name(cf_parameter) for cf_parameter in parameters]
