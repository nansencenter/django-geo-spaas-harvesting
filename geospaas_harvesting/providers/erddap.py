"""ERDDAP providers"""
from .base import Provider
from ..arguments import ListArgument
from ..crawlers import ERDDAPTableCrawler


class ERDDAPTableProvider(Provider):
    """Provider for tabledap APIs"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = kwargs['url'].rstrip('/')
        self.entry_id_prefix = kwargs.get('entry_id_prefix', '')
        self.id_attr = kwargs['id_attr']
        self.longitude_attr = kwargs['longitude_attr']
        self.latitude_attr = kwargs['latitude_attr']
        self.time_attr = kwargs['time_attr']
        self.position_qc_attr = kwargs['position_qc_attr']
        self.time_qc_attr = kwargs['time_qc_attr']
        self.valid_qc_codes = kwargs['valid_qc_codes']
        self.variables = kwargs['variables']
        self.search_parameters_parser.add_arguments([ListArgument('search_terms', required=False)])

    def _make_crawler(self, parameters):
        time_range = (parameters.pop('start_time', None), parameters.pop('end_time'), None)
        location = parameters.pop('location', None)
        search_terms = parameters.pop('search_terms', [])
        search_terms.extend(self._make_spatial_condition(location))
        search_terms.extend(self._make_temporal_condition(time_range))
        return ERDDAPTableCrawler(
            self.url,
            self.id_attr,
            entry_id_prefix=self.entry_id_prefix,
            longitude_attr=self.longitude_attr,
            latitude_attr=self.latitude_attr,
            time_attr=self.time_attr,
            position_qc_attr=self.position_qc_attr,
            time_qc_attr=self.time_qc_attr,
            valid_qc_codes=self.valid_qc_codes,
            search_terms=search_terms,
            variables=self.variables)

    def _make_spatial_condition(self, location):
        """Make a tabledap spatial condition from a shapely geometry"""
        result = []
        if location:
            min_lon, min_lat, max_lon, max_lat = location.bounds
            result = [
                f"{self.longitude_attr}>={min_lon}",
                f"{self.longitude_attr}<={max_lon}",
                f"{self.latitude_attr}>={min_lat}",
                f"{self.latitude_attr}<={max_lat}",
            ]
        return result

    def _make_temporal_condition(self, time_range):
        """Make a tabledap spatial condition from a couple of datetime
        objects
        """
        result = []
        time_format = '%Y-%m-%dT%H:%M:%SZ'
        if time_range[0]:
            result.append(f"{self.time_attr}>={time_range[0].strftime(time_format)}")
        if time_range[1]:
            result.append(f"{self.time_attr}<={time_range[1].strftime(time_format)}")
        return result