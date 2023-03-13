# Developer documentation

## Design

### Components

This application is composed of three main components:

- providers: offer an interface to search for data. Use crawlers and ingester.
- crawlers: explore data repositories and find the metadata of useful datasets
- ingester: write metadata to the database

More details below.

#### Providers

The role of a Provider object is to offer an interface for searching through the
data of a particular data provider (Creodias, CMEMS, NASA's PO.DAAC, etc.).
They are defined in the [providers subpackage](./geospaas_harvesting/providers/), each provider
being defined in its own module. The base classes used by providers are defined in the
[providers.base module](./geospaas_harvesting/providers/base.py).

A provider is responsible for parsing search parameters and instanciating the relevant crawler.
The main interface for this is the `search()` method, which returns a `SearchResults` object.

For now, a `SearchResults` object is a lazy iterator which allows to iterate through the metadata
found by the crawler, with optional output filtering. It can also be used to trigger the ingestion
of all of a crawler's results. When the web interface for this package is created, the plan is to
extend this class to allow navigation through the results before ingestion.

Providers use an `ArgumentParser` to parse the search parameters.
See the [arguments parsing section](#arguments-parsing).

#### Crawlers

The role of crawlers is to explore a data repository and find the metadata of 
relevant datasets.

They are iterables which, given a data repository URL, return the metadata found
when exploring this repository.

To extract the relevant metadata from the raw metadata, most crawlers use the
[metanorm](https://github.com/nansencenter/metanorm) library.

Base crawlers are defined in the [crawlers module](./geospaas_harvesting/crawlers.py). These can be
used for common cases or as base classes for more specific crawlers, which might be required for
some providers. In that case, the dedicated crawler is defined in the provider's module.
This allows to keep all the code specific to a data provider in the same place.

#### Ingester

The role of the ingester is to write to the database the metadata found by crawlers.

The tasks of an ingester are primarily I/O bound, so they are multi-threaded.

The ingester class is defined in the [ingesters module](./geospaas_harvesting/ingesters.py).

#### Arguments parsing

There are several cases where we need to parse and validate some arguments (for example when
parsing configuration file or starting a search). This is managed by the classes in the
[arguments](./geospaas_harvesting/arguments.py) module.

Each type of argument is represented by a subclass of the `Argument` class. It is responsible for
validating and outputing the data in the right format.
For simple cases like string arguments, only validation happens.
For more complex cases like datetimes, the input string is transformed into a `datetime` object.
All `Argument` objects have a name, an optional default value, an optional description and can be
mandatory or not.

To do the actual parsing, the `ArgumentParser` class is used. It is initialized with a list of
argument objects, then its `parse()` method can be used to parse a dictionary.

Basic example:

```python
import geospaas_harvesting.arguments as arguments

#define a parser
parser = arguments.ArgumentParser([
    arguments.StringArgument(name='foo'),
    arguments.DatetimeArgument(name='bar', required=True),
    arguments.ChoiceArgument(name='baz', valid_options=[1, 2])
])
#define the data to parse
raw_data = {
    'foo': 'something',
    'bar': '2023-01-01',
    'baz': 1
}
# parse the data
parsed_data = parser.parse(raw_data)
# parsed_data contents:
# {
#     'bar': datetime.datetime(2023, 1, 1, 0, 0, tzinfo=datetime.timezone.utc),
#     'baz': 1,
#     'foo': 'something'
# }
```

#### Configuration

Configuration is managed using the classes in the [config module](./geospaas_harvesting/config.py).
`Configuration` objects can be initialized from a dictionary or a YAML file path.
They use an [ArgumentParser](#arguments-parsing) to parse the raw configuration.
The parameters are set as attributes of the `Configuration` objects.
