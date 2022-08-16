[![Build Status](https://github.com/nansencenter/django-geo-spaas-harvesting/actions/workflows/tests_build.yml/badge.svg)](https://github.com/nansencenter/django-geo-spaas-harvesting/actions/workflows/tests_build.yml)
[![Coverage Status](https://coveralls.io/repos/github/nansencenter/django-geo-spaas-harvesting/badge.svg?branch=master)](https://coveralls.io/github/nansencenter/django-geo-spaas-harvesting?branch=master)

# Data gathering for GeoSPaaS

This application can be used to search for satellite or model data from various providers and ingest
metadata into a GeoSPaaS database. It relies on Django for data access. Specifically, it uses the
models defined in [django-geo-spaas](https://github.com/nansencenter/django-geo-spaas).

## Interfaces

The main interface is the CLI.
A Web interface may be implemented in the future.

### Command line

The CLI can be accessed through the `geospaas_harvesting.cli` module. If no option is given, it will
use the [default configuration file](./geospaas_harvesting/config.yml).

Example:

```shell
python -m geospaas_harvesting.cli harvest
```

#### Options

##### -c, --config <path>

Path to a custom configuration file can be specified.
See [this section](#configyml) for more details.

Example:

```shell
python -m geospaas_harvesting.cli -c ./config.yml harvest
```

##### -s, --search <path>

A path to a search configuration file.
See [this section](#search-configuration) for more details.

```shell
python -m geospaas_harvesting.cli -c ./config.yml harvest -s ./search.yml
```

#### Subcommands

##### harvest

The `harvest` subcommand runs searches based on the `search.yml` file (example
[here](./geospaas_harvesting/search.yml)) and ingests the results in the database.

##### help

### Web interface

Not implemented yet.


### Warning before starting the harvesting process

Before harvesting data, the database must be initialized with `Vocabulary` objects.
The update can be done automatically and is controlled by the `update_vocabularies`, 
`update_pythesint` and `pythesint_versions` in the configuration file.
If you don't know what this means, it is best to keep the default values.

## Configuration

### Files

All configuration files are in YAML. The `!ENV` tag allows to use environment variables as values.

#### `config.yml`

The configuration of the harvesters is defined in this file.
An example can be seen in the [default configuration file](./geospaas_harvesting/harvest.yml).

**Top-level keys**:

- **update_vocabularies** (default: True): update the Vocabulary objects stored in the database
  with the local `pythesint` data. If **update_pythesint** is also set to True, the local data is
  refreshed before the database is updated.
- **update_pythesint** (default: False): update the local pythesint data before harvesting.
  Note that setting this parameter to `True` will have no effect if **update_vocabularies** is set
  to `False`.
- **pythesint_versions** (default: None): the pythesint vocabularies versions to use.
  This is a dictionary in which each key is a pythesint vocabulary name and each value is the
  corresponding version string.
- **providers**: dictionary mapping the providers names to a dictionary containing their settings.

##### Providers configuration

The properties which are common to every harvester are:

- **type** (mandatory): the type of provider. For a list of available harvesters see
  [harvesters.py](./geospaas_harvesting/harvesters.py).

The rest depends on the harvester and will be detailed in each provider's documentation.


#### Search configuration

This file is used to set the search parametersfor each provider you wish to use.
By default, the CLI looks for a file called `search.yml` in the folder from which the search/harvest
command is run.


It contains two sections:
- **common**: dictionary of parameters which will be applied to all the searches, unless overriden
- **searches**: a list of dictionaries, each containing search parameters suited to a provider.
  Each dictionary contained in that list must have the `provider_name` key defined.

##### Common parameters

These search parameters can be used for every provider:
- **start_time** and **end_time**: used to define the temporal coverage. Most usual date formats are
  supported, for example `'2020-05-09T00:00:00Z'`
- **location**: a WKT string defining a shape defining the spatial coverage.

##### Example

```yaml
---
common: # these are common to all searches
  start_time: '2022-07-13'
  end_time: '2022-07-14'
  location: 'POLYGON ((-43.2346 59.8972, -37.1701 62.2756, -31.8527 64.3661, -25.8762 65.8635, -20.7126 68.37690000000001, -19.9435 69.3939, -22.756 70.0712, -26.6232 68.8853, -32.2922 68.25920000000001, -36.6867 66.7291, -41.1252 65.0235, -42.6633 62.8226, -43.2346 59.8972))'
searches:
- provider_name: 'creodias'
  collection: 'Sentinel1'
  processingLevel: 'LEVEL1'
  productType: 'GRD'

- provider_name: 'earthdata_cmr'
  short_name: 'VIIRSJ1_L2_OC_NRT'
  start_time: '2018-12-01T00:00:00Z'
  end_time: '2018-12-04T12:00:00Z'
```

### Environment variables

Generic configuration can be defined using environment variables:

- `GEOSPAAS_HARVESTING_LOG_CONF_PATH`: path to the logging configuration file
- `GEOSPAAS_FAILED_INGESTIONS_DIR`: path to the directory where information about datasets for which errors occurred is stored
- `SECRET_KEY`: Django secret key
- `GEOSPAAS_DB_HOST`: database hostname
- `GEOSPAAS_DB_PORT`: database port
- `GEOSPAAS_DB_NAME`: database name
- `GEOSPAAS_DB_USER`: database username
- `GEOSPAAS_DB_PASSWORD`: database password

Other environment variables can be defined in the configuration files by using the `!ENV` tag.

## Design

### Components

This application is composed of three main components:

- providers: offer an interface to search for data. Use crawlers and ingester.
- crawlers: explore data repositories and find the metadata of useful datasets
- ingester: write metadata to the database

More details below.

#### Crawlers

The role of crawlers is to explore a data repository and find the metadata of 
relevant datasets.

They are iterables which, given a data repository URL, return the metadata found
when exploring this repository.

To extract the relevant metadata from the raw metadata, most crawlers use the
[metanorm](https://github.com/nansencenter/metanorm) library.

#### Ingester

The role of the ingester is to write to the database the metadata found by
crawlers.

The tasks of an ingester are primarily I/O bound, so they are multi-threaded.

#### Providers

The role of a Provider object is to offer an interface for searching through the
data of a particular data provider (Creodias, CMEMS, NASA's PO.DAAC, etc.).
