[![Build Status](https://travis-ci.org/nansencenter/django-geo-spaas-harvesting.svg?branch=master)](https://travis-ci.org/nansencenter/django-geo-spaas-harvesting)
[![Coverage Status](https://coveralls.io/repos/github/nansencenter/django-geo-spaas-harvesting/badge.svg?branch=master)](https://coveralls.io/github/nansencenter/django-geo-spaas-harvesting?branch=master)

# Data gathering for GeoSPaaS

This application crawls through data repositories to ingest metadata into a GeoSPaaS database. It
relies on the Django for data access. Specifically, it uses the models defined in
[django-geo-spaas](https://github.com/nansencenter/django-geo-spaas).

## Command line

Harvesting can be launched by executing the `harvest.py` script. If no option is given, it will use
the [default configuration file](./geospaas_harvesting/harvest.yml).

Example:

```shell
python harvest.py
```

### Options

#### -c, --config path

A path to a custom configuration file can be specified.

Example:

```shell
python harvest.py -c ./harvest.yml
```
### Warning for commencing the harvest process
Before commencing the harvest process, the vocabulary must be updated by ```update_vocabularies``` command of django-geo-spaas. This will be done each time the *harvest.py* is executed (before the start of harvesting process). Otherwise, without an updated vocabulary, it will not add (assign) any parameter to the harvested dataset.

## Configuration

### YAML file

The configuration of the harvesters is defined in a YAML file.
An example can be seen in the [default configuration file](./geospaas_harvesting/harvest.yml).

**Top-level keys**:

- **endless** (default: False): boolean controlling the endless harvesting mode. If True, the
  harvesters will be indefinitely re-run after they finish harvesting.
- **poll_interval** (default: 600): the interval in seconds at which the main process checks if the
  running harvester processes have finished executing.
- **harvesters**: dictionary mapping the harvesters names to a dictionary containing their
  properties.

### Environment variables

Generic configuration can be defined using environment variables:

- `GEOSPAAS_HARVESTING_LOG_CONF_PATH`: path to the logging configuration file
- `GEOSPAAS_PERSISTENCE_DIR`: path to the persistence directory
- `SECRET_KEY`: Django secret key
- `GEOSPAAS_DB_HOST`: database hostname
- `GEOSPAAS_DB_PORT`: database port
- `GEOSPAAS_DB_NAME`: database name
- `GEOSPAAS_DB_USER`: database username
- `GEOSPAAS_DB_PASSWORD`: database password

Other environment variables can be defined in each harvester's configuration.

### Harvesters configuration

The properties which are common to every harvester are:

- **class** (mandatory): the class of the harvester. For a list of available harvesters see
  [harvesters.py](./geospaas_harvesting/harvesters.py).

- **max_fetcher_threads** (optional): maximum number of threads in the pool which handles metadata
  fetching and normalization. The optimal value varies from ingester to ingester.

- **max_db_threads** (optional): maximum number of threads in the pool which handles database
  writing. One is generally sufficient. Since each harvester runs in a separate process and each
  thread maintains a database connection open, the total number of database threads across all
  harvesters must be inferior to the maximum number of connections the database accepts (100 is the
  default for Postgresql), with room to spare for other components which might access the database
  (like a web interface or API).

The rest depends on the harvester and will be detailed in each harvester's documentation.

- **time_range** (optional): a two-elements list containing two date strings which define a time
  range to which the crawler will be limited.

## Design

### Components

This application is composed of three main components:

- crawlers: explore data repositories and find URLs of useful datasets
- ingesters: given the URL of a given dataset, fetches its metadata and writes it to the database
- harvesters: orchestrates crawlers and harvesters to get metadata from various repositories

More details below.

#### Crawlers

The role of crawlers is to explore a data repository and find the URLs of the relevant datasets.

They are iterables which, given a data repository URL, return the URLs found when exploring this
repository.

The currently available crawlers are:

- OpenDAP (tested on PO.DAAC's OpenDAP repository)
- OData API (tested on Copernicus Sentinel API Hub)

#### Ingesters

The role of ingesters is to write to the database the relevant metadata about the datasets found by
crawlers.

Given the URL of a dataset, an ingester fetches the metadata from this URL, normalizes it into the
format needed for GeoSPaaS, and writes it into the database.

The tasks of an ingester are primarily I/O bound, so they are multi-threaded. Two thread pools are
used:

- one contains threads which fetch and normalize the metadata
- the other contains threads which write it to the database

The threads of the first thread pool put the normalized metadata in a queue. The threads which write
to the database read from this queue.

To extract the relevant metadata from the raw metadata, most ingesters use the
[metanorm](https://github.com/nansencenter/metanorm) library.

The currently available ingesters are:

- DDX ingester: uses the DDX metadata provided by OpenDAP repositories
- Copernicus OData API ingester: specific to the OData API from Copernicus API hub
- Nansat ingester: uses [Nansat](https://github.com/nansencenter/nansat) to open a local or remote
  file and get its metadata

#### Harvesters

The role of harvesters is to aggregate crawlers and ingesters into an element which can be used to
harvest data from a given provider.

Each harvester has at least two attributes:

- a list of crawlers
- an ingester

The harvester iterates over each of the crawlers and feeds the URLs to the ingester.

The currently available harvesters are:

- PO.DAAC harvester: harvests VIIRS and MODIS data from NASA's PO.DAAC repository
- Copernicus Sentinel harvester: harvests Sentinel 1, 2 and 3 data from the Copernicus API Hub

#### The `harvest.py` script

The entry point of this application is the [harvest.py](./geospaas_harvesting/harvest.py) script.
It takes care of several tasks:

- based on the configuration file, instantiate and run each harvester in a separate process
  so that each data repository can be harvested in parallel.
- when receiving a SIGTERM or SIGINT signal, shut down the harvesters gracefully and dump their
  state so that the harvesting can be resumed where it stopped.
