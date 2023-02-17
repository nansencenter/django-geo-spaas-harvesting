# Developer documentation

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
