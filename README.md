
Tgres is a tool for receiving and reporting on simple time series
written in Go which uses PostgreSQL for storage.

[GoDoc](https://godoc.org/github.com/tgres/tgres)

### Current Status

Phase 1 or proof-of-concept for the project is the ability to (mostly)
act as a drop-in replacement for Graphite (except for chart
generation) and Statsd. Currently Tgres supports nearly all of
Graphite functions.

As of Aug 2016 Tgres is feature-complete for phase 1, which means that
the development will focus on tests, documentation and stability for a
while.

Tgres is not ready for production use, but is definitely stable enough
for tinkering for those interested.

### Getting Started

You need a newer Go (1.6.1+) and PostgreSQL 9.5 or later.

```
$ go get github.com/tgres/tgres
```

Now you should have a tgres binary in `$GOPATH/bin`.

Look in `$GOPATH/src/github.com/tgres/tgres/etc` for a sample config
file.  Make a copy of this file and edit it, at the very least check
the `db-connect-string` setting. Also check `log-file` directory, it
must be writable.

The user of the PostgreSQL database needs CREATE TABLE permissions. On
first run tgres will create three tables (ds, rra and ts) and two
views (tv and tvd).

Tgres is invoked like this:
```
$ $GOPATH/bin/tgres -c /path/to/config
```

### Other (random) notes

* Tgres can be used as a standalone daemon, but is also suitable for
  use as a Golang package for applications that want to provide TS
  capabilities.

* Tgres can receive data using Graphite Text, UDP and Pickle
  protocols, as well as Statsd (counters, gauges and timers). It
  supports enough of a Graphite HTTP API to be usable with
  Grafana. Tgres implements the majority of the Graphite functions.

* Tgres supports a simplistic form of clustering - currently the
  cluster must share the PG back-end, but otherwise looking quite
  promising.

* A round-robin archive is split over a number of PostgreSQL rows of
  arrays.  This means that a series occupies a constant number of rows
  in the database and can be updated in single row chunks for IO
  efficiency. Tgres provides two PostgreSQL views to make time series
  appear as a regular table.

* Tgres caches datapoints in memory until a certain number of
  datapoints has accumulated or a certain time period has been reached
  to reduce IO.

* One of the main goals of Tgres is to address the data isolation
  problem with most of the TS databases out there. TS data can reside
  in the same PostgreSQL database as other data and can be easily
  joined, analyzed, etc.

* It has to be simple. There shouldn't be many daemons/components -
  just this one process to receive and serve the data and the database
  server.

* Graphite API was used only as a starting point, to be able to prove
  the concept and test against existing tools (mostly Grafana). Longer
  term there may be more interesting methods of exchanging data,
  e.g. gob or Redis protocol, etc.
