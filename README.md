
Tgres is a program comprised of several packages which together can be
used to receive, store and present time-series data using a relational
database as persistent storage (currently only PostgreSQL).

You can currently use the standalone Tgres daemon as Graphite-like API
and Statsd replacement all-in-one, or you can use the Tgres packages
to incorporate time series collection and reporting functionality into
your application.

See [GoDoc](https://godoc.org/github.com/tgres/tgres) for package
details.

Tgres aims to provide TS functionality as a core feature of any database
application without having to rely on external specialized storage,
using the same database as the rest of the application data.

While Tgres is very performant, it isn't meant to compete with high
end TS databases where specialized storage is a must. It is meant to
address the needs of apps that require time-series processing without
having to rely on complex external infrastructure.

Whether you use standalone Tgres or as a package, the time series data
will appear in your database in a compact and efficient format, while
at the same time simple to process using any other tool, language,
or framework because it is just a table (or a view, rather). For a more
detailed description of how Tgres stores data see this
[article](https://grisha.org/blog/2016/12/16/storing-time-series-in-postgresql-part-ii/).

### Current Status

Feb 2017 Note: A major change in the database structure has been made,
Tgres now uses the "write optimized" / "vertical" storage. This change
affected most of the internal code, and as far overall status, it set
us back a bit, all tests are currently broken, but on the bright side,
write performance is amazing now.

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

You need a newer Go (1.6.1+) and PostgreSQL 9.5 or later. To get the
daemon compiled all you need is:

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
