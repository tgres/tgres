
[![Build Status](https://travis-ci.com/tgres/tgres.svg?branch=master)](https://travis-ci.com/tgres/tgres)

Tgres is a program comprised of several packages which together can be
used to receive, store and present time-series data using a relational
database as persistent storage (currently only PostgreSQL).

You can currently use the standalone Tgres daemon as Graphite-like API
and Statsd replacement all-in-one, or you can use the Tgres packages
to incorporate time series collection and reporting functionality into
your application.

See [GoDoc](https://godoc.org/github.com/tgres/tgres) for package
details.

Whether you use standalone Tgres or as a package, the time series data
will appear in your database in a compact and efficient format (by
default as a view called `tv`), while at the same time simple to
process using any other tool, language, or framework because it is
just a table (or a view, rather). For a more detailed description of
how Tgres stores data see this
[article](https://grisha.org/blog/2017/01/21/storing-time-seris-in-postgresql-optimize-for-write/)

### Current Status

Feb 7 2018: This project is not actively maintained. You may find
quite a bit of time-series wisdom here, but there are probably still a
lot of bugs.

Jul 5 2017: See this [status update](https://grisha.org/blog/2017/07/04/tgres-status-july-2017/)

Jun 15 2017: Many big changes since March, most notably data point
versioning and instoduction of ds_state and rra_state tables which
contain frequently changed attributes as arrays, similar to the way
data points are stored eliminating the need to update ds and rra
tables, these are now essentially immutable. Ability to delete series
with NOTIFY to Tgres to purge it from the cache.

Mar 22 2017: Version 0.10.0b was tagged. This is our first beta (which
is more stable than alpha). Please try it out, and take a minute to
open an issue or even a PR if you see/fix any problems. Your feedback
is most appreciated!

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

You need a newer Go (1.7+) and PostgreSQL 9.5 or later. To get the
daemon compiled all you need is:

```
$ go get github.com/tgres/tgres
```

Now you should have a tgres binary in `$GOPATH/bin`.

There is also a Makefile which lets you build Tgres with `make` which
will use a slightly more elaborate command and the resulting tgres
binary will be able to report its build time and git revision, but
otherwise it's the same.

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

### For Developers

There is nothing specific you need to know. If you'd like to submit a
bug fix, or for anything else - use Github.

### Migrating Graphite Data

Included in cmd/whisper_import is a program that can copy whisper data
into Tgres, its command-line arguments are self-explanatory. You
should be able to start sending data to Tgres and then migrate your
Graphite data retroactively by running whisper_import to avoid gaps in
data. It's probably a good idea to test a small subset of series first,
migrations can be time consuming and resource-intensive.
