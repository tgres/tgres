
Timeriver is a "Time Series Database" which uses PostgreSQL for
storage, written in Go.

It is presently work-in-progress, still under development, things in
flux.

Timeriver can receive data using Graphite Text, UDP and Pickle
protocols, as well as Statsd (counters, gauges and timers). It
supports enough of a Graphite HTTP API to be usable with
Grafana. Timeriver implements the majority of the Graphite functions.

Timeriver places emphasis on accuracy and stores data similarly to
RRDTool, using a weighted average of points that arrive within the
same step rather than discarding them.

Internally a datapoint refers to a time interval rather than a point
in time. This is a more accurate and flexible representation which
should also allow correct updates of past data (NIY).

Timeriver stores data in a round-robin database split over a number of
PostgreSQL rows of arrays. This means that a series occupies a
constant number of rows in a database and can be updated in single row
chunks for IO efficiency. The number of datapoints per row is
configurable for each series individually. Timeriver provides two
PostgreSQL views to make time series appear as a regular table.

Timeriver caches datapoints in memory until a certain number of
datapoints has accumulated or a certain time period has been reached
to reduce IO.

Timeriver relies on goroutines heavily and will take advantage of
multiple CPU's on SMP architectures.

One of the main goals of Timeriver is to address the data isolation
problem with most of the TS databases out there. Your TS data can reside
in the same PostgreSQL database as other data and can be easily
joined, analyzed, etc.

Other (future) goals/notes:

It has to be simple. There shouldn't be many daemons/components - just
this one process to receive and serve the data and the database server.

Horizontal scaling is an interesting problem, a consensus algorithm
such as Raft may or may not be the right answer, we recognize that
this is a tough call and that bridge, if we ever get there, will be
crossed very carefully. Ideally scaling should happen in tandem with
the best practices on the PostgreSQL side, which appears to be a
moving target at this point.

Presently Timeriver does not store the original datapoints as they
come in, data is always aggregated. This may change.

Graphite API was used only as a starting point, to be able to prove the
concept and test against existing tools (mostly Grafana). Longer term
there may be more interesting methods of exchanging data, e.g. gob or
Redis protocol, etc.
