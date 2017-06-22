
### Tgres FAQ ###

Q. What is in the name?

A. It is T (which probably stands for "time") and "gres", which is a
   suffix used in a bunch of database names, including Postgres, which
   is similar to the root "gress" (latin: step) as in progress,
   egress, etc.

Q. Are all Graphite functions supported?

A. Not all of them, most of them. The ones that are very new, or hard
   to implement or seem obscure were omitted. The specific list is in
   [dsl/funcs.go](https://github.com/tgres/tgres/blob/master/dsl/funcs.go#L220).

   Some functions may work sligtly differently (e.g. the Holt-Winters
   ones use a different implementation), there may be slight
   incompatibilities, such as obscure keyword arguments not supported,
   etc.


Q. When will all Graphite functions be supported?

A. Probably never. Full Graphite compatibility was never a goal, it
   was just a way to prove the concept. I don't like the idea of
   piling up an ever-increasing list of functions. The longer term
   goal is to provide a new DSL which is simpler and more flexible.


Q. How performant is Tgres?

A. Unbelievably performant. See
   [here](https://grisha.org/blog/2017/02/28/tgres-load-testing-follow-up/)


Q. Is there a Grafana plug-in?

A. Yes: https://github.com/tgres/grafana-plugin

   You can also just specify Tgres as a Graphite data source and
   expect some functions not to work, sometimes this is easier than
   installing a plugin.

Q. What can it do?

A. Tgres can do everything Graphite and Statsd do. And because your
   data is in PostgreSQL, you can also do anything Postgres does,
   including, but not limited to SQL, replication, backups, etc.

   Tgres also supports clustering, though it's not yet fully baked.


Q. Why should I use it?

A. If you would like to have full SQL power at your disposal, if you
   like Go, if you want to try something fresh and new, if you're
   tired of running an arrangement of processes and prefer to have
   just a single binary and a simple config file.

   You can also use Tgres packages individually in your programs if
   you would like to provide time-series processing and reporting
   capabilities or for instrumenting your app.
