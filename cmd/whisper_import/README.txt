
This is a little utility to load whisper files into the Tgres
database. It is meant to be run from the box that has the whisper
files accessible via filesystem. It does not use the Tgres daemon and
communicates directly with the database.

To migrate your graphite data to tgres, the process is approximately
as follows:

1. Stand up an instanc of Tgres. We do not have any kind of a formula
   on what hardware specs should Tgres have relative to what is used
   by Graphite. We know it can be lesser hardware. In our largest
   migration we went from a 24CPU 64GB RAM SSD machine to two AWS
   instances, an m4.xlarge for the Tgres daemon and db.m4.2xlarge for
   the Postgers instance. On AWS higher IOPS for the db instance is
   better.

   Note that you will need TCP connectivity between the server where
   Graphite data is and the Postgres database. The migration tool
   communicates with the database directly, it does not involve the
   Tgres daemon in any way.

2. Consider the size and resolution of RRAs you will be storing. Keep
   in mind that database load is (somewhat counter-inuitively) a
   function of the number of RRAs and the resolution, *not* the
   incoming data points per second.

   For example, if you have an RRA at a 10 second resolution, it will
   write a data point to the database every 10 seconds regardless of
   how many points per second is received. Even if the data points
   arrive once per minute, there is still 6 points in the database to
   be filled each minute.

3. On the server where your Graphite data is, run the following
   command to create (only create, no data copied) the Data Sources:

   ./whisper_import \
       -dbconnect="host=DBHOST dbname=tgres user=tgres password=PASSWORD" \
       -stale-days=30 \
       -spec="10s:6h,1m:5d,10m:93d,1d:5y:1" \
       -whisper-dir=/data/graphite/whisper \
       -root=/data/graphite/whisper \
       -mode=create

   -spec is what controls the RRA configuration, it does not have to
   match what is in Graphite whisper files. It should, however, match
   what is in your Tgres config, though it's not an error if it does
   not. If you do not provide -spec, the step/span will be inferred
   from the whisper files, but we recommend you use -spec and give it
   some careful consideration.

   -whisper-dir is the root of where the whisper files are, while
   -root is a subdirectory of -whisper-dir which will be processed by
   the tool. Both need to be known to correctly construct the name,
   e.g. if whisper-dir is /foo/bar and root is /foo/bar/baz, then all
   series names will begin with "baz.".

   -stale-days is the number of days past which the whisper file is
   ignored.

   -mode=create is essential at this point in the process. You want to
   first create all the DSs and RRAs so that they have segment and
   bundle ids assigned to them. Once those are created, the tool can
   intelligently write datapoints for an entire step of a segment in
   one SQL operation.

   For our Graphite instance with over 300K series, this step took
   about an hour.

4. Start Tgres and start sending data to it. Let it run for a while
   (an hour, or a day), observe how it performs. Tgres automatically
   maintains many metrics about itself (names begin with "tgres.*"),
   the following is a quick description of the ones we find most
   interesting to gauge how it's holding up:

   serde.flush_channel.blocked is when the channel to the database
   flushers is full. When this happens the data stays in the cache,
   and Tgres will try to write it next time. If the flush_channel is
   blocked too much, the cache will keep growing and eventually Tgres
   will run out of memory or start dropping data points (if you have
   max-memory-bytes configured). It is fine for it to be above zero,
   but examine the trend. If it spikes occasionally, that's OK, but if
   it's just going up, you may need more workers (4 is default), and if
   that doesn't help, then your DB instance isn't keeping up.

   Another parameter that might be able to increase the write
   performance of the database is the segment width (pg-segment-width)
   along with -width paramter of whisper_import. This is how many
   series are stored in a single segment row, or how many data points
   can be written in one SQL operation. Setting it larger means writes
   are more efficient, but it also means that queries will be
   slower. The default is 200, meaning it takes 5 segments to store 1K
   RRAs. If you set it to 1000, then that becomes a single write. Note
   that this setting only matters when the Tgres tables are created,
   you need to drop all tables before changing it.

   receiver.datapoints.total: total incoming datapoints. Make sure it
   matches your expectation.

   receiver.datapoints.dropped: how many points were dropped because
   some resource was maxed out. Ideally it should be 0 all of the
   time.

   receiver.queue_len: the size of the receiver queue. When it reaches
   max-receiver-queue-size, data points are dropped.

   serde.flush_*.sql_ops: the number of SQL operations per second. As
   you increase pg-segment-width, this number should go down.

5. Once you are satisfied with Tgres performance under full load, you
   can start back-filling old Graphite data by running the almost
   exact same command with -mode=populate parameter.

   ./whisper_import \
       -dbconnect="host=DBHOST dbname=tgres user=tgres password=PASSWORD" \
       -stale-days=30 \
       -whisper-dir=/data/graphite/whisper \
       -root=/data/graphite/whisper \
       -mode=populate

    This step in our case took about 12 hours, so be patient.

    The tool keeps track of the segments that have been processed via
    the whisper_import_status table in the db. If you quit and
    restart, it will not process already processed segments. If you
    want to start population from scratch, remember to truncate or
    drop the whisper_import_status table first.
