
This is a little utility to load whisper files into the database in
Tgres format. It is meant to be run from the box that has the
whisper files accessible via filesystem.

To migrate your graphite to tgres, the process is approximately as
follows:

1. Stand up an instance of Tgres.

2. Make sure that the config (step and span) matches the RRAs that you
   have in Graphite. When this tool is running, it will use the RRA
   span/step that it sees in the Whisper files. If a different
   configuration already exists in the DB, then what is in the database
   will take precedence. Therefore if the configs do not match, you
   will have varying RRAs depending on how the DS came about. This is
   not a problem, except for the perfectionists amongst us. You can also
   specify a spec using the -spec option to whisper_import.

3. Make sure that you have max-receiver-queue-size set if you have a
   lot of data coming in. This will prevent Tgres from running out of
   memory if it has to create a lot of RRAs (which is relatively slow).

4. Ideally you're using carbon-relay-ng and you can just send some
   data to Tgres and see how it holds up and that everything is
   working. Once you're happy with that, and Tgres is receiving data,
   you can run this tool to back-fill old data.

5. Run whisper_import on the machine that has the whisper files. It
   will assemble a whole segment (i.e. 200 complete series with all
   their RRAs) in memory, then start saving them to the DB while
   starting on the next segment, which is somewhat memory
   intensive. Whisper_import talks to the database directly, it
   doesn't need Tgres to be running. The import process is time
   consuming, be patient.

The command might look like this:

./whisper_import -whisper-dir="/data/graphite/whisper" \
                 -root="/data/graphite/whisper/stats/" \
                 -dbconnect="host=HOSTNAME dbname=DBNAME user=USER password=PASS"

In the above example the name of the stat is the "difference" between
whisperDir and root, i.e. "stats." + whatever follows. You can also
specify a prefix, and in general just look at the code and tweak it to
your satisfaction. If you find yourself adding something useful, pull
requests are welcome.
