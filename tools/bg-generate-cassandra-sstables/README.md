# Generate Cassandra SSTables

Note: using bg-import-whisper directly without bulkimport is often faster.

```bash
$ mvn -Djava.library.path="${CASSANDRA_HOME}/lib/sigar-bin" test install
$ bg-import-whisper --driver cassandra --cassandra_bulkimport --loglevel DEBUG --process 1 --ignored_stages '11520*60s' -- /opt/graphite/storage/whisper/
$ mvn install  && mvn exec:java -Djava.library.path=$CASSANDRA_HOME/lib/sigar-bin -Dexec.mainClass="biggraphite.BgGenerateCassandraSSTables" -Dexec.args="biggraphite datapoints_720p_3600s_0 720*3600s_0.cql 720*3600s_0.csv"
$ sstableloader -d $(hostname -f) biggraphite/datapoints_720p_3600s
```

You can optionally use these helper scripts is you convert data from multiple hosts:

```bash
$ mkdir out
$ sh tools/concatenate-dumps.sh data/ out/
$ sh tools/generate-sstables.sh out/
$ for file in out/data/biggraphite/datapoints*; do sstableloader -d cstars04e01-par.storage.criteo.preprod $file/; done
