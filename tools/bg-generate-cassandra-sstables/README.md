# Generate Cassandra SSTables

```bash
$ mvn -Djava.library.path="${CASSANDRA_HOME}/lib/sigar-bin" test install
$ bg-import-whisper --driver cassandra --cassandra_bulkimport --loglevel DEBUG --process 1 --ignored_stages '11520*60s' -- /opt/graphite/storage/whisper/
$ mvn install  && mvn exec:java -Djava.library.path=$CASSANDRA_HOME/lib/sigar-bin -Dexec.mainClass="biggraphite.BgGenerateCassandraSSTables" -Dexec.args="biggraphite datapoints_720p_3600s 720*3600s.cql 720*3600s.csv"
```
