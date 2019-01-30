The content of this folder provide means build and run docker images for testing biggraphite using docker-compose

There are six images:
- `bg-cassandra` contains an instance of cassandra used to run the biggraphite database,
- `bg-helper` is a container used to configure `bg-cassandra`; it will shutdown when its job is done,
- `bg-elasticsearch` contains an instance of elasticsearch to store metadata,
- `bg-kibana` is provided to check what is going on within the elasticsearch instance,
- `graphite-aggregator-cache` is used to write data, and
- `graphite-web` is used to read data.

All these images can be build by running the `build.sh` bash script. That script also setup some configuration files that are generated in folders (also created by the same script) that are being mounted on some container. In a nutshell, the folder named `volumes_foobar` is mounted on the machine `foobar`.

After the build, containers can be launched using `docker-compose up`. By default, the kibana image is not launched; to do so some lines must be uncommented in the `docker-compose.yml` file. Graphite web ui can be accessed on port 8080 and data can be written on port 2003 (e.g., ```echo "local.random.diceroll 4 `date +%s`" | nc localhost 2003```). If launched, kibana can be accessed on port 5601.

Note that at least 12GB of free memory may be needed.
