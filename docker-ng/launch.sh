set -x
set -e
docker run --network=biggraphite-network --rm -d --name cassandra cassandra:3.11.3
sleep 20
docker exec cassandra cqlsh -e "CREATE KEYSPACE IF NOT EXISTS biggraphite WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes = false;" localhost --cqlversion="3.4.4"
docker run -p 9200:9200 --rm --name elasticsearch -d --network=biggraphite-network docker.elastic.co/elasticsearch/elasticsearch:6.3.2
sleep 20
docker run -d --rm --name kibana -p 5601:5601 --network=biggraphite-network docker.elastic.co/kibana/kibana:6.3.2
docker run -d --rm --name aggregator-cache -p 2003:2003 --network=biggraphite-network -v $(pwd)/aggregator-cache/:/conf aggregator-cache
docker run -d --rm --name graphite-web -p 8080:8080 --network=biggraphite-network graphite-web
