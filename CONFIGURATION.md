# How to configure BigGraphite

Settings are either set in Graphite/Carbon configuration (when embedded) or
via command line flags or environment variables.

## Generic

- ```STORAGE_DIR```: Storage directory to use, currently mostly use to store
the cache.
- ```BG_DRIVER```: One of ```cassandra``` (for produciton) or ```memory``` (for debug).

## Cassandra Backend

This is the main backend for BigGraphite and the one that should be used in
production.

- ```BG_CASSANDRA_KEYSPACE```: keyspace to use (default: ```biggraphite```).
- ```BG_CASSANDRA_CONTACT_POINTS```: Cassandra contact points (default: ```127.0.0.1```).
- ```BG_CASSANDRA_PORT```: port to use to contact Cassandra (default: ```9042```).
- ```BG_CASSANDRA_CONNECTIONS```: number of Cassandra workers to use (default: ```4```).
- ```BG_CASSANDRA_TIMEOUT```: default timeout for operations (default: ```None```).

## Memory Backend

The memory backend can be used during development to make debubging easier.

