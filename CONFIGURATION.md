# How to configure BigGraphite

Settings are either set in Graphite/Carbon configuration (when embedded), via command line flags, or via environment variables.

## Generic

- ```BG_STORAGE_DIR```: Storage directory to use, currently mostly use to store the cache
- ```BG_DRIVER```: One of ```cassandra``` (for production) or ```memory``` (for debug)

## Cassandra Backend

This is the main backend for BigGraphite and the one that should be used in production.

- ```BG_CASSANDRA_KEYSPACE```: keyspace to use (default: ```biggraphite```)
- ```BG_CASSANDRA_CONTACT_POINTS```: Cassandra contact points (default: ```127.0.0.1```)
- ```BG_CASSANDRA_PORT```: port to use to contact Cassandra (default: ```9042```)
- ```BG_CASSANDRA_CONNECTIONS```: number of Cassandra workers to use (default: ```4```)
- ```BG_CASSANDRA_TIMEOUT```: default timeout for operations (default: ```None```)

## Memory Backend

The in-memory backend can be used during development to make debuging easier.

## Cache

In order to improve performances a metric metadata cache is used. This cache
reduces the number of round-trips between graphite/carbon and cassandra.

- ```BG_CACHE```: cache driver to use (memory or disk).
- ```BG_CACHE_SIZE```: size of the cache.
- ```BG_CACHE_TTL```: TTL of items in the cache
- ```BG_CACHE_SYNC```: when using disk cache, should writes be synchronous or not.