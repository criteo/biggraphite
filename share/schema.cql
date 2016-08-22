
CREATE KEYSPACE IF NOT EXISTS biggraphite_metadata WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '3'
} AND durable_writes = true;

CREATE KEYSPACE IF NOT EXISTS biggraphite WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '2'
} AND durable_writes = false;
