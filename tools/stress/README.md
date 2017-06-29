# Cassandra Benchmarks

## Run

```
cassandra-stress user profile=biggraphite.yaml truncate=once n=100000 'ops(insert=1)' cl=ONE -rate threads=24 \
  -graph file=biggraphite-results.html \
  -node 127.0.0.1 -port native=10042
```