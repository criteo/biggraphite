"""Import data to elasticsearch."""

from concurrent import futures
import argparse
import csv
import elasticsearch
import cassandra.util
import json
import os
import sys
import threading
import uuid

UUID_NAMESPACE = uuid.UUID('{00000000-1111-2222-3333-444444444444}')

DIRECTORIES = set()
INDEX_PREFIX = 'biggraphite_'
INDEX_BODY_METRICS = {
    "settings": {
        "index": {
            "number_of_shards": 3,
            "number_of_replicas": 1,
        },
    },
    "mappings": {
        "metric": {
            "properties": {
                "length": {"type": "long"},
                "created_on": {"type": "date"},
                "read_on": {"type": "date"},
                "updated_on": {"type": "date"},
                "name": {
                    "type": "text",
                    "index": "not_analyzed",
                },
                "uuid": {
                    "type": "text",
                    "index": "not_analyzed",
                },
                # TODO: Maybe make that non-analyzed
                "config": {
                    "type": "object",
                    "index": "no",
                },
            },
        },
    },
}
INDEX_BODY_DIRECTORIES = {
    "settings": {
        "index": {
            "number_of_shards": 3,
            "number_of_replicas": 1,
        },
    },
    "mappings": {
        "directory": {
            "properties": {
                "length": {"type": "long"},
                "name": {
                    "type": "text",
                    "index": "not_analyzed",
                },
            },
        },
    },
}

for i in range(64):
    INDEX_BODY_METRICS["mappings"]["metric"]["properties"]["p%d" % i] = {
        "type": "text",
        "index": "not_analyzed",
    }
    INDEX_BODY_DIRECTORIES["mappings"]["directory"]["properties"]["p%d" % i] = {
        "type": "text",
        "index": "not_analyzed",
    }

def uuid_to_datetime(u):
    try:
        if u:
            return cassandra.util.datetime_from_uuid1(uuid.UUID(u))
    except Exception as e:
        print("%s: %s" % (u, e))
        return None
    return None


def document_base(name):
    data = {
        "length": name.count("."),
        "name": name,
    }

    for i, component in enumerate(name.split(".")):
        data["p%d" % i] = component

    return data


def document(metric, config, created_on, updated_on, read_on, uid):
    """Creates a document."""
    try:
        config = config.replace("'", '"')
        config = json.loads(config)
    except ValueError:
        config = {}

    data = document_base(metric)
    data.update({
        "uuid": uid,
        "created_on": uuid_to_datetime(created_on),
        "updated_on": uuid_to_datetime(updated_on),
        "read_on": uuid_to_datetime(read_on),
        "config": config,
    })
    return data


def read_metrics(filename):
    """Read metrics from the filename."""
    fp = open(filename)
    reader = csv.reader(fp)
    for row in reader:
        yield row


def create(es, row):
    """Creates a metric."""
    metric = row[0]
    create_metric(es, row)

    # Create directories
    components = metric.split(".")
    components = list(filter(None, components))
    path = []
    for part in components[:-1]:
        path.append(part)
        directory = ".".join(path)
        if directory not in DIRECTORIES:
            DIRECTORIES.add(directory)
            create_directory(es, directory)


def create_metric(es, row):
    metric, config, created_on, uid, read_on, updated_on = row
    print(metric)
    es.create(
        index=INDEX_PREFIX + "metrics",
        doc_type="metric",
        id=uid,
        body=document(metric, config, created_on, updated_on, read_on, uid),
        ignore=409,
    )


def create_directory(es, directory):
    print(directory)
    es.create(
        index=INDEX_PREFIX + "directories",
        doc_type="directory",
        id=directory,
        body=document_base(directory),
        ignore=409,
    )


def callback(future, sem):
    """Consume the future."""
    sem.release()
    try:
        future.result()
    except Exception as e:
        print(e)


def parse_opts(args):
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Import data to ElasticSearch.')
    parser.add_argument('--max_workers', default=75, type=int)
    parser.add_argument('--username', default=os.getenv('ES_USERNAME'))
    parser.add_argument('--password', default=os.getenv('ES_PASSWORD'))
    parser.add_argument('--cluster', default='127.0.0.1')
    parser.add_argument('--sniff', action='store_true', default=False)
    parser.add_argument('--port', default=9002, type=int)
    parser.add_argument('--cleanup', action='store_true', default=False)
    parser.add_argument('--input', required=True)
    return parser.parse_args(args[1:])


def main():
    """Import .csv data from a metrics_metadata dump."""
    opts = parse_opts(sys.argv)
    max_workers = opts.max_workers
    sem = threading.Semaphore(max_workers * 2)

    es = elasticsearch.Elasticsearch(
        [opts.cluster],
        port=opts.port,
        http_auth=(opts.username, opts.password),
        sniff_on_start=opts.sniff,
        sniff_on_connection_fail=opts.sniff,
    )
    print('Connected:', es.info())
    if opts.cleanup:
        es.indices.delete(index=INDEX_PREFIX + "metrics", ignore=[400, 404])
        es.indices.delete(index=INDEX_PREFIX + "directories", ignore=[400, 404])
    es.indices.create(
        index=INDEX_PREFIX + "metrics",
        body=INDEX_BODY_METRICS,
        ignore=400
    )
    es.indices.create(
        index=INDEX_PREFIX + "directories",
        body=INDEX_BODY_DIRECTORIES,
        ignore=400
    )

    # TODO: Also create directories.
    rows = read_metrics(opts.input)
    executor = futures.ThreadPoolExecutor(max_workers=max_workers)
    for row in rows:
        # We use the semaphore to avoid reading *all* the file.
        sem.acquire()
        future = executor.submit(create, es, row)
        future.add_done_callback(lambda f: callback(f, sem))
    executor.shutdown()

    print(es.cluster.health(wait_for_status='yellow', request_timeout=1))
    # print(es.search(index=INDEX))


if __name__ == '__main__':
    main()
