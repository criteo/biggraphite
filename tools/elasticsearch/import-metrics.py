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
import urllib.parse

INDEX_PREFIX = "biggraphite_"

INDEX_SCHEMA_METRICS_PATH = os.path.join(
    os.path.dirname(__file__),
    '../../biggraphite/drivers/elasticsearch_schema.json'
)


def uuid_to_datetime(u):
    try:
        if u:
            return cassandra.util.datetime_from_uuid1(uuid.UUID(u))
    except Exception as e:
        print("%s: %s" % (u, e))
        return None
    return None


def document_base(name):
    """Build base document with components."""
    data = {"depth": name.count("."), "name": name}

    for i, component in enumerate(name.split(".")):
        data["p%d" % i] = component

    return data


def document(metric, config, created_on, updated_on, read_on, uid, labels):
    """Creates a document."""
    try:
        config = config.replace("'", '"')
        config = json.loads(config)
    except ValueError:
        config = {}

    data = document_base(metric)
    data.update(
        {
            "uuid": uid,
            "created_on": uuid_to_datetime(created_on),
            "updated_on": uuid_to_datetime(updated_on),
            "read_on": uuid_to_datetime(read_on),
            "config": config,
        }
    )
    labels = labels or {}
    for k, v in labels.items():
        data["label_%s" % k] = v
    print(data)
    return data


def read_metrics(filename):
    """Read metrics from the filename."""
    fp = open(filename)
    reader = csv.reader(fp)
    for row in reader:
        yield row


def guess_labels(metric, prefix):
    """Consider prefix as metric name and the rest as label0.value0.label1.value1"""
    labels = {}
    metric = metric.replace(prefix, "")

    # Split into components
    components = metric.split(".")
    components = list(filter(None, components))

    # First part is metric name
    metric = prefix + "." + components[0]
    components = components[1:]

    for i in range(0, len(components) - 1, 2):
        # url decode because people will often have urlencoded the value.
        labels[components[i]] = urllib.parse.unquote(components[i + 1])
    return metric, labels


def create(opts, es, row):
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
        # Detect labelled metrics
        if directory in opts.prefix_labelled:
            metric, labels = guess_labels(metric, directory)
            row[0] = metric
            create_metric(es, row, labels=labels)


def create_metric(es, row, labels=None):
    """Create a metric."""
    metric, config, created_on, uid, read_on, updated_on = row
    print(metric, labels)

    if labels:
        # Make sure there are no collisions for labelled metrics
        doc_id = chr((ord(uid[0]) + 1) % 254) + uid[1:]
    else:
        doc_id = uid

    es.create(
        index=INDEX_PREFIX + "metrics",
        doc_type="metric",
        id=doc_id,
        body=document(metric, config, created_on, updated_on, read_on, uid, labels),
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
    parser = argparse.ArgumentParser(description="Import data to ElasticSearch.")
    parser.add_argument("--max_workers", default=75, type=int)
    parser.add_argument("--username", default=os.getenv("ES_USERNAME"))
    parser.add_argument("--password", default=os.getenv("ES_PASSWORD"))
    parser.add_argument("--cluster", default="127.0.0.1")
    parser.add_argument("--sniff", action="store_true", default=False)
    parser.add_argument("--port", default=9002, type=int)
    parser.add_argument("--cleanup", action="store_true", default=False)
    # Prefix of metrics that will be stored with "labels"
    parser.add_argument("--prefix_labelled", type=str)
    parser.add_argument("--input", required=True)
    return parser.parse_args(args[1:])


def main():
    """Import .csv data from a metrics_metadata dump."""
    opts = parse_opts(sys.argv)
    if opts.prefix_labelled:
        opts.prefix_labelled = set(opts.prefix_labelled.split(","))
    max_workers = opts.max_workers
    sem = threading.Semaphore(max_workers * 2)

    es = elasticsearch.Elasticsearch(
        [opts.cluster],
        port=opts.port,
        http_auth=(opts.username, opts.password),
        sniff_on_start=opts.sniff,
        sniff_on_connection_fail=opts.sniff,
    )
    print("Connected:", es.info())
    if opts.cleanup:
        es.indices.delete(index=INDEX_PREFIX + "metrics", ignore=[400, 404])
    if not es.indices.exists(INDEX_PREFIX + "metrics"):
        with open(INDEX_SCHEMA_METRICS_PATH, "r") as es_schema_file:
            json_schema = json.load(es_schema_file)
            es.indices.create(
                index=INDEX_PREFIX + "metrics", body=json_schema, ignore=409
            )

    rows = read_metrics(opts.input)
    executor = futures.ThreadPoolExecutor(max_workers=max_workers)
    for row in rows:
        # We use the semaphore to avoid reading *all* the file.
        sem.acquire()
        future = executor.submit(create, opts, es, row)
        future.add_done_callback(lambda f: callback(f, sem))
    executor.shutdown()

    print(es.cluster.health(wait_for_status="yellow", request_timeout=1))
    # print(es.search(index=INDEX))


if __name__ == "__main__":
    main()
