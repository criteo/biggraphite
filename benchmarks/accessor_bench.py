import time
from biggraphite import test_utils as bg_test_utils
from biggraphite import utils as bg_utils
from biggraphite.drivers import cassandra as bg_cassandra
from biggraphite import accessor as bg_accessor
import random
import string
import os
import pytest

if bool(os.getenv("CASSANDRA_HOME")):
    BASE_CLASS = bg_test_utils.TestCaseWithAccessor
    ROUNDS = 100
    ROUNDS_LARGE = 10
    ITERATIONS = 10
else:
    BASE_CLASS = bg_test_utils.TestCaseWithFakeAccessor
    ROUNDS = 1000
    ROUNDS_LARGE = 10
    ITERATIONS = 100

CASSANDRA_REACTOR = os.getenv("CASSANDRA_REACTOR")


class Bencher(BASE_CLASS):
    port = os.getenv("CASSANDRA_PORT")
    reactor = None

    def __init__(self, reactor=None, *args, **kwargs):
        super(BASE_CLASS, self).__init__(*args, **kwargs)
        self.reactor = reactor or CASSANDRA_REACTOR

    def __enter__(self):
        bg_cassandra.REACTOR_TO_USE = self.reactor
        self.setUpClass()
        self.setUp()
        return self

    def __exit__(self, *args, **kwargs):
        bg_cassandra.REACTOR_TO_USE = None
        self.tearDownClass()

    def runTest(self):
        return True


def _gen_metric(accessor):
    digits = "".join(
        [random.choice(string.digits+string.letters) for i in xrange(10)])
    metadata = bg_accessor.MetricMetadata()
    return accessor.make_metric(digits, metadata)


def make_metric(benchmark):
    with Bencher() as tc:
        ac = tc.get_accessor()
        digits = "".join(
            [random.choice(string.digits+string.letters) for i in xrange(10)] )
        benchmark.pedantic(
            ac.make_metric, args=(digits, {'retention': ""}),
            iterations=ITERATIONS, rounds=ROUNDS)

@pytest.mark.benchmark(group="metadata")
def test_make_metrics(benchmark):
    make_metric(benchmark)


def has_metric(benchmark):
    with Bencher() as tc:
        ac = tc.get_accessor()
        benchmark.pedantic(
            ac.has_metric, args=("toto",),
            iterations=ITERATIONS, rounds=ROUNDS)

@pytest.mark.benchmark(group="metadata")
def test_has_metric(benchmark):
    has_metric(benchmark)


def get_metrics(benchmark):
    with Bencher() as tc:
        ac = tc.get_accessor()
        benchmark.pedantic(
            ac.has_metric, args=("toto",),
            iterations=ITERATIONS, rounds=ROUNDS)


@pytest.mark.benchmark(group="metadata")
def test_get_metric(benchmark):
    get_metrics(benchmark)


@pytest.mark.benchmark(group="metadata")
def test_glob_dir_name(benchmark):
    with Bencher() as tc:
        ac = tc.get_accessor()
        benchmark.pedantic(
            ac.glob_directory_names, args=("toto.tutu.*.tata.*.titi.*.chipiron",),
            iterations=ITERATIONS, rounds=ROUNDS)


@pytest.mark.benchmark(group="metadata")
def test_glob_metric_name(benchmark):
    with Bencher() as tc:
        ac = tc.get_accessor()
        benchmark.pedantic(
            ac.glob_metric_names, args=("toto.tutu.*.tata.*.titi.*.chipiron",),
            iterations=ITERATIONS, rounds=ROUNDS)


def insert_points_async(benchmark):
    with Bencher() as tc:
        ac = tc.get_accessor()
        now = int(time.time())

        metrics = [_gen_metric(ac) for x in xrange(0,100)]
        for m in metrics:
            ac.create_metric(m)

        def run(metrics):
            for m in metrics:
                ac.insert_points_async(m, [(now, 5050)])

        benchmark.pedantic(
            run, args=(metrics,),
            iterations=ITERATIONS, rounds=ROUNDS_LARGE)


@pytest.mark.benchmark(group="data")
def test_insert_metrics_async(benchmark):
    insert_points_async(benchmark)


def insert_points_sync(benchmark):
    with Bencher() as tc:
        ac = tc.get_accessor()
        now = int(time.time())

        metrics = [_gen_metric(ac) for x in xrange(0,100)]
        for m in metrics:
            ac.create_metric(m)

        def run(metrics):
            for m in metrics:
                ac.insert_points(m, [(now, 5050)])

        benchmark.pedantic(
            run, args=(metrics,),
            iterations=ITERATIONS, rounds=ROUNDS_LARGE)


@pytest.mark.benchmark(group="data")
def test_insert_metrics_sync(benchmark):
    insert_points_sync(benchmark)


def get_points(benchmark):
    with Bencher() as tc:
        ac = tc.get_accessor()
        now = int(time.time())

        metrics = [_gen_metric(ac) for x in xrange(0,100)]
        for m in metrics:
            ac.create_metric(m)
            ac.insert_points_async(m, [(now, 5050)])

        def run(metrics):
            for m in metrics:
                ac.fetch_points(m, 0, 36000, bg_accessor.Stage(60,60))

        benchmark.pedantic(
            run, args=(metrics,),
            iterations=ITERATIONS, rounds=ROUNDS_LARGE)


@pytest.mark.benchmark(group="data")
def test_get_points(benchmark):
    get_points(benchmark)
