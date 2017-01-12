import time
from biggraphite import test_utils as bg_test_utils
from biggraphite import utils as bg_utils
from biggraphite.drivers import cassandra as bg_cassandra
from biggraphite import accessor as bg_accessor
import random
import string
import os
import pytest

if not bool(os.getenv("CASSANDRA_HOME")):
    raise Exception("You must specify a CASSANDRA_HOME to run benchmarks")

class Bencher(bg_test_utils.TestCaseWithAccessor):
    contact_points = os.getenv("CASSANDRA_CONTACTS_POINTS")
    port = os.getenv("CASSANDRA_PORT")
    reactor = None


    def __init__(self, reactor=None, *args, **kwargs):
        super(bg_test_utils.TestCaseWithAccessor, self).__init__(*args, **kwargs)
        self.reactor = reactor

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


def make_metric(benchmark, reactor):
    with Bencher(reactor) as tc:
        ac = tc.get_accessor()
        digits = "".join([random.choice(string.digits+string.letters) for i in xrange(10)] )
        benchmark.pedantic(ac.make_metric, args=(digits, {'retention': ""}), iterations=1000, rounds=100)

@pytest.mark.benchmark(group="make_metric")
def test_make_metrics_libev(benchmark):
    make_metric(benchmark, "LIBEV")

@pytest.mark.benchmark(group="make_metric")
def test_make_metrics_twisted(benchmark):
    make_metric(benchmark, "TWISTED")




def has_metric(benchmark, reactor):
    with Bencher(reactor) as tc:
        ac = tc.get_accessor()
        benchmark.pedantic(ac.has_metric, args=("toto",), iterations=1000, rounds=100)

@pytest.mark.benchmark(group="has_metric")
def test_has_metric_libev(benchmark):
    has_metric(benchmark, "LIBEV")

@pytest.mark.benchmark(group="has_metric")
def test_has_metric_twisted(benchmark):
    has_metric(benchmark, "TWISTED")





def get_metrics(benchmark, reactor):
    with Bencher(reactor) as tc:
        ac = tc.get_accessor()
        benchmark.pedantic(ac.has_metric, args=("toto",), iterations=1000, rounds=100)

@pytest.mark.benchmark(group="get_metric")
def test_get_metric_libev(benchmark):
    get_metrics(benchmark, "LIBEV")

@pytest.mark.benchmark(group="get_metric")
def test_get_metric_twisted(benchmark):
    get_metrics(benchmark, "TWISTED")




@pytest.mark.benchmark(group="glob")
def test_glob_dir_name(benchmark):
    with Bencher() as tc:
        ac = tc.get_accessor()
        benchmark.pedantic(ac.glob_directory_names, args=("toto.tutu.*.tata.*.titi.*.chipiron",), iterations=1000, rounds=100)

@pytest.mark.benchmark(group="glob")
def test_glob_metric_name(benchmark):
    with Bencher() as tc:
        ac = tc.get_accessor()
        benchmark.pedantic(ac.glob_metric_names, args=("toto.tutu.*.tata.*.titi.*.chipiron",), iterations=1000, rounds=100)




def insert_points(benchmark, reactor):
    with Bencher(reactor) as tc:
        ac = tc.get_accessor()
        digits = "".join([random.choice(string.digits+string.letters) for i in xrange(10)] )
        metadata = bg_accessor.MetricMetadata()
        metric = ac.make_metric(digits, metadata)
        ac.create_metric(metric)

        points = [(12121212, 5050)] * 5000
        benchmark.pedantic(ac.insert_points_async, args=(metric, points), iterations=1000, rounds=100)

@pytest.mark.benchmark(group="insert_points")
def test_insert_metrics_libev(benchmark):
    insert_points(benchmark, "LIBEV")

@pytest.mark.benchmark(group="insert_points")
def test_insert_metrics_twisted(benchmark):
    insert_points(benchmark, "TWISTED")




def get_points(benchmark, reactor):
    with Bencher(reactor) as tc:
        ac = tc.get_accessor()
        digits = "".join([random.choice(string.digits+string.letters) for i in xrange(10)] )
        metadata = bg_accessor.MetricMetadata()
        metric = ac.make_metric(digits, metadata)
        ac.create_metric(metric)

        points = [(12121212, 5050)] * 5000
        ac.insert_points_async(metric, points)
        benchmark.pedantic(ac.fetch_points, args=(metric, 0, 36000, bg_accessor.Stage(60,60)), iterations=1000, rounds=1000)

@pytest.mark.benchmark(group="get_points")
def test_get_points_twisted(benchmark):
    get_points(benchmark, "TWISTED")

@pytest.mark.benchmark(group="get_points")
def test_get_points_libev(benchmark):
    get_points(benchmark, "LIBEV")
