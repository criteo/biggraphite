import time
from biggraphite import test_utils as bg_test_utils
from biggraphite import utils as bg_utils
from biggraphite.drivers import cassandra as bg_cassandra
import random
import string
import os
import pytest

if not bool(os.getenv("CASSANDRA_HOME")):
    raise Exception("You must specify a CASSANDRA_HOME to run benchmarks")

class Bencher(bg_test_utils.TestCaseWithAccessor):
    contact_points = os.getenv("CASSANDRA_CONTACTS_POINTS")
    port = os.getenv("CASSANDRA_PORT")

    def __enter__(self):
        self.setUpClass()
        self.setUp()
        return self

    def __exit__(self, *args, **kwargs):
        self.tearDownClass()

    def runTest(self):
        return True


def test_make_metrics(benchmark):
    with Bencher() as tc:
        ac = tc.get_accessor()
        digits = "".join([random.choice(string.digits+string.letters) for i in xrange(10)] )
        benchmark.pedantic(ac.make_metric, args=(digits, {'retention': ""}), iterations=1000, rounds=100)

def test_has_metric(benchmark):
    with Bencher() as tc:
        ac = tc.get_accessor()
        benchmark.pedantic(ac.has_metric, args=("toto",), iterations=1000, rounds=100)

def test_get_metric(benchmark):
    with Bencher() as tc:
        ac = tc.get_accessor()
        benchmark.pedantic(ac.has_metric, args=("toto",), iterations=1000, rounds=100)

def test_glob_dir_name(benchmark):
    with Bencher() as tc:
        ac = tc.get_accessor()
        benchmark.pedantic(ac.glob_directory_names, args=("toto.tutu.*.tata.*.titi.*.chipiron",), iterations=1000, rounds=100)

def test_glob_metric_name(benchmark):
    with Bencher() as tc:
        ac = tc.get_accessor()
        benchmark.pedantic(ac.glob_metric_names, args=("toto.tutu.*.tata.*.titi.*.chipiron",), iterations=1000, rounds=100)
