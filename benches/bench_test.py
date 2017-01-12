import time

def something(duration=0.000001):
    """
    Function that needs some serious benchmarking.
    """
    time.sleep(duration)
    # You may return anything you want, like the result of a computation
    return 123

def test_example(benchmark):
    # benchmark something

    # Simple bench
    result = benchmark(something)
    # result = benchmark.pedantic(something, iterations=10, rounds=100)
    # Extra code, to verify that the run completed correctly.
    # Sometimes you may want to check the result, fast functions
    # are no good if they return incorrect results :-)
    assert result == 123
