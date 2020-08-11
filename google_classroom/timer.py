import time
import logging


def _timer_message(func, elapsed):
    """Private function for formatting log message so the correct
    unit is displayed based on the elapsed seconds"""
    unit = "seconds"
    if elapsed >= 60:
        elapsed = round(elapsed / 60, 2)
        unit = "minutes"

    return f"{func.__qualname__} completed in {elapsed} {unit}."


def elapsed(func):
    """Decorator function used to compute and report the time a
    function takes to complete. Add to a function as: @elapsed"""

    def wrapper(*args, **kwargs):
        start = time.time()
        results = func(*args, **kwargs)
        end = time.time()
        elapsed = round(end - start, 2)
        logging.info(_timer_message(func, elapsed))
        return results

    return wrapper
