import logging
import time
from sys import version_info

from distributionstatistics import Statistics


# noinspection SpellCheckingInspection
class MicroBench(object):

    DEFAULT_ITERATIONS=100
    DEFAULT_WARMUP=15
    VERSION = version_info[0]
    timer = time.time

    def __init__(self, warmup=DEFAULT_WARMUP, iterations=DEFAULT_ITERATIONS):
        self.__warmup = warmup
        self.__iterations = iterations
        if self.VERSION > 2:
            self.timer = time.monotonic

    @staticmethod
    def repeat(what, count):
        for i in range(count):
            what()

    def bench_simple(self, what):
        # Warmup
        logger = logging.getLogger(__name__)
        logger.debug("warming up")
        self.repeat(what, self.__warmup)
        stats = Statistics()

        def measure():
            start = self.timer()
            what()
            end = self.timer()
            stats.add(end - start)

        logger.debug("measuring time")
        self.repeat(measure, self.__iterations)
        logger.debug("mean time is %g seconds", stats.mean)
        return stats

    def bench(self, pre=None, what=None, post=None):

        def warmup():
            if pre:
                pre()
            what()
            if post:
                post()

        def measure():
            if pre:
                pre()
            start = self.timer()
            what()
            end = self.timer()
            stats.add(end - start)
            if post:
                post()

        stats = Statistics()
        self.repeat(warmup, self.__warmup)
        self.repeat(measure, self.__iterations)
        return stats
