# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import time
from sys import version_info

from distributionstatistics import Statistics


# noinspection SpellCheckingInspection
class MicroBench(object):

    DEFAULT_ITERATIONS = 100
    DEFAULT_WARMUP = 15
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
        logger = logging.getLogger(__name__)

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
        logger.debug("warming up")
        self.repeat(warmup, self.__warmup)
        logger.debug("measuring time")
        self.repeat(measure, self.__iterations)
        logger.debug("mean time is %g seconds", stats.mean)
        return stats
