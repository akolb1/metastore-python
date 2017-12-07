from __future__ import print_function

import csv
import logging
from math import sqrt

import re

from microbench import MicroBench


class BenchSuite(object):
    __suite = dict()
    __result = dict()
    # __benchmarks keeps track of the order in which tests are added to preserve it for runs
    __benchmarks = []

    def __init__(self, bench=None, scale=1, sanitize=False):
        self.__scale = scale
        self.__sanitize = sanitize
        if not bench:
            bench = MicroBench()
        self.__bench = bench
        self.logger = logging.getLogger(__name__)

    def add(self, name, test):
        self.__suite[name] = test
        self.__benchmarks.append(name)
        return self

    def list(self, filters=None):
        """
        Return list of benchmarks matching filters

        :param filters:
        :type filters: list[str]
        :return: List of matching benchmarks
        :rtype: list[str]
        """

        def matches(word, filter_list):
            """
            :param word: word to check
            :type word: str
            :type filter_list: list[str]

            :param filter_list: list of regexps
            :return: True iff word matches any of the filters using regexp match
            """
            for f in filter_list:
                if re.search(f, word):
                    return True
            return False

        if not filters:
            return self.__benchmarks
        return [n for n in self.__benchmarks if matches(n, filters)]

    def run(self, filters=None):
        for name in self.list(filters):
            self.logger.debug('Running benchmark "%s"', name)
            b = self.__suite[name]
            result = b(self.__bench)
            self.__result[name] = result if not self.__sanitize else result.sanitize()

    @property
    def result(self):
        return self.__result

    def _min_mean(self):
        """
        :return: Return minimum Mean value across all suits
        :rtype: float
        """
        return min([data.mean for data in self.__result.values()])

    def print(self, file):
        file.write('{:30s}{:8s} {:8s} {:8s} {:8s} {:8s} {:8s}\n'.format('Name', 'AMean',
                                                                        'Mean', 'Med', 'Min', 'Max', 'Stdev%'))
        min_val = self._min_mean()
        for name in sorted(self.__result.keys()):
            result = self.__result[name]
            mean = result.mean
            file.write('{:30s}{:<8.3g} {:<8.3g} {:<8.3g} {:<8.3g} {:<8.3g} {:<8.3g}\n'
                       .format(name,
                               (mean - min_val) * self.__scale,
                               mean * self.__scale,
                               result.median * self.__scale,
                               result.min * self.__scale,
                               result.max * self.__scale,
                               result.stdev * 100 / mean))

    def print_csv(self, name, delimiter='\t'):
        if isinstance(name, str):
            with open(name, 'w', newline='') as f:
                self._print_csv(f, delimiter)
        else:
            self._print_csv(name, delimiter)

    def _print_csv(self, file, delimiter):
        min_val = self._min_mean()
        writer = csv.writer(file, delimiter=delimiter, quotechar='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['Name', 'AMean', 'Mean', 'Med', 'Min', 'Max', 'Stdev%'])
        for name in sorted(self.__result.keys()):
            result = self.__result[name]
            mean = result.mean
            values = [
                '{:g}'.format((mean - min_val) * self.__scale),
                '{:g}'.format(mean * self.__scale),
                '{:g}'.format(result.median * self.__scale),
                '{:g}'.format(result.min * self.__scale),
                '{:g}'.format(result.max * self.__scale),
                '{:g}'.format(result.stdev) * 100 / mean,
            ]
            writer.writerow([name] + values)
