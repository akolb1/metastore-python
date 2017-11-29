from __future__ import print_function

import csv
import logging
from math import sqrt

from microbench import MicroBench


class BenchSuite(object):
    __suite = dict()
    __result = dict()

    def __init__(self, bench=None, scale=1, sanitize=False):
        self.__scale = scale
        self.__sanitize = sanitize
        if not bench:
            bench = MicroBench()
        self.__bench = bench

    def add(self, name, test):
        self.__suite[name] = test

    def run(self):
        logger = logging.getLogger(__name__)
        for name, b in self.__suite.items():
            logger.debug('Running benchmark "%s"', name)
            result = b(self.__bench)
            self.__result[name] = result if not self.__sanitize else result.sanitize()

    @property
    def result(self):
        return self.__result

    def print(self, file):
        file.write('{:20s}{:6s}{:6s}{:6s}{:6s}{:6s}{:6s}\n'.format('Name', 'Mean', 'Med', 'Min', 'Max', 'Stdev', 'Var'))
        for name in sorted(self.__result.keys()):
            result = self.__result[name]
            file.write('{:20s}{:<6.2g}{:<6.2g}{:<6.2g}{:<6.2g}{:<6.2g}{:<6.2g}\n'.format(name,
                                                                                  result.mean * self.__scale,
                                                                                  result.median * self.__scale,
                                                                                  result.min * self.__scale,
                                                                                  result.max * self.__scale,
                                                                                  result.stdev,
                                                                                  sqrt(result.pvariance)))

    def print_csv(self, name, delimiter='\t'):
        if isinstance(name, str):
            with open(name, 'w', newline='') as f:
                self._print_csv(f, delimiter)
        else:
            self._print_csv(name, delimiter)

    def _print_csv(self, file, delimiter):
        writer = csv.writer(file, delimiter=delimiter, quotechar='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['Name', 'Mean', 'Med', 'Min', 'Max', 'Stdev', 'Var'])
        for name in sorted(self.__result.keys()):
            result = self.__result[name]
            values = [
                '{:g}'.format(result.mean * self.__scale),
                '{:g}'.format(result.median * self.__scale),
                '{:g}'.format(result.min * self.__scale),
                '{:g}'.format(result.max * self.__scale),
                '{:g}'.format(result.stdev),
                '{:g}'.format(sqrt(result.pvariance)),
            ]
            writer.writerow([name] + values)
