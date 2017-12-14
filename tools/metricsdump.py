#!/usr/bin/env python3

from sys import stdout
import csv
import json
from argparse import ArgumentParser


"""
Given Hive metrics file, print in CSV statistics about each api
"""
if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--dummy', help='dummy argument')
    parser.add_argument('files', metavar='FILE', nargs='*', help='files to read, if empty, stdin is used')
    args = parser.parse_args()
    writer = csv.writer(stdout, delimiter='\t', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    writer.writerow(['Name', 'Weight', 'Count', 'Mean', 'Min', 'Max', 'p75'])

    for name in args.files:
        with open(name) as f:
            metric = json.load(f).get('timers')
            for api in sorted(metric.keys()):
                values = metric.get(api)
                count = values.get('count')
                mean = values.get('mean')
                min = values.get('min')
                max = values.get('max')
                p75 = values.get('p75')
                # Weight is count multiplied by mean value
                weight = count * mean
                writer.writerow([api[4:], weight, count, mean, min, max, p75])
