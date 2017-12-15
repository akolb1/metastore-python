#!/usr/bin/env python3

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
