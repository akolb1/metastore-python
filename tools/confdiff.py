#!/usr/bin/env python

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

from xml.etree import ElementTree
import sys
from difflib import unified_diff

def parse_file(name):
    dom = ElementTree.parse(name)
    result = {}

    for p in dom.findall('property'):
        name = p.find('name').text
        value = p.find('value').text
        result[name] = value

    return result


def lines(properties):
    result = []
    for key in sorted(properties.keys()):
        result.append('{}={}'.format(key, properties[key]))
    return result


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: confdiff file1 file2")
        exit(1)

    n1 = sys.argv[1]
    n2 = sys.argv[2]

    parsed1 = lines(parse_file(n1))
    parsed2 = lines(parse_file(n2))

    for line in unified_diff(parsed1, parsed2, fromfile=sys.argv[1], tofile=sys.argv[2]):
        print(line)