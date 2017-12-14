#!/usr/bin/env python

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