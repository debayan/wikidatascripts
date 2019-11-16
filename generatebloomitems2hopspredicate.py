#!/usr/bin/python


import sys
from pybloom import ScalableBloomFilter
import redis


d1 = {}
red = redis.StrictRedis(host='localhost', port=6379, db=0)
bloom  = ScalableBloomFilter(mode=ScalableBloomFilter.LARGE_SET_GROWTH)
red.set('linesread7',0)
with open("wikidata-raw-2018.08.01.ttl") as infile:
    for line in infile:
        line = line.strip()
        if line[0] == '#':
            continue
        red.incr('linesread7')
        tokens = line.split(' ')
        s = tokens[0][1:-1]
        p = tokens[1][1:-1]
        o = tokens[2][1:-1]
        if 'wikidata.dbpedia.org/resource' not in o:
            continue
        if s not in d1:
            d1[s] = set()
            d1[s].add(p)
        else:
            d1[s].add(p)
        if o not in d1:
            d1[o] = set()
            d1[o].add(p)
        else:
            d1[o].add(p)


red.set('linesread7',0)
with open("wikidata-raw-2018.08.01.ttl") as infile:
    for line in infile:
        line = line.strip()
        if line[0] == '#':
            continue
        red.incr('linesread7')
        tokens = line.split(' ')
        s = tokens[0][1:-1]
        p = tokens[1][1:-1]
        o = tokens[2][1:-1]
        if 'wikidata.dbpedia.org/resource' not in o:
            continue
        if o in d1:
            for pred in d1[o]:
                bloom.add(s+':'+pred)
        if s in d1:
            for pred in d1[s]:
                bloom.add(o+':'+pred)

f = open('bloom/bloom2hoppredicate.pickle','wb')
bloom.tofile(f)
f.close()
