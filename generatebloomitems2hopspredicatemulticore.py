#!/usr/bin/python


import sys
from pybloom import ScalableBloomFilter
import redis


red = redis.StrictRedis(host='localhost', port=6379, db=1)
red.set('linesread7',0)
#bloom  = ScalableBloomFilter(mode=ScalableBloomFilter.LARGE_SET_GROWTH)#capacity=500000000,error_rate=0.00001)
with open("wikidata-raw-2018.08.01.ttl") as infile:
    for line in infile:
        red.incr('linesread7')
        line = line.strip()
        if line[0] == '#':
            continue
        tokens = line.split(' ')
        s = tokens[0][1:-1]
        o = tokens[2][1:-1]
        if 'wikidata.dbpedia.org/resource' not in o or 'wikidata.dbpedia.org/resource' not in s:
            continue
        red.sadd(s,o)
        red.sadd(o,s)

#count = 0
#if int(red.get('linesread7')) > 0:
#    red.set('linesread7',0)
#with open("wikidata-raw-2018.08.01.ttl") as infile:
#    for line in infile:
#        line = line.strip()
#        if line[0] == '#':
#            continue
#        count += 1
#        if count < int(sys.argv[1]):
#            continue
#        if count > int(sys.argv[2]):
#            break
#        red.incr('linesread7')
#        tokens = line.split(' ')
#        s = tokens[0][1:-1]
#        p = tokens[1][1:-1]
#        o = tokens[2][1:-1]
#        if 'wikidata.dbpedia.org/resource' not in o:
#            continue
#        for pred in red.smembers(o):
#            bloom.add(s+':'+str(pred))
#        for pred in red.smembers(s):
#            bloom.add(o+':'+str(pred))
#
#f = open('bloom/bloom2hoppredicate%s-%s.pickle'%(sys.argv[1],sys.argv[2]),'wb')
#bloom.tofile(f)
#f.close()
