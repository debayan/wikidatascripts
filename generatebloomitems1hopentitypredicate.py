#!/usr/bin/python


import sys
from pybloom import ScalableBloomFilter
import redis

red = redis.StrictRedis(host='localhost', port=6379, db=0)
red.set('linesread5',0)
bloom1  = ScalableBloomFilter(mode=ScalableBloomFilter.LARGE_SET_GROWTH)
bloom2  = ScalableBloomFilter(mode=ScalableBloomFilter.LARGE_SET_GROWTH)
with open("wikidata-raw-2018.08.01.ttl") as infile:
    for line in infile:
        try:
            line = line.strip()
            if line[0] == '#':
                continue
            red.incr('linesread5')
            tokens = line.split(' ')
            url1 = tokens[0][1:-1]
            if 'resource' not in url1:
                continue
            sid = url1[37:]
            url2 = tokens[1][1:-1]
            if 'entity' not in url2:
                continue
            pid = url2[31:]
            bloom1.add(sid+':'+pid)
            url3 = tokens[2][1:-1]
            if 'resource' not in url3:
                continue
            oid = url3[37:]
            bloom2.add(sid+':'+oid)
        except Exception as e:
            print(e)
f = open('bloom/wikidatabloom1hoppredicate.pickle','wb')
bloom1.tofile(f)
f.close()
f = open('bloom/wikidatabloom1hopentity.pickle','wb')
bloom2.tofile(f)
f.close()
