#!/usr/bin/python


import sys
from pybloom import ScalableBloomFilter
import redis

red = redis.StrictRedis(host='localhost', port=6379, db=0)
red.set('linesread6',0)
bloom1  = ScalableBloomFilter(mode=ScalableBloomFilter.LARGE_SET_GROWTH)
with open("wikidata-raw-2018.08.01_reifiedqualifiers.ttl") as infile:
    for line in infile:
        try:
            line = line.strip()
            if line[0] == '#':
                continue
            red.incr('linesread6')
            tokens = line.split(' ')
            url1 = tokens[0][1:-1]
            s,p,o = (url1[37:]).split('_')
            url2 = tokens[1][1:-1]
            qualifier = url2[31:]
            bloom1.add(s+':'+p+'_'+qualifier)
            bloom1.add(o+':'+p+'_'+qualifier)
        except Exception as e:
            print(e)
     
f = open('bloom/wikidatabloom1.5hopqualifiers.pickle','wb')
bloom1.tofile(f)
f.close()
