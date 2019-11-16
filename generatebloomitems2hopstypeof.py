import sys
from pybloom import ScalableBloomFilter
import redis


red = redis.StrictRedis(host='localhost', port=6379, db=0)
bloom1hop  = ScalableBloomFilter(mode=ScalableBloomFilter.LARGE_SET_GROWTH)#capacity=200000000, error_rate=0.0001)
count = 0
red.set('linesread7',0)
with open('wikidata-instance-types-2018.08.01.ttl') as infile:
    for line in infile:
        try:
            line = line.strip()
            if line[0] == '#':
                continue
            red.incr('linesread7')
            tokens = line.split(' ')
            s = tokens[0][1:-1][37:]
            o = tokens[2][1:-1][28:]
            bloom1hop.add(s+':'+o)
        except Exception as e:
            print(e)


f = open('bloom/bloom1hoptypeofentity.pickle','wb')
bloom1hop.tofile(f)
f.close()
