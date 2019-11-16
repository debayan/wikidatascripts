import sys
from pybloom import ScalableBloomFilter
import redis


d = {}
red = redis.StrictRedis(host='localhost', port=6379, db=0)
#bloom1hop  = ScalableBloomFilter(mode=ScalableBloomFilter.LARGE_SET_GROWTH)#capacity=200000000, error_rate=0.0001)
bloomreiqual = ScalableBloomFilter(mode=ScalableBloomFilter.LARGE_SET_GROWTH)#capacity=200000000, error_rate=0.0001)
count = 0
red.set('linesread3',0)
with open('wikidata-raw-2018.08.01_reifiedqualifiers.ttl') as infile:
    for line in infile:
        try:
            line = line.strip()
            if line[0] == '#':
                continue
            red.incr('linesread3')
            tokens = line.split(' ')
            s = tokens[0][1:-1]
            p = tokens[1][1:-1]
            o = tokens[2][1:-1]
            _s,_p,_o = s[37:].split('_')
            _qualrel = p[31:]
            _qualent = p[37:]
            bloomreiqual.add(_s+':'+_qualrel+'_'+_qualent)
            bloomreiqual.add(_o+':'+_qualrel+'_'+_qualent)
        except Exception as e:
            print(e)

f = open('bloom/bloomreifiedqualifiers.pickle','wb')
bloomreiqual.tofile(f)
f.close()
