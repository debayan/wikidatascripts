#!/usr/bin/python


import sys
import redis
import json

red = redis.StrictRedis(host='localhost', port=6379, db=0)
red.set('linesread5',0)
s = set()
with open("wikidata-raw-2018.08.01.ttl") as infile:
    for line in infile:
        try:
            line = line.strip()
            if line[0] == '#':
                continue
            red.incr('linesread5')
            tokens = line.split(' ')
            url2 = tokens[1][1:-1]
            if 'entity' not in url2:
                continue
            pid = url2[31:]
            if pid != 'P31':
                continue
            url3 = tokens[2][1:-1]
            if 'resource' not in url3:
                continue
            oid = url3[37:]
            s.add(oid)
        except Exception as e:
            print(e)

f = open('types.json','w')
f.write(json.dumps(list(s)))
f.close()
