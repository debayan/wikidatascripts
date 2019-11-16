#!/usr/bin/python

from elasticsearch import Elasticsearch
from elasticsearch import helpers
import sys,os,json

es = Elasticsearch()
actions = []
d = json.loads(open('wikiurilabeldict1.json').read())
doccount = 0
with open("wikidata-raw-2018.08.01.ttl") as infile:
    for line in infile:
        line = line.strip()
        if line[0] == '#':
            continue
        tokens = line.split(' ')
        url1 = tokens[0][1:-1]
        url2 = tokens[1][1:-1]
        url3 = tokens[2][1:-1]
        if 'wikidata.dbpedia.org/resource' not in url3 or 'entity' not in url2:
            continue
        action = { "_index": "wikidataentitypairrelationlabels01", "_type": "records", "_source": { "uripair": url1+':'+url3, "relationuri":url2, "relationlabels": d[url2] } }
        actions.append(action)
        if len(actions) == 100000:
            print("indexing 100k docs ....")
            helpers.bulk(es, actions)
            doccount += 100000
            print("%d done"%(doccount))
            actions = []
helpers.bulk(es, actions)
print("All %d done"%(doccount + len(actions)))


