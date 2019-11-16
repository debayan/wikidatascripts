import sys,json,urllib
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import itertools
from annoy import AnnoyIndex
import urllib.request

es = Elasticsearch()

reld = json.loads(open('/data/home/sda-srv05/debayan/masterthesis/EARL/data/wikiurilabeldict1.json').read())

t = AnnoyIndex(300, 'angular') #approx nearest neighbour search lib
numberlabelurlhash = {}

count = 0
seenlabel = {}
doccount = 0
actions = []
linecount = 0
seenurlpair = {}
es = Elasticsearch()
with open("wikidata-raw-2018.08.01_reifiedqualifiers.ttl") as infile:
    for line in infile:
        if line[0] == '#':
            continue
        line = line.strip()
        if linecount % 100 == 0:
            print(linecount)
        linecount += 1
        tokens = line.split(" ")
        try:
            if 'resource' in tokens[2]:
                qualifier = tokens[1].replace('<','').replace('>','')
                qualifierentity = tokens[2].replace('<','').replace('>','')
                if qualifier+qualifierentity in seenurlpair:
                    continue
                else:
                    seenurlpair[qualifier+qualifierentity] = None
                qualifierlabels = reld[qualifier]
                qualifierentitylabels = []
                res = es.search(index="wikidataentitylabelindex01", body={"query":{"term":{"uri": qualifierentity}},"size":100})
                for record in res['hits']['hits']:
                    qualifierentitylabels.append(record['_source']['wikidataLabel'])
                combinedlabeltuples = list(itertools.product(qualifierlabels, qualifierentitylabels))
                combinedlabels = [' '.join(list(x)) for x in combinedlabeltuples]
                params = json.dumps({'chunks':combinedlabels}).encode('utf8')
                req =  urllib.request.Request('http://localhost:8887/ftwv', data=params, headers={'content-type': 'application/json'})
                vectors = json.loads(urllib.request.urlopen(req).read())
                for vector in vectors:
                    t.add_item(count,vector)
                numberlabelurlhash[count] = {'urls':[qualifier, qualifierentity]}
                count += 1
        except Exception as e:
            print(e)
            continue   
                
t.build(10)
t.save('qualent1.ann')
f = open('numberlabelurlhashqualifier1.json','w')
f.write(json.dumps(numberlabelurlhash))
f.close()
            
#        label = label.replace('"','').replace('@en .','')
#        action = { "_index": "wikidataentitylabelindex01", "_type": "records", "_source": { "uri": url, "wikidataLabel": label } }
#        actions.append(action)
#        if len(actions) == 100000:
#            print("indexing 100k docs ....")
#            helpers.bulk(es, actions)
#            doccount += 100000
#            print("%d done"%(doccount))
#            actions = []
#helpers.bulk(es, actions)
#print("All %d done"%(doccount + len(actions)))

f.close()
