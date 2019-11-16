import sys
from elasticsearch import Elasticsearch
from elasticsearch import helpers

doccount = 0
actions = []
es = Elasticsearch()
with open("wikidata-description-2018.08.01.ttl") as infile:
    for line in infile:
        if line[0] == '#':
            continue
        line = line.strip()
        tokens = line.split(" ")
        url = tokens[0].replace('<','').replace('>','')
        label = ' '.join(tokens[2:])
        if '@en' not in label:
            continue
        label = label.replace('"','').replace('@en .','')
        action = { "_index": "wikidataentitylabelindex01", "_type": "records", "_source": { "uri": url, "wikidataLabel": label } }
        actions.append(action)
        if len(actions) == 100000:
            print("indexing 100k docs ....")
            helpers.bulk(es, actions)
            doccount += 100000
            print("%d done"%(doccount))
            actions = []
helpers.bulk(es, actions)
print("All %d done"%(doccount + len(actions)))

f.close()
