import sys,os,json

relurldictset = {}

with open('wikidata-properties-2018.08.01.ttl') as propfile:
    for line in propfile:
        if line[0] == '#':
            continue
        line = line.strip()
        tokens = line.split(" ")
        url = tokens[0].replace('<','').replace('>','')
        label = ' '.join(tokens[2:])
        if '@en' not in label:
            continue
        label = label.replace('"','').replace('@en .','')
        if ' , ' in label:
            for labl in label.split(' , '):
                if labl in relurldictset:
                    relurldictset[labl].add(url)
                else:
                    relurldictset[labl] = set()
                    relurldictset[labl].add(url)
        else:
            if label in relurldictset:
                relurldictset[label].add(url)
            else:
                relurldictset[label] = set()
                relurldictset[label].add(url)
relurldictarr = {}
for k,v in relurldictset.items():
    relurldictarr[k] = list(v)
       
with open('wikidatareluri.json', 'w', encoding='utf-8') as f:
    json.dump(relurldictarr, f, ensure_ascii=False, indent=4) 
