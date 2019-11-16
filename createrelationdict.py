import sys,os,json

relurldictset = {}

urlreldictset = {}

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
                if url in urlreldictset:
                    urlreldictset[url].add(labl)
                else:
                    urlreldictset[url] = set()
                    urlreldictset[url].add(labl)
        else:
            if label in relurldictset:
                relurldictset[label].add(url)
            else:
                relurldictset[label] = set()
                relurldictset[label].add(url)
            if url in urlreldictset:
                urlreldictset[url].add(label)
            else:
                urlreldictset[url] = set()
                urlreldictset[url].add(label)
with open('wikidata-raw-2018.08.01_reifiedqualifiers.ttl') as qualfile:
    for line in qualfile:
        if line[0] == '#':
            continue
        line = line.strip()
        tokens = line.split(" ")
        url = tokens[0].replace('<','').replace('>','')
        rel1 = url.split('_')[1]
        url1 = 'http://www.wikidata.org/entity/'+rel1

        rel2 = tokens[1].replace('<','').replace('>','')

        label1 = urlreldictset[url1]
        label2 = urlreldictset[rel2]
        
        rel2 = rel2.split('http://www.wikidata.org/entity/')[1]
        newurl = 'http://www.wikidata.org/entity/'+rel1+'_'+rel2
        for lab1 in label1:
            for lab2 in label2:
                newlabel = lab1 +' '+ lab2
                if newlabel in relurldictset:
                    relurldictset[newlabel].add(newurl)
                else:
                   relurldictset[newlabel] = set()
                   relurldictset[newlabel].add(newurl)
relurldictarr = {}
for k,v in relurldictset.items():
    relurldictarr[k] = list(v)

with open('wikidatareluri.json', 'w', encoding='utf-8') as f:
    json.dump(relurldictarr, f, ensure_ascii=False, indent=4) 
