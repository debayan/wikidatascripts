import sys,os,re


properties = set()
p = re.compile('/Q(.*)>')
with open("wikidata-raw-2018.08.01.ttl") as infile:
    for line in infile:
        line = line.strip()
        if line[0] == '#':
            continue
        tokens = line.split(' ')
        url1 = tokens[0]
        properties.add(int(p.findall(url1)[0]))
        url3 = tokens[2]
        if 'wikidata.dbpedia.org/resource' not in url3:
            continue
        properties.add(int(p.findall(url3)[0]))
print(len(properties))
