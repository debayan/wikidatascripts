import codecs
import sys

def cleanFile(fileName):

    with codecs.open(fileName,"r") as source, codecs.open(fileName+'.clean.ttl',"w") as cleaned, codecs.open("err.ttl","w") as err:
        for line in source:
            if line[0]=="#":
                continue
            else:
                terms = line.split(" ")
                obj = terms[2]
                if obj.count("<") > 1 or obj.count(">") > 1:
                    err.write(line)
                else:
                    cleaned.write(line)

    print("Done")

if __name__ == '__main__':
    if len(sys.argv) == 2:
        cleanFile(sys.argv[1])

