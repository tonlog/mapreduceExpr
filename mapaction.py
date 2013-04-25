import random
import re
import thread, threading
import Queue
import work

def mgroup(data):
    dt = dict()
    for g in data:
        for (k, v) in g.items():
            if k in dt:
                dt[k].append(v)
            else:
                dt[k] = [v]
    return dt
    

def mseparate(dt, groupSize):
    keys = dt.keys()
    keys.sort()
    l = len(keys)
    resultpacks = []
    for i in range(len(keys)/groupSize + 1):
        pack = []
        for j in range(groupSize):
            index = i * groupSize + j
            if(index - l < 0):
                k = keys[index]
                v = dt[k]
                if k and v:
                    pack.append({k:v})
        if len(pack) > 0: yield pack



def msend(routetable, pack):
        end = len(routetable)-1
        if (end >= 0 and routetable):
            tg = random.randint(0, end)
            routetable[tg].fill_singletask(pack)



def domap(args):
    words = args
    records = dict()
    for element in words:
        element = element.lower()
        if ( element in records ):
            records[element] += 1
        else:
            records[element] = 1
    return records

def mcheck(resultset):
    return len(resultset) > 0

def mpre_handle(args):
    ptn = re.compile(delmet)
    raw_split = ptn.split(args[1])
    return mdenoise(raw_split)

def mdenoise(words, ptn = ""):
    for el in words:
        if re.match("[\s]", el) or el == '':
            words.remove(el)
    return words    