import re, time
import thread, threading
import Queue
import work
from mapaction import *
from reduceaction import *

catg = {
        "MapWorker" : "map",
        "ReduceWorker" : "reduce",
        }
delmet = "[\W\s]"


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



function_group = {
    "map" : {
        "prehandle" : mpre_handle,
        "process" : domap,
        "check" : mcheck,
        },
    "reduce" : {
        "prehandle" : rpre_handle,
        "process" : doreduce,
        "check" : rcheck,
        },
    }

def get_instance_of(type):
    return lambda collector: work.Worker(collector, function_group[catg[type]])