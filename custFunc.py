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