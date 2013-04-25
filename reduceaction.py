import re, time
import thread, threading
import Queue
import work


def rgroup(data):
    return data

def rseparate(dt, groupSize):
    return dt

def rsend(routetable, pack):
    print "received: ", pack

def doreduce(reducelist):
    return [[{k:sum(v)} for (k, v) in el.items()][0] for el in reducelist]

def rcheck(resultset):
    return len(resultset) > 0
    
def rpre_handle(args):
    return args