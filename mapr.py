import re
import threading, thread
import time
import Queue
import custFunc
import work
import collector
import workgroup

def partition(fname, delmt = "\n"):
    f = open(fname, "r")
    content = f.read()
    ptn_newline = re.compile(delmt)    
    partitions = ptn_newline.split(content)
    return partitions

def secureThread(tHandle,timeout):
    tHandle.setDaemon(True) 
    tHandle.start()
    tHandle.join(timeout)


      


if __name__ == "__main__":
    raw = partition("x:\\Desk\\paper.txt")
    rclt = collector.Collector(collector.get_behav_set("rcollector"))
    rgroup = workgroup.WorkingGroup(rclt, "ReduceWorker")
    mclt = collector.Collector(collector.get_behav_set("mcollector"), routetable = [rgroup])
    group = workgroup.WorkingGroup(mclt, "MapWorker")
    key = 1
    tlist = []
    for part in raw:
        tlist.append((key, part))
        key += 1
    t = threading.Thread(target = mclt.report)
    t3 = threading.Thread(target = rclt.report)
    t2 = threading.Thread(target = secureThread, args = (t, 20))
    t4 = threading.Thread(target = secureThread, args = (t3, 30))
    t2.start()
    t4.start()
    group.fill_tasklist(tlist)
    group.kick_off()
    rgroup.kick_off()
    #group.wait_all()
    mclt.group()
    rclt.group()
    