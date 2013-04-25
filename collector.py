import Queue
import time, random
import workgroup
from mapaction import *
from reduceaction import *

collect_behav = {
    "mcollector" : {
        "group" : mgroup,
        "separate" : mseparate,
        "send" : msend,
        },
    "rcollector" : {
        "group" : rgroup,
        "separate" : rseparate,
        "send" : rsend,
        },
    }

def get_behav_set(type):
    return collect_behav[type]

class RouteTable(list):
    def __init__(self, neighborlist = []):
        for neighbor in neighborlist:
            self.append(neighbor)
    

class Collector(list):
    def __init__(self, actions, groupSize = 5, interval = 3, routetable = []):
        self.queue = Queue.Queue()
        self.cp = []
        self.actions = actions
        self.groupSize = groupSize
        self.routetable = routetable
        self.interval = interval

    def gather(self, data):
        self.append(data)

    def report(self):
        while (True):
            time.sleep(self.interval)
            self.cp = self[:]
            del self[:]
            self.cp.sort()
            if (len(self.cp) > 0):
                self.queue.put(self.cp)

    def separate(self, dt):
        for pack in self.actions["separate"](dt, self.groupSize):
            self.send(pack)
                
    def send(self, pack):
        self.actions["send"](self.routetable, pack)

    def group(self):
        data = self.queue.get()
        dt = self.actions["group"](data)
        self.separate(dt)