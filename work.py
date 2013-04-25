import re, time
import thread, threading
import Queue

class Worker:
    def __init__(self, collector, exec_group):
        self.target_collector = collector
        self.actions = exec_group

    def do_job(self, args):
        prehandled = self.actions["prehandle"](args)
        resultset = self.actions["process"](prehandled)
        if self.actions["check"](resultset):
            self.send(resultset)

    def send(self, handled_data):
        self.target_collector.gather(handled_data)    
        

class WorkSlot(threading.Thread):
    def __init__(self, queue, worker):
        threading.Thread.__init__(self)
        self.queue = queue
        self.worker = worker
        self.isFree = True

    def run(self):
        while True:
            try:
                args = self.queue.get(block = False)
                if args:
                    self.isFree = False
                    self.worker.do_job(args)
                    self.queue.task_done()
                    args = 0
                    self.isFree = True
            except:
                continue