import Queue
import thread, threading
import custFunc
import work

class WorkingGroup:
    def __init__(self, collector, worker_type, thread_num = 5, max_task_num = 0):
        self.collector = collector
        self.queue = Queue.Queue(maxsize = max_task_num)
        self.threads = []
        self.worker_role = worker_type 
        self.__init_pool(thread_num)
        self.kicked_off = False        

    def __init_pool(self, thread_num):
        for i in range(thread_num):
            worker = custFunc.get_instance_of(self.worker_role)(self.collector)
            self.threads.append(work.WorkSlot(self.queue, worker))
                                                               
    def fill_singletask(self, args):
        self.queue.put(args)

    def fill_tasklist(self, tlist):
        for task in tlist:
            self.queue.put(task)

    def kick_off(self):
        if(not self.kicked_off):
            for t in self.threads:
                    t.setDaemon(True)
                    t.start()

    def add_worker(self, num):
        for i in range(num):
            worker = custFunc.get_instance_of(self.worker_role)(self.collector)
            t = work.WorkSlot(self.queue, worker)
            t.start()
            self.threads.append(t)

    def reduce_worker(self, num):
        pass    
                         
    def wait_all(self, timeout = 2):  
        for t in self.threads:  
            if t.isAlive():
                t.join(timeout)  