## Transaction manager.
from IO import fs
from sites import Site
import time
class Transaction:
    def __init__ (self, id, ro = False):
        self.id = id
        self.ro = ro
        self.time = time.time()
        self.jobs = []
class TransMgr:
    class Job:
        def __init__(self, tid, rw, data, val = -1):
            self.tid = tid
            self.rw = rw
            self.data = data
            self.val = val
            self.succ = False

    def __init__(self):
        self.db = fs()
        self.sites = [Site(i + 1, self) for i in range(10)]
        self.active = {}
        self.history = {}
        self.jobs = []
        self.R = 0
        self.W = 1

    def read(self, data, tid):
        pass
    def write(self, data, val, tid):
        pass

    def tick(self):
        n_jobs = len(self.jobs)
        while n_jobs > 0 and len(self.jobs) < n_jobs:
            n_jobs = len(self.jobs)
            newjobs = []
            for j in self.jobs:
                succ = self.read(j.data, j.tid) if j.rw == self.R \
                    else self.write(j.data, j.val, j.tid)
                j.succ = succ
                if not succ:
                    newjobs.append(j)
            self.jobs = newjobs

    def exec(self, cmd, p1, p2, p3):
        if cmd[0] == 'e':
            int(p1[1:]) # end p1
        elif cmd[0] == 'w':
            self.write(
                int(p2[1:]), # x?
                int(p3), # v?
                int(p1[1:]) # T?
            )
        elif cmd[0] == 'f':
            self.sites[int(p1)].fail() # serv
        elif cmd[0] == 'd':
            pass # dump
        elif cmd[:5] == 'begin':
            tid = int(p1[1:])
            self.active[tid] = Transaction(tid, len(cmd) > 5)
        elif cmd[:3] == 'rec':
            self.sites[int (p1)].rec() #recover
        elif cmd == 'r':
            tid = int(p2[1:])
            data = int(p1[1:])
            if not self.read(tid, data):
                self.jobs.append(self.Job(tid, self.R, data))
        self.tick()


