# see https://github.com/sunyinqi0508/dbproj for updated documentations

from common import Job, Transaction, Lock

class Site: # Data manager
    def __init__(self, id, transmgr):
        self.id = id
        self.datamap = [-1]*21
        self.x=[-1]*12 # x[0..9] => x1..10; x10, 11 => site specific
        self.xhistory = [[] for _ in range(12)]
        self.locks = [Lock() for _ in range(12)]
        self.new_x=[0] * 12 # x[0..9] => x1..10; x10, 11 => site specific
        for i in range(20):
            ii = i + 1
            if ii % 2 == 0:
                self.datamap[ii] = ii // 2 - 1
                self.x[ii//2 - 1] = ii*10
        if not id % 2:
            self.x[10] = (id - 1)*10
            self.x[11] = (id + 9)*10
            self.datamap[id-1] = 10
            self.datamap[id+9] = 11
            self.datarev_map = [0]*12
        else:
            self.datarev_map = [0]*10
        for i, v in enumerate(self.datamap):
            self.datarev_map[v] = i

        self.transactions = set()
        self.transmgr = transmgr
        self.rec(True)

    def ro(self, tr:Transaction, d):
        self.transactions.add(tr)
        tr.sites.add(self)
        data = self.datamap[d]
        lastcommit = self.locks[data].lastcommit
        if (data > 9 or (lastcommit < 0 or lastcommit >= self.startup_time)) and lastcommit <= tr.time:
            tr.read_out(d, self.x[data], self.id)
            return True
        for t, v in reversed(self.xhistory[data]):
            if ((data > 9 or t < 0 or t >= self.startup_time) and t <= tr.time):
                tr.read_out(d, v, self.id)
                return True
        return False

    def rw(self, job:Job):
        tr, d, val = job.info()
        self.transactions.add(tr)
        tr.sites.add(self)
        data = self.datamap[d]
        this_lock = self.locks[data]
        status, owners = this_lock.info()
        def _read():
            if status <= 1: # readlocked or no lock
                this_lock.read_lock(tr) # add self
                tr.read_out(d, self.x[data], self.id)
            elif tr in owners: # write lock by same tr
                tr.read_out(d, self.new_x[data], self.id) # read new data
            else:
                return False
            return True

        def _write():
            if status == 0 or status == 4 or tr in owners:
                if status == 0 or status == 4:
                    this_lock.write_lock(tr)
                if status == 1:
                    if len(owners) > 1:
                        return False # other trs also hold read lock
                    for j in this_lock.jobs:
                        if j.tr != tr and j.halfdone:
                            return False # exist halfdone jobs
                    this_lock.lock = 2 # promote to X lock

                self.new_x[data] = val
                job.halfdone = True
                return True
            return False

        res =  _read() if val is None else _write()
        if not res and val is not None:
            this_lock.jobs.append(job)
        else:
            job.succ += res
            # job.halfdone = job.succ < job.s_cnt
        return res

    def tick(self):
        changed = self.up
        overall_changed = False
        while changed:
            changed = False
            for l in self.locks:
                old_j = l.jobs
                if len(old_j):
                    l.jobs = []
                    c = True
                    for j in old_j:
                        if c:
                            c = self.rw(j) and c
                            changed = changed or c
                        else:
                            l.jobs.append(j)
                
            overall_changed = overall_changed or changed
        if not overall_changed:
            for i, l in enumerate(self.locks):
                for j in l.jobs:
                    j.tr.blocking = True
                    # cmdtick, id, edge,
                    for o in l.owners:
                        if o.id != j.tr.id:
                            j.tr.block_out(self.transmgr.cmd_tick, self.datarev_map[i], o.id, l.lock)
                            self.transmgr.dep_graph.add((o.id, j.tr.id))
        return overall_changed

    def abort(self, tr):
        for l in self.locks:
            status, owner = l.info()
            if status >= 0 and status != 4 and tr in owner:
                l.unlock(tr)
        self.transactions.remove(tr)

    def update_rohistory(self):
        pass
    def commit(self, tr):
        for i, l in enumerate(self.locks):
            status, owner = l.info()
            if status > 0 and tr in owner:
                if status == 2: # a write lock was acquired. 
                    if len(self.transmgr.readonlys) > 0:
                        self.xhistory[i].append((l.lastcommit, self.x[i]))
                    
                    self.x[i] = self.new_x[i]
                    l.lastcommit = self.transmgr.time
                    
                l.unlock(tr)
        self.transactions.remove(tr)

    def fail(self):
        self.up = False
        for tr in self.transactions:
            if not tr.ro:
                tr.aborted = True
                tr.reason = 4 # site failure

    def rec(self, init = False):
        self.up = True
        self.locks = [Lock(not init and i < 10) for i in range(12)]
        self.startup_time = self.transmgr.time