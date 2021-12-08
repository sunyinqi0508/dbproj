# see https://github.com/sunyinqi0508/dbproj for updated documentations

import time as sys_time

class Transaction:
    def __init__ (self, seq, time, id, ro = False):
        self.id = id
        self.ro = ro
        self.seq = seq
        self.time = time
        self.abstime = sys_time.time()
        self.sites = set()
        self.lock_conflicts = set()
        self.str = ''
        self.terminating = False
        self.blocking = False
        self.aborted = False
        self.seq = 0
        self.reason = 0
        self.reasons = ['deadlock prevention.', 'write operation has no available sites.', 'sites down during transactions.', 'read operation has no available sites.', 'site failure.']
    def getreason(self):
        return 'Because ' + self.reasons[self.reason] if self.aborted else ''
    def out(self, string):
        self.str += f'{string}\n'
    def block_out(self, cmd_tick, data, blocker, locktype):
        if (cmd_tick, data, blocker, locktype) not in self.lock_conflicts:
            self.lock_conflicts.add((cmd_tick, data, blocker, locktype))
            self.str+=f"T{self.id} blocked at tick {cmd_tick} due to a {'read' if locktype == 1 else 'write'} lock conflict on x{data} held by T{blocker}.\n"
    def read_out(self, x, v, s = -1):
        self.str += f'T{self.id}: x{x}.{s}: {v}\n'
    def write_out(self, data, sites):
        s = f'T{self.id} writes on x{data}. Sites Affected: '
        for i, si in enumerate(sites):
            s += f"{si}{', ' if i < len(sites)-1 else ''}"
        self.str += s + "\n"
    def print(self):
        if not self.aborted:
            print(self.str, end='')

class Lock:
    def __init__(self, writeonly = False):
        self.lastcommit = -1
        self.lock = 0 if not writeonly else 4
        self.owners = set()
        self.jobs = []

    def read_lock(self, tr):
        if self.lock <= 1:
            self.lock = 1
            self.owners.add(tr)
            return True
        elif tr in self.owners:
            return True
        else:
            return False

    def write_lock(self, tr):
        if self.lock == 0 or  self.lock == 4 or\
            (tr in self.owners and len(self.owners) == 1):
            self.lock = 2
            self.owners.add(tr)
            return True
        else:
            return False

    def unlock(self, tr):
        if tr in self.owners:
            self.owners.remove(tr)
            if not len(self.owners):
                self.lock = 0
    def clear(self):
        self.jobs.clear()
        self.lastcommit = -1
        self.owners.clear()
        self.lock = 0

    def info(self):
        return self.lock, self.owners

class Job:
    def __init__(self, tr, data, seq, val = None):
        self.tr = tr
        self.rw = val is None
        self.data = data
        self.val = val
        self.succ = False
        self.halfdone = False
        self.s_cnt = 0
        self.seq = seq
    def info(self):
        return self.tr, self.data, self.val