import sites
from common import Job, Transaction, Lock
class TransMgr:
    def __init__(self):
        self.active = {}
        self.transid_seq_map = {}
        self.transactions = []
        self.time = 0
        self.jobs = []
        self.global_locks = [Lock() for _ in range(20)]
        self.sites = [sites.Site(i + 1, self) for i in range(10)]
        self.dep_graph = set()
        self.readonlys = []
        self.cmd_tick = 0
    def read(self, tr, data):
        j = Job(tr, data)
        if not data % 2: # rep
            for s in self.sites:
                if s.up:
                    if tr.ro:
                        if s.ro(tr, data):
                            return
                    else:
                        j.succ = s.rw(j)
                        return 

                elif not tr.ro and s in tr.sites:
                    tr.aborted = True  # some rw trans has failed sites => abort
        else: # n-rep
            ret = self.sites[data%10]
            if tr.ro:
                if ret.ro(tr, data): 
                    return
            else: 
                j.succ = ret.rw(j)
                return 
        
        tr.aborted = True # ro or rw has no avail sites

    def write(self, tr, data, val):
        j = Job(tr, data,val)
        if not data % 2:
            ups = []
            for s in self.sites:
                if s.up:
                    s.rw(j)
                    j.s_cnt += 1
                    ups.append(s.id)
                else:
                    if s in tr.sites:
                        tr.aborted = True
                tr.write_out(ups)
            return j.succ
        else:
            j.succ = self.sites[data%10].rw(j)
            tr.write_out([data%10])

        return j.succ

    def cycle_dect_dfs(self, curr, active, mintr):
        active.append(curr)
        for e in self.dep_graph:
            if e[0] == curr:
                if e[1] in active:
                    for a in active:
                        if self.active[a].time > mintr[1]:
                            mintr[1] = self.active[a].time
                            mintr[0] = a
                else:
                    self.cycle_dect_dfs(e[1], active, mintr)
        active.pop()

    def cycle_detector(self):
        tids = [tid for tid in self.active]
        mintr = [-1, -1]
        active = []
        for tid in tids:
            self.cycle_dect_dfs(tid, active, mintr)

        if mintr[0] >= 0:
            self.active[mintr[0]].blocking = False
            self.active[mintr[0]].aborted = True
            self.active[mintr[0]].terminating = True
            return True
        return False

    def update(self):
        terminated = [k for k, v in self.active.items() if not v.aborted and not v.blocking and v.terminating]
        aborted = [k for k, v in self.active.items() if v.aborted and v.terminating]
        def _update(k, op):
            tr:Transaction = self.active.get(k, None)
            if tr.ro:
                self.readonlys.remove(tr)
            for s in tr.sites:
                getattr(s, op)(tr)
            tr.print()
            print(f"T{k} {op}ed.")
            del self.active[k]
        for k in terminated: 
            _update(k, 'commit')
        for k in aborted:
            _update(k, 'abort')

    def tick(self):
        changed = True
        while changed:
            changed = False
            self.dep_graph = set()
            for _,v in self.active.items():
                v.blocking = False
            for s in self.sites:
                if s.up:
                    res = s.tick()
                    changed = changed or res
            self.update()
            self.time += 1
        blocking = [k for k, v in self.active.items() if v.blocking and not v.aborted]
        if len(blocking) > 0 and self.cycle_detector():
            self.update()
            self.tick()

    def exec(self, cmd, p1, p2, p3):
        if cmd[0] == 'e':
            tid = int(p1[1:]) # end p1
            tr : Transaction = self.active.get(tid, None)
            if tr is not None:
                tr.terminating = True
            else:
                print(f"Transaction T{tid} terminated before completion.")

        elif cmd[0] == 'w':
            tid = int(p1[1:])
            data = int(p2[1:])
            val = int(p3)
            tr = self.active.get(tid)
            if tr is not None:
                self.write(tr, data, val)
            else:
                print(f"error: transaction terminated: W({tid}, {data}, {val})")

        elif cmd[0] == 'f':
            self.sites[int(p1) - 1].fail() # serv
        elif cmd[0] == 'd':
            self.dump()
        elif cmd[:5] == 'begin':
            tid = int(p1[1:])
            seq = len(self.transactions)
            self.transid_seq_map[tid] = seq
            ro = len(cmd) > 5
            tr = Transaction(seq, self.time, tid, ro)
            self.transactions.append(tr)
            self.active[tid] = tr
            if ro:
                self.readonlys.append(tr)

        elif cmd[:3] == 'rec':
            self.sites[int (p1) - 1].rec() #recover
        elif cmd == 'r':
            tid = int(p1[1:])
            data = int(p2[1:])
            tr = self.active.get(tid)
            if tr is not None:
                self.read(tr, data)
            else:
                print(f"error: transaction terminated: R({tid}, {data})")
        self.tick()
        self.cmd_tick += 1
        
    def dump(self):
        for s in self.sites:
            print(f"Site {s.id} - ", end = '')
            out = []
            for rvmap in s.datarev_map:
                x = s.datamap[rvmap]
                if s.locks[x].lastcommit >= 0:
                    out.append((rvmap, s.x[x]))
            out.sort(key=lambda x: x[0])
            for i, oi in enumerate(out):
                print(f"x{oi[0]}: {oi[1]}{', ' if i < len(out)-1 else ''}" , end = '')
            print()