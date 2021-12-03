from multiprocessing import Lock
import threading
import time
from transmgr import TransMgr

class Site: # Data manager
    def __init__(self, id, transmgr : TransMgr, recovery = False):
        self.id = id
        self.x=[10*(i + 1) for i in range(20)] # x[0..9] => x1..10; x10, 11 => site specific
        
        self.db = transmgr.db
        self.transmgr = transmgr
        self.thread = threading.Thread(target=self.run)
        
        self.input_lock = threading.Lock()
        self.input = []
        self.up = True

        self.data_lock = [threading.Lock() for _ in range(12)]
        
        self.startup_time = time.time()
        if recovery :
            pass
    
    def run(self):
        while self.up:
            if self.input_lock.locked():
                if input[0]: # read
                    input[1]
                else:
                    input[1]
                    input[2]
                self.input_lock.release()
            time.sleep(.01)
    
    def fail(self):
        pass
    def rec(self):
        pass