import os
from transmgr import TransMgr
import re
    
class parser:
    def __init__(self, input, transmgr):
        cmds = open(input, 'r').read().split('\n')
        self.ws = re.compile('\s+')

        for c in cmds:
            self.parse(c)

        self.transmgr = transmgr

    def parse(self, cmd):
        cmd = re.sub('', cmd).lower()
        cmd = cmd[:cmd.find('\\')] 
        
        lparen = cmd.find('(')
        rparen = cmd.rfind(')')
        predicate = cmd[:lparen]
        params = cmd[lparen+1:rparen].split(',')
        params += [None] * (3-len(params))
        if len(predicate) > 0:
            try:
                self.transmgr.exec(predicate, params[0], params[1], params[2])
            except (TypeError, ValueError):
                pass
        
    def prompt(self):
        cmd = ''
        while cmd != 'exit':
            cmd = input()
            self.parse(cmd)

class Formatter:
    pass