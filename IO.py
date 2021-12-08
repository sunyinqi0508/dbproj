import transmgr
import re
import sys

class parser:
    def __init__(self, input, transmgr, prompt = True):
        self.transmgr = transmgr
        self.ws = re.compile('\s+')
        
        if input is not None:
            self.input(input)
        if prompt:
            self.prompt()
    def input(self, file):
        if file.endswith('.DS_Store'):
            return
        cmds = open(file, 'r').read().split('\n')
        for c in cmds:
            self.parse(c)
        self.transmgr = transmgr.TransMgr()

    def parse(self, cmd):
        cmd = self.ws.sub('', cmd).lower()
        end = cmd.find('//') 
        end = len(cmd) if end < 0 else end
        cmd = cmd[:end] 
        
        lparen = cmd.find('(')
        rparen = cmd.rfind(')')
        rparen = len(cmd) if rparen < 0 else rparen
        predicate = cmd[:lparen]
        params = cmd[lparen+1:rparen].split(',')
        params += [None] * (3-len(params))
        if len(predicate) > 0:
            try:
                self.transmgr.exec(predicate, params[0], params[1], params[2])
            except (TypeError, ValueError) as e:
                print(f'Error: {e}')
                pass
    def mega(self, loc):
        from os import listdir
        from os.path import isfile, join
        tests = [f for f in listdir(loc) if isfile(join(loc, f))]
        for t in tests:
            self.input(loc+'/'+t)
    def prompt(self):
        cmd = ''
        while cmd != 'exit':
            cmd = input()
            if cmd.startswith('source'):
                self.transmgr = transmgr.TransMgr()
                self.input(cmd[cmd.find(' '):].strip())
            elif cmd.startswith('mega'):
                self.mega(cmd[cmd.find(' '):].strip())
            elif cmd.startswith('h'):
                print("hello")
            else:
                self.parse(cmd)

class Formatter:
    def out(self, str):
        print(str)

if __name__ == '__main__':
    if len(sys.argv) > 1:
        if(sys.argv[1].startswith('mega')):
            p = parser(None, transmgr.TransMgr(), False)
            p.mega(sys.argv[2].strip())
        else:
            parser(sys.argv[1], transmgr.TransMgr(), False)
    else:
        parser('input', transmgr.TransMgr())