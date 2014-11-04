import sys
import getopt
import traceback

sys.path.append('../core')
from pp_hbase_thrift_cli import *


def Help(cmdName):
    print "USAGE: %s -h host -p port" % cmdName


class HbasePotShell(HbasePot):
    acceptedCmd = set(['scan',
                      'set' ,
                      'get' ,
                      'table',
                      'family',
                      'resetscan'
                     ])

    configCmd = set(['table',
                     'family'])

    def __init__(self):
        HbasePot.__init__(self)
        self.scanId = None
        self.defaultTable = None
        self.familyName = None

    def ExecuteCmdLine(self, l):
        try:
            cmdElems = l.strip().split()
            cmdName = cmdElems[0].lower()
            if not cmdName in HbasePotShell.configCmd:
                if (not hbaseShell.defaultTable):
                    raise Exception("no table appointed, use table [TABLE NAME]")
                if (not hbaseShell.familyName):
                    raise Exception("no family appointed, use family [FAMILY NAME]")
            if cmdName in HbasePotShell.acceptedCmd:
                return getattr(self, cmdName)(*cmdElems[1:])
            else:
                raise Exception("input command is not accepted")
        except Exception as e:
            raise e


    def table(self, defaultTable):
        self.scanId = None
        self.defaultTable = defaultTable


    def family(self, familyName):
        self.scanId = None
        self.familyName = familyName

    def resetscan(self):
        self.scanId = self.Execute('scannerOpen', self.defaultTable, '',
                                    [self.familyName], None)

    def scan(self):
        if not self.scanId:
            #self.scanId = self.Execute('scannerOpen', self.defaultTable, '',
            #                      [c.strip() for c in columns.split(',')], None)
            self.scanId = self.Execute('scannerOpen', self.defaultTable, '',
                                        [self.familyName], None)
        rslts = self.Execute('scannerGetList', self.scanId, 10)
        return self.FormatRslts(rslts)


    def set(self, rowKey, KVs):
        mutations = [Mutation(column='%s:%s' % (self.familyName,k), value=v) for k,v in
                                    [kv.split('=') for kv in
                                                       KVs.split(',')]]
        return self.Execute('mutateRow', self.defaultTable, rowKey, mutations, None)


    def get(self, rowKey):
        rslts = self.Execute('getRow', self.defaultTable, rowKey, None)
        return self.FormatRslts(rslts)

    def FormatRslts(self, rslts):
        if rslts:
            return '\n'.join(["\t%s\t%s" % (r.row, r.columns) for r in rslts])
        else:
            return None

if __name__ == '__main__':
    try:
        paraDict = dict(getopt.getopt(sys.argv[1:], "h:p:")[0])
        assert "-h" in paraDict
        assert "-p" in paraDict
    except:
        print traceback.format_exc()
        Help(sys.argv[0])
        sys.exit(-1)

    hbaseShell = HbasePotShell()
    try:
        hbaseShell.Connect(host=paraDict['-h'], port=int(paraDict['-p']), useLongConn=False)
    except:
        print "connect fail"
        sys.exit(-1)
    while True:
        try:
            cmdLine = raw_input("hbase> ")
            if not cmdLine.strip():
                continue
            if cmdLine == "exit":
                sys.exit(0)
            print "[RES]:", hbaseShell.ExecuteCmdLine(cmdLine)
        except Exception as e:
            print "[ERROR]:", str(e)


