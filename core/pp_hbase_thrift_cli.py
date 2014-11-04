#-*- coding: utf-8 -*-
#author:PhanYoung@pypot.com
#date  :2014.07.10
#version:1.0
import sys
sys.path.append('../core/gen-py')

from pp_general_thrift_cli import ShortRpcConn
from pp_general_thrift_cli import LongRpcConn

from hbase import Hbase
from hbase.ttypes import ColumnDescriptor, Mutation, BatchMutation, TRegionInfo
from hbase.ttypes import IOError, AlreadyExists

import traceback
import requests



class HbasePot(object):
    def __init__(self):
        self.hbaseConn = None

    def Connect(self, host, port, useLongConn=False):
        if useLongConn:
            self.hbaseConn = LongRpcConn('B', Hbase.Client, host=host, port=port)
        else:
            self.hbaseConn = ShortRpcConn('B', Hbase.Client, host=host, port=port)


    def Execute(self, funcName, *paras):
        return self.hbaseConn.GeneralReq(funcName, *paras)


    @staticmethod
    def CreateHbaseConn(host, port, useLongConn=False):
        hbasePot = HbasePot()
        hbasePot.connect(host, port, useLongConn)

