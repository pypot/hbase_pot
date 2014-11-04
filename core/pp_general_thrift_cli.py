#-*- coding:utf-8 -*-
#author:yangfan51
#date  :2014.07.10
#version:1.1

import sys, os, time, datetime
import traceback
sys.path.append("./gen-py")
from common.name_service_py.nameservice import NameService

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.transport import THttpClient
from thrift.protocol import TBinaryProtocol

__DEBUG__ = False
__LOG__ = True


class ZkCreateError(Exception):
    pass

class ZkMonitor(object):
    def __init__(self, zk_server, zk_device, server_name):
        self.name_service = NameService(zk_server, zk_device)
        self.server_name = server_name

        if self.name_service.watch_service(server_name) == False:
            if __LOG__:
                print("Fatal", 'watch zk %s, %s:%s'
                         % (server_name, zk_server, zk_device), 'error')
            raise ZkCreateError("watch zk %s , %s:%s,error"
                                % (server_name, zk_server, zk_device))
        if self.name_service.start() == False:
            if __LOG__:
                print("Fatal", 'zk start %s, %s:%s'
                         % (server_name, zk_server, zk_device), 'fail')
            raise ZkCreateError("start zk %s, %s:%s, error"
                                % (server_name, zk_server, zk_device))

        if __LOG__:
            print 'zk start %s, %s:%s' % (server_name, zk_server, zk_device)
            print("Log", 'zk start %s, %s:%s'
                     % (server_name, zk_server, zk_device), 'OK')
        time.sleep(2)

    def Stop(self):
        self.name_service.stop()

    def GetServerInfo(self):
        ret_str, server_host, server_port = self.name_service.get_service_host_and_port(
                                         self.server_name, self.name_service.EPOLICY_RANDOM)
        retry = 0
        while ret_str != 'ok' and retry < 5:
            time.sleep(2)
            ret_str, server_host, server_port = self.name_service.get_service_host_and_port(
                                           self.server_name,self.name_service.EPOLICY_RANDOM)
            retry += 1
            print("Error", 'cannot get zk info','retry %d' % retry)
        return server_host, server_port



_DEFAULT_SERVER_TIMEOUT = 3000

class RpcClientError(Exception):
    pass


class RpcConn(object):
    def __init__(self, zk_server=None, zk_device=None, server_name=None,
                 host=None, port=None):
        '''RPC连接初始化
            Parameters:
             - transType   <str>: 传输类型
                                   F- TFramedTransport
                                   B- TBufferedTransport
             - ClientType  <obj>: 服务定义的Clent类
        '''
        self.zk = None
        if zk_server and zk_device and server_name:
            self.zk = ZkMonitor(zk_server, zk_device, server_name)
        elif host and port:
            self.host = host
            self.port = port
        else:
            raise RpcClientError('GeneralClient init para error!')





class ShortRpcConn(RpcConn):
    """通用 thrift RPC client
    """
    def __init__(self, transType, ClientType, **kwargs):
        RpcConn.__init__(self, **kwargs)
        self.transType = transType
        self.ClientType = ClientType

    def GetServerInfo(self):
        if self.zk:
            return self.zk.GetServerInfo()
        else:
            return self.host, self.port


    def GeneralReq(self, reqFuncName, *reqParas):
        '''
          - reqFuncName <str>: 接口函数名称
          - *reqParas        : 接口函数参数

        Return:
          1: 返回码，0正常，负数异常
          2: CouponTaskInfo 结构list
        '''
        server_host, server_port = self.GetServerInfo()
        if not server_host:
            return -1
        if __DEBUG__:
            print server_host, server_port
        tsocket = None
        transport = None
        rslt = None
        try:
            tsocket = TSocket.TSocket(server_host, server_port)
            tsocket.setTimeout(_DEFAULT_SERVER_TIMEOUT)

            if 'F' == self.transType or 'f' == self.transType:
                transport = TTransport.TFramedTransport(tsocket)
            else:
                transport = TTransport.TBufferedTransport(tsocket)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = self.ClientType(protocol)

            transport.open()

            rslt = getattr(client, reqFuncName)(*reqParas)

        except Thrift.TException, tx:
            if __LOG__:
                print ("Error", "Thrift.TException occurred", tx.message)
                print ("Error", "Thrift.TException when req", str(reqParas))
            rslt = None

        except Exception, e:
            err_info = traceback.format_exc()
            if __LOG__:
                print ("Error", "Exception occurred", err_info)
                print ("Error", "Thrift.TException when req", str(reqParas))
            rslt = None

        finally:
            if transport != None:
                transport.close()
            if tsocket != None:
                tsocket.close()
            return rslt



class LongRpcConn(RpcConn):
    """通用 thrift RPC client(需要ZK)
    """
    def __init__(self, transType, ClientType, **kwargs):
        RpcConn.__init__(self, **kwargs)
        if self.zk:
            self.host, self.port = self.zk.GetServerInfo()

        try:
            self.tsocket = TSocket.TSocket(self.host, self.port)
            self.transport = None
            if 'F' == transType or 'f' == transType:
                self.transport = TTransport.TFramedTransport(self.tsocket)
            else:
                self.transport = TTransport.TBufferedTransport(self.tsocket)
            self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
            self.client = ClientType(self.protocol)
            self.transport.open()

        except Thrift.TException as tx:
            if __LOG__:
                print("Error", "Thrift.TException occurred", tx.message)
            if self.transport != None:
                self.transport.close()
                self.transport = None
            if self.tsocket != None:
                self.tsocket.close()
                self.tsocket = None
            raise tx

        except Exception as e:
            err_info = traceback.format_exc()
            print err_info
            if self.transport != None:
                self.transport.close()
                self.transport = None
            if self.tsocket != None:
                self.tsocket.close()
                self.tsocket = None
            raise e


    def __del__(self):
        if self.transport != None:
            self.transport.close()
            self.transport = None
        if self.tsocket != None:
            self.tsocket.close()
            self.tsocket = None


    def GeneralReq(self, reqFuncName, *reqParas):
        return getattr(self.client, reqFuncName)(*reqParas)




