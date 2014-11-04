#! /usr/bin/python2.7
# coding: utf-8
# The Class Lib to communicate with zookeeper servers.
# author:shaojun3
# date: 2013.4.19
# version: 1.0

import zookeeper, time, threading, thread
import random
import socket
import fcntl
import struct

from collections import namedtuple
from os.path import basename, join


DEFAULT_TIMEOUT_MS = 3000     #which is '3s'
VERBOSE = True
SERVICE_ROOT_PATH = "/wanda/services"
RUNNING_SERVICE_ROOT_PATH = "/wanda/runnings"
REG_MIN_MIILISECS = 2     #which is '2s'
REG_MAX_MIILISECS = 4      #which is '4s'
REWATCH_MIN_MIILISECS = 4      #which is '4s'
REWATCH_MAX_MIILISECS = 8      #which is '8s'
THREAD_SLEEP_MILLISECS = 1      #which is '0.1s'
RECONNECT_MIN_MIILISECS = 2      #which is '2s'
RECONNECT_MAX_MIILISECS = 4      #which is '4s'
SEP_CHAR = ':'
#LOCAL_IP_ETH_NAME = "eth0"
LOCAL_IP_BOND_NAME = "bond0"
LOCAL_IP_EM_NAME = "em1"
NODE_NAME_SEG_NUM = 5         #'Ip:port:version:client_id:sequence'

ZOO_OPEN_ACL_UNSAFE = {"perms":0x1f, "scheme":"world", "id" :"anyone"}


class NameService(object):
    EPOLICY_RANDOM = 0
    EPOLICY_LOCAL_PREFERRABLE = 1

    def __init__(self, servers, ip_method="", is_running_service=False, timeout=DEFAULT_TIMEOUT_MS):
        self.servers = servers
        self.ip_method = ip_method
        self.is_running_service = is_running_service
        self.timeout = timeout
        self.connected = False
        #self.conn_handler = threading.Condition()
        self.conn_wr = threading.Condition()
        self.handle = -1
        self.service_name = ""
        self.service_port = 0
        self.service_version = ""
        self.reg_node_name = ""
        self.reg_node_retry_time = 0
        self.reconnect_time = 0
        self.watch_service_set = set()
        self.thread_ptr = None
        self.root_path = SERVICE_ROOT_PATH
        #add mesg for dead lock 2013/7/18
        self.zk_mutex = threading.Condition()
        self.messages_ = []

        if self.is_running_service == True :
            self.root_path = RUNNING_SERVICE_ROOT_PATH


        #e.g. '{"recom" : {"host_list" : [("10.1.169.84", 2181), ("10.1.169.84", 2182)], "rewatch_ts" : 1366276699}, ...}'
        self.watch_service_info_dict = {}
        self.local_ip = ""
        self.need_stop = False


    def __del__(self) :
        if self.thread_ptr != None :
            self.thread_ptr.join()
            self.thread_ptr = None

        if self.handle != -1 :
            self.__close__()
            
        self.__clear_message__()
        
    def stop(self):
      self.need_stop = True
      
    def register_service(self, service_name, service_port, service_version) :
        if self.thread_ptr != None :
            print "register_service() must be invoked before start()."
            return False

        self.conn_wr.acquire()

        try :
            self.service_name = service_name
            self.service_port = int(service_port)
            self.service_version = service_version.replace(SEP_CHAR, "_")

            return True

        except Exception, e :
            print "Exception in register_service() :", Exception, e
            return False
        finally :
            self.conn_wr.release()


    def watch_service(self, service_name) :
        if self.thread_ptr != None :
            print "watch_service() must be invoked before start()."
            return False

        self.conn_wr.acquire()

        try :
            self.watch_service_set.add(str(service_name))

            return True

        except Exception, e :
            print "Exception in watch_service() :", Exception, e
            return False
        finally :
            self.conn_wr.release()


    def get_service_host_and_port(self, service_name, policy) :
        if policy not in [self.EPOLICY_RANDOM, self.EPOLICY_LOCAL_PREFERRABLE] :
            print "'policy' value invalid."
            return ("failed", "", 0)

        if (service_name not in self.watch_service_info_dict) or (len(self.watch_service_info_dict[service_name]["host_list"]) == 0) :
            print "No value saved."
            return ("failed", "", 0)

        host_list_tmp = self.watch_service_info_dict[service_name]["host_list"]

        self.conn_wr.acquire()

        try :
            if (policy == self.EPOLICY_LOCAL_PREFERRABLE) and (self.local_ip == host_list_tmp[0][0]) :
                return ("ok", host_list_tmp[0][0], host_list_tmp[0][1])

            rand_item = random.choice(host_list_tmp)

            return ("ok", rand_item[0], rand_item[1])

        except Exception, e :
            print "Exception in get_service_host_and_port() :", Exception, e
            return ("failed", "", 0)
        finally :
            self.conn_wr.release()


    def __event_watcher__(self, h, type, state, path):
        msg = {"type" :type, "state" : state, "path" : path}
        self.__push_message__(msg)


    def __connection_notice__(self):
        #self.conn_handler.acquire()

        try :
            if self.handle == -1 :
                return

            self.connected = True

            if len(self.service_name) != 0 :
                self.__register_service_inner__()

            for wservice in self.watch_service_set :
                self.__fetch_and_watch_service__(wservice)

        except Exception, e :
            print "Exception in __connection_notice__() :", Exception, e
        finally :
            #self.conn_handler.release()
            pass


    def __session_expired_notice__(self):
        #self.conn_handler.acquire()

        try :
            if self.handle == -1 :
                return

            self.__close__()

        except Exception, e :
            print "Exception in __session_expired_notice__() :", Exception, e
        finally :
            #self.conn_handler.release()
            pass


    def __node_changed_notice__(self, path):
        #self.conn_handler.acquire()

        try :
            if self.handle == -1 :
                return

            if (len(self.service_name) != 0) and (self.reg_node_name == path) :
                try :
                    if self.__exists__(path, self.__event_watcher__) != None :
                        return
                    else :
                        self.__register_service_inner__()
                        return

                except Exception, e :
                    print "Exception in __node_changed_notice__() when self.__exists__() :", Exception, e
                    self.__close__()
            elif len(self.watch_service_set) != 0 :
                self.__fetch_and_watch_service__(basename(path))


        except Exception, e :
            print "Exception in __node_changed_notice__() :", Exception, e
        finally :
            #self.conn_handler.release()
            pass


    def __register_service_inner__(self) :
        self.reg_node_name = ""
        self.reg_node_retry_time = 0

        try :
            if self.handle == -1 :
                return

            if not self.__exists__(self.root_path + "/" + self.service_name) :
                self.__create__(self.root_path + "/" + self.service_name)

            reg_node_name_tmp = self.__create__(self.root_path + "/" + self.service_name + "/" + self.__get_host_node_path__(), "", flags=zookeeper.EPHEMERAL | zookeeper.SEQUENCE)
            self.__exists__(reg_node_name_tmp, self.__event_watcher__)
            self.reg_node_name = reg_node_name_tmp

        except Exception, e :
            print "Exception in __register_service_inner__() :", Exception, e
            self.reg_node_retry_time = time.time() + random.randint(REG_MIN_MIILISECS, REG_MAX_MIILISECS) 


    def __get_local_ip_address__(self, ifname) :
        try :
            if (ifname == None) or (len(ifname) == 0) :
                return ""

            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            return socket.inet_ntoa(fcntl.ioctl(
                   s.fileno(),
                   0x8915, # SIOCGIFADDR
                   struct.pack('256s', ifname[:15])
                   )[20:24])
        except Exception, e :
            print "Exception in __get_local_ip_address__() : %s, ifname(%s)" % (str(e), ifname)
            return ""


    def __get_host_node_path__(self) :
        client_id = zookeeper.client_id(self.handle)[0]

        if self.is_running_service == True :
            return self.local_ip + SEP_CHAR + self.service_version + SEP_CHAR + str(client_id) + SEP_CHAR
        else :
            return self.local_ip + SEP_CHAR + str(self.service_port) + SEP_CHAR + self.service_version + SEP_CHAR + str(client_id) + SEP_CHAR


    def __parse_host_node_name__(self, node_name) :
        if (node_name == None) or (len(node_name) == 0) :
            return ("error", [])

        node_name_list = node_name.split(SEP_CHAR)

        if len(node_name_list) != NODE_NAME_SEG_NUM :
            return ("error", [])

        return ("ok", node_name_list)


    def __fetch_and_watch_service__(self, service_name) :
        if (self.handle == -1) or (service_name not in self.watch_service_info_dict) :
            return

        self.conn_wr.acquire()

        try :
            children = self.__get_children__(SERVICE_ROOT_PATH + "/" + service_name, self.__event_watcher__)

            #watch_service_info_dict, e.g. '{"recom" : {"host_list" : [("10.1.169.84", 2181), ("10.1.169.84", 2182)], "rewatch_ts" : 1366276699}, ...}'
            self.watch_service_info_dict[service_name] = {"host_list" : [], "rewatch_ts" : 0}

            #child_str format 'Ip:port:version:client_id:sequence'
            for child_str in children :
                (ret_str, info_list) = self.__parse_host_node_name__(child_str)
                info_item = (info_list[0], int(info_list[1]))

                if (ret_str == "ok") and (info_item not in self.watch_service_info_dict[service_name]["host_list"]) :
                    if info_list[0] == self.local_ip :
                        self.watch_service_info_dict[service_name]["host_list"].insert(0, info_item)
                    else :
                        self.watch_service_info_dict[service_name]["host_list"].append(info_item)



        except Exception, e :
            print "Exception in __fetch_and_watch_service__() :", Exception, e
            self.watch_service_info_dict[service_name]["rewatch_ts"] = time.time() + random.randint(REWATCH_MIN_MIILISECS, REWATCH_MAX_MIILISECS) 
        finally :
            self.conn_wr.release()


    def start(self) :
        self.conn_wr.acquire()

        try :
            if self.thread_ptr != None :
                print "NameService already started."
                return False

            if (self.servers == None) or (len(self.servers) == 0) :
                print "'Servers' needed."
                return False

            if (len(self.service_name) == 0) and (len(self.watch_service_set) == 0) :
                print "Must register service or watch services."
                return False

            #self.local_ip = self.__get_local_ip_address__(LOCAL_IP_ETH_NAME)

            if (self.ip_method != None) and (len(self.ip_method) != 0) :
                self.local_ip = self.__get_local_ip_address__(self.ip_method)

                if len(self.local_ip) == 0 :
                    print "Failed to get local ip : ip_method(%s)." % str(self.ip_method)
                    return False

            else :
                self.local_ip = self.__get_local_ip_address__(LOCAL_IP_BOND_NAME)

                if len(self.local_ip) == 0 :
                    print "Failed to get local ip : ip_method(%s)." % LOCAL_IP_BOND_NAME

                    self.local_ip = self.__get_local_ip_address__(LOCAL_IP_EM_NAME)

                    if len(self.local_ip) == 0 :
                        print "Failed to get local ip : ip_method(%s)." % LOCAL_IP_EM_NAME
                        return False

            print "Got [%s]'s ip : %s" % (self.ip_method, self.local_ip)


            for watch_item in self.watch_service_set :
                self.watch_service_info_dict[watch_item] = {"host_list" : [], "rewatch_ts" : 0}

            zookeeper.set_debug_level(zookeeper.LOG_LEVEL_WARN)

            self.thread_ptr = threading.Thread(target=self.__thread_main__, args=())

            if self.thread_ptr == None :
                print "Failed to create thread."
                return False

            self.thread_ptr.start()

            return True

        except Exception, e :
            print "Exception in start() :", Exception, e
            return False
        finally :
            self.conn_wr.release()


    def __thread_main__(self) :
        while self.thread_ptr != None :
            if self.need_stop :
              print "need stop zookeeper"
              self.__close__()
              thread.exit()
              
            self.__check_handle__()

            if len(self.service_name) != 0 :
                self.__check_register_service__()

            if len(self.watch_service_set) != 0 :
                self.__check_watch_service__()
            
            while 1:
              msg = self.__pop_message__()
              if msg == None:
                break
              self.__deal_message__(msg["type"], msg["state"], msg["path"])

            time.sleep(THREAD_SLEEP_MILLISECS)


    def __check_handle__(self) :
        #self.conn_handler.acquire()

        try :
            #if self.handle != -1 :
            #    return

            if self.handle != -1 :
                state = zookeeper.state(self.handle)

                if (state == zookeeper.CONNECTED_STATE) or (state == zookeeper.CONNECTING_STATE) :
                    return

                print "handle != -1, BUT the state is not 'CONNECTED_STATE' ..."
                self.__close__()
                self.reconnect_time = time.time() + random.randint(RECONNECT_MIN_MIILISECS, RECONNECT_MAX_MIILISECS) 
                return


            now = time.time()

            if (self.reconnect_time < now) or (self.reconnect_time > (now + RECONNECT_MAX_MIILISECS)) :
                if VERBOSE: print("Connecting to %s" % (self.servers))
                start_time = time.time()
                self.handle = zookeeper.init(self.servers, self.__event_watcher__, self.timeout)

                self.connected = False

                if VERBOSE:
                    print("Connected in %d ms, handle is %d"
                          % (int((time.time() - start_time) ), self.handle))


        except Exception, e :
            print "Exception in __check_handle__() :", Exception, e
            self.reconnect_time = time.time() + random.randint(RECONNECT_MIN_MIILISECS, RECONNECT_MAX_MIILISECS) 
        finally :
            #self.conn_handler.release()
            pass


    def __check_register_service__(self) :
        #self.conn_handler.acquire()

        try :
            if (self.handle == -1) or (self.connected == False) :
                return

            if (len(self.service_name) != 0) and (len(self.reg_node_name) == 0) :
                now = time.time()

                if (self.reg_node_retry_time < now) or (self.reg_node_retry_time > (now + REG_MAX_MIILISECS)) :
                    self.__register_service_inner__()


        except Exception, e :
            print "Exception in __check_register_service__() :", Exception, e
        finally :
            #self.conn_handler.release()
            pass


    def __check_watch_service__(self) :
        if len(self.watch_service_set) == 0 :
            return

        rewatch_service_list = []

        self.conn_wr.acquire()

        try :
            now = time.time()

            #watch_service_info_dict, e.g. '{"recom" : {"host_list" : [("10.1.169.84", 2181), ("10.1.169.84", 2182)], "rewatch_ts" : 1366276699}, ...}'
            for service_name_tmp in self.watch_service_info_dict :
                rewatch_ts_tmp = self.watch_service_info_dict[service_name_tmp]["rewatch_ts"]

                if (rewatch_ts_tmp != 0) and ((rewatch_ts_tmp < now) or (self.reconnect_time > (now + REWATCH_MAX_MIILISECS))) :
                    rewatch_service_list.append(service_name_tmp)

        except Exception, e :
            print "Exception in __check_watch_service__() after conn_wr.acquire() :", Exception, e
        finally :
            self.conn_wr.release()

        if len(rewatch_service_list) == 0 :
            return

        #deal with each rewatch item
        #self.conn_handler.acquire()

        try :
            if (self.handle == -1) or (self.connected == False) :
                return

            for rewatch_item in rewatch_service_list :
                self.__fetch_and_watch_service__(rewatch_item)


        except Exception, e :
            print "Exception in __check_watch_service__() after conn_handler.acquire() :", Exception, e
        finally :
            #self.conn_handler.release()
            pass


    def __close__(self):
        zookeeper.close(self.handle)
        self.handle = -1
        self.connected = False
        self.__clear_message__()

    def __create__(self, path, data="", flags=0, acl=[ZOO_OPEN_ACL_UNSAFE]):
        start_time = time.time()
        result = zookeeper.create(self.handle, path, data, acl, flags)
        if VERBOSE:
            print("Node %s created in %d ms"
                  % (path, int((time.time() - start_time))))
        return result


    def __exists__(self, path, watcher=None):
        return zookeeper.exists(self.handle, path, watcher)


    def __get_children__(self, path, watcher=None):
        return zookeeper.get_children(self.handle, path, watcher)
    
    
    def __clear_message__(self):
      self.zk_mutex.acquire()
      self.messages_ = []
      self.zk_mutex.release()
    
    def __push_message__(self, msg):
      self.zk_mutex.acquire()
      self.messages_.append(msg)
      self.zk_mutex.release()
      
    def __pop_message__(self):
      self.zk_mutex.acquire()
      msg = None
      if len(self.messages_) > 0:
        msg = self.messages_.pop()
      self.zk_mutex.release()
      return msg
    
    def __deal_message__(self, tp, state, path = None):
      if tp == zookeeper.SESSION_EVENT:
        if state == zookeeper.CONNECTED_STATE:
          #connect ok
          return self.__connection_notice__()
        elif state == zookeeper.EXPIRED_SESSION_STATE:
          #session timeout
          return self.__session_expired_notice__()
        #other event ignore it
        return
      elif path != None and len(str(path)) > 0:
        return self.__node_changed_notice__(path)