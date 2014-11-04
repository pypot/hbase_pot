#! /usr/bin/python2.7
# coding: utf-8
# The script to watch servers info from zookeeper servers.
# author:shaojun3
# date: 2013.4.19
# version: 1.0

import logging
import time
import sys

from os.path import basename, join
from nameservice import NameService


if __name__ == "__main__":

    if len(sys.argv) != 3 :
        print "Usage : %s service_name, ip_method" % sys.argv[0]
        exit()

    name_service = NameService("localhost:2181", sys.argv[2])

    if name_service.watch_service(sys.argv[1]) == False :
        print "Failed to watch_service()", sys.argv[1]
        exit()

    if name_service.start() == False :
        print "Failed to start()"
        exit()

    print "Started!"


    while raw_input() != 'q' :
        (ret_str, host, port) = name_service.get_service_host_and_port(sys.argv[1], name_service.EPOLICY_RANDOM)

        if ret_str != "ok" :
            print "Failed to get_service_host_and_port()"
            continue

        print "server_ip :", host, "server_port :", port

