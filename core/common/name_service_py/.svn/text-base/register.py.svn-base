#! /usr/bin/python2.7
# coding: utf-8
# The script to register servers info with zookeeper servers.
# author:shaojun3
# date: 2013.4.19
# version: 1.0

import logging
import time
import sys

from os.path import basename, join
from nameservice import NameService


if __name__ == "__main__":

    if len(sys.argv) != 5 :
        print "Usage : %s service_name, service_port, service_version, ip_method" % sys.argv[0]
        exit()

    name_service = NameService("localhost:2181", sys.argv[4])

    if name_service.register_service(sys.argv[1], int(sys.argv[2]), sys.argv[3]) == False :
        print "Failed to register_service()", sys.argv[1], int(sys.argv[2]), sys.argv[3]
        exit()

    if name_service.start() == False :
        print "Failed to start()"
        exit()

    print "Started!"

    time.sleep(1000)
