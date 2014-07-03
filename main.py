#!/usr/bin/env python
#
# Copyright 2014 Gautam R Singh
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License


#from __future__ import print_function
import os
import platform
import logging
import sys


sys.path = [sys.path[0],
            os.path.dirname(os.path.abspath(__file__))] + sys.path

from resources import Node
from resources import constants
from resources import common
from resources import config
#from py2neo import neo4j
import argparse

config.DEBUG = False
CACHE = None

parser = argparse.ArgumentParser(
    description='Gathers/imports process, open file, socket info ' +
    'and tries to generate a local dependency graph. Stores state in etcd ' +
    'other instances can co-ordinate to generate a global graph.')

parser.add_argument('--verbose', '-v', action='count',
                    help='Use multiple "-v" options',
                    default=0)

group_etcd = parser.add_argument_group('etcd')
group_etcd.add_argument('--etcdhost', type=str,
                        required=True)
group_etcd.add_argument('--etcdport', type=int,
                        default=4001)

group_in = parser.add_mutually_exclusive_group(required=True)
group_in.add_argument('--inputlocal', action='store_true',
                      help="scan the local system, to map dependencies")
group_in.add_argument('--inputlog', default=None, type=str,
                      help="generate dependencies from the agent log file")
#group_in.add_argument('--inputjson', default=None, type=str, nargs=2,
#                      help="generate dependencies from the outputjson file")


group_out = parser.add_mutually_exclusive_group(required=True)
group_out.add_argument('--outputneo4j', type=str,
                       help="store results in neo4j, specify neo4j REST url",
                       default=None)
group_out.add_argument('--outputkafka', type=str, default=None,
                       help='store results in kafka, specify kafka router url')
group_out.add_argument('--outputjson', type=str, default=None, nargs=2,
                       help='store results in json files, specify two files.' +
                       'files. First file for nodes, second for rels')


args = parser.parse_args()

lg = common.setup_logger(level=args.verbose,
                         logfile=None)
hs = platform.node().split(".")[0]
ldict = {"step": "main",
         "hostname": hs}
l = logging.LoggerAdapter(common.fetch_lg(), ldict)


if args.inputlog:
    if not os.path.exists(args.inputlog):
        l.error("input log file does not exist")
        sys.exit(1)
    g = Node.myGraph(cache=None,
                     inputfile=args.inputlog,
                     etcdhost=args.etcdhost,
                     etcdport=args.etcdport)
elif args.inputlocal:
    g = Node.myGraph(cache=None,
                     etcdhost=args.etcdhost,
                     etcdport=args.etcdport)
else:
    sys.exit(1)
    #if not os.path.exists(args.inputjson[0]) or \
    #        not os.path.exists(args.inputjson[1]):
    #    pass
    #else:
    #    l.error("input json files do not exist")
    #    sys.exit(1)

if args.outputjson:
    from resources.output import out
    out.output_json(Node.CONST_GRAPH_NODE.db,
                    args.outputjson[0],
                    args.outputjson[1])
elif args.outputneo4j:
    from resources.output import outneo4j
    outneo4j.output_neo4j(Node.CONST_GRAPH_NODE.db,
                          Node.CONST_GRAPH_NODE.registry,
                          args.outputneo4j)
elif args.outputkafka:
    from resources.output import outkafka
    outkafka.output_kafka(Node.CONST_GRAPH_NODE.db,
                          Node.CONST_GRAPH_NODE.registry,
                          args.outputkafka)
l.info("Have a good day! Bye!")
