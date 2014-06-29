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
    description='Sillyfacter fetches facts about the state of the ' +
    'system. Gathers process, open file, socket info ' +
    'and then outputs a JSON (currently). Designed for ' +
    'dependency mappings.')
parser.add_argument('--outputnodes', type=str,
                    default=None)
parser.add_argument('--outputrels', type=str,
                    default=None)
parser.add_argument('--inputlog', type=str,
                    default=None)
parser.add_argument('--etcdhost', type=str,
                    required=True)
parser.add_argument('--etcdport', type=int,
                    required=True)
parser.add_argument('--outputneo4j', default=False,
                    action='store_true',
                    help='store results in neo4j')
parser.add_argument('--outputneo4jurl', type=str,
                    default=None)
parser.add_argument('--outputkafka', default=False,
                    action='store_true',
                    help='store results in kafka')
parser.add_argument('--outputkafkaurl', type=str,
                    default=None)
parser.add_argument('--verbose', '-v', action='count',
                    help='Use multiple "-v" options',
                    default=0)
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
else:
    g = Node.myGraph(cache=None,
                     etcdhost=args.etcdhost,
                     etcdport=args.etcdport)

if args.outputrels or args.outputnodes:
    from resources.output import out
    out.output_json(Node.CONST_GRAPH_NODE.db,
                    args.outputnodes,
                    args.outputrels)
if args.outputneo4j:
    from resources.output import outneo4j
    outneo4j.output_neo4j(Node.CONST_GRAPH_NODE.db,
                          Node.CONST_GRAPH_NODE.registry,
                          args.outputneo4jurl)
if args.outputkafka:
    from resources.output import outkafka
    outkafka.output_kafka(Node.CONST_GRAPH_NODE.db,
                          Node.CONST_GRAPH_NODE.registry,
                          args.outputkafkaurl)
l.info("Have a good day! Bye!")
