#!/usr/bin/env python

import random
#import smhasher
import re


CONST_NODE_TYPE = {
    "graph": 1,
    "host": 10,
    "proc": 11,
    "user": 12,
    "mount": 13,
    "socket": 14,
    "file": 15,
    "group": 16,
    "connection": 17,
    "nfs": 18,
    "device": 19,
    "remote_connection": 91,
    "remote_fs": 92,
    "ip": 21
}

CONST_NODE_TYPE_REVERSE = {v: k for k, v in CONST_NODE_TYPE.items()}

CONST_REL_TYPE = {
    "has_client": 51,
    "has_connect": 52,
    "has_connection": 53,
    "has_device": 54,
    "has_localservice": 55,
    "has_listen": 56,
    "has_localclient": 57,
    "has_mount": 58,
    "has_open": 59,
    "has_proc": 60,
    "has_run": 61,
    "has_service": 62,
    "has_socket": 63,
    "is_mounted_from": 64,
    "has_ip": 65,
}

CONST_REL_TYPE_REVERSE = {v: k for k, v in CONST_REL_TYPE.items()}


def isNodeRemoteType(s="host"):
    retval = False
    node_type = s
    if isinstance(node_type, int):
        node_type = getNodeType(node_type)
    if re.match("remote_.*", node_type, re.IGNORECASE):
        retval = True
    return retval


def getNodeType(s="host"):
    if isinstance(s, int):
        return CONST_NODE_TYPE_REVERSE[s]
    return CONST_NODE_TYPE[s]


def getRelType(s="host"):
    if isinstance(s, int):
        return CONST_REL_TYPE_REVERSE[s]
    return CONST_REL_TYPE[s]


def getGraphId(s):
    return random.randint(1000, 9999)


def makeHash(val=None):
    hashString = None
    if val is not None:
        if isinstance(val, basestring):
            val = val.lower()
        elif isinstance(val, list):
            val = "".join(str(val)).lower()
        hashString = hash(val)
    return hashString

