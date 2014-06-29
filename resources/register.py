#!/usr/bin/env python
import math
import re
import etcd
#import cgi
import time
#import cgitb
#cgitb.enable()
import logging
from resources.common import *
import platform


ldict = {"step": MODULEFILE + "/" + inspect.stack()[0][3],
         "hostname": platform.node().split(".")[0]}
l = logging.LoggerAdapter(fetch_lg(), ldict)


class AppException(Exception):
    exception_str = "Application Exception {} raised: {}"

    def __init__(self, key, value):
        self._key = key
        self._val = value

    def __unicode__(self):
        return self.exception_str.format(self._key, self._val)

    def __str__(self):
        return self.__unicode__()


# Define Exception class for retry
class RetryException(Exception):
    u_str = "Exception ({}) raised after {} tries. Waiting {}" + \
        "seconds each time"

    def __init__(self, exp, max_retry, sleep):
        self.exp = exp
        self.max_retry = max_retry
        self.sleep = sleep

    def __unicode__(self):
        return self.u_str.format(self.exp, self.max_retry, self.sleep)

    def __str__(self):
        return self.__unicode__()


# Define retry util function
def retry_func(func, max_retry=5, sleep=10,):
    """
    @param func: The function that needs to be retry
    @param max_retry: Maximum retry of `func` function, default is `10`
    @return: func
    @raise: RetryException if retries exceeded than max_retry
    """
    for retry in range(1, max_retry + 1):
        try:
            return func()
        except:
            if retry <= max_retry:
                l.info('Failed to call {}, in retry({}/{})'.
                       format(func.__name__, retry, max_retry))
                time.sleep(sleep)
            else:
                raise
    #else:
    #    raise RetryException(e, max_retry, sleep)


class Registrar():
    def __init__(self,
                 host="127.0.0.1",
                 port=4001,
                 root_path='/cs',
                 counter_init_val=10000,
                 ttl=3600):
        self.host = host
        self.port = port

        def conn():
            l.info("Trying connection to etcd://{}/{}".
                   format(self.host, self.port))
            return etcd.Client(host=self.host,
                               port=self.port)
        self.conn = conn
        self.c = retry_func(
            self.conn,
            max_retry=2,
            sleep=1
            )
        self.root = root_path
        self.counter = root_path + "/counter"
        self.counter_init_val = counter_init_val
        self.ttl = ttl
        self.cs = self._get_root()
        self.counter_retry = 10

    def _get_root(self):
        return self._get_node(self.root, dir=True, immortal=True)
    #

    def _get_countval(self):
        return self._incr_node(self.counter, self.counter_init_val)
    #

    def _incr_node(self, p, v, dir=False, retry=0):
        retval = 0
        try:
            n = self._get_node(p, v, immortal=True)
            retval = int(n.value)
            n.value = retval + 1
            try:
                #print "Counter: Updating {} to {}".format(n.key, n.value)
                self.c.update(n)
                n = self._get_node(p, v, immortal=True)
                if n.value > retval:
                    #print "Counter: Incr success"
                    pass
                else:
                    #print "Counter: Incr failed"
                    raise AppException(
                        "CounterUpdateError",
                        "Counter value constant even after an update")
            except:
                if retry > self.counter_retry:
                    raise
                else:
                    #print "Counter: Exception while updating, retrying..."
                    time.sleep(0.5)
                    retval = self._incr_node(p, v, retry=(retry + 1))
        except:
            raise
        return retval

    def _get_node(self,
                  p,
                  v=None,
                  dir=False,
                  immortal=False):
        n = None
        try:
            if immortal:
                n = self.c.write(p, v, prevExist=False, dir=dir)
            else:
                n = self.c.write(p, v, prevExist=False, dir=dir, ttl=self.ttl)
            #print "Node: writing node for {} -> {} [{}]".\
            #    format(n.key, n.value, v)
        except:
            n = self.c.get(p)
            #print "Node: getting node for {} -> {} [{}]".\
            #    format(n.key, n.value, v)
        return n

    def register_host(self, host):
        retval = 0
        try:
            hostnode = self._get_node(
                '{}/host/{}'.format(self.root, host),
                self._get_countval()
                )
            self.c.write(
                "{}/host_lookup/{}".format(self.root, hostnode.value),
                host)
            retval = int(hostnode.value)
        except:
            raise
        return retval

    def get_config(self, key, val="default"):
        retval = val
        config_path = "{}/config/{}".format(self.root, key)
        try:
            n = self._get_node(config_path, val, immortal=True)
            retval = n.value
        except:
            pass
        return retval

    def put_config(self, key, val):
        retval = val
        config_path = "{}/config/{}".format(self.root, key)
        try:
            n = self._get_node(config_path, val, immortal=True)
            if val != n.value:
                self.c.write(config_path, val)
        except:
            pass
        return retval

    def find_remote_node(self, node_type, node_hash,
                         node_ts, node_exportable,
                         node_ts_max_drift=None):
        retval = 0
        p = '{}/remote_nodes/{}/{}'.format(
            self.root,
            node_type,
            re.sub('[ ,!/;]', '_', node_hash)
            )
        if not node_ts_max_drift:
            node_ts_max_drift = self.ttl / 2
        try:
            children = self.c.read(p, sorted=True).children
            min_drift = None
            exportable = None
            for c in children:
                drift = math.fabs(int(c.key.split('/')[-1]) - node_ts)
                if drift < node_ts_max_drift:
                    if not min_drift:
                        min_drift = drift
                        exportable = c.value
                    if min_drift > drift:
                        exportable = c.value
                        min_drift = drift
            if min_drift and min_drift < node_ts_max_drift:
                retval = exportable
            else:
                raise AppException(
                    "FindRemoteNode", "Unable to find a recent remote_node")
        except:
            remote_node = self._get_node(
                '{}/{}'.format(
                    p,
                    node_ts
                    ),
                node_exportable
                )
            retval = remote_node.value
        return retval
    #
