
import calendar
import time
import socket
from persistent import Persistent
import transaction
from collections import Counter
import re
import os
import platform
import ZODB
import ZODB.FileStorage
from BTrees.OOBTree import OOBTree
import inspect
import json

from resources.constants import *
from resources.common import *
from resources import lsof
from resources import register


MODULEFILE = re.sub('\.py', '',
                            os.path.basename(inspect.stack()[0][1]))
"""
9 22337 20 368547 7580 8L
# 09223 72 036854 7758 0
# ^     ^  ^      ^    ^- Reserved
# |     |  |      +------ Increment
# |     |  +------------- TimeStamp
# |     +---------------- Node Type
# +---------------------- Agent ID
"""

global CONST_GRAPH_NODE
CONST_GRAPH_NODE = None


class GraphAppException(Exception):
    exception_str = "Graph Application Exception {} raised: {}"

    def __init__(self, key, value):
        self._key = key
        self._val = value

    def __unicode__(self):
        return self.exception_str.format(self._key, self._val)

    def __str__(self):
        return self.__unicode__()


class GraphElement():

    def __init__(self, t=None, h=["default"], p={}, e_id=None,
                 g_id=0, g_incr=0, g_name="default", ts=None,
                 rel=False, rel_src=None, rel_dst=None):
        if ts is None:
            self._discovered_on = calendar.timegm(time.gmtime())
        else:
            self._discovered_on = ts
        self._g_name = g_name
        self._type_str = t
        if t is None:
            raise GraphAppException(
                "NodeRelClassError",
                "NodeRel type not defined")
        if rel:
            if rel_src is None or rel_dst is None:
                raise GraphAppException(
                    "NodeRelClassError",
                    "rel_src or rel_dst not defined")
            self.node = False
            self.rel = True
            self._seen_on = self._discovered_on
            self._seen_count = 1
            try:
                self._type = getRelType(t)
                self._src_type = rel_src.type()
                self._dst_type = rel_dst.type()
                self._src_hash = rel_src.hash()
                self._dst_hash = rel_dst.hash()
                self._hash_raw = [self._src_hash,
                                  self._type_str,
                                  self._dst_hash]
                self._src_id = rel_src.id()
                self._dst_id = rel_dst.id()
            except:
                raise
                raise GraphAppException(
                    "NodeRelClassError",
                    "Invalid relations arguments")
        else:
            self.node = True
            self.rel = False
            self._type = getNodeType(t)
            self._hash_raw = h

        if e_id is None:
            if not rel:
                self._id = "2{:04d}{:02d}{:06d}{:04d}2".\
                    format(
                        int(str(g_id)[:4]),
                        self._type,
                        int(str(self._discovered_on)[-6:]),
                        int(g_incr)
                        )
            else:
                self._id = "1{:06d}{:02d}{:06d}{:04d}1".\
                    format(
                        int(str(g_id)[:6]),
                        self._type,
                        int(str(self._discovered_on)[-6:]),
                        int(g_incr)
                        )
        else:
            self._id = e_id
        self._id = int(self._id)
        self._hash = makeHash(self._hash_raw)
        self._properties = {}
        self.property(p)
        self._property = self._properties

    def typeId(self):
        return self._type

    def typeStr(self):
        return self._type_str

    def type(self):
        return self.typeId()

    def id(self):
        return self._id

    def hash(self):
        return self._hash

    def hashRaw(self):
        return self._hash_raw

    def property(self, d=None, v=None):
        if isinstance(d, dict):
            self._properties = d
            self._update()
            self._property = d
        elif isinstance(d, basestring):
            if v is None:
                if d in self._properties:
                    return self._properties[d]
                else:
                    return None
            else:
                self._properties[d] = v
                self._update()
                self._property = self._properties
        else:
            self._update()
            return self._properties

    def properties(self, d=None, v=None):
        return self.property(d=d, v=v)

    def _update(self):
        self._properties["belongs_to"] = self._g_name
        self._properties["discovered_on"] = self.discoveredOn()
        if self.node:
            self._properties["hash_raw"] = self.hashRaw()
        if self.rel:
            self._properties["seen_count"] = self._seen_count
            self._properties["seen_on"] = self._seen_on
            self._properties["src_id"] = self._src_id
            self._properties["dst_id"] = self._dst_id
        self._property = self._properties

    def discoveredOn(self):
        return self._discovered_on

    def __str__(self):
        retval = "<<<< GENERIC GraphElement >>>>"
        return retval


class Node(GraphElement, Persistent):
    """def __init__(self, t=None, h=["default"], p=None,
                 g_id=0, g_incr=0, g_name="default", ts=None,
                 rel=False, rel_src=None, rel_dst=None):
    """

    def __init__(self, node_type, node_hash,
                 graph_id, graph_incr, graph_name,
                 node_timestamp,
                 node_properties
                 ):
        #print "Node: {},{}".format(node_type, node_timestamp)
        GraphElement.__init__(
            self,
            t=node_type,
            h=node_hash,
            p=node_properties,
            e_id=None,
            g_id=graph_id,
            g_incr=graph_incr,
            g_name=graph_name,
            ts=node_timestamp
            )

    def exporter(self):
        self._properties["exported_on"] = calendar.timegm(time.gmtime())
        exportable = {}
        exportable["id"] = self.id()
        exportable["hash"] = self.hash()
        exportable["type"] = self.type()
        exportable["discovered_on"] = self.discoveredOn()
        exportable["properties"] = self.properties()
        return json.dumps(exportable, sort_keys=True,
                          indent=4, separators=(',', ': '))

    def importer(self, s):
        try:
            importable = json.loads(s)
        except:
            raise
        self._id = int(importable["id"])
        self._discovered_on = importable["discovered_on"]
        self._properties = importable["properties"]
        self._property = self._properties
        self._g_name = self._properties["belongs_to"]
        self._type_str = getNodeType(importable["type"])
        self._type = importable["type"]
        self._hash_raw = importable["properties"]["hash_raw"]
        self._hash = importable["hash"]
        self._properties["imported_on"] = calendar.timegm(time.gmtime())

    def __str__(self):
        return "{}_node((id={}), type={}, hash={}, property={})".\
            format(
                self.typeStr(),
                self._id,
                self._type,
                self._hash,
                self._properties)


class Rel(GraphElement, Persistent):
    """def __init__(self, t=None, h=["default"], p=None,
                 g_id=0, g_incr=0, g_name="default", ts=None,
                 rel=False, rel_src=None, rel_dst=None):
    """

    def __init__(self, src_node, rel_type, dst_node,
                 graph_id, graph_incr, graph_name,
                 rel_timestamp=None,
                 rel_properties=dict(),
                 ):
        GraphElement.__init__(
            self,
            t=rel_type,
            h=None,
            p=rel_properties,
            e_id=None,
            g_id=graph_id,
            g_incr=graph_incr,
            g_name=graph_name,
            ts=rel_timestamp,
            rel=True,
            rel_src=src_node,
            rel_dst=dst_node
            )

    def srcId(self):
        return self._src_id

    def dstId(self):
        return self._dst_id

    def srcType(self):
        return self._src_type

    def dstType(self):
        return self._dst_type

    def srcHash(self):
        return self._src_hash

    def dstHash(self):
        return self._dst_hash

    def seenOn(self):
        return self._seen_on

    def seenCount(self):
        return self._seen_count

    def seen(self):
        return self.seenCount()

    def lastSeen(self, t=None):
        if t is not None:
            if t - self._seen_on > 1:
                self._seen_on = t
                self._seen_count += 1
                self._update()
        return self

    def __str__(self):
        return ("{}_rel((id={}), src={}, type={}, " +
                "dst={}, property={})").\
            format(
                self.typeStr(),
                self._id,
                self._src_id,
                self.typeStr(),
                self._dst_id,
                self._properties)


class RemoteNode(Node):
    """ def find_remote_node(self, node_type, node_hash,
                         node_ts, node_exportable,
                         node_ts_max_drift=None):
    """
    def __init__(self, N, r):
        """def find_remote_node(self, node_type, node_hash,
                         node_ts, node_exportable,
                         node_ts_max_drift=None):
        """
        #print ">>> findin in remote registry:", N.typeStr()
        #print ">>> findin in remote registry:", "_".join(N.hashRaw())
        #print ">>> findin in remote registry:", N.discoveredOn()
        #print ">>> findin in remote registry:", N.exporter()
        importable = r.find_remote_node(
            node_type=N.typeStr(),
            node_hash="_".join(N.hashRaw()),
            node_ts=N.discoveredOn(),
            node_exportable=N.exporter())
        Node.importer(self, importable)
        self.node = True
        self.rel = False


class Graph():
    def __init__(self, n, logger, cache=None,
                 offline=False, sysinfo=dict(),
                 registry=None):
        self._offline = offline
        self._logger = logger
        self.debug = self._logger.debug
        self.info = self._logger.info
        self.warn = self._logger.warning
        self.error = self._logger.error
        if cache is not None:
            self._db = ZODB.DB(ZODB.FileStorage.FileStorage(cache))
        else:
            self._db = ZODB.DB(None)
        self._db_conn = self._db.open()
        self._root = self._db_conn.root()
        self._transaction = transaction
        self.registry = registry
        if self.registry:
            self._id_short = self.registry.register_host(n)
        else:
            self._id_short = getGraphId(n)
        self._name = "graph_for_" + n
        #implement uniq
        self._type = getNodeType("graph")
        self._timestamp = sysinfo['ts']
        self._id = "{:06d}{:03d}{}".format(self._id_short,
                                           self._type,
                                           str(self._timestamp)[-6:])
        self._id = int(self._id)
        self._hash = hash(n)
        self._properties = dict()
        self._dbs = dict()
        self._counter = Counter()
        self.db("id")
        if offline:
            cache = self.db('_device_to_mount')
            for i, j in sysinfo['mt']['_device_to_mount'].iteritems():
                cache[i] = j
            cache = self.db('_device_num_to_list')
            for i, j in sysinfo['mt']['_device_num_to_list'].iteritems():
                cache[i] = j
        else:
            self.getInfoFromDeviceNumber('0,0')
        self._sysinfo = sysinfo
        self._host_addr = {}
        self._host_if = {}

    def host(self, sysinfo=None):
        if sysinfo is None:
            pass
        else:
            self._host = self.myNode("host",
                                     [sysinfo['host']],
                                     {"name": sysinfo['host']})
            for host_if in sysinfo['ip']:
                my_if = self.myNode("device",
                                    host_if,
                                    {"name": host_if})
                self._host_if[host_if] = my_if
                self.myRel(self._host,
                           "has_device",
                           my_if)
                for addr in sysinfo['ip'][host_if]:
                    if len(addr) > 0:
                        a = self.myNode("ip",
                                        addr,
                                        {"name": addr})
                        self._host_addr[addr] = a
                        self.myRel(my_if,
                                   "has_ip",
                                   a)
        return self._host

    def hostIp(self, host_addr=None):
        if host_addr:
            if re.search("^\*", host_addr):
                return self._host_addr.values()
            elif host_addr in self._host_addr:
                return [self._host_addr[host_addr]]
            else:
                return []
        else:
            return self._host_addr.keys()

    def id(self):
        return self._id_short

    def hash(self, val=None):
        if val is not None:
            self._hash_raw = val
            self._hash = makeHash(val)
        return self._hash

    def property(self, d=None, v=None):
        if isinstance(d, dict):
            self._properties = d
        elif isinstance(d, basestring):
            if v is None:
                if d in self._properties:
                    return self._properties[d]
                else:
                    return None
            else:
                self._properties[d] = v
        else:
            self._update()
            return self._properties

    def db(self, nodeType):
        if nodeType not in self._root:
            self._root[nodeType] = OOBTree()
            self._transaction.commit()
            #self._dbs[nodeType] = OOBTree()
        return self._root[nodeType]

    def myNode(self, nodeType, hashList, propertyDict=dict(), seen=True,
               ts=None):
        hashString = makeHash(hashList)
        _log_action = "Found"
        if hashString not in self.db(nodeType):
            self._counter['node'] += 1
            self._counter['node:{}'.format(nodeType)] += 1
            if ts is None:
                ts = calendar.timegm(time.gmtime())
            newNode = Node(node_type=nodeType,
                           node_hash=hashList,
                           graph_id=self._id,
                           graph_incr=self._counter['node'],
                           graph_name=self._name,
                           node_timestamp=ts,
                           node_properties=propertyDict)
            _log_action = "Creating"
            if isNodeRemoteType(nodeType):
                foundNode = RemoteNode(newNode, self.registry)
                if foundNode.id() != newNode.id():
                    _log_action = "Found Remote"
                else:
                    _log_action = "Creating Remote"
                newNode = foundNode
            self.info("{} Nod [{:05d}]: db({})[{}] -> {}".format(
                      _log_action,
                      self._counter['node'],
                      nodeType,
                      hashString,
                      newNode))
            self.db(nodeType)[hashString] = newNode
            self._transaction.commit()
        else:
            pass
        return self.db(nodeType)[hashString]

    def myRel(self, srcNode, relType, dstNode, propertyDict=dict(),
              seen=True, ts=None):
        hashString = makeHash([srcNode.hash(), relType, dstNode.hash()])
        if hashString not in self.db(relType):
            self._counter['rel'] += 1
            self._counter['rel:{}'.format(relType)] += 1
            if ts is None:
                ts = calendar.timegm(time.gmtime())
            newRel = Rel(src_node=srcNode,
                         rel_type=relType,
                         dst_node=dstNode,
                         graph_id=self._id,
                         graph_incr=self._counter['rel'],
                         graph_name=self._name,
                         rel_timestamp=ts,
                         rel_properties=propertyDict)
            self.info("Creating Rel [{:05d}]: {} -> {}".format(
                      self._counter['rel'],
                      hashString,
                      newRel))
            self.db(relType)[hashString] = newRel
            self._transaction.commit()
        else:
            if seen:
                if self._offline:
                    last_seen = ts
                else:
                    last_seen = calendar.timegm(time.gmtime())
                updateRel = self.db(relType)[hashString].\
                    lastSeen(last_seen)
                #print "Updating Rel: ", updateRel
                if updateRel._seen_count != \
                        self.db(relType)[hashString].seen():
                    self.db(relType)[hashString] = updateRel
                    self.info("Updated Rel [{:05d}]: {} -> {}".format(
                              self._counter['rel'],
                              hashString,
                              updateRel))
                    self._transaction.commit()
        return self.db(relType)[hashString]

    def myNodeExists(self, nodeType, hashList):
        hashString = makeHash(hashList)
        if hashString not in self.db(nodeType):
            return False
        return True

    def isDeviceRemoteFileSystem(self, d):
        if self.getInfoFromDeviceNumber(d) is None:
            return True
        return False

    def getInfoFromDeviceNumber(self, d):
        retval = None
        d = parse_device_number(d)
        #
        device_cache = self.db("_device_num_to_list")
        if d not in device_cache and self._offline is False:
            try:
                m = []
                with open('/proc/mounts', 'r') as f:
                    m = f.readlines()
                for l in m:
                    if re.search(' (nfs|smbfs|cifs|pnfs|afs) ', l):
                        pass
                    else:
                        mount = l.split()[1]
                        devic = l.split()[0]
                        try:
                            stdev = os.stat(mount).st_dev
                            key = "{},{}".format(os.major(stdev),
                                                 os.minor(stdev))
                            device_cache[key] = \
                                [devic, mount]
                        except:
                            pass
            except:
                pass
        try:
            retval = device_cache[d]
        except:
            pass
        return retval

    def getDeviceFromDeviceNumber(self, d):
        retval = None
        m = self.getInfoFromDeviceNumber(d)
        if m is not None:
            retval = m[0]
        return retval

    def getMountFromDeviceNumber(self, d):
        retval = None
        m = self.getInfoFromDeviceNumber(d)
        if m is not None:
            retval = m[1]
        return retval

    def getMountFromDevice(self, d):
        retval = None
        device_cache = self.db("_device_to_mount")
        if d not in device_cache and self._offline is False:
            try:
                m = []
                with open('/proc/mounts', 'r') as f:
                    m = f.readlines()
                for l in m:
                    device = l.split()[0]
                    mount = l.split()[1]
                    device_cache[device] = mount
            except:
                pass
        try:
            retval = device_cache[d]
        except:
            pass
        return retval


#            cmd='lsof -FkugpRDtnc0 -l -i -nP -w -r 10mmmmm%smmmmm',
def myGraph(cache=None,
            cmd='lsof -FkugpRDtnc0 -l -i -nP -w -r 10mmmmm%smmmmm',
            inputfile=None,
            runlsof=True,
            etcdhost='127.0.0.1',
            etcdport=4001):

    ldict = {"step": MODULEFILE + "/" + inspect.stack()[0][3],
             "hostname": platform.node().split(".")[0]}
    l = logging.LoggerAdapter(fetch_lg(), ldict)
   #
    sysinfo = {}
    if inputfile:
        offline = True
        parsed = []
        with open(inputfile, 'r') as ifile:
            parsed = ifile.read().splitlines()
        host_index1 = parsed.index('### host ###')
        host_index2 = parsed.index('### host ###', host_index1 + 1)
        sysinfo['host'] = parsed[host_index1 + 1:host_index2][0]
        ip_index1 = parsed.index('### ip ###')
        ip_index2 = parsed.index('### ip ###', ip_index1 + 1)
        sysinfo['ip'] = parse_ip_output(parsed[ip_index1 + 1:ip_index2])
        mt_index1 = parsed.index('### mounts ###')
        mt_index2 = parsed.index('### mounts ###', mt_index1 + 1)
        sysinfo['mt'] = parse_mt_output(parsed[mt_index1 + 1:mt_index2])
        ts_index1 = parsed.index('### time ###')
        ts_index2 = parsed.index('### time ###', ts_index1 + 1)
        sysinfo['ts'] = parsed[ts_index1 + 1:ts_index2][0]
        sysinfo['ls'] = []
        marker = '### lsof ###'
        min = None
        max = -1
        while True:
            try:
                min = parsed.index(marker, max + 1)
                max = parsed.index(marker, min + 1)
            except:
                break
            else:
                sysinfo['ls'].append(parsed[min + 1: max])
        sysinfo['cmds'] = sysinfo['ls']
    else:
        import netifaces
        offline = False
        sysinfo['host'] = socket.gethostname()
        sysinfo['ip'] = {}
        for host_if in netifaces.interfaces():
            sysinfo['ip'][host_if] = []
            if netifaces.AF_INET in netifaces.ifaddresses(host_if):
                for addr in netifaces.ifaddresses(host_if)[netifaces.AF_INET]:
                    addr = addr['addr'].split('%')[0]
                    sysinfo['ip'][host_if].append(addr)
            if netifaces.AF_INET6 in netifaces.ifaddresses(host_if):
                for addr in netifaces.ifaddresses(host_if)[netifaces.AF_INET6]:
                    addr = addr['addr']
                    addr = addr.replace("[", "")
                    addr = addr.replace("]", "").split('%')[0]
                    sysinfo['ip'][host_if].append(addr)
        sysinfo['mt'] = parse_ip_output(
            run_command(
                ["awk",
                 '!/fuse./{"stat" $2 "-c 0x%D" | getline ss; ' +
                 'printf "%s %s %s %s\\n", $1, $2, ss, $3}',
                 '/proc/mounts'])[1])
        sysinfo['ts'] = calendar.timegm(time.gmtime())
        sysinfo['cmds'] = [cmd]
    #
    global CONST_GRAPH_NODE
    if CONST_GRAPH_NODE is None:
        registry = register.Registrar(host=etcdhost, port=etcdport)
        CONST_GRAPH_NODE = Graph(sysinfo['host'],
                                 logger=l,
                                 cache=cache,
                                 offline=offline,
                                 sysinfo=sysinfo,
                                 registry=registry)
        CONST_GRAPH_NODE.host(sysinfo)
        #print "host_daddr", CONST_GRAPH_NODE._host_addr
        #print "sysinfo---", CONST_GRAPH_NODE._sysinfo
        #print "hostif----", CONST_GRAPH_NODE._host_if
        #sys.exit(0)
        c = 0
        for cmd in sysinfo['cmds']:
            c += 1
            lg = logging.LoggerAdapter(fetch_lg(), {
                                       "step": "lsof()".format(c),
                                       "hostname": sysinfo["host"]})
            lsofer = lsof.lsof(graph=CONST_GRAPH_NODE,
                               cmd=cmd,
                               logger=lg,
                               offline=offline,
                               timeout=25)
            if runlsof is True:
                lsofer.run()
                #time.sleep(5)
    return CONST_GRAPH_NODE
