#!/usr/bin/env python

from subprocess import Popen, PIPE
import re
import os
#from ..Node import *
import threading
##cmd="lsof -FugpRDtnc0 -l -nP -r 2mmmmm",
import shlex
import time


class lsof():
    def __init__(self,
                 graph,
                 cmd="lsof -F0",
                 offline=False,
                 timeout=30,
                 logger=None
                 ):
        self._logger = logger
        self.info = self._logger.info
        self.warn = self._logger.warning
        self.debug = self._logger.debug
        self.error = self._logger.error
        self._graph = graph
        self.g = self._graph
        self._cmd = cmd
        self._timeout = timeout
        self._process = None
        self._offline = offline
        self._iter = None
        self._ts = None
        self._ps = {}
        self.listening_sockets = set()
        self.host_ips = []
        if offline:
            self.info("Initializing lsof in offline mode")
        else:
            import psutil as p
            self.info("Initializing lsof in online mode, " +
                      "executing command: {}".format(cmd))
            self._psutil = p

    def run(self):
        """
        run lsof and parse the results into a tmp array that gets passed back
        to the caller.
        """
        self = self

        def target():
            self.info("Run thread started")
            try:
                if not self._offline:
                    self.debug("Setting up process and readline")
                    self._process = Popen(shlex.split(self._cmd),
                                          stdout=PIPE,
                                          close_fds=True)
                    self._iter = iter(self._process.stdout.readline, '')
                else:
                    self.debug("Setting up offline " +
                               "output of {} lines".format(len(self._cmd)))
                    self._iter = self._cmd
                proc_blocks = []
                proc_block = []
                ps_blocks = []
                self._i = 1
                self._ts = None
                block_type = 'normal'
                for line in self._iter:
                    line = line.rstrip()
                    if len(line) < 1:
                        continue
                    #self.debug("({}-raw): {}".format(block_type,
                    #           line.rstrip()))
                    if line == '### ps ###':
                        if block_type == 'ps':
                            block_type = 'normal'
                            self._ps = self._parse_ps_blocks(ps_blocks,
                                                             self._i)
                            ps_blocks = []
                        else:
                            block_type = 'ps'
                    if block_type == 'ps':
                        ps_blocks.append(line)
                        continue
                    if line[0:3] == 'mmm':
                        self.debug("Processing a new block")
                        ts = re.match('mm+([0-9]+)m+', line)
                        if ts:
                            self._ts = int(ts.group(1))
                        if len(proc_block) > 0:
                            proc_blocks.append(proc_block)
                            proc_block = []
                        self._parse_blocks(proc_blocks, self._ts, self._i)
                        proc_blocks = []
                        proc_block = []
                        self._i += 1
                    elif line[0] == "p":
                        if len(proc_block) > 0:
                            proc_blocks.append(proc_block)
                            proc_block = []
                        proc_block.append(line)
                    else:
                        proc_block.append(line)
                self.debug("finished reading lsof output")
                if not self._offline:
                    self._process.communicate()
                self.info('Run thread finished')
            except:
                if not self._offline:
                    self._process.terminate()
                self.error("Got an exception: ")
                raise
        #
        thread = threading.Thread(target=target)
        thread.start()
        thread.join(self._timeout)
        #
        if thread.is_alive():
            self.warn('Run thread exceed timeout seconds, terminating it'.
                      format(self._timeout))
            if not self._offline:
                self._process.terminate()
            thread.join()
        #self.info(self._process.returncode)

    def _parse_ps_blocks(self, blocks, i):
        retval = {}
        self.info("Parsing ps for Iteration [{:05d}]".format(i))
        for l in blocks[1:]:
            l = l.strip().split()
            pid = l[0]
            user = l[1]
            name = os.path.basename(l[2])
            cmdline = " ".join(l[2:])
            retval[pid] = [user, name, cmdline]
            #self.debug("[   ps] Adding {} -> {}".format(pid, retval[pid]))
        return retval

    def _parse_blocks(self, blocks, ts, i):
        self.info("Starting Iteration [{:05d}] at {}".format(i, ts))
        self._find_all_listening_sockets(blocks)
        #print "========================================================"
        #print "---- ALL lsitening sockets", self.listening_sockets
        #print "---- ALL lsitening sockets", self.host_ips
        #print "========================================================"

        t = time.time()
        c = 1
        for b in blocks:
            try:
                self._parse_block(b, ts=ts, n=c)
            except:
                raise
                self._logger.exception("Got an exception: ")
            finally:
                c += 1
        self.info("Completed Iteration [{:05d}] in {} seconds".format(i,
                  int(time.time() - t)))
        #

    def _find_all_listening_sockets(self, blocks):
        self.info("finding all listening sockets")
        listening_sockets = []
        host_ips = self.g.hostIp()
        for b in blocks:
            for line in b:
                try:
                    if line[0] == 't':
                        if re.search("->", line):
                            pass
                        else:
                            socket = line.rstrip('\n').split('\0')[1]
                            socket = socket[1:]
                            if socket == "*:*":
                                continue
                            m = re.match('^(\*|0\.0\.0\.0).*:([0-9]+)', socket)
                            if m:
                                port = m.group(2)
                                for ip in host_ips:
                                    s = "{}:{}".format(ip, port)
                                    listening_sockets.append(s)
                                continue
                            m = re.match('^(.+):([0-9]+)', socket)
                            if m:
                                ip = m.group(1).\
                                    replace('[', '').\
                                    replace(']', '')
                                port = m.group(2)
                                s = "{}:{}".format(ip, port)
                                listening_sockets.append(s)
                except:
                    raise
                    pass
        self.listening_sockets = set(listening_sockets)
        self.host_ips = host_ips

    def _parse_block(self, b, ts=None, n=None):
        myGraph = self.g
        myNode = self.g.myNode
        myRel = self.g.myRel
        myNode = self.g.myNode
        myNode = self.g.myNode
        myNode = self.g.myNode
        try:
            if b[0][0] != 'p':
                return
        except:
            return
        proc = dict()
        head = b[0].rstrip('\n').split('\0')
        if 'g0' in head or 'g1' in head:
            self.debug("[{:05d}] is kernel block".format(n))
            return
        else:
            self.debug("[{:05d}] is normal block".format(n))
            for h in head:
                if len(h) > 1 and h[1:] != ' ':
                    proc[h[0]] = h[1:]
            #self.debug("(raw) head: {}".format(head))
            #self.debug("(prs) head: {}".format(proc))
            #def myNode(nodeType, hashList, propertyDict=dict()):
            if not self._offline:
                try:
                    p = self._psutil.\
                        Process(int(proc['p']))
                    t = p.as_dict(attrs=['name', 'username',
                                  'cmdline', 'create_time',
                                  'num_threads', 'num_fds',
                                  'cpu_percent', 'memory_percent'])
                    t['cmdline'] = " ".join(t['cmdline'])
                except:
                    return
                    #raise
                else:
                    for i, j in t.iteritems():
                        if not j:
                            if i is "cmdline":
                                j = t['name']
                            else:
                                j = ""
                    proc = dict(proc.items() + t.items())
            else:
                if proc['p'] in self._ps:
                    proc['name'] = self._ps[proc['p']][1]
                    proc['cmdline'] = self._ps[proc['p']][2]
                    proc['username'] = self._ps[proc['p']][0]
                else:
                    proc['name'] = proc['c']
                    proc['cmdline'] = proc['c']
                    proc['username'] = proc['u']
            proc_node = myNode("proc",
                               [proc['p'], proc['g'], proc['name']],
                               proc,
                               ts=ts)
            user_node = myNode("user",
                               [proc['u']],
                               {"name": proc['username']},
                               ts=ts)
            myRel(user_node, "has_run", proc_node, ts=ts)
            myRel(myGraph.host(), "has_proc", proc_node, ts=ts)
            #Parsing the rest
            ag = {"fs": {},
                  "net": {}}
            for line in b[1:]:
                of = dict()
                for item in line.rstrip('\n').split('\0'):
                    if len(item) > 1 and item[1:] != ' ':
                        of[item[0]] = item[1:]
                if "t" in of:
                    if re.match('(REG|DIR|CWD)', of['t'], re.IGNORECASE):
                        if of['D'] not in ag['fs']:
                            ag['fs'][of['D']] = of
                    elif re.match('(IPv4|IPv6)', of['t'], re.IGNORECASE):
                        if of['n'] not in ag['net']:
                            ag['net'][of['n']] = of
                    else:
                        pass
            self.debug("(prs)   fs: {}".format(ag['fs']))
            self.debug("(prs)  net: {}".format(ag['net']))
            #
            for f in ag['fs']:
                try:
                    self._parse_block_fs(proc_node, ag['fs'][f], ts)
                except:
                    raise
            if ag['net'] > 0:
                self._parse_block_network(proc_node, ag['net'].values(), ts)

    # networks
    def _parse_block_network(self, proc_node, Aof, ts=None):
        myGraph = self.g
        myNode = self.g.myNode
        myRel = self.g.myRel
        myNodeExists = self.g.myNodeExists
        myNode = self.g.myNode
        myNode = self.g.myNode
        # net: {'t': 'IPv4', 'n': '172.16.237.128:47164->74.125.236.167:80'}
        listen_list = []
        connection_list = []
        self.debug("(prs)  net: Arry: {}".format(Aof))
        for n in Aof:
            n['n'] = n['n'].replace("[", "")
            n['n'] = n['n'].replace("]", "")
            if re.search('->', n['n']):
                connection_list.append(n)
            else:
                listen_list.append(n)
        #
        for of in listen_list:
            #self.debug("(prs)  net: Listen: {}".format(of))
            socket_ip = ':'.join(of['n'].split(':')[0:-1])
            socket_pt = of['n'].split(':')[-1]
            socket_ip.replace('[', '')
            socket_ip.replace(']', '')
            #
            #self.debug("(prs)  net: Addr: {}".format(myGraph.hostIp()))
            for host_addr_node in myGraph.hostIp(socket_ip):
                host_addr = host_addr_node.property('name')
                socket_addr = "{}:{}".format(host_addr, socket_pt)
                socket_node = myNode("socket",
                                     socket_addr,
                                     {"name": socket_addr,
                                      "ip": host_addr,
                                      "port": socket_pt},
                                     ts)
                myRel(proc_node, "has_listen", socket_node, ts)
                myRel(host_addr_node, "has_socket", socket_node, ts)
                if re.search('^127\.0\.', host_addr_node.property('name')) or \
                        re.search('^::1', host_addr_node.property('name')):
                    pass
                else:
                    myRel(myGraph.host(), "has_service", proc_node, ts)
        #
        for of in connection_list:
            self.debug("(prs)  net: Connection: {}".format(of))
            conn_pair = [of['n'].split('-')[0], of['n'].split('>')[1]]
            #self.debug(("(prs)  net: Connection: of[n]: {} " +
            #           "connpair: {}").format(of['n'], conn_pair))
            #if myNodeExists("socket", conn_pair[0]):
            #print "----------------------------"
            #print "=== Check if conn_pair {} is among: {}".\
            #    format(conn_pair, self.listening_sockets)
            #print "----------------------------"
            if conn_pair[1] in self.listening_sockets:
                # or \
                # myNodeExists("socket", "*:" + conn_pair[0].split(':')[1]):
                src = conn_pair[0]
                dst = conn_pair[1]
                #print "--- {} in listening_sockets so {} -> {}".\
                #    format(conn_pair[1], src, dst)
            elif conn_pair[0] in self.listening_sockets:
                src = conn_pair[1]
                dst = conn_pair[0]
                #print "--- {} in listening_sockets so {} -> {}".\
                #    format(conn_pair[0], src, dst)
            else:
                first_ip = ':'.join(conn_pair[0].split(':')[0:-1])
                secnd_ip = ':'.join(conn_pair[1].split(':')[0:-1])
                if first_ip in self.host_ips and\
                        secnd_ip not in self.host_ips:
                    src = conn_pair[0]
                    dst = conn_pair[1]
                    #print "--- {} in hostIps so {} -> {}".\
                    #    format(conn_pair[0], src, dst)
                elif secnd_ip in self.host_ips and\
                        first_ip not in self.host_ips:
                    src = conn_pair[1]
                    dst = conn_pair[0]
                    #print "--- {} in hostIps so {} -> {}".\
                    #    format(conn_pair[1], src, dst)
                else:
                    return
            src_ip = ':'.join(src.split(':')[0:-1])
            src_pt = src.split(':')[-1]
            dst_ip = ':'.join(dst.split(':')[0:-1])
            dst_pt = dst.split(':')[-1]
            src_ip.replace('[', '')
            src_ip.replace(']', '')
            dst_ip.replace('[', '')
            dst_ip.replace(']', '')
            i = {"src": {},
                 "dst": {},
                 "local": {},
                 "remote": {}}
            i["src"]["ip"] = src_ip
            i["src"]["pt"] = src_pt
            i["dst"]["ip"] = dst_ip
            i["dst"]["pt"] = dst_pt
            i["remote"]["src"] = i["src"]
            i["remote"]["dst"] = i["dst"]
            host_ips = self.host_ips
            if dst_ip not in host_ips and src_ip not in host_ips:
                return
            if src_ip in host_ips:
                i["local"]["src"] = i["src"]
                i["local"]["src"]["addr_node"] = \
                    myGraph.hostIp(src_ip)[0]
                i["local"]["src"]["addr"] = \
                    i["local"]["src"]["addr_node"].property('name')
                i["remote"].pop("src", None)
                sname = "{}:{}".format(
                    i["local"]["src"]["ip"],
                    i["local"]["src"]["pt"])
                i["local"]["src"]["socket_node"] = \
                    myNode(
                        "socket", sname,
                        {"name": sname,
                         "ip": i["local"]["src"]["addr"],
                         "port": i["local"]["src"]["pt"]}, ts)
                myRel(
                    proc_node, "has_connect",
                    i["local"]["src"]["socket_node"], ts)
                myRel(
                    i["local"]["src"]["addr_node"], "has_socket",
                    i["local"]["src"]["socket_node"], ts)
                i["src_node"] = i["local"]["src"]["socket_node"]
                myRel(myGraph.host(), "has_client", proc_node, ts)
            if dst_ip in host_ips:
                i["local"]["dst"] = i["dst"]
                i["local"]["dst"]["addr_node"] = \
                    myGraph.hostIp(dst_ip)[0]
                i["local"]["dst"]["addr"] = \
                    i["local"]["dst"]["addr_node"].property('name')
                i["remote"].pop("dst", None)
                sname = "{}:{}".format(
                    i["local"]["dst"]["ip"],
                    i["local"]["dst"]["pt"])
                i["local"]["dst"]["socket_node"] = \
                    myNode(
                        "socket", sname,
                        {"name": sname,
                         "ip": i["local"]["dst"]["addr"],
                         "port": i["local"]["dst"]["pt"]}, ts)
                myRel(
                    proc_node, "has_connect",
                    i["local"]["dst"]["socket_node"], ts)
                myRel(
                    i["local"]["dst"]["addr_node"], "has_socket",
                    i["local"]["dst"]["socket_node"], ts)
                i["dst_node"] = i["local"]["dst"]["socket_node"]

            if "dst_node" not in i and "src_node" in i:
                #dst must be remote
                s = i["local"]["src"]
                d = i["remote"]["dst"]
                ss = "{}:{}".format(s["ip"], s["pt"])
                ds = "{}:{}".format(d["ip"], d["pt"])
                #print "--- creating remote_connection with dst_not as remote"
                #print "\t\t\t\tsrc: {}".format(ss)
                #print "\t\t\t\tdst: {}".format(ds)
                i["dst_node"] = \
                    myNode(
                        "remote_connection",
                        [ss, ds],
                        {"name": "{},{}".format(ss, ds)},
                        ts)

            if "src_node" not in i and "dst_node" in i:
                #dst must be remote
                d = i["local"]["dst"]
                s = i["remote"]["src"]
                ss = "{}:{}".format(s["ip"], s["pt"])
                ds = "{}:{}".format(d["ip"], d["pt"])
                #print "--- creating remote_connection with src_not as remote"
                ##print "\t\t\t\tsrc: {}".format(ss)
                #print "\t\t\t\tdst: {}".format(ds)
                i["src_node"] = \
                    myNode(
                        "remote_connection",
                        [ss, ds],
                        {"name": "{},{}".format(ss, ds)},
                        ts)
            if dst_ip in host_ips and src_ip in host_ips:
                #print "--- local connection between:"
                #print"\t\t\t\tcon : {} --> {}".\
                #    format(i["src_node"], i["dst_node"])
                myRel(i["src_node"],
                      "has_connection", i["dst_node"], ts)
            else:
                #print "--- remote connection between:"
                #print"\t\t\t\tcon : {} --> {}".\
                #    format(i["src_node"], i["dst_node"])
                myRel(i["src_node"],
                      "has_connection", i["dst_node"], ts)

    # networks
    def _parse_block_fs(self, proc_node, of, ts=None):
        myGraph = self.g
        myNode = self.g.myNode
        myRel = self.g.myRel
        getMountFromDeviceNumber = self.g.getMountFromDeviceNumber
        getDeviceFromDeviceNumber = self.g.getDeviceFromDeviceNumber
        isDeviceRemoteFileSystem = self.g.isDeviceRemoteFileSystem
        getMountFromDevice = self.g.getMountFromDevice
        #
        fsType = isDeviceRemoteFileSystem(of['D'])
        self.debug("(prs)   fs: {} type -> {}".format(of, fsType))
        if fsType:
            #self.debug("(prs)   RemoteDevice found")
            m = re.search(' \(([^)]+)\)$', of['n'])
            if m:
                sname = m.group(1)
                mname = getMountFromDevice(sname)
                self.debug("(prs)   fs: remote-fs mount: " +
                           "{} server-export: {}".format(mname, sname))
                remote_fs_node = myNode("remote_fs", sname,
                                        {"name": sname},
                                        ts)
                mount_node = myNode("mount", mname,
                                    {"name": mname},
                                    ts)
                myRel(proc_node, "has_open", mount_node, ts)
                myRel(mount_node, "is_mounted_from", remote_fs_node, ts)
                myRel(myGraph.host(), "has_mount", mount_node, ts)
            else:
                pass
        else:
            #self.debug("(prs)   normal fs found")
            mname = getMountFromDeviceNumber(of['D'])
            dname = getDeviceFromDeviceNumber(of['D'])
            mount_node = myNode("mount", mname, {"name": mname}, ts)
            device_node = myNode("device", dname, {"name": dname}, ts)
            myRel(proc_node, "has_open", mount_node, ts)
            myRel(mount_node, "is_mounted_from", device_node, ts)
            myRel(myGraph.host(), "has_mount", mount_node, ts)
            myRel(myGraph.host(), "has_device", device_node, ts)

