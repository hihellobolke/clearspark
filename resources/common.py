from __future__ import print_function
from __future__ import absolute_import
import os
import re
import subprocess
import threading
import logging
import socket
import inspect
import tempfile
import stat
import shlex
import platform
from . import config

#from common import *

MODULEFILE = re.sub('\.py', '',
                            os.path.basename(inspect.stack()[0][1]))
DEBUG = config.DEBUG
STRICT = config.STRICT


def debugprint(log_level=None):
    def dprint(lines=[], module=""):
        if type(lines) is not list:
            lines = [lines]
        if len(module) != 0:
            module = "[{}]".format(module)
        if len(lines) == 0:
            print("{}:".format(log_level), module, "-" * 40)
        else:
            for line in lines:
                print("{}:".format(log_level), module, line)
        return []

    def noop(a=[], b=""):
        return []

    if log_level == "DEBUG" or DEBUG:
        return dprint
    else:
        return noop


def timeout(func, args=(), kwargs={},
            timeout_duration=10, default=None, log=None):
    """This function will spawn a thread and run the given function
    using the args, kwargs and return the given default value if the
    timeout_duration is exceeded.
    """
    class InterruptableThread(threading.Thread):

        def __init__(self):
            threading.Thread.__init__(self)
            self.result = default

        def run(self):
            self.result = func(*args, **kwargs)
    try:
        if log:
            log.info("Starting tiemoutthread for '{}' timeout in {}s".format(
                func.__name__, timeout_duration))
        it = InterruptableThread()
        it.start()
        it.join(timeout_duration)
        if it.isAlive():
            return it.result
        else:
            return it.result
    except:
        if log:
            log.warning("Exception occurred in timerthread for '{}'".format(
                func.__name__))
        return default


def _execute(cmd, stdout=None, stderr=None, shell=False):
    retval = None
    if stderr is None:
        stderr = stdout
    try:
        proc = subprocess.Popen(cmd,
                                universal_newlines=True,
                                stdout=stdout,
                                stderr=stderr,
                                shell=shell)
        retval = proc.wait()
    except Exception as _:
        if STRICT:
            raise
        if DEBUG:
            print("Exception has occurred in _execute(cmd, stdout, stderr)")
            dp = debugprint()
            dp("Error in executing '{}' exception is '{}'".
               format(cmd, _), "_execute")
        pass
    return retval


def _read_unlink_handle(fh, fn):
    retval = []
    try:
        with os.fdopen(fh) as fd:
            fd.seek(0)
            retval = fd.readlines()
    except:
        if STRICT:
            raise
        pass
    finally:
        try:
            os.unlink(fn)
        except:
            pass
    return retval


class Command(object):
    def __init__(self, cmd, log=False, shell=False):
        self.cmd = cmd
        self.shell = shell
        self.process = None
        self.output = []
        self.errput = []
        self.errfile_handle, self.errfile_name = tempfile.mkstemp()
        self.outfile_handle, self.outfile_name = tempfile.mkstemp()

    def run(self, maxtime=30):
        self.retval = timeout(_execute,
                              args=(self.cmd,
                                    self.outfile_handle,
                                    self.errfile_handle,
                                    self.shell),
                              timeout_duration=maxtime)
        self.output = _read_unlink_handle(self.outfile_handle,
                                          self.outfile_name)

        self.errput = _read_unlink_handle(self.errfile_handle,
                                          self.errfile_name)
        return self.retval


def run_command(args, split=True):
    if isinstance(args, basestring):
        if split:
            args = shlex.split(args)
        args = args
    try:
        if split:
            prg = Command(args)
        else:
            prg = Command(args, shell=True)
        prg.run()
    except Exception as _:
        if STRICT:
            raise
        if DEBUG:
            dp = debugprint()
            dp("Error in executing '{}' exception is '{}'".
               format(args, _), "run_command")
        rc = 1
        out = "UnknownError"
        err = "UnknownError"
    else:
        rc, out, err = prg.retval, ''.join(prg.output), ''.join(prg.errput)
    return (rc, out, err)


def is_executable(path):
    '''is the given path executable?'''
    return (stat.S_IXUSR & os.stat(path)[stat.ST_MODE]
            or stat.S_IXGRP & os.stat(path)[stat.ST_MODE]
            or stat.S_IXOTH & os.stat(path)[stat.ST_MODE])


def get_bin_path(arg, required=False, opt_dirs=[]):
    '''
    find system executable in PATH.
    Optional arguments:
       - required:  if executable is not found and required is true, fail_json
       - opt_dirs:  optional list of directories to search in addition to PATH
    if found return full path; otherwise return None
    '''
    sbin_paths = ['/sbin', '/usr/sbin', '/usr/local/sbin']
    paths = []
    for d in opt_dirs:
        if d is not None and os.path.exists(d):
            paths.append(d)
    paths += os.environ.get('PATH', '').split(os.pathsep)
    bin_path = None
    # mangle PATH to include /sbin dirs
    for p in sbin_paths:
        if p not in paths and os.path.exists(p):
            paths.append(p)
    for d in paths:
        path = os.path.join(d, arg)
        if os.path.exists(path) and is_executable(path):
            bin_path = path
            break
    return bin_path


def pretty_bytes(size=0):
    ranges = ((1 << 70L, 'ZB'),
              (1 << 60L, 'EB'),
              (1 << 50L, 'PB'),
              (1 << 40L, 'TB'),
              (1 << 30L, 'GB'),
              (1 << 20L, 'MB'),
              (1 << 10L, 'KB'),
              (1, 'Bytes'))
    for limit, suffix in ranges:
        if size >= limit:
            break
    return '%.2f %s' % (float(size)/limit, suffix)


def fetch_dns(ip="127.0.0.1"):
    try:
        name = socket.gethostbyaddr(str(ip))[0]
    except:
        if re.match('^127\.[.0-9]*$', ip):
            name = "localhost"
        else:
            name = ip
    return name



#def fetchprocessor(modules=[]):
#    return fetcher(importer(modules))


def fetch_lg(name=None):
    return logging.getLogger("clearspark")


def setup_logger(name=None,
                 logfile=None,
                 console=True,
                 level=60,
                 ):
    log = fetch_lg()
    mylevel = 60 - level*20
    if mylevel < 10:
        log.setLevel(logging.DEBUG)
    elif mylevel >= 60:
        console = False
        log.setLevel(logging.CRITICAL)
    else:
        log.setLevel(mylevel)
    #
    if logfile is not None:
        fh = logging.FileHandler(file)
        fh.setLevel(logging.DEBUG)
        fmtf = logging.Formatter(
            '%(asctime)s - %(name)s ' +
            '%(hostname)-16s %(levelname)-8s ' +
            '@%(step)-30s %(message)s')
        fh.setFormatter(fmtf)
        log.addHandler(fh)
    #
    if console is True:
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        fmtc = logging.Formatter(
            '%(asctime)s - %(name)s ' +
            '%(hostname)-16s %(levelname)-8s ' +
            '@%(step)-30s %(message)s')
        ch.setFormatter(fmtc)
        log.addHandler(ch)
    return log

def parse_ip_output(output):
    """
    1: lo: <LOOPBACK,UP,LOWER_UP> mtu 16436 qdisc noqueue
    link/loopback 00:00:00:00:00:00 brd 00:00:00:00:00:00
    inet 127.0.0.1/8 scope host lo
    inet6 ::1/128 scope host
       valid_lft forever preferred_lft forever
2: eth0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc pfifo_fast qlen 1000
    link/ether 00:50:56:8e:01:af brd ff:ff:ff:ff:ff:ff
    inet 171.70.42.153/24 brd 171.70.42.255 scope global eth0
    inet6 fe80::250:56ff:fe8e:1af/64 scope link
       valid_lft forever preferred_lft forever
3: sit0: <NOARP> mtu 1480 qdisc noop
    link/sit 0.0.0.0 brd 0.0.0.0
    """
    out = {}
    cur = None
    for l in output:
        r = re.match('[0-9]+: ([a-z]+[0-9]*): <.*', l)
        if r:
            cur = r.group(1)
            out[cur] = []
            continue
        r = re.match(' *inet ([0-9\.]+).*', l)
        if r:
            if cur is None:
                continue
            else:
                out[cur].append(r.group(1))
                continue
        r = re.match(' *inet6 ([a-zA-Z0-9\[\]:\.]+).*', l)
        if r:
            if cur is None:
                continue
            else:
                out[cur].append(r.group(1))
                continue
    return out


def parse_device_number(d):
    retval = None
    if d[0:2] == '0x':
        # a hex nice
        d_minor = int(int(d, 16) & 0xff)
        d_major = int(int(d, 16) >> 8 & 0xff)
        retval = "{},{}".format(d_major, d_minor)
    elif re.match('[0-9]+,[0-9]+', d):
        retval = d
    return retval

# sudo awk  '!/fuse\./{"stat "$2" -c 0x%D" | getline ss; printf "%s %s %s %s\n"
#  , $1, $2, ss, $3}'  /proc/mounts  2>/dev/null


def parse_mt_output(output):
    _device_to_mount = {}
    _device_num_to_list = {}
    for l in output:
        parsed = shlex.split(l)
        device = parsed[0]
        mount = parsed[1]
        device_number = parsed[2]
        fs_type = parsed[3]
        _device_to_mount[device] = mount
        if fs_type not in ['nfs', 'aufs', 'cifs', 'pnfs', 'smbfs']:
            _device_num_to_list[parse_device_number(device_number)] = \
                [device, mount]
    return {"_device_to_mount": _device_to_mount,
            "_device_num_to_list": _device_num_to_list}
