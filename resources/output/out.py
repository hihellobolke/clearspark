import json
from resources import constants
from resources import common
import logging
import os
import inspect
import re
import platform
MODULEFILE = re.sub('\.py', '',
                            os.path.basename(inspect.stack()[0][1]))


def output_json(graph_db,
                out_file_nodes,
                out_file_rels,
                as_list=False):
    ldict = {"step": MODULEFILE + "/" + inspect.stack()[0][3],
             "hostname": platform.node().split(".")[0]}
    l = logging.LoggerAdapter(common.fetch_lg(), ldict)
    if out_file_nodes is None:
        out_file_nodes = "/dev/stdout"
    if out_file_rels is None:
        out_file_rels = "/dev/stdout"
    nod_obj = {}
    rel_obj = {}
    l.info("Iterating all nodes and rels")
    for r in constants.CONST_REL_TYPE:
        for k, v in graph_db(r).iteritems():
            rel_obj[v.id()] = v
            nod_obj[v.srcId()] = \
                graph_db(constants.getNodeType(v.srcType()))[v.srcHash()]
            nod_obj[v.dstId()] = \
                graph_db(constants.getNodeType(v.dstType()))[v.dstHash()]

    if as_list:
        return (_convert_node_json(nod_obj, l),
                _convert_rel_json(rel_obj, nod_obj, l))

    if out_file_nodes:
        nodes_list = _convert_node_json(nod_obj, l)
        with open(out_file_nodes, 'w') as w:
            for n in nodes_list:
                w.write(n + "\n")
            l.info("wrote node list as json to {}".format(out_file_nodes))

    if out_file_rels:
        rels_list = _convert_rel_json(rel_obj, nod_obj, l)
        with open(out_file_rels, 'w') as w:
            for r in rels_list:
                w.write(r + "\n")
            l.info("wrote rel list as json to {}".format(out_file_rels))


def _convert_node_json(nod_obj, l):
    l.info("convering nodes to json list")
    retval = list()
    for k, v in nod_obj.iteritems():
        retval.append(
            json.dumps({
                "id": v.id(),
                "hash": v.hash(),
                "node_type": v.typeStr(),
                "discovered_on": v.discoveredOn(),
                "name": v.property("name"),
                "properties": v.property()}
                )
            )
    return retval


def _convert_rel_json(rel_obj, nod_obj, l):
    l.info("convering rels to json list")
    retval = list()
    for k, v in rel_obj.iteritems():
        retval.append(
            json.dumps({
                "id": v.id(),
                "src_id": v.srcId(),
                "dst_id": v.dstId(),
                "hash": v.hash(),
                "rel_type": v.typeStr(),
                "discovered_on": v.discoveredOn(),
                "seen_on": v.seenOn(),
                "seen_count": v.seenCount(),
                "name": nod_obj[v.srcId()].property("name") +
                "-[:{}]->".format(v.typeStr()) +
                nod_obj[v.dstId()].property("name"),
                "properties": v.property()
                })
            )
    return retval
