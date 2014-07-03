from resources import constants
from resources import common
import logging
import os
import inspect
import re
import platform
from py2neo import neo4j

MODULEFILE = re.sub('\.py', '',
                            os.path.basename(inspect.stack()[0][1]))


def output_neo4j(graph_db, registry, neo4j_url=None, debug=False):
    ldict = {"step": MODULEFILE + "/" + inspect.stack()[0][3],
             "hostname": platform.node().split(".")[0]}
    l = logging.LoggerAdapter(common.fetch_lg(), ldict)
    nod_obj = {}
    rel_obj = {}
    nod_neo = {}
    #rel_neo = {}
    for r in constants.CONST_REL_TYPE:
        for k, v in graph_db(r).iteritems():
            rel_obj[v.id()] = v
            nod_obj[v.srcId()] = \
                graph_db(constants.getNodeType(v.srcType()))[v.srcHash()]
            nod_obj[v.dstId()] = \
                graph_db(constants.getNodeType(v.dstType()))[v.dstHash()]
    if neo4j_url is None:
        neo4j_url = registry.get_config("neo4j_url",
                                        'http://localhost:7474/db/data')
    else:
        l.info("Updating registry with neo4j_url: {}".format(neo4j_url))
        registry.put_config("neo4j_url",
                            neo4j_url)
    l.info("Connecting to neo4j url {}".format(neo4j_url))
    neo4j_db = neo4j.GraphDatabaseService(neo4j_url)
    batch = neo4j.WriteBatch(neo4j_db)
    # write all nodes
    for k, v in nod_obj.iteritems():
        batch.\
            get_or_create_indexed_node("vid",
                                       "vid",
                                       k,
                                       v.property())
    l.info("Submitting node data to neo4j")
    result = batch.submit()
    if debug:
        results = "\t\t"
        for i in result:
            results += "\n\t\t{}".format(i)
        l.info("Got all node: {}".format(results))

    for k, v in zip(nod_obj.keys(), result):
        nod_neo[k] = v

    # add node labels and index it
    batch = neo4j.WriteBatch(neo4j_db)
    for k, v in nod_obj.iteritems():
        batch.set_labels(nod_neo[k],
                         constants.getNodeType(v.type()))
        batch.add_to_index(neo4j.Node,
                           "Name",
                           "name",
                           v.property()["name"],
                           nod_neo[k])
    l.info("Submitting node index/label data to neo4j")
    result = batch.submit()

    # add rel
    batch = neo4j.WriteBatch(neo4j_db)
    for k, v in rel_obj.iteritems():
        batch.get_or_create_path(nod_neo[v.srcId()],
                                 constants.getRelType(v.type()),
                                 nod_neo[v.dstId()])
    l.info("Submitting rel data to neo4j")
    result = batch.submit()

    if debug:
        results = "\t\t"
        for i in result:
            results += "\n\t\t{}".format(i)
        l.info("Got all node: {}".format(results))

"""
def list_duplicates(seq):
    tally = defaultdict(list)
    for i,item in enumerate(seq):
        tally[item].append(i)
    return ((key,locs) for key,locs in tally.items()
                            if len(locs)>1)

for dup in sorted(list_duplicates(source)):
    print dup
        # = src_node_obj
        batch.\
            get_or_create_indexed_node("vid",
                                       "vid",
                                       src_node_obj.id(),
                                       src_node_obj.property())
        batch.\
            get_or_create_indexed_node("vid",
                                       "vid",
                                       dst_node_obj.id(),
                                       dst_node_obj.property())
        nodes = batch.submit()
        l.info("Nodes: Submitted batch result: {}".format(nodes))
        if nodes[0] is not None and nodes[1] is not None:
            s = nodes[0]
            d = nodes[1]
            batch = neo4j.WriteBatch(neo4j_db)
            batch.set_labels(s,
                             constants.getNodeType(src_node_obj.type()))
            batch.add_to_index(neo4j.Node,
                               "Name",
                               "name",
                               src_node_obj.property()["name"],
                               s)
            batch.set_labels(d,
                             constants.getNodeType(dst_node_obj.type()))
            batch.add_to_index(neo4j.Node,
                               "Name",
                               "name",
                               dst_node_obj.property()["name"],
                               d)
            #l.debug("src_node: {}, \nproperty: {}".format(src_node_obj,
            #        src_node_obj.property()))
            #l.debug("dst_node: {}, \nproperty: {}".format(dst_node_obj,
            #        dst_node_obj.property()))
            indexes = batch.submit()

            l.info("Index: Submitted batch result: {}".format(indexes))

            batch = neo4j.WriteBatch(neo4j_db)
            p = batch.get_or_create_path(nodes[0],
                             constants.getRelType(v.type()),
                             nodes[1])
            l.info("Rel: Submitted batch result: {}".format(batch.submit()))


#storage = ZODB.FileStorage.FileStorage('/tmp/clear.db')
"""
