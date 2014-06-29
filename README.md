# clearspark - A Dependency Mapping for your Datacenter

This is alpha level version of dependency mapping tool. This python package installs the clearspark module/agent which runs on a host, gathers all the process running and sends the output to either Kafka, Neo4j or dumps as JSON, for creating the dependency graph. It provides a sample scala program that can be run on Apache Spark - GraphX to generate the graph from the Kafka/JSON input.

To calculate dependencies between multiple host, it uses etcd as a arbitrator for vertex information. Each agent would communicate with it, to determine vertex ids. And once the local dependency graph is created on that host (for that host), it outputs the data to Kafka/JSON/Neo4j to create the Global Graph.

As an analyst one would query the Global Graph, to determine dependencies at datacenter level.

# License

Copyright 2014, Gautam R Singh under Apache License, v2.0. See `LICENSE`

# Status

The current version of is ALPHA **0.1100**

# Requirements

Quite a few, to enable a fully functional system, one would need:

- A Neo4j db or
- Kafka and Apache Spark cluster

For the module itself
- etcd
- Python 2.7.*
- netifaces, py2neo, kazoo, etcd-python, kafka-python client,
- ZODB

# Usage

## High level
Download/git clone it, ensure all requirements are present.

For a single host setup:

```
cd clearspark

python main.py --etcdhost your-etcd-host --etcdport 4001 --outputneo4j --outputneo4jurl http://your-db-host:7474/db/data -vv

```

Check out your neo4jdb http://your-db-host:7474/browser/

