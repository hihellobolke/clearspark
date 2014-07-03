from resources import common
import logging
import os
import inspect
import re
import platform

from kafka.client import KafkaClient
from kafka.producer import SimpleProducer

from resources.output import out

MODULEFILE = re.sub('\.py', '',
                            os.path.basename(inspect.stack()[0][1]))


def output_kafka(graph_db, registry,
                 kafka_url=None):
    ldict = {"step": MODULEFILE + "/" + inspect.stack()[0][3],
             "hostname": platform.node().split(".")[0]}
    l = logging.LoggerAdapter(common.fetch_lg(), ldict)
    kafka_topic = "cs"
    if kafka_url is None:
        kafka_url = registry.get_config("kafka_url",
                                        "localhost:9092")
    else:
        l.info("Updating registry with kafka_url: {}".format(kafka_url))
        registry.put_config("kafka_url",
                            kafka_url)
    (nodes, rels) = out.output_json(graph_db, None, None, as_list=True)
    l.info("Connecting to kafka_url {}".format(kafka_url))
    kafka = KafkaClient(kafka_url)
    # To send messages asynchronously
    producer = SimpleProducer(kafka)
    l.info("Sending nodes to kafka {}/{}".format(kafka_url, kafka_topic))
    for n in nodes:
        producer.send_messages(kafka_topic, n)
    l.info("Sending rels to kafka {}/{}".format(kafka_url, kafka_topic))
    for n in rels:
        producer.send_messages(kafka_topic, n)
    kafka.close()
