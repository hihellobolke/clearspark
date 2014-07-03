package org.clearspace.spark

import org.apache.spark._
import org.apache.spark.SparkContext._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD

import org.json4s._
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonAST._
import scala.util.control.NonFatal
import scala.util.Try

object ClearSpacer {

  def main(args: Array[String]): Unit = {

    implicit val formats = org.json4s.DefaultFormats

    case class NodeFromJson(
      hash: Long,
      name: String,
      discovered_on: Long,
      properties: Map[String, String],
      node_type: String,
      id: Long)

    case class RelFromJson(
      rel_type: String,
      properties: Map[String, String],
      src_id: Long,
      name: String,
      discovered_on: Long,
      hash: Long,
      seen_count: Int,
      seen_on: Long,
      id: Long,
      dst_id: Long)

    def nodeJsonTester(s: String): Boolean = {
      val result = Try({
        implicit val formats = org.json4s.DefaultFormats
        parse(s).extract[NodeFromJson]
      })
      if (result.isSuccess) { true } else { false }
    }

    def nodeParser(s: String): Option[(Long, (String, String, Long, Long, Map[String, String]))] = {
      Try {
        implicit val formats = org.json4s.DefaultFormats
        val p = parse(s).extract[NodeFromJson]
        (p.id, (p.name, p.node_type,
          p.discovered_on, p.id,
          p.properties))
      }.toOption
    }

    def relParser(s: String): Option[Edge[(String, String, Int, Long, Long, Long, Long, Long, Map[String, String])]] = {
      Try {
        implicit val formats = org.json4s.DefaultFormats
        val p = parse(s).extract[RelFromJson];
        Edge(p.src_id, p.dst_id,
          (p.name, p.rel_type, p.seen_count,
            p.discovered_on, p.seen_on, p.id,
            p.src_id, p.dst_id, p.properties))
      }.toOption
    }

    val master = "spark://solo-kumbu:7077"

    val sparkConf = new SparkConf().setAppName("ClearSpacer").setMaster(master)
    val sc = new SparkContext(sparkConf)

    val nodeFile = sc.textFile("/home/epiclulz/workspace/tmp/nodes.json")
    val relFile = sc.textFile("/home/epiclulz/workspace/tmp/rels.json")

    val relationships: RDD[Edge[(String, String, Int, Long, Long, Long, Long, Long, Map[String, String])]] = relFile.flatMap(a => relParser(a))
    val vertexes: RDD[(VertexId, (String, String, Long, Long, Map[String, String]))] = nodeFile.flatMap(a => nodeParser(a))

    val defaultUser = ("John Doe", "Missing", 0, 0, 0, Map("Nothing" -> "Here"))
    val graph = Graph(vertexes, relationships)

    println("Graph edge count is " + graph.edges.count())
    println("Graph node count is " + graph.vertices.count())

    val graphOps = new GraphOps(graph)
    graphOps.stronglyConnectedComponents(3)

    graph.triplets.map(
      triplet => triplet.srcAttr._1 + "(type: " + triplet.srcAttr._2 + ")" +
        " is the " + triplet.attr + " of " +
        triplet.dstAttr._1 + "(type: " + triplet.dstAttr._2 + ")").collect.foreach(println(_))

  }

}
