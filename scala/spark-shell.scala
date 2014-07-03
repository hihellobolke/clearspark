
export SPARK_CLASSPATH=/home/epiclulz/local/cs/lib/cs.jar
export ADD_JARS=/home/epiclulz/local/cs/lib/cs.jar
/home/epiclulz/local/spark/bin/spark-shell --master spark://solo-kumbu:7077 --jars $ADD_JARS

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._

//--
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD

//-from-here
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats
import scala.util.control.NonFatal
import scala.util.Try

//import org.json4s.Format
import org.apache.spark.SparkContext._
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonAST._
implicit lazy val formats = org.json4s.DefaultFormats
//--
import org.clearspace.spark._


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


def nodeParser(s: String): Option[(Long, (String, String, Long, Long, Map[String, String]))] = {
  Try {
    val p = parse(s).extract[NodeFromJson]
    (p.id, (p.name, p.node_type,
      p.discovered_on, p.id,
      p.properties))
  }.toOption

}


def nodeParser(s: String): Boolean = {

    val p = Try(parse(s).extract[NodeFromJson]) match {
    case Success(lines) => True
    case Failure(_) => False
  }

}


def nodeParser(s: String): (Long, (String, String, Long, Long, Map[String, String])) = {

    val p = parse(s).extract[NodeFromJson]
    (p.id, (p.name, p.node_type,
      p.discovered_on, p.id,
      p.properties))
}

val nodeFile = sc.textFile("/home/epiclulz/workspace/tmp/nodes.json")
val vertexes: RDD[(VertexId, (String, String, Long, Long, Map[String, String]))] = sc.parallelize(nodeFile.flatMap(a => nodeParser(a)))

val vertexes: RDD[(VertexId, (String, String, Long, Long, Map[String, String]))] = nodeFile.map(a => nodeParser(a))

vertexes.count

def relParser(s: String): Option[Edge[(String, String, Int, Long, Long, Long, Long, Long, Map[String, String])]] = {
  Try {
    val p = parse(s).extract[RelFromJson];
    Edge(p.src_id, p.dst_id,
      (p.name, p.rel_type, p.seen_count,
        p.discovered_on, p.seen_on, p.id,
        p.src_id, p.dst_id, p.properties))
  }.toOption
}

    val nodeFile = sc.textFile("/home/epiclulz/workspace/tmp/nodes.json")
    val relFile = sc.textFile("/home/epiclulz/workspace/tmp/rels.json")

        val vertexes: RDD[(VertexId, (String, String, Long, Long, Map[String, String]))] = nodeFile.flatMap(a => nodeParser(a))
        val relationships: RDD[Edge[(String, String, Int, Long, Long, Long, Long, Long, Map[String, String])]] = relFile.flatMap(a => relParser(a))

    val defaultUser = ("John Doe", "Missing", 0, 0, 0, Map("Nothing" -> "Here"))
    val graph = Graph(vertexes, relationships)
