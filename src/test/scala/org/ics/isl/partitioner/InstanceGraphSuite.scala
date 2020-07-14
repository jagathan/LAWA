package org.ics.isl.partitioner

import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession
import org.ics.isl.partitioner.InstanceGraph.{IE, IV}
import org.ics.isl.utils.SparkSessionWrapper
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter

class InstanceGraphSuite extends AnyFunSuite with SparkSessionWrapper with BeforeAndAfter {

  private val dataPath = "src/test/resources/rawtriples"

  var g: Graph[IV, IE] = _

  before {
    implicit val session: SparkSession = spark
    g = InstanceGraph.generateGraph(dataPath + "/graph.nt")
  }

  test("instance graph has with 3 vertices : <John>,<Kostas>,<Giorgos>") {
    implicit val session: SparkSession = spark

    val vertices = g.vertices.map(v => v._2.uri).collect.sorted

    val expectedVertices = Array(
      "<John>",
      "<Kostas>",
      "<Giorgos>").sorted

    assert(g.numVertices == 3)
    assert(vertices.deep == expectedVertices.deep) // vertices
  }

  test("instance graph has 3 edges ") {
    val edges = g.edges.collect
    assert(g.numEdges == 3)
  }

  test("instance graph has 2 distinct edge uris ") {
    val edgesUris = g.edges.map(e => e.attr.uri).distinct.collect
    assert(edgesUris.length == 2) // distinct edges
  }

  test("vertex <John> has rdf_types <Person>,<Software Engineer>") {
    // types of John
    val query1 = g.vertices.filter(v => v._2.uri == "<John>")
      .flatMap(v => v._2.types).collect.sorted

    val query1Expected = Array("<Person>", "<Software Engineer>").sorted
    // Test queries
    assert(query1.deep == query1Expected.deep)
  }

  test("vertex <John> does not have labels"){
    // labels of John
    val query2 = g.vertices.filter(v => v._2.uri == "<John>")
      .flatMap(v => v._2.literals).collect.sorted
    assert(query2.deep == Array[(String, String)]().deep)
  }
}
