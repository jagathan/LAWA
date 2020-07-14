package org.ics.isl.partitioner

import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession
import org.ics.isl.partitioner.SchemaGraph.{SE, SV}
import org.ics.isl.utils.SparkSessionWrapper
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter

class SchemaGraphSuite extends AnyFunSuite with SparkSessionWrapper with BeforeAndAfter {
  private val dataPath = "src/test/resources/rawtriples"

  var g: Graph[SV, SE] = _

  before {
    implicit val session: SparkSession = spark
    g = SchemaGraph.generateGraph(dataPath + "/graph.nt")
  }

  test("schema graph with 5 vertices: <John>,<Person>,<Software Engineer>,<Kostas>,<Giorgos>") {
    implicit val session: SparkSession = spark
    val vertices = g.vertices.map(v => v._2.uri).collect.sorted

    val expectedVertices = Array(
      "<John>",
      "<Person>",
      "<Software Engineer>",
      "<Kostas>",
      "<Giorgos>").sorted

    assert(g.numVertices == 5)
    assert(vertices.deep == expectedVertices.deep)
  }

  test("schema graph with 5 edges with initial 0 weight"){
    val edges = g.edges.collect
    val edgesWeight = edges.map(e => e.attr.weight).last

    assert(g.numEdges == 5)
    assert(edgesWeight == 0.0)
  }

  test("schema graph with 3 distinct edge uris") {
    val edgesUris = g.edges.map(e => e.attr.uri).distinct.collect
    assert(edgesUris.length == 3)
  }

  test("<John> is connected with <Kostas> with the 1 edge: <hasFriend>") {
    val result = g.triplets.filter(t => t.srcAttr.uri == "<John>" & t.dstAttr.uri == "<Kostas>")
      .map(t => t.attr.uri)
    assert(result.count == 1)
    assert(result.collect.last == "<hasFriend>")
  }
}
