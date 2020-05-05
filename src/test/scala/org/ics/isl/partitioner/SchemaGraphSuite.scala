package org.ics.isl.partitioner

import org.apache.spark.sql.SparkSession
import org.ics.isl.utils.SparkSessionWrapper
import org.scalatest.funsuite.AnyFunSuite

class SchemaGraphSuite extends AnyFunSuite with SparkSessionWrapper {
  private val dataPath = "src/test/resources/rawtriples"

  test("Generates a schema graph with 5 edges and 5 vertices") {
    implicit val session: SparkSession = spark

    val g = SchemaGraph.generateGraph(dataPath + "/graph.nt")

    val vertices = g.vertices.map(v => v._2.uri).collect.sorted

    val expectedVertices = Array(
      "<John>",
      "<Person>",
      "<Software Engineer>",
      "<Kostas>",
      "<Giorgos>").sorted

    val edges = g.edges.collect
    val edgesWeight = edges.map(e => e.attr.weight).last
    val edgesUris = edges.map(e => e.attr.uri).distinct

    // Query graph
    val result = g.triplets.filter(t => t.srcAttr.uri == "<John>" & t.dstAttr.uri == "<Kostas>")
      .map(t => t.attr.uri)

    // Tests vertices
    assert(g.numVertices == 5)
    assert(vertices.deep == expectedVertices.deep) //vertices

    // Test edges
    assert(g.numEdges == 5)
    assert(edgesUris.length == 3) //distinct edges
    assert(edgesWeight == 0.0)

    // Test query
    assert(result.count == 1)
    assert(result.collect.last == "<hasFriend>")
  }
}
