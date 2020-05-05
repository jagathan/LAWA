package org.ics.isl.partitioner

import org.apache.spark.sql.SparkSession
import org.ics.isl.utils.SparkSessionWrapper
import org.scalatest.funsuite.AnyFunSuite

class InstanceGraphSuite extends AnyFunSuite with SparkSessionWrapper{

  private val dataPath = "src/test/resources/rawtriples"

  test("Generates an instance graph with 5 edges and 5 vertices") {
    implicit val session: SparkSession = spark

    val g = InstanceGraph.generateGraph(dataPath + "/graph.nt")

    val vertices = g.vertices.map(v => v._2.uri).collect.sorted

    val expectedVertices = Array(
      "<John>",
      "<Kostas>",
      "<Giorgos>").sorted

    val edges = g.edges.collect
    val edgesUris = edges.map(e => e.attr.uri).distinct

    // types of John
    val query1 = g.vertices.filter(v => v._2.uri == "<John>")
        .flatMap(v => v._2.types).collect.sorted

    // labels of John
    val query2 = g.vertices.filter(v => v._2.uri == "<John>")
      .flatMap(v => v._2.literals).collect.sorted

    val query1Expected = Array("<Person>", "<Software Engineer>").sorted

    // Tests vertices
    assert(g.numVertices == 3)
    assert(vertices.deep == expectedVertices.deep) // vertices

    // Test edges
    assert(g.numEdges == 3)
    assert(edgesUris.length == 2) // distinct edges

    // Test queries
    assert(query1.deep == query1Expected.deep)
    assert(query2.deep == Array[(String, String)]().deep)
  }
}
