package org.ics.isl.partitioner

import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession
import org.ics.isl.partitioner.InstanceGraph.{IE, IV}
import org.ics.isl.partitioner.SchemaGraph.{SE, SV}
import org.ics.isl.utils.SparkSessionWrapper
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class GraphMetricsUtilsSuite extends AnyFunSuite with SparkSessionWrapper with BeforeAndAfter {

  private var instanceGraph: Graph[IV, IE] = _
  private var schemaGraph: Graph[SV, SE] = _

  private val dataPath = "src/test/resources/rawtriples"

  before {
    implicit val session: SparkSession = spark
    instanceGraph = InstanceGraph.generateGraph(dataPath + "/graph.nt")
    schemaGraph = SchemaGraph.generateGraph(dataPath + "/graph.nt")
  }

  test("Graph with 2 distinct types with 1 instance each") {
    val resultMap = GraphMetricsUtils.computeSchemaNodeCount(instanceGraph)

    assert(resultMap.size == 2)
    assert(resultMap("<Person>") == 1)
    assert(resultMap("<Software Engineer>") == 1)
  }


  test("Graph with 2 distinct types with frequency 1 each") {
    val resultMap = GraphMetricsUtils.computeSchemaNodeFreq(instanceGraph)

    assert(resultMap.size == 2)
    assert(resultMap("<Person>") == 1)
    assert(resultMap("<Software Engineer>") == 1)
  }

  test("Importance of 2 schema nodes") {
    implicit val session: SparkSession = spark
    // use this function the same way it is done in code
    def normalizeValue(value: Double, min: Double, max: Double): Double =
      (value - min) / (max - min)

    // mock input
    val nodeFreqMap = Map("<Dummy>" -> 0,"<Person>" -> 1, "<Software Engineer>" -> 2)
    val bcMap = Map("<Person>" -> 0.8, "<Software Engineer>" -> 0.4)
    // generate importance of schemaGraph
    val resultMap = GraphMetricsUtils.computeNodeImportance(schemaGraph, nodeFreqMap, bcMap)
    // Find which VertexIds from schema graph correspond to the uris used
    val schemaVertexMap = schemaGraph.vertices.map{case(id, v) => (v.uri, id)}.collectAsMap
    val personID = schemaVertexMap("<Person>")
    val softwareEngineerID = schemaVertexMap("<Software Engineer>")

    // normalize person value for bc and freq values (see above maps)
    val personNormImp = normalizeValue(0.8, 0.4, 0.8) + normalizeValue(1, 0, 2)
    // normalize software engineer value for bc and freq values (see above maps)
    val softwareEngineerNormImp = normalizeValue(0.4, 0.4, 0.8) + normalizeValue(2, 0, 2)

    assert(resultMap.size == 2)
    assert(resultMap(personID) == personNormImp)
    assert(resultMap(softwareEngineerID) == softwareEngineerNormImp)
  }
}

