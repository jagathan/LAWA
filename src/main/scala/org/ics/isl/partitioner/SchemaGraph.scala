package org.ics.isl.partitioner

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SchemaGraph {

  case class SchemaVertex(id: VertexId, uri: String)
  case class SchemaEdge(src: VertexId, dst: VertexId, e: (String, Double))

  case class SV(uri: String)
  case class SE(attr: (String, Double))


  /**
   * Generates schema Graph from the given path of triples
   *
   * @param triplesPath HDFS path of schema triples
   * @param hdfsBasePath the HDFS base path (working dir)
   * @param spark implicit param SparkSession
   * @return
   */
  def generateGraph(triplesPath: String, hdfsBasePath: String)
                   (implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val schemaDs = RDFTriple.loadTriplesDs(triplesPath)

    // !RDD used for GraphX compatibility!

    // Dataset of all distinct vertex
    val vertexDs = schemaDs.flatMap{t => Seq(t.s, t.o)}.distinct
    // Transform to rdd and zip vertex with ID
    val vertexRdd = vertexDs.rdd.zipWithIndex.cache

    val vertexMap = vertexRdd.collectAsMap
    // broadcast map of vertex for performance
    val brVertexMap = spark.sparkContext.broadcast(vertexMap)

    // Build RDD[Edge]
    val schemaEdgeDs = schemaDs
      .map(t => SchemaEdge(brVertexMap.value(t.s), brVertexMap.value(t.o), (t.p, 0.0)))
      .distinct // TODO remove?

    val schemaVertexDs = vertexRdd.map{case(uri, id) => SchemaVertex(id, uri)}.toDS

    HdfsUtils.writeDs(schemaEdgeDs, hdfsBasePath + Constants.SCHEMA_EDGES)
    HdfsUtils.writeDs(schemaVertexDs, hdfsBasePath + Constants.SCHEMA_VERTICES)
  }

  /**
   * Loads schema graph from the given hdfs path
   *
   * @param hdfsBasePath the HDFS base path (working dir)
   * @param spark implicit param SparkSession
   * @return
   */
  def loadGraph(hdfsBasePath: String)(implicit spark: SparkSession): Graph[SV, SE] = {
    import spark.implicits._

    val vertexDs = HdfsUtils.loadDf(hdfsBasePath + Constants.SCHEMA_VERTICES)
      .as[SchemaVertex]
    val edgeDs = HdfsUtils.loadDf(hdfsBasePath + Constants.SCHEMA_EDGES)
      .as[SchemaEdge]

    // Transform to RDD for GraphX
    val vertexRdd = vertexDs.map(v => (v.id, v.uri)).rdd
    val edgeRdd = edgeDs.map(e => Edge(e.src, e.dst, e.e)).rdd

    Graph(vertexRdd, edgeRdd).asInstanceOf[Graph[SV, SE]]
  }

}