package org.ics.isl.partitioner

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SchemaGraph {

  case class SchemaVertex(id: VertexId, uri: String)
  case class SchemaEdge(src: VertexId, dst: VertexId, e: (String, Double))

  case class SV(uri: String)
  case class SE(uri: String, weight: Double)

  final val SCHEMA_VERTICES = "schema_vertices"
  final val SCHEMA_EDGES = "schema_edges"


  def generateGraph(triplesPath: String)
                   (implicit spark: SparkSession): Graph[SV, SE] = {
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
    val schemaEdgeRdd: RDD[Edge[SE]] = schemaDs
      .map(t => Edge(brVertexMap.value(t.s), brVertexMap.value(t.o), SE(t.p, 0.0)))
      .distinct.rdd// TODO remove?

    val schemaVertexRdd = vertexRdd.map{case(uri, id) => (id, SV(uri))}

    Graph(schemaVertexRdd, schemaEdgeRdd)
  }

  /**
   *
   * @param g
   * @param hdfsBasePath
   * @param spark
   */
  def writeGraph(g: Graph[SV, SE], hdfsBasePath: String)
                (implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val vertexDs = g.vertices.map(v => SchemaVertex(v._1, v._2.uri)).toDS
    val edgeDs = g.edges.map(e => SchemaEdge(e.srcId, e.dstId, (e.attr.uri, e.attr.weight))).toDS

    HdfsUtils.writeDs(edgeDs, hdfsBasePath + SCHEMA_EDGES)
    HdfsUtils.writeDs(vertexDs, hdfsBasePath + SCHEMA_VERTICES)
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

    val vertexDs = HdfsUtils.loadDf(hdfsBasePath + SCHEMA_VERTICES)
      .as[SchemaVertex]
    val edgeDs = HdfsUtils.loadDf(hdfsBasePath + SCHEMA_EDGES)
      .as[SchemaEdge]

    // Transform to RDD for GraphX
    val vertexRdd: RDD[(VertexId, SV)] = vertexDs.map(v => (v.id, SV(v.uri))).rdd
    val edgeRdd: RDD[Edge[SE]] = edgeDs.map(e => Edge(e.src, e.dst, SE(e.e._1, e.e._2))).rdd

    Graph(vertexRdd, edgeRdd)
  }

}