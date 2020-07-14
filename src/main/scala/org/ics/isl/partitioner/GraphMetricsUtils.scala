package org.ics.isl.partitioner

import org.apache.spark.graphx.{Graph, VertexId}
import org.ics.isl.partitioner.InstanceGraph.{IE, IV}
import org.ics.isl.partitioner.SchemaGraph.{SE, SV}
import ml.sparkling.graph.operators.measures.vertex.betweenness.hua.HuaBC
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.Map

object GraphMetricsUtils {

  case class CC(triple: RDFTriple.Triple, numInstances: Long, distinctNumInstances: Long )

  final val CC_STR = "cc"
  final val BC = "bc"
  final val SCHEMA_NODE_FREQ = "schema_node_freq"
  final val SCHEMA_NODE_COUNT = "schema_node_count"
  final val SCHEMA_NODE_IMPORTANCE = "schema_node_importance"


  /**
   *
   * @param schemaGraph
   * @param instanceGraph
   * @param hdfsBasePath
   * @param localBasePath
   * @param spark
   */
  def computeGraphMetrics(schemaGraph: Graph[SV, SE],
                          instanceGraph: Graph[IV, IE],
                          hdfsBasePath: String,
                          localBasePath: String)
                         (implicit spark: SparkSession): Unit = {

    val ccDs = computeCC(instanceGraph)
    writeCC(ccDs, hdfsBasePath)

    val bcMap = computeBC(schemaGraph)
    writeBC(bcMap, localBasePath)

    val schemaNodeFreq = computeSchemaNodeFreq(instanceGraph)
    writeSchemaNodeFreq(schemaNodeFreq, localBasePath)

    val schemaNodeCount = computeSchemaNodeCount(instanceGraph)
    writeSchemaNodeCount(schemaNodeCount, localBasePath)

    val nodeImportance = computeNodeImportance(schemaGraph, schemaNodeFreq, bcMap)
    writeNodeImportance(nodeImportance, localBasePath)
  }


  /**
   * Compute Betweennes centrality of the schema graph
   * and writes in a file in the local fileSystem
   * @param schemaGraph RDF schema Graph
   */
  def computeBC(schemaGraph: Graph[SV, SE]): Map[String, Double] = {
    val vertexMap = schemaGraph.vertices.map(v => (v._1, v._2.uri)).collectAsMap()

    val bcMap: Map[String, Double] = HuaBC.computeBC(schemaGraph)
      .sortBy(_._2, ascending = false) // sort by bc value
      .map(x => (vertexMap(x._1), x._2)) // replace vertexId with uri
      .collectAsMap()
    bcMap
  }


  /**
   * Loads betweenes centrality of schema graph from local file System
   * @param localBasePath base working dir of local file system
   * @return a Map[String, Double] -> (uri, bcValue)
   */
  def loadBC(localBasePath: String): Map[String, Double] = {
    IOUtils.loadMap(localBasePath + BC)
      .map{case(k: String, v: String) => (k, v.toString.toDouble)}
  }


  /**
   *
   * @param bcMap
   * @param localBasePath
   */
  private def writeBC(bcMap: Map[String, Double], localBasePath: String): Unit = {
    IOUtils.writeMap(bcMap, localBasePath  + BC)
  }


  /**
   *
   * @param instanceGraph Graph with RDF Instances
   */
  def computeCC(instanceGraph: Graph[IV, IE])
               (implicit spark: SparkSession): Dataset[CC] = {

    import spark.implicits._

    val typedTripleRdd = instanceGraph.triplets
      .filter(triplet =>
        triplet.srcAttr.types.nonEmpty &&
          triplet.dstAttr.types.nonEmpty)

    val typeTripleInstances: RDD[(RDFTriple.Triple, Seq[(Long, Long)])] =
      typedTripleRdd.flatMap(triplet =>
        yieldTripleTypes(triplet.srcAttr.types,
          triplet.attr.uri,
          triplet.dstAttr.types
        ).map((_, Seq((triplet.srcId, triplet.dstId)))))

    val ccDs: Dataset[CC] = typeTripleInstances.reduceByKey((a,b) => a ++ b)
      .map{case(t, instances) => CC(t, instances.size, instances.map(_._2).distinct.size)}
      .toDS
    ccDs
  }


  /**
   *
   * @param ccDs
   * @param hdfsBasePath
   */
  private def writeCC(ccDs: Dataset[CC], hdfsBasePath: String): Unit = {
    HdfsUtils.writeDs(ccDs, hdfsBasePath + CC_STR)
  }


  /**
   * Loads a Dataset with Cardinality Closeness of rdf_types
   * @param hdfsBasePath base working path of hdfs
   * @param spark implicit param Spark Session
   * @return Dataset[CC]
   */
  def loadCC(hdfsBasePath: String)(implicit spark: SparkSession): Dataset[CC] = {
    import spark.implicits._
    HdfsUtils.loadDf(hdfsBasePath + CC_STR).as[CC]
  }


  /**
   * Produce all possible combinations of srcTypeUri predicate and dstUriType
   *
   * @param srcTypes Types of src uri
   * @param predicate predicate of triple
   * @param dstTypes types of dst Uri
   * @return a list of Triples
   */
  private def yieldTripleTypes(
                                  srcTypes: Seq[String],
                                  predicate: String,
                                  dstTypes: Seq[String]): Seq[RDFTriple.Triple] = {
    // produce all combinations of Triples
    // from lists subjectType, objType with the given predicate
    for {
      subjectType <- srcTypes
      objType <- dstTypes
      if subjectType.nonEmpty && objType.nonEmpty
    } yield RDFTriple.Triple(subjectType, predicate, objType)
  }


  /**
   *
   * @param instanceGraph Graph with RDF Instances
   */
  def computeSchemaNodeFreq(instanceGraph: Graph[IV, IE]): Map[String, Int] = {
    // Rdd with all vertices that have at least one type
    val vertexAttrRdd = instanceGraph.triplets
      .filter(t => t.srcAttr.types.nonEmpty || t.dstAttr.types.nonEmpty)
      .flatMap(triplet => Seq(triplet.srcAttr, triplet.dstAttr))
    // Rdd with all types
    val vertexTypeRdd = vertexAttrRdd.filter(_.types.nonEmpty)
      .distinct
      .flatMap(_.types)

    // Rdd with frequency of a type
    val nodeTypeFreq = vertexTypeRdd.map(c => (c, 1))
      .reduceByKey(_+_)

    // Write Type frequency to local file
    // Distinct types should be small, so it can be collected
    nodeTypeFreq.sortBy(_._2, ascending = false).collectAsMap
  }


  /**
   *
   * @param nodeFreqMap
   * @param localBasePath
   */
  private def writeSchemaNodeFreq(nodeFreqMap: Map[String, Int], localBasePath: String): Unit = {
    val outputPath = localBasePath + SCHEMA_NODE_FREQ
    IOUtils.writeMap(nodeFreqMap, outputPath)
  }


  /**
   * Loads frequency of schema nodes from local file system
   * @param localBasePath base working dir of local filesystem
   * @return a Map[String, Int] of uri,frequency
   */
  def loadSchemaNodeFreq(localBasePath: String): Map[String, Int] = {
    IOUtils.loadMap(localBasePath + SCHEMA_NODE_FREQ)
      .map{case(k: String, v: String) => (k, v.toString.toInt)}
  }


  /**
   *
   * @param instanceGraph Graph with RDF Instances
   */
  def computeSchemaNodeCount(instanceGraph: Graph[IV, IE]): Map[String, Int] = {
    // Rdd with all vertices that have at least one type
    val vertexTypeRdd = instanceGraph.vertices
      .flatMap{case(id, v) => v.types}
    // Rdd with frequency of a type
    val nodeTypeFreq = vertexTypeRdd.map(c => (c, 1))
      .reduceByKey(_+_)

    // Write Type frequency to local file
    // Distinct types should be small, so it can be collected
    nodeTypeFreq.sortBy(_._2, ascending = false).collectAsMap
  }


  //TODO rename count in method and clarify meaning
  /**
   * Loads the count of schema nodes from local file system
   * @param localBasePath base working dir of local filesystem
   * @return a Map[String, Int] of uri, count
   */
  def loadSchemaNodeCount(localBasePath: String): Map[String, Long] = {
    IOUtils.loadMap(localBasePath + SCHEMA_NODE_COUNT)
      .map{case(k: String, v: String) => (k, v.toString.toLong)}
  }


  /**
   *
   * @param nodeCountMap
   * @param localBasePath
   */
  private def writeSchemaNodeCount(nodeCountMap: Map[String, Int], localBasePath: String): Unit = {
    val outputPath = localBasePath + SCHEMA_NODE_COUNT
    IOUtils.writeMap(nodeCountMap, outputPath)

  }


  /**
   *
   * @param schemaGraph Graph with RDF Schema
   * @param spark implicit parameter SparkSession
   * @return
   */
  def computeNodeImportance(schemaGraph: Graph[SV, SE],
                            nodeFreq: Map[String, Int],
                            bcMap: Map[String, Double])
                           (implicit spark: SparkSession): Map[VertexId, Double] = {
    val brNodeFreq = spark.sparkContext.broadcast(nodeFreq)

    // map with schemaUri -> frequency
    val schemaFreq: Map[String, Int] = schemaGraph.vertices
      .map{case(id, vertex) => (vertex.uri, brNodeFreq.value.getOrElse(vertex.uri, 0))}
      .collectAsMap

    val bcMax = bcMap.valuesIterator.max // min value of bc
    val bcMin = bcMap.valuesIterator.min // max value of bc
    val freqMax = schemaFreq.valuesIterator.max
    val freqMin = schemaFreq.valuesIterator.min
    // map uri -> id
    val uriIDMap: Map[String, VertexId] = schemaGraph.vertices
      .map{case(id, v) => (v.uri, id)}
      .collectAsMap

    // Build a map with vertexId -> importance
    val importanceMap: Map[VertexId, Double] = bcMap.map{case(uri, bcValue) =>
      val normBc = normalizeValue(bcValue, bcMin, bcMax)
      val normFreq = normalizeValue(schemaFreq(uri), freqMin, freqMax)
      if(uriIDMap.contains(uri))
        (uriIDMap(uri), normBc + normFreq)
      else
        (uriIDMap(uri), 0.0001)
    }
    importanceMap
  }


  /**
   *
    * @param importanceMap
   * @param localBasePath
   */
  private def writeNodeImportance(importanceMap: Map[VertexId, Double], localBasePath: String): Unit = {
    IOUtils.writeMap(importanceMap, localBasePath + SCHEMA_NODE_IMPORTANCE)
  }

  /**
   * Loads a map with the importance of each vertex
   * @param localBasePath base working dir of local filesystem
   * @return Map[Long, Double] -> (VertexId, importance)
   */
  def loadNodeImportance(localBasePath: String): Map[Long, Double] = {
    IOUtils.loadMap(localBasePath + SCHEMA_NODE_IMPORTANCE)
      .map{case(k: String, v: String) => (k.toString.toLong, v.toString.toDouble)}
  }


  /**
   *
   * @param value
   * @param min
   * @param max
   * @return
   */
  private def normalizeValue(value: Double, min: Double, max: Double): Double =
    (value - min) / (max - min)
}