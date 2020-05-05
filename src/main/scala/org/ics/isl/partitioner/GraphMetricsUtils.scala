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


  def computeGraphMetrics(schemaGraph: Graph[SV, SE],
                          instanceGraph: Graph[IV, IE],
                          hdfsBasePath: String,
                          localBasePath: String)
                         (implicit spark: SparkSession): Unit = {

    computeCC(instanceGraph, hdfsBasePath)
    computeBC(schemaGraph, localBasePath)
    computeSchemaNodeFreq(instanceGraph, localBasePath)
    computeSchemaNodeCount(instanceGraph, localBasePath)
    computeNodeImportance(schemaGraph, localBasePath)
  }

  /**
   * Compute Betweenness centrality of the schema graph
   * and writes in a file in the local fileSystem
   * @param schemaGraph RDF schema Graph
   * @param localBasePath base working dir of local file system
   */
  def computeBC(schemaGraph: Graph[SV, SE], localBasePath: String): Unit = {
    val vertexMap = schemaGraph.vertices.map(v => (v._1, v._2.uri)).collectAsMap()

    val bcMap: Map[String, Double] = HuaBC.computeBC(schemaGraph)
      .sortBy(_._2, ascending = false) // sort by bc value
      .map(x => (vertexMap(x._1), x._2)) // replace vertexId with uri
      .collectAsMap()

    IOUtils.writeMap(bcMap, localBasePath  + Constants.BC)
  }

  /**
   * Loads betweenes centrality of schema graph from local file System
   * @param localBasePath base working dir of local file system
   * @return a Map[String, Double] -> (uri, bcValue)
   */
  def loadBC(localBasePath: String): Map[String, Double] = {
    IOUtils.loadMap(localBasePath + Constants.BC)
      .map{case(k: String, v: String) => (k, v.toString.toDouble)}
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
  def writeCC(ccDs: Dataset[CC], hdfsBasePath: String): Unit = {
    HdfsUtils.writeDs(ccDs, hdfsBasePath + Constants.CC)
  }

  /**
   * Loads a Dataset with Cardinality Closeness of rdf_types
   * @param hdfsBasePath base working path of hdfs
   * @param spark implicit param Spark Session
   * @return Dataset[CC]
   */
  def loadCC(hdfsBasePath: String)(implicit spark: SparkSession): Dataset[CC] = {
    import spark.implicits._
    HdfsUtils.loadDf(hdfsBasePath + Constants.CC).as[CC]
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
   * @param localBasePath base working dir of local filesystem
   */
  def computeSchemaNodeFreq(instanceGraph: Graph[IV, IE], localBasePath: String): Unit = {
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
    val outputPath = localBasePath + Constants.SCHEMA_NODE_FREQ
    val nodeFreqMap = nodeTypeFreq.sortBy(_._2, ascending = false).collectAsMap
    IOUtils.writeMap(nodeFreqMap, outputPath)
  }

  /**
   * Loads frequency of schema nodes from local file system
   * @param localBasePath base working dir of local filesystem
   * @return a Map[String, Int] of uri,frequency
   */
  def loadSchemaNodeFreq(localBasePath: String): Map[String, Int] = {
    IOUtils.loadMap(localBasePath + Constants.SCHEMA_NODE_FREQ)
      .map{case(k: String, v: String) => (k, v.toString.toInt)}
  }


  /**
   *
   * @param instanceGraph Graph with RDF Instances
   * @param localBasePath base working dir of local filesystem
   */
  def computeSchemaNodeCount(instanceGraph: Graph[IV, IE], localBasePath: String): Unit = {
    // Rdd with all vertices that have at least one type
    val vertexTypeRdd = instanceGraph.vertices
      .flatMap{case(id, v) => v.types}
    // Rdd with frequency of a type
    val nodeTypeFreq = vertexTypeRdd.map(c => (c, 1))
      .reduceByKey(_+_)

    // Write Type frequency to local file
    // Distinct types should be small, so it can be collected
    val outputPath = localBasePath + Constants.SCHEMA_NODE_COUNT
    val nodeCountMap = nodeTypeFreq.sortBy(_._2, ascending = false).collectAsMap
    IOUtils.writeMap(nodeCountMap, outputPath)
  }


  //TODO rename count in method and clarify meaning
  /**
   * Loads the count of schema nodes from local file system
   * @param localBasePath base working dir of local filesystem
   * @return a Map[String, Int] of uri, count
   */
  def loadSchemaNodeCount(localBasePath: String): Map[String, Int] = {
    IOUtils.loadMap(localBasePath + Constants.SCHEMA_NODE_COUNT)
      .map{case(k: String, v: String) => (k, v.toString.toInt)}
  }


  /**
   *
   * @param schemaGraph Graph with RDF Schema
   * @param localBasePath base working dir of local filesystem
   * @param spark implicit parameter SparkSession
   * @return
   */
  def computeNodeImportance(schemaGraph: Graph[SV, SE], localBasePath: String)(implicit spark: SparkSession): Unit = {
    val nodeFreq: Map[String, Int] = loadSchemaNodeFreq(localBasePath)
    val brNodeFreq = spark.sparkContext.broadcast(nodeFreq)

    nodeFreq.foreach(println)
    schemaGraph.vertices.collect.foreach(println)
    // map with schemaUri -> frequency
    val schemaFreq: Map[String, Int] = schemaGraph.vertices
      .map{case(id, vertex) => (vertex.uri, brNodeFreq.value.getOrElse(vertex.uri, 0))}
      .collectAsMap

    schemaFreq.foreach(println)

    val bcMap: Map[String, Double] = loadBC(localBasePath)

    val bcMax = bcMap.valuesIterator.max // min value of bc
    val bcMin = bcMap.valuesIterator.min // max value of bc

    val freqMax = schemaFreq.valuesIterator.max
    val freqMin = schemaFreq.valuesIterator.min
    // map uri -> id
    val uriIDMap: Map[String, VertexId] = schemaGraph.vertices
      .map{case(id, v) => (v.uri, id)}
      .collectAsMap

    // Build a map with vertexId -> importance
    val importanceMap: Map[Long, Double] = bcMap.map{case(uri, bcValue) =>
      val normBc = normalizeValue(bcValue, bcMin, bcMax)
      println(schemaFreq(uri), freqMin, freqMax)
      val normFreq = normalizeValue(schemaFreq(uri), freqMin, freqMax)
      if(uriIDMap.contains(uri))
        (uriIDMap(uri), normBc + normFreq)
      else
        (uriIDMap(uri), 0.0001)
    }
    IOUtils.writeMap(importanceMap, localBasePath + Constants.SCHEMA_NODE_IMPORTANCE)
  }

  /**
   * Loads a map with the importance of each vertex
   * @param localBasePath base working dir of local filesystem
   * @return Map[Long, Double] -> (VertexId, importance)
   */
  def loadNodeImportance(localBasePath: String): Map[Long, Double] = {
    IOUtils.loadMap(localBasePath + Constants.SCHEMA_NODE_IMPORTANCE)
      .map{case(k: String, v: String) => (k.toString.toLong, v.toString.toDouble)}
  }

  private def normalizeValue(value: Double, min: Double, max: Double): Double =
    (value - min) / (max - min)

}
