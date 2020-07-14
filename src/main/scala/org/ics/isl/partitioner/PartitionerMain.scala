package org.ics.isl.partitioner

import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.SparkSession
import org.ics.isl.partitioner.InstanceGraph.{IE, IV}
import org.ics.isl.partitioner.SchemaGraph.{SE, SV}

import scala.collection.Map

object PartitionerMain {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = createSparkSession()

    val arguments = ArgumentParser.init().parse(args, ArgumentParser.Arguments())

    val hdfsBasePath = buildWorkingHdfsPath(arguments)
    val localBasePath = buildWorkingLocalPath(arguments)

    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.setCheckpointDir(hdfsBasePath + "checkpoint") // needed for BC library

    printArguments(arguments)


    val schemaGraph: Graph[SV, SE] = SchemaGraph.generateGraph(arguments.get.schemaPath)

    val instanceGraph: Graph[IV, IE] = InstanceGraph.generateGraph(arguments.get.instancesPath)

    GraphMetricsUtils.computeGraphMetrics(schemaGraph, instanceGraph, hdfsBasePath, localBasePath)

    val schemaVertexIdMap = schemaGraph.vertices.map{case(id, v) => (v.uri, id)}.collectAsMap
    val importanceMap = GraphMetricsUtils.loadNodeImportance(localBasePath)
    val ccDs = GraphMetricsUtils.loadCC(hdfsBasePath)
    // load schemaNodeCount and map the uri to its ID
    val schemaNodeCount = GraphMetricsUtils.loadSchemaNodeCount(localBasePath)
      .flatMap{case(uri, c) =>
        if(schemaVertexIdMap.contains(uri))
          Option((schemaVertexIdMap(uri), c))
        else None
      }

    val partitioner: LAWAPartitioner = if(arguments.get.partitionerMode == 1) BLAP else LAP

    val schemaGraphPartitions: Map[VertexId, Array[RDFTriple.Triple]] =
      partitioner.partitionSchemaGraph(
        schemaGraph,
        ccDs,
        importanceMap,
        schemaNodeCount,
        instanceGraph.numEdges,
        arguments.get.numPartitions)

    val classIndex: Map[String, Seq[VertexId]] = Index.generateClassIndex(schemaGraphPartitions)

    val partitionedInstances = partitioner.partitionInstances(instanceGraph, classIndex)
    partitioner.storePartitions(partitionedInstances, hdfsBasePath, arguments.get.numPartitions)
    partitioner.subPartitionClusters(hdfsBasePath)
    partitioner.computePartitionStats(partitionedInstances, localBasePath)

    spark.stop()
  }

  /**
   * Creates the base path in HDFS that the application will work
   * @param arguments
   * @return
   */
  def buildWorkingHdfsPath(arguments:Option[ArgumentParser.Arguments]): String = {
    val mode = if(arguments.getOrElse() == 0) "lap" else "blap"

    arguments.get.hdfsPath + "/" +
      arguments.get.datasetName + "/" +
      mode + "/" +
      arguments.get.numPartitions + "/"
  }

  /**
   * Creates the base path in HDFS that the application will work
   * @param arguments
   * @return
   */
  def buildWorkingLocalPath(arguments:Option[ArgumentParser.Arguments]): String = {
    val mode = if(arguments.getOrElse() == 0) "lap" else "blap"

    "lawa_local/" +
      arguments.get.datasetName + "/" +
      mode + "/" +
      arguments.get.numPartitions + "/"
  }

  /**
   * Prints applications input arguments
   * @param arguments inout argumerts Parsed
   */
  def printArguments(arguments: Option[ArgumentParser.Arguments]): Unit = {
    println(
      "lawa partitioner submited with arguments:",
      arguments.get.partitionerMode,
      arguments.get.numPartitions,
      arguments.get.datasetName,
      arguments.get.hdfsPath,
      arguments.get.instancesPath,
      arguments.get.schemaPath)
  }

  /**
   * Builds SparkSession
   * @return SparkSessiom
   */
  def createSparkSession(): SparkSession= {
    SparkSession.builder
      .appName("Partitioner")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "1024m")
      .config("spark.driver.maxResultSize", "10G")
      .config("spark.hadoop.dfs.replication", "1")
      .getOrCreate()
  }

}
