package org.ics.isl.partitioner

import org.apache.spark.sql.SparkSession

object PartitionerMain {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = createSparkSession()
    spark.sparkContext.setLogLevel("ERROR")

    val arguments = ArgumentParser.init().parse(args, ArgumentParser.Arguments())
    val hdfsBasePath = buildWorkingHdfsPath(arguments)

    printArguments(arguments)

    SchemaGraph.generateGraph(arguments.get.schemaPath, hdfsBasePath)

    val schemaGraph = SchemaGraph.loadGraph(hdfsBasePath)

    InstanceGraph.generateGraph(arguments.get.instancesPath, hdfsBasePath)

    val instanceGraph = InstanceGraph.loadGraph(hdfsBasePath)

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
