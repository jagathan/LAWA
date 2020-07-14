package org.ics.isl.partitioner

import org.ics.isl.partitioner.RDFTriple.Triple
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.Map

object Index {

  final val CLASS_INDEX = "class_index"
  final val NODE_INDEX = "node_index"
  final val CENTROID_DELIM = " "

  /**
   *
   * @param schemaPartitions
   */
  def generateClassIndex(schemaPartitions: Map[VertexId, Array[Triple]]):
  Map[String, Seq[VertexId]] = {

    schemaPartitions.flatMap{case(cId, triples) =>
      triples.flatMap(t => Seq((t.s, cId), (t.o, cId)))}
      .groupBy(_._1)
      .map{case(uri, cIdMap) => (uri, cIdMap.values.toSeq)}
  }

  /**
   *
   * @param index
   * @param localBasePath
   */
  def writeClassIndex(index: Map[String, Seq[Long]], localBasePath: String): Unit = {
    val indexToWrite = index.map{case(k, v) => (k, v.mkString(CENTROID_DELIM))}
    IOUtils.writeMap(indexToWrite, localBasePath + CLASS_INDEX)
  }


  /**
   *
   * @param localBasePath
   */
  def loadClassIndex(localBasePath: String): Unit = {
    IOUtils.loadMap(localBasePath + CLASS_INDEX)
      .map{case(k, v) => v.split(CENTROID_DELIM).map(_.toLong)}
  }


  /**
   *
   * @param partitionedTriples
   * @param spark
   * @return
   */
  def generateNodeIndex(partitionedTriples: Dataset[(RDFTriple.Triple, Long)])
                       (implicit spark: SparkSession): Dataset[(String, String)] = {
    import spark.implicits._
    partitionedTriples.filter(_._1.p == Constants.RDF_TYPE)
      .map{case(t, _) => (t.s, t.o)}
  }


  /**
   *
   * @param indexDs
   * @param hdfsBasePath
   * @param spark
   */
  def writeNodeIndex(indexDs: Dataset[(String, String)], hdfsBasePath: String)
                    (implicit spark: SparkSession): Unit = {
    HdfsUtils.writeDs(indexDs, hdfsBasePath + NODE_INDEX)
  }


  /**
   *
   * @param hdfsBasePath
   * @param spark
   * @return
   */
  def loadNodeIndex(hdfsBasePath: String)
                   (implicit spark: SparkSession): Dataset[(String, String)] = {
    import spark.implicits._
    HdfsUtils.loadDf(hdfsBasePath + NODE_INDEX).as[(String, String)]
  }

}
