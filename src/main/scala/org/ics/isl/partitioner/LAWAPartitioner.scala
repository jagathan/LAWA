package org.ics.isl.partitioner

import org.apache.spark.Partitioner
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.ics.isl.partitioner.GraphMetricsUtils.CC
import org.ics.isl.partitioner.InstanceGraph.{IE, IV}
import org.ics.isl.partitioner.RDFTriple.PTriple
import org.ics.isl.partitioner.SchemaGraph.{SE, SV}
import org.ics.isl.partitioner.WeightedShortestPath.{LEdge, SPMap}

import scala.collection.Map

trait LAWAPartitioner extends java.io.Serializable {

  final val PARTITIONS = "partitions"
  final val PARTITION_STATS = "partition_stats"


  // abstract
  def partitionSchemaGraph(
                            schemaGraph: Graph[SV, SE],
                            ccDs: Dataset[CC],
                            impMap: Map[VertexId, Double],
                            schemaNodeCount: Map[VertexId, Long],
                            numInstances: Long,
                            numPartitions: Int)
                          (implicit spark: SparkSession): Map[VertexId, Array[RDFTriple.Triple]]



  /**
   * Generates and assigns a partition to each RED_TRIPLE
   * @param instanceGraph
   * @param brClassIndex
   * @param spark
   * @return
   */
  private def assignRdfTriplesToPartition(
                                           instanceGraph: Graph[IV, IE],
                                           brClassIndex: Map[String, Seq[Long]])
                                         (implicit spark: SparkSession)
  : Dataset[(RDFTriple.Triple, Long)] = {

    import spark.implicits._
    // Generate rdf_triples from the vertex attributes
    val rdfTypeTriplesDs: Dataset[RDFTriple.Triple] = instanceGraph.triplets.toDS
      .filter(t => t.srcAttr.types.nonEmpty || t.dstAttr.types.nonEmpty)
      .flatMap(t => generateRdfTriples(t))
      .distinct
    // Assign a partition to each Rdf Triple based on its object
    // in this case the object is the type
    val partitionRdfTypeTriples = rdfTypeTriplesDs
      .map(t => (t, brClassIndex.getOrElse(t.o, Seq[Long]())))
      .filter(_._2.nonEmpty)
      .flatMap{case(t, partitions) => partitions.map(p => (t, p))}
      .distinct

    partitionRdfTypeTriples
  }


  /**
   * Generates all rdf triples from the given Triple
   */
  private val generateRdfTriples: EdgeTriplet[IV, IE] => Seq[RDFTriple.Triple] = t => {
    t.srcAttr.types.map(typeUri =>
      RDFTriple.Triple(t.srcAttr.uri, Constants.RDF_TYPE, typeUri)) ++
      t.dstAttr.types.map(typeUri =>
        RDFTriple.Triple(t.dstAttr.uri, Constants.RDF_TYPE, typeUri))
  }


  /**
   * Assigns a partition to each instance triple
   * @param instanceGraph
   * @param brClassIndex
   * @param spark
   * @return
   */
  private def assignInstancesToPartition(
                                          instanceGraph: Graph[IV, IE],
                                          brClassIndex: Map[String, Seq[Long]])
                                        (implicit spark: SparkSession)
  : Dataset[(RDFTriple.Triple, Long)] = {

    import spark.implicits._
    val tripleTypeRdd = instanceGraph.triplets.toDS
      .filter(t => t.srcAttr.types.nonEmpty || t.dstAttr.types.nonEmpty)
      .map(t =>
        (RDFTriple.Triple(t.srcAttr.uri, t.attr.uri, t.dstAttr.uri),
          (t.srcAttr.types ++ t.dstAttr.types).distinct))
      .map{case(t, types) => (t, getTypePartitions(types, brClassIndex))}

    tripleTypeRdd.flatMap{case(t, partitions) => partitions.map(p => (t, p))}
      .distinct
  }

  /**
   * Generates label triples and assigns a partition to each
   * @param instanceGraph
   * @param brClassIndex
   * @param spark
   * @return
   */
  private def assignLabelsToPartition(
                                       instanceGraph: Graph[IV, IE],
                                       brClassIndex: Map[String, Seq[Long]])
                                     (implicit spark: SparkSession)
  : Dataset[(RDFTriple.Triple, Long)] = {

    import spark.implicits._

    val labelTriplesDs: Dataset[(RDFTriple.Triple, Seq[String])] = instanceGraph.triplets.toDS
      .filter(t =>
        (t.srcAttr.literals.nonEmpty && t.srcAttr.types.nonEmpty) ||
          (t.dstAttr.literals.nonEmpty && t.dstAttr.types.nonEmpty))
      .flatMap(t => generateLabels(t))

    val partitionedLabelTriples = labelTriplesDs
      .map{case(t, types) => (t, getTypePartitions(types, brClassIndex))}
      .filter(_._2.nonEmpty)
      .flatMap{case(t, partitions) => partitions.map(p => (t, p))}
      .distinct
    partitionedLabelTriples
  }

  val getTypePartitions: (Seq[String], Map[String, Seq[Long]]) => Seq[Long] = (uris, classIndex) => {
    val partitions = uris.map(classIndex.getOrElse(_, Seq[Long]()))
    if (partitions.nonEmpty) {
      partitions.reduce(_ ++ _).distinct
    } else {
      Seq[Long]()
    }
  }

  /**
   * Generates all label triples stored in the vertices of the given triple
   */
  private val generateLabels: EdgeTriplet[IV, IE] => Seq[(RDFTriple.Triple, Seq[String])] = t => {
    populateLabels(t.srcAttr) ++ populateLabels(t.dstAttr)
  }


  /**
   * Populates the labels stored in the given vertex
   */
  private val populateLabels: IV => Seq[(RDFTriple.Triple, Seq[String])] = v => {
    v.literals.map{case(p, o) => (RDFTriple.Triple(v.uri, p, o), v.types)}
  }


  /**
   * Assigns a partitions in each triple of the given graph
   * @param instanceGraph
   * @param classIndex
   * @param spark
   * @return
   */
  def partitionInstances(instanceGraph: Graph[IV, IE], classIndex: Map[String, Seq[Long]])
                        (implicit spark: SparkSession): Dataset[(RDFTriple.Triple, Long)] = {
    val brIndex = spark.sparkContext.broadcast(classIndex)
    // assign instance triples to partitions
    val partitionInstanceTriples = assignInstancesToPartition(instanceGraph, brIndex.value)
    // assign rdf_type triples to partitions
    val partitionRdfTypeTriples = assignRdfTriplesToPartition(instanceGraph, brIndex.value)
    // assign triples with labels to partitions
    val partitionedLabelTriples = assignLabelsToPartition(instanceGraph, brIndex.value)

    // merge all
    partitionInstanceTriples
      .union(partitionRdfTypeTriples)
      .union(partitionedLabelTriples)
  }

  /**
   * Stores the triples from the given dataset in partitions
   * @param partitionedTripleDs Dataset containing (Triple, partitionID)
   * @param hdfsBasePath base working dir in hdfs
   * @param numPartitions number of partitions to store the dataset into
   * @param spark implicit param spark
   */
  def storePartitions(partitionedTripleDs: Dataset[(RDFTriple.Triple, Long)],
                      hdfsBasePath: String,
                      numPartitions: Int)
                     (implicit spark: SparkSession): Unit = {
    import spark.implicits._

    /**
     * Custom partitioner to store data by the given partitionID
     */
    class CustomPartitioner(override val numPartitions: Int) extends Partitioner {
      override def getPartition(key: Any): Int = {
        return key.toString.toInt % numPartitions
      }
      override def equals(other: scala.Any): Boolean = {
        other match {
          case obj : CustomPartitioner => obj.numPartitions == numPartitions
          case _ => false
        }
      }
    }

    // store triples in each partition ID
    val partitionedDs = partitionedTripleDs.map(_.swap).rdd
      .partitionBy(new CustomPartitioner(numPartitions))
      .map(_._2)
      .toDF()

      HdfsUtils.writeDs(partitionedDs, hdfsBasePath + PARTITIONS)
  }


  /**
   * Creates a local file
   * @param partitionedTripleDs
   * @param localBasePath
   * @param spark
   */
  def computePartitionStats(
                             partitionedTripleDs: Dataset[(RDFTriple.Triple, Long)],
                             localBasePath: String)
                           (implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val partitionStats: Array[(Long, Int)] = partitionedTripleDs.map(x => (x._2, 1))
      .groupByKey(_._1)
      .mapGroups((cId, triples) => (cId, triples.size))
      .collect()

    IOUtils.writeMap(partitionStats.toMap, localBasePath + PARTITION_STATS)
  }

  /**
   * Re-partitions each partition (stored into one file) into a folder
   * partitioned by predicate.
   * @param hdfsBasePath
   * @param spark
   */
  def subPartitionClusters(hdfsBasePath: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val path = hdfsBasePath + PARTITIONS + "/"

    val partitions = HdfsUtils.getDirFilePaths(path)

    val roundUp: Double => Int = math.ceil(_).toInt

    // non-alpha numeric characters cause issues in hdfs naming
    val uriToStorage: String => String =
      _.replaceAll("[^A-Za-z0-9]", "_").toLowerCase

    partitions.foreach(p => {
      // load one-file partition
      val partition = HdfsUtils.loadDf(p).as[RDFTriple.Triple]
      val start = p.indexOf("part-") + 5
      // create name for new partition folder
      val partitionName = p.substring(start, start + 5)
      val partSize = partition.rdd.partitions.length

      // store one file partition into a folder partitioned by predicate
      partition.repartition(roundUp(partSize/7.0))
        .filter(t => t.p.length < 150)
        .map(t => (t.s, uriToStorage(t.p), t.o))
        .write.partitionBy("_2").parquet(path + partitionName)

      HdfsUtils.deleteFile(p) // delete initial one-file partition
    })
  }

  // Computes partition statistics
  def computePartitionStatistics(): Unit = {

  }


  /**
   * Computes dependence from each node in the graph to every centroid
   * @param spGraph
   * @param centroids
   * @param impMap
   * @return
   */
  protected def computeNodeToCentroidDependence(
                                       spGraph: Graph[SPMap, SE],
                                       centroids: Seq[VertexId],
                                       impMap:Map[VertexId, Double])
  : Array[(VertexId, Map[VertexId, (Double, Seq[PTriple])])] = {

    // Transform LEdge from WeightedShortestPaths to PTriple
    val toPTriple: Seq[LEdge] => Seq[PTriple] = path =>
      path.map(e => PTriple(e.srcId, e.dstId, e.e))

    // Shortest paths each node
    // Each vertex contains the VertexId and
    // A map with all the other vertices and the (summed weight, shortest path the VertexId)
    val spCentroids: RDD[(VertexId, Map[VertexId, (Double, Seq[PTriple])])] =
    spGraph.vertices
      .filter(v => centroids.contains(v._1))
      .map{case(vId, m) => (vId, m.map{case(vId, (w, path)) => (vId, (w, toPTriple(path)))})}

    // creating: nodeId -> Map(cId -> (sumW, path))
    val nodeCentroidRdd: RDD[(VertexId, Map[VertexId, (Double, Seq[PTriple])])] =
      spCentroids.flatMap{ case(cId, vMap) =>
        vMap.flatMap{ case(nodeId, (sumW, path)) =>
          // centroids give infinity dependence
          if(centroids.contains(nodeId)) None
          else Option((nodeId, Map(cId -> (sumW, path))))
        }
      }.reduceByKey(_++_)
        .filter(node => node._2.nonEmpty) // filter out empty maps


    nodeCentroidRdd.collect().foreach(println)

    // Calculate min and max (srcBC - weightSum) from each node to each centroid for normalization
    val (minDiff, maxDiff) = dependenceDiffs(nodeCentroidRdd, impMap)

    // find for each node the dependence to each centroid
    // Construct an array (NodeID, Map(cID -> (dependenceToCentroid, pathToCentroid))
    val nodeCentroidDependence: Array[(VertexId, Map[VertexId, (Double, Seq[PTriple])])] =
    nodeCentroidRdd.map{ case(nodeId, cMap) =>
      (nodeId, dependence(impMap, cMap, maxDiff, minDiff))
    }.collect

    nodeCentroidDependence
  }


  /**
   *
   * @param importanceMap
   * @param centroidMap
   * @param max
   * @param min
   * @return
   */
  private def dependence(
                          importanceMap: Map[Long, Double],
                          centroidMap: Map[Long, (Double, Seq[PTriple])],
                          max: Double,
                          min: Double): Map[Long, (Double, Seq[PTriple])] = {
    centroidMap.map(x =>
      (x._1,
        (computeDependence(importanceMap(x._1), x._2._1, x._2._2.size, max, min), x._2._2)
      )
    )
  }


  /**
   *
   * @param rdd
   * @param impMap
   * @return
   */
  private def dependenceDiffs(
                               rdd: RDD[(VertexId, Map[VertexId, (Double, Seq[PTriple])])],
                               impMap: Map[Long, Double]): (Double, Double) = {
    val dependenceDiffs = rdd.flatMap(node => dependenceDiff(impMap, node._2))
    (dependenceDiffs.min, dependenceDiffs.max)
  }


  /**
   * Computes Dependence based on the formula:
   * 1/(pathSize**2) * normalized(srcBC - weightSum)
   *
   * @param srcBC Betweens centrality of the source noce
   * @param weightSum sum of weights of the path between the source and destination
   * @param pathSize size of path
   * @param maxDiff maxDiff used for normalization
   * @param minDiff minDiff used for normalization
   * @return
   */
  private def computeDependence(
                                 srcBC: Double,
                                 weightSum: Double,
                                 pathSize: Int,
                                 maxDiff: Double,
                                 minDiff: Double): Double = {

    val pathSizeSquared = scala.math.pow(pathSize, 2) //path size ^ 2
    val diff = srcBC - weightSum
    val normDiff = (diff - minDiff) / (maxDiff - minDiff)
    (1 / pathSizeSquared).toDouble * normDiff
  }


  /**
   *
   * @param impMap
   * @param centroidMap
   * @return
   */
  private def dependenceDiff(
                              impMap: Map[Long, Double],
                              centroidMap: Map[Long, (Double, Seq[PTriple])]): Seq[Double] = {
    centroidMap.map(x => impMap(x._1) - x._2._1).toSeq
  }


  /**
   * Add weights in the given schema graph.
   * The weights are calculated based on CC and importance values of each triple
   * @param schemaGraph
   * @param ccDs
   * @param impMap
   * @param k
   * @param spark
   * @return
   */
  def createWeightedGraph(schemaGraph: Graph[SV, SE],
                          ccDs: Dataset[CC],
                          impMap: Map[Long, Double],
                          k: Int)
                         (implicit spark: SparkSession): Graph[SV, SE] = {

    import spark.implicits._

    // Create a map of rdf_type Triple, (numInstances, distinctNumInstances
    val ccMap: Map[RDFTriple.Triple, (Long, Long)] = ccDs.map(x =>
      (x.triple, (x.numInstances, x.distinctNumInstances)))
      .collect.toMap
    // broadcast map
    val ccMapBr = spark.sparkContext.broadcast(ccMap)

    // Add to graph the reverse direction of each edge to do behave as undirected
    val bothDirectionalSchema = Graph(
      schemaGraph.vertices,
      schemaGraph.edges.union(schemaGraph.edges.reverse))

    val numVertices = schemaGraph.numVertices
    // Add weight to edges
    val weightedEdges = bothDirectionalSchema.triplets
      .map(triplet => addWeight(triplet, impMap, ccMapBr.value, numVertices))

    Graph(bothDirectionalSchema.vertices, weightedEdges) //create weighted schema graph
  }


  /**
   * Adds weight to the given edge.
   * The weight of the triple is computed as:
   * importance of dstNode / CC of the triple
   *
   * @param triplet triple
   * @param impMap Map of Importance
   * @param ccMap map of CC
   * @param c num of vertices
   * @return a new Edge with updated weight
   */
  private def addWeight(
                 triplet: EdgeTriplet[SV, SE],
                 impMap: Map[Long, Double],
                 ccMap: Map[RDFTriple.Triple, (Long, Long)],
                 c: Long): Edge[SE] = {

    val destImportance = impMap(triplet.dstId)
    val curCC = cardinalityCloseness(triplet, c, ccMap)
    val newEdge = new Edge[SE]
    newEdge.attr = SE(triplet.attr.uri, destImportance / curCC)
    newEdge.srcId = triplet.srcId
    newEdge.dstId = triplet.dstId
    newEdge
  }


  /**
   * Computes CardinalityCloseness of the given triple
   * CC of a triple is computed from:
   * 1 + numVertices / numVertices + distinctNumInstances / numInstances
   * @param triplet triple
   * @param c num of vertices
   * @param ccMap map of CC
   * @return
   */
  private def cardinalityCloseness(
                            triplet: EdgeTriplet[SV, SE],
                            c: Long,
                            ccMap: Map[RDFTriple.Triple, (Long, Long)]): Double = {

    // create the Triple out of the given EdgeTriplet
    val t1 = RDFTriple.Triple(triplet.srcAttr.uri, triplet.attr.uri, triplet.dstAttr.uri)
    val t = if (ccMap.contains(t1)) t1 else RDFTriple.Triple(t1.o, t1.p, t1.s)

    val (numInstances: Long, distinctNumInstances: Long) = ccMap.getOrElse(t, (1L, 0L))
    val constant = (1 + c.toDouble) / c.toDouble

    if(distinctNumInstances == 0)
      constant
    else
      constant + (distinctNumInstances.toDouble / numInstances.toDouble)
  }


  /**
   * Finds the IDS of the k nodes with the highest importance.
   * These nodes will be considered as the centroids of the partitions.
   * @param impMap Map Importance of each node [NodeID, ImValue]
   * @param idUriMap A map of schema nodes ID -> uri
   * @param k num of centroids
   * @return a Seq with the node IDs of the k nodes with highest importance.
   */
  def findCentroids(impMap: Map[Long, Double], idUriMap: Map[VertexId, String], k: Int): Seq[Long] = {
    // filter map to consider only nodes with valid uris (hack for specific dataset cases)
    val filteredMap = impMap.filter(x => idUriMap(x._1).contains("http"))
    // Sort by importance
    val sortedImpIds = filteredMap.toSeq
      .sortWith(_._2 > _._2)
      .map(_._1)
    sortedImpIds.take(k)
  }
}