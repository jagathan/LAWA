package org.ics.isl.partitioner

import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.ics.isl.partitioner.GraphMetricsUtils.CC
import org.ics.isl.partitioner.RDFTriple.PTriple
import org.ics.isl.partitioner.SchemaGraph.{SE, SV}
import org.ics.isl.partitioner.WeightedShortestPath.SPMap

import scala.collection.Map

object BLAP extends LAWAPartitioner {

  /**
   * Partitions Schema graph based on BLAP algorithm
   *
   * @param schemaGraph schema graph
   * @param ccDs Dataset of CC
   * @param impMap Map indicating the importance of each node
   * @param numInstances number of Instances
   * @param numPartitions number of partitions to split the graph into
   * @param spark implicit param SparkSession
   * @return
   */
  override def partitionSchemaGraph(
                     schemaGraph: Graph[SV, SE],
                     ccDs: Dataset[CC],
                     impMap: Map[VertexId, Double],
                     schemaNodeCount: Map[VertexId, Long],
                     numInstances: Long,
                     numPartitions: Int)
                   (implicit spark: SparkSession): Map[VertexId, Array[RDFTriple.Triple]] = {

    val schemaVertices = schemaGraph.vertices
      .map{case(id, v) => (id, v.uri)}
      .collectAsMap

    val vertexIDs = schemaVertices.keys.toSeq
    val centroids = findCentroids(impMap, schemaVertices, numPartitions)

    // add weights to graph based on importance and CC
    val weightedSchemaGraph = createWeightedGraph(schemaGraph, ccDs, impMap, numPartitions)

    // ASSP - Graph containing in every node all shortest paths to all other nodes
    val spGraph: Graph[SPMap, SE] = WeightedShortestPath.run(weightedSchemaGraph, vertexIDs)

    //Compute for every node the dependence to every centroid
    val nodeCentroidDependence: Array[(VertexId, Map[VertexId, (Double, Seq[PTriple])])] =
      computeNodeToCentroidDependence(spGraph, centroids, impMap)

    nodeCentroidDependence.foreach(println)

    val centroidMap = aggregateCentroids(nodeCentroidDependence)

    val schemaPartitions = createBalancedPartitions(
      centroidMap,
      schemaVertices,
      schemaNodeCount,
      numInstances,
      numPartitions)

    schemaPartitions.map{case(id, triples) =>
      (id, triples.map(t =>
        RDFTriple.Triple(schemaVertices(t.srcId), t.e, schemaVertices(t.dstId))))}
  }


  /**
   * Transforms the given Sequence into a map of partitionCentroid, partitionContent
   * Input form: Seq(NodeId , Map[cID -> (dep, path)])
   * Output form: Map[cId, Array[(dep, nodeId, path)]
   *
   * @param nodeCentroidDependence
   * @return
   */
  private def aggregateCentroids(nodeCentroidDependence: Seq[(VertexId, Map[VertexId, (Double, Seq[PTriple])])]):
  Map[VertexId, Array[(Double, VertexId, Seq[PTriple])]] = {

    var centroidMap = Map[VertexId, Array[(Double, VertexId, Seq[PTriple])]]()

    nodeCentroidDependence.foreach{case(node, depMap) => {
      depMap.foreach{case(cId, (dep, path)) => {
        val depArray: Array[(Double, VertexId, Seq[PTriple])] = Array((dep, node, path))
        if(centroidMap.contains(cId)) {
          centroidMap = centroidMap + (cId -> (centroidMap(cId) ++ depArray))
        }
        else {
          centroidMap = centroidMap + (cId -> depArray)
        }
      }}
    }}
    centroidMap.map{case(cId, nodeDependences) =>
      (cId, nodeDependences.sortBy(_._1)(Ordering[Double].reverse))}
  }

  /**
   * URIS thas should be excluded from DBpedia Dataset.
   */
  private val invalidUris: Array[String] = Array(
  "<http://www.w3.org/2004/02/skos/core#Concept>",
  "<http://www.w3.org/2002/07/owl#Class>",
  "<Version 3.8>",
  "<http://dbpedia.org/ontology/>")


  /**
   * Creates a balanced distribution in the given partitions.
   *
   * @param centroidMap Map that contain the content of each partition
   * @param schemaVertices Map for vertexId -> vertexUri
   * @param schemaNodeCount Map vertexId -> numOfInstances
   * @param instanceCount number of instances
   * @param numPartitions number of partitions
   * @return
   */
  private def createBalancedPartitions(
                                 centroidMap: Map[VertexId, Array[(Double, VertexId, Seq[PTriple])]],
                                 schemaVertices: Map[VertexId, String],
                                 schemaNodeCount: Map[VertexId, Long],
                                 instanceCount: Long,
                                 numPartitions: Int): Map[VertexId, Array[PTriple]] = {
    import util.control.Breaks._

    val threshold: Long = instanceCount / numPartitions
    var error: Long = threshold / numPartitions

    var partitionedNodes = Array[VertexId]()

    // Map used to calculate the size of each partition
    var partitionCounter = Map[VertexId, Long]()
    // Map that stores the nodes of each partition
    var partitionNodes = Map[VertexId, Array[Long]]()
    // Map indicating the triples of each partitions
    var partitionTriples = Map[VertexId, Array[PTriple]]()

    //all nodes except centroids and invalid
    val intialValidNodes = schemaVertices.keys
      .filter(x => !centroidMap.keys.toSeq.contains(x) && !invalidUris.contains(schemaVertices(x)))

    var leftoutNodes: Set[VertexId] = intialValidNodes.toSet
    var leftoutSize: Long = leftoutNodes.map(schemaNodeCount.getOrElse(_, 0L)).sum

    while(leftoutSize != 0) {
      centroidMap.foreach{case(cId, dependenceMap) => {
        dependenceMap.foreach{case(_, node, path) => {
          breakable {
            //if node is partitioned continue
            if(partitionedNodes.contains(node) || invalidUris.contains(schemaVertices(node)))
              break
            //find nodes that the current partition does not contain
            val newNodes: Seq[VertexId] = findNewNodes(path, partitionNodes.getOrElse(cId, Array[VertexId]()))

            if(newNodes.isEmpty) { //no nodes to enter
              partitionedNodes = partitionedNodes :+ node
              break //continue
            }
            val newNodeSize: Long = newNodes.map(schemaNodeCount.getOrElse(_, 0L)).sum

            val curPartitionSize: Long = partitionCounter.getOrElse(cId, 0L)
            //if incoming nodes fit into partition OR partition is empty
            if(curPartitionSize == 0 || curPartitionSize + newNodeSize < threshold) {
              partitionNodes += (cId -> (partitionNodes.getOrElse(cId, Array[Long]()) ++ newNodes))
              partitionCounter += (cId -> (partitionCounter.getOrElse(cId, 0L) + newNodeSize))
              partitionTriples += (cId -> (partitionTriples.getOrElse(cId, Array[PTriple]()) ++ path))
              partitionedNodes :+= node
            }
            else if(curPartitionSize + newNodeSize < threshold + error) {
              partitionNodes += (cId -> (partitionNodes.getOrElse(cId, Array[Long]()) ++ newNodes))
              partitionCounter += (cId -> (partitionCounter.getOrElse(cId, 0L) + newNodeSize))
              partitionTriples += (cId -> (partitionTriples.getOrElse(cId, Array[PTriple]()) ++ path))
              partitionedNodes :+= node
            }
          }
        }}
      }}
      leftoutNodes = leftoutNodes -- partitionedNodes.toSet
      leftoutSize = if(leftoutNodes.nonEmpty){
        leftoutNodes.map(schemaNodeCount.getOrElse(_, 0L)).sum
      }
      else 0L
      error = error + error / numPartitions
    }
    partitionTriples
  }


  /**
   * Finds which nodes of the current triple does not exist already in the partition
   * @param path a Sequence of Triples
   * @param curPartitionNodes list of nodes that the current partition has
   * @return
   */
  private def findNewNodes(path: Seq[PTriple], curPartitionNodes: Array[VertexId]): Seq[VertexId] = {
    path.flatMap(e => Array(e.srcId, e.dstId))
      .distinct
      .filter(x => !curPartitionNodes.contains(x))
  }

}
