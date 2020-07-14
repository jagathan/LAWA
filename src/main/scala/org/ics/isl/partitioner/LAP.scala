package org.ics.isl.partitioner

import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.ics.isl.partitioner.BLAP.{aggregateCentroids, computeNodeToCentroidDependence, createBalancedPartitions, createWeightedGraph, findCentroids}
import org.ics.isl.partitioner.GraphMetricsUtils.CC
import org.ics.isl.partitioner.RDFTriple.PTriple
import org.ics.isl.partitioner.SchemaGraph.{SE, SV}
import org.ics.isl.partitioner.WeightedShortestPath.SPMap

import scala.collection.Map

object LAP extends LAWAPartitioner {

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

    val schemaEdges = schemaGraph.triplets
      .map(t => PTriple(t.srcId, t.dstId, t.attr.uri))
      .distinct
      .collect

    val schemaTriples = schemaGraph.triplets
      .map(t => RDFTriple.Triple(t.srcAttr.uri, t.attr.uri, t.dstAttr.uri))
      .distinct
      .collect

    val vertexIDs = schemaVertices.keys.toSeq
    val centroids = findCentroids(impMap, schemaVertices, numPartitions)

    // add weights to graph based on importance and CC
    val weightedSchemaGraph = createWeightedGraph(schemaGraph, ccDs, impMap, numPartitions)

    // ASSP - Graph containing in every node all shortest paths to all other nodes
    val spGraph: Graph[SPMap, SE] = WeightedShortestPath.run(weightedSchemaGraph, vertexIDs)

    //Compute for every node the dependence to every centroid
    val nodeCentroidDependence: Array[(VertexId, Map[VertexId, (Double, Seq[PTriple])])] =
      computeNodeToCentroidDependence(spGraph, centroids, impMap)

    val schemaClusters = nodeCentroidDependence.flatMap(node => mapIdToTriples(node._2.maxBy(_._2._1)))

    //replace every reverse triple with the original one
    val validSchemaClusters = schemaClusters.map{case(cId, triple) => {
      if(schemaEdges.contains(triple))
        (cId, replaceVertexIds(triple, schemaVertices))
      else
        (cId, replaceVertexIds(PTriple(triple.dstId,  triple.srcId, triple.e), schemaVertices))
    }}
    //Hack to index centroids that are empty
    val emptyCentroidMap = centroids.diff(validSchemaClusters.map(_._2).distinct)
      .map(cId => (cId, schemaVertices(cId)))
      .toArray

  //  createClassIndex(validSchemaClusters, centroids, emptyCentroidMap)

    //map each node in the cluster with every triple it is connected
    val extendedSchemaCluster = validSchemaClusters.flatMap{case(cId, curEdge) =>
      extendCluster(cId,
        curEdge,
        schemaTriples)
    }.distinct

    val emptyCentroids = centroids.diff(extendedSchemaCluster.map(_._2).distinct)

    val extendedCentroids = emptyCentroids.flatMap(cId => extendCentroid(cId, schemaVertices(cId), schemaTriples))
//      .flatMap(list => list)
      .distinct

    val finalSchemaCluster = extendedSchemaCluster.union(extendedCentroids).distinct

    finalSchemaCluster.map(_.swap).groupBy(_._1).map(x => (x._1, x._2.map(t => Seq(t._2)).reduce(_++_).toArray))
  }

  /**
   * Helper function
   * Extends an empty partition by its nodes incoming/outgoing triples
   */
  def extendCentroid(cId: Long, centroidUri: String, schemaEdges: Array[RDFTriple.Triple]) = {
    schemaEdges.filter(edge => centroidUri == edge.s
      || centroidUri == edge.o)
      .map(triple => (triple, cId))
  }

  /**
   * Used after initial schema partitiong.
   * Replicates each node with all its incoming/outgoing edges
   */
  def extendCluster(cId: Long, curEdge: RDFTriple.Triple, schemaEdges: Array[RDFTriple.Triple]) = {
    schemaEdges.filter(edge => Seq(curEdge.s, curEdge.o).contains(edge.s)
      || Seq(curEdge.s, curEdge.o).contains(edge.o))
      .map(triple => (triple, cId))
  }

  /**
   * Helper function to map triple to centroid Id (used for flattening)
   */
  def mapIdToTriples(centroidTuple: (Long, (Double, Seq[PTriple]))) = {
    centroidTuple._2._2.map(triple => (centroidTuple._1, triple))
  }

  def replaceVertexIds(triple: PTriple, map: Map[Long, String]): RDFTriple.Triple = {
    RDFTriple.Triple(map(triple.srcId), triple.e, map(triple.dstId))
  }

}
