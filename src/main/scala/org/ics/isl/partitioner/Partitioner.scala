package org.ics.isl.partitioner

trait Partitioner {

  // Will be implemented by children
  def partitionSchemaGraph(): Unit

  def partitionInstances(): Unit = {

  }

  // Stores instances into clusters
  def clusterInstances(): Unit = {

  }

  //  Builds an index of node to class
  def generateNodeIndex(): Unit = {

  }

  //  Predicate partitions each cluster
  def subPartitionClusters(): Unit = {

  }

  // Computes partition statistics
  def computePartitionStatistics(): Unit = {

  }

}
