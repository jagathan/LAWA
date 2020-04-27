package org.ics.isl.partitioner

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.sql.{Dataset, SparkSession}

object InstanceGraph {

  /**
   *
   * @param id   ID of Vertex
   * @param data a tuple containing (uri, Seq[RDF_TYPES], Seq[(attr, label)]
   *             where Seq[RDF_TYPES] is a list of the types that the uri has
   *             eg. [Person, Student]
   *             where Seq[(attr, label)] is a list of the labels that the uri has
   *             e.g [(hasName, "John"), ...]
   */
  case class InstanceVertex(id: VertexId, data: (String, Seq[String], Seq[(String, String)]))

  case class InstanceEdge(src: VertexId, dst: VertexId, e: String)

  case class IV(uri: String)

  case class IE(attr: (String, Double))

  case class LabeledVertex(uri: String, labels: Seq[(String, String)])


  /**
   * Takes as input a Triple
   *
   * If the triple is about rdf_type or literal Returns Seq[(uri, Seq[(predicate, object)])]
   * Otherwise Seq[(uri, Seq.empty].
   */
  private val addLabelToVertex: RDFTriple.Triple => Seq[(String, Seq[(String, String)])] = t => {
    if (t.p == Constants.RDF_TYPE)
      Seq((t.s, Seq(("rdf:type", t.o))))
    else if (isLiteral(t.o))
      Seq((t.s, Seq((t.p, t.o))))
    else
      Seq((t.s, Seq.empty[(String, String)]),
        (t.o, Seq.empty[(String, String)]))
  }


  /**
   * Takes as input a tuple: (uri, Seq[(predicate, object)], id
   * Returns: (uris, Seq[rdf_types], Seq[(label, object)], id
   */
  private val buildVertex: (String, Seq[(String, String)], Long) =>
    (String, (Seq[String], Seq[(String, String)], Long)) = (uri, labels, id) => {
    val typeList: Seq[String] = labels.filter(_._1 == "rdf:type").map(_._2)
    val labelList: Seq[(String, String)] = labels.filter(_._1 != "rdf:type")
    (uri, (typeList, labelList, id))
  }


  /**
   * Generates instance Graph from the given path of triples
   *
   * @param triplesPath
   * @param hdfsBasePath
   * @param spark
   */
  def generateGraph(triplesPath: String, hdfsBasePath: String)
                   (implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val instanceDs = RDFTriple.loadTriplesDs(triplesPath)

    // Create an rdd with all vertices uris and its types / labels
    val vertexTypeRdd = instanceDs.flatMap(addLabelToVertex).rdd
    // Create an rdd with the unique vertices and their types/literals
    val vertexRdd = vertexTypeRdd.reduceByKey(_++_)
      .zipWithIndex
      .map{case((uri, labels), id) => buildVertex(uri, labels, id)}

    // Throw away all rdf_types and literal as they exist in vertex labels
    val filteredInstances = instanceDs
      .filter(t => t.p != Constants.RDF_TYPE && !isLiteral(t.o))
      .map(t => (t.s, (t.p, t.o))).rdd

    val numPartitions = vertexRdd.partitions.length
    // map uri to id (to be able to join later on uri)
    val vertexUriIdRdd = vertexRdd.map{case(uri, (types, labels, id)) => (uri, id)}
      .repartition(numPartitions*2)

    // Create edges

    // Join RDDs to get Subject VertexIds
    val edgeSubjIds = filteredInstances.join(vertexUriIdRdd)
      .map{case(s, ((p, o), sId)) => (o, (p, sId))}
    // Join to get Object VertexIds
    val edges = edgeSubjIds.join(vertexUriIdRdd)
      .map{case(o, ((p, sId), oId)) => (sId, oId, p)}.distinct

    // Build final datasets and store
    val edgeDs = edges.filter{case(s, o, p) => s != -1L && o != -1L}
      .map{case(s, o, p) => InstanceEdge(s, o, p)}.toDF.as[InstanceEdge]

    val vertexDs = vertexRdd.map{case(uri, (types, labels, id)) =>
      InstanceVertex(id, (uri, types, labels))}.toDF.as[InstanceVertex]

    HdfsUtils.writeDs(edgeDs, hdfsBasePath + Constants.INSTANCE_EDGES)
    HdfsUtils.writeDs(vertexDs, hdfsBasePath + Constants.INSTANCE_VERTICES)
  }

  /**
   * Loads instance graph from the given hdfs path
   *
   * @param hdfsBasePath
   * @param spark
   * @return
   */
  def loadGraph(hdfsBasePath: String)(implicit spark: SparkSession): Graph[IV, IE] = {
    import spark.implicits._

    val vertexDs = HdfsUtils.loadDf(hdfsBasePath + Constants.INSTANCE_VERTICES)
      .as[InstanceVertex]
    val edgeDs = HdfsUtils.loadDf(hdfsBasePath + Constants.INSTANCE_EDGES)
      .as[InstanceEdge]

    // Transform to RDD for GraphX
    val vertexRdd = vertexDs.map(v => (v.id, v.data)).rdd
    val edgeRdd = edgeDs.map(e => Edge(e.src, e.dst, e.e)).rdd

    Graph(vertexRdd, edgeRdd).asInstanceOf[Graph[IV, IE]]
  }


  /**
   * if string is literal returns true, else false
   */
  private val isLiteral: String => Boolean = _.contains("\"")

}