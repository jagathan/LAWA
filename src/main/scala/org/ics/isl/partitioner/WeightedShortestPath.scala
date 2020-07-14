package org.ics.isl.partitioner

import org.apache.spark.graphx._
import org.ics.isl.partitioner.SchemaGraph.{SE, SV}


/**
 * Computes weighted shortest paths to the given set of landmark vertices, returning a graph where each
 * vertex attribute is a map containing the shortest-path distance to each reachable landmark.
 * Currently supports only Graph of [VD, Double], where VD is an arbitrary vertex type.
 */
object WeightedShortestPath extends Serializable {

    case class LEdge(srcId: Long, dstId: Long, e: String)

    //Initialize Map [ Centroid_Vertex_Id, (Sum(BC/CC, Path_Triples)) ]
    type SPMap = Map[VertexId, (Double, Seq[LEdge])]

    // initial and infinity values, use to relax edges
    private val INITIAL = 0.0.toDouble
    private val INFINITY = Double.MaxValue

    private def makeMap(x: (VertexId, (Double, Seq[LEdge]))*) = Map(x: _*)

    private def incrementMap(spmap: SPMap, delta: (Double, String, Long, Long)): SPMap = {
        val edge = LEdge(delta._3, delta._4, delta._2)
        spmap.map{ case (v, d) => v -> Tuple2(d._1 + delta._1, d._2 :+ edge)}
    }

    private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap = {
        (spmap1.keySet ++ spmap2.keySet).map {
            k => (

                if(getFirst(spmap1.get(k)) < getFirst(spmap2.get(k)))
                    k -> spmap1.getOrElse(k, (Double.MaxValue, Seq[LEdge]()))
                else
                    k -> spmap2.getOrElse(k, (Double.MaxValue, Seq[LEdge]()))
            )
        }.toMap
    }

    // at this point it does not really matter what vertex type is
    def run[VD](graph: Graph[SV, SE], landmarks: Seq[VertexId]): Graph[SPMap, SE] = {
        val spGraph = graph.mapVertices {
            (vid, attr) => (
                if (landmarks.contains(vid)) makeMap(vid -> Tuple2(INITIAL, Seq[LEdge]()))
                else makeMap()
            )
        }

        val initialMessage = makeMap()

        def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
            addMaps(attr, msg)
        }

        def sendMessage(edge: EdgeTriplet[SPMap, SE]): Iterator[(VertexId, SPMap)] = {
            val newAttr = incrementMap(
                edge.dstAttr,
                (edge.attr.weight, edge.attr.uri, edge.srcId, edge.dstId))
            if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
            else Iterator.empty
        }

        Pregel(spGraph, initialMessage, Int.MaxValue, EdgeDirection.Either)(vertexProgram _, sendMessage _, addMaps _)
    }

    def getFirst(x: Option[Any]): Double = x match {
        case Some(y: (_, _)) => getConstant(y._1)
        case _  => Double.MaxValue
    }

    def getConstant(x: Any): Double = x match {
        case f: Double => f
        case _ => Double.MaxValue
    }

    def getSeq(x: Any): Seq[LEdge] = x match {
        case s: Seq[LEdge] => s
        case _ => Seq[LEdge]()
    }

    def getValue(x: Option[Any]): (Double, Seq[LEdge]) = x match {
        case Some(y: (_, _)) => (getConstant(y._1), getSeq(y._2))
        case _  => (Double.MaxValue, Seq[LEdge]())
    }
}