package org.ics.isl.partitioner

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import scala.util.{Failure, Success, Try}

object RDFTriple {

  case class Triple(s: String, p: String, o: String)

  private final val TRIPLE_SIZE = 3
  private final val TRIPLE_DELIM = "\\s+"

  /**
   * Loads a Dataset of Triples from the given HDFS path
   * @param path path of triples in HDFS
   * @param spark implicit param SparkSession
   * @return a Dataset[Triples]
   */
  def loadTriplesDs(path: String)(implicit spark: SparkSession): Dataset[Triple] = {
    import spark.implicits._
    val schemaDf = spark.read.text(path)
    schemaDf.flatMap(r => extractTriple(r))
  }


  /**
   * Helper method to extract a Triple case class from the Row
   */
  private val extractTriple: Row => Option[Triple] = row => {
    val tripleStr = Try(row.getString(0)) //TODO does it fail?
    tripleStr match {
      case Success(value) => value.startsWith("#") match {
        case true => None // skip comments
        case false => buildTriple(value)
      }
      case Failure(exception) => None
    }
  }


  /**
   * Builds a case class Triple from the given String
   */
  private val buildTriple: String => Option[Triple] = t => {
    val s = rTrimDot(t).trim // remove trailing "." and trim spaces again
    val a: Array[String] = s
      .split(TRIPLE_DELIM, TRIPLE_SIZE)
      .map(s => toUri(s.trim)) // trim for spaces and check for <>
    a.length match {
      case TRIPLE_SIZE => Option(Triple(a(0), a(1), a(2)))
      case _ => None // if not proper triple -> None
    }
  }


  // Trim for spaces and remove trailing "."
  private val rTrimDot: String => String = _.trim.replaceAll("\\.$", "")


  /**
   * Formats URI with prefix and suffix <>
   */
  private val toUri: String => String = uri => {
    if(isLiteral(uri))
      uri
    else if(!uri.startsWith("<") && !uri.endsWith(">"))
      "<" + uri + ">"
    else if (!uri.startsWith("<"))
      "<" + uri
    else if (!uri.endsWith(">"))
      uri + ">"
    else
      uri
  }


  /**
   * if string is literal returns true, else false
   */
  private val isLiteral: String => Boolean = _.contains("\"")

}
