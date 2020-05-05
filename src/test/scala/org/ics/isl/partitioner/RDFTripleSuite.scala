package org.ics.isl.partitioner

import org.apache.spark.sql.{Dataset, SparkSession}
import org.ics.isl.utils.{DataFrameComparator, SparkSessionWrapper}
import org.scalatest.funsuite.AnyFunSuite

class RDFTripleSuite extends AnyFunSuite with SparkSessionWrapper with DataFrameComparator {

  private val dataPath = "src/test/resources/rawtriples"

  /**
   * Test data output
   * @return a Seq[RDFTriple.Triple] with 3 identical elements
   */
  private def getResult: Seq[RDFTriple.Triple] = {
    Seq.fill(3) {
      RDFTriple.Triple(
        "<http://my_src_uri.org>", "<http://my_predicate.org>", "<http://my_src_uri.org>"
      )
    }
  }

  /**
   * Loads Dataset of Triples from the given path
   * @param path path containing triples
   * @param spark implicit param SparkSession
   * @return
   */
  private def getTripleDs(path: String)(implicit spark: SparkSession): Dataset[RDFTriple.Triple] =
    RDFTriple.loadTriplesDs(path)


  test("Creates a Dataset with 3 rows of type RDFTriple.Triple"+
    "The input file contains 3 valid triples") {

    implicit val session: SparkSession = spark
    import spark.implicits._

    val resultDs = getTripleDs(dataPath + "/3_valid_triples.nt")

    val expectedDs = spark.createDataset(getResult).toDF

    assert(resultDs.count == 3)

    assert(resultDs.take(1).last.isInstanceOf[RDFTriple.Triple])

    assert(assertDataFramesEqual(resultDs.toDF, expectedDs.toDF))
  }

  test("Creates a Dataset with 3 rows of type RDFTriple.Triple" +
    "The input file contains triples without ending character '.'") {

    implicit val session: SparkSession = spark
    import spark.implicits._

    val path = dataPath + "/3_triples_without_dot.nt"

    val resultDs = getTripleDs(path)

    val expectedDs = spark.createDataset(getResult).toDF

    assert(resultDs.count == 3)

    assert(resultDs.take(1).last.isInstanceOf[RDFTriple.Triple])

    assert(assertDataFramesEqual(resultDs.toDF, expectedDs.toDF))
  }

  test("Creates a Dataset with 0 rows" +
    "The input file contains 3 invalid triples") {

    implicit val session: SparkSession = spark
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val path = dataPath + "/invalid_triples.nt"

    val resultDs = getTripleDs(path)

    val expectedDs = spark.emptyDataset[RDFTriple.Triple]

    assert(resultDs.count == 0)

    assert(assertDataFramesEqual(resultDs.toDF, expectedDs.toDF))
  }

}
