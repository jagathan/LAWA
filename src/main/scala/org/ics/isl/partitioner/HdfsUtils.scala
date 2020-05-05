package org.ics.isl.partitioner

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object HdfsUtils {

  /**
   *
   * @param ds
   * @param path
   * @tparam T
   */
  def writeDs[T](ds: Dataset[T], path: String): Unit =
    ds.write.mode("overwrite").parquet(path)

  /**
   *
   * @param path
   * @param spark
   * @return
   */
  def loadDf(path: String)(implicit spark: SparkSession): DataFrame =
    spark.read.parquet(path)
}
