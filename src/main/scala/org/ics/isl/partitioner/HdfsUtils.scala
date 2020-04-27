package org.ics.isl.partitioner

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object HdfsUtils {

  def writeDs[T](ds: Dataset[T], path: String): Unit =
    ds.write.mode("overwrite").parquet(path)

  def loadDf[T](path: String)(implicit spark: SparkSession): DataFrame =
    spark.read.parquet(path)
}
