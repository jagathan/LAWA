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


  /**
   *
   * @param path
   * @param spark
   * @return
   */
  def getDirFilePaths(path: String)(implicit spark: SparkSession): Array[String] = {
    import org.apache.hadoop.fs.{FileSystem, Path}
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.listStatus(new Path(path)).map(_.getPath.toString).filter(!_.contains("SUCCESS"))
  }

  /**
   *
   * @param path
   * @param spark
   */
  def deleteFile(path: String)(implicit spark: SparkSession): Unit = {
    import org.apache.hadoop.fs.{FileSystem, Path}
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val filePath = new Path(path)
    if (fs.exists(filePath))
      fs.delete(filePath, true)
  }
}
