package org.ics.isl.utils

import org.apache.spark.sql.DataFrame

trait DataFrameComparator {

  def assertDataFramesEqual(df1: DataFrame, df2: DataFrame): Boolean = {
    df1.cache()
    df2.cache()

    val df1Count = df1.count()
    val df2Count = df2.count()

    if(df1Count != df2Count) {
      false
    }
    else {
      val diff1 = df1.except(df2)
      val diff2 = df2.except(df1)

      if(diff1.count != 0) {
        diff1.show(false)
        false
      }
      else if (diff2.count != 0) {
        diff2.show(false)
        false
      }
      else {
        true
      }
    }
  }
}
