package com.mfsi.spark.ml

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object CabBookingCancellationNew {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    //      .load("../Kaggle/predicting-cab-booking-cancellations/Kaggle_YourCabs_score.csv")

    //    val df2 = df.withColumn("features", struct(df("from_lat"), df("from_long")))
    //    df2.show()

  }

}