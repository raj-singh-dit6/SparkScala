package com.mfsi.spark.ml

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object CabBookingCancellation {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "Cab booking cancellation")
    val sqlContext = new SQLContext(sc);
    val schema = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("user_id", IntegerType, true),
      StructField("vehicle_model_id", IntegerType, true),
      StructField("package_id", StringType, true),
      StructField("travel_type_id", IntegerType, true),
      StructField("from_area_id", StringType, true),
      StructField("to_area_id", StringType, true),
      StructField("from_city_id", StringType, true),
      StructField("to_city_id", StringType, true),
      StructField("from_date", StringType, true),
      StructField("to_date", StringType, true),
      StructField("online_booking", IntegerType, true),
      StructField("mobile_site_booking", IntegerType, true),
      StructField("booking_created", IntegerType, true),
      StructField("from_lat", StringType, true),
      StructField("from_long", StringType, true),
      StructField("to_lat", StringType, true),
      StructField("to_long", StringType, true)))

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("charset", "UTF8")
      .schema(schema)
      .load("../Kaggle/predicting-cab-booking-cancellations/Kaggle_YourCabs_score.csv")

    //    val df2 = df.withColumn("features", struct(df("from_lat"), df("from_long")))
    //    df2.show()

    val featuresCol = Array("from_lat", "from_long")
    val assmblr = new VectorAssembler().setInputCols(featuresCol).setOutputCol("features");
    val df2 = assmblr.transform(df)
    df2.show

    val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3), 5403)

    val kmeans = new KMeans().setK(8).setFeaturesCol("features").setPredictionCol("predictions")
    val model = kmeans.fit(df2)
    println("Final centers")
    model.clusterCenters.foreach(println);
  }

}