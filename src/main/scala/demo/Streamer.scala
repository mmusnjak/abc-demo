package demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, substring}

object Streamer extends App {
   val spark = SparkSession.builder.appName("CSV to Kafka streamer").getOrCreate()
   spark.sparkContext.setLogLevel("ERROR")

   val inputDF = readCsvData(spark, inputCSV)

   val inputSchema = inputDF.schema

   val inputStream = spark
      .readStream
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .schema(inputSchema)
      .csv("input/fragments/*.csv")

   val inputStreamWithCountry = inputStream.withColumn("country", substring(col("monitoringSiteIdentifier"), 0, 2))
   inputStreamWithCountry.createOrReplaceTempView("aggregates")
   val countryMaxMean = spark.sql("select country, max(resultMeanValue) as maximumOfMean from aggregates group by country")
   val countryMaxMeanKV = countryMaxMean
      .selectExpr("CAST(country AS STRING) AS key", "CONCAT(CAST(country AS STRING) ,':', CAST(maximumOfMean AS STRING)) AS value")

   val output = countryMaxMeanKV
      .writeStream
      .format("kafka")
      .outputMode("complete")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("topic", "abc-demo")
      .option("checkpointLocation", "/tmp/checkpoint")
      .start()

   output.awaitTermination()
}
