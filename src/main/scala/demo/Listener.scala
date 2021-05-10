package demo

import org.apache.spark.sql.SparkSession

object Listener extends App {
   val spark = SparkSession.builder.appName("Kafka listener").getOrCreate()
   import spark.implicits._
   spark.sparkContext.setLogLevel("ERROR")

   val input = readFromKafka(spark, "abc-demo")
   val inputKV  = input.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .as[(String, String)]
   val consoleWriter = inputKV.writeStream.format("console").start()

   consoleWriter.awaitTermination()
}
