package demo

import org.apache.spark.sql.SparkSession

object ConvertCsvToParquet extends App {
   val spark = SparkSession.builder.appName("CSV to Parquet converter").getOrCreate()

   val inputDF = readCsvData(spark, inputCSV)

   writeAsParquet(inputDF, outputLocation)
   spark.stop()

   val sparkValidationSession = SparkSession.builder.appName("CSV to Parquet conversion validator").getOrCreate()
   val inputSize = readCsvData(sparkValidationSession, inputCSV).count()
   val outputSize = sparkValidationSession.read.format("parquet").load(outputLocation).count()
   sparkValidationSession.stop()

   if (inputSize != outputSize) {
      println("Input and output sizes do not match")
      throw new RuntimeException("Input and output size does not match")
   } else {
      println("Input and output sizes match")
   }
}
