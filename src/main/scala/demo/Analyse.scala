package demo

import org.apache.spark.sql.functions.{col, substring}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Analyse extends App {
   def getCountByCountry(df: DataFrame): DataFrame = df.groupBy("country").count().orderBy(col("count").desc)

   object ConvertCsvToParquet extends App {
      val spark = SparkSession.builder.appName("CSV to Parquet converter").getOrCreate()

      val inputDF = readParquetData(spark, outputLocation)
      val df_with_country = inputDF.withColumn("country", substring(col("monitoringSiteIdentifier"), 0, 2))
      df_with_country.createOrReplaceTempView("aggregates")

      // Try out sql with a basic query
      spark.sql("select resultObservationStatus, count(*) from aggregates group by 1").show

      // val countByCountry = spark.sql("select country, count(*) AS count from aggregates group by 1 order by 2 desc")
      val countByCountry = getCountByCountry(df_with_country)
      val countByCountryKV = countByCountry.selectExpr("CAST(country AS STRING) AS key", "CONCAT(CAST(country AS STRING) ,':', CAST(count AS STRING)) AS value")

      flushToKafkaTopic(countByCountryKV, "abc-demo")
   }
}
