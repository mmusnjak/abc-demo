import org.apache.spark.sql.{DataFrame, SparkSession}

package object demo {
   val inputCSV = "input/clean_waterbase.csv"
   val outputLocation = "output"

   def readCsvData(spark: SparkSession, path: String): DataFrame = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .load(path)

   def writeAsParquet(dataFrame: DataFrame, path: String): Unit = dataFrame
      .coalesce(1)
      .write
      .mode("overwrite")
      .parquet(path)

   def readParquetData(spark: SparkSession, path: String): DataFrame = spark
      .read
      .format("parquet")
      .load(path)

   def flushToKafkaTopic(df: DataFrame, topic: String) = df
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("topic", topic)
      .save()

   def readFromKafka(spark: SparkSession, topic: String) = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("includeHeaders", "false")
      .load()
}
