# Technical task for Data Engineer position

Task requires the conversion of CSV to Parquet/Avro, some cleaning/aggregation and output to Kafka.

## Basic setup

### Data download and simple cleansing

Some lines in each file do not have clearly encoded line breaks.
The AWK statements below fix such cases (line ends in semicolon).

```bash
cd input
curlie -s https://cmshare.eea.europa.eu/s/B9dGkQGHtJoqPqJ/download > water_data.zip
unzip water_data.zip Waterbase_v2018_1_T_WISE4_AggregatedData.csv
awk '{printf "%s"(/;$/?FS:RS),$0}' < Waterbase_v2018_1_T_WISE4_AggregatedData.csv > clean_waterbase.csv

curlie -s https://www.eea.europa.eu/data-and-maps/data/waterbase-water-quality-2/tables-definitions/table-definitions-csv-file/download > definitions.zip
unzip definitions.zip Waterbase_v2018_1_dataset_definition_T_WISE4_AggregatedData.csv 
awk '{printf "%s"(/;$/?FS:RS),$0}' < Waterbase_v2018_1_dataset_definition_T_WISE4_AggregatedData.csv > clean_definition_waterbase.csv
```

### Kafka setup

The [docker compose](docker-compose.yml) file sets up Kafka and the Schema Registry.
The file is a copy of the standard Confluent Platform example, with unnecessary components removed.

```bash
docker compose up -d
docker compose ps
# Confirm port below by checking output of docker compose ps 
export KB=127.0.0.1:9092
```

### Kafka topic setup

We will set up a topic with three partitions and no replication, due to running on a single docker broker.
After that, confirm that the topic was created, and show config.

For observing the progress of Spark jobs, we can immediately start consuming from the topic using kafkacat.

```bash
kafka-topics --create  --bootstrap-server $KB --partitions 3 --replication-factor 1 --topic abc-demo
kafka-topics --bootstrap-server $KB --list | grep abc-demo
kafka-topics --describe --bootstrap-server $KB --topic abc-demo

# Immediately start consuming from the topic, do not stop when reaching the end
# Output format is 'partition: key: value'
# Leave this running in a separate terminal
kafkacat -C -b ${KB} -t abc-demo -o beginning -f '%p: %k: %s\n'
```

### Spark shell

This starts an interactive Spark Scala shell, with the local executor.
No code is executed on any remote clusters.

``` bash
SPARK_VERSION=3.1.1
spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_VERSION}
```

## Tasks

### Load data from CSV and convert to Parquet

Quote and escape options need tuning to match the CSV.
Schema inference works fine for this dataset.

Use coalesce to make sure we end up with only one file.

```scala
val df = (spark
    .read
    .format("csv")
    .option("header","true")
    .option("inferSchema", "true")
    .option("quote", "\"")
    .option("escape", "\"")
    .load("input/clean_waterbase.csv")
)


df.coalesce(1).write.mode("overwrite").parquet("output")
```

### Run aggregation tasks on data

We'll use the generated parquet output for later steps to improve performance.

Add a new column, extracting the two-character country code from the start of the field `monitoringSiteIdentifier`.
Confirm field exists in new schema.

Make the dataframe available in Spark SQL by creating a temporary view.

Run some basic queries, define a helper function for writing to Kafka, and write out a simple dataset.
At this point, the kafkacat should show new values in the topic.

```scala
val parquet_df = spark.read.format("parquet").load("output")

val df_with_country = parquet_df.withColumn("country",substring('monitoringSiteIdentifier,0,2))
df_with_country.printSchema

df_with_country.createOrReplaceTempView("aggregates")

// Try out sql with a basic query
spark.sql("select resultObservationStatus, count(*) from aggregates group by 1").show

val country_count = spark.sql("select country,count(*) AS count from aggregates group by 1 order by 2 desc")
val count_by_country = country_count.selectExpr("CAST(country AS STRING) AS key", "CONCAT(CAST(country AS STRING) ,':', CAST(count AS STRING)) AS value")

def flushToKafka(df: org.apache.spark.sql.DataFrame) = df.write.format("kafka").option("kafka.bootstrap.servers","127.0.0.1:9092").option("topic","abc-demo").save()

flushToKafka(count_by_country)
```

We can continue running SQL statements (or working with dataframe API) and sending output to Kafka.

### Streaming items from CSV files in small increments

We will set up Spark streaming to watch a directory with CSV files, perform some basic operations on records and send them on to Kafka

```bash
mkdir input/fragments

# clean up checkpoint location if you want to reprocess the files
rm -rf /tmp/checkpoint 
```

```scala
val inputSchema = (spark
    .read
    .format("csv")
    .option("header","true")
    .option("inferSchema", "true")
    .option("quote", "\"")
    .option("escape", "\"")
    .load("input/clean_waterbase.csv")
    .schema
)

val inputStream = (spark
    .readStream
    .option("header","true")
    .option("quote", "\"")
    .option("escape", "\"")
    .schema(inputSchema)
    .csv("input/fragments")
)

val inputStreamWithCountry = inputStream.withColumn("country",substring('monitoringSiteIdentifier,0,2))
inputStreamWithCountry.createOrReplaceTempView("aggregates")
val country_median = spark.sql("select country, max(resultMeanValue) as maximumOfMean from aggregates group by country")
val country_median_kv = country_median.selectExpr("CAST(country AS STRING) AS key", "CONCAT(CAST(country AS STRING) ,':', CAST(maximumOfMean AS STRING)) AS value")

// country_median.writeStream.format("console").option("truncate","true").outputMode("complete").start()

val output = (
    country_median_kv
        .writeStream
        .format("kafka")
        .outputMode("complete")
        .option("kafka.bootstrap.servers","127.0.0.1:9092")
        .option("topic","abc-demo")
        .option("checkpointLocation","/tmp/checkpoint")
        .start()
)
```

Use the following commands to feed parts of the input to Spark Streaming and observe output on :

```bash
sed -n -e 1p -e  2,10p input/clean_waterbase.csv > input/frag.csv ; mv input/frag.csv input/fragments/0.csv
sed -n -e 1p -e 11,20p input/clean_waterbase.csv > input/frag.csv ; mv input/frag.csv input/fragments/1.csv
sed -n -e 1p -e 21,30p input/clean_waterbase.csv > input/frag.csv ; mv input/frag.csv input/fragments/2.csv
sed -n -e 1p -e 21,30p input/clean_waterbase.csv > input/frag.csv ; mv input/frag.csv input/fragments/3.csv
sed -n -e 1p -e 31,40p input/clean_waterbase.csv > input/frag.csv ; mv input/frag.csv input/fragments/4.csv
```
