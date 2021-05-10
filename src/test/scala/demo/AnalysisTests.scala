package demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class AnalysisTests extends AnyFunSuite with BeforeAndAfterAll {
   private var spark: SparkSession = _

   val schema = StructType(
      StructField("country", StringType, true) :: Nil
   )

   override def beforeAll() {
      val sparkConf = new SparkConf()
         .setMaster("local[*]")
         .setAppName("Unit testing")
         .set("spark.ui.enabled", "false")

      spark = SparkSession.builder()
                          .config(sparkConf)
                          .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
   }

   test("getCountByCountry on an empty DF should return no results") {
      val input = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      val output = Analyse.getCountByCountry(input).collect()
      assert(output.length == 0)
   }

   test("getCountByCountry on a non-empty DF should return some results") {
      val inputData = Seq(Row("sweden"), Row("sweden"), Row("croatia"))
      val input = spark.createDataFrame(spark.sparkContext.parallelize(inputData), schema)
      val output = Analyse.getCountByCountry(input).collect()

      // There are two different countries in the input
      assert(output.length == 2)
   }

   override def afterAll(): Unit = {
      spark.stop()
   }

}
