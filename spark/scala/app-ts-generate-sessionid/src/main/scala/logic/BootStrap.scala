package logic
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql

object BootStrap extends App {

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.master", "local[1]")
    .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._
  val df = spark.sparkContext.parallelize(Seq(1, 2, 3, 4, 5)).toDF("Number")
  df.printSchema()
  df.show()
}
