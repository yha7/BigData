package logic

import org.apache.spark.sql.SparkSession

object BootStrap extends App {

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.master", "local[1]")
    .getOrCreate()

  SessionIdGenerator.process(spark)
}