package logic
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
//import org.apache.spark.sql.catalyst.plans.logical.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

object BootStrap extends App {

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.master", "local[1]")
    .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  val rawDataframe = Seq(("2018-01-01 11:00:00","u1"),
  ("2018-01-01 12:10:00","u1"),
  ("2018-01-01 13:00:00","u1"),
  ("2018-01-01 13:25:00","u1"),
  ("2018-01-01 14:40:00","u1"),
  ("2018-01-01 15:10:00","u1"),
  ("2018-01-01 16:20:00","u1"),
  ("2018-01-01 16:50:00","u1"),
  ("2018-01-01 11:00:00","u2"),
  ("2018-01-02 11:00:00","u2") ).toDF("ts","user_id")

  val df = rawDataframe.withColumn("ts_converted", to_timestamp(col("ts"),"yyyy-MM-dd HH:mm:ss"))
    .withColumn("ts_lag",lag(col("ts_converted"),1)
      .over(Window.partitionBy(col("user_id")).orderBy("ts")))

  val df1 =df.withColumn("uid",
    when(col("ts_lag").isNull,concat(col("user_id"),col("ts_converted")))
      .when((unix_timestamp(col("ts_converted"))-unix_timestamp(col("ts_lag")))/(60) >= 30 ,
        concat(col("user_id"),col("ts_converted")))
          .otherwise(concat(col("user_id"),col("ts_lag"))
  ))
  df1.show(false)
  df1.printSchema()
}
