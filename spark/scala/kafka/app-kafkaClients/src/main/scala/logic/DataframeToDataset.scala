package logic

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.rdd.RDD

object DataframeToDataset extends App {

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.master", "local[1]")
    .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames

  import spark.implicits._
  import org.apache.spark.sql.Encoders
  import scala.reflect.runtime.universe._


  case class schemaProvider(Name: String, Age: Int, Salary: String)


  val df: DataFrame = spark.sparkContext.parallelize(Seq(("Kane", 27, "5000"), ("Maxwell", 23, "7000"))).toDF("Name", "Age", "Salary")
  println("The schema of a datframe is: ")
  df.show()
  df.printSchema()

  //Create a Dataset

  //Approach 1
  //Dataframe to Dataset
  println("Approach1")

  val dataset = df.as[schemaProvider]
  dataset.show()
  dataset.printSchema()
  // The schema of the dataframe(column names) should match with case class schema.Otherwise we will hit an exception


  //Approach 2
  //Dataframe to Dataset using TypeTags. It may be an over-engineering
  println("Approach2")

  def getDS[T <: Product](df: DataFrame)(implicit tag: TypeTag[T]) = {
    val ds = df.as[T]
    ds.show()
    ds.printSchema()
  }

  getDS[schemaProvider](df)
  // Schema should be same while creating dataframe and dataframe to dataset

  //Approach3
  //RDD to Dataframe to Dataset
  println("Approach3")

  val rdd = spark.sparkContext.textFile("/home/yha7/IdeaProjects/BigData/spark/scala/kafka/app-kafkaClients/src/main/resources/testData")

  def getDSFromRdd[T <: Product](rdd: RDD[String])(implicit tag: TypeTag[T]) = {

    // case class schema is converted to struct type schema which is an arg for schema method below
    val schema: StructType = Encoders.product[T].schema

    // Create a Dataframe
    val df2 = spark.read.schema(schema).json(rdd)

    // Create a Dataset
    val ds2 = df2.as[T]
    ds2.show()
    ds2.printSchema()
  }
  getDSFromRdd[schemaProvider](rdd)

}
