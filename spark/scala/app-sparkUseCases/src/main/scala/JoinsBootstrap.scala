import org.apache.spark._
import org.apache.spark.sql.SparkSession

object JoinsBootstrap extends App {

  val spark: SparkSession = SparkSession.builder().appName("app-implementSparkUsecases").master("local[2]").getOrCreate()
  import spark.implicits._

  val person = Seq(
    (0, "Bill Chambers", 0, Seq(100)),
    (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
    (2, "Michael Armbrust", 1, Seq(250, 100)))
    .toDF("id", "name", "graduate_program", "spark_status")

  val graduateProgram = Seq(
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley"))
    .toDF("id", "degree", "department", "school")

  val sparkStatus = Seq(
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor"))
    .toDF("id", "status")

  person.show()
  graduateProgram.show()
  sparkStatus.show()

  person.createOrReplaceTempView("person")
  graduateProgram.createOrReplaceTempView("graduateProgram")
  sparkStatus.createOrReplaceTempView("sparkStatus")

  // inner join
  val inner_joinExpression = person.col("graduate_program") === graduateProgram.col("id")
  val innerjoinedDF = person.join(graduateProgram,inner_joinExpression,"inner")
  innerjoinedDF.show(false)

  //outer join
  val outer_joinExpression = person.col("graduate_program") === graduateProgram.col("id")
  val outerjoinedDF = person.join(graduateProgram,outer_joinExpression,"outer")
  outerjoinedDF.show(false)

  // duplicate joining column names
  val gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")
  val joinExpr = gradProgramDupe.col("graduate_program") === person.col(
    "graduate_program")
  // erroraneous line
  //gradProgramDupe.join(person,joinExpr,"inner").show()

  // Two working approaches when we have duplicate joining column names:

  // Approach 1:
  //gradProgramDupe.join(person,"graduate_program").show()

  // Approach 2:
  //person.join(gradProgramDupe, joinExpr).drop(person.col("graduate_program")).show()

}
