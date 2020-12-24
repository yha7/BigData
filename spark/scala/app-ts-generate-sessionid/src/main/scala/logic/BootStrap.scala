package logic
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.joda.time.DateTime
//import org.apache.spark.sql.catalyst.plans.logical.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.udf
import java.text.SimpleDateFormat
import java.util.Date
import org.joda.time
import org.joda.time.Seconds

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

/*  val df1 =df.withColumn("uid",
    when(col("ts_lag").isNull,concat(col("user_id"),col("ts_converted")))
      .when((unix_timestamp(col("ts_converted"))-unix_timestamp(col("ts_lag")))/(60) >= 30 ,
        concat(col("user_id"),col("ts_converted")))
          .otherwise(concat(col("user_id"),col("ts_lag"))
   ))*/

  val df2 = df.withColumn("ts_diff",
    (unix_timestamp(col("ts_converted"))-unix_timestamp(col("ts_lag"))))
    .withColumn("ts_diff",when(col("ts_diff").isNull,0).otherwise(col("ts_diff")))

  val df3 = df2.groupBy("user_id").agg(collect_list(col("ts")).as("clickList"),
    collect_list(col("ts_diff")).as("tsList"))


  /*df3.show(false)
  df3.printSchema()*/
  //creation of udf
  def generateSessionId(userId:String,clickList: Seq[String],tsList:Seq[Long]) =
    {
      val tmo1 =  60 * 60
      val tmo2 = 2 * 60 * 60
      val DATE_FORMAT= "yyyy-MM-dd HH:mm:ss"
      val dateFormat=new SimpleDateFormat(DATE_FORMAT)
      val clickListInDate=clickList.map(s=>new DateTime(dateFormat.parse(s)))
      var currentSessionStartTime= clickListInDate(0);
      var sessionStartTime=Seq(currentSessionStartTime)

      val totalElements=clickList.length
      for(i<-1 to totalElements-1)
        {
          if(tsList(i)>=tmo1)
            {
              currentSessionStartTime=clickListInDate(i)
            }
            else {
            val timeDiff=Seconds.secondsBetween(currentSessionStartTime,clickListInDate(i)).getSeconds
            if(timeDiff>=tmo2)
              {
                currentSessionStartTime=clickListInDate(i)
              }
              else
              {
                println("else block")
                println(clickList(i))
                currentSessionStartTime=currentSessionStartTime
              }
          }
          sessionStartTime=sessionStartTime:+currentSessionStartTime
        }
      //column(sessionStartTime)
      //println(":::::::::::::::::::::::::::::::::::::::"+sessionStartTime.size)
      //val q = sessionStartTime.map(x=>x.toString + userId) zip clickList
      //q.foreach(println(_))
      sessionStartTime.map(x=>x.toString + userId) zip clickList

    }

  //val x = spark.udf.register("sessionIdGenerator",generateSessionId)
  //udf(generateSessionId)
  val  udf1 = udf[Seq[(String, String)],String, Seq[String],Seq[Long]](generateSessionId)
  val df4 = df3.withColumn("session_idAndclick_time",explode(udf1(col("user_id"),col("clickList"),col("tsList"))))
    .select($"user_id", $"session_idAndclick_time._1".as("sessionId"), $"session_idAndclick_time._2".as("click_time"))

    //.withColumn("clickList",explode(col("clickList")))
  //val df5 = df4.select(col("user_id"),col("clickList"),col("session_id"))
  df4.show(false)
  df4.printSchema()
}