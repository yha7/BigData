package logic

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.joda.time.DateTime
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.udf
import java.text.SimpleDateFormat
import org.joda.time.Seconds

object SessionIdGenerator {

  //creation of udf
  def generateSessionId(userId: String, clickList: Seq[String], tsList: Seq[Long]) = {
    val tmo1 = 60 * 60
    val tmo2 = 2 * 60 * 60
    val DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"
    val dateFormat = new SimpleDateFormat(DATE_FORMAT)
    val clickListInDate = clickList.map(s => new DateTime(dateFormat.parse(s)))
    var currentSessionStartTime = clickListInDate(0);
    var sessionStartTime = Seq(currentSessionStartTime)

    val totalElements = clickList.length
    for (i <- 1 to totalElements - 1) {
      if (tsList(i) >= tmo1) {
        currentSessionStartTime = clickListInDate(i)
      }
      else {
        val timeDiff = Seconds.secondsBetween(currentSessionStartTime, clickListInDate(i)).getSeconds
        if (timeDiff >= tmo2) {
          currentSessionStartTime = clickListInDate(i)
        }
        else {
          currentSessionStartTime = currentSessionStartTime
        }
      }

      sessionStartTime = sessionStartTime :+ currentSessionStartTime

    }

    val activityTimeList = tsList.map(x=> if(x > 3600) 0 else x)
    ((sessionStartTime.map(x => x.toString + userId) zip clickList) zip activityTimeList).map(x=>(x._1._1,x._1._2,x._2))

  }

  def getSessionIds(rawDataframe:DataFrame):DataFrame =
  {
    //Adding two columns ts_converted and ts_lag in timestamp format to raw Datframe where ts_converted is a timestamp format of user click time.
    val dataframeWithClickTimeAsTimestamp = rawDataframe
      .withColumn("ts_converted", to_timestamp(col("ts"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("ts_lag", lag(col("ts_converted"), 1)
        .over(Window.partitionBy(col("user_id")).orderBy("ts")))

    //Adding a column ts_diff in Long format which is the time difference of current click time and previous click time.
    val dataframeWithTimeDifferenceColumn = dataframeWithClickTimeAsTimestamp.withColumn("ts_diff",
      (unix_timestamp(col("ts_converted")) - unix_timestamp(col("ts_lag"))))
      .withColumn("ts_diff", when(col("ts_diff").isNull, 0).otherwise(col("ts_diff")))

   //Aggregating the values by doing GROUP BY on user_id and collecting the user clicks and time differences as a list
    val dataframeWithAggregatedValues = dataframeWithTimeDifferenceColumn.groupBy("user_id").
      agg(collect_list(col("ts")).as("clickList"),
      collect_list(col("ts_diff")).as("tsList"))

    //Creating a UDF which generates a session ID's
    val generateSessionId_UDF = udf[Seq[(String, String, Long)], String, Seq[String], Seq[Long]](generateSessionId)

    //Calling the UDF which creates a session ID and doing MD 5 hash on session ID column
    val finalDataframeWithSessionID = dataframeWithAggregatedValues
      .withColumn("session_idAndclick_time",
        explode(generateSessionId_UDF(col("user_id"),
          col("clickList"), col("tsList"))))
      .select(col("user_id"),
        col("session_idAndclick_time._1").as("sessionId"),
        col("session_idAndclick_time._2").as("click_time"),
        col("session_idAndclick_time._3").as("activity_time"))
      .withColumn("sessionId", md5(col("sessionId")))

    finalDataframeWithSessionID

  }

  def process(spark:SparkSession): Unit = {
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val rawDataframe = Seq(("2018-01-01 11:00:00", "u1"),
      ("2018-01-01 12:10:00", "u1"),
      ("2018-01-01 13:00:00", "u1"),
      ("2018-01-01 13:25:00", "u1"),
      ("2018-01-01 14:40:00", "u1"),
      ("2018-01-01 15:10:00", "u1"),
      ("2018-01-01 16:20:00", "u1"),
      ("2018-01-01 16:50:00", "u1"),
      ("2018-01-01 11:00:00", "u2"),
      ("2018-01-02 11:00:00", "u2")).toDF("ts", "user_id")

    //Adding a columns year,month and date using click_time to partition the dataframe which writing to sink.
    val finalDataframe = getSessionIds(rawDataframe:DataFrame).withColumn("yr",year(col("click_time")))
      .withColumn("mm",month(col("click_time")))
      .withColumn("dd",dayofmonth(col("click_time")))

    //Writing to sink
    finalDataframe.coalesce(1).write.partitionBy("yr","mm","dd").format("csv").mode("overwrite")
      .save("/home/yha7/data/output/sessionIdData")

    //Creating a temporary view on a Dataframe
    finalDataframe.createOrReplaceTempView("user_clicks_info")

    //Queries to check the time spent by a user in a day and a month
    val checkTimeSpentInMonth = spark.sql("SELECT user_id,yr,mm,sum(activity_time) FROM user_clicks_info GROUP BY user_id,yr,mm")
    val checkTimeSpentInDay = spark.sql("SELECT user_id,yr,mm,dd,sum(activity_time) FROM user_clicks_info GROUP BY user_id,yr,mm,dd")
    checkTimeSpentInMonth.show(false)
    checkTimeSpentInDay.show(false)
  }
}