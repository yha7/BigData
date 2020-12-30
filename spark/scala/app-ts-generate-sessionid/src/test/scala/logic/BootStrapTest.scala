package logic

import org.scalatest._
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class BootStrapTest extends AnyFlatSpec{
  "A method generateSessionId" should "return seq of tuples" in {

    val user_id = "u1"

    val clickList: Seq[String] = Seq("2018-01-01 11:00:00",
      "2018-01-01 12:10:00",
      "2018-01-01 13:00:00",
      "2018-01-01 13:25:00",
      "2018-01-01 14:40:00",
      "2018-01-01 15:10:00",
      "2018-01-01 16:20:00",
      "2018-01-01 16:50:00")

    val tsList: Seq[Long] = Seq(0, 4200, 3000, 1500, 4500, 1800, 4200, 1800)

    val actualSequence = logic.SessionIdGenerator.generateSessionId(user_id,clickList,tsList)
    val expectedSequence = Seq(
      ("2018-01-01T11:00:00.000+05:30u1","2018-01-01 11:00:00",0),
      ("2018-01-01T12:10:00.000+05:30u1","2018-01-01 13:00:00",3000),
      ("2018-01-01T12:10:00.000+05:30u1","2018-01-01 12:10:00",0),
      ("2018-01-01T12:10:00.000+05:30u1","2018-01-01 13:25:00",1500),
      ("2018-01-01T14:40:00.000+05:30u1","2018-01-01 14:40:00",0),
      ("2018-01-01T14:40:00.000+05:30u1","2018-01-01 15:10:00",1800),
      ("2018-01-01T16:20:00.000+05:30u1","2018-01-01 16:20:00",0),
      ("2018-01-01T16:20:00.000+05:30u1","2018-01-01 16:50:00",1800)
    )

    assertResult(true)(expectedSequence.toSet == actualSequence.toSet)

  }
  "A method getSessionId" should "return a Dataframe" in {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local[1]")
      .getOrCreate()

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

    val actualDataframe = SessionIdGenerator.getSessionIds(rawDataframe)

    val expectedDataframe = Seq(
      ("u1","2bea585598988c15bbdd24e25be9f223","2018-01-01 11:00:00",0    ),
      ("u1","326861b864ad8f5ddbc00ec22692899b","2018-01-01 12:10:00",0    ),
      ("u1","326861b864ad8f5ddbc00ec22692899b","2018-01-01 13:00:00",3000 ),
      ("u1","326861b864ad8f5ddbc00ec22692899b","2018-01-01 13:25:00",1500 ),
      ("u1","80f5cfbf3c9c599c6bf6d9a37334975a","2018-01-01 14:40:00",0    ),
      ("u1","80f5cfbf3c9c599c6bf6d9a37334975a","2018-01-01 15:10:00",1800 ),
      ("u1","0e17ddbe864422f7da140836eab96307","2018-01-01 16:20:00",0    ),
      ("u1","0e17ddbe864422f7da140836eab96307","2018-01-01 16:50:00",1800 ),
      ("u2","dc587172df934d03848aa972122fb4a3","2018-01-01 11:00:00",0    ),
      ("u2","b95f1297ade3e6ebaf52a98fb2980f8d","2018-01-02 11:00:00",0    )
    ).toDF("user_id","sessionId","click_time","activity_time")

       assertResult(true)(expectedDataframe.except(actualDataframe).count() == 0)

  }


}

