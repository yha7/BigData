import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.eventhubs._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, Trigger}

object EventhubBootstrap  extends App{

  val spark = SparkSession.builder().appName("app-EventHubConsumer").master("local[2]").getOrCreate()

  // Create EventHub consumer
  val endpoint = ""
  val connectionString: String = ConnectionStringBuilder().setEventHubName("test-eventhub").build
  val eventHubConf: EventHubsConf = EventHubsConf(connectionString).setConsumerGroup("$Default").setStartingPosition(EventPosition.fromStartOfStream)

  val batchdf: DataFrame = spark.readStream.format("eventhubs").options(eventHubConf.toMap).load()

  val batchdfTypeCasted: DataFrame = batchdf.withColumn("Body", col("Body").cast(StringType))

  //val test1: DataStreamWriter[Row] = batchdfTypeCasted.writeStream

  val streamingQuery: StreamingQuery = batchdfTypeCasted.writeStream.outputMode("append").trigger(Trigger.ProcessingTime(0))
    .format("console").option("truncate", false).start()

  // The below lines added to stop the streaming job after certain period of time
  streamingQuery.awaitTermination(1000)

  // Stops the execution of this query if it is running. This waits until the termination of the query execution threads or until a timeout is hit.
  streamingQuery.stop()
}
