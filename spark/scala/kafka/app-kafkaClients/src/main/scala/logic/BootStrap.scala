package logic

import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.{Logger, PropertyConfigurator}
import java.util.Properties
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import java.util

object BootStrap extends App {

  val log = Logger.getLogger(BootStrap.getClass)
  PropertyConfigurator.configure("src/main/resources/conf/log4j.properties")

  log.info(" Creating Spark Context and Streaming Context")
  val conf = new SparkConf().setMaster("local[2]").setAppName("app-kafka-clients")

  val ssc: StreamingContext = new StreamingContext(conf, Seconds(1))
  val sc: SparkContext = ssc.sparkContext

  // Need to use slf4j,log4j dependencies.Otherwise we get an exception while using kafka api
  log.info("Creating Producer and Consumer property object")
  val producerProperties = new Properties()
  val bootstrapServers = "127.0.0.1:9092"

  // Using Masks from Producer config object
  // Serializer package is present in common package
  producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  producerProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")


  // Consumer properties
  val kafkaParams: scala.collection.Map[scala.Predef.String, java.lang.Object] = Map[String, Object](
    "bootstrap.servers" -> bootstrapServers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "test-group2",
    "auto.offset.reset" -> "earliest")

  log.info("Creating Kafka producer connection object recipe and broadcasting to all executors")
  val kafkaSink = sc.broadcast(KafkaSink(producerProperties))

  val topics: scala.Iterable[java.lang.String] = List("input_topic")

  val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))


  /*
  // foreachPartition is now not required
   dstream.foreachRDD(rdd => {
      rdd.foreachPartition { partitionRecords =>
        partitionRecords.foreach(cr => println(cr.value()))
      }
    })
  */

  dstream.foreachRDD(rdd => {
    if (!rdd.isEmpty()) {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreach { cr =>
        println(cr.value())
        //Implement producer logic here

        kafkaSink.value.send("output_topic", cr.value())

        // Test: Creates producer for every batch
        /*val producer: KafkaProducer[String, String] = new KafkaProducer(producerProperties)
        println("PRODUCER RECORD OBJECT IS::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::"+producer)
        val record: org.apache.kafka.clients.producer.ProducerRecord[String, String] = new ProducerRecord[String,String]("output_topic",cr.value())
        producer.send(record)*/

        // Test:Creates one producer per executor which is used across the batches

        /*val x: KafkaProducer[String, String] =  kafkaSink.value.producer
        println("PRODUCER RECORD OBJECT IS::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::"+x)
        x.send(new ProducerRecord("output_topic",cr.value()))*/


      }
      dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges, new OffsetCommitCallback {
        override def onComplete(map: util.Map[TopicPartition, OffsetAndMetadata], e: Exception): Unit =
          if (e != null) {
            log.error(s"commiting offsets to kafka failed due to " /*+ e.getMessage*/)
            // throw  e
            // Try to avoid throwing exception here.
            // We get multiple exceptions, when commit fails,rebalance takes place or network issues.
          }

      })
    }
    else
      log.info("Ignoring Empty rdd")
  })

  ssc.start()
  ssc.awaitTermination()

}
