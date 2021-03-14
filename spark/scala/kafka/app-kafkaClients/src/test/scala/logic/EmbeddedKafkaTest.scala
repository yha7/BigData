package logic

import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.consumer._
import java.nio.file.Files
import java.util.Properties
import kafka.server.KafkaConfig
import kafka.server.KafkaServerStartable
import org.apache.curator.test.TestingServer
import java.util
import scala.collection.JavaConverters._

object EmbeddedKafkaTest {

  //TODO (Work In Progress) Need to resolve dependency issues

  def main(args: Array[String]): Unit = {

    val testLogsDir = "Kafka-test-logs"
    val defaultPort = 9090
    val stringSerializer = "org.apache.kafka.common.serialization.StringSerializer"
    val stringDeserializer = "org.apache.kafka.common.serialization.StringDeserializer"
    val groupId = "testGroup"
    val key = "I am key"
    val value = "I am value"
    val topicName = "test_topic"

    val zooKeeperServer  = new TestingServer()
    val kafkaHost = "localhost"
    val kafkaPort = defaultPort

    val kafkaProps = new Properties()
    kafkaProps.put("broker.id","0")
    kafkaProps.put("log.dirs",Files.createTempDirectory(testLogsDir).toAbsolutePath.toString)
    kafkaProps.put("zookeeper.connect",zooKeeperServer.getConnectString)
    kafkaProps.put("port",kafkaPort.toString)
    kafkaProps.put("host.name","localhost")
    kafkaProps.setProperty("offsets.topic.replication.factor","1")

    val kafkaconfig = new KafkaConfig(kafkaProps)
    val kafkasever = new KafkaServerStartable(kafkaconfig)


    //kafka client properties

    val kafkaClientConf: util.Map[String, Object] = Map(
      "bootstrap.servers" -> s"$kafkaHost:$kafkaPort",
      "auto.offset.reset" -> "earliest",
      "key.serializer" -> stringSerializer,
      "value.serializer" -> stringSerializer,
      "key.Deserilizer" -> stringDeserializer,
      "value.Deserializer" -> stringDeserializer,
      "group.id" -> groupId
    ).asInstanceOf[Map[String,Object]].asJava

    zooKeeperServer.start()
    Thread.sleep(10000)
    kafkasever.startup()

    //creating producer and consumer
    //org.apache.zookeeper.AsyncCallback

    val kafkaProducer =  new KafkaProducer[String,String](kafkaClientConf)
    val kafkaConsumer = new KafkaConsumer[String,String](kafkaClientConf)

    val topicList = List(topicName)
    kafkaProducer.send(new ProducerRecord(topicName,key,value))

    kafkaConsumer.subscribe(topicList.asJava)

    while(true)
      {
        val records: ConsumerRecords[String, String] = kafkaConsumer.poll(200)
        for(i <- records.asScala)
          {

            println("key " +i.key() +
            "Value " +i.value())
          }

      }

    kafkaProducer.close()
    kafkasever.shutdown()
    zooKeeperServer.close()
    zooKeeperServer.stop()
  }
}
