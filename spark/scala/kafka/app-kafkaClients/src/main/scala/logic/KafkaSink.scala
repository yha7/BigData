package logic

import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.producer.KafkaProducer
import java.util.Properties

class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {

  lazy val producer = createProducer()

  def send(topic: String, value: String): Unit = producer.send(new ProducerRecord(topic, value))
}
// Companion Object
object KafkaSink {
  def apply(producerProperties: Properties): KafkaSink = {
    val f = () => {
      new KafkaProducer[String, String](producerProperties)
    }
    new KafkaSink(f)
  }
}