package logic

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import java.util
import java.util.Properties

//Rebalance Listeners  cannot be achieved for Spark streaming job as subscribe method
// of Spark streaming api do not accept Rebalance Listener object as an argument where standalone kafka api's subscribe method does!!!

object HandleRebalance extends ConsumerRebalanceListener{
  override def onPartitionsRevoked(collection: util.Collection[TopicPartition]): Unit =
    {

    }

  override def onPartitionsAssigned(collection: util.Collection[TopicPartition]): Unit = ???
  /*val c = new KafkaConsumer[String,String](new Properties())
  c.subscribe()*/
}


