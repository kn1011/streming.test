package ww.nz.test.streaming

import org.apache.kafka.clients.producer.{KafkaProducer, _}

//kafkaproducer wrapper
class KProducer(createProducer: () => Producer[String,String]) extends Serializable{
  lazy val producer:Producer[String,String] = createProducer()

  //(topic,value)
  def aksend(topic:String,value:String):Unit ={
    producer.send(
      new ProducerRecord[String,String](topic,value),
      new Callback {
        override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {}
      }
    )
  }
}

object KProducer{
  def apply(config:java.util.Properties):KProducer  ={
    val createFunc= () => {
      new KafkaProducer[String,String](config)
    }
    new KProducer(createFunc)
  }
}
