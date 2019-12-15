package ww.nz.test.streaming

import org.apache.kafka.clients.producer.{KafkaProducer, _}
import org.slf4j.{Logger, LoggerFactory}

//kafkaproducer wrapper
class KProducer(createProducer: () => Producer[String,String]) extends Serializable{
  lazy val producer:Producer[String,String] = createProducer()

  val logger:Logger = LoggerFactory.getLogger(this.getClass)

  var counter:Long = 0
  //log out records count then clear counter
  def count():Unit = {
    logger.debug(s"##################Records count: $counter##################")
    counter=0
  }

  //(topic,value)
  def aksend(topic:String,value:String):Unit ={
    producer.send(
      new ProducerRecord[String,String](topic,value),
      new Callback {
        override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
          counter +=1
        }
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
