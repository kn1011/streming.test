package ww.nz.test.streaming

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer

object ConfigAll {

  //producer config
  def producerConfig(producerClusterHost:String):java.util.Properties = {
    val prop = new java.util.Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,producerClusterHost)
    prop.put(ProducerConfig.CLIENT_ID_CONFIG,s"producer_sink")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,s"org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,s"org.apache.kafka.common.serialization.StringSerializer")
    prop
  }

  def consumerConfig(consumerClusterHost:String):Map[String,Object] = Map[String, Object](
    "bootstrap.servers" -> consumerClusterHost,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> s"nz_test_streaming",
    "auto.offset.reset" -> "lastest",
    "enable.auto.commit" -> (false: java.lang.Boolean))

}
