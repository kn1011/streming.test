package ww.nz.test.streaming

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RunStreaming {

  //spark streaming demo
  def main(args:Array[String]):Unit={

    val producerCluster:String = "host1:9092,host2:9092,host3:9092"
    val consumerCluster:String = "host1:9092,host2:9092,host3:9092"

    val sparkConf = new SparkConf()
      .set("spark.streaming.kafka.maxRatePerPartition","10000")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sparkStreaming = new StreamingContext(new SparkContext(sparkConf),Seconds(3))

    val producer :Broadcast[KProducer]= sparkStreaming.sparkContext.broadcast(KProducer.apply(ConfigAll.producerConfig(producerCluster)))
    //configure several topics to consume
    val topics:Array[String] =Array("topic1","topic2","topic3")
    val directKafkaStream = KafkaUtils.createDirectStream(sparkStreaming, PreferConsistent, Subscribe[String, String](topics,ConfigAll.consumerConfig(consumerCluster)))
    var offsetsRanges:Array[OffsetRange] = null

    directKafkaStream.foreachRDD(rdd => {
      //record offsets
      offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.foreachPartition(part =>
        //function: re-send record to a new topic
        part.foreach(record => producer.value.aksend(record.topic()+"_reproduce",record.value())
        ))

      producer.value.count()
      //submit offsets
      directKafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetsRanges)
    })

    sparkStreaming.start()
    sparkStreaming.awaitTermination()
  }
}
