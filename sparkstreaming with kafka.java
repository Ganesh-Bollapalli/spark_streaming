Kfka - SparkStreaming : https://sparkbyexamples.com/spark/spark-streaming-with-kafka/

Linking :

groupId = org.apache.spark
artifactId = spark-streaming-kafka-0-10_2.12
version = 3.4.0

Creating DStream :

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

val kafkaParams = Map[String, Object](
  "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
  "key.deserializer" -> classOf[StringDeserializer],
  "value.deserializer" -> classOf[StringDeserializer],
  "group.id" -> "use_a_separate_group_id_for_each_stream",
  "auto.offset.reset" -> "latest",
  "enable.auto.commit" -> (false: java.lang.Boolean)
)

val topics = Array("topicA", "topicB")
val stream = KafkaUtils.createDirectStream[String, String](
  streamingContext,
  PreferConsistent,
  Subscribe[String, String](topics, kafkaParams)
)

stream.map(record => (record.key, record.value))


Creating RDD :


val offsetRanges = Array(
  // topic, partition, inclusive starting offset, exclusive ending offset
  OffsetRange("test", 0, 0, 100),
  OffsetRange("test", 1, 0, 100)
)

val rdd = KafkaUtils.createRDD[String, String](sparkContext, kafkaParams, offsetRanges, PreferConsistent)


Locations Stratergies : 

For performance reasons that the Spark integration keep cached consumers on executors (rather than recreating them for each batch), 
and prefer to schedule partitions on the host locations that have the appropriate consumers.
For cachcing consumer on spark excutors for getting partitions (preferCosistent, preferBrokers, preferFixed)

Consumer Stratergies :

ConsumerStrategies provides an abstraction that allows Spark to obtain properly configured consumers
ConsumerStrategies.Subscribe to subscribe specific topics abd we need to give partiitons and offset also


*****Be aware that the one-to-one mapping between RDD partition and Kafka partition does not 
remain after any methods that shuffle or repartition, e.g. reduceByKey() or window().*****

Obtaining offsets :

stream.foreachRDD { rdd =>
  val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
  rdd.foreachPartition { iter =>
    val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
    println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
  }
}


///// we need to store our offsets for getting msgs atleast once, atmost once, exactly once ////
1. checkpointing
2. kafka itself
3. your own datastore


2. kafka itself :
Commit offset after output is stored :

stream.foreachRDD { rdd =>
  val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

  // some time later, after outputs have completed
  stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
}