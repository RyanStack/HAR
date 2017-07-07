import com.typesafe.config.Config
import com.datastax.spark.connector._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.scheduler._

/**
  * Created by ryanstack on 5/22/17.
  */
object KafkaStreaming {
  def getKafkaOffsets(implicit sc: SparkContext, config: Config): Array[ApplicationKafkaOffsets] = {
    if (config.getBoolean("kafka-offsets-table-init-skip")) {
      println("Skipping kafka offsets initialization")
      Array[ApplicationKafkaOffsets]()
    } else {
      val appName = config.getString("app_name")
      val topic = config.getString("topic")
      val offsets = sc.cassandraTable[ApplicationKafkaOffsets](config.getString("keyspace"), config.getString("kafka-offsets-table")).
        filter(offsetdata => {
          offsetdata.application.equalsIgnoreCase(appName) && offsetdata.topic.equalsIgnoreCase(topic)
        }).collect()
      offsets
      //TODO:CHECK IF OFFSETS ARE VALID!
    }
  }

  def createKafkaStream(savedKafkaOffsets: Array[ApplicationKafkaOffsets])
                       (implicit ssc: StreamingContext,
                        kafkaParams: Map[String, Object],
                        kafkaTopics: Array[String]): InputDStream[ConsumerRecord[String, String]] = {
    if (savedKafkaOffsets.size > 0) {
      val kafkaTopicPartitionsAndOffsetsMap = savedKafkaOffsets.map {o =>
        (new TopicPartition(o.topic, o.partition), o.offset)
      }.toMap
      KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        ConsumerStrategies.Assign[String, String](kafkaTopicPartitionsAndOffsetsMap.keys.toList, kafkaParams, kafkaTopicPartitionsAndOffsetsMap)
      )
    } else {
      KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](kafkaTopics, kafkaParams))
    }
  }



}

class JobListener(sc: SparkContext, completeBatch: (Int, SparkContext, BatchInfo) => Unit) extends StreamingListener {

  var batchNum = 0
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    batchNum += 1
    println("Finished running" + batchNum.toString + "micro batch")
    completeBatch(batchNum, sc, batchCompleted.batchInfo)
  }
}

case class ApplicationKafkaOffsets(application:String, topic:String, partition:Int, offset:Long) extends Serializable