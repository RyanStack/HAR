import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf

import java.util.Properties

import com.typesafe.config.Config

/**
  * Created by ryanstack on 5/19/17.
  */
object InitializeConfigurations {
  def createKafkaProducer()(implicit config:Config): KafkaProducer[String, String] = {

    val props = new Properties()
    props.put("bootstrap.servers", config.getString("brokers"))
    props.put("client.id", "HarGenerator")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val kafkaProducer = new KafkaProducer[String, String](props)
    kafkaProducer
  }

  def createSparkConf()(implicit config:Config): SparkConf = {
    val sparkConf = new SparkConf().setAppName(config.getString("app_name"))
    sparkConf
  }

  def createKafkaParams()(implicit config:Config): Map[String, Object] = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> config.getString("brokers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> config.getString("group_id"),
      "auto.offset.reset" -> config.getString("auto.offset.reset"),
      "enable.auto.commit" -> config.getString("enable.auto.commit")
    )
    kafkaParams
  }

}
