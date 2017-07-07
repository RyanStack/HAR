import com.typesafe.config.ConfigFactory
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.TaskContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.scheduler._

import com.datastax.spark.connector._

/**
  * Created by ryanstack on 5/12/17.
  */
object HarConsumer extends App {

  //TODO: INVALIDATE CACHES?
  implicit val config = ConfigFactory.load().getConfig("har_consumer")

  println("Initializing HarConsumer")

  val sparkConf = InitializeConfigurations.createSparkConf()

  //TODO: KRYO CLASSES
  //TODO: METRIC ACCUMULATORS

  implicit val ssc = new StreamingContext(sparkConf, Seconds(config.getInt("batch_interval")))
  implicit val sc = ssc.sparkContext

  implicit val kafkaParams = InitializeConfigurations.createKafkaParams()
  implicit val kafkaTopics = Array(config.getString("topic"))

  val savedKafkaOffsets = KafkaStreaming.getKafkaOffsets
  var currentOffsetRanges = Array[ApplicationKafkaOffsets]()


  val kafkaStream = KafkaStreaming.createKafkaStream(savedKafkaOffsets).transform { rdd =>
    currentOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges.map { o =>
      println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      new ApplicationKafkaOffsets(config.getString("app_name"), o.topic, o.partition, o.untilOffset)
    }
    rdd
  }

//  val steps: Unit = kafkaStream.foreachRDD { rdd =>
//    rdd.foreach { record =>
//
////      record.filter(rec => rec.value().split(",")(0)==1)
//      println("Printing key and value")
//      println(record.key)
//      print(record.value)
//
//      val parts = record.value().split(",")
//      val id = parts(0)
//      val action = parts(1)
//      val time = parts(2)
//      val x = parts(3)
//      val y = parts(4)
//      val z = parts(5)
//
//      val step = Step(id, action, time, x, y, z)
//      step
//    }
//  }

  val mySteps: DStream[Step] = kafkaStream.mapPartitions { iterPart =>
    iterPart.map { record =>

      val parts = record.value().split(",")
      val id = parts(0)
      val action = parts(1)
      val time = parts(2)
      val x = parts(3)
      val y = parts(4)
      val z = parts(5)

      val step = Step(id, action, time, x, y, z)
      step

    }
  }

  save(mySteps)


  def save(mySteps: DStream[Step]): Unit ={
    val keySpace = config.getString("cassandra.keyspace")
    val tableName = config.getString("cassandra.data-table")
    val colSelector = SomeColumns("id", "action", "time", "x", "y", "z")
    mySteps.foreachRDD(rdd => {
      rdd.saveToCassandra(keySpace, tableName, columns = colSelector)
    })


  }
//  println(myCount)

  println("Beginning and End of application")

  ssc.addStreamingListener(new JobListener(
    sc=ssc.sparkContext,
    completeBatch=this.completeBatch
  ))



  ssc.start()             // Start the computation
  ssc.awaitTermination()  // Wait for the computation to terminate




  def completeBatch(batchNum: Int, sc: SparkContext, batchInfo: BatchInfo): Unit = {
    println("In complete batch")
  }
}


case class Step(id:String, action:String, time:String, x:String, y:String, z:String) extends Serializable