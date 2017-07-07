import java.io.InputStream
import java.net.URL
import java.util.{Date, Properties}

import scala.collection.JavaConversions._


import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import com.typesafe.config.ConfigFactory

import org.joda.time._

import scala.io.Source

/**
  * Created by ryanstack on 5/12/17.
  */
object HarGenerator extends App {

  implicit val config = ConfigFactory.load().getConfig("har_generator")
  val producer = InitializeConfigurations.createKafkaProducer()

  val events = config.getInt("events")
  val topic = config.getString("topic")

  val file: InputStream = getClass.getResourceAsStream("har.txt")
  val raw_lines: List[String] = scala.io.Source.fromInputStream(file).getLines.toList
  val lines = raw_lines.filter(step => step.split(",")(0) != "")
  val activityByUUID: Map[String, List[String]] = lines.groupBy(line => line.split(",")(0))
  val UUIDCount: Int = activityByUUID.size
  val keys = activityByUUID.keys


  val time = System.currentTimeMillis()

  while (true) {
    var counter = 0

    val runtime = new Date().getTime()

    1 to UUIDCount foreach { uuid =>
      val UUIDString = uuid.toString

      val time_series_uuid = activityByUUID.get(UUIDString)
      val time_series_length = time_series_uuid.size
      val time_step = counter % time_series_length
      val time_step_data = time_series_uuid.get(time_step)

      val data = new ProducerRecord[String, String](topic, UUIDString, time_step_data)

      //async
      //producer.send(data, (m,e) => {})
      //sync
      producer.send(data)
    }

    counter += 1
    Thread.sleep(1)
  }
  producer.close()

}
