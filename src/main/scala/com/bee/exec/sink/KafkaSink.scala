package com.bee.exec.sink


import java.util.Properties
import java.util.concurrent.ExecutionException

import com.bee.common.BeeConf
import com.bee.common.util.Logging
import com.bee.core.exec.BeeSink
import com.bee.core.exception._
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

/**
  * Created by han.kedang on 16/8/31.
  */
class KafkaSink(conf: BeeConf)
  extends BeeSink(conf: BeeConf){

  private val producer = new Producer(conf)

  override def execute(data: T): Long = {

    val t: (Int, String) = (data.i.toInt, data.d.toString)
    producer.send(t)
//    println(s"Kafka Sink: ${t._1}, ${t._2}")
    t._1

  }

}

/**
  *  kafka 生产者
  */
private class Producer(conf: BeeConf) extends Logging{
  private val server = conf.get("bee.sink.server", null)
  private val topic = conf.get("bee.sink.topic", null)
  private val isAsync: Boolean = conf.getBoolean("bee.sink.async", false)
  private val name = conf.get("bee.name", null)

  type T = (Int, String)

  if(server == null)
    throw new BeeException("kafka's server must be set !")

  if(topic == null)
    throw new BeeException("kafka's topic must be set !")

  private val pro = new Properties
  pro.put("bootstrap.servers", server)
  pro.put("client.id", name)
  pro.put("acks", "all");
  pro.put("retries", "0");
  pro.put("batch.size", "16384");
  pro.put("linger.ms", "1");
  pro.put("buffer.memory", "33554432");
  pro.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
  pro.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  private val producer = new KafkaProducer[Int, String](pro)


  def send(data: T) = {
    val (x, y) = data
    val startTime = System.currentTimeMillis

    try {
      if (isAsync) {
        producer.send(new ProducerRecord[Int, String](topic, x, y),
          new ProducerCallBack(startTime, x, y))
      } else {
        producer.send(new ProducerRecord[Int, String](topic, x, y)).get
        debug(s"Sent message: (${x}, ${y})")
      }
    }
    catch {
      case e: (InterruptedException, ExecutionException) => e.getMessage
    }
  }

  class ProducerCallBack(startTime: Long, key: Int, message: String)
    extends Callback with Logging{

    override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
      val elapsedTime: Long = System.currentTimeMillis - startTime

      if(recordMetadata != null)
        debug("message(" + key + ", " + message + ") " +
          "sent to partition(" + recordMetadata.partition + "), " +
          "offset(" + recordMetadata.offset + ") in " + elapsedTime + " ms")
      else {
        error(s"put kafka fail! key: ${key}, message: ${message}, ${e.printStackTrace()}")
      }

    }

  }


}


