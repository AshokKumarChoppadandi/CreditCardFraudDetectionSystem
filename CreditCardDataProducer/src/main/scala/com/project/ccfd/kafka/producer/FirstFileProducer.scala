package com.project.ccfd.kafka.producer

import java.nio.file.{Files, Paths}
import java.util
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

object FirstFileProducer {
  def main(args: Array[String]): Unit = {

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "root_kafka-broker3_1:9092,root_kafka-broker2_1:9092,root_kafka-broker1_1:9092")
    properties.setProperty("acks","all")
    properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    var index = 0
    val producer = new KafkaProducer[String, String](properties)
    try{
      val lines = Source.fromFile("src/main/resources/TestInput.txt").getLines()
      lines.foreach(x => {
        producer.send(new ProducerRecord[String, String]("test", Integer.toString(index + 1), x))
      })
    } catch {
      case ex: Exception => {
        println("Exception Occurred while producing the data...!!!")
        throw ex
      }
    } finally {
      producer.close()
    }
  }
}
