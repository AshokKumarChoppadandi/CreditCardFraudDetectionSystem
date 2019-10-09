package com.project.ccfd.kafka.consumer

import java.time.Duration
import java.util.Arrays
import java.util.Properties
import scala.collection.JavaConversions._

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}

object FirstKafkaConsumer {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group1")
    properties.setProperty("enable.auto.commit", "true")
    properties.setProperty("auto.commit.interval.ms", "1000")
    properties.setProperty("auto.offset.reset", "earliest")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer = new KafkaConsumer[String, String](properties)
    consumer.subscribe(Arrays.asList("test"))

    while(true) {
      val records = consumer.poll(Duration.ofMillis(100))

      for(record <- records) {
        System.out.println(
          s"""
            | Partition :: ${record.partition()}, Offset :: ${record.offset()}, Key :: ${record.key()}. Message :: ${record.value()}
          """.stripMargin)
      }
    }
  }
}
