package com.example.avro.producer.reviews

import com.example.ReviewsV1
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.Collections
import java.util.Properties

import java.time.Duration


object ReviewsKafkaAvroConsumerV1 {

  def main(args:Array[String]):Unit={
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "127.0.0.1:9092")
    properties.put("group.id", "customer-consumer-group-v1")
    properties.put("auto.commit.enable", "false")
    properties.put("auto.offset.reset", "earliest")

    // avro part (deserializer)
    properties.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    properties.setProperty("value.deserializer", classOf[KafkaAvroDeserializer].getName)
    properties.setProperty("schema.registry.url", "http://127.0.0.1:8081")
    properties.setProperty("specific.avro.reader", "true")

    val kafkaConsumer = new KafkaConsumer[String, ReviewsV1](properties)

    val topic = "reviewsV1-avro"
    kafkaConsumer.subscribe(Collections.singleton(topic))

    println("Waiting for data...")

    while (true){
      System.out.println("Polling");
     val records: ConsumerRecords[String, ReviewsV1] = kafkaConsumer.poll(Duration.ofMillis(1000));

      records.forEach(record => {
        val reviews = record.value()
        println(reviews);
      })

      kafkaConsumer.commitSync()
    }
  }

}
