package com.example.avro.producer.customer

import com.example.Customer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

class GenerateCustomer {

}


object GenerateCustomer {

  val properties = new Properties
  // normal producer
  properties.setProperty("bootstrap.servers", "127.0.0.1:9092")
  properties.setProperty("acks", "all")
  properties.setProperty("retries", "10")
  // avro part
  properties.setProperty("key.serializer", classOf[StringSerializer].getName)
  properties.setProperty("value.serializer", classOf[KafkaAvroSerializer].getName)
  properties.setProperty("schema.registry.url", "http://127.0.0.1:8081")

  val topic = "customer-avro"

  def main(args: Array[String]): Unit = {
    val producer: Producer[String, Customer] = new KafkaProducer[String, Customer](properties)

    val customer = Customer.newBuilder
      .setAge(34)
      .setAutomatedEmail(false)
      .setFirstName("John")
      .setLastName("Doe")
      .setHeight(178f)
      .setWeight(75f)
      .build

    val producerRecord= new ProducerRecord[String, Customer](topic, customer)
    println(customer)

    producer.send(producerRecord)

    producer.flush();
    producer.close();
  }



}
