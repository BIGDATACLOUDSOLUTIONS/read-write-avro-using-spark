package com.example.json.producer.amazon_reviews

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.json4s.jackson.Serialization.write
import org.json4s.DefaultFormats

import java.util.Properties
import scala.io.Source

class AmazonReviewsKafkaJsonProducer(reviewFilePath: String) extends Thread {

  val topic = "amazon-reviews-json"

  val ipaddress = "localhost"
  val bootStrapServerIP = s"$ipaddress:8097,$ipaddress:8098,$ipaddress:8099"

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServerIP)
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "emp_producer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  implicit val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

  override def run() {

    implicit val formats: DefaultFormats.type = DefaultFormats

    Source.fromFile(reviewFilePath).getLines.foreach { line =>
      val key = line.split("\t")
      val caseClassObject = amazonCustomerReviewsUSModel(key(0), key(1), key(2), key(3), key(4), key(5), key(6), key(7), key(8), key(9), key(10), key(11), key(12), key(13), key(14))
      val reviewPayload= write(caseClassObject)

      val record = new ProducerRecord[String, String](topic, reviewPayload)
      producer.send(record)
    }
  }
}

object AmazonReviewsKafkaJsonProducer {

  def main(args: Array[String]): Unit = {

    val basePath = "C:\\RajeshKumar\\Datasets\\AmazonUS_CustomerReviewsDataset"
    List(s"$basePath\\amazon_reviews_multilingual_US_v1_00.tsv",
      s"$basePath\\amazon_reviews_us_Apparel_v1_00.tsv")
      .foreach { file =>
        val producer = new AmazonReviewsKafkaJsonProducer(file)
        producer.setName(file)
        producer.start()
      }
  }

}
