package com.example.avro.producer.reviews

import com.example.ReviewsV1
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

class ReviewsKafkaAvroProducerV1(productFilePath: String) extends Thread {

  implicit val properties: Properties = new Properties
  // normal producer
  properties.setProperty("bootstrap.servers", "127.0.0.1:9092")
  properties.setProperty("acks", "all")
  properties.setProperty("retries", "10")
  // avro part
  properties.setProperty("key.serializer", classOf[StringSerializer].getName)
  properties.setProperty("value.serializer", classOf[KafkaAvroSerializer].getName)
  properties.setProperty("schema.registry.url", "http://127.0.0.1:8081")

  val topic = "reviewsV1-avro"

  override def run(): Unit = {
    val productList:Array[Product] = ReviewFieldGenerator(productFilePath)
    val producer: Producer[String, ReviewsV1] = new KafkaProducer[String, ReviewsV1](properties)

    while(true) {
      val reviewModel: ReviewModel = ReviewFieldGenerator.generateReviewModel(productList)
      val reviews = ReviewsV1.newBuilder
        .setMarketplace(reviewModel.marketplace)
        .setCustomerId(reviewModel.customer_id)
        .setReviewId(reviewModel.review_id)
        .setProductId(reviewModel.product_id)
        .setProductTitle(reviewModel.product_title)
        .setCategory(reviewModel.category)
        .setSubCategory(reviewModel.sub_category)
        .setBrand(reviewModel.brand)
        .setSalePrice(reviewModel.sale_price)
        .setMarketPrice(reviewModel.market_price)
        .setProductType(reviewModel.product_type)
        .setStarRating(reviewModel.star_rating)
        .setHelpfulVotes(reviewModel.helpful_votes)
        .setTotalVotes(reviewModel.total_votes)
        .setVerifiedPurchase(reviewModel.verified_purchase)
        .setReviewDate(reviewModel.review_date)
        .build

      val producerRecord = new ProducerRecord[String, ReviewsV1](topic, reviews)
      println(reviews)
      producer.send(producerRecord)
    }
    producer.flush();
    producer.close();
  }

}

object ReviewsKafkaAvroProducerV1 {

  def main(args: Array[String]): Unit = {

    val productFilePath = "C:\\Users\\RAJES\\IdeaProjects\\Learning2023\\kafka-generator-with-spark\\datasets\\BigBasketProducts.json"
    val noOfThreads=2

    (1 to noOfThreads)
      .foreach { thread =>
        val producer = new ReviewsKafkaAvroProducerV1(productFilePath)
        producer.setName(productFilePath)
        producer.start()
      }
  }
}
