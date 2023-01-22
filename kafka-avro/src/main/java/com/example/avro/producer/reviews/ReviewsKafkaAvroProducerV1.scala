package com.example.avro.producer.reviews

import com.example.ReviewsV1
import com.example.avro.producer.AsynchronousProducerCallback
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.io.{File, IOException}
import java.nio.file.FileSystems
import java.util.Properties


class ReviewsKafkaAvroProducerV1(threadNum: Int,
                                 numberOfMessage: Long = 100,
                                 writeAvroToFile: Boolean = false) extends Thread {

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

  val projectRootDir: String = FileSystems.getDefault.getPath("").toAbsolutePath.toString
  val productFilePath: String = projectRootDir + "/datasets/BigBasketProducts.json"
  val avroFilePath: String = projectRootDir + "/kafka-avro/" + s"src/main/resources/data/output/review-specific_${threadNum}.avro"

  def readAvroFile(): Unit = {
    val datumReader = new SpecificDatumReader[ReviewsV1](classOf[ReviewsV1])
    try {
      println("Reading our specific record")
      val dataFileReader = new DataFileReader[ReviewsV1](new File(avroFilePath), datumReader)
      while (dataFileReader.hasNext) {
        val readReviews: ReviewsV1 = dataFileReader.next
        println(readReviews.toString)
      }
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }

  override def run(): Unit = {
    val productList: Array[Product] = ReviewFieldGenerator(productFilePath)
    val producer: Producer[String, ReviewsV1] = new KafkaProducer[String, ReviewsV1](properties)

    val writer: DataFileWriter[ReviewsV1] = if (writeAvroToFile) {
      val datumWriter = new SpecificDatumWriter[ReviewsV1](classOf[ReviewsV1])
      val dataFileWriter = new DataFileWriter[ReviewsV1](datumWriter)
      dataFileWriter.create(new ReviewsV1().getSchema, new File(avroFilePath))
    } else null

    var startIndex = 1
    while (startIndex <= numberOfMessage) {
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
      if (writeAvroToFile) writer.append(reviews)
      else {
        println(reviews)
        producer.send(producerRecord, new AsynchronousProducerCallback)
      }
      startIndex += 1
    }
    producer.flush()
    producer.close()
    writer.close()

    if (writeAvroToFile) readAvroFile()
  }
}

object ReviewsKafkaAvroProducerV1 {

  def main(args: Array[String]): Unit = {

    val noOfThreads = 1
    val numberOfMessage = 1000
    val writeAvroToFile: Boolean = true

    (1 to noOfThreads)
      .foreach { thread =>
        val producer = new ReviewsKafkaAvroProducerV1(thread, numberOfMessage, writeAvroToFile)
        producer.setName(s"producer-${thread}")
        producer.start()
      }
  }


}
