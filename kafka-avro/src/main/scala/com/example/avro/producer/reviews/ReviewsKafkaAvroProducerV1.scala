package com.example.avro.producer.reviews

import com.example.ReviewsV1
import com.example.avro.producer.AppConfig._
import com.example.avro.producer.AsynchronousProducerCallback
import com.example.avro.producer.Utils._

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import java.io.{File, IOException}
import java.util.Properties


class ReviewsKafkaAvroProducerV1(threadNum: Int,
                                 writeAvroToFile: Boolean = false) extends Thread {

  implicit val properties: Properties = new Properties
  // normal producer
  properties.setProperty("bootstrap.servers", conf.getString(REVIEW_KAFKA_BROKER_LIST))
  properties.setProperty("acks", "all")
  properties.setProperty("retries", "10")

  // avro part
  properties.setProperty("key.serializer", classOf[StringSerializer].getName)
  properties.setProperty("value.serializer", classOf[KafkaAvroSerializer].getName)
  properties.setProperty("schema.registry.url", conf.getString(REVIEW_SCHEMA_REGISTRY_URL))

  val topic: String = conf.getString(REVIEW_KAFKA_TOPIC)
  val numberOfMessage: Long = conf.getString(REVIEW_PRODUCER_NUMBER_OF_MESSAGE_TO_PUBLISH).toLong
  val productFilePath: String = moduleRootDir + conf.getString(REVIEW_PRODUCER_PRODUCT_INPUT_FILE_PATH)
  val avroFilePath: String = conf.getString(REVIEW_PRODUCER_RESULT_AVRO_OUTPUT_PATH) + s"review-specific_${threadNum}.avro"
  createDirIfNotExists(conf.getString(REVIEW_PRODUCER_RESULT_AVRO_OUTPUT_PATH))
  println(s"avroFilePath: $avroFilePath")
  deleteFileIfExists(avroFilePath)

  override def run(): Unit = {
    val reviewFieldGenerator = new ReviewFieldGenerator()
    val productData: Array[Product] = reviewFieldGenerator.readProductFile(productFilePath)

    val producer: Producer[String, ReviewsV1] = new KafkaProducer[String, ReviewsV1](properties)

    val writer: DataFileWriter[ReviewsV1] = if (writeAvroToFile) {
      val datumWriter = new SpecificDatumWriter[ReviewsV1](classOf[ReviewsV1])
      val dataFileWriter = new DataFileWriter[ReviewsV1](datumWriter)
      dataFileWriter.create(new ReviewsV1().getSchema, new File(avroFilePath))
    } else null

    var startIndex = 1
    while (startIndex <= numberOfMessage) {
      val reviewModel: ReviewModel = reviewFieldGenerator.generateReviewModel(productData)
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
        producer.send(producerRecord, new AsynchronousProducerCallback)
        println(reviews)
      }
      startIndex += 1
    }
    producer.flush()
    producer.close()
    if (writeAvroToFile) {
      writer.close()
      ReviewsKafkaAvroProducerV1.readAvroFile(avroFilePath)
    }
  }
}

object ReviewsKafkaAvroProducerV1 {

  def readAvroFile(avroFilePath: String): Unit = {
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

  def main(args: Array[String]): Unit = {

    val writeAvroToFile: Boolean = false

    val noOfThreads = conf.getString(REVIEW_PRODUCER_NO_OF_THREADS).toInt

    (1 to noOfThreads)
      .foreach { thread =>
        val producer = new ReviewsKafkaAvroProducerV1(thread, writeAvroToFile)
        producer.setName(s"producer-${thread}")
        producer.start()
      }
  }


}
