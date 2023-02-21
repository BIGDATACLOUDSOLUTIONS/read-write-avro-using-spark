package com.spark.stream.example.avro.confluent_kafka_to_spark_to_kafka.confluent

import com.spark.batch.example.Utils.deleteNonEmptyDir
import com.spark.batch.example.AvroBinaryDecoder
import com.spark.stream.example.AppConfig._
import com.spark.stream.example.Utils.moduleRootDir
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

import java.io.File


object ConfluentKafkaAvroReader extends Serializable {
  implicit lazy val conf: Config = ConfigFactory.parseFile(new File(s"$moduleRootDir/src/main/resources/config/common.conf"))

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Kafka Avro Sink Demo")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()

    val topicName = conf.getString(REVIEW_AVRO_RAW_KAFKA_TOPIC)

    val kafkaSourceDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.getString(KAFKA_BROKER_LIST))
      .option("subscribe", topicName)
      .option("startingOffsets", "earliest")
      .load()

    val avroBinaryDecoder = new AvroBinaryDecoder(topicName, conf.getString(SCHEMA_REGISTRY_URL))

    val deserialized = kafkaSourceDF
      .select("value")
      .withColumn("marketplace", avroBinaryDecoder.decodeAvro(col("value"), lit("marketplace")))
      .withColumn("customer_id", avroBinaryDecoder.decodeAvro(col("value"), lit("customer_id")))
      .withColumn("review_id", avroBinaryDecoder.decodeAvro(col("value"), lit("review_id")))
      .withColumn("product_id", avroBinaryDecoder.decodeAvro(col("value"), lit("product_id")))
      .withColumn("product_title", avroBinaryDecoder.decodeAvro(col("value"), lit("product_title")))
      .withColumn("category", avroBinaryDecoder.decodeAvro(col("value"), lit("category")))
      .drop("value")

    val checkPointLocation = "chk-point-dir/spark-streaming/SparkStructuredStream"
    deleteNonEmptyDir(checkPointLocation)

    val query = deserialized
      .writeStream
      .format("console")
      .outputMode("append")
      .option("checkpointLocation", checkPointLocation)
      .start()

    query.awaitTermination()
  }
}
