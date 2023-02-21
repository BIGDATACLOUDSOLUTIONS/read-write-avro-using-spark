package com.spark.stream.example.avro.confluent_kafka_to_spark_to_kafka.abris

import com.spark.batch.example.Utils.deleteNonEmptyDir
import com.spark.stream.example.AppConfig._
import com.spark.stream.example.Utils.moduleRootDir
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import za.co.absa.abris.avro.functions.from_avro
import za.co.absa.abris.config.AbrisConfig

import java.io.File

object ConfluentKafkaAvroReader extends App{

  implicit lazy val conf: Config = ConfigFactory.parseFile(new File(s"$moduleRootDir/src/main/resources/config/confluentKafkaReaderWriter.conf"))
  val APP_CONFIG = CONFIGURATION("ConfluentKafkaAvroReader")


  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("ReadFromConfluentKafkaWithSchemaRegistry")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()

  val kafkaSourceDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", conf.getString(KAFKA_BROKER_LIST))
    .option("subscribe", conf.getString(REVIEW_AVRO_RAW_KAFKA_TOPIC))
    .option("startingOffsets", "earliest")
    .load()

  val abrisConfig = AbrisConfig
    .fromConfluentAvro
    .downloadReaderSchemaByLatestVersion
    .andTopicNameStrategy(conf.getString(REVIEW_AVRO_RAW_KAFKA_TOPIC))
    .usingSchemaRegistry(conf.getString(SCHEMA_REGISTRY_URL))

  val deserialized = kafkaSourceDF.select(from_avro(col("value"), abrisConfig) as 'data).select("data.*")

  val checkPointLocation=APP_CONFIG("CHECKPOINT_DIR")
  deleteNonEmptyDir(checkPointLocation)

  val query = deserialized
    .writeStream
    .format("console")
    .outputMode("append")
    .option("checkpointLocation",checkPointLocation )
    .start()

  query.awaitTermination()

}
