package com.spark.batch.example.ReadWriteKafka.withSchemaRegistry.Abris

import com.spark.batch.example.Utils.deleteNonEmptyDir
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import za.co.absa.abris.avro.functions.from_avro
import za.co.absa.abris.config.AbrisConfig


object ConfluentKafkaAvroReader extends App{

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("ConfluentKafkaAvroReader-Batch")
    .getOrCreate()

  val kafkaSourceDF = spark
    .read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "review-avro-raw")
    .option("startingOffsets", "earliest")
    .load()

  val abrisConfig = AbrisConfig
    .fromConfluentAvro
    .downloadReaderSchemaByLatestVersion
    .andTopicNameStrategy("review-avro-raw")
    .usingSchemaRegistry("http://localhost:8081")

  val deserialized = kafkaSourceDF.select(from_avro(col("value"), abrisConfig) as 'data).select("data.*")

  deserialized.show(false)


}
