package com.spark.batch.example.ReadWriteKafka.withSchemaRegistry.Abris

import com.spark.batch.example.AppConfig._
import com.spark.batch.example.Utils._
import org.apache.spark.sql.functions.{col, struct, sum}
import org.apache.spark.sql.SparkSession
import za.co.absa.abris.avro.functions.to_avro
import za.co.absa.abris.config.AbrisConfig


object ConfluentKafkaAvroWriter extends App{
  val spark = SparkSession
    .builder()
    .appName("ConfluentKafkaAvroWriter-Batch")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("INFO")

  val sourceAvroDataFilePath = conf.getString(REVIEW_SOURCE_DATA_FILE_PATH)
  val sourceAvroSchemaFilePath = moduleRootDir + conf.getString(REVIEW_SOURCE_SCHEMA_FILE_PATH)
  val sourceAvroSchema = getSchemaFromAvroFile(sourceAvroSchemaFilePath)
  val valueDF = createDFFromAvroFile(sourceAvroDataFilePath, sourceAvroSchema)
  val finalSourceDF = valueDF.groupBy("marketplace").agg(sum(col("market_price") - col("sale_price")).as("profit"))
  finalSourceDF.show(false)

  val allColumns = struct(finalSourceDF.columns.map(col).toIndexedSeq: _*)

  val kafkaTopicName = "ConfluentKafkaAvroWriter-Batch"

  val targetAvroSchemaFilePath = moduleRootDir + conf.getString(REVIEW_TARGET_SCHEMA_FILE_PATH)
  val targetAvroSchema = getSchemaFromAvroFile(targetAvroSchemaFilePath)

  val abrisConfig = AbrisConfig
    .toConfluentAvro
    .provideAndRegisterSchema(targetAvroSchema)
    .usingTopicNameStrategy(kafkaTopicName)
    .usingSchemaRegistry(conf.getString(REVIEW_SCHEMA_REGISTRY_URL))

  val avroFrame = finalSourceDF.select(to_avro(allColumns, abrisConfig) as "value")

  avroFrame
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", conf.getString(REVIEW_KAFKA_BROKER_LIST))
    .option("topic", kafkaTopicName)
    .save()



}
