package com.spark.stream.example.avro
package confluent_kafka_to_spark_to_kafka.abris

import com.spark.stream.example.AppConfig._
import com.spark.stream.example.Utils.moduleRootDir
import com.spark.batch.example.Utils.{deleteNonEmptyDir, getSchemaFromAvroFile}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, struct, sum}
import org.apache.spark.sql.SparkSession
import za.co.absa.abris.avro.functions.to_avro
import za.co.absa.abris.config.AbrisConfig

import java.io.File

object ConfluentKafkaAvroWriter extends App {

  implicit lazy val conf: Config = ConfigFactory.parseFile(new File(s"$moduleRootDir/src/main/resources/config/confluentKafkaReaderWriter.conf"))
  val APP_CONFIG = CONFIGURATION("ConfluentKafkaAvroWriter")

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("ConfluentKafkaAvroWriter")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

  val schemaFilePath = moduleRootDir + conf.getString(REVIEW_AVRO_RAW_FILE_SCHEMA_LOCATION)
  val schemaAvro = getSchemaFromAvroFile(schemaFilePath)

  val rawDF = spark
    .readStream
    .format("avro")
    .option("avroSchema", schemaAvro)
    .option("path", conf.getString(REVIEW_AVRO_RAW_FILE_LOCATION))
    .option("maxFilesPerTrigger", 1)
    .load()

 val finalSourceDF = rawDF.groupBy("marketplace").agg(sum(col("market_price") - col("sale_price")).as("profit"))
  finalSourceDF.printSchema

  val checkPointLocation = conf.getString(APP_CONFIG("CHECKPOINT_DIR"))
  deleteNonEmptyDir(checkPointLocation) //Just for testing

/*
  val query = finalSourceDF
    .writeStream
    .format("console")
    .outputMode("update")
    .option("checkpointLocation", checkPointLocation)
    .start()

*/

    val allColumns = struct(finalSourceDF.columns.map(col).toIndexedSeq: _*)

    val kafkaTopicName = conf.getString(APP_CONFIG("TARGET_KAFKA_TOPIC"))

    val targetAvroSchemaFilePath = moduleRootDir + conf.getString(APP_CONFIG("REVIEW_TARGET_SCHEMA_FILE_PATH"))
    val targetAvroSchema = getSchemaFromAvroFile(targetAvroSchemaFilePath)

    val abrisConfig = AbrisConfig
      .toConfluentAvro
      .provideAndRegisterSchema(targetAvroSchema)
      .usingTopicNameStrategy(kafkaTopicName)
      .usingSchemaRegistry(conf.getString(SCHEMA_REGISTRY_URL))

    val avroFrame = finalSourceDF.select(to_avro(allColumns, abrisConfig) as "value")

    val query = avroFrame
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.getString(KAFKA_BROKER_LIST))
      .option("topic", kafkaTopicName)
      .outputMode("update")
      .option("checkpointLocation", checkPointLocation)
      .start()

    query.awaitTermination()

}