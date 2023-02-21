package com.spark.stream.example.avro
package avro_file_to_kafka

import com.spark.stream.example.Utils._
import com.spark.stream.example.AppConfig._
import com.spark.batch.example.Utils.{deleteNonEmptyDir, getSchemaFromAvroFile}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.to_avro
import org.apache.spark.sql.functions.{col, expr, struct}

import java.io.File

/**
 * This application reads the avro file and schema generated bu confluent kafka avro producer and push it to kafka
 * Here schema registry is not used. We are using avsc file for schema
 *
 */
object StreamFromFileToKafka extends App {

  implicit lazy val conf: Config = ConfigFactory.parseFile(new File(s"$moduleRootDir/src/main/resources/config/stream_from_file_to_kafka.conf"))
  val APP_CONFIG = CONFIGURATION("StreamFromFileToKafkaProp")

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("File Streaming Demo")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

  val schemaFilePath = moduleRootDir + conf.getString(REVIEW_AVRO_RAW_FILE_SCHEMA_LOCATION)
  val schemaAvro = getSchemaFromAvroFile(schemaFilePath)
  println(schemaAvro)

  val rawDF = spark.readStream
    .format("avro")
    .option("avroSchema", schemaAvro)
    .option("path", conf.getString(REVIEW_AVRO_RAW_FILE_LOCATION))
    .option("maxFilesPerTrigger", 1)
    .load()

  val checkPointLocation = conf.getString(APP_CONFIG("CHECKPOINT_DIR"))
  deleteNonEmptyDir(checkPointLocation) //Just for testing

/*  val query = rawDF.select((struct("*")).alias("value"))
    .writeStream
    .format("console")
    .outputMode("append")
    .option("checkpointLocation", checkPointLocation)
    .start()*/

    val kafkaTargetDF = rawDF
      .select(col("customer_id").cast("string").as("key"),
        to_avro(struct("*")).alias("value"))

    val query = kafkaTargetDF
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.getString(KAFKA_BROKER_LIST))
      .option("topic", conf.getString(APP_CONFIG("TARGET_TOPIC")))
      .outputMode("append")
      .option("checkpointLocation", checkPointLocation)
      .start()

  query.awaitTermination()

}
