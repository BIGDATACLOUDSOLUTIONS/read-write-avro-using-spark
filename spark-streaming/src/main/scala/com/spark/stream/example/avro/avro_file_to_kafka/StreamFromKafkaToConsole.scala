package com.spark.stream.example.avro
package avro_file_to_kafka

import com.spark.stream.example.Utils._
import com.spark.stream.example.AppConfig._
import com.spark.batch.example.Utils.{deleteNonEmptyDir, getSchemaFromAvroFile}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions.col

import java.io.File

/**
 * This application read the kafka topic data written by application: StreamFromFileToKafka
 * NOTE: Here, while reading avro data from kafka in spark, the schema of asvc file is different from the actual file used in confluent kafka producer.
 * So we have to create a new schema file review_avro_spark_kafka_schema.avsc by modifying original file: reviewsV1.avsc
 * We have to remove default value and change the type of all the columns.
 * eg: if the type of a column in confluent kafka schema is ["null", "string"] --> This has to change like ["string", "null"]
 * So all the types should have format like ["actual data type defined in original file","null"]
 *
 * Also, here schema registry is not used. We are using avsc file for schema
 *
 */

object StreamFromKafkaToConsole extends App {

  implicit lazy val conf: Config = ConfigFactory.parseFile(new File(s"$moduleRootDir/src/main/resources/config/stream_from_kafka_to_console.conf"))
  val APP_CONFIG = CONFIGURATION("StreamFromKafkaToConsole")

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Kafka Streaming Demo")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

  val kafkaSourceDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", conf.getString(KAFKA_BROKER_LIST))
    .option("subscribe", conf.getString(APP_CONFIG("SOURCE_TOPIC")))
    .option("startingOffsets", "earliest")
    .load()

  val schemaFilePath = moduleRootDir + conf.getString(APP_CONFIG("SOURCE_SCHEMA_FILE_PATH"))
  val avroSchema = getSchemaFromAvroFile(schemaFilePath)

  val valueDF = kafkaSourceDF
    .select(from_avro(col("value"), avroSchema).alias("value"))
    .select("value.*")

  valueDF.printSchema()

  val checkPointLocation = conf.getString(APP_CONFIG("CHECKPOINT_DIR"))
  deleteNonEmptyDir(checkPointLocation) //Just for testing

  val query = valueDF
    .writeStream
    .format("console")
    .outputMode("append")
    .option("checkpointLocation", checkPointLocation)
    .start()

  query.awaitTermination()

}
