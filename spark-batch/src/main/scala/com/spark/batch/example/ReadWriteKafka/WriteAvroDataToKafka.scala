package com.spark.batch.example.ReadWriteKafka

import com.spark.batch.example.AppConfig._
import com.spark.batch.example.Context.spark
import com.spark.batch.example.Context.spark.implicits._
import com.spark.batch.example.Utils
import com.spark.batch.example.Utils.{conf, moduleRootDir,_}
import org.apache.avro.Schema
import org.apache.spark.sql.avro.functions._
import org.apache.spark.sql.functions._


object AppOneKafkaWriter extends App {
  val avroDataFilePath = moduleRootDir + conf.getString(REVIEW_SOURCE_DATA_FILE_PATH)
  val avroSchemaFilePath = moduleRootDir + conf.getString(REVIEW_SOURCE_SCHEMA_FILE_PATH)

  //We can read schema from either file/schema registry, both will give the same correct results
  val schemaAvro = Utils.getSchemaFromAvroFile(avroSchemaFilePath)
  val sourceDF = Utils.createDFFromAvroFile(avroDataFilePath, schemaAvro)

  sourceDF.printSchema()
  sourceDF.show(false)

  val kafkaDF = sourceDF.select(expr("cast(customer_id as string) as key"), to_avro(struct("*")).alias("value"))

  Utils.writeToKafkaTopic(kafkaDF, "AppOneKafkaWriter")
  Utils.registerSchemaWithSchemaRegistry("AppOneKafkaWriter", avroSchemaFilePath)

}

// This fails to read data from kafka
object AppOneKafkaReaderOne extends App {

  val avroSchemaFilePath = moduleRootDir + conf.getString(REVIEW_SOURCE_SCHEMA_FILE_PATH)

  //We can read schema from either file/schema registry, both will give the same correct results
  val schemaAvro = Utils.getSchemaFromAvroFile(avroSchemaFilePath)

  val valueDF = Utils.readKafkaTopic("AppOneKafkaWriter")

  val output = valueDF
    .select(from_avro($"value", schemaAvro).as("data"))
    .select("data.*")

  output.printSchema()
  output.show(false)

}

// This fails to read data from kafka
object AppOneKafkaReaderTwo extends App {

  val avroSchema=getSchemaFromAvroSchemaRegistry("AppOneKafkaWriter","manually")
  println(avroSchema)

  val valueDF = Utils.readKafkaTopic("AppOneKafkaWriter").selectExpr( "CAST(key AS STRING)", "value AS data")
  val parsedDF = valueDF.select($"key", DspAvroDecoder.serdeUDF(col("data"), lit("marketplace")).alias("marketplace"))

  parsedDF.show(false)

}

object AppOneKafkaReaderThree extends App {
  import za.co.absa.abris.avro.functions.from_avro
  import za.co.absa.abris.config.AbrisConfig

  val abrisConfig = AbrisConfig
    .fromConfluentAvro
    .downloadReaderSchemaByLatestVersion
    .andTopicNameStrategy("AppOneKafkaWriter")
    .usingSchemaRegistry("http://localhost:8081")

  //println(abrisConfig)

  val kafkaSourceDF = Utils.readKafkaTopic("AppOneKafkaWriter")
  val deserialized = kafkaSourceDF.select(from_avro(col("value"), abrisConfig) as 'data).select("data.*")

  deserialized.show(false)

}

object AppOneKafkaReaderFour extends App {
  import spark.implicits._
  val avroSchemaFilePath = moduleRootDir + conf.getString(REVIEW_SOURCE_SCHEMA_FILE_PATH)

  //We can read schema from either file/schema registry, both will give the same correct results
  val schemaAvro = Utils.getSchemaFromAvroFile(avroSchemaFilePath)

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("startingOffsets", "earliest")
    .option("subscribe", "AppOneKafkaWriter")
    .load()

  // 1. Decode the Avro data into a struct;
  val output = df
    .select(from_avro(col("value"), schemaAvro).as("value"))

  val query = output
    .writeStream
    .format("console")
    .outputMode("append")
    .option("checkpointLocation", "chk-point-dir/kafka-batch/AppOneKafkaReaderFour")
    .start()


}



