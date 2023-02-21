package com.spark.batch.example.ReadWriteKafka.withSchemaRegistry.confluent

import com.spark.batch.example.AppConfig.REVIEW_TARGET_SCHEMA_SPARK_KAFKA_COMPATIBLE
import com.spark.batch.example.Utils
import com.spark.batch.example.Utils.{conf, moduleRootDir}
import org.apache.spark.sql.avro.functions.from_avro
import com.spark.batch.example.Context.spark.implicits._

/**
 *
 */


object ReadFromKafkaWrittenBySpark extends App {

  val avroSchemaFilePath = moduleRootDir + conf.getString(REVIEW_TARGET_SCHEMA_SPARK_KAFKA_COMPATIBLE)

  //We can read schema from either file/schema registry, both will give the same correct results
  val schemaAvro = Utils.getSchemaFromAvroFile(avroSchemaFilePath)

  val topicName="AppOneKafkaWriter-withSchemaRegistry"
  val valueDF = Utils.readKafkaTopic(topicName)

  val output = valueDF
    .select(from_avro($"value", schemaAvro).as("data"))
    .select("data.*")

  output.printSchema()
  output.show(false)

}
