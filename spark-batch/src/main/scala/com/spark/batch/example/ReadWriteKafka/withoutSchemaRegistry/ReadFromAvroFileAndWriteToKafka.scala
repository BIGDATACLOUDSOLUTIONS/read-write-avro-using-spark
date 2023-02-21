package com.spark.batch.example.ReadWriteKafka.withoutSchemaRegistry

import com.spark.batch.example.AppConfig.{REVIEW_SOURCE_DATA_FILE_PATH, REVIEW_SOURCE_SCHEMA_FILE_PATH}
import com.spark.batch.example.Utils
import com.spark.batch.example.Utils.{conf, moduleRootDir}
import org.apache.spark.sql.avro.functions.to_avro
import org.apache.spark.sql.functions.{expr, struct}

/**
 *
 *
 */

object ReadFromAvroFileAndWriteToKafka extends App {
  val avroDataFilePath = conf.getString(REVIEW_SOURCE_DATA_FILE_PATH)
  val avroSchemaFilePath = moduleRootDir + conf.getString(REVIEW_SOURCE_SCHEMA_FILE_PATH)

  //We can read schema from either file/schema registry, both will give the same correct results
  val schemaAvro = Utils.getSchemaFromAvroFile(avroSchemaFilePath)
  val sourceDF = Utils.createDFFromAvroFile(avroDataFilePath, schemaAvro)

  sourceDF.printSchema()
  sourceDF.show(false)

  val kafkaDF = sourceDF.select(expr("cast(customer_id as string) as key"),
    to_avro(struct("*")).alias("value"))

  Utils.writeToKafkaTopic(kafkaDF, "AppOneKafkaWriter-withoutSchemaRegistry")

}