package com.spark.stream.example.avro.confluent_kafka_to_spark_to_kafka.confluent

import com.spark.stream.example.AppConfig.SCHEMA_REGISTRY_URL
import com.spark.stream.example.Utils.moduleRootDir
import com.spark.batch.example.Utils.getSchemaFromAvroFile
import com.typesafe.config.{Config, ConfigFactory}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.RestService
import org.apache.avro.Schema

import java.io.File
import java.nio.file.FileSystems

object SchemaRegistryHandler extends App {
  implicit lazy val conf: Config = ConfigFactory.parseFile(new File(s"$moduleRootDir/src/main/resources/config/common.conf"))

  def registerSchemaWithSchemaRegistry(topicName: String, avroSchemaFilePath: String): Unit = {
    import io.confluent.kafka.schemaregistry.client.rest.RestService

    val schemaRegistryUrl = conf.getString(SCHEMA_REGISTRY_URL)

    // Create a new RestService instance
    val restService = new RestService(schemaRegistryUrl)

    // Parse the Avro schema from the schema string
    val avroSchema = getSchemaFromAvroFile(avroSchemaFilePath)

    // Register the schema with the Schema Registry
    val schemaId = restService.registerSchema(avroSchema, topicName + "-value")

    // Print the ID of the registered schema
    println(s"Schema for topic: $topicName has been registered with Schema Registry \nschema Id: $schemaId \nschema-name: $topicName-value")
  }

}
