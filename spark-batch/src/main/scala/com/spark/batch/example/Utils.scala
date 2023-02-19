package com.spark.batch.example

import com.spark.batch.example.AppConfig.{REVIEW_KAFKA_BROKER_LIST, REVIEW_SCHEMA_REGISTRY_URL}
import com.spark.batch.example.Context.spark
import com.typesafe.config.{Config, ConfigFactory}
import io.confluent.kafka.schemaregistry.client.rest.RestService
import org.apache.avro.Schema
import org.apache.spark.sql.DataFrame

import java.io.File
import java.nio.file.{FileSystems, Files, Paths}
import scala.reflect.io.Directory

object Utils {
  val moduleRootDir: String = FileSystems.getDefault.getPath("").toAbsolutePath.toString + "/spark-batch/"
  implicit lazy val conf: Config = ConfigFactory.parseFile(new File(s"$moduleRootDir/src/main/resources/config/review_ingest.conf"))

  def deleteNonEmptyDir(directory: String): Unit = {
    val dir = new Directory(new File(directory))
    dir.deleteRecursively()
  }

  def deleteFileIfExists(filePath: String): Unit = {
    Files.deleteIfExists(Paths.get(filePath))
  }

  def createDFFromAvroFile(avroDataFilePath: String, schemaAvro: String): DataFrame = {
    spark
      .read
      .format("com.spark.stream.example.avro")
      .option("avroSchema", schemaAvro)
      .load(avroDataFilePath)
  }


  def readKafkaTopic(topicName: String): DataFrame = {
    spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.getString(REVIEW_KAFKA_BROKER_LIST))
      .option("subscribe", topicName)
      .option("startingOffsets", "earliest")
      .load()
  }

  def writeToKafkaTopic(df: DataFrame, topicName: String): Unit = {
    df
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", topicName)
      .save()
  }

  def getSchemaFromAvroFile(avroSchemaFilePath: String): String = {
    val schemaAvro = new Schema.Parser().parse(new File(avroSchemaFilePath))
    schemaAvro.toString()
  }

  /**
   * Read the schema registered on schema registry
   *
   * @param topicName
   * @param conf
   * @return
   */
  def getSchemaFromAvroSchemaRegistry(topicName: String,
                                      schemaRegisteredBy: String = "kafka")(implicit conf: Config): String = {
    val restService = new RestService(conf.getString(REVIEW_SCHEMA_REGISTRY_URL))

    val schemaName =
    //if the schema is registered by kafka producer, -value gets appended by producer in the schema name
      if (schemaRegisteredBy.equalsIgnoreCase("kafka")) topicName + "-value"
      else topicName
    val valueRestResponseSchema = restService.getLatestVersion(schemaName)
    val avroSchema = valueRestResponseSchema.getSchema
    avroSchema
  }

  def registerSchemaWithSchemaRegistry(topicName: String, avroSchemaFilePath: String): Unit = {
    import io.confluent.kafka.schemaregistry.client.rest.RestService

    val schemaRegistryUrl = conf.getString(REVIEW_SCHEMA_REGISTRY_URL)

    // Create a new RestService instance
    val restService = new RestService(schemaRegistryUrl)

    // Parse the Avro schema from the schema string
    val avroSchema = Utils.getSchemaFromAvroFile(avroSchemaFilePath)

    // Register the schema with the Schema Registry
    val schemaId = restService.registerSchema(avroSchema, topicName)

    // Print the ID of the registered schema
    println(s"Schema for topic: $topicName has been registered with Schema Registry \nschema Id: $schemaId \nschema-name: $topicName")
  }

}
