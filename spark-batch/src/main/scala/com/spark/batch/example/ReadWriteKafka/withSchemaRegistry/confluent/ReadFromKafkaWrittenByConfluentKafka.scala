package com.spark.batch.example.ReadWriteKafka
package withSchemaRegistry.confluent

import com.spark.batch.example.AppConfig._
import com.spark.batch.example.{AvroBinaryDecoder, Utils}
import com.spark.batch.example.Utils.conf
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.spark.sql.functions.{col, lit, udf}
import za.co.absa.abris.avro.functions.from_avro
import za.co.absa.abris.config.AbrisConfig

import java.nio.ByteBuffer


/** App2:
 * About the source of this application:
 * Using Standard avro schema file(reviewsV1.avsc) and Confluent Kakfa producer, Data was written to Kafka Topic in avro format(Data type: avro).
 * and schema was registered to Confluent Schema Registry.
 *
 * This application
 * 1. pulls the schema from Schema Registry
 * 2. reads data from kafka
 * 3. Applies the schema to extract the columns from DataFrame
 */

object ReadFromConfluentKafka extends App {

  // Either we read schema from file or schema registry, we can't read the data correctly without custom deserializer
  val schemaAvro = Utils.getSchemaFromAvroSchemaRegistry(conf.getString(REVIEW_KAFKA_SOURCE_TOPIC))

  val kafkaSourceDF = Utils.readKafkaTopic(conf.getString(REVIEW_KAFKA_SOURCE_TOPIC))

  val deserialized = kafkaSourceDF.select(from_avro(col("value"), schemaAvro) as 'data).select("data.*")

  deserialized.show(false)

  /**
   * This may/may not fail as the data on kafka is avro format not binary format.
   * The data on kafka was serialized using io.confluent.kafka.serializers.KafkaAvroSerializer which not a generic serializer,
   * So spark by default is not able to deserialize by itself
   *
   * Error: Caused by: java.lang.ArrayIndexOutOfBoundsException: -51
   * Even we get the data, it will not be in string format
   *
   * To deserialize, either we have to write our own deserializer or we can use abris framework
   */

}

object ReadFromKafkaWrittenByConfluentKafka extends Serializable with App {

  val topicName = "review-avro-raw"
  val kafkaSourceDF = Utils.readKafkaTopic(topicName)

  val avroBinaryDecoder = new AvroBinaryDecoder(topicName,conf.getString(REVIEW_SCHEMA_REGISTRY_URL))

  val deserialized = kafkaSourceDF
    .select("value")
    .withColumn("marketplace", avroBinaryDecoder.decodeAvro(col("value"), lit("marketplace")))
    .withColumn("customer_id", avroBinaryDecoder.decodeAvro(col("value"), lit("customer_id")))
    .withColumn("review_id", avroBinaryDecoder.decodeAvro(col("value"), lit("review_id")))
    .withColumn("product_id", avroBinaryDecoder.decodeAvro(col("value"), lit("product_id")))
    .withColumn("product_title", avroBinaryDecoder.decodeAvro(col("value"), lit("product_title")))
    .withColumn("category", avroBinaryDecoder.decodeAvro(col("value"), lit("category")))
    .drop("value")

  deserialized.show(1500, false)

  /**
   * The data on kafka was serialized using io.confluent.kafka.serializers.KafkaAvroSerializer which not a generic serializer
   * +-----------+-----------+---------+----------+----------------------------------------------------+----------------------+
   * |marketplace|customer_id|review_id|product_id|product_title                                       |category              |
   * +-----------+-----------+---------+----------+----------------------------------------------------+----------------------+
   * |US         |49088      |5380959  |18866     |Papad - Triangle                                    |Snacks & Branded Foods|
   * |UK         |25311      |9055844  |11398     |Premium Baby Wet Wipes With Aloe Vera - Paraben Free|Baby Care             |
   * +-----------+-----------+---------+----------+----------------------------------------------------+----------------------
   */

}