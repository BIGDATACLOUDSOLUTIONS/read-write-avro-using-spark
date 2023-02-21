package com.spark.batch.example

import Utils._
import AppConfig.REVIEW_SCHEMA_REGISTRY_URL
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.nio.ByteBuffer

class AvroBinaryDecoder(topicName: String,
                        schemaRegistryUrl:String)  extends Serializable  {

    def deserializeMessage(msg: Array[Byte], attribute: String): String = {
      val bb = ByteBuffer.wrap(msg)
      bb.get()

      val schemaId = bb.getInt //Don't remove this line as this is increasing the pointer
      val schemaRegistryClient: SchemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 10)
      val schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(topicName + "-value")

      val schema = new Schema.Parser().parse(schemaMetadata.getSchema)
      val decoder: BinaryDecoder = DecoderFactory.get().binaryDecoder(msg, bb.position(), bb.remaining(), null)
      val reader = new GenericDatumReader[GenericRecord](schema)
      val record = reader.read(null, decoder)
      try {
        record.get(attribute).toString
      } catch {
        case e: Exception => null
      }

    }
     val decodeAvro:UserDefinedFunction= udf(deserializeMessage _)
}
