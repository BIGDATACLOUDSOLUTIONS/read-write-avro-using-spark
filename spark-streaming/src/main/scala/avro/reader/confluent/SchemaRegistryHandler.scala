package avro.reader.confluent

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.RestService
import org.apache.avro.Schema

import java.io.File
import java.nio.file.FileSystems

object SchemaRegistryHandler extends App {


  def registerSchemaWithSchemaRegistry() = {

    val schemaRegistry = "http://127.0.0.1:8081"
    val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistry, 10)

    val projectRootDir = FileSystems.getDefault.getPath("").toAbsolutePath.toString
    val avroSchemaFilePath = projectRootDir + "/spark-batch/" + "src/main/resources/schema/reviewsV1.avsc"

    val schemaAvro = new Schema.Parser().parse(new File(avroSchemaFilePath))
    schemaRegistryClient.register("WriteAvroDataToKafka-App2", schemaAvro)
  }

  def getSchemaFromAvroSchemaRegistry(topicName: String): String = {
    val restService = new RestService("http://127.0.0.1:8081")
    val valueRestResponseSchema = restService.getLatestVersion(topicName + "-value")
    val avroSchema = valueRestResponseSchema.getSchema
    avroSchema
  }

  registerSchemaWithSchemaRegistry()

}
