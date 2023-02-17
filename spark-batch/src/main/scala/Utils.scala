import Context.spark
import io.confluent.kafka.schemaregistry.client.rest.RestService
import org.apache.avro.Schema
import org.apache.spark.sql.DataFrame

import java.io.File
import java.nio.file.FileSystems

object Utils {

  def createDFFromAvroFile(avroDataFilePath: String, schemaAvro: String): DataFrame = {
    spark
      .read
      .format("avro")
      .option("avroSchema", schemaAvro)
      .load(avroDataFilePath)
  }


  def readKafkaTopic(topicName: String): DataFrame = {
    spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topicName)
      .option("startingOffsets", "earliest")
      .load()
  }

  def writeToKafkaTopic(df:DataFrame,topicName: String): Unit = {
    df
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", topicName)
      .save()
  }

  def getSchemaFromAvroFile(avroSchemaFileName: String): String = {
    val projectRootDir = FileSystems.getDefault.getPath("").toAbsolutePath.toString
    val avroSchemaFilePath = projectRootDir + "/spark-batch/" + s"src/main/resources/schema/$avroSchemaFileName"
    val schemaAvro = new Schema.Parser().parse(new File(avroSchemaFilePath))
    schemaAvro.toString()
  }

  def getSchemaFromAvroSchemaRegistry(topicName: String): String = {
    val restService = new RestService("http://127.0.0.1:8081")
    val valueRestResponseSchema = restService.getLatestVersion(topicName + "-value")
    val avroSchema = valueRestResponseSchema.getSchema
    avroSchema
  }

}
