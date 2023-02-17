import AppConfig.REVIEW_SCHEMA_REGISTRY_URL
import Context.spark
import io.confluent.kafka.schemaregistry.client.rest.RestService
import org.apache.avro.Schema
import org.apache.spark.sql.DataFrame
import com.typesafe.config.{Config, ConfigFactory}

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

  def createDirIfNotExists(outputDir: String): Unit = {
    val path = Paths.get(outputDir)
    Files.createDirectories(path)
  }

  def recreateOutputDir(outputDir: String): Unit = {
    val path = Paths.get(outputDir)
    if (Files.exists(path)) deleteNonEmptyDir(outputDir)
    Files.createDirectories(path)
  }


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

  def writeToKafkaTopic(df: DataFrame, topicName: String): Unit = {
    df
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", topicName)
      .save()
  }

  def getSchemaFromAvroFile(avroSchemaFileName: String): String = {
    val schemaAvro = new Schema.Parser().parse(new File(avroSchemaFileName))
    schemaAvro.toString()
  }

  def getSchemaFromAvroSchemaRegistry(topicName: String, extraString: String = "-value")(implicit conf: Config): String = {
    val restService = new RestService(conf.getString(REVIEW_SCHEMA_REGISTRY_URL))
    val valueRestResponseSchema = restService.getLatestVersion(topicName + extraString)
    val avroSchema = valueRestResponseSchema.getSchema
    avroSchema
  }

}
