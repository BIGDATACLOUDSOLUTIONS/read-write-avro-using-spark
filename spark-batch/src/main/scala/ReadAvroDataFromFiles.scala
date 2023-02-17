import java.nio.file.FileSystems

/**
 * About the source of AvroFileReadApp1 application:
 * Using Standard avro schema file(reviewsV1.avsc) and Confluent Kafka producer, Data was written to files in avro format.
 * The same schema file and source avro data files are used here in this application.
 *
 * This application reads data from Avro file and applies the avro schema and creates dataframe.
 */

object AvroFileReadApp1 extends App {

  val projectRootDir = FileSystems.getDefault.getPath("").toAbsolutePath.toString
  val avroDataFilePath = projectRootDir + "/spark-batch/" + "src/main/resources/data/review_avro/*"

  val schemaAvro = Utils.getSchemaFromAvroFile("reviewsV1.avsc")

  val valueDF = Utils.createDFFromAvroFile(avroDataFilePath, schemaAvro)
  valueDF.printSchema()
  valueDF.show(false)
}

/**
 * About the source of AvroFileReadApp2 application:
 * Using Standard avro schema file(reviewsV1.avsc) and Confluent Kafka producer, Data was written to files in avro format.
 * In case the schema of the above data has been registered with Schema Registry and we want to use the same.
 *
 * This application reads data from Avro file and applies the avro schema and creates dataframe.
 */
object AvroFileReadApp2 extends App {

  val projectRootDir = FileSystems.getDefault.getPath("").toAbsolutePath.toString
  val avroDataFilePath = projectRootDir + "/spark-batch/" + "src/main/resources/data/review_avro/*"

  val schemaAvro = Utils.getSchemaFromAvroSchemaRegistry("reviewsV1-avro")

  val valueDF = Utils.createDFFromAvroFile(avroDataFilePath, schemaAvro)
  valueDF.printSchema()
  valueDF.show(false)
}

