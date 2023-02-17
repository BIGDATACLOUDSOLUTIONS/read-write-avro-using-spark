import AppConfig._
import Utils._

/**
 * About the source of AvroFileReadApp1 application:
 * Using Standard avro schema file(reviewsV1.avsc) and Confluent Kafka producer, Data was written to files in avro format.
 * The same schema file and source avro data files are used here in this application.
 *
 * This application reads data from Avro file and applies the avro schema and creates dataframe.
 */

object AvroFileReadApp1 extends App {

  val avroDataFilePath = moduleRootDir + conf.getString(REVIEW_SOURCE_DATA_FILE_PATH)
  val avroSchemaFilePath = moduleRootDir + conf.getString(REVIEW_SOURCE_SCHEMA_FILE_PATH)

  val schemaAvro = Utils.getSchemaFromAvroFile(avroSchemaFilePath)

  val valueDF = Utils.createDFFromAvroFile(avroDataFilePath, schemaAvro)
  valueDF.printSchema()
  valueDF.show(false)
}

/**
 * About the source of AvroFileReadApp2 application:
 * Using Standard avro schema file(reviewsV1.avsc) and Confluent Kafka producer, Data was written to files in avro format.
 * In case the schema of the above data has been registered with Schema Registry as well and we want to use the same.
 *
 * This application reads data from Avro file and applies the avro schema and creates dataframe.
 */
object AvroFileReadApp2 extends App {

  val avroDataFilePath = moduleRootDir + conf.getString(REVIEW_SOURCE_DATA_FILE_PATH)

  val schemaAvro = Utils.getSchemaFromAvroSchemaRegistry(conf.getString(REVIEW_KAFKA_SOURCE_TOPIC))

  val valueDF = Utils.createDFFromAvroFile(avroDataFilePath, schemaAvro)
  valueDF.printSchema()
  valueDF.show(false)
}

