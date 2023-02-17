import org.apache.spark.sql.avro.functions._
import org.apache.spark.sql.functions._

import java.nio.file.FileSystems
import Context.spark
import Context.spark.implicits._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.StringType

object App1Writer {
  val projectRootDir = FileSystems.getDefault.getPath("").toAbsolutePath.toString
  val avroDataFilePath = projectRootDir + "/spark-batch/" + "src/main/resources/data/review_avro/*"

  //We can read schema from either file/schema registry, both will give the same correct results
  val schemaAvro = Utils.getSchemaFromAvroFile("reviewsV1.avsc")

  val sourceDF = Utils.createDFFromAvroFile(avroDataFilePath, schemaAvro)

  sourceDF.printSchema()
  sourceDF.show(false)

  val kafkaDF = sourceDF.select(expr("cast(customer_id as string) as key"), to_avro(struct("*")).alias("value"))

  Utils.writeToKafkaTopic(kafkaDF, "WriteAvroDataToKafka-App2")

}

object App1Reader {
  val projectRootDir = FileSystems.getDefault.getPath("").toAbsolutePath.toString
  val avroDataFilePath = projectRootDir + "/spark-batch/" + "src/main/resources/data/review_avro/*"

  //We can read schema from either file/schema registry, both will give the same correct results
  val schemaAvro = Utils.getSchemaFromAvroFile("reviewsV1.avsc")

  val valueDF = Utils.readKafkaTopic("WriteAvroDataToKafka-App2_1")

  val output = valueDF
    .select(from_avro($"value", schemaAvro).as("data"))
    .select("data.*")

  output.printSchema()
  output.show(false)

}

/**
 * Read from Avro File with avro schema -> Parse the data in dataframe
 * Write to Kafka
 * Sample Data:
 *
 *
 *
 */

object App2Writer extends App {

  val kafkaSourceDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "invoices-1")
    .option("startingOffsets", "earliest")
    .load()

  import org.apache.spark.sql.types.{StringType, StructType}

  val schema = new StructType()
    .add("InvoiceNumber", StringType, true)
    .add("CreatedTime", StringType, true)
    .add("StoreID", StringType, true)
    .add("PosID", StringType, true)
    .add("CustomerType", StringType, true)


  //val kafkaSourceDF = Utils.readKafkaTopic("invoices-1")

  val kafkaDF = kafkaSourceDF
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
    .withColumn("value", from_json(col("value"), schema))
    .select("value.*")

  //kafkaDF.show(false)

  val avroDF = kafkaDF.select(expr("InvoiceNumber as key"),
    to_avro(struct("*")).alias("value"))

    val query = avroDF
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "invoices-output-1")
      .outputMode("append")
      .option("checkpointLocation", "chk-point-dir/invoices-output-1")
      .start()

    query.awaitTermination()

}

object App2Reader extends App {

  val schemaAvro = new Schema.Parser().parse(
    """{"type":"record","name":"myrecord","fields":[
      |{"name":"key","type":"string"},
      |{"name":"invoicenumber","type":"string"},
      |{"name":"createdtime","type":"string"}
      |]}""".stripMargin)

  val valueDF = Utils.readKafkaTopic("invoices-output-1")

  val output = valueDF.select(from_avro(col("value"), schemaAvro.toString()).alias("value"))
    .select("value.*")

  output.printSchema()
  output.show(false)


}
object App2Reader1 extends App {

  val schemaAvro = new Schema.Parser().parse(
    """{"type":"record","name":"myrecord","fields":[
      |    {"name": "InvoiceNumber","type": ["string", "null"]},
      |    {"name": "CreatedTime","type": ["long", "null"]},
      |    {"name": "StoreID","type": ["string", "null"]},
      |    {"name": "PosID","type": ["string", "null"]},
      |    {"name": "CustomerType","type": ["string", "null"]},
      |    {"name": "CustomerCardNo","type": ["string", "null"]}
      |]}""".stripMargin)

  //val valueDF = Utils.readKafkaTopic("invoices-output-1")
  val kafkaSourceDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "invoices-output-1")
    .option("startingOffsets", "earliest")
    .load()

  val output = kafkaSourceDF.select(from_avro(col("value"), schemaAvro.toString()).alias("value"))
    .select("value.*")

  val query = output
    .writeStream
    .format("console")
    .outputMode("append")
    .option("checkpointLocation", "chk-point-dir/console-output-1")
    .start()

  query.awaitTermination()

}