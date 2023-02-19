package com.spark.stream.example.avro

import com.spark.stream.example.Utils._
import com.spark.batch.example.Utils.{getSchemaFromAvroFile, deleteNonEmptyDir}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.to_avro
import org.apache.spark.sql.functions.{col, expr, struct}

object StreamFromFileToKafka extends App {

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("File Streaming Demo")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

  val schemaFilePath = moduleRootDir + "src/main/resources/schema/reviewsV1.avsc"
  val schemaAvro = getSchemaFromAvroFile(schemaFilePath)

  val rawDF = spark.readStream
    .format("avro")
    .option("avroSchema", schemaAvro)
    .option("path", "datasets/output/kafka_avro/review_avro/")
    .option("maxFilesPerTrigger", 1)
    .load()

  val checkPointLocation = "chk-point-dir/spark-streaming/StreamFromFileToKafka"
  deleteNonEmptyDir(checkPointLocation) //Just for testing

  /*
    val query = rawDF.select((struct("*")).alias("value"))
      .writeStream
      .format("console")
      .outputMode("append")
      .option("checkpointLocation", "chk-point-dir/spark-streaming/StreamFromFileToKafka")
      .start()
  */

  val kafkaTargetDF = rawDF
    .select(to_avro(struct("marketplace", "customer_id", "review_id")).alias("value"))

  val query = kafkaTargetDF
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "stream_from_file_to_kafka-4")
    .outputMode("append")
    .option("checkpointLocation", checkPointLocation)
    .start()

  query.awaitTermination()

}
