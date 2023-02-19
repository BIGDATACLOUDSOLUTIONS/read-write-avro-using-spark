package com.spark.stream.example.avro

import com.spark.stream.example.Utils._
import com.spark.batch.example.Utils.{getSchemaFromAvroFile,deleteNonEmptyDir}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions.col

object StreamFromKafkaToConsole extends App {

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Kafka Streaming Demo")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

  val kafkaSourceDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "stream_from_file_to_kafka-4")
    .option("startingOffsets", "earliest")
    .load()

  val schemaFilePath = moduleRootDir + "src/main/resources/schema/review_transformed_schema.avsc"
  val avroSchema = getSchemaFromAvroFile(schemaFilePath)

  val valueDF = kafkaSourceDF
    .select(from_avro(col("value"), avroSchema).alias("value"))
    .select("value.*")

  val checkPointLocation = "chk-point-dir/spark-streaming/StreamFromKafkaToConsole"
  deleteNonEmptyDir(checkPointLocation) //Just for testing

  val query = valueDF
    .writeStream
    .format("console")
    .outputMode("append")
    .option("checkpointLocation", checkPointLocation)
    .start()

  query.awaitTermination()

}
