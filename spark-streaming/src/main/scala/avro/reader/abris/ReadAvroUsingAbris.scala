package avro.reader.abris

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import za.co.absa.abris.avro.functions.from_avro
import za.co.absa.abris.config.AbrisConfig

object ReadAvroUsingAbris extends App {
  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Kafka Avro Sink Demo")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()

  val kafkaSourceDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "customer-avro")
    .option("startingOffsets", "earliest")
    .load()

  val abrisConfig = AbrisConfig
    .fromConfluentAvro
    .downloadReaderSchemaByLatestVersion
    .andTopicNameStrategy("customer-avro")
    .usingSchemaRegistry("http://localhost:8081")

  val deserialized = kafkaSourceDF.select(from_avro(col("value"), abrisConfig) as 'data).select("data.*")

  val query = deserialized
    .writeStream
    .format("console")
    .outputMode("append")
    .option("checkpointLocation", "chk-point-dir")
    .start()

  query.awaitTermination()


}
