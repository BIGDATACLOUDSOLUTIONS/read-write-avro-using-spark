package com.spark.stream.example.avro.reader.confluent

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

object SparkStructuredStream extends Serializable {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Kafka Avro Sink Demo")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()

    import spark.implicits._

    val kafkaSourceDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "com.spark.batch.example.ReadWriteKafka.AppOneKafkaWriter")
      .option("startingOffsets", "earliest")
      .load()

    val valueDF = kafkaSourceDF.selectExpr("topic", "offset", "CAST(key AS STRING)", "value AS data")
    val parsedDF = valueDF.select($"key", DspAvroDecoder.serdeUDF(col("data"), lit("marketplace")).alias("marketplace"))

    //parsedDF.show(false)
    val query = parsedDF
      .writeStream
      .format("console")
      //.option("numRows", 2)
      .outputMode("append")
      .option("checkpointLocation", "chk-point-dir/spark-streaming/app1")
      .start()

    query.awaitTermination()
  }
}
