package com.spark.stream.example

object AppConfig {
  val KAFKA_BROKER_LIST = "common.kafka_broker_list"
  val SCHEMA_REGISTRY_URL = "common.schema_registry_url"
  val REVIEW_AVRO_RAW_FILE_LOCATION = "common.sources.review_avro_raw_file_location"
  val REVIEW_AVRO_RAW_FILE_SCHEMA_LOCATION = "common.sources.review_avro_raw_file_schema_location"
  val REVIEW_AVRO_RAW_KAFKA_TOPIC = "common.sources.review_avro_raw_kafka_topic"

  val CONFIGURATION: Map[String, Map[String, String]] =
    Map("StreamFromFileToKafkaProp" ->
      Map("TARGET_TOPIC" -> "stream_from_file_to_kafka.target.kafka_topic_name",
        "CHECKPOINT_DIR" -> "stream_from_file_to_kafka.target.checkpoint_location"
      ),
      "StreamFromKafkaToConsole" ->
        Map("SOURCE_TOPIC" -> "stream_from_kafka_to_console.source.kafka_topic_name",
          "SOURCE_SCHEMA_FILE_PATH" -> "stream_from_kafka_to_console.source.schema_file_path",
          "CHECKPOINT_DIR" -> "stream_from_kafka_to_console.target.checkpoint_location"
        ),
      "ConfluentKafkaAvroReader" ->
        Map("CHECKPOINT_DIR" -> "confluentKafkaReaderWriter.reader.target.checkpoint_location",
        ),
      "ConfluentKafkaAvroWriter" ->
        Map(
          "REVIEW_TARGET_SCHEMA_FILE_PATH" -> "confluentKafkaReaderWriter.writer.target.review_avro_schema_file_path",
          "TARGET_KAFKA_TOPIC" -> "confluentKafkaReaderWriter.writer.target.kafka_topic_name",
          "CHECKPOINT_DIR" -> "confluentKafkaReaderWriter.writer.target.checkpoint_location",
        )
    )

}
