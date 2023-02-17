package com.example.avro.producer

object AppConfig {
  val KAFKA_BROKER_LIST: String = "127.0.0.1:9092"
  val SCHEMA_REGISTRY_URL: String = "http://127.0.0.1:8081"

  val REVIEW_KAFKA_BROKER_LIST = "review.kafka_broker_list"
  val REVIEW_SCHEMA_REGISTRY_URL = "review.schema_registry_url"
  val REVIEW_KAFKA_TOPIC = "review.kafka_topic"
  val REVIEW_PRODUCER_NO_OF_THREADS="review.producer.number_of_threads"
  val REVIEW_PRODUCER_NUMBER_OF_MESSAGE_TO_PUBLISH = "review.producer.number_of_message_to_publish_by_each_thread"
  val REVIEW_PRODUCER_PRODUCT_INPUT_FILE_PATH= "review.producer.product_input_file_path"
  val REVIEW_PRODUCER_RESULT_AVRO_OUTPUT_PATH= "review.producer.result_avro_output_path"
  val REVIEW_CONSUMER_NUMBER_OF_MESSAGE_TO_CONSUME="review.consumer.number_of_message_to_consume"
  val REVIEW_CONSUMER_AVRO_FILE_PATH_TO_READ="review.consumer.avro_file_path_to_read"

}
