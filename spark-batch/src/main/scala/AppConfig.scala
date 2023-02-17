object AppConfig {
  val REVIEW_KAFKA_BROKER_LIST = "review_ingest.kafka_broker_list"
  val REVIEW_SCHEMA_REGISTRY_URL = "review_ingest.schema_registry_url"

  val REVIEW_SOURCE_DATA_FILE_PATH = "review_ingest.source_type.file.data_file_path"
  val REVIEW_SOURCE_SCHEMA_FILE_PATH = "review_ingest.source_type.file.schema_file_path"

  val REVIEW_KAFKA_SOURCE_TOPIC = "review_ingest.source_type.kafka.kafka_topic"

  val REVIEW_TARGET_DATA_FILE_PATH = "review_ingest.target_type.file.data_file_path"
  val REVIEW_TARGET_SCHEMA_FILE_PATH = "review_ingest.target_type.schema_file_path"
  val REVIEW_KAFKA_TARGET_TOPIC = "review_ingest.target_type.kafka.kafka_topic"


}
