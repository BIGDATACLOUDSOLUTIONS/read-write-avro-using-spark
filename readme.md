This codebase has been created to show integration of avro data, schema registry, confluent kafka and spark.
Modules: 
1. kafka-avro: This has code to generate avro data either in file or on confluent kafka with schema on schema registry
2. kafka-avro-schema: This module is a repository of schema file used for module kafka-avro. 
   It contains avro schema files which can be used to generate their corresponding sources.
3. kafka-json: This has code to generate json data kafka.
4. spark-batch: This is to read and write avro data either in file or kafka as a batch application.
5. spark-streaming: This is to read and write avro data either in file or kafka as a streaming application.

Pre-requites for spark-batch and spark-streaming:
1. Start the confluent kafka platform on docker using the script: docker-scripts/kafka-avro-schema_registry/docker-compose.yml
   This will start all the kafka services.
   > docker compose up
2. All the application in spark modules are using either avro file to kafka as source. Both of these are written by application in kafka-avro module.
   </br>**Class to Run** kafka-avro/src/main/scala/com/example/avro/producer/reviews/ReviewsKafkaAvroProducerV1.scala
    - It has a variable writeAvroToFile which can have below values:
      - true: If set to true, producer will write the data to file
      - false: If set to false, producer will write the data to kafka and register the schema with schema registry