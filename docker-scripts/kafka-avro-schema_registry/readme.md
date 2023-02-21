**References**

<a href="https://xebia.udemy.com/course/confluent-schema-registry/learn/lecture/8643672#questions/14492608/" target="new">Udemy Course By Stephan Mareek: Video Course</a>
<br><a href="https://courses.datacumulus.com/downloads/confluent-schema-registry-3r3/" target="new">Confluent Schema Registry & REST Proxy: Code</a>

Run the docker container using the yaml file: docker-scripts/kafka-avro-schema_registry/docker-compose.yml
This will start the kafka services
<br> Run the below commands: </br>

```
cd docker-scripts/kafka-avro-schema_registry/
docker compose up
```

#### Rest API: http://127.0.0.1:3030/
#### Kafka Broker: 127.0.0.1:9092
#### Schema Registry URL: http://127.0.0.1:8081