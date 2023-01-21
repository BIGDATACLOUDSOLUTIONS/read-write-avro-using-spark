# pos-invoice-generator
mvn clean install

# java -cp <jar file path> <topic_name> <noOfProducers> <produceSpeed: producer sleep time>
java -cp target/kafka-json-1.0-SNAPSHOT-jar-with-dependencies.jar com.example.json.producer.pos_invoice.PosSimulator pos-invoice 3 1