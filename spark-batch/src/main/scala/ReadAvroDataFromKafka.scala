import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.spark.sql.functions.{col, lit, udf}
import za.co.absa.abris.avro.functions.from_avro
import za.co.absa.abris.config.AbrisConfig

import java.nio.ByteBuffer


/** App2:
 * About the source of this application:
 * Using Standard avro schema file(reviewsV1.avsc) and Confluent Kakfa producer, Data was written to Kafka Topic in avro format.
 * and schema was registered to Confluent Schema Registry.
 *
 * This application
 * 1. pulls the schema from Schema Registry
 * 2. reads data from kafka
 * 3. Applies the schema to extract the columns from DataFrame
 */

object App1 extends App {

  // Either we read schema from file or schema registry, we can't read the data correctly without custom deserializer
  val schemaAvro = Utils.getSchemaFromAvroSchemaRegistry("reviewsV1-avro")
  val kafkaSourceDF = Utils.readKafkaTopic("reviewsV1-avro")

  val deserialized = kafkaSourceDF.select(from_avro(col("value"), schemaAvro) as 'data).select("data.*")

  deserialized.show(false)

  /**
   * This may/may not fail as the data on kafka is avro format not binary format.
   * The data on kafka was serialized using io.confluent.kafka.serializers.KafkaAvroSerializer which not a generic serializer,
   * So spark by default is not able to deserialize by itself
   *
   * Error: Caused by: java.lang.ArrayIndexOutOfBoundsException: -51
   * Even we get the data, it will not be in string format

   To deserialize, either we have to write our own deserializer or we can use abris framework
   */

}

object App2 extends Serializable with App {

  // Writing Custom deserialize for avro dataType
  def deserializeMessage(msg: Array[Byte], attribute: String): String = {
    val bb = ByteBuffer.wrap(msg)
    bb.get()
    val schemaRegistry = "http://127.0.0.1:8081"
    val schemaId = bb.getInt
    @transient lazy val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistry, 10);
    @transient lazy val schema = schemaRegistryClient.getByID(schemaId)
    val decoder = DecoderFactory.get().binaryDecoder(msg, bb.position(), bb.remaining(), null)
    @transient lazy val reader = new GenericDatumReader[GenericRecord](schema)
    reader.read(null, decoder).get(attribute).toString
  }

  private val deserialize = deserializeMessage _
  val serdeUDF = udf(deserialize)

  //val schemaAvro = Utils.getSchemaFromAvroSchemaRegistry("WriteAvroDataToKafka-App2")
  val kafkaSourceDF = Utils.readKafkaTopic("WriteAvroDataToKafka-App2")

  val deserialized = kafkaSourceDF
    .select("value")
    .withColumn("marketplace", serdeUDF(col("value"), lit("marketplace")))
    .withColumn("customer_id", serdeUDF(col("value"), lit("customer_id")))
    .withColumn("review_id", serdeUDF(col("value"), lit("review_id")))
    .withColumn("product_id", serdeUDF(col("value"), lit("product_id")))
    .withColumn("product_title", serdeUDF(col("value"), lit("product_title")))
    .withColumn("category", serdeUDF(col("value"), lit("category")))
    .drop("value")

  deserialized.show(2, false)

  /**
   * The data on kafka was serialized using io.confluent.kafka.serializers.KafkaAvroSerializer which not a generic serializer
    +-----------+-----------+---------+----------+----------------------------------------------------+----------------------+
    |marketplace|customer_id|review_id|product_id|product_title                                       |category              |
    +-----------+-----------+---------+----------+----------------------------------------------------+----------------------+
    |US         |49088      |5380959  |18866     |Papad - Triangle                                    |Snacks & Branded Foods|
    |UK         |25311      |9055844  |11398     |Premium Baby Wet Wipes With Aloe Vera - Paraben Free|Baby Care             |
    +-----------+-----------+---------+----------+----------------------------------------------------+----------------------
   */

}

object App3 extends Serializable with App {
  val schemaAvro = Utils.getSchemaFromAvroSchemaRegistry("reviewsV1-avro")
  val kafkaSourceDF = Utils.readKafkaTopic("reviewsV1-avro")

  val abrisConfig = AbrisConfig
    .fromConfluentAvro
    .downloadReaderSchemaByLatestVersion
    .andTopicNameStrategy("reviewsV1-avro")
    .usingSchemaRegistry("http://localhost:8081")

  val deserialized = kafkaSourceDF.select(from_avro(col("value"), abrisConfig) as 'data).select("data.*")

  deserialized.show(2,false)

  /**
   * The data on kafka was serialized using io.confluent.kafka.serializers.KafkaAvroSerializer which not a generic serializer
    +-----------+-----------+---------+----------+----------------------------------------------------+----------------------+
    |marketplace|customer_id|review_id|product_id|product_title                                       |category              |
    +-----------+-----------+---------+----------+----------------------------------------------------+----------------------+
    |US         |49088      |5380959  |18866     |Papad - Triangle                                    |Snacks & Branded Foods|
    |UK         |25311      |9055844  |11398     |Premium Baby Wet Wipes With Aloe Vera - Paraben Free|Baby Care             |
    +-----------+-----------+---------+----------+----------------------------------------------------+----------------------
   */

}
