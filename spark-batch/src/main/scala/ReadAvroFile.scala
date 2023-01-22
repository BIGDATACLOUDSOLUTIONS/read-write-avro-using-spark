import org.apache.spark.sql.SparkSession
import org.apache.avro.Schema

import java.nio.file.{FileSystems, Files, Paths}
import java.io.File

object ReadAvroFile extends App{

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Kafka Stream Demo")
    .getOrCreate()

  import spark.implicits._
  val projectRootDir= FileSystems.getDefault.getPath("").toAbsolutePath.toString;
  val avroSchemaFilePath = projectRootDir + "/spark-batch/" + "src/main/resources/schema/reviewsV1.avsc"
  val avroDataFilePath = projectRootDir + "/spark-batch/" + "src/main/resources/data/review_avro/*"

  val schemaAvro = new Schema.Parser().parse(new File(avroSchemaFilePath))

  val valueDF = spark
  .read
    .format("avro")
    .option("avroSchema", schemaAvro.toString)
    .load(avroDataFilePath)

  valueDF.show(false)


}
