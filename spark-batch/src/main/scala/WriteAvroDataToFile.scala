import Context.spark
import java.nio.file.FileSystems

object AppOneWriter extends App {
  val projectRootDir = FileSystems.getDefault.getPath("").toAbsolutePath.toString
  val avroDataFilePath = projectRootDir + "/spark-batch/" + "src/main/resources/data/review_avro/*"
  val outputPath = projectRootDir + "/spark-batch/" + "src/main/resources/spark_output/avro"

  //Schema from Schema File- reviewsV1.avsc
  val schemaAvro = Utils.getSchemaFromAvroFile("reviewsV1.avsc")

  val valueDF = Utils.createDFFromAvroFile(avroDataFilePath, schemaAvro)
  valueDF.show(false)

  valueDF.coalesce(1).write.format("avro").mode("overwrite").save(s"$outputPath/AppOneWriter")
}

object AppOneReader extends App {

  val projectRootDir = FileSystems.getDefault.getPath("").toAbsolutePath.toString
  val sourcePath = projectRootDir + "/spark-batch/" + "src/main/resources/spark_output/avro/AppOneWriter"

  //Schema from Schema File- reviewsV1.avsc
  val schemaAvro = Utils.getSchemaFromAvroFile("reviewsV1.avsc")
  val df=  Utils.createDFFromAvroFile(sourcePath,schemaAvro)
  df.show(false)
}

object AppOneReaderTwo extends App {

  val projectRootDir = FileSystems.getDefault.getPath("").toAbsolutePath.toString
  val sourcePath = projectRootDir + "/spark-batch/" + "src/main/resources/spark_output/avro/AppOneWriter"

  //Schema from Schema Registry. Same data was pushed to kafka topic with same schema
  val schemaAvro = Utils.getSchemaFromAvroSchemaRegistry("reviewsV1-avro")
  val df=  Utils.createDFFromAvroFile(sourcePath,schemaAvro)
  df.show(false)
}