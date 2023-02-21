package com.spark.batch.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.nio.file.FileSystems
import scala.util.Random

object PlayPin extends App {

  implicit lazy val spark: SparkSession = SparkSession.builder().master("local[3]").appName("Spark-Batch").getOrCreate()
  import spark.implicits._

  val moduleRootDir: String = FileSystems.getDefault.getPath("").toAbsolutePath.toString + "/kafka-avro/"
  val df = spark
    .read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(s"$moduleRootDir/src/main/resources/data/BigBasketProducts.csv")

  val random: Random.type = Random

  val transformedDF = df.where("description is not null").drop("description")
    .withColumn("index", $"index".cast("int"))
    .filter("index is not null")
    .withColumnRenamed("index", "product_id")
    .withColumnRenamed("product", "product_title")
    .withColumnRenamed("type", "product_type")
    .withColumn("sale_price", $"sale_price".cast("double"))
    .withColumn("market_price", $"market_price".cast("double"))
    .withColumn("rating", $"rating".cast("double"))
    .filter("product_id>=10000 and product_id<=20042")
    .withColumn("product_title", when(col("product_id") % random.nextInt(200) === 0, null).otherwise(col("product_title")))
    .withColumn("sub_category", when(col("product_id") % random.nextInt(200) === 0, null).otherwise(col("sub_category")))
    .withColumn("brand", when(col("product_id") % random.nextInt(200) === 0, null).otherwise(col("brand")))
    .withColumn("product_type", when(col("product_id") % random.nextInt(200) === 0, null).otherwise(col("product_type")))

  println("Total count: " + transformedDF.count)

  val nullRecordsDF = transformedDF
    .filter("""product_title is null or sub_category is null or brand is null or product_type is null""")

  println("Total null count: " + nullRecordsDF.count)
  nullRecordsDF.show(false)

  transformedDF
    .orderBy($"product_id".asc)
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("json")
    .save("datasets/kafka-avro/product_data/")

}
