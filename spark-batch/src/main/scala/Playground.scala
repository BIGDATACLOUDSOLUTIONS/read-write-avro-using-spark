import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object Playground {

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Kafka Stream Demo")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val df = spark
      .read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("C:\\Users\\RAJES\\IdeaProjects\\Learning2023\\kafka-generator-with-spark\\datasets\\BigBasketProducts.csv")

    df.where("description is not null").drop("description")
      .withColumn("index", $"index".cast("int"))
      .filter("index is not null")
      .withColumnRenamed("index", "product_id")
      .withColumnRenamed("product", "product_title")
      .withColumnRenamed("type", "product_type")
      .withColumn("sale_price",$"sale_price".cast("double"))
      .withColumn("market_price",$"market_price".cast("double"))
      .withColumn("rating",$"rating".cast("double"))
      .filter("product_id>=10000 and product_id<=20042")
      .orderBy($"product_id".asc)
      .coalesce(1)
      .write
      .mode("overwrite")
      .format("json")
      .save("C:\\\\Users\\\\RAJES\\\\IdeaProjects\\\\Learning2023\\\\kafka-generator-with-spark\\\\datasets\\\\output")
  }


}
