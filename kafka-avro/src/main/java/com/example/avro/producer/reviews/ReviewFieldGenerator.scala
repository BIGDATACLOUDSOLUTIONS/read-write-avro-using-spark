package com.example.avro.producer.reviews

import org.json4s.DefaultFormats
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.io.Source
import scala.util.Random

case class Product(
                    product_id: Int,
                    product_title: String = null,
                    category: String = null,
                    sub_category: String = null,
                    brand: String = null,
                    sale_price: Double = 0,
                    market_price: Double = 0,
                    product_type: String = null,
                    rating: String = null)

object ReviewFieldGenerator {

  val random: Random.type = Random
  val listOfMarket = List("UK", "US", "India", "China")

  def apply(productFilePath: String): Array[Product] = {
    readInputFile(productFilePath)
  }

  def readInputFile(productFilePath: String): Array[Product] = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val bigBasketProduct: Array[Product] = Source.fromFile(productFilePath).getLines.map(line => {
      val jsValue = parse(s"""$line""")
      jsValue.extract[Product]
    }).toArray
    bigBasketProduct
  }

  def generateReviewModel(bigBasketProduct: Array[Product]): ReviewModel = {
    val marketPlace = listOfMarket(random.nextInt(4))
    val customerId = random.nextInt(50000)
    val review_id = random.nextInt(9999999)

    val bigBasketProductCaseClassObject = bigBasketProduct(random.nextInt(10000))
    val product_id: Int = bigBasketProductCaseClassObject.product_id
    val product_title: String = bigBasketProductCaseClassObject.product_title
    val category: String = bigBasketProductCaseClassObject.category
    val sub_category: String = bigBasketProductCaseClassObject.sub_category
    val brand: String = bigBasketProductCaseClassObject.brand
    val sale_price: Double = bigBasketProductCaseClassObject.sale_price
    val market_price: Double = bigBasketProductCaseClassObject.market_price
    val product_type: String = bigBasketProductCaseClassObject.product_type
    val star_rating: String = bigBasketProductCaseClassObject.rating
    val helpful_votes: Int = random.nextInt(5000)
    val total_votes: Int = random.nextInt(10000)
    val verified_purchase: Boolean = random.nextBoolean()
    val review_date: String = s"${List("2020", "2021", "2022")(random.nextInt(3))}-${"%02d".format((1 to 12) (random.nextInt(12)))}-${"%02d".format((1 to 31) (random.nextInt(31)))}"

    ReviewModel(marketPlace, customerId, review_id, product_id, product_title, category,
      sub_category, brand, sale_price, market_price, product_type, star_rating, helpful_votes,
      total_votes, verified_purchase, review_date
    )
  }

  def main(args:Array[String]):Unit={
    val productFilePath = "C:\\Users\\RAJES\\IdeaProjects\\Learning2023\\kafka-generator-with-spark\\datasets\\BigBasketProducts.json"

    while(true){
      println(generateReviewModel(readInputFile(productFilePath)))
    }
  }

}
