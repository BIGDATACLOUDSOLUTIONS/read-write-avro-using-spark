package com.example.avro.producer.reviews

import com.example.avro.producer.AppConfig._
import com.example.avro.producer.Utils._
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

class ReviewFieldGenerator {

  val random: Random.type = Random
  val listOfMarket = List("UK", "US", "India", "China")

  def readProductFile(productFilePath: String): Array[Product] = {
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
    val product_title: Option[String] = Some(bigBasketProductCaseClassObject.product_title)
    val category: String = bigBasketProductCaseClassObject.category
    val sub_category: Option[String] = Some(bigBasketProductCaseClassObject.sub_category)
    val brand: Option[String] = Some(bigBasketProductCaseClassObject.brand)
    val sale_price: Double = bigBasketProductCaseClassObject.sale_price
    val market_price: Double = bigBasketProductCaseClassObject.market_price
    val product_type: Option[String] = Some(bigBasketProductCaseClassObject.product_type)
    val star_rating: String = bigBasketProductCaseClassObject.rating
    val helpful_votes: Int = random.nextInt(5000)
    val total_votes: Int = random.nextInt(10000)
    val verified_purchase: Option[String] = Some(List("true", "false", null)(random.nextInt(3)))
    val review_date: String = s"${List("2020", "2021", "2022")(random.nextInt(3))}-${"%02d".format((1 to 12) (random.nextInt(12)))}-${"%02d".format((1 to 31) (random.nextInt(31)))}"

    ReviewModel(marketPlace, customerId, review_id, product_id, product_title, category,
      sub_category, brand, sale_price, market_price, product_type, star_rating, helpful_votes,
      total_votes, verified_purchase, review_date
    )
  }

  def printReviewModel(review: ReviewModel): String = {
    s"""{${review.marketplace},${review.customer_id},${review.review_id},${review.product_id},${review.product_title.orNull},${review.category},${review.sub_category.orNull},${review.brand.orNull},${review.sale_price},${review.market_price},${review.product_type.orNull},${review.star_rating},${review.helpful_votes},${review.total_votes},${review.verified_purchase.orNull},${review.review_date}}"""
  }
}

object ReviewFieldGenerator {

  def main(args: Array[String]): Unit = {

    val productFilePath = moduleRootDir + conf.getString(REVIEW_PRODUCER_PRODUCT_INPUT_FILE_PATH)

    val reviewFieldGenerator = new ReviewFieldGenerator()
    val productData = reviewFieldGenerator.readProductFile(productFilePath)

    val numberOfMessage: Int = conf.getString(REVIEW_PRODUCER_NUMBER_OF_MESSAGE_TO_PUBLISH).toInt

    (1 to numberOfMessage).foreach(x => println(s"$x => ${reviewFieldGenerator.printReviewModel(reviewFieldGenerator.generateReviewModel(productData))}"))
  }

}
