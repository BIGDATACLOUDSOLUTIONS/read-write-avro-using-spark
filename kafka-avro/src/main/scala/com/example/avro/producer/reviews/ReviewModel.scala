package com.example.avro.producer.reviews

case class ReviewModel(marketplace: String,
                       customer_id: Int,
                       review_id: Int,
                       product_id: Int,
                       product_title: Option[String]=None,
                       category: String,
                       sub_category: Option[String]=None,
                       brand: Option[String]=None,
                       sale_price: Double,
                       market_price: Double,
                       product_type: Option[String]=None,
                       star_rating: String,
                       helpful_votes: Int,
                       total_votes: Int,
                       verified_purchase: Option[String]=None,
                       review_date: String)

