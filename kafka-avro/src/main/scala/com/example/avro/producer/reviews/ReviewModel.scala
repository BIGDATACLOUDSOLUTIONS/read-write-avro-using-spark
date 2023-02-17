package com.example.avro.producer.reviews

case class ReviewModel(marketplace: String,
                       customer_id: Int,
                       review_id: Int,
                       product_id: Int,
                       product_title: String,
                       category: String,
                       sub_category: String,
                       brand: String,
                       sale_price: Double,
                       market_price: Double,
                       product_type: String,
                       star_rating: String,
                       helpful_votes: Int,
                       total_votes: Int,
                       verified_purchase: Boolean,
                       review_date: String)

