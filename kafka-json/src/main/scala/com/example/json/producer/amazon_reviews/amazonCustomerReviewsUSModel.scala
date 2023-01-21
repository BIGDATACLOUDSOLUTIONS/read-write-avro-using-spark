package com.example.json.producer.amazon_reviews

case class amazonCustomerReviewsUSModel(marketplace: String,
                                        customer_id: String,
                                        review_id: String,
                                        productid: String,
                                        product_parent: String,
                                        product_title: String,
                                        product_category: String,
                                        star_rating: String,
                                        helpful_votes: String,
                                        total_votes: String,
                                        vine: String,
                                        verified_purchase: String,
                                        review_headline: String,
                                        review_body: String,
                                        review_date: String)

