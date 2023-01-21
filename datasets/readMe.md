1. Dataset: BigBasketProducts.csv
   <br><a href="https://www.kaggle.com/datasets/surajjha101/bigbasket-entire-product-list-28k-datapoints?select=BigBasket+Products.csv" target="new">DownloadLink</a>
   <br>This dataset contains 10 attributes with simple meaning and which are described as follows:
   - index - Simply the Index!
   - product - Title of the product (as they're listed)
   - category - Category into which product has been classified
   - sub_category - Subcategory into which product has been kept
   - brand - Brand of the product
   - sale_price - Price at which product is being sold on the site
   - market_price - Market price of the product
   - type - Type into which product falls
   - rating - Rating the product has got from its consumers
   - description - Description of the dataset (in detail)
     <br>While doing FE, "discount" can be created as (market_price - sale_price)/ market_price * 100 which will help in getting what consumers are getting better here!

   **Code to convert CSV to JSON**
    ```
     val df = spark
          .read
          .format("csv")
          .option("inferSchema", "true")
          .option("header", "true")
          .load("src/main/resources/input/BigBasketProducts.csv")
    
        df
          .withColumn("index", $"index".cast("int"))
          .filter("index is not null")
          .drop("description")
          .withColumnRenamed("index", "product_id")
          .withColumnRenamed("product", "product_title")
          .filter("product_id>=10000 and product_id<=20000")  // Reduce the size of the file
          .orderBy($"product_id".asc)
          .coalesce(1)
          .write
          .mode("overwrite")
          .format("json")
          .save("src/main/resources/output")
    ```
   
2. 