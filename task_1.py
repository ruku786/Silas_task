from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum

# Create a Spark session
spark = SparkSession.builder.appName("EcommerceProcessing").getOrCreate()

# Load the CSV files into DataFrames
products_df = spark.read.csv("products.csv", header=True, inferSchema=True)
orders_items_df = spark.read.csv("order_items.csv", header=True, inferSchema=True)
customers_df = spark.read.csv("customer_records.csv", header=True, inferSchema=True)


# Perform joins to associate data
order_details_df = orders_items_df.join(customers_df, "customer_id", "inner") \
    .withColumnRenamed("name", "customer_name") \
    .join(products_df, orders_items_df.products.keys()[0], "inner") \
    .withColumnRenamed("name", "product_name")

# Grouping, Aggregation, and Summarization
order_summary_df = order_details_df.groupBy("customer_name") \
    .agg(count("order_id").alias("order_count"),
         sum("total_amount").alias("total_spent"))

# Filtering
out_of_stock_products_df = products_df.filter(col("stock_quantity") <= 0)

# 2.Save the transformed data to a Parquet file
#transformed_data_df.write.parquet("transformed_data.parquet")

# Stop the Spark session
spark.stop()
