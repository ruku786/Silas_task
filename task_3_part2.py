#To modify the data pipeline from Task 1 to include data validation steps for identifying
# and handling errors, you can integrate validation checks at various stages of your Spark job.
# Here's an outline of how you can achieve this:

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, count
from pyspark.sql.types import DecimalType
from pyspark.sql.utils import AnalysisException

# Create a Spark session
spark = SparkSession.builder.appName("EcommerceProcessingWithValidation").getOrCreate()

# Load the CSV files into DataFrames
products_df = spark.read.csv("products.csv", header=True, inferSchema=True)
customers_df = spark.read.csv("customers.csv", header=True, inferSchema=True)
orders_df = spark.read.csv("orders.csv", header=True, inferSchema=True)

# Data Validation: Check for invalid product prices
products_df = products_df.withColumn(
    "price",
    when(col("price") > 0, col("price")).otherwise(lit(None).cast(DecimalType(10, 2)))
)

# Data Validation: Check for out-of-stock products
products_df = products_df.withColumn(
    "stock_quantity",
    when(col("stock_quantity") >= 0, col("stock_quantity")).otherwise(0)
)

# Join and Handle Missing Customers
try:
    order_details_df = orders_df.join(customers_df, "customer_id", "inner") \
        .withColumnRenamed("name", "customer_name")
except AnalysisException as e:
    print("Error: " + str(e))
    # Handle missing customer data here

# Data Validation: Validate product IDs in orders
def validate_products(products_map):
    try:
        parsed_map = eval(products_map)
        valid_product_ids = [str(row.product_id) for row in products_df.select("product_id").collect()]
        validated_map = {k: v for k, v in parsed_map.items() if k in valid_product_ids}
        return str(validated_map)
    except Exception as e:
        return None

validate_products_udf = spark.udf.register("validate_products", validate_products)

order_details_df = order_details_df.withColumn("products_validated", validate_products_udf("products"))

# Grouping, Aggregation, and Summarization
order_summary_df = order_details_df.groupBy("customer_name") \
    .agg(count("order_id").alias("order_count"),
         sum("total_amount").alias("total_spent"))

# Data Skew Handling: Handle invalid or missing products
def handle_missing_products(products_map):
    try:
        parsed_map = eval(products_map)
        valid_product_ids = [str(row.product_id) for row in products_df.select("product_id").collect()]
        cleaned_map = {k: v for k, v in parsed_map.items() if k in valid_product_ids}
        return str(cleaned_map) if cleaned_map else None
    except Exception as e:
        return None

handle_missing_products_udf = spark.udf.register("handle_missing_products", handle_missing_products)

order_summary_df = order_summary_df.withColumn("products_cleaned", handle_missing_products_udf("products_validated"))

# Stop the Spark session
spark.stop()


#This modified pipeline includes data validation steps to address the synthetic errors introduced earlier:

#Invalid Product Prices: Invalid prices are replaced with NULL values.
#Out-of-Stock Products: Negative stock quantities are set to 0.
#Missing Customers: An exception is caught when joining with missing customer data.
#Invalid Product IDs in Orders: Product IDs not found in the products dataset are removed.
#Handling Missing Products: Invalid or missing products in orders are cleaned.
#Please customize these validation and error-handling mechanisms based on your specific requirements and use case.