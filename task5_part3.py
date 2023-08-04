#Addressing Data Skew:
#To mitigate data skew, we can perform data re-partitioning to distribute the data more evenly across
# partitions. This can help avoid the processing imbalance caused by skewed data.
# Here's how you can do it:
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Create a Spark session
spark = SparkSession.builder.appName("DataProcessing").getOrCreate()

# Load the dataset
products_df = spark.read.csv("products.csv", header=True, inferSchema=True)

# Introduce data skew by duplicating orders for a specific customer
skewed_orders = products_df.filter(products_df.customer_id == 'skewed_customer')
skewed_orders = skewed_orders.union(skewed_orders).union(skewed_orders)

# Combine skewed and non-skewed orders
combined_orders = products_df.union(skewed_orders)

# Perform the join and aggregation as before, but with re-partitioning
order_items_df = spark.read.csv("order_items.csv", header=True, inferSchema=True)

# Repartition the combined_orders DataFrame to evenly distribute data
repartitioned_orders = combined_orders.repartition("customer_id")

# Join and aggregation
joined_df = repartitioned_orders.join(order_items_df, on="order_id")
customer_total_spending = joined_df.groupBy("customer_id").agg(sum("order_amount").alias("total_spending"))

# Show the results
customer_total_spending.show()

# Stop the Spark session
spark.stop()


#By repartitioning the combined_orders DataFrame based on the "customer_id" column,
# we aim to distribute the data more evenly across partitions, reducing the impact of data skew.


