#Data Skew:
#Data skew occurs when certain partitions or key values become disproportionately larger than others.
# This can lead to processing imbalances and slow down the overall job. In our example,
# let's simulate data skew by duplicating a specific customer's orders excessively.

#Assuming you have a orders DataFrame with columns order_id, customer_id, and order_amount,
# we'll introduce data skew for a particular customer:

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

# Perform the join and aggregation as before
order_items_df = spark.read.csv("order_items.csv", header=True, inferSchema=True)
joined_df = combined_orders.join(order_items_df, on="order_id")
customer_total_spending = joined_df.groupBy("customer_id").agg(sum("order_amount").alias("total_spending"))

# Show the results
customer_total_spending.show()

# Stop the Spark session
spark.stop()


#By excessively duplicating orders for a specific customer, we've introduced data skew,
# and the processing of this customer's data will likely take longer compared to other customers.

#By introducing these bottlenecks, you can observe how they impact the overall performance of your
# data pipeline or Spark job. This exercise will help you understand the challenges that can arise
# in real-world scenarios and guide you in optimizing your pipeline for such situations.