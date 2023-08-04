#Implementing a basic data monitoring system to track data quality metrics and generate alerts for
# anomalies involves setting up a process to periodically assess the data and notify stakeholders when
# issues are detected. Below is a high-level outline of how you can achieve this using Python,
# a scheduling library like schedule, and sending email notifications.

import schedule
import time
import smtplib
from email.mime.text import MIMEText

# Import necessary Spark components
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("DataQualityMonitoring").getOrCreate()

# Load your dataset(s)
products_df = spark.read.csv("products.csv", header=True, inferSchema=True)
customers_df = spark.read.csv("customers.csv", header=True, inferSchema=True)
orders_df = spark.read.csv("orders.csv", header=True, inferSchema=True)

# Define data quality checks and anomaly detection logic
def data_quality_check():
    # Implement your data quality checks here
    anomalies = []

    # Example: Check for negative prices in products
    negative_prices = products_df.filter(col("price") < 0)
    if negative_prices.count() > 0:
        anomalies.append(f"Negative prices found in products: {negative_prices.count()} records")

    # Example: Check for missing customers in orders
    missing_customers = orders_df.join(customers_df, "customer_id", "left_anti")
    if missing_customers.count() > 0:
        anomalies.append(f"Orders with missing customer data: {missing_customers.count()} records")

    # Send email alerts if anomalies are detected
    if anomalies:
        send_alert_email(anomalies)

# Send email alerts
def send_alert_email(anomalies):
    msg = MIMEText("\n".join(anomalies))
    msg["Subject"] = "Data Quality Alert"
    msg["From"] = "your@example.com"
    msg["To"] = "recipient@example.com"

    # Send the email using an SMTP server
    server = smtplib.SMTP("smtp.example.com", 587)
    server.starttls()
    server.login("your@example.com", "your_password")
    server.sendmail("your@example.com", ["recipient@example.com"], msg.as_string())
    server.quit()

# Schedule the data quality check
schedule.every(24).hours.do(data_quality_check)

# Run the monitoring script indefinitely
while True:
    schedule.run_pending()
    time.sleep(1)


