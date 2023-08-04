#Introduce synthetic data errors (e.g., invalid data, missing values) in the sample dataset
#Invalid Product Price:
import pandas as pd

# Load the products dataset
products_df = pd.read_csv("products.csv")

# Introduce invalid price for a product
products_df.loc[0, "price"] = -100

# Save the modified dataset
products_df.to_csv("products_with_errors.csv", index=False)

#Out-of-Stock Product:
# Introduce out-of-stock situation for a product
products_df.loc[1, "stock_quantity"] = -10

# Save the modified dataset
products_df.to_csv("products_with_errors.csv", index=False)

#Incomplete Customer Information:
# Load the customers dataset
customers_df = pd.read_csv("customers.csv")

# Introduce missing information for a customer
new_customer = {"customer_id": "123456", "first_name": "", "last_name": "Doe", "email": "johndoe@example.com"}
customers_df = customers_df.append(new_customer, ignore_index=True)

# Save the modified dataset
customers_df.to_csv("customers_with_errors.csv", index=False)

#Missing Customer in an Order:

# Load the orders dataset
orders_df = pd.read_csv("orders.csv")

# Introduce a missing customer in an order
new_order = {"order_id": "987654", "customer_id": "nonexistent_customer", "order_date": "2023-08-04", "total_amount": 150, "products": "{}"}
orders_df = orders_df.append(new_order, ignore_index=True)

# Save the modified dataset
orders_df.to_csv("orders_with_errors.csv", index=False)

#Incorrect Product Quantity:
# Introduce incorrect product quantity in an order
orders_df.loc[0, "products"] = '{"product_id_1": -2, "product_id_2": 5}'

# Save the modified dataset
orders_df.to_csv("orders_with_errors.csv", index=False)

#Mismatched Product IDs:
# Introduce mismatched product ID in an order
orders_df.loc[1, "products"] = '{"product_id_3": 3, "product_id_4": 2}'

# Save the modified dataset
orders_df.to_csv("orders_with_errors.csv", index=False)
