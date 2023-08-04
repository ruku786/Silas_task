#2.Design a simple data model that represents the data downloaded in task 1 along
#with the result of your operations on the data.

#CREATE keyspace data_model with replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
#cqlsh> use data_model;
#cqlsh:data_model> CREATE TABLE IF NOT EXISTS products (
#              ...   product_id UUID PRIMARY KEY,
#              ...   name TEXT,
#              ...   description TEXT,
#              ...   price DECIMAL,
#              ...   stock_quantity INT
#              ... );

#3.Write Python code to insert, update, and retrieve data from the NoSQL database
#(Kindly consider bulk operations as well).

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

# Connect to the Scylla cluster
cluster = Cluster(['127.0.0.1'])  # Replace with your Scylla cluster IP addresses
session = cluster.connect()


# Insert single row
insert_query = session.prepare("INSERT INTO products (product_id, name, description, price, stock_quantity ) VALUES (?, ?, ?, ?, ?)")
data = ("1e35de4f-e915-4fc5-8d14-544363c3eab7", "Novel", 437.67, "The kite Runner", 5000)
session.execute(insert_query, data)
print("Inserted single row.")

# Bulk insert using batch statement
batch = BatchStatement()
batch.add(insert_query, ("2e35de4f-e915-4fc5-8d14-544363c3eab7", "Fiction", 256.30, "My dream London", 9876))
batch.add(insert_query, ("3e35de4f-e915-4fc5-8d14-544363c3eab7", "Drama", 283.67, "The Fault in our stars", 670))
session.execute(batch)
print("Bulk inserted rows.")

# Update a row
update_query = session.prepare("UPDATE products SET price = ? WHERE product_id = ?")
data = (789, "1e35de4f-e915-4fc5-8d14-544363c3eab7")
session.execute(update_query, data)
print("Row updated.")

# Retrieve data
select_query = session.prepare("SELECT * FROM products WHERE price > ?")
retrieved_rows = session.execute(select_query, (789,))
for row in retrieved_rows:
    print(row.product_id, row.name, row.description, row.price, row.stock_quality)

# Close the connection
cluster.shutdown()
