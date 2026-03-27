#!/usr/bin/env python3

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("load_data") \
    .config("spark.driver.memory", "2g") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
    .getOrCreate()

df = spark.read.parquet("/opt/airflow/data")

users = df.select("user_id", "user_phone").distinct().dropna(subset=["user_id"])
drivers = df.select("driver_id", "driver_phone").distinct().dropna(subset=["driver_id"])
stores = df.select("store_id", "store_address").distinct().dropna(subset=["store_id"])
items = df.select("item_id", "item_title", "item_category").distinct().dropna(subset=["item_id"])
orders = df.select("order_id", "user_id", "store_id", "driver_id", "created_at", "address_text", "paid_at", "delivery_started_at", "delivered_at", "canceled_at", "payment_type", "order_discount", "order_cancellation_reason", "delivery_cost").distinct().dropna(subset=["order_id"])
order_items = df.select("order_id", "item_id", "item_quantity", "item_price", "item_discount", "item_canceled_quantity", "item_replaced_id").dropna(subset=["order_id", "item_id"])

jdbc_url = "jdbc:postgresql://postgres:5432/delivery_db"
jdbc_props = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

users.write.jdbc(jdbc_url, "users", mode="overwrite", properties=jdbc_props)
drivers.write.jdbc(jdbc_url, "drivers", mode="overwrite", properties=jdbc_props)
stores.write.jdbc(jdbc_url, "store", mode="overwrite", properties=jdbc_props)
items.write.jdbc(jdbc_url, "item", mode="overwrite", properties=jdbc_props)
orders.write.jdbc(jdbc_url, "orders", mode="overwrite", properties=jdbc_props)
order_items.write.jdbc(jdbc_url, "order_item", mode="overwrite", properties=jdbc_props)

spark.stop()
