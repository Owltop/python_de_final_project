#!/usr/bin/env python3

from pyspark.sql import SparkSession
import psycopg2

spark = SparkSession.builder \
    .appName("load_data") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
    .getOrCreate()

df = spark.read.parquet("/opt/airflow/data")

users = df.select("user_id", "user_phone").distinct().dropna(subset=["user_id"])
drivers = df.select("driver_id", "driver_phone").distinct().dropna(subset=["driver_id"])
stores = df.select("store_id", "store_address").distinct().dropna(subset=["store_id"])
items = df.select("item_id", "item_title", "item_category").distinct().dropna(subset=["item_id"])
orders = df.select(
    "order_id", "user_id", "store_id", "driver_id",
    "created_at", "address_text", "paid_at",
    "delivery_started_at", "delivered_at", "canceled_at",
    "payment_type", "order_discount", "order_cancellation_reason",
    "delivery_cost"
).distinct().dropna(subset=["order_id"])
order_items = df.select(
    "order_id", "item_id", "item_quantity", "item_price",
    "item_discount", "item_canceled_quantity", "item_replaced_id"
).dropna(subset=["order_id", "item_id"])

jdbc_url = "jdbc:postgresql://postgres:5432/delivery_db"
jdbc_props = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Дропаем и пересоздаём таблицы без PRIMARY KEY 
conn = psycopg2.connect(
    host="postgres", port=5432,
    dbname="delivery_db",
    user="postgres", password="postgres"
)
cur = conn.cursor()
cur.execute("""
    DROP TABLE IF EXISTS order_item CASCADE;
    DROP TABLE IF EXISTS orders CASCADE;
    DROP TABLE IF EXISTS store CASCADE;
    DROP TABLE IF EXISTS item CASCADE;
    DROP TABLE IF EXISTS users CASCADE;
    DROP TABLE IF EXISTS drivers CASCADE;

    CREATE TABLE users (
        user_id    BIGINT,
        user_phone TEXT
    );

    CREATE TABLE drivers (
        driver_id    BIGINT,
        driver_phone TEXT
    );

    CREATE TABLE store (
        store_id      BIGINT,
        store_address TEXT
    );

    CREATE TABLE item (
        item_id       BIGINT,
        item_title    TEXT,
        item_category TEXT
    );

    CREATE TABLE orders (
        order_id                  BIGINT,
        user_id                   BIGINT,
        store_id                  BIGINT,
        driver_id                 BIGINT,
        created_at                TIMESTAMP,
        address_text              TEXT,
        paid_at                   TIMESTAMP,
        delivery_started_at       TIMESTAMP,
        delivered_at              TIMESTAMP,
        canceled_at               TIMESTAMP,
        payment_type              TEXT,
        order_discount            NUMERIC(5,2),
        order_cancellation_reason TEXT,
        delivery_cost             NUMERIC(10,2)
    );

    CREATE TABLE order_item (
        order_id               BIGINT,
        item_id                BIGINT,
        item_quantity          BIGINT,
        item_price             NUMERIC(10,2),
        item_discount          NUMERIC(5,2),
        item_canceled_quantity BIGINT,
        item_replaced_id       BIGINT
    );
""")
conn.commit()
cur.close()
conn.close()

print("Таблицы пересозданы")

users.write.jdbc(jdbc_url, "users", mode="append", properties=jdbc_props)
print("users загружены")

drivers.write.jdbc(jdbc_url, "drivers", mode="append", properties=jdbc_props)
print("drivers загружены")

stores.write.jdbc(jdbc_url, "store", mode="append", properties=jdbc_props)
print("store загружены")

items.write.jdbc(jdbc_url, "item", mode="append", properties=jdbc_props)
print("item загружены")

orders.write.jdbc(jdbc_url, "orders", mode="append", properties=jdbc_props)
print("orders загружены")

order_items.write.jdbc(jdbc_url, "order_item", mode="append", properties=jdbc_props)
print("order_item загружены")

spark.stop()
