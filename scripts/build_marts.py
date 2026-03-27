from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("BuildMarts") \
    .config("spark.driver.memory", "4g") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
    .getOrCreate()

PG_URL = "jdbc:postgresql://postgres:5432/delivery_db"
PG_CREDS = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

def read_table(table_name):
    return spark.read.jdbc(PG_URL, table_name, properties=PG_CREDS)

orders = read_table("orders")

order_items = read_table("order_item")

stores = read_table("store")
items = read_table("item")

stores = stores.withColumn(
    "city",
    F.trim(F.split(F.col("store_address"), ",")[1])
)

orders = orders \
    .withColumn("year",  F.year("created_at")) \
    .withColumn("month", F.month("created_at")) \
    .withColumn("day",   F.dayofmonth("created_at"))

order_financials = order_items \
    .join(
        orders.select("order_id", "order_discount"),
        on="order_id",
        how="left"
    ) \
    .withColumn(
        "line_turnover",
        F.col("item_price")
        * F.col("item_quantity")
        * (1 - F.col("item_discount") / 100)
        * (1 - F.col("order_discount") / 100)
    ) \
    .withColumn(
        "line_revenue",
        F.col("item_price")
        * (F.col("item_quantity") - F.col("item_canceled_quantity"))
        * (1 - F.col("item_discount") / 100)
        * (1 - F.col("order_discount") / 100)
    ) \
    .groupBy("order_id") \
    .agg(
        F.round(F.sum("line_turnover"), 2).alias("order_turnover"),
        F.round(F.sum("line_revenue"),  2).alias("order_revenue")
    )

mart_orders = orders \
    .join(stores.select("store_id", "city"), on="store_id", how="left") \
    .join(order_financials, on="order_id", how="left") \
    .groupBy("year", "month", "day", "city", "store_id") \
    .agg(

        F.round(F.sum("order_turnover"), 2).alias("turnover"),
        F.round(F.sum("order_revenue"),  2).alias("revenue"),
        F.round(
            F.sum("order_revenue") - F.sum("delivery_cost"), 2
        ).alias("profit"),

        F.countDistinct("order_id").alias("orders_created"),
        F.count(
            F.when(F.col("delivered_at").isNotNull(), 1)
        ).alias("orders_delivered"),
        F.count(
            F.when(F.col("canceled_at").isNotNull(), 1)
        ).alias("orders_canceled"),
        F.count(
            F.when(
                F.col("canceled_at").isNotNull() &
                F.col("delivered_at").isNotNull(), 1
            )
        ).alias("orders_canceled_after_delivery"),
        F.count(
            F.when(
                F.col("order_cancellation_reason").isin(
                    "Ошибка приложения", "Проблемы с оплатой"
                ), 1
            )
        ).alias("orders_canceled_service_error"),

        F.countDistinct("user_id").alias("unique_customers"),
        F.round(F.avg("order_revenue"), 2).alias("avg_check"),

        F.countDistinct("driver_id").alias("active_couriers"),

        F.lit(0).alias("courier_changes"),
    ) \
    .withColumn(
        "orders_per_customer",
        F.round(F.col("orders_created") / F.col("unique_customers"), 2)
    ) \
    .withColumn(
        "revenue_per_customer",
        F.round(F.col("revenue") / F.col("unique_customers"), 2)
    )

mart_items = order_items \
    .join(
        orders.select("order_id", "store_id", "created_at",
                      "canceled_at", "year", "month", "day"),
        on="order_id",
        how="left"
    ) \
    .join(stores.select("store_id", "city"),  on="store_id", how="left") \
    .join(items,                               on="item_id",  how="left") \
    .withColumn(
        "item_turnover",
        F.col("item_price")
        * F.col("item_quantity")
        * (1 - F.col("item_discount") / 100)
    ) \
    .groupBy(
        "year", "month", "day",
        "city", "store_id",
        "item_id", "item_title", "item_category"
    ) \
    .agg(
        F.round(F.sum("item_turnover"), 2).alias("item_turnover"),
        F.sum("item_quantity").alias("units_ordered"),
        F.sum("item_canceled_quantity").alias("units_canceled"),
        F.countDistinct("order_id").alias("orders_with_item"),
        F.count(
            F.when(F.col("item_canceled_quantity") > 0, 1)
        ).alias("orders_with_item_canceled")
    )

def write_mart(df, table_name):
    df.write.jdbc(
        url=PG_URL,
        table=table_name,
        mode="overwrite",
        properties=PG_CREDS
    )
    print(f"✓ {table_name}: {df.count()} строк")

write_mart(mart_orders, "mart_orders")
write_mart(mart_items,  "mart_items")

spark.stop()