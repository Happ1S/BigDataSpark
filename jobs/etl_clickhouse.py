import urllib.request
import urllib.error
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("ETL: Star Schema -> ClickHouse Reports") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

PG_URL = "jdbc:postgresql://postgres:5432/bigdata"
PG_PROPS = {
    "user": "spark",
    "password": "spark123",
    "driver": "org.postgresql.Driver"
}

CH_URL  = "jdbc:clickhouse://clickhouse:8123/default"
CH_PROPS = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}
CH_HTTP = "http://clickhouse:8123/"


def ch_exec(sql: str):
    req = urllib.request.Request(CH_HTTP, data=sql.encode())
    try:
        with urllib.request.urlopen(req) as resp:
            resp.read()
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"ClickHouse error ({e.code}): {e.read().decode()}")


def write_ch(df, table: str):
    ch_exec(f"TRUNCATE TABLE IF EXISTS {table}")
    df.write.jdbc(CH_URL, table, mode="append", properties=CH_PROPS)
    print(f"{table}: {df.count()} rows written")


# ── create ClickHouse tables ──────────────────────────────────────────────────
ch_exec("""
CREATE TABLE IF NOT EXISTS report_products
(
    product_id     Int32,
    product_name   String,
    category       String,
    brand          String,
    total_quantity Int64,
    total_revenue  Float64,
    avg_price      Float64,
    avg_rating     Float64,
    review_count   Int32,
    sales_rank     Int32
)
ENGINE = MergeTree()
ORDER BY (category, total_revenue)
""")

ch_exec("""
CREATE TABLE IF NOT EXISTS report_customers
(
    customer_id   Int32,
    customer_name String,
    country       String,
    total_spent   Float64,
    order_count   Int64,
    avg_order     Float64,
    customer_rank Int32
)
ENGINE = MergeTree()
ORDER BY total_spent
""")

ch_exec("""
CREATE TABLE IF NOT EXISTS report_time
(
    year            Int32,
    month           Int32,
    total_revenue   Float64,
    order_count     Int64,
    avg_order_size  Float64
)
ENGINE = MergeTree()
ORDER BY (year, month)
""")

ch_exec("""
CREATE TABLE IF NOT EXISTS report_stores
(
    store_id      Int32,
    store_name    String,
    city          String,
    country       String,
    total_revenue Float64,
    order_count   Int64,
    avg_order     Float64,
    revenue_rank  Int32
)
ENGINE = MergeTree()
ORDER BY total_revenue
""")

ch_exec("""
CREATE TABLE IF NOT EXISTS report_suppliers
(
    supplier_id        Int32,
    supplier_name      String,
    city               String,
    country            String,
    total_revenue      Float64,
    avg_product_price  Float64,
    product_count      Int64,
    revenue_rank       Int32
)
ENGINE = MergeTree()
ORDER BY total_revenue
""")

ch_exec("""
CREATE TABLE IF NOT EXISTS report_quality
(
    product_id    Int32,
    product_name  String,
    category      String,
    avg_rating    Float64,
    review_count  Int32,
    total_sales   Int64,
    total_revenue Float64,
    rating_rank   Int32
)
ENGINE = MergeTree()
ORDER BY avg_rating
""")

print("ClickHouse tables created/verified.")

# ── load star schema ──────────────────────────────────────────────────────────
fact   = spark.read.jdbc(PG_URL, "fact_sales",   properties=PG_PROPS)
prod   = spark.read.jdbc(PG_URL, "dim_product",  properties=PG_PROPS)
cust   = spark.read.jdbc(PG_URL, "dim_customer", properties=PG_PROPS)
store  = spark.read.jdbc(PG_URL, "dim_store",    properties=PG_PROPS)
supp   = spark.read.jdbc(PG_URL, "dim_supplier", properties=PG_PROPS)
date   = spark.read.jdbc(PG_URL, "dim_date",     properties=PG_PROPS)

fact.cache()

# ── 1. report_products ────────────────────────────────────────────────────────
rank_w = Window.orderBy(F.desc("total_quantity"))

report_products = fact \
    .join(prod, fact["product_id"] == prod["product_id"]) \
    .groupBy(prod["product_id"], prod["name"], prod["category"],
             prod["brand"], prod["rating"], prod["reviews"]) \
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("total_price").alias("total_revenue"),
        F.avg(prod["price"]).alias("avg_price")
    ) \
    .withColumn("avg_rating",  F.col("rating").cast("double")) \
    .withColumn("review_count", F.col("reviews").cast("int")) \
    .withColumn("sales_rank", F.row_number().over(rank_w)) \
    .select(
        prod["product_id"].alias("product_id"),
        F.col("name").alias("product_name"),
        "category", "brand",
        "total_quantity", "total_revenue", "avg_price",
        "avg_rating", "review_count", "sales_rank"
    )

write_ch(report_products, "report_products")

# ── 2. report_customers ───────────────────────────────────────────────────────
cust_rank_w = Window.orderBy(F.desc("total_spent"))

report_customers = fact \
    .join(cust, fact["customer_id"] == cust["customer_id"]) \
    .groupBy(cust["customer_id"],
             F.concat_ws(" ", cust["first_name"], cust["last_name"]).alias("customer_name"),
             cust["country"]) \
    .agg(
        F.sum("total_price").alias("total_spent"),
        F.count("sale_id").alias("order_count"),
        F.avg("total_price").alias("avg_order")
    ) \
    .withColumn("customer_rank", F.row_number().over(cust_rank_w)) \
    .select(
        cust["customer_id"].alias("customer_id"),
        "customer_name", "country",
        "total_spent", "order_count", "avg_order", "customer_rank"
    )

write_ch(report_customers, "report_customers")

# ── 3. report_time ────────────────────────────────────────────────────────────
report_time = fact \
    .join(date, fact["date_id"] == date["date_id"]) \
    .groupBy(date["year"], date["month"]) \
    .agg(
        F.sum("total_price").alias("total_revenue"),
        F.count("sale_id").alias("order_count"),
        F.avg("total_price").alias("avg_order_size")
    ) \
    .orderBy("year", "month")

write_ch(report_time, "report_time")

# ── 4. report_stores ──────────────────────────────────────────────────────────
store_rank_w = Window.orderBy(F.desc("total_revenue"))

report_stores = fact \
    .join(store, fact["store_id"] == store["store_id"]) \
    .groupBy(store["store_id"], store["name"], store["city"], store["country"]) \
    .agg(
        F.sum("total_price").alias("total_revenue"),
        F.count("sale_id").alias("order_count"),
        F.avg("total_price").alias("avg_order")
    ) \
    .withColumn("revenue_rank", F.row_number().over(store_rank_w)) \
    .select(
        store["store_id"].alias("store_id"),
        F.col("name").alias("store_name"),
        "city", "country",
        "total_revenue", "order_count", "avg_order", "revenue_rank"
    )

write_ch(report_stores, "report_stores")

# ── 5. report_suppliers ───────────────────────────────────────────────────────
supp_rank_w = Window.orderBy(F.desc("total_revenue"))

report_suppliers = fact \
    .join(supp, fact["supplier_id"] == supp["supplier_id"]) \
    .join(prod, fact["product_id"] == prod["product_id"]) \
    .groupBy(supp["supplier_id"], supp["name"], supp["city"], supp["country"]) \
    .agg(
        F.sum("total_price").alias("total_revenue"),
        F.avg(prod["price"]).alias("avg_product_price"),
        F.countDistinct(prod["product_id"]).alias("product_count")
    ) \
    .withColumn("revenue_rank", F.row_number().over(supp_rank_w)) \
    .select(
        supp["supplier_id"].alias("supplier_id"),
        F.col("name").alias("supplier_name"),
        "city", "country",
        "total_revenue", "avg_product_price", "product_count", "revenue_rank"
    )

write_ch(report_suppliers, "report_suppliers")

# ── 6. report_quality ─────────────────────────────────────────────────────────
rating_rank_w = Window.orderBy(F.desc("avg_rating"))

report_quality = fact \
    .join(prod, fact["product_id"] == prod["product_id"]) \
    .groupBy(prod["product_id"], prod["name"], prod["category"],
             prod["rating"], prod["reviews"]) \
    .agg(
        F.sum("quantity").alias("total_sales"),
        F.sum("total_price").alias("total_revenue")
    ) \
    .withColumn("avg_rating",  F.col("rating").cast("double")) \
    .withColumn("review_count", F.col("reviews").cast("int")) \
    .withColumn("rating_rank", F.row_number().over(rating_rank_w)) \
    .select(
        prod["product_id"].alias("product_id"),
        F.col("name").alias("product_name"),
        "category", "avg_rating", "review_count",
        "total_sales", "total_revenue", "rating_rank"
    )

write_ch(report_quality, "report_quality")

print("All ClickHouse reports written successfully.")
spark.stop()
