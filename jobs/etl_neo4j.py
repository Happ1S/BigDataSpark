from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("ETL: Star Schema -> Neo4j Reports") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

PG_URL = "jdbc:postgresql://postgres:5432/bigdata"
PG_PROPS = {
    "user": "spark",
    "password": "spark123",
    "driver": "org.postgresql.Driver"
}

NEO4J_URL  = "bolt://neo4j:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "neo4j123"

NEO4J_OPTS = {
    "url":  NEO4J_URL,
    "authentication.basic.username": NEO4J_USER,
    "authentication.basic.password": NEO4J_PASS,
}


def write_neo4j(df, label: str, node_keys: str):
    df.write \
        .format("org.neo4j.spark.DataSource") \
        .mode("Overwrite") \
        .options(**NEO4J_OPTS) \
        .option("labels", f":{label}") \
        .option("node.keys", node_keys) \
        .save()
    print(f":{label} — {df.count()} nodes written")


# ── load star schema ──────────────────────────────────────────────────────────
fact  = spark.read.jdbc(PG_URL, "fact_sales",   properties=PG_PROPS)
prod  = spark.read.jdbc(PG_URL, "dim_product",  properties=PG_PROPS)
cust  = spark.read.jdbc(PG_URL, "dim_customer", properties=PG_PROPS)
store = spark.read.jdbc(PG_URL, "dim_store",    properties=PG_PROPS)
supp  = spark.read.jdbc(PG_URL, "dim_supplier", properties=PG_PROPS)
date  = spark.read.jdbc(PG_URL, "dim_date",     properties=PG_PROPS)

fact.cache()

# ── 1. :ProductReport ─────────────────────────────────────────────────────────
rank_w = Window.orderBy(F.desc("total_quantity"))

product_report = fact \
    .join(prod, "product_id") \
    .groupBy(prod["product_id"], prod["name"], prod["category"],
             prod["brand"], prod["rating"], prod["reviews"]) \
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("total_price").alias("total_revenue"),
        F.avg(prod["price"]).alias("avg_price")
    ) \
    .withColumn("sales_rank", F.row_number().over(rank_w)) \
    .select(
        prod["product_id"].alias("product_id"),
        F.col("name").alias("product_name"),
        "category", "brand",
        "total_quantity", "total_revenue", "avg_price",
        F.col("rating").alias("avg_rating"),
        F.col("reviews").alias("review_count"),
        "sales_rank"
    )

write_neo4j(product_report, "ProductReport", "product_id")

# ── 2. :CustomerReport ────────────────────────────────────────────────────────
cust_rank_w = Window.orderBy(F.desc("total_spent"))

customer_report = fact \
    .join(cust, "customer_id") \
    .groupBy(
        cust["customer_id"],
        F.concat_ws(" ", cust["first_name"], cust["last_name"]).alias("customer_name"),
        cust["country"]
    ) \
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

write_neo4j(customer_report, "CustomerReport", "customer_id")

# ── 3. :TimeReport ────────────────────────────────────────────────────────────
time_report = fact \
    .join(date, "date_id") \
    .groupBy(date["year"], date["month"]) \
    .agg(
        F.sum("total_price").alias("total_revenue"),
        F.count("sale_id").alias("order_count"),
        F.avg("total_price").alias("avg_order_size")
    ) \
    .withColumn("period_id",
        (F.col("year") * 100 + F.col("month")).cast("int")
    ) \
    .select("period_id", "year", "month",
            "total_revenue", "order_count", "avg_order_size")

write_neo4j(time_report, "TimeReport", "period_id")

# ── 4. :StoreReport ───────────────────────────────────────────────────────────
store_rank_w = Window.orderBy(F.desc("total_revenue"))

store_report = fact \
    .join(store, "store_id") \
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

write_neo4j(store_report, "StoreReport", "store_id")

# ── 5. :SupplierReport ────────────────────────────────────────────────────────
supp_rank_w = Window.orderBy(F.desc("total_revenue"))

supplier_report = fact \
    .join(supp, "supplier_id") \
    .join(prod, "product_id") \
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

write_neo4j(supplier_report, "SupplierReport", "supplier_id")

# ── 6. :QualityReport ────────────────────────────────────────────────────────
rating_rank_w = Window.orderBy(F.desc("avg_rating"))

quality_report = fact \
    .join(prod, "product_id") \
    .groupBy(prod["product_id"], prod["name"], prod["category"],
             prod["rating"], prod["reviews"]) \
    .agg(
        F.sum("quantity").alias("total_sales"),
        F.sum("total_price").alias("total_revenue")
    ) \
    .withColumn("rating_rank", F.row_number().over(rating_rank_w)) \
    .select(
        prod["product_id"].alias("product_id"),
        F.col("name").alias("product_name"),
        "category",
        F.col("rating").alias("avg_rating"),
        F.col("reviews").alias("review_count"),
        "total_sales", "total_revenue", "rating_rank"
    )

write_neo4j(quality_report, "QualityReport", "product_id")

# ── graph relationships ───────────────────────────────────────────────────────
# SOLD_IN: (:ProductReport)-[:SOLD_IN]->(:StoreReport)
sold_in = fact \
    .join(prod, "product_id") \
    .join(store, "store_id") \
    .groupBy(prod["product_id"], store["store_id"]) \
    .agg(F.sum("total_price").alias("revenue")) \
    .select(
        prod["product_id"].alias("source_product_id"),
        store["store_id"].alias("target_store_id"),
        "revenue"
    )

sold_in.write \
    .format("org.neo4j.spark.DataSource") \
    .mode("Overwrite") \
    .options(**NEO4J_OPTS) \
    .option("relationship", "SOLD_IN") \
    .option("relationship.save.strategy", "keys") \
    .option("relationship.source.labels", ":ProductReport") \
    .option("relationship.source.node.keys", "source_product_id:product_id") \
    .option("relationship.target.labels", ":StoreReport") \
    .option("relationship.target.node.keys", "target_store_id:store_id") \
    .save()

print(":SOLD_IN relationships written")

# SUPPLIED_BY: (:ProductReport)-[:SUPPLIED_BY]->(:SupplierReport)
supplied_by = fact \
    .join(prod, "product_id") \
    .join(supp, "supplier_id") \
    .groupBy(prod["product_id"], supp["supplier_id"]) \
    .agg(F.sum("total_price").alias("revenue")) \
    .select(
        prod["product_id"].alias("source_product_id"),
        supp["supplier_id"].alias("target_supplier_id"),
        "revenue"
    )

supplied_by.write \
    .format("org.neo4j.spark.DataSource") \
    .mode("Overwrite") \
    .options(**NEO4J_OPTS) \
    .option("relationship", "SUPPLIED_BY") \
    .option("relationship.save.strategy", "keys") \
    .option("relationship.source.labels", ":ProductReport") \
    .option("relationship.source.node.keys", "source_product_id:product_id") \
    .option("relationship.target.labels", ":SupplierReport") \
    .option("relationship.target.node.keys", "target_supplier_id:supplier_id") \
    .save()

print(":SUPPLIED_BY relationships written")

print("All Neo4j report nodes and relationships written successfully.")
spark.stop()
