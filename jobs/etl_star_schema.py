from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("ETL: Raw -> Star Schema") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

PG_URL = "jdbc:postgresql://postgres:5432/bigdata"
PG_PROPS = {
    "user": "spark",
    "password": "spark123",
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(PG_URL, "mock_data", properties=PG_PROPS)
df.cache()

# ── dim_customer ──────────────────────────────────────────────────────────────
dim_customer = df.select(
    F.col("sale_customer_id").alias("customer_id"),
    F.col("customer_first_name").alias("first_name"),
    F.col("customer_last_name").alias("last_name"),
    F.col("customer_age").alias("age"),
    F.col("customer_email").alias("email"),
    F.col("customer_country").alias("country"),
    F.col("customer_postal_code").alias("postal_code"),
    F.col("customer_pet_type").alias("pet_type"),
    F.col("customer_pet_name").alias("pet_name"),
    F.col("customer_pet_breed").alias("pet_breed")
).dropDuplicates(["customer_id"])

dim_customer.write.jdbc(PG_URL, "dim_customer", mode="overwrite", properties=PG_PROPS)
print(f"dim_customer: {dim_customer.count()} rows")

# ── dim_seller ────────────────────────────────────────────────────────────────
dim_seller = df.select(
    F.col("sale_seller_id").alias("seller_id"),
    F.col("seller_first_name").alias("first_name"),
    F.col("seller_last_name").alias("last_name"),
    F.col("seller_email").alias("email"),
    F.col("seller_country").alias("country"),
    F.col("seller_postal_code").alias("postal_code")
).dropDuplicates(["seller_id"])

dim_seller.write.jdbc(PG_URL, "dim_seller", mode="overwrite", properties=PG_PROPS)
print(f"dim_seller: {dim_seller.count()} rows")

# ── dim_product ───────────────────────────────────────────────────────────────
dim_product = df.select(
    F.col("sale_product_id").alias("product_id"),
    F.col("product_name").alias("name"),
    F.col("product_category").alias("category"),
    F.col("product_price").alias("price"),
    F.col("product_quantity").alias("stock_quantity"),
    F.col("product_weight").alias("weight"),
    F.col("product_color").alias("color"),
    F.col("product_size").alias("size"),
    F.col("product_brand").alias("brand"),
    F.col("product_material").alias("material"),
    F.col("product_description").alias("description"),
    F.col("product_rating").alias("rating"),
    F.col("product_reviews").alias("reviews"),
    F.col("product_release_date").alias("release_date"),
    F.col("product_expiry_date").alias("expiry_date"),
    F.col("pet_category")
).dropDuplicates(["product_id"])

dim_product.write.jdbc(PG_URL, "dim_product", mode="overwrite", properties=PG_PROPS)
print(f"dim_product: {dim_product.count()} rows")

# ── dim_store ─────────────────────────────────────────────────────────────────
store_window = Window.orderBy("store_name")

dim_store = df.select(
    "store_name", "store_location", "store_city",
    "store_state", "store_country", "store_phone", "store_email"
).dropDuplicates(["store_name"]) \
 .withColumn("store_id", F.row_number().over(store_window)) \
 .select(
     "store_id",
     F.col("store_name").alias("name"),
     F.col("store_location").alias("location"),
     F.col("store_city").alias("city"),
     F.col("store_state").alias("state"),
     F.col("store_country").alias("country"),
     F.col("store_phone").alias("phone"),
     F.col("store_email").alias("email")
 )

dim_store.write.jdbc(PG_URL, "dim_store", mode="overwrite", properties=PG_PROPS)
print(f"dim_store: {dim_store.count()} rows")

# ── dim_supplier ──────────────────────────────────────────────────────────────
supplier_window = Window.orderBy("supplier_name")

dim_supplier = df.select(
    "supplier_name", "supplier_contact", "supplier_email",
    "supplier_phone", "supplier_address", "supplier_city", "supplier_country"
).dropDuplicates(["supplier_name"]) \
 .withColumn("supplier_id", F.row_number().over(supplier_window)) \
 .select(
     "supplier_id",
     F.col("supplier_name").alias("name"),
     F.col("supplier_contact").alias("contact"),
     F.col("supplier_email").alias("email"),
     F.col("supplier_phone").alias("phone"),
     F.col("supplier_address").alias("address"),
     F.col("supplier_city").alias("city"),
     F.col("supplier_country").alias("country")
 )

dim_supplier.write.jdbc(PG_URL, "dim_supplier", mode="overwrite", properties=PG_PROPS)
print(f"dim_supplier: {dim_supplier.count()} rows")

# ── dim_date ──────────────────────────────────────────────────────────────────
dim_date = df.select("sale_date").distinct() \
    .withColumn("date_parsed", F.to_date(F.col("sale_date"), "M/d/yyyy")) \
    .withColumn("date_id", F.date_format("date_parsed", "yyyyMMdd").cast("int")) \
    .select(
        "date_id",
        F.col("sale_date").alias("full_date"),
        F.col("date_parsed").alias("date"),
        F.dayofmonth("date_parsed").alias("day"),
        F.month("date_parsed").alias("month"),
        F.quarter("date_parsed").alias("quarter"),
        F.year("date_parsed").alias("year")
    ).dropDuplicates(["date_id"])

dim_date.write.jdbc(PG_URL, "dim_date", mode="overwrite", properties=PG_PROPS)
print(f"dim_date: {dim_date.count()} rows")

# ── fact_sales ────────────────────────────────────────────────────────────────
df_with_date = df.withColumn(
    "date_id",
    F.date_format(F.to_date(F.col("sale_date"), "M/d/yyyy"), "yyyyMMdd").cast("int")
)

df_with_store = df_with_date.join(
    dim_store.select(F.col("store_id"), F.col("name").alias("_store_name")),
    df_with_date["store_name"] == F.col("_store_name"),
    "left"
).drop("_store_name")

df_with_supplier = df_with_store.join(
    dim_supplier.select(F.col("supplier_id"), F.col("name").alias("_supplier_name")),
    df_with_store["supplier_name"] == F.col("_supplier_name"),
    "left"
).drop("_supplier_name")

fact_sales = df_with_supplier.select(
    F.col("id").alias("sale_id"),
    F.col("sale_customer_id").alias("customer_id"),
    F.col("sale_seller_id").alias("seller_id"),
    F.col("sale_product_id").alias("product_id"),
    F.col("store_id"),
    F.col("supplier_id"),
    F.col("date_id"),
    F.col("sale_quantity").alias("quantity"),
    F.col("sale_total_price").alias("total_price")
)

fact_sales.write.jdbc(PG_URL, "fact_sales", mode="overwrite", properties=PG_PROPS)
print(f"fact_sales: {fact_sales.count()} rows")

print("Star schema built successfully.")
spark.stop()
