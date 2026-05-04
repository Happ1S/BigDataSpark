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
ORDER BY (category, total_revenue);

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
ORDER BY total_spent;

CREATE TABLE IF NOT EXISTS report_time
(
    year            Int32,
    month           Int32,
    total_revenue   Float64,
    order_count     Int64,
    avg_order_size  Float64
)
ENGINE = MergeTree()
ORDER BY (year, month);

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
ORDER BY total_revenue;

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
ORDER BY total_revenue;

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
ORDER BY avg_rating;
