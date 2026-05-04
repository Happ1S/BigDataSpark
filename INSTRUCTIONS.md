# BigDataSpark — Lab #2: ETL with Apache Spark

## Prerequisites

- Docker + Docker Compose
- 6 GB RAM available for containers

---

## Project Structure

```
BigDataSpark/
├── исходные данные/          # 10 source CSV files (10 000 rows total)
├── docker-compose.yml
├── init/
│   └── 01_init.sh            # Creates mock_data table and loads all CSVs into PostgreSQL
├── clickhouse-init/
│   └── 01_create_tables.sql  # Creates 6 report tables in ClickHouse (run by clickhouse-init container)
├── jobs/
│   ├── etl_star_schema.py    # Raw -> Star Schema in PostgreSQL
│   ├── etl_clickhouse.py     # Star Schema -> 6 reports in ClickHouse
│   └── etl_neo4j.py          # Star Schema -> 6 report node labels in Neo4j
└── INSTRUCTIONS.md
```

---

## Step 1 — Start the infrastructure

```bash
docker-compose up -d
```

Wait ~30 seconds for PostgreSQL, ClickHouse, and Neo4j to initialize.
The `clickhouse-init` container will automatically create the 6 report tables in ClickHouse.

Verify all containers are running:
```bash
docker-compose ps
```

Verify mock_data loaded (should return 10000):
```bash
docker exec postgres psql -U spark -d bigdata -c "SELECT COUNT(*) FROM mock_data;"
```

Verify ClickHouse tables created (should return 6):
```bash
docker exec clickhouse clickhouse-client --query "SHOW TABLES"
```

---

## Step 2 — Run Spark Job 1: Build Star Schema in PostgreSQL

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.7.3 \
  /opt/spark/work-dir/etl_star_schema.py
```

This creates the following tables in PostgreSQL:
- `dim_customer`, `dim_seller`, `dim_product`
- `dim_store`, `dim_supplier`, `dim_date`
- `fact_sales`

Verify:
```bash
docker exec postgres psql -U spark -d bigdata -c "\dt"
docker exec postgres psql -U spark -d bigdata -c "SELECT COUNT(*) FROM fact_sales;"
```

---

## Step 3 — Run Spark Job 2: Reports in ClickHouse (required)

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.3:all \
  /opt/spark/work-dir/etl_clickhouse.py
```

This writes 6 report tables into ClickHouse:
`report_products`, `report_customers`, `report_time`,
`report_stores`, `report_suppliers`, `report_quality`

Verify via DBeaver (connect to `localhost:8123`, driver ClickHouse) or:
```bash
docker exec clickhouse clickhouse-client --query "SELECT COUNT(*) FROM report_products;"
docker exec clickhouse clickhouse-client --query "SELECT * FROM report_products ORDER BY sales_rank LIMIT 10;"
```

---

## Step 4 — Run Spark Job 3: Reports in Neo4j (bonus)

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.7.3,org.neo4j:neo4j-connector-apache-spark_2.12:5.3.2_for_spark_3 \
  /opt/spark/work-dir/etl_neo4j.py
```

This creates 6 node labels in Neo4j:
`:ProductReport`, `:CustomerReport`, `:TimeReport`,
`:StoreReport`, `:SupplierReport`, `:QualityReport`

Plus relationships: `:SOLD_IN`, `:SUPPLIED_BY`

Verify via Neo4j Browser at **http://localhost:7474** (login: `neo4j` / `neo4j123`):

```cypher
// Count all report nodes
MATCH (n) RETURN labels(n)[0] AS label, COUNT(n) AS count ORDER BY label;

// Top-10 products by sales
MATCH (p:ProductReport)
RETURN p.product_name, p.total_quantity, p.total_revenue
ORDER BY p.sales_rank ASC LIMIT 10;

// Top-10 customers by spend
MATCH (c:CustomerReport)
RETURN c.customer_name, c.country, c.total_spent
ORDER BY c.customer_rank ASC LIMIT 10;

// Monthly revenue trend
MATCH (t:TimeReport)
RETURN t.year, t.month, t.total_revenue, t.order_count
ORDER BY t.year, t.month;

// Top-5 stores
MATCH (s:StoreReport)
RETURN s.store_name, s.city, s.country, s.total_revenue
ORDER BY s.revenue_rank ASC LIMIT 5;

// Top-5 suppliers
MATCH (s:SupplierReport)
RETURN s.supplier_name, s.country, s.total_revenue
ORDER BY s.revenue_rank ASC LIMIT 5;

// Products by quality (highest rated)
MATCH (q:QualityReport)
RETURN q.product_name, q.avg_rating, q.review_count, q.total_sales
ORDER BY q.rating_rank ASC LIMIT 10;

// Graph: which products are sold in which stores
MATCH (p:ProductReport)-[r:SOLD_IN]->(s:StoreReport)
RETURN p.product_name, s.store_name, r.revenue
ORDER BY r.revenue DESC LIMIT 10;
```

---

## Connections for DBeaver

| Database    | Host      | Port | User  | Password | Database |
|-------------|-----------|------|-------|----------|----------|
| PostgreSQL  | localhost | 5432 | spark | spark123 | bigdata  |
| ClickHouse  | localhost | 8123 | —     | —        | default  |

Neo4j Browser: http://localhost:7474

---

## Stop the infrastructure

```bash
docker-compose down
```

To also remove all data volumes:
```bash
docker-compose down -v
```
