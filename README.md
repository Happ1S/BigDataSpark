# BigDataSpark

Лабораторная работа №2: ETL на Apache Spark (модель «звезда» в PostgreSQL и витрины в ClickHouse, бонус — Neo4j).

## Требования

- Docker и Docker Compose
- ~6 ГБ свободной RAM для контейнеров

## Структура проекта

```
BigDataSpark/
├── исходные данные/          # 10 CSV-файлов (10 000 строк)
├── docker-compose.yml
├── init/
│   └── 01_init.sh            # таблица mock_data и загрузка CSV в PostgreSQL
├── clickhouse-init/
│   └── 01_create_tables.sql  # 6 таблиц отчётов в ClickHouse
├── jobs/
│   ├── etl_star_schema.py    # mock_data → звёздная схема в PostgreSQL
│   ├── etl_clickhouse.py     # звёздная схема → 6 отчётов в ClickHouse
│   └── etl_neo4j.py          # звёздная схема → 6 меток узлов в Neo4j (бонус)
└── README.md
```

## Шаг 1 — Запуск инфраструктуры

```bash
docker compose up -d
```

Подождите ~30 секунд, пока инициализируются PostgreSQL, ClickHouse и Neo4j.  
Контейнер `clickhouse-init` автоматически создаст 6 таблиц отчётов в ClickHouse.

Проверка контейнеров:

```bash
docker compose ps
```

Проверка загрузки сырых данных (ожидается `10000`):

```bash
docker exec postgres psql -U spark -d bigdata -c "SELECT COUNT(*) FROM mock_data;"
```

Проверка таблиц в ClickHouse (ожидается 6 таблиц):

```bash
docker exec clickhouse clickhouse-client --query "SHOW TABLES"
```

## Шаг 2 — Spark Job 1: построение звёздной схемы в PostgreSQL

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.7.3 \
  /opt/spark/work-dir/etl_star_schema.py
```

Создаются таблицы:

- `dim_customer`, `dim_seller`, `dim_product`
- `dim_store`, `dim_supplier`, `dim_date`
- `fact_sales`

Проверка:

```bash
docker exec postgres psql -U spark -d bigdata -c "\dt"
docker exec postgres psql -U spark -d bigdata -c "SELECT COUNT(*) FROM fact_sales;"
```

Ожидается `10000` строк в `fact_sales`.

## Шаг 3 — Spark Job 2: отчёты в ClickHouse (обязательно)

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.3 \
  /opt/spark/work-dir/etl_clickhouse.py
```

Заполняются 6 таблиц:

| Таблица | Витрина по ТЗ |
|---------|----------------|
| `report_products` | продажи по продуктам |
| `report_customers` | продажи по клиентам |
| `report_time` | продажи по времени |
| `report_stores` | продажи по магазинам |
| `report_suppliers` | продажи по поставщикам |
| `report_quality` | качество продукции |

Проверка (DBeaver: `localhost:8123`, драйвер ClickHouse) или из терминала:

```bash
docker exec clickhouse clickhouse-client --query "SELECT COUNT(*) FROM report_products;"
docker exec clickhouse clickhouse-client --query "SELECT * FROM report_products ORDER BY sales_rank LIMIT 10;"
```

Примеры запросов по витринам:

```sql
-- Топ-10 продуктов по количеству продаж
SELECT product_name, total_quantity, total_revenue
FROM report_products ORDER BY sales_rank LIMIT 10;

-- Выручка по категориям
SELECT category, sum(total_revenue) AS revenue
FROM report_products GROUP BY category ORDER BY revenue DESC;

-- Топ-10 клиентов
SELECT customer_name, country, total_spent
FROM report_customers ORDER BY customer_rank LIMIT 10;

-- Тренд по месяцам
SELECT year, month, total_revenue, order_count
FROM report_time ORDER BY year, month;

-- Топ-5 магазинов
SELECT store_name, city, country, total_revenue
FROM report_stores ORDER BY revenue_rank LIMIT 5;

-- Топ-5 поставщиков
SELECT supplier_name, country, total_revenue
FROM report_suppliers ORDER BY revenue_rank LIMIT 5;

-- Продукты с лучшим рейтингом
SELECT product_name, avg_rating, review_count, total_sales
FROM report_quality ORDER BY rating_rank LIMIT 10;
```

## Шаг 4 — Spark Job 3: отчёты в Neo4j (бонус)

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.7.3,org.neo4j:neo4j-connector-apache-spark_2.12:5.3.2_for_spark_3 \
  /opt/spark/work-dir/etl_neo4j.py
```

Создаются метки узлов: `:ProductReport`, `:CustomerReport`, `:TimeReport`, `:StoreReport`, `:SupplierReport`, `:QualityReport`, а также связи `:SOLD_IN` и `:SUPPLIED_BY`.

Neo4j Browser: [http://localhost:7474](http://localhost:7474) (логин `neo4j` / `neo4j123`).

```cypher
MATCH (n) RETURN labels(n)[0] AS label, count(n) AS count ORDER BY label;

MATCH (p:ProductReport)
RETURN p.product_name, p.total_quantity, p.total_revenue
ORDER BY p.sales_rank ASC LIMIT 10;

MATCH (p:ProductReport)-[r:SOLD_IN]->(s:StoreReport)
RETURN p.product_name, s.store_name, r.revenue
ORDER BY r.revenue DESC LIMIT 10;
```

## Подключение через DBeaver

| БД | Host | Port | User | Password | Database |
|----|------|------|------|----------|----------|
| PostgreSQL | localhost | 5432 | spark | spark123 | bigdata |
| ClickHouse | localhost | 8123 | — | — | default |

Neo4j Browser: http://localhost:7474

## Остановка

```bash
docker compose down
```

С удалением данных:

```bash
docker compose down -v
```
