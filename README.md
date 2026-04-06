# Industry SQL data warehouse project 


Repository structure:
data-warehouse/
│
├── bronze/
│   └── bronze_load_sales_incremental.sql
│
├── silver/
│   └── silver_transform_sales.sql
│
├── gold/
│   ├── dim_customer_scd2.sql
│   ├── dim_product.sql
│   └── fact_sales_partitioned.sql
│
└── orchestration/
    └── run_pipeline.sql

Bronze layer (incremental ingestion).
The Bronze layer captures raw data exactly as it appears in the source system and stores ingestion metadata. Incremental loading prevents reloading the entire dataset.

/* ---------------------------------------------------------
BRONZE LAYER
Purpose: Incrementally ingest raw operational data
--------------------------------------------------------- */

CREATE SCHEMA bronze;
GO

CREATE TABLE bronze.sales_raw (
    sales_id        INT,
    product_id      INT,
    customer_id     INT,
    quantity        INT,
    total_sales     DECIMAL(18,2),
    price           DECIMAL(18,2),
    transaction_dt  DATETIME,
    source_system   VARCHAR(50),
    load_timestamp  DATETIME DEFAULT GETDATE()
);

-- Control table to track incremental loads
CREATE TABLE bronze.etl_watermark (
    table_name VARCHAR(100),
    last_load_datetime DATETIME
);

-- Incremental load
DECLARE @last_load DATETIME;

SELECT @last_load = last_load_datetime
FROM bronze.etl_watermark
WHERE table_name = 'sales';

INSERT INTO bronze.sales_raw
SELECT
    sales_id,
    product_id,
    customer_id,
    quantity,
    total_sales,
    price,
    transaction_date,
    'ERP'
FROM source_system.dbo.sales
WHERE transaction_date > ISNULL(@last_load, '1900-01-01');

-- Update watermark
MERGE bronze.etl_watermark AS t
USING (SELECT 'sales' AS table_name, GETDATE() AS last_load_datetime) s
ON t.table_name = s.table_name
WHEN MATCHED THEN
    UPDATE SET last_load_datetime = s.last_load_datetime
WHEN NOT MATCHED THEN
    INSERT VALUES(s.table_name, s.last_load_datetime);

Silver layer (standardization and cleansing).
This layer applies business rules and resolves data quality issues.
/* ---------------------------------------------------------
SILVER LAYER
Purpose: Clean and standardize data
--------------------------------------------------------- */

CREATE SCHEMA silver;
GO

CREATE TABLE silver.sales_clean (
    sales_id INT PRIMARY KEY,
    product_id INT,
    customer_id INT,
    quantity INT,
    revenue DECIMAL(18,2),
    unit_price DECIMAL(18,2),
    sales_date DATE,
    etl_timestamp DATETIME DEFAULT GETDATE()
);

INSERT INTO silver.sales_clean
SELECT
    sales_id,
    product_id,
    customer_id,

    CASE 
        WHEN quantity < 0 THEN 0
        ELSE quantity
    END,

    total_sales,

    CASE
        WHEN price IS NULL OR price <= 0
            THEN total_sales / NULLIF(quantity,0)
        ELSE price
    END,

    CAST(transaction_dt AS DATE)

FROM bronze.sales_raw;

Gold layer – dimension table with Slowly Changing Dimension Type 2.
SCD Type 2 tracks historical changes by creating new rows rather than overwriting records.

/* ---------------------------------------------------------
GOLD LAYER
Dimension: Customer (SCD Type 2)
--------------------------------------------------------- */

CREATE SCHEMA gold;
GO

CREATE TABLE gold.dim_customer (
    customer_sk INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT,
    customer_name VARCHAR(100),
    region VARCHAR(50),

    effective_start_date DATE,
    effective_end_date DATE,
    is_current BIT
);

MERGE gold.dim_customer AS target
USING staging_customer AS src
ON target.customer_id = src.customer_id
AND target.is_current = 1

WHEN MATCHED 
    AND (
        target.customer_name <> src.customer_name
        OR target.region <> src.region
    )

THEN
    UPDATE SET
        target.effective_end_date = GETDATE(),
        target.is_current = 0

WHEN NOT MATCHED BY TARGET
THEN
    INSERT (
        customer_id,
        customer_name,
        region,
        effective_start_date,
        effective_end_date,
        is_current
    )
    VALUES (
        src.customer_id,
        src.customer_name,
        src.region,
        GETDATE(),
        '9999-12-31',
        1
    );

  Product dimension with surrogate key.

  CREATE TABLE gold.dim_product (
    product_sk INT IDENTITY(1,1) PRIMARY KEY,
    product_id INT,
    product_name VARCHAR(100),
    category VARCHAR(100)
);

Partitioned fact table for scalable analytics.
Partitioning improves performance for large datasets.

/* ---------------------------------------------------------
FACT TABLE WITH PARTITIONING
--------------------------------------------------------- */

-- Partition function by year
CREATE PARTITION FUNCTION pf_sales_year (DATE)
AS RANGE RIGHT FOR VALUES
(
    '2022-01-01',
    '2023-01-01',
    '2024-01-01',
    '2025-01-01'
);

-- Partition scheme
CREATE PARTITION SCHEME ps_sales_year
AS PARTITION pf_sales_year
ALL TO ([PRIMARY]);

CREATE TABLE gold.fact_sales (
    sales_sk BIGINT IDENTITY(1,1) PRIMARY KEY,
    product_sk INT,
    customer_sk INT,
    quantity INT,
    revenue DECIMAL(18,2),
    sales_date DATE
)
ON ps_sales_year(sales_date);


Loading the fact table using joins from Silver and dimension tables.

INSERT INTO gold.fact_sales
(
    product_sk,
    customer_sk,
    quantity,
    revenue,
    sales_date
)

SELECT
    p.product_sk,
    c.customer_sk,
    s.quantity,
    s.revenue,
    s.sales_date

FROM silver.sales_clean s

JOIN gold.dim_product p
    ON s.product_id = p.product_id

JOIN gold.dim_customer c
    ON s.customer_id = c.customer_id
    AND c.is_current = 1;

Pipeline orchestration script (execution order).

EXEC bronze_load_sales_incremental;
EXEC silver_transform_sales;
EXEC load_customer_dimension;
EXEC load_product_dimension;
EXEC load_fact_sales;
