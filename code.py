-----------------------------------------------------------------------------------------------------------------------------------------------------------------
Load the data into data warehouse in Azure Blob using pyspark 

from pyspark.sql import SparkSession

spark = SparkSession.builder \.appName("AzureBlobLoad") \.getOrCreate()

storage_account_name = "<your_storage_account_name>"
storage_account_key = "<your_storage_account_key>"spark.conf.set(f"fs.azure.account.key.{storage_account_name}", storage_account_key)


container_name = "<your_container_name>"
hier_file_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/hier_table.gz"
fact_file_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/fact_table.gz"

hier_df = spark.read \
    .option("delimiter", "|") \
    .option("header", True) \
    .csv(hier_file_path)

fact_df = spark.read \
    .option("delimiter", "|") \
    .option("header", True) \
    .csv(fact_file_path)

# Validate data
hier_df.filter(col("id").isNull()).count()  # Check for null primary keys
hier_df.groupBy("id").count().filter("count > 1").count()  # Check for uniqueness
fact_df.join(hier_df, fact_df["hier_id"] == hier_df["id"], "left_anti").count()  # Check foreign key constraints

# Write data to Delta Lake
hier_df.write.format("delta").mode("overwrite").save("/mnt/delta/hier_table")
fact_df.write.format("delta").mode("overwrite").save("/mnt/delta/fact_table")

--------------------------------------------------------------------------------------------------------------------------------------------------------------------

performing data validation using pyspark 
from pyspark.sql.functions import col

# Check for non-null primary key
hier_df.filter(col("id").isNull()).count()  # Should be 0

# Check for uniqueness of primary key
hier_df.groupBy("id").count().filter("count > 1").count()  # Should be 0

# Check foreign key constraint
fact_df.join(hier_df, fact_df["hier_id"] == hier_df["id"], "left_anti").count()  # Should be 0

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE mview_weekly_sales AS
SELECT
    pos_site_id,
    sku_id,
    fsclwk_id,
    price_substate_id,
    type,
    SUM(sales_units) AS total_sales_units,
    SUM(sales_dollars) AS total_sales_dollars,
    SUM(discount_dollars) AS total_discount_dollars
FROM
    fact_sales
GROUP BY
    pos_site_id, sku_id, fsclwk_id, price_substate_id, type;


B.incremental calculation 
-- Step 1: Query new data
WITH new_data AS (
    SELECT
        pos_site_id,
        sku_id,
        fsclwk_id,
        price_substate_id,
        type,
        SUM(sales_units) AS total_sales_units,
        SUM(sales_dollars) AS total_sales_dollars,
        SUM(discount_dollars) AS total_discount_dollars
    FROM
        fact_sales
    WHERE
        last_updated > (SELECT last_processed_timestamp FROM metadata_last_processed WHERE table_name = 'fact_sales')
    GROUP BY
        pos_site_id, sku_id, fsclwk_id, price_substate_id, type
)

-- Step 2: Merge with existing data
INSERT INTO mview_weekly_sales (
    pos_site_id,
    sku_id,
    fsclwk_id,
    price_substate_id,
    type,
    total_sales_units,
    total_sales_dollars,
    total_discount_dollars
)
SELECT
    pos_site_id,
    sku_id,
    fsclwk_id,
    price_substate_id,
    type,
    total_sales_units,
    total_sales_dollars,
    total_discount_dollars
FROM
    new_data
ON CONFLICT (pos_site_id, sku_id, fsclwk_id, price_substate_id, type)
DO UPDATE SET
    total_sales_units = mview_weekly_sales.total_sales_units + EXCLUDED.total_sales_units,
    total_sales_dollars = mview_weekly_sales.total_sales_dollars + EXCLUDED.total_sales_dollars,
    total_discount_dollars = mview_weekly_sales.total_discount_dollars + EXCLUDED.total_discount_dollars;

-- Step 3: Update last processed timestamp
UPDATE metadata_last_processed
SET last_processed_timestamp = (SELECT MAX(last_updated) FROM fact_sales)
WHERE table_name = 'fact_sales';


----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------