# =============================================================================
# SCHEMA REGISTRY
# PROJECT : azure3_team7_project — Retail Supply Chain & Inventory
# PURPOSE : Single source of truth for all table schemas (bronze + silver)
#           Import this in every ingestion and transformation notebook.
# =============================================================================

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType,
    DecimalType, BooleanType, TimestampType, DateType, ArrayType
)

# =============================================================================
# BRONZE SCHEMAS
# Rule: nullable=True for ALL source fields (we accept whatever source sends)
#       Only the 3 audit columns are nullable=False (we always add those)
#       No business logic, no type strictness — that is Silver's job
# =============================================================================

# -----------------------------------------------------------------------------
# customers.csv | Daily | 100,000 rows
# Fields: customer_id, age_group, gender, zip_code, loyalty_tier,
#         first_purchase_date, total_spend, preferred_categories
# Note  : preferred_categories arrives as a JSON array string e.g. ["Books","Clothing"]
#         Keep as raw string in bronze — parse in silver
# -----------------------------------------------------------------------------
BRONZE_CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id",           StringType(),    nullable=True),
    StructField("age_group",             StringType(),    nullable=True),
    StructField("gender",                StringType(),    nullable=True),
    StructField("zip_code",              StringType(),    nullable=True),   # keep as string, never integer
    StructField("loyalty_tier",          StringType(),    nullable=True),
    StructField("first_purchase_date",   StringType(),    nullable=True),   # raw string → cast in silver
    StructField("total_spend",           DoubleType(),    nullable=True),
    StructField("preferred_categories",  StringType(),    nullable=True),   # raw JSON array string
    # --- audit columns (added by pipeline, not from source) ---
    StructField("source_file",           StringType(),    nullable=False),
    StructField("ingested_at",           TimestampType(), nullable=False),
    StructField("pipeline_run_id",       StringType(),    nullable=False),
    StructField("ingested_date",         StringType(), nullable=True),  # partition col, derived from ingested_at
])

# -----------------------------------------------------------------------------
# product_master.parquet | Weekly + incremental | SCD Type 2 | 50,000 rows
# Fields: product_id, product_name, category, subcategory, brand,
#         supplier_id, cost_price, selling_price, weight, length,
#         width, height, status, effective_date, expiry_date
# Note  : expiry_date is null for currently active records
#         SCD2 columns (scd_hash, valid_from, valid_to, is_current)
#         are computed and added in SILVER — not present in bronze
# -----------------------------------------------------------------------------
BRONZE_PRODUCT_MASTER_SCHEMA = StructType([
    StructField("product_id",      StringType(),    nullable=True),
    StructField("product_name",    StringType(),    nullable=True),
    StructField("category",        StringType(),    nullable=True),
    StructField("subcategory",     StringType(),    nullable=True),
    StructField("brand",           StringType(),    nullable=True),
    StructField("supplier_id",     StringType(),    nullable=True),
    StructField("cost_price",      DoubleType(),    nullable=True),
    StructField("selling_price",   DoubleType(),    nullable=True),
    StructField("weight",          DoubleType(),    nullable=True),
    StructField("dimensions",      StringType(),    nullable=True),
    StructField("status",          StringType(),    nullable=True),   # active / discontinued
    StructField("effective_date",  StringType(),    nullable=True),   # raw string → cast in silver
    StructField("expiry_date",     IntegerType(),   nullable=True),  # days since epoch, cast in silver
    # --- audit ---
    StructField("source_file",     StringType(),    nullable=False),
    StructField("ingested_at",     TimestampType(), nullable=False),
    StructField("pipeline_run_id", StringType(),    nullable=False),
])

# -----------------------------------------------------------------------------
# store_master.csv | Weekly + incremental | SCD Type 2 | 500 rows
# Fields: store_id, store_name, region, city, store_type, opening_date
# Note  : SCD2 columns (scd_hash, valid_from, valid_to, is_current)
#         are computed and added in SILVER — not present in bronze
# -----------------------------------------------------------------------------
BRONZE_STORE_MASTER_SCHEMA = StructType([
    StructField("store_id",        StringType(),    nullable=True),
    StructField("store_name",      StringType(),    nullable=True),
    StructField("region",          StringType(),    nullable=True),
    StructField("city",            StringType(),    nullable=True),
    StructField("store_type",      StringType(),    nullable=True),   # Warehouse / Superstore
    StructField("opening_date",    StringType(),    nullable=True),   # raw string → cast in silver
    # --- audit ---
    StructField("source_file",     StringType(),    nullable=False),
    StructField("ingested_at",     TimestampType(), nullable=False),
    StructField("pipeline_run_id", StringType(),    nullable=False),
])

# -----------------------------------------------------------------------------
# supplier_master.csv | Weekly + incremental | SCD Type 2 | 200 rows
# Fields: supplier_id, supplier_name, category, performance_rating,
#         on_time_delivery_pct
# Note  : SCD2 columns (scd_hash, valid_from, valid_to, is_current)
#         are computed and added in SILVER — not present in bronze
# -----------------------------------------------------------------------------
BRONZE_SUPPLIER_MASTER_SCHEMA = StructType([
    StructField("supplier_id",           StringType(),    nullable=True),
    StructField("supplier_name",         StringType(),    nullable=True),
    StructField("category",              StringType(),    nullable=True),
    StructField("performance_rating",    DoubleType(),    nullable=True),
    StructField("on_time_delivery_pct",  DoubleType(),    nullable=True),
    # --- audit ---
    StructField("source_file",           StringType(),    nullable=False),
    StructField("ingested_at",           TimestampType(), nullable=False),
    StructField("pipeline_run_id",       StringType(),    nullable=False),
])

# -----------------------------------------------------------------------------
# purchase_orders.csv | Daily + CDC | 100,000 rows
# Fields: po_id, supplier_id, product_id, order_date, expected_delivery_date,
#         actual_delivery_date, quantity_ordered, unit_cost, status,
#         quality_rating, delivery_notes
# Note  : delivery_notes is a NESTED JSON string:
#         {"carrier":"UPS","tracking_number":"TRACK831632",
#          "delivery_status":"delayed","notes":"Address issue"}
#         Keep raw in bronze — explode/flatten in silver
#         CDC means rows can be updates to existing POs (status changes)
# -----------------------------------------------------------------------------
BRONZE_PURCHASE_ORDERS_SCHEMA = StructType([
    StructField("po_id",                    StringType(),    nullable=True),
    StructField("supplier_id",              StringType(),    nullable=True),
    StructField("product_id",               StringType(),    nullable=True),
    StructField("order_date",               StringType(),    nullable=True),
    StructField("expected_delivery_date",   StringType(),    nullable=True),
    StructField("actual_delivery_date",     StringType(),    nullable=True),
    StructField("quantity_ordered",         IntegerType(),   nullable=True),
    StructField("unit_cost",                DoubleType(),    nullable=True),
    StructField("status",                   StringType(),    nullable=True),   # pending/delivered
    StructField("quality_rating",           DoubleType(),    nullable=True),   # nullable in source
    StructField("delivery_notes",           StringType(),    nullable=True),   # raw nested JSON string
    # --- audit ---
    StructField("source_file",              StringType(),    nullable=False),
    StructField("ingested_at",              TimestampType(), nullable=False),
    StructField("pipeline_run_id",          StringType(),    nullable=False),
    StructField("ingested_date",            StringType(), nullable=True),  # partition col, derived from ingested_at
])

# -----------------------------------------------------------------------------
# store_inventory_snapshots.jsonl | Every 15 mins | Kafka/JSONL
# Fields: store_id, product_id, current_quantity, last_restocked_date,
#         shelf_location, expiry_date, temperature_reading
# Note  : temperature_reading is a NESTED JSON string (when present):
#         {"sensor_id":"SENSOR_27","temperature_celsius":1.62,"humidity":61.64}
#         null when not a temperature-sensitive product
#         expiry_date null for non-perishables
#         Keep all raw in bronze — flatten in silver
# -----------------------------------------------------------------------------
BRONZE_STORE_INVENTORY_SCHEMA = StructType([
    StructField("store_id",             StringType(),    nullable=True),
    StructField("product_id",           StringType(),    nullable=True),
    StructField("current_quantity",     IntegerType(),   nullable=True),
    StructField("last_restocked_date",  StringType(),    nullable=True),   # raw string → cast in silver
    StructField("shelf_location",       StringType(),    nullable=True),
    StructField("expiry_date",          StringType(),    nullable=True),   # null for non-perishables
    StructField("temperature_reading",  StringType(),    nullable=True),   # raw nested JSON string
    # --- audit ---
    StructField("source_file",          StringType(),    nullable=False),
    StructField("ingested_at",          TimestampType(), nullable=False),
    StructField("pipeline_run_id",      StringType(),    nullable=False),
    StructField("ingested_date",        StringType(), nullable=True),  # partition col, derived from ingested_at
])

# -----------------------------------------------------------------------------
# pos_transactions_stream.jsonl | Real-time streaming
# pos_transactions_sample.csv   | Daily batch (same fields, different format)
# Fields: transaction_id, store_id, product_id, customer_id, timestamp,
#         quantity, unit_price, total_amount, payment_method, channel
# Note  : Both sources land in the SAME bronze table — bronze.pos_transactions
#         Distinguish source via source_file audit column
#         timestamp arrives as ISO 8601 string: "2023-08-19T22:26:11Z"
# -----------------------------------------------------------------------------
BRONZE_POS_TRANSACTIONS_SCHEMA = StructType([
    StructField("transaction_id",   StringType(),    nullable=True),
    StructField("store_id",         StringType(),    nullable=True),
    StructField("product_id",       StringType(),    nullable=True),
    StructField("customer_id",      StringType(),    nullable=True),
    StructField("timestamp",        StringType(),    nullable=True),   # ISO string → cast in silver
    StructField("quantity",         IntegerType(),   nullable=True),
    StructField("unit_price",       DoubleType(),    nullable=True),
    StructField("total_amount",     DoubleType(),    nullable=True),
    StructField("payment_method",   StringType(),    nullable=True),   # cash/credit_card/debit_card
    StructField("channel",          StringType(),    nullable=True),   # online/offline
    StructField("_source",          StringType(),    nullable=False),   # pos_realtime_stream/pos_batch_csv
    # --- audit ---
    StructField("source_file",      StringType(),    nullable=False),
    StructField("ingested_at",      TimestampType(), nullable=False),
    StructField("pipeline_run_id",  StringType(),    nullable=False),
    StructField("ingested_date",    StringType(), nullable=True),  # partition col, derived from ingested_at
])

# -----------------------------------------------------------------------------
# warehouse_inventory.parquet | Hourly
# Fields: warehouse_id, product_id, current_stock, reserved_stock,
#         available_stock, reorder_level, max_stock, last_updated, location_zone
# Note  : available_stock = current_stock - reserved_stock (derived, but keep in bronze as-is)
#         last_updated arrives as timestamp from source
# -----------------------------------------------------------------------------
BRONZE_WAREHOUSE_INVENTORY_SCHEMA = StructType([
    StructField("warehouse_id",     StringType(),    nullable=True),
    StructField("product_id",       StringType(),    nullable=True),
    StructField("current_stock",    LongType(),   nullable=True),
    StructField("reserved_stock",   LongType(),   nullable=True),
    StructField("available_stock",  LongType(),   nullable=True),
    StructField("reorder_level",    LongType(),   nullable=True),
    StructField("max_stock",        LongType(),   nullable=True),
    StructField("last_updated",     StringType(), nullable=True),
    StructField("location_zone",    StringType(),    nullable=True),
    # --- audit ---
    StructField("source_file",      StringType(),    nullable=False),
    StructField("ingested_at",      TimestampType(), nullable=False),
    StructField("pipeline_run_id",  StringType(),    nullable=False),
    StructField("ingested_date",    StringType(), nullable=True),  # partition col, derived from ingested_at
])


# =============================================================================
# SILVER SCHEMAS
# Rule: nullable=False on all key/critical columns — enforced here
#       Proper types cast (no raw strings for dates/timestamps)
#       Nested JSON fields exploded into proper columns
#       3 audit columns become 4: +processed_at, +source_system
#       SCD2 columns added for product_master only
# =============================================================================

# -----------------------------------------------------------------------------
# silver.customers
# Changes from bronze:
#   - first_purchase_date: String → DateType
#   - total_spend: Double → Decimal(10,2) for precision
#   - preferred_categories: raw JSON string → ArrayType(StringType)
#   - nullable=False enforced on key columns
# -----------------------------------------------------------------------------
SILVER_CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id",           StringType(),         nullable=False),
    StructField("age_group",             StringType(),         nullable=True),
    StructField("gender",                StringType(),         nullable=True),
    StructField("zip_code",              StringType(),         nullable=True),
    StructField("loyalty_tier",          StringType(),         nullable=True),
    StructField("first_purchase_date",   DateType(),           nullable=True),
    StructField("total_spend",           DecimalType(10, 2),   nullable=True),
    StructField("preferred_categories",  ArrayType(StringType()), nullable=True),  # parsed from JSON
    # --- audit ---
    StructField("ingested_at",           TimestampType(),      nullable=False),
    StructField("processed_at",          TimestampType(),      nullable=False),
    StructField("pipeline_run_id",       StringType(),         nullable=False),
    StructField("source_system",         StringType(),         nullable=False),
])

# -----------------------------------------------------------------------------
# silver.product_master  — SCD Type 2
# Changes from bronze:
#   - effective_date / expiry_date: String → DateType
#   - cost_price / selling_price: Double → Decimal(10,2)
#   - SCD2 columns added: scd_hash, valid_from, valid_to, is_current
#   - scd_hash: MD5 of all tracked business columns — used to detect changes
# -----------------------------------------------------------------------------
SILVER_PRODUCT_MASTER_SCHEMA = StructType([
    StructField("product_id",      StringType(),         nullable=False),
    StructField("product_name",    StringType(),         nullable=True),
    StructField("category",        StringType(),         nullable=True),
    StructField("subcategory",     StringType(),         nullable=True),
    StructField("brand",           StringType(),         nullable=True),
    StructField("supplier_id",     StringType(),         nullable=True),
    StructField("cost_price",      DecimalType(10, 2),   nullable=True),
    StructField("selling_price",   DecimalType(10, 2),   nullable=True),
    StructField("weight",          DoubleType(),         nullable=True),
    StructField("length",          DoubleType(),         nullable=True),
    StructField("width",           DoubleType(),         nullable=True),
    StructField("height",          DoubleType(),         nullable=True),
    StructField("status",          StringType(),         nullable=True),
    StructField("effective_date",  DateType(),           nullable=True),
    StructField("expiry_date",     DateType(),           nullable=True),
    # --- SCD2 columns ---
    StructField("scd_hash",        StringType(),         nullable=False),  # MD5 of business cols
    StructField("valid_from",      TimestampType(),      nullable=False),  # when this version became active
    StructField("valid_to",        TimestampType(),      nullable=True),   # null = current active record
    StructField("is_current",      BooleanType(),        nullable=False),  # True = latest version
    # --- audit ---
    StructField("ingested_at",     TimestampType(),      nullable=False),
    StructField("processed_at",    TimestampType(),      nullable=False),
    StructField("pipeline_run_id", StringType(),         nullable=False),
    StructField("source_system",   StringType(),         nullable=False),
])

# -----------------------------------------------------------------------------
# silver.store_master
# Changes from bronze:
#   - opening_date: String → DateType
#   - SCD Type 2 — weekly + incremental, same approach as product_master
# -----------------------------------------------------------------------------
SILVER_STORE_MASTER_SCHEMA = StructType([
    StructField("store_id",        StringType(),    nullable=False),
    StructField("store_name",      StringType(),    nullable=True),
    StructField("region",          StringType(),    nullable=True),
    StructField("city",            StringType(),    nullable=True),
    StructField("store_type",      StringType(),    nullable=True),
    StructField("opening_date",    DateType(),      nullable=True),
    # --- SCD2 columns ---
    StructField("scd_hash",        StringType(),    nullable=False),
    StructField("valid_from",      TimestampType(), nullable=False),
    StructField("valid_to",        TimestampType(), nullable=True),
    StructField("is_current",      BooleanType(),   nullable=False),
    # --- audit ---
    StructField("ingested_at",     TimestampType(), nullable=False),
    StructField("processed_at",    TimestampType(), nullable=False),
    StructField("pipeline_run_id", StringType(),    nullable=False),
    StructField("source_system",   StringType(),    nullable=False),
])

# -----------------------------------------------------------------------------
# silver.supplier_master
# Changes from bronze:
#   - performance_rating / on_time_delivery_pct: Double → Decimal for precision
#   - SCD Type 2 — weekly + incremental, same approach as product_master
# -----------------------------------------------------------------------------
SILVER_SUPPLIER_MASTER_SCHEMA = StructType([
    StructField("supplier_id",           StringType(),        nullable=False),
    StructField("supplier_name",         StringType(),        nullable=True),
    StructField("category",              StringType(),        nullable=True),
    StructField("performance_rating",    DecimalType(4, 2),   nullable=True),
    StructField("on_time_delivery_pct",  DecimalType(5, 2),   nullable=True),
    # --- SCD2 columns ---
    StructField("scd_hash",              StringType(),        nullable=False),
    StructField("valid_from",            TimestampType(),     nullable=False),
    StructField("valid_to",              TimestampType(),     nullable=True),
    StructField("is_current",            BooleanType(),       nullable=False),
    # --- audit ---
    StructField("ingested_at",           TimestampType(),     nullable=False),
    StructField("processed_at",          TimestampType(),     nullable=False),
    StructField("pipeline_run_id",       StringType(),        nullable=False),
    StructField("source_system",         StringType(),        nullable=False),
])

# -----------------------------------------------------------------------------
# silver.purchase_orders
# Changes from bronze:
#   - All date fields: String → DateType
#   - unit_cost: Double → Decimal(10,2)
#   - delivery_notes (nested JSON) exploded into 4 flat columns:
#       carrier, tracking_number, delivery_status, delivery_notes_text
#   - CDC handled via MERGE on po_id — status updates handled correctly
# -----------------------------------------------------------------------------
SILVER_PURCHASE_ORDERS_SCHEMA = StructType([
    StructField("po_id",                    StringType(),        nullable=False),
    StructField("supplier_id",              StringType(),        nullable=False),
    StructField("product_id",               StringType(),        nullable=False),
    StructField("order_date",               DateType(),          nullable=True),
    StructField("expected_delivery_date",   DateType(),          nullable=True),
    StructField("actual_delivery_date",     DateType(),          nullable=True),
    StructField("quantity_ordered",         IntegerType(),       nullable=True),
    StructField("unit_cost",                DecimalType(10, 2),  nullable=True),
    StructField("status",                   StringType(),        nullable=True),
    StructField("quality_rating",           DecimalType(4, 2),   nullable=True),
    StructField("order_year_month",         StringType(),        nullable=True),  # partition col, derived from order_date
    # --- flattened from delivery_notes nested JSON ---
    StructField("carrier",                  StringType(),        nullable=True),
    StructField("tracking_number",          StringType(),        nullable=True),
    StructField("delivery_status",          StringType(),        nullable=True),
    StructField("delivery_notes_text",      StringType(),        nullable=True),
    # --- audit ---
    StructField("ingested_at",              TimestampType(),     nullable=False),
    StructField("processed_at",             TimestampType(),     nullable=False),
    StructField("pipeline_run_id",          StringType(),        nullable=False),
    StructField("source_system",            StringType(),        nullable=False),
])

# -----------------------------------------------------------------------------
# silver.store_inventory
# Changes from bronze:
#   - last_restocked_date / expiry_date: String → DateType
#   - temperature_reading (nested JSON) exploded into 3 flat columns:
#       sensor_id, temperature_celsius, humidity
#   - nullable=False on key columns
# -----------------------------------------------------------------------------
SILVER_STORE_INVENTORY_SCHEMA = StructType([
    StructField("store_id",             StringType(),        nullable=False),
    StructField("product_id",           StringType(),        nullable=False),
    StructField("current_quantity",     IntegerType(),       nullable=False),
    StructField("last_restocked_date",  DateType(),          nullable=True),
    StructField("shelf_location",       StringType(),        nullable=True),
    StructField("expiry_date",          DateType(),          nullable=True),
    # --- flattened from temperature_reading nested JSON ---
    StructField("sensor_id",            StringType(),        nullable=True),   # null if not temp-sensitive
    StructField("temperature_celsius",  DoubleType(),        nullable=True),
    StructField("humidity",             DoubleType(),        nullable=True),
    # --- audit ---
    StructField("ingested_at",          TimestampType(),     nullable=False),
    StructField("processed_at",         TimestampType(),     nullable=False),
    StructField("pipeline_run_id",      StringType(),        nullable=False),
    StructField("source_system",        StringType(),        nullable=False),
])

# -----------------------------------------------------------------------------
# silver.pos_transactions
# Changes from bronze:
#   - timestamp: ISO string → TimestampType
#   - unit_price / total_amount: Double → Decimal(10,2)
#   - Deduplication on transaction_id (stream + batch may overlap)
#   - nullable=False on all key business columns
# -----------------------------------------------------------------------------
SILVER_POS_TRANSACTIONS_SCHEMA = StructType([
    StructField("transaction_id",   StringType(),        nullable=False),
    StructField("store_id",         StringType(),        nullable=False),
    StructField("product_id",       StringType(),        nullable=False),
    StructField("customer_id",      StringType(),        nullable=True),   # can be anonymous
    StructField("transaction_ts",   TimestampType(),     nullable=False),  # renamed from timestamp
    StructField("quantity",         IntegerType(),       nullable=False),
    StructField("unit_price",       DecimalType(10, 2),  nullable=False),
    StructField("total_amount",     DecimalType(10, 2),  nullable=False),
    StructField("payment_method",   StringType(),        nullable=True),
    StructField("channel",          StringType(),        nullable=True),
    StructField("transaction_date", StringType(),        nullable=True),  # derived from transaction_ts
    # --- audit ---
    StructField("ingested_at",      TimestampType(),     nullable=False),
    StructField("processed_at",     TimestampType(),     nullable=False),
    StructField("pipeline_run_id",  StringType(),        nullable=False),
    StructField("source_system",    StringType(),        nullable=False),
])

# -----------------------------------------------------------------------------
# silver.warehouse_inventory
# Changes from bronze:
#   - available_stock recomputed (don't trust source derivation)
#   - stock columns: Integer → LongType for large warehouses
#   - nullable=False on key columns
# -----------------------------------------------------------------------------
SILVER_WAREHOUSE_INVENTORY_SCHEMA = StructType([
    StructField("warehouse_id",     StringType(),    nullable=False),
    StructField("product_id",       StringType(),    nullable=False),
    StructField("current_stock",    LongType(),      nullable=False),
    StructField("reserved_stock",   LongType(),      nullable=False),
    StructField("available_stock",  LongType(),      nullable=False),  # recomputed: current - reserved
    StructField("reorder_level",    LongType(),      nullable=True),
    StructField("max_stock",        LongType(),      nullable=True),
    StructField("last_updated",     TimestampType(), nullable=True),
    StructField("location_zone",    StringType(),    nullable=True),
    # --- audit ---
    StructField("ingested_at",      TimestampType(), nullable=False),
    StructField("processed_at",     TimestampType(), nullable=False),
    StructField("pipeline_run_id",  StringType(),    nullable=False),
    StructField("source_system",    StringType(),    nullable=False),
])