import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

# ----------------------------------------
# 1. Load environment variables
# ----------------------------------------
load_dotenv()

# ----------------------------------------
# 2. File to table mapping with table types
# ----------------------------------------
FILES = {
    "olist_orders_dataset.csv"              : "raw_orders",
    "olist_customers_dataset.csv"           : "raw_customers",
    "olist_order_items_dataset.csv"         : "raw_order_items",
    "olist_order_payments_dataset.csv"      : "raw_payments",
    "olist_order_reviews_dataset.csv"       : "raw_reviews",
    "olist_products_dataset.csv"            : "raw_products",
    "olist_sellers_dataset.csv"             : "raw_sellers",
    "olist_geolocation_dataset.csv"         : "raw_geolocation",
    "product_category_name_translation.csv" : "raw_category_translation"
}


# RAW_DATA_PATH = "C:\\Users\\zinga\\DEV\\ecommerce-pipeline\\raw_data"
RAW_DATA_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "raw_data")
STAGE_NAME    = "ECOMMERCE_DB.RAW.ECOMMERCE_STAGE"

# ----------------------------------------
# 3. Connect to Snowflake
# ----------------------------------------
def get_snowflake_connection():
    conn = snowflake.connector.connect(
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
        database  = os.getenv("SNOWFLAKE_DATABASE"),
        schema    = os.getenv("SNOWFLAKE_SCHEMA"),
        role      = os.getenv("SNOWFLAKE_ROLE")
    )
    return conn

# ----------------------------------------
# 4. Create TRANSIENT table dynamically
#    from CSV columns
# ----------------------------------------
def create_transient_table(cursor, table_name, df):
    columns = ", ".join([f"{col} VARCHAR" for col in df.columns])
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    # TRANSIENT = no fail safe, saves cost for raw layer
    cursor.execute(f"""
        CREATE TRANSIENT TABLE {table_name} ({columns})
        DATA_RETENTION_TIME_IN_DAYS = 1
    """)
    print(f"  ✅ Transient table {table_name} created")

# ----------------------------------------
# 5. Upload CSV to internal stage
# ----------------------------------------
def upload_to_stage(conn, filepath, filename):
    cursor = conn.cursor()
    print(f"  Uploading {filename} to stage...")
    # PUT command uploads local file to Snowflake internal stage
    cursor.execute(f"""
        PUT file://{os.path.abspath(filepath)} 
        @{STAGE_NAME} 
        AUTO_COMPRESS=TRUE 
        OVERWRITE=TRUE
    """)
    print(f"  ✅ {filename} uploaded to stage")
    cursor.close()

# ----------------------------------------
# 6. COPY INTO table from stage
#    This is the fast, production way to load
# ----------------------------------------
def copy_into_table(cursor, table_name, filename):
    # Snowflake compresses files with .gz extension automatically
    staged_file = f"@{STAGE_NAME}/{filename}.gz"
    cursor.execute(f"""
        COPY INTO {table_name}
        FROM {staged_file}
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            NULL_IF = ('', 'NULL', 'null', 'None')
            EMPTY_FIELD_AS_NULL = TRUE
            DATE_FORMAT = 'AUTO'
            TIMESTAMP_FORMAT = 'AUTO'
        )
        ON_ERROR = 'CONTINUE'
    """)
    print(f"  ✅ Data copied into {table_name}")

# ----------------------------------------
# 7. Clean up stage after loading
#    Good practice — don't leave files sitting
# ----------------------------------------
def clean_stage(cursor, filename):
    cursor.execute(f"REMOVE @{STAGE_NAME}/{filename}.gz")
    print(f"  ✅ Stage cleaned for {filename}")

# ----------------------------------------
# 8. Main ingestion function per file
# ----------------------------------------
def ingest_file(conn, filename, table_name):
    filepath = os.path.join(RAW_DATA_PATH, filename)

    print(f"\n📂 Processing {filename}...")

    # Read CSV to get column names
    df = pd.read_csv(filepath, nrows=1)
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]

    cursor = conn.cursor()

    # Step 1: Create transient table
    create_transient_table(cursor, table_name, df)

    # Step 2: Upload CSV to internal stage
    upload_to_stage(conn, filepath, filename)

    # Step 3: COPY INTO table from stage (fast bulk load)
    copy_into_table(cursor, table_name, filename)

    # Step 4: Verify row count
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    row_count = cursor.fetchone()[0]
    print(f"  ✅ {row_count} rows loaded into {table_name}")

    # Step 5: Clean up stage
    clean_stage(cursor, filename)

    cursor.close()

# ----------------------------------------
# 9. Main function
# ----------------------------------------
def main():
    print("🔌 Connecting to Snowflake...")
    conn = get_snowflake_connection()
    print("✅ Connected!\n")

    for filename, table_name in FILES.items():
        ingest_file(conn, filename, table_name)

    conn.close()
    print("\n🎉 All files ingested successfully!")

if __name__ == "__main__":
    main()