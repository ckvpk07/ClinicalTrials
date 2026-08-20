import os
import sys
import json
import logging
import requests
import time
import pandas as pd
from datetime import date, datetime, timedelta
from typing import Generator, List, Dict, Any, Tuple
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization

# ============================================================
# 1. PARAMETER FILE INITIALIZATION
# ============================================================
CONFIG_FILE = "config.json"

if not os.path.exists(CONFIG_FILE):
    print(f"CRITICAL ERROR: Configuration parameter file '{CONFIG_FILE}' missing.")
    sys.exit(1)

with open(CONFIG_FILE, "r", encoding="utf-8") as f:
    config = json.load(f)

# Extract structured variables out of the parameter file
API_URL = config["api_configuration"]["target_url"]
PAGE_SIZE = config["api_configuration"]["page_size"]
DEFAULT_CUTOFF = config["api_configuration"]["default_cutoff_date"]

TABLE_NAME = config["warehouse_configuration"]["target_table"]
FACT_TABLE = config["warehouse_configuration"].get("fact_table", "STAGING.FCT_STUDY_INTERVENTIONS") 
BATCH_SIZE = config["warehouse_configuration"]["batch_chunk_size"]

LOG_DIR = config["logging_configuration"]["local_log_directory"]
LOG_PREFIX = config["logging_configuration"]["log_file_prefix"]

# ============================================================
# 2. LOGGING CONFIGURATION
# ============================================================
os.makedirs(LOG_DIR, exist_ok=True)
current_run_timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
log_filename = os.path.join(LOG_DIR, f"{LOG_PREFIX}_{current_run_timestamp}.log")

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

log_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(log_filename, encoding="utf-8", delay=False)
file_handler.setFormatter(log_formatter)
root_logger.addHandler(file_handler)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_formatter)
root_logger.addHandler(console_handler)

logger = logging.getLogger(__name__)


# ============================================================
# 3. CORE OPERATIONAL PIPELINE FUNCTIONS
# ============================================================
def get_snowflake_connection():
    """Establish Snowflake connection using RSA key-pair authentication."""
    private_key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]

    with open(private_key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None
        )

    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        host=os.environ["SNOWFLAKE_HOST"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=private_key_bytes,
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        role=os.environ["SNOWFLAKE_ROLE"]
    )

def fetch_api_page_with_retry(params: dict, max_retries: int = 5) -> dict:
    """Handles transient network dropped packets and API throttling (429) smoothly."""
    backoff = 2
    for attempt in range(max_retries):
        try:
            response = requests.get(API_URL, params=params, timeout=30)
            
            if response.status_code == 429:
                logger.warning(f"Rate limited (429). Retrying in {backoff}s... (Attempt {attempt+1}/{max_retries})")
                time.sleep(backoff)
                backoff *= 2
                continue
                
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                logger.error(f"API connection permanently failed after {max_retries} attempts.")
                raise e
            logger.warning(f"Network glitch encountered: {e}. Retrying in {backoff}s...")
            time.sleep(backoff)
            backoff *= 2

def fetch_api_stream(filter_mode: str, filter_date_str: str) -> Generator[Dict[str, Any], None, None]:
    """Streams data page-by-page forcing server-side date boundaries using proper API v2 syntax."""
    # Base phase and type filters
    base_query = "AREA[StudyType]INTERVENTIONAL AND (AREA[Phase]PHASE2 OR AREA[Phase]PHASE3)"
    
    # 1. FIXED MANDATORY FILTER: Always pulled dynamically via config parameter file
    fixed_submission_filter = f"AREA[StudyFirstSubmitDate]RANGE[{DEFAULT_CUTOFF}, MAX]"
    
    # 2. INCREMENTAL FILTER OVERLAY: Applied directly via API if fact rows exist
    if filter_mode == "INCREMENTAL":
        api_query = f"{base_query} AND {fixed_submission_filter} AND AREA[LastUpdatePostDate]RANGE[{filter_date_str}, MAX]"
    else:
        api_query = f"{base_query} AND {fixed_submission_filter}"
        
    logger.info(f"Executing Server-Side Filtered API Query: {api_query}")

    params = {
        "query.term": api_query,
        "pageSize": PAGE_SIZE
    }
    page_number = 1
    
    while True:
        logger.info(f"Fetching API page {page_number}...")
        data = fetch_api_page_with_retry(params)
        
        studies = data.get("studies", [])
        logger.info(f"Received {len(studies)} raw records from page {page_number}")
        
        for study in studies:
            yield study
            
        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break
            
        params["pageToken"] = next_page_token
        page_number += 1

def process_and_batch_stream(study_stream: Generator[Dict[str, Any], None, None]) -> Generator[List[Dict[str, Any]], None, None]:
    """Batches data coming from pre-filtered API streams cleanly into data pipeline arrays."""
    current_batch = []
    for study in study_stream:
        serialized_study = json.dumps(study, separators=(",", ":"), default=str)
        current_batch.append({"RAW_JSON": serialized_study})
        
        if len(current_batch) >= BATCH_SIZE:
            yield current_batch
            current_batch = []
            
    if current_batch:
        yield current_batch

def determine_pipeline_mode(conn) -> Tuple[str, str]:
    """Queries max LastUpdatePostDate to pick INCREMENTAL mode, or falls back to INITIAL mode."""
    cursor = conn.cursor()
    try:
        db = os.environ.get("SNOWFLAKE_DATABASE", "").strip().upper()
        table_config = FACT_TABLE.strip().upper()

        if "." in table_config:
            fully_qualified_path = f"{db}.{table_config}"
        else:
            schema = os.environ.get("SNOWFLAKE_SCHEMA", "").strip().upper()
            fully_qualified_path = f"{db}.{schema}.{table_config}"

        logger.info(f"Evaluating data footprint watermark at: {fully_qualified_path}...")
        
        # Pull largest raw update date from target operational layer
        query = f"SELECT TO_VARCHAR(MAX(LAST_UPDATE_POST_DATE), 'YYYY-MM-DD') FROM {fully_qualified_path};"
        cursor.execute(query)
        
        result = cursor.fetchone()
        
        # Safely extract first element from tuple and confirm string is valid
        if result and result[0] is not None:
            last_date_str = str(result[0])
            base_date = datetime.strptime(last_date_str, "%Y-%m-%d").date()
            
            # Apply exactly -1 day loopback cushion for incremental changes
            adjusted_date = base_date - timedelta(days=1)
            adjusted_date_str = adjusted_date.strftime("%Y-%m-%d")
            
            logger.info(f"Watermark found. Mode: INCREMENTAL, Appending Filter: LastUpdatePostDate >= {adjusted_date_str}")
            return "INCREMENTAL", adjusted_date_str
            
        logger.info(f"Fact table is empty or missing metrics. Mode: INITIAL, Applying fixed baseline: {DEFAULT_CUTOFF}")
        return "INITIAL", DEFAULT_CUTOFF
    except Exception as e:
        logger.warning(f"Metadata lookup failed ({e}). Defaulting to Fallback. Mode: INITIAL, Cutoff: {DEFAULT_CUTOFF}")
        return "INITIAL", DEFAULT_CUTOFF
    finally:
        cursor.close()

def truncate_raw_table(conn):
    """Safely truncates the target staging table."""
    cursor = conn.cursor()
    try:
        logger.info(f"Truncating staging table: {TABLE_NAME}...")
        cursor.execute("TRUNCATE TABLE IDENTIFIER(%s);", (TABLE_NAME,))
        logger.info("Table truncated successfully.")
    except Exception as e:
        logger.error(f"Failed to truncate table {TABLE_NAME}: {e}")
        raise
    finally:
        cursor.close()

def main():
    conn = None
    try:
        logger.info("Initializing configuration-driven clinical trials data pipeline...")
        conn = get_snowflake_connection()
        
        # 1. Dynamically evaluate what data already exists to pick the operational strategy
        filter_mode, filter_date_str = determine_pipeline_mode(conn)
        
        # 2. Always clear out staging target table to ensure fresh write loops
        truncate_raw_table(conn)
        
        # 3. Pull data stream leveraging server-side parameter conjunctions
        raw_stream = fetch_api_stream(filter_mode, filter_date_str)
        batched_stream = process_and_batch_stream(raw_stream)
        
        total_rows_loaded = 0
        for chunk in batched_stream:
            df = pd.DataFrame(chunk)
            df.columns = ["RAW_JSON"]
            
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name=TABLE_NAME,
                database=os.environ["SNOWFLAKE_DATABASE"],
                schema=os.environ["SNOWFLAKE_SCHEMA"],
                auto_create_table=False
            )
            if success:
                total_rows_loaded += nrows
                logger.info(f"Successfully loaded chunk of {nrows} rows. Total row count: {total_rows_loaded}")
        
        logger.info(f"Ingestion completed successfully. Total records added to {TABLE_NAME}: {total_rows_loaded}")
    except Exception as e:
        logger.critical(f"Pipeline crashed during execution boundary: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()