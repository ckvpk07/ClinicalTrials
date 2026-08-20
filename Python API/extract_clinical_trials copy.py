import os
import sys
import json
import logging
import requests
import time
import pandas as pd
from datetime import date, datetime, timedelta
from typing import Generator, List, Dict, Any
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

STAGING_TABLE = config["warehouse_configuration"]["target_table"]
FACT_TABLE = config["warehouse_configuration"].get("fact_table", "STAGING.FCT_STUDY_INTERVENTIONS") 
BATCH_SIZE = config["warehouse_configuration"]["batch_chunk_size"]

LOG_DIR = config["logging_configuration"]["local_log_directory"]
LOG_PREFIX = config["logging_configuration"]["log_file_prefix"]

# ============================================================
# 2. LOGGING CONFIGURATION
# ============================================================

os.makedirs(LOG_DIR, exist_ok=True)

# Added precise timestamp hours, minutes, and seconds to ensure unique file creation per run
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
    """Establish Snowflake connection with explicit network timeout configurations."""
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
        role=os.environ["SNOWFLAKE_ROLE"],
        network_timeout=60,   # Prevents un-interruptible database hangs
        login_timeout=30
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

def fetch_api_stream(dynamic_cutoff: str) -> Generator[Dict[str, Any], None, None]:
    """Streams data page-by-page, applying the date filter directly on the API server."""
    api_query = (
        f"AREA[StudyType]INTERVENTIONAL AND (AREA[Phase]PHASE2 OR AREA[Phase]PHASE3) "
        f"AND AREA[LastUpdatePostDate]RANGE[{dynamic_cutoff}, MAX]"
    )
    logger.info(f"Generated API Query Term: {api_query}")

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
    """Batches data coming from the pre-filtered API stream directly."""
    current_batch = []
    for study in study_stream:
        serialized_study = json.dumps(study, separators=(",", ":"), default=str)
        current_batch.append({"RAW_JSON": serialized_study})
        
        if len(current_batch) >= BATCH_SIZE:
            yield current_batch
            current_batch = []
            
    if current_batch:
        yield current_batch

def get_fact_last_update_date(conn) -> str:
    """Queries FCT_STUDY_INTERVENTIONS via direct cross-schema execution and applies 1-day cushion."""
    cursor = conn.cursor()
    try:
        db = os.environ.get("SNOWFLAKE_DATABASE", "").strip().upper()
        table_config = FACT_TABLE.strip().upper()

        if "." in table_config:
            fully_qualified_path = f"{db}.{table_config}"
        else:
            schema = os.environ.get("SNOWFLAKE_SCHEMA", "").strip().upper()
            fully_qualified_path = f"{db}.{schema}.{table_config}"

        logger.info(f"Fetching highest study_last_update_date watermark from: {fully_qualified_path}...")
        
        query = f"SELECT TO_VARCHAR(MAX(STUDY_LAST_UPDATE_DATE), 'YYYY-MM-DD') FROM {fully_qualified_path};"
        cursor.execute(query)
        
        result = cursor.fetchone()
        if result and result[0] is not None:
            last_date_str = str(result[0])
            
            base_date = datetime.strptime(last_date_str, "%Y-%m-%d").date()
            adjusted_date = base_date - timedelta(days=1)
            adjusted_date_str = adjusted_date.strftime("%Y-%m-%d")
            
            logger.info(f"Watermark raw date: {last_date_str}. Adjusted with 1-day lookback: {adjusted_date_str}")
            return adjusted_date_str
            
        logger.info(f"Fact table {fully_qualified_path} is empty. Falling back to default baseline: {DEFAULT_CUTOFF}")
        return DEFAULT_CUTOFF
    except Exception as e:
        logger.warning(f"Could not read from Fact table {FACT_TABLE} ({e}). Falling back to baseline: {DEFAULT_CUTOFF}")
        return DEFAULT_CUTOFF
    finally:
        cursor.close()

def main():
    conn = None
    try:
        logger.info("Initializing configuration-driven clinical trials data pipeline...")
        conn = get_snowflake_connection()
        
        incremental_cutoff_date = get_fact_last_update_date(conn)
        logger.info(f"Pipeline running using active cutoff filtration date: {incremental_cutoff_date}")
        
        raw_stream = fetch_api_stream(incremental_cutoff_date)
        batched_stream = process_and_batch_stream(raw_stream)
        
        total_rows_loaded = 0
        for chunk in batched_stream:
            df = pd.DataFrame(chunk)
            df.columns = ["RAW_JSON"]
            
            logger.info(f"Preparing to upload {len(df)} rows to Snowflake via write_pandas...")
            
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name=STAGING_TABLE,
                database=os.environ["SNOWFLAKE_DATABASE"],
                schema=os.environ["SNOWFLAKE_SCHEMA"],
                auto_create_table=False  
            )
            
            if success:
                total_rows_loaded += nrows
                logger.info(f"Successfully loaded chunk of {nrows} rows to staging. Total: {total_rows_loaded}")
                
        logger.info(f"Ingestion completed successfully. Total records added to {STAGING_TABLE}: {total_rows_loaded}")
        
    except Exception as e:
        logger.critical(f"Pipeline crashed during execution boundary: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
