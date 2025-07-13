import os
import json
import psycopg2
from dotenv import load_dotenv
import logging
from datetime import datetime
from tqdm import tqdm

# --- 1. Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('data_loader.log')
    ]
)
logger = logging.getLogger(__name__)

# --- 2. Load Environment Variables ---
load_dotenv()

DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST') # Should be 'db' when running in Docker Compose
DB_PORT = os.getenv('DB_PORT', 5432) # Default to 5432

RAW_DATA_LAKE_PATH = 'data/raw/telegram_messages'
RAW_TABLE_NAME = 'raw_telegram_messages'
RAW_SCHEMA_NAME = 'raw' # As per challenge document

# --- 3. Database Connection Function ---
def get_db_connection():
    """Establishes and returns a PostgreSQL database connection."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        logger.info("Successfully connected to PostgreSQL database.")
        return conn
    except psycopg2.Error as e:
        logger.critical(f"Error connecting to PostgreSQL database: {e}")
        raise

# --- 4. Create Raw Table Function ---
def create_raw_table(cursor):
    """Creates the raw_telegram_messages table if it doesn't exist."""
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA_NAME};")
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {RAW_SCHEMA_NAME}.{RAW_TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            message_id BIGINT NOT NULL,
            channel_username VARCHAR(255) NOT NULL,
            message_data JSONB NOT NULL,
            scraped_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (message_id, channel_username) -- Ensure unique messages per channel
        );
    """)
    logger.info(f"Ensured table {RAW_SCHEMA_NAME}.{RAW_TABLE_NAME} exists.")

# --- 5. Load Data Function ---
def load_json_to_postgres():
    """Reads JSON files from data lake and loads them into PostgreSQL."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        create_raw_table(cur) # Ensure table exists before loading

        total_files_processed = 0
        total_messages_inserted = 0

        # Iterate through the partitioned data lake structure
        for date_dir in tqdm(os.listdir(RAW_DATA_LAKE_PATH), desc="Processing Dates"):
            date_path = os.path.join(RAW_DATA_LAKE_PATH, date_dir)
            if not os.path.isdir(date_path) or date_dir == 'images': # Skip 'images' folder
                continue

            for channel_dir in os.listdir(date_path):
                channel_path = os.path.join(date_path, channel_dir)
                if not os.path.isdir(channel_path):
                    continue

                for json_file in os.listdir(channel_path):
                    if json_file.endswith('.json'):
                        file_path = os.path.join(channel_path, json_file)
                        total_files_processed += 1
                        logger.info(f"Loading data from: {file_path}")

                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                messages = json.load(f)

                            if not messages:
                                logger.warning(f"File {json_file} is empty or contains no messages.")
                                continue

                            # Prepare data for insertion
                            insert_values = []
                            for msg in messages:
                                # Ensure message_id and channel_username are present
                                msg_id = msg.get('id')
                                channel_user = msg.get('channel_username')
                                if msg_id is None or channel_user is None:
                                    logger.warning(f"Skipping message due to missing ID or channel_username in file {json_file}: {msg}")
                                    continue
                                insert_values.append((msg_id, channel_user, json.dumps(msg))) # json.dumps to store dict as JSON string

                            if insert_values:
                                # Use ON CONFLICT DO NOTHING to handle duplicates (from UNIQUE constraint)
                                # This makes the load idempotent - running it multiple times won't duplicate data
                                insert_query = f"""
                                    INSERT INTO {RAW_SCHEMA_NAME}.{RAW_TABLE_NAME} (message_id, channel_username, message_data)
                                    VALUES (%s, %s, %s)
                                    ON CONFLICT (message_id, channel_username) DO NOTHING;
                                """
                                cur.executemany(insert_query, insert_values)
                                inserted_rows = cur.rowcount # Number of rows actually inserted (not skipped by ON CONFLICT)
                                total_messages_inserted += inserted_rows
                                logger.info(f"Inserted/skipped {len(insert_values)} messages from {json_file}. Actual new inserts: {inserted_rows}")
                            conn.commit() # Commit after each file or batch

                        except json.JSONDecodeError as e:
                            logger.error(f"Error decoding JSON from {json_file}: {e}")
                        except Exception as e:
                            logger.error(f"Error processing {json_file}: {e}")

        logger.info(f"Data loading complete. Total files processed: {total_files_processed}. Total new messages inserted: {total_messages_inserted}.")

    except Exception as e:
        logger.critical(f"Fatal error during data loading process: {e}")
        if conn:
            conn.rollback() # Rollback in case of critical error
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

if __name__ == '__main__':
    load_json_to_postgres()