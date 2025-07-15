import os
import json
import psycopg2
from dotenv import load_dotenv
import logging
from ultralytics import YOLO
from tqdm import tqdm
from datetime import datetime

# --- 1. Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('yolo_enricher.log')
    ]
)
logger = logging.getLogger(__name__)

# --- 2. Load Environment Variables ---
load_dotenv()

DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT', 5432)

IMAGE_DOWNLOAD_DIR = 'data/raw/telegram_messages/images'
RAW_YOLO_TABLE_NAME = 'raw_yolo_detections'
RAW_SCHEMA_NAME = 'raw'

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
        logger.info("Successfully connected to PostgreSQL database for YOLO enrichment.")
        return conn
    except psycopg2.Error as e:
        logger.critical(f"Error connecting to PostgreSQL database for YOLO enrichment: {e}")
        raise

# --- 4. Create Raw YOLO Detections Table Function ---
def create_raw_yolo_table(cursor):
    """Creates the raw_yolo_detections table if it doesn't exist."""
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA_NAME};")
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {RAW_SCHEMA_NAME}.{RAW_YOLO_TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            message_id BIGINT NOT NULL,
            image_filename VARCHAR(512) NOT NULL,
            detected_object_class VARCHAR(255) NOT NULL,
            confidence REAL NOT NULL,
            bounding_box JSONB, -- Store bounding box as JSONB
            detection_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            -- Ensure unique detection for a given message, object, and confidence (or just message_id, object_class if you want simpler uniqueness)
            UNIQUE (message_id, image_filename, detected_object_class, confidence)
        );
    """)
    logger.info(f"Ensured table {RAW_SCHEMA_NAME}.{RAW_YOLO_TABLE_NAME} exists.")

# --- 5. Main YOLO Enrichment Function ---
def run_yolo_enrichment():
    """
    Loads a YOLO model, processes images, and saves detection results to PostgreSQL.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        create_raw_yolo_table(cur)

        # Load a pre-trained YOLOv8n model
        logger.info("Loading YOLOv8n model. This may download weights if not cached...")
        model = YOLO('yolov8n.pt') # Uses the nano version, smallest and fastest

        total_images_processed = 0
        total_detections_inserted = 0

        # Get a list of all image files
        image_files = [f for f in os.listdir(IMAGE_DOWNLOAD_DIR) if f.lower().endswith(('.png', '.jpg', '.jpeg', '.gif'))]

        if not image_files:
            logger.warning("No images found in the download directory to process.")
            return

        logger.info(f"Found {len(image_files)} images to process for YOLO detection.")

        for image_filename in tqdm(image_files, desc="Processing Images with YOLO"):
            image_path = os.path.join(IMAGE_DOWNLOAD_DIR, image_filename)

            # Extract message_id from filename (assuming format: channel_username_messageid_photoid.jpg)
            try:
                parts = image_filename.split('_')
                # Assuming message ID is the second part, e.g., 'tikvahpharma_12345_67890.jpg'
                message_id = int(parts[1])
            except (IndexError, ValueError) as e:
                logger.warning(f"Could not extract message_id from filename {image_filename}: {e}. Skipping.")
                continue

            # Check if this image/message_id has already been processed (simple idempotency)
            # This check is basic; a more robust check would involve hashing the image or checking specific detection entries
            cur.execute(
                f"SELECT COUNT(*) FROM {RAW_SCHEMA_NAME}.{RAW_YOLO_TABLE_NAME} WHERE message_id = %s AND image_filename = %s;",
                (message_id, image_filename)
            )
            if cur.fetchone()[0] > 0:
                logger.info(f"Image {image_filename} (message_id: {message_id}) already processed. Skipping.")
                continue

            total_images_processed += 1

            try:
                # Run YOLO inference
                results = model(image_path) # Predict on the image

                detections_for_image = []
                for r in results: # Iterate over detection results for each image
                    for box in r.boxes: # Iterate over detected bounding boxes
                        class_id = int(box.cls[0])
                        confidence = float(box.conf[0])
                        class_name = model.names[class_id] # Get class name from model

                        # Bounding box coordinates (x1, y1, x2, y2)
                        bbox = box.xyxy[0].tolist()

                        detections_for_image.append((
                            message_id,
                            image_filename,
                            class_name,
                            confidence,
                            json.dumps(bbox) # Store bounding box as JSON string
                        ))

                if detections_for_image:
                    insert_query = f"""
                        INSERT INTO {RAW_SCHEMA_NAME}.{RAW_YOLO_TABLE_NAME}
                        (message_id, image_filename, detected_object_class, confidence, bounding_box)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (message_id, image_filename, detected_object_class, confidence) DO NOTHING;
                    """
                    cur.executemany(insert_query, detections_for_image)
                    inserted_count = cur.rowcount
                    total_detections_inserted += inserted_count
                    logger.info(f"Processed {image_filename}. Inserted {inserted_count} new detections.")
                else:
                    logger.info(f"No objects detected in {image_filename}.")

                conn.commit() # Commit after each image or batch

            except Exception as e:
                logger.error(f"Error processing image {image_filename} with YOLO: {e}")
                conn.rollback() # Rollback current image's transaction on error

        logger.info(f"YOLO enrichment complete. Total images processed: {total_images_processed}. Total new detections inserted: {total_detections_inserted}.")

    except Exception as e:
        logger.critical(f"Fatal error during YOLO enrichment: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed for YOLO enrichment.")

if __name__ == '__main__':
    run_yolo_enrichment()