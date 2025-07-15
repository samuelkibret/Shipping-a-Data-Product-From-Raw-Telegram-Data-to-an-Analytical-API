# src/api/database.py

import os
import psycopg2
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)

# Load environment variables (ensure .env is loaded at app startup)
load_dotenv()

DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST') # This will be 'db' in Docker Compose
DB_PORT = os.getenv('DB_PORT', 5432)

def get_db_connection():
    """
    Establishes and returns a new PostgreSQL database connection.
    It's recommended to use this function within a 'with' statement
    or ensure connections are closed.
    """
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        logger.info("Database connection established.")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Error connecting to database: {e}")
        raise

def get_db_cursor():
    """
    Provides a database connection and cursor.
    Use as a context manager:
    with get_db_cursor() as (conn, cur):
        # do database operations
    """
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        yield conn, cur
    except Exception as e:
        logger.error(f"Error getting database cursor: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            logger.info("Database connection closed.")