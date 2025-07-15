# src/api/main.py

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager
import logging
from typing import List, Optional
from datetime import date, datetime

from .database import get_db_connection, get_db_cursor
from .schemas import Message, Channel, ImageDetection, SearchQuery, MessageSearchResult, ChannelActivity, TopObjects

# --- 1. Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- 2. FastAPI App Lifespan (for DB connection testing) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Context manager for application lifespan events.
    Used to test database connection on startup.
    """
    logger.info("FastAPI app starting up...")
    try:
        # Test database connection
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
            logger.info("Database connection test successful.")
    except Exception as e:
        logger.critical(f"Database connection test failed on startup: {e}")
        # In a real production app, you might want to exit here or have a retry mechanism
        raise RuntimeError("Failed to connect to database on startup.") from e

    yield # Application runs
    logger.info("FastAPI app shutting down.")

# --- 3. Initialize FastAPI App ---
app = FastAPI(
    title="Telegram Data Analytics API",
    description="API to query cleaned and enriched Telegram channel data.",
    version="1.0.0",
    lifespan=lifespan # Attach the lifespan context manager
)

# --- 4. API Endpoints ---

@app.get("/", response_class=HTMLResponse, summary="Root endpoint with basic info")
async def read_root():
    """
    Returns a simple HTML page with API information.
    """
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Telegram Data Analytics API</title>
        <link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet">
        <style>
            body { font-family: 'Inter', sans-serif; }
        </style>
    </head>
    <body class="bg-gray-100 text-gray-900 p-8">
        <div class="max-w-3xl mx-auto bg-white p-6 rounded-lg shadow-md">
            <h1 class="text-3xl font-bold mb-4 text-blue-700">Welcome to the Telegram Data Analytics API!</h1>
            <p class="mb-4">This API provides access to cleaned, transformed, and enriched data from public Telegram channels.</p>
            <h2 class="text-2xl font-semibold mb-3 text-blue-600">Explore the API:</h2>
            <ul class="list-disc list-inside mb-4">
                <li><a href="/docs" class="text-blue-500 hover:underline">Interactive API Documentation (Swagger UI)</a></li>
                <li><a href="/redoc" class="text-blue-500 hover:underline">Alternative API Documentation (ReDoc)</a></li>
            </ul>
            <h2 class="text-2xl font-semibold mb-3 text-blue-600">Available Endpoints (Examples):</h2>
            <ul class="list-disc list-inside mb-4">
                <li><code>GET /messages/</code>: Retrieve a list of messages.</li>
                <li><code>GET /channels/</code>: Retrieve a list of channels.</li>
                <li><code>GET /detections/</code>: Retrieve image detections.</li>
                <li><code>GET /search/messages/</code>: Search messages by keyword.</li>
                <li><code>GET /reports/channel-activity/{channel_username}</code>: Get daily message count for a channel.</li>
                <li><code>GET /reports/top-objects/</code>: Get top detected objects from images.</li>
            </ul>
            <p class="text-sm text-gray-600 mt-6">Data pipeline built using Python, Docker, PostgreSQL, dbt, YOLOv8, and FastAPI.</p>
        </div>
    </body>
    </html>
    """

@app.get("/messages/", response_model=List[Message], summary="Retrieve a list of messages")
async def get_messages(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    channel_username: Optional[str] = Query(None, description="Filter by channel username"),
    start_date: Optional[date] = Query(None, description="Filter messages from this date (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Filter messages up to this date (YYYY-MM-DD)")
):
    """
    Retrieves a list of messages from the fct_messages table.
    Allows filtering by channel, date range, and pagination.
    """
    query_parts = []
    params = []

    if channel_username:
        query_parts.append("dc.channel_username ILIKE %s")
        params.append(f"%{channel_username}%")
    if start_date:
        query_parts.append("fm.message_timestamp >= %s")
        params.append(start_date)
    if end_date:
        query_parts.append("fm.message_timestamp <= %s")
        params.append(end_date)

    where_clause = "WHERE " + " AND ".join(query_parts) if query_parts else ""

    sql_query = f"""
        SELECT
            fm.message_id,
            fm.message_text,
            fm.message_timestamp,
            fm.views_count,
            fm.forwards_count,
            fm.image_path,
            fm.message_length,
            fm.has_image,
            fm.channel_sk,
            fm.date_sk
        FROM
            public.fct_messages fm
        LEFT JOIN
            public.dim_channels dc ON fm.channel_sk = dc.channel_sk
        {where_clause}
        ORDER BY
            fm.message_timestamp DESC
        LIMIT %s OFFSET %s;
    """
    params.extend([limit, offset])

    try:
        with get_db_cursor() as (conn, cur):
            cur.execute(sql_query, params)
            messages_data = cur.fetchall()
            # Get column names from cursor description
            column_names = [desc[0] for desc in cur.description]
            # Map rows to dictionaries for Pydantic parsing
            messages_dicts = [dict(zip(column_names, row)) for row in messages_data]
            return [Message(**msg) for msg in messages_dicts]
    except Exception as e:
        logger.error(f"Error retrieving messages: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/channels/", response_model=List[Channel], summary="Retrieve a list of channels")
async def get_channels(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """
    Retrieves a list of channels from the dim_channels table.
    """
    sql_query = """
        SELECT
            channel_sk,
            channel_username,
            first_message_date,
            last_message_date,
            total_messages_scraped
        FROM
            public.dim_channels
        ORDER BY
            channel_username ASC
        LIMIT %s OFFSET %s;
    """
    try:
        with get_db_cursor() as (conn, cur):
            cur.execute(sql_query, (limit, offset))
            channels_data = cur.fetchall()
            column_names = [desc[0] for desc in cur.description]
            channels_dicts = [dict(zip(column_names, row)) for row in channels_data]
            return [Channel(**ch) for ch in channels_dicts]
    except Exception as e:
        logger.error(f"Error retrieving channels: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/detections/", response_model=List[ImageDetection], summary="Retrieve a list of image detections")
async def get_detections(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    object_class: Optional[str] = Query(None, description="Filter by detected object class"),
    message_id: Optional[int] = Query(None, description="Filter by original Telegram message ID")
):
    """
    Retrieves a list of image detections from the fct_image_detections table.
    Allows filtering by object class, message ID, and pagination.
    """
    query_parts = []
    params = []

    if object_class:
        query_parts.append("detected_object_class ILIKE %s")
        params.append(f"%{object_class}%")
    if message_id:
        query_parts.append("message_id = %s")
        params.append(message_id)

    where_clause = "WHERE " + " AND ".join(query_parts) if query_parts else ""

    sql_query = f"""
        SELECT
            detection_id,
            message_id,
            image_filename,
            detected_object_class,
            confidence,
            bounding_box,
            detection_timestamp
        FROM
            public.fct_image_detections
        {where_clause}
        ORDER BY
            detection_timestamp DESC
        LIMIT %s OFFSET %s;
    """
    params.extend([limit, offset])

    try:
        with get_db_cursor() as (conn, cur):
            cur.execute(sql_query, params)
            detections_data = cur.fetchall()
            column_names = [desc[0] for desc in cur.description]
            detections_dicts = [dict(zip(column_names, row)) for row in detections_data]
            return [ImageDetection(**det) for det in detections_dicts]
    except Exception as e:
        logger.error(f"Error retrieving detections: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/search/messages/", response_model=MessageSearchResult, summary="Search messages by keyword")
async def search_messages(
    search_query: str = Query(..., min_length=3, description="Keyword to search in message text"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """
    Searches for messages containing a specific keyword in their text.
    """
    # Count total results first
    count_query = """
        SELECT COUNT(*)
        FROM public.fct_messages
        WHERE message_text ILIKE %s;
    """
    search_pattern = f"%{search_query}%"

    try:
        with get_db_cursor() as (conn, cur):
            cur.execute(count_query, (search_pattern,))
            total_results = cur.fetchone()[0]

            sql_query = f"""
                SELECT
                    message_id,
                    message_text,
                    message_timestamp,
                    views_count,
                    forwards_count,
                    image_path,
                    message_length,
                    has_image,
                    channel_sk,
                    date_sk
                FROM
                    public.fct_messages
                WHERE
                    message_text ILIKE %s
                ORDER BY
                    message_timestamp DESC
                LIMIT %s OFFSET %s;
            """
            cur.execute(sql_query, (search_pattern, limit, offset))
            messages_data = cur.fetchall()
            column_names = [desc[0] for desc in cur.description]
            messages_dicts = [dict(zip(column_names, row)) for row in messages_data]
            messages = [Message(**msg) for msg in messages_dicts]

            return MessageSearchResult(total_results=total_results, messages=messages)
    except Exception as e:
        logger.error(f"Error searching messages: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/reports/channel-activity/{channel_username}", response_model=List[ChannelActivity], summary="Get daily message count for a channel")
async def get_channel_activity(
    channel_username: str,
    start_date: Optional[date] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="End date (YYYY-MM-DD)")
):
    """
    Retrieves the daily message count for a specific channel within a date range.
    """
    query_parts = []
    params = []

    query_parts.append("dc.channel_username = %s")
    params.append(channel_username)

    if start_date:
        query_parts.append("dd.date_actual >= %s")
        params.append(start_date)
    if end_date:
        query_parts.append("dd.date_actual <= %s")
        params.append(end_date)

    where_clause = "WHERE " + " AND ".join(query_parts) if query_parts else ""

    sql_query = f"""
        SELECT
            dc.channel_username,
            dd.date_actual AS date,
            COUNT(fm.message_id)::int AS message_count
        FROM
            public.fct_messages fm
        JOIN
            public.dim_channels dc ON fm.channel_sk = dc.channel_sk
        JOIN
            public.dim_dates dd ON fm.date_sk = dd.date_sk
        {where_clause}
        GROUP BY
            dc.channel_username, dd.date_actual
        ORDER BY
            dd.date_actual ASC;
    """
    try:
        with get_db_cursor() as (conn, cur):
            cur.execute(sql_query, params)
            activity_data = cur.fetchall()
            column_names = [desc[0] for desc in cur.description]
            activity_dicts = [dict(zip(column_names, row)) for row in activity_data]
            return [ChannelActivity(**act) for act in activity_dicts]
    except Exception as e:
        logger.error(f"Error retrieving channel activity for {channel_username}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/reports/top-objects/", response_model=List[TopObjects], summary="Get top detected objects from images")
async def get_top_objects(
    limit: int = Query(10, ge=1, le=100),
    min_confidence: float = Query(0.5, ge=0.0, le=1.0, description="Minimum confidence score for detections")
):
    """
    Retrieves the most frequently detected objects across all images.
    """
    sql_query = """
        SELECT
            detected_object_class,
            COUNT(*)::int AS count
        FROM
            public.fct_image_detections
        WHERE
            confidence >= %s
        GROUP BY
            detected_object_class
        ORDER BY
            count DESC
        LIMIT %s;
    """
    try:
        with get_db_cursor() as (conn, cur):
            cur.execute(sql_query, (min_confidence, limit))
            top_objects_data = cur.fetchall()
            column_names = [desc[0] for desc in cur.description]
            top_objects_dicts = [dict(zip(column_names, row)) for row in top_objects_data]
            return [TopObjects(**obj) for obj in top_objects_dicts]
    except Exception as e:
        logger.error(f"Error retrieving top objects: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")