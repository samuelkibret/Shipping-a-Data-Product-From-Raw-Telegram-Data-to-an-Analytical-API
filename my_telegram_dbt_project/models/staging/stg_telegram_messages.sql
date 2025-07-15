-- models/staging/stg_telegram_messages.sql

-- This staging model cleans and lightly transforms raw Telegram message data.
-- It selects relevant fields, casts data types, and extracts information
-- from the JSONB message_data column.

{{ config(
    
    materialized='view'
    
    ) }} 

SELECT
    message_id,
    channel_username,
    -- Extract relevant fields from the JSONB message_data
    message_data ->> 'message' AS message_text, -- The actual text content of the message
    (message_data ->> 'date')::timestamp AS message_timestamp, -- Convert ISO string to timestamp
    message_data ->> 'views' AS views_count, -- Number of views
    message_data ->> 'forwards' AS forwards_count, -- Number of forwards
    message_data ->> 'image_download_path' AS image_path, -- Path to downloaded image
    message_data ->> 'id' AS original_message_id_str, -- Keep original string ID if needed
    message_data #>> '{peer_id, channel_id}' AS peer_channel_id, -- Nested channel ID
    -- Add more fields as needed based on your raw JSON structure
    message_data AS full_message_json -- Keep the full JSON for debugging/future use
FROM
    {{ source('raw', 'raw_telegram_messages') }} -- Reference the raw table in the 'raw' schema
WHERE
    message_data IS NOT NULL
    AND message_data ->> 'message' IS NOT NULL
    AND TRIM(message_data ->> 'message') != '' -- Also filter out empty strings after trimming whitespace