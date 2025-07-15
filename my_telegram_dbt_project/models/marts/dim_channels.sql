-- models/marts/dim_channels.sql

-- Dimension table for Telegram channels
-- Contains unique information about each channel
{{ config(
    
    materialized='table',
    unique_key='channel_sk'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['channel_username']) }} AS channel_sk, -- Generate a surrogate key
    channel_username,
    -- Add more channel-specific attributes if available in raw data or from other sources
    -- e.g., channel_name, channel_description
    MIN(message_timestamp) AS first_message_date,
    MAX(message_timestamp) AS last_message_date,
    COUNT(DISTINCT message_id) AS total_messages_scraped
FROM
    {{ ref('stg_telegram_messages') }} -- Reference the staging model
GROUP BY
    channel_username