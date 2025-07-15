-- models/marts/fct_messages.sql

-- Fact table for Telegram messages
-- Links to dimension tables and includes key metrics
{{ config(
    materialized='table',
    unique_key='message_id',)
}}

SELECT
    stg.message_id,
    stg.message_text,
    stg.message_timestamp,
    stg.views_count::INT AS views_count, -- Cast to integer
    stg.forwards_count::INT AS forwards_count, -- Cast to integer
    stg.image_path,
    LENGTH(stg.message_text) AS message_length,
    CASE WHEN stg.image_path IS NOT NULL THEN TRUE ELSE FALSE END AS has_image,

    -- Foreign keys to dimension tables
    dim_c.channel_sk,
    dim_d.date_sk
FROM
    {{ ref('stg_telegram_messages') }} stg
LEFT JOIN
    {{ ref('dim_channels') }} dim_c
    ON stg.channel_username = dim_c.channel_username
LEFT JOIN
    {{ ref('dim_dates') }} dim_d
    ON stg.message_timestamp::date = dim_d.date_actual
WHERE
    stg.message_id IS NOT NULL -- Ensure we only include valid messages