# src/api/schemas.py

from pydantic import BaseModel
from datetime import datetime
from typing import Optional, List, Dict, Any

# --- Schemas for dbt Models ---

class Channel(BaseModel):
    """Pydantic model for dim_channels."""
    channel_sk: str
    channel_username: str
    first_message_date: Optional[datetime]
    last_message_date: Optional[datetime]
    total_messages_scraped: Optional[int]

    class Config:
        from_attributes = True # Allow mapping from SQLAlchemy/ORM objects

class Date(BaseModel):
    """Pydantic model for dim_dates."""
    date_sk: int
    date_actual: datetime
    year: int
    month: int
    month_name_short: str
    day_of_month: int
    day_of_week_num: int
    day_of_week_name: str
    is_weekend: bool

    class Config:
        from_attributes = True

class Message(BaseModel):
    """Pydantic model for fct_messages."""
    message_id: int
    message_text: Optional[str]
    message_timestamp: datetime
    views_count: Optional[int]
    forwards_count: Optional[int]
    image_path: Optional[str]
    message_length: Optional[int]
    has_image: Optional[bool]
    channel_sk: str
    date_sk: int

    class Config:
        from_attributes = True

class ImageDetection(BaseModel):
    """Pydantic model for fct_image_detections."""
    detection_id: str
    message_id: int
    image_filename: str
    detected_object_class: str
    confidence: float
    bounding_box: Optional[Dict[str, Any]] # JSONB field, Pydantic handles dict
    detection_timestamp: datetime

    class Config:
        from_attributes = True

# --- API Request/Response Schemas (Examples) ---

class SearchQuery(BaseModel):
    """Schema for message search requests."""
    query: str
    limit: int = 10
    offset: int = 0

class MessageSearchResult(BaseModel):
    """Schema for message search responses."""
    total_results: int
    messages: List[Message]

class ChannelActivity(BaseModel):
    """Schema for channel activity responses."""
    channel_username: str
    date: datetime
    message_count: int

class TopObjects(BaseModel):
    """Schema for top detected objects."""
    detected_object_class: str
    count: int