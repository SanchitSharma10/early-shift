"""
exceptions.py - Custom exceptions for Early Shift
"""


class EarlyShiftError(Exception):
    """Base exception for Early Shift errors."""
    pass


class DatabaseError(EarlyShiftError):
    """Database operation failures."""
    pass


class APIError(EarlyShiftError):
    """API request failures."""
    
    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class ConfigurationError(EarlyShiftError):
    """Configuration or environment variable errors."""
    pass


class YouTubeAPIError(APIError):
    """YouTube API specific errors."""
    pass


class RobloxAPIError(APIError):
    """Roblox/RoProxy API specific errors."""
    pass


class NotionAPIError(APIError):
    """Notion API specific errors."""
    pass


class SchemaError(DatabaseError):
    """Database schema errors."""
    pass