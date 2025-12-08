"""
config.py - Centralized configuration management
"""

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from constants import (
    DEFAULT_DB_PATH,
    DEFAULT_CACHE_HOURS,
    DEFAULT_CONCURRENCY,
    CHANNELS_FILE,
)


@dataclass
class Config:
    """Application configuration."""
    
    # Database
    db_path: str = DEFAULT_DB_PATH
    
    # YouTube API
    youtube_api_key: str = ""
    
    # API Configuration
    cache_hours: int = DEFAULT_CACHE_HOURS
    max_concurrency: int = DEFAULT_CONCURRENCY
    
    # Paths
    channels_file: Path = CHANNELS_FILE
    
    @classmethod
    def load(cls, env_file: Path | None = None) -> "Config":
        """
        Load configuration from environment variables.
        
        Args:
            env_file: Optional path to .env file
            
        Returns:
            Config instance with values from environment
        """
        if env_file is None:
            env_file = Path(__file__).parent / ".env"
        
        if env_file.exists():
            load_dotenv(env_file)
        
        return cls(
            db_path=os.getenv("DB_PATH", DEFAULT_DB_PATH),
            youtube_api_key=os.getenv("YOUTUBE_API_KEY", ""),
            cache_hours=int(os.getenv("CACHE_HOURS", str(DEFAULT_CACHE_HOURS))),
            max_concurrency=int(os.getenv("MAX_CONCURRENCY", str(DEFAULT_CONCURRENCY))),
            channels_file=Path(os.getenv("CHANNELS_FILE", str(CHANNELS_FILE))),
        )
    
    def validate_youtube_api_key(self) -> None:
        """
        Validate that YouTube API key is configured.
        
        Raises:
            ValueError: If API key is not set
        """
        if not self.youtube_api_key:
            raise ValueError(
                "YOUTUBE_API_KEY environment variable is required. "
                "Add it to your .env file or set it in your environment."
            )


# Global config instance (lazy-loaded)
_config: Config | None = None


def get_config() -> Config:
    """
    Get the global configuration instance.
    
    Returns:
        Config instance
    """
    global _config
    if _config is None:
        _config = Config.load()
    return _config