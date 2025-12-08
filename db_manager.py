"""
db_manager.py - Centralized database connection management
"""

from contextlib import contextmanager
from typing import Generator

import duckdb

from constants import DEFAULT_DB_PATH


@contextmanager
def get_db_connection(
    db_path: str = DEFAULT_DB_PATH, 
    read_only: bool = False
) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """
    Context manager for database connections.
    
    Args:
        db_path: Path to the DuckDB database file
        read_only: Whether to open the database in read-only mode
        
    Yields:
        duckdb.DuckDBPyConnection: The database connection
        
    Example:
        with get_db_connection() as db:
            result = db.execute("SELECT * FROM games").fetchall()
    """
    db = duckdb.connect(db_path, read_only=read_only)
    try:
        yield db
    finally:
        db.close()


class DatabaseManager:
    """
    Manages persistent database connections for long-running processes.
    Use context manager (get_db_connection) for short-lived operations.
    """
    
    def __init__(self, db_path: str = DEFAULT_DB_PATH) -> None:
        self.db_path = db_path
        self._db: duckdb.DuckDBPyConnection | None = None
    
    @property
    def db(self) -> duckdb.DuckDBPyConnection:
        """Lazy-load database connection."""
        if self._db is None:
            self._db = duckdb.connect(self.db_path)
        return self._db
    
    def close(self) -> None:
        """Close the database connection."""
        if self._db is not None:
            self._db.close()
            self._db = None
    
    def __enter__(self) -> "DatabaseManager":
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()