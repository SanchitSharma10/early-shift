"""Unit tests for mechanic detector logic."""
import os
import tempfile
from datetime import datetime, timezone

import duckdb
import pytest

from mechanic_detector import (
    MechanicSpike,
    _extract_mechanic,
    _video_matches_game,
    detect_mechanic_spikes,
    get_historical_spikes,
)


def test_extract_mechanic():
    """Test mechanic extraction from video titles."""
    assert "Dragon Quest" in _extract_mechanic("NEW Dragon Quest Update!")
    assert "Pet Fusion" in _extract_mechanic("Secret Pet Fusion Mechanic Found")
    assert "Code" in _extract_mechanic("Update: New Code for Free Items")
    
    # Fallback to full title when no keyword
    result = _extract_mechanic("Blox Fruits Is Amazing")
    assert len(result) > 0


def test_video_matches_game():
    """Test game name matching with fuzzy logic."""
    assert _video_matches_game("Pet Simulator X", "Pet Simulator X New Update!")
    assert _video_matches_game("Adopt Me", "New Adopt Me Secret Code")
    assert not _video_matches_game("Blox Fruits", "Random Gaming Video")
    
    # Test with keyword hints
    assert _video_matches_game("Brookhaven", "NEW Brookhaven Update 2025")



@pytest.fixture
def mock_db():
    """Create on-disk test database with mock data."""
    fd, db_path = tempfile.mkstemp(suffix=".duckdb")
    os.close(fd)
    os.unlink(db_path)

    db = duckdb.connect(db_path)

    # Create games table with mock CCU data
    db.execute(
        """
        CREATE TABLE games (
            universe_id INTEGER,
            name TEXT,
            ccu INTEGER,
            timestamp TIMESTAMP
        )
        """
    )

    # Create game_metadata table
    db.execute(
        """
        CREATE TABLE game_metadata (
            universe_id INTEGER PRIMARY KEY,
            name TEXT
        )
        """
    )

    # Create youtube_videos table
    db.execute(
        """
        CREATE TABLE youtube_videos (
            video_id TEXT PRIMARY KEY,
            channel_title TEXT,
            title TEXT,
            published_at TIMESTAMP,
            view_count INTEGER
        )
        """
    )

    # Create mechanic_spikes table for persistence tests
    db.execute(
        """
        CREATE TABLE mechanic_spikes (
            universe_id BIGINT,
            game_name TEXT,
            current_ccu INTEGER,
            week_ago_ccu INTEGER,
            growth_percent DOUBLE,
            mechanic TEXT,
            video_title TEXT,
            video_url TEXT,
            channel_title TEXT,
            published_at TIMESTAMP,
            detected_at TIMESTAMP
        )
        """
    )

    # Insert mock game with growth
    now = datetime.now(timezone.utc)
    db.execute("INSERT INTO games VALUES (123, 'Test Game', 10000, ?)", (now,))
    db.execute(
        "INSERT INTO games VALUES (123, 'Test Game', 5000, ? - INTERVAL 7 DAY)",
        (now,)
    )
    db.execute("INSERT INTO game_metadata VALUES (123, 'Test Game')")

    # Insert mock YouTube video mentioning the game
    db.execute(
        """
        INSERT INTO youtube_videos VALUES
        ('vid1', 'TestChannel', 'NEW Test Game Mechanic Update!', ?, 50000)
        """,
        (now,)
    )

    db.commit()
    db.close()

    yield db_path

    try:
        os.unlink(db_path)
    except FileNotFoundError:
        pass
    except PermissionError:
        pass



def test_detect_mechanic_spikes_with_mock_data(mock_db):
    """Test full spike detection pipeline with mock data."""
    spikes = detect_mechanic_spikes(
        db_path=mock_db,
        lookback_hours=48,
        growth_threshold=0.25,
        persist=False
    )
    
    assert len(spikes) >= 1
    spike = spikes[0]
    
    assert spike.universe_id == 123
    assert spike.game_name == "Test Game"
    assert spike.current_ccu == 10000
    assert spike.week_ago_ccu == 5000
    assert spike.growth_percent == 100.0  # 100% growth
    assert "Mechanic" in spike.mechanic
    assert spike.video_url.startswith("https://youtube.com")


def test_persistence_and_retrieval(mock_db):
    """Test that spikes are persisted and retrievable."""
    # Detect and persist
    spikes = detect_mechanic_spikes(
        db_path=mock_db,
        persist=True
    )
    
    assert len(spikes) >= 1
    
    # Retrieve historical
    historical = get_historical_spikes(db_path=mock_db, limit=10)
    
    assert len(historical) == len(spikes)
    assert historical[0].game_name == spikes[0].game_name
    assert historical[0].growth_percent == spikes[0].growth_percent


def test_no_spikes_when_no_growth(mock_db):
    """Test that no spikes detected when growth threshold not met."""
    db = duckdb.connect(mock_db)
    
    # Add game with minimal growth (< 25%)
    now = datetime.now(timezone.utc)
    db.execute("INSERT INTO games VALUES (456, 'Slow Game', 5100, ?)", (now,))
    db.execute("INSERT INTO games VALUES (456, 'Slow Game', 5000, ? - INTERVAL 7 DAY)", (now,))
    db.execute("INSERT INTO game_metadata VALUES (456, 'Slow Game')")
    
    db.commit()
    db.close()
    
    spikes = detect_mechanic_spikes(
        db_path=mock_db,
        growth_threshold=0.25,  # 25% required, only has 2%
        persist=False
    )
    
    # Should not include the slow game
    assert not any(s.universe_id == 456 for s in spikes)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
