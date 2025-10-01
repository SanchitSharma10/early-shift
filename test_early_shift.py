"""
test_early_shift.py - Quick test to verify Early Shift is working
"""

import asyncio

import duckdb

from roproxy_client import RoProxyClient


async def test_early_shift() -> bool:
    """Test basic functionality."""

    print("[TEST] Testing Early Shift components...\n")

    print("1) Testing RoProxy connection...")
    client = RoProxyClient()

    snapshot = await client._fetch_universe_snapshot(  # type: ignore[attr-defined]
        await aiohttp_session(),
        994732206,
    )
    if snapshot.ccu > 0:
        print(f"[OK] RoProxy working! Blox Fruits CCU: {snapshot.ccu:,}")
    else:
        print("[FAIL] RoProxy connection failed")
        return False

    print("\n2) Testing database connectivity...")
    try:
        db = duckdb.connect("early_shift.db")
        tables = db.execute("SHOW TABLES").fetchall()
        print(f"[OK] Database accessible. Tables: {[t[0] for t in tables]}")
    except Exception as exc:  # pragma: no cover - smoke test only
        print(f"[FAIL] Database error: {exc}")
        return False

    print("\n3) Polling top universes (sample)...")
    universe_ids = await client.get_top_universe_ids(limit=3)
    await client.poll_top_games(universe_ids)
    print(f"[OK] Collected snapshots for universes: {universe_ids}")

    print("\n" + "=" * 50)
    print("Early Shift is ready!")
    print("=" * 50)
    print("\nNext steps:")
    print("1. Add a studio: python add_studio.py --name 'Studio' --token 'XXX' --database 'YYY'")
    print("2. Run monitoring: python main.py")
    print("3. Check Notion after 7 days for first trends")

    return True


async def aiohttp_session():
    import aiohttp

    if not hasattr(aiohttp_session, "_session"):
        aiohttp_session._session = aiohttp.ClientSession()  # type: ignore[attr-defined]
    return aiohttp_session._session  # type: ignore[attr-defined]


async def close_session():
    session = getattr(aiohttp_session, "_session", None)
    if session:
        await session.close()


if __name__ == "__main__":
    try:
        asyncio.run(test_early_shift())
    finally:
        asyncio.run(close_session())
