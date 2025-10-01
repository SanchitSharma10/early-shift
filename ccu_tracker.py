"""Convenience runner for polling CCU snapshots.
Wraps RoProxyClient to collect the top 200 universes."""
import asyncio

from roproxy_client import RoProxyClient

DEFAULT_LIMIT = 200


async def run(limit: int = DEFAULT_LIMIT) -> None:
    client = RoProxyClient()
    universe_ids = await client.get_top_universe_ids(limit=limit)
    await client.poll_top_games(universe_ids)


if __name__ == "__main__":
    asyncio.run(run())
