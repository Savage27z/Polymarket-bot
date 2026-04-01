import logging
from pathlib import Path

import aiosqlite

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent.parent / "polybot_trades.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp REAL NOT NULL,
    market_condition_id TEXT NOT NULL,
    market_question TEXT,
    asset TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    side TEXT NOT NULL,
    entry_price REAL NOT NULL,
    size_usdc REAL NOT NULL,
    size_shares REAL NOT NULL,
    edge REAL NOT NULL,
    confidence REAL NOT NULL,
    implied_prob REAL NOT NULL,
    market_prob REAL NOT NULL,
    cex_price_at_entry REAL NOT NULL,
    strike_price REAL NOT NULL,
    order_id TEXT,
    status TEXT DEFAULT 'open',
    exit_price REAL,
    pnl REAL,
    resolved_at REAL,
    dry_run BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp REAL NOT NULL,
    portfolio_value REAL NOT NULL,
    daily_pnl REAL NOT NULL,
    open_positions INTEGER NOT NULL,
    total_trades INTEGER NOT NULL,
    win_rate REAL
);

CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
"""


class Database:
    def __init__(self, path: Path | None = None) -> None:
        self._path = path or DB_PATH

    async def init(self) -> None:
        try:
            async with aiosqlite.connect(self._path) as db:
                await db.executescript(SCHEMA)
                await db.commit()
            logger.info("Database initialized at %s", self._path)
        except Exception as exc:
            logger.error("Database init failed: %s", exc)
            raise

    async def log_trade(self, trade_data: dict) -> int | None:
        cols = [
            "timestamp", "market_condition_id", "market_question", "asset",
            "timeframe", "side", "entry_price", "size_usdc", "size_shares",
            "edge", "confidence", "implied_prob", "market_prob",
            "cex_price_at_entry", "strike_price", "order_id", "status", "dry_run",
        ]
        placeholders = ", ".join(["?"] * len(cols))
        col_str = ", ".join(cols)
        values = [trade_data.get(c) for c in cols]
        try:
            async with aiosqlite.connect(self._path) as db:
                cursor = await db.execute(
                    f"INSERT INTO trades ({col_str}) VALUES ({placeholders})", values
                )
                await db.commit()
                return cursor.lastrowid
        except Exception as exc:
            logger.error("Failed to log trade: %s", exc)
            return None

    async def update_trade_result(
        self, order_id: str, status: str, exit_price: float, pnl: float
    ) -> None:
        try:
            async with aiosqlite.connect(self._path) as db:
                await db.execute(
                    "UPDATE trades SET status=?, exit_price=?, pnl=?, resolved_at=? "
                    "WHERE order_id=?",
                    (status, exit_price, pnl, __import__("time").time(), order_id),
                )
                await db.commit()
        except Exception as exc:
            logger.error("Failed to update trade result: %s", exc)

    async def get_recent_trades(self, limit: int = 10) -> list[dict]:
        try:
            async with aiosqlite.connect(self._path) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    "SELECT * FROM trades ORDER BY timestamp DESC LIMIT ?", (limit,)
                )
                rows = await cursor.fetchall()
                return [dict(r) for r in rows]
        except Exception as exc:
            logger.error("Failed to get recent trades: %s", exc)
            return []

    async def get_open_positions(self) -> list[dict]:
        try:
            async with aiosqlite.connect(self._path) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    "SELECT * FROM trades WHERE status='open' ORDER BY timestamp DESC"
                )
                rows = await cursor.fetchall()
                return [dict(r) for r in rows]
        except Exception as exc:
            logger.error("Failed to get open positions: %s", exc)
            return []

    async def get_stats(self) -> dict:
        try:
            async with aiosqlite.connect(self._path) as db:
                cursor = await db.execute("SELECT COUNT(*) FROM trades")
                total = (await cursor.fetchone())[0]

                cursor = await db.execute(
                    "SELECT COUNT(*) FROM trades WHERE status='won'"
                )
                wins = (await cursor.fetchone())[0]

                cursor = await db.execute(
                    "SELECT COALESCE(SUM(pnl), 0) FROM trades WHERE pnl IS NOT NULL"
                )
                total_pnl = (await cursor.fetchone())[0]

                closed = 0
                cursor = await db.execute(
                    "SELECT COUNT(*) FROM trades WHERE status IN ('won', 'lost')"
                )
                closed = (await cursor.fetchone())[0]

                win_rate = (wins / closed * 100) if closed > 0 else 0.0

                return {
                    "total_trades": total,
                    "wins": wins,
                    "closed": closed,
                    "win_rate": win_rate,
                    "total_pnl": total_pnl,
                }
        except Exception as exc:
            logger.error("Failed to get stats: %s", exc)
            return {
                "total_trades": 0,
                "wins": 0,
                "closed": 0,
                "win_rate": 0.0,
                "total_pnl": 0.0,
            }

    async def snapshot_portfolio(
        self, value: float, pnl: float, positions: int
    ) -> None:
        try:
            stats = await self.get_stats()
            async with aiosqlite.connect(self._path) as db:
                await db.execute(
                    "INSERT INTO portfolio_snapshots "
                    "(timestamp, portfolio_value, daily_pnl, open_positions, total_trades, win_rate) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        __import__("time").time(),
                        value,
                        pnl,
                        positions,
                        stats["total_trades"],
                        stats["win_rate"],
                    ),
                )
                await db.commit()
        except Exception as exc:
            logger.error("Failed to snapshot portfolio: %s", exc)
