import argparse
import asyncio
import logging
import sys
from pathlib import Path

from src.alerts.telegram import TelegramAlerter
from src.config import Settings
from src.engine.executor import Executor
from src.engine.risk import RiskManager
from src.engine.signals import SignalEngine
from src.feeds.binance_ws import BinanceFeed
from src.feeds.polymarket_feed import PolymarketFeed
from src.storage.db import Database

LOG_FILE = Path(__file__).resolve().parent.parent / "polybot.log"


def _setup_logging() -> None:
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(LOG_FILE)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    root.addHandler(fh)

    sh = logging.StreamHandler(sys.stderr)
    sh.setLevel(logging.WARNING)
    sh.setFormatter(fmt)
    root.addHandler(sh)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="polybot",
        description="Polymarket Latency Arbitrage Bot",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Log trades without executing (no real orders)",
    )
    parser.add_argument(
        "--web",
        action="store_true",
        default=False,
        help="Launch browser dashboard instead of terminal TUI (default port 8080)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Port for web dashboard (default: 8080)",
    )
    return parser.parse_args()


def _build_components(settings: Settings):
    db = Database()
    telegram = TelegramAlerter(settings.telegram_bot_token, settings.telegram_chat_id)
    binance = BinanceFeed(settings)
    polymarket = PolymarketFeed(settings)
    signal_engine = SignalEngine(settings, binance, polymarket)
    executor = Executor(settings)
    risk = RiskManager(settings=settings)
    return db, telegram, binance, polymarket, signal_engine, executor, risk


def main() -> None:
    args = _parse_args()
    _setup_logging()
    logger = logging.getLogger(__name__)

    settings = Settings()
    settings.dry_run = args.dry_run

    if settings.dry_run:
        logger.info("Starting in DRY-RUN mode — no real orders will be placed")
    else:
        if not settings.poly_private_key or settings.poly_private_key == "0x...":
            logger.error(
                "POLY_PRIVATE_KEY not set. Set it in .env or use --dry-run for testing."
            )
            sys.exit(1)

    db, telegram, binance, polymarket, signal_engine, executor, risk = _build_components(settings)
    asyncio.run(db.init())

    if args.web:
        import uvicorn
        from src.dashboard.web import WebDashboard

        dashboard = WebDashboard(
            settings=settings,
            binance=binance,
            polymarket=polymarket,
            signal_engine=signal_engine,
            executor=executor,
            risk=risk,
            telegram=telegram,
            db=db,
        )
        mode = "DRY-RUN" if settings.dry_run else "LIVE"
        print(f"\n  Polymarket Arbitrage Bot [{mode}]")
        print(f"  Dashboard: http://localhost:{args.port}\n")
        uvicorn.run(dashboard.app, host="0.0.0.0", port=args.port, log_level="warning")
    else:
        from src.dashboard.app import PolybotApp

        app = PolybotApp(
            settings=settings,
            binance=binance,
            polymarket=polymarket,
            signal_engine=signal_engine,
            executor=executor,
            risk=risk,
            telegram=telegram,
            db=db,
        )
        app.run()

    logger.info("Bot shutdown complete")


if __name__ == "__main__":
    main()
