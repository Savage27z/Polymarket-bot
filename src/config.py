import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:
    poly_private_key: str = field(
        default_factory=lambda: os.getenv("POLY_PRIVATE_KEY", "")
    )
    poly_funder_address: str = field(
        default_factory=lambda: os.getenv("POLY_FUNDER_ADDRESS", "")
    )
    poly_host: str = "https://clob.polymarket.com"
    poly_chain_id: int = 137
    poly_signature_type: int = 1

    binance_ws_url: str = "wss://stream.binance.com:9443"

    gamma_api_url: str = "https://gamma-api.polymarket.com"

    min_edge_detection: float = 0.02
    min_edge_execution: float = 0.03
    max_position_usdc: float = 1.0
    max_position_pct: float = 0.08
    min_confidence: float = 0.60
    kelly_fraction: float = 0.5

    max_daily_drawdown: float = 0.20
    initial_portfolio_value: float = field(
        default_factory=lambda: float(os.getenv("INITIAL_PORTFOLIO_USDC", "100"))
    )

    telegram_bot_token: str = field(
        default_factory=lambda: os.getenv("TELEGRAM_BOT_TOKEN", "")
    )
    telegram_chat_id: str = field(
        default_factory=lambda: os.getenv("TELEGRAM_CHAT_ID", "")
    )

    market_discovery_interval: int = 30
    polymarket_poll_interval: float = 1.0

    dry_run: bool = False

    drawdown_alert_thresholds: list[float] = field(
        default_factory=lambda: [0.05, 0.10, 0.15, 0.20]
    )
