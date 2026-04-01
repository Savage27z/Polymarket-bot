import logging
import time
from dataclasses import dataclass, field

from src.config import Settings

logger = logging.getLogger(__name__)


def calculate_kelly_size(
    edge: float,
    market_price: float,
    portfolio_value: float,
    kelly_fraction: float = 0.5,
    max_position_pct: float = 0.08,
    max_position_usdc: float = 1.0,
) -> float:
    if market_price <= 0 or market_price >= 1:
        return 0.0

    win_prob = market_price + edge
    win_prob = max(0.01, min(0.99, win_prob))

    odds = (1.0 / market_price) - 1.0
    if odds <= 0:
        return 0.0

    loss_prob = 1.0 - win_prob
    kelly_full = (odds * win_prob - loss_prob) / odds
    kelly_full = max(0.0, kelly_full)

    size = portfolio_value * kelly_full * kelly_fraction
    pct_cap = portfolio_value * max_position_pct

    return min(size, pct_cap, max_position_usdc)


@dataclass
class Position:
    condition_id: str
    market_question: str
    asset: str
    timeframe: str
    side: str
    entry_price: float
    size_usdc: float
    size_shares: float
    edge: float
    confidence: float
    cex_price_at_entry: float
    strike_price: float
    order_id: str
    opened_at: float
    end_time: float
    dry_run: bool = False
    trade_db_id: int | None = None


@dataclass
class RiskManager:
    settings: Settings
    daily_pnl: float = 0.0
    daily_start_value: float = 0.0
    kill_switch_active: bool = False
    alerted_thresholds: set = field(default_factory=set)
    open_positions: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.daily_start_value <= 0:
            self.daily_start_value = self.settings.initial_portfolio_value

    def check_drawdown(self, current_portfolio_value: float) -> bool:
        if self.daily_start_value <= 0:
            return False
        drawdown = (
            (self.daily_start_value - current_portfolio_value) / self.daily_start_value
        )
        if drawdown >= self.settings.max_daily_drawdown:
            self.kill_switch_active = True
            logger.critical(
                "KILL SWITCH ACTIVATED: drawdown %.1f%% exceeds limit",
                drawdown * 100,
            )
            return True
        return False

    def get_drawdown(self, current_portfolio_value: float) -> float:
        if self.daily_start_value <= 0:
            return 0.0
        return (
            (self.daily_start_value - current_portfolio_value) / self.daily_start_value
        )

    def check_drawdown_alerts(
        self, current_portfolio_value: float
    ) -> list[float]:
        triggered: list[float] = []
        drawdown = self.get_drawdown(current_portfolio_value)
        for threshold in self.settings.drawdown_alert_thresholds:
            if drawdown >= threshold and threshold not in self.alerted_thresholds:
                self.alerted_thresholds.add(threshold)
                triggered.append(threshold)
        return triggered

    def add_position(self, pos: Position) -> None:
        self.open_positions[pos.condition_id] = pos

    def remove_position(self, condition_id: str) -> Position | None:
        return self.open_positions.pop(condition_id, None)

    def reset_daily(self, portfolio_value: float) -> None:
        self.daily_start_value = portfolio_value
        self.daily_pnl = 0.0
        self.kill_switch_active = False
        self.alerted_thresholds.clear()
        logger.info("Daily risk counters reset, portfolio: $%.2f", portfolio_value)
