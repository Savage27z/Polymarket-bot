import logging
import time
from dataclasses import dataclass

import numpy as np
from scipy.stats import norm

from src.config import Settings
from src.engine.risk import calculate_kelly_size
from src.feeds.binance_ws import BinanceFeed
from src.feeds.polymarket_feed import MarketInfo, PolymarketFeed, SLUG_INTERVALS

logger = logging.getLogger(__name__)


@dataclass
class Signal:
    market: MarketInfo
    side: str
    edge: float
    confidence: float
    implied_prob: float
    market_prob: float
    cex_price: float
    strike_price: float
    time_remaining: float
    timestamp: float
    recommended_size: float


def calculate_implied_probability(
    current_price: float,
    strike_price: float,
    time_remaining_seconds: float,
    volatility_per_second: float,
) -> float:
    if time_remaining_seconds <= 0:
        return 1.0 if current_price >= strike_price else 0.0
    if volatility_per_second <= 0 or current_price <= 0:
        return 0.5
    sigma = volatility_per_second * np.sqrt(time_remaining_seconds)
    if sigma == 0:
        return 1.0 if current_price >= strike_price else 0.0
    d = (np.log(current_price / strike_price) + 0.5 * sigma**2) / sigma
    return float(norm.cdf(d))


def calculate_confidence(
    edge: float,
    time_remaining: float,
    total_duration: float,
    liquidity_depth: float,
    volatility: float,
    price_distance_pct: float,
) -> float:
    time_pct = time_remaining / total_duration if total_duration > 0 else 0
    time_score = min(1.0, time_pct * 2) if time_pct > 0.1 else 0.3
    edge_score = min(1.0, abs(edge) / 0.15)
    liquidity_score = min(1.0, liquidity_depth / 5000)
    vol_score = min(1.0, volatility * 100) if volatility > 0.0001 else 0.3
    distance_score = min(1.0, price_distance_pct / 0.01)

    confidence = (
        0.30 * edge_score
        + 0.25 * distance_score
        + 0.20 * time_score
        + 0.15 * liquidity_score
        + 0.10 * vol_score
    )
    return round(confidence, 4)


class SignalEngine:
    def __init__(
        self,
        settings: Settings,
        binance: BinanceFeed,
        polymarket: PolymarketFeed,
    ) -> None:
        self._settings = settings
        self._binance = binance
        self._polymarket = polymarket

    async def scan(self) -> list[Signal]:
        signals: list[Signal] = []
        for market in self._polymarket.markets.values():
            sig = await self._evaluate(market)
            if sig:
                signals.append(sig)
        return signals

    async def _evaluate(self, market: MarketInfo) -> Signal | None:
        price_state = self._binance.get_price(market.asset)
        if price_state.current_price <= 0:
            return None

        now = time.time()
        time_remaining = market.end_time - now
        if time_remaining <= 30:
            return None

        total_duration = float(SLUG_INTERVALS.get(market.timeframe, 300))

        strike = market.strike_price
        if strike <= 0:
            strike = price_state.current_price

        implied_prob = calculate_implied_probability(
            current_price=price_state.current_price,
            strike_price=strike,
            time_remaining_seconds=time_remaining,
            volatility_per_second=price_state.volatility,
        )

        edge_yes = implied_prob - market.yes_price
        edge_no = (1.0 - implied_prob) - market.no_price

        if abs(edge_yes) >= abs(edge_no):
            side = "YES"
            edge = edge_yes
            market_prob = market.yes_price
        else:
            side = "NO"
            edge = edge_no
            market_prob = market.no_price

        if abs(edge) < self._settings.min_edge_detection:
            return None

        price_distance_pct = (
            abs(price_state.current_price - strike) / strike
            if strike > 0 else 0.0
        )

        token_id = (
            market.yes_token_id if side == "YES" else market.no_token_id
        )
        liquidity = await self._polymarket.get_order_book_depth(token_id)

        confidence = calculate_confidence(
            edge=edge,
            time_remaining=time_remaining,
            total_duration=total_duration,
            liquidity_depth=liquidity,
            volatility=price_state.volatility,
            price_distance_pct=price_distance_pct,
        )

        recommended_size = calculate_kelly_size(
            edge=edge,
            market_price=market_prob,
            portfolio_value=self._settings.initial_portfolio_value,
            kelly_fraction=self._settings.kelly_fraction,
            max_position_pct=self._settings.max_position_pct,
            max_position_usdc=self._settings.max_position_usdc,
        )

        return Signal(
            market=market,
            side=side,
            edge=edge,
            confidence=confidence,
            implied_prob=implied_prob,
            market_prob=market_prob,
            cex_price=price_state.current_price,
            strike_price=market.strike_price,
            time_remaining=time_remaining,
            timestamp=now,
            recommended_size=recommended_size,
        )

    def should_execute(self, signal: Signal, kill_switch: bool) -> bool:
        if kill_switch:
            return False
        if abs(signal.edge) < self._settings.min_edge_execution:
            return False
        if signal.confidence < self._settings.min_confidence:
            return False
        if signal.recommended_size <= 0:
            return False
        if signal.time_remaining <= 30:
            return False
        return True
