import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field

import numpy as np
import websockets

from src.config import Settings

logger = logging.getLogger(__name__)


@dataclass
class PriceState:
    current_price: float = 0.0
    last_update: float = 0.0
    prices: deque = field(default_factory=lambda: deque(maxlen=300))
    timestamps: deque = field(default_factory=lambda: deque(maxlen=300))

    @property
    def volatility(self) -> float:
        if len(self.prices) < 10:
            return 0.0
        prices = list(self.prices)
        log_returns = np.diff(np.log(prices))
        if len(log_returns) == 0:
            return 0.0
        time_span = self.timestamps[-1] - self.timestamps[0]
        if time_span <= 0:
            return 0.0
        return float(np.std(log_returns) * np.sqrt(len(log_returns) / time_span))


class BinanceFeed:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._prices: dict[str, PriceState] = {
            "BTC": PriceState(),
            "ETH": PriceState(),
        }
        self._lock = asyncio.Lock()
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._running = False
        self._task: asyncio.Task | None = None

    def get_price(self, symbol: str) -> PriceState:
        return self._prices.get(symbol.upper(), PriceState())

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run_forever())

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run_forever(self) -> None:
        backoff = 1.0
        url = (
            f"{self._settings.binance_ws_url}"
            "/stream?streams=btcusdt@trade/ethusdt@trade"
        )
        while self._running:
            try:
                async with websockets.connect(
                    url, ping_interval=30, ping_timeout=10
                ) as ws:
                    self._ws = ws
                    backoff = 1.0
                    logger.info("Binance WebSocket connected")
                    await self._consume(ws)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                if not self._running:
                    return
                logger.warning(
                    "Binance WS disconnected (%s), reconnecting in %.0fs",
                    exc,
                    backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    async def _consume(self, ws: websockets.WebSocketClientProtocol) -> None:
        async for raw in ws:
            try:
                msg = json.loads(raw)
                data = msg.get("data", {})
                if data.get("e") != "trade":
                    continue
                symbol_raw = data.get("s", "")
                price = float(data["p"])
                ts = time.time()

                if symbol_raw == "BTCUSDT":
                    key = "BTC"
                elif symbol_raw == "ETHUSDT":
                    key = "ETH"
                else:
                    continue

                async with self._lock:
                    state = self._prices[key]
                    state.current_price = price
                    state.last_update = ts
                    state.prices.append(price)
                    state.timestamps.append(ts)
            except (KeyError, ValueError, TypeError) as exc:
                logger.debug("Skipping malformed Binance message: %s", exc)
