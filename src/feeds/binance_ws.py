import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field

import httpx
import numpy as np
import websockets

from src.config import Settings

logger = logging.getLogger(__name__)

WS_ENDPOINTS = [
    "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade",
    "wss://stream.binance.us:9443/stream?streams=btcusdt@trade/ethusdt@trade",
]

REST_FALLBACK_URL = "https://api.coingecko.com/api/v3/simple/price"


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
        self._rest_task: asyncio.Task | None = None
        self._ws_connected = False

    def get_price(self, symbol: str) -> PriceState:
        return self._prices.get(symbol.upper(), PriceState())

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run_forever())
        self._rest_task = asyncio.create_task(self._rest_fallback_loop())

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()
        for task in (self._task, self._rest_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def _run_forever(self) -> None:
        while self._running:
            for url in WS_ENDPOINTS:
                if not self._running:
                    return
                try:
                    logger.info("Trying WebSocket: %s", url.split("/stream")[0])
                    async with websockets.connect(
                        url, ping_interval=30, ping_timeout=10
                    ) as ws:
                        self._ws = ws
                        self._ws_connected = True
                        logger.info("WebSocket connected: %s", url.split("/stream")[0])
                        await self._consume(ws)
                except asyncio.CancelledError:
                    return
                except Exception as exc:
                    logger.warning("WebSocket failed (%s): %s", url.split("/stream")[0], exc)
                    self._ws_connected = False
                    continue

            if not self._running:
                return
            logger.warning("All WebSocket endpoints failed, retrying in 10s (REST fallback active)")
            await asyncio.sleep(10)

    async def _rest_fallback_loop(self) -> None:
        await asyncio.sleep(5)
        while self._running:
            try:
                if self._ws_connected:
                    btc = self._prices["BTC"]
                    if btc.last_update > 0 and (time.time() - btc.last_update) < 5:
                        await asyncio.sleep(3)
                        continue

                await self._fetch_rest_prices()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.debug("REST fallback error: %s", exc)
            await asyncio.sleep(3)

    async def _fetch_rest_prices(self) -> None:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                REST_FALLBACK_URL,
                params={"ids": "bitcoin,ethereum", "vs_currencies": "usd"},
            )
            if resp.status_code != 200:
                logger.debug("CoinGecko REST returned %d", resp.status_code)
                return
            data = resp.json()
            ts = time.time()

            btc_price = data.get("bitcoin", {}).get("usd")
            eth_price = data.get("ethereum", {}).get("usd")

            async with self._lock:
                if btc_price:
                    state = self._prices["BTC"]
                    state.current_price = float(btc_price)
                    state.last_update = ts
                    state.prices.append(float(btc_price))
                    state.timestamps.append(ts)
                if eth_price:
                    state = self._prices["ETH"]
                    state.current_price = float(eth_price)
                    state.last_update = ts
                    state.prices.append(float(eth_price))
                    state.timestamps.append(ts)

            source = "REST/CoinGecko"
            if btc_price:
                logger.info("Price update (%s): BTC=$%s ETH=$%s", source, btc_price, eth_price)

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
