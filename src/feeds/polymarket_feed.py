import asyncio
import logging
import math
import re
import time
from dataclasses import dataclass, field

import httpx
from py_clob_client.client import ClobClient

from src.config import Settings

logger = logging.getLogger(__name__)

SLUG_INTERVALS = {"5m": 300, "15m": 900}
ASSETS = ["btc", "eth"]
PRICE_RE = re.compile(r"\$([0-9,]+(?:\.[0-9]+)?)")


@dataclass
class MarketInfo:
    condition_id: str
    question: str
    asset: str
    timeframe: str
    slug: str
    yes_token_id: str
    no_token_id: str
    yes_price: float
    no_price: float
    end_time: float
    strike_price: float
    tick_size: str
    neg_risk: bool
    min_order_size: float
    last_updated: float


class PolymarketFeed:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._markets: dict[str, MarketInfo] = {}
        self._lock = asyncio.Lock()
        self._running = False
        self._clob = ClobClient(
            host=settings.poly_host, chain_id=settings.poly_chain_id
        )
        self._gamma_semaphore = asyncio.Semaphore(5)
        self._clob_semaphore = asyncio.Semaphore(10)

    @property
    def markets(self) -> dict[str, MarketInfo]:
        return dict(self._markets)

    async def start(self) -> None:
        self._running = True

    async def stop(self) -> None:
        self._running = False

    async def discover_markets(self) -> list[MarketInfo]:
        found: list[MarketInfo] = []
        found.extend(await self._slug_lookup())
        if not found:
            found.extend(await self._tag_search())
        async with self._lock:
            for m in found:
                self._markets[m.condition_id] = m
        return found

    async def poll_prices(self) -> None:
        async with self._lock:
            markets = list(self._markets.values())
        for market in markets:
            if time.time() > market.end_time:
                async with self._lock:
                    self._markets.pop(market.condition_id, None)
                continue
            try:
                yes_mid, no_mid = await self._fetch_midpoints(market)
                async with self._lock:
                    if market.condition_id in self._markets:
                        self._markets[market.condition_id].yes_price = yes_mid
                        self._markets[market.condition_id].no_price = no_mid
                        self._markets[market.condition_id].last_updated = time.time()
            except Exception as exc:
                logger.debug("Price poll failed for %s: %s", market.slug, exc)

    async def get_order_book_depth(self, token_id: str) -> float:
        async with self._clob_semaphore:
            try:
                book = await asyncio.to_thread(self._clob.get_order_book, token_id)
                total = 0.0
                for side in ("bids", "asks"):
                    for level in (book.get(side) or [])[:5]:
                        price = float(level.get("price", 0))
                        size = float(level.get("size", 0))
                        total += price * size
                return total
            except Exception:
                return 0.0

    async def _fetch_midpoints(self, market: MarketInfo) -> tuple[float, float]:
        async with self._clob_semaphore:
            yes_resp = await asyncio.to_thread(
                self._clob.get_midpoint, market.yes_token_id
            )
            no_resp = await asyncio.to_thread(
                self._clob.get_midpoint, market.no_token_id
            )
        yes_mid = float(yes_resp.get("mid", market.yes_price))
        no_mid = float(no_resp.get("mid", market.no_price))
        return yes_mid, no_mid

    async def _slug_lookup(self) -> list[MarketInfo]:
        now = time.time()
        found: list[MarketInfo] = []
        async with httpx.AsyncClient(timeout=10) as http:
            for asset in ASSETS:
                for tf, interval in SLUG_INTERVALS.items():
                    for offset in (0, -1):
                        ts = int(math.floor(now / interval) * interval) + offset * interval
                        slug = f"{asset}-updown-{tf}-{ts}"
                        try:
                            async with self._gamma_semaphore:
                                resp = await http.get(
                                    f"{self._settings.gamma_api_url}/markets",
                                    params={"slug": slug},
                                )
                            if resp.status_code != 200:
                                logger.debug("Slug lookup %s: HTTP %d", slug, resp.status_code)
                                continue
                            data = resp.json()
                            if not data:
                                logger.debug("Slug lookup %s: empty response", slug)
                                continue
                            items = data if isinstance(data, list) else [data]
                            for item in items:
                                cid = item.get("condition_id", "")
                                if not cid:
                                    continue
                                if cid in self._markets:
                                    continue
                                market = self._parse_market(item, asset.upper(), tf, slug)
                                if market:
                                    found.append(market)
                                    logger.info("Slug found market: %s (cid=%s)", slug, cid[:12])
                                else:
                                    logger.debug(
                                        "Slug %s: parse failed for cid=%s q=%s",
                                        slug, cid[:12], item.get("question", "")[:60],
                                    )
                        except Exception as exc:
                            logger.debug("Slug lookup failed for %s: %s", slug, exc)
        return found

    async def _tag_search(self) -> list[MarketInfo]:
        found: list[MarketInfo] = []
        try:
            async with httpx.AsyncClient(timeout=10) as http:
                for search_params in (
                    {"tag": "crypto", "active": "true", "closed": "false", "limit": "50"},
                    {"active": "true", "closed": "false", "limit": "100"},
                ):
                    async with self._gamma_semaphore:
                        resp = await http.get(
                            f"{self._settings.gamma_api_url}/events",
                            params=search_params,
                        )
                    if resp.status_code != 200:
                        logger.debug("Tag search HTTP %d for params %s", resp.status_code, search_params)
                        continue
                    events = resp.json()
                    if not isinstance(events, list):
                        events = [events]
                    logger.debug("Tag search returned %d events (params=%s)", len(events), search_params)
                    for event in events:
                        event_title = (event.get("title") or event.get("question") or "").lower()
                        markets = event.get("markets", [])
                        if not isinstance(markets, list):
                            continue
                        for item in markets:
                            q = (item.get("question") or "").lower()
                            desc = (item.get("description") or "").lower()
                            text = f"{q} {desc} {event_title}"
                            asset = ""
                            if any(k in text for k in ("btc", "bitcoin")):
                                asset = "BTC"
                            elif any(k in text for k in ("eth", "ethereum")):
                                asset = "ETH"
                            else:
                                continue
                            tf = ""
                            if any(k in text for k in ("5 minute", "5m", "5-min")):
                                tf = "5m"
                            elif any(k in text for k in ("15 minute", "15m", "15-min")):
                                tf = "15m"
                            else:
                                continue
                            if not item.get("accepting_orders", True):
                                continue
                            cid = item.get("condition_id", "")
                            if cid in self._markets:
                                continue
                            slug = item.get("market_slug", item.get("slug", ""))
                            market = self._parse_market(item, asset, tf, slug)
                            if market:
                                found.append(market)
                                logger.info("Tag search found: %s-%s q=%s", asset, tf, q[:60])
                    if found:
                        break
        except Exception as exc:
            logger.debug("Tag search failed: %s", exc)
        return found

    def _parse_market(
        self, data: dict, asset: str, timeframe: str, slug: str
    ) -> MarketInfo | None:
        try:
            condition_id = data.get("condition_id", "")
            if not condition_id:
                return None

            question = data.get("question", "")
            tokens = data.get("tokens", [])
            if not tokens or len(tokens) < 2:
                clobTokenIds = data.get("clobTokenIds")
                if clobTokenIds and len(clobTokenIds) >= 2:
                    tokens = [
                        {"token_id": clobTokenIds[0], "outcome": "Yes"},
                        {"token_id": clobTokenIds[1], "outcome": "No"},
                    ]
                else:
                    return None

            yes_token = None
            no_token = None
            for t in tokens:
                outcome = (t.get("outcome") or "").lower()
                if outcome in ("yes", "up"):
                    yes_token = t
                elif outcome in ("no", "down"):
                    no_token = t
            if not yes_token or not no_token:
                if len(tokens) >= 2:
                    yes_token = tokens[0]
                    no_token = tokens[1]
                else:
                    return None

            yes_token_id = yes_token.get("token_id", "")
            no_token_id = no_token.get("token_id", "")
            if not yes_token_id or not no_token_id:
                return None

            yes_price = float(yes_token.get("price", 0.5))
            no_price = float(no_token.get("price", 0.5))

            end_ts = data.get("end_date_iso")
            if end_ts:
                from datetime import datetime, timezone

                end_time = datetime.fromisoformat(
                    end_ts.replace("Z", "+00:00")
                ).timestamp()
            else:
                interval = SLUG_INTERVALS.get(timeframe, 300)
                end_time = time.time() + interval

            strike_price = 0.0
            match = PRICE_RE.search(question)
            if match:
                strike_price = float(match.group(1).replace(",", ""))

            tick_size = "0.01"
            neg_risk = False
            try:
                tick_size = str(self._clob.get_tick_size(condition_id))
            except Exception:
                pass
            try:
                neg_risk = bool(self._clob.get_neg_risk(condition_id))
            except Exception:
                pass

            return MarketInfo(
                condition_id=condition_id,
                question=question,
                asset=asset,
                timeframe=timeframe,
                slug=slug,
                yes_token_id=yes_token_id,
                no_token_id=no_token_id,
                yes_price=yes_price,
                no_price=no_price,
                end_time=end_time,
                strike_price=strike_price,
                tick_size=tick_size,
                neg_risk=neg_risk,
                min_order_size=1.0,
                last_updated=time.time(),
            )
        except Exception as exc:
            logger.debug("Failed to parse market: %s", exc)
            return None
