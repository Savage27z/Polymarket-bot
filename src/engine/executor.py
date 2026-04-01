import asyncio
import logging
import time

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType

from src.config import Settings
from src.engine.signals import Signal

logger = logging.getLogger(__name__)


def create_trading_client(settings: Settings) -> ClobClient:
    client = ClobClient(
        host=settings.poly_host,
        chain_id=settings.poly_chain_id,
        key=settings.poly_private_key,
        signature_type=settings.poly_signature_type,
        funder=settings.poly_funder_address,
    )
    client.set_api_creds(client.create_or_derive_api_key())
    return client


class Executor:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: ClobClient | None = None

    def _ensure_client(self) -> ClobClient:
        if self._client is None:
            self._client = create_trading_client(self._settings)
        return self._client

    async def execute(self, signal: Signal) -> dict:
        if self._settings.dry_run:
            return await self._dry_run(signal)
        return await self._live_execute(signal)

    async def _dry_run(self, signal: Signal) -> dict:
        token_id = (
            signal.market.yes_token_id
            if signal.side == "YES"
            else signal.market.no_token_id
        )
        price = (
            signal.market.yes_price
            if signal.side == "YES"
            else signal.market.no_price
        )
        order_price = round(min(price + 0.01, 0.99), 2)
        shares = round(signal.recommended_size / order_price, 2) if order_price > 0 else 0

        msg = (
            f"[DRY-RUN] Would execute: BUY {shares} shares of {signal.side} "
            f"@ ${order_price:.2f} on {signal.market.asset}-{signal.market.timeframe} "
            f"(edge={signal.edge:.1%}, conf={signal.confidence:.1%})"
        )
        logger.info(msg)

        return {
            "success": True,
            "dry_run": True,
            "order_id": f"dry-{int(time.time())}",
            "side": signal.side,
            "price": order_price,
            "size_shares": shares,
            "size_usdc": signal.recommended_size,
            "message": msg,
        }

    async def _live_execute(self, signal: Signal) -> dict:
        last_exc: Exception | None = None
        for attempt in range(3):
            try:
                client = self._ensure_client()
                result = await self._place_order(client, signal)
                return result
            except Exception as exc:
                last_exc = exc
                err_msg = str(exc)
                if "L2 AUTH NOT AVAILABLE" in err_msg:
                    logger.warning("Re-deriving API credentials (attempt %d)", attempt + 1)
                    try:
                        self._client = create_trading_client(self._settings)
                    except Exception as auth_exc:
                        logger.error("Failed to re-derive credentials: %s", auth_exc)
                elif "insufficient balance" in err_msg.lower():
                    logger.warning("Insufficient balance, skipping trade")
                    return {"success": False, "error": "insufficient_balance"}
                else:
                    logger.warning(
                        "Order attempt %d failed: %s", attempt + 1, exc
                    )
                if attempt < 2:
                    await asyncio.sleep(2**attempt)

        logger.error("Order execution failed after 3 attempts: %s", last_exc)
        return {"success": False, "error": str(last_exc)}

    async def _place_order(self, client: ClobClient, signal: Signal) -> dict:
        token_id = (
            signal.market.yes_token_id
            if signal.side == "YES"
            else signal.market.no_token_id
        )
        price = (
            signal.market.yes_price
            if signal.side == "YES"
            else signal.market.no_price
        )
        order_price = round(min(price + 0.01, 0.99), 2)
        size = round(signal.recommended_size / order_price, 2) if order_price > 0 else 0

        if size <= 0:
            return {"success": False, "error": "calculated_size_zero"}

        resp = await asyncio.to_thread(
            client.create_and_post_order,
            OrderArgs(
                token_id=token_id,
                price=order_price,
                size=size,
                side="BUY",
            ),
            {
                "tickSize": signal.market.tick_size,
                "negRisk": signal.market.neg_risk,
            },
            OrderType.FOK,
        )

        order_id = ""
        if isinstance(resp, dict):
            order_id = resp.get("orderID", resp.get("order_id", ""))

        logger.info(
            "TRADE EXECUTED: BUY %s shares of %s @ $%.2f on %s-%s (order=%s)",
            size,
            signal.side,
            order_price,
            signal.market.asset,
            signal.market.timeframe,
            order_id,
        )

        return {
            "success": True,
            "dry_run": False,
            "order_id": order_id,
            "side": signal.side,
            "price": order_price,
            "size_shares": size,
            "size_usdc": signal.recommended_size,
            "response": resp,
        }
