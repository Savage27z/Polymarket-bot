import logging

import httpx

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


class TelegramAlerter:
    def __init__(self, token: str, chat_id: str) -> None:
        self._token = token
        self._chat_id = chat_id
        self._enabled = bool(token and chat_id)
        if not self._enabled:
            logger.warning(
                "Telegram alerts disabled — TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set"
            )

    @property
    def enabled(self) -> bool:
        return self._enabled

    async def send(self, message: str) -> None:
        if not self._enabled:
            return
        url = TELEGRAM_API.format(token=self._token)
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    url,
                    json={
                        "chat_id": self._chat_id,
                        "text": message,
                        "parse_mode": "Markdown",
                    },
                )
                if resp.status_code != 200:
                    logger.debug("Telegram send failed: %s", resp.text)
        except Exception as exc:
            logger.debug("Telegram send error: %s", exc)

    async def trade_alert(
        self,
        side: str,
        asset: str,
        timeframe: str,
        edge: float,
        size: float,
        confidence: float,
        price: float,
        dry_run: bool = False,
    ) -> None:
        icon = "\U0001f535" if dry_run else "\U0001f7e2"
        label = "DRY-RUN" if dry_run else "TRADE"
        action = "Would BUY" if dry_run else "BUY"
        msg = (
            f"{icon} *{label}*: {action} {side} on {asset} {timeframe} | "
            f"Edge: {edge:.1%} | Size: ${size:.2f} | "
            f"Confidence: {confidence:.0%} | Price: ${price:.2f}"
        )
        await self.send(msg)

    async def drawdown_alert(
        self, drawdown_pct: float, remaining: float
    ) -> None:
        msg = (
            f"\u26a0\ufe0f *DRAWDOWN*: Portfolio down {drawdown_pct:.1%} today "
            f"(${remaining:.2f} remaining)"
        )
        await self.send(msg)

    async def kill_switch_alert(self) -> None:
        msg = (
            "\U0001f534 *KILL SWITCH ACTIVATED*: "
            "Daily drawdown exceeded 20%. All trading halted."
        )
        await self.send(msg)

    async def market_found_alert(
        self,
        asset: str,
        timeframe: str,
        strike: float,
        end_time_str: str,
    ) -> None:
        msg = (
            f"\U0001f4ca *New market found*: {asset} {timeframe} Up/Down "
            f"(Strike: ${strike:,.2f}) expiring at {end_time_str}"
        )
        await self.send(msg)

    async def error_alert(self, error: str) -> None:
        msg = f"\u274c *ERROR*: {error}"
        await self.send(msg)
