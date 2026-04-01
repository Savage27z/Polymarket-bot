from __future__ import annotations

import asyncio
import datetime as dt
import logging
import time

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.widgets import DataTable, Footer, Header, RichLog, Static

from src.alerts.telegram import TelegramAlerter
from src.config import Settings
from src.engine.executor import Executor
from src.engine.risk import Position, RiskManager
from src.engine.signals import Signal, SignalEngine
from src.feeds.binance_ws import BinanceFeed
from src.feeds.polymarket_feed import PolymarketFeed
from src.storage.db import Database

logger = logging.getLogger(__name__)

TCSS = """
Screen {
    layout: vertical;
}
#portfolio-box {
    height: 5;
    border: solid green;
    padding: 0 1;
}
#mid-row {
    height: 8;
    layout: horizontal;
}
#feeds-box {
    width: 1fr;
    border: solid cyan;
    padding: 0 1;
}
#signals-box {
    width: 1fr;
    border: solid yellow;
    padding: 0 1;
}
#positions-box {
    height: 8;
    border: solid magenta;
}
#trades-box {
    height: 10;
    border: solid blue;
}
#log-box {
    height: 1fr;
    min-height: 6;
    border: solid white;
}
.title-label {
    text-style: bold;
}
.kill-active {
    color: red;
    text-style: bold blink;
}
"""


class PortfolioWidget(Static):
    pass


class FeedsWidget(Static):
    pass


class SignalsWidget(Static):
    pass


class PolybotApp(App):
    CSS = TCSS
    TITLE = "Polymarket Arbitrage Bot"
    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("k", "toggle_kill", "Kill Switch"),
        Binding("d", "toggle_dry", "Dry Run"),
        Binding("r", "refresh_now", "Refresh"),
    ]

    def __init__(
        self,
        settings: Settings,
        binance: BinanceFeed,
        polymarket: PolymarketFeed,
        signal_engine: SignalEngine,
        executor: Executor,
        risk: RiskManager,
        telegram: TelegramAlerter,
        db: Database,
    ) -> None:
        super().__init__()
        self.settings = settings
        self.binance = binance
        self.polymarket = polymarket
        self.signal_engine = signal_engine
        self.executor = executor
        self.risk = risk
        self.telegram = telegram
        self.db = db
        self._latest_signals: list[Signal] = []
        self._log_widget: RichLog | None = None
        self._kill_switch_alerted: bool = False

    def compose(self) -> ComposeResult:
        yield Header()
        yield PortfolioWidget(id="portfolio-box")
        with Horizontal(id="mid-row"):
            yield FeedsWidget(id="feeds-box")
            yield SignalsWidget(id="signals-box")
        yield DataTable(id="positions-box")
        yield DataTable(id="trades-box")
        yield RichLog(id="log-box", highlight=True, markup=True, max_lines=200)
        yield Footer()

    def on_mount(self) -> None:
        self._log_widget = self.query_one("#log-box", RichLog)
        self._setup_positions_table()
        self._setup_trades_table()
        self._log("Bot started" + (" [DRY-RUN MODE]" if self.settings.dry_run else " [LIVE MODE]"))

        self.run_worker(self._binance_worker(), exclusive=False)
        self.run_worker(self._discovery_worker(), exclusive=False)
        self.run_worker(self._signal_loop(), exclusive=False)
        self.run_worker(self._resolution_worker(), exclusive=False)
        self.run_worker(self._daily_reset_worker(), exclusive=False)
        self.set_interval(1.0, self._refresh_ui)

    def _setup_positions_table(self) -> None:
        table = self.query_one("#positions-box", DataTable)
        table.add_columns("Market", "Side", "Entry", "Size", "Edge", "Time Left")

    def _setup_trades_table(self) -> None:
        table = self.query_one("#trades-box", DataTable)
        table.add_columns("Time", "Market", "Side", "Price", "Size", "Result")

    def _log(self, msg: str) -> None:
        if self._log_widget:
            ts = dt.datetime.now(dt.timezone.utc).strftime("%H:%M:%S")
            self._log_widget.write(f"[dim]{ts}[/dim] {msg}")

    async def _refresh_ui(self) -> None:
        try:
            await self._update_portfolio()
            self._update_feeds()
            self._update_signals()
            await self._update_positions()
            await self._update_trades()
        except Exception as exc:
            logger.debug("UI refresh error: %s", exc)

    async def _update_portfolio(self) -> None:
        widget = self.query_one("#portfolio-box", PortfolioWidget)
        stats = await self.db.get_stats()
        pv = self.settings.initial_portfolio_value + stats["total_pnl"]
        daily_pnl = self.risk.daily_pnl
        daily_pct = (daily_pnl / self.risk.daily_start_value * 100) if self.risk.daily_start_value > 0 else 0
        mode = "[bold red blink]DRY-RUN[/]" if self.settings.dry_run else "[bold green]LIVE[/]"
        kill = "[bold red blink]ON[/]" if self.risk.kill_switch_active else "[green]OFF[/]"
        text = (
            f"[bold]Portfolio[/] Value: [bold]${pv:.2f}[/]    "
            f"Daily P&L: {'[green]' if daily_pnl >= 0 else '[red]'}{daily_pnl:+.2f} ({daily_pct:+.1f}%)[/]    "
            f"Win Rate: {stats['win_rate']:.1f}%   Trades: {stats['total_trades']}\n"
            f"Mode: {mode}    Kill Switch: {kill}    Max Position: ${self.settings.max_position_usdc:.2f}"
        )
        widget.update(text)

    def _update_feeds(self) -> None:
        widget = self.query_one("#feeds-box", FeedsWidget)
        btc = self.binance.get_price("BTC")
        eth = self.binance.get_price("ETH")
        now = time.time()
        btc_age = f"{now - btc.last_update:.1f}s" if btc.last_update > 0 else "N/A"
        eth_age = f"{now - eth.last_update:.1f}s" if eth.last_update > 0 else "N/A"
        text = (
            f"[bold]Live Feeds[/]\n"
            f"BTC: [bold]${btc.current_price:,.2f}[/] ({btc_age})  "
            f"Vol: {btc.volatility:.6f}\n"
            f"ETH: [bold]${eth.current_price:,.2f}[/] ({eth_age})  "
            f"Vol: {eth.volatility:.6f}\n"
            f"Markets: {len(self.polymarket.markets)}"
        )
        widget.update(text)

    def _update_signals(self) -> None:
        widget = self.query_one("#signals-box", SignalsWidget)
        if not self._latest_signals:
            widget.update("[bold]Active Signals[/]\n[dim]No signals[/dim]")
            return
        lines = ["[bold]Active Signals[/]"]
        for sig in self._latest_signals[:4]:
            color = "green" if sig.edge > 0 else "red"
            executable = "\u2713" if self.signal_engine.should_execute(sig, self.risk.kill_switch_active) else " "
            lines.append(
                f"[{color}]{sig.market.asset}-{sig.market.timeframe} "
                f"{sig.side} Edge:{sig.edge:+.1%} Conf:{sig.confidence:.0%} {executable}[/]"
            )
        widget.update("\n".join(lines))

    async def _update_positions(self) -> None:
        table = self.query_one("#positions-box", DataTable)
        table.clear()
        for pos in self.risk.open_positions.values():
            remaining = pos.end_time - time.time()
            table.add_row(
                f"{pos.asset}-{pos.timeframe}",
                pos.side,
                f"${pos.entry_price:.2f}",
                f"${pos.size_usdc:.2f}",
                f"{pos.edge:.1%}",
                f"{max(0, remaining):.0f}s",
            )

    async def _update_trades(self) -> None:
        table = self.query_one("#trades-box", DataTable)
        table.clear()
        trades = await self.db.get_recent_trades(10)
        for t in trades:
            ts = dt.datetime.fromtimestamp(
                t["timestamp"], tz=dt.timezone.utc
            ).strftime("%H:%M")
            result = ""
            if t.get("pnl") is not None:
                result = f"{'+'if t['pnl'] >= 0 else ''}{t['pnl']:.2f}"
            elif t.get("status") == "open":
                result = "open"
            table.add_row(
                ts,
                f"{t['asset']}-{t['timeframe']}",
                t["side"],
                f"${t['entry_price']:.2f}",
                f"${t['size_usdc']:.2f}",
                result,
            )

    async def _binance_worker(self) -> None:
        self._log("Starting Binance WebSocket feed...")
        try:
            await self.binance.start()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self._log(f"[red]Binance feed error: {exc}[/]")
            await self.telegram.error_alert(f"Binance feed error: {exc}")

    async def _discovery_worker(self) -> None:
        self._log("Starting market discovery...")
        while True:
            try:
                found = await self.polymarket.discover_markets()
                for m in found:
                    end_str = dt.datetime.fromtimestamp(
                        m.end_time, tz=dt.timezone.utc
                    ).strftime("%H:%M UTC")
                    self._log(
                        f"[cyan]Market: {m.asset}-{m.timeframe} "
                        f"Strike=${m.strike_price:,.2f} Exp={end_str}[/]"
                    )
                    await self.telegram.market_found_alert(
                        m.asset, m.timeframe, m.strike_price, end_str
                    )
            except asyncio.CancelledError:
                return
            except Exception as exc:
                self._log(f"[red]Discovery error: {exc}[/]")
            await asyncio.sleep(self.settings.market_discovery_interval)

    async def _signal_loop(self) -> None:
        self._log("Starting signal engine...")
        await asyncio.sleep(3)
        while True:
            try:
                await self.polymarket.poll_prices()
                signals = await self.signal_engine.scan()
                self._latest_signals = signals

                if signals:
                    best = max(signals, key=lambda s: abs(s.edge))
                    self._log(
                        f"[cyan]Signals: {len(signals)} | Best: {best.market.asset}-{best.market.timeframe} "
                        f"{best.side} edge={best.edge:+.1%} conf={best.confidence:.0%} "
                        f"CEX=${best.cex_price:,.2f} strike=${best.strike_price:,.2f}[/]"
                    )

                for sig in signals:
                    if not self.signal_engine.should_execute(
                        sig, self.risk.kill_switch_active
                    ):
                        continue

                    if sig.market.condition_id in self.risk.open_positions:
                        continue

                    self._log(
                        f"[bold green]EXECUTING: {sig.market.asset}-{sig.market.timeframe} "
                        f"{sig.side} edge={sig.edge:.1%} conf={sig.confidence:.0%}[/]"
                    )

                    result = await self.executor.execute(sig)

                    if result.get("success"):
                        order_id = result.get("order_id", "")
                        trade_db_id = await self.db.log_trade({
                            "timestamp": time.time(),
                            "market_condition_id": sig.market.condition_id,
                            "market_question": sig.market.question,
                            "asset": sig.market.asset,
                            "timeframe": sig.market.timeframe,
                            "side": sig.side,
                            "entry_price": result.get("price", 0),
                            "size_usdc": result.get("size_usdc", 0),
                            "size_shares": result.get("size_shares", 0),
                            "edge": sig.edge,
                            "confidence": sig.confidence,
                            "implied_prob": sig.implied_prob,
                            "market_prob": sig.market_prob,
                            "cex_price_at_entry": sig.cex_price,
                            "strike_price": sig.strike_price,
                            "order_id": order_id,
                            "status": "open",
                            "dry_run": self.settings.dry_run,
                        })

                        self.risk.add_position(
                            Position(
                                condition_id=sig.market.condition_id,
                                market_question=sig.market.question,
                                asset=sig.market.asset,
                                timeframe=sig.market.timeframe,
                                side=sig.side,
                                entry_price=result.get("price", 0),
                                size_usdc=result.get("size_usdc", 0),
                                size_shares=result.get("size_shares", 0),
                                edge=sig.edge,
                                confidence=sig.confidence,
                                cex_price_at_entry=sig.cex_price,
                                strike_price=sig.strike_price,
                                order_id=order_id,
                                opened_at=time.time(),
                                end_time=sig.market.end_time,
                                dry_run=self.settings.dry_run,
                                trade_db_id=trade_db_id,
                            )
                        )

                        await self.telegram.trade_alert(
                            side=sig.side,
                            asset=sig.market.asset,
                            timeframe=sig.market.timeframe,
                            edge=sig.edge,
                            size=result.get("size_usdc", 0),
                            confidence=sig.confidence,
                            price=result.get("price", 0),
                            dry_run=self.settings.dry_run,
                        )
                    else:
                        err = result.get("error", "unknown")
                        self._log(f"[red]Trade failed: {err}[/]")
                        await self.telegram.error_alert(f"Order execution failed: {err}")

                stats = await self.db.get_stats()
                pv = self.settings.initial_portfolio_value + stats["total_pnl"]
                self.risk.check_drawdown(pv)
                alerts = self.risk.check_drawdown_alerts(pv)
                for threshold in alerts:
                    await self.telegram.drawdown_alert(threshold, pv)
                    self._log(f"[yellow]Drawdown alert: {threshold:.0%}[/]")
                if self.risk.kill_switch_active and not self._kill_switch_alerted:
                    self._kill_switch_alerted = True
                    await self.telegram.kill_switch_alert()
                    self._log("[bold red]KILL SWITCH ACTIVATED[/]")

            except asyncio.CancelledError:
                return
            except Exception as exc:
                self._log(f"[red]Signal loop error: {exc}[/]")
                logger.exception("Signal loop error")
            await asyncio.sleep(self.settings.polymarket_poll_interval)

    async def _resolution_worker(self) -> None:
        await asyncio.sleep(10)
        while True:
            try:
                now = time.time()
                resolved: list[str] = []
                for cid, pos in list(self.risk.open_positions.items()):
                    if now <= pos.end_time + 60:
                        continue

                    try:
                        market_data = await asyncio.to_thread(
                            self.polymarket._clob.get_market, pos.condition_id
                        )
                        if not market_data.get("closed", False):
                            continue
                        tokens = market_data.get("tokens", [])
                        yes_won = any(
                            t.get("winner", False)
                            and t.get("outcome", "").lower() in ("yes", "up")
                            for t in tokens
                        )
                        no_won = any(
                            t.get("winner", False)
                            and t.get("outcome", "").lower() in ("no", "down")
                            for t in tokens
                        )
                        if not yes_won and not no_won:
                            continue
                        won = (pos.side == "YES" and yes_won) or (
                            pos.side == "NO" and no_won
                        )
                    except Exception as exc:
                        logger.debug(
                            "Resolution check failed for %s: %s",
                            pos.condition_id,
                            exc,
                        )
                        continue

                    if won:
                        pnl = pos.size_usdc * ((1.0 / pos.entry_price) - 1.0)
                        status = "won"
                    else:
                        pnl = -pos.size_usdc
                        status = "lost"

                    self.risk.daily_pnl += pnl
                    if pos.trade_db_id:
                        exit_price = 1.0 if won else 0.0
                        await self.db.update_trade_result(
                            pos.trade_db_id, status, exit_price, pnl
                        )
                    resolved.append(cid)
                    self._log(
                        f"[{'green' if won else 'red'}]Resolved: {pos.asset}-{pos.timeframe} "
                        f"{pos.side} -> {status} P&L: {pnl:+.2f}[/]"
                    )

                for cid in resolved:
                    self.risk.remove_position(cid)

                if self.risk.open_positions:
                    stats = await self.db.get_stats()
                    pv = self.settings.initial_portfolio_value + stats["total_pnl"]
                    await self.db.snapshot_portfolio(
                        pv, self.risk.daily_pnl, len(self.risk.open_positions)
                    )

            except asyncio.CancelledError:
                return
            except Exception as exc:
                self._log(f"[red]Resolution error: {exc}[/]")
            await asyncio.sleep(15)

    async def _daily_reset_worker(self) -> None:
        while True:
            try:
                now = dt.datetime.now(dt.timezone.utc)
                tomorrow = (now + dt.timedelta(days=1)).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                wait = (tomorrow - now).total_seconds()
                await asyncio.sleep(wait)

                stats = await self.db.get_stats()
                pv = self.settings.initial_portfolio_value + stats["total_pnl"]
                self.risk.reset_daily(pv)
                self._log("[cyan]Daily risk counters reset[/]")
            except asyncio.CancelledError:
                return
            except Exception as exc:
                self._log(f"[red]Daily reset error: {exc}[/]")
                await asyncio.sleep(60)

    def action_toggle_kill(self) -> None:
        self.risk.kill_switch_active = not self.risk.kill_switch_active
        if not self.risk.kill_switch_active:
            self._kill_switch_alerted = False
        state = "ACTIVE" if self.risk.kill_switch_active else "INACTIVE"
        self._log(f"[yellow]Kill switch toggled: {state}[/]")

    def action_toggle_dry(self) -> None:
        self.settings.dry_run = not self.settings.dry_run
        mode = "DRY-RUN" if self.settings.dry_run else "LIVE"
        self._log(f"[yellow]Mode switched to: {mode}[/]")

    def action_refresh_now(self) -> None:
        self._log("[dim]Manual refresh triggered[/dim]")
