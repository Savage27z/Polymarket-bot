from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import time
from collections import deque
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

from src.alerts.telegram import TelegramAlerter
from src.config import Settings
from src.engine.executor import Executor
from src.engine.risk import Position, RiskManager
from src.engine.signals import Signal, SignalEngine
from src.feeds.binance_ws import BinanceFeed
from src.feeds.polymarket_feed import PolymarketFeed
from src.storage.db import Database

logger = logging.getLogger(__name__)

LOG_BUFFER_SIZE = 500


class WebDashboard:
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
        self.settings = settings
        self.binance = binance
        self.polymarket = polymarket
        self.signal_engine = signal_engine
        self.executor = executor
        self.risk = risk
        self.telegram = telegram
        self.db = db
        self._latest_signals: list[Signal] = []
        self._log_buffer: deque[str] = deque(maxlen=LOG_BUFFER_SIZE)
        self._kill_switch_alerted: bool = False
        self._clients: set[WebSocket] = set()
        self._tasks: list[asyncio.Task] = []
        self._start_time = time.time()
        self._snapshot_interval = 60

        self.app = FastAPI(title="Polymarket Arbitrage Bot")
        self._register_routes()

    def _log(self, msg: str, level: str = "info") -> None:
        ts = dt.datetime.now(dt.timezone.utc).strftime("%H:%M:%S")
        entry = json.dumps({"ts": ts, "msg": msg, "level": level})
        self._log_buffer.append(entry)
        logger.info(msg)

    def _register_routes(self) -> None:
        app = self.app

        @app.on_event("startup")
        async def startup():
            self._tasks = [
                asyncio.create_task(self._binance_worker()),
                asyncio.create_task(self._discovery_worker()),
                asyncio.create_task(self._signal_loop()),
                asyncio.create_task(self._resolution_worker()),
                asyncio.create_task(self._daily_reset_worker()),
                asyncio.create_task(self._broadcast_loop()),
                asyncio.create_task(self._snapshot_worker()),
            ]
            mode = "DRY-RUN" if self.settings.dry_run else "LIVE"
            self._log(f"Bot started [{mode}]")

        @app.on_event("shutdown")
        async def shutdown():
            for t in self._tasks:
                t.cancel()
            await self.binance.stop()
            self._log("Bot shutdown")

        @app.get("/", response_class=HTMLResponse)
        async def index():
            return DASHBOARD_HTML

        @app.get("/api/pnl_history")
        async def pnl_history():
            data = await self.db.get_pnl_history(200)
            return JSONResponse(data)

        @app.get("/api/snapshots")
        async def snapshots():
            data = await self.db.get_hourly_snapshots(24)
            return JSONResponse(data)

        @app.get("/api/breakdown")
        async def breakdown():
            data = await self.db.get_asset_breakdown()
            return JSONResponse(data)

        @app.websocket("/ws")
        async def ws_endpoint(ws: WebSocket):
            await ws.accept()
            self._clients.add(ws)
            try:
                while True:
                    data = await ws.receive_text()
                    try:
                        msg = json.loads(data)
                        action = msg.get("action")
                        if action == "toggle_kill":
                            self.risk.kill_switch_active = not self.risk.kill_switch_active
                            if not self.risk.kill_switch_active:
                                self._kill_switch_alerted = False
                            state = "ACTIVE" if self.risk.kill_switch_active else "INACTIVE"
                            self._log(f"Kill switch toggled: {state}", "warn" if self.risk.kill_switch_active else "info")
                        elif action == "toggle_dry":
                            self.settings.dry_run = not self.settings.dry_run
                            mode = "DRY-RUN" if self.settings.dry_run else "LIVE"
                            self._log(f"Mode switched to: {mode}", "warn")
                        elif action == "update_settings":
                            vals = msg.get("values", {})
                            if "min_edge_execution" in vals:
                                self.settings.min_edge_execution = float(vals["min_edge_execution"]) / 100
                            if "min_confidence" in vals:
                                self.settings.min_confidence = float(vals["min_confidence"]) / 100
                            if "max_position_usdc" in vals:
                                self.settings.max_position_usdc = float(vals["max_position_usdc"])
                            if "max_concurrent_positions" in vals:
                                self.settings.max_concurrent_positions = int(vals["max_concurrent_positions"])
                            self._log("Settings updated", "info")
                    except json.JSONDecodeError:
                        pass
            except WebSocketDisconnect:
                self._clients.discard(ws)
            except Exception:
                self._clients.discard(ws)

    async def _build_state(self) -> dict:
        stats = await self.db.get_stats()
        pv = self.settings.initial_portfolio_value + stats["total_pnl"]
        daily_pnl = self.risk.daily_pnl
        daily_pct = (daily_pnl / self.risk.daily_start_value * 100) if self.risk.daily_start_value > 0 else 0
        drawdown = self.risk.get_drawdown(pv)

        btc = self.binance.get_price("BTC")
        eth = self.binance.get_price("ETH")
        now = time.time()
        uptime = int(now - self._start_time)

        can_trade, reason = self.risk.can_trade()

        signals = []
        for sig in self._latest_signals[:10]:
            executable = self.signal_engine.should_execute(sig, self.risk.kill_switch_active)
            signals.append({
                "asset": sig.market.asset,
                "timeframe": sig.market.timeframe,
                "side": sig.side,
                "edge": round(sig.edge * 100, 2),
                "confidence": round(sig.confidence * 100, 1),
                "implied_prob": round(sig.implied_prob * 100, 1),
                "market_prob": round(sig.market_prob * 100, 1),
                "cex_price": round(sig.cex_price, 2),
                "strike": round(sig.strike_price, 2),
                "time_remaining": round(sig.time_remaining),
                "size": round(sig.recommended_size, 2),
                "executable": executable,
            })

        positions = []
        for pos in self.risk.open_positions.values():
            remaining = pos.end_time - now
            positions.append({
                "asset": pos.asset,
                "timeframe": pos.timeframe,
                "side": pos.side,
                "entry_price": round(pos.entry_price, 2),
                "size_usdc": round(pos.size_usdc, 2),
                "edge": round(pos.edge * 100, 1),
                "confidence": round(pos.confidence * 100, 1),
                "time_left": max(0, round(remaining)),
                "dry_run": pos.dry_run,
            })

        trades = await self.db.get_recent_trades(30)
        trade_list = []
        for t in trades:
            ts = dt.datetime.fromtimestamp(t["timestamp"], tz=dt.timezone.utc).strftime("%H:%M:%S")
            result = ""
            pnl_val = None
            if t.get("pnl") is not None:
                result = f"{'+'if t['pnl'] >= 0 else ''}{t['pnl']:.2f}"
                pnl_val = t["pnl"]
            elif t.get("status") == "open":
                result = "open"
            trade_list.append({
                "time": ts,
                "asset": t["asset"],
                "timeframe": t["timeframe"],
                "side": t["side"],
                "entry_price": round(t["entry_price"], 2),
                "size_usdc": round(t["size_usdc"], 2),
                "result": result,
                "pnl": pnl_val,
                "status": t.get("status", ""),
                "dry_run": bool(t.get("dry_run")),
                "edge": round(t["edge"] * 100, 1),
                "confidence": round(t["confidence"] * 100, 1),
            })

        logs = []
        for entry in list(self._log_buffer)[-80:]:
            try:
                logs.append(json.loads(entry))
            except Exception:
                logs.append({"ts": "", "msg": entry, "level": "info"})

        return {
            "portfolio": {
                "value": round(pv, 2),
                "daily_pnl": round(daily_pnl, 2),
                "daily_pct": round(daily_pct, 2),
                "total_pnl": round(stats["total_pnl"], 2),
                "win_rate": round(stats["win_rate"], 1),
                "total_trades": stats["total_trades"],
                "wins": stats["wins"],
                "losses": stats["losses"],
                "closed": stats["closed"],
                "avg_win": round(stats["avg_win"], 2),
                "avg_loss": round(stats["avg_loss"], 2),
                "best_trade": round(stats["best_trade"], 2),
                "worst_trade": round(stats["worst_trade"], 2),
                "drawdown": round(drawdown * 100, 1),
            },
            "mode": "DRY-RUN" if self.settings.dry_run else "LIVE",
            "kill_switch": self.risk.kill_switch_active,
            "can_trade": can_trade,
            "trade_block_reason": reason,
            "risk": self.risk.session_stats,
            "settings": {
                "max_position_usdc": self.settings.max_position_usdc,
                "min_edge_execution": round(self.settings.min_edge_execution * 100, 1),
                "min_confidence": round(self.settings.min_confidence * 100, 1),
                "max_concurrent_positions": self.settings.max_concurrent_positions,
                "max_daily_drawdown": round(self.settings.max_daily_drawdown * 100, 1),
            },
            "feeds": {
                "btc_price": round(btc.current_price, 2),
                "btc_age": round(now - btc.last_update, 1) if btc.last_update > 0 else -1,
                "btc_vol": round(btc.volatility, 6),
                "eth_price": round(eth.current_price, 2),
                "eth_age": round(now - eth.last_update, 1) if eth.last_update > 0 else -1,
                "eth_vol": round(eth.volatility, 6),
                "markets_count": len(self.polymarket.markets),
                "ws_connected": self.binance._ws_connected,
            },
            "signals": signals,
            "positions": positions,
            "trades": trade_list,
            "log": logs,
            "uptime": uptime,
            "timestamp": now,
        }

    async def _broadcast_loop(self) -> None:
        while True:
            try:
                if self._clients:
                    state = await self._build_state()
                    payload = json.dumps(state)
                    dead: list[WebSocket] = []
                    for ws in self._clients:
                        try:
                            await ws.send_text(payload)
                        except Exception:
                            dead.append(ws)
                    for ws in dead:
                        self._clients.discard(ws)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.debug("Broadcast error: %s", exc)
            await asyncio.sleep(1.0)

    async def _snapshot_worker(self) -> None:
        await asyncio.sleep(30)
        while True:
            try:
                stats = await self.db.get_stats()
                pv = self.settings.initial_portfolio_value + stats["total_pnl"]
                await self.db.snapshot_portfolio(pv, self.risk.daily_pnl, len(self.risk.open_positions))
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.debug("Snapshot error: %s", exc)
            await asyncio.sleep(self._snapshot_interval)

    async def _binance_worker(self) -> None:
        self._log("Starting price feeds...")
        try:
            await self.binance.start()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self._log(f"Binance feed error: {exc}", "error")
            await self.telegram.error_alert(f"Binance feed error: {exc}")

    async def _discovery_worker(self) -> None:
        self._log("Starting market discovery...")
        while True:
            try:
                found = await self.polymarket.discover_markets()
                for m in found:
                    end_str = dt.datetime.fromtimestamp(m.end_time, tz=dt.timezone.utc).strftime("%H:%M UTC")
                    self._log(f"Market: {m.asset}-{m.timeframe} Strike=${m.strike_price:,.2f} Exp={end_str}")
                    await self.telegram.market_found_alert(m.asset, m.timeframe, m.strike_price, end_str)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                self._log(f"Discovery error: {exc}", "error")
            await asyncio.sleep(self.settings.market_discovery_interval)

    async def _signal_loop(self) -> None:
        self._log("Starting signal engine...")
        await asyncio.sleep(3)
        while True:
            try:
                btc = self.binance.get_price("BTC")
                if btc.current_price <= 0 or (time.time() - btc.last_update) > 120:
                    await asyncio.sleep(self.settings.polymarket_poll_interval)
                    continue

                await self.polymarket.poll_prices()
                signals = await self.signal_engine.scan()
                self._latest_signals = signals

                if signals:
                    best = max(signals, key=lambda s: abs(s.edge))
                    self._log(
                        f"Signals: {len(signals)} | Best: {best.market.asset}-{best.market.timeframe} "
                        f"{best.side} edge={best.edge:+.1%} conf={best.confidence:.0%}"
                    )

                can_trade, block_reason = self.risk.can_trade()

                for sig in signals:
                    if not can_trade:
                        break
                    if not self.signal_engine.should_execute(sig, self.risk.kill_switch_active):
                        continue
                    if sig.market.condition_id in self.risk.open_positions:
                        continue
                    if len(self.risk.open_positions) >= self.settings.max_concurrent_positions:
                        break

                    self._log(
                        f"EXECUTING: {sig.market.asset}-{sig.market.timeframe} "
                        f"{sig.side} edge={sig.edge:.1%} conf={sig.confidence:.0%}",
                        "warn",
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

                        self.risk.add_position(Position(
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
                        ))

                        tag = "DRY" if self.settings.dry_run else "LIVE"
                        self._log(
                            f"[{tag}] Opened: {sig.market.asset}-{sig.market.timeframe} "
                            f"{sig.side} ${result.get('size_usdc', 0):.2f} @ ${result.get('price', 0):.2f}",
                            "success",
                        )

                        await self.telegram.trade_alert(
                            side=sig.side, asset=sig.market.asset,
                            timeframe=sig.market.timeframe, edge=sig.edge,
                            size=result.get("size_usdc", 0),
                            confidence=sig.confidence,
                            price=result.get("price", 0),
                            dry_run=self.settings.dry_run,
                        )
                    else:
                        err = result.get("error", "unknown")
                        self._log(f"Trade failed: {err}", "error")
                        await self.telegram.error_alert(f"Order failed: {err}")

                stats = await self.db.get_stats()
                pv = self.settings.initial_portfolio_value + stats["total_pnl"]
                self.risk.check_drawdown(pv)
                alerts = self.risk.check_drawdown_alerts(pv)
                for threshold in alerts:
                    await self.telegram.drawdown_alert(threshold, pv)
                    self._log(f"Drawdown alert: {threshold:.0%}", "warn")
                if self.risk.kill_switch_active and not self._kill_switch_alerted:
                    self._kill_switch_alerted = True
                    await self.telegram.kill_switch_alert()
                    self._log("KILL SWITCH ACTIVATED", "error")

            except asyncio.CancelledError:
                return
            except Exception as exc:
                self._log(f"Signal loop error: {exc}", "error")
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
                            t.get("winner", False) and t.get("outcome", "").lower() in ("yes", "up")
                            for t in tokens
                        )
                        no_won = any(
                            t.get("winner", False) and t.get("outcome", "").lower() in ("no", "down")
                            for t in tokens
                        )
                        if not yes_won and not no_won:
                            continue
                        won = (pos.side == "YES" and yes_won) or (pos.side == "NO" and no_won)
                    except Exception as exc:
                        logger.debug("Resolution check failed for %s: %s", pos.condition_id, exc)
                        continue

                    if won:
                        pnl = pos.size_usdc * ((1.0 / pos.entry_price) - 1.0)
                        status = "won"
                    else:
                        pnl = -pos.size_usdc
                        status = "lost"

                    self.risk.record_result(won, pnl)

                    if pos.trade_db_id:
                        exit_price = 1.0 if won else 0.0
                        await self.db.update_trade_result(pos.trade_db_id, status, exit_price, pnl)
                    resolved.append(cid)

                    icon = "+" if won else ""
                    self._log(
                        f"Resolved: {pos.asset}-{pos.timeframe} {pos.side} -> {status.upper()} P&L: {icon}{pnl:.2f}",
                        "success" if won else "error",
                    )

                for cid in resolved:
                    self.risk.remove_position(cid)

                if self.risk.open_positions or resolved:
                    stats = await self.db.get_stats()
                    pv = self.settings.initial_portfolio_value + stats["total_pnl"]
                    await self.db.snapshot_portfolio(pv, self.risk.daily_pnl, len(self.risk.open_positions))

            except asyncio.CancelledError:
                return
            except Exception as exc:
                self._log(f"Resolution error: {exc}", "error")
            await asyncio.sleep(15)

    async def _daily_reset_worker(self) -> None:
        while True:
            try:
                now = dt.datetime.now(dt.timezone.utc)
                tomorrow = (now + dt.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                wait = (tomorrow - now).total_seconds()
                await asyncio.sleep(wait)

                stats = await self.db.get_stats()
                pv = self.settings.initial_portfolio_value + stats["total_pnl"]
                self.risk.reset_daily(pv)
                self._log("Daily risk counters reset")
            except asyncio.CancelledError:
                return
            except Exception as exc:
                self._log(f"Daily reset error: {exc}", "error")
                await asyncio.sleep(60)


DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Polymarket Bot</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');
:root{
  --bg:#060b18;--surface:#0c1225;--card:#111a33;--card-hover:#162040;
  --border:#1c2a4a;--border-light:#263356;
  --text:#e8ecf4;--text-secondary:#8892a8;--text-dim:#5a6580;
  --green:#00d68f;--green-bg:rgba(0,214,143,.08);--green-border:rgba(0,214,143,.2);
  --red:#ff6b6b;--red-bg:rgba(255,107,107,.08);--red-border:rgba(255,107,107,.2);
  --yellow:#ffc857;--yellow-bg:rgba(255,200,87,.08);
  --blue:#4dabf7;--blue-bg:rgba(77,171,247,.08);--blue-border:rgba(77,171,247,.2);
  --purple:#b197fc;--purple-bg:rgba(177,151,252,.08);
  --cyan:#3bc9db;
  --accent:#6c5ce7;--accent-light:#a29bfe;
  --radius:10px;--radius-sm:6px;
}
body{background:var(--bg);color:var(--text);font-family:'Inter',system-ui,sans-serif;font-size:13px;line-height:1.5;min-height:100vh;overflow-x:hidden}
::-webkit-scrollbar{width:6px;height:6px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px}
::-webkit-scrollbar-thumb:hover{background:var(--border-light)}

/* HEADER */
.header{background:linear-gradient(180deg,rgba(12,18,37,.98) 0%,rgba(12,18,37,.95) 100%);backdrop-filter:blur(20px);border-bottom:1px solid var(--border);padding:0 24px;height:56px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100}
.header-left{display:flex;align-items:center;gap:16px}
.logo{display:flex;align-items:center;gap:10px}
.logo svg{width:28px;height:28px}
.logo-text{font-size:16px;font-weight:700;letter-spacing:-.3px}
.logo-text span{color:var(--accent-light)}
.header-meta{display:flex;align-items:center;gap:12px;font-size:12px;color:var(--text-secondary)}
.header-right{display:flex;align-items:center;gap:10px}
.badge{padding:4px 10px;border-radius:20px;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;display:flex;align-items:center;gap:5px}
.badge-live{background:var(--green-bg);color:var(--green);border:1px solid var(--green-border)}
.badge-live::before{content:'';width:6px;height:6px;border-radius:50%;background:var(--green);animation:blink 2s infinite}
.badge-dry{background:var(--blue-bg);color:var(--blue);border:1px solid var(--blue-border)}
.badge-kill{background:var(--red-bg);color:var(--red);border:1px solid var(--red-border);animation:pulse 1.5s infinite}
.badge-cooldown{background:var(--yellow-bg);color:var(--yellow);border:1px solid rgba(255,200,87,.3)}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.3}}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
.btn{padding:6px 14px;border-radius:var(--radius-sm);border:1px solid var(--border);background:var(--surface);color:var(--text-secondary);font-size:11px;cursor:pointer;font-weight:600;transition:all .15s;font-family:inherit;text-transform:uppercase;letter-spacing:.3px}
.btn:hover{background:var(--card);color:var(--text);border-color:var(--border-light)}
.btn-danger{border-color:var(--red-border);color:var(--red)}
.btn-danger:hover{background:var(--red-bg)}
.btn-danger.active{background:var(--red-bg);border-color:var(--red);box-shadow:0 0 20px rgba(255,107,107,.15)}
.dot{width:7px;height:7px;border-radius:50%;display:inline-block}
.dot-ok{background:var(--green);box-shadow:0 0 8px var(--green)}
.dot-err{background:var(--red);box-shadow:0 0 8px var(--red)}
.dot-warn{background:var(--yellow);box-shadow:0 0 8px var(--yellow)}

/* LAYOUT */
.layout{display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;padding:16px 24px;max-width:1600px;margin:0 auto}
.col-span-2{grid-column:span 2}
.col-span-3{grid-column:span 3}

/* CARDS */
.card{background:var(--card);border:1px solid var(--border);border-radius:var(--radius);overflow:hidden;transition:border-color .2s}
.card:hover{border-color:var(--border-light)}
.card-header{padding:12px 16px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between}
.card-title{font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.8px;color:var(--text-dim)}
.card-badge{font-size:11px;color:var(--text-dim);font-variant-numeric:tabular-nums}
.card-body{padding:14px 16px}

/* STATS */
.stats-row{display:grid;grid-template-columns:repeat(6,1fr);gap:10px}
.stat-box{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-sm);padding:14px 16px;position:relative;overflow:hidden}
.stat-box::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:var(--border)}
.stat-box.accent::before{background:var(--accent)}
.stat-box.green::before{background:var(--green)}
.stat-box.red::before{background:var(--red)}
.stat-box.blue::before{background:var(--blue)}
.stat-label{font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.6px;margin-bottom:6px;font-weight:600}
.stat-value{font-size:22px;font-weight:800;font-variant-numeric:tabular-nums;letter-spacing:-.5px;line-height:1.1}
.stat-sub{font-size:11px;color:var(--text-dim);margin-top:4px;font-variant-numeric:tabular-nums}
.pos{color:var(--green)}.neg{color:var(--red)}.neutral{color:var(--text-secondary)}

/* FEEDS */
.feed-item{display:flex;align-items:center;gap:16px;padding:12px 0;border-bottom:1px solid rgba(28,42,74,.5)}
.feed-item:last-child{border-bottom:none}
.feed-icon{width:36px;height:36px;border-radius:8px;display:flex;align-items:center;justify-content:center;font-weight:800;font-size:13px;letter-spacing:-.3px}
.feed-icon.btc{background:rgba(255,200,87,.1);color:var(--yellow)}
.feed-icon.eth{background:rgba(177,151,252,.1);color:var(--purple)}
.feed-data{flex:1}
.feed-price{font-size:20px;font-weight:700;font-variant-numeric:tabular-nums;letter-spacing:-.3px}
.feed-meta{font-size:11px;color:var(--text-dim);display:flex;gap:12px;margin-top:2px}

/* SIGNALS */
.signal-row{display:flex;align-items:center;gap:12px;padding:10px 14px;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-sm);margin-bottom:6px;transition:all .15s}
.signal-row:last-child{margin-bottom:0}
.signal-row.exec{border-color:var(--green-border);background:var(--green-bg)}
.signal-pair{font-weight:700;font-size:13px;min-width:65px}
.signal-side{padding:2px 8px;border-radius:3px;font-size:10px;font-weight:700;letter-spacing:.3px}
.signal-side.yes,.signal-side.up{background:rgba(0,214,143,.12);color:var(--green)}
.signal-side.no,.signal-side.down{background:rgba(255,107,107,.12);color:var(--red)}
.signal-edge{font-size:16px;font-weight:800;font-variant-numeric:tabular-nums;margin-left:auto}
.signal-info{font-size:11px;color:var(--text-dim);text-align:right}

/* TABLE */
table{width:100%;border-collapse:collapse;font-size:12px}
thead th{text-align:left;padding:8px 10px;font-size:10px;text-transform:uppercase;letter-spacing:.5px;color:var(--text-dim);border-bottom:1px solid var(--border);font-weight:600;position:sticky;top:0;background:var(--card);z-index:1}
tbody td{padding:8px 10px;border-bottom:1px solid rgba(28,42,74,.3);font-variant-numeric:tabular-nums}
tbody tr{transition:background .1s}
tbody tr:hover{background:rgba(28,42,74,.3)}
.table-wrap{max-height:280px;overflow-y:auto}

/* LOG */
.log-wrap{height:180px;overflow-y:auto;padding:10px 14px;background:var(--surface);border-radius:var(--radius-sm);border:1px solid var(--border);font-family:'JetBrains Mono',monospace;font-size:11.5px;line-height:1.8}
.log-entry{display:flex;gap:8px;opacity:.85}
.log-entry:hover{opacity:1}
.log-ts{color:var(--text-dim);min-width:58px;flex-shrink:0}
.log-msg{word-break:break-word}
.log-entry.error .log-msg{color:var(--red)}
.log-entry.warn .log-msg{color:var(--yellow)}
.log-entry.success .log-msg{color:var(--green)}

/* CHART */
.chart-wrap{height:200px;position:relative}

/* RISK PANEL */
.risk-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}
.risk-item{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-sm);padding:10px 12px;text-align:center}
.risk-item .val{font-size:18px;font-weight:700;font-variant-numeric:tabular-nums}
.risk-item .lbl{font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.4px;margin-top:2px}

/* EMPTY */
.empty{text-align:center;padding:24px;color:var(--text-dim);font-size:12px}
.tag{display:inline-block;padding:1px 6px;border-radius:3px;font-size:10px;font-weight:600;letter-spacing:.3px}
.tag-dry{background:var(--blue-bg);color:var(--blue)}
.tag-live{background:var(--green-bg);color:var(--green)}

/* SETTINGS */
.settings-row{display:flex;align-items:center;gap:10px;margin-bottom:8px}
.settings-row:last-child{margin-bottom:0}
.settings-label{font-size:11px;color:var(--text-secondary);min-width:120px}
.settings-input{background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--text);padding:5px 8px;font-size:12px;width:80px;font-family:'JetBrains Mono',monospace;text-align:right}
.settings-input:focus{outline:none;border-color:var(--accent)}
.settings-unit{font-size:11px;color:var(--text-dim)}

@media(max-width:1100px){.layout{grid-template-columns:1fr 1fr}.col-span-3{grid-column:span 2}}
@media(max-width:700px){.layout{grid-template-columns:1fr;padding:10px}.col-span-2,.col-span-3{grid-column:1}.stats-row{grid-template-columns:repeat(3,1fr)}.risk-grid{grid-template-columns:repeat(2,1fr)}.header{padding:0 12px}}
</style>
</head>
<body>

<div class="header">
  <div class="header-left">
    <div class="logo">
      <svg viewBox="0 0 28 28" fill="none"><rect width="28" height="28" rx="6" fill="#6c5ce7"/><path d="M8 14l4 4 8-8" stroke="#fff" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/></svg>
      <span class="logo-text">Poly<span>Bot</span></span>
    </div>
    <div class="header-meta">
      <span><span class="dot dot-err" id="ws-dot"></span> <span id="ws-label">Connecting</span></span>
      <span id="uptime-label">--</span>
    </div>
  </div>
  <div class="header-right">
    <span class="badge badge-dry" id="mode-badge">--</span>
    <span class="badge" id="status-badge" style="display:none"></span>
    <button class="btn" id="btn-dry" onclick="send('toggle_dry')">Toggle Mode</button>
    <button class="btn btn-danger" id="btn-kill" onclick="send('toggle_kill')">Kill Switch</button>
  </div>
</div>

<div class="layout">
  <!-- Stats Row -->
  <div class="col-span-3">
    <div class="stats-row" id="stats-row">
      <div class="stat-box accent"><div class="stat-label">Portfolio</div><div class="stat-value" id="pv">$0.00</div><div class="stat-sub" id="pv-sub">--</div></div>
      <div class="stat-box green"><div class="stat-label">Daily P&L</div><div class="stat-value" id="daily-pnl">$0.00</div><div class="stat-sub" id="daily-pct">0.00%</div></div>
      <div class="stat-box"><div class="stat-label">Total P&L</div><div class="stat-value" id="total-pnl">$0.00</div><div class="stat-sub" id="total-sub">0 trades</div></div>
      <div class="stat-box blue"><div class="stat-label">Win Rate</div><div class="stat-value" id="win-rate">0%</div><div class="stat-sub" id="win-detail">0W / 0L</div></div>
      <div class="stat-box"><div class="stat-label">Avg Win / Loss</div><div class="stat-value" id="avg-wl">--</div><div class="stat-sub" id="best-worst">--</div></div>
      <div class="stat-box"><div class="stat-label">Drawdown</div><div class="stat-value" id="drawdown">0%</div><div class="stat-sub" id="dd-limit">Limit: 20%</div></div>
    </div>
  </div>

  <!-- P&L Chart -->
  <div class="card col-span-2">
    <div class="card-header"><span class="card-title">Cumulative P&L</span><span class="card-badge" id="chart-trades">0 trades</span></div>
    <div class="card-body"><div class="chart-wrap"><canvas id="pnl-chart"></canvas></div></div>
  </div>

  <!-- Live Feeds -->
  <div class="card">
    <div class="card-header"><span class="card-title">Live Feeds</span><span class="card-badge" id="markets-count">0 markets</span></div>
    <div class="card-body">
      <div class="feed-item">
        <div class="feed-icon btc">BTC</div>
        <div class="feed-data">
          <div class="feed-price" id="btc-price">$0.00</div>
          <div class="feed-meta"><span id="btc-age">--</span><span id="btc-vol">Vol: --</span></div>
        </div>
      </div>
      <div class="feed-item">
        <div class="feed-icon eth">ETH</div>
        <div class="feed-data">
          <div class="feed-price" id="eth-price">$0.00</div>
          <div class="feed-meta"><span id="eth-age">--</span><span id="eth-vol">Vol: --</span></div>
        </div>
      </div>
    </div>

    <div class="card-header" style="margin-top:4px"><span class="card-title">Risk Status</span></div>
    <div class="card-body">
      <div class="risk-grid">
        <div class="risk-item"><div class="val" id="r-streak">0</div><div class="lbl">Win Streak</div></div>
        <div class="risk-item"><div class="val" id="r-lstreak">0</div><div class="lbl">Loss Streak</div></div>
        <div class="risk-item"><div class="val" id="r-positions">0/3</div><div class="lbl">Positions</div></div>
        <div class="risk-item"><div class="val" id="r-cooldown">--</div><div class="lbl">Cooldown</div></div>
      </div>
    </div>
  </div>

  <!-- Signals -->
  <div class="card col-span-2">
    <div class="card-header"><span class="card-title">Active Signals</span><span class="card-badge" id="sig-count">0</span></div>
    <div class="card-body" id="signals-box"><div class="empty">Scanning for signals...</div></div>
  </div>

  <!-- Settings -->
  <div class="card">
    <div class="card-header"><span class="card-title">Settings</span><button class="btn" onclick="saveSettings()" style="font-size:10px;padding:3px 10px">Save</button></div>
    <div class="card-body">
      <div class="settings-row"><span class="settings-label">Min Edge</span><input class="settings-input" id="set-edge" type="number" step="0.5"><span class="settings-unit">%</span></div>
      <div class="settings-row"><span class="settings-label">Min Confidence</span><input class="settings-input" id="set-conf" type="number" step="5"><span class="settings-unit">%</span></div>
      <div class="settings-row"><span class="settings-label">Max Position</span><input class="settings-input" id="set-maxpos" type="number" step="1"><span class="settings-unit">USDC</span></div>
      <div class="settings-row"><span class="settings-label">Max Positions</span><input class="settings-input" id="set-maxconcur" type="number" step="1" min="1" max="10"><span class="settings-unit">concurrent</span></div>
    </div>
  </div>

  <!-- Open Positions -->
  <div class="card col-span-3">
    <div class="card-header"><span class="card-title">Open Positions</span><span class="card-badge" id="pos-count">0</span></div>
    <div class="table-wrap">
      <table><thead><tr><th>Market</th><th>Side</th><th>Entry</th><th>Size</th><th>Edge</th><th>Confidence</th><th>Time Left</th><th>Type</th></tr></thead><tbody id="pos-body"></tbody></table>
    </div>
    <div class="empty" id="pos-empty">No open positions</div>
  </div>

  <!-- Recent Trades -->
  <div class="card col-span-3">
    <div class="card-header"><span class="card-title">Trade History</span></div>
    <div class="table-wrap" style="max-height:340px">
      <table><thead><tr><th>Time</th><th>Market</th><th>Side</th><th>Price</th><th>Size</th><th>Edge</th><th>Conf</th><th>Result</th><th>Type</th></tr></thead><tbody id="trades-body"></tbody></table>
    </div>
    <div class="empty" id="trades-empty">No trades yet</div>
  </div>

  <!-- Activity Log -->
  <div class="card col-span-3">
    <div class="card-header"><span class="card-title">Activity Log</span></div>
    <div class="card-body"><div class="log-wrap" id="log-box"></div></div>
  </div>
</div>

<script>
let ws, chart, reconnectDelay = 1000, settingsInitialized = false;

function initChart() {
  const ctx = document.getElementById('pnl-chart').getContext('2d');
  chart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: [],
      datasets: [{
        label: 'Cumulative P&L',
        data: [],
        borderColor: '#6c5ce7',
        backgroundColor: 'rgba(108,92,231,.08)',
        borderWidth: 2,
        fill: true,
        tension: 0.3,
        pointRadius: 0,
        pointHitRadius: 10,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: '#111a33',
          borderColor: '#1c2a4a',
          borderWidth: 1,
          titleColor: '#8892a8',
          bodyColor: '#e8ecf4',
          padding: 10,
          displayColors: false,
          callbacks: {
            label: (ctx) => `P&L: $${ctx.parsed.y.toFixed(2)}`
          }
        }
      },
      scales: {
        x: { display: true, grid: { color: 'rgba(28,42,74,.3)' }, ticks: { color: '#5a6580', font: { size: 10 }, maxTicksLimit: 8 } },
        y: { display: true, grid: { color: 'rgba(28,42,74,.3)' }, ticks: { color: '#5a6580', font: { size: 10 }, callback: v => '$' + v.toFixed(2) } }
      },
      interaction: { intersect: false, mode: 'index' }
    }
  });
}

async function loadPnlHistory() {
  try {
    const resp = await fetch('/api/pnl_history');
    const data = await resp.json();
    if (!data.length || !chart) return;
    chart.data.labels = data.map((d,i) => {
      const dt = new Date(d.timestamp * 1000);
      return dt.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'});
    });
    chart.data.datasets[0].data = data.map(d => d.cumulative);
    const last = data[data.length-1].cumulative;
    chart.data.datasets[0].borderColor = last >= 0 ? '#00d68f' : '#ff6b6b';
    chart.data.datasets[0].backgroundColor = last >= 0 ? 'rgba(0,214,143,.06)' : 'rgba(255,107,107,.06)';
    chart.update('none');
    document.getElementById('chart-trades').textContent = data.length + ' resolved';
  } catch(e) {}
}

function connect() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(`${proto}//${location.host}/ws`);
  ws.onopen = () => {
    document.getElementById('ws-dot').className = 'dot dot-ok';
    document.getElementById('ws-label').textContent = 'Connected';
    reconnectDelay = 1000;
  };
  ws.onclose = () => {
    document.getElementById('ws-dot').className = 'dot dot-err';
    document.getElementById('ws-label').textContent = 'Disconnected';
    setTimeout(connect, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 1.5, 10000);
  };
  ws.onerror = () => {};
  ws.onmessage = (evt) => { try { update(JSON.parse(evt.data)); } catch(e) {} };
}

function send(action, extra={}) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify({action, ...extra}));
}

function saveSettings() {
  send('update_settings', { values: {
    min_edge_execution: parseFloat(document.getElementById('set-edge').value),
    min_confidence: parseFloat(document.getElementById('set-conf').value),
    max_position_usdc: parseFloat(document.getElementById('set-maxpos').value),
    max_concurrent_positions: parseInt(document.getElementById('set-maxconcur').value),
  }});
}

function f$(n) { return '$' + n.toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2}); }
function fp(n,d=1) { return n.toFixed(d) + '%'; }
function fmtUptime(s) {
  const h = Math.floor(s/3600), m = Math.floor((s%3600)/60);
  return h > 0 ? `${h}h ${m}m` : `${m}m`;
}

let lastTradeCount = 0;

function update(d) {
  const p = d.portfolio;

  // Portfolio value
  document.getElementById('pv').textContent = f$(p.value);
  document.getElementById('pv-sub').textContent = `Initial: ${f$(d.settings.max_position_usdc)} max/trade`;

  // Daily P&L
  const dpnl = document.getElementById('daily-pnl');
  dpnl.textContent = (p.daily_pnl >= 0 ? '+' : '') + f$(p.daily_pnl);
  dpnl.className = 'stat-value ' + (p.daily_pnl >= 0 ? 'pos' : 'neg');
  const dpct = document.getElementById('daily-pct');
  dpct.textContent = (p.daily_pct >= 0 ? '+' : '') + fp(p.daily_pct);

  // Total P&L
  const tpnl = document.getElementById('total-pnl');
  tpnl.textContent = (p.total_pnl >= 0 ? '+' : '') + f$(p.total_pnl);
  tpnl.className = 'stat-value ' + (p.total_pnl >= 0 ? 'pos' : 'neg');
  document.getElementById('total-sub').textContent = p.total_trades + ' trades';

  // Win Rate
  document.getElementById('win-rate').textContent = fp(p.win_rate);
  document.getElementById('win-rate').className = 'stat-value ' + (p.win_rate >= 50 ? 'pos' : p.closed > 0 ? 'neg' : 'neutral');
  document.getElementById('win-detail').textContent = `${p.wins}W / ${p.losses}L`;

  // Avg Win/Loss
  document.getElementById('avg-wl').textContent = `${f$(p.avg_win)} / ${f$(p.avg_loss)}`;
  document.getElementById('avg-wl').className = 'stat-value';
  document.getElementById('best-worst').textContent = `Best: ${f$(p.best_trade)} | Worst: ${f$(p.worst_trade)}`;

  // Drawdown
  const dd = document.getElementById('drawdown');
  dd.textContent = fp(p.drawdown);
  dd.className = 'stat-value ' + (p.drawdown > 10 ? 'neg' : p.drawdown > 5 ? 'neutral' : 'pos');
  document.getElementById('dd-limit').textContent = `Limit: ${fp(d.settings.max_daily_drawdown)}`;

  // Mode badge
  const mb = document.getElementById('mode-badge');
  mb.textContent = d.mode;
  mb.className = 'badge ' + (d.mode === 'LIVE' ? 'badge-live' : 'badge-dry');

  // Status badge
  const sb = document.getElementById('status-badge');
  if (d.kill_switch) {
    sb.style.display = '';
    sb.className = 'badge badge-kill';
    sb.textContent = 'KILL SWITCH';
  } else if (!d.can_trade && d.trade_block_reason) {
    sb.style.display = '';
    sb.className = 'badge badge-cooldown';
    sb.textContent = d.trade_block_reason;
  } else {
    sb.style.display = 'none';
  }

  // Kill button
  const kb = document.getElementById('btn-kill');
  kb.className = 'btn btn-danger' + (d.kill_switch ? ' active' : '');
  kb.textContent = d.kill_switch ? 'KILL: ON' : 'Kill Switch';

  // Uptime
  document.getElementById('uptime-label').textContent = 'Up: ' + fmtUptime(d.uptime);

  // Feeds
  const f = d.feeds;
  document.getElementById('btc-price').textContent = f$(f.btc_price);
  document.getElementById('eth-price').textContent = f$(f.eth_price);
  document.getElementById('btc-age').textContent = f.btc_age >= 0 ? f.btc_age.toFixed(0) + 's ago' : 'Waiting...';
  document.getElementById('eth-age').textContent = f.eth_age >= 0 ? f.eth_age.toFixed(0) + 's ago' : 'Waiting...';
  document.getElementById('btc-vol').textContent = 'Vol: ' + f.btc_vol.toFixed(6);
  document.getElementById('eth-vol').textContent = 'Vol: ' + f.eth_vol.toFixed(6);
  document.getElementById('markets-count').textContent = f.markets_count + ' markets';

  // Feed WS status
  const wsDot = document.getElementById('ws-dot');
  if (f.ws_connected) {
    wsDot.className = 'dot dot-ok';
  } else if (f.btc_price > 0) {
    wsDot.className = 'dot dot-warn';
  }

  // Risk
  const r = d.risk;
  const wse = document.getElementById('r-streak');
  wse.textContent = r.win_streak;
  wse.className = 'val' + (r.win_streak >= 3 ? ' pos' : '');
  const lse = document.getElementById('r-lstreak');
  lse.textContent = r.loss_streak;
  lse.className = 'val' + (r.loss_streak >= 2 ? ' neg' : '');
  document.getElementById('r-positions').textContent = d.positions.length + '/' + d.settings.max_concurrent_positions;
  document.getElementById('r-cooldown').textContent = r.cooldown_remaining > 0 ? r.cooldown_remaining + 's' : 'Ready';

  // Settings (only init once)
  if (!settingsInitialized && d.settings) {
    document.getElementById('set-edge').value = d.settings.min_edge_execution;
    document.getElementById('set-conf').value = d.settings.min_confidence;
    document.getElementById('set-maxpos').value = d.settings.max_position_usdc;
    document.getElementById('set-maxconcur').value = d.settings.max_concurrent_positions;
    settingsInitialized = true;
  }

  // Signals
  const sc = document.getElementById('signals-box');
  document.getElementById('sig-count').textContent = d.signals.length;
  if (!d.signals.length) {
    sc.innerHTML = '<div class="empty">No active signals</div>';
  } else {
    sc.innerHTML = d.signals.map(s => `
      <div class="signal-row ${s.executable ? 'exec' : ''}">
        <span class="signal-pair">${s.asset}-${s.timeframe}</span>
        <span class="signal-side ${s.side.toLowerCase()}">${s.side}</span>
        <span style="font-size:11px;color:var(--text-dim)">CEX: ${f$(s.cex_price)} | Strike: ${f$(s.strike)}</span>
        <span class="signal-edge ${s.edge >= 0 ? 'pos' : 'neg'}">${s.edge >= 0 ? '+' : ''}${s.edge.toFixed(1)}%</span>
        <span class="signal-info">Conf: ${s.confidence.toFixed(0)}% | ${s.time_remaining}s | ${f$(s.size)}</span>
      </div>
    `).join('');
  }

  // Positions
  const pb = document.getElementById('pos-body');
  const pe = document.getElementById('pos-empty');
  document.getElementById('pos-count').textContent = d.positions.length;
  if (!d.positions.length) {
    pb.innerHTML = '';
    pe.style.display = '';
  } else {
    pe.style.display = 'none';
    pb.innerHTML = d.positions.map(p => {
      const tl = p.time_left;
      const tlColor = tl < 30 ? 'neg' : tl < 60 ? 'neutral' : '';
      return `<tr>
        <td><strong>${p.asset}-${p.timeframe}</strong></td>
        <td><span class="signal-side ${p.side.toLowerCase()}">${p.side}</span></td>
        <td>${f$(p.entry_price)}</td>
        <td>${f$(p.size_usdc)}</td>
        <td class="${p.edge >= 0 ? 'pos' : 'neg'}">${p.edge >= 0 ? '+' : ''}${p.edge.toFixed(1)}%</td>
        <td>${p.confidence.toFixed(0)}%</td>
        <td class="${tlColor}">${tl}s</td>
        <td>${p.dry_run ? '<span class="tag tag-dry">DRY</span>' : '<span class="tag tag-live">LIVE</span>'}</td>
      </tr>`;
    }).join('');
  }

  // Trades
  const tb = document.getElementById('trades-body');
  const te = document.getElementById('trades-empty');
  if (!d.trades.length) {
    tb.innerHTML = '';
    te.style.display = '';
  } else {
    te.style.display = 'none';
    tb.innerHTML = d.trades.map(t => {
      let rc = '', icon = '';
      if (t.status === 'won') { rc = 'pos'; icon = '+'; }
      else if (t.status === 'lost') { rc = 'neg'; }
      return `<tr>
        <td>${t.time}</td>
        <td><strong>${t.asset}-${t.timeframe}</strong></td>
        <td><span class="signal-side ${t.side.toLowerCase()}">${t.side}</span></td>
        <td>${f$(t.entry_price)}</td>
        <td>${f$(t.size_usdc)}</td>
        <td class="${t.edge >= 0 ? 'pos' : 'neg'}">${t.edge >= 0 ? '+' : ''}${t.edge.toFixed(1)}%</td>
        <td>${t.confidence.toFixed(0)}%</td>
        <td class="${rc}">${t.result || '<span style="color:var(--text-dim)">pending</span>'}</td>
        <td>${t.dry_run ? '<span class="tag tag-dry">DRY</span>' : '<span class="tag tag-live">LIVE</span>'}</td>
      </tr>`;
    }).join('');
  }

  // Reload chart if new trades resolved
  const currentClosed = p.closed;
  if (currentClosed !== lastTradeCount) {
    lastTradeCount = currentClosed;
    loadPnlHistory();
  }

  // Log
  const lb = document.getElementById('log-box');
  lb.innerHTML = d.log.map(l => {
    const cls = l.level || 'info';
    return `<div class="log-entry ${cls}"><span class="log-ts">${l.ts}</span><span class="log-msg">${l.msg}</span></div>`;
  }).join('');
  lb.scrollTop = lb.scrollHeight;
}

initChart();
connect();
loadPnlHistory();
setInterval(loadPnlHistory, 30000);
</script>
</body>
</html>"""
