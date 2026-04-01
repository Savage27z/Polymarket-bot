from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import time
from collections import deque
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from src.alerts.telegram import TelegramAlerter
from src.config import Settings
from src.engine.executor import Executor
from src.engine.risk import Position, RiskManager
from src.engine.signals import Signal, SignalEngine
from src.feeds.binance_ws import BinanceFeed
from src.feeds.polymarket_feed import PolymarketFeed
from src.storage.db import Database

logger = logging.getLogger(__name__)

LOG_BUFFER_SIZE = 200


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

        self.app = FastAPI(title="Polymarket Arbitrage Bot")
        self._register_routes()

    def _log(self, msg: str) -> None:
        ts = dt.datetime.now(dt.timezone.utc).strftime("%H:%M:%S")
        entry = f"{ts} {msg}"
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
            ]
            self._log("Bot started" + (" [DRY-RUN]" if self.settings.dry_run else " [LIVE]"))

        @app.on_event("shutdown")
        async def shutdown():
            for t in self._tasks:
                t.cancel()
            await self.binance.stop()
            self._log("Bot shutdown")

        @app.get("/", response_class=HTMLResponse)
        async def index():
            return DASHBOARD_HTML

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
                            self._log(f"Kill switch toggled: {state}")
                        elif action == "toggle_dry":
                            self.settings.dry_run = not self.settings.dry_run
                            mode = "DRY-RUN" if self.settings.dry_run else "LIVE"
                            self._log(f"Mode switched to: {mode}")
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

        btc = self.binance.get_price("BTC")
        eth = self.binance.get_price("ETH")
        now = time.time()

        signals = []
        for sig in self._latest_signals[:8]:
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
                "time_left": max(0, round(remaining)),
                "dry_run": pos.dry_run,
            })

        trades = await self.db.get_recent_trades(20)
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

        return {
            "portfolio": {
                "value": round(pv, 2),
                "daily_pnl": round(daily_pnl, 2),
                "daily_pct": round(daily_pct, 2),
                "win_rate": round(stats["win_rate"], 1),
                "total_trades": stats["total_trades"],
                "total_pnl": round(stats["total_pnl"], 2),
                "wins": stats["wins"],
                "closed": stats["closed"],
            },
            "mode": "DRY-RUN" if self.settings.dry_run else "LIVE",
            "kill_switch": self.risk.kill_switch_active,
            "max_position": self.settings.max_position_usdc,
            "feeds": {
                "btc_price": round(btc.current_price, 2),
                "btc_age": round(now - btc.last_update, 1) if btc.last_update > 0 else -1,
                "btc_vol": round(btc.volatility, 6),
                "eth_price": round(eth.current_price, 2),
                "eth_age": round(now - eth.last_update, 1) if eth.last_update > 0 else -1,
                "eth_vol": round(eth.volatility, 6),
                "markets_count": len(self.polymarket.markets),
            },
            "signals": signals,
            "positions": positions,
            "trades": trade_list,
            "log": list(self._log_buffer)[-50:],
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

    async def _binance_worker(self) -> None:
        self._log("Starting Binance price feed...")
        try:
            await self.binance.start()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self._log(f"Binance feed error: {exc}")
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
                self._log(f"Discovery error: {exc}")
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
                        f"Signals: {len(signals)} | Best: {best.market.asset}-{best.market.timeframe} "
                        f"{best.side} edge={best.edge:+.1%} conf={best.confidence:.0%} "
                        f"CEX=${best.cex_price:,.2f} strike=${best.strike_price:,.2f}"
                    )

                for sig in signals:
                    if not self.signal_engine.should_execute(sig, self.risk.kill_switch_active):
                        continue
                    if sig.market.condition_id in self.risk.open_positions:
                        continue
                    if len(self.risk.open_positions) >= self.settings.max_concurrent_positions:
                        break

                    self._log(
                        f"EXECUTING: {sig.market.asset}-{sig.market.timeframe} "
                        f"{sig.side} edge={sig.edge:.1%} conf={sig.confidence:.0%}"
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
                        self._log(f"Trade failed: {err}")
                        await self.telegram.error_alert(f"Order execution failed: {err}")

                stats = await self.db.get_stats()
                pv = self.settings.initial_portfolio_value + stats["total_pnl"]
                self.risk.check_drawdown(pv)
                alerts = self.risk.check_drawdown_alerts(pv)
                for threshold in alerts:
                    await self.telegram.drawdown_alert(threshold, pv)
                    self._log(f"Drawdown alert: {threshold:.0%}")
                if self.risk.kill_switch_active and not self._kill_switch_alerted:
                    self._kill_switch_alerted = True
                    await self.telegram.kill_switch_alert()
                    self._log("KILL SWITCH ACTIVATED")

            except asyncio.CancelledError:
                return
            except Exception as exc:
                self._log(f"Signal loop error: {exc}")
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

                    self.risk.daily_pnl += pnl
                    if pos.trade_db_id:
                        exit_price = 1.0 if won else 0.0
                        await self.db.update_trade_result(pos.trade_db_id, status, exit_price, pnl)
                    resolved.append(cid)
                    self._log(f"Resolved: {pos.asset}-{pos.timeframe} {pos.side} -> {status} P&L: {pnl:+.2f}")

                for cid in resolved:
                    self.risk.remove_position(cid)

                if self.risk.open_positions:
                    stats = await self.db.get_stats()
                    pv = self.settings.initial_portfolio_value + stats["total_pnl"]
                    await self.db.snapshot_portfolio(pv, self.risk.daily_pnl, len(self.risk.open_positions))

            except asyncio.CancelledError:
                return
            except Exception as exc:
                self._log(f"Resolution error: {exc}")
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
                self._log(f"Daily reset error: {exc}")
                await asyncio.sleep(60)


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Polymarket Arbitrage Bot</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{
  --bg:#0a0e17;--card:#111827;--card2:#1a2235;--border:#1e293b;
  --text:#e2e8f0;--dim:#64748b;--green:#22c55e;--red:#ef4444;
  --yellow:#eab308;--cyan:#06b6d4;--purple:#a855f7;--blue:#3b82f6;
  --accent:#6366f1;
}
body{background:var(--bg);color:var(--text);font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;font-size:14px;line-height:1.5;min-height:100vh}
.header{background:linear-gradient(135deg,#111827 0%,#1e1b4b 100%);border-bottom:1px solid var(--border);padding:12px 24px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100}
.header h1{font-size:18px;font-weight:700;background:linear-gradient(135deg,var(--cyan),var(--purple));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.header .status{display:flex;gap:12px;align-items:center}
.badge{padding:3px 10px;border-radius:20px;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.5px}
.badge-live{background:rgba(34,197,94,.15);color:var(--green);border:1px solid rgba(34,197,94,.3)}
.badge-dry{background:rgba(59,130,246,.15);color:var(--blue);border:1px solid rgba(59,130,246,.3)}
.badge-kill{background:rgba(239,68,68,.15);color:var(--red);border:1px solid rgba(239,68,68,.3);animation:pulse 1.5s infinite}
.conn-dot{width:8px;height:8px;border-radius:50%;display:inline-block}
.conn-ok{background:var(--green);box-shadow:0 0 6px var(--green)}
.conn-err{background:var(--red);box-shadow:0 0 6px var(--red)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;padding:16px 24px;max-width:1600px;margin:0 auto}
.grid-full{grid-column:1/-1}
.card{background:var(--card);border:1px solid var(--border);border-radius:12px;overflow:hidden}
.card-header{padding:12px 16px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between}
.card-header h2{font-size:13px;font-weight:600;text-transform:uppercase;letter-spacing:.8px;color:var(--dim)}
.card-body{padding:16px}
.stat-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:16px}
.stat{background:var(--card2);border-radius:8px;padding:12px 16px}
.stat .label{font-size:11px;color:var(--dim);text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px}
.stat .value{font-size:22px;font-weight:700;font-variant-numeric:tabular-nums}
.stat .sub{font-size:12px;color:var(--dim);margin-top:2px}
.pos{color:var(--green)}.neg{color:var(--red)}
.feed-row{display:flex;align-items:center;justify-content:space-between;padding:10px 0;border-bottom:1px solid var(--border)}
.feed-row:last-child{border-bottom:none}
.feed-symbol{font-weight:700;font-size:15px;min-width:40px}
.feed-price{font-size:20px;font-weight:700;font-variant-numeric:tabular-nums}
.feed-meta{font-size:11px;color:var(--dim)}
table{width:100%;border-collapse:collapse;font-size:13px}
thead th{text-align:left;padding:8px 12px;font-size:11px;text-transform:uppercase;letter-spacing:.5px;color:var(--dim);border-bottom:1px solid var(--border);font-weight:600;position:sticky;top:0;background:var(--card)}
tbody td{padding:8px 12px;border-bottom:1px solid var(--border);font-variant-numeric:tabular-nums}
tbody tr:hover{background:var(--card2)}
.signal-card{background:var(--card2);border-radius:8px;padding:12px 16px;margin-bottom:8px;display:flex;align-items:center;justify-content:space-between;border-left:3px solid var(--dim)}
.signal-card.exec{border-left-color:var(--green)}
.signal-card .sig-left{display:flex;align-items:center;gap:12px}
.signal-card .sig-pair{font-weight:700;font-size:14px}
.signal-card .sig-side{padding:2px 8px;border-radius:4px;font-size:11px;font-weight:700}
.sig-side.yes{background:rgba(34,197,94,.15);color:var(--green)}
.sig-side.no{background:rgba(239,68,68,.15);color:var(--red)}
.signal-card .sig-edge{font-size:18px;font-weight:700}
.signal-card .sig-meta{font-size:11px;color:var(--dim)}
.log-box{height:200px;overflow-y:auto;padding:8px 12px;font-family:'JetBrains Mono','Fira Code',monospace;font-size:12px;line-height:1.8;background:var(--card2);border-radius:8px}
.log-box .log-ts{color:var(--dim);margin-right:8px}
.log-box .log-line{margin:0}
.btn-row{display:flex;gap:8px}
.btn{padding:6px 14px;border-radius:6px;border:1px solid var(--border);background:var(--card2);color:var(--text);font-size:12px;cursor:pointer;font-weight:600;transition:all .15s}
.btn:hover{background:var(--border)}
.btn-danger{border-color:rgba(239,68,68,.4);color:var(--red)}
.btn-danger:hover{background:rgba(239,68,68,.1)}
.btn-danger.active{background:rgba(239,68,68,.2);border-color:var(--red)}
.empty{text-align:center;padding:24px;color:var(--dim);font-size:13px}
.tag{display:inline-block;padding:1px 6px;border-radius:3px;font-size:10px;font-weight:600}
.tag-dry{background:rgba(59,130,246,.15);color:var(--blue)}
.table-scroll{max-height:300px;overflow-y:auto}
@media(max-width:900px){.grid{grid-template-columns:1fr}.grid>*{grid-column:1}}
</style>
</head>
<body>
<div class="header">
  <h1>Polymarket Arbitrage Bot</h1>
  <div class="status">
    <span class="conn-dot conn-err" id="conn-dot" title="WebSocket disconnected"></span>
    <span class="badge badge-dry" id="mode-badge">--</span>
    <div class="btn-row">
      <button class="btn" id="btn-dry" onclick="send('toggle_dry')">Toggle Dry</button>
      <button class="btn btn-danger" id="btn-kill" onclick="send('toggle_kill')">Kill Switch</button>
    </div>
  </div>
</div>

<div class="grid">
  <!-- Portfolio -->
  <div class="card grid-full">
    <div class="card-header"><h2>Portfolio</h2><span id="last-update" style="font-size:11px;color:var(--dim)"></span></div>
    <div class="card-body">
      <div class="stat-grid" id="portfolio-stats">
        <div class="stat"><div class="label">Value</div><div class="value" id="pv">$0.00</div></div>
        <div class="stat"><div class="label">Daily P&L</div><div class="value" id="daily-pnl">$0.00</div><div class="sub" id="daily-pct">0.00%</div></div>
        <div class="stat"><div class="label">Total P&L</div><div class="value" id="total-pnl">$0.00</div></div>
        <div class="stat"><div class="label">Win Rate</div><div class="value" id="win-rate">0%</div><div class="sub" id="win-detail">0/0</div></div>
        <div class="stat"><div class="label">Trades</div><div class="value" id="total-trades">0</div></div>
        <div class="stat"><div class="label">Max Position</div><div class="value" id="max-pos">$1.00</div></div>
      </div>
    </div>
  </div>

  <!-- Live Feeds -->
  <div class="card">
    <div class="card-header"><h2>Live Feeds</h2><span id="markets-count" style="font-size:11px;color:var(--dim)">0 markets</span></div>
    <div class="card-body">
      <div class="feed-row">
        <span class="feed-symbol" style="color:var(--yellow)">BTC</span>
        <span class="feed-price" id="btc-price">$0.00</span>
        <div style="text-align:right">
          <div class="feed-meta" id="btc-age">--</div>
          <div class="feed-meta" id="btc-vol">Vol: --</div>
        </div>
      </div>
      <div class="feed-row">
        <span class="feed-symbol" style="color:var(--purple)">ETH</span>
        <span class="feed-price" id="eth-price">$0.00</span>
        <div style="text-align:right">
          <div class="feed-meta" id="eth-age">--</div>
          <div class="feed-meta" id="eth-vol">Vol: --</div>
        </div>
      </div>
    </div>
  </div>

  <!-- Active Signals -->
  <div class="card">
    <div class="card-header"><h2>Active Signals</h2><span id="sig-count" style="font-size:11px;color:var(--dim)">0</span></div>
    <div class="card-body" id="signals-container">
      <div class="empty">Scanning for signals...</div>
    </div>
  </div>

  <!-- Open Positions -->
  <div class="card grid-full">
    <div class="card-header"><h2>Open Positions</h2><span id="pos-count" style="font-size:11px;color:var(--dim)">0</span></div>
    <div class="table-scroll">
      <table>
        <thead><tr><th>Market</th><th>Side</th><th>Entry</th><th>Size</th><th>Edge</th><th>Time Left</th><th>Type</th></tr></thead>
        <tbody id="pos-body"></tbody>
      </table>
    </div>
    <div class="empty" id="pos-empty">No open positions</div>
  </div>

  <!-- Recent Trades -->
  <div class="card grid-full">
    <div class="card-header"><h2>Recent Trades</h2></div>
    <div class="table-scroll">
      <table>
        <thead><tr><th>Time</th><th>Market</th><th>Side</th><th>Price</th><th>Size</th><th>Edge</th><th>Conf</th><th>Result</th><th>Type</th></tr></thead>
        <tbody id="trades-body"></tbody>
      </table>
    </div>
    <div class="empty" id="trades-empty">No trades yet</div>
  </div>

  <!-- Activity Log -->
  <div class="card grid-full">
    <div class="card-header"><h2>Activity Log</h2></div>
    <div class="card-body">
      <div class="log-box" id="log-box"></div>
    </div>
  </div>
</div>

<script>
let ws;
let reconnectDelay = 1000;

function connect() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(`${proto}//${location.host}/ws`);

  ws.onopen = () => {
    document.getElementById('conn-dot').className = 'conn-dot conn-ok';
    document.getElementById('conn-dot').title = 'Connected';
    reconnectDelay = 1000;
  };

  ws.onclose = () => {
    document.getElementById('conn-dot').className = 'conn-dot conn-err';
    document.getElementById('conn-dot').title = 'Disconnected';
    setTimeout(connect, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 1.5, 10000);
  };

  ws.onerror = () => {};

  ws.onmessage = (evt) => {
    try { update(JSON.parse(evt.data)); } catch(e) {}
  };
}

function send(action) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify({action}));
}

function fmt(n, d=2) { return n.toLocaleString('en-US', {minimumFractionDigits:d, maximumFractionDigits:d}); }

function update(d) {
  const p = d.portfolio;
  document.getElementById('pv').textContent = '$' + fmt(p.value);
  const dpnl = document.getElementById('daily-pnl');
  dpnl.textContent = (p.daily_pnl >= 0 ? '+' : '') + '$' + fmt(p.daily_pnl);
  dpnl.className = 'value ' + (p.daily_pnl >= 0 ? 'pos' : 'neg');
  const dpct = document.getElementById('daily-pct');
  dpct.textContent = (p.daily_pct >= 0 ? '+' : '') + fmt(p.daily_pct,1) + '%';
  dpct.className = 'sub ' + (p.daily_pct >= 0 ? 'pos' : 'neg');
  const tpnl = document.getElementById('total-pnl');
  tpnl.textContent = (p.total_pnl >= 0 ? '+' : '') + '$' + fmt(p.total_pnl);
  tpnl.className = 'value ' + (p.total_pnl >= 0 ? 'pos' : 'neg');
  document.getElementById('win-rate').textContent = fmt(p.win_rate,1) + '%';
  document.getElementById('win-detail').textContent = p.wins + '/' + p.closed + ' closed';
  document.getElementById('total-trades').textContent = p.total_trades;
  document.getElementById('max-pos').textContent = '$' + fmt(d.max_position);

  // Mode
  const mb = document.getElementById('mode-badge');
  mb.textContent = d.mode;
  mb.className = 'badge ' + (d.mode === 'LIVE' ? 'badge-live' : 'badge-dry');

  // Kill switch
  const kb = document.getElementById('btn-kill');
  kb.className = 'btn btn-danger' + (d.kill_switch ? ' active' : '');
  kb.textContent = d.kill_switch ? 'Kill: ON' : 'Kill Switch';

  // Feeds
  const f = d.feeds;
  document.getElementById('btc-price').textContent = '$' + fmt(f.btc_price);
  document.getElementById('eth-price').textContent = '$' + fmt(f.eth_price);
  document.getElementById('btc-age').textContent = f.btc_age >= 0 ? fmt(f.btc_age,1) + 's ago' : 'N/A';
  document.getElementById('eth-age').textContent = f.eth_age >= 0 ? fmt(f.eth_age,1) + 's ago' : 'N/A';
  document.getElementById('btc-vol').textContent = 'Vol: ' + f.btc_vol.toFixed(6);
  document.getElementById('eth-vol').textContent = 'Vol: ' + f.eth_vol.toFixed(6);
  document.getElementById('markets-count').textContent = f.markets_count + ' markets';

  // Signals
  const sc = document.getElementById('signals-container');
  document.getElementById('sig-count').textContent = d.signals.length;
  if (d.signals.length === 0) {
    sc.innerHTML = '<div class="empty">No active signals</div>';
  } else {
    sc.innerHTML = d.signals.map(s => `
      <div class="signal-card ${s.executable ? 'exec' : ''}">
        <div class="sig-left">
          <span class="sig-pair">${s.asset}-${s.timeframe}</span>
          <span class="sig-side ${s.side.toLowerCase()}">${s.side}</span>
        </div>
        <div style="text-align:right">
          <div class="sig-edge ${s.edge >= 0 ? 'pos' : 'neg'}">${s.edge >= 0 ? '+' : ''}${s.edge.toFixed(1)}%</div>
          <div class="sig-meta">Conf: ${s.confidence.toFixed(0)}% | ${s.time_remaining}s | $${s.size.toFixed(2)}</div>
        </div>
      </div>
    `).join('');
  }

  // Positions
  const pb = document.getElementById('pos-body');
  const pe = document.getElementById('pos-empty');
  if (d.positions.length === 0) {
    pb.innerHTML = '';
    pe.style.display = '';
  } else {
    pe.style.display = 'none';
    pb.innerHTML = d.positions.map(p => `<tr>
      <td><strong>${p.asset}-${p.timeframe}</strong></td>
      <td><span class="sig-side ${p.side.toLowerCase()}">${p.side}</span></td>
      <td>$${p.entry_price.toFixed(2)}</td>
      <td>$${p.size_usdc.toFixed(2)}</td>
      <td class="${p.edge >= 0 ? 'pos' : 'neg'}">${p.edge >= 0 ? '+' : ''}${p.edge.toFixed(1)}%</td>
      <td>${p.time_left}s</td>
      <td>${p.dry_run ? '<span class="tag tag-dry">DRY</span>' : ''}</td>
    </tr>`).join('');
  }
  document.getElementById('pos-count').textContent = d.positions.length;

  // Trades
  const tb = document.getElementById('trades-body');
  const te = document.getElementById('trades-empty');
  if (d.trades.length === 0) {
    tb.innerHTML = '';
    te.style.display = '';
  } else {
    te.style.display = 'none';
    tb.innerHTML = d.trades.map(t => {
      let rc = '';
      if (t.status === 'won') rc = 'pos';
      else if (t.status === 'lost') rc = 'neg';
      return `<tr>
        <td>${t.time}</td>
        <td><strong>${t.asset}-${t.timeframe}</strong></td>
        <td><span class="sig-side ${t.side.toLowerCase()}">${t.side}</span></td>
        <td>$${t.entry_price.toFixed(2)}</td>
        <td>$${t.size_usdc.toFixed(2)}</td>
        <td>${t.edge >= 0 ? '+' : ''}${t.edge.toFixed(1)}%</td>
        <td>${t.confidence.toFixed(0)}%</td>
        <td class="${rc}">${t.result || '--'}</td>
        <td>${t.dry_run ? '<span class="tag tag-dry">DRY</span>' : ''}</td>
      </tr>`;
    }).join('');
  }

  // Log
  const lb = document.getElementById('log-box');
  lb.innerHTML = d.log.map(l => {
    const sp = l.indexOf(' ');
    const ts = l.substring(0, sp);
    const msg = l.substring(sp + 1);
    return `<p class="log-line"><span class="log-ts">${ts}</span>${msg}</p>`;
  }).join('');
  lb.scrollTop = lb.scrollHeight;

  // Update time
  const now = new Date();
  document.getElementById('last-update').textContent = 'Updated ' + now.toLocaleTimeString();
}

connect();
</script>
</body>
</html>"""
