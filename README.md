# Polymarket Latency Arbitrage Bot

Monitors Polymarket BTC/ETH 5-minute and 15-minute up/down contracts, compares their implied odds with real-time CEX prices from Binance WebSocket, and executes trades when the edge exceeds configurable thresholds.

## Architecture

```
Binance WS (BTC/ETH trades)          Gamma API (market discovery)
        │                                      │
        ▼                                      ▼
  ┌──────────┐                         ┌───────────────┐
  │ BinanceFeed │◄── real-time prices ──│ PolymarketFeed │◄── CLOB midpoints
  └─────┬────┘                         └───────┬───────┘
        │                                      │
        └──────────┬───────────────────────────┘
                   ▼
            ┌─────────────┐
            │ SignalEngine │  GBM implied prob vs market price
            └──────┬──────┘
                   │ Signal (edge, confidence, kelly size)
                   ▼
            ┌──────────┐
            │ Executor  │  FOK orders via py-clob-client
            └──────┬───┘
                   │
         ┌─────────┼──────────┐
         ▼         ▼          ▼
     SQLite    Telegram    Textual TUI
```

## Quick Start

```bash
# Clone
git clone https://github.com/Savage27z/Polymarket-bot.git
cd Polymarket-bot

# Install
pip install -e .

# Configure
cp .env.example .env
# Edit .env with your Polymarket private key and (optionally) Telegram credentials

# Run in dry-run mode (no real trades)
polybot --dry-run

# Run live
polybot
```

## Configuration

All settings live in `.env` — see `.env.example` for the full list.

| Variable | Description | Default |
|---|---|---|
| `POLY_PRIVATE_KEY` | Exported private key from polymarket.com/settings (POLY_PROXY wallet) | *required for live* |
| `POLY_FUNDER_ADDRESS` | Your proxy wallet address | *required for live* |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token for alerts | *optional* |
| `TELEGRAM_CHAT_ID` | Telegram chat ID for alerts | *optional* |
| `INITIAL_PORTFOLIO_USDC` | Starting portfolio value for tracking | `100` |

## Trading Parameters

| Parameter | Value | Description |
|---|---|---|
| Lag detection threshold | 3% | Minimum edge to flag a signal |
| Execution threshold | 5% | Minimum edge to place a trade |
| Max position size | **$1.00** | Hard cap per trade |
| Max position % | 8% | Secondary cap as % of portfolio |
| Min confidence | 85% | Weighted confidence score required |
| Kelly fraction | 50% | Half-Kelly position sizing |
| Daily drawdown limit | 20% | Kill switch activation threshold |

## Dashboard Keybindings

| Key | Action |
|---|---|
| `q` | Quit |
| `k` | Toggle kill switch |
| `d` | Toggle dry-run mode |
| `r` | Force refresh |

## Project Structure

```
src/
├── main.py              # CLI entry point + orchestration
├── config.py            # Settings dataclass from .env
├── feeds/
│   ├── binance_ws.py    # Real-time BTC/ETH price stream
│   └── polymarket_feed.py  # Market discovery + CLOB price polling
├── engine/
│   ├── signals.py       # GBM probability, edge calc, confidence scoring
│   ├── risk.py          # Kelly sizing, drawdown kill switch, position tracking
│   └── executor.py      # Order placement (live + dry-run)
├── alerts/
│   └── telegram.py      # Trade/drawdown/error alerts via Telegram Bot API
├── storage/
│   └── db.py            # SQLite trade log + portfolio snapshots
└── dashboard/
    └── app.py           # Textual TUI with live data tables
```

## How It Works

1. **Binance WebSocket** streams real-time BTC/ETH trades and calculates rolling volatility.
2. **Market discovery** queries the Gamma API every 30s for active 5m/15m up/down contracts using slug-based lookup (primary) and tag-based search (fallback).
3. **Signal engine** calculates implied probability via Geometric Brownian Motion, compares it to Polymarket's yes/no token prices, and computes edge + confidence.
4. **Execution** fires Fill-or-Kill limit orders when edge > 5%, confidence > 85%, and Kelly-sized position passes all caps ($1 hard cap, 8% portfolio cap).
5. **Resolution** tracks open positions and marks them won/lost after market expiry.
6. **Risk management** tracks daily P&L and activates a kill switch at 20% drawdown.

## Requirements

- Python 3.11+
- Polymarket POLY_PROXY wallet credentials (for live trading)
- Internet access (Binance WS + Polymarket API)
