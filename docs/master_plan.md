# Crypto Spread Monitoring Platform — Master Plan

## Project Goal

Build a reliable crypto futures market monitoring platform for manual trading decisions.

This system is NOT an automated trading bot.

The platform is designed only for:
- monitoring futures markets
- monitoring prices and volumes
- monitoring funding rates
- calculating spreads between exchanges
- calculating fee-adjusted net spreads
- detecting market inefficiencies
- generating high-quality trading signals
- sending Telegram notifications

The user manually executes all trades.

The platform must NOT:
- place orders
- manage positions
- auto-execute trades
- implement HFT execution systems
- implement portfolio management systems

The primary goal is:
HIGH-QUALITY SIGNAL GENERATION.

---

# Core Priorities

Prioritize:
1. Data quality
2. Signal quality
3. Reliability
4. Stability
5. Maintainability
6. Scalability

Latency optimization is secondary.

False positive reduction is extremely important.

---

# Technology Stack

## Rust
Use Rust for:
- websocket collectors
- exchange connections
- high-frequency data ingestion
- hot paths
- performance-critical systems

## Python
Use Python for:
- analytics
- spread calculations
- signal generation
- orchestration
- Telegram bot
- configuration
- historical analysis

---

# System Architecture

The platform should consist of:

1. Exchange collectors
2. Normalization layer
3. Spread engine
4. Funding engine
5. Fee engine
6. Signal engine
7. Telegram notification service
8. Historical storage
9. Monitoring and logging

---

# Development Philosophy

The system must be built incrementally.

Never generate the entire system at once.

Always:
- split work into small steps
- explain architecture before coding
- explain planned changes before editing files
- ask for confirmation before modifying files
- prefer minimal safe changes
- generate tests for critical systems

---

# Exchange Support Plan

## Initial Exchanges
1. Binance
2. Bybit
3. OKX

Only after stable implementation:
- add more exchanges

---

# Development Phases

# Phase 1 — Architecture

Goal:
Design the entire system architecture before implementation.

Tasks:
- define services
- define data flow
- define exchange abstractions
- define normalized market models
- define logging strategy
- define configuration system

Deliverables:
- architecture.md
- dataflow.md
- exchanges.md

---

# Phase 2 — Exchange Abstraction Layer

Goal:
Create scalable multi-exchange architecture.

Tasks:
- exchange interfaces
- normalized ticker model
- normalized funding model
- normalized volume model
- symbol normalization

---

# Phase 3 — Binance Collector

Goal:
Implement first production-grade collector.

Requirements:
- websocket support
- reconnect handling
- stale data detection
- structured logging
- graceful shutdown
- async architecture
- sequence validation

---

# Phase 4 — Symbol Normalization

Goal:
Normalize symbols across all exchanges.

Examples:
BTCUSDT
BTC-USDT
XBTUSDT

↓

BTCUSDT

---

# Phase 5 — Multi-Exchange Expansion

Add:
- Bybit
- OKX

Validate:
- architecture scalability
- normalization correctness
- spread calculation consistency

---

# Phase 6 — Spread Engine

Goal:
Calculate:
- raw spread
- fee-adjusted spread
- funding-adjusted spread

Prioritize calculation correctness over speed.

---

# Phase 7 — Signal Engine

Goal:
Filter low-quality opportunities.

Signal filters should include:
- minimum spread
- minimum volume
- funding thresholds
- stale market detection
- spread stability
- confidence scoring

---

# Phase 8 — Telegram Notification System

Goal:
Deliver high-quality signals.

Features:
- Telegram alerts
- customizable filters
- signal formatting
- cooldowns
- user commands

---

# Phase 9 — Historical Storage

Goal:
Store market data for:
- analytics
- replay
- debugging
- historical research

Use:
- PostgreSQL
- Redis

---

# Phase 10 — Replay and Validation

Goal:
Replay historical market situations.

Use for:
- debugging
- validation
- signal analysis
- strategy improvement

---

# Engineering Rules

Always:
[18.05.2026 20:47] Мой мтс 001: - use structured logging
- validate exchange payloads
- detect stale feeds
- handle reconnects safely
- write modular code
- avoid spaghetti architecture
- prefer composition over complexity

Never:
- trust exchange payloads blindly
- hardcode exchange-specific logic everywhere
- optimize prematurely
- generate huge monolithic systems

---

# Communication Rules

Always:
- communicate in Russian
- explain architecture simply
- explain planned changes before editing
- ask for confirmation before modifying files

Assume the user is not an expert programmer.