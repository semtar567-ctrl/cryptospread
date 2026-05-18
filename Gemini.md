[18.05.2026 15:18] Семен: # Project Overview
# Communication Rules

- Always communicate with the user in Russian
- Always explain planned changes before editing files
- Always ask for confirmation before changing files
- Never overwrite files without confirmation
- Never delete files without confirmation
- Explain architecture decisions in simple language
- Prefer small incremental changes
- Never generate the entire system at once
- Split all work into small steps
- Assume the user is not an expert programmer                                                                                        
This repository is a professional crypto market inefficiency research and execution platform.

The system is designed for:
- market microstructure research
- funding rate analysis
- perp/spot basis analysis
- order book imbalance analysis
- liquidation analysis
- cross-exchange inefficiency detection
- signal generation
- systematic execution
- backtesting
- risk management

The codebase must prioritize:
1. correctness
2. reliability
3. deterministic behavior
4. observability
5. performance
6. extensibility

Never sacrifice correctness for premature optimization.

---

# General Engineering Rules

- Always write production-grade code
- Never generate placeholder implementations
- Never generate fake logic
- Never silently swallow exceptions
- Never invent APIs or exchange endpoints
- Never assume exchange behavior without verification
- Prefer explicitness over abstraction
- Prefer simple architecture over clever architecture
- Prefer maintainability over micro-optimizations
- Keep functions small and testable
- Keep modules cohesive
- Use dependency injection where appropriate
- Avoid hidden global mutable state
- Prefer composition over inheritance
- Avoid tight coupling between modules

---

# Repository Structure

The repository should follow this structure:

research/
python/
rust_core/
collectors/
orderbooks/
signals/
strategies/
execution/
risk/
backtests/
infra/
configs/
tests/
docs/

---

# Python Standards

- Use Python 3.12+
- Use asyncio for concurrent systems
- Never block the event loop
- Use uv package manager
- Use Polars instead of pandas unless compatibility requires pandas
- Use PyArrow where appropriate
- Use pydantic-settings for configuration
- Use structlog for structured logging
- Use mypy-compatible typing
- Use pytest for testing
- Use Ruff for linting
- Avoid mutable shared state
- Prefer pure functions for signal computation
- Prefer explicit typing everywhere

Preferred libraries:
- polars
- numpy
- pyarrow
- aiohttp
- websockets
- asyncpg
- numba
- vectorbt
- pydantic
- structlog

---

# Rust Standards

- Use stable Rust
- Prefer Tokio async runtime
- Prefer deterministic concurrency
- Prefer zero-copy parsing where practical
- Minimize unnecessary allocations
- Use serde for serialization
- Use tracing for logging
- Use anyhow or error-stack for error handling
- Benchmark critical paths
- Avoid unnecessary cloning
- Prefer explicit ownership semantics
- Prefer lock-free or low-contention structures where appropriate

Preferred libraries:
- tokio
- tokio-tungstenite
- serde
- tracing
- rayon
- dashmap
- anyhow
- bytes

---

# Async System Rules

- Never mix sync and async incorrectly
- Never use blocking I/O in async code
- Never use sleep-based synchronization
- Use bounded queues
- Use graceful shutdown handling
- Use cancellation-safe patterns
- Use reconnect state machines
- Use exponential backoff for retries
- All websocket systems must support reconnection

---

# Market Data Rules

- Never trust exchange data blindly
- Validate sequence numbers
- Detect stale books
- Detect duplicate updates
- Detect missing updates
- Preserve raw exchange payloads when possible
- Normalize timestamps to UTC
- Explicitly specify timestamp precision
- All timestamps must use milliseconds or nanoseconds
- REST snapshots may lag websocket streams
- Exchanges may produce inconsistent data during volatility

---

# Exchange Adapter Rules

- Every exchange adapter must implement a shared interface
- Exchange-specific logic must remain isolated
- Never hardcode exchange assumptions globally
- Normalize exchange-specific formats
- Support reconnect logic
- Support rate-limit handling
- Support retry handling
- Log reconnect events
- Log dropped messages
- Log sequence mismatches

---

# Order Book Reconstruction Rules

- Support snapshot + incremental updates
- Detect sequence gaps
- Trigger resynchronization on checksum mismatch
- Preserve deterministic ordering
- Minimize allocations during updates
- Separate parsing from state mutation
- Support multiple symbols concurrently
[18.05.2026 15:18] Семен: - Preserve exchange timestamps

---

# Signal Engine Rules

- Signals must be deterministic
- Signal generation must be isolated from execution
- Avoid hidden state
- All thresholds must be configurable
- Support dry-run mode
- Support replay mode
- Prefer stateless computation where possible
- Separate feature engineering from strategy logic

---

# Strategy Rules

Strategies may include:
- funding divergence
- perp/spot basis
- order book imbalance
- liquidation reactions
- cross-exchange spread analysis
- mean reversion
- volatility dislocations

Rules:
- Never assume profitability
- Always account for fees
- Always account for slippage
- Always account for latency
- Always account for liquidity constraints
- Always consider liquidation risk
- Always support configurable position sizing

---

# Execution Engine Rules

- Execution must be isolated from strategy logic
- All orders must support retry logic
- All retries must use exponential backoff
- Detect duplicate orders
- Detect stale acknowledgements
- Support idempotency keys
- Never assume exchange consistency
- Support partial fills
- Support cancel/replace flows
- Track execution latency
- Track reject reasons

---

# Risk Management Rules

- Risk systems must fail safely
- Support max position limits
- Support max exposure limits
- Support max leverage limits
- Support kill switches
- Support emergency shutdown
- Detect abnormal exchange behavior
- Detect stale market data
- Prevent runaway order loops

---

# Backtesting Rules

- Never use future information
- Prevent lookahead bias
- Include trading fees
- Include slippage
- Include latency assumptions
- Simulate partial fills where possible
- Simulate exchange delays where practical
- Prefer event-driven backtesting
- Preserve timestamp ordering
- Keep research assumptions explicit

---

# Observability Rules

- Use structured logging everywhere
- Include exchange name in logs
- Include symbol in logs
- Include timestamps in logs
- Include latency metrics
- Include reconnect counters
- Include dropped message counters
- Include sequence mismatch counters
- Include order execution metrics

---

# Infrastructure Rules

- All services must support Docker
- Use Docker Compose for local development
- Never hardcode credentials
- Use environment variables for secrets
- Support reproducible local environments
- Support graceful service shutdown
- Support health checks

---

# Performance Philosophy

Optimize only:
- websocket ingestion
- order book updates
- serialization
- signal hot paths
- execution latency

Do not prematurely optimize:
- research code
- orchestration code
- configuration systems
- analytics notebooks

Correctness is more important than optimization.

---

# Known Failure Modes

Assume the following can happen:
- websocket disconnects
- exchange downtime
- stale order books
- duplicated updates
- out-of-order messages
- REST snapshot lag
- funding timestamp drift
- partial exchange outages
- inconsistent exchange APIs
- exchange-side throttling
- volatility spikes

Systems must handle these scenarios gracefully.

---

# AI Assistant Rules

Before implementing:
1. explain architecture
2. identify bottlenecks
3. identify failure scenarios
4. identify performance-sensitive paths

After implementing:
1. generate tests
2. explain tradeoffs
3. explain limitations
4. suggest monitoring
5. suggest benchmarks

Additional rules:
- Prefer incremental refactors over rewrites
- Ask clarifying questions if requirements are ambiguous
- Do not invent undocumented behavior
- Keep implementations production-oriented
- Prefer deterministic systems
- Prefer observable systems

---

# Definition of Done

A task is complete only if:
- code compiles
- tests pass
- typing passes
- linting passes
- logging added
- edge cases handled
- reconnect logic implemented
- metrics exposed
- configuration externalized
- failure scenarios considered

---

# Anti-Patterns

Never:
- block async event loops
- hide exceptions
- use magic constants
- hardcode exchange assumptions
- generate fake implementations
[18.05.2026 15:18] Семен: - tightly couple execution and strategy logic
- mix research and production code
- optimize without measurement
- use unbounded queues in hot paths

---

# Preferred Development Workflow

1. Research hypothesis
2. Define architecture
3. Build data collectors
4. Normalize market data
5. Implement features
6. Implement signals
7. Backtest realistically
8. Benchmark performance
9. Add observability
10. Deploy carefully

Always prioritize:
- data quality
- execution correctness
- risk management
over alpha assumptions.