 # Project Context

## Main Idea

This project is a crypto futures market monitoring platform for MANUAL trading decisions.

The user manually opens and closes all trades.

The platform should help identify potentially profitable market inefficiencies across crypto futures exchanges.

This is NOT an automated trading bot.

---

# What The User Wants

The user wants:
- high-quality signals
- reliable market monitoring
- spread analysis
- funding analysis
- fee-adjusted calculations
- liquidity-aware analysis
- low false positive rate
- useful Telegram alerts

The user also wants flexible Telegram-based filtering and notifications.

The Telegram system should allow:
- configuring spread thresholds
- configuring funding thresholds
- configuring minimum volume filters
- configuring exchange filters
- configuring signal cooldowns
- enabling/disabling specific signal types

The platform should send Telegram notifications when:
- spread conditions match configured filters
- funding opportunities appear
- significant market divergences are detected
- high-confidence signals are found

The Telegram interface should become the primary control and monitoring interface for the system.

The user does NOT want:
- auto-trading
- order execution
- HFT systems
- portfolio management
- overcomplicated trading infrastructure

---

# Main Focus

The project should prioritize:
1. Signal quality
2. Data quality
3. Stability
4. Reliability
5. Clear architecture
6. Simplicity
7. Maintainability

Latency optimization is NOT the primary goal.

False positive reduction is extremely important.

---

# Important Concepts

The platform should focus heavily on:
- stale data detection
- symbol normalization
- funding accuracy
- fee accuracy
- spread persistence
- liquidity filtering
- false positive reduction
- exchange reliability analysis
- market divergence detection

---

# Telegram Interaction Model

Telegram should act as:
- signal delivery system
- monitoring dashboard
- filtering interface
- configuration interface

The user should be able to:
- configure filters from Telegram
- enable/disable signal categories
- adjust thresholds dynamically
- monitor current market opportunities
- receive formatted high-quality alerts

The platform should avoid Telegram spam by using:
- cooldowns
- deduplication
- confidence scoring
- persistence checks

---

# Development Style

The user prefers:
- small safe changes
- step-by-step implementation
- architecture-first approach
- explanations before coding
- confirmation before file modifications

The user is not an expert programmer, so explanations should be clear and simple.

---

# Engineering Philosophy

The project should be built as a reliable market intelligence platform.

Correctness is more important than speed.

Reliable and validated market data is more important than ultra-low latency.

The project should evolve gradually and safely.

Avoid:
- giant refactors
- overengineering
- unnecessary complexity
- premature optimization

Prefer:
- modular architecture
- scalable design
- exchange abstraction
- maintainable code
- clear system boundaries

---

# Expected Workflow

The AI assistant should:
1. Analyze before coding
2. Explain architecture decisions
3. Suggest minimal next steps
4. Avoid unnecessary complexity
5. Avoid giant refactors
6. Prefer stable solutions over clever solutions
7. Ask for confirmation before editing files

---

# Long-Term Goal

Build a reliable and scalable market intelligence platform for crypto futures monitoring and high-quality signal generation for manual trading decisions.