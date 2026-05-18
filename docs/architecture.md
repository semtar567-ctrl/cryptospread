# System Architecture

The platform consists of:

1. Exchange collectors
2. Normalization layer
3. Spread engine
4. Funding engine
5. Fee engine
6. Signal engine
7. Telegram bot
8. PostgreSQL
9. Redis

Data flow:

Exchanges
→ Collectors
→ Normalization
→ Spread/Funding analysis
→ Signal generation
→ Telegram alerts