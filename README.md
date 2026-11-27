# ⚡ CryptoSpread — High-Frequency Inter-Exchange Arbitrage Platform

> **CryptoSpread** — институциональная платформа мониторинга и анализа межбиржевого арбитража бессрочных криптофьючерсов (Perpetual Futures) в реальном времени.
> Архитектура объединяет высокопроизводительное ядро на **Rust** (100 мс тик-цикл, 15 биржевых коллекторов) и гибкий интеллект-слой на **Python 3.12** с Web-терминалом графиков и Telegram-алертингом.

---

## 🏛 Архитектура платформы

Система разделена на изолированные слои в соответствии с принципами высокой доступности (High Availability) и суб-секундной задержки:

```mermaid
flowchart TD
    subgraph DataLayer ["Слой сбора данных (Performance Layer — Rust Core)"]
        EX[15 Криптобирж: Binance, Bybit, OKX, MEXC, Bitget, Gate, BingX, KuCoin, Hyperliquid, Aster DEX, HTX, dYdX, LBank, XT, CoinEx]
        WS[Асинхронные WebSocket-коллекторы Tokio / Tungstenite]
        OB[Потокобезопасные стаканы DashMap / Lock-Free State]
        TL[Tick Loop 100ms: Расчет сырых спредов & VWAP]
        EX --> WS --> OB --> TL
    end

    subgraph MessageBroker ["Шина данных & Хранилище"]
        RS[(Redis 7 Streams: market_signals)]
        RKV[(Redis K/V: meta:*, active_symbols_map)]
        PG[(PostgreSQL 16: пользователи, настройки, история)]
        TL -->|XADD MAXLEN ~5000| RS
        TL <-->|Pub/Sub: user_updates| RKV
    end

    subgraph IntelligenceLayer ["Слой аналитики (Intelligence Layer — Python 3.12)"]
        VM[Volume Monitor: сбор суточных объемов & фандинга]
        SE[Signal Engine: расчет проскальзывания, Taker Fee, дедупликация]
        VM -->|Кэширование meta:*| RKV
        RS --> SE
        PG <--> SE
    end

    subgraph DeliveryLayer ["Слой интерфейса и доставки"]
        TG[Telegram Bot: интерактивные карточки, пагинатор, управление]
        WEB[FastAPI Web-Server: стриминг спредов /ws/live-spread]
        TV[TradingView Lightweight Charts: исторические свечи спреда On-Demand]
        SE --> TG
        RKV --> WEB --> TV
    end

    subgraph Supervisor ["Оркестрация"]
        WD[Watchdog Supervisor: мониторинг здоровья процессов, авто-рестарт, аудит инцидентов]
        WD -.->|Управляет всеми процессами| DataLayer
        WD -.->|Управляет всеми процессами| IntelligenceLayer
        WD -.->|Управляет всеми процессами| DeliveryLayer
    end
```

---

## 🚀 Ключевые технологические особенности

### 1. Rust Core (Performance Layer)
- **100 мс Tick Loop:** аналитический цикл рассчитывает сотни тысяч комбинаций пар каждую десятую долю секунды.
- **Оптимизация `is_dirty`:** пересчёт только для монет, получивших свежие данные из WebSocket в текущем тике.
- **Детекция устаревания (Stale Data Detection):** котировки старше 3 секунд автоматически признаются «протухшими» и отбраковываются.
- **Мгновенная реакция на настройки:** подписка на Redis Pub/Sub (`user_updates`) обновляет фильтры ядра без перезапуска.
- **Параллельные пулы сокетов:** умный шардинг подписок для DEX и CEX (до 150 потоков на сокет, чанкинг по 30–50 топиков) для исключения `os error 10054`.

### 2. Математическая строгость расчетов (Signal Quality)
- **VWAP (Volume-Weighted Average Price):** каждый спред рассчитывается не по первой строчке стакана, а по реальному объёму сделки пользователя (`working_volume_usd`) с учётом полного проскальзывания.
- **Чистый спред (Net Spread):** автоматический вычет комиссий обеих бирж (двойной Taker Fee с динамической загрузкой через CCXT).
- **Сквозная нормализация мем-токенов:** автоматическое приведение множителей (`1000PEPE`, `1000000MOG`, `kPEPE`, `1000BONK`) к стандарту 1-к-1.
- **Коррекция `contract_size`:** учет размера контракта (напр. Gate.io, OKX, KuCoin) для исключения 1000-кратных искажений ликвидности.
- **Учёт ставок финансирования (Funding Rate):** расчёт спреда фандинга и точного времени до следующей выплаты (00:00, 08:00, 16:00 UTC).

### 3. Web-терминал графиков спреда
- **TradingView Lightweight Charts:** интерактивное визуальное отображение свечей спреда в процентах ($\text{Spread \%} = ((S / B) - 1.0) \times 100\%$).
- **Линия паритета (Parity Line):** четкая горизонтальная отметка `0.00%`, визуализирующая точку схождения арбитражного спреда.
- **Свечи On-Demand:** расчёт свечей по таймфреймам (`1m`, `5m`, `15m`, `1h`, `4h`, `1D`) «на лету» без пожирания ОЗУ сервера.
- **WebSocket Live Stream (`/ws/live-spread`):** отрисовка тиков спреда в реальном времени с нулевой задержкой.

### 4. Автономность и самовосстановление (Watchdog)
- Единая точка оркестрации: `watchdog.py` непрерывно контролирует все 5 подсистем.
- Автоматический перезапуск упавших или зависших процессов.
- Побайтовый аудит логов без утечек оперативной памяти (дедупликация инцидентов, журнал `INCIDENTS.md`).

---

## 🏛 15 поддерживаемых бирж

| Биржа | Тип | WebSocket протокол | Глубина стакана | Нормализация мем-токенов | Contract Size |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Binance** | CEX | Linear Swap | Depth 50 | ✅ Да (`1000`) | 1.0 |
| **Bybit** | CEX | Linear Swap | Depth 50 | ✅ Да (`1000`) | 1.0 |
| **OKX** | CEX | Linear Swap | Books 50 | ✅ Да | ✅ Да (ctVal) |
| **MEXC** | CEX | Linear Swap | Depth 30 | ✅ Да | ✅ Да |
| **Bitget** | CEX | Linear Swap | Books 50 | ✅ Да | 1.0 |
| **Gate.io** | CEX | Linear Swap | Order Book | ✅ Да | ✅ Да (contract_size) |
| **BingX** | CEX | Linear Swap | Depth 5 (Gzip) | ✅ Да | 1.0 |
| **KuCoin** | CEX | Linear Swap | Level 2 (Bullet API) | ✅ Да | ✅ Да |
| **Hyperliquid** | DEX | L2 Book | Depth 50 | ✅ Да (`k` префиксы) | 1.0 |
| **Aster DEX** | DEX | Multi-Socket Stream | Depth 20 @ 100ms | ✅ Да | 1.0 |
| **HTX (Huobi)** | CEX | Linear Swap (Gzip) | Depth Step0 | ✅ Да | ✅ Да |
| **dYdX v4** | DEX | Indexer WS | Orderbook delta | ✅ Да | 1.0 |
| **LBank** | CEX | Linear Swap | Depth 100 | ✅ Да | 1.0 |
| **XT.com** | CEX | Linear Swap | Depth 50 | ✅ Да | 1.0 |
| **CoinEx** | CEX | Linear Swap | Depth 50 | ✅ Да | 1.0 |

---

## 🛠 Стек технологий

- **Rust Core:** Tokio (асинхронный рантайм), Tungstenite (WebSockets), DashMap (высокопроизводительные конкурентные хэш-таблицы), Serde (десериализация JSON), Tracing (структурированные логи), Flate2 (Gzip/Zlib декомпрессия биржевых потоков).
- **Python Intelligence:** Python 3.12+, менеджер пакетов `uv`, `asyncio`, `structlog`, `ccxt.pro`, `SQLAlchemy 2.0` (asyncpg), `aiogram 3.x`, `FastAPI`, `Uvicorn`, `Pydantic v2`.
- **Инфраструктура:** Docker Compose, PostgreSQL 16, Redis 7 (Streams, Pub/Sub, KV Cache).
- **Фронтенд:** TradingView Lightweight Charts, WebSocket Client, Vanilla JS / Modern CSS3.

---

## 📁 Структура проекта

```text
cryptospread/
├── rust_core/                 # Ядро на Rust (Performance Layer)
│   ├── src/
│   │   ├── collectors/        # 15 биржевых WebSocket коллекторов
│   │   ├── engine.rs          # Tick Loop, стаканы, VWAP расчёт
│   │   ├── user_manager.rs    # Кэширование настроек пользователей
│   │   ├── models.rs          # Модели данных ордеров и стаканов
│   │   └── main.rs            # Точка входа ядра
│   └── Cargo.toml
├── signal_engine/             # Движок сигналов и аналитики (Python)
│   ├── main.py                # Обработка стрима из Redis, расчет Net Spread
│   └── volume_monitor.py      # Мониторинг 24h оборотов и ставок фандинга
├── telegram_bot/              # Telegram-интерфейс
│   ├── database/              # Модели SQLAlchemy & миграции
│   ├── utils/                 # Инлайн-клавиатуры, карточки, Redis-синглтон
│   └── main.py                # Aiogram 3 хэндлеры и FSM
├── web/                       # Web-терминал графиков спреда
│   ├── spread_engine/         # Калькулятор свечей и CCXT-фетчер
│   ├── static/                # HTML, CSS, TradingView Lightweight Charts
│   └── server.py              # FastAPI сервер и WebSocket эндпоинты
├── docs/                      # Архитектурная документация и мат. модели
├── watchdog.py                # Главный управляющий супервизор
├── compose.yml                # Docker Compose (PostgreSQL 5433, Redis 6380)
├── MEMORY.md                  # Журнал выполненных задач и текущий статус
├── ROADMAP.md                 # Дорожная карта масштабирования
├── pyproject.toml             # Зависимости Python (uv)
└── .env.example               # Шаблон переменных окружения
```

---

## ⚡ Быстрый старт (Quick Start)

### 1. Предварительные требования
- Установленный [Docker Desktop](https://www.docker.com/)
- Менеджер пакетов [uv](https://github.com/astral-sh/uv) (Python 3.12+)
- Компилятор [Rust & Cargo](https://rustup.rs/) (edition 2021)

### 2. Клонирование и настройка окружения
```bash
git clone https://github.com/semtar567-ctrl/cryptospread.git
cd cryptospread

# Создать .env из шаблона
cp .env.example .env
```

Отредактируйте `.env`:
```env
TELEGRAM_BOT_TOKEN=ваш_токен_бота
REDIS_HOST=127.0.0.1
REDIS_PORT=6380
DATABASE_URL=postgresql+asyncpg://crypto:crypto@localhost:5433/crypto
SPREAD_CHART_URL=http://127.0.0.1:8000
```

### 3. Запуск баз данных
```bash
docker compose up -d
```

### 4. Запуск всей системы под управлением Watchdog
```bash
uv run watchdog.py
```
> Супервизор `watchdog.py` автоматически скомпилирует Rust Core в режиме `--release`, запустит Volume Monitor, Signal Engine, Telegram-бота и Web-сервер на [http://127.0.0.1:8000](http://127.0.0.1:8000).

---

## 🗺 Стратегия и Дорожная карта (Roadmap)

- [x] **v6.0 - v15.0**: Подключение и стабилизация 15 бирж, нормализация мем-токенов, VWAP-валидатор, Redis Streams.
- [x] **v15.0 - v16.0**: Внедрение Web-сервера графиков TradingView, устранение утечек сокетов и блокировок Event Loop.
- [ ] **Этап 1 (Текущий)**: Live Arbitrage Data-Grid (живая матрица спредов на вебе) + аудио-алерты.
- [ ] **Этап 2**: Квант-индикатор Z-Score (статистический спред) и Wallet Sentinel (детектор ввода/вывода сетей).
- [ ] **Этап 3**: Автоматическое открытие/закрытие позиций (One-Click Execution & Auto Take-Profit).
- [ ] **Этап 4**: Перенос на выделенный Linux VPS кластер (24/7 автономная работа).

Подробная стратегия описана в файле [ROADMAP.md](ROADMAP.md).
