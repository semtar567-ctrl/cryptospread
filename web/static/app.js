/**
 * CryptoSpread Chart - Instant Rendering Application Script with Live WebSockets
 * Integrates TradingView Lightweight Charts, Telegram WebApp SDK, and Dynamic 867 Symbols
 */

function initCryptoSpreadApp() {
  console.log('Initializing CryptoSpread App with Live WebSocket Streaming...');

  // 1. Initialize Telegram WebApp SDK safely
  if (window.Telegram && window.Telegram.WebApp) {
    try {
      window.Telegram.WebApp.ready();
      window.Telegram.WebApp.expand();
    } catch (e) {
      console.warn('Telegram WebApp init warning:', e);
    }
  }

  // 2. DOM Elements
  const chartContainer = document.getElementById('chart-container');
  const buyExchangeSelect = document.getElementById('buy-exchange');
  const sellExchangeSelect = document.getElementById('sell-exchange');
  const pairSymbolSelect = document.getElementById('pair-symbol');
  const pairCountSpan = document.getElementById('pair-count');
  const timeframeButtonsContainer = document.getElementById('timeframe-buttons');
  const refreshBtn = document.getElementById('refresh-btn');
  const loadingOverlay = document.getElementById('loading-overlay');
  const errorBanner = document.getElementById('error-banner');
  const errorMessage = document.getElementById('error-message');

  const comboLabel = document.getElementById('combo-label');
  const spreadValue = document.getElementById('spread-value');
  const pricesValue = document.getElementById('prices-value');
  const liveIndicator = document.getElementById('live-indicator');

  let activeTimeframe = '1m';
  let activeWebSocket = null;
  let currentCandles = [];

  // Helper Functions
  function showSpinner() {
    if (loadingOverlay) loadingOverlay.classList.remove('hidden');
  }

  function hideSpinner() {
    if (loadingOverlay) loadingOverlay.classList.add('hidden');
  }

  function showError(msg) {
    if (errorBanner && errorMessage) {
      errorMessage.textContent = msg;
      errorBanner.classList.remove('hidden');
    }
  }

  function hideError() {
    if (errorBanner) {
      errorBanner.classList.add('hidden');
    }
  }

  function getTimeframeSeconds(tf) {
    const map = {
      '1m': 60,
      '5m': 300,
      '15m': 900,
      '1h': 3600,
      '4h': 14400,
      '1d': 86400,
      '1D': 86400,
    };
    return map[tf] || 60;
  }

  function formatPriceUsd(price) {
    if (price === undefined || price === null || isNaN(price) || price <= 0) return '--';
    const num = Number(price);
    if (num >= 1000) return '$' + num.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    if (num >= 1) return '$' + num.toFixed(4);
    return '$' + num.toFixed(6);
  }

  // Helper to safely select an option
  function setSelectValue(selectElement, targetValue) {
    if (!selectElement || !targetValue) return;
    const targetNorm = targetValue.toLowerCase().replace(/[^a-z0-9]/g, '');

    for (let i = 0; i < selectElement.options.length; i++) {
      const optVal = selectElement.options[i].value.toLowerCase().replace(/[^a-z0-9]/g, '');
      const optText = selectElement.options[i].text.toLowerCase().replace(/[^a-z0-9]/g, '');
      if (optVal === targetNorm || optText === targetNorm) {
        selectElement.selectedIndex = i;
        return;
      }
    }

    // Add option if not present
    const newOpt = document.createElement('option');
    newOpt.value = targetValue;
    newOpt.text = targetValue.toUpperCase();
    newOpt.selected = true;
    selectElement.appendChild(newOpt);
  }

  // 3. Verify Chart Library Availability
  if (typeof LightweightCharts === 'undefined') {
    showError('Chart library failed to load.');
    console.error('LightweightCharts global variable not found!');
    return;
  }

  if (!chartContainer) {
    console.error('Chart container div #chart-container not found!');
    return;
  }

  const getWidth = () => Math.max(chartContainer.clientWidth || chartContainer.offsetWidth || 300, 300);
  const getHeight = () => Math.max(chartContainer.clientHeight || chartContainer.offsetHeight || 400, 400);

  let chart;
  let candlestickSeries;
  let parityLine;

  try {
    chart = LightweightCharts.createChart(chartContainer, {
      width: getWidth(),
      height: getHeight(),
      autoSize: true,
      layout: {
        background: { color: '#0a0e17' },
        textColor: '#d1d4dc',
        fontSize: 12,
        fontFamily: "'Inter', sans-serif",
      },
      grid: {
        vertLines: { color: '#1f293d', style: LightweightCharts.LineStyle.Dotted },
        horzLines: { color: '#1f293d', style: LightweightCharts.LineStyle.Dotted },
      },
      crosshair: {
        mode: LightweightCharts.CrosshairMode.Normal,
      },
      timeScale: {
        borderColor: '#2B2B43',
        timeVisible: true,
        secondsVisible: false,
      },
      rightPriceScale: {
        borderColor: '#2B2B43',
        scaleMargins: { top: 0.1, bottom: 0.1 },
        mode: LightweightCharts.PriceScaleMode.Normal,
      },
    });

    candlestickSeries = chart.addCandlestickSeries({
      upColor: '#26a69a',
      downColor: '#ef5350',
      borderVisible: false,
      wickUpColor: '#26a69a',
      wickDownColor: '#ef5350',
      priceFormat: {
        type: 'custom',
        formatter: (price) => {
          if (typeof price !== 'number' || isNaN(price)) return '--';
          const sign = price > 0 ? '+' : '';
          return `${sign}${price.toFixed(2)}%`;
        },
        minMove: 0.01,
      },
    });

    // Horizontal Parity Reference Line at 0.00% (Convergence Target)
    parityLine = candlestickSeries.createPriceLine({
      price: 0.0000,
      color: '#3b82f6',
      lineWidth: 2,
      lineStyle: LightweightCharts.LineStyle.Dashed,
      axisLabelVisible: true,
      title: '0.00% (PARITY)',
    });

  } catch (err) {
    console.error('Failed to create LightweightChart:', err);
    showError(`Failed to initialize chart: ${err.message}`);
    return;
  }

  // Handle Resize
  window.addEventListener('resize', () => {
    if (chart && chartContainer) {
      chart.applyOptions({
        width: getWidth(),
        height: getHeight(),
      });
    }
  });

  // 4. Dynamic Symbol Loading (867 symbols from Redis)
  async function loadDynamicSymbols() {
    try {
      const res = await fetch('/api/v1/symbols');
      if (!res.ok) return;
      const data = await res.json();
      if (data.symbols && data.symbols.length > 0) {
        const currentVal = pairSymbolSelect.value;
        pairSymbolSelect.innerHTML = '';
        data.symbols.forEach((sym) => {
          const opt = document.createElement('option');
          opt.value = sym;
          opt.textContent = sym;
          pairSymbolSelect.appendChild(opt);
        });
        if (pairCountSpan) {
          pairCountSpan.textContent = data.count || data.symbols.length;
        }
        if (currentVal) {
          setSelectValue(pairSymbolSelect, currentVal);
        }
      }
    } catch (e) {
      console.warn('Could not fetch dynamic symbols list:', e);
    }
  }

  // 5. Parse URL search parameters (e.g. ?buy=binance&sell=bybit&symbol=BTC/USDT&tf=5m)
  function parseUrlParamsAndApply() {
    const urlParams = new URLSearchParams(window.location.search);
    const buyParam = urlParams.get('buy') || urlParams.get('buy_exchange');
    const sellParam = urlParams.get('sell') || urlParams.get('sell_exchange');
    const symbolParam = urlParams.get('symbol') || urlParams.get('pair');
    const tfParam = urlParams.get('tf') || urlParams.get('timeframe');

    let finalSymbol = symbolParam;
    if (finalSymbol && !finalSymbol.includes('/')) {
      const upperSym = finalSymbol.toUpperCase();
      for (const quote of ['USDT', 'USDC', 'BUSD', 'USD', 'BTC']) {
        if (upperSym.endsWith(quote) && upperSym.length > quote.length) {
          finalSymbol = upperSym.slice(0, -quote.length) + '/' + quote;
          break;
        }
      }
    }

    if (buyParam) setSelectValue(buyExchangeSelect, buyParam);
    if (sellParam) setSelectValue(sellExchangeSelect, sellParam);
    if (finalSymbol) setSelectValue(pairSymbolSelect, finalSymbol);

    if (tfParam && timeframeButtonsContainer) {
      const tfButtons = timeframeButtonsContainer.querySelectorAll('.tf-btn');
      tfButtons.forEach((btn) => {
        if (btn.getAttribute('data-tf').toLowerCase() === tfParam.toLowerCase()) {
          tfButtons.forEach((b) => b.classList.remove('active'));
          btn.classList.add('active');
          activeTimeframe = btn.getAttribute('data-tf');
        }
      });
    }
  }

  // 6. Connect Real-time WebSocket
  function connectWebSocket(buyEx, sellEx, symbol) {
    if (activeWebSocket) {
      try {
        activeWebSocket.close();
      } catch (e) {}
      activeWebSocket = null;
    }

    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${wsProtocol}//${window.location.host}/ws/live-spread?buy_exchange=${encodeURIComponent(buyEx)}&sell_exchange=${encodeURIComponent(sellEx)}&symbol=${encodeURIComponent(symbol)}`;

    try {
      activeWebSocket = new WebSocket(wsUrl);

      activeWebSocket.onopen = () => {
        if (liveIndicator) {
          liveIndicator.innerHTML = '<span class="live-dot pulse"></span> REALTIME';
        }
      };

      activeWebSocket.onmessage = (event) => {
        try {
          const tick = JSON.parse(event.data);
          if (!tick || tick.spread_percent === undefined) return;

          const spreadPct = Number(tick.spread_percent);
          const buyPrice = tick.buy_price;
          const sellPrice = tick.sell_price;

          // Update header stats
          if (spreadValue) {
            const sign = spreadPct > 0 ? '+' : '';
            spreadValue.textContent = `${sign}${spreadPct.toFixed(2)}%`;
            spreadValue.className = 'stat-value';
            if (spreadPct > 0.0001) {
              spreadValue.classList.add('positive');
            } else if (spreadPct < -0.0001) {
              spreadValue.classList.add('negative');
            } else {
              spreadValue.classList.add('neutral');
            }
          }

          if (pricesValue) {
            pricesValue.textContent = `Buy: ${formatPriceUsd(buyPrice)} | Sell: ${formatPriceUsd(sellPrice)}`;
          }

          // Update current candle on chart with proper bucketed timeframe support
          if (candlestickSeries && currentCandles.length > 0) {
            const lastCandle = currentCandles[currentCandles.length - 1];
            const nowSec = tick.time || Math.floor(Date.now() / 1000);
            const tfSec = getTimeframeSeconds(activeTimeframe);
            const bucketTime = Math.floor(nowSec / tfSec) * tfSec;

            if (bucketTime > lastCandle.time) {
              // Timeframe rolled over -> start new candle
              const newCandle = {
                time: bucketTime,
                open: spreadPct,
                high: spreadPct,
                low: spreadPct,
                close: spreadPct,
              };
              currentCandles.push(newCandle);
              candlestickSeries.update(newCandle);
            } else {
              // Inside existing timeframe candle -> update High / Low / Close
              lastCandle.high = Math.max(lastCandle.high, spreadPct);
              lastCandle.low = Math.min(lastCandle.low, spreadPct);
              lastCandle.close = spreadPct;

              candlestickSeries.update({
                time: lastCandle.time,
                open: lastCandle.open,
                high: lastCandle.high,
                low: lastCandle.low,
                close: lastCandle.close,
              });
            }
          }
        } catch (err) {
          console.warn('Error processing WS tick:', err);
        }
      };

      activeWebSocket.onclose = () => {
        if (liveIndicator) {
          liveIndicator.innerHTML = '<span class="live-dot paused"></span> PAUSED';
        }
      };
    } catch (e) {
      console.warn('Could not establish WebSocket connection:', e);
    }
  }

  // 7. Main Fetch & Render Function
  async function fetchAndRenderSpreadData() {
    showSpinner();
    hideError();

    const buyEx = buyExchangeSelect ? buyExchangeSelect.value : 'binance';
    const sellEx = sellExchangeSelect ? sellExchangeSelect.value : 'bybit';
    const symbol = pairSymbolSelect ? pairSymbolSelect.value : 'BTC/USDT';

    // Update active combination label
    if (comboLabel) {
      comboLabel.textContent = `${buyEx.toUpperCase()} / ${sellEx.toUpperCase()} (${symbol})`;
    }

    try {
      const queryParams = new URLSearchParams({
        symbol: symbol,
        buy_exchange: buyEx,
        sell_exchange: sellEx,
        timeframe: activeTimeframe,
        limit: '300',
      });

      const response = await fetch(`/api/v1/spread-candles?${queryParams.toString()}`);
      if (!response.ok) {
        const errJson = await response.json().catch(() => ({ detail: response.statusText }));
        throw new Error(errJson.detail || `HTTP Error ${response.status}`);
      }

      const data = await response.json();

      if (!data.candles || data.candles.length === 0) {
        showError('No spread candles returned for the selected exchange pair & timeframe.');
        if (candlestickSeries) candlestickSeries.setData([]);
        if (spreadValue) spreadValue.textContent = '--';
        if (pricesValue) pricesValue.textContent = '--';
        return;
      }

      currentCandles = data.candles;

      // Update Candlestick Series in Percentage Spread %
      if (candlestickSeries) {
        candlestickSeries.setData(currentCandles);
      }
      if (chart) {
        chart.timeScale().fitContent();
        try {
          chart.timeScale().scrollToRealtime();
        } catch (e) {}
      }

      // Calculate latest stats directly in percentage space
      const latestCandle = currentCandles[currentCandles.length - 1];
      const latestSpread = latestCandle.close;

      // Update Stat Cards
      if (spreadValue) {
        const sign = latestSpread > 0 ? '+' : '';
        spreadValue.textContent = `${sign}${latestSpread.toFixed(2)}%`;

        spreadValue.className = 'stat-value';
        if (latestSpread > 0.0001) {
          spreadValue.classList.add('positive');
        } else if (latestSpread < -0.0001) {
          spreadValue.classList.add('negative');
        } else {
          spreadValue.classList.add('neutral');
        }
      }

      // Start live WebSocket stream for this pair
      connectWebSocket(buyEx, sellEx, symbol);

    } catch (err) {
      console.error('Error fetching spread candles:', err);
      showError(`Failed to load data: ${err.message}`);
    } finally {
      hideSpinner();
    }
  }

  // 8. Setup Event Listeners
  if (buyExchangeSelect) {
    buyExchangeSelect.addEventListener('change', fetchAndRenderSpreadData);
  }
  if (sellExchangeSelect) {
    sellExchangeSelect.addEventListener('change', fetchAndRenderSpreadData);
  }
  if (pairSymbolSelect) {
    pairSymbolSelect.addEventListener('change', fetchAndRenderSpreadData);
  }

  if (timeframeButtonsContainer) {
    timeframeButtonsContainer.addEventListener('click', (e) => {
      const targetBtn = e.target.closest('.tf-btn');
      if (!targetBtn) return;

      const tfButtons = timeframeButtonsContainer.querySelectorAll('.tf-btn');
      tfButtons.forEach((b) => b.classList.remove('active'));
      targetBtn.classList.add('active');

      activeTimeframe = targetBtn.getAttribute('data-tf');
      fetchAndRenderSpreadData();
    });
  }

  if (refreshBtn) {
    refreshBtn.addEventListener('click', fetchAndRenderSpreadData);
  }

  // Initial Startup Sequence
  loadDynamicSymbols().then(() => {
    parseUrlParamsAndApply();
    fetchAndRenderSpreadData();
  });
}

// Ensure execution when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initCryptoSpreadApp);
} else {
  initCryptoSpreadApp();
}
