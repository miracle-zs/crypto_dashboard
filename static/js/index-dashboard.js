        lucide.createIcons({ attrs: { 'stroke-width': 1.6 } });

        // ============================================
        // 1. 数据获取 (Fetch from API)
        // ============================================

        const DASHBOARD_UPDATE_INTERVAL = 60000;
        const TRADE_LOG_MOBILE_COLLAPSE_LIMIT = 6;
        const TRADE_PAGE_SIZE = 10;
        let warnedConfig = false;
        let latestTrades = [];
        let tradeLogMobileExpanded = false;
        let tradePageOffset = 0;
        let lastValidTradePageOffset = 0;
        let tradePageHasNext = false;
        let totalTradePages = 1;

        function toggleTradeLogMobile() {
            tradeLogMobileExpanded = !tradeLogMobileExpanded;
            renderTable(latestTrades);
        }

        function goToPrevTradePage() {
            if (tradePageOffset <= 0) return;
            tradePageOffset = Math.max(0, tradePageOffset - TRADE_PAGE_SIZE);
            tradeLogMobileExpanded = false;
            fetchDashboardData();
        }

        function goToNextTradePage() {
            if (!tradePageHasNext) return;
            tradePageOffset += TRADE_PAGE_SIZE;
            tradeLogMobileExpanded = false;
            fetchDashboardData();
        }

        function jumpToTradePage() {
            const selectEl = document.getElementById('trade-page-select');
            if (!selectEl) return;
            const page = parseInt(String(selectEl.value || '').trim(), 10);
            if (!Number.isFinite(page) || page < 1) {
                return;
            }
            tradePageOffset = (page - 1) * TRADE_PAGE_SIZE;
            tradeLogMobileExpanded = false;
            fetchDashboardData({ suppressOutOfRangeAlert: false });
        }

        function renderTradePageOptions() {
            const selectEl = document.getElementById('trade-page-select');
            if (!selectEl) return;
            const currentPage = Math.floor(tradePageOffset / TRADE_PAGE_SIZE) + 1;
            const pages = Math.max(1, totalTradePages);
            const existing = Number(selectEl.dataset.pageCount || 0);
            if (existing !== pages) {
                const options = [];
                for (let i = 1; i <= pages; i++) {
                    options.push(`<option value="${i}">第 ${i} 页</option>`);
                }
                selectEl.innerHTML = options.join('');
                selectEl.dataset.pageCount = String(pages);
            }
            selectEl.value = String(Math.min(currentPage, pages));
        }

        function updateTradePagination(visibleCount) {
            const pageInfoEl = document.getElementById('trade-page-info');
            const prevBtn = document.getElementById('trade-page-prev');
            const nextBtn = document.getElementById('trade-page-next');
            if (!pageInfoEl || !prevBtn || !nextBtn) return;

            const pageNumber = Math.floor(tradePageOffset / TRADE_PAGE_SIZE) + 1;
            if (visibleCount > 0) {
                const start = tradePageOffset + 1;
                const end = tradePageOffset + visibleCount;
                pageInfoEl.innerText = `第 ${pageNumber} 页 · ${start}-${end}`;
            } else {
                pageInfoEl.innerText = `第 ${pageNumber} 页 · 0 条`;
            }

            prevBtn.disabled = tradePageOffset <= 0;
            nextBtn.disabled = !tradePageHasNext;
            renderTradePageOptions();
        }

        async function fetchDashboardData(options = {}) {
            const suppressOutOfRangeAlert = Boolean(options.suppressOutOfRangeAlert);
            try {
                const tradesUrl = `/api/trades?limit=${TRADE_PAGE_SIZE + 1}&offset=${tradePageOffset}`;
                // Parallel fetching for performance
                const [statusRes, summaryRes, tradesRes, aggregatesRes, dailyStatsRes] = await Promise.all([
                    fetch('/api/status'),
                    fetch('/api/summary'),
                    fetch(tradesUrl),
                    fetch('/api/trades-aggregates'),
                    fetch('/api/daily-stats')
                ]);

                // Check status
                const status = await statusRes.json();

                if (!status.configured) {
                    if (!warnedConfig) {
                        alert("System Warning: API Keys not configured in .env file");
                        warnedConfig = true;
                    }
                    return;
                }

                // Process data
                const summaryData = await summaryRes.json();
                const tradesRaw = await tradesRes.json();
                const tradesWithProbe = Array.isArray(tradesRaw) ? tradesRaw : [];
                tradePageHasNext = tradesWithProbe.length > TRADE_PAGE_SIZE;
                const tradesData = tradePageHasNext
                    ? tradesWithProbe.slice(-TRADE_PAGE_SIZE)
                    : tradesWithProbe;
                if (tradesData.length === 0 && tradePageOffset > 0) {
                    const attemptedOffset = tradePageOffset;
                    tradePageOffset = lastValidTradePageOffset;
                    tradePageHasNext = false;
                    if (!suppressOutOfRangeAlert) {
                        alert("页码超出范围，已返回最近可用页。");
                    }
                    if (attemptedOffset !== tradePageOffset) {
                        return fetchDashboardData({ suppressOutOfRangeAlert: true });
                    }
                }
                if (tradesData.length > 0 || tradePageOffset === 0) {
                    lastValidTradePageOffset = tradePageOffset;
                }
                const aggregatesData = await aggregatesRes.json();
                const dailyStatsData = await dailyStatsRes.json();

                updateUI(summaryData, tradesData, dailyStatsData, aggregatesData);
                updateLastUpdated();

            } catch (error) {
                console.error("Error fetching data:", error);
                document.getElementById('disp-balance').innerText = "Error";
            }
        }

        // ============================================
        // 2. UI 更新逻辑
        // ============================================
        function updateUI(stats, trades, dailyStats, aggregates) {
            latestTrades = Array.isArray(trades) ? trades : [];
            const tradeAggregates = aggregates || {};
            totalTradePages = Math.max(1, Math.ceil((Number(stats.total_trades) || 0) / TRADE_PAGE_SIZE));
            applyMood(stats);

            // 资金与盈亏
            const equityCurve = Array.isArray(stats.equity_curve) ? stats.equity_curve : [];
            const balance = Number(equityCurve.length ? equityCurve[equityCurve.length - 1] : 0) || 0;
            document.getElementById('disp-balance').innerText = `$${balance.toLocaleString()}`;

            const pnlEl = document.getElementById('disp-pnl');
            pnlEl.innerText = `${stats.total_pnl > 0 ? '+' : ''}$${stats.total_pnl.toLocaleString()}`;
            pnlEl.className = `text-l2 font-bold pulse-text ${stats.total_pnl >= 0 ? 'text-green-400' : 'text-red-500'}`;

            // ROI Calculation - uses a fixed initial capital based on the UI text
            const initialCapital = 24000; 
            const roi = initialCapital > 0 ? (stats.total_pnl / initialCapital) * 100 : 0;
            document.getElementById('disp-roi').innerText = `${roi.toFixed(2)}%`;
            document.getElementById('disp-roi').className = `text-l2 font-bold ${roi >= 0 ? 'text-green-400' : 'text-red-500'}`;

            document.getElementById('disp-fees').innerText = `$${stats.total_fees}`;
            document.getElementById('disp-wins').innerText = stats.win_count;
            document.getElementById('disp-losses').innerText = stats.loss_count;

            // 高级指标
            document.getElementById('metric-winrate').innerText = stats.win_rate.toFixed(1) + '%';
            document.getElementById('bar-winrate').style.width = stats.win_rate + '%';

            document.getElementById('metric-rr').innerText = stats.risk_reward_ratio.toFixed(2);
            document.getElementById('metric-rr').className = `text-l2 font-bold mt-1 ${stats.risk_reward_ratio >= 1.5 ? 'text-amber-400' : 'text-gray-500'}`;

            document.getElementById('metric-pf').innerText = stats.profit_factor.toFixed(2);
            document.getElementById('metric-pf').className = `text-l2 font-bold mt-1 ${stats.profit_factor >= 2 ? 'text-green-400' : 'text-gray-400'}`;

            const evEl = document.getElementById('metric-ev');
            evEl.innerText = `$${stats.expected_value.toFixed(0)}`;
            evEl.className = `text-l3 font-bold mt-1 ${stats.expected_value >= 0 ? 'text-emerald-400' : 'text-red-400'}`;

            // Kelly
            const kellyPct = (stats.kelly_criterion * 100).toFixed(1);
            const kellyEl = document.getElementById('metric-kelly');
            kellyEl.innerText = stats.kelly_criterion > 0 ? `${kellyPct}%` : '0%';
            kellyEl.className = `text-l2 font-bold mt-2 ${stats.kelly_criterion > 0.2 ? 'text-purple-400' : 'text-gray-500'}`;

            // SQN
            const sqnEl = document.getElementById('metric-sqn');
            sqnEl.innerText = stats.sqn.toFixed(2);
            let sqnColor = 'text-gray-500';
            if (stats.sqn > 1.6) sqnColor = 'text-blue-400'; // Average
            if (stats.sqn > 2.0) sqnColor = 'text-green-400'; // Good
            if (stats.sqn > 3.0) sqnColor = 'text-purple-400'; // Holy Grail
            sqnEl.className = `text-l2 font-bold mt-2 ${sqnColor}`;

            // Streak & Drawdown
            const streakEl = document.getElementById('metric-streak');
            streakEl.innerText = stats.current_streak > 0 ? `+${stats.current_streak} W` : `${stats.current_streak} L`;
            streakEl.className = `text-l3 font-bold mt-1 ${stats.current_streak > 0 ? 'text-green-400' : 'text-red-500'}`;

            const bestEl = document.getElementById('metric-streak-best');
            const worstEl = document.getElementById('metric-streak-worst');
            if (bestEl && worstEl) {
                if (stats.total_trades > 0) {
                    bestEl.innerText = `+${stats.best_win_streak} W`;
                    worstEl.innerText = `-${Math.abs(stats.worst_loss_streak)} L`;
                } else {
                    bestEl.innerText = '--';
                    worstEl.innerText = '--';
                }
            }

            const maxSingleLoss = Number(
                stats.max_single_loss !== undefined ? stats.max_single_loss : stats.max_drawdown
            ) || 0;
            document.getElementById('metric-mdd').innerText = `-$${Math.abs(maxSingleLoss)}`;

            renderChart(stats.equity_curve);
            renderDurationChart(tradeAggregates.duration_points || []);
            renderHourlyChart(tradeAggregates.hourly_pnl || []);
            renderDailyTradesChart(dailyStats);
            renderTable(latestTrades);
            updateTradePagination(latestTrades.length);
            renderSymbolLeaderboard(tradeAggregates.symbol_rank || { winners: [], losers: [] });
            initKpiFocus();
        }

        function updateLastUpdated() {
            const el = document.getElementById('last-updated');
            if (!el) return;
            const ts = new Date().toLocaleString('sv-SE', { timeZone: 'Asia/Shanghai' });
            el.innerText = ts;
            el.classList.remove('update-flash');
            void el.offsetWidth;
            el.classList.add('update-flash');
        }

        function applyMood(stats) {
            const down = stats && stats.total_pnl < 0;
            document.body.classList.toggle('mood-down', down);
        }


        // ============================================
        // 3. 图表渲染 (ApexCharts)
        // ============================================
        let equityChartInstance = null;
        let durationChartInstance = null;
        let hourlyChartInstance = null;
        let dailyTradesChartInstance = null;

        function renderChart(dataPoints) {
            const categories = dataPoints.map((_, i) => i === 0 ? 'Start' : i);

            const options = {
                chart: {
                    type: 'area',
                    height: 250,
                    toolbar: { show: false },
                    background: 'transparent',
                    fontFamily: 'Share Tech Mono'
                },
                series: [{ name: 'Equity', data: dataPoints }],
                theme: { mode: document.body.classList.contains('dark-mode') ? 'dark' : 'light' },
                stroke: { curve: 'smooth', width: 2.5, colors: ['#0ea5e9'] },
                fill: {
                    type: 'gradient',
                    gradient: {
                        shadeIntensity: 1,
                        opacityFrom: 0.7,
                        opacityTo: 0.1,
                        stops: [0, 90, 100],
                        colorStops: [{ offset: 0, color: '#0ea5e9', opacity: 0.5 }, { offset: 100, color: '#0ea5e9', opacity: 0 }]
                    }
                },
                dataLabels: { enabled: false },
                grid: { borderColor: document.body.classList.contains('dark-mode') ? '#334155' : '#e2e8f0', strokeDashArray: 4 },
                xaxis: {
                    categories: categories,
                    labels: { style: { colors: '#64748b' }, show: false }, // Hide labels if too many
                    axisBorder: { show: false },
                    axisTicks: { show: false }
                },
                yaxis: {
                    labels: {
                        style: { colors: '#64748b' },
                        formatter: (value) => { return "$" + (value / 1000).toFixed(1) + "k" }
                    }
                },
                tooltip: { theme: document.body.classList.contains('dark-mode') ? 'dark' : 'light' }
            };

            if (equityChartInstance) {
                equityChartInstance.destroy();
            }
            equityChartInstance = new ApexCharts(document.querySelector("#equityChart"), options);
            equityChartInstance.render();
        }

        function renderDurationChart(durationPoints) {
            const wins = [];
            const losses = [];

            durationPoints.forEach(p => {
                const xVal = Number(p.x) || 0;
                const yVal = Number(p.y) || 0;
                const payload = {
                    x: xVal,
                    y: yVal,
                    symbol: p.symbol || '--',
                    time: p.time || '--',
                };
                if (yVal >= 0) {
                    wins.push(payload);
                } else {
                    losses.push(payload);
                }
            });

            const options = {
                chart: {
                    type: 'scatter',
                    height: 200,
                    toolbar: { show: false },
                    background: 'transparent',
                    fontFamily: 'Share Tech Mono',
                    zoom: { enabled: true, type: 'xy' }
                },
                series: [
                    { name: 'Wins', data: wins },
                    { name: 'Losses', data: losses }
                ],
                colors: ['#10b981', '#ef4444'], // Green, Red
                grid: {
                    borderColor: document.body.classList.contains('dark-mode') ? '#334155' : '#e2e8f0',
                    strokeDashArray: 4,
                    xaxis: { lines: { show: true } },
                    yaxis: { lines: { show: true } },
                },
                xaxis: {
                    type: 'numeric',
                    tickAmount: 10,
                    labels: {
                        style: { colors: '#64748b' },
                        formatter: (val) => { return val < 60 ? val.toFixed(0) + 'm' : (val / 60).toFixed(1) + 'h'; }
                    },
                    title: { text: 'Duration', style: { color: '#475569', fontSize: '11px' } }
                },
                yaxis: {
                    labels: { style: { colors: '#64748b' } },
                },
                markers: { size: 4, strokeWidth: 0, hover: { size: 6 } },
                tooltip: {
                    theme: document.body.classList.contains('dark-mode') ? 'dark' : 'light',
                    custom: function({series, seriesIndex, dataPointIndex, w}) {
                        const data = w.config.series[seriesIndex].data[dataPointIndex];
                        return `
                            <div class="px-2 py-1 bg-slate-800 border border-gray-700 text-xs">
                                <div class="font-bold text-white">${data.symbol}</div>
                                <div class="text-gray-400">Time: ${data.time}</div>
                                <div class="${data.y >= 0 ? 'text-green-400' : 'text-red-400'}">PnL: $${data.y}</div>
                            </div>
                        `;
                    }
                }
            };

            if (durationChartInstance) {
                durationChartInstance.destroy();
            }
            durationChartInstance = new ApexCharts(document.querySelector("#durationChart"), options);
            durationChartInstance.render();
        }

        function renderHourlyChart(hourlyPnlRaw) {
            const hourlyPnL = Array.from({ length: 24 }, (_, i) => Number(hourlyPnlRaw[i]) || 0);

            // 2. Configure Chart
            const options = {
                chart: {
                    type: 'bar',
                    height: 200,
                    toolbar: { show: false },
                    fontFamily: 'Share Tech Mono',
                    background: 'transparent'
                },
                series: [{
                    name: 'Net PnL',
                    data: hourlyPnL
                }],
                colors: ['#0ea5e9'],
                plotOptions: {
                    bar: {
                        borderRadius: 2,
                        columnWidth: '60%',
                        colors: {
                            ranges: [{
                                from: -Infinity,
                                to: -0.01,
                                color: '#ef4444' // Red for Loss
                            }, {
                                from: 0,
                                to: Infinity,
                                color: '#10b981' // Green for Profit
                            }]
                        }
                    }
                },
                dataLabels: { enabled: false },
                grid: {
                    borderColor: document.body.classList.contains('dark-mode') ? '#334155' : '#e2e8f0',
                    strokeDashArray: 4,
                    yaxis: { lines: { show: true } },
                    xaxis: { lines: { show: false } }
                },
                xaxis: {
                    categories: Array.from({length: 24}, (_, i) => i),
                    labels: {
                        style: { colors: '#64748b', fontSize: '11px' },
                        // Show every 4th hour to avoid clutter (0, 4, 8...)
                        formatter: function(val) {
                            return val % 4 === 0 ? val + 'h' : '';
                        }
                    },
                    axisBorder: { show: false },
                    axisTicks: { show: false }
                },
                yaxis: {
                    labels: {
                        style: { colors: '#64748b', fontSize: '11px' },
                        formatter: (val) => {
                            return val >= 1000 || val <= -1000
                                ? (val/1000).toFixed(1) + 'k'
                                : val.toFixed(0);
                        }
                    }
                },
                tooltip: {
                    theme: document.body.classList.contains('dark-mode') ? 'dark' : 'light',
                    y: {
                        formatter: function (val) {
                            return "$" + val.toLocaleString();
                        }
                    }
                }
            };

            if (hourlyChartInstance) {
                hourlyChartInstance.destroy();
            }
            hourlyChartInstance = new ApexCharts(document.querySelector("#hourlyChart"), options);
            hourlyChartInstance.render();
        }

        function renderDailyTradesChart(dailyStats) {
            if (!dailyStats || dailyStats.length === 0) return;

            // 按日期正序排列（API返回的是倒序）
            const sortedData = [...dailyStats].reverse();

            // 只显示最近14天
            const recentData = sortedData.slice(-14);

            const dates = recentData.map(d => {
                const raw = String(d.date || '');
                if (raw.length === 8) {
                    // YYYYMMDD -> MM-DD
                    return `${raw.slice(4, 6)}-${raw.slice(6, 8)}`;
                }
                // Fallback for YYYY-MM-DD or other formats
                return raw.substring(5);
            });
            const tradeCounts = recentData.map(d => d.trade_count);
            const totalAmounts = recentData.map(d => d.total_amount);

            const options = {
                chart: {
                    type: 'line',
                    height: 180,
                    toolbar: { show: false },
                    fontFamily: 'Share Tech Mono',
                    background: 'transparent'
                },
                series: [
                    {
                        name: '开单数量',
                        type: 'column',
                        data: tradeCounts
                    },
                    {
                        name: '开单金额',
                        type: 'line',
                        data: totalAmounts
                    }
                ],
                colors: ['#0ea5e9', '#f59e0b'],
                stroke: {
                    width: [0, 3],
                    curve: 'smooth'
                },
                plotOptions: {
                    bar: {
                        borderRadius: 3,
                        columnWidth: '50%'
                    }
                },
                dataLabels: { enabled: false },
                tooltip: {
                    theme: document.body.classList.contains('dark-mode') ? 'dark' : 'light',
                    shared: true,
                    intersect: false,
                    y: [
                        { formatter: (val) => `${val.toFixed(0)} 单` },
                        { formatter: (val) => val >= 1000 ? `$${(val/1000).toFixed(1)}k` : `$${val.toFixed(0)}` }
                    ]
                },
                grid: {
                    borderColor: document.body.classList.contains('dark-mode') ? '#334155' : '#e2e8f0',
                    strokeDashArray: 4
                },
                xaxis: {
                    categories: dates,
                    labels: {
                        style: { colors: '#64748b', fontSize: '9px' },
                        rotate: -45,
                        rotateAlways: true
                    },
                    axisBorder: { show: false },
                    axisTicks: { show: false }
                },
                yaxis: [
                    {
                        title: {
                            text: '数量',
                            style: { color: '#0ea5e9', fontSize: '11px' }
                        },
                        labels: {
                            style: { colors: '#64748b', fontSize: '11px' },
                            formatter: (val) => val.toFixed(0)
                        }
                    },
                    {
                        opposite: true,
                        title: {
                            text: '金额',
                            style: { color: '#f59e0b', fontSize: '11px' }
                        },
                        labels: {
                            style: { colors: '#64748b', fontSize: '11px' },
                            formatter: (val) => {
                                return val >= 1000 ? (val/1000).toFixed(0) + 'k' : val.toFixed(0);
                            }
                        }
                    }
                ],
                legend: {
                    show: true,
                    position: 'top',
                    horizontalAlign: 'right',
                    fontSize: '11px',
                    labels: { colors: '#94a3b8' },
                    markers: { width: 8, height: 8 }
                },
                tooltip: {
                    theme: document.body.classList.contains('dark-mode') ? 'dark' : 'light',
                    shared: true,
                    y: {
                        formatter: function (val, { seriesIndex }) {
                            if (seriesIndex === 0) return val + ' 笔';
                            return '$' + val.toLocaleString();
                        }
                    }
                }
            };

            if (dailyTradesChartInstance) {
                dailyTradesChartInstance.destroy();
            }
            dailyTradesChartInstance = new ApexCharts(document.querySelector("#dailyTradesChart"), options);
            dailyTradesChartInstance.render();
        }

        // ============================================
        // 4. 表格渲染
        // ============================================
        function renderTable(tradeData) {
            const tbody = document.getElementById('trade-table-body');
            const mobileContainer = document.getElementById('trade-list-mobile');
            const mobileToggleWrap = document.getElementById('trade-list-mobile-toggle-wrap');
            const mobileToggleBtn = document.getElementById('trade-list-mobile-toggle');
            const isMobile = window.matchMedia('(max-width: 767px)').matches;

            if (tbody) tbody.innerHTML = '';
            if (mobileContainer) mobileContainer.innerHTML = '';

            // Reverse to show latest first
            const reversedData = [...tradeData].reverse();

            if (reversedData.length === 0) {
                 if(tbody) tbody.innerHTML = '<tr><td colspan="8" class="px-6 py-8 text-center text-gray-500">暂无交易记录</td></tr>';
                 if(mobileContainer) mobileContainer.innerHTML = '<div class="text-center text-gray-500 py-8">暂无交易记录</div>';
                 if (mobileToggleWrap) mobileToggleWrap.classList.add('hidden');
                 return;
            }

            const hasMoreOnMobile = reversedData.length > TRADE_LOG_MOBILE_COLLAPSE_LIMIT;
            const mobileVisibleData = (isMobile && !tradeLogMobileExpanded)
                ? reversedData.slice(0, TRADE_LOG_MOBILE_COLLAPSE_LIMIT)
                : reversedData;

            reversedData.forEach(t => {
                const isWin = t.pnl_net > 0;
                // Parse date string MM-DD HH:MM
                const dateParts = t.entry_time.split(' ');
                const dateStr = dateParts[0].substring(5); // Remove YYYY-
                const timeStr = dateParts[1].substring(0, 5); // HH:MM

                // Desktop Table Row
                const row = `
                    <tr class="table-row hover:bg-white/5 transition-colors border-b border-gray-800">
                        <td class="px-6 py-4">
                            <div class="text-white font-bold">${timeStr}</div>
                            <div class="text-[11px] text-gray-500">${dateStr}</div>
                        </td>
                        <td class="px-6 py-4 font-bold text-cyan-400">${t.symbol}</td>
                        <td class="px-6 py-4 text-xs font-mono">${t.holding_time}</td>
                        <td class="px-6 py-4">
                            <span class="${t.side === 'LONG' ? 'text-green-400 border-green-500/30' : 'text-red-400 border-red-500/30'} text-xs border px-1 rounded">
                                ${t.side}
                            </span>
                        </td>
                        <td class="px-6 py-4 text-xs font-mono">${t.entry_price} → ${t.exit_price}</td>
                        <td class="px-6 py-4 text-right font-mono ${isWin ? 'text-green-400' : 'text-red-500'}">
                            ${isWin ? '+' : ''}${t.pnl_net}
                        </td>
                        <td class="px-6 py-4 text-right font-mono ${isWin ? 'text-green-400' : 'text-red-500'}">
                            ${t.return_rate}
                        </td>
                        <td class="px-6 py-4 text-center">
                            <span class="text-[11px] px-2 py-1 rounded ${isWin ? 'bg-green-500/10 text-green-400' : 'bg-red-500/10 text-red-400'}">
                                ${isWin ? 'WIN' : 'LOSS'}
                            </span>
                        </td>
                    </tr>
                `;
                if(tbody) tbody.innerHTML += row;
            });

            mobileVisibleData.forEach(t => {
                const isWin = t.pnl_net > 0;
                const dateParts = t.entry_time.split(' ');
                const dateStr = dateParts[0].substring(5);
                const timeStr = dateParts[1].substring(0, 5);

                const card = `
                    <div class="p-4 bg-gray-800/30 border border-gray-700/50 rounded-lg hover:border-gray-600 transition-colors">
                        <div class="flex justify-between items-start mb-3 border-b border-gray-700/50 pb-2">
                            <div class="flex flex-col">
                                <span class="font-bold text-white text-base">${t.symbol}</span>
                                <span class="text-[10px] text-gray-500">${dateStr} ${timeStr}</span>
                            </div>
                            <div class="flex flex-col items-end gap-1">
                                <span class="${t.side === 'LONG' ? 'text-green-400 border-green-500/30' : 'text-red-400 border-red-500/30'} text-[10px] border px-1.5 py-0.5 rounded font-bold">
                                    ${t.side}
                                </span>
                                <span class="text-[10px] ${isWin ? 'text-green-400' : 'text-red-400'} font-bold">
                                    ${isWin ? 'WIN' : 'LOSS'}
                                </span>
                            </div>
                        </div>

                        <div class="space-y-2 text-sm">
                            <div class="flex justify-between items-center">
                                <span class="text-gray-500 text-xs">PnL (Net)</span>
                                <span class="font-mono font-bold ${isWin ? 'text-green-400' : 'text-red-500'}">
                                    ${isWin ? '+' : ''}${t.pnl_net}
                                </span>
                            </div>
                            <div class="flex justify-between items-center">
                                <span class="text-gray-500 text-xs">ROI</span>
                                <span class="font-mono ${isWin ? 'text-green-400' : 'text-red-500'}">
                                    ${t.return_rate}
                                </span>
                            </div>
                             <div class="flex justify-between items-center">
                                <span class="text-gray-500 text-xs">Price</span>
                                <span class="font-mono text-xs text-gray-300">
                                    ${t.entry_price} → ${t.exit_price}
                                </span>
                            </div>
                            <div class="flex justify-between items-center">
                                <span class="text-gray-500 text-xs">Duration</span>
                                <span class="font-mono text-xs text-gray-300">
                                    ${t.holding_time}
                                </span>
                            </div>
                        </div>
                    </div>
                `;
                if(mobileContainer) mobileContainer.innerHTML += card;
            });

            if (mobileToggleWrap && mobileToggleBtn) {
                if (isMobile && hasMoreOnMobile) {
                    mobileToggleWrap.classList.remove('hidden');
                    mobileToggleBtn.innerText = tradeLogMobileExpanded
                        ? '收起'
                        : `展开全部 (${reversedData.length})`;
                } else {
                    mobileToggleWrap.classList.add('hidden');
                }
            }
        }

        function renderSymbolLeaderboard(symbolRank) {
            const redEl = document.getElementById('redlist');
            const blackEl = document.getElementById('blacklist');
            if (!redEl || !blackEl) return;
            const winners = Array.isArray(symbolRank?.winners) ? symbolRank.winners : [];
            const losers = Array.isArray(symbolRank?.losers) ? symbolRank.losers : [];

            const maxWinPnl = winners.length ? Math.max(...winners.map(r => Number(r.pnl) || 0)) : 1;
            const maxLossPnl = losers.length ? Math.min(...losers.map(r => Number(r.pnl) || 0)) : -1;

            const renderList = (list, el, positive) => {
                if (!list.length) {
                    el.innerHTML = `<div class="text-[11px] text-gray-500">暂无数据</div>`;
                    return;
                }
                el.innerHTML = list.map(item => {
                    const winRate = Number(item.win_rate || 0).toFixed(0);
                    const contribution = Number(item.share || 0).toFixed(0);
                    const pnl = Number(item.pnl) || 0;
                    const denom = positive ? maxWinPnl : Math.abs(maxLossPnl);
                    const barPct = denom > 0 ? Math.min((Math.abs(pnl) / denom) * 100, 100) : 0;
                    return `
                        <div class="space-y-1">
                            <div class="grid grid-cols-[1fr_auto] items-center gap-2">
                                <div class="text-white font-mono truncate">${item.symbol}</div>
                                <div class="grid grid-cols-[7ch_6ch_6ch_6ch] gap-2 text-right font-mono tabular-nums">
                                    <span class="${positive ? 'text-emerald-400' : 'text-red-400'} font-mono">${positive ? '+' : ''}${pnl.toFixed(0)}</span>
                                    <span class="text-gray-500 font-mono">${Number(item.trade_count || 0)}</span>
                                    <span class="text-gray-500 font-mono">${winRate}%</span>
                                    <span class="text-gray-500 font-mono">${contribution}%</span>
                                </div>
                            </div>
                            <div class="grid grid-cols-[1fr_auto] items-center gap-2">
                                <div class="h-1.5 bg-gray-800 rounded-full overflow-hidden">
                                    <div class="h-full ${positive ? 'bg-emerald-400' : 'bg-red-400'}" style="width: ${barPct}%"></div>
                                </div>
                                <div></div>
                            </div>
                        </div>
                    `;
                }).join('');
            };

            renderList(winners, redEl, true);
            renderList(losers, blackEl, false);
        }

        let kpiBound = false;
        function initKpiFocus() {
            if (kpiBound) return;
            const cards = document.querySelectorAll('.kpi-card');
            if (!cards.length) return;
            kpiBound = true;
            cards.forEach(card => {
                card.addEventListener('click', () => {
                    const isActive = card.classList.contains('kpi-active');
                    cards.forEach(c => c.classList.remove('kpi-active', 'kpi-muted'));
                    if (!isActive) {
                        cards.forEach(c => { if (c !== card) c.classList.add('kpi-muted'); });
                        card.classList.add('kpi-active');
                    }
                });
            });
        }

        // 启动
        fetchDashboardData();
        setInterval(fetchDashboardData, DASHBOARD_UPDATE_INTERVAL);
        fetchMonthlyProgress();

        // ============================================
        // 5. 月度目标进度
        // ============================================
        async function fetchMonthlyProgress() {
            try {
                const res = await fetch('/api/monthly-progress');
                const data = await res.json();
                updateMonthlyProgress(data);
            } catch (error) {
                console.error("Error fetching monthly progress:", error);
            }
        }

        function updateMonthlyProgress(data) {
            const currentEl = document.getElementById('monthly-current');
            const targetEl = document.getElementById('monthly-target');
            const progressBar = document.getElementById('monthly-progress-bar');
            const progressPct = document.getElementById('monthly-progress-pct');
            const remainingEl = document.getElementById('monthly-remaining');

            const current = data.current;
            const target = data.target;
            const progress = Math.min(Math.max(data.progress, 0), 100);
            const remaining = Math.max(target - current, 0);

            // 更新显示
            currentEl.innerText = `$${current.toLocaleString()}`;
            currentEl.className = `text-l2 font-bold ${current >= 0 ? 'text-emerald-400' : 'text-red-400'}`;

            targetEl.innerText = `$${target.toLocaleString()}`;
            progressBar.style.width = `${progress}%`;
            progressPct.innerText = `${progress.toFixed(1)}%`;
            remainingEl.innerText = `$${remaining.toLocaleString()}`;

            // 根据进度改变颜色
            if (progress >= 100) {
                progressBar.style.background = 'linear-gradient(90deg, #10b981, #34d399, #fbbf24)';
                progressPct.className = 'text-yellow-400';
            } else if (progress >= 75) {
                progressBar.style.background = 'linear-gradient(90deg, #10b981, #34d399)';
            } else if (progress >= 50) {
                progressBar.style.background = 'linear-gradient(90deg, #0ea5e9, #10b981)';
            } else {
                progressBar.style.background = 'linear-gradient(90deg, #6366f1, #0ea5e9)';
            }
        }

        function openTargetModal() {
            const modal = document.getElementById('targetModal');
            modal.classList.remove('hidden');
            modal.classList.add('flex');
            // 获取当前目标值填入输入框
            fetch('/api/monthly-progress')
                .then(res => res.json())
                .then(data => {
                    document.getElementById('targetInput').value = data.target;
                });
        }

        function closeTargetModal() {
            const modal = document.getElementById('targetModal');
            modal.classList.add('hidden');
            modal.classList.remove('flex');
        }

        async function saveTarget() {
            const input = document.getElementById('targetInput');
            const target = parseFloat(input.value);

            if (!target || target <= 0) {
                alert('请输入有效的目标金额');
                return;
            }

            try {
                const res = await fetch(`/api/monthly-target?target=${target}`, {
                    method: 'POST'
                });

                if (res.ok) {
                    closeTargetModal();
                    fetchMonthlyProgress();
                } else {
                    alert('保存失败');
                }
            } catch (error) {
                console.error("Error saving target:", error);
                alert('保存失败');
            }
        }

        // Theme Handling
        const btnThemeToggle = document.getElementById('btn-theme-toggle');
        let isDarkMode = localStorage.getItem('theme') === 'dark';

        function applyTheme(dark) {
            if (dark) {
                document.documentElement.classList.add('dark-mode');
                document.body.classList.add('dark-mode');
                if (btnThemeToggle) btnThemeToggle.innerHTML = '<i data-lucide="sun" class="w-4 h-4"></i>';
            } else {
                document.documentElement.classList.remove('dark-mode');
                document.body.classList.remove('dark-mode');
                if (btnThemeToggle) btnThemeToggle.innerHTML = '<i data-lucide="moon" class="w-4 h-4"></i>';
            }
            lucide.createIcons();
            // Refresh data to update charts theme
            fetchDashboardData();
        }

        if (btnThemeToggle) {
            btnThemeToggle.addEventListener('click', () => {
                isDarkMode = !isDarkMode;
                localStorage.setItem('theme', isDarkMode ? 'dark' : 'light');
                applyTheme(isDarkMode);
            });
        }

        // Initialize Theme
        applyTheme(isDarkMode);

