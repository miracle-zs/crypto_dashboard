    const themeKey = 'theme';
    const btnTheme = document.getElementById('btn-theme-toggle');
    const btnRefresh = document.getElementById('btn-refresh');
    const statusText = document.getElementById('status-text');
    const lastUpdated = document.getElementById('last-updated');
    const metricTop = document.getElementById('metric-top');
    const metricDate = document.getElementById('metric-date');
    const metricTime = document.getElementById('metric-time');
    const snapshotContextMain = document.getElementById('snapshot-context-main');
    const snapshotContextSub = document.getElementById('snapshot-context-sub');
    const gainersTitle = document.getElementById('gainers-title');
    const losersTitle = document.getElementById('losers-title');
    const tableBody = document.getElementById('leaderboard-body');
    const cards = document.getElementById('leaderboard-cards');
    const losersTableBody = document.getElementById('losers-body');
    const losersCards = document.getElementById('losers-cards');
    const reboundPanels = {
        14: {
            summary: document.getElementById('rebound-14-summary'),
            window: document.getElementById('rebound-14-window'),
            body: document.getElementById('rebound-14-body'),
            cards: document.getElementById('rebound-14-cards'),
        },
        30: {
            summary: document.getElementById('rebound-30-summary'),
            window: document.getElementById('rebound-30-window'),
            body: document.getElementById('rebound-30-body'),
            cards: document.getElementById('rebound-30-cards'),
        },
        60: {
            summary: document.getElementById('rebound-60-summary'),
            window: document.getElementById('rebound-60-window'),
            body: document.getElementById('rebound-60-body'),
            cards: document.getElementById('rebound-60-cards'),
        },
    };
    const losersHitSummary = document.getElementById('losers-hit-summary');
    const losersHitSymbols = document.getElementById('losers-hit-symbols');
    const metric2Summary = document.getElementById('metric2-summary');
    const metric2HitSymbols = document.getElementById('metric2-hit-symbols');
    const metric2SampleNote = document.getElementById('metric2-sample-note');
    const metric2ContinuationPool = document.getElementById('metric2-continuation-pool');
    const metric2Body = document.getElementById('metric2-body');
    const metric2Cards = document.getElementById('metric2-cards');
    const metric3Summary = document.getElementById('metric3-summary');
    const metric3SampleNote = document.getElementById('metric3-sample-note');
    const metric3DistLabel = document.getElementById('metric3-dist-label');
    const metric3DistNeg = document.getElementById('metric3-dist-neg');
    const metric3DistMid = document.getElementById('metric3-dist-mid');
    const metric3DistPos = document.getElementById('metric3-dist-pos');
    const metric3Body = document.getElementById('metric3-body');
    const metric3Cards = document.getElementById('metric3-cards');
    const metricsHistoryBody = document.getElementById('metrics-history-body');
    const metricsHistoryCards = document.getElementById('metrics-history-cards');
    const snapshotDateSelect = document.getElementById('snapshot-date');
    const btnMobileGainers = document.getElementById('btn-mobile-gainers');
    const btnMobileLosers = document.getElementById('btn-mobile-losers');
    const panelGainers = document.getElementById('panel-gainers');
    const panelLosers = document.getElementById('panel-losers');
    const metricTabButtons = Array.from(document.querySelectorAll('[data-metric-tab]'));
    const metricTabPanels = {
        m1: document.getElementById('metric-panel-m1'),
        m2: document.getElementById('metric-panel-m2'),
        m3: document.getElementById('metric-panel-m3'),
        history: document.getElementById('metric-panel-history'),
    };

    function setTheme(darkMode) {
        document.body.classList.toggle('dark-mode', darkMode);
        localStorage.setItem(themeKey, darkMode ? 'dark' : 'light');
        if (btnTheme) {
            btnTheme.innerHTML = darkMode
                ? '<i data-lucide="sun" class="w-4 h-4"></i>'
                : '<i data-lucide="moon" class="w-4 h-4"></i>';
            lucide.createIcons();
        }
    }

    btnTheme?.addEventListener('click', () => {
        setTheme(!document.body.classList.contains('dark-mode'));
    });

    setTheme(localStorage.getItem(themeKey) === 'dark');

    function formatVolume(value) {
        const num = Number(value) || 0;
        if (num >= 1_000_000_000) return `${(num / 1_000_000_000).toFixed(2)}B`;
        if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(1)}M`;
        if (num >= 1_000) return `${(num / 1_000).toFixed(1)}K`;
        return num.toFixed(0);
    }

    function formatChange(value) {
        const num = Number(value) || 0;
        return `${num >= 0 ? '+' : ''}${num.toFixed(2)}%`;
    }

    function formatPrice(value) {
        const num = Number(value);
        if (!Number.isFinite(num) || num <= 0) return '--';
        if (num >= 1000) return num.toLocaleString(undefined, {maximumFractionDigits: 2});
        if (num >= 1) return num.toFixed(4);
        return num.toFixed(6);
    }

    function formatPercent(value) {
        const num = Number(value);
        if (!Number.isFinite(num)) return '--';
        return `${num.toFixed(2)}%`;
    }

    function formatChangeOrDash(value) {
        const num = Number(value);
        if (!Number.isFinite(num)) return '--';
        return formatChange(num);
    }

    function getDeltaMeta(delta) {
        if (typeof delta !== 'number') return {text: 'N/A', cls: 'rank-flat'};
        if (delta > 0) return {text: `↑ +${delta}`, cls: 'rank-up'};
        if (delta < 0) return {text: `↓ ${delta}`, cls: 'rank-down'};
        return {text: '→ 0', cls: 'rank-flat'};
    }

    function setMobileBoard(board) {
        const showGainers = board !== 'losers';
        if (panelGainers) panelGainers.classList.toggle('mobile-board-hidden', !showGainers);
        if (panelLosers) panelLosers.classList.toggle('mobile-board-hidden', showGainers);
        if (btnMobileGainers) btnMobileGainers.classList.toggle('active', showGainers);
        if (btnMobileLosers) btnMobileLosers.classList.toggle('active', !showGainers);
    }

    function setMetricTab(tabKey) {
        metricTabButtons.forEach((btn) => {
            btn.classList.toggle('active', btn.dataset.metricTab === tabKey);
        });

        Object.entries(metricTabPanels).forEach(([key, panel]) => {
            if (!panel) return;
            panel.classList.toggle('active', key === tabKey);
        });
    }

    function renderLosersReversal(stats) {
        if (!losersHitSummary || !losersHitSymbols) return;

        const hits = Number(stats?.hits || 0);
        const baseCount = Number(stats?.base_count || 0);
        const prevDate = stats?.prev_snapshot_date || '--';
        const symbols = Array.isArray(stats?.symbols) ? stats.symbols.filter(Boolean) : [];

        if (baseCount <= 0) {
            losersHitSummary.textContent = '命中 --/-- · 概率 --';
            losersHitSymbols.textContent = '命中 symbols: --（缺少前一日涨幅榜快照）';
            return;
        }

        losersHitSummary.textContent = `命中 ${hits}/${baseCount} · 概率 ${formatPercent(stats?.probability_pct)}`;
        losersHitSymbols.textContent = `对比 ${prevDate} 涨幅榜 · 命中 symbols: ${symbols.length ? symbols.join(', ') : '--'}`;
    }

    function renderMetric2(metric) {
        if (
            !metric2Summary || !metric2HitSymbols || !metric2SampleNote
            || !metric2ContinuationPool || !metric2Body || !metric2Cards
        ) return;

        metric2Body.innerHTML = '';
        metric2Cards.innerHTML = '';

        const baseDate = metric?.base_snapshot_date || '--';
        const sampleSize = Number(metric?.sample_size || 0);
        const evaluatedCount = Number(metric?.evaluated_count || 0);
        const hits = Number(metric?.hits || 0);
        const thresholdPct = Number(metric?.threshold_pct);
        const details = Array.isArray(metric?.details) ? metric.details : [];
        const hitSymbols = Array.isArray(metric?.hit_symbols) ? metric.hit_symbols : [];

        if (evaluatedCount <= 0) {
            metric2Summary.textContent = '命中 --/-- · 概率 --';
            metric2HitSymbols.textContent = '命中 symbols: --';
            metric2SampleNote.textContent = sampleSize > 0
                ? `基于 ${baseDate} 涨幅榜Top${sampleSize} · 当前快照缺少有效次日涨跌幅`
                : '样本信息: 缺少前一日涨幅榜快照';
            metric2ContinuationPool.textContent = '延续上涨池: --';
            return;
        }

        metric2Summary.textContent = `命中 ${hits}/${evaluatedCount} · 概率 ${formatPercent(metric?.probability_pct)}`;
        metric2HitSymbols.textContent = `命中 symbols: ${
            hitSymbols.length
                ? hitSymbols.map((item) => `${item.symbol}(${formatChangeOrDash(item.next_change_pct)})`).join(', ')
                : '--'
        }`;
        metric2SampleNote.textContent = `基于 ${baseDate} 涨幅榜Top${sampleSize} · 有效样本 ${evaluatedCount} · 阈值 ${formatChangeOrDash(thresholdPct)}`;
        const continuationRows = Array.isArray(metric?.continuation_pool?.rows)
            ? metric.continuation_pool.rows
            : [];
        const continuationPreview = continuationRows.slice(0, 8).map((item) => {
            const symbol = item?.symbol || '--';
            const pct = formatChangeOrDash(item?.next_change_pct);
            const rankTag = item?.today_gainer_rank ? `#${item.today_gainer_rank}` : '-';
            return `${symbol}(${pct},今日榜位${rankTag})`;
        });
        const stillUpCount = Number(metric?.continuation_pool?.still_up_count || continuationRows.length || 0);
        const stillInTopCount = Number(metric?.continuation_pool?.still_in_gainers_top_count || 0);
        metric2ContinuationPool.textContent = continuationRows.length > 0
            ? `延续上涨池: ${stillUpCount} 个（仍在今日涨幅榜 ${stillInTopCount} 个） · ${continuationPreview.join(', ')}`
            : '延续上涨池: 0 个';

        details.forEach((item) => {
            const hitBadge = item.is_hit
                ? '<span class="metric-hit-badge">YES</span>'
                : '<span class="metric-miss-badge">NO</span>';
            const nextChangeClass = Number(item.next_change_pct) <= thresholdPct ? 'rank-down' : 'rank-flat';

            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td class="px-4 py-3 font-mono">${item.prev_rank || '--'}</td>
                <td class="px-4 py-3 font-semibold">${item.symbol || '--'}</td>
                <td class="px-4 py-3 text-right font-mono">${formatChangeOrDash(item.prev_change_pct)}</td>
                <td class="px-4 py-3 text-right font-mono ${nextChangeClass}">${formatChangeOrDash(item.next_change_pct)}</td>
                <td class="px-4 py-3">${hitBadge}</td>
            `;
            metric2Body.appendChild(tr);

            const card = document.createElement('div');
            card.className = 'mobile-row';
            card.innerHTML = `
                <div class="flex justify-between items-center mb-2">
                    <span class="font-mono text-xs">昨排 #${item.prev_rank || '--'}</span>
                    ${hitBadge}
                </div>
                <div class="font-bold">${item.symbol || '--'}</div>
                <div class="mt-1 text-xs text-slate-500">昨日涨幅: ${formatChangeOrDash(item.prev_change_pct)}</div>
                <div class="mt-1 text-xs ${nextChangeClass}">次日涨跌: ${formatChangeOrDash(item.next_change_pct)}</div>
            `;
            metric2Cards.appendChild(card);
        });
    }

    function renderMetric3(metric) {
        if (
            !metric3Summary || !metric3SampleNote || !metric3DistLabel
            || !metric3DistNeg || !metric3DistMid || !metric3DistPos
            || !metric3Body || !metric3Cards
        ) {
            return;
        }

        metric3Body.innerHTML = '';
        metric3Cards.innerHTML = '';

        const baseDate = metric?.base_snapshot_date || '--';
        const sampleSize = Number(metric?.sample_size || 0);
        const evaluatedCount = Number(metric?.evaluated_count || 0);
        const details = Array.isArray(metric?.details) ? metric.details : [];

        if (evaluatedCount <= 0) {
            metric3Summary.textContent = '下跌占比 -- · <-10%占比 -- · >+10%占比 --';
            metric3SampleNote.textContent = sampleSize > 0
                ? `基于 ${baseDate} 涨幅榜Top${sampleSize} · 当前快照缺少48h有效样本`
                : '样本信息: 缺少两天前涨幅榜快照';
            metric3DistLabel.textContent = '--';
            metric3DistNeg.style.width = '0%';
            metric3DistMid.style.width = '0%';
            metric3DistPos.style.width = '0%';
            return;
        }

        metric3SampleNote.textContent = `基于 ${baseDate} 涨幅榜Top${sampleSize} · 有效样本 ${evaluatedCount}`;

        const changes = details
            .map((item) => Number(item.change_pct ?? item.return_pct))
            .filter((val) => Number.isFinite(val));
        const downCount = changes.filter((val) => val < 0).length;
        const negCount = changes.filter((val) => val < -10).length;
        const midCount = changes.filter((val) => val >= -10 && val <= 10).length;
        const posCount = changes.filter((val) => val > 10).length;
        const downPct = evaluatedCount > 0 ? (downCount * 100.0 / evaluatedCount) : 0;
        const negPct = evaluatedCount > 0 ? (negCount * 100.0 / evaluatedCount) : 0;
        const midPct = evaluatedCount > 0 ? (midCount * 100.0 / evaluatedCount) : 0;
        const posPct = evaluatedCount > 0 ? (posCount * 100.0 / evaluatedCount) : 0;

        metric3Summary.textContent = `下跌占比 ${formatPercent(downPct)} · <-10%占比 ${formatPercent(negPct)} · >+10%占比 ${formatPercent(posPct)}`;

        metric3DistNeg.style.width = `${negPct.toFixed(2)}%`;
        metric3DistMid.style.width = `${midPct.toFixed(2)}%`;
        metric3DistPos.style.width = `${posPct.toFixed(2)}%`;
        metric3DistLabel.textContent = `<-10% ${negCount} · -10~+10% ${midCount} · >+10% ${posCount}`;

        const sortedDetails = [...details].sort((a, b) => {
            const ar = Number(a?.change_pct ?? a?.return_pct);
            const br = Number(b?.change_pct ?? b?.return_pct);
            if (Number.isFinite(ar) && Number.isFinite(br)) return ar - br;
            if (Number.isFinite(ar)) return -1;
            if (Number.isFinite(br)) return 1;
            return Number(a?.prev_rank || 9999) - Number(b?.prev_rank || 9999);
        });

        sortedDetails.forEach((item) => {
            const chgNum = Number(item.change_pct ?? item.return_pct);
            const retClass = !Number.isFinite(chgNum)
                ? 'rank-flat'
                : (chgNum < 0 ? 'rank-down' : (chgNum > 0 ? 'rank-up' : 'rank-flat'));

            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td class="px-4 py-3 font-mono">${item.prev_rank || '--'}</td>
                <td class="px-4 py-3 font-semibold">${item.symbol || '--'}</td>
                <td class="px-4 py-3 text-right font-mono">${formatPrice(item.entry_price)}</td>
                <td class="px-4 py-3 text-right font-mono">${formatPrice(item.current_price)}</td>
                <td class="px-4 py-3 text-right font-mono ${retClass}">${formatChangeOrDash(item.change_pct ?? item.return_pct)}</td>
            `;
            metric3Body.appendChild(tr);

            const card = document.createElement('div');
            card.className = 'mobile-row';
            card.innerHTML = `
                <div class="flex justify-between items-center mb-2">
                    <span class="font-mono text-xs">昨排 #${item.prev_rank || '--'}</span>
                    <span class="${retClass} font-bold">${formatChangeOrDash(item.change_pct ?? item.return_pct)}</span>
                </div>
                <div class="font-bold">${item.symbol || '--'}</div>
                <div class="mt-1 text-xs text-slate-500">进场价: ${formatPrice(item.entry_price)}</div>
                <div class="mt-1 text-xs text-slate-500">48h价: ${formatPrice(item.current_price)}</div>
                <div class="mt-1 text-xs ${retClass}">48h涨跌幅: ${formatChangeOrDash(item.change_pct ?? item.return_pct)}</div>
            `;
            metric3Cards.appendChild(card);
        });
    }

    function renderMetricsHistory(rows) {
        if (!metricsHistoryBody || !metricsHistoryCards) return;

        metricsHistoryBody.innerHTML = '';
        metricsHistoryCards.innerHTML = '';

        if (!Array.isArray(rows) || rows.length === 0) {
            metricsHistoryBody.innerHTML = '<tr><td class="px-4 py-3 text-slate-500" colspan="6">暂无数据</td></tr>';
            return;
        }

        rows.forEach((item) => {
            const m1 = formatPercent(item.m1_prob_pct);
            const m2 = formatPercent(item.m2_prob_pct);
            const m3Neg = formatPercent(item.m3_lt_neg10_pct);
            const m3Pos = formatPercent(item.m3_gt_pos10_pct);
            const evalCount = Number(item.m3_eval_count || 0);

            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td class="px-4 py-3 font-mono">${item.snapshot_date || '--'}</td>
                <td class="px-4 py-3 text-right font-mono">${m1}</td>
                <td class="px-4 py-3 text-right font-mono">${m2}</td>
                <td class="px-4 py-3 text-right font-mono">${m3Neg}</td>
                <td class="px-4 py-3 text-right font-mono">${m3Pos}</td>
                <td class="px-4 py-3 text-right font-mono">${evalCount}</td>
            `;
            metricsHistoryBody.appendChild(tr);

            const card = document.createElement('div');
            card.className = 'mobile-row';
            card.innerHTML = `
                <div class="flex justify-between items-center mb-2">
                    <span class="font-mono text-xs">${item.snapshot_date || '--'}</span>
                    <span class="text-xs text-slate-500">样本 ${evalCount}</span>
                </div>
                <div class="mt-1 text-xs text-slate-500">M1 进跌幅榜: ${m1}</div>
                <div class="mt-1 text-xs text-slate-500">M2 次日&lt;-10%: ${m2}</div>
                <div class="mt-1 text-xs text-slate-500">M3 &lt;-10%: ${m3Neg}</div>
                <div class="mt-1 text-xs text-slate-500">M3 &gt;+10%: ${m3Pos}</div>
            `;
            metricsHistoryCards.appendChild(card);
        });
    }

    async function loadMetricsHistory() {
        try {
            const response = await fetch('/api/leaderboard/metrics-history?limit=30');
            const data = await response.json();
            renderMetricsHistory(Array.isArray(data.rows) ? data.rows : []);
        } catch (err) {
            renderMetricsHistory([]);
        }
    }

    function renderRows(rows) {
        tableBody.innerHTML = '';
        cards.innerHTML = '';

        rows.forEach((row, idx) => {
            const held = row.is_held ? '<span class="held-badge">已持仓</span>' : '';

            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td class="px-4 py-3 font-mono">${idx + 1}</td>
                <td class="px-4 py-3 font-semibold">${row.symbol} ${held}</td>
                <td class="px-4 py-3 text-right font-mono">${formatPrice(row.last_price ?? row.price)}</td>
                <td class="px-4 py-3 text-right rank-up font-bold">${formatChange(row.change)}</td>
                <td class="px-4 py-3 text-right font-mono">${formatVolume(row.volume)}</td>
            `;
            tableBody.appendChild(tr);

            const card = document.createElement('div');
            card.className = 'mobile-row';
            card.innerHTML = `
                <div class="flex justify-between items-center mb-2">
                    <span class="font-mono text-xs">#${idx + 1}</span>
                    <span class="rank-up font-bold">${formatChange(row.change)}</span>
                </div>
                <div class="font-bold">${row.symbol} ${held}</div>
                <div class="mobile-core">
                    <div>
                        <div class="k">Price</div>
                        <div class="v">${formatPrice(row.last_price ?? row.price)}</div>
                    </div>
                    <div>
                        <div class="k">Change</div>
                        <div class="v rank-up">${formatChange(row.change)}</div>
                    </div>
                    <div>
                        <div class="k">24h Vol</div>
                        <div class="v">${formatVolume(row.volume)}</div>
                    </div>
                </div>
            `;
            cards.appendChild(card);
        });
    }

    function renderLosersRows(rows) {
        losersTableBody.innerHTML = '';
        losersCards.innerHTML = '';

        rows.forEach((row, idx) => {
            const held = row.is_held ? '<span class="held-badge">已持仓</span>' : '';
            const reversal = row.was_prev_gainer_top
                ? `<span class="reversal-badge">昨涨#${row.prev_gainer_rank}</span>`
                : '<span class="text-xs text-slate-400">--</span>';
            const reversalText = row.was_prev_gainer_top
                ? `是 (昨涨#${row.prev_gainer_rank})`
                : '否';

            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td class="px-4 py-3 font-mono">${idx + 1}</td>
                <td class="px-4 py-3 font-semibold">${row.symbol} ${held}</td>
                <td class="px-4 py-3 text-right font-mono">${formatPrice(row.last_price ?? row.price)}</td>
                <td class="px-4 py-3 text-right rank-down font-bold">${formatChange(row.change)}</td>
                <td class="px-4 py-3 text-right font-mono">${formatVolume(row.volume)}</td>
                <td class="px-4 py-3">${reversal}</td>
            `;
            losersTableBody.appendChild(tr);

            const card = document.createElement('div');
            card.className = 'mobile-row';
            card.innerHTML = `
                <div class="flex justify-between items-center mb-2">
                    <span class="font-mono text-xs">#${idx + 1}</span>
                    <span class="rank-down font-bold">${formatChange(row.change)}</span>
                </div>
                <div class="font-bold">${row.symbol} ${held}</div>
                <div class="mobile-core">
                    <div>
                        <div class="k">Price</div>
                        <div class="v">${formatPrice(row.last_price ?? row.price)}</div>
                    </div>
                    <div>
                        <div class="k">Change</div>
                        <div class="v rank-down">${formatChange(row.change)}</div>
                    </div>
                    <div>
                        <div class="k">24h Vol</div>
                        <div class="v">${formatVolume(row.volume)}</div>
                    </div>
                </div>
                <details class="mobile-more">
                    <summary>展开详情</summary>
                    <div class="mt-1 text-xs text-slate-500">昨涨幅榜Top10: ${reversalText}</div>
                </details>
            `;
            losersCards.appendChild(card);
        });
    }

    function renderReboundRows(windowDays, rows) {
        const panel = reboundPanels[windowDays];
        if (!panel?.body || !panel?.cards) return;

        const reboundBody = panel.body;
        const reboundCards = panel.cards;
        const lowField = `low_${windowDays}d`;
        const lowAtField = `low_${windowDays}d_at_utc`;
        const reboundField = `rebound_${windowDays}d_pct`;

        reboundBody.innerHTML = '';
        reboundCards.innerHTML = '';

        rows.forEach((row, idx) => {
            const reboundPct = Number(row[reboundField]);
            const reboundClass = Number.isFinite(reboundPct)
                ? (reboundPct >= 0 ? 'rank-up' : 'rank-down')
                : 'rank-flat';
            const held = row.is_held ? '<span class="held-badge">已持仓</span>' : '';

            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td class="px-4 py-3 font-mono">${row.rank || idx + 1}</td>
                <td class="px-4 py-3 font-semibold">${row.symbol || '--'} ${held}</td>
                <td class="px-4 py-3 text-right font-mono">${formatPrice(row[lowField])}</td>
                <td class="px-4 py-3 text-right font-mono">${formatPrice(row.current_price)}</td>
                <td class="px-4 py-3 text-right ${reboundClass} font-bold">${formatChangeOrDash(row[reboundField])}</td>
                <td class="px-4 py-3 text-right font-mono">${row[lowAtField] || '--'}</td>
            `;
            reboundBody.appendChild(tr);

            const card = document.createElement('div');
            card.className = 'mobile-row';
            card.innerHTML = `
                <div class="flex justify-between items-center mb-2">
                    <span class="font-mono text-xs">#${row.rank || idx + 1}</span>
                    <span class="${reboundClass} font-bold">${formatChangeOrDash(row[reboundField])}</span>
                </div>
                <div class="font-bold">${row.symbol || '--'} ${held}</div>
                <div class="mobile-core">
                    <div>
                        <div class="k">${windowDays}D Low</div>
                        <div class="v">${formatPrice(row[lowField])}</div>
                    </div>
                    <div>
                        <div class="k">Current</div>
                        <div class="v">${formatPrice(row.current_price)}</div>
                    </div>
                    <div>
                        <div class="k">Rebound</div>
                        <div class="v ${reboundClass}">${formatChangeOrDash(row[reboundField])}</div>
                    </div>
                </div>
                <details class="mobile-more">
                    <summary>展开详情</summary>
                    <div class="mt-2 text-xs text-slate-500">低点日期(UTC): ${row[lowAtField] || '--'}</div>
                </details>
            `;
            reboundCards.appendChild(card);
        });
    }

    async function loadReboundSnapshot(windowDays, selectedDate) {
        const panel = reboundPanels[windowDays];
        if (!panel?.summary || !panel?.window || !panel?.body || !panel?.cards) return;

        const reboundSummary = panel.summary;
        const reboundWindow = panel.window;
        const reboundBody = panel.body;
        const reboundCards = panel.cards;

        const windowEndpoint = Number(windowDays) === 14 ? '7d' : `${windowDays}d`;
        const endpoint = selectedDate
            ? `/api/rebound-${windowEndpoint}?date=${encodeURIComponent(selectedDate)}`
            : `/api/rebound-${windowEndpoint}`;

        try {
            const response = await fetch(endpoint);
            const data = await response.json();
            if (!data.ok) {
                reboundSummary.textContent = 'Top -- · --';
                reboundWindow.textContent = data.message || `暂无${windowDays}D反弹快照`;
                reboundBody.innerHTML = '';
                reboundCards.innerHTML = '';
                return;
            }

            const topCountRaw = data.top_count ?? data.top ?? (Array.isArray(data.rows) ? data.rows.length : 0);
            const topCount = Number(topCountRaw || 0);
            reboundSummary.textContent = `Top ${topCount} · ${data.snapshot_date || '--'} ${(data.snapshot_time || '').split(' ').slice(1).join(' ') || '--'}`;
            reboundWindow.textContent = `Window ${data.window_start_utc || '--'} UTC → snapshot`;
            renderReboundRows(windowDays, Array.isArray(data.rows) ? data.rows : []);
        } catch (err) {
            reboundSummary.textContent = 'Top -- · --';
            reboundWindow.textContent = `加载失败: ${err.message || err}`;
            reboundBody.innerHTML = '';
            reboundCards.innerHTML = '';
        }
    }

    async function loadSnapshotDates() {
        try {
            const response = await fetch('/api/leaderboard/dates?limit=120');
            const payload = await response.json();
            const dates = Array.isArray(payload.dates) ? payload.dates : [];
            snapshotDateSelect.innerHTML = '<option value="">LATEST</option>';
            dates.forEach((d) => {
                const opt = document.createElement('option');
                opt.value = d;
                opt.textContent = d;
                snapshotDateSelect.appendChild(opt);
            });
        } catch (err) {
            statusText.textContent = `加载日期列表失败: ${err.message || err}`;
        }
    }

    async function loadLeaderboard() {
        btnRefresh.disabled = true;
        statusText.textContent = '正在读取快照...';
        const selectedDate = snapshotDateSelect.value;
        try {
            const endpoint = selectedDate
                ? `/api/leaderboard?date=${encodeURIComponent(selectedDate)}`
                : '/api/leaderboard';
            const response = await fetch(endpoint);
            const data = await response.json();

            if (!data.ok) {
                statusText.textContent = `加载失败: ${data.message || data.reason || 'unknown'}`;
                metricTop.textContent = '--';
                metricDate.textContent = '--';
                metricTime.textContent = '--';
                if (snapshotContextMain) snapshotContextMain.textContent = 'Snapshot: --';
                if (snapshotContextSub) snapshotContextSub.textContent = 'Top -- · Window --';
                if (gainersTitle) gainersTitle.textContent = 'GAINERS TOP N';
                if (losersTitle) losersTitle.textContent = 'LOSERS TOP N';
                tableBody.innerHTML = '';
                cards.innerHTML = '';
                losersTableBody.innerHTML = '';
                losersCards.innerHTML = '';
                renderLosersReversal(null);
                renderMetric2(null);
                renderMetric3(null);
                renderMetricsHistory([]);
                await Promise.all([14, 30, 60].map((d) => loadReboundSnapshot(d, selectedDate)));
                return;
            }

            metricTop.textContent = data.top;
            metricDate.textContent = data.snapshot_date;
            metricTime.textContent = (data.snapshot_time || '').split(' ').slice(1).join(' ') || '--';
            const gainersTopCount = Number(data.gainers_top_count ?? data.top ?? 0);
            const losersTopCount = Number(data.losers_top_count ?? (data.losers_rows || []).length ?? 0);
            if (gainersTitle) gainersTitle.textContent = `GAINERS TOP ${gainersTopCount || 'N'}`;
            if (losersTitle) losersTitle.textContent = `LOSERS TOP ${losersTopCount || 'N'}`;
            statusText.textContent = `快照日期: ${data.snapshot_date} · 计算区间: ${data.window_start_utc} UTC 至快照时刻`;
            lastUpdated.textContent = data.snapshot_time;
            if (snapshotContextMain) {
                snapshotContextMain.textContent = `Snapshot: ${data.snapshot_date} ${(data.snapshot_time || '').split(' ').slice(1).join(' ') || '--'} (UTC+8)`;
            }
            if (snapshotContextSub) {
                snapshotContextSub.textContent = `Top ${gainersTopCount || '--'} · Window ${data.window_start_utc || '--'} UTC → snapshot`;
            }

            const rows = data.rows || [];
            const losersRows = data.losers_rows || [];
            renderRows(rows);
            renderLosersRows(losersRows);
            renderLosersReversal(data.losers_reversal || null);
            const metric2Data = data.next_day_drop_metric || null;
            if (metric2Data && !metric2Data.continuation_pool) {
                metric2Data.continuation_pool = data.continuation_pool || null;
            }
            renderMetric2(metric2Data);
            renderMetric3(data.change_48h_metric || data.short_48h_metric || data.hold_48h_metric || null);
            await loadMetricsHistory();
            await Promise.all([14, 30, 60].map((d) => loadReboundSnapshot(d, selectedDate)));
        } catch (err) {
            statusText.textContent = `请求异常: ${err.message || err}`;
            renderLosersReversal(null);
            renderMetric2(null);
            renderMetric3(null);
            renderMetricsHistory([]);
            await Promise.all([14, 30, 60].map((d) => loadReboundSnapshot(d, selectedDate)));
        } finally {
            btnRefresh.disabled = false;
            lucide.createIcons();
        }
    }

    btnRefresh?.addEventListener('click', loadLeaderboard);
    snapshotDateSelect?.addEventListener('change', loadLeaderboard);
    btnMobileGainers?.addEventListener('click', () => setMobileBoard('gainers'));
    btnMobileLosers?.addEventListener('click', () => setMobileBoard('losers'));
    metricTabButtons.forEach((btn) => {
        btn.addEventListener('click', () => setMetricTab(btn.dataset.metricTab || 'm2'));
    });

    setMobileBoard('gainers');
    setMetricTab('m2');

    (async () => {
        await loadSnapshotDates();
        await loadLeaderboard();
    })();
