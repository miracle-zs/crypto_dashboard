(function () {
    const els = {
        asOf: document.getElementById('crash-risk-as-of'),
        source: document.getElementById('crash-risk-source'),
        window: document.getElementById('crash-risk-window'),
        refreshBtn: document.getElementById('refresh-btn'),
        refreshState: document.getElementById('crash-risk-refresh-state'),
        summaryTotal: document.getElementById('summary-total'),
        summaryHigh: document.getElementById('summary-high'),
        summaryWarning: document.getElementById('summary-warning'),
        summaryWatch: document.getElementById('summary-watch'),
        tableBody: document.getElementById('crash-risk-body'),
        tableShell: document.querySelector('.table-shell'),
        emptyState: document.getElementById('crash-risk-empty'),
        emptyStateTitle: document.getElementById('crash-risk-empty-title'),
        emptyStateBody: document.getElementById('crash-risk-empty-body'),
        detailRank: document.getElementById('crash-risk-detail-rank'),
        detailTitle: document.getElementById('crash-risk-detail-title'),
        detailHint: document.getElementById('crash-risk-detail-hint'),
        detailStage: document.getElementById('crash-risk-detail-stage'),
        detailScore: document.getElementById('crash-risk-detail-score'),
        detailDriversCount: document.getElementById('crash-risk-detail-drivers-count'),
        detailSourcePill: document.getElementById('crash-risk-detail-source-pill'),
        detailDrivers: document.getElementById('crash-risk-detail-drivers'),
        detailComponents: document.getElementById('crash-risk-detail-components'),
        detailSource: document.getElementById('crash-risk-detail-source'),
        detailAsOf: document.getElementById('crash-risk-detail-asof'),
    };

    const componentLabels = {
        oi_divergence: 'OI Divergence',
        price_extension: 'Price Extension',
        momentum_fade: 'Momentum Fade',
        support_fragility: 'Support Fragility',
        trigger_confirmation: 'Trigger Confirm',
    };

    const state = {
        payload: null,
        selectedSymbol: null,
        loading: false,
    };
    const emptyMessages = {
        empty: {
            title: '暂无可展示的 crash-risk 结果',
            body: '请点击“手动刷新”重新读取最新 leaderboard 快照。',
        },
        error: {
            title: '无法加载 crash-risk 数据',
            body: '请稍后重试或检查 /api/crash-risk 是否可用。',
        },
    };

    function clamp(num, low, high) {
        return Math.max(low, Math.min(high, num));
    }

    function formatInteger(value) {
        const num = Number(value);
        if (!Number.isFinite(num)) return '--';
        return new Intl.NumberFormat(undefined, {maximumFractionDigits: 0}).format(Math.round(num));
    }

    function formatScore(value) {
        const num = Number(value);
        if (!Number.isFinite(num)) return '--';
        return `${Math.round(num)}`;
    }

    function formatStage(stage) {
        return stage || '观察';
    }

    function formatText(value, fallback = '--') {
        if (value === null || value === undefined || value === '') return fallback;
        return String(value);
    }

    function escapeHtml(value) {
        return formatText(value)
            .replaceAll('&', '&amp;')
            .replaceAll('<', '&lt;')
            .replaceAll('>', '&gt;')
            .replaceAll('"', '&quot;')
            .replaceAll("'", '&#39;');
    }

    function stageClass(stage) {
        if (stage === '高危') return 'high';
        if (stage === '警惕') return 'warn';
        return 'watch';
    }

    function scoreClass(score) {
        const num = Number(score);
        if (!Number.isFinite(num)) return 'watch';
        if (num >= 70) return 'high';
        if (num >= 40) return 'warn';
        return 'watch';
    }

    function setRefreshState(text, tone = 'watch') {
        if (!els.refreshState) return;
        els.refreshState.innerHTML = `<span class="status-dot"></span><span>${text}</span>`;
        els.refreshState.className = `status-pill ${tone}`;
    }

    function setLoading(loading) {
        state.loading = loading;
        if (els.refreshBtn) {
            els.refreshBtn.disabled = loading;
            els.refreshBtn.textContent = loading ? '刷新中...' : '手动刷新';
        }
    }

    function renderSnapshotSummary(payload) {
        const source = payload?.source_snapshot || null;
        if (els.asOf) {
            els.asOf.textContent = `As of: ${formatText(payload?.as_of)}`;
        }
        if (els.source) {
            els.source.textContent = source
                ? `Source: ${formatText(source.source)} · snapshot ${formatText(source.snapshot_date)}`
                : 'Source: --';
        }
        if (els.window) {
            els.window.textContent = source && source.window_start_utc
                ? `Window start UTC: ${source.window_start_utc}`
                : 'Window: --';
        }
    }

    function renderSummary(payload) {
        const summary = payload?.summary || {};
        if (els.summaryTotal) els.summaryTotal.textContent = formatInteger(summary.total);
        if (els.summaryHigh) els.summaryHigh.textContent = formatInteger(summary.high_risk);
        if (els.summaryWarning) els.summaryWarning.textContent = formatInteger(summary.warning);
        if (els.summaryWatch) els.summaryWatch.textContent = formatInteger(summary.watch);
    }

    function clearNode(node) {
        if (!node) return;
        while (node.firstChild) node.removeChild(node.firstChild);
    }

    function renderDetail(row, rank, payload) {
        clearNode(els.detailDrivers);
        clearNode(els.detailComponents);
        clearNode(els.detailSource);

        if (!row) {
            if (els.detailRank) els.detailRank.textContent = 'Rank --';
            if (els.detailTitle) els.detailTitle.textContent = '点击左侧任意标的';
            if (els.detailHint) {
                els.detailHint.textContent = '这里会显示风险分数、驱动因子、分项评分和来源快照。';
            }
            if (els.detailStage) {
                els.detailStage.textContent = '观察';
                els.detailStage.className = 'stage-pill watch';
            }
            if (els.detailScore) els.detailScore.textContent = '--';
            if (els.detailDriversCount) els.detailDriversCount.textContent = '0';
            if (els.detailSourcePill) els.detailSourcePill.textContent = '--';
            if (els.detailAsOf) els.detailAsOf.textContent = '';
            return;
        }

        const drivers = Array.isArray(row.drivers) ? row.drivers : [];
        const componentScores = row.component_scores && typeof row.component_scores === 'object'
            ? row.component_scores
            : {};
        const source = payload?.source_snapshot || null;

        if (els.detailRank) els.detailRank.textContent = `Rank ${rank}`;
        if (els.detailTitle) els.detailTitle.textContent = row.symbol || '--';
        if (els.detailHint) {
            els.detailHint.textContent = `候选池内的 ${row.symbol || '--'} 当前处于 ${formatStage(row.stage)}，用于快速定位结构开始恶化的强势币。`;
        }
        if (els.detailStage) {
            els.detailStage.textContent = formatStage(row.stage);
            els.detailStage.className = `stage-pill ${stageClass(row.stage)}`;
        }
        if (els.detailScore) els.detailScore.textContent = formatScore(row.risk_score);
        if (els.detailDriversCount) els.detailDriversCount.textContent = `${drivers.length}`;
        if (els.detailSourcePill) {
            els.detailSourcePill.textContent = source ? formatText(source.source) : '--';
        }
        if (els.detailAsOf) {
            const lines = [];
            if (source?.snapshot_time) lines.push(`snapshot_time: ${source.snapshot_time}`);
            if (source?.snapshot_date) lines.push(`snapshot_date: ${source.snapshot_date}`);
            if (source?.window_start_utc) lines.push(`window_start_utc: ${source.window_start_utc}`);
            els.detailAsOf.textContent = lines.length ? lines.join(' · ') : '';
        }

        if (drivers.length === 0) {
            const li = document.createElement('li');
            li.textContent = '暂无显著驱动，结构仍偏观察。';
            els.detailDrivers.appendChild(li);
        } else {
            drivers.forEach((driver) => {
                const li = document.createElement('li');
                li.textContent = driver;
                els.detailDrivers.appendChild(li);
            });
        }

        const componentOrder = [
            'oi_divergence',
            'price_extension',
            'momentum_fade',
            'support_fragility',
            'trigger_confirmation',
        ];
        componentOrder.forEach((key) => {
            const value = Number(componentScores[key] || 0);
            const chip = document.createElement('span');
            chip.className = 'component-chip';
            chip.textContent = `${componentLabels[key] || key}: ${formatScore(value)}`;
            els.detailComponents.appendChild(chip);
        });

        if (source) {
            const sourceRows = [
                ['source', source.source],
                ['snapshot_date', source.snapshot_date],
                ['snapshot_time', source.snapshot_time],
                ['window_start_utc', source.window_start_utc],
            ];
            sourceRows.forEach(([label, value]) => {
                const li = document.createElement('li');
                li.textContent = `${label}: ${formatText(value)}`;
                els.detailSource.appendChild(li);
            });
        } else {
            const li = document.createElement('li');
            li.textContent = 'source_snapshot 未返回。';
            els.detailSource.appendChild(li);
        }
    }

    function rowRankClass(row) {
        return scoreClass(row?.risk_score);
    }

    function renderRows(payload) {
        const rows = Array.isArray(payload?.rows) ? payload.rows : [];
        clearNode(els.tableBody);

        if (els.tableShell) {
            els.tableShell.classList.toggle('hidden', rows.length === 0);
        }
        if (els.emptyState) {
            els.emptyState.classList.toggle('hidden', rows.length !== 0);
        }

        rows.forEach((row, index) => {
            const tr = document.createElement('tr');
            tr.dataset.symbol = row.symbol || '';
            tr.tabIndex = 0;
            const orderedComponents = [
                'oi_divergence',
                'price_extension',
                'momentum_fade',
                'support_fragility',
                'trigger_confirmation',
            ];
            tr.innerHTML = `
                <td class="font-mono">${index + 1}</td>
                <td class="symbol-cell">${escapeHtml(row.symbol)}</td>
                <td><span class="score-pill ${rowRankClass(row)}">${formatScore(row.risk_score)}</span></td>
                <td><span class="stage-pill ${stageClass(row.stage)}">${formatStage(row.stage)}</span></td>
                <td>
                    <div class="driver-list">
                        ${(Array.isArray(row.drivers) && row.drivers.length
                            ? row.drivers.map((driver) => `<span class="driver-chip">${escapeHtml(driver)}</span>`).join('')
                            : '<span class="driver-chip">No major driver</span>')}
                    </div>
                </td>
                <td>
                    <div class="flex flex-wrap gap-2">
                        ${orderedComponents
                            .filter((key) => Object.prototype.hasOwnProperty.call(row.component_scores || {}, key))
                            .map((key) => {
                                const value = row.component_scores[key];
                                const label = componentLabels[key] || key;
                                return `<span class="component-chip">${label}: ${formatScore(value)}</span>`;
                            })
                            .join('')}
                    </div>
                </td>
            `;

            tr.addEventListener('click', () => {
                state.selectedSymbol = row.symbol || null;
                highlightSelectedRow();
                renderDetail(row, index + 1, payload);
            });

            tr.addEventListener('keydown', (event) => {
                if (event.key === 'Enter' || event.key === ' ') {
                    event.preventDefault();
                    tr.click();
                }
            });

            if (row.symbol && row.symbol === state.selectedSymbol) {
                tr.classList.add('selected');
            }

            els.tableBody.appendChild(tr);
        });

        let selectedRow = rows.find((item) => item.symbol === state.selectedSymbol) || null;
        if (!selectedRow && rows.length > 0) {
            selectedRow = rows[0];
        }
        state.selectedSymbol = selectedRow?.symbol || null;
        highlightSelectedRow();
        const selectedRank = selectedRow ? rows.indexOf(selectedRow) + 1 : 0;
        renderDetail(selectedRow, selectedRank, payload);
    }

    function highlightSelectedRow() {
        if (!els.tableBody) return;
        Array.from(els.tableBody.querySelectorAll('tr')).forEach((tr) => {
            tr.classList.toggle('selected', tr.dataset.symbol === state.selectedSymbol);
        });
    }

    function setEmptyState(kind = 'empty') {
        const message = emptyMessages[kind] || emptyMessages.empty;
        if (els.emptyStateTitle) els.emptyStateTitle.textContent = message.title;
        if (els.emptyStateBody) els.emptyStateBody.textContent = message.body;
    }

    async function fetchCrashRisk(url, options = {}) {
        const response = await fetch(url, {
            credentials: 'same-origin',
            method: options.method || 'GET',
        });
        if (!response.ok) {
            throw new Error(`Request failed: ${response.status}`);
        }
        return response.json();
    }

    async function loadCrashRisk(url = '/api/crash-risk', options = {}) {
        setLoading(true);
        setRefreshState('Loading', 'warn');
        try {
            const payload = await fetchCrashRisk(url, options);
            state.payload = payload;
            renderSnapshotSummary(payload);
            renderSummary(payload);
            renderRows(payload);
            setRefreshState('Ready', 'watch');
            return payload;
        } catch (error) {
            console.error('Failed to load crash risk data', error);
            setRefreshState('Load failed', 'high');
            if (els.emptyState) {
                els.emptyState.classList.remove('hidden');
                setEmptyState('error');
            }
            clearNode(els.tableBody);
            renderDetail(null, 0, null);
            throw error;
        } finally {
            setLoading(false);
        }
    }

    async function refreshCrashRisk() {
        try {
            setRefreshState('Refreshing', 'warn');
            const payload = await loadCrashRisk('/api/crash-risk/refresh', {method: 'POST'});
            setRefreshState('Updated', 'watch');
            return payload;
        } catch (error) {
            setRefreshState('Refresh failed', 'high');
            throw error;
        }
    }

    if (els.refreshBtn) {
        els.refreshBtn.addEventListener('click', () => {
            refreshCrashRisk().catch(() => {});
        });
    }

    setEmptyState('empty');
    loadCrashRisk().catch(() => {});
})();
