/**
 * myit — SPA 프론트엔드
 * "내 질문이 내 지식이 되고, 최신 정보가 내 관점을 업데이트한다"
 */

const API_BASE = '/api';
let authToken = localStorage.getItem('myit_token');
let currentUser = null;

// ============================================================
// API 호출 헬퍼
// ============================================================

async function api(endpoint, method = 'GET', body = null) {
    const headers = { 'Content-Type': 'application/json' };
    if (authToken) headers['Authorization'] = `Bearer ${authToken}`;

    const opts = { method, headers };
    if (body) opts.body = JSON.stringify(body);

    const res = await fetch(`${API_BASE}${endpoint}`, opts);
    if (res.status === 401) {
        logout();
        throw new Error('인증이 필요합니다');
    }
    if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail || '요청 실패');
    }
    return res.json();
}

async function apiUpload(endpoint, file) {
    const headers = {};
    if (authToken) headers['Authorization'] = `Bearer ${authToken}`;

    const form = new FormData();
    form.append('file', file);

    const res = await fetch(`${API_BASE}${endpoint}`, {
        method: 'POST', headers, body: form,
    });
    if (!res.ok) throw new Error('업로드 실패');
    return res.json();
}

// ============================================================
// 라우터
// ============================================================

function navigateTo(page) {
    // 페이지 전환
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
    const target = document.getElementById(`page-${page}`);
    if (target) target.classList.add('active');

    // 네비 하이라이트
    document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
    const link = document.querySelector(`[data-page="${page}"]`);
    if (link) link.classList.add('active');

    // 페이지별 데이터 로드
    if (page === 'home') loadHomeFeed();
    if (page === 'cards') loadCards();
    if (page === 'events') loadEvents();
    if (page === 'trades') loadTrades();
}

// ============================================================
// 인증
// ============================================================

function setupAuth() {
    // 로그인
    document.getElementById('login-form').addEventListener('submit', async (e) => {
        e.preventDefault();
        try {
            const data = await api('/auth/login', 'POST', {
                email: document.getElementById('login-email').value,
                password: document.getElementById('login-password').value,
            });
            authToken = data.access_token;
            currentUser = data.user;
            localStorage.setItem('myit_token', authToken);
            showApp();
            toast('로그인 성공! 환영합니다 👋', 'success');
        } catch (err) {
            toast(err.message, 'error');
        }
    });

    // 회원가입
    document.getElementById('register-form').addEventListener('submit', async (e) => {
        e.preventDefault();
        try {
            const data = await api('/auth/register', 'POST', {
                email: document.getElementById('reg-email').value,
                password: document.getElementById('reg-password').value,
                nickname: document.getElementById('reg-nickname').value || null,
            });
            authToken = data.access_token;
            currentUser = data.user;
            localStorage.setItem('myit_token', authToken);
            showApp();
            toast('가입 완료! myit에 오신 것을 환영합니다 🎉', 'success');
        } catch (err) {
            toast(err.message, 'error');
        }
    });
}

function showApp() {
    document.getElementById('sidebar').style.display = 'flex';
    navigateTo('home');
}

function logout() {
    authToken = null;
    currentUser = null;
    localStorage.removeItem('myit_token');
    document.getElementById('sidebar').style.display = 'none';
    navigateTo('login');
}

// ============================================================
// 홈 피드
// ============================================================

async function loadHomeFeed() {
    try {
        const feed = await api('/home/feed');
        renderHomeFeed(feed);
    } catch {
        // 데이터 없을 시 기본 상태 유지
    }
}

function renderHomeFeed(feed) {
    // My Pulse
    const pulseEl = document.getElementById('my-pulse');
    if (feed.my_pulse && feed.my_pulse.length > 0) {
        pulseEl.innerHTML = feed.my_pulse.map(item => `
            <div class="pulse-item glass-card">
                <div class="pulse-ticker">${item.ticker}</div>
                <div class="pulse-name">${item.ticker_name || ''}</div>
                ${item.latest_event ? `
                    <div class="pulse-event">${item.latest_event.title}</div>
                ` : '<div class="pulse-event" style="color:var(--text-muted)">이벤트 없음</div>'}
            </div>
        `).join('');
    } else {
        pulseEl.innerHTML = '<p class="empty-state">워치리스트에 종목을 추가하세요</p>';
    }

    // Since Your Last Visit
    const sinceEl = document.getElementById('since-last-visit');
    if (feed.since_last_visit && feed.since_last_visit.length > 0) {
        sinceEl.innerHTML = feed.since_last_visit.map(u => `
            <div class="update-item glass-card">
                카드 #${u.card_id}에 새 이벤트 연결됨
                ${u.impact ? `— 영향: ${u.impact}` : ''}
            </div>
        `).join('');
    }

    // Today's 1 Insight
    const insightEl = document.getElementById('todays-insight');
    if (feed.todays_insight) {
        const ti = feed.todays_insight;
        insightEl.innerHTML = `
            <div class="spotlight-question">💬 ${ti.original_question}</div>
            <div class="spotlight-title">${ti.title}</div>
            <div class="spotlight-summary">${ti.summary || ''}</div>
        `;
    }

    // Greeting
    const hour = new Date().getHours();
    const greet = hour < 12 ? '좋은 아침이에요 ☀️' : hour < 18 ? '좋은 오후예요 ☁️' : '좋은 저녁이에요 🌙';
    document.getElementById('home-greeting').textContent = greet;
}

// ============================================================
// 질문하기 → 인사이트 카드 생성
// ============================================================

function setupAsk() {
    document.getElementById('ask-submit').addEventListener('click', async () => {
        const question = document.getElementById('question-input').value.trim();
        if (!question) return toast('질문을 입력하세요', 'error');

        const tickerStr = document.getElementById('ask-tickers').value.trim();
        const tickers = tickerStr ? tickerStr.split(',').map(t => t.trim()) : [];

        const btn = document.getElementById('ask-submit');
        btn.textContent = '⏳ 생성 중...';
        btn.disabled = true;

        try {
            const card = await api('/insights/from-qa', 'POST', { question, tickers });
            renderNewCard(card);
            toast('인사이트 카드가 생성되었습니다! 📋', 'success');
        } catch (err) {
            toast(err.message, 'error');
        } finally {
            btn.textContent = '✨ 인사이트 카드 생성';
            btn.disabled = false;
        }
    });
}

function renderNewCard(card) {
    const resultEl = document.getElementById('ask-result');
    resultEl.style.display = 'block';
    resultEl.innerHTML = `
        <div class="insight-card glass-card">
            <div class="card-title">${card.title}</div>
            <div class="card-question">💬 ${card.original_question}</div>
            <div class="card-summary">${card.summary || ''}</div>
            ${card.tags && card.tags.length ? `
                <div class="card-tags">
                    ${card.tags.map(t =>
                        `<span class="tag tag-${t.tag_type}">${t.tag_value}</span>`
                    ).join('')}
                </div>
            ` : ''}
            <div class="card-meta">
                <span class="card-status status-${card.status}">${card.status}</span>
                <span>${new Date(card.created_at).toLocaleDateString('ko-KR')}</span>
            </div>
        </div>
    `;
}

// ============================================================
// 인사이트 카드 목록
// ============================================================

async function loadCards() {
    try {
        const status = document.getElementById('cards-filter-status').value;
        const params = status ? `?status=${status}` : '';
        const data = await api(`/insights/${params}`);
        renderCardsList(data);
    } catch {
        // 에러 시 기본 상태
    }
}

function renderCardsList(data) {
    const listEl = document.getElementById('cards-list');
    if (!data.cards || data.cards.length === 0) {
        listEl.innerHTML = '<p class="empty-state">아직 인사이트 카드가 없습니다.<br>질문을 하면 자동으로 카드가 생성됩니다.</p>';
        return;
    }

    listEl.innerHTML = data.cards.map(card => `
        <div class="insight-card glass-card" onclick="viewCard(${card.id})">
            <div class="card-title">${card.title}</div>
            <div class="card-question">💬 ${card.original_question}</div>
            <div class="card-summary">${card.summary || ''}</div>
            ${card.tags && card.tags.length ? `
                <div class="card-tags">
                    ${card.tags.map(t =>
                        `<span class="tag tag-${t.tag_type}">${t.tag_value}</span>`
                    ).join('')}
                </div>
            ` : ''}
            <div class="card-meta">
                <span class="card-status status-${card.status}">${card.status}</span>
                <span>v${card.current_version} · ${new Date(card.updated_at).toLocaleDateString('ko-KR')}</span>
            </div>
        </div>
    `).join('');
}

async function viewCard(cardId) {
    try {
        const card = await api(`/insights/${cardId}`);
        // 간단한 모달 대체 — 카드 상세를 alert로 표시
        const detail = [
            `📋 ${card.title}`,
            `\n💬 질문: ${card.original_question}`,
            `\n📝 요약:\n${card.summary || '(없음)'}`,
            card.hypothesis ? `\n💡 내 가설:\n${card.hypothesis}` : '',
            card.risk_rebuttal ? `\n⚠️ 리스크/반박:\n${card.risk_rebuttal}` : '',
            `\n📅 기준 시점: ${card.data_cutoff ? new Date(card.data_cutoff).toLocaleString('ko-KR') : '(미정)'}`,
            `\n🏷️ 버전: v${card.current_version}`,
        ].filter(Boolean).join('');
        alert(detail);
    } catch (err) {
        toast(err.message, 'error');
    }
}

// ============================================================
// 공시/뉴스
// ============================================================

async function loadEvents() {
    try {
        const data = await api('/events/');
        renderEvents(data);
    } catch {
        // 에러 시 기본
    }
}

function renderEvents(data) {
    const listEl = document.getElementById('events-list');
    if (!data.events || data.events.length === 0) {
        listEl.innerHTML = "<p class='empty-state'>'새로고침'을 눌러 최신 공시/뉴스를 가져오세요</p>";
        return;
    }

    listEl.innerHTML = data.events.map(e => `
        <div class="event-item glass-card">
            <span class="event-source source-${e.source}">${e.source}</span>
            <div>
                <div class="event-title">
                    ${e.url ? `<a href="${e.url}" target="_blank" style="color:inherit;text-decoration:none">${e.title}</a>` : e.title}
                </div>
                ${e.summary ? `<div class="event-summary">${e.summary}</div>` : ''}
                <div class="event-time">${e.published_at ? new Date(e.published_at).toLocaleString('ko-KR') : ''}</div>
            </div>
        </div>
    `).join('');
}

function setupEvents() {
    document.getElementById('refresh-events').addEventListener('click', async () => {
        const btn = document.getElementById('refresh-events');
        btn.textContent = '⏳ 수집 중...';
        btn.disabled = true;

        try {
            const result = await api('/events/refresh', 'POST', { source: 'all' });
            toast(result.message, 'success');
            await loadEvents();
        } catch (err) {
            toast(err.message, 'error');
        } finally {
            btn.textContent = '🔄 새로고침';
            btn.disabled = false;
        }
    });
}

// ============================================================
// 매매 기록
// ============================================================

async function loadTrades() {
    try {
        const [tradeData, perfData] = await Promise.all([
            api('/trades/'),
            api('/trades/performance'),
        ]);
        renderTrades(tradeData);
        renderPerformance(perfData);
    } catch {
        // 에러 시 기본
    }
}

function renderTrades(data) {
    const listEl = document.getElementById('trades-list');
    if (!data.trades || data.trades.length === 0) {
        listEl.innerHTML = '<p class="empty-state">매매 기록이 없습니다</p>';
        return;
    }

    listEl.innerHTML = `
        <table>
            <thead>
                <tr>
                    <th>종목</th>
                    <th>구분</th>
                    <th>수량</th>
                    <th>단가</th>
                    <th>금액</th>
                    <th>손익</th>
                    <th>체결일</th>
                </tr>
            </thead>
            <tbody>
                ${data.trades.map(t => `
                    <tr>
                        <td><strong>${t.ticker}</strong> ${t.ticker_name || ''}</td>
                        <td class="direction-${t.direction}">${t.direction === 'buy' ? '매수' : '매도'}</td>
                        <td>${t.quantity.toLocaleString()}</td>
                        <td>${t.price.toLocaleString()}</td>
                        <td>${(t.total_amount || 0).toLocaleString()}</td>
                        <td class="${t.realized_pnl > 0 ? 'pnl-positive' : t.realized_pnl < 0 ? 'pnl-negative' : ''}">
                            ${t.realized_pnl != null ? t.realized_pnl.toLocaleString() : '-'}
                        </td>
                        <td>${new Date(t.traded_at).toLocaleDateString('ko-KR')}</td>
                    </tr>
                `).join('')}
            </tbody>
        </table>
    `;
}

function renderPerformance(perf) {
    const el = document.getElementById('performance-summary');
    if (perf.total_trades === 0) {
        el.style.display = 'none';
        return;
    }

    el.style.display = 'grid';
    el.innerHTML = `
        <div class="perf-stat">
            <div class="perf-value">${perf.total_trades}</div>
            <div class="perf-label">총 거래</div>
        </div>
        <div class="perf-stat">
            <div class="perf-value" style="color:${perf.win_rate >= 0.5 ? 'var(--accent-success)' : 'var(--accent-danger)'}">
                ${(perf.win_rate * 100).toFixed(1)}%
            </div>
            <div class="perf-label">승률</div>
        </div>
        <div class="perf-stat">
            <div class="perf-value ${perf.total_pnl >= 0 ? 'pnl-positive' : 'pnl-negative'}">
                ${perf.total_pnl.toLocaleString()}
            </div>
            <div class="perf-label">총 손익</div>
        </div>
        <div class="perf-stat">
            <div class="perf-value">${perf.avg_return_pct.toFixed(2)}%</div>
            <div class="perf-label">평균 수익률</div>
        </div>
        <div class="perf-stat">
            <div class="perf-value">${perf.avg_holding_days.toFixed(1)}일</div>
            <div class="perf-label">평균 보유일</div>
        </div>
    `;
}

function setupTrades() {
    // 매매 추가 토글
    document.getElementById('add-trade-btn').addEventListener('click', () => {
        const form = document.getElementById('trade-form-container');
        form.style.display = form.style.display === 'none' ? 'block' : 'none';
    });

    document.getElementById('cancel-trade').addEventListener('click', () => {
        document.getElementById('trade-form-container').style.display = 'none';
    });

    // 매매 입력 제출
    document.getElementById('trade-form').addEventListener('submit', async (e) => {
        e.preventDefault();
        try {
            await api('/trades/', 'POST', {
                ticker: document.getElementById('trade-ticker').value,
                ticker_name: document.getElementById('trade-name').value || null,
                direction: document.getElementById('trade-direction').value,
                quantity: parseFloat(document.getElementById('trade-quantity').value),
                price: parseFloat(document.getElementById('trade-price').value),
                traded_at: new Date(document.getElementById('trade-time').value).toISOString(),
                entry_reason: document.getElementById('trade-reason').value || null,
            });
            toast('매매 기록이 저장되었습니다 ✅', 'success');
            document.getElementById('trade-form-container').style.display = 'none';
            document.getElementById('trade-form').reset();
            loadTrades();
        } catch (err) {
            toast(err.message, 'error');
        }
    });

    // CSV 업로드
    document.getElementById('csv-upload').addEventListener('change', async (e) => {
        const file = e.target.files[0];
        if (!file) return;
        try {
            const result = await apiUpload('/trades/upload-csv', file);
            toast(result.message, 'success');
            loadTrades();
        } catch (err) {
            toast(err.message, 'error');
        }
    });
}

// ============================================================
// 검색
// ============================================================

function setupSearch() {
    const submit = () => {
        const q = document.getElementById('search-input').value.trim();
        if (q) performSearch(q);
    };

    document.getElementById('search-submit').addEventListener('click', submit);
    document.getElementById('search-input').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') submit();
    });
}

async function performSearch(query) {
    try {
        const data = await api(`/search/?q=${encodeURIComponent(query)}`);
        renderSearchResults(data);
    } catch (err) {
        toast(err.message, 'error');
    }
}

function renderSearchResults(data) {
    const el = document.getElementById('search-results');
    if (!data.results || data.results.length === 0) {
        el.innerHTML = '<p class="empty-state">검색 결과가 없습니다</p>';
        return;
    }

    el.innerHTML = data.results.map(r => `
        <div class="search-result-item glass-card">
            <div class="result-score">유사도: ${(1 - r.distance).toFixed(3)}</div>
            <div class="result-text">${r.document}</div>
            <div class="card-meta">
                <span>${r.metadata.type || ''}</span>
                <span>${r.metadata.card_id ? `카드 #${r.metadata.card_id}` : ''}</span>
            </div>
        </div>
    `).join('');
}

// ============================================================
// 카드 필터
// ============================================================

function setupFilters() {
    document.getElementById('cards-filter-status').addEventListener('change', loadCards);
}

// ============================================================
// 토스트
// ============================================================

function toast(message, type = 'success') {
    const t = document.createElement('div');
    t.className = `toast toast-${type}`;
    t.textContent = message;
    document.body.appendChild(t);
    setTimeout(() => t.remove(), 3500);
}

// ============================================================
// 초기화
// ============================================================

document.addEventListener('DOMContentLoaded', () => {
    // 네비게이션
    document.querySelectorAll('.nav-link').forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            const page = link.dataset.page;
            if (page) navigateTo(page);
        });
    });

    // 이벤트 핸들러 설정
    setupAuth();
    setupAsk();
    setupEvents();
    setupTrades();
    setupSearch();
    setupFilters();

    // 토큰이 있으면 앱 표시, 없으면 로그인
    if (authToken) {
        showApp();
    } else {
        document.getElementById('sidebar').style.display = 'none';
        navigateTo('login');
    }
});
