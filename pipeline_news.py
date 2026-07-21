// pgNews.js — TradeWithTech Market Feed Page
// Tabs: Insider Trading, Financial Results, Corp Actions, Market News, Announcements

const R2_NEWS = 'https://r2-uploader.tradewithtechnical2025.workers.dev';

const NEWS_TABS = [
  { id: 'insider',       label: 'Insider Trading',    file: 'nse_insider_trading.json',  emoji: '🔍' },
  { id: 'results',       label: 'Results',             file: 'nse_results_feed.json',     emoji: '📊' },
  { id: 'corp',          label: 'Corp Actions',        file: 'nse_corp_actions.json',     emoji: '🗓️' },
  { id: 'news',          label: 'Market News',         file: 'market_news.json',          emoji: '📰' },
  { id: 'announcements', label: 'Announcements',       file: 'nse_announcements.json',    emoji: '📢' },
];

let _newsActiveTab = 'insider';
let _newsData = {};
let _newsSearch = '';
let _newsFilter = '';
let _newsInited = false;
let _resultsDetailMap = null;   // link -> parsed P&L detail from nse_results_detailed.json
let _resultsDetailLoading = null; // in-flight promise, avoids duplicate fetches

// --- Auto-update & notifications ---
const NEWS_POLL_MS = 5 * 60 * 1000;   // cache buster bhi 5-min hai, isse kam ka fayda nahi
let _newsPollTimer = null;
let _newsSeen = {};                    // tabId -> Set of item hashes (already seen)
let _newsUnseen = {};                  // tabId -> count of new unseen items

function _newsHash(s) {
  let h = 5381;
  for (let i = 0; i < s.length; i++) h = ((h << 5) + h + s.charCodeAt(i)) >>> 0;
  return h.toString(36);
}

function _newsItemKey(id, item) {
  if (id === 'insider') {
    return _newsHash([item.symbol, item.insider_name, item.trade_date_from,
      item.transaction_type, item.qty, item.pre_qty].join('|'));
  }
  return _newsHash([item.link, item.title, item.summary].join('|'));
}

function _newsLoadSeen() {
  try {
    const raw = localStorage.getItem('twt_news_seen_v1');
    if (!raw) return;
    const obj = JSON.parse(raw);
    for (const k in obj) _newsSeen[k] = new Set(obj[k]);
  } catch (e) {}
}

function _newsSaveSeen() {
  try {
    const obj = {};
    for (const k in _newsSeen) obj[k] = [..._newsSeen[k]].slice(-600); // cap per tab
    localStorage.setItem('twt_news_seen_v1', JSON.stringify(obj));
  } catch (e) {}
}

function _newsMarkSeen(id) {
  const items = _newsData[id] || [];
  if (!_newsSeen[id]) _newsSeen[id] = new Set();
  items.forEach(it => _newsSeen[id].add(_newsItemKey(id, it)));
  _newsUnseen[id] = 0;
  _newsUpdateBadge(id);
  _newsSaveSeen();
}

function _newsCountUnseen(id) {
  const items = _newsData[id] || [];
  if (!_newsSeen[id] || _newsSeen[id].size === 0) {
    // First ever load — sab seen maano, warna purane 600 items pe notification flood
    return -1;
  }
  return items.filter(it => !_newsSeen[id].has(_newsItemKey(id, it))).length;
}

function _newsUpdateBadge(id) {
  const el = document.getElementById('newsTabNew-' + id);
  if (!el) return;
  const n = _newsUnseen[id] || 0;
  el.textContent = n > 0 ? '+' + n : '';
  el.style.display = n > 0 ? 'inline' : 'none';
}

async function newsInit(tabId) {
  if (!_newsInited) {
    _newsInited = true;
    _newsLoadSeen();
    _buildNewsUI();
    _newsStartPolling();
  }
  if (tabId) _newsActiveTab = tabId;
  _setNewsTab(_newsActiveTab);
  _loadNewsTab(_newsActiveTab);
}

function _newsStartPolling() {
  if (_newsPollTimer) return;
  _newsPollTimer = setInterval(_newsPollAll, NEWS_POLL_MS);
  setTimeout(_newsPollAll, 30 * 1000);   // first poll jaldi — pichle session ke baad ke items pakde
}

function _newsNotify(title, body) {
  if (!('Notification' in window) || Notification.permission !== 'granted') return;
  try {
    // Desktop browsers
    const n = new Notification(title, { body: body, tag: 'twt-news' });
    n.onclick = function () { window.focus(); n.close(); };
  } catch (e) {
    // Android Chrome: new Notification() throws — Service Worker route try karo
    if (navigator.serviceWorker && navigator.serviceWorker.ready) {
      navigator.serviceWorker.ready.then(reg => {
        reg.showNotification(title, { body: body, tag: 'twt-news' });
      }).catch(() => {});
    }
  }
}

async function _newsPollAll() {
  // Sab tabs silently fetch karo, diff karo, badges/notifications update karo
  let notifLines = [];
  for (const tab of NEWS_TABS) {
    try {
      const tok = await getR2Token();
      if (!tok) return;
      const v = Math.floor(Date.now() / 3e5);
      const r = await fetch(R2_NEWS + '/' + tab.file + '?v=' + v, {
        headers: { Authorization: 'Bearer ' + tok }, cache: 'default'
      });
      if (!r.ok) continue;
      const data = await r.json();
      const fresh = data.items || [];
      _newsData[tab.id] = fresh;

      // Background poll bypasses _loadNewsTab (which normally awaits this
      // before rendering), so without this, a newly-arrived result would
      // render with no P&L data until a full page reload.
      if (tab.id === 'results') await _loadResultsDetail(true);

      const cntEl = document.getElementById('newsTabCnt-' + tab.id);
      if (cntEl) cntEl.textContent = '(' + fresh.length + ')';

      const unseen = _newsCountUnseen(tab.id);
      if (unseen === -1) {            // first load of this tab's data
        _newsMarkSeen(tab.id);
        continue;
      }
      _newsUnseen[tab.id] = unseen;
      _newsUpdateBadge(tab.id);
      if (unseen > 0) notifLines.push(unseen + ' ' + tab.label);
    } catch (e) { /* silent */ }
  }

  // Active tab pe naya data hai → user scroll mein disturb na ho
  const feed = document.getElementById('newsFeed');
  const onNewsPage = feed && feed.offsetParent !== null;
  if (onNewsPage && (_newsUnseen[_newsActiveTab] || 0) > 0) {
    if (feed.scrollTop < 50) {
      _renderNewsFeed(_newsActiveTab);
      _newsMarkSeen(_newsActiveTab);
    } else {
      _newsShowPill(_newsUnseen[_newsActiveTab]);
    }
  }

  // Browser notification — jab tab hidden ho YA user app ke kisi aur page pe ho
  const feedVisible = feed && feed.offsetParent !== null;
  console.log('[news poll] unseen:', JSON.stringify(_newsUnseen),
    'hidden:', document.hidden, 'feedVisible:', feedVisible);
  if (notifLines.length && (document.hidden || !feedVisible)) {
    _newsNotify('TradeWithTech — Market Feed', notifLines.join(' · '));
  }
}

function _newsShowPill(n) {
  let pill = document.getElementById('newsNewPill');
  const feed = document.getElementById('newsFeed');
  if (!feed) return;
  if (!pill) {
    pill = document.createElement('div');
    pill.id = 'newsNewPill';
    pill.style.cssText = 'position:sticky;top:6px;z-index:5;align-self:center;margin:0 auto;width:fit-content;' +
      'background:var(--accent);color:#000;font-family:var(--font-data);font-size:.76rem;font-weight:800;' +
      'padding:5px 14px;border-radius:99px;cursor:pointer;box-shadow:0 2px 10px rgba(0,0,0,.4)';
    pill.onclick = function () {
      pill.remove();
      _renderNewsFeed(_newsActiveTab);
      _newsMarkSeen(_newsActiveTab);
      const f = document.getElementById('newsFeed');
      if (f) f.scrollTop = 0;
    };
    feed.prepend(pill);
  }
  pill.textContent = '↑ ' + n + ' new';
}

function _newsToggleNotif() {
  const btn = document.getElementById('newsNotifBtn');
  if (!('Notification' in window)) {
    alert('Browser notifications not supported');
    return;
  }
  if (Notification.permission === 'granted') {
    _newsNotify('TradeWithTech — Test', 'Notifications are working ✓');
    return;
  }
  if (Notification.permission === 'denied') {
    alert('Notifications are blocked. Enable them in your browser site settings (lock icon in address bar).');
    return;
  }
  Notification.requestPermission().then(p => {
    if (btn) btn.style.color = p === 'granted' ? 'var(--accent)' : 'var(--muted)';
    if (p === 'granted') _newsNotify('TradeWithTech — Test', 'Notifications enabled ✓');
  });
}

function _buildNewsUI() {
  const pg = document.getElementById('pgNews');
  if (!pg) return;

  pg.innerHTML = `
    <div id="newsWrap" style="display:flex;flex-direction:column;height:100%;overflow:hidden;background:var(--bg)">

      <!-- Header -->
      <div style="display:flex;align-items:center;gap:10px;padding:10px 14px 0;flex-shrink:0">
        <span style="font-family:var(--font-data);font-size:.9rem;font-weight:800;color:var(--text);flex:1">Market Feed</span>
        <span id="newsLiveTag" style="display:flex;align-items:center;gap:5px;font-family:var(--font-data);font-size:.7rem;color:var(--muted)">
          <span id="newsDot" style="width:6px;height:6px;border-radius:50%;background:var(--green);animation:newsPulse 1.5s infinite"></span>
          Live
        </span>
        <button id="newsNotifBtn" onclick="_newsToggleNotif()" title="Browser notifications"
          style="background:none;border:1px solid var(--border);border-radius:6px;cursor:pointer;padding:4px 9px;font-family:var(--font-data);font-size:.78rem;color:var(--muted)">
          🔔
        </button>
        <button onclick="_refreshNewsTab()" title="Refresh"
          style="background:none;border:1px solid var(--border);border-radius:6px;color:var(--muted);cursor:pointer;padding:4px 9px;font-family:var(--font-data);font-size:.78rem">
          ⟳
        </button>
      </div>

      <!-- Tabs -->
      <div id="newsTabs" style="display:flex;gap:0;padding:8px 14px 0;border-bottom:1px solid var(--border);flex-shrink:0;overflow-x:auto"></div>

      <!-- Search + Filter -->
      <div style="display:flex;gap:8px;padding:8px 14px;flex-shrink:0">
        <input id="newsSearch" type="text" placeholder="Search symbol, company, title…"
          oninput="_newsSearchChange(this.value)"
          style="flex:1;background:var(--surface2);border:1px solid var(--border);border-radius:7px;padding:6px 10px;color:var(--text);font-family:var(--font-data);font-size:.82rem;outline:none">
        <select id="newsFilter" onchange="_newsFilterChange(this.value)"
          style="background:var(--surface2);border:1px solid var(--border);border-radius:7px;padding:6px 8px;color:var(--text);font-family:var(--font-data);font-size:.82rem;outline:none;cursor:pointer">
          <option value="">All</option>
        </select>
      </div>

      <!-- Stats bar -->
      <div id="newsStats" style="display:flex;gap:12px;padding:0 14px 8px;flex-shrink:0"></div>

      <!-- Feed -->
      <div id="newsFeed" style="flex:1;overflow-y:auto;padding:0 14px 14px"></div>
    </div>

    <style>
      @keyframes newsPulse { 0%,100%{opacity:1} 50%{opacity:.3} }
      .news-card { display:flex;gap:10px;align-items:flex-start;padding:10px 12px;
        background:var(--surface2);border:1px solid var(--border);border-radius:9px;
        margin-bottom:6px;cursor:pointer;transition:background .1s }
      .news-card:hover { background:var(--surface3,var(--surface)) }
      .news-badge { font-size:.7rem;font-weight:800;padding:2px 7px;border-radius:99px;flex-shrink:0;margin-top:1px;line-height:1.6 }
      .news-body { flex:1;min-width:0 }
      .news-title { font-family:var(--font-data);font-size:.9rem;font-weight:600;color:var(--text);
        white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-bottom:2px }
      .news-sub { font-family:var(--font-data);font-size:.76rem;color:var(--muted);line-height:1.5;
        display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden }
      .news-time { font-family:var(--font-data);font-size:.72rem;color:var(--muted);flex-shrink:0;margin-top:2px }
      .news-empty { display:flex;flex-direction:column;align-items:center;justify-content:center;
        padding:60px 20px;gap:10px;color:var(--muted);font-family:var(--font-data);font-size:.85rem;text-align:center }
    </style>
  `;

  // Build tabs
  const tabsEl = document.getElementById('newsTabs');
  tabsEl.innerHTML = NEWS_TABS.map(t => `
    <div id="newsTab-${t.id}" onclick="_setNewsTab('${t.id}');_loadNewsTab('${t.id}')"
      style="padding:7px 14px;font-family:var(--font-data);font-size:.82rem;font-weight:600;
        cursor:pointer;border-bottom:2px solid transparent;white-space:nowrap;color:var(--muted);
        transition:color .15s,border-color .15s">
      ${t.emoji} ${t.label}
      <span id="newsTabCnt-${t.id}" style="font-size:.7rem;color:var(--muted);margin-left:3px"></span>
      <span id="newsTabNew-${t.id}" style="display:none;font-size:.68rem;font-weight:800;color:#000;background:var(--accent);border-radius:99px;padding:1px 6px;margin-left:4px;vertical-align:1px"></span>
    </div>
  `).join('');

  // Bell state
  const nb = document.getElementById('newsNotifBtn');
  if (nb && 'Notification' in window && Notification.permission === 'granted') {
    nb.style.color = 'var(--accent)';
  }
}

function _setNewsTab(id) {
  _newsActiveTab = id;
  _newsSearch = '';
  _newsFilter = '';
  const searchEl = document.getElementById('newsSearch');
  if (searchEl) searchEl.value = '';

  NEWS_TABS.forEach(t => {
    const el = document.getElementById('newsTab-' + t.id);
    if (!el) return;
    const active = t.id === id;
    el.style.color = active ? 'var(--accent)' : 'var(--muted)';
    el.style.borderBottomColor = active ? 'var(--accent)' : 'transparent';
  });
}

async function _loadResultsDetail(forceRefresh = false) {
  if (_resultsDetailMap && !forceRefresh) return _resultsDetailMap;
  if (_resultsDetailLoading && !forceRefresh) return _resultsDetailLoading;

  _resultsDetailLoading = (async () => {
    try {
      const tok = await getR2Token();
      if (!tok) throw new Error('Not authenticated');
      const v = Math.floor(Date.now() / 3e5);
      const r = await fetch(R2_NEWS + '/nse_results_detailed.json?v=' + v, {
        headers: { Authorization: 'Bearer ' + tok }, cache: 'default'
      });
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const data = await r.json();
      const map = {};
      (data.items || []).forEach(it => { if (it.link) map[it.link] = it; });
      _resultsDetailMap = map;
      return map;
    } catch (e) {
      console.warn('[results detail] load failed:', e.message);
      _resultsDetailMap = {};
      return _resultsDetailMap;
    } finally {
      _resultsDetailLoading = null;
    }
  })();

  return _resultsDetailLoading;
}

async function _loadNewsTab(id, forceRefresh = false) {
  const tab = NEWS_TABS.find(t => t.id === id);
  if (!tab) return;
  const feed = document.getElementById('newsFeed');
  if (!feed) return;

  if (id === 'results') await _loadResultsDetail(forceRefresh);

  // Use cache if available
  if (_newsData[id] && !forceRefresh) {
    _renderNewsFeed(id);
    _newsMarkSeen(id);
    return;
  }

  feed.innerHTML = '<div class="news-empty"><div style="font-size:1.5rem">⏳</div>Loading…</div>';

  try {
    const tok = await getR2Token();
    if (!tok) throw new Error('Not authenticated');
    const v = Math.floor(Date.now() / 3e5);
    const r = await fetch(R2_NEWS + '/' + tab.file + '?v=' + v, {
      headers: { Authorization: 'Bearer ' + tok }, cache: 'default'
    });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const data = await r.json();
    _newsData[id] = data.items || [];

    // Update tab count badge
    const cntEl = document.getElementById('newsTabCnt-' + id);
    if (cntEl) cntEl.textContent = '(' + _newsData[id].length + ')';

    _renderNewsFeed(id);
    _newsMarkSeen(id);   // user dekh raha hai — sab seen
  } catch (e) {
    feed.innerHTML = `<div class="news-empty"><div style="font-size:1.5rem">⚠️</div>${e.message}</div>`;
  }
}

function _refreshNewsTab() {
  delete _newsData[_newsActiveTab];
  if (_newsActiveTab === 'results') _resultsDetailMap = null;
  _loadNewsTab(_newsActiveTab, true);
}

function _newsSearchChange(val) {
  _newsSearch = val.toLowerCase().trim();
  _renderNewsFeed(_newsActiveTab);
}

function _newsFilterChange(val) {
  _newsFilter = val;
  _renderNewsFeed(_newsActiveTab);
}

function _renderNewsFeed(id) {
  const feed = document.getElementById('newsFeed');
  const filterEl = document.getElementById('newsFilter');
  const statsEl = document.getElementById('newsStats');
  if (!feed) return;

  const items = _newsData[id] || [];
  if (!items.length) {
    feed.innerHTML = '<div class="news-empty"><div style="font-size:1.5rem">📭</div>No data yet — run the pipeline first</div>';
    return;
  }

  // Build filter options
  const filterOpts = _getFilterOpts(id, items);
  if (filterEl) {
    const cur = filterEl.value;
    filterEl.innerHTML = '<option value="">All</option>' +
      filterOpts.map(o => `<option value="${o}">${o}</option>`).join('');
    if (filterOpts.includes(cur)) filterEl.value = cur;
  }

  // Apply search + filter
  let filtered = items.filter(item => {
    const text = [item.company, item.symbol, item.title, item.insider_name,
      item.source, item.summary, item.transaction_type].filter(Boolean).join(' ').toLowerCase();
    if (_newsSearch && !text.includes(_newsSearch)) return false;
    if (_newsFilter && _getFilterVal(id, item) !== _newsFilter) return false;
    return true;
  });

  // Stats bar
  if (statsEl) statsEl.innerHTML = _buildStats(id, filtered);

  // Render cards
  if (!filtered.length) {
    feed.innerHTML = '<div class="news-empty"><div style="font-size:1.5rem">🔍</div>No results</div>';
    return;
  }

  let toRender = filtered;
  if (id === 'results') toRender = _groupResultsItems(filtered);

  feed.innerHTML = toRender.slice(0, 200).map(item => _cardHTML(id, item)).join('');
}

// Groups Standalone + Consolidated feed entries for the same company/quarter
// into a single row (item.__mergedItems), so they render as one card with
// two side-by-side columns instead of two separate rows. Grouping key needs
// the parsed detail (scrip_code + board_meeting_date + period_end) — items
// whose XBRL hasn't been parsed yet (no detail match) are left ungrouped
// rather than guessed at.
function _groupResultsItems(items) {
  const groups = new Map();
  const order = [];
  items.forEach(it => {
    const detail = (_resultsDetailMap || {})[it.link];
    const key = (detail && detail.meta)
      ? ['g', detail.meta.scrip_code, detail.meta.board_meeting_date, (detail.quarter && detail.quarter.period_end) || ''].join('|')
      : 'u|' + it.link; // ungrouped — no detail yet, keep as its own row
    if (!groups.has(key)) {
      const primary = Object.assign({}, it, { __mergedItems: [it] });
      groups.set(key, primary);
      order.push(key);
    } else {
      groups.get(key).__mergedItems.push(it);
    }
  });
  return order.map(k => groups.get(k));
}

function _getFilterOpts(id, items) {
  if (id === 'insider') return [...new Set(items.map(i => i.transaction_type).filter(Boolean))].sort();
  if (id === 'corp')    return [...new Set(items.map(i => _corpPurpose(i.summary)).filter(Boolean))].sort();
  if (id === 'news')    return [...new Set(items.map(i => i.source).filter(Boolean))].sort();
  return [];
}

function _getFilterVal(id, item) {
  if (id === 'insider') return item.transaction_type || '';
  if (id === 'corp')    return _corpPurpose(item.summary) || '';
  if (id === 'news')    return item.source || '';
  return '';
}

function _corpPurpose(summary) {
  if (!summary) return '';
  const s = summary.toUpperCase();
  if (s.includes('BONUS'))    return 'Bonus';
  if (s.includes('SPLIT'))    return 'Split';
  if (s.includes('DIVIDEND')) return 'Dividend';
  if (s.includes('BUYBACK'))  return 'Buyback';
  if (s.includes('AGM'))      return 'AGM';
  return 'Other';
}

function _buildStats(id, items) {
  if (id === 'insider') {
    const buys  = items.filter(i => i.transaction_type === 'Buy').length;
    const sells = items.filter(i => i.transaction_type === 'Sell').length;
    const totalVal = items.reduce((a, i) => a + (parseFloat(i.value_inr) || 0), 0);
    return `
      <span style="font-family:var(--font-data);font-size:.78rem;color:var(--muted)">
        <span style="color:var(--green);font-weight:700">${buys} Buy</span> ·
        <span style="color:var(--red);font-weight:700">${sells} Sell</span> ·
        Total: <span style="color:var(--text)">${_fmtVal(totalVal)}</span>
      </span>`;
  }
  return `<span style="font-family:var(--font-data);font-size:.78rem;color:var(--muted)">${items.length} items</span>`;
}

function _cardHTML(id, item) {
  const time = _fmtTime(item.published || item.date_filing || '');

  if (id === 'insider') {
    const isBuy  = item.transaction_type === 'Buy';
    const isSell = item.transaction_type === 'Sell';
    const badgeBg = isBuy ? 'rgba(0,230,118,.15)' : isSell ? 'rgba(255,61,90,.12)' : 'rgba(200,200,200,.1)';
    const badgeColor = isBuy ? 'var(--green)' : isSell ? 'var(--red)' : 'var(--muted)';
    const avgPrice = item.qty && item.value_inr
      ? (parseFloat(item.value_inr) / parseFloat(item.qty)).toFixed(1) : null;

    return `
      <div class="news-card" onclick="_newsOpenStock('${item.symbol}', '${item.html_url || item.xml_url || ''}')">
        <span class="news-badge" style="background:${badgeBg};color:${badgeColor}">
          ${item.transaction_type || '—'}
        </span>
        <div class="news-body">
          <div class="news-title">
            <span style="color:var(--accent);font-weight:800">${item.symbol || ''}</span>
            ${item.symbol ? ' — ' : ''}${item.insider_name || item.company || ''}
          </div>
          <div class="news-sub">
            ${item.insider_category ? `<span style="color:var(--text2)">${item.insider_category}</span> · ` : ''}
            Qty: <b style="color:var(--text)">${_fmtQty(item.qty)}</b>
            ${item.value_inr ? ` · Value: <b style="color:var(--text)">${_fmtVal(item.value_inr)}</b>` : ''}
            ${avgPrice ? ` · ~₹${parseFloat(avgPrice).toLocaleString('en-IN')}` : ''}
            ${item.mode ? ` · ${item.mode}` : ''}
          </div>
        </div>
        <div class="news-time">${time}</div>
      </div>`;
  }

  if (id === 'results') {
    // Builds the 3 display lines (main/QoQ/YoY) for a single detail object
    // (one basis — Standalone or Consolidated). Shared by both the
    // single-column and two-column (merged) layouts below.
    function _buildResultColumn(detail) {
      const q = detail && detail.quarter;
      const nature = detail && detail.meta && detail.meta.standalone_consolidated;
      let mainLine = '', qoqLine = '', yoyLine = '';

      if (q && (q.revenue != null || q.pat != null)) {
        const mainParts = [];
        if (q.revenue != null) mainParts.push(`Rev: <b style="color:var(--text)">${_fmtVal(q.revenue)}</b>`);
        if (q.pat != null) {
          const patColor = q.pat >= 0 ? 'var(--green)' : 'var(--red)';
          mainParts.push(`PAT: <b style="color:${patColor}">${_fmtVal(q.pat)}</b>`);
        }
        if (q.eps_basic != null) mainParts.push(`EPS: <b style="color:var(--text)">₹${q.eps_basic}</b>`);
        mainLine = mainParts.join(' · ');

        function buildCompareLine(label, revPrior, revPct, patPrior, patPct, epsPrior, epsPct, opmCurrentPct, opmPP, verified, tip) {
          const bits = [];
          const prefix = verified ? '' : '~';
          if (revPrior != null && revPct != null) {
            const color = revPct >= 0 ? 'var(--green)' : 'var(--red)';
            bits.push(`Rev ${_fmtVal(revPrior)} <b style="color:${color}" title="${tip}">(${prefix}${revPct >= 0 ? '+' : ''}${revPct.toFixed(1)}%)</b>`);
          }
          if (patPrior != null && patPct != null) {
            const color = patPct >= 0 ? 'var(--green)' : 'var(--red)';
            bits.push(`PAT ${_fmtVal(patPrior)} <b style="color:${color}" title="${tip}">(${prefix}${patPct >= 0 ? '+' : ''}${patPct.toFixed(1)}%)</b>`);
          }
          if (epsPrior != null && epsPct != null) {
            const color = epsPct >= 0 ? 'var(--green)' : 'var(--red)';
            bits.push(`EPS ₹${epsPrior} <b style="color:${color}" title="${tip}">(${prefix}${epsPct >= 0 ? '+' : ''}${epsPct.toFixed(1)}%)</b>`);
          }
          if (opmCurrentPct != null && opmPP != null) {
            const color = opmPP >= 0 ? 'var(--green)' : 'var(--red)';
            bits.push(`OPM ${opmCurrentPct.toFixed(1)}% <b style="color:${color}" title="${tip}">(${prefix}${opmPP >= 0 ? '+' : ''}${opmPP.toFixed(1)}pp)</b>`);
          }
          return bits.length ? `${label}: ${bits.join(' · ')}` : '';
        }

        const curOpmPct = q.opm != null ? q.opm * 100 : null;

        const qf = detail.qoq_fundamentals;
        if (qf) {
          const verified = !!qf.basis_verified;
          const tip = verified
            ? `Verified same-basis comparison (${qf.basis}) vs ${qf.prior_header}`
            : `Approximate — from fundamentals data, basis may differ`;
          qoqLine = buildCompareLine('QoQ', qf.sales_prior, qf.sales_qoq_pct, qf.pat_prior, qf.pat_qoq_pct,
            qf.eps_prior, qf.eps_qoq_pct, curOpmPct, qf.opm_qoq_pp, verified, tip);
        }

        const yoy = detail.yoy_comparison;
        const yf = detail.yoy_fundamentals;
        if (yoy) {
          function pct(curV, priorV) {
            if (curV == null || priorV == null || priorV === 0) return null;
            return ((curV - priorV) / Math.abs(priorV)) * 100;
          }
          const opmPP = (q.opm != null && yoy.opm != null) ? (q.opm - yoy.opm) * 100 : null;
          yoyLine = buildCompareLine('YoY', yoy.revenue, pct(q.revenue, yoy.revenue), yoy.pat, pct(q.pat, yoy.pat),
            yoy.eps_basic, pct(q.eps_basic, yoy.eps_basic), curOpmPct, opmPP, true, 'Verified same-basis comparison (from XBRL filing)');
        } else if (yf) {
          const verified = !!yf.basis_verified;
          const tip = verified
            ? `Verified same-basis comparison (${yf.basis}) vs ${yf.prior_header}`
            : `Approximate — from fundamentals data, basis may differ`;
          yoyLine = buildCompareLine('YoY', yf.sales_prior, yf.sales_yoy_pct, yf.pat_prior, yf.pat_yoy_pct,
            yf.eps_prior, yf.eps_yoy_pct, curOpmPct, yf.opm_yoy_pp, verified, tip);
        }
      }
      return { nature, mainLine, qoqLine, yoyLine };
    }

    // item.__mergedItems is set by _renderNewsFeed when it groups the
    // Standalone + Consolidated feed entries for the same company/quarter
    // into one row (see _groupResultsItems). Falls back to just this item
    // when there's nothing to merge (e.g. only one basis was filed).
    const mergedLinks = (item.__mergedItems || [item]).map(m => m.link);
    const details = mergedLinks.map(l => (_resultsDetailMap || {})[l]).filter(Boolean);
    details.sort((a, b) => {
      const na = (a.meta && a.meta.standalone_consolidated) || '';
      return na === 'Consolidated' ? -1 : 1;
    });

    const anySymbol = details.map(d => d.meta && d.meta.symbol).find(Boolean) || '';
    const clickTarget = mergedLinks[0];

    if (details.length >= 2) {
      // Side-by-side two-column layout: one column per basis
      const cols = details.slice(0, 2).map(_buildResultColumn);
      const colHtml = cols.map(c => `
        <div style="flex:1;min-width:0">
          <div style="font-family:var(--font-data);font-size:.6rem;font-weight:800;color:var(--accent);text-transform:uppercase;letter-spacing:.4px;margin-bottom:2px">${c.nature ? c.nature.toUpperCase() : ''}</div>
          <div class="news-sub">${c.mainLine || '—'}</div>
          ${c.qoqLine ? `<div class="news-sub" style="margin-top:2px">${c.qoqLine}</div>` : ''}
          ${c.yoyLine ? `<div class="news-sub" style="margin-top:2px">${c.yoyLine}</div>` : ''}
        </div>`).join('<div style="width:1px;background:var(--border);flex-shrink:0"></div>');

      return `
        <div class="news-card" onclick="_newsOpenStock('${anySymbol}', '${clickTarget}')">
          <span class="news-badge" style="background:rgba(0,212,255,.12);color:var(--accent)">RESULT</span>
          <div class="news-body">
            <div class="news-title">${item.title || '—'}</div>
            <div style="display:flex;gap:12px;margin-top:2px">${colHtml}</div>
          </div>
          <div class="news-time">${time}</div>
        </div>`;
    }

    // Single-basis (or not-yet-parsed) fallback — original single-column look
    const detail = details[0];
    const { nature, mainLine, qoqLine, yoyLine } = detail
      ? _buildResultColumn(detail)
      : { nature: null, mainLine: item.summary || '', qoqLine: '', yoyLine: '' };

    return `
      <div class="news-card" onclick="_newsOpenStock('${anySymbol}', '${clickTarget}')">
        <span class="news-badge" style="background:rgba(0,212,255,.12);color:var(--accent)">${nature ? nature.toUpperCase() : 'RESULT'}</span>
        <div class="news-body">
          <div class="news-title">${item.title || '—'}</div>
          <div class="news-sub">${mainLine}</div>
          ${qoqLine ? `<div class="news-sub" style="margin-top:2px">${qoqLine}</div>` : ''}
          ${yoyLine ? `<div class="news-sub" style="margin-top:2px">${yoyLine}</div>` : ''}
        </div>
        <div class="news-time">${time}</div>
      </div>`;
  }

  if (id === 'corp') {
    const purpose = _corpPurpose(item.summary);
    const purposeColor = purpose === 'Dividend' ? 'rgba(0,230,118,.15)' :
      purpose === 'Bonus' ? 'rgba(255,215,64,.15)' :
      purpose === 'Buyback' ? 'rgba(167,139,250,.15)' : 'rgba(200,200,200,.1)';
    const purposeText = purpose === 'Dividend' ? 'var(--green)' :
      purpose === 'Bonus' ? 'var(--yellow)' :
      purpose === 'Buyback' ? 'var(--purple)' : 'var(--muted)';
    return `
      <div class="news-card" onclick="_newsOpenStock('${(item.title || '').replace(/'/g, "\\'")}', '${item.link}')">
        <span class="news-badge" style="background:${purposeColor};color:${purposeText}">${purpose}</span>
        <div class="news-body">
          <div class="news-title">${item.title || '—'}</div>
          <div class="news-sub">${(item.summary || '').replace(/\|/g,' · ')}</div>
        </div>
        <div class="news-time">${time}</div>
      </div>`;
  }

  if (id === 'news') {
    return `
      <div class="news-card" onclick="window.open('${item.link}','_blank')">
        <span class="news-badge" style="background:rgba(167,139,250,.15);color:var(--purple)">NEWS</span>
        <div class="news-body">
          <div class="news-title">${item.title || '—'}</div>
          <div class="news-sub">
            <span style="color:var(--accent)">${item.source || ''}</span>
            ${item.summary ? ' · ' + item.summary : ''}
          </div>
        </div>
        <div class="news-time">${time}</div>
      </div>`;
  }

  if (id === 'announcements') {
    return `
      <div class="news-card" onclick="_newsOpenStock('${(item.title || '').replace(/'/g, "\\'")}', '${item.link}')">
        <span class="news-badge" style="background:rgba(251,146,60,.12);color:var(--orange)">ANNC</span>
        <div class="news-body">
          <div class="news-title">${item.title || '—'}</div>
          <div class="news-sub">${(item.summary || '')}</div>
        </div>
        <div class="news-time">${time}</div>
      </div>`;
  }

  return '';
}

let _pgNewsNameMap = null;

function _pgNewsNormName(s) {
  return (s || '').toUpperCase()
    .replace(/\bLIMITED\b/g, '')
    .replace(/\bLTD\.?\b/g, '')
    .replace(/\bCOMPANY\b/g, '')
    .replace(/\bCO\.?\b/g, '')
    .replace(/[^A-Z0-9]/g, '')
    .trim();
}

// Builds a company-name -> symbol lookup from fundaMap (already loaded
// elsewhere on the page for Results Comparison / Peer Comparison), since
// corp actions / announcements only carry the company name, not the symbol.
function _pgNewsResolveSymbolByName(name) {
  if (!name) return null;
  if (!_pgNewsNameMap) {
    _pgNewsNameMap = {};
    const src = (typeof fundaMap !== 'undefined' && fundaMap) ? fundaMap : null;
    if (src) {
      Object.keys(src).forEach(sym => {
        const nm = src[sym] && src[sym].name;
        if (nm) _pgNewsNameMap[_pgNewsNormName(nm)] = sym;
      });
    }
  }
  return _pgNewsNameMap[_pgNewsNormName(name)] || null;
}

// symbolOrName: either an exact NSE symbol (insider trading, results — both
// carry the real symbol) or a company name (corp actions, announcements —
// only the title/company name is available). fallbackLink is used only if
// no symbol can be resolved either way, so the click still goes somewhere.
function _newsOpenStock(symbolOrName, fallbackLink) {
  let sym = null;
  if (symbolOrName) {
    const upper = symbolOrName.toUpperCase();
    if (typeof allStocks !== 'undefined' && allStocks.some(s => s.stock === upper)) {
      sym = upper;
    } else {
      sym = _pgNewsResolveSymbolByName(symbolOrName);
    }
  }
  if (sym && typeof ovLoad === 'function') {
    ovLoad(sym);
  } else if (fallbackLink) {
    window.open(fallbackLink, '_blank');
  }
}

function _fmtTime(ts) {
  if (!ts) return '';
  // Try to extract just time if today
  const match = ts.match(/(\d{2}:\d{2})/);
  if (match) return match[1];
  return ts.slice(0, 10);
}

function _fmtQty(qty) {
  if (!qty) return '—';
  const n = parseFloat(qty);
  if (isNaN(n)) return qty;
  if (n >= 1e7) return (n / 1e7).toFixed(2) + ' Cr';
  if (n >= 1e5) return (n / 1e5).toFixed(2) + ' L';
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return n.toLocaleString('en-IN');
}

function _fmtVal(val) {
  if (!val) return '—';
  const n = parseFloat(val);
  if (isNaN(n)) return '—';
  if (n >= 1e7) return '₹' + (n / 1e7).toFixed(2) + ' Cr';
  if (n >= 1e5) return '₹' + (n / 1e5).toFixed(2) + ' L';
  return '₹' + n.toLocaleString('en-IN');
}

// Called by notification.js navigate event
window.addEventListener('notif:navigate', function(e) {
  const tabMap = {
    insider: 'insider', results: 'results',
    corp: 'corp', news: 'news', announcements: 'announcements'
  };
  const tab = tabMap[e.detail];
  if (tab && typeof switchPage === 'function') {
    switchPage('market');
    setTimeout(() => newsInit(tab), 100);
  }
});
