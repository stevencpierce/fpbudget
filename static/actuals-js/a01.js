/* FPBudget Actuals workspace — a01: core state, data, rendering, selection,
   keyboard, toasts. (ACTUALS_HANDOFF Phase 3, 2026-11-01.)

   Classic script, same split-file pattern as static/budget-js: a02.js
   (drag and drop + recent targets) and a03.js (actions + modals) extend
   the window.AW namespace this file creates. Server values arrive via
   window.__AJ (see templates/actuals.html). Data is ONE payload,
   GET /projects/<pid>/actuals/workspace.json, re-fetched after every
   mutation — the screen never patches state it did not get from the
   server. */
(function () {
  'use strict';
  const AJ = window.__AJ || {};
  const AW = window.AW = window.AW || {};
  const PID = AJ.pid;
  const $ = id => document.getElementById(id);
  AW.$ = $;
  AW.PID = PID;
  AW.BID = AJ.bid;

  // ── Utilities ────────────────────────────────────────────────────────
  const esc = s => String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  const money = v => {
    const n = Number(v || 0);
    return (n < 0 ? '-' : '') + '$' + Math.abs(n).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  };
  const whole = v => {
    const n = Math.round(Number(v || 0));
    return (n < 0 ? '-' : '') + Math.abs(n).toLocaleString('en-US');
  };
  const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  const shortDate = iso => {
    if (!iso) return '—';
    const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(iso);
    if (!m) return iso;
    const y = +m[1], mo = +m[2], d = +m[3];
    const thisYear = new Date().getFullYear();
    return MONTHS[mo - 1] + ' ' + d + (y !== thisYear ? ' ' + String(y).slice(2) : '');
  };
  const whenText = iso => {
    if (!iso) return '';
    const d = new Date(iso.length === 16 ? iso + ':00' : iso);
    if (isNaN(d)) return iso;
    const mins = Math.round((Date.now() - d.getTime()) / 60000);
    if (mins < 2) return 'just now';
    if (mins < 60) return mins + 'm ago';
    const h = Math.round(mins / 60);
    if (h < 36) return h + 'h ago';
    return shortDate(iso.slice(0, 10));
  };
  AW.esc = esc; AW.money = money; AW.whole = whole; AW.shortDate = shortDate; AW.whenText = whenText;

  AW.get = async function (url) {
    const r = await fetch(url, { credentials: 'same-origin' });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(d.error || ('HTTP ' + r.status));
    return d;
  };
  AW.post = async function (url, body, method) {
    const r = await fetch(url, { method: method || 'POST', credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body || {}) });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) { const e = new Error(d.error || ('HTTP ' + r.status)); e.data = d; e.status = r.status; throw e; }
    return d;
  };
  AW.postForm = async function (url, fd) {
    const r = await fetch(url, { method: 'POST', credentials: 'same-origin', body: fd });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) { const e = new Error(d.error || ('HTTP ' + r.status)); e.data = d; throw e; }
    return d;
  };
  AW.txnUrl = (tid, tail) => `/projects/${PID}/actuals/transaction/${tid}/${tail}`;

  // ── State ────────────────────────────────────────────────────────────
  const VIEWS = ['unclaimed', 'assigned', 'needs_backup', 'receipts', 'not_project', 'sync'];
  const S = AW.state = {
    data: null, view: VIEWS.includes(AJ.initialView) ? AJ.initialView : 'unclaimed',
    q: '', card: '', from: '', to: '',
    sel: new Set(), lastIdx: null, rows: [],      // rows: txn rows in render order (for shift-click)
    lineFilter: null, loading: false, focusTxn: AJ.focusTxn || null,
    byId: new Map(), lineById: new Map(), sectionOfLine: new Map(),
  };

  // ── Data ─────────────────────────────────────────────────────────────
  AW.load = async function (opts) {
    opts = opts || {};
    S.loading = true;
    try {
      const d = await AW.get(AJ.urls.workspace);
      S.data = d;
      S.byId = new Map();
      (d.transactions || []).forEach(t => S.byId.set(t.id, t));
      (d.not_project || []).forEach(t => S.byId.set(t.id, t));
      S.lineById = new Map(); S.sectionOfLine = new Map();
      ((d.budget || {}).sections || []).forEach(sec => (sec.lines || []).forEach(l => {
        S.lineById.set(l.id, l); S.sectionOfLine.set(l.id, sec);
      }));
      // Drop selections that no longer exist in the pool.
      Array.from(S.sel).forEach(id => { if (!S.byId.has(id)) S.sel.delete(id); });
      AW.render();
      if (S.focusTxn && !opts.keepFocus) AW.focusTransaction(S.focusTxn);
      S.focusTxn = null;
    } catch (e) {
      $('aw-table').innerHTML = `<p class="aw-empty">Could not load the workspace: ${esc(e.message || e)}. Refresh to retry.</p>`;
      $('aw-budget').innerHTML = `<p class="aw-empty">Budget unavailable.</p>`;
    } finally { S.loading = false; }
  };

  // ── Derived helpers ──────────────────────────────────────────────────
  AW.isAssigned = t => !!(t.assigned_line || (t.splits && t.splits.length) || t.section_only || (t.itemized && t.itemized.length));
  AW.isContainer = t => !!((t.splits && t.splits.length) || (t.itemized && t.itemized.length));
  AW.sectionCodeOf = t => {
    if (t.assigned_line && S.sectionOfLine.get(t.assigned_line.id)) return S.sectionOfLine.get(t.assigned_line.id).code;
    if (t.splits && t.splits.length) { const sec = S.sectionOfLine.get(t.splits[0].budget_line_id); if (sec) return sec.code; }
    if (t.section_only) return t.section_only.code;
    if (t.assigned_line) { const m = /^(\d+)/.exec(t.assigned_line.label || ''); if (m) return AW.sectionFor(+m[1]); }
    return null;
  };
  AW.sectionFor = code => {
    const secs = ((S.data || {}).budget || {}).sections || [];
    let best = null;
    secs.forEach(s => { if (s.code != null && s.code <= code && (best == null || s.code > best)) best = s.code; });
    return best;
  };
  AW.sectionName = code => {
    const secs = ((S.data || {}).budget || {}).sections || [];
    const s = secs.find(x => x.code === code);
    return s ? s.name : '';
  };

  function matchesFilters(t) {
    if (S.q) {
      const q = S.q.toLowerCase();
      const hay = ((t.vendor || '') + ' ' + (t.memo || '') + ' ' + (t.name || '') + ' ' + (t.card_last4 || '') + ' ' +
                   ((t.assigned_line || {}).label || '') + ' ' + (t.payee || '')).toLowerCase();
      const amt = t.amount != null ? Math.abs(Number(t.amount)).toFixed(2) : '';
      if (!hay.includes(q) && !amt.includes(q.replace(/[$,]/g, ''))) return false;
    }
    if (S.card && (t.card_last4 || '') !== S.card) return false;
    if (S.from && (t.date || '') && t.date < S.from) return false;
    if (S.to && (t.date || '') && t.date > S.to) return false;
    return true;
  }

  AW.viewRows = function (view) {
    const d = S.data; if (!d) return [];
    const txns = d.transactions || [];
    if (view === 'unclaimed') return txns.filter(t => !AW.isAssigned(t));
    if (view === 'assigned') return txns.filter(t => AW.isAssigned(t) && (!S.lineFilter || AW.touchesLine(t, S.lineFilter)));
    if (view === 'needs_backup') return txns.filter(t => (t.flags.red || []).includes('needs_backup'));
    if (view === 'receipts') return d.receipts || [];
    if (view === 'not_project') return d.not_project || [];
    return [];
  };
  AW.touchesLine = (t, lid) => (t.assigned_line && t.assigned_line.id === lid)
    || (t.splits || []).some(s => s.budget_line_id === lid)
    || (t.itemized || []).some(k => k.budget_line_id === lid);

  // ── Render ───────────────────────────────────────────────────────────
  AW.render = function () {
    const d = S.data; if (!d) return;
    // Counts on the segmented control.
    const c = d.counts || {};
    document.querySelectorAll('#aw-views [data-count]').forEach(el => {
      const k = el.dataset.count; el.textContent = c[k] != null ? c[k] : 0;
    });
    document.querySelectorAll('#aw-views .aw-view').forEach(b => {
      if (b.dataset.view === S.view) b.setAttribute('aria-current', 'page'); else b.removeAttribute('aria-current');
    });
    // Sync status in the header.
    const sy = d.sync || {};
    $('aw-syncdot').classList.toggle('off', !sy.connected);
    $('aw-synctext').textContent = sy.connected
      ? `QuickBooks synced ${sy.last_synced ? whenText(sy.last_synced) : 'never'} · ${sy.accounts} account${sy.accounts === 1 ? '' : 's'}`
      : 'QuickBooks not connected';
    $('aw-syncnow').disabled = !sy.connected || !sy.accounts;
    // Card filter options (kept in sync with the pool).
    const cards = new Set();
    (d.transactions || []).concat(d.not_project || []).forEach(t => { if (t.card_last4) cards.add(t.card_last4); });
    const sel = $('aw-card');
    const have = Array.from(sel.options).map(o => o.value).slice(1).join(',');
    const want = Array.from(cards).sort();
    if (have !== want.join(',')) {
      sel.innerHTML = '<option value="">All cards</option>' + want.map(c => `<option value="${esc(c)}">••${esc(c)}</option>`).join('');
      sel.value = S.card;
    }
    renderBudget();
    renderTable();
    renderActionBar();
    if (AW.renderRecent) AW.renderRecent();
    $('aw-btitle').textContent = d.budget && d.budget.name ? `Working budget · ${d.budget.name}` : 'Working budget';
  };

  function sourceChip(t) {
    const s = (t.source || '');
    if (s === 'qbo_sync' || s === 'reconciled') return '<span class="aw-src">QBO</span>';
    if (s.includes('csv')) return '<span class="aw-src">CSV</span>';
    if (s === 'receipt' || s === 'doc_upload') return '<span class="aw-src receipt">Receipt</span>';
    return '<span class="aw-src manual">Manual</span>';
  }
  function cardCell(t) {
    if (t.card_last4) return '••' + esc(t.card_last4);
    const pm = t.payment_method;
    if (pm) return ({ cash: 'Cash', personal_card: 'Personal', check: 'Check', ach: 'ACH', company_card: 'Card', other: 'Other' })[pm] || esc(pm);
    if ((t.qbo_txn_type || '') === 'BillPayment') return 'ACH';
    return '';
  }
  function flagChip(t) {
    const red = t.flags.red || [], yel = t.flags.yellow || [];
    if (red.includes('no_line')) return '<span class="aw-flag red">No line</span>';
    if (red.includes('split_mismatch')) return '<span class="aw-flag red">Split does not sum</span>';
    if (red.includes('needs_backup')) return '<span class="aw-flag red">Needs backup</span>';
    if (yel.includes('possible_duplicate')) return '<span class="aw-flag yellow">Possible duplicate</span>';
    if (yel.includes('amount_mismatch')) return '<span class="aw-flag yellow">Receipt amount differs</span>';
    if (yel.includes('date_far')) return '<span class="aw-flag yellow">Receipt date far off</span>';
    if (yel.includes('paid_by_other')) return `<span class="aw-flag yellow"><span class="ell">Paid by ${esc(t.payee || 'other')}</span></span>`;
    if (AW.isAssigned(t) && t.has_backup) {
      if (t.receipt) return '<span class="aw-flag green">Receipt</span>';
      if (t.no_charge_reason) return `<span class="aw-flag yellow" title="${esc(t.no_charge_reason)}"><span class="ell">No receipt · reason</span></span>`;
      return '<span class="aw-flag green">Backed up</span>';
    }
    return '<span class="aw-flag none">—</span>';
  }
  function lineCell(t, view) {
    if (t.splits && t.splits.length) {
      const n = t.splits.length;
      return `<span class="aw-chip solid" title="${esc(t.splits.map(s => (s.label || '') + ' ' + money(s.amount)).join('\n'))}"><span>Split · ${n} lines</span></span>`;
    }
    if (t.itemized && t.itemized.length) return `<span class="aw-chip solid"><span>Itemized · ${t.itemized.length} lines</span></span>`;
    if (t.assigned_line) return `<span class="aw-chip solid" title="${esc(t.assigned_line.label)}"><span>${esc(t.assigned_line.label)}</span></span>`;
    if (t.section_only) return `<span class="aw-chip solid" title="Coded to the section, no line yet"><span>${esc(t.section_only.label)} · section</span></span>`;
    if (t.receipt_suggestion && !t.suggested_line) {
      return `<span class="aw-chip info" title="A receipt looks like this charge"><span>Receipt suggested · ${Math.round((t.receipt_suggestion.confidence || 0) * 100)}%</span></span>`;
    }
    if (t.suggested_line) {
      const conf = Math.round((t.suggested_line.confidence || 0) * 100);
      const target = t.suggested_line.id ? `data-suggest-line="${t.suggested_line.id}"` : `data-suggest-section="${esc(t.suggested_line.section)}"`;
      return `<button type="button" class="aw-chip ghost" ${target} data-tid="${t.id}" title="${esc(t.suggested_line.reason || 'Accept this suggestion')}"><span>${esc(t.suggested_line.label)}</span>${conf ? `<span class="conf">${conf}%</span>` : ''}</button>`;
    }
    return '<span class="muted" style="font-size:12px">No suggestion</span>';
  }
  function txnRow(t, idx, view) {
    const selected = S.sel.has(t.id);
    const amtCls = Number(t.amount) < 0 ? 'amt credit' : 'amt';
    const memo = t.memo && t.memo !== t.vendor ? `<span class="sub ell">${esc(t.memo)}</span>` : '';
    const container = AW.isContainer(t);
    return `<div role="row" class="aw-row" draggable="true" tabindex="0" data-tid="${t.id}" data-idx="${idx}" aria-selected="${selected}"
         ${container ? 'data-container="1"' : ''} ${t.receipt ? 'data-has-doc="1"' : ''}>
      <div class="aw-cell"><input type="checkbox" ${selected ? 'checked' : ''} aria-label="Select ${esc(t.vendor)}" data-sel="${t.id}"></div>
      <div class="aw-cell num muted">${esc(shortDate(t.date))}</div>
      <div class="aw-cell"><span class="vendor ell">${esc(t.vendor)}</span>${memo}</div>
      <div class="aw-cell num right ${amtCls}">${money(t.amount)}</div>
      <div class="aw-cell num muted">${cardCell(t)}</div>
      <div class="aw-cell">${sourceChip(t)}</div>
      <div class="aw-cell">${lineCell(t, view)}</div>
      <div class="aw-cell flagcell">${flagChip(t)}<span class="aw-rowacts">
        <button type="button" class="aw-btn sm" data-act="assign-one" data-tid="${t.id}" ${container ? 'disabled title="Split or itemized rows carry their coding on the sublines"' : ''}>${AW.isAssigned(t) ? 'Move' : 'Assign'}</button>
        <button type="button" class="aw-btn sm icon" data-act="menu" data-tid="${t.id}" aria-label="More actions for ${esc(t.vendor)}" aria-haspopup="menu">
          <svg aria-hidden="true" width="12" height="12" viewBox="0 0 12 12" fill="currentColor"><circle cx="2" cy="6" r="1.3"/><circle cx="6" cy="6" r="1.3"/><circle cx="10" cy="6" r="1.3"/></svg>
        </button></span></div>
    </div>`;
  }
  const HEAD = (lineHead) => `<div role="row" class="aw-hrow">
      <div class="aw-cell"><input type="checkbox" id="aw-selall" aria-label="Select all"></div>
      <div class="aw-cell">Date</div><div class="aw-cell">Vendor</div><div class="aw-cell right">Amount</div>
      <div class="aw-cell">Card</div><div class="aw-cell">Source</div><div class="aw-cell">${lineHead}</div>
      <div class="aw-cell">Flag</div></div>`;

  function renderTable() {
    const d = S.data, host = $('aw-table'), hint = $('aw-hint');
    host.className = 'aw-table ' + S.view;
    S.rows = [];
    hint.innerHTML = '';
    const summary = $('aw-summary');
    $('aw-filters').hidden = (S.view === 'sync');
    if (S.view === 'sync') { host.setAttribute('aria-label', 'Sync'); host.innerHTML = renderSync(); summary.textContent = ''; return; }
    let rows = AW.viewRows(S.view).filter(matchesFilters);
    if (S.view === 'receipts') {
      host.setAttribute('aria-label', 'Receipts inbox');
      summary.textContent = `${rows.length} receipt${rows.length === 1 ? '' : 's'} waiting`;
      host.innerHTML = renderReceipts(rows);
      return;
    }
    if (S.view === 'not_project') {
      host.setAttribute('aria-label', 'Not project expenses');
      summary.textContent = `${rows.length} excluded · ${money(rows.reduce((a, t) => a + Number(t.amount || 0), 0))}`;
      host.innerHTML = renderNotProject(rows);
      return;
    }
    host.setAttribute('aria-label', ({ unclaimed: 'Unclaimed transactions', assigned: 'Assigned transactions', needs_backup: 'Transactions that need backup' })[S.view]);
    summary.textContent = `${rows.length} charge${rows.length === 1 ? '' : 's'} · ${money(rows.reduce((a, t) => a + Number(t.amount || 0), 0))}`;
    if (!rows.length) {
      host.innerHTML = `<p class="aw-empty">${emptyText()}</p>`;
      return;
    }
    let html = HEAD(S.view === 'unclaimed' ? 'Suggested line' : 'Line');
    if (S.view === 'assigned') {
      // Grouped by section, then by line.
      const groups = new Map();
      rows.forEach(t => {
        const sec = AW.sectionCodeOf(t);
        const lineKey = (t.splits && t.splits.length) ? 'split' : (t.itemized && t.itemized.length) ? 'itemized'
          : t.assigned_line ? ('L' + t.assigned_line.id) : 'section';
        const lineLabel = (t.splits && t.splits.length) ? 'Split across lines' : (t.itemized && t.itemized.length) ? 'Itemized documents'
          : t.assigned_line ? t.assigned_line.label : 'Section only · no line yet';
        if (!groups.has(sec)) groups.set(sec, new Map());
        const g = groups.get(sec);
        if (!g.has(lineKey)) g.set(lineKey, { label: lineLabel, rows: [] });
        g.get(lineKey).rows.push(t);
      });
      const order = Array.from(groups.keys()).sort((a, b) => (a == null) - (b == null) || (a - b));
      order.forEach(sec => {
        const g = groups.get(sec);
        const all = Array.from(g.values()).flatMap(x => x.rows);
        html += `<div class="aw-grp" role="row"><span class="code num">${sec != null ? sec : ''}</span><span>${esc(sec != null ? AW.sectionName(sec) : 'Other')}</span><span class="tot num">${all.length} · ${money(all.reduce((a, t) => a + Number(t.amount || 0), 0))}</span></div>`;
        Array.from(g.values()).sort((a, b) => a.label.localeCompare(b.label)).forEach(line => {
          html += `<div class="aw-grp line" role="row"><span class="ell">${esc(line.label)}</span><span class="tot num">${line.rows.length} · ${money(line.rows.reduce((a, t) => a + Number(t.amount || 0), 0))}</span></div>`;
          line.rows.forEach(t => { html += txnRow(t, S.rows.length, S.view); S.rows.push(t); });
        });
      });
    } else {
      rows.forEach(t => { html += txnRow(t, S.rows.length, S.view); S.rows.push(t); });
    }
    host.innerHTML = html;
    const all = $('aw-selall');
    if (all) { all.checked = S.rows.length > 0 && S.rows.every(t => S.sel.has(t.id)); all.indeterminate = !all.checked && S.rows.some(t => S.sel.has(t.id)); }
    hint.innerHTML = S.view === 'unclaimed'
      ? 'Drag rows onto a budget line, or select and press <kbd>A</kbd> to assign · <kbd>N</kbd> marks not project · <kbd>Esc</kbd> clears'
      : S.view === 'assigned' ? 'Drag a row onto another line to move it · the row menu has Unassign, Split, Attach and Add reason'
      : 'Attach a receipt, link one from the inbox, or give a reason from the row menu';
    if (S.lineFilter && S.view === 'assigned') {
      const l = S.lineById.get(S.lineFilter);
      hint.innerHTML = `<span class="aw-linefilter">Showing ${esc(l ? l.label : 'line')} <button type="button" data-act="clear-linefilter" aria-label="Show every line"><svg aria-hidden="true" width="10" height="10" viewBox="0 0 14 14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M2 2l10 10M12 2L2 12"/></svg></button></span>`;
    }
  }
  function emptyText() {
    if (S.q || S.card || S.from || S.to) return 'Nothing matches these filters.';
    return ({
      unclaimed: 'Every charge has a line. Sync again when QuickBooks has more.',
      assigned: S.lineFilter ? 'Nothing is assigned to this line yet.' : 'Nothing assigned yet. Drag a charge from Unclaimed onto a line.',
      needs_backup: 'Every assigned charge has a receipt or a reason.',
      receipts: 'No receipts waiting. Uploads land here until they are matched.',
      not_project: 'Nothing has been excluded. Mark a charge Not project and it waits here.',
    })[S.view] || 'Nothing here.';
  }

  function renderReceipts(rows) {
    if (!rows.length) return `<p class="aw-empty">${emptyText()}</p>`;
    const selOne = S.sel.size === 1 ? S.byId.get(Array.from(S.sel)[0]) : null;
    let html = `<div role="row" class="aw-hrow"><div class="aw-cell"></div><div class="aw-cell">Date</div><div class="aw-cell">Receipt</div>
      <div class="aw-cell right">Amount</div><div class="aw-cell">Status</div><div class="aw-cell">Actions</div></div>`;
    rows.forEach(r => {
      const status = [];
      if (r.conflict) status.push(`<span class="aw-chip solid" title="Claimed by another project"><span>Claimed by ${esc(r.conflict.claimed_by)}</span></span>`);
      if (r.suggested_charge_id) {
        const c = S.byId.get(r.suggested_charge_id);
        status.push(`<button type="button" class="aw-chip info" data-act="goto" data-tid="${r.suggested_charge_id}" title="Open the charge this receipt looks like"><span>Suggested for ${esc(c ? c.vendor : 'a charge')}${c ? ' · ' + money(c.amount) : ''}</span></button>`);
      }
      if (r.dup_pending) status.push('<span class="aw-flag yellow">Possible duplicate</span>');
      if (!status.length) status.push('<span class="aw-flag none">Waiting</span>');
      const linkTitle = selOne ? `Link to ${selOne.vendor} ${money(selOne.amount)}` : 'Select one charge in Unclaimed or Assigned first';
      html += `<div role="row" class="aw-row" data-doc="${r.doc_id}" tabindex="0">
        <div class="aw-cell" style="padding-right:0"><a href="/docs/upload/${r.doc_id}/raw" target="_blank" rel="noopener" aria-label="Open ${esc(r.name)}"><img class="aw-thumb" src="${esc(r.thumb)}" alt="" loading="lazy" onerror="this.style.visibility='hidden'"></a></div>
        <div class="aw-cell num muted">${esc(shortDate(r.date))}</div>
        <div class="aw-cell" style="flex-direction:column;align-items:flex-start;justify-content:center;gap:1px">
          <span class="vendor ell" style="max-width:100%">${esc(r.vendor || r.name)}</span>
          <a class="ell muted" style="font-size:11px;max-width:100%" href="/docs/upload/${r.doc_id}/raw" target="_blank" rel="noopener">${esc(r.name)}</a>
        </div>
        <div class="aw-cell num right amt">${r.amount != null ? money(r.amount) : '—'}</div>
        <div class="aw-cell">${status.join(' ')}</div>
        <div class="aw-cell" style="gap:6px">
          <button type="button" class="aw-btn sm" data-act="link-selected" data-doc="${r.doc_id}" ${selOne ? '' : 'disabled'} title="${esc(linkTitle)}">Link to selected</button>
          <button type="button" class="aw-btn sm primary" data-act="create-expense" data-doc="${r.doc_id}">Create expense</button>
          <button type="button" class="aw-btn sm danger" data-act="receipt-not-project" data-doc="${r.doc_id}" data-txn="${r.txn_id || ''}">Not project</button>
        </div>
      </div>`;
    });
    return html;
  }

  function renderNotProject(rows) {
    const d = S.data;
    let html = '';
    if (!rows.length) html += `<p class="aw-empty">${emptyText()}</p>`;
    else {
      html += `<div role="row" class="aw-hrow"><div class="aw-cell"></div><div class="aw-cell">Date</div><div class="aw-cell">Vendor</div>
        <div class="aw-cell right">Amount</div><div class="aw-cell">Card</div><div class="aw-cell">Source</div><div class="aw-cell">Excluded</div><div class="aw-cell"></div></div>`;
      rows.forEach(t => {
        const x = t.excluded || {};
        const who = `by ${esc(x.by || 'someone')}${x.at ? ' · ' + esc(shortDate(x.at.slice(0, 10))) : ''}`;
        html += `<div role="row" class="aw-row excluded" data-tid="${t.id}" tabindex="0">
          <div class="aw-cell"></div>
          <div class="aw-cell num muted">${esc(shortDate(t.date))}</div>
          <div class="aw-cell"><span class="vendor ell">${esc(t.vendor)}</span></div>
          <div class="aw-cell num right amt">${money(t.amount)}</div>
          <div class="aw-cell num muted">${cardCell(t)}</div>
          <div class="aw-cell">${sourceChip(t)}</div>
          <div class="aw-cell" style="font-size:12px"><span class="ell" title="${esc(x.reason || '')}">${who}${x.reason ? ' · ' + esc(x.reason) : ''}</span></div>
          <div class="aw-cell right"><button type="button" class="aw-btn sm" data-act="recover" data-tid="${t.id}">Recover</button></div>
        </div>`;
      });
    }
    const ce = d.claimed_elsewhere || [];
    if (ce.length) {
      html += `<div class="aw-grp" role="row" style="position:static;margin-top:12px"><span>Claimed by other projects</span><span class="tot num">${ce.length}</span></div>`;
      ce.forEach(t => {
        html += `<div role="row" class="aw-row excluded" tabindex="0"><div class="aw-cell"></div>
          <div class="aw-cell num muted">${esc(shortDate(t.date))}</div>
          <div class="aw-cell"><span class="vendor ell" style="text-decoration:none">${esc(t.vendor)}</span></div>
          <div class="aw-cell num right amt">${money(t.amount)}</div><div class="aw-cell"></div><div class="aw-cell"></div>
          <div class="aw-cell" style="font-size:12px"><span class="aw-chip solid"><span>${esc(t.claimed_by)}</span></span></div><div class="aw-cell"></div></div>`;
      });
    }
    return html;
  }

  function renderSync() {
    const d = S.data, sy = d.sync || {}, c = d.counts || {};
    const connected = !!sy.connected;
    return `<div class="aw-sync">
      <div class="aw-card">
        <h3>QuickBooks</h3>
        <dl class="aw-kv">
          <dt>Status</dt><dd>${connected ? '<span class="aw-flag green">Connected</span>' : `<span class="aw-flag yellow">Not connected</span> <a href="${esc(AJ.urls.oauth)}">Connect</a>`}</dd>
          <dt>Accounts</dt><dd class="num">${sy.accounts || 0} selected</dd>
          <dt>Last sync</dt><dd class="num">${sy.last_synced ? esc(sy.last_synced.replace('T', ' ')) + ' (' + whenText(sy.last_synced) + ')' : 'never'}</dd>
          <dt>Change capture</dt><dd class="num">${sy.last_cdc_sync ? esc(sy.last_cdc_sync.replace('T', ' ')) : '—'}</dd>
          <dt>Synced through</dt><dd class="num">${esc(sy.sync_through || '—')}</dd>
          <dt>Automatic</dt><dd>${esc(sy.auto || 'twice a day')} · never past yesterday, 14-day overlap</dd>
        </dl>
        <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
          <label for="aw-sync-from" class="vis-hidden">Sync from</label><input id="aw-sync-from" type="date" title="Sync from (optional)">
          <label for="aw-sync-to" class="vis-hidden">Sync to</label><input id="aw-sync-to" type="date" title="Sync to (optional)">
          <button type="button" class="aw-btn primary" data-act="sync-now" ${connected && sy.accounts ? '' : 'disabled'}>Sync now</button>
        </div>
        <span class="help" style="font-size:12px;color:var(--text-muted)">Only charges accepted into the books reach the API; items still in QuickBooks' For Review tab arrive once accepted.</span>
      </div>
      <div class="aw-card">
        <h3>This project</h3>
        <dl class="aw-kv">
          <dt>Unclaimed</dt><dd class="num">${c.unclaimed || 0}</dd>
          <dt>Assigned</dt><dd class="num">${c.assigned || 0}</dd>
          <dt>Needs backup</dt><dd class="num">${c.needs_backup || 0}</dd>
          <dt>Receipts waiting</dt><dd class="num">${c.receipts || 0}</dd>
          <dt>Not project</dt><dd class="num">${c.not_project || 0}</dd>
          <dt>Claimed elsewhere</dt><dd class="num">${c.claimed_elsewhere || 0}</dd>
        </dl>
        <span class="help" style="font-size:12px;color:var(--text-muted)">Uploads: <a href="${esc(AJ.urls.docs)}">Documents</a> · <a href="${esc(AJ.urls.upload)}">mobile upload</a></span>
      </div>
      <div class="aw-card" style="grid-column:1 / -1">
        <h3>Accounts feeding this project</h3>
        <span class="help" style="font-size:12px;color:var(--text-muted)">Only transactions from the checked bank and credit-card accounts are pulled in on Sync.</span>
        <div id="aw-accts" style="display:flex;flex-direction:column;gap:6px"><span class="muted" style="font-size:13px">${connected ? 'Loading accounts' : 'Connect QuickBooks to pick accounts.'}</span></div>
        <div style="display:flex;gap:8px;justify-content:flex-end;align-items:center">
          <span id="aw-accts-status" class="muted" style="font-size:12px;margin-right:auto"></span>
          <button type="button" class="aw-btn" data-act="accts-load" ${connected ? '' : 'disabled'}>Refresh</button>
          <button type="button" class="aw-btn primary" data-act="accts-save" ${connected ? '' : 'disabled'}>Save accounts</button>
        </div>
      </div>
    </div>`;
  }

  function renderBudget() {
    const d = S.data, host = $('aw-budget');
    const secs = ((d.budget || {}).sections) || [];
    if (!secs.length) { host.innerHTML = '<p class="aw-empty">This project has no current budget yet. Build one on the Budget page to get drop targets.</p>'; $('aw-grand').innerHTML = '<span>0</span><span>0</span><span>0</span>'; return; }
    const varCell = v => `<span class="var ${v < 0 ? 'neg' : v > 0 ? 'pos' : ''}">${whole(v)}</span>`;
    let html = '';
    let ge = 0, ga = 0;
    secs.forEach(sec => {
      ge += sec.estimate || 0; ga += sec.actual || 0;
      const target = sec.is_drop_target ? `data-target="section" data-code="${sec.code}" data-name="${esc(sec.name)}"` : '';
      html += `<div class="aw-sec" data-sec="${sec.code}">
        <div class="aw-sec-head" ${target} ${sec.is_drop_target ? 'tabindex="0" role="button" aria-label="Section ' + esc(sec.code + ' ' + sec.name) + ': assign the selection to this section"' : ''}>
          <span class="code num">${sec.code != null ? sec.code : ''}</span><span class="ell">${esc(sec.name)}</span><span class="spacer"></span>
          <div class="aw-nums num"><span>${whole(sec.estimate)}</span><span>${whole(sec.actual)}</span>${varCell(sec.variance)}</div>
        </div>`;
      const lines = (sec.lines || []).slice();
      // Children ride under their parent. Phase 4 (Steven, Oct 30): the
      // per-department Payroll Fringes lines live in 6500 Administrative
      // and close that card, followed by the auto-calc lines (Workers'
      // Comp, Production Insurance, Payroll Service Fee, Production
      // Company Fee); all of them are coding targets. A labor section
      // has no fringe row here — its fringe summary is on the budget page.
      const computed = l => l.line_tag === 'fringe_rollup' || l.line_tag === 'auto_calc';
      const byParent = new Map();
      lines.forEach(l => { if (l.parent_line_id) { if (!byParent.has(l.parent_line_id)) byParent.set(l.parent_line_id, []); byParent.get(l.parent_line_id).push(l); } });
      const ordered = [];
      const seen = new Set();
      lines.forEach(l => {
        if (seen.has(l.id) || l.parent_line_id || computed(l)) return;
        ordered.push(l); seen.add(l.id);
        (byParent.get(l.id) || []).forEach(k => { ordered.push(k); seen.add(k.id); });
      });
      lines.forEach(l => { if (!seen.has(l.id) && !computed(l)) { ordered.push(l); seen.add(l.id); } });
      lines.forEach(l => { if (l.line_tag === 'fringe_rollup') ordered.push(l); });
      lines.forEach(l => { if (l.line_tag === 'auto_calc') ordered.push(l); });
      ordered.forEach(l => {
        const fringe = l.line_tag === 'fringe_rollup';
        const off = !l.is_drop_target;
        const cls = 'aw-line' + (l.parent_line_id ? ' child' : '') + (fringe ? ' fringe' : '') + (off ? ' off' : '') + (S.lineFilter === l.id ? ' sel' : '');
        const target = !off ? `data-target="line" data-line-id="${l.id}" data-code="${l.account_code}" data-label="${esc(l.label)}" tabindex="0" role="button" aria-label="${esc(l.label)}: assign the selection here, or click to list its transactions"` : '';
        const name = esc(l.description || l.label);
        const auto = (l.line_tag === 'auto_calc' || (l.line_tag || '').startsWith('auto_')) ? '<span class="auto">auto</span>' : '';
        html += `<div class="${cls}" ${target} data-line="${l.id}">
          <span class="name">${name}${auto}</span>
          <div class="aw-nums num">${off ? '<span>—</span><span>—</span><span>—</span>' : `<span>${whole(l.estimate)}</span><span>${whole(l.actual)}</span>${varCell(l.variance)}`}</div>
        </div>`;
      });
      html += '</div>';
    });
    host.innerHTML = html;
    $('aw-grand').innerHTML = `<span>${whole(ge)}</span><span>${whole(ga)}</span>${varCell(ge - ga)}`;
  }

  // ── Selection ────────────────────────────────────────────────────────
  AW.selectedTxns = () => Array.from(S.sel).map(id => S.byId.get(id)).filter(Boolean);
  AW.selTotal = () => AW.selectedTxns().reduce((a, t) => a + Number(t.amount || 0), 0);
  function renderActionBar() {
    const bar = $('aw-actionbar');
    const n = S.sel.size;
    if (!n) { bar.hidden = true; return; }
    bar.hidden = false;
    const ts = AW.selectedTxns();
    const tot = AW.selTotal();
    const one = ts.length === 1 ? ts[0] : null;
    $('aw-selinfo').textContent = `${n} selected · ${money(tot)}` + (one && S.view === 'receipts' ? ` · ${one.vendor}` : '');
    const inReceipts = S.view === 'receipts';
    bar.querySelector('[data-act="split"]').disabled = n !== 1 || (one && AW.isContainer(one) && !(one.splits && one.splits.length));
    bar.querySelector('[data-act="attach"]').disabled = n !== 1 || (one && !!one.receipt);
    bar.querySelector('[data-act="assign"]').disabled = inReceipts || ts.some(AW.isContainer);
    bar.querySelector('[data-act="not-project"]').disabled = inReceipts;
    document.querySelectorAll('#aw-table .aw-row[data-tid]').forEach(r => r.setAttribute('aria-selected', S.sel.has(+r.dataset.tid) ? 'true' : 'false'));
    document.querySelectorAll('#aw-table input[data-sel]').forEach(cb => { cb.checked = S.sel.has(+cb.dataset.sel); });
    const all = $('aw-selall');
    if (all) { all.checked = S.rows.length > 0 && S.rows.every(t => S.sel.has(t.id)); all.indeterminate = !all.checked && S.rows.some(t => S.sel.has(t.id)); }
    if (inReceipts) renderTable();
  }
  AW.setSelection = ids => { S.sel = new Set(ids); renderActionBar(); if (S.view === 'receipts') renderTable(); };
  AW.clearSelection = () => { S.sel.clear(); S.lastIdx = null; renderActionBar(); if (S.view === 'receipts') renderTable(); };
  function clickRow(row, e) {
    const tid = +row.dataset.tid, idx = +row.dataset.idx;
    if (e.shiftKey && S.lastIdx != null && S.rows.length) {
      const [a, b] = [Math.min(S.lastIdx, idx), Math.max(S.lastIdx, idx)];
      for (let i = a; i <= b; i++) S.sel.add(S.rows[i].id);
    } else if (e.metaKey || e.ctrlKey || e.target.closest('input[type="checkbox"]')) {
      if (S.sel.has(tid)) S.sel.delete(tid); else S.sel.add(tid);
      S.lastIdx = idx;
    } else {
      if (S.sel.size === 1 && S.sel.has(tid)) S.sel.clear();
      else { S.sel = new Set([tid]); }
      S.lastIdx = idx;
    }
    renderActionBar();
  }

  AW.focusTransaction = function (tid) {
    const t = S.byId.get(tid);
    if (!t) { AW.toast('That transaction is not in this project\'s pool.', 'yellow'); return; }
    const d = S.data;
    const view = (d.not_project || []).some(x => x.id === tid) ? 'not_project' : (AW.isAssigned(t) ? 'assigned' : 'unclaimed');
    S.q = ''; S.card = ''; S.from = ''; S.to = ''; S.lineFilter = null;
    $('aw-q').value = ''; $('aw-card').value = ''; $('aw-from').value = ''; $('aw-to').value = '';
    if (view !== 'not_project') S.sel = new Set([tid]);
    AW.setView(view);
    const row = document.querySelector(`#aw-table .aw-row[data-tid="${tid}"]`);
    if (row) { row.scrollIntoView({ block: 'center' }); row.classList.add('focus-flash'); row.focus({ preventScroll: true }); }
  };

  AW.setView = function (v) {
    if (!VIEWS.includes(v)) return;
    S.view = v;
    if (v !== 'assigned') S.lineFilter = null;
    S.lastIdx = null;
    AW.render();
    if (v === 'sync' && S.data && (S.data.sync || {}).connected && AW.actions && AW.actions['accts-load']) AW.actions['accts-load']();
    try { history.replaceState(null, '', location.pathname + (v === 'unclaimed' ? '' : '?view=' + v)); } catch (e) {}
  };

  // ── Events ───────────────────────────────────────────────────────────
  $('aw-views').addEventListener('click', e => {
    const b = e.target.closest('.aw-view'); if (!b) return;
    AW.setView(b.dataset.view);
  });
  let qTimer = null;
  $('aw-q').addEventListener('input', e => { clearTimeout(qTimer); qTimer = setTimeout(() => { S.q = e.target.value.trim(); renderTable(); }, 120); });
  $('aw-card').addEventListener('change', e => { S.card = e.target.value; renderTable(); });
  $('aw-from').addEventListener('change', e => { S.from = e.target.value; renderTable(); });
  $('aw-to').addEventListener('change', e => { S.to = e.target.value; renderTable(); });

  $('aw-table').addEventListener('click', e => {
    const act = e.target.closest('[data-act]');
    if (act) {
      if (act.dataset.act === 'clear-linefilter') { S.lineFilter = null; AW.render(); return; }
      if (AW.actions && AW.actions[act.dataset.act]) { e.preventDefault(); AW.actions[act.dataset.act](act, e); }
      return;
    }
    if (e.target.id === 'aw-selall') {
      if (e.target.checked) S.rows.forEach(t => S.sel.add(t.id)); else S.rows.forEach(t => S.sel.delete(t.id));
      renderActionBar(); return;
    }
    const ghost = e.target.closest('.aw-chip.ghost[data-tid]');
    if (ghost) { if (AW.actions && AW.actions['accept-suggestion']) AW.actions['accept-suggestion'](ghost, e); return; }
    if (e.target.closest('a')) return;
    const row = e.target.closest('.aw-row[data-tid][data-idx]');
    if (row) clickRow(row, e);
  });
  $('aw-table').addEventListener('keydown', e => {
    const row = e.target.closest && e.target.closest('.aw-row[data-tid][data-idx]');
    if (!row || e.target !== row) return;
    if (e.key === ' ' || e.key === 'Enter') { e.preventDefault(); clickRow(row, e); }
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      e.preventDefault();
      const idx = +row.dataset.idx + (e.key === 'ArrowDown' ? 1 : -1);
      const next = document.querySelector(`#aw-table .aw-row[data-idx="${idx}"]`);
      if (next) { next.focus(); if (e.shiftKey) { S.sel.add(+next.dataset.tid); S.lastIdx = idx; renderActionBar(); } }
    }
  });
  $('aw-actionbar').addEventListener('click', e => {
    const b = e.target.closest('[data-act]'); if (!b) return;
    if (b.dataset.act === 'clear') { AW.clearSelection(); return; }
    if (AW.actions && AW.actions[b.dataset.act]) AW.actions[b.dataset.act](b, e);
  });
  document.getElementById('aw').addEventListener('click', e => {
    const b = e.target.closest('.aw-head [data-act]'); if (!b) return;
    if (AW.actions && AW.actions[b.dataset.act]) AW.actions[b.dataset.act](b, e);
  });
  // Budget pane: click a line → list its transactions; Enter/Space on a
  // target with a selection → assign (the keyboard path for every drop).
  $('aw-budget').addEventListener('click', e => {
    const line = e.target.closest('.aw-line[data-target="line"]');
    if (line) {
      const lid = +line.dataset.lineId;
      if (S.sel.size && S.view !== 'receipts' && S.view !== 'not_project') {
        if (AW.actions && AW.actions['assign-to']) AW.actions['assign-to']({ lineId: lid, code: line.dataset.code, label: line.dataset.label });
        return;
      }
      S.lineFilter = (S.lineFilter === lid) ? null : lid;
      S.view = 'assigned'; AW.render();
      return;
    }
    const head = e.target.closest('.aw-sec-head[data-target="section"]');
    if (head && S.sel.size && S.view !== 'receipts' && S.view !== 'not_project') {
      if (AW.actions && AW.actions['assign-to']) AW.actions['assign-to']({ code: head.dataset.code, name: head.dataset.name });
    }
  });
  $('aw-budget').addEventListener('keydown', e => {
    if (e.key !== 'Enter' && e.key !== ' ') return;
    const t = e.target.closest('[data-target]'); if (!t || e.target !== t) return;
    e.preventDefault(); t.click();
  });

  // Global keys: A assign, N not project, Esc clear — never inside an
  // input or an open dialog.
  document.addEventListener('keydown', e => {
    if (document.querySelector('dialog[open]')) return;
    const tag = (e.target.tagName || '').toLowerCase();
    if (tag === 'input' || tag === 'textarea' || tag === 'select' || e.target.isContentEditable) return;
    if (e.metaKey || e.ctrlKey || e.altKey) return;
    if (e.key === 'Escape') { if (S.sel.size) { AW.clearSelection(); e.preventDefault(); } if (AW.closeMenu) AW.closeMenu(); return; }
    if (!S.sel.size || S.view === 'receipts' || S.view === 'not_project') return;
    if (e.key === 'a' || e.key === 'A') { e.preventDefault(); if (AW.actions) AW.actions.assign(); }
    if (e.key === 'n' || e.key === 'N') { e.preventDefault(); if (AW.actions) AW.actions['not-project'](); }
  });

  // ── Toasts (with undo through ActivityLog) ───────────────────────────
  AW.toast = function (msg, kind, opts) {
    opts = opts || {};
    const host = $('aw-toasts');
    const el = document.createElement('div');
    el.className = 'aw-toast' + (kind ? ' ' + kind : '');
    el.setAttribute('role', 'status');
    el.innerHTML = `<span>${esc(msg)}</span>`;
    const ids = (opts.activityIds || []).filter(Boolean);
    if (opts.activityId) ids.unshift(opts.activityId);
    if (ids.length && AW.BID) {
      const u = document.createElement('button');
      u.type = 'button'; u.className = 'aw-btn'; u.textContent = 'Undo';
      u.addEventListener('click', async () => {
        u.disabled = true; u.textContent = 'Undoing';
        try {
          if (ids.length === 1) {
            const r = await AW.post(`/projects/${PID}/budget/${AW.BID}/activity/${ids[0]}/undo`, {});
            if (r.ok === false) throw new Error(r.message || r.error || 'Could not undo');
          } else {
            const r = await AW.post(`/projects/${PID}/budget/${AW.BID}/activity/undo-batch`, { ids });
            if (r.failed && r.failed.length && !r.undone) throw new Error('Could not undo: ' + (r.failed[0].error || ''));
          }
          el.remove();
          AW.toast('Undone.', '');
          await AW.load();
        } catch (err) { u.disabled = false; u.textContent = 'Undo'; AW.toast(err.message || 'Could not undo', 'red'); }
      });
      el.appendChild(u);
    }
    const x = document.createElement('button');
    x.type = 'button'; x.className = 'aw-btn icon'; x.setAttribute('aria-label', 'Dismiss');
    x.innerHTML = '<svg aria-hidden="true" width="12" height="12" viewBox="0 0 14 14" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"><path d="M2 2l10 10M12 2L2 12"/></svg>';
    x.addEventListener('click', () => el.remove());
    el.appendChild(x);
    host.appendChild(el);
    const ttl = opts.ttl || (ids.length ? 9000 : (kind === 'red' ? 8000 : 4500));
    setTimeout(() => { if (el.isConnected) el.remove(); }, ttl);
    return el;
  };

  // ── Boot ─────────────────────────────────────────────────────────────
  AW.load();
})();
