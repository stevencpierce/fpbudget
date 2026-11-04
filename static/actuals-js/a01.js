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
  // Shared rules (static/fp-utils.js, loaded by base.html) under the local names.
  const esc = FP.esc;
  const money = v => FP.money(v || 0);
  const whole = FP.whole;
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
  const VIEWS = ['unclaimed', 'assigned', 'needs_backup', 'receipts', 'duplicates', 'not_project', 'sync'];
  const S = AW.state = {
    data: null, view: VIEWS.includes(AJ.initialView) ? AJ.initialView : 'unclaimed',
    q: '', card: '', from: '', to: '',
    sel: new Set(), lastIdx: null, rows: [],      // rows: txn rows in render order (for shift-click)
    lineFilter: null, loading: false, focusTxn: AJ.focusTxn || null,
    byId: new Map(), lineById: new Map(), sectionOfLine: new Map(),
    // Phase 5: the selected receipt (Receipts view), the charge ticked in
    // the candidate panel, and the charge Find backup is open on.
    selDoc: null, pickCharge: null, findFor: null, docById: new Map(),
    openSecCoded: new Set(),
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
      S.docById = new Map();
      (d.receipts || []).forEach(r => S.docById.set(r.doc_id, r));
      if (S.selDoc && !S.docById.has(S.selDoc)) S.selDoc = null;
      if (S.pickCharge && !S.byId.has(S.pickCharge)) S.pickCharge = null;
      if (S.findFor && !S.byId.has(S.findFor)) S.findFor = null;
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
    if (t.splits && t.splits.length) {
      const p = t.splits[0];
      if (!p.budget_line_id && p.account_code) return AW.sectionFor(+p.account_code);
      const sec = S.sectionOfLine.get(p.budget_line_id); if (sec) return sec.code;
    }
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
                   ((t.assigned_line || {}).label || '') + ' ' + (t.payee || '') + ' ' + (t.payee_name || '') + ' ' + (t.payee_via || '')).toLowerCase();
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
    if (view === 'duplicates') return (d.duplicates || []).flatMap(g => g.members.map(m => S.byId.get(m.id) || m));
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
    // Phase 5: the Matches-to-review chip (both directions are the same pairs).
    const rc = $('aw-reviewchip');
    if (rc) { rc.hidden = !(c.matches_to_review > 0); rc.querySelector('[data-count="matches_to_review"]').textContent = c.matches_to_review || 0; }
    // Sync status in the header.
    const sy = d.sync || {};
    $('aw-syncdot').classList.toggle('off', !sy.connected);
    // Connecting is company-wide — it lives on the Admin hub (AJ.urls.oauth).
    $('aw-synctext').innerHTML = sy.connected
      ? esc(`QuickBooks synced ${sy.last_synced ? whenText(sy.last_synced) : 'never'} · ${sy.accounts} account${sy.accounts === 1 ? '' : 's'}`)
      : `QuickBooks not connected · <a href="${esc(AJ.urls.oauth)}">Connect in Admin</a>`;
    $('aw-syncnow').disabled = !sy.connected || !sy.accounts || !AJ.canSync;
    // Company ledger (2026-11-03): charges this project claimed that another
    // project claimed too are flagged for review — one banner, admin page.
    const ban = $('aw-banner');
    if (ban) {
      const n = (c.claim_conflicts || 0);
      if (n && d.conflicts_url) {
        ban.innerHTML = `<span>${n} charge${n === 1 ? ' is' : 's are'} claimed by two projects</span> <a href="${esc(d.conflicts_url)}">Resolve</a>`;
        ban.hidden = false;
      } else { ban.hidden = true; ban.innerHTML = ''; }
    }
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
    if (AW.panelActive && AW.panelActive()) { if (AW.renderPanel) AW.renderPanel(); } else renderBudget();
    renderTable();
    renderActionBar();
    if (AW.renderRecent) AW.renderRecent();
    $('aw-btitle').textContent = d.budget && d.budget.name ? `Working budget · ${d.budget.name}` : 'Working budget';
  };

  function sourceChip(t) {
    const s = (t.source || '');
    if (s === 'qbo_sync' || s === 'reconciled') {
      if (t.claim_conflict) return '<span class="aw-src" style="color:var(--orange)" title="Claimed by two projects — see the banner">QBO · review</span>';
      return t.pooled ? '<span class="aw-src" title="In the company pool — unclaimed by any project">QBO · pool</span>' : '<span class="aw-src">QBO</span>';
    }
    if (s.includes('csv')) return '<span class="aw-src">CSV</span>';
    if (s === 'receipt' || s === 'doc_upload') return '<span class="aw-src receipt">Receipt</span>';
    return '<span class="aw-src manual">Manual</span>';
  }
  function cardCell(t) {
    if (t.card_last4) return '••' + esc(t.card_last4);
    const pm = t.payment_method;
    if (pm) return esc((window.__FP_PAYMENT_METHODS || {})[pm] || pm);   // the ONE label map (Cleanup D)
    if ((t.qbo_txn_type || '') === 'BillPayment') return 'ACH';
    return '';
  }
  function flagChip(t) {
    const red = t.flags.red || [], yel = t.flags.yellow || [];
    if (red.includes('voided_in_qbo')) return '<span class="aw-flag red">Voided in QuickBooks</span>';
    // 2026-11-04: the invoice is coded on its own AND as this charge.
    if (yel.includes('counted_twice')) return `<button type="button" class="aw-flag yellow linky" data-act="detail" data-tid="${t.id}" title="Its invoice is counted on its own as well — open to attach it to the charge">Counted twice</button>`;
    // A possible duplicate outranks the red flags here: the person should
    // settle whether it is one charge or two before coding it.
    if (yel.includes('possible_duplicate') && t.dup_group) return `<button type="button" class="aw-flag yellow linky" data-act="open-dup" data-key="${esc(t.dup_group)}" data-tid="${t.id}" title="Compare and settle this duplicate here (D)">Possible duplicate</button>`;
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
    const rs = t.receipt_suggestion;
    const rsTitle = rs ? esc(`${rs.vendor || rs.name} · ${rs.amount != null ? money(rs.amount) : ''} · ${(rs.reasons || []).join(' · ')}`) : '';
    if (rs && !t.suggested_line) {
      return `<span class="aw-match"><span class="aw-chip info${rs.strong ? ' strong' : ''}" title="${rsTitle}"><span>Receipt ${Math.round((rs.confidence || 0) * 100)}%</span></span>
        <button type="button" class="aw-btn sm${rs.strong ? ' primary' : ''}" data-act="confirm-match" data-tid="${t.id}" title="Approve the receipt match (M)">Approve</button>
        <button type="button" class="aw-btn sm" data-act="dismiss-match" data-tid="${t.id}" title="Not a match">No</button></span>`;
    }
    if (t.suggested_line) {
      const conf = Math.round((t.suggested_line.confidence || 0) * 100);
      const target = t.suggested_line.id ? `data-suggest-line="${t.suggested_line.id}"` : `data-suggest-section="${esc(t.suggested_line.section)}"`;
      return `<button type="button" class="aw-chip ghost" ${target} data-tid="${t.id}" title="${esc(t.suggested_line.reason || 'Accept this suggestion')}"><span>${esc(t.suggested_line.label)}</span>${conf ? `<span class="conf">${conf}%</span>` : ''}</button>`
        + (rs ? ` <span class="aw-chip info${rs.strong ? ' strong' : ''}" title="${rsTitle} — approve with M or in Review matches"><span>Receipt ${Math.round((rs.confidence || 0) * 100)}%</span></span>` : '');
    }
    return '<span class="muted" style="font-size:12px">No suggestion</span>';
  }
  function txnRow(t, idx, view) {
    const selected = S.sel.has(t.id);
    const amtCls = Number(t.amount) < 0 ? 'amt credit' : 'amt';
    // Loan-out (2026-11-04): the person behind the company, "Danielle Miller · via Two Foot Shuffle LLC".
    const via = t.payee_via && t.payee_name ? `<span class="sub ell" title="Paid to ${esc(t.payee_name)} through the loan-out ${esc(t.payee_via)}">${esc(t.payee_name)} · via ${esc(t.payee_via)}</span>` : '';
    const memo = via || (t.memo && t.memo !== t.vendor ? `<span class="sub ell">${esc(t.memo)}</span>` : '');
    const container = AW.isContainer(t);
    return `<div role="row" class="aw-row" draggable="true" tabindex="0" data-tid="${t.id}" data-idx="${idx}" aria-selected="${selected}"
         ${container ? 'data-container="1"' : ''} ${t.receipt ? 'data-has-doc="1"' : ''}>
      <div class="aw-cell"><input type="checkbox" ${selected ? 'checked' : ''} aria-label="Select ${esc(t.vendor)}" data-sel="${t.id}"></div>
      <div class="aw-cell num muted">${esc(shortDate(t.date))}</div>
      <div class="aw-cell"><span class="vendor ell" title="Double-click the row or press Enter for details">${esc(t.vendor)}</span>${memo}</div>
      <div class="aw-cell num right ${amtCls}">${money(t.amount)}</div>
      <div class="aw-cell num muted">${cardCell(t)}</div>
      <div class="aw-cell">${sourceChip(t)}</div>
      <div class="aw-cell">${lineCell(t, view)}</div>
      <div class="aw-cell flagcell">${flagChip(t)}<span class="aw-rowacts">
        ${t.dup_group
          // 2026-11-04: hovering a row hides its flag chip under these
          // actions, so a possible duplicate offers Compare right here —
          // settle one charge or two before coding it.
          ? `<button type="button" class="aw-btn sm warn" data-act="open-dup" data-key="${esc(t.dup_group)}" data-tid="${t.id}" title="Compare the possible duplicates side by side and settle them here (D)">Compare</button>`
          : view === 'needs_backup' && !t.receipt
          ? `<button type="button" class="aw-btn sm" data-act="find-backup" data-tid="${t.id}" title="Ranked receipts for this charge">Find backup</button>`
          : `<button type="button" class="aw-btn sm" data-act="assign-one" data-tid="${t.id}" ${container ? 'disabled title="Split or itemized rows carry their coding on the sublines"' : ''}>${AW.isAssigned(t) ? 'Move' : 'Assign'}</button>`}
        <button type="button" class="aw-btn sm" data-act="detail" data-tid="${t.id}" title="Open the details (Enter)">Details</button>
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
      hint.innerHTML = 'Click a receipt to see the charges it could back · tick a charge there, then Link to selected · <kbd>M</kbd> confirms a suggested match · drag a receipt onto a charge in the panel';
      return;
    }
    if (S.view === 'duplicates') {
      host.setAttribute('aria-label', 'Possible duplicate charges');
      const groups = (d.duplicates || []), voided = (d.voided || []);
      summary.textContent = `${groups.length} group${groups.length === 1 ? '' : 's'}${voided.length ? ` · ${voided.length} voided in QuickBooks` : ''}`;
      host.innerHTML = AW.renderDuplicates ? AW.renderDuplicates(groups, voided, matchesFilters) : '';
      hint.innerHTML = 'Each group is one event that arrived twice. Compare shows them side by side; Keep both remembers your answer; Delete the extra moves its coding to the keeper first · <kbd>D</kbd> compares the selected row\'s group';
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
      ? 'Drag rows onto a budget line, or select and press <kbd>A</kbd> to assign · <kbd>N</kbd> marks not project · <kbd>M</kbd> approves a suggested receipt · <kbd>Esc</kbd> clears'
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
      duplicates: 'No charge looks like a repeat of another. Groups appear when the same amount and vendor post twice within a few days.',
      not_project: 'Nothing has been excluded. Mark a charge Not project and it waits here.',
    })[S.view] || 'Nothing here.';
  }

  function renderReceipts(rows) {
    if (!rows.length) return `<p class="aw-empty">${emptyText()}</p>`;
    // "Link to selected" needs BOTH a receipt (this row selected) and a
    // charge: one ticked in the candidate panel, or one selected in
    // Unclaimed / Assigned before coming here. (Steven, Nov 3: "it says
    // Link to selected, but there's nowhere to select a charge".)
    const selCharge = S.pickCharge ? S.byId.get(S.pickCharge) : (S.sel.size === 1 ? S.byId.get(Array.from(S.sel)[0]) : null);
    let html = `<div role="row" class="aw-hrow"><div class="aw-cell"></div><div class="aw-cell">Date</div><div class="aw-cell">Receipt</div>
      <div class="aw-cell right">Amount</div><div class="aw-cell">Match</div><div class="aw-cell">Actions</div></div>`;
    rows.forEach(r => {
      const status = [];
      if (r.conflict) status.push(`<span class="aw-chip solid" title="Claimed by another project"><span>Claimed by ${esc(r.conflict.claimed_by)}</span></span>`);
      const sc = r.suggested_charge;
      if (sc) {
        const pct = Math.round((sc.confidence || 0) * 100);
        status.push(`<span class="aw-match"><span class="aw-chip info${sc.strong ? ' strong' : ''}" title="${esc((sc.reasons || []).join(' · '))}"><span>Matches charge · ${esc(sc.vendor)} · ${esc(shortDate(sc.date))} · ${money(sc.amount)} · ${pct}%${sc.strong ? ' · strong' : ''}</span></span>
          <span class="acts"><button type="button" class="aw-btn sm${sc.strong ? ' primary' : ''}" data-act="confirm-match" data-tid="${sc.id}" title="Approve the match (M)">Confirm</button>
          <button type="button" class="aw-btn sm" data-act="dismiss-match" data-tid="${sc.id}" title="Separate them; not suggested again">Not a match</button></span></span>`);
      }
      if (r.dup_pending) status.push('<span class="aw-flag yellow">Possible duplicate</span>');
      if (r.counted_twice) status.push(`<button type="button" class="aw-flag yellow linky" data-act="doc-detail" data-doc="${r.doc_id}" title="Coded on its own AND as a charge — open to attach it">Counted twice · attach to ${esc(r.counted_twice.vendor || 'the charge')}</button>`);
      if (!status.length) status.push('<span class="aw-flag none">Waiting</span>');
      const selected = S.selDoc === r.doc_id;
      const canLink = selected && !!selCharge;
      const linkTitle = canLink ? `Link to ${selCharge.vendor} ${money(selCharge.amount)}` : 'Select a receipt and a charge';
      html += `<div role="row" class="aw-row" data-doc="${r.doc_id}" tabindex="0" aria-selected="${selected}" draggable="true">
        <div class="aw-cell" style="padding-right:0"><a href="/docs/upload/${r.doc_id}/raw" target="_blank" rel="noopener" aria-label="Open ${esc(r.name)}"><img class="aw-thumb" src="${esc(r.thumb)}" alt="" loading="lazy" onerror="this.style.visibility='hidden'"></a></div>
        <div class="aw-cell num muted">${esc(shortDate(r.date))}</div>
        <div class="aw-cell" style="flex-direction:column;align-items:flex-start;justify-content:center;gap:1px">
          <span class="vendor ell" style="max-width:100%">${esc(r.vendor || r.name)}</span>
          <a class="ell muted" style="font-size:11px;max-width:100%" href="/docs/upload/${r.doc_id}/raw" target="_blank" rel="noopener">${esc(r.name)}</a>
        </div>
        <div class="aw-cell num right amt">${r.amount != null ? money(r.amount) : '—'}</div>
        <div class="aw-cell">${status.join(' ')}</div>
        <div class="aw-cell" style="gap:6px">
          <button type="button" class="aw-btn sm${canLink ? ' primary' : ''}" data-act="link-selected" data-doc="${r.doc_id}" ${canLink ? '' : 'disabled'} title="${esc(linkTitle)}">Link to selected</button>
          <button type="button" class="aw-btn sm" data-act="doc-detail" data-doc="${r.doc_id}" title="Open the details">Details</button>
          <button type="button" class="aw-btn sm" data-act="create-expense" data-doc="${r.doc_id}">Create expense</button>
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
        const who = `by ${esc(x.by || 'someone')}${FP.aiBadge(x.by_ai_agent)}${x.at ? ' · ' + esc(shortDate(x.at.slice(0, 10))) : ''}`;
        html += `<div role="row" class="aw-row excluded" data-tid="${t.id}" tabindex="0">
          <div class="aw-cell"></div>
          <div class="aw-cell num muted">${esc(shortDate(t.date))}</div>
          <div class="aw-cell"><span class="vendor ell">${esc(t.vendor)}</span></div>
          <div class="aw-cell num right amt">${money(t.amount)}</div>
          <div class="aw-cell num muted">${cardCell(t)}</div>
          <div class="aw-cell">${sourceChip(t)}</div>
          <div class="aw-cell" style="font-size:12px"><span class="ell" title="${esc(x.reason || '')}">${who}${x.reason ? ' · ' + esc(x.reason) : ''}</span></div>
          <div class="aw-cell right" style="gap:4px"><button type="button" class="aw-btn sm" data-act="detail" data-tid="${t.id}">Details</button><button type="button" class="aw-btn sm" data-act="recover" data-tid="${t.id}">Recover</button></div>
        </div>`;
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
          <dt>Status</dt><dd>${connected ? '<span class="aw-flag green">Connected</span>' : `<span class="aw-flag yellow">Not connected</span> <a href="${esc(AJ.urls.oauth)}">Connect in Admin</a>`}</dd>
          <dt>Accounts</dt><dd class="num">${sy.accounts || 0} company-wide${AJ.canSync ? ` · <a href="${esc(AJ.urls.accounts)}">choose in Admin</a>` : ''}</dd>
          <dt>Last sync</dt><dd class="num">${sy.last_synced ? esc(sy.last_synced.replace('T', ' ')) + ' (' + whenText(sy.last_synced) + ')' : 'never'}</dd>
          <dt>Change capture</dt><dd class="num">${sy.last_cdc_sync ? esc(sy.last_cdc_sync.replace('T', ' ')) : '—'}</dd>
          <dt>Synced through</dt><dd class="num">${esc(sy.sync_through || '—')}</dd>
          <dt>Automatic</dt><dd>${esc(sy.auto || 'twice a day')} · never past yesterday, 14-day overlap</dd>
        </dl>
        <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
          <label for="aw-sync-from" class="vis-hidden">Sync from</label><input id="aw-sync-from" type="date" title="Sync from (optional)">
          <label for="aw-sync-to" class="vis-hidden">Sync to</label><input id="aw-sync-to" type="date" title="Sync to (optional)">
          <button type="button" class="aw-btn primary" data-act="sync-now" ${connected && sy.accounts && AJ.canSync ? '' : 'disabled'}>Sync now</button>
        </div>
        <span class="help" style="font-size:12px;color:var(--text-muted)">The sync is company-wide: every account chosen in Admin is pulled into one pool, and a sync from any project refreshes it for all. Only charges accepted into the books reach the API; items still in QuickBooks' For Review tab arrive once accepted.</span>
      </div>
      <div class="aw-card">
        <h3>This project</h3>
        <dl class="aw-kv">
          <dt>Unclaimed</dt><dd class="num">${c.unclaimed || 0}</dd>
          <dt>Assigned</dt><dd class="num">${c.assigned || 0}</dd>
          <dt>Needs backup</dt><dd class="num">${c.needs_backup || 0}</dd>
          <dt>Receipts waiting</dt><dd class="num">${c.receipts || 0}</dd>
          <dt>Not project</dt><dd class="num">${c.not_project || 0}</dd>
          <dt>In the company pool</dt><dd class="num">${c.pooled || 0}</dd>
          <dt>Claimed by two projects</dt><dd class="num">${c.claim_conflicts || 0}${(c.claim_conflicts && S.data.conflicts_url) ? ` · <a href="${esc(S.data.conflicts_url)}">resolve</a>` : ''}</dd>
        </dl>
        <span class="help" style="font-size:12px;color:var(--text-muted)">Uploads: <a href="${esc(AJ.urls.docs)}">Documents</a> · <a href="${esc(AJ.urls.upload)}">mobile upload</a></span>
      </div>
      <div class="aw-card" style="grid-column:1 / -1">
        <h3>How the pool works</h3>
        <span class="help" style="font-size:12px;color:var(--text-muted)">Every QuickBooks charge exists once for the whole company. Unclaimed charges show in every project's Unclaimed view; assigning a line or section, splitting, attaching a receipt or marking Not project claims the charge for this project and it leaves the other projects' views. Unassigning, unmatching or recovering it with nothing else on it returns it to the pool.</span>
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
      // Section-coded spend (owner, Nov 4: "Rare Medium … assigned to the
      // 4700 post-production services section … I can't see it"): charges
      // coded to the section with no line + section split pieces, under the
      // header; expandable to the charges; a drop here codes to the section.
      const sc = sec.section_coded;
      if (sc && sc.count) {
        const open = S.openSecCoded && S.openSecCoded.has(sec.code);
        html += `<div class="aw-line aw-seccoded" data-target="section" data-code="${sec.code}" data-name="${esc(sec.name)}" data-seccoded="${sec.code}" tabindex="0" role="button" aria-expanded="${!!open}"
            aria-label="Section-coded ${money(sc.total)}, ${sc.count} charge${sc.count === 1 ? '' : 's'}: click to ${open ? 'hide' : 'list'} them; drop to code to the section">
          <span class="name"><span class="chev" aria-hidden="true">${open ? '\u25BE' : '\u25B8'}</span> Section-coded <span class="muted">(${sc.count})</span></span>
          <div class="aw-nums num"><span></span><span>${whole(sc.total)}</span><span></span></div></div>`;
        if (open) sc.rows.forEach(x => {
          html += `<div class="aw-line aw-seccoded-row" data-open-tid="${x.id}" tabindex="0" role="button" aria-label="Open ${esc(x.vendor)} ${money(x.amount)}">
            <span class="name"><span class="muted num">${esc(shortDate(x.date))}</span> ${esc(x.vendor)}${x.split ? ' <span class="muted">· split piece</span>' : ''}${+x.code !== +sec.code ? ` <span class="muted">· ${esc(x.code)}</span>` : ''}</span>
            <div class="aw-nums num"><span></span><span>${money(x.amount)}</span><span></span></div></div>`;
        });
      }
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
    $('aw-selinfo').textContent = `${n} selected · ${money(tot)}` + (one && S.view === 'receipts' ? ` · ${one.vendor} — now pick a receipt` : '');
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
    if (v !== 'receipts' && S.selDoc) { S.selDoc = null; S.pickCharge = null; if (AW.closePanel) AW.closePanel(true); }
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
    if (row) {
      clickRow(row, e);
      if (window.FPDetail && FPDetail.isOpen() && !e.shiftKey && !e.metaKey && !e.ctrlKey && !e.target.closest('input')) AW.openDetail(+row.dataset.tid);
      return;
    }
    const xrow = e.target.closest('.aw-row.excluded[data-tid]');
    if (xrow) { AW.openDetail(+xrow.dataset.tid); return; }
    const drow = e.target.closest('.aw-row[data-doc]');
    if (drow && S.view === 'receipts' && AW.selectDoc) AW.selectDoc(+drow.dataset.doc);
  });
  // The detail window (static/txn-detail.js, 2026-11-04): Details on a row,
  // a double-click, or Enter opens it; while it is open a plain click on
  // another row swaps it to that row.
  $('aw-table').addEventListener('dblclick', e => {
    if (e.target.closest('button, a, input')) return;
    const row = e.target.closest('.aw-row[data-tid]');
    if (row) { AW.openDetail(+row.dataset.tid); return; }
    const drow = e.target.closest('.aw-row[data-doc]');
    if (drow) AW.openDocDetail(+drow.dataset.doc);
  });
  $('aw-table').addEventListener('keydown', e => {
    const drow = e.target.closest && e.target.closest('.aw-row[data-doc]');
    if (drow && e.target === drow && e.key === 'Enter') { e.preventDefault(); AW.openDocDetail(+drow.dataset.doc); return; }
    if (drow && e.target === drow && e.key === ' ') { e.preventDefault(); if (AW.selectDoc) AW.selectDoc(+drow.dataset.doc); return; }
    const row = e.target.closest && e.target.closest('.aw-row[data-tid]');
    if (!row || e.target !== row) return;
    if (e.key === 'Enter') { e.preventDefault(); AW.openDetail(+row.dataset.tid); return; }
    if (!row.dataset.idx) return;
    if (e.key === ' ') { e.preventDefault(); clickRow(row, e); }
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
    const b = e.target.closest('.aw-head [data-act], .aw-filters [data-act]'); if (!b) return;
    if (AW.actions && AW.actions[b.dataset.act]) AW.actions[b.dataset.act](b, e);
  });
  // Budget pane: click a line → list its transactions; Enter/Space on a
  // target with a selection → assign (the keyboard path for every drop).
  $('aw-budget').addEventListener('click', e => {
    const openRow = e.target.closest('[data-open-tid]');
    if (openRow) { AW.openDetail(+openRow.dataset.openTid); return; }
    const sc = e.target.closest('[data-seccoded]');
    if (sc && !(S.sel.size && S.view !== 'receipts' && S.view !== 'not_project')) {
      const code = +sc.dataset.seccoded;
      if (S.openSecCoded.has(code)) S.openSecCoded.delete(code); else S.openSecCoded.add(code);
      renderBudget(); return;
    }
    if (sc) { if (AW.actions && AW.actions['assign-to']) AW.actions['assign-to']({ code: sc.dataset.code, name: sc.dataset.name }); return; }
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
    if (window.FPCompare && FPCompare.isOpen()) return;      // the Compare panel owns the keys
    const tag = (e.target.tagName || '').toLowerCase();
    if (tag === 'input' || tag === 'textarea' || tag === 'select' || e.target.isContentEditable) return;
    if (e.metaKey || e.ctrlKey || e.altKey) return;
    if (e.key === 'Escape') {
      if (S.sel.size) { AW.clearSelection(); e.preventDefault(); }
      else if (AW.panelActive && AW.panelActive()) { AW.closePanel(); e.preventDefault(); }
      if (AW.closeMenu) AW.closeMenu(); return;
    }
    // Phase 5: M approves the selected suggestion (charge side or receipt
    // side), D opens the selected row's duplicate group.
    if (e.key === 'm' || e.key === 'M') {
      let tid = null;
      if (S.view === 'receipts' && S.selDoc) { const r = S.docById.get(S.selDoc); tid = r && r.suggested_charge_id; }
      else if (S.sel.size === 1) { const t = S.byId.get(Array.from(S.sel)[0]); tid = t && t.receipt_suggestion ? t.id : null; }
      if (tid && AW.actions) { e.preventDefault(); AW.actions['confirm-match']({ dataset: { tid: String(tid) } }); }
      return;
    }
    if (e.key === 'd' || e.key === 'D') {
      // 2026-11-04: D compares the selected row's duplicate group in place.
      if (S.sel.size === 1) { const t = S.byId.get(Array.from(S.sel)[0]); if (t && t.dup_group && AW.openCompare) { e.preventDefault(); AW.openCompare({ key: t.dup_group, tid: t.id }); } }
      return;
    }
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

  // ── Detail window glue ───────────────────────────────────────────────
  const detailHooks = {
    openLine: lid => { FPDetail.close(); S.lineFilter = lid; AW.setView('assigned'); },
    openSection: code => {
      FPDetail.close();
      const el = document.querySelector(`#aw-budget .aw-sec[data-sec="${AW.sectionFor(code)}"]`);
      if (el) { el.scrollIntoView({ block: 'start', behavior: 'smooth' }); S.openSecCoded.add(AW.sectionFor(code)); renderBudget(); }
    },
  };
  AW.openDetail = tid => { if (window.FPDetail && tid) FPDetail.open({ pid: PID, tid, hooks: detailHooks, onChange: () => AW.load({ keepFocus: true }) }); };
  AW.openDocDetail = did => { if (window.FPDetail && did) FPDetail.open({ pid: PID, did, tid: null, hooks: detailHooks, onChange: () => AW.load({ keepFocus: true }) }); };
  // A drag needs the budget pane: the detail window steps aside.
  document.addEventListener('dragstart', () => { if (window.FPDetail && FPDetail.isOpen()) FPDetail.close(); }, true);

  // ── Boot ─────────────────────────────────────────────────────────────
  AW.load();
})();
