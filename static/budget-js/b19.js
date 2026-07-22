// Extracted from a templates/budget.html inline block (audit M9, 2026-07-20).
// Jinja values arrive via window.__BJ (set by the inline preamble before this
// script tag). Classic script, document order — semantics identical to inline.

(function lineLedgerIIFE() {
  // Scope trap: NO global PROJ_ID in budget.html script blocks — derive pid
  // from the URL (this has bitten three prior builds). (2026-07.)
  const _m = window.location.pathname.match(/\/projects\/(\d+)\//);
  const PID = _m ? _m[1] : '';

  const overlay = document.getElementById('line-ledger-overlay');
  const panel   = document.getElementById('line-ledger-panel');
  const elCrumb = document.getElementById('ll-crumb');
  const elSum   = document.getElementById('ll-summary');
  const elChips = document.getElementById('ll-chips');
  const elBody  = document.getElementById('ll-body');
  const btnRevAll = document.getElementById('ll-review-all');
  if (!panel) return;

  let _lid = null;        // the line id the panel was opened with
  let _rows = [];         // full chronological txn list (with running balance)
  let _filter = 'all';    // active exception chip
  let _sel = -1;          // selected row index within the FILTERED view

  function _esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }
  function _money(n) {
    if (n == null || isNaN(n)) return '—';
    const neg = n < 0;
    return (neg ? '-' : '') + '$' + Math.abs(n).toLocaleString('en-US',
      { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }

  // Which rows pass the active exception filter.
  function _passes(r) {
    switch (_filter) {
      case 'nodoc':    return !r.matched;
      case 'unmatched':return !r.matched;
      case 'unreviewed': return !r.reviewed;
      case 'flagged':  return !!r.flagged;
      default:         return true;
    }
  }
  function _visibleRows() { return _rows.filter(_passes); }

  function _renderChips() {
    const counts = {
      all: _rows.length,
      nodoc: _rows.filter(r => !r.matched).length,
      unmatched: _rows.filter(r => !r.matched).length,
      unreviewed: _rows.filter(r => !r.reviewed).length,
      flagged: _rows.filter(r => r.flagged).length,
    };
    const defs = [
      ['all', 'All'],
      ['nodoc', '📄 No document'],
      ['unmatched', '🔗 Unmatched'],
      ['unreviewed', '⬜ Unreviewed'],
      ['flagged', '⚠ Flagged'],
    ];
    elChips.innerHTML = defs.map(([k, label]) =>
      '<button type="button" class="ll-chip' + (_filter === k ? ' is-active' : '') + '" '
      + 'data-filter="' + k + '">' + _esc(label)
      + '<span class="ll-chip-n">' + (counts[k] || 0) + '</span></button>'
    ).join('');
  }

  function _renderSummary(line) {
    const budget = line.budget_total;
    const coded  = line.coded_total || 0;
    const remain = (budget == null) ? null : (budget - coded);
    const overCls = (remain != null && remain < 0) ? ' ll-over' : '';
    elSum.innerHTML =
      '<div><span class="ll-k">Budget</span><span class="ll-v">'
        + (budget == null ? '—' : _money(budget)) + '</span></div>'
      + '<div><span class="ll-k">Coded</span><span class="ll-v">' + _money(coded) + '</span></div>'
      + '<div><span class="ll-k">Remaining</span><span class="ll-v' + overCls + '">'
        + (remain == null ? '—' : _money(remain)) + '</span></div>';
  }

  function _renderRows() {
    const vis = _visibleRows();
    if (!vis.length) {
      elBody.innerHTML = '<div class="ll-empty">No matching transactions.</div>';
      return;
    }
    let html = '<table class="ll-table"><thead><tr>'
      + '<th>Date</th><th>Vendor</th><th>Doc</th><th class="ll-num">Amount</th>'
      + '<th class="ll-num">Balance</th><th class="ll-rev-cell">✓</th></tr></thead><tbody>';
    vis.forEach((r) => {
      const amtNeg = (r.amount != null && r.amount < 0);
      let docCell;
      if (r.doc && r.doc.has_thumb) {
        docCell = '<a href="/projects/' + PID + '/docs/' + r.doc.id
          + '/editor?from=budget" class="ll-doc-link" data-doc="' + r.doc.id + '" '
          + 'title="' + _esc(r.doc.filename) + '">'
          + '<img loading="lazy" src="/docs/upload/' + r.doc.id + '/thumb" alt=""></a>';
      } else if (r.doc) {
        docCell = '<a href="/projects/' + PID + '/docs/' + r.doc.id
          + '/editor?from=budget" class="ll-doc-link" data-doc="' + r.doc.id + '">📎 '
          + _esc((r.doc.filename || 'receipt').slice(0, 22)) + '</a>';
      } else {
        docCell = '<span class="ll-no-doc">no receipt</span>';
      }
      const splitTag = r.is_split
        ? '<span class="ll-split" title="Part of a document split across multiple lines">(split)</span>'
        : '';
      const flagTag = r.flagged ? '<span class="ll-flag" title="Has an unresolved flag">⚠</span>' : '';
      html += '<tr class="ll-row' + (r._idx === _sel ? ' is-sel' : '') + '" data-tid="' + r.id
        + '" data-rowidx="' + r._idx + '">'
        + '<td style="color:var(--text-muted);font-variant-numeric:tabular-nums;white-space:nowrap">'
          + _esc(r.date || '—') + '</td>'
        + '<td>' + _esc(r.vendor || '—') + splitTag + flagTag + '</td>'
        + '<td class="ll-doc-cell">' + docCell + '</td>'
        + '<td class="ll-num' + (amtNeg ? ' ll-amt-neg' : '') + '">'
          + (amtNeg ? '↩ ' : '') + (r.amount != null ? _money(r.amount) : '—') + '</td>'
        + '<td class="ll-num" style="color:var(--text-muted)">' + _money(r._balance) + '</td>'
        + '<td class="ll-rev-cell"><input type="checkbox" class="ll-rev"'
          + (r.reviewed ? ' checked' : '') + ' data-tid="' + r.id + '"></td>'
        + '</tr>';
      (r.backups || []).forEach(bk => {
        html += '<tr class="ll-bk-row" style="background:rgba(224,192,96,.05)"><td></td>'
          + '<td colspan="4" style="font-size:.74rem;color:var(--text-muted);padding:2px 6px 4px 14px">📎 backup: '
          + (bk.doc_id
              ? '<a href="/projects/' + PID + '/docs/' + bk.doc_id + '/editor?from=budget" class="ll-doc-link">' + _esc(bk.filename || 'receipt') + '</a>'
              : _esc(bk.filename || 'receipt'))
          + (bk.amount != null ? ' · ' + _money(bk.amount) : '') + '</td><td></td></tr>';
      });
    });
    html += '</tbody></table>';
    elBody.innerHTML = html;
  }

  // Running balance recomputed over the FULL chronological list (not the
  // filtered view). Each row keeps its full-list cumulative balance + a stable
  // index used for selection across filters.
  function _recomputeBalances() {
    let bal = 0;
    _rows.forEach((r, i) => {
      bal += (r.amount != null ? r.amount : 0);
      r._balance = bal;
      r._idx = i;
    });
  }

  async function _load() {
    elBody.innerHTML = '<div class="ll-empty">Loading…</div>';
    try {
      const res = await fetch('/projects/' + PID + '/actuals/line/' + _lid + '/ledger.json',
        { credentials: 'same-origin' });
      const d = await res.json();
      if (!res.ok || !d.ok) {
        elBody.innerHTML = '<div class="ll-empty">Could not load ('
          + _esc((d && d.error) || res.status) + ').</div>';
        return;
      }
      _rows = d.txns || [];
      _recomputeBalances();
      const sec = d.section || {};
      const ln  = d.line || {};
      elCrumb.innerHTML =
        '<span class="ll-sec">' + _esc(sec.code || '') + ' ' + _esc(sec.name || '') + '</span>'
        + '<span class="ll-arrow">→</span>'
        + '<strong>' + _esc(ln.account_code || '') + ' ' + _esc(ln.description || '') + '</strong>';
      _renderSummary(ln);
      _renderChips();
      _sel = -1;
      _renderRows();
    } catch (e) {
      elBody.innerHTML = '<div class="ll-empty">Load failed: ' + _esc(e.message) + '</div>';
    }
  }

  function _open() {
    overlay.classList.add('is-open');
    panel.classList.add('is-open');
    panel.setAttribute('aria-hidden', 'false');
    document.addEventListener('keydown', _onKey, true);
  }
  function _close() {
    overlay.classList.remove('is-open');
    panel.classList.remove('is-open');
    panel.setAttribute('aria-hidden', 'true');
    document.removeEventListener('keydown', _onKey, true);
    _lid = null; _rows = []; _sel = -1; _filter = 'all';
  }

  // Optimistic review toggle — flip UI immediately, revert on failure.
  async function _setReviewed(tid, want) {
    const r = _rows.find(x => x.id === tid);
    if (r) r.reviewed = want;
    // Re-render only if the current filter could hide/show this row.
    _renderChips();
    _renderRows();
    try {
      const res = await fetch('/projects/' + PID + '/actuals/txn/' + tid + '/review',
        { method: 'POST', headers: { 'Content-Type': 'application/json' },
          credentials: 'same-origin', body: JSON.stringify({ reviewed: want }) });
      const d = await res.json();
      if (!res.ok || !d.ok) throw new Error((d && d.error) || res.status);
      if (r) { r.reviewed = !!d.reviewed; r.reviewed_by = d.reviewed_by; r.reviewed_at = d.reviewed_at; }
    } catch (e) {
      if (r) r.reviewed = !want;   // revert
      _renderChips();
      _renderRows();
    }
  }

  // Keyboard: j/k move selection, space toggles reviewed, Enter opens doc,
  // Esc closes. Selection is over the FILTERED (visible) list.
  function _onKey(e) {
    if (!panel.classList.contains('is-open')) return;
    const tag = (e.target && e.target.tagName || '').toLowerCase();
    if (e.key === 'Escape') { e.preventDefault(); _close(); return; }
    // Don't hijack typing in inputs (there are none of note, but be safe).
    if (tag === 'input' && e.key !== 'Escape') {
      if (e.key === ' ') { /* checkbox handles its own toggle */ return; }
    }
    const vis = _visibleRows();
    if (!vis.length) return;
    // Map _sel (a full-list index) to its position in the visible list.
    let pos = vis.findIndex(r => r._idx === _sel);
    if (e.key === 'j' || e.key === 'ArrowDown') {
      e.preventDefault();
      pos = Math.min(vis.length - 1, pos < 0 ? 0 : pos + 1);
      _sel = vis[pos]._idx; _renderRows(); _scrollSel();
    } else if (e.key === 'k' || e.key === 'ArrowUp') {
      e.preventDefault();
      pos = pos <= 0 ? 0 : pos - 1;
      _sel = vis[pos]._idx; _renderRows(); _scrollSel();
    } else if (e.key === ' ') {
      e.preventDefault();   // stop the page scrolling
      if (pos < 0) return;
      const r = vis[pos];
      _setReviewed(r.id, !r.reviewed);
    } else if (e.key === 'Enter') {
      if (pos < 0) return;
      const r = vis[pos];
      if (r.doc) { e.preventDefault(); window.location.href =
        '/projects/' + PID + '/docs/' + r.doc.id + '/editor?from=budget'; }
    }
  }
  function _scrollSel() {
    const el = elBody.querySelector('tr.ll-row.is-sel');
    if (el && el.scrollIntoView) el.scrollIntoView({ block: 'nearest' });
  }

  // ── Wiring ────────────────────────────────────────────────────────────
  document.getElementById('ll-close').addEventListener('click', _close);
  overlay.addEventListener('click', _close);

  elChips.addEventListener('click', e => {
    const c = e.target.closest('.ll-chip');
    if (!c) return;
    _filter = c.dataset.filter || 'all';
    _sel = -1;
    _renderChips();
    _renderRows();
  });

  elBody.addEventListener('click', e => {
    // Reviewed checkbox → optimistic toggle.
    const cb = e.target.closest('.ll-rev');
    if (cb) {
      e.stopPropagation();
      const tid = parseInt(cb.dataset.tid);
      _setReviewed(tid, cb.checked);
      return;
    }
    // Doc cell (link/img) navigates via the anchor — let it through.
    if (e.target.closest('.ll-doc-link')) return;
    // Elsewhere on the row → select it.
    const row = e.target.closest('tr.ll-row');
    if (row) {
      _sel = parseInt(row.dataset.rowidx);
      _renderRows();
    }
  });

  btnRevAll.addEventListener('click', async () => {
    if (!_lid) return;
    btnRevAll.disabled = true;
    const orig = btnRevAll.textContent;
    btnRevAll.textContent = '…';
    try {
      const res = await fetch('/projects/' + PID + '/actuals/line/' + _lid + '/review-all',
        { method: 'POST', headers: { 'Content-Type': 'application/json' },
          credentials: 'same-origin', body: '{}' });
      const d = await res.json();
      if (!res.ok || !d.ok) throw new Error((d && d.error) || res.status);
      await _load();   // refresh so counts + checkmarks reflect the DB
    } catch (e) {
      /* leave state as-is on failure */
    } finally {
      btnRevAll.disabled = false;
      btnRevAll.textContent = orig;
    }
  });

  // Public entry point (attached to window so the context menu + inline 📒
  // button can open the panel). Review changes never move totals, so we do
  // NOT call refreshActualCells here.
  window.openLineLedger = function(lineId) {
    if (typeof _IS_ACTUAL_VIEW !== 'undefined' && _IS_ACTUAL_VIEW) return;
    _lid = parseInt(lineId);
    if (!_lid) return;
    _open();
    _load();
  };
})();
