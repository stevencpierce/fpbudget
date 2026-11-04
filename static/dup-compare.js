/* FPBudget — Compare duplicates in place (2026-11-04).

   Owner, Nov 4: "when I go to Actuals → Unclaimed I see 'possible
   duplicate'. Great. Can I compare them? Can we have some kind of compare
   and settle this right in this view, and then apply that other places
   wherever the duplicates go?"

   ONE side panel in the detail window's shell (.fpd styles) over ONE
   payload: GET /projects/<pid>/actuals/duplicates/compare.json?key=|tid=
   (routes/actuals._dup_compare_payload over the ONE duplicate rule). The
   members of the group are columns; the fields that differ are
   highlighted; the suggested keeper (pick_keeper) is marked with its
   reason. Settling goes through the existing safe paths:
     Keep both            POST …/dup-transactions/dismiss {dup_key}
     Keep this one        POST …/duplicates/settle {key, keeper_id}
                          (every other member: merge_duplicate_into, then
                          deleted — logged, Undo puts them back)
     Not project (one)    the Actuals reason dialog → mark-not-project

     FPCompare.open({pid, key | tid, onSettled(result), notProject(tid),
                     toast(msg, kind, opts)})
     FPCompare.close()   FPCompare.isOpen()

   Keyboard while open: ←/→ move between columns, Enter keeps the focused
   column (then Enter again confirms), Esc cancels the confirm or closes.
   At 1024 wide with more than two charges the columns sit two-up with a
   horizontal scroll. Classic script; HTML is built with FP.esc. */
(function () {
  'use strict';
  const FP = window.FP = window.FP || {};
  const esc = s => FP.esc(s);
  const money = v => FP.money(v == null ? 0 : v);
  const $q = (root, sel) => root.querySelector(sel);
  const ICON_X = '<svg aria-hidden="true" width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"><path d="M2 2l10 10M12 2L2 12"/></svg>';
  const LAB_W = 112;
  const C = { el: null, opts: null, data: null, focus: 0, confirm: null, busy: false };
  const FPCompare = window.FPCompare = {};

  async function jget(url) {
    const r = await fetch(url, { credentials: 'same-origin' });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) { const e = new Error(d.error || ('HTTP ' + r.status)); e.data = d; throw e; }
    return d;
  }
  async function jpost(url, body) {
    const r = await fetch(url, { method: 'POST', credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body || {}) });
    const d = await r.json().catch(() => ({}));
    if (!r.ok || d.ok === false) { const e = new Error(d.error || ('HTTP ' + r.status)); e.data = d; throw e; }
    return d;
  }

  function shell() {
    if (C.el) return C.el;
    const el = document.createElement('aside');
    el.className = 'fpd fpc';
    el.id = 'fpc';
    el.setAttribute('role', 'dialog');
    el.setAttribute('aria-modal', 'false');
    el.setAttribute('aria-labelledby', 'fpc-title');
    el.hidden = true;
    el.innerHTML = `<div class="fpd-head"><div class="fpd-htxt"><span class="fpd-kicker" id="fpc-kicker"></span><h2 id="fpc-title">Compare</h2></div>
      <button type="button" class="fpd-x" data-fpc="close" aria-label="Close compare">${ICON_X}</button></div>
      <div class="fpd-body fpc-body" id="fpc-body"><p class="fpd-muted">Loading</p></div>`;
    document.body.appendChild(el);
    el.addEventListener('click', onClick);
    el.addEventListener('focusin', e => {
      const c = e.target.closest && e.target.closest('[data-col]');
      if (c && C.data) { const i = +c.dataset.col; if (i !== C.focus) { C.focus = i; paintFocus(); } }
    });
    window.addEventListener('resize', () => { if (FPCompare.isOpen()) sizeColumns(); });
    C.el = el;
    return el;
  }

  FPCompare.isOpen = () => !!(C.el && !C.el.hidden);
  FPCompare.open = async function (opts) {
    C.opts = Object.assign({}, opts);
    C.confirm = null; C.focus = 0; C.data = null;
    if (window.FPDetail && FPDetail.isOpen()) FPDetail.close();
    const el = shell();
    el.hidden = false;
    document.body.classList.add('fpd-open');
    $q(el, '#fpc-body').innerHTML = '<p class="fpd-muted">Loading</p>';
    await load();
  };
  FPCompare.close = function () {
    if (!C.el) return;
    C.el.hidden = true;
    C.confirm = null;
    document.body.classList.remove('fpd-open');
    if (C.opts && C.opts.onClose) C.opts.onClose();
  };

  async function load() {
    const o = C.opts, body = $q(C.el, '#fpc-body');
    const q = o.key ? 'key=' + encodeURIComponent(o.key) : 'tid=' + encodeURIComponent(o.tid);
    try {
      C.data = await jget(`/projects/${o.pid}/actuals/duplicates/compare.json?${q}`);
      C.opts.key = C.data.key;
      const ki = C.data.columns.findIndex(c => c.is_keeper);
      C.focus = o.tid ? Math.max(0, C.data.columns.findIndex(c => c.id === o.tid)) : Math.max(0, ki);
      render();
      const h = $q(C.el, `.fpc-h[data-col="${C.focus}"]`);
      if (h) { h.focus({ preventScroll: true }); reveal(C.focus); }
    } catch (e) {
      body.innerHTML = `<p class="fpd-err">${esc(e.message || e)}</p>`;
      if (e.data && e.data.gone && C.opts.onSettled) { try { await C.opts.onSettled({ gone: true }); } catch (x) {} }
    }
  }

  function srcBadge(label) {
    const cls = ({ QuickBooks: 'qbo', Receipt: 'rcpt', Manual: 'man', CSV: 'csv' })[label] || 'man';
    return `<span class="fpd-src ${cls}">${esc(label)}</span>`;
  }
  function cell(key, c) {
    const v = c[key];
    if (key === 'source') return `<span class="fpc-line">${srcBadge(c.source)} <span class="fpd-muted">#${esc(c.id)}</span>${c.pooled ? ' <span class="fpd-chip">pool</span>' : ''}</span>`;
    if (key === 'date') return esc(c.date || '—');
    if (key === 'amount') return `<span class="fpc-amt${Number(c.amount) < 0 ? ' credit' : ''}">${money(c.amount)}</span>`;
    if (key === 'payee') {
      const p = c.payee || {};
      let h = `<span class="fpc-strong">${esc(p.vendor || '—')}</span>`;
      if (p.qbo_payee && p.qbo_payee !== p.vendor) h += `<span class="fpc-sub">QuickBooks payee · ${esc(p.qbo_payee)}</span>`;
      if (p.label) h += `<span class="fpc-sub">Payee · ${esc(p.label)}</span>`;
      return h;
    }
    if (key === 'card') return esc(v || '—');
    if (key === 'qbo') return v ? `<span class="fpc-line">${esc(v.type || '')} <span class="fpd-muted">#${esc(v.id)}</span></span>` : '<span class="fpd-muted">Not from QuickBooks</span>';
    if (key === 'memo') return v ? esc(v) : '<span class="fpd-muted">—</span>';
    if (key === 'coding') return v ? `<span class="fpd-node">${esc(v)}</span>` : '<span class="fpd-muted">Not coded</span>';
    if (key === 'backup') {
      if (!v) return '<span class="fpd-muted">None</span>';
      return `<div class="fpc-doc"><a href="${esc(v.url)}" target="_blank" rel="noopener" aria-label="Open ${esc(v.name)}"><img src="${esc(v.thumb)}" alt="" loading="lazy" onerror="this.style.visibility='hidden'"></a>
        <span class="fpc-docmeta"><span class="ell" title="${esc(v.name)}">${esc(v.name)}</span><a class="fpd-btn sm" href="${esc(v.url)}" target="_blank" rel="noopener">Open</a></span></div>`;
    }
    if (key === 'match') return esc(v || '—');
    if (key === 'imported') {
      const at = (v && v.at) ? v.at.replace('T', ' ') : '—';
      return `${esc(at)}<span class="fpc-sub">${esc((v && v.by) || '')}</span>`;
    }
    return esc(v == null ? '—' : v);
  }

  function render() {
    const d = C.data, body = $q(C.el, '#fpc-body');
    const cols = d.columns || [];
    $q(C.el, '#fpc-kicker').innerHTML = `<span class="fpd-flag yellow">Possible duplicate</span> <span class="fpd-chip">${cols.length} charges</span>`;
    $q(C.el, '#fpc-title').textContent = `${d.vendor} · ${money(d.amount)}`;
    const keeper = cols.find(c => c.is_keeper);
    const diff = new Set(d.diff || []);
    let h = `<div class="fpc-sum"><span>Suggested keeper <strong>#${esc(keeper ? keeper.id : '')}</strong> — ${esc(d.keeper_reason || '')}.</span>`
      + (d.payee_note ? ` <span class="fpd-muted">${esc(d.payee_note)}</span>` : '')
      + (diff.size ? ` <span class="fpd-muted">Highlighted: what differs.</span>` : ` <span class="fpd-muted">Every field matches.</span>`) + `</div>`;
    h += `<div class="fpc-scroll" id="fpc-scroll"><div class="fpc-grid" id="fpc-grid" role="grid" aria-label="Duplicate charges side by side" style="--n:${cols.length}">`;
    h += `<div class="fpc-lab fpc-corner" role="columnheader"></div>`;
    cols.forEach((c, i) => {
      h += `<div class="fpc-h${c.is_keeper ? ' keeper' : ''}" data-col="${i}" tabindex="0" role="columnheader" aria-label="Charge #${c.id}${c.is_keeper ? ', suggested keeper' : ''}">
        ${c.is_keeper ? `<span class="fpc-badge keep">Suggested keeper</span><span class="fpc-why">${esc(d.keeper_reason || '')}</span>` : '<span class="fpc-badge">Duplicate?</span>'}</div>`;
    });
    (d.fields || []).forEach(f => {
      const isDiff = diff.has(f.key);
      h += `<div class="fpc-lab${isDiff ? ' diff' : ''}" role="rowheader">${esc(f.label)}${isDiff ? '<span class="fpc-dot" aria-label="differs"></span>' : ''}</div>`;
      cols.forEach((c, i) => { h += `<div class="fpc-v${isDiff ? ' diff' : ''}" data-col="${i}" role="gridcell">${cell(f.key, c)}</div>`; });
    });
    h += `<div class="fpc-lab" role="rowheader">If kept</div>`;
    cols.forEach((c, i) => { h += `<div class="fpc-v fpc-eff" data-col="${i}" role="gridcell">${esc(c.keep_effect || '')}</div>`; });
    h += `<div class="fpc-lab" role="rowheader"></div>`;
    cols.forEach((c, i) => {
      const conf = C.confirm === i;
      h += `<div class="fpc-v fpc-acts" data-col="${i}" role="gridcell">${conf
        ? `<div class="fpc-confirm" role="alert"><span>Keep #${c.id}? ${esc(c.keep_effect || '')}. Undo is on the next toast.</span>
            <span class="fpd-acts"><button type="button" class="fpd-btn sm primary" data-fpc="keep-go" data-i="${i}">Confirm keep</button>
            <button type="button" class="fpd-btn sm" data-fpc="keep-cancel">Cancel</button></span></div>`
        : `<button type="button" class="fpd-btn sm${c.is_keeper ? ' primary' : ''}" data-fpc="keep" data-i="${i}" title="Keep this charge; the others are removed through the safe path (Enter)">Keep this one</button>
           <button type="button" class="fpd-btn sm" data-fpc="not-project" data-i="${i}" title="Not a project expense — for this project only">Not project</button>`}</div>`;
    });
    h += `</div></div>`;
    h += `<div class="fpc-foot"><button type="button" class="fpd-btn" data-fpc="keep-both" title="These are separate charges — remembered, never flagged again">Keep both — they're different</button>
      <span class="fpd-muted fpc-keys"><kbd>←</kbd> <kbd>→</kbd> columns · <kbd>Enter</kbd> keep the focused one · <kbd>Esc</kbd> close</span></div>
      <div class="fpd-err" id="fpc-err"></div>`;
    body.innerHTML = h;
    sizeColumns();
    paintFocus();
  }

  // Columns: side by side when they fit; two-up with a horizontal scroll
  // when there are more than two and the panel is narrow (iPad 1024).
  function sizeColumns() {
    const sc = $q(C.el, '#fpc-scroll'), grid = $q(C.el, '#fpc-grid');
    if (!sc || !grid || !C.data) return;
    const n = (C.data.columns || []).length || 1, gap = 10;
    const avail = sc.clientWidth - LAB_W - gap * n;
    let per = avail / n;
    // more than two charges on a narrow panel (iPad 1024): two-up + scroll
    if (n > 2 && (per < 300 || sc.clientWidth < 1000)) per = avail / 2 + gap / 2;
    per = Math.max(200, Math.floor(per));
    C.per = per;
    grid.style.gridTemplateColumns = `${LAB_W}px repeat(${n}, ${per}px)`;
  }
  // Scroll the column strip so column i sits whole beside the sticky labels.
  function reveal(i) {
    const sc = $q(C.el, '#fpc-scroll'); if (!sc || !C.per) return;
    const gap = 10, left = LAB_W + gap + i * (C.per + gap), right = left + C.per;
    if (left - sc.scrollLeft < LAB_W + gap) sc.scrollLeft = left - LAB_W - gap;
    else if (right - sc.scrollLeft > sc.clientWidth) sc.scrollLeft = right - sc.clientWidth;
  }
  function paintFocus() {
    if (!C.el) return;
    C.el.querySelectorAll('[data-col]').forEach(x => x.classList.toggle('focus', +x.dataset.col === C.focus));
  }
  function moveFocus(delta) {
    const n = (C.data && C.data.columns || []).length;
    if (!n) return;
    C.focus = (C.focus + delta + n) % n;
    if (C.confirm != null) { C.confirm = null; render(); }
    paintFocus();
    const h = $q(C.el, `.fpc-h[data-col="${C.focus}"]`);
    if (h) { h.focus({ preventScroll: true }); reveal(C.focus); }
  }
  function askKeep(i) {
    const sc = $q(C.el, '#fpc-scroll'), x = sc ? sc.scrollLeft : 0;
    C.confirm = i; C.focus = i;
    render();
    const sc2 = $q(C.el, '#fpc-scroll'); if (sc2) sc2.scrollLeft = x;
    const go = $q(C.el, '[data-fpc="keep-go"]');
    if (go) go.focus({ preventScroll: true });
    reveal(i);
  }

  async function settled(msg, r, ids) {
    FPCompare.close();
    if (C.opts.onSettled) { try { await C.opts.onSettled(r || {}); } catch (e) {} }
    if (C.opts.toast) C.opts.toast(msg, '', { activityIds: ids || [] });
  }
  function err(msg) { const e = $q(C.el, '#fpc-err'); if (e) e.textContent = msg || ''; }

  async function onClick(e) {
    const b = e.target.closest('[data-fpc]'); if (!b || C.busy) return;
    const act = b.dataset.fpc, d = C.data, o = C.opts;
    if (act === 'close') return FPCompare.close();
    if (!d) return;
    const col = d.columns[+b.dataset.i];
    if (act === 'keep') return askKeep(+b.dataset.i);
    if (act === 'keep-cancel') { C.confirm = null; render(); return; }
    if (act === 'keep-go') {
      C.busy = true; b.disabled = true; err('');
      try {
        const r = await jpost(`/projects/${o.pid}/actuals/duplicates/settle`, { key: d.key, keeper_id: col.id });
        C.busy = false;
        const n = (r.deleted || []).length;
        return settled(`Kept #${col.id} · ${n} duplicate${n === 1 ? '' : 's'} removed`, r, r.activity_ids);
      } catch (x) { C.busy = false; b.disabled = false; err(x.message || 'Could not settle'); }
      return;
    }
    if (act === 'keep-both') {
      if (!confirm('Keep both? These are separate charges and will not be flagged as duplicates again — in any project.')) return;
      C.busy = true; b.disabled = true; err('');
      try {
        const r = await jpost(`/projects/${o.pid}/actuals/dup-transactions/dismiss`, { dup_key: d.key });
        C.busy = false;
        return settled('Kept both. This group will not be flagged again.', r, []);
      } catch (x) { C.busy = false; b.disabled = false; err(x.message || 'Could not save'); }
      return;
    }
    if (act === 'not-project') {
      FPCompare.close();
      if (o.notProject) o.notProject(col.id);
    }
  }

  document.addEventListener('keydown', e => {
    if (!FPCompare.isOpen() || document.querySelector('dialog[open]')) return;
    const tag = (e.target.tagName || '').toLowerCase();
    if (tag === 'input' || tag === 'textarea' || tag === 'select' || e.target.isContentEditable) return;
    if (e.metaKey || e.ctrlKey || e.altKey) return;
    if (e.key === 'Escape') {
      e.preventDefault(); e.stopPropagation();
      if (C.confirm != null) { C.confirm = null; render(); return; }
      FPCompare.close(); return;
    }
    if (e.key === 'ArrowLeft' || e.key === 'ArrowRight') {
      e.preventDefault(); e.stopPropagation(); moveFocus(e.key === 'ArrowRight' ? 1 : -1); return;
    }
    if (e.key === 'Enter') {
      if (tag === 'button' || tag === 'a') return;          // the focused control handles it
      e.preventDefault(); e.stopPropagation();
      if (C.confirm != null) { const go = $q(C.el, '[data-fpc="keep-go"]'); if (go) go.click(); return; }
      askKeep(C.focus);
    }
  }, true);
})();
