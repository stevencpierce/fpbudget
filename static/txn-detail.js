/* FPBudget — the charge / receipt detail window and the ONE line picker
   (2026-11-04).

   Owner, Nov 4: "I should be able to open these up and see what they're
   linked to … chain of custody … remove it, interact with it, change it.
   What if the amount is wrong?" and "split across lines should work exactly
   the same way as assign … I can't assign it to a main section. I can't
   search."

   ONE component for every row that opens it — Actuals (every view) and the
   budget page's Actual drill-down — over ONE payload:
     GET /projects/<pid>/actuals/transaction/<tid>/detail.json
     GET /projects/<pid>/actuals/doc/<did>/detail.json   (receipt, no row)
   Every change goes through the routes that already exist (set-line,
   set-coa, split, edit, link-doc, unmatch, upload-receipt, confirm-match,
   dismiss-suggestion, activity undo) plus detach-doc / fold-itemization.

     FPDetail.open({pid, tid | did, onChange})   open (or swap) the window
     FPDetail.close()
     FP.linePickerOptions(groups, q, opts)       the ONE option builder —
                                                 Assign dialog and split rows
     FP.LinePicker(host, opts)                   the searchable combobox:
                                                 lines, "All <section>", and
                                                 "+ New line…"
     FP.newLine(pid, code, description)          POST budget-line/new

   Classic script, no framework; HTML strings are built with FP.esc. */
(function () {
  'use strict';
  const FP = window.FP = window.FP || {};
  const esc = s => FP.esc(s);
  const money = v => FP.money(v == null ? 0 : v);
  const $q = (root, sel) => root.querySelector(sel);

  async function jget(url) {
    const r = await fetch(url, { credentials: 'same-origin' });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(d.error || ('HTTP ' + r.status));
    return d;
  }
  async function jpost(url, body, method) {
    const r = await fetch(url, { method: method || 'POST', credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body || {}) });
    const d = await r.json().catch(() => ({}));
    if (!r.ok || d.ok === false) { const e = new Error(d.error || d.message || ('HTTP ' + r.status)); e.data = d; throw e; }
    return d;
  }
  const cents = v => Math.round(Number(String(v == null ? '' : v).replace(/[$,]/g, '')) * 100);

  // ── The ONE line-picker option builder ───────────────────────────────
  // groups: [{code, name, opts:[{id, label, kind}]}] (actuals.
  // build_line_picker_options). Returns rows: {group} headings, then
  // choosable {value, label, kind:'line'|'section'|'new', sub, child}.
  // Search matches the line label, the code and the section name; every
  // group offers "All <section>" (section-only coding); extra COA sections
  // that have no lines yet appear when the search names them; "+ New
  // line…" ends the list.
  FP.linePickerOptions = function (groups, q, opts) {
    opts = opts || {};
    q = (q || '').trim().toLowerCase();
    const out = [];
    const recent = (opts.recent || []).filter(Boolean);
    if (!q && recent.length) {
      out.push({ group: 'Recent' });
      recent.forEach(r => out.push({ value: String(r.value), label: r.label, kind: r.kind || 'line', sub: r.sub || '' }));
    }
    const seen = new Set();
    (groups || []).forEach(g => {
      seen.add(+g.code);
      const secHit = !q || String(g.code).includes(q) || (g.name || '').toLowerCase().includes(q);
      const lines = (g.opts || []).filter(o => !q || secHit || o.label.toLowerCase().includes(q));
      if (!lines.length && !secHit) return;
      out.push({ group: `${g.code} · ${g.name}` });
      if (opts.sections !== false) out.push({ value: 'section:' + g.code, label: `All ${g.code} · ${g.name}`, kind: 'section', sub: 'section, no line' });
      lines.forEach(o => out.push({ value: String(o.id), label: o.label, kind: 'line', child: o.kind === 'child',
        sub: (opts.lineSub ? opts.lineSub(o) : '') }));
    });
    if (q && opts.sections !== false) {
      const extra = (opts.allSections || []).filter(s => !seen.has(+s.code)
        && (String(s.code).includes(q) || (s.name || '').toLowerCase().includes(q)));
      if (extra.length) {
        out.push({ group: 'Other sections' });
        extra.slice(0, 8).forEach(s => out.push({ value: 'section:' + s.code, label: `All ${s.code} · ${s.name}`, kind: 'section', sub: 'not in the budget yet' }));
      }
    }
    if (opts.allowNew !== false) out.push({ value: '__new__', label: q ? `+ New line “${(opts.rawQuery || q)}”…` : '+ New line…', kind: 'new', sub: '' });
    return out;
  };

  FP.newLine = async function (pid, code, description) {
    const d = await jpost(`/projects/${pid}/actuals/budget-line/new`, { account_code: code, description });
    return { value: String(d.line_id), label: `${d.account_code} · ${d.label}`, code: d.account_code,
             sectionName: d.section_name, id: d.line_id };
  };

  // Ask for the new line's section + name in a small inline form. Resolves
  // {code, description} or null. Uses a <dialog> so it works inside the
  // detail window and the Actuals dialogs alike.
  FP.askNewLine = function (groups, allSections, hintQuery) {
    return new Promise(resolve => {
      const dlg = document.createElement('dialog');
      dlg.className = 'fpd-newline';
      const secs = (allSections && allSections.length ? allSections : (groups || []).map(g => ({ code: g.code, name: g.name })));
      const hint = (hintQuery || '').trim();
      const guess = secs.find(s => hint && (String(s.code) === hint || (s.name || '').toLowerCase().includes(hint.toLowerCase())));
      dlg.innerHTML = `<form method="dialog" class="fpd-nl">
        <h3>New budget line</h3>
        <label>Section<select name="code">${secs.map(s => `<option value="${esc(s.code)}" ${guess && +guess.code === +s.code ? 'selected' : ''}>${esc(s.code)} · ${esc(s.name)}</option>`).join('')}</select></label>
        <label>Line name<input name="desc" type="text" maxlength="200" required value="${esc(guess ? '' : hint)}" placeholder="Parking, Editor, Color"></label>
        <div class="fpd-nl-acts"><button type="button" value="cancel" class="fpd-btn">Cancel</button><button type="submit" value="ok" class="fpd-btn primary">Add line</button></div>
      </form>`;
      document.body.appendChild(dlg);
      const done = v => { try { dlg.close(); } catch (e) {} dlg.remove(); resolve(v); };
      dlg.querySelector('[value="cancel"]').addEventListener('click', () => done(null));
      dlg.addEventListener('cancel', e => { e.preventDefault(); done(null); });
      dlg.querySelector('form').addEventListener('submit', e => {
        e.preventDefault();
        const f = e.target;
        const desc = f.desc.value.trim();
        if (!desc) { f.desc.focus(); return; }
        done({ code: +f.code.value, description: desc });
      });
      dlg.showModal();
      setTimeout(() => dlg.querySelector('input[name="desc"]').focus(), 0);
    });
  };

  // ── FP.LinePicker: the searchable combobox ───────────────────────────
  // opts: {groups, allSections, value, label, pid, placeholder, onChange,
  //        recent, lineSub}. picker.value → '123' | 'section:4700' | ''.
  let pickerSeq = 0;
  FP.LinePicker = function (host, opts) {
    opts = opts || {};
    const id = 'fplp' + (++pickerSeq);
    const wrap = document.createElement('div');
    wrap.className = 'fplp';
    wrap.innerHTML = `<input type="text" class="fplp-in" role="combobox" aria-expanded="false" aria-autocomplete="list"
        aria-controls="${id}-list" autocomplete="off" placeholder="${esc(opts.placeholder || 'Search a line, a code or a section')}" aria-label="Budget line or section">
      <div class="fplp-list" id="${id}-list" role="listbox" aria-label="Budget lines"></div>`;
    host.appendChild(wrap);
    const inp = $q(wrap, 'input'), list = $q(wrap, '.fplp-list');
    const api = { value: opts.value ? String(opts.value) : '', label: opts.label || '', el: wrap, input: inp };
    let idx = 0, rows = [];
    function labelFor(v) {
      if (!v) return '';
      if (v.startsWith('section:')) {
        const code = +v.split(':')[1];
        const g = (opts.groups || []).find(x => +x.code === code) || (opts.allSections || []).find(x => +x.code === code);
        return g ? `All ${code} · ${g.name}` : `Section ${code}`;
      }
      for (const g of (opts.groups || [])) { const o = (g.opts || []).find(x => String(x.id) === v); if (o) return o.label; }
      return api.label || ('Line #' + v);
    }
    inp.value = api.label || labelFor(api.value);
    function render() {
      rows = FP.linePickerOptions(opts.groups, inp.dataset.typed === '1' ? inp.value : '',
        { allSections: opts.allSections, recent: opts.recent, lineSub: opts.lineSub, rawQuery: inp.value.trim() });
      const choosable = rows.filter(r => !r.group);
      if (idx >= choosable.length) idx = Math.max(0, choosable.length - 1);
      let i = -1;
      list.innerHTML = rows.map(r => {
        if (r.group) return `<div class="fplp-grp">${esc(r.group)}</div>`;
        i++;
        return `<div class="fplp-opt ${r.kind}${r.child ? ' child' : ''}${r.value === api.value ? ' cur' : ''}" role="option" id="${id}-o${i}"
          data-i="${i}" data-value="${esc(r.value)}" aria-selected="${i === idx}"><span class="l">${esc(r.label)}</span>${r.sub ? `<span class="r">${esc(r.sub)}</span>` : ''}</div>`;
      }).join('');
      const cur = $q(list, '[aria-selected="true"]');
      if (cur) { cur.scrollIntoView({ block: 'nearest' }); inp.setAttribute('aria-activedescendant', cur.id); }
    }
    function open() { wrap.classList.add('open'); inp.setAttribute('aria-expanded', 'true'); render(); }
    function close() { wrap.classList.remove('open'); inp.setAttribute('aria-expanded', 'false'); inp.dataset.typed = ''; inp.value = api.value ? (api.label || labelFor(api.value)) : ''; }
    async function choose(el) {
      const v = el && el.dataset.value; if (!v) return;
      if (v === '__new__') {
        const q = inp.value.trim();
        close();
        const ans = await FP.askNewLine(opts.groups, opts.allSections, q);
        if (!ans || !opts.pid) return;
        try {
          const nl = await FP.newLine(opts.pid, ans.code, ans.description);
          let g = (opts.groups || []).find(x => +x.code === +nl.code);
          if (!g) { g = { code: nl.code, name: nl.sectionName, opts: [] }; (opts.groups = opts.groups || []).push(g); }
          g.opts.push({ id: nl.id, label: nl.label, kind: 'top' });
          set(nl.value, nl.label);
          if (opts.onNewLine) opts.onNewLine(nl);
        } catch (e) { alert('Could not add the line: ' + (e.message || e)); }
        return;
      }
      set(v, $q(el, '.l').textContent);
      close();
    }
    function set(v, label) {
      api.value = v ? String(v) : ''; api.label = label || labelFor(api.value);
      inp.value = api.label;
      wrap.classList.toggle('has', !!api.value);
      if (opts.onChange) opts.onChange(api.value, api.label);
    }
    api.set = set;
    api.focus = () => inp.focus();
    inp.addEventListener('focus', () => { inp.select(); open(); });
    inp.addEventListener('input', () => { inp.dataset.typed = '1'; idx = 0; open(); });
    inp.addEventListener('keydown', e => {
      const n = list.querySelectorAll('[role="option"]').length;
      if (e.key === 'ArrowDown') { e.preventDefault(); if (!wrap.classList.contains('open')) open(); idx = Math.min(n - 1, idx + 1); render(); }
      else if (e.key === 'ArrowUp') { e.preventDefault(); idx = Math.max(0, idx - 1); render(); }
      else if (e.key === 'Enter') { e.preventDefault(); choose($q(list, '[aria-selected="true"]')); }
      else if (e.key === 'Escape' && wrap.classList.contains('open')) { e.preventDefault(); e.stopPropagation(); close(); }
      else if (e.key === 'Tab') close();
    });
    list.addEventListener('mousedown', e => { const o = e.target.closest('[role="option"]'); if (o) { e.preventDefault(); choose(o); } });
    document.addEventListener('mousedown', e => { if (!wrap.contains(e.target) && wrap.classList.contains('open')) close(); });
    wrap.classList.toggle('has', !!api.value);
    return api;
  };

  // ── The detail window ────────────────────────────────────────────────
  const ICON_X = '<svg aria-hidden="true" width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"><path d="M2 2l10 10M12 2L2 12"/></svg>';
  const ICON_LOCK = '<svg aria-hidden="true" width="12" height="12" viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="1.4"><rect x="2" y="5.5" width="8" height="5.5" rx="1"/><path d="M4 5.5V4a2 2 0 014 0v1.5"/></svg>';
  const ICON_ARROW = '<svg aria-hidden="true" width="12" height="12" viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"><path d="M2 6h8M7 3l3 3-3 3"/></svg>';
  const D = { el: null, opts: null, data: null, dirty: false, editing: null };
  const FPDetail = window.FPDetail = {};

  function shell() {
    if (D.el) return D.el;
    const el = document.createElement('aside');
    el.className = 'fpd';
    el.id = 'fpd';
    el.setAttribute('role', 'dialog');
    el.setAttribute('aria-modal', 'false');
    el.setAttribute('aria-labelledby', 'fpd-title');
    el.hidden = true;
    el.innerHTML = `<div class="fpd-head"><div class="fpd-htxt"><span class="fpd-kicker" id="fpd-kicker"></span><h2 id="fpd-title">Details</h2></div>
      <button type="button" class="fpd-x" data-fpd="close" aria-label="Close details">${ICON_X}</button></div>
      <div class="fpd-body" id="fpd-body"><p class="fpd-muted">Loading</p></div>
      <div class="fpd-toast" id="fpd-toast" role="status" aria-live="polite"></div>`;
    document.body.appendChild(el);
    el.addEventListener('click', onClick);
    D.el = el;
    return el;
  }

  FPDetail.isOpen = () => !!(D.el && !D.el.hidden);
  FPDetail.open = async function (opts) {
    D.opts = Object.assign({}, D.opts && D.opts.pid === opts.pid ? { onChange: D.opts.onChange, hooks: D.opts.hooks } : {}, opts);
    D.editing = null;
    const el = shell();
    el.hidden = false;
    document.body.classList.add('fpd-open');
    await load();
    const x = $q(el, '.fpd-x'); if (x && !el.contains(document.activeElement)) x.focus({ preventScroll: true });
  };
  FPDetail.close = function () {
    if (!D.el) return;
    D.el.hidden = true;
    document.body.classList.remove('fpd-open');
    if (D.opts && D.opts.onClose) D.opts.onClose(D.dirty);
    D.dirty = false;
  };
  FPDetail.reload = () => load();

  function url() {
    const o = D.opts;
    return o.tid ? `/projects/${o.pid}/actuals/transaction/${o.tid}/detail.json`
                 : `/projects/${o.pid}/actuals/doc/${o.did}/detail.json`;
  }
  async function load() {
    const body = $q(D.el, '#fpd-body');
    try {
      D.data = await jget(url());
      if (D.data.txn && !D.opts.tid) D.opts.tid = D.data.txn.id;
      render();
    } catch (e) {
      body.innerHTML = `<p class="fpd-err">Could not load: ${esc(e.message || e)}</p>`;
    }
  }
  function tUrl(tail) { return `/projects/${D.opts.pid}/actuals/transaction/${D.data.txn.id}/${tail}`; }
  async function changed(msg, activityId) {
    D.dirty = true;
    if (D.opts.onChange) { try { await D.opts.onChange(); } catch (e) {} }
    await load();
    if (msg) toast(msg, '', activityId);
  }
  function toast(msg, kind, activityId) {
    const t = $q(D.el, '#fpd-toast');
    t.className = 'fpd-toast show' + (kind ? ' ' + kind : '');
    t.innerHTML = `<span>${esc(msg)}</span>` + (activityId && D.data && D.data.undo_bid ? `<button type="button" class="fpd-btn sm" data-fpd="undo" data-aid="${activityId}">Undo</button>` : '');
    clearTimeout(t._h); t._h = setTimeout(() => { t.className = 'fpd-toast'; }, activityId ? 9000 : 5000);
  }

  function badge(label) {
    const cls = ({ QuickBooks: 'qbo', Receipt: 'rcpt', Invoice: 'inv', Manual: 'man', CSV: 'csv' })[label] || 'man';
    return `<span class="fpd-src ${cls}">${esc(label)}</span>`;
  }
  function kv(k, v) { return v == null || v === '' ? '' : `<dt>${esc(k)}</dt><dd>${v}</dd>`; }

  function render() {
    const d = D.data, body = $q(D.el, '#fpd-body');
    if (d.kind === 'doc') return renderDocOnly();
    const t = d.txn;
    $q(D.el, '#fpd-kicker').innerHTML = badge(t.source_label) + (t.pooled ? ' <span class="fpd-chip">In the company pool</span>' : '')
      + (t.excluded ? ' <span class="fpd-chip">Not project</span>' : '');
    $q(D.el, '#fpd-title').textContent = t.vendor || 'Transaction';
    let h = '';
    // Header facts
    const amtEdit = D.editing === 'amount';
    h += `<section class="fpd-sec fpd-facts">
      <div class="fpd-amt">${amtEdit
        ? `<input id="fpd-amt-in" type="text" inputmode="decimal" class="fpd-num" value="${esc(Math.abs(t.amount).toFixed(2))}" aria-label="Amount">
           <button type="button" class="fpd-btn sm primary" data-fpd="amount-save">Save</button><button type="button" class="fpd-btn sm" data-fpd="amount-cancel">Cancel</button>`
        : `<span class="fpd-big ${t.amount < 0 ? 'credit' : ''}">${money(t.amount)}</span>
           ${d.amount_edit && d.amount_edit.allowed
             ? '<button type="button" class="fpd-btn sm" data-fpd="amount-edit">Change amount</button>'
             : `<span class="fpd-lock" title="${esc((d.amount_edit || {}).reason || '')}">${ICON_LOCK} <span>${esc((d.amount_edit || {}).reason || '')}</span></span>`}`}
      </div>
      <dl class="fpd-kv">
        ${kv('Date', esc(t.date || '—'))}
        ${kv('Card / account', t.card_last4 ? '••' + esc(t.card_last4) + (t.account ? ` · account ${esc(t.account)}` : '') : (t.payment_method_label ? esc(t.payment_method_label) + (t.paid_by ? ' · ' + esc(t.paid_by) : '') : (t.account ? 'account ' + esc(t.account) : '')))}
        ${t.qbo ? kv('QuickBooks', `${esc(t.qbo.payee || '—')} · ${esc(t.qbo.type || '')} <span class="fpd-muted">#${esc(t.qbo.id)}</span>${t.qbo.category ? ` · <span class="fpd-muted">${esc(t.qbo.category)}</span>` : ''}`) : ''}
        ${kv('Payee', t.payee ? `<a href="${esc(t.payee.url)}">${esc(t.payee.name)}</a>${t.payee.via ? ` <span class="fpd-muted">· via ${esc(t.payee.via)}</span>` : ''}` : '<span class="fpd-muted">Not linked</span>')}
        ${t.memo ? kv('Memo', esc(t.memo)) : ''}
        ${t.excluded ? kv('Excluded', esc(t.excluded.reason || 'Not a project expense')) : ''}
      </dl>
      <div class="fpd-err" id="fpd-amt-err"></div>
    </section>`;
    // Counted twice
    const ct = d.counted_twice;
    if (ct && ct.kind === 'doc_and_charge') {
      h += `<section class="fpd-sec fpd-warn"><strong>Counted twice.</strong> ${ct.doc_row_id === t.id
        ? `This invoice is counted on its own and as the charge ${esc(ct.charge_vendor || '')} ${money(ct.charge_amount)} on ${esc(ct.charge_date)}.`
        : `Its invoice ${esc(ct.doc_name || '')} is coded on its own as well.`}
        <div class="fpd-acts"><button type="button" class="fpd-btn sm primary" data-fpd="attach-twice" data-charge="${ct.charge_id}" data-doc="${ct.doc_id}">Attach this invoice to the charge</button></div></section>`;
    } else if (ct && ct.kind === 'split_and_itemized') {
      h += `<section class="fpd-sec fpd-warn"><strong>Counted twice.</strong> This charge is split and its invoice is itemized as well.
        <div class="fpd-acts"><button type="button" class="fpd-btn sm primary" data-fpd="fold" data-keep="charge">Keep the split, drop the itemization</button>
        <button type="button" class="fpd-btn sm" data-fpd="fold" data-keep="invoice">Use the invoice's lines</button></div></section>`;
    }
    // Receipt payee differs from QuickBooks (company ledger) — a person and
    // their loan-out company are one payee (actuals.same_payee, 2026-11-04).
    const pd = d.payee_differs;
    if (pd) {
      h += `<section class="fpd-sec fpd-warn" id="fpd-payee-differs"><strong>Payee differs.</strong> The receipt names ${esc(pd.receipt_payee || '—')}; QuickBooks paid ${esc(pd.qbo_payee || '—')}.`;
      if (pd.different_confirmed) h += ` <span class="fpd-muted">Confirmed as different payees.</span>`;
      else if (pd.proposal && pd.can_resolve) {
        const p = pd.proposal;
        h += `<div class="fpd-acts"><button type="button" class="fpd-btn sm" data-fpd="same-person" data-person="${p.person_id}" data-company="${esc(p.company_name)}" data-qbo="${esc(p.qbo_vendor_id || '')}">Same person — ${esc(p.company_name)} is ${esc(p.person_name)}'s loan-out</button>
          <button type="button" class="fpd-btn sm" data-fpd="payee-different">Different</button></div>`;
      } else if (pd.proposal) h += ` <span class="fpd-muted">If ${esc(pd.proposal.company_name)} is ${esc(pd.proposal.person_name)}'s loan-out, an admin can link it here.</span>`;
      else h += ` <span class="fpd-muted">If one is the other's loan-out company, add it on the person's Payee page and this clears.</span>`;
      h += `</section>`;
    }
    if (d.coding && d.coding.parent_still_coded) {
      h += `<section class="fpd-sec fpd-warn">This charge is split but still carries a line of its own, so the line counts it twice. Saving the split below clears it.</section>`;
    }
    h += renderBackup();
    h += renderCoding();
    h += renderHistory();
    body.innerHTML = h;
    mountPreviews();
    if (D.editing === 'split') mountSplit();
    if (D.editing && D.editing.indexOf && D.editing.indexOf('pick:') === 0) mountPick();
    if (amtEdit) { const i = $q(body, '#fpd-amt-in'); if (i) { i.focus(); i.select(); } }
  }

  function renderDocOnly() {
    const d = D.data, doc = d.doc, body = $q(D.el, '#fpd-body');
    $q(D.el, '#fpd-kicker').innerHTML = badge((doc.category || '') === 'invoice' ? 'Invoice' : 'Receipt');
    $q(D.el, '#fpd-title').textContent = doc.vendor || doc.name;
    body.innerHTML = `<section class="fpd-sec fpd-facts"><div class="fpd-amt"><span class="fpd-big">${doc.amount != null ? money(doc.amount) : '—'}</span></div>
      <dl class="fpd-kv">${kv('Date', esc(doc.date || '—'))}${kv('File', esc(doc.name))}</dl>
      <p class="fpd-muted">Waiting in Receipts to place: no charge or expense uses it yet. Match it to a charge from the Receipts view, or create an expense from it.</p></section>`
      + `<section class="fpd-sec"><h3>Backup</h3>${docCard(doc, false)}</section>`;
    mountPreviews();
  }

  function docCard(b, actions) {
    const isInv = (b.category || '') === 'invoice';
    const kindLabel = ({ primary: isInv ? 'Invoice' : 'Receipt', backup: 'Backup', itemized: 'Invoice', suggested: 'Suggested match' })[b.kind] || 'Document';
    return `<div class="fpd-doc" data-doc="${b.doc_id}">
      <div class="fpd-prev" data-prev="${b.doc_id}" data-pdf="${b.is_pdf ? 1 : ''}" data-img="${b.is_image ? 1 : ''}" data-raw="${esc(b.raw_url)}" data-thumb="${esc(b.thumb_url)}" data-link="${esc(b.preview_url)}"><span class="fpd-muted">Loading preview</span></div>
      <div class="fpd-docmeta"><div class="fpd-docname"><span class="fpd-chip">${esc(kindLabel)}</span> <span class="ell">${esc(b.name)}</span></div>
        <div class="fpd-muted">${esc(b.vendor || '')}${b.amount != null ? ' · ' + money(b.amount) : ''}${b.date ? ' · ' + esc(b.date) : ''}</div>
        <div class="fpd-acts">
          <a class="fpd-btn sm" href="${esc(b.raw_url)}" target="_blank" rel="noopener">Open</a>
          ${actions && b.kind === 'suggested' ? `<button type="button" class="fpd-btn sm primary" data-fpd="approve">Approve match</button><button type="button" class="fpd-btn sm" data-fpd="reject">Not a match</button>` : ''}
          ${actions && b.kind !== 'suggested' ? `<button type="button" class="fpd-btn sm" data-fpd="replace" data-doc="${b.doc_id}">Replace</button>
            <button type="button" class="fpd-btn sm danger" data-fpd="detach" data-doc="${b.doc_id}">Detach</button>` : ''}
        </div></div></div>`;
  }

  function renderBackup() {
    const d = D.data, t = d.txn;
    let h = `<section class="fpd-sec"><h3>Backup ${d.needs_backup ? '<span class="fpd-flag red">Needs backup</span>' : (d.has_backup ? '<span class="fpd-flag green">Backed up</span>' : '')}</h3>`;
    const docs = d.backup || [];
    if (!docs.length) h += `<p class="fpd-muted">${t.no_charge_reason ? 'No receipt · ' + esc(t.no_charge_reason) : 'No document attached.'}</p>`;
    docs.forEach(b => { h += docCard(b, t.source !== 'doc_upload'); });
    if (t.source !== 'doc_upload' && !docs.some(b => b.kind !== 'suggested')) {
      h += `<div class="fpd-acts"><button type="button" class="fpd-btn sm primary" data-fpd="find-backup">Find backup</button>
        <button type="button" class="fpd-btn sm" data-fpd="upload">Upload a file</button>
        <input type="file" class="fpd-file" accept="image/*,.pdf,application/pdf" hidden></div>`;
    }
    if (D.editing === 'find') h += `<div class="fpd-find" id="fpd-find"><input type="search" id="fpd-find-q" placeholder="Search receipts: vendor, amount, file" autocomplete="off"><div id="fpd-find-list"><p class="fpd-muted">Loading</p></div></div>`;
    return h + '</section>';
  }

  function chainRow(c, i, mode) {
    const t = D.data.txn;
    const tgt = c.kind === 'section' ? `<span class="fpd-node sec">${esc(c.label)} · section</span>` : `<span class="fpd-node">${esc(c.label || 'No line')}</span>`;
    const docs = (c.docs || []).map(id => { const b = (D.data.backup || []).find(x => x.doc_id === id); return b ? `<span class="fpd-node doc" title="${esc(b.name)}">${esc(b.kind === 'itemized' || (b.category || '') === 'invoice' ? 'Invoice' : 'Receipt')}</span>` : ''; }).join('');
    const acts = mode === 'split' || mode === 'itemized' ? '' : `<span class="fpd-acts">
        <button type="button" class="fpd-btn sm" data-fpd="open-target" data-kind="${c.kind}" data-line="${c.line_id || ''}" data-code="${c.code || c.section || ''}">Open</button>
        <button type="button" class="fpd-btn sm" data-fpd="change" data-i="${i}">Change</button>
        <button type="button" class="fpd-btn sm danger" data-fpd="unlink" data-kind="${c.kind}">Unlink</button></span>`;
    return `<div class="fpd-chain"><span class="fpd-node charge">${mode === 'split' || mode === 'itemized' ? money(c.amount) : 'Charge'}</span>${ICON_ARROW}${tgt}${docs ? ICON_ARROW + docs : ''}${c.note ? `<span class="fpd-muted fpd-note ell" title="${esc(c.note)}">${esc(c.note)}</span>` : ''}${acts}</div>`
      + (D.editing === 'pick:' + i ? `<div class="fpd-pickhost" id="fpd-pickhost"></div>` : '');
  }

  function renderCoding() {
    const d = D.data, c = d.coding || {}, t = d.txn;
    let h = `<section class="fpd-sec"><h3>Coding</h3>`;
    if (t.source === 'doc_upload' && !t.activated) {
      return h + `<p class="fpd-muted">A receipt, not a charge: it counts once it is matched to a charge or turned into an expense.</p></section>`;
    }
    if (D.editing === 'split') {
      return h + `<div id="fpd-split"></div></section>`;
    }
    if (c.mode === 'none') {
      h += `<p class="fpd-muted">Not assigned to a line yet.</p>`;
      h += `<div class="fpd-acts"><button type="button" class="fpd-btn sm primary" data-fpd="change" data-i="0">Assign</button>
        <button type="button" class="fpd-btn sm" data-fpd="split-edit">Split across lines</button></div>`;
      if (D.editing === 'pick:0') h += `<div class="fpd-pickhost" id="fpd-pickhost"></div>`;
    } else if (c.mode === 'split') {
      h += `<p class="fpd-muted">Split ${money(t.amount)} across ${c.chain.length} pieces. The ${d.backup && d.backup.length ? 'document backs every piece' : 'pieces share one backup'}.</p>`;
      c.chain.forEach((x, i) => { h += chainRow(x, i, 'split'); });
      h += `<div class="fpd-acts"><button type="button" class="fpd-btn sm primary" data-fpd="split-edit">Edit the split</button>
        <button type="button" class="fpd-btn sm danger" data-fpd="split-remove">Remove the split</button></div>`;
    } else if (c.mode === 'itemized') {
      h += `<p class="fpd-muted">Itemized by its invoice (the older mechanism).</p>`;
      c.chain.forEach((x, i) => { h += chainRow(x, i, 'itemized'); });
      h += `<div class="fpd-acts"><button type="button" class="fpd-btn sm primary" data-fpd="fold" data-keep="invoice">Turn into a split</button></div>`;
    } else {
      c.chain.forEach((x, i) => { h += chainRow(x, i, c.mode); });
      h += `<div class="fpd-acts"><button type="button" class="fpd-btn sm" data-fpd="split-edit">Split across lines</button></div>`;
    }
    if (d.split_offer && c.mode !== 'split') {
      h += `<div class="fpd-offer"><div><strong>Split this charge by the invoice.</strong> <span class="fpd-muted">${esc(d.split_offer.doc_name)} lists ${d.split_offer.items.length} line items; the one document backs every piece.</span></div>
        <button type="button" class="fpd-btn sm primary" data-fpd="split-offer">Split by the invoice</button></div>`;
    }
    return h + '</section>';
  }

  function renderHistory() {
    const d = D.data;
    const items = d.history || [];
    let h = `<section class="fpd-sec"><h3>History</h3>`;
    if (!items.length) return h + '<p class="fpd-muted">No changes recorded yet.</p></section>';
    h += '<ol class="fpd-hist">';
    items.forEach(x => {
      h += `<li class="${x.undone ? 'undone' : ''}"><div class="fpd-hwho">${esc(x.who)}${FP.aiBadge ? FP.aiBadge(x.by_ai_agent) : ''} <span class="fpd-muted">${esc((x.at || '').replace('T', ' '))}</span>${x.undone ? ' <span class="fpd-chip">Undone</span>' : ''}</div>
        <div class="fpd-hnote">${esc(x.note)}</div>
        ${x.undoable && d.undo_bid ? `<button type="button" class="fpd-btn sm" data-fpd="undo" data-aid="${x.id}">Undo</button>` : ''}</li>`;
    });
    return h + '</ol></section>';
  }

  // Inline previews: a Dropbox temporary link when there is one, else the
  // raw route (PDF in an iframe, an image as <img>), else the thumbnail.
  function mountPreviews() {
    D.el.querySelectorAll('.fpd-prev[data-prev]').forEach(async box => {
      const isPdf = !!box.dataset.pdf, isImg = !!box.dataset.img;
      let src = null;
      try { const r = await jget(box.dataset.link); if (r && r.url) src = r.url; } catch (e) {}
      if (!src) {
        try { const h = await fetch(box.dataset.raw, { method: 'HEAD', credentials: 'same-origin' }); if (h.ok) src = box.dataset.raw; } catch (e) {}
      }
      if (!src) {
        box.innerHTML = `<div class="fpd-noprev"><svg aria-hidden="true" width="28" height="34" viewBox="0 0 28 34" fill="none" stroke="currentColor" stroke-width="1.4"><path d="M3 1h15l7 7v25H3z"/><path d="M18 1v7h7M8 16h12M8 21h12M8 26h8"/></svg><span>${isPdf ? 'PDF' : 'File'} · preview needs Dropbox</span></div>`;
        return;
      }
      if (isPdf) box.innerHTML = `<iframe title="Document preview" src="${esc(src)}#view=FitH" loading="lazy"></iframe>`;
      else if (isImg) box.innerHTML = `<img alt="Document preview" src="${esc(src)}" onerror="this.onerror=null;this.src='${esc(box.dataset.thumb)}'">`;
      else box.innerHTML = `<img alt="Document thumbnail" src="${esc(box.dataset.thumb)}" onerror="this.replaceWith(Object.assign(document.createElement('span'),{className:'fpd-muted',textContent:'No preview'}))">`;
    });
  }

  // ── Split editor (the SAME picker as Assign) ─────────────────────────
  let splitRows = [];
  function mountSplit() {
    const d = D.data, t = d.txn, host = $q(D.el, '#fpd-split');
    host.innerHTML = `<div class="fpd-srows" id="fpd-srows"></div>
      <div class="fpd-acts"><button type="button" class="fpd-btn sm" data-fpd="split-add">Add a piece</button></div>
      <div class="fpd-remain" id="fpd-remain"><span>Remainder</span><span class="fpd-num" id="fpd-remain-amt"></span></div>
      <div class="fpd-err" id="fpd-split-err"></div>
      <div class="fpd-acts"><button type="button" class="fpd-btn sm" data-fpd="split-cancel">Cancel</button>
        <button type="button" class="fpd-btn sm primary" data-fpd="split-save" id="fpd-split-save" disabled>Save split</button></div>`;
    const list = $q(host, '#fpd-srows');
    splitRows.forEach(r => addSplitRow(list, r));
    recalcSplit();
  }
  function addSplitRow(list, r) {
    const row = document.createElement('div');
    row.className = 'fpd-srow';
    row.innerHTML = `<div class="fpd-spick"></div><input type="text" inputmode="decimal" class="fpd-num fpd-samt" placeholder="0.00" aria-label="Amount" value="${r.amount != null && r.amount !== '' ? esc(Number(r.amount).toFixed(2)) : ''}">
      <input type="text" class="fpd-snote" placeholder="Note" aria-label="Note" maxlength="300" value="${esc(r.note || '')}">
      <button type="button" class="fpd-x sm" data-fpd="split-rm" aria-label="Remove this piece">${ICON_X}</button>`;
    list.appendChild(row);
    row._picker = FP.LinePicker($q(row, '.fpd-spick'), { groups: D.data.picker, allSections: D.data.sections, value: r.value || '', label: r.label || '',
      pid: D.opts.pid, onChange: recalcSplit, onNewLine: () => { D.dirty = true; } });
    $q(row, '.fpd-samt').addEventListener('input', recalcSplit);
    return row;
  }
  function readSplit() {
    return Array.from(D.el.querySelectorAll('#fpd-srows .fpd-srow')).map(r => ({
      value: r._picker.value, amount: $q(r, '.fpd-samt').value, note: $q(r, '.fpd-snote').value.trim() }));
  }
  function recalcSplit() {
    const t = D.data.txn, rows = readSplit();
    const total = Math.round(Math.abs(t.raw_amount) * 100);
    const sum = rows.reduce((a, r) => a + (isNaN(cents(r.amount)) ? 0 : cents(r.amount)), 0);
    const remain = (total - sum) / 100;
    const amt = $q(D.el, '#fpd-remain-amt'); if (!amt) return;
    amt.textContent = money(remain);
    const complete = rows.length >= 2 && rows.every(r => r.value && !isNaN(cents(r.amount)) && cents(r.amount) !== 0);
    const ok = complete && total === sum;
    $q(D.el, '#fpd-remain').className = 'fpd-remain ' + (ok ? 'ok' : (rows.some(r => r.amount) ? 'bad' : ''));
    $q(D.el, '#fpd-split-save').disabled = !ok;
    $q(D.el, '#fpd-split-err').textContent = rows.length < 2 ? 'A split needs at least two pieces.' : (complete && !ok ? `Off by ${money(remain)} — the pieces must add up to ${money(Math.abs(t.raw_amount))}.` : '');
  }
  function startSplit(prefill) {
    const d = D.data, c = d.coding || {};
    if (prefill) splitRows = prefill;
    else if (c.mode === 'split') splitRows = (c.splits || []).map(s => ({ value: s.picker_value, label: s.label, amount: s.amount, note: s.note }));
    else {
      const cur = (c.chain || [])[0];
      splitRows = [{ value: cur ? cur.picker_value : '', label: cur ? (cur.kind === 'section' ? `All ${cur.label}` : cur.label) : '', amount: '' }, { value: '', amount: '' }];
    }
    D.editing = 'split';
    render();
  }

  // ── Single target picker (Assign / Change) ───────────────────────────
  function mountPick() {
    const host = $q(D.el, '#fpd-pickhost'); if (!host) return;
    const p = FP.LinePicker(host, { groups: D.data.picker, allSections: D.data.sections, pid: D.opts.pid,
      onNewLine: () => { D.dirty = true; },
      onChange: async v => { if (v) await applyTarget(v); } });
    host.insertAdjacentHTML('beforeend', '<button type="button" class="fpd-btn sm" data-fpd="pick-cancel">Cancel</button>');
    setTimeout(() => p.focus(), 0);
  }
  async function applyTarget(v) {
    try {
      let r;
      if (v.startsWith('section:')) r = await jpost(tUrl('set-coa'), { account_code: +v.split(':')[1] });
      else r = await jpost(tUrl('set-line'), { budget_line_id: +v });
      D.editing = null;
      await changed('Coding saved', r.activity_id);
    } catch (e) { D.editing = null; render(); toast(e.message || 'Could not save', 'red'); }
  }

  async function findBackup(q) {
    const list = $q(D.el, '#fpd-find-list'); if (!list) return;
    try {
      const r = await jget(tUrl('receipt-candidates') + (q ? '?q=' + encodeURIComponent(q) : ''));
      const rows = (r.candidates || []).concat(r.pool || []);
      list.innerHTML = rows.length ? rows.map(c => `<div class="fpd-cand"><span class="ell"><strong>${esc(c.vendor || c.name)}</strong> <span class="fpd-muted">${esc(c.date || '')} · ${c.amount != null ? money(c.amount) : '—'}${c.confidence ? ' · ' + Math.round(c.confidence * 100) + '%' : ''}</span></span>
        <button type="button" class="fpd-btn sm" data-fpd="link" data-doc="${c.doc_id}">Link</button></div>`).join('')
        : '<p class="fpd-muted">No receipt in the inbox looks like this charge. Search, or upload one.</p>';
    } catch (e) { list.innerHTML = `<p class="fpd-err">${esc(e.message)}</p>`; }
  }

  // ── Clicks ───────────────────────────────────────────────────────────
  async function onClick(e) {
    const b = e.target.closest('[data-fpd]'); if (!b) return;
    const act = b.dataset.fpd, d = D.data;
    const run = async (fn, okMsg) => {
      b.disabled = true;
      try { const r = await fn(); await changed(okMsg, r && r.activity_id); }
      catch (x) { b.disabled = false; toast(x.message || 'Could not save', 'red'); }
    };
    if (act === 'close') return FPDetail.close();
    if (act === 'undo') {
      return run(() => jpost(`/projects/${D.opts.pid}/budget/${d.undo_bid}/activity/${b.dataset.aid}/undo`, {}), 'Undone');
    }
    if (act === 'amount-edit') { D.editing = 'amount'; return render(); }
    if (act === 'amount-cancel') { D.editing = null; return render(); }
    if (act === 'amount-save') {
      const v = $q(D.el, '#fpd-amt-in').value.replace(/[$,]/g, '');
      if (isNaN(parseFloat(v))) { $q(D.el, '#fpd-amt-err').textContent = 'Amount must be a number.'; return; }
      b.disabled = true;
      try { const r = await jpost(tUrl('edit'), { amount: Math.abs(parseFloat(v)) }); D.editing = null; await changed('Amount changed', r.activity_id); }
      catch (x) { b.disabled = false; $q(D.el, '#fpd-amt-err').textContent = x.message; }
      return;
    }
    if (act === 'find-backup') { D.editing = D.editing === 'find' ? null : 'find'; render(); if (D.editing) { findBackup(''); const q = $q(D.el, '#fpd-find-q'); let h; q.addEventListener('input', () => { clearTimeout(h); h = setTimeout(() => findBackup(q.value.trim()), 200); }); q.focus(); } return; }
    if (act === 'link') { D.editing = null; return run(() => jpost(tUrl('link-doc'), { doc_upload_id: +b.dataset.doc }), 'Receipt linked'); }
    if (act === 'upload' || act === 'replace') {
      const doReplace = act === 'replace';
      let fi = $q(D.el, '.fpd-file');
      if (!fi) { fi = document.createElement('input'); fi.type = 'file'; fi.accept = 'image/*,.pdf,application/pdf'; fi.hidden = true; fi.className = 'fpd-file'; D.el.appendChild(fi); }
      fi.onchange = async () => {
        const f = fi.files && fi.files[0]; fi.value = ''; if (!f) return;
        toast('Uploading ' + f.name + ' and reading it with OCR');
        try {
          if (doReplace) await jpost(tUrl('detach-doc'), { doc_id: +b.dataset.doc });
          const fd = new FormData(); fd.append('file', f);
          const r = await fetch(tUrl('upload-receipt'), { method: 'POST', credentials: 'same-origin', body: fd });
          const j = await r.json().catch(() => ({}));
          if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status));
          await changed(doReplace ? 'Receipt replaced' : 'Receipt attached', j.activity_id);
        } catch (x) { toast(x.message || 'Upload failed', 'red'); await load(); }
      };
      fi.click();
      return;
    }
    if (act === 'detach') {
      if (!confirm('Detach this document? A receipt goes back to Receipts to place; the charge keeps its coding and shows Needs backup.')) return;
      return run(() => jpost(tUrl('detach-doc'), { doc_id: +b.dataset.doc }), 'Document detached');
    }
    if (act === 'approve') return run(() => jpost(tUrl('confirm-match'), {}), 'Receipt match approved');
    if (act === 'reject') return run(() => jpost(tUrl('dismiss-suggestion'), { remember: true }), 'Not a match. It will not be suggested again.');
    if (act === 'attach-twice') {
      return run(() => jpost(`/projects/${D.opts.pid}/actuals/transaction/${b.dataset.charge}/link-doc`, { doc_upload_id: +b.dataset.doc }), 'Invoice attached to the charge — counted once now');
    }
    if (act === 'same-person') {
      const company = b.dataset.company, who = (d.payee_differs.proposal || {}).person_name || 'this person';
      if (!confirm(`Link ${company} as ${who}'s loan-out company and clear this flag?\n\nCharges and invoices from ${company} will count as ${who}'s. Nothing changes in QuickBooks; the link can be removed on the Payee page.`)) return;
      return run(() => jpost('/admin/ledger/conflicts/same-person', { txn_id: d.txn.id, person_id: +b.dataset.person, company_name: company, qbo_vendor_id: b.dataset.qbo || null }),
        `Linked — ${company} is ${who}'s loan-out`);
    }
    if (act === 'payee-different') {
      if (!confirm('Keep these as different payees? The flag stays until the receipt is unmatched or cleared on the ledger review page.')) return;
      return run(() => jpost('/admin/ledger/conflicts/different', { txn_id: d.txn.id }), 'Kept as different payees');
    }
    if (act === 'fold') {
      return run(() => jpost(tUrl('fold-itemization'), { keep: b.dataset.keep }), 'The itemization is the split now');
    }
    if (act === 'change') { D.editing = 'pick:' + (b.dataset.i || 0); return render(); }
    if (act === 'pick-cancel') { D.editing = null; return render(); }
    if (act === 'unlink') {
      if (b.dataset.kind === 'section') return run(() => jpost(tUrl('set-coa'), { account_code: null }), 'Unassigned — back in Unclaimed');
      return run(() => jpost(tUrl('set-line'), { budget_line_id: null }), 'Unassigned — back in Unclaimed');
    }
    if (act === 'open-target') {
      const h = (D.opts.hooks || {});
      if (b.dataset.kind === 'line' && h.openLine) return h.openLine(+b.dataset.line);
      if (b.dataset.kind === 'section' && h.openSection) return h.openSection(+b.dataset.code);
      return;
    }
    if (act === 'split-edit') return startSplit();
    if (act === 'split-offer') {
      const o = d.split_offer;
      return startSplit(o.items.map(it => ({ value: it.picker_value || '', amount: it.amount, note: it.description })));
    }
    if (act === 'split-cancel') { D.editing = null; return render(); }
    if (act === 'split-add') { addSplitRow($q(D.el, '#fpd-srows'), { value: '', amount: '' }); return recalcSplit(); }
    if (act === 'split-rm') { b.closest('.fpd-srow').remove(); return recalcSplit(); }
    if (act === 'split-remove') {
      if (!confirm('Remove the split? The charge goes back to Unclaimed until it is assigned again.')) return;
      return run(() => jpost(tUrl('split'), {}, 'DELETE'), 'Split removed');
    }
    if (act === 'split-save') {
      const rows = readSplit().map(r => {
        const sec = r.value.startsWith('section:');
        return { budget_line_id: sec ? null : +r.value, account_code: sec ? +r.value.split(':')[1] : null,
                 amount: cents(r.amount) / 100, note: r.note || null };
      });
      b.disabled = true;
      try {
        const r = await jpost(tUrl('split'), { splits: rows });
        D.editing = null; splitRows = [];
        await changed(`Split across ${rows.length} pieces`, r.activity_id);
      } catch (x) {
        b.disabled = false;
        const dd = x.data || {};
        $q(D.el, '#fpd-split-err').textContent = (x.message || 'Could not save') + (dd.delta != null ? ` (off by ${money(dd.delta)})` : '');
      }
    }
  }
  document.addEventListener('keydown', e => {
    if (e.key === 'Escape' && FPDetail.isOpen() && !document.querySelector('dialog[open]')
        && !(e.target.closest && e.target.closest('.fplp.open'))) {
      FPDetail.close();
    }
  });
})();
