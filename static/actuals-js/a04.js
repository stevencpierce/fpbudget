/* FPBudget Actuals workspace — a04: matching and duplicates.
   (ACTUALS_HANDOFF Phase 5, 2026-11-03.)

   Three pieces, all fed by ONE server rule (actuals.match_candidates /
   find_duplicate_groups):
     1. The candidate panel. Selecting a receipt in the Receipts view, or
        Find backup on a charge, swaps the budget pane for a ranked list
        from the engine (top 8, with confidence and reasons) plus a search
        box over every open row on the other side. Tick a charge → the
        receipt row's "Link to selected" turns on; or click Link here.
        A receipt row can also be dragged onto a candidate charge.
     2. Review matches: a dialog listing every suggested pair — receipt |
        charge | confidence + reasons — with Approve / Not a match and
        Approve all strong. Approve = confirm-match (absorbs the receipt's
        placeholder row), never automatic.
     3. The Duplicates view: groups of charges that look like one event,
        the suggested keeper marked, with Keep both (remembered), Delete
        the extra (confirm dialog showing both rows; coding moves to the
        keeper first when the extra carries any) and Not a project
        expense. Rows QuickBooks voided after import sit under a Voided
        group with Recover / Acknowledge. */
(function () {
  'use strict';
  const AW = window.AW;
  const S = AW.state, $ = AW.$, PID = AW.PID, esc = AW.esc, money = AW.money, shortDate = AW.shortDate;
  const A = AW.actions = AW.actions || {};
  const panel = $('aw-panel');
  const right = document.querySelector('.aw-right');

  // ── Candidate panel ──────────────────────────────────────────────────
  let P = { kind: null, id: null, data: null, q: '', loading: false };
  AW.panelActive = () => !!P.kind;
  const pct = c => (c == null ? '' : Math.round(c * 100) + '%');
  const why = r => (r && r.length ? r.join(' · ') : '');

  async function fetchPanel() {
    if (!P.kind) return;
    const url = P.kind === 'doc'
      ? `/projects/${PID}/actuals/doc/${P.id}/charge-candidates`
      : AW.txnUrl(P.id, 'receipt-candidates');
    P.loading = true;
    try {
      P.data = await AW.get(url + '?limit=8' + (P.q ? '&q=' + encodeURIComponent(P.q) : ''));
    } catch (e) {
      P.data = { error: e.message || String(e) };
    } finally { P.loading = false; }
    AW.renderPanel();
  }

  AW.openDocPanel = function (docId) {
    P = { kind: 'doc', id: docId, data: null, q: '', loading: true };
    right.classList.add('panel-open'); panel.hidden = false;
    AW.renderPanel(); fetchPanel();
  };
  AW.openChargePanel = function (tid) {
    S.findFor = tid;
    P = { kind: 'txn', id: tid, data: null, q: '', loading: true };
    right.classList.add('panel-open'); panel.hidden = false;
    AW.renderPanel(); fetchPanel();
  };
  AW.closePanel = function (quiet) {
    P = { kind: null, id: null, data: null, q: '', loading: false };
    S.findFor = null;
    right.classList.remove('panel-open'); panel.hidden = true; panel.innerHTML = '';
    if (!quiet) AW.render();
  };
  AW.selectDoc = function (docId) {
    if (S.selDoc === docId) { S.selDoc = null; S.pickCharge = null; AW.closePanel(); return; }
    S.selDoc = docId; S.pickCharge = null;
    AW.openDocPanel(docId);
    AW.render();
  };

  function candRow(c, kind) {
    // kind: 'charge' rows (doc panel) or 'receipt' rows (charge panel).
    const isCharge = kind === 'charge';
    const id = isCharge ? c.id : c.doc_id;
    const picked = isCharge ? (S.pickCharge === id) : (P.pickDoc === id);
    const conf = c.confidence;
    const l2 = [];
    if (isCharge) {
      l2.push(esc(shortDate(c.date)));
      if (c.card_last4) l2.push('••' + esc(c.card_last4)); else if (c.qbo_txn_type === 'BillPayment') l2.push('ACH');
      if (c.assigned) l2.push('assigned');
    } else {
      l2.push(esc(shortDate(c.date)));
      l2.push(esc(c.name || ''));
    }
    const reasons = why(c.reasons);
    return `<div class="aw-cand${picked ? ' picked' : ''}${c.strong ? ' strong' : ''}" data-cand="${id}" data-kind="${kind}" tabindex="0" role="option" aria-selected="${picked}" title="${esc(reasons)}">
      <input type="radio" name="aw-cand-pick" ${picked ? 'checked' : ''} aria-label="Tick ${esc(c.vendor || '')}" tabindex="-1">
      <div class="main">
        <div class="l1">${!isCharge ? `<img class="aw-thumb" style="width:28px;height:28px" src="${esc(c.thumb)}" alt="" onerror="this.style.visibility='hidden'">` : ''}<span class="vendor">${esc(c.vendor || '—')}</span><span class="amt num">${c.amount != null ? money(c.amount) : '—'}</span></div>
        <div class="l2">${l2.join(' · ')}${reasons ? ' · ' + esc(reasons) : ''}</div>
      </div>
      <div class="acts2">
        ${conf != null ? `<span class="conf${c.strong ? ' strong' : ''}">${pct(conf)}${c.strong ? ' · strong' : ''}</span>` : '<span class="conf muted">search</span>'}
        <button type="button" class="aw-btn sm${c.strong ? ' primary' : ''}" data-act="panel-link" data-id="${id}" data-kind="${kind}">Link</button>
      </div>
    </div>`;
  }

  AW.renderPanel = function () {
    if (!P.kind) return;
    const d = P.data || {};
    let head, sub, facts = '';
    if (P.kind === 'doc') {
      const r = S.docById.get(P.id) || d.doc || {};
      head = 'Charges this receipt could back';
      sub = 'Ranked by amount, date, vendor and card. Tick one, then Link to selected on the receipt row — or Link here.';
      facts = `<div class="facts"><img class="aw-thumb" src="${esc(r.thumb || '')}" alt="" onerror="this.style.visibility='hidden'">
        <div style="min-width:0"><div class="ell" style="font-weight:600">${esc(r.vendor || r.name || 'Receipt')}</div>
        <div class="muted num" style="font-size:12px">${esc(shortDate(r.date))} · ${r.amount != null ? money(r.amount) : '—'}</div></div></div>`;
    } else {
      const t = S.byId.get(P.id) || {};
      head = 'Receipts that could back this charge';
      sub = 'Ranked by amount, date, vendor and card; unfiled inbox receipts included.';
      facts = `<div class="facts"><div style="min-width:0"><div class="ell" style="font-weight:600">${esc(t.vendor || 'Charge')}</div>
        <div class="muted num" style="font-size:12px">${esc(shortDate(t.date))} · ${money(t.amount)}${t.card_last4 ? ' · ••' + esc(t.card_last4) : ''}</div></div></div>`;
    }
    const kind = P.kind === 'doc' ? 'charge' : 'receipt';
    let body = '';
    if (P.loading && !P.data) body = '<p class="aw-empty">Finding candidates</p>';
    else if (d.error) body = `<p class="aw-empty">Could not rank candidates: ${esc(d.error)}</p>`;
    else {
      const cands = d.candidates || [], pool = d.pool || [];
      if (cands.length) body += `<div class="aw-pgrp2">Suggested · top ${cands.length}</div>` + cands.map(c => candRow(c, kind)).join('');
      else body += `<p class="aw-empty">Nothing close on amount and date${P.q ? '' : ' — search for it below'}.</p>`;
      if (P.q) {
        const seen = new Set(cands.map(c => kind === 'charge' ? c.id : c.doc_id));
        const extra = pool.filter(c => !seen.has(kind === 'charge' ? c.id : c.doc_id));
        body += `<div class="aw-pgrp2">Search · ${extra.length} ${kind === 'charge' ? 'unmatched charge' : 'open receipt'}${extra.length === 1 ? '' : 's'}</div>`;
        body += extra.length ? extra.map(c => candRow(c, kind)).join('') : '<p class="aw-empty">No match for that search.</p>';
      }
    }
    panel.innerHTML = `<div class="aw-phead">
        <div class="top"><div><h2>${head}</h2><div class="sub">${sub}</div></div>
          <button type="button" class="aw-close" data-act="panel-close" aria-label="Back to the budget"><svg aria-hidden="true" width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"><path d="M2 2l10 10M12 2L2 12"/></svg></button></div>
        ${facts}
        <label for="aw-panel-q" class="vis-hidden">Search ${kind === 'charge' ? 'charges' : 'receipts'}</label>
        <input id="aw-panel-q" type="search" placeholder="${kind === 'charge' ? 'Search every unmatched charge: vendor, amount, date, last four' : 'Search every open receipt: vendor, amount, date, file'}" autocomplete="off" value="${esc(P.q)}">
      </div>
      <div class="aw-plist2" id="aw-plist2" role="listbox" aria-label="Candidates">${body}</div>
      <div class="aw-pfoot"><span>${kind === 'charge' ? 'Drop a receipt row on a charge to link it' : 'Linking makes the receipt this charge’s backup'}</span><button type="button" class="aw-btn sm" data-act="panel-close">Back to budget</button></div>`;
    const q = $('aw-panel-q');
    let timer = null;
    q.addEventListener('input', () => { clearTimeout(timer); timer = setTimeout(() => { P.q = q.value.trim(); const pos = q.selectionStart; fetchPanel().then(() => { const nq = $('aw-panel-q'); if (nq) { nq.focus(); try { nq.setSelectionRange(pos, pos); } catch (e) {} } }); }, 220); });
  };

  panel.addEventListener('click', e => {
    const b = e.target.closest('[data-act]');
    if (b) {
      if (b.dataset.act === 'panel-close') { S.selDoc = null; S.pickCharge = null; AW.closePanel(); return; }
      if (b.dataset.act === 'panel-link') {
        const id = +b.dataset.id;
        if (b.dataset.kind === 'charge') AW.linkDocToCharge(P.id, id);
        else AW.linkDocToCharge(id, P.id);
        return;
      }
    }
    const row = e.target.closest('.aw-cand');
    if (!row) return;
    const id = +row.dataset.cand;
    if (row.dataset.kind === 'charge') { S.pickCharge = (S.pickCharge === id ? null : id); AW.renderPanel(); AW.render(); }
    else { P.pickDoc = (P.pickDoc === id ? null : id); AW.renderPanel(); }
  });
  panel.addEventListener('keydown', e => {
    const row = e.target.closest && e.target.closest('.aw-cand');
    if (!row || e.target !== row) return;
    if (e.key === ' ') { e.preventDefault(); row.click(); }
    if (e.key === 'Enter') { e.preventDefault(); const b = row.querySelector('[data-act="panel-link"]'); if (b) b.click(); }
  });

  // Drag a receipt row onto a candidate charge in the panel.
  let dragDoc = null;
  $('aw-table').addEventListener('dragstart', e => {
    const r = e.target.closest && e.target.closest('.aw-row[data-doc]');
    if (!r) return;
    if (e.target.closest('button, a, input')) { e.preventDefault(); return; }
    dragDoc = +r.dataset.doc;
    try { e.dataTransfer.setData('text/plain', 'doc:' + dragDoc); e.dataTransfer.effectAllowed = 'link'; } catch (x) {}
    if (S.selDoc !== dragDoc) AW.selectDoc(dragDoc);
  });
  $('aw-table').addEventListener('dragend', () => { dragDoc = null; panel.querySelectorAll('.aw-cand.picked-drag').forEach(x => x.classList.remove('picked-drag')); });
  panel.addEventListener('dragover', e => {
    if (!dragDoc) return;
    const row = e.target.closest && e.target.closest('.aw-cand[data-kind="charge"]');
    panel.querySelectorAll('.aw-cand.picked').forEach(x => { if (x !== row) x.classList.remove('picked'); });
    if (!row) return;
    e.preventDefault(); e.dataTransfer.dropEffect = 'link'; row.classList.add('picked');
  });
  panel.addEventListener('drop', async e => {
    if (!dragDoc) return;
    const row = e.target.closest && e.target.closest('.aw-cand[data-kind="charge"]');
    if (!row) return;
    e.preventDefault();
    const doc = dragDoc; dragDoc = null;
    await AW.linkDocToCharge(doc, +row.dataset.cand);
  });

  A['find-backup'] = btn => {
    const tid = +btn.dataset.tid; if (!tid) return;
    S.selDoc = null; S.pickCharge = null;
    AW.openChargePanel(tid);
  };

  // ── Review matches ───────────────────────────────────────────────────
  function pairs() {
    return ((S.data || {}).transactions || []).filter(t => t.receipt_suggestion)
      .sort((a, b) => (b.receipt_suggestion.strong - a.receipt_suggestion.strong) || (b.receipt_suggestion.confidence - a.receipt_suggestion.confidence));
  }
  function renderReview() {
    const ps = pairs();
    const host = $('aw-review-list');
    const strong = ps.filter(t => t.receipt_suggestion.strong).length;
    $('aw-review-sub').textContent = ps.length ? `${ps.length} pair${ps.length === 1 ? '' : 's'} waiting · ${strong} strong. Nothing links until you approve it.` : 'No suggested matches right now.';
    const all = $('aw-review-allstrong'); all.disabled = !strong; all.textContent = `Approve all strong${strong ? ' (' + strong + ')' : ''}`;
    // Invoices counted twice (2026-11-04): an invoice coded on its own AND a
    // charge with the same payee and amount — "Attach this invoice to the
    // charge — it's counted twice". Attaching folds them into one.
    const twice = ((S.data || {}).transactions || []).filter(t => t.counted_twice && t.counted_twice.kind === 'doc_and_charge' && t.counted_twice.charge_id === t.id);
    let twiceHtml = '';
    if (twice.length) {
      twiceHtml = `<div class="aw-rhead">Counted twice · attach the invoice to its charge</div>` + twice.map(t => {
        const c = t.counted_twice;
        return `<div class="aw-rpair" data-tid="${t.id}" style="box-shadow:inset 3px 0 0 var(--yellow)">
          <a href="/docs/upload/${c.doc_id}/raw" target="_blank" rel="noopener"><img class="aw-thumb" src="/docs/upload/${c.doc_id}/thumb" alt="" loading="lazy" onerror="this.style.visibility='hidden'"></a>
          <div class="side"><span class="v">${esc(c.doc_name || 'Invoice')}</span><span class="m num">${esc(shortDate(c.doc_date))} · coded on its own</span></div>
          <div class="arrow" aria-hidden="true">→</div>
          <div class="side"><span class="v">${esc(t.vendor)}</span><span class="m num">${esc(shortDate(t.date))} · ${money(t.amount)}</span></div>
          <div class="why"><span class="pct">Counted twice</span><span>same payee and amount</span></div>
          <div class="acts"><button type="button" class="aw-btn sm primary" data-act="review-attach" data-tid="${t.id}" data-doc="${c.doc_id}">Attach</button>
            <button type="button" class="aw-btn sm" data-act="review-open" data-tid="${t.id}">Details</button></div>
        </div>`;
      }).join('');
      $('aw-review-sub').textContent = `${ps.length} pair${ps.length === 1 ? '' : 's'} waiting · ${strong} strong · ${twice.length} counted twice. Nothing links until you approve it.`;
    }
    if (!ps.length) { host.innerHTML = twiceHtml || '<p class="aw-empty">Every receipt is linked or has no candidate. Upload a receipt or sync QuickBooks and the engine will pair them.</p>'; return; }
    host.innerHTML = twiceHtml + `<div class="aw-rhead" style="display:grid;grid-template-columns:48px minmax(0,1.2fr) 24px minmax(0,1.2fr) 150px 190px;gap:12px"><span></span><span>Receipt</span><span></span><span>Charge</span><span>Confidence</span><span></span></div>`
      + ps.map(t => {
        const r = t.receipt_suggestion;
        return `<div class="aw-rpair${r.strong ? ' strong' : ''}" data-tid="${t.id}">
          <a href="/docs/upload/${r.doc_id}/raw" target="_blank" rel="noopener"><img class="aw-thumb" src="${esc(r.thumb)}" alt="" loading="lazy" onerror="this.style.visibility='hidden'"></a>
          <div class="side"><span class="v">${esc(r.vendor || r.name)}</span><span class="m num">${esc(shortDate(r.date))} · ${r.amount != null ? money(r.amount) : '—'} · ${esc(r.name)}</span></div>
          <div class="arrow" aria-hidden="true">→</div>
          <div class="side"><span class="v">${esc(t.vendor)}</span><span class="m num">${esc(shortDate(t.date))} · ${money(t.amount)}${t.card_last4 ? ' · ••' + esc(t.card_last4) : ''}${t.assigned_line ? ' · ' + esc(t.assigned_line.label) : ''}</span></div>
          <div class="why"><span class="pct">${pct(r.confidence)}${r.strong ? ' · strong' : ''}</span><span>${esc(why(r.reasons))}</span></div>
          <div class="acts"><button type="button" class="aw-btn sm${r.strong ? ' primary' : ''}" data-act="review-approve" data-tid="${t.id}">Approve</button><button type="button" class="aw-btn sm" data-act="review-reject" data-tid="${t.id}">Not a match</button></div>
        </div>`;
      }).join('');
  }
  A['review-matches'] = () => {
    renderReview();
    const dlg = $('aw-review'); if (!dlg.open) dlg.showModal();
  };
  $('aw-review').addEventListener('click', async e => {
    const b = e.target.closest('[data-act]'); if (!b) return;
    const act = b.dataset.act;
    if (act === 'review-open') { $('aw-review').close(); AW.openDetail(+b.dataset.tid); return; }
    if (act === 'review-attach') {
      b.disabled = true;
      try {
        const r = await AW.post(AW.txnUrl(+b.dataset.tid, 'link-doc'), { doc_upload_id: +b.dataset.doc });
        AW.toast('Invoice attached — it counts once now', '', { activityId: r.activity_id });
        await AW.load({ keepFocus: true }); renderReview();
      } catch (x) { b.disabled = false; AW.toast(x.message || 'Could not attach', 'red'); }
      return;
    }
    if (act === 'review-approve' || act === 'review-reject') {
      const tid = +b.dataset.tid; b.disabled = true;
      try {
        const r = await AW.post(AW.txnUrl(tid, act === 'review-approve' ? 'confirm-match' : 'dismiss-suggestion'), act === 'review-approve' ? {} : { remember: true });
        AW.toast(act === 'review-approve' ? 'Match approved' : 'Not a match. It will not be suggested again.', '', { activityId: r.activity_id });
        await AW.load({ keepFocus: true }); renderReview();
      } catch (x) { b.disabled = false; AW.toast(x.message || 'Could not update', 'red'); }
    } else if (act === 'approve-all-strong') {
      const tids = pairs().filter(t => t.receipt_suggestion.strong).map(t => t.id);
      if (!tids.length) return;
      b.disabled = true;
      try {
        const r = await AW.post(`/projects/${PID}/actuals/matches/confirm-bulk`, { tids });
        AW.toast(`${r.confirmed || 0} strong match${r.confirmed === 1 ? '' : 'es'} approved${r.failed ? ', ' + r.failed + ' failed' : ''}`, '');
        await AW.load({ keepFocus: true }); renderReview();
      } catch (x) { AW.toast(x.message || 'Could not approve', 'red'); }
      finally { b.disabled = false; }
    }
  });

  // ── Duplicates view ──────────────────────────────────────────────────
  function srcChip(src) {
    if (src === 'qbo_sync' || src === 'reconciled') return '<span class="aw-src">QBO</span>';
    if ((src || '').includes('csv')) return '<span class="aw-src">CSV</span>';
    if (src === 'receipt') return '<span class="aw-src receipt">Receipt</span>';
    return '<span class="aw-src manual">Manual</span>';
  }
  function coded(m) {
    if (m.splits && m.splits.length) return `Split · ${m.splits.length} lines`;
    if (m.assigned_line) return m.assigned_line.label;
    return '';
  }
  AW.renderDuplicates = function (groups, voided, filterFn) {
    let html = '';
    const vis = groups.filter(g => g.members.some(m => filterFn(S.byId.get(m.id) || m)));
    if (!vis.length && !voided.length) return `<p class="aw-empty">No charge looks like a repeat of another. Groups appear when the same amount and vendor post twice within a few days.</p>`;
    vis.forEach(g => {
      const n = g.members.length;
      html += `<div class="aw-dgrp" role="row" id="dup-${esc(g.key)}" data-key="${esc(g.key)}">
        <span class="v">${esc(g.vendor)}</span><span class="num">${money(g.amount)}</span><span class="k">${n} charges · keep the one ${esc(g.keeper_reason)}</span>
        <span class="acts"><button type="button" class="aw-btn sm primary" data-act="dup-compare" data-key="${esc(g.key)}" title="Compare these charges side by side and settle them here">Compare</button>
          <button type="button" class="aw-btn sm" data-act="dup-keep-both" data-key="${esc(g.key)}" title="These are two real charges — do not flag them again">Keep both</button></span></div>`;
      g.members.forEach(m => {
        const t = S.byId.get(m.id) || m;
        const isK = m.id === g.keeper_id;
        const tag = isK ? '<span class="aw-badge">Keeper</span>' : '<span class="aw-badge extra">Extra</span>';
        const bk = m.receipt ? '<span class="aw-flag green">Receipt</span>' : (m.has_backup ? '<span class="aw-flag yellow">Reason</span>' : '');
        const mode = m.delete_mode === 'merge' ? 'Move coding to keeper, then delete' : 'Delete the extra';
        html += `<div role="row" class="aw-row aw-drow${isK ? ' keeper' : ''}" data-tid="${m.id}" data-idx="${S.rows.length}" tabindex="0" aria-selected="${S.sel.has(m.id)}" draggable="true">
          <div class="aw-cell"><input type="checkbox" ${S.sel.has(m.id) ? 'checked' : ''} aria-label="Select ${esc(m.vendor)}" data-sel="${m.id}"></div>
          <div class="aw-cell num muted">${esc(shortDate(m.date))}</div>
          <div class="aw-cell"><span class="vendor ell">${esc(m.vendor)}</span>${m.qbo_txn_id ? `<span class="sub">${esc(m.qbo_txn_type || 'QBO')} #${esc(m.qbo_txn_id)}</span>` : ''}</div>
          <div class="aw-cell num right amt">${money(m.amount)}</div>
          <div class="aw-cell num muted">${m.card_last4 ? '••' + esc(m.card_last4) : (m.qbo_txn_type === 'BillPayment' ? 'ACH' : '')}</div>
          <div class="aw-cell">${srcChip(m.source)}</div>
          <div class="aw-cell">${tag} ${coded(m) ? `<span class="aw-chip solid"><span>${esc(coded(m))}</span></span>` : ''} ${bk}</div>
          <div class="aw-cell acts">${isK ? '' : `<button type="button" class="aw-btn sm danger" data-act="dup-delete" data-tid="${m.id}" data-keeper="${g.keeper_id}" data-key="${esc(g.key)}" title="${esc(mode)}">${m.delete_mode === 'merge' ? 'Move coding and delete' : 'Delete the extra'}</button>
            <button type="button" class="aw-btn sm" data-act="not-project" data-tid="${m.id}">Not project</button>`}</div>
        </div>`;
        S.rows.push(t);
      });
    });
    if (voided.length) {
      html += `<div class="aw-dgrp" role="row" style="margin-top:12px"><span class="v">Voided in QuickBooks</span><span class="k">${voided.length} kept because they carry coding or a receipt — decide what happens to the work on them</span></div>`;
      voided.forEach(m => {
        html += `<div role="row" class="aw-row aw-drow excluded" data-tid="${m.id}" tabindex="0">
          <div class="aw-cell"></div>
          <div class="aw-cell num muted">${esc(shortDate(m.date))}</div>
          <div class="aw-cell"><span class="vendor ell">${esc(m.vendor)}</span>${m.qbo_txn_id ? `<span class="sub">${esc(m.qbo_txn_type || 'QBO')} #${esc(m.qbo_txn_id)}</span>` : ''}</div>
          <div class="aw-cell num right amt">${money(m.amount)}</div>
          <div class="aw-cell num muted">${m.card_last4 ? '••' + esc(m.card_last4) : ''}</div>
          <div class="aw-cell">${srcChip(m.source)}</div>
          <div class="aw-cell"><span class="aw-badge red">Voided in QuickBooks</span> ${coded(m) ? `<span class="aw-chip solid"><span>${esc(coded(m))}</span></span>` : ''} ${m.receipt ? '<span class="aw-flag green">Receipt</span>' : ''}</div>
          <div class="aw-cell acts"><button type="button" class="aw-btn sm" data-act="recover" data-tid="${m.id}" title="Keep it anyway — back to the pool">Keep it</button>
            <button type="button" class="aw-btn sm" data-act="voided-ack" data-tid="${m.id}" title="Leave it excluded and clear the flag">Leave excluded</button></div>
        </div>`;
      });
    }
    return html;
  };
  AW.openDupGroup = function (key) {
    AW.setView('duplicates');
    const el = document.getElementById('dup-' + key);
    if (el) { el.scrollIntoView({ block: 'center' }); el.classList.add('focus-flash'); }
  };
  // Compare in place (2026-11-04): the chip, D and the group's Compare
  // button open the Compare panel (static/dup-compare.js) on THIS view; the
  // Duplicates tab stays the list of every group.
  AW.openCompare = function (opts) {
    if (!window.FPCompare) { if (opts.key) AW.openDupGroup(opts.key); return; }
    FPCompare.open({ pid: PID, key: opts.key || null, tid: opts.tid || null,
      onSettled: () => AW.load({ keepFocus: true }),
      toast: (msg, kind, o) => AW.toast(msg, kind, o),
      notProject: tid => { if (A['not-project']) A['not-project']({ dataset: { tid: String(tid) } }); } });
  };
  A['open-dup'] = btn => {
    const t = S.byId.get(+btn.dataset.tid);
    const key = btn.dataset.key || (t || {}).dup_group;
    if (key || t) AW.openCompare({ key, tid: btn.dataset.tid ? +btn.dataset.tid : null });
  };
  A['dup-compare'] = btn => AW.openCompare({ key: btn.dataset.key });
  A['dup-keep-both'] = async btn => {
    const key = btn.dataset.key;
    try {
      await AW.post(`/projects/${PID}/actuals/dup-transactions/dismiss`, { dup_key: key });
      AW.toast('Kept both. This pair will not be flagged again.', '');
      await AW.load();
    } catch (e) { AW.toast(e.message || 'Could not save', 'red'); }
  };
  let dupDel = null;
  A['dup-delete'] = btn => {
    const extra = S.byId.get(+btn.dataset.tid), keeper = S.byId.get(+btn.dataset.keeper);
    if (!extra || !keeper) return;
    const g = ((S.data || {}).duplicates || []).find(x => x.key === btn.dataset.key) || {};
    const mx = (g.members || []).find(m => m.id === extra.id) || {};
    dupDel = { extra, keeper, mode: mx.delete_mode || 'merge' };
    $('aw-dupdel-sub').textContent = `${extra.vendor} · ${money(extra.amount)}`;
    const line = m => `<div class="r ${m === keeper ? 'keep' : 'gone'}"><span>${m === keeper ? '<span class="aw-badge">Keeper</span>' : '<span class="aw-badge extra">Extra</span>'}</span><span class="num muted">${esc(shortDate(m.date))}</span><span class="ell">${esc(m.vendor)}${m.qbo_txn_id ? ` <span class="muted" style="font-size:11px">${esc(m.qbo_txn_type || '')} #${esc(m.qbo_txn_id)}</span>` : ''}</span><span class="num" style="font-weight:600;text-align:right">${money(m.amount)}</span><span class="ell" style="font-size:12px">${esc(coded(m) || (m.receipt ? 'receipt' : '—'))}</span></div>`;
    $('aw-dupdel-rows').innerHTML = line(keeper) + line(extra);
    $('aw-dupdel-help').textContent = dupDel.mode === 'merge'
      ? 'The extra carries coding, a receipt or a reason: those move onto the keeper first, then the extra is deleted.'
      : 'The extra has no coding and no receipt, so it is simply deleted. The keeper is untouched.';
    $('aw-dupdel-go').textContent = dupDel.mode === 'merge' ? 'Move coding and delete' : 'Delete the extra';
    $('aw-dupdel-err').textContent = '';
    const dlg = $('aw-dupdel'); if (!dlg.open) dlg.showModal();
  };
  $('aw-dupdel-go').addEventListener('click', async () => {
    if (!dupDel) return;
    const { extra, keeper } = dupDel;
    const go = $('aw-dupdel-go'); go.disabled = true;
    try {
      const r = await AW.post(AW.txnUrl(extra.id, 'merge-duplicate'), { keeper_id: keeper.id });
      $('aw-dupdel').close();
      AW.toast(`${extra.vendor} ${money(extra.amount)} removed${r.mode === 'merge' ? ' — coding moved to the keeper' : ''}`, '', { activityIds: r.activity_ids || [] });
      AW.clearSelection(); await AW.load();
    } catch (e) { $('aw-dupdel-err').textContent = e.message || 'Could not delete'; }
    finally { go.disabled = false; }
  });
  A['voided-ack'] = async btn => {
    const tid = +btn.dataset.tid; const t = S.byId.get(tid) || {};
    try {
      const r = await AW.post(AW.txnUrl(tid, 'edit'), { no_charge_reason: '' });
      AW.toast(`${t.vendor || 'Charge'} stays excluded`, '', { activityId: r.activity_id });
      await AW.load();
    } catch (e) { AW.toast(e.message || 'Could not update', 'red'); }
  };
})();
