/* FPBudget Actuals workspace — a03: actions and modals.
   (ACTUALS_HANDOFF Phase 3, 2026-11-01.)

   Every button, key and menu item in the workspace lands in AW.actions,
   keyed by its data-act. Each action calls an EXISTING route (nothing new
   server-side beyond activity ids on the responses), shows a toast with
   Undo when the route returned an activity id, and re-fetches the
   workspace. Modals are native <dialog> elements (focus trap and Esc for
   free). */
(function () {
  'use strict';
  const AW = window.AW;
  const AJ = window.__AJ || {};
  const S = AW.state, $ = AW.$, PID = AW.PID, esc = AW.esc, money = AW.money;
  const A = AW.actions = AW.actions || {};

  // ── Dialog helpers ───────────────────────────────────────────────────
  function openDialog(id) {
    const dlg = $(id);
    if (!dlg.open) dlg.showModal();
    return dlg;
  }
  document.querySelectorAll('dialog.aw-dialog').forEach(dlg => {
    dlg.addEventListener('click', e => { if (e.target.closest('[data-close]')) { e.preventDefault(); dlg.close(); } });
    dlg.addEventListener('cancel', () => { /* Esc closes; nothing to clean */ });
  });
  function selOne(verb) {
    const ts = AW.selectedTxns();
    if (ts.length !== 1) { AW.toast(`Select exactly one charge to ${verb}.`, 'yellow'); return null; }
    return ts[0];
  }
  function tidFrom(btn) { return btn && btn.dataset && btn.dataset.tid ? +btn.dataset.tid : null; }
  function targetTxns(btn) {
    // A row button acts on that row; the action bar and keys act on the selection.
    const tid = tidFrom(btn);
    if (tid) { const t = S.byId.get(tid); return t ? [t] : []; }
    return AW.selectedTxns();
  }

  // ── Line picker (A, Assign to line, Move) ────────────────────────────
  let pickerFor = [];      // txns the pick applies to
  let pickerIdx = 0;
  function pickerOptions(q) {
    q = (q || '').trim().toLowerCase();
    const out = [];
    const recent = (AW.recentTargets ? AW.recentTargets() : []).filter(e => e.kind === 'line' && S.lineById.has(+e.lineId));
    if (!q && recent.length) {
      out.push({ group: 'Recent' });
      recent.forEach(e => { const l = S.lineById.get(+e.lineId); out.push({ lineId: l.id, label: l.label, est: l.estimate, act: l.actual }); });
    }
    (AJ.lineGroups || []).forEach(g => {
      const opts = g.opts.filter(o => (!q || o.label.toLowerCase().includes(q) || String(g.code).includes(q) || (g.name || '').toLowerCase().includes(q)));
      const secMatch = q && (String(g.code).includes(q) || (g.name || '').toLowerCase().includes(q));
      if (!opts.length && !secMatch) return;
      out.push({ group: `${g.code} · ${g.name}` });
      out.push({ section: true, code: g.code, name: g.name, label: `${g.code} · ${g.name} — whole section`, sub: 'no line yet' });
      opts.forEach(o => { const l = S.lineById.get(o.id) || {}; out.push({ lineId: o.id, label: o.label, child: o.kind === 'child', est: l.estimate, act: l.actual }); });
    });
    return out;
  }
  function renderPicker() {
    const list = $('aw-picker-list');
    const opts = pickerOptions($('aw-picker-q').value);
    const choosable = opts.filter(o => !o.group);
    if (pickerIdx >= choosable.length) pickerIdx = Math.max(0, choosable.length - 1);
    let i = -1;
    list.innerHTML = opts.map(o => {
      if (o.group) return `<div class="aw-pgrp">${esc(o.group)}</div>`;
      i++;
      const sel = i === pickerIdx;
      const right = o.section ? esc(o.sub) : (o.est != null ? `est ${AW.whole(o.est)} · act ${AW.whole(o.act)}` : '');
      return `<div class="aw-popt${o.child ? ' child' : ''}${o.section ? ' section' : ''}" role="option" id="aw-popt-${i}" aria-selected="${sel}" data-i="${i}"
        ${o.section ? `data-code="${esc(o.code)}" data-name="${esc(o.name)}"` : `data-line-id="${o.lineId}" data-label="${esc(o.label)}"`}>
        <span class="l">${esc(o.label)}</span><span class="r num">${right}</span></div>`;
    }).join('') || '<div class="aw-popt"><span class="l muted">No line matches</span></div>';
    const cur = list.querySelector('[aria-selected="true"]');
    if (cur) { cur.scrollIntoView({ block: 'nearest' }); $('aw-picker-q').setAttribute('aria-activedescendant', cur.id); }
  }
  function pickCurrent(el) {
    el = el || $('aw-picker-list').querySelector('[aria-selected="true"]');
    if (!el || (!el.dataset.lineId && !el.dataset.code)) return;
    const target = el.dataset.lineId ? { lineId: +el.dataset.lineId, label: el.dataset.label } : { code: el.dataset.code, name: el.dataset.name };
    const tids = pickerFor.map(t => t.id);
    $('aw-picker').close();
    AW.applyDrop(target, tids);
  }
  AW.openPicker = function (txns) {
    pickerFor = (txns || []).filter(t => !AW.isContainer(t));
    if (!pickerFor.length) { AW.toast('Nothing to assign. Split or itemized charges carry their coding on the sublines.', 'yellow'); return; }
    const total = pickerFor.reduce((a, t) => a + Number(t.amount || 0), 0);
    $('aw-picker-title').textContent = pickerFor.length === 1 && pickerFor[0].assigned_line ? 'Move to another line' : 'Assign to line';
    $('aw-picker-sub').textContent = pickerFor.length === 1
      ? `${pickerFor[0].vendor} · ${money(pickerFor[0].amount)}${pickerFor[0].assigned_line ? ' · now on ' + pickerFor[0].assigned_line.label : ''}`
      : `${pickerFor.length} charges · ${money(total)}`;
    $('aw-picker-q').value = ''; pickerIdx = 0;
    renderPicker();
    openDialog('aw-picker');
    setTimeout(() => $('aw-picker-q').focus(), 0);
  };
  $('aw-picker-q').addEventListener('input', () => { pickerIdx = 0; renderPicker(); });
  $('aw-picker-q').addEventListener('keydown', e => {
    const n = $('aw-picker-list').querySelectorAll('[role="option"]').length;
    if (e.key === 'ArrowDown') { e.preventDefault(); pickerIdx = Math.min(n - 1, pickerIdx + 1); renderPicker(); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); pickerIdx = Math.max(0, pickerIdx - 1); renderPicker(); }
    else if (e.key === 'Enter') { e.preventDefault(); pickCurrent(); }
  });
  $('aw-picker-list').addEventListener('click', e => { const o = e.target.closest('[role="option"]'); if (o) pickCurrent(o); });
  $('aw-picker').addEventListener('submit', e => e.preventDefault());

  A.assign = () => AW.openPicker(AW.selectedTxns());
  A['assign-one'] = btn => AW.openPicker(targetTxns(btn));
  A['assign-to'] = target => AW.applyDrop(target, Array.from(S.sel));
  A['accept-suggestion'] = btn => {
    const tid = +btn.dataset.tid;
    if (btn.dataset.suggestLine) AW.applyDrop({ lineId: +btn.dataset.suggestLine }, [tid]);
    else if (btn.dataset.suggestSection) AW.applyDrop({ code: btn.dataset.suggestSection, name: AW.sectionName(+btn.dataset.suggestSection) }, [tid]);
  };
  A.goto = btn => { const tid = +btn.dataset.tid; if (tid) AW.focusTransaction(tid); };

  // ── Unassign ─────────────────────────────────────────────────────────
  A.unassign = async btn => {
    const ts = targetTxns(btn).filter(AW.isAssigned);
    if (!ts.length) return;
    try {
      const ids = [];
      for (const t of ts) {
        if (t.splits && t.splits.length) { const r = await AW.post(AW.txnUrl(t.id, 'split'), {}, 'DELETE'); ids.push(r.activity_id); }
        else if (t.itemized && t.itemized.length) { AW.toast(`${t.vendor} is itemized. Clear the itemization from the document first.`, 'yellow'); continue; }
        else if (t.assigned_line) { const r = await AW.post(AW.txnUrl(t.id, 'set-line'), { budget_line_id: null }); ids.push(r.activity_id); }
        else if (t.section_only) { const r = await AW.post(AW.txnUrl(t.id, 'set-coa'), { account_code: null }); ids.push(r.activity_id); }
      }
      AW.toast(`${ts.length === 1 ? ts[0].vendor + ' ' + money(ts[0].amount) : ts.length + ' charges'} back in Unclaimed`, '', { activityIds: ids });
      AW.clearSelection(); await AW.load();
    } catch (e) { AW.toast('Could not unassign: ' + (e.message || e), 'red'); }
  };

  // ── Not project (optional reason) ────────────────────────────────────
  let reasonFor = [];
  A['not-project'] = btn => {
    reasonFor = targetTxns(btn);
    if (!reasonFor.length) return;
    const total = reasonFor.reduce((a, t) => a + Number(t.amount || 0), 0);
    $('aw-reason-sub').textContent = reasonFor.length === 1 ? `${reasonFor[0].vendor} · ${money(reasonFor[0].amount)}` : `${reasonFor.length} charges · ${money(total)}`;
    $('aw-reason-text').value = ''; $('aw-reason-err').textContent = '';
    openDialog('aw-reason');
    setTimeout(() => $('aw-reason-text').focus(), 0);
  };
  $('aw-reason-save').addEventListener('click', async () => {
    const reason = $('aw-reason-text').value.trim();
    const ts = reasonFor.slice();
    $('aw-reason').close();
    try {
      const ids = [];
      let failed = 0;
      for (const t of ts) {
        try { const r = await AW.post(AW.txnUrl(t.id, 'mark-not-project'), reason ? { reason } : {}); ids.push(r.activity_id); }
        catch (e) { failed++; AW.toast(`${t.vendor}: ${e.message}`, 'red'); }
      }
      if (ids.length) AW.toast(`${ids.length === 1 ? ts[0].vendor + ' ' + money(ts[0].amount) : ids.length + ' charges'} marked not project${reason ? ' — ' + reason : ''}`, '', { activityIds: ids });
      AW.clearSelection(); await AW.load();
    } catch (e) { AW.toast('Could not mark: ' + (e.message || e), 'red'); }
  });
  $('aw-reason-text').addEventListener('keydown', e => { if (e.key === 'Enter') { e.preventDefault(); $('aw-reason-save').click(); } });

  A['receipt-not-project'] = async btn => {
    const tid = btn.dataset.txn ? +btn.dataset.txn : null;
    if (!tid) { AW.toast('This receipt has no ledger row yet. Open it in Documents and mark it there.', 'yellow'); return; }
    try {
      const r = await AW.post(AW.txnUrl(tid, 'mark-not-project'), {});
      AW.toast('Receipt marked not project', '', { activityId: r.activity_id });
      await AW.load();
    } catch (e) { AW.toast('Could not mark: ' + (e.message || e), 'red'); }
  };

  // ── Recover ──────────────────────────────────────────────────────────
  A.recover = async btn => {
    const tid = tidFrom(btn); const t = S.byId.get(tid) || {};
    try {
      const r = await AW.post(AW.txnUrl(tid, 'recover'), {});
      AW.toast(`${t.vendor || 'Charge'} ${money(t.amount)} is back in Unclaimed`, '', { activityId: r.activity_id });
      await AW.load();
    } catch (e) { AW.toast('Could not recover: ' + (e.message || e), 'red'); }
  };

  // ── Split ────────────────────────────────────────────────────────────
  let splitFor = null;
  function splitRow(lineId, amount) {
    const row = document.createElement('div');
    row.className = 'aw-srow';
    const sel = document.createElement('select');
    sel.setAttribute('aria-label', 'Budget line');
    sel.appendChild($('aw-line-opts').content.cloneNode(true));
    if (lineId) sel.value = String(lineId);
    const inp = document.createElement('input');
    inp.type = 'text'; inp.inputMode = 'decimal'; inp.className = 'num'; inp.placeholder = '0.00';
    inp.setAttribute('aria-label', 'Amount');
    if (amount != null && amount !== '') inp.value = Number(amount).toFixed(2);
    const rm = document.createElement('button');
    rm.type = 'button'; rm.className = 'aw-btn icon'; rm.style.height = '36px'; rm.style.width = '36px';
    rm.setAttribute('aria-label', 'Remove this line');
    rm.innerHTML = '<svg aria-hidden="true" width="12" height="12" viewBox="0 0 14 14" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"><path d="M2 2l10 10M12 2L2 12"/></svg>';
    rm.addEventListener('click', () => { row.remove(); splitRecalc(); });
    row.append(sel, inp, rm);
    inp.addEventListener('input', splitRecalc);
    sel.addEventListener('change', splitRecalc);
    return row;
  }
  function splitRows() {
    return Array.from($('aw-split-rows').querySelectorAll('.aw-srow')).map(r => ({
      budget_line_id: +r.querySelector('select').value || null,
      amount: parseFloat(String(r.querySelector('input').value).replace(/[$,]/g, '')),
    }));
  }
  function splitRecalc() {
    if (!splitFor) return;
    const total = Math.round(Number(splitFor.amount || 0) * 100);
    const rows = splitRows();
    const sum = rows.reduce((a, r) => a + (isNaN(r.amount) ? 0 : Math.round(r.amount * 100)), 0);
    const remain = (total - sum) / 100;
    const el = $('aw-split-remain');
    $('aw-split-remain-amt').textContent = money(remain);
    const complete = rows.length >= 2 && rows.every(r => r.budget_line_id && !isNaN(r.amount) && r.amount !== 0);
    const ok = complete && Math.abs(remain) < 0.005;
    el.className = 'aw-remain num ' + (ok ? 'ok' : (rows.some(r => !isNaN(r.amount)) ? 'bad' : ''));
    $('aw-split-save').disabled = !ok;
    $('aw-split-err').textContent = (complete && !ok) ? `Off by ${money(remain)} — the lines must add up to ${money(splitFor.amount)}.` : (!complete && rows.length < 2 ? 'A split needs at least two lines.' : '');
  }
  A.split = btn => {
    const ts = targetTxns(btn);
    if (ts.length !== 1) { AW.toast('Select exactly one charge to split.', 'yellow'); return; }
    const t = ts[0];
    if (t.itemized && t.itemized.length) { AW.toast('This document is itemized. Clear the itemization before splitting it.', 'yellow'); return; }
    splitFor = t;
    $('aw-split-sub').textContent = `${t.vendor} · ${money(t.amount)} · ${AW.shortDate(t.date)}`;
    const host = $('aw-split-rows'); host.innerHTML = '';
    if (t.splits && t.splits.length) t.splits.forEach(s => host.appendChild(splitRow(s.budget_line_id, s.amount)));
    else { host.appendChild(splitRow(t.assigned_line ? t.assigned_line.id : null, '')); host.appendChild(splitRow(null, '')); }
    $('aw-split-err').textContent = '';
    splitRecalc();
    openDialog('aw-split');
    setTimeout(() => { const f = host.querySelector('select'); if (f) f.focus(); }, 0);
  };
  $('aw-split-add').addEventListener('click', () => { $('aw-split-rows').appendChild(splitRow(null, '')); splitRecalc(); });
  $('aw-split-save').addEventListener('click', async () => {
    if (!splitFor) return;
    const rows = splitRows().map(r => ({ budget_line_id: r.budget_line_id, amount: Math.round(r.amount * 100) / 100 }));
    const t = splitFor;
    try {
      const r = await AW.post(AW.txnUrl(t.id, 'split'), { splits: rows });
      $('aw-split').close();
      AW.toast(`${t.vendor} ${money(t.amount)} split across ${rows.length} lines`, '', { activityId: r.activity_id });
      AW.clearSelection(); await AW.load();
    } catch (e) {
      const d = e.data || {};
      $('aw-split-err').textContent = (e.message || 'Could not save') + (d.delta != null ? ` (off by ${money(d.delta)})` : '');
      if (d.delta != null) { $('aw-split-remain-amt').textContent = money(d.delta); $('aw-split-remain').className = 'aw-remain num bad'; }
      $('aw-split-save').disabled = true;
    }
  });

  // ── Attach receipt (upload or link from inbox) ───────────────────────
  let attachFor = null, attachDoc = null;
  function renderInbox() {
    const host = $('aw-attach-inbox');
    const rs = (S.data && S.data.receipts) || [];
    if (!rs.length) { host.innerHTML = '<div class="aw-empty" style="margin:10px">No receipts waiting in the inbox.</div>'; return; }
    host.innerHTML = rs.map(r => `<label><input type="radio" name="aw-inbox" value="${r.doc_id}">
      <img class="aw-thumb" style="width:28px;height:28px" src="${esc(r.thumb)}" alt="" onerror="this.style.visibility='hidden'">
      <span class="n">${esc(r.vendor || r.name)} <span class="muted" style="font-size:11px">${esc(r.name)}</span></span>
      <span class="num muted" style="font-size:12px">${esc(AW.shortDate(r.date))}</span>
      <span class="num" style="font-weight:600">${r.amount != null ? money(r.amount) : '—'}</span></label>`).join('');
  }
  A.attach = btn => {
    const ts = targetTxns(btn);
    if (ts.length !== 1) { AW.toast('Select exactly one charge to attach a receipt to.', 'yellow'); return; }
    const t = ts[0];
    if (t.receipt) { AW.toast('This charge already has a receipt. Unlink it from the row menu to replace it.', 'yellow'); return; }
    attachFor = t; attachDoc = null;
    $('aw-attach-sub').textContent = `${t.vendor} · ${money(t.amount)} · ${AW.shortDate(t.date)}`;
    $('aw-attach-err').textContent = ''; $('aw-attach-note').textContent = '';
    $('aw-attach-link').disabled = true;
    renderInbox();
    openDialog('aw-attach');
  };
  $('aw-attach-inbox').addEventListener('change', e => {
    const r = e.target.closest('input[type="radio"]'); if (!r) return;
    attachDoc = +r.value; $('aw-attach-link').disabled = false;
  });
  $('aw-attach-link').addEventListener('click', async () => {
    if (!attachFor || !attachDoc) return;
    try {
      const r = await AW.post(AW.txnUrl(attachFor.id, 'link-doc'), { doc_upload_id: attachDoc });
      $('aw-attach').close();
      AW.toast(`Receipt linked to ${attachFor.vendor}: ${r.filed_filename || ''}`, '', { activityId: r.activity_id });
      AW.clearSelection(); await AW.load();
    } catch (e) { $('aw-attach-err').textContent = e.message || 'Could not link'; }
  });
  const fileInput = $('aw-file');
  $('aw-attach-browse').addEventListener('click', () => fileInput.click());
  $('aw-attach-drop').addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); fileInput.click(); } });
  fileInput.addEventListener('change', async () => {
    const f = fileInput.files && fileInput.files[0]; fileInput.value = '';
    if (!f || !attachFor) return;
    $('aw-attach').close();
    try { await AW.uploadReceipt(attachFor.id, f); AW.clearSelection(); } catch (e) {}
  });
  const drop = $('aw-attach-drop');
  ['dragenter', 'dragover'].forEach(ev => drop.addEventListener(ev, e => { e.preventDefault(); drop.classList.add('over'); }));
  drop.addEventListener('dragleave', () => drop.classList.remove('over'));
  drop.addEventListener('drop', async e => {
    e.preventDefault(); drop.classList.remove('over');
    const f = e.dataTransfer.files && e.dataTransfer.files[0];
    if (!f || !attachFor) return;
    $('aw-attach').close();
    try { await AW.uploadReceipt(attachFor.id, f); AW.clearSelection(); } catch (x) {}
  });

  // ── Link an inbox receipt to the selected charge (Receipts view) ─────
  A['link-selected'] = async btn => {
    // Phase 5: the charge is the one ticked in the candidate panel, else
    // the single charge selected in Unclaimed / Assigned.
    const doc = +btn.dataset.doc;
    const t = S.pickCharge ? S.byId.get(S.pickCharge) : (S.sel.size === 1 ? S.byId.get(Array.from(S.sel)[0]) : null);
    if (!t) { AW.toast('Select a receipt and a charge: tick a charge in the panel on the right.', 'yellow'); return; }
    await AW.linkDocToCharge(doc, t.id);
  };
  AW.linkDocToCharge = async function (doc, tid) {
    const t = S.byId.get(tid) || {};
    try {
      const r = await AW.post(AW.txnUrl(tid, 'link-doc'), { doc_upload_id: doc });
      AW.toast(`Receipt linked to ${t.vendor || 'the charge'} ${money(t.amount)}`, '', { activityId: r.activity_id });
      S.pickCharge = null; S.selDoc = null; S.findFor = null;
      if (AW.closePanel) AW.closePanel(true);
      AW.clearSelection(); await AW.load();
    } catch (e) { AW.toast('Could not link: ' + (e.message || e), 'red'); }
  };

  // ── Receipt suggestion on a charge: confirm / dismiss; unlink ────────
  A['confirm-match'] = async btn => {
    const tid = tidFrom(btn);
    try { const r = await AW.post(AW.txnUrl(tid, 'confirm-match'), {}); AW.toast('Receipt match confirmed', '', { activityId: r.activity_id }); await AW.load(); }
    catch (e) { AW.toast(e.message || 'Could not confirm', 'red'); }
  };
  A['dismiss-match'] = async btn => {
    const tid = tidFrom(btn);
    try { const r = await AW.post(AW.txnUrl(tid, 'dismiss-suggestion'), { remember: true }); AW.toast('Not a match. It will not be suggested again.', '', { activityId: r.activity_id }); await AW.load(); }
    catch (e) { AW.toast(e.message || 'Could not dismiss', 'red'); }
  };
  A.unmatch = async btn => {
    const tid = tidFrom(btn);
    try { const r = await AW.post(AW.txnUrl(tid, 'unmatch'), {}); AW.toast('Receipt unlinked and returned to the inbox', '', { activityId: r.activity_id }); await AW.load(); }
    catch (e) { AW.toast(e.message || 'Could not unlink', 'red'); }
  };

  // ── Missing-backup reason ────────────────────────────────────────────
  let backupFor = null;
  A['add-reason'] = btn => {
    const ts = targetTxns(btn);
    if (ts.length !== 1) { AW.toast('Pick one charge to give a reason for.', 'yellow'); return; }
    backupFor = ts[0];
    $('aw-backup-sub').textContent = `${backupFor.vendor} · ${money(backupFor.amount)} · ${AW.shortDate(backupFor.date)}`;
    $('aw-backup-text').value = backupFor.no_charge_reason || ''; $('aw-backup-err').textContent = '';
    openDialog('aw-backup');
    setTimeout(() => $('aw-backup-text').focus(), 0);
  };
  $('aw-backup-save').addEventListener('click', async () => {
    const reason = $('aw-backup-text').value.trim();
    if (!reason) { $('aw-backup-err').textContent = 'Say why there is no receipt.'; return; }
    try {
      const r = await AW.post(AW.txnUrl(backupFor.id, 'edit'), { no_charge_reason: reason });
      $('aw-backup').close();
      AW.toast(`Reason saved for ${backupFor.vendor}`, '', { activityId: r.activity_id });
      await AW.load();
    } catch (e) { $('aw-backup-err').textContent = e.message || 'Could not save'; }
  });
  $('aw-backup-text').addEventListener('keydown', e => { if (e.key === 'Enter') { e.preventDefault(); $('aw-backup-save').click(); } });

  // ── Create expense from a receipt ────────────────────────────────────
  let createFor = null, createLine = null, createIdx = 0;
  function createOptions(q) {
    q = (q || '').trim().toLowerCase();
    const out = [];
    const recent = (AW.recentTargets ? AW.recentTargets() : []).filter(e => e.kind === 'line' && S.lineById.has(+e.lineId));
    if (!q && recent.length) { out.push({ group: 'Suggested' }); recent.forEach(e => { const l = S.lineById.get(+e.lineId); out.push({ lineId: l.id, label: l.label, est: l.estimate, act: l.actual }); }); }
    (AJ.lineGroups || []).forEach(g => {
      const opts = g.opts.filter(o => (!q || o.label.toLowerCase().includes(q) || String(g.code).includes(q) || (g.name || '').toLowerCase().includes(q)));
      if (!opts.length) return;
      out.push({ group: `${g.code} · ${g.name}` });
      opts.forEach(o => { const l = S.lineById.get(o.id) || {}; out.push({ lineId: o.id, label: o.label, child: o.kind === 'child', est: l.estimate, act: l.actual }); });
    });
    return out;
  }
  function renderCreateList() {
    const list = $('aw-create-list');
    const opts = createOptions($('aw-create-line').value);
    let i = -1;
    list.innerHTML = opts.map(o => {
      if (o.group) return `<div class="aw-pgrp">${esc(o.group)}</div>`;
      i++;
      return `<div class="aw-popt${o.child ? ' child' : ''}" role="option" id="aw-copt-${i}" aria-selected="${i === createIdx}" data-line-id="${o.lineId}" data-label="${esc(o.label)}">
        <span class="l">${esc(o.label)}</span><span class="r num">${o.est != null ? `est ${AW.whole(o.est)} · act ${AW.whole(o.act)}` : ''}</span></div>`;
    }).join('') || '<div class="aw-popt"><span class="l muted">No line matches</span></div>';
    const cur = list.querySelector('[aria-selected="true"]'); if (cur) cur.scrollIntoView({ block: 'nearest' });
  }
  function chooseCreateLine(el) {
    el = el || $('aw-create-list').querySelector('[aria-selected="true"]');
    if (!el || !el.dataset.lineId) return;
    createLine = +el.dataset.lineId;
    $('aw-create-line').value = el.dataset.label;
    $('aw-create-combo').classList.remove('open');
    $('aw-create-line').setAttribute('aria-expanded', 'false');
  }
  function payMethod() { const r = document.querySelector('#aw-create-pay input:checked'); return r ? r.value : 'cash'; }
  function syncPayUI() {
    const pm = payMethod();
    $('aw-create-who-wrap').style.visibility = (pm === 'personal_card' || pm === 'cash') ? 'visible' : 'hidden';
    $('aw-create-why-req').textContent = pm === 'cash' ? '(required for cash)' : pm === 'other' ? '(required)' : '(optional)';
  }
  A['create-expense'] = btn => {
    const doc = +btn.dataset.doc;
    const r = ((S.data || {}).receipts || []).find(x => x.doc_id === doc);
    if (!r) return;
    createFor = r; createLine = null; createIdx = 0;
    $('aw-create-file').textContent = r.name || '';
    $('aw-create-open').href = `/docs/upload/${r.doc_id}/raw`;
    const img = $('aw-create-img'); img.style.display = ''; img.src = r.thumb || '';
    const read = [];
    if (r.vendor) read.push('vendor'); if (r.amount != null) read.push(money(r.amount)); if (r.date) read.push(AW.shortDate(r.date));
    $('aw-create-ocr').textContent = read.length ? 'OCR read: ' + read.join(', ') : 'OCR read nothing usable';
    $('aw-create-ocr').className = 'aw-flag ' + (read.length ? 'green' : 'yellow');
    document.querySelector('#aw-create-pay input[value="cash"]').checked = true;
    $('aw-create-who').value = ''; $('aw-create-why').value = '';
    $('aw-create-vendor').value = r.vendor || ''; $('aw-create-amount').value = r.amount != null ? Number(r.amount).toFixed(2) : '';
    $('aw-create-date').value = r.date || ''; $('aw-create-line').value = ''; $('aw-create-err').textContent = '';
    syncPayUI(); renderCreateList();
    openDialog('aw-create');
    setTimeout(() => $('aw-create-why').focus(), 0);
  };
  $('aw-create-pay').addEventListener('change', syncPayUI);
  const combo = $('aw-create-combo'), lineInp = $('aw-create-line');
  lineInp.addEventListener('focus', () => { combo.classList.add('open'); lineInp.setAttribute('aria-expanded', 'true'); renderCreateList(); });
  lineInp.addEventListener('input', () => { createLine = null; createIdx = 0; combo.classList.add('open'); lineInp.setAttribute('aria-expanded', 'true'); renderCreateList(); });
  lineInp.addEventListener('keydown', e => {
    const n = $('aw-create-list').querySelectorAll('[role="option"]').length;
    if (e.key === 'ArrowDown') { e.preventDefault(); createIdx = Math.min(n - 1, createIdx + 1); renderCreateList(); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); createIdx = Math.max(0, createIdx - 1); renderCreateList(); }
    else if (e.key === 'Enter') { e.preventDefault(); chooseCreateLine(); }
    else if (e.key === 'Escape' && combo.classList.contains('open')) { e.preventDefault(); e.stopPropagation(); combo.classList.remove('open'); }
  });
  $('aw-create-list').addEventListener('mousedown', e => { const o = e.target.closest('[role="option"]'); if (o) { e.preventDefault(); chooseCreateLine(o); } });
  document.addEventListener('click', e => { if (!combo.contains(e.target)) combo.classList.remove('open'); });
  $('aw-create-form').addEventListener('submit', async e => {
    e.preventDefault();
    if (!createFor) return;
    const pm = payMethod();
    const why = $('aw-create-why').value.trim();
    const who = $('aw-create-who').value;
    const err = $('aw-create-err'); err.textContent = '';
    if (pm === 'cash' && !why) { err.textContent = 'Say why no card was used — it is required for cash.'; $('aw-create-why').focus(); return; }
    if (pm === 'other' && !why && !who) { err.textContent = 'Add a short detail for Other.'; $('aw-create-why').focus(); return; }
    const amount = parseFloat(String($('aw-create-amount').value).replace(/[$,]/g, ''));
    if (isNaN(amount)) { err.textContent = 'Amount must be a number.'; $('aw-create-amount').focus(); return; }
    const body = { payment_method: pm, detail: why, vendor: $('aw-create-vendor').value.trim(), amount, txn_date: $('aw-create-date').value };
    if (who && (pm === 'personal_card' || pm === 'cash')) body.paid_by_crew_member_id = +who;
    if (createLine) body.budget_line_id = createLine;
    const save = $('aw-create-save'); save.disabled = true;
    try {
      const r = await AW.post(`/docs/upload/${createFor.doc_id}/log-expense`, body);
      $('aw-create').close();
      AW.toast(`Expense created: ${body.vendor || createFor.vendor || 'receipt'} ${money(amount)}${createLine ? ' → ' + $('aw-create-line').value : ''}`, '', { activityId: r.activity_id });
      if (r.warning) AW.toast(r.warning, 'yellow');
      if (createLine) { const l = S.lineById.get(createLine); if (l) AW.recordRecent({ kind: 'line', lineId: l.id, code: l.account_code, name: l.description, label: l.label }); }
      await AW.load();
    } catch (x) { err.textContent = x.message || 'Could not create the expense'; }
    finally { save.disabled = false; }
  });

  // ── Row menu (…) ─────────────────────────────────────────────────────
  const menu = $('aw-menu');
  let menuBtn = null;
  AW.closeMenu = () => { menu.classList.remove('open'); menu.innerHTML = ''; if (menuBtn) { menuBtn.setAttribute('aria-expanded', 'false'); menuBtn = null; } };
  A.menu = (btn, e) => {
    const t = S.byId.get(+btn.dataset.tid); if (!t) return;
    if (menuBtn === btn) { AW.closeMenu(); return; }
    AW.closeMenu();
    menuBtn = btn; btn.setAttribute('aria-expanded', 'true');
    const items = [];
    const assigned = AW.isAssigned(t);
    if (!AW.isContainer(t)) items.push({ act: 'assign-one', label: assigned ? 'Move to another line' : 'Assign to line' });
    if (assigned) items.push({ act: 'unassign', label: (t.splits && t.splits.length) ? 'Remove the split' : 'Unassign' });
    if (!(t.itemized && t.itemized.length)) items.push({ act: 'split', label: (t.splits && t.splits.length) ? 'Edit the split' : 'Split across lines' });
    items.push({ hr: true });
    if (t.receipt) items.push({ act: 'unmatch', label: 'Unlink the receipt' });
    else {
      if (t.receipt_suggestion) { items.push({ act: 'confirm-match', label: `Approve receipt match (${Math.round((t.receipt_suggestion.confidence || 0) * 100)}%)` }); items.push({ act: 'dismiss-match', label: 'Not a match' }); }
      items.push({ act: 'find-backup', label: 'Find backup' });
      items.push({ act: 'attach', label: 'Attach receipt' });
      items.push({ act: 'add-reason', label: t.no_charge_reason ? 'Edit the missing-backup reason' : 'Add missing-backup reason' });
    }
    if (t.dup_group) { items.push({ hr: true }); items.push({ act: 'open-dup', label: 'Open the duplicate group', key: t.dup_group }); }
    items.push({ hr: true });
    items.push({ act: 'not-project', label: 'Not a project expense', danger: true });
    if (AJ.canSync && t.qbo_txn_id && !t.pooled) items.push({ act: 'release-pool', label: 'Release to unclaimed (company pool)', danger: true });
    menu.innerHTML = items.map(i => i.hr ? '<hr>' : `<button type="button" role="menuitem" data-act="${i.act}" data-tid="${t.id}" ${i.key ? `data-key="${esc(i.key)}"` : ''} class="${i.danger ? 'danger' : ''}">${esc(i.label)}</button>`).join('');
    const r = btn.getBoundingClientRect();
    menu.style.top = Math.min(window.innerHeight - 320, r.bottom + 4) + 'px';
    menu.style.left = Math.max(8, r.right - 220) + 'px';
    menu.classList.add('open');
    const first = menu.querySelector('button'); if (first) first.focus();
  };
  menu.addEventListener('click', e => {
    const b = e.target.closest('[data-act]'); if (!b) return;
    const act = b.dataset.act; const fake = { dataset: { tid: b.dataset.tid, key: b.dataset.key } };
    AW.closeMenu();
    if (A[act]) A[act](fake, e);
  });
  menu.addEventListener('keydown', e => {
    const bs = Array.from(menu.querySelectorAll('button')); const i = bs.indexOf(document.activeElement);
    if (e.key === 'ArrowDown') { e.preventDefault(); (bs[i + 1] || bs[0]).focus(); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); (bs[i - 1] || bs[bs.length - 1]).focus(); }
    else if (e.key === 'Escape') { e.preventDefault(); const b = menuBtn; AW.closeMenu(); if (b) b.focus(); }
  });
  document.addEventListener('click', e => { if (menu.classList.contains('open') && !menu.contains(e.target) && !(menuBtn && menuBtn.contains(e.target))) AW.closeMenu(); });
  document.addEventListener('scroll', () => AW.closeMenu(), true);

  // ── Sync now + accounts picker ───────────────────────────────────────
  A['sync-now'] = async btn => {
    const body = {};
    const f = $('aw-sync-from'), t = $('aw-sync-to');
    if (f && f.value) body.start_date = f.value;
    if (t && t.value) body.end_date = t.value;
    const b = btn || $('aw-syncnow'); const orig = b.textContent;
    b.disabled = true; b.textContent = 'Syncing';
    try {
      const r = await AW.post(`/projects/${PID}/actuals/sync-now`, body);
      AW.toast(`QuickBooks sync: +${r.imported || 0} new, ${r.enriched || 0} enriched, ${r.match_suggestions || 0} receipt matches suggested, ${r.cdc_additions || 0} via change capture`
        + (r.qbo_deleted ? `, ${r.qbo_deleted} removed (deleted in QuickBooks)` : '') + (r.qbo_voided ? `, ${r.qbo_voided} flagged voided — see Duplicates` : ''), '');
      await AW.load();
    } catch (e) {
      AW.toast('Sync failed: ' + (e.message || e), 'red');
    } finally { b.disabled = false; b.textContent = orig; }
  };
  // (The per-project account picker is gone: the company account set lives
  // in Admin → QuickBooks — AJ.urls.accounts.)

  // ── Release to unclaimed + unmatch receipt (admins; company ledger) ──
  A['release-pool'] = async btn => {
    const tid = tidFrom(btn); const t = S.byId.get(tid) || {};
    if (!confirm(`Release ${t.vendor || 'this charge'} ${money(t.amount)} to the unclaimed pool?\n\nIts line, section, splits and receipt link are removed; the receipt goes back to Receipts to place. Logged and restorable from Admin → Maintenance → Undo a ledger merge.`)) return;
    try {
      await AW.post('/admin/ledger/release', { txn_id: tid });
      AW.toast(`${t.vendor || 'Charge'} ${money(t.amount)} released to the company pool`, '');
      await AW.load();
    } catch (e) { AW.toast('Could not release: ' + (e.message || e), 'red'); }
  };
})();
