/* FPBudget Actuals workspace — a02: drag and drop + recent targets.
   (ACTUALS_HANDOFF Phase 3, 2026-11-01.)

   Ported from the legacy budget tab (static/budget-js/b04.js ~3260-3790:
   _coaApplyDrop, _recordRecent / #coa-recent, the file-drop-on-row
   upload, the single-highlight rule). Native HTML5 DnD, no library.

   Rules kept from the legacy code:
     - Dragging a row that sits in the selection drags the whole selection;
       dragging an unselected row drags just that row.
     - ONE highlighted target at a time; per-element dragleave is
       unreliable across child nodes, so every dragover first clears all
       highlights, and dragend wipes them.
     - A file dropped on a row posts to upload-receipt; the row keeps its
       dashed outline for the whole hover (re-asserted on every dragover).
   New here: the ghost shows count and total, a split / itemized parent
   refuses to be dragged (its sublines carry the coding), and every drop
   goes through AW.applyDrop which set-line-bulk's many rows in one call. */
(function () {
  'use strict';
  const AW = window.AW;
  const S = AW.state, $ = AW.$, PID = AW.PID;
  const RECENT_KEY = 'fpRecentCoa.' + PID;   // same key the legacy tab used
  let drag = null;          // { tids:[], total }
  let hl = null;            // the highlighted target element
  let fileHL = null;        // row highlighted for a file drop

  const isFileDrag = e => !!(e.dataTransfer && e.dataTransfer.types && Array.from(e.dataTransfer.types).includes('Files'));
  const dropnoteFor = () => drag ? `Drop ${drag.tids.length} charge${drag.tids.length === 1 ? '' : 's'} · ${AW.money(drag.total)}` : '';

  function clearHL() {
    if (hl) {
      hl.classList.remove('aw-hl');
      const badge = hl.querySelector('.aw-dropbadge'); if (badge) badge.remove();
      hl = null;
    }
  }
  function setHL(el, note, cls) {
    if (hl === el) return;
    clearHL();
    hl = el;
    el.classList.add('aw-hl');
    if (note !== false) {
      const b = document.createElement('span');
      b.className = 'aw-dropbadge num' + (cls ? ' ' + cls : '');
      b.textContent = note || dropnoteFor();
      const nums = el.querySelector('.aw-nums');
      if (nums) el.insertBefore(b, nums); else el.appendChild(b);
    }
  }
  function clearFileHL() { if (fileHL) { fileHL.classList.remove('filehl'); fileHL = null; } }

  // ── Recent targets strip (localStorage, per project) ─────────────────
  function recentLoad() { try { return JSON.parse(localStorage.getItem(RECENT_KEY) || '[]'); } catch (e) { return []; } }
  AW.recordRecent = function (entry) {
    if (!entry || entry.code === 'not_project') return;
    let list = recentLoad();
    const key = o => o.kind + ':' + (o.lineId != null ? ('L' + o.lineId) : ('S' + o.code));
    list = list.filter(x => key(x) !== key(entry));
    list.unshift(entry);
    try { localStorage.setItem(RECENT_KEY, JSON.stringify(list.slice(0, 5))); } catch (e) {}
    AW.renderRecent();
  };
  AW.recentTargets = recentLoad;
  AW.renderRecent = function () {
    const host = $('aw-recent'); if (!host) return;
    const list = recentLoad().filter(e => e.kind === 'section' || S.lineById.has(+e.lineId));
    if (!list.length || S.view === 'sync' || S.view === 'not_project' || S.view === 'receipts') { host.innerHTML = ''; return; }
    host.innerHTML = '<span class="aw-label">Recent targets</span>' + list.map((e, i) => {
      const label = e.kind === 'line' ? (S.lineById.get(+e.lineId) || {}).label || e.label : `${e.code} · ${e.name || e.label || ''}`;
      return `<button type="button" class="aw-pill" data-recent="${i}" data-kind="${e.kind}" ${e.kind === 'line' ? `data-line-id="${e.lineId}"` : `data-code="${AW.esc(e.code)}" data-name="${AW.esc(e.name || '')}"`}
        title="${e.kind === 'line' ? 'Assign the selection to this line, or drop charges here' : 'Code the selection to this section (no line yet)'}"><span>${AW.esc(label)}</span></button>`;
    }).join('') + `<button type="button" class="aw-pill" data-recent-clear aria-label="Clear recent targets" style="color:var(--text-muted)"><span>clear</span></button>`;
  };
  $('aw-recent').addEventListener('click', e => {
    if (e.target.closest('[data-recent-clear]')) { try { localStorage.removeItem(RECENT_KEY); } catch (x) {} AW.renderRecent(); return; }
    const pill = e.target.closest('.aw-pill[data-recent]'); if (!pill) return;
    if (!S.sel.size) { AW.toast('Select a charge first, then click a recent target to assign it.', ''); return; }
    const target = pill.dataset.kind === 'line'
      ? { lineId: +pill.dataset.lineId, label: pill.textContent.trim() }
      : { code: pill.dataset.code, name: pill.dataset.name };
    AW.actions['assign-to'](target);
  });

  // ── Apply a drop / a keyboard assign ─────────────────────────────────
  // target: {lineId, label} for a line, {code, name} for a section.
  AW.applyDrop = async function (target, tids) {
    tids = (tids || []).map(Number).filter(Boolean);
    if (!tids.length) return;
    const containers = tids.filter(id => { const t = S.byId.get(id); return t && AW.isContainer(t); });
    if (containers.length) {
      AW.toast('A split or itemized charge carries its coding on its sublines. Remove the split first.', 'yellow');
      tids = tids.filter(id => !containers.includes(id));
      if (!tids.length) return;
    }
    const ts = tids.map(id => S.byId.get(id)).filter(Boolean);
    const total = ts.reduce((a, t) => a + Number(t.amount || 0), 0);
    try {
      let activityIds = [], label = '';
      if (target.lineId != null) {
        label = target.label || (S.lineById.get(+target.lineId) || {}).label || 'line';
        if (tids.length === 1) {
          const r = await AW.post(AW.txnUrl(tids[0], 'set-line'), { budget_line_id: +target.lineId });
          activityIds = [r.activity_id];
        } else {
          const r = await AW.post(`/projects/${PID}/actuals/transactions/set-line-bulk`, { transaction_ids: tids, budget_line_id: +target.lineId });
          activityIds = r.activity_ids || [];
          if (r.failed) AW.toast(`${r.failed} could not be assigned${r.errors && r.errors.length ? ': ' + r.errors.join(', ') : ''}.`, 'yellow');
        }
        const l = S.lineById.get(+target.lineId);
        AW.recordRecent({ kind: 'line', lineId: +target.lineId, code: l ? l.account_code : null, name: l ? l.description : label, label });
      } else {
        label = `${target.code} · ${target.name || ''}`.trim();
        if (tids.length === 1) {
          const r = await AW.post(AW.txnUrl(tids[0], 'set-coa'), { account_code: target.code, account_code_name: target.name || '' });
          activityIds = [r.activity_id];
        } else {
          const r = await AW.post(`/projects/${PID}/actuals/transactions/set-line-bulk`, { transaction_ids: tids, account_code: target.code });
          activityIds = r.activity_ids || [];
          if (r.failed) AW.toast(`${r.failed} could not be coded.`, 'yellow');
        }
        AW.recordRecent({ kind: 'section', code: target.code, name: target.name, label: target.name });
      }
      AW.toast(`${tids.length === 1 ? ts[0].vendor + ' ' + AW.money(ts[0].amount) : tids.length + ' charges · ' + AW.money(total)} → ${label}`, '', { activityIds });
      AW.clearSelection();
      await AW.load();
    } catch (e) {
      AW.toast('Could not assign: ' + (e.message || e), 'red');
    }
  };

  // ── Row drag source (delegated: rows re-render) ──────────────────────
  const table = $('aw-table');
  table.addEventListener('dragstart', e => {
    const row = e.target.closest && e.target.closest('.aw-row[data-tid][data-idx]');
    if (!row) return;
    if (e.target.closest('button, a, input, select')) { e.preventDefault(); return; }
    const tid = +row.dataset.tid;
    const t = S.byId.get(tid);
    if (t && AW.isContainer(t)) {
      e.preventDefault();
      AW.toast('This charge is split or itemized. Its sublines carry the coding; remove the split to move it as one.', 'yellow');
      return;
    }
    const tids = S.sel.has(tid) ? Array.from(S.sel).filter(id => !AW.isContainer(S.byId.get(id) || {})) : [tid];
    const total = tids.reduce((a, id) => a + Number((S.byId.get(id) || {}).amount || 0), 0);
    drag = { tids, total };
    const ghost = $('aw-ghost');
    ghost.textContent = `${tids.length} charge${tids.length === 1 ? '' : 's'} · ${AW.money(total)}`;
    try { e.dataTransfer.setDragImage(ghost, 16, 18); } catch (x) {}
    e.dataTransfer.effectAllowed = 'move';
    try { e.dataTransfer.setData('text/plain', tids.join(',')); } catch (x) {}
    tids.forEach(id => { const r = table.querySelector(`.aw-row[data-tid="${id}"]`); if (r) r.classList.add('dragging'); });
  });
  table.addEventListener('dragend', () => {
    table.querySelectorAll('.aw-row.dragging').forEach(r => r.classList.remove('dragging'));
    drag = null; clearHL(); clearFileHL();
  });
  document.addEventListener('dragend', () => { clearHL(); clearFileHL(); });

  // ── Drop targets: budget lines, section headers, recent pills ────────
  function targetOf(e) {
    const line = e.target.closest && e.target.closest('.aw-line[data-target="line"]');
    if (line) return { el: line, lineId: +line.dataset.lineId, label: line.dataset.label };
    const head = e.target.closest && e.target.closest('.aw-sec-head[data-target="section"]');
    if (head) return { el: head, code: head.dataset.code, name: head.dataset.name, section: true };
    const pill = e.target.closest && e.target.closest('.aw-pill[data-recent]');
    if (pill) return pill.dataset.kind === 'line'
      ? { el: pill, lineId: +pill.dataset.lineId, label: pill.textContent.trim(), pill: true }
      : { el: pill, code: pill.dataset.code, name: pill.dataset.name, section: true, pill: true };
    return null;
  }
  function onDragOver(e) {
    if (!drag) return;
    const t = targetOf(e);
    if (!t) { clearHL(); return; }
    e.preventDefault(); e.stopPropagation();
    e.dataTransfer.dropEffect = 'move';
    if (t.pill) setHL(t.el, false);
    else if (t.section) setHL(t.el, 'codes to section, no line yet', 'quiet');
    else setHL(t.el);
  }
  function onDragLeave(e) {
    if (!drag || !hl) return;
    if (!hl.contains(e.relatedTarget)) clearHL();
  }
  async function onDrop(e) {
    if (!drag) return;
    const t = targetOf(e);
    if (!t) return;
    e.preventDefault(); e.stopPropagation();
    const tids = drag.tids.slice();
    drag = null; clearHL();
    table.querySelectorAll('.aw-row.dragging').forEach(r => r.classList.remove('dragging'));
    await AW.applyDrop(t.lineId != null ? { lineId: t.lineId, label: t.label } : { code: t.code, name: t.name }, tids);
  }
  [$('aw-budget'), $('aw-recent')].forEach(host => {
    host.addEventListener('dragover', onDragOver);
    host.addEventListener('dragenter', onDragOver);
    host.addEventListener('dragleave', onDragLeave);
    host.addEventListener('drop', onDrop);
  });

  // ── File drop on a row → upload-receipt ──────────────────────────────
  table.addEventListener('dragenter', e => {
    if (!isFileDrag(e)) return;
    const row = e.target.closest && e.target.closest('.aw-row[data-tid][data-idx]');
    if (!row) return;
    e.preventDefault();
    if (fileHL !== row) { clearFileHL(); fileHL = row; row.classList.add('filehl'); }
  });
  table.addEventListener('dragover', e => {
    if (!isFileDrag(e)) return;
    const row = e.target.closest && e.target.closest('.aw-row[data-tid][data-idx]');
    if (!row) { clearFileHL(); return; }
    e.preventDefault();
    e.dataTransfer.dropEffect = 'copy';
    if (fileHL !== row) { clearFileHL(); fileHL = row; row.classList.add('filehl'); }
  });
  table.addEventListener('dragleave', e => {
    if (fileHL && !fileHL.contains(e.relatedTarget)) clearFileHL();
  });
  table.addEventListener('drop', async e => {
    if (!isFileDrag(e)) return;
    const row = e.target.closest && e.target.closest('.aw-row[data-tid][data-idx]');
    if (!row) return;
    e.preventDefault(); e.stopPropagation();
    clearFileHL();
    const files = e.dataTransfer.files;
    if (!files || !files.length) return;
    if (files.length > 1) { AW.toast('Drop one file at a time onto a charge.', 'yellow'); return; }
    const tid = +row.dataset.tid;
    const t = S.byId.get(tid);
    if (t && t.receipt) { AW.toast('This charge already has a receipt. Unlink it from the row menu to replace it.', 'yellow'); return; }
    await AW.uploadReceipt(tid, files[0]);
  });

  AW.uploadReceipt = async function (tid, file) {
    const t = S.byId.get(tid) || {};
    const busy = AW.toast(`Uploading ${file.name} and reading it with OCR`, '', { ttl: 60000 });
    const fd = new FormData();
    fd.append('file', file);
    try {
      const d = await AW.postForm(AW.txnUrl(tid, 'upload-receipt'), fd);
      busy.remove();
      AW.toast(`Receipt attached to ${t.vendor || 'the charge'}: ${d.filed_filename || file.name}`, '', { activityId: d.activity_id });
      await AW.load();
      return d;
    } catch (e) {
      busy.remove();
      AW.toast('Upload failed: ' + (e.message || e), 'red');
      throw e;
    }
  };
})();
