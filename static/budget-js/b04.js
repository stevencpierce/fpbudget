// Extracted from a templates/budget.html inline block (audit M9, 2026-07-20).
// Jinja values arrive via window.__BJ (set by the inline preamble before this
// script tag). Classic script, document order — semantics identical to inline.
//
// Cleanup B (2026-11-02): the in-page Actuals pane (#tab-actuals) is gone —
// the Actuals workspace is /projects/<pid>/actuals (static/actuals-js). What
// is left here is what the budget page still uses: the Dashboard / Action
// Center, the 'Find duplicates' reconcile modal it opens, the budget-line
// picker glue the document editor modal (b03.js + doc-editor.js) mounts
// with, and the shared toast.

  (function(){
    const PROJ_ID = window.__BJ["b04_PROJ_ID"];

    // ── ＋ New budget line from ANY picker (2026-07-20) ──────────────────
    // Creates a Working-budget line under any COA section (added to the budget
    // if missing) then patches the shared options template + every live picker.
    window._pickerInsertNewLine = function (nl) {
      function patch(sel) {
        if (!sel) return;
        let grp = null;
        sel.querySelectorAll('optgroup').forEach(g => {
          if (!grp && (g.label || '').indexOf(String(nl.account_code)) === 0) grp = g;
        });
        if (!grp) {
          grp = document.createElement('optgroup');
          grp.label = nl.account_code + '  ' + nl.section_name;
          const so = document.createElement('option');
          so.value = 'section:' + nl.account_code;
          so.textContent = '📂 All ' + nl.section_name + ' (section-only, no specific line)';
          grp.appendChild(so);
          sel.appendChild(grp);
        }
        if (!grp.querySelector('option[value="' + nl.line_id + '"]')) {
          const o = document.createElement('option');
          o.value = String(nl.line_id);
          o.textContent = nl.label;
          grp.appendChild(o);
        }
      }
      patch(document.getElementById('actualsLineOptsTpl'));
      document.querySelectorAll('select.actuals-line-picker[data-populated="1"], select.ditem-line').forEach(patch);
    };
    window._newBudgetLineFlow = function (onCreated) {
      let dlg = document.getElementById('newBudgetLineDlg');
      if (!dlg) {
        dlg = document.createElement('div');
        dlg.id = 'newBudgetLineDlg';
        dlg.style.cssText = 'position:fixed;inset:0;z-index:10050;background:rgba(0,0,0,.55);display:flex;align-items:center;justify-content:center';
        const secs = (window.COA_SECTIONS || []).map(s => '<option value="' + s[0] + '">' + s[0] + ' · ' + s[1] + '</option>').join('');
        dlg.innerHTML = '<div style="background:var(--bg-card);border:1px solid var(--border);border-radius:10px;padding:16px 18px;width:340px;max-width:92vw">'
          + '<div style="font-weight:700;font-size:14px;margin-bottom:10px">＋ New budget line</div>'
          + '<label style="font-size:11px;color:var(--text-muted)">COA section (any — added to the budget if missing)</label>'
          + '<select id="nblSection" style="width:100%;margin:3px 0 10px;padding:6px;background:var(--bg-input);color:var(--text);border:1px solid var(--border);border-radius:6px">' + secs + '</select>'
          + '<label style="font-size:11px;color:var(--text-muted)">Line description</label>'
          + '<input id="nblDesc" placeholder="e.g. Parking" style="width:100%;margin:3px 0 4px;padding:6px;background:var(--bg-input);color:var(--text);border:1px solid var(--border);border-radius:6px">'
          + '<div id="nblStatus" style="font-size:11px;color:#e0a13a;min-height:14px"></div>'
          + '<div style="display:flex;gap:8px;justify-content:flex-end;margin-top:8px">'
          + '<button type="button" id="nblCancel" style="padding:6px 12px;border-radius:6px;background:none;border:1px solid var(--border);color:var(--text);cursor:pointer">Cancel</button>'
          + '<button type="button" id="nblCreate" style="padding:6px 14px;border-radius:6px;background:#1f7a4d;border:none;color:#fff;font-weight:600;cursor:pointer">Create line</button>'
          + '</div></div>';
        document.body.appendChild(dlg);
      }
      dlg.style.display = 'flex';
      const desc = dlg.querySelector('#nblDesc'), stat = dlg.querySelector('#nblStatus');
      desc.value = ''; stat.textContent = '';
      dlg.querySelector('#nblCancel').onclick = () => { dlg.style.display = 'none'; };
      dlg.querySelector('#nblCreate').onclick = async () => {
        const code = dlg.querySelector('#nblSection').value;
        const d = desc.value.trim();
        if (!d) { stat.style.color = '#e0a13a'; stat.textContent = 'Enter a description.'; return; }
        stat.style.color = 'var(--text-muted)'; stat.textContent = 'Creating…';
        try {
          const r = await fetch('/projects/' + PROJ_ID + '/actuals/budget-line/new', {
            method: 'POST', credentials: 'same-origin', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ account_code: code, description: d }) });
          const j = await r.json();
          if (r.ok && j.ok) { dlg.style.display = 'none'; onCreated(j); }
          else { stat.style.color = '#e0a13a'; stat.textContent = j.error || ('Failed (' + r.status + ')'); }
        } catch (e) { stat.style.color = '#e0a13a'; stat.textContent = 'Error: ' + e.message; }
      };
      setTimeout(() => desc.focus(), 50);
    };

    // 📎 v2 backup chooser: list likely targets (itemized invoice sublines
    // first, ranked by amount closeness) and hard-link the receipt to the one
    // picked, so the Line Ledger shows the receipt beside that line.
    window._backupChooser = async function (tid, selectEl) {
      let cands = [];
      try {
        const r = await fetch(`/projects/${PROJ_ID}/actuals/transaction/${tid}/backup-candidates`, {credentials:'same-origin'});
        const j = await r.json();
        cands = (j && j.candidates) || [];
      } catch (e) {}
      const esc = FP.esc;                 // shared rule (fp-utils.js)
      const money = v => FP.money(v);
      let dlg = document.getElementById('backupChooserDlg');
      if (dlg) dlg.remove();
      dlg = document.createElement('div');
      dlg.id = 'backupChooserDlg';
      dlg.style.cssText = 'position:fixed;inset:0;z-index:10060;background:rgba(0,0,0,.55);display:flex;align-items:center;justify-content:center';
      const rows = cands.map(c =>
        '<label style="display:flex;gap:8px;align-items:center;padding:6px 8px;border:1px solid var(--border);border-radius:6px;margin-bottom:6px;cursor:pointer">'
        + '<input type="radio" name="bkTarget" value="' + c.id + '">'
        + '<span style="flex:1;min-width:0">' + (c.is_split ? '↳ ' : '') + esc(c.vendor || '—')
        + (c.note ? ' <span style="color:var(--text-muted);font-size:.75rem">' + esc(c.note) + '</span>' : '') + '</span>'
        + '<span style="color:var(--text-muted);font-size:.78rem;white-space:nowrap">' + esc(c.date || '') + '</span>'
        + '<b style="white-space:nowrap">' + money(c.amount) + '</b></label>').join('');
      dlg.innerHTML = '<div style="background:var(--bg-card);border:1px solid var(--border);border-radius:10px;padding:16px 18px;width:520px;max-width:94vw;max-height:80vh;overflow:auto">'
        + '<div style="font-weight:700;font-size:14px;margin-bottom:4px">📎 This receipt backs up…</div>'
        + '<div style="font-size:11.5px;color:var(--text-muted);margin-bottom:10px">Pick the charge it documents (invoice line items listed first). Its own charge stops counting; the file stays attached in the Line Ledger.</div>'
        + (rows || '<div style="color:var(--text-muted);font-size:.85rem;margin-bottom:8px">No likely targets found.</div>')
        + '<label style="display:flex;gap:8px;align-items:center;padding:6px 8px;cursor:pointer;color:var(--text-muted)">'
        + '<input type="radio" name="bkTarget" value=""> No specific charge — just mark as backup</label>'
        + '<div style="display:flex;gap:8px;justify-content:flex-end;margin-top:10px">'
        + '<button type="button" id="bkCancel" style="padding:6px 12px;border-radius:6px;background:none;border:1px solid var(--border);color:var(--text);cursor:pointer">Cancel</button>'
        + '<button type="button" id="bkSave" style="padding:6px 14px;border-radius:6px;background:#1f7a4d;border:none;color:#fff;font-weight:600;cursor:pointer">Mark as backup</button>'
        + '</div></div>';
      document.body.appendChild(dlg);
      dlg.querySelector('#bkCancel').onclick = () => dlg.remove();
      dlg.querySelector('#bkSave').onclick = async () => {
        const sel = dlg.querySelector('input[name="bkTarget"]:checked');
        if (!sel) { return; }
        try {
          const r = await fetch(`/projects/${PROJ_ID}/actuals/transaction/${tid}/mark-backup`, {
            method:'POST', credentials:'same-origin', headers:{'Content-Type':'application/json'},
            body: JSON.stringify(sel.value ? { target_txn_id: parseInt(sel.value) } : { backs_up: '' }) });
          const j = await r.json();
          if (r.ok && j.ok) {
            dlg.remove();
            _actualsToast('📎 Marked as backup — charge no longer counts.', 'green');
          } else if (typeof _actualsToast === 'function') _actualsToast(j.error || 'Failed', 'yellow');
        } catch (e) { if (typeof _actualsToast === 'function') _actualsToast('Error: ' + e.message, 'yellow'); }
      };
    };

    // Click-to-code from the document editor modal's budget-line picker
    // (DocEditor.init → setLine). The dropdown supports these value forms:
    //   ""                — no-op (the "— pick budget line —" placeholder)
    //   "__clear__"       — clear the assignment via unlink
    //   "section:<code>"  — section-only (no specific line) → /set-coa
    //   "<line_id>"       — specific line → /set-line
    //   "__backup__" / "not_project" / "__newline__" — see below
    window.actualsSetLine = async function (selectEl) {
      const tid    = parseInt(selectEl.dataset.tid);
      const value  = selectEl.value;
      if (!value) return;  // placeholder — nothing to do
      if (value === '__newline__') {
        // Revert the visible pick, run the create flow, then re-enter with the
        // real new line id via a synthetic change (works for actuals rows AND
        // the doc-detail main picker, which share this handler). (2026-07-20.)
        selectEl.value = selectEl.dataset.current || '';
        window._newBudgetLineFlow(function (nl) {
          window._pickerInsertNewLine(nl);
          selectEl.value = String(nl.line_id);
          selectEl.dispatchEvent(new Event('change'));
        });
        return;
      }
      selectEl.disabled = true;
      try {
        let url, body;
        if (value === '__clear__') {
          // Clear: send null to /set-line which calls unlink_transaction
          url  = `/projects/${PROJ_ID}/actuals/transaction/${tid}/set-line`;
          body = JSON.stringify({ budget_line_id: null });
        } else if (value.startsWith('section:')) {
          // Section-only: set account_code without a specific line.
          const code = value.slice(8);
          const _rowS = selectEl.closest('.actuals-txn-row');
          const _mkExpS = !!(_rowS && _rowS.dataset.source === 'doc_upload' && _rowS.dataset.activated === '0');
          if (_mkExpS && _rowS) _rowS.dataset.activated = '1';
          url  = `/projects/${PROJ_ID}/actuals/transaction/${tid}/set-coa`;
          body = JSON.stringify(_mkExpS ? { account_code: code, create_expense: true }
                                        : { account_code: code });
        } else if (value === '__backup__') {
          // Receipt is DOCUMENTATION for a charge already on the ledger — open
          // the target chooser (v2); it POSTs mark-backup itself.
          selectEl.value = selectEl.dataset.current || '';
          selectEl.disabled = false;
          window._backupChooser(tid, selectEl);
          return;
        } else if (value === 'not_project') {
          // Mark not-a-project-expense right from the picker (User 2026-06-22).
          url  = `/projects/${PROJ_ID}/actuals/transaction/${tid}/set-coa`;
          body = JSON.stringify({ account_code: 'not_project' });
        } else {
          // Specific budget line. On an un-activated DOCUMENT row this is the
          // Create-expense step (Actualizing 2.0 A1): the server stamps
          // activated_at and the receipt rides along as backup evidence.
          const _row = selectEl.closest('.actuals-txn-row');
          const _mkExp = !!(_row && _row.dataset.source === 'doc_upload' && _row.dataset.activated === '0');
          if (_mkExp && _row) _row.dataset.activated = '1';
          url  = `/projects/${PROJ_ID}/actuals/transaction/${tid}/set-line`;
          body = JSON.stringify(_mkExp ? { budget_line_id: value, create_expense: true }
                                       : { budget_line_id: value });
          if (_mkExp && typeof _actualsToast === 'function')
            setTimeout(() => _actualsToast('＋ Expense created from this document — coded, with the file attached as backup.', 'green'), 400);
        }
        const r = await fetch(url, {
          method:  'POST',
          headers: { 'Content-Type': 'application/json' },
          body:    body,
        });
        const d = await r.json();
        if (!r.ok) {
          alert('Could not save: ' + (d.error || r.status));
          return;
        }
        // First-time auto-init toasts (only for line-level links).
        if (d.working_was_just_created && d.actual_was_just_created) {
          _actualsToast('Working budget initialized from Estimated. Actual budget started for live tracking.');
        } else if (d.actual_was_just_created) {
          _actualsToast('Actual budget started — Working was cloned for live tracking.');
        } else if (value === '__clear__') {
          _actualsToast('Cleared.', 'yellow');
        } else if (value === 'not_project') {
          _actualsToast('Marked “not a project expense.”', 'yellow');
        } else if (value.startsWith('section:')) {
          _actualsToast('Coded to section. Pick a specific line for per-line tracking.', 'green');
        }
        // (Cleanup B, 2026-11-02: the per-row patching that followed here served
        // the retired #tab-actuals list; on this page the only picker is the
        // document editor modal's, and the Line Ledger re-fetches on open.)
        // After a clear or not-project, force the dropdown back to the placeholder.
        if (value === 'not_project') {
          selectEl.value = '';
        }
        if (value === '__clear__') {
          selectEl.value = '';
          // Hide the Clear option until the row is reassigned again.
          const clearOpt = selectEl.querySelector('option[value="__clear__"]');
          if (clearOpt) clearOpt.remove();
        }
      } catch (e) {
        alert('Save failed: ' + e.message);
      } finally {
        selectEl.disabled = false;
      }
    };

    // Lazy-fill a single picker from the shared options template (perf:
    // options aren't baked into every row). Idempotent. 2026-05-29.
    window._actualsFillPicker = function (sel) {
      if (!sel || sel.dataset.populated === '1') return;
      const tpl = document.getElementById('actualsLineOptsTpl');
      if (!tpl) return;
      sel.innerHTML = tpl.innerHTML;
      sel.dataset.populated = '1';
      sel.value = sel.dataset.current || '';
    };

    // ── Dashboard / Action Center (review queue) — 2026-06-18 ──────────
    function _dashEsc(s) { return FP.esc(s); }   // shared rule (fp-utils.js; this copy escaped only '<')
    window.dashLoadAnomalies = async function () {
      const host = document.getElementById('dash-action-center');
      if (!host) return;
      host.innerHTML = '<div style="font-size:.7rem;color:var(--text-muted)">Loading review queue…</div>';
      try {
        const j = await (await fetch(`/projects/${PROJ_ID}/anomalies`)).json();
        dashRenderAnomalies(j.items || []);
      } catch (e) {
        host.innerHTML = '<div style="color:#e08080;font-size:.7rem">Could not load review queue: ' + _dashEsc(e.message) + '</div>';
      }
    };
    // Type metadata for the grouped, collapsible Action Center. (User 2026-06-22:
    // the flat list got too long to scroll — group by kind, collapse, and cap the
    // height so each kind is one compact header until expanded.)
    const _DASH_GROUP_META = {
      double_coded:     { icon: '⧉',  label: 'Duplicate charges',   order: 1 },
      duplicate_receipt:{ icon: '🧾', label: 'Duplicate receipts',  order: 1.5 },
      code_suggestion:  { icon: '✨', label: 'Codes to confirm',     order: 2 },
      budget_mismatch: { icon: '📊', label: 'Budget mismatches',   order: 3 },
      data_issue:      { icon: '⚠',  label: 'Check extracted data', order: 4 },
      people_line:     { icon: '👤', label: 'Assign to person',     order: 5 },
      vendor_cleanup:  { icon: '🏷️', label: 'Confirm vendors',      order: 6 },
    };
    window._dashCodeMap = window._dashCodeMap || {};
    function dashRenderAnomalies(items) {
      const host = document.getElementById('dash-action-center');
      if (!host) return;
      if (!items.length) {
        host.innerHTML = '<div style="border:1px dashed var(--border);border-radius:8px;padding:14px;text-align:center;color:var(--text-muted);font-size:.8rem">✓ Nothing needs review. Settings → Maintenance has “Run clean up” and “Scan now” to check again.</div>';
        return;
      }
      const sev = { high: '#e08080', medium: '#e0c060', low: '#9ec0ff' };
      const groups = {};
      items.forEach(it => { (groups[it.type] = groups[it.type] || []).push(it); });
      const types = Object.keys(groups).sort((a, b) =>
        ((_DASH_GROUP_META[a]?.order || 9) - (_DASH_GROUP_META[b]?.order || 9)));
      const head = `<style>#dash-action-center .dash-group>summary::-webkit-details-marker{display:none}
          #dash-action-center .dash-group>summary::marker{content:''}</style>
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
          <h4 style="margin:0;font-size:.92rem">⚠ Action Center</h4>
          <span style="font-size:.7rem;color:var(--text-muted)">${items.length} item${items.length > 1 ? 's' : ''} need review</span>
          <button type="button" class="btn btn-xs btn-ghost" style="margin-left:auto" onclick="dashToggleAllGroups()">Expand / collapse all</button>
        </div>`;
      const body = types.map((t, i) => {
        const m = _DASH_GROUP_META[t] || { icon: '•', label: t };
        const cards = groups[t].map(it => dashCardHtml(it, sev)).join('');
        // Open the top-priority group by default (when not huge); collapse the rest.
        const openAttr = (i === 0 && groups[t].length <= 8) ? ' open' : '';
        return `<details class="dash-group"${openAttr} style="margin-bottom:8px;border:1px solid var(--border);border-radius:8px;background:var(--bg-card)">
            <summary style="cursor:pointer;list-style:none;padding:8px 12px;display:flex;align-items:center;gap:8px;font-size:.82rem;font-weight:600">
              <span>${m.icon}</span><span>${_dashEsc(m.label)}</span>
              <span style="color:var(--text-muted);font-weight:400">(${groups[t].length})</span>
              <span style="margin-left:auto;color:var(--text-muted);font-size:.78rem">▾</span>
            </summary>
            <div style="padding:0 10px 6px">${cards}</div>
          </details>`;
      }).join('');
      host.innerHTML = head +
        `<div id="dash-ac-scroll" style="max-height:440px;overflow-y:auto;padding-right:4px">${body}</div>`;
    }
    window.dashToggleAllGroups = function () {
      const gs = document.querySelectorAll('#dash-ac-scroll .dash-group');
      const anyClosed = [...gs].some(g => !g.open);
      gs.forEach(g => { g.open = anyClosed; });
    };
    function dashCardHtml(it, sev) {
      const c = sev[it.severity] || '#9ec0ff';
      let actions = '';
      if (it.type === 'vendor_cleanup') {
        const s = (it.payload && it.payload.suggested) || '';
        if (s) actions = `<button type="button" class="btn btn-xs btn-primary" onclick="dashResolve(${it.id},'apply_vendor',this)">Use “${_dashEsc(s)}”</button>`;
      } else if (it.type === 'data_issue') {
        if (it.payload && it.payload.suggested_amount != null)
          actions += `<button type="button" class="btn btn-xs btn-primary" onclick="dashResolve(${it.id},'accept_amount',this)">Accept $${_dashEsc(it.payload.suggested_amount)}</button>`;
      } else if (it.type === 'double_coded') {
        const members = (it.payload && it.payload.members || []).filter(m => m.coded);
        // Differentiate the rows: the one WITH a receipt is the keeper (green),
        // the bare copy is the likely duplicate (amber). Each in its own box;
        // Delete is red, Un-code amber. (User 2026-06-18.)
        actions += '<div style="font-size:.7rem;color:var(--text-muted);margin:2px 0 5px">Same spend coded twice — keep the one with a receipt, remove the other.</div>';
        actions += members.map(m => {
          const hasDoc = m.doc_upload_id != null;
          const tone = hasDoc ? { bd: '#2a6a48', bg: 'rgba(116,198,157,.10)', tag: '#74c69d', label: 'KEEP · has receipt' }
                              : { bd: '#7a5a2a', bg: 'rgba(224,200,96,.08)', tag: '#e0c060', label: 'likely DUPLICATE' };
          return `<div style="border:1px solid ${tone.bd};background:${tone.bg};border-radius:6px;padding:6px 8px;margin-bottom:5px">
             <div style="display:flex;gap:6px;align-items:center;font-size:.72rem">
               <span style="flex:none;font-weight:700;color:${tone.tag};font-size:.62rem;text-transform:uppercase;letter-spacing:.04em">${tone.label}</span>
               <span style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">#${m.tid} · ${_dashEsc(m.vendor)} · $${(m.amount != null ? Math.abs(m.amount).toFixed(2) : '?')}${m.account_code_name ? (' · ' + _dashEsc(m.account_code_name)) : ''}</span>
             </div>
             <div style="display:flex;gap:6px;margin-top:5px;justify-content:flex-end">
               <button type="button" class="btn btn-xs btn-ghost" onclick="dashViewTxn(${m.tid}, ${hasDoc ? m.doc_upload_id : 'null'})" title="Show this charge">View</button>
               <button type="button" class="btn btn-xs" style="border-color:#7a5a2a;color:#e0c060" onclick="dashResolveTxn(${it.id},'uncode',${m.tid},this)" title="Remove this charge's budget coding (keeps the charge)">Un-code</button>
               <button type="button" class="btn btn-xs" style="border-color:#7a3a3a;color:#e08080" onclick="dashResolveTxn(${it.id},'delete_txn',${m.tid},this)" title="Delete this duplicate charge">Delete</button>
             </div>
           </div>`;
        }).join('');
        actions += `<div style="margin-top:2px;display:flex;gap:6px"><button type="button" class="btn btn-xs btn-ghost" onclick="dashAskAi(${it.id},this)">🤖 Ask AI</button><button type="button" class="btn btn-xs btn-ghost" onclick="openReconcile()" title="Open the full side-by-side duplicate view">🔍 Open in Find duplicates</button></div>`;
      } else if (it.type === 'budget_mismatch') {
        const p = it.payload || {};
        if (p.kind === 'po_over_cap') {
          actions += `<a class="btn btn-xs btn-primary" href="/projects/${PROJ_ID}/pos">Open POs</a>`;
        } else {
          actions += `<a class="btn btn-xs btn-primary" href="/projects/${PROJ_ID}/actuals">Open Actuals</a>`;
        }
        actions += `<button type="button" class="btn btn-xs btn-ghost" onclick="dashAskAi(${it.id},this)">🤖 Ask AI</button>`;
      } else if (it.type === 'people_line') {
        const p = it.payload || {};
        actions += `<button type="button" class="btn btn-xs btn-primary" onclick="dashResolve(${it.id},'apply_people_line',this)" title="Code this charge to ${_dashEsc(p.line_label||'their line')} and attach ${_dashEsc(p.crew_name||'the person')}">Apply → ${_dashEsc(p.line_label || 'line')}</button>`;
      } else if (it.type === 'duplicate_receipt') {
        const p = it.payload || {};
        const docs = p.docs || [];
        actions += '<div style="font-size:.7rem;color:var(--text-muted);margin:2px 0 5px">Same spend has more than one receipt — keep one, delete the rest.</div>';
        actions += docs.map(d => {
          const isKeep = d.id === p.keep_doc_id;
          const tone = isKeep
            ? { bd: '#2a6a48', bg: 'rgba(116,198,157,.10)', tag: '#74c69d',
                label: d.matched ? 'KEEP · matched' : (d.coded ? 'KEEP · coded' : 'KEEP') }
            : { bd: '#7a5a2a', bg: 'rgba(224,200,96,.08)', tag: '#e0c060', label: 'duplicate' };
          return `<div style="border:1px solid ${tone.bd};background:${tone.bg};border-radius:6px;padding:6px 8px;margin-bottom:5px">
             <div style="display:flex;gap:6px;align-items:center;font-size:.72rem">
               <span style="flex:none;font-weight:700;color:${tone.tag};font-size:.62rem;text-transform:uppercase;letter-spacing:.04em">${tone.label}</span>
               <span style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${_dashEsc(d.filename)}</span>
             </div>
             <div style="display:flex;gap:6px;margin-top:5px;justify-content:flex-end">
               <button type="button" class="btn btn-xs btn-ghost" onclick="dashViewDoc(${d.id})" title="Open this receipt">View</button>
               ${isKeep ? '' : `<button type="button" class="btn btn-xs" style="border-color:#7a3a3a;color:#e08080" onclick="dashDeleteDupDoc(${d.id}, ${it.id}, this)" title="Move this duplicate receipt to Trash">🗑 Delete</button>`}
             </div>
           </div>`;
        }).join('');
      } else if (it.type === 'code_suggestion') {
        const p = it.payload || {};
        window._dashCodeMap[p.tid] = { code: p.code, name: p.name || '' };
        const lbl = _dashEsc(p.name || String(p.code || ''));
        actions += `<button type="button" class="btn btn-xs btn-primary" onclick="dashAcceptCode(${p.tid},this)" title="Code this charge to ${lbl}">Accept → ${lbl}</button>`;
        actions += `<button type="button" class="btn btn-xs btn-ghost" onclick="dashViewTxn(${p.tid}, null)" title="Show this charge in Actuals">🔍 View charge</button>`;
      }
      // Universal: always let the user OPEN the underlying item to decide. For
      // doc-based flags this opens the document (image + editable fields), which
      // also gives 'check extracted data' a real action. (User 2026-06-18.)
      if (it.doc_upload_id) {
        actions += `<button type="button" class="btn btn-xs btn-ghost" onclick="dashViewDoc(${it.doc_upload_id})" title="Open the document — image + vendor/amount/date you can fix">🔍 View document</button>`;
      } else if (it.transaction_id && it.type !== 'double_coded' && it.type !== 'code_suggestion') {
        actions += `<button type="button" class="btn btn-xs btn-ghost" onclick="dashViewTxn(${it.transaction_id}, null)" title="Show this charge in Actuals">🔍 View charge</button>`;
      }
      // Code suggestions use a string id (code-<tid>) + a different dismiss path.
      const _dismissCall = it.type === 'code_suggestion'
        ? `dashDismissCode(${(it.payload || {}).tid}, this)`
        : `dashDismiss(${it.id}, this)`;
      return `<div class="dash-flag" data-fid="${it.id}" style="border:1px solid var(--border);border-left:3px solid ${c};border-radius:8px;padding:10px 12px;margin-bottom:8px;background:var(--bg-card)">
        <div style="display:flex;align-items:flex-start;gap:8px">
          <div style="flex:1;min-width:0">
            <div style="font-size:.82rem;font-weight:600">${_dashEsc(it.title || it.type)}${it.confidence != null ? ` <span style="color:var(--text-muted);font-weight:400">· ${Math.round(it.confidence * 100)}%</span>` : ''}</div>
            <div style="font-size:.74rem;color:var(--text-muted);margin-top:2px">${_dashEsc(it.explanation || '')}</div>
            <div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap;align-items:center">${actions}</div>
          </div>
          <button type="button" class="btn btn-xs btn-ghost" onclick="${_dismissCall}" title="Dismiss — not an issue">✕</button>
        </div>
        <div class="dash-ai-verdict" style="display:none;margin-top:6px;font-size:.74rem;color:#b79dff"></div>
      </div>`;
    }
    function _dashRemoveCard(fid) {
      const el = document.querySelector('.dash-flag[data-fid="' + fid + '"]');
      if (el) el.remove();
      const host = document.getElementById('dash-action-center');
      if (host && !host.querySelector('.dash-flag')) dashRenderAnomalies([]);
    }
    // Phase 2: accept / dismiss an AI code suggestion straight from the queue.
    window.dashAcceptCode = async function (tid, btn) {
      const s = (window._dashCodeMap || {})[tid];
      if (!s) return;
      if (btn) { btn.disabled = true; btn.textContent = 'Coding…'; }
      try {
        const r = await fetch(`/projects/${PROJ_ID}/actuals/transaction/${tid}/set-coa`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ account_code: s.code, account_code_name: s.name }) });
        if (r.ok) {
          _dashRemoveCard('code-' + tid);
          if (typeof _actualsToast === 'function') _actualsToast('Coded ✓', 'green');
        } else {
          if (btn) { btn.disabled = false; btn.textContent = 'Accept'; }
          if (typeof _actualsToast === 'function') _actualsToast('Coding failed.', 'yellow');
        }
      } catch (e) {
        if (btn) { btn.disabled = false; btn.textContent = 'Accept'; }
        if (typeof _actualsToast === 'function') _actualsToast('Coding error: ' + e.message, 'yellow');
      }
    };
    window.dashDismissCode = async function (tid, btn) {
      try { await fetch(`/projects/${PROJ_ID}/actuals/transaction/${tid}/dismiss-code`, { method: 'POST' }); } catch (e) {}
      _dashRemoveCard('code-' + tid);
    };
    // Phase: delete a duplicate receipt straight from the queue (trashes the doc,
    // resolves the flag). (User 2026-06-22.)
    window.dashDeleteDupDoc = async function (docId, fid, btn) {
      if (!confirm('Move this duplicate receipt to Trash? You can restore it from Docs → Trash.')) return;
      if (btn) { btn.disabled = true; btn.textContent = 'Deleting…'; }
      try {
        const r = await fetch(`/docs/upload/${docId}/delete`, { method: 'POST', credentials: 'same-origin' });
        if (!r.ok) {
          if (btn) { btn.disabled = false; btn.textContent = '🗑 Delete'; }
          if (typeof _actualsToast === 'function') _actualsToast('Delete failed.', 'yellow');
          return;
        }
        if (typeof _actualsToast === 'function') _actualsToast('Duplicate receipt trashed ✓', 'green');
        try { await fetch(`/projects/${PROJ_ID}/anomalies/${fid}/dismiss`, { method: 'POST' }); } catch (e) {}
        _dashRemoveCard(fid);
      } catch (e) {
        if (btn) { btn.disabled = false; btn.textContent = '🗑 Delete'; }
        if (typeof _actualsToast === 'function') _actualsToast('Delete error: ' + e.message, 'yellow');
      }
    };
    // Open the underlying document (image + editable vendor/amount/date) or charge
    // so the user can SEE what a flag refers to and fix it. (User 2026-06-18.)
    window.dashViewDoc = function (docId) {
      if (typeof window.openDocDetail === 'function') window.openDocDetail(docId, null);
      else window.open('/docs/upload/' + docId + '/raw', '_blank');
    };
    window.dashViewTxn = function (tid, docId) {
      // Prefer the receipt (modal) when there is one; otherwise open the Actuals
      // workspace focused on the charge (?txn= scrolls to + highlights its row).
      // (User 2026-06-18; Cleanup B 2026-11-02 — the in-page pane is gone.)
      if (docId && typeof window.openDocDetail === 'function') { window.openDocDetail(docId, null); return; }
      window.location.href = '/projects/' + PROJ_ID + '/actuals?txn=' + encodeURIComponent(tid);
    };
    window.dashResolve = async function (fid, action, btn) {
      if (btn) btn.disabled = true;
      try {
        const r = await fetch(`/projects/${PROJ_ID}/anomalies/${fid}/resolve`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ action }) });
        if (r.ok) { _dashRemoveCard(fid); if (typeof _actualsToast === 'function') _actualsToast('Done ✓', 'green'); }
        else { if (btn) btn.disabled = false; alert('Action failed'); }
      } catch (e) { if (btn) btn.disabled = false; alert('Error: ' + e.message); }
    };
    window.dashResolveTxn = async function (fid, action, tid, btn) {
      if (action === 'delete_txn' && !confirm('Delete charge #' + tid + '? This removes the duplicate from Actuals.')) return;
      if (btn) btn.disabled = true;
      try {
        const r = await fetch(`/projects/${PROJ_ID}/anomalies/${fid}/resolve`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ action, transaction_id: tid }) });
        if (r.ok) { _dashRemoveCard(fid); if (typeof _actualsToast === 'function') _actualsToast('Resolved ✓', 'green'); }
        else { if (btn) btn.disabled = false; const d = await r.json().catch(() => ({})); alert(d.error || 'Action failed'); }
      } catch (e) { if (btn) btn.disabled = false; alert('Error: ' + e.message); }
    };
    window.dashDismiss = async function (fid, btn) {
      if (btn) btn.disabled = true;
      try { const r = await fetch(`/projects/${PROJ_ID}/anomalies/${fid}/dismiss`, { method: 'POST' }); if (r.ok) _dashRemoveCard(fid); else if (btn) btn.disabled = false; }
      catch (e) { if (btn) btn.disabled = false; }
    };
    window.dashAskAi = async function (fid, btn) {
      const card = document.querySelector('.dash-flag[data-fid="' + fid + '"]');
      const vEl = card && card.querySelector('.dash-ai-verdict');
      if (btn) { btn.disabled = true; btn.textContent = '🤖 thinking…'; }
      try {
        const j = await (await fetch(`/projects/${PROJ_ID}/anomalies/${fid}/ask-ai`, { method: 'POST' })).json();
        const v = j.verdict || {};
        if (vEl) {
          vEl.style.display = 'block';
          vEl.textContent = '🤖 ' + (v.explanation || (v.is_duplicate ? 'Likely the same spend (duplicate).' : 'These look like separate charges.')) + (v.confidence ? ` (${Math.round(v.confidence * 100)}%)` : '') + ' · suggests: ' + (v.recommended_action || 'review');
        }
      } catch (e) { if (vEl) { vEl.style.display = 'block'; vEl.textContent = 'AI error: ' + e.message; } }
      if (btn) { btn.disabled = false; btn.textContent = '🤖 Ask AI'; }
    };
    // Loop one cleanup endpoint until it drains. Returns {applied,flagged,processed}.
    async function _loopCleanup(url, onTick) {
      let applied = 0, flagged = 0, processed = 0;
      for (let i = 0; i < 120; i++) {
        let r;
        try { r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ limit: 8 }) }); }
        catch (e) { break; }
        if (!r.ok) break;
        const j = await r.json();
        applied += j.applied || 0; flagged += j.flagged || 0; processed += j.processed || 0;
        if (onTick) onTick(applied);
        if (!j.processed || !j.remaining) break;
      }
      return { applied, flagged, processed };
    }
    // Dashboard "Clean up data" — sweeps BOTH receipts (Docs) and charges
    // (Actuals: CSV/QBO/manual with no receipt) so the whole project converges.
    // Unified cleanup (Phase 1 consolidation 2026-06-22): ONE action that cleans
    // BOTH receipts and charges, wherever it's triggered. Dashboard + Actuals
    // buttons both call this so they no longer do different things.
    window.aiCleanupAll = async function (btn, reloadAfter) {
      const _o = btn ? btn.textContent : '';
      if (btn) { btn.disabled = true; btn.textContent = '✨ Cleaning…'; }
      try {
        const tick = (n) => { if (btn) btn.textContent = `✨ ${n} cleaned…`; };
        const docs = await _loopCleanup(`/projects/${PROJ_ID}/docs/ai-cleanup`, (n) => tick(n));
        const txns = await _loopCleanup(`/projects/${PROJ_ID}/actuals/ai-cleanup`, (n) => tick(docs.applied + n));
        const applied = docs.applied + txns.applied, flagged = docs.flagged + txns.flagged;
        const total = docs.processed + txns.processed;
        if (typeof _actualsToast === 'function') _actualsToast(total
          ? `Cleaned ${applied} vendor name(s) across receipts + charges; ${flagged} flagged for review.`
          : 'Nothing left to clean.', 'green');
        if (typeof dashLoadAnomalies === 'function') dashLoadAnomalies();
        if (reloadAfter && (applied || flagged)) setTimeout(() => window.location.reload(), 1100);
      } catch (e) { if (typeof _actualsToast === 'function') _actualsToast('Cleanup failed: ' + e.message, 'yellow'); }
      if (btn) { btn.disabled = false; btn.textContent = _o || '✨ Clean up'; }
    };
    window.dashCleanup    = function (btn) { return aiCleanupAll(btn, false); };

    window.dashScanDupes = async function (btn) {
      const _o = btn ? btn.textContent : '';
      if (btn) { btn.disabled = true; btn.textContent = '🔍 Scanning…'; }
      try {
        const j = await (await fetch(`/projects/${PROJ_ID}/actuals/scan-anomalies`, { method: 'POST' })).json();
        const total = (j.total_flags != null) ? j.total_flags : (j.flags || 0);
        if (typeof _actualsToast === 'function') _actualsToast(
          total ? `Found ${j.flags || 0} duplicate(s) + ${j.budget_flags || 0} budget mismatch(es) + ${j.people_flags || 0} person→line suggestion(s).`
                : 'No issues found.', (total ? 'yellow' : 'green'));
        dashLoadAnomalies();
      } catch (e) { if (typeof _actualsToast === 'function') _actualsToast('Scan failed: ' + e.message, 'yellow'); }
      if (btn) { btn.disabled = false; btn.textContent = _o || '🔍 Scan for issues'; }
    };
    // Load the queue when the Dashboard tab opens, and on first paint if active.
    (function () {
      const tb = document.querySelector('[data-tab="topsheet"]');
      if (tb) tb.addEventListener('click', () => setTimeout(dashLoadAnomalies, 150));
      const panel = document.getElementById('tab-topsheet');
      if (panel && panel.classList.contains('active')) setTimeout(dashLoadAnomalies, 500);
    })();

    function _actualsToast(msg, color) {
      const t = document.createElement('div');
      t.textContent = msg;
      const c = color || 'green';
      const palettes = {
        green:  { bg: '#1a3a2a', fg: '#74c69d', bd: '#2a5a3a' },
        yellow: { bg: '#2a2414', fg: '#f0c060', bd: '#4a3a1a' },
        red:    { bg: '#2a1414', fg: '#e08080', bd: '#4a2020' },
      }[c] || { bg: '#1a3a2a', fg: '#74c69d', bd: '#2a5a3a' };
      t.style.cssText = `position:fixed;bottom:24px;left:50%;transform:translateX(-50%);
        background:${palettes.bg};color:${palettes.fg};border:1px solid ${palettes.bd};
        border-radius:6px;padding:10px 18px;font-size:.85rem;z-index:9999;
        box-shadow:0 6px 24px rgba(0,0,0,.5);max-width:520px`;
      document.body.appendChild(t);
      setTimeout(() => t.remove(), 4000);
    }

    // ── Reconcile (Phase 1, User 2026-06-17) — unified duplicate review ──
    // One workspace over reconcile-scan: each spend cluster shows its receipts
    // + charges in two lanes with the dedup actions inline. Reuses the existing
    // resolve endpoints (docs resolve-batch, txn delete-duplicate, dismiss).
    window._rcBuild = function () {
      let m = document.getElementById('reconcile-modal');
      if (m) return m;
      m = document.createElement('div');
      m.id = 'reconcile-modal';
      m.style.cssText = 'display:none;position:fixed;inset:0;z-index:10000;background:rgba(0,0,0,.8)';
      m.innerHTML = `
        <style>
          #reconcile-modal .mr-chip{font-size:.62rem;padding:1px 6px;border-radius:4px;white-space:nowrap}
          #reconcile-modal .mr-chip.ok{background:#10231a;color:#5fd0a0;border:1px solid #1f6f4a}
          #reconcile-modal .mr-chip.warn{background:#2a2414;color:#e0c060;border:1px solid #5a4520}
          #reconcile-modal .mr-chip.mut{background:var(--bg-input,#22263a);color:var(--text-muted)}
          #reconcile-modal .rc-lane{border:1px solid var(--border);border-radius:7px;padding:8px;margin-top:6px}
          #reconcile-modal .rc-lane h4{margin:0 0 6px;font-size:.66rem;text-transform:uppercase;letter-spacing:.05em;color:var(--text-muted)}
          #reconcile-modal label.rc-row{display:flex;align-items:center;gap:9px;padding:5px 7px;border-radius:6px;border:1px solid var(--border);margin-bottom:4px;font-size:.76rem;cursor:pointer}
        </style>
        <div style="position:absolute;top:2vh;left:50%;transform:translateX(-50%);width:900px;max-width:96vw;height:96vh;display:flex;flex-direction:column;background:var(--bg-card,#1a1d27);border:1px solid var(--border);border-radius:12px;overflow:hidden">
          <div style="display:flex;align-items:center;gap:12px;padding:12px 16px;border-bottom:1px solid var(--border)">
            <h3 style="margin:0;font-size:1rem">🧹 Reconcile</h3>
            <span id="rc-count" style="font-size:.8rem;color:var(--text-muted)"></span>
            <button type="button" class="btn btn-sm btn-ghost" style="margin-left:auto" onclick="rcClose()">✕ Close</button>
          </div>
          <div id="rc-body" style="flex:1;overflow-y:auto;padding:12px 16px"></div>
        </div>`;
      document.body.appendChild(m);
      m.addEventListener('click', e => { if (e.target === m) rcClose(); });
      return m;
    };
    // Resolve clusters in-place (no full-page reload mid-session). We refresh
    // the page ONCE, on close, and only if something actually changed — so the
    // actuals totals behind the modal re-sync without losing your place in the
    // queue. (User 2026-06-17.)
    window._rcDirty = false;
    window.rcClose = function () {
      const m = document.getElementById('reconcile-modal');
      if (m) m.style.display = 'none';
      if (window._rcDirty) { window._rcDirty = false; location.reload(); }
    };
    function _rcCardDone(card, dirty) {
      if (card) card.remove();
      if (dirty) window._rcDirty = true;
      const remaining = document.querySelectorAll('#rc-body .mr-card[data-rc]').length;
      const cnt = document.getElementById('rc-count');
      if (cnt) cnt.textContent = remaining ? (remaining + ' spend' + (remaining !== 1 ? 's' : '') + ' to reconcile') : '';
      if (!remaining) {
        const body = document.getElementById('rc-body');
        if (body) body.innerHTML = '<div class="muted" style="padding:28px;text-align:center">✓ All clear'
          + (window._rcDirty ? ' — close to refresh the totals.' : '.') + '</div>';
      }
    }
    // Recount visible (non-pending) reconcile cards.
    function _rcRefreshCount() {
      const remaining = document.querySelectorAll('#rc-body .mr-card[data-rc]:not([data-rc-pending])').length;
      const cnt = document.getElementById('rc-count');
      if (cnt) cnt.textContent = remaining ? (remaining + ' spend' + (remaining !== 1 ? 's' : '') + ' to reconcile') : '';
    }
    // 20-second undo toast for reconcile actions. (User 2026-06-22.)
    function _rcUndoToast(msg, onUndo, secs) {
      secs = secs || 20;
      const t = document.createElement('div');
      t.style.cssText = `position:fixed;bottom:24px;left:50%;transform:translateX(-50%);
        background:#1a3a2a;color:#cfe9d8;border:1px solid #2a5a3a;border-radius:8px;
        padding:10px 16px;font-size:.85rem;z-index:10002;box-shadow:0 6px 24px rgba(0,0,0,.5);
        display:flex;align-items:center;gap:14px;max-width:560px`;
      const span = document.createElement('span'); span.textContent = msg;
      const left = document.createElement('span');
      left.style.cssText = 'color:#8fcfa8;font-variant-numeric:tabular-nums;flex:none';
      const undo = document.createElement('button');
      undo.textContent = 'Undo';
      undo.style.cssText = `background:none;border:1px solid #2a5a3a;color:#9fe0b6;border-radius:5px;
        padding:3px 12px;font-size:.8rem;cursor:pointer;flex:none`;
      let remaining = secs, done = false;
      const fin = () => { if (!t._iv) return; clearInterval(t._iv); t._iv = null; t.remove(); };
      undo.onclick = () => { if (done) return; done = true; fin(); try { onUndo(); } catch (e) {} };
      left.textContent = remaining + 's';
      t._iv = setInterval(() => { remaining--; left.textContent = remaining + 's'; if (remaining <= 0) fin(); }, 1000);
      t.appendChild(span); t.appendChild(left); t.appendChild(undo);
      document.body.appendChild(t);
      return { dismiss: fin };
    }
    // Collapse a reconcile card immediately, commit the (destructive) action only
    // after a 20s undo window — so an accidental click is freely reversible and
    // there's no confirm() dialog. (User 2026-06-22.)
    function _rcDeferAndUndo(card, label, doAction) {
      if (!card) return;
      card.dataset.rcPending = '1';
      const h = card.offsetHeight;
      card.style.transition = 'max-height .3s ease, opacity .25s, margin .3s, padding .3s';
      card.style.overflow = 'hidden';
      card.style.maxHeight = h + 'px';
      requestAnimationFrame(() => {
        card.style.maxHeight = '0'; card.style.opacity = '0';
        card.style.marginTop = '0'; card.style.marginBottom = '0';
        card.style.paddingTop = '0'; card.style.paddingBottom = '0';
      });
      _rcRefreshCount();
      let committed = false, cancelled = false;
      const timer = setTimeout(async () => {
        if (cancelled) return;
        committed = true;
        try { await doAction(); _rcCardDone(card, true); }
        catch (e) {
          delete card.dataset.rcPending;
          card.style.maxHeight = ''; card.style.opacity = ''; card.style.margin = ''; card.style.padding = '';
          _rcRefreshCount();
          if (typeof _actualsToast === 'function') _actualsToast('Action failed: ' + e.message, 'red');
        }
      }, 20000);
      _rcUndoToast(label + ' — done.', () => {
        if (committed) return;
        cancelled = true; clearTimeout(timer);
        delete card.dataset.rcPending;
        card.style.maxHeight = ''; card.style.opacity = '';
        card.style.marginTop = ''; card.style.marginBottom = '';
        card.style.paddingTop = ''; card.style.paddingBottom = '';
        _rcRefreshCount();
        if (typeof _actualsToast === 'function') _actualsToast('Undone — nothing was changed.', 'yellow');
      });
    }
    window.openReconcile = async function () {
      const m = window._rcBuild(); m.style.display = '';
      const body = document.getElementById('rc-body');
      body.innerHTML = '<div class="muted" style="padding:24px;text-align:center">Scanning every spend for duplicates…</div>';
      try {
        const d = await (await fetch(`/projects/${PROJ_ID}/actuals/reconcile-scan`)).json();
        window._rcData = d; rcRender(d);
      } catch (e) { body.innerHTML = '<div style="padding:20px;color:#e08080">Scan failed: ' + e.message + '</div>'; }
    };
    window.rcRender = function (d) {
      const body = document.getElementById('rc-body');
      const esc = FP.esc;                 // shared rule (fp-utils.js; this copy escaped only '<')
      const money = a => FP.money(a, {empty: '$?', abs: true});
      const clusters = d.clusters || [];
      document.getElementById('rc-count').textContent = clusters.length ? `${clusters.length} spend${clusters.length !== 1 ? 's' : ''} to reconcile` : '';
      if (!clusters.length) { body.innerHTML = '<div class="muted" style="padding:28px;text-align:center">✓ Nothing to reconcile — no duplicate receipts or charges found.</div>'; return; }
      // Friendly source labels — a 'doc_upload' transaction is NOT a separate
      // bank charge, it's the receipt's own auto-created ledger row.
      const srcLabel = s => ({
        csv_import: 'Bank charge', qbo_sync: 'Bank charge', reconciled: 'Bank charge',
        doc_upload: 'Receipt’s own ledger row', manual_entry: 'Manual entry',
      }[s] || s);
      body.innerHTML = clusters.map((c, ci) => {
        const bankCharges = c.charges.filter(x => x.source !== 'doc_upload');
        const codedBank = bankCharges.find(x => x.coded);
        // One plain-English line saying what this cluster actually is.
        let summary;
        if (c.flags.dup_receipts && c.receipts.length > 1) {
          summary = `📎 The same receipt was uploaded ${c.receipts.length} times. Keep one — the rest get filed as duplicates.`;
        } else if (c.flags.phantom) {
          summary = `♻ Counted twice. This receipt is already attached to its real bank charge — but it <b>also</b> has its own leftover ledger row (auto-created when the receipt was uploaded), which double-counts the ${money(c.amount)}. Remove the leftover row below; the bank charge stays${codedBank ? (' (coded ' + esc(codedBank.coded) + ')') : ''}.`;
        } else if (c.flags.dup_charges) {
          summary = `🏦 This charge was imported ${bankCharges.length} times. Keep one, remove the duplicate import(s).`;
        } else if (c.flags.needs_match) {
          summary = `🔗 An unmatched charge and a receipt for the same spend — likely the same purchase, not yet linked.`;
        } else {
          summary = `Same amount, date, vendor and card — check whether these are one spend or genuinely separate.`;
        }
        // Default-keep the copy ALREADY attached to a charge (so the link is
        // preserved without re-pointing), else the first. (User 2026-06-17.)
        const recKeep = ((c.receipts.find(r => r.on_charge) || c.receipts[0]) || {}).doc_id;
        const _thumb = r => r.is_image
          ? `<img loading="lazy" src="/docs/upload/${r.doc_id}/raw" title="Click to view this receipt" onclick="event.preventDefault();event.stopPropagation();openDocDetail(${r.doc_id}, null)" style="width:30px;height:38px;object-fit:cover;border-radius:4px;cursor:zoom-in;flex-shrink:0">`
          : `<span title="Click to view this receipt" onclick="event.preventDefault();event.stopPropagation();openDocDetail(${r.doc_id}, null)" style="width:30px;height:38px;display:inline-flex;align-items:center;justify-content:center;background:var(--bg-input,#22263a);border-radius:4px;cursor:zoom-in;flex-shrink:0">📄</span>`;
        const recRows = c.receipts.map(r => `
          <label class="rc-row">
            ${c.receipts.length > 1 ? `<input type="radio" name="rc-rec-${ci}" value="${r.doc_id}" ${r.doc_id === recKeep ? 'checked' : ''}>` : '<span style="width:13px"></span>'}
            ${_thumb(r)}
            <span style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(r.file) || ('Receipt #' + r.doc_id)} <span style="color:var(--text-muted)">· ${money(r.amount)}</span></span>
            ${r.dup ? '<span class="mr-chip warn">identical copy</span>' : ''}${r.on_charge ? '<span class="mr-chip ok">✓ attached to a charge</span>' : ''}
          </label>`).join('');
        const recAction = c.receipts.length > 1
          ? `<button class="btn btn-xs btn-primary" onclick="rcMergeReceipts(${ci})">Keep selected · merge the rest</button>` : '';
        const chKeep = ((bankCharges.find(x => x.coded) || bankCharges[0] || c.charges[0]) || {}).tid;
        const chRows = c.charges.map(r => `
          <label class="rc-row">
            ${c.charges.length > 1 ? `<input type="radio" name="rc-ch-${ci}" value="${r.tid}" ${r.tid === chKeep ? 'checked' : ''}>` : '<span style="width:13px"></span>'}
            <span style="flex:1;min-width:0">${srcLabel(r.source)} <span style="color:var(--text-muted)">· ${money(r.amount)} · ${r.coded ? esc(r.coded) : 'uncoded'}${(r.doc_upload_id && r.source !== 'doc_upload') ? ' · ✓ has receipt' : ''}</span></span>
            ${r.phantom ? '<span class="mr-chip warn">= the receipt above · remove</span>' : ''}${r.reimport ? '<span class="mr-chip warn">imported again</span>' : ''}${(!r.deletable && !r.phantom) ? '<span class="mr-chip mut" title="Kept; not removable here">keep</span>' : ''}
          </label>`).join('');
        const chDeletable = c.charges.filter(x => x.deletable).length;
        let chAction = '';
        if (chDeletable) {
          const chLabel = (c.charges.length > 1) ? 'Keep selected · remove the rest' : '🗑 Remove the duplicate ledger row';
          chAction = `<button class="btn btn-xs btn-primary" onclick="rcRemoveCharges(${ci})">${chLabel}</button>`;
        }
        // "Link them": an unmatched charge + an unlinked receipt for the same spend
        // → link the receipt to the charge (instead of only removing). (User 2026-06-18.)
        let linkAction = '';
        const _uCharge = c.charges.find(x => x.source !== 'doc_upload' && !x.doc_upload_id && x.match_status !== 'confirmed');
        const _uReceipt = c.receipts.find(r => !r.on_charge);
        if (_uCharge && _uReceipt) {
          linkAction = `<button class="btn btn-xs btn-primary" onclick="rcLinkThem(${ci}, ${_uCharge.tid}, ${_uReceipt.doc_id})" title="Same spend — attach this receipt to this charge and mark it matched">🔗 Link them</button>`;
        }
        return `<div class="mr-card" data-rc="${ci}" style="border:1px solid var(--border);border-radius:8px;padding:10px 12px;margin-bottom:12px;background:var(--bg-card,#1a1d27)">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;flex-wrap:wrap">
            <span style="font-weight:600">${money(c.amount)}</span>
            <span style="color:var(--text-muted);font-size:.78rem">${esc(c.vendor) || '—'} · ${c.date || ''}${c.card ? (' · ••' + c.card) : ''}</span>
            <span style="margin-left:auto;display:flex;gap:6px">
              <button class="btn btn-xs btn-ghost" onclick="rcAiJudge(${ci})" title="Ask AI: are these the same spend (duplicate) or genuinely separate?">🤖 Ask AI</button>
              <button class="btn btn-xs btn-ghost" onclick="rcDismiss('${c.key}',${ci})">Not a duplicate</button>
            </span>
          </div>
          <div style="font-size:.74rem;color:var(--text);line-height:1.45;margin-bottom:6px">${summary}</div>
          ${linkAction ? ('<div style="margin-bottom:8px">' + linkAction + '</div>') : ''}
          <div id="rc-ai-${ci}" style="margin-bottom:6px"></div>
          ${c.receipts.length ? `<div class="rc-lane"><h4>📎 Receipt${c.receipts.length > 1 ? 's (' + c.receipts.length + ')' : ''}</h4>${recRows}${recAction ? ('<div style="margin-top:5px">' + recAction + '</div>') : ''}</div>` : ''}
          ${c.charges.length ? `<div class="rc-lane"><h4 title="Each line in your Actuals ledger for this spend — a bank/card charge or a receipt's own auto-created entry. Duplicates here double-count the money; keep one and remove the rest. This only cleans up the ledger entries — it does NOT change any budget-line coding.">🏦 Charges in the ledger (${c.charges.length}) <span style="font-weight:400;color:var(--text-muted);font-size:.82em">ⓘ</span></h4>${chRows}${chAction ? ('<div style="margin-top:5px">' + chAction + '</div>') : ''}</div>` : ''}
        </div>`;
      }).join('');
    };
    // Ask Claude whether a cluster's records are the same spend (duplicate) or
    // genuinely separate (e.g. two hotel nights for different guests). Reasons
    // over the JSON only; advisory — never auto-acts. (User 2026-06-17.)
    window.rcAiJudge = async function (ci) {
      const c = (window._rcData.clusters || [])[ci];
      const out = document.getElementById('rc-ai-' + ci);
      if (!c || !out) return;
      out.innerHTML = '<span style="font-size:.72rem;color:var(--text-muted)">🤖 Asking Claude…</span>';
      const items = [
        ...c.receipts.map(r => ({ type: 'receipt', vendor: c.vendor, amount: r.amount, date: c.date, card: c.card, file: r.file, on_charge: r.on_charge })),
        ...c.charges.map(x => ({ type: 'charge', source: x.source, vendor: c.vendor, amount: x.amount, date: c.date, card: c.card, coded: x.coded || null })),
      ];
      if (items.length < 2) { out.innerHTML = '<span style="font-size:.72rem;color:var(--text-muted)">Need two records to compare.</span>'; return; }
      try {
        const r = await fetch(`/projects/${PROJ_ID}/actuals/reconcile-ai-judge`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ items, doc_id: (c.receipts[0] || {}).doc_id }) });
        const d = await r.json().catch(() => ({}));
        if (!r.ok) { out.innerHTML = '<span style="font-size:.72rem;color:#e08080">AI error: ' + (d.error || r.status) + '</span>'; return; }
        const v = d.verdict || {};
        if (v._provider === 'none') { out.innerHTML = '<span style="font-size:.72rem;color:#e0c060">AI unavailable (no key set).</span>'; return; }
        const conf = Math.round((v.confidence || 0) * 100);
        const dup = !!v.is_duplicate;
        const expl = v.explanation || (v.anomalies && v.anomalies[0] && v.anomalies[0].explanation) || '';
        const color = dup ? '#e0c060' : '#5fd0a0';
        const verdictTxt = dup ? ('⚠ Likely the SAME spend (duplicate) · ' + conf + '%')
                               : ('✓ Likely SEPARATE charges · ' + conf + '%');
        out.innerHTML = '<div style="font-size:.74rem;background:#10131c;border:1px solid var(--border);border-radius:6px;padding:6px 9px;line-height:1.4">'
          + '<span style="color:' + color + ';font-weight:600">🤖 ' + verdictTxt + '</span>'
          + (expl ? ('<br><span style="color:var(--text-muted)">' + String(expl).replace(/</g, '&lt;') + '</span>') : '')
          + (v.recommended_action ? ('<br><span style="color:var(--text-muted);font-size:.68rem">recommended: ' + v.recommended_action + '</span>') : '')
          + '</div>';
      } catch (e) { out.innerHTML = '<span style="font-size:.72rem;color:#e08080">AI error: ' + e.message + '</span>'; }
    };
    window.rcDismiss = async function (key, ci) {
      try {
        await fetch(`/projects/${PROJ_ID}/actuals/dup-transactions/dismiss`, {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({dup_key:key})});
        _rcCardDone(document.querySelector('.mr-card[data-rc="' + ci + '"]'), false);
        _actualsToast('Marked separate ✓', 'green');
      } catch (e) { _actualsToast('Error: ' + e.message, 'red'); }
    };
    window.rcMergeReceipts = async function (ci) {
      const card = document.querySelector('.mr-card[data-rc="' + ci + '"]');
      const keep = card && card.querySelector('input[name="rc-rec-' + ci + '"]:checked');
      if (!keep) { _actualsToast('Pick the receipt to keep.', 'yellow'); return; }
      const keepId = parseInt(keep.value);
      const cluster = (window._rcData.clusters || [])[ci] || {receipts: []};
      const confirmIds = cluster.receipts.filter(r => r.doc_id !== keepId).map(r => r.doc_id);
      if (!confirmIds.length) { _actualsToast('Nothing to merge.', 'yellow'); return; }
      // No confirm() — collapse now, commit after a 20s undo window. (User 2026-06-22.)
      _rcDeferAndUndo(card, `Merged ${confirmIds.length} duplicate receipt${confirmIds.length > 1 ? 's' : ''}`, async () => {
        const r = await fetch(`/docs/${PROJ_ID}/duplicates/resolve-batch`, {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({keep:[keepId], confirm:confirmIds, force:true, link_mode:'transfer'})});
        const j = await r.json().catch(() => ({}));
        if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status));
      });
    };
    window.rcRemoveCharges = async function (ci) {
      const card = document.querySelector('.mr-card[data-rc="' + ci + '"]');
      const keep = card && card.querySelector('input[name="rc-ch-' + ci + '"]:checked');
      const cluster = (window._rcData.clusters || [])[ci] || {charges: []};
      const keepTid = keep ? parseInt(keep.value) : null;
      const toDelete = cluster.charges.filter(x => x.tid !== keepTid && x.deletable);
      if (!toDelete.length) { _actualsToast('Nothing removable here — the others are receipt-linked (match/unmatch them).', 'yellow'); return; }
      // No confirm() — collapse now, commit after a 20s undo window. (User 2026-06-22.)
      _rcDeferAndUndo(card, `Removed ${toDelete.length} duplicate ledger row${toDelete.length > 1 ? 's' : ''}`, async () => {
        let ok = 0, fail = 0;
        for (const x of toDelete) {
          try {
            const resp = await fetch(`/projects/${PROJ_ID}/actuals/transaction/${x.tid}/delete-duplicate`, {method:'POST', headers:{'Content-Type':'application/json'}, body:'{}'});
            if (resp.ok) ok++; else fail++;
          } catch (e) { fail++; }
        }
        if (!ok && fail) throw new Error(fail + ' failed');
      });
    };
    // 🔗 Link them — attach the receipt to the charge (same spend) + mark matched.
    window.rcLinkThem = async function (ci, tid, docId) {
      const card = document.querySelector('.mr-card[data-rc="' + ci + '"]');
      try {
        const r = await fetch(`/projects/${PROJ_ID}/actuals/transaction/${tid}/link-doc`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ doc_upload_id: docId }) });
        const j = await r.json().catch(() => ({}));
        if (!r.ok) { _actualsToast('Link failed: ' + (j.error || r.status), 'red'); return; }
        _actualsToast('Linked the receipt to the charge ✓', 'green');
        if (card) _rcCardDone(card, true);
      } catch (e) { _actualsToast('Link error: ' + e.message, 'red'); }
    };
  })();
