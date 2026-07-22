// Extracted from a templates/budget.html inline block (audit M9, 2026-07-20).
// Jinja values arrive via window.__BJ (set by the inline preamble before this
// script tag). Classic script, document order — semantics identical to inline.

  (function(){
    const PROJ_ID = window.__BJ["b04_PROJ_ID"];

    window.toggleActualsPanel = function (id) {
      const el = document.getElementById(id);
      if (!el) return;
      const willOpen = el.style.display === 'none';
      el.style.display = willOpen ? '' : 'none';
      // Lazy-load the QBO account picker the first time the Settings
      // panel opens (skips an empty render if the user never opens it).
      if (willOpen && id === 'actuals-settings-panel') {
        actualsLoadQboAccounts(PROJ_ID);
      }
      // Seed the "To" date picker with today on first open of the
      // sync panel. Only runs once (tracked via dataset).
      if (willOpen && id === 'actuals-sync-panel') {
        document.querySelectorAll('#actuals-sync-panel input[type="date"]').forEach(el2 => {
          if (el2.id === 'actuals-sync-end' && !el2.value) {
            const today = new Date();
            el2.value = today.toISOString().slice(0, 10);
          }
        });
      }
    };

    // ── Bank/credit-card CSV import: preview then commit ──────────────
    const _csvEsc = s => String(s == null ? '' : s).replace(/[&<>"']/g,
      c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    const _csvFmt = n => '$' + Number(n || 0).toLocaleString(undefined,
      {minimumFractionDigits: 2, maximumFractionDigits: 2});

    window.actualsCsvPreview = async function (pid) {
      const fileEl = document.getElementById('actuals-csv-file');
      const status = document.getElementById('actuals-csv-status');
      const out    = document.getElementById('actuals-csv-preview');
      const incPay = document.getElementById('actuals-csv-include-payments');
      if (!fileEl || !fileEl.files || !fileEl.files.length) {
        if (status) { status.textContent = '✕ Choose a CSV file first.'; status.style.color = '#e08080'; }
        return;
      }
      if (status) { status.textContent = 'Analyzing…'; status.style.color = 'var(--text-muted)'; }
      const fd = new FormData(); fd.append('file', fileEl.files[0]);
      const qs = (incPay && incPay.checked) ? '?include_payments=1' : '';
      try {
        const r = await fetch(`/projects/${pid}/actuals/import-bank-csv${qs}`, { method: 'POST', body: fd });
        const d = await r.json();
        if (!r.ok) {
          if (status) { status.textContent = '✕ ' + (d.error || ('HTTP ' + r.status)); status.style.color = '#e05555'; }
          if (out) out.style.display = 'none';
          return;
        }
        if (status) status.textContent = '';
        const byCard = Object.entries(d.by_card || {}).map(([k, v]) => `${_csvEsc(k)}: ${v}`).join(' · ') || '—';
        const rows = (d.sample || []).map(s => `<tr>
            <td style="padding:2px 8px">${_csvEsc(s.date)}</td>
            <td style="padding:2px 8px">${_csvEsc(s.vendor)}</td>
            <td style="padding:2px 8px;text-align:right;color:${s.expense ? 'var(--text)' : 'var(--green)'}">${s.expense ? '' : '+'}${_csvFmt(s.amount)}</td>
            <td style="padding:2px 8px">${_csvEsc(s.card || '—')}</td>
            <td style="padding:2px 8px;color:var(--text-muted)">${_csvEsc(s.note || '')}</td></tr>`).join('');
        out.innerHTML = `
          <div style="font-size:.8rem;line-height:1.6;margin-bottom:8px">
            <strong style="color:var(--green)">${d.to_import}</strong> to import
            (${d.charges} charges, ${d.credits} credits)
            ${d.date_range ? `· <span class="muted">${_csvEsc(d.date_range[0])} → ${_csvEsc(d.date_range[1])}</span>` : ''}<br>
            <span class="muted">Skipped: ${d.skipped_card_payments} card payments · ${d.skipped_transfers || 0} transfers · ${d.skipped_duplicates} already imported · ${d.skipped_zero} zero${d.parse_error_count ? ` · <span style="color:#e0a040">${d.parse_error_count} unparseable</span>` : ''}</span><br>
            <span class="muted">Cards: ${byCard}</span>
          </div>
          ${rows ? `<div style="max-height:240px;overflow:auto;border:1px solid var(--border);border-radius:6px">
            <table style="width:100%;border-collapse:collapse;font-size:.76rem">
              <thead><tr style="position:sticky;top:0;background:var(--bg-card)">
                <th style="padding:4px 8px;text-align:left">Date</th><th style="padding:4px 8px;text-align:left">Vendor</th>
                <th style="padding:4px 8px;text-align:right">Amount</th><th style="padding:4px 8px;text-align:left">Card</th>
                <th style="padding:4px 8px;text-align:left">Category</th></tr></thead>
              <tbody>${rows}</tbody></table>
            </div><div class="muted" style="font-size:.72rem;margin-top:4px">Showing first ${(d.sample || []).length} of ${d.to_import}.</div>` : ''}
          <div style="margin-top:10px;display:flex;gap:8px;align-items:center">
            <button type="button" class="btn btn-sm btn-primary" ${d.to_import ? '' : 'disabled'} onclick="actualsCsvCommit(${pid})">✓ Import ${d.to_import} transactions</button>
            <span id="actuals-csv-commit-status" class="muted" style="font-size:.78rem"></span>
          </div>`;
        out.style.display = '';
      } catch (e) {
        if (status) { status.textContent = '✕ ' + e.message; status.style.color = '#e05555'; }
      }
    };

    window.actualsCsvCommit = async function (pid) {
      const fileEl = document.getElementById('actuals-csv-file');
      const incPay = document.getElementById('actuals-csv-include-payments');
      const cs     = document.getElementById('actuals-csv-commit-status');
      if (!fileEl || !fileEl.files || !fileEl.files.length) return;
      if (cs) { cs.textContent = 'Importing…'; cs.style.color = 'var(--text-muted)'; }
      const fd = new FormData(); fd.append('file', fileEl.files[0]);
      const qs = '?apply=1' + ((incPay && incPay.checked) ? '&include_payments=1' : '');
      try {
        const r = await fetch(`/projects/${pid}/actuals/import-bank-csv${qs}`, { method: 'POST', body: fd });
        const d = await r.json();
        if (!r.ok) { if (cs) { cs.textContent = '✕ ' + (d.error || ('HTTP ' + r.status)); cs.style.color = '#e05555'; } return; }
        if (cs) { cs.textContent = `✓ Imported ${d.created || 0}. Reloading…`; cs.style.color = 'var(--green)'; }
        setTimeout(() => location.reload(), 900);
      } catch (e) { if (cs) { cs.textContent = '✕ ' + e.message; cs.style.color = '#e05555'; } }
    };

    // ── Per-project QBO account picker ────────────────────────────────
    window.actualsLoadQboAccounts = async function (pid) {
      const list   = document.getElementById('actuals-qbo-accounts-list');
      const status = document.getElementById('actuals-qbo-accounts-status');
      if (!list) return;
      list.innerHTML = '<div class="muted" style="font-size:.78rem;padding:6px">Loading accounts…</div>';
      if (status) { status.textContent = ''; status.style.color = ''; }
      try {
        const r = await fetch(`/projects/${pid}/actuals/qbo-accounts`);
        const d = await r.json();
        if (!r.ok) {
          list.innerHTML = `<div style="font-size:.78rem;color:#e08080;padding:6px">${d.error || 'HTTP ' + r.status}${d.needs_oauth ? ' — connect QuickBooks first.' : ''}</div>`;
          return;
        }
        if (!d.accounts || !d.accounts.length) {
          list.innerHTML = '<div class="muted" style="font-size:.78rem;padding:6px">No bank or credit-card accounts found on your QBO realm.</div>';
          return;
        }
        const _esc = s => String(s||'').replace(/[&<>"']/g, c =>
          ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
        list.innerHTML = d.accounts.map(a => `
          <label style="display:flex;align-items:center;gap:8px;padding:6px 8px;border:1px solid var(--border);border-radius:5px;background:var(--bg-input);font-size:.8rem;cursor:pointer">
            <input type="checkbox" class="actuals-qbo-acct" value="${_esc(a.id)}" ${a.enabled ? 'checked' : ''}>
            <span style="flex:1">${_esc(a.name)}</span>
            <span style="font-size:.7rem;color:var(--text-muted)">
              ${a.type === 'Credit Card' ? 'CC' : 'Bank'}${a.mask ? ' · ' + _esc(a.mask) : ''}
            </span>
          </label>
        `).join('');
      } catch (e) {
        list.innerHTML = `<div style="font-size:.78rem;color:#e08080;padding:6px">Network error: ${e.message}</div>`;
      }
    };

    window.actualsSaveQboAccounts = async function (pid) {
      const status = document.getElementById('actuals-qbo-accounts-status');
      const ids = Array.from(document.querySelectorAll('.actuals-qbo-acct:checked'))
                       .map(cb => cb.value);
      // Was the sync panel already gated by "no accounts selected"
      // (server-rendered from project.qbo_account_ids)? If so, the
      // panel HTML is stale after this save and the user has to
      // refresh before the sync button appears. Detect that and
      // reload the page automatically once the save completes.
      // Heuristic: count was 0 before AND we're saving > 0 now, OR
      // count > 0 before AND we're saving 0 now (toggling visibility
      // of the gate).
      const prevCount = (window.__qboAcctsPrevCount != null)
        ? window.__qboAcctsPrevCount
        : Array.from(document.querySelectorAll('.actuals-qbo-acct'))
                .filter(cb => cb.dataset.initiallyEnabled === '1').length;
      const willTriggerReload = (prevCount === 0 && ids.length > 0)
                              || (prevCount  >  0 && ids.length === 0);
      if (status) { status.textContent = 'Saving…'; status.style.color = 'var(--text-muted)'; }
      try {
        const r = await fetch(`/projects/${pid}/actuals/qbo-accounts`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ account_ids: ids }),
        });
        const d = await r.json();
        if (!r.ok) {
          if (status) { status.textContent = '✕ ' + (d.error || 'failed'); status.style.color = '#e08080'; }
          return;
        }
        if (status) {
          status.textContent = `✓ Saved ${d.count} account${d.count !== 1 ? 's' : ''}`;
          status.style.color = 'var(--green)';
        }
        // Track count for the next save in this session.
        window.__qboAcctsPrevCount = ids.length;
        // If the sync panel's gate just flipped, reload so the panel
        // re-renders with the right controls (no manual refresh).
        if (willTriggerReload) {
          setTimeout(() => window.location.reload(), 350);
        }
      } catch (e) {
        if (status) { status.textContent = '✕ ' + e.message; status.style.color = '#e08080'; }
      }
    };

    // By Department: click a section row to toggle its detail row
    // (transactions categorized to that section). Bound once at IIFE
    // start since the rows are server-rendered.
    document.querySelectorAll('.bydept-row').forEach(row => {
      row.addEventListener('click', () => {
        const code = row.dataset.secCode;
        const detail = document.querySelector(`.bydept-detail[data-for-sec="${code}"]`);
        if (!detail) return;  // section has no transactions
        const open = detail.style.display !== 'none';
        detail.style.display = open ? 'none' : '';
        const chev = row.querySelector('.bydept-chev');
        if (chev && chev.textContent !== '·') chev.textContent = open ? '▸' : '▾';
      });
    });

    window.showActualsPane = function (which) {
      document.getElementById('actuals-pane-match').style.display    = which === 'match'    ? '' : 'none';
      document.getElementById('actuals-pane-bydept').style.display   = which === 'bydept'   ? '' : 'none';
      const recPane = document.getElementById('actuals-pane-reconcile');
      if (recPane) recPane.style.display = which === 'reconcile' ? '' : 'none';
      const codePane = document.getElementById('actuals-pane-code');
      if (codePane) codePane.style.display = which === 'code' ? '' : 'none';
      // Hide the global filter bar in Reconcile + Code views — each has its
      // own controls (Reconcile: mini-filters; Code: department grouping).
      const fbar = document.getElementById('actuals-filter-bar');
      if (fbar) fbar.style.display = (which === 'reconcile' || which === 'code') ? 'none' : '';
      // toggle subtab visual state
      document.querySelectorAll('.actuals-subtab').forEach(b => {
        b.style.borderBottomColor = 'transparent';
        b.style.color = 'var(--text-muted)';
      });
      const active = document.getElementById('actuals-subtab-' + which);
      if (active) {
        active.style.borderBottomColor = 'var(--blue)';
        active.style.color = 'var(--text)';
      }
      // First time entering Reconcile → render the two columns.
      if (which === 'reconcile' && typeof window._reconcileRender === 'function') {
        window._reconcileRender();
      }
      // Entering Code → restore the saved tiles/list preference, then
      // lazy-load thumbnails for any already-open department (the "Needs
      // coding" group is open by default).
      if (which === 'code') {
        let _v = 'tiles';
        try { _v = localStorage.getItem('fpCodeView') || 'tiles'; } catch (e) {}
        if (typeof window.codeSetView === 'function') window.codeSetView(_v);
        else if (typeof window.codeLoadThumbs === 'function')
          document.querySelectorAll('#actuals-pane-code .code-group.open').forEach(window.codeLoadThumbs);
      }
    };

    // Recompute the 5 stat cards from the current state of the rows.
    // Replaces the round-trip / page-refresh that the user reported.
    function _actualsRecountStats() {
      const rows  = document.querySelectorAll('.actuals-txn-row');
      let total = 0, coded = 0, uncoded = 0, needRcpt = 0, docOnly = 0;
      let qboCnt = 0, manCnt = 0;
      rows.forEach(r => {
        // Skip rows hidden by a filter — we want the underlying counts,
        // not "what's currently visible". A filtered view shouldn't
        // change the totals.
        const claimedElsewhere = r.dataset.claimedElsewhere === '1';
        if (claimedElsewhere) return;  // admin-only awareness, not actionable
        total++;
        const codedRow      = r.dataset.coded === '1';
        const hasDoc        = r.dataset.hasDoc === '1';
        const notProj       = r.dataset.notProject === '1';
        const src           = r.dataset.source || '';
        if (notProj) return;  // dropped from rollup
        if (codedRow) coded++; else uncoded++;
        // Need-receipt flag: electronic-only. Cash / off-system / manual
        // rows are valid without a paper trail.
        if (!hasDoc && (src === 'qbo_sync' || src === 'reconciled' || src === 'csv_import')) needRcpt++;
        if (src === 'doc_upload')   docOnly++;
        if (src === 'qbo_sync' || src === 'csv_import') qboCnt++;
        if (src === 'manual_entry') manCnt++;
      });
      const setN = (filt, n) => {
        const card = document.querySelector(`.actuals-stat-card[data-filter="${filt}"]`);
        if (card) {
          const numEl = card.querySelector('div:first-child');
          if (numEl) numEl.textContent = n;
        }
      };
      // Also recount Finished + Review-OCR since both depend on row state
      // we just re-read. Without this, coding a section-only row lets the
      // Finished card drift until full page reload.
      let finished = 0, ocrReview = 0;
      rows.forEach(r => {
        if (r.dataset.notProject === '1') return;
        if (r.dataset.coded === '1' &&
            r.dataset.hasDoc === '1' &&
            r.dataset.matchStatus === 'confirmed') {
          finished++;
        }
        if (r.dataset.source === 'doc_upload') {
          const amt = parseFloat(r.dataset.amount || '0');
          const vendor = (r.querySelector('.actuals-txn-vendor')?.textContent || '').trim();
          if (!vendor || vendor === '— vendor unknown —' || amt === 0) ocrReview++;
        }
      });
      setN('all', total);
      setN('coded', coded);
      setN('uncoded', uncoded);
      setN('no_doc', needRcpt);
      setN('doc_only', docOnly);
      setN('finished', finished);
      setN('ocr_review', ocrReview);
    }
    // Exposed so the Docs script block (separate IIFE) can recount after it
    // deletes/patches a doc that backs Actuals rows. (User 2026-06-11.)
    window._actualsRecountStats = _actualsRecountStats;

    // Click-to-code: change a budget-line dropdown → POST → refresh row state.
    // The dropdown supports three value forms:
    //   ""                — no-op (the "— pick budget line —" placeholder)
    //   "__clear__"       — clear the assignment via unlink
    //   "section:<code>"  — section-only (no specific line) → /set-coa
    //   "<line_id>"       — specific line → /set-line (auto-clones Actual)
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
      const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g,
        c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
      const money = v => v == null ? '—' : '$' + Number(v).toLocaleString('en-US',{minimumFractionDigits:2});
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
            if (typeof _refreshRowAfterCode === 'function') _refreshRowAfterCode(tid, false, true);
            if (typeof _actualsToast === 'function') _actualsToast('📎 Marked as backup — charge no longer counts.', 'green');
            else location.reload();
          } else if (typeof _actualsToast === 'function') _actualsToast(j.error || 'Failed', 'yellow');
        } catch (e) { if (typeof _actualsToast === 'function') _actualsToast('Error: ' + e.message, 'yellow'); }
      };
    };

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
          url  = `/projects/${PROJ_ID}/actuals/transaction/${tid}/set-coa`;
          body = JSON.stringify({ account_code: code });
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
          // Specific budget line.
          url  = `/projects/${PROJ_ID}/actuals/transaction/${tid}/set-line`;
          body = JSON.stringify({ budget_line_id: value });
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
        // Reflect coded state on the row. The line CELL recolors itself off
        // data-coded via CSS (the three-bucket layout), so we only flip the
        // attribute here — no whole-row repaint.
        const row = document.querySelector(`.actuals-txn-row[data-tid="${tid}"]`);
        if (row) {
          const isCoded = value !== '__clear__' && value !== 'not_project';
          row.dataset.coded = isCoded ? '1' : '0';
          if (value === 'not_project') row.dataset.notProject = '1';
          // Keep the section-filter's source of truth current (2026-06-04):
          // section coding → we know the code; line coding → code unknown
          // client-side, drop the attr so the filter falls back to the
          // (populated, user just used it) picker's optgroup; clear → empty.
          if (value.startsWith('section:')) row.dataset.acctCode = value.slice(8);
          else row.dataset.acctCode = '';
          // Coded now → the suggestion is spent; remove BOTH the old heuristic
          // chip AND the ✨ AI chip in place so it doesn't linger in the list
          // after coding (incl. from the popup picker). (User 2026-06-22.)
          if (isCoded) {
            row.querySelectorAll('.actuals-suggest-btn, .actuals-ai-suggest-btn').forEach(b => b.remove());
          }
        }
        _actualsRecountStats();
        // Re-apply the active filter so a freshly-coded row auto-leaves a
        // filtered view (e.g. "Needs coding"). BUT not while reviewing matches:
        // re-filtering there un-hides every non-suggested row and kicks the user
        // out of Review mode — their #7 complaint. (User 2026-06-03.)
        if (!window._actualsInReview) {
          actualsApplyFilter(_actualsActiveFilter);
        }
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

    // Apply a smart vendor→line suggestion: select that option in the
    // row's picker and run the normal code path. 2026-05-29.
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
    window._actualsPopulatePickers = function () {
      document.querySelectorAll('#actuals-txn-list .actuals-line-picker:not([data-populated="1"])')
        .forEach(window._actualsFillPicker);
    };

    // ↗ Jump from a coded Actuals row to its budget line on the Budget tab + flash it.
    window.actualsJumpToLine = function (workingLineId) {
      if (!workingLineId) return;
      const bt = document.querySelector('[data-tab="working"]');
      if (bt) bt.click();
      setTimeout(() => {
        const t = document.querySelector('.line-row[data-id="' + workingLineId + '"]');
        if (t) {
          t.scrollIntoView({ behavior: 'smooth', block: 'center' });
          t.style.transition = 'background .8s';
          t.style.background = 'rgba(91,138,249,.25)';
          setTimeout(() => { t.style.background = ''; }, 1500);
        } else if (typeof _actualsToast === 'function') {
          _actualsToast('That line lives on a different budget view (Estimated/Actual).', 'yellow');
        }
      }, 300);
    };

    window.actualsApplySuggestion = function (btn) {
      const row = btn.closest('.actuals-txn-row');
      const sel = row && row.querySelector('.actuals-line-picker');
      if (!sel) return;
      window._actualsFillPicker(sel);   // ensure options exist before setting
      sel.value = String(btn.dataset.lineId || '');
      if (sel.value !== String(btn.dataset.lineId || '')) {
        alert('Suggested line is no longer in the picker — please choose one manually.');
        return;
      }
      btn.disabled = true;
      btn.textContent = '💡 coding…';
      actualsSetLine(sel);
      // Row re-renders / flips to coded on success; if it doesn't, the
      // button is gone after the next filter pass anyway.
    };

    // ── AI auto-coding (advisory) ─────────────────────────────────────
    // Accept a per-row ✨ suggestion: code the charge to the suggested COA
    // section via the same set-coa path as drag-drop. Reinforces the learned
    // vendor mapping server-side. (User 2026-06-18.)
    // 📎 v3: accept a backup suggestion — link the receipt to the matched
    // invoice subline via the existing mark-backup endpoint.
    window.actualsAcceptBackup = async function (btn) {
      const tid = btn.dataset.tid, target = btn.dataset.target;
      if (!tid || !target) return;
      const _orig = btn.textContent;
      btn.disabled = true; btn.textContent = '📎 linking…';
      try {
        const r = await fetch(`/projects/${PROJ_ID}/actuals/transaction/${tid}/mark-backup`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ target_txn_id: parseInt(target) }) });
        const j = await r.json();
        if (r.ok && j.ok) {
          if (typeof _refreshRowAfterCode === 'function') _refreshRowAfterCode(tid, false, true);
          if (typeof _actualsToast === 'function') _actualsToast('📎 Marked as backup — charge no longer counts.', 'green');
          btn.remove();
        } else {
          btn.disabled = false; btn.textContent = _orig;
          if (typeof _actualsToast === 'function') _actualsToast(j.error || 'Failed', 'yellow');
        }
      } catch (e) {
        btn.disabled = false; btn.textContent = _orig;
        if (typeof _actualsToast === 'function') _actualsToast('Error: ' + e.message, 'yellow');
      }
    };

    window.actualsAcceptAiCode = async function (btn) {
      const tid  = btn.dataset.tid;
      const code = btn.dataset.code;
      const name = btn.dataset.name || '';
      if (!tid || !code) return;
      const _orig = btn.textContent;
      btn.disabled = true; btn.textContent = '✨ coding…';
      try {
        const r = await fetch(`/projects/${PROJ_ID}/actuals/transaction/${tid}/set-coa`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ account_code: parseInt(code), account_code_name: name }) });
        if (r.ok) {
          if (typeof _refreshRowAfterCode === 'function') _refreshRowAfterCode(tid, true, false);
          if (typeof _recordRecent === 'function') _recordRecent({ kind: 'section', code: parseInt(code), name: name, label: name });
          if (typeof _actualsToast === 'function') _actualsToast(`Coded to ${code} · ${name}`, 'green');
          btn.remove();
          if (typeof _actualsRefreshSuggBar === 'function') _actualsRefreshSuggBar();
        } else {
          btn.disabled = false; btn.textContent = _orig;
          if (typeof _actualsToast === 'function') _actualsToast('Coding failed.', 'yellow');
        }
      } catch (e) {
        btn.disabled = false; btn.textContent = _orig;
        if (typeof _actualsToast === 'function') _actualsToast('Coding error: ' + e.message, 'yellow');
      }
    };

    // Toolbar: run AI categorize over every uncoded charge, in small server
    // batches, looping until none remain. Suggestions are advisory — the page
    // reloads so the ✨ chips render; nothing is auto-applied.
    window.actualsSuggestCodes = async function (btn) {
      const _orig = btn ? btn.textContent : '';
      if (btn) { btn.disabled = true; btn.textContent = '✨ Thinking…'; }
      let suggested = 0, processed = 0;
      try {
        for (let i = 0; i < 60; i++) {   // hard cap on batches
          const r = await fetch(`/projects/${PROJ_ID}/actuals/ai-suggest-codes`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ limit: 10 }) });
          if (!r.ok) break;
          const j = await r.json();
          suggested += j.suggested || 0; processed += j.processed || 0;
          if (btn) btn.textContent = `✨ ${suggested} suggested…`;
          if (!j.processed || !j.remaining) break;
        }
        if (typeof _actualsToast === 'function') {
          _actualsToast(processed
            ? `AI suggested codes for ${suggested} of ${processed} charge(s). Reloading…`
            : 'No uncoded charges to suggest.', 'green');
        }
        if (processed) setTimeout(() => window.location.reload(), 1000);
        else if (btn) { btn.disabled = false; btn.textContent = _orig; }
      } catch (e) {
        if (btn) { btn.disabled = false; btn.textContent = _orig; }
        if (typeof _actualsToast === 'function') _actualsToast('Suggest codes failed: ' + e.message, 'yellow');
      }
    };

    // ── Dashboard / Action Center (review queue) — 2026-06-18 ──────────
    function _dashEsc(s) { return (s || '').toString().replace(/</g, '&lt;'); }
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
        host.innerHTML = '<div style="border:1px dashed var(--border);border-radius:8px;padding:14px;text-align:center;color:var(--text-muted);font-size:.8rem">✓ Nothing needs review. Use “Clean up data” or “Scan for duplicates” to check again.</div>';
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
          actions += `<button type="button" class="btn btn-xs btn-primary" onclick="document.querySelector('[data-tab=actuals]').click()">Open Actuals</button>`;
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
      // Prefer the receipt (modal) when there is one; otherwise reliably show the
      // charge in Actuals by scrolling to + flashing its row. (User 2026-06-18.)
      if (docId && typeof window.openDocDetail === 'function') { window.openDocDetail(docId, null); return; }
      const show = () => {
        const row = document.querySelector('.actuals-txn-row[data-tid="' + tid + '"]');
        if (row) {
          row.scrollIntoView({ behavior: 'smooth', block: 'center' });
          const prev = row.style.background;
          row.style.transition = 'background .8s';
          row.style.background = 'rgba(91,138,249,.22)';
          setTimeout(() => { row.style.background = prev || ''; }, 1600);
        } else if (typeof window.actualsOpenEditTxn === 'function') {
          window.actualsOpenEditTxn(tid, docId || null);
        } else if (typeof _actualsToast === 'function') {
          _actualsToast('Could not locate that charge — it may be filtered or coded away.', 'yellow');
        }
      };
      const panel = document.getElementById('tab-actuals');
      if (panel && panel.classList.contains('active')) { show(); }
      else { const t = document.querySelector('[data-tab="actuals"]'); if (t) t.click(); setTimeout(show, 450); }
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
        else { if (btn) btn.disabled = false; alert('Action failed'); }
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
    window.actualsCleanup = function (btn) { return aiCleanupAll(btn, true); };
    // 🤖 AI match — loop the match pass over unmatched charges, then reload so the
    // ⚡ Suggested-match banners (Confirm / Not a match) appear. (User 2026-06-18.)
    window.actualsAiMatch = async function (btn) {
      const _o = btn ? btn.textContent : '';
      if (btn) { btn.disabled = true; btn.textContent = '🤖 Matching…'; }
      let suggested = 0, processed = 0, errored = false;
      try {
        for (let i = 0; i < 80; i++) {
          const r = await fetch(`/projects/${PROJ_ID}/actuals/ai-match`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ limit: 6 }) });
          if (!r.ok) {
            errored = true;
            let detail = r.status;
            try { const ej = await r.json(); if (ej && ej.error) detail = ej.error; } catch (e) {}
            if (typeof _actualsToast === 'function') _actualsToast('AI match failed (' + detail + ').', 'yellow');
            break;
          }
          const j = await r.json();
          suggested += j.suggested || 0; processed += j.processed || 0;
          if (btn) btn.textContent = `🤖 ${suggested} matched…`;
          if (!j.processed || !j.remaining) break;
        }
        if (!errored && suggested) {
          // New proposals — reload so the gold banners render, then auto-open the
          // Review-matches view (flag survives the reload).
          if (typeof _actualsToast === 'function') _actualsToast(`AI proposed ${suggested} match(es). Opening review…`, 'green');
          try { sessionStorage.setItem('fpOpenMatchReview', '1'); } catch (e) {}
          setTimeout(() => window.location.reload(), 1000);
        } else if (!errored) {
          // Nothing new examined. If matches are already waiting, open review now.
          const rc = document.getElementById('actualsReviewBtn');
          if (rc && rc.offsetParent !== null && typeof actualsReviewMatches === 'function') {
            if (typeof _actualsToast === 'function') _actualsToast('No new charges — opening the matches already waiting.', 'green');
            actualsReviewMatches();
          } else if (typeof _actualsToast === 'function') {
            _actualsToast('No unmatched charges left to match.', 'green');
          }
          if (btn) { btn.disabled = false; btn.textContent = _o || '🤖 AI match'; }
        } else if (btn) { btn.disabled = false; btn.textContent = _o || '🤖 AI match'; }
      } catch (e) {
        if (btn) { btn.disabled = false; btn.textContent = _o || '🤖 AI match'; }
        if (typeof _actualsToast === 'function') _actualsToast('AI match failed: ' + e.message, 'yellow');
      }
    };
    // Open the Review-matches view from the Dashboard card.
    window.dashOpenMatchReview = function () {
      const tb = document.querySelector('[data-tab="actuals"]');
      if (tb) tb.click();
      setTimeout(() => { if (typeof actualsReviewMatches === 'function') actualsReviewMatches(); }, 350);
    };

    // ── AI line-suggestion review bar (Actuals) — 2026-06-18 ──────────
    function _actualsRefreshSuggBar() {
      const bar = document.getElementById('actuals-sugg-bar');
      if (!bar) return;
      const n = document.querySelectorAll('.actuals-ai-suggest-btn').length;
      const cEl = document.getElementById('actuals-sugg-count');
      if (cEl) cEl.textContent = n;
      bar.style.display = n ? 'flex' : 'none';
      if (!n) {
        const panel = document.getElementById('tab-actuals');
        if (panel) panel.classList.remove('ax-suggested-only');
        const tg = document.getElementById('actuals-sugg-toggle');
        if (tg) tg.textContent = 'Show only these';
      }
    }
    window._actualsRefreshSuggBar = _actualsRefreshSuggBar;
    window.actualsToggleSuggestedOnly = function (btn) {
      const panel = document.getElementById('tab-actuals');
      if (!panel) return;
      const on = panel.classList.toggle('ax-suggested-only');
      if (btn) btn.textContent = on ? 'Show all charges' : 'Show only these';
    };
    window.actualsAcceptAllSuggestions = async function (btn) {
      const chips = Array.from(document.querySelectorAll('.actuals-ai-suggest-btn'));
      if (!chips.length) return;
      if (!confirm('Apply all ' + chips.length + ' AI-suggested codes? You can re-code any row afterward.')) return;
      const _o = btn ? btn.textContent : '';
      if (btn) btn.disabled = true;
      let ok = 0, fail = 0;
      for (const chip of chips) {
        const tid = chip.dataset.tid, code = chip.dataset.code, name = chip.dataset.name || '';
        if (btn) btn.textContent = 'Applying ' + (ok + fail + 1) + '/' + chips.length + '…';
        try {
          const r = await fetch(`/projects/${PROJ_ID}/actuals/transaction/${tid}/set-coa`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ account_code: parseInt(code), account_code_name: name }) });
          if (r.ok) { ok++; if (typeof _refreshRowAfterCode === 'function') _refreshRowAfterCode(tid, true, false); chip.remove(); }
          else fail++;
        } catch (e) { fail++; }
      }
      if (typeof _actualsToast === 'function') _actualsToast('Coded ' + ok + ' charge(s)' + (fail ? ' (' + fail + ' failed)' : '') + '.', fail ? 'yellow' : 'green');
      _actualsRefreshSuggBar();
      if (btn) { btn.disabled = false; btn.textContent = _o || 'Accept all'; }
    };
    // Wipe all AI suggestions, then re-run the suggest pass from scratch (testing).
    window.actualsClearSuggestions = async function (btn) {
      if (!confirm('Clear all AI suggestions and re-run from scratch?')) return;
      const _o = btn ? btn.textContent : '';
      if (btn) { btn.disabled = true; btn.textContent = '↻ Clearing…'; }
      try {
        const r = await fetch(`/projects/${PROJ_ID}/actuals/clear-suggestions`, { method: 'POST', credentials: 'same-origin' });
        if (!r.ok) {
          if (btn) { btn.disabled = false; btn.textContent = _o; }
          if (typeof _actualsToast === 'function') _actualsToast('Clear failed.', 'yellow');
          return;
        }
        // Re-run the suggest pass — it loops then reloads the page when done.
        if (typeof actualsSuggestCodes === 'function') { await actualsSuggestCodes(btn); }
        else { window.location.reload(); }
      } catch (e) {
        if (btn) { btn.disabled = false; btn.textContent = _o; }
        if (typeof _actualsToast === 'function') _actualsToast('Clear error: ' + e.message, 'yellow');
      }
    };
    // Refresh the bar when the Actuals tab opens (and on load if it's active).
    (function () {
      const tb = document.querySelector('[data-tab="actuals"]');
      if (tb) tb.addEventListener('click', () => setTimeout(_actualsRefreshSuggBar, 200));
      const p = document.getElementById('tab-actuals');
      if (p && p.classList.contains('active')) setTimeout(_actualsRefreshSuggBar, 400);
      // After an AI-match run reloads the page, auto-open the Review-matches view.
      let _wantReview = false;
      try { _wantReview = sessionStorage.getItem('fpOpenMatchReview') === '1'; if (_wantReview) sessionStorage.removeItem('fpOpenMatchReview'); } catch (e) {}
      if (_wantReview) setTimeout(() => {
        const t = document.querySelector('[data-tab="actuals"]'); if (t) t.click();
        setTimeout(() => { if (typeof actualsReviewMatches === 'function') actualsReviewMatches(); }, 400);
      }, 500);
    })();

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

    // Expand/collapse a department; lazy-load its receipt thumbnails the
    // first time it opens so we never fire hundreds of image requests at once.
    window.codeToggleGroup = function (head) {
      const grp = head.closest('.code-group');
      const body = grp.querySelector('.code-group-body');
      const open = grp.classList.toggle('open');
      body.style.display = open ? '' : 'none';
      if (open) codeLoadThumbs(grp);
    };
    window.codeLoadThumbs = function (grp) {
      grp.querySelectorAll('.code-thumb[data-img="1"]:not([data-loaded="1"])').forEach(th => {
        const doc = th.dataset.doc; if (!doc) return;
        th.dataset.loaded = '1';
        const img = document.createElement('img');
        img.loading = 'lazy'; img.alt = 'receipt';
        img.src = `/docs/upload/${doc}/raw`;
        img.onerror = () => { th.dataset.loaded = '0'; img.remove(); };
        th.innerHTML = ''; th.appendChild(img);
      });
    };
    // Inline coding — reuse the Match handler (it persists + updates the
    // canonical row + stat cards), then reflect coded state on THIS row.
    window.codeSetLine = function (sel) {
      const row = sel.closest('.code-row');
      window.actualsSetLine(sel);
      if (row && sel.value && sel.value !== '__clear__') {
        row.dataset.coded = '1'; row.dataset.needs = '0';
        const chip = row.querySelector('.code-sugg'); if (chip) chip.remove();
        if (document.getElementById('code-needs-only') && document.getElementById('code-needs-only').checked) {
          row.style.display = 'none';
        }
      }
    };
    window.codeApplySuggestion = function (btn) {
      const row = btn.closest('.code-row');
      const sel = row && row.querySelector('.code-picker');
      if (!sel) return;
      window._actualsFillPicker(sel);
      sel.value = String(btn.dataset.lineId || '');
      if (sel.value !== String(btn.dataset.lineId || '')) {
        alert('Suggested line is no longer in the picker — please pick one manually.');
        return;
      }
      btn.disabled = true; btn.textContent = '💡 coding…';
      codeSetLine(sel);
    };
    // Selection + bulk coding.
    window.codeSelCount = function () {
      const n = document.querySelectorAll('#actuals-pane-code .code-chk:checked').length;
      const el = document.getElementById('code-selcount');
      if (el) el.textContent = n + ' selected';
    };
    window.codeToggleAll = function (cb) {
      document.querySelectorAll('#actuals-pane-code .code-chk').forEach(c => {
        const row = c.closest('.code-row');
        if (row && row.offsetParent !== null) c.checked = cb.checked;  // only visible rows
      });
      codeSelCount();
    };
    window._codeFillBulk = function (sel) {
      if (sel.dataset.populated === '1') return;
      const tpl = document.getElementById('actualsLineOptsTpl');
      if (!tpl) return;
      sel.innerHTML = '<option value="">Code selected to…</option>' + tpl.innerHTML;
      sel.dataset.populated = '1'; sel.value = '';
    };
    window.codeBulkApply = async function () {
      const val = (document.getElementById('code-bulk-line') || {}).value;
      if (!val) { _actualsToast('Pick a line to code the selected rows to.', 'yellow'); return; }
      const checked = [...document.querySelectorAll('#actuals-pane-code .code-chk:checked')];
      if (!checked.length) { _actualsToast('No rows selected.', 'yellow'); return; }
      if (!confirm('Code ' + checked.length + ' selected expense' + (checked.length !== 1 ? 's' : '') + ' to this line?')) return;
      for (const c of checked) {
        const row = c.closest('.code-row');
        const sel = row && row.querySelector('.code-picker');
        if (!sel) continue;
        window._actualsFillPicker(sel);
        sel.value = val;
        if (sel.value === val) { await window.actualsSetLine(sel);
          row.dataset.coded = '1'; row.dataset.needs = '0';
          const chip = row.querySelector('.code-sugg'); if (chip) chip.remove();
          c.checked = false; }
      }
      codeSelCount();
      _actualsToast('Coded ' + checked.length + ' expense' + (checked.length !== 1 ? 's' : '') + '. They’ll group under their department on reload.', 'green');
    };
    // "Only needs coding" — hide already-coded rows + departments.
    window.codeFilterNeeds = function (cb) {
      const on = cb.checked;
      document.querySelectorAll('#actuals-pane-code .code-row').forEach(r => {
        r.style.display = (on && r.dataset.needs === '0') ? 'none' : '';
      });
      document.querySelectorAll('#actuals-pane-code .code-group').forEach(g => {
        if (!on) { g.style.display = ''; return; }
        const anyNeeds = [...g.querySelectorAll('.code-row')].some(r => r.dataset.needs === '1');
        g.style.display = anyNeeds ? '' : 'none';
        if (anyNeeds && !g.classList.contains('open')) {
          g.classList.add('open');
          const body = g.querySelector('.code-group-body'); if (body) body.style.display = '';
          codeLoadThumbs(g);
        }
      });
    };
    // Toggle tile grid vs compact list (same rows, reshaped via CSS).
    window.codeSetView = function (mode) {
      const pane = document.getElementById('actuals-pane-code');
      if (!pane) return;
      const tiles = mode !== 'list';
      pane.classList.toggle('code-view-tiles', tiles);
      const tb = document.getElementById('code-view-tiles-btn');
      const lb = document.getElementById('code-view-list-btn');
      if (tb) tb.classList.toggle('on', tiles);
      if (lb) lb.classList.toggle('on', !tiles);
      try { localStorage.setItem('fpCodeView', mode); } catch (e) {}
      if (tiles) {  // tiles need the images; load any open group's thumbs
        pane.querySelectorAll('.code-group.open').forEach(window.codeLoadThumbs);
      }
    };
    // Show/hide the (uncoded) "Needs coding" bucket — hidden by default since
    // this tab is for reviewing what's ALREADY coded. (User 2026-06-16.)
    window.codeToggleNeeds = function (btn) {
      const grp = document.querySelector('#actuals-pane-code .code-group.code-needs');
      if (!grp) { if (btn) btn.style.display = 'none'; return; }
      const show = grp.style.display === 'none';
      grp.style.display = show ? '' : 'none';
      if (btn) btn.textContent = show ? 'Hide needs coding' : 'Show needs coding';
      if (show && !grp.classList.contains('open')) {  // auto-expand on first show
        grp.classList.add('open');
        const body = grp.querySelector('.code-group-body'); if (body) body.style.display = '';
        codeLoadThumbs(grp);
      }
    };

    // Recompute suggestions on demand and reconcile the 💡 chips in the
    // current DOM — called when the Actuals tab is (re)opened so the chips
    // reflect doc corrections + new line assignments made since page load.
    // 2026-05-29. Lightweight: one GET + a per-row chip add/update/remove.
    let _actualsSuggestRefreshing = false;
    window.refreshActualsSuggestions = async function () {
      // Disabled (2026-06-18): the token-overlap green chips are retired — the
      // AI suggestion (purple ✨ chip) is now the single suggestion source.
      return;
      if (_actualsSuggestRefreshing) return;
      _actualsSuggestRefreshing = true;
      try {
        const r = await fetch(`/projects/${PROJ_ID}/actuals/suggestions`, { credentials: 'same-origin' });
        if (!r.ok) return;
        const sugg = (await r.json()).suggestions || {};
        // Rows live in #actuals-sections after sectionize — include both
        // containers or this silently no-ops. (Review 2026-06-04.)
        document.querySelectorAll('#actuals-sections .actuals-txn-row, #actuals-txn-list .actuals-txn-row').forEach(row => {
          const tid    = row.dataset.tid;
          const coded  = row.dataset.coded === '1';
          const notProj= row.dataset.notProject === '1';
          let chip     = row.querySelector('.actuals-suggest-btn');
          const s      = sugg[tid];
          if (coded || notProj || !s) { if (chip) chip.remove(); return; }
          if (!chip) {
            chip = document.createElement('button');
            chip.type = 'button';
            chip.className = 'actuals-suggest-btn';
            chip.setAttribute('onclick', 'actualsApplySuggestion(this)');
            chip.style.cssText = 'flex-shrink:0;font-size:.66rem;padding:2px 8px;background:#10231a;'
              + 'border:1px solid #1f6f4a;border-radius:4px;color:#5fd0a0;cursor:pointer;'
              + 'white-space:nowrap;max-width:180px;overflow:hidden;text-overflow:ellipsis';
            const picker = row.querySelector('.actuals-line-picker');
            if (picker) picker.parentNode.insertBefore(chip, picker);
            else row.appendChild(chip);
          }
          chip.dataset.tid    = tid;
          chip.dataset.lineId = s.line_id;
          if (s.kind === 'section') {
            chip.title = `Looks like ${s.label} — click to drop into that category (subdivide later)`;
            chip.textContent = `📂 ${s.label}`;
          } else {
            chip.title = `Smart match from the vendor name — click to code to ${s.code} · ${s.label}`;
            chip.textContent = `💡 ${s.code} · ${s.label}`;
          }
        });
      } catch (e) { /* silent — suggestions are best-effort */ }
      finally { _actualsSuggestRefreshing = false; }
    };

    window.actualsMarkNotProject = async function (tid) {
      if (!confirm('Mark as not a project expense? It will be greyed out and excluded from rollups.')) return;
      try {
        const r = await fetch(`/projects/${PROJ_ID}/actuals/transaction/${tid}/mark-not-project`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}',
        });
        if (!r.ok) {
          const d = await r.json().catch(() => ({}));
          alert('Could not mark: ' + (d.error || r.status));
          return;
        }
        window.location.reload();
      } catch (e) {
        alert('Failed: ' + e.message);
      }
    };

    window.actualsSyncNow = async function (pid, firstSync) {
      const status = document.getElementById('actuals-sync-status');
      if (status) { status.textContent = 'Syncing…'; status.style.color = 'var(--text-muted)'; }
      // Read date inputs if the panel is in first-sync or
      // advanced-custom-range mode. Otherwise let the server use
      // its watermark logic.
      const startEl = document.getElementById('actuals-sync-start');
      const endEl   = document.getElementById('actuals-sync-end');
      const body = {};
      if (firstSync) {
        if (!startEl || !startEl.value) {
          if (status) { status.textContent = '✕ Pick a start date.'; status.style.color = '#e08080'; }
          return;
        }
        body.start_date = startEl.value;
        if (endEl && endEl.value) body.end_date = endEl.value;
      }
      try {
        const r = await fetch(`/projects/${pid}/actuals/sync-now`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });
        const d = await r.json();
        if (!r.ok) {
          if (status) {
            status.textContent = '✕ ' + (d.error || ('HTTP ' + r.status));
            status.style.color = '#e05555';
          }
          return;
        }
        // If QBO returned transactions on accounts the user hasn't
        // selected, surface that as a clear actionable warning before
        // reloading. Common cause: a corporate credit card typed as
        // "Other Current Liability" in QBO that the user hasn't ticked
        // in Settings → QBO accounts. Without this, a sync that returns
        // 0 looks identical whether QBO has nothing OR everything's on
        // an unselected account.
        const unmatched = d.unmatched_accounts || [];
        if (unmatched.length && (d.imported || 0) === 0) {
          if (status) {
            // Build inline "Add & re-sync" buttons for each unmatched
            // account so the user doesn't have to scroll the picker.
            const safe = (s) => String(s).replace(/[&<>"']/g, c => (
              {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]
            ));
            const btns = unmatched.map(u =>
              `<button type="button" class="btn btn-xs btn-primary qbo-add-acct-btn" data-acct-id="${safe(u.id)}" data-acct-label="${safe(u.label)}" style="margin:2px 4px 2px 0">+ Add ${safe(u.label)}</button>`
            ).join('');
            status.innerHTML =
              `⚠ 0 imported. QuickBooks returned transactions on accounts you haven't enabled: ` +
              `<div style="margin-top:6px">${btns}</div>`;
            status.style.color = '#e0a040';
            // Wire each button: tick the matching checkbox in the
            // picker, save accounts, then re-trigger the same sync.
            status.querySelectorAll('.qbo-add-acct-btn').forEach(btn => {
              btn.addEventListener('click', async () => {
                const acctId = btn.dataset.acctId;
                const lbl    = btn.dataset.acctLabel;
                btn.disabled = true;
                btn.textContent = `Adding ${lbl}…`;
                // Tick the checkbox if present so the picker stays
                // visually in sync with what we're about to save.
                const cb = document.querySelector(`.actuals-qbo-acct[value="${acctId}"]`);
                if (cb) cb.checked = true;
                // Build the new id list = currently-checked + this id
                // (defensive in case the checkbox isn't in the DOM).
                const ids = new Set(Array.from(
                  document.querySelectorAll('.actuals-qbo-acct:checked')
                ).map(c => c.value));
                ids.add(acctId);
                console.log('[qbo-add-acct] saving ids:', Array.from(ids), 'adding:', acctId);
                try {
                  const sr = await fetch(`/projects/${pid}/actuals/qbo-accounts`, {
                    method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ account_ids: Array.from(ids) }),
                  });
                  if (!sr.ok) {
                    const ed = await sr.json().catch(() => ({}));
                    btn.textContent = `✕ ${ed.error || 'save failed'}`;
                    return;
                  }
                  btn.textContent = `✓ Added — re-syncing…`;
                  // Re-trigger the sync without firstSync gating; pass
                  // any remembered date window from the form.
                  const body2 = {};
                  if (startEl && startEl.value) body2.start_date = startEl.value;
                  if (endEl   && endEl.value)   body2.end_date   = endEl.value;
                  const r2 = await fetch(`/projects/${pid}/actuals/sync-now`, {
                    method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(body2),
                  });
                  const d2 = await r2.json();
                  if (!r2.ok) {
                    btn.textContent = `✕ ${d2.error || 'sync failed'}`;
                    return;
                  }
                  status.textContent = `✓ Imported ${d2.imported || 0} txns. Reloading…`;
                  status.style.color = 'var(--green)';
                  setTimeout(() => window.location.reload(), 600);
                } catch (e) {
                  btn.textContent = `✕ ${e.message}`;
                }
              });
            });
          }
          return;
        }
        if (status) {
          let msg = `✓ Imported ${d.imported || 0} txns (CDC: ${d.cdc_additions || 0}). Reloading…`;
          if (unmatched.length) {
            const lbls = unmatched.map(u => u.label).join(', ');
            msg += ` ⚠ Also saw txns on unselected accounts: ${lbls}.`;
          }
          status.textContent = msg;
          status.style.color = 'var(--green)';
        }
        setTimeout(() => window.location.reload(), 800);
      } catch (e) {
        if (status) { status.textContent = '✕ ' + e.message; status.style.color = '#e05555'; }
      }
    };

    // Single-source filter resolver. Reads both:
    //   • stat-card filter (_actualsActiveFilter)
    //   • filter bar (date / vendor / section / receipt / amount)
    // and computes per-row visibility. Both subsystems call this
    // whenever they change; we don't have one writing display only
    // for the other to overwrite.
    let _actualsActiveFilter = 'uncoded';

    // Section starts parsed ONCE from the shared options template (whose
    // optgroups are now real COA sections). _actualsSectionForCode(2610) →
    // 2600. Used by the section filter so it works off each row's
    // data-acct-code instead of requiring a populated picker. (2026-06-04.)
    let _actualsSectionStarts = null;
    function _actualsSectionForCode(code) {
      if (_actualsSectionStarts === null) {
        _actualsSectionStarts = [];
        const tpl = document.getElementById('actualsLineOptsTpl');
        if (tpl) {
          tpl.querySelectorAll('optgroup').forEach(g => {
            const m = (g.label || '').match(/^(\d+)/);
            if (m) _actualsSectionStarts.push(parseInt(m[1], 10));
          });
          _actualsSectionStarts.sort((a, b) => a - b);
        }
      }
      let best = NaN;
      for (const s of _actualsSectionStarts) {
        if (code >= s) best = s; else break;
      }
      return best;
    }

    function _actualsResolveFilters() {
      const f = _actualsActiveFilter;
      // Filter-bar values
      const dFrom    = (document.getElementById('actuals-filter-date-from') || {}).value || '';
      const dTo      = (document.getElementById('actuals-filter-date-to')   || {}).value || '';
      const vendor   = ((document.getElementById('actuals-filter-vendor')  || {}).value || '').toLowerCase().trim();
      const card     = (document.getElementById('actuals-filter-card')     || {}).value || '';
      const section  = (document.getElementById('actuals-filter-section')  || {}).value || '';
      const receipt  = (document.getElementById('actuals-filter-receipt')  || {}).value || '';
      const uploader = (document.getElementById('actuals-filter-uploader') || {}).value || '';
      const amtMin   = parseFloat((document.getElementById('actuals-filter-amount-min') || {}).value || 'NaN');
      const amtMax   = parseFloat((document.getElementById('actuals-filter-amount-max') || {}).value || 'NaN');

      // Index banners ONCE — a full-document querySelector per row inside the
      // loop was the filter hot-path cost on ~1,200 rows. (Perf 2026-06-11.)
      const bannerByTid = {};
      document.querySelectorAll('.actuals-suggested-banner').forEach(b => { bannerByTid[b.dataset.tid] = b; });

      if (typeof window._actualsSaveFilterState === 'function') window._actualsSaveFilterState();

      let shown = 0, total = 0;
      document.querySelectorAll('.actuals-txn-row').forEach(r => {
        total++;
        let pass = true;

        // Cross-project claim: admin-only "claimed elsewhere" rows show
        // ONLY in the All view. Every other filter is about work-to-do
        // for this project, and those rows aren't this project's work.
        if (r.dataset.claimedElsewhere === '1' && f !== 'all') {
          r.style.display = 'none';
          return;
        }

        // Stat-card filter
        if (f === 'coded')         pass = r.dataset.coded === '1';
        else if (f === 'uncoded')  pass = r.dataset.coded === '0' && r.dataset.notProject !== '1';
        else if (f === 'no_doc')   pass = r.dataset.hasDoc === '0' && r.dataset.notProject !== '1'
                                          && (r.dataset.source === 'qbo_sync' || r.dataset.source === 'reconciled' || r.dataset.source === 'csv_import');
        else if (f === 'doc_only') pass = r.dataset.source === 'doc_upload';
        else if (f === 'finished') {
          // Audit-clean: coded + has receipt + confirmed.
          pass = (r.dataset.coded === '1' &&
                  r.dataset.hasDoc === '1' &&
                  r.dataset.matchStatus === 'confirmed' &&
                  r.dataset.notProject !== '1');
        }
        else if (f === 'ocr_review') {
          // Doc-source rows that look like OCR failed: blank vendor or
          // zero/null amount. data-amount is "0" when amount is null
          // or 0 from the server-side render.
          const amt = parseFloat(r.dataset.amount || '0');
          const vendor = (r.querySelector('.actuals-txn-vendor')?.textContent || '').trim();
          pass = (r.dataset.source === 'doc_upload' &&
                  r.dataset.notProject !== '1' &&
                  (!vendor || vendor === '— vendor unknown —' || amt === 0));
        }

        // Filter-bar
        if (pass && dFrom && (r.dataset.txnDate || '') < dFrom) pass = false;
        if (pass && dTo   && (r.dataset.txnDate || '') > dTo)   pass = false;
        if (pass && vendor) {
          const hay = ((r.querySelector('.actuals-txn-vendor')?.textContent || '') + ' ' +
                       (r.dataset.note || '')).toLowerCase();
          if (!hay.includes(vendor)) pass = false;
        }
        if (pass && section) {
          // Primary: the row's own account code → section (works for lazy
          // pickers, which only seed one option with no optgroup — the old
          // OPTGROUP-only check hid every coded row whose picker hadn't been
          // focused; review 2026-06-04). Fallback: populated-picker optgroup
          // (covers rows coded this session before any reload).
          let matched = false;
          const ac = parseInt(r.dataset.acctCode || '', 10);
          if (!isNaN(ac)) {
            matched = (_actualsSectionForCode(ac) === parseInt(section, 10));
          } else {
            const sel = r.querySelector('.actuals-line-picker');
            if (sel && sel.value) {
              if (sel.value.startsWith('section:')) {
                matched = sel.value.slice(8) === String(section);
              } else {
                const opt = sel.options[sel.selectedIndex];
                const grp = opt && opt.parentElement;
                if (grp && grp.tagName === 'OPTGROUP') {
                  matched = grp.label.startsWith(String(section) + ' ');
                }
              }
            }
          }
          if (!matched) pass = false;
        }
        if (pass && receipt) {
          if (receipt === 'yes' && r.dataset.hasDoc !== '1') pass = false;
          if (receipt === 'no'  && r.dataset.hasDoc === '1') pass = false;
        }
        if (pass && uploader && (r.dataset.uploaderId || '') !== uploader) pass = false;
        if (pass && card && (r.dataset.sortCard4 || '') !== card) pass = false;
        if (pass && !isNaN(amtMin) && parseFloat(r.dataset.amount || 0) < amtMin) pass = false;
        if (pass && !isNaN(amtMax) && parseFloat(r.dataset.amount || 0) > amtMax) pass = false;

        r.style.display = pass ? 'flex' : 'none';
        const banner = bannerByTid[r.dataset.tid];
        if (banner) banner.style.display = pass ? 'flex' : 'none';
        if (pass) shown++;
      });
      const lbl = document.getElementById('actuals-filter-count');
      if (lbl) lbl.textContent = (shown === total) ? `${total} txns` : `${shown} of ${total} shown`;
      // Sorting is intentionally NOT called here. Filtering only toggles row
      // visibility and never changes order, so re-sorting (which re-appends all
      // ~1,200 rows) on every tile click was the main tile-switch freeze. Sort
      // runs only when the sort dropdown changes, on init, and after a
      // re-bucket. (User 2026-06-03.)
    }

    // Sort the visible rows in DOM order. The list container is
    // #actuals-txn-list; the suggested-match banners live as siblings
    // of the rows, so we move both together.
    function _actualsApplySort() {
      const sortBy = (document.getElementById('actuals-sort-by') || {}).value || 'date-desc';
      const cmp = (a, b) => {
        const ad = a.dataset.txnDate || '';
        const bd = b.dataset.txnDate || '';
        const aa = parseFloat(a.dataset.amount || 0);
        const ba = parseFloat(b.dataset.amount || 0);
        const av = (a.querySelector('.actuals-txn-vendor')?.textContent || '').toLowerCase();
        const bv = (b.querySelector('.actuals-txn-vendor')?.textContent || '').toLowerCase();
        const ac = a.dataset.sortCard4 || '';
        const bc = b.dataset.sortCard4 || '';
        const af = parseInt(a.dataset.fulfill || 0, 10);
        const bf = parseInt(b.dataset.fulfill || 0, 10);
        switch (sortBy) {
          case 'date-asc':    return ad < bd ? -1 : ad > bd ? 1 : 0;
          case 'date-desc':   return ad > bd ? -1 : ad < bd ? 1 : 0;
          case 'amount-asc':  return aa - ba;
          case 'amount-desc': return ba - aa;
          case 'vendor-asc':  return av.localeCompare(bv);
          case 'vendor-desc': return bv.localeCompare(av);
          // Blanks (no card captured) sort to the bottom either direction.
          case 'card4-asc':   return (ac||'~').localeCompare(bc||'~');
          case 'card4-desc':  return (bc||'~').localeCompare(ac||'~');
          // Restack by match progress; tiebreak newest-first within a tier
          // so each "stack" of equally-matched items stays date-ordered.
          case 'progress-desc': return (bf - af) || (ad > bd ? -1 : ad < bd ? 1 : 0);
          case 'progress-asc':  return (af - bf) || (ad > bd ? -1 : ad < bd ? 1 : 0);
        }
        return 0;
      };
      // Sort WITHIN each container. In grouped view that's each section body;
      // otherwise the flat list. Keeps each suggested-match banner with its row.
      const wrap = document.getElementById('actuals-sections');
      let containers;
      if (wrap) {
        containers = Array.from(wrap.querySelectorAll(':scope > div > div:nth-child(2)'));
      } else {
        const list = document.getElementById('actuals-txn-list');
        containers = list ? [list] : [];
      }
      // Index banners once (was a full-document querySelector PER row → O(n)
      // doc scans) and re-append via a DocumentFragment (one reflow per
      // container, not per row). (User 2026-06-03.)
      const _bannerByTid = {};
      document.querySelectorAll('.actuals-suggested-banner').forEach(b => { _bannerByTid[b.dataset.tid] = b; });
      containers.forEach(c => {
        const rows = Array.from(c.querySelectorAll('.actuals-txn-row'));
        rows.sort(cmp);
        const frag = document.createDocumentFragment();
        rows.forEach(r => {
          const banner = _bannerByTid[r.dataset.tid];
          if (banner) frag.appendChild(banner);
          frag.appendChild(r);
        });
        c.appendChild(frag);
      });
    }

    window.actualsApplyFilter = function (filter) {
      _actualsActiveFilter = filter;
      document.querySelectorAll('.actuals-stat-card').forEach(c => {
        c.style.borderColor = c.dataset.filter === filter ? 'var(--blue)' : 'var(--border)';
      });
      // Stat-card filters only act on the Match sub-tab (the row list).
      // If the user is currently on Reconcile and clicks a chip, switch
      // them into Match so the click does something visible. Otherwise
      // the filter applies "silently" to a hidden pane and nothing
      // appears to happen.
      const matchPane = document.getElementById('actuals-pane-match');
      const onMatch = matchPane && matchPane.style.display !== 'none';
      if (!onMatch && typeof window.showActualsPane === 'function') {
        window.showActualsPane('match');
      }
      _actualsResolveFilters();
    };

    // Grouped 3-section view is the default. Show ALL rows (so sections are
    // complete), then bucket them into Needs-receipt / Receipts / Linked.
    // Stat-card + filter-bar still work on top (they hide rows within sections).
    function _actualsInitView() {
      // Restore the saved view (stat-card filter + filter fields + sort) so a
      // reload — e.g. after uploading a receipt — doesn't reset it. (2026-06-11.)
      let _saved = null;
      try {
        if (typeof window._actualsPopulateCardFilter === 'function') window._actualsPopulateCardFilter();
        if (typeof _actualsRestoreFilterState === 'function') _saved = _actualsRestoreFilterState();
      } catch (e) { _saved = null; }
      if (typeof actualsApplyFilter === 'function') actualsApplyFilter((_saved && _saved.f) || 'all');
      if (typeof window._actualsSectionize === 'function') window._actualsSectionize();
      if (typeof _actualsApplySort === 'function') _actualsApplySort();   // initial order
    }
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', _actualsInitView);
    } else {
      _actualsInitView();
    }

    // Initial recount on page load — defensive; server-rendered values
    // should already be correct but this catches any drift between the
    // server-side count and the DOM state (e.g. a row hidden by CSS
    // would mismatch).
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', _actualsRecountStats);
    } else {
      _actualsRecountStats();
    }

    // ── Review-matches mode: focused list of suggested pairings + bulk ──
    function _actualsRefreshReviewBtn() {
      const n = document.querySelectorAll('.actuals-suggested-banner').length;
      const btn = document.getElementById('actualsReviewBtn');
      const cnt = document.getElementById('actualsReviewCount');
      if (btn) { btn.style.display = n ? '' : 'none'; if (cnt) cnt.textContent = n ? '(' + n + ')' : ''; }
      return n;
    }
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', _actualsRefreshReviewBtn);
    } else { _actualsRefreshReviewBtn(); }

    let _reviewThumbObs = null;
    function _initReviewThumbs() {
      if (_reviewThumbObs) _reviewThumbObs.disconnect();
      _reviewThumbObs = new IntersectionObserver((entries) => {
        entries.forEach(e => {
          if (!e.isIntersecting) return;
          const banner = e.target;
          _reviewThumbObs.unobserve(banner);
          if (banner.dataset.thumbLoaded === '1' || banner.dataset.docImg !== '1') return;
          banner.dataset.thumbLoaded = '1';
          const docId = banner.dataset.docId, slot = banner.querySelector('.recpt-thumb');
          if (!docId || !slot) return;
          const img = new Image();
          img.src = '/docs/upload/' + docId + '/raw';
          img.style.cssText = 'width:100%;height:100%;object-fit:cover';
          img.onload = () => { slot.textContent = ''; slot.appendChild(img); };
        });
      }, { rootMargin: '300px' });
      document.querySelectorAll('.actuals-suggested-banner').forEach(b => _reviewThumbObs.observe(b));
    }

    window.actualsReviewMatches = function () {
      window._actualsInReview = true;
      if (typeof window.showActualsPane === 'function') window.showActualsPane('match');
      const banners = [...document.querySelectorAll('.actuals-suggested-banner')];
      if (!banners.length) { if (typeof _actualsToast === 'function') _actualsToast('No suggested matches to review', 'red'); return; }
      const sugTids = new Set(banners.map(b => b.dataset.tid));
      document.querySelectorAll('.actuals-txn-row').forEach(r => {
        r.style.display = sugTids.has(r.dataset.tid) ? '' : 'none';
      });
      // Suggested rows live in the (collapsed-by-default) Linked section, so
      // EXPAND every section body during review or they'd stay hidden inside
      // a collapsed section. (User 2026-06-03.)
      const _wrap = document.getElementById('actuals-sections');
      if (_wrap) {
        _wrap.querySelectorAll(':scope > div > div:nth-child(2)').forEach(b => { b.style.display = 'flex'; });
        _wrap.querySelectorAll('.sec-chev').forEach(c => { c.textContent = '▾'; });
      }
      ['actuals-filter-bar', 'actuals-bulk-bar'].forEach(id => { const el = document.getElementById(id); if (el) el.style.display = 'none'; });
      document.getElementById('actuals-review-bar').style.display = 'flex';
      const hc = banners.filter(b => parseFloat(b.dataset.confidence || 0) >= 0.9).length;
      document.getElementById('actuals-review-count').textContent = banners.length;
      document.getElementById('actuals-review-hc').textContent = hc;
      _initReviewThumbs();
      window.scrollTo({ top: 0, behavior: 'smooth' });
    };

    window.actualsExitReview = function () {
      // Restore the normal view in place — no full reload (was slow). Clear the
      // review flag, hide the review bar, bring the filter/bulk bars back, then
      // re-show rows per the active filter and re-bucket sections to default.
      // (User 2026-06-03.)
      window._actualsInReview = false;
      const rb = document.getElementById('actuals-review-bar'); if (rb) rb.style.display = 'none';
      const fb = document.getElementById('actuals-filter-bar'); if (fb) fb.style.display = '';
      // Bulk bar only returns if rows are actually selected — restoring it
      // unconditionally showed an empty "0 selected" bar. (Review 2026-06-04.)
      const bb = document.getElementById('actuals-bulk-bar');
      if (bb) bb.style.display = document.querySelector('.actuals-txn-row[data-selected="1"]') ? 'flex' : 'none';
      if (typeof actualsApplyFilter === 'function') actualsApplyFilter(_actualsActiveFilter || 'all');
      if (typeof window._actualsSectionize === 'function') window._actualsSectionize();
    };

    async function _actualsBulkMatch(url, payload, verb) {
      const bar = document.getElementById('actuals-review-bar');
      bar.style.opacity = '.6'; bar.style.pointerEvents = 'none';
      try {
        const r = await fetch('/projects/' + PROJ_ID + url, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload || {}) });
        const d = await r.json();
        if (!r.ok) { if (typeof _actualsToast === 'function') _actualsToast(verb + ' failed: ' + (d.error || r.status), 'red'); bar.style.opacity = ''; bar.style.pointerEvents = ''; return; }
        const k = (verb === 'Confirm') ? d.confirmed : d.dismissed;
        if (typeof _actualsToast === 'function') _actualsToast(verb + 'ed ' + (k || 0) + (d.failed ? (' · ' + d.failed + ' failed') : ''), 'green');
        setTimeout(() => location.reload(), 700);
      } catch (e) { if (typeof _actualsToast === 'function') _actualsToast('Error: ' + e.message, 'red'); bar.style.opacity = ''; bar.style.pointerEvents = ''; }
    }

    window.actualsBulkConfirm = function (minConf) {
      const banners = document.querySelectorAll('.actuals-suggested-banner');
      const n = minConf ? [...banners].filter(b => parseFloat(b.dataset.confidence || 0) >= minConf).length : banners.length;
      if (!n) { if (typeof _actualsToast === 'function') _actualsToast('Nothing to confirm', 'red'); return; }
      const lbl = minConf ? ('the ' + n + ' high-confidence') : ('all ' + n);
      if (!confirm('Confirm ' + lbl + ' match' + (n !== 1 ? 'es' : '') + '? Each links its receipt to the bank charge (the receipt’s placeholder row is merged in).')) return;
      _actualsBulkMatch('/actuals/matches/confirm-bulk', { min_confidence: minConf || 0 }, 'Confirm');
    };

    window.actualsBulkDismiss = function () {
      const n = document.querySelectorAll('.actuals-suggested-banner').length;
      if (!n) return;
      if (!confirm('Dismiss all ' + n + ' suggested matches? They’ll be unlinked (re-run Auto-Match anytime).')) return;
      _actualsBulkMatch('/actuals/matches/dismiss-bulk', {}, 'Dismiss');
    };

    // ── Per-row "Find receipt" picker (manual match without dragging) ──
    let _findRcptTid = null, _findRcptCands = [];
    const _frEsc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    const _frFmt = n => n == null ? '—' : ('$' + Number(n).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}));
    window.actualsFindReceipt = async function (tid) {
      _findRcptTid = tid;
      const ov = document.getElementById('findRcptOverlay');
      const list = document.getElementById('findRcptList');
      document.getElementById('findRcptSearch').value = '';
      list.innerHTML = '<div class="muted" style="padding:12px;font-size:.85rem">Loading…</div>';
      ov.style.display = 'flex';
      try {
        const r = await fetch('/projects/' + PROJ_ID + '/actuals/transaction/' + tid + '/receipt-candidates');
        const d = await r.json();
        if (!r.ok) { list.innerHTML = '<div style="padding:12px;color:#e08080">' + (d.error || 'Failed') + '</div>'; return; }
        document.getElementById('findRcptTxn').innerHTML =
          'Charge: <strong style="color:var(--text)">' + _frEsc(d.txn.vendor || '(no vendor)') + '</strong> · '
          + _frFmt(d.txn.amount) + ' · ' + _frEsc(d.txn.date || '') + ' &nbsp;·&nbsp; ' + d.total_unlinked + ' unlinked receipts';
        _findRcptCands = d.candidates || [];
        _renderFindRcpt();
      } catch (e) { list.innerHTML = '<div style="padding:12px;color:#e08080">Error: ' + e.message + '</div>'; }
    };
    function _renderFindRcpt() {
      const list = document.getElementById('findRcptList');
      const q = (document.getElementById('findRcptSearch').value || '').toLowerCase();
      const rows = _findRcptCands.filter(c => !q || (c.vendor + ' ' + c.filename).toLowerCase().includes(q));
      if (!rows.length) { list.innerHTML = '<div class="muted" style="padding:12px;font-size:.85rem">No matching receipts.</div>'; return; }
      list.innerHTML = rows.map(c => {
        const badges = [];
        if (c.only_exact) badges.push('<span style="color:#f0c060">⭐ only $ match</span>');
        if (c.amt_exact) badges.push('<span style="color:#74c69d">✓ exact $</span>');
        else if (c.close) badges.push('<span style="color:#e0a040">~ close $</span>');
        else if (c.cents_match) badges.push('<span style="color:#9aa4b2">¢ cents match</span>');
        if (c.day_gap != null) badges.push('<span style="color:#bba070">' + c.day_gap + 'd apart</span>');
        if (c.vendor_score >= 0.45) badges.push('<span style="color:#7ba6e8">vendor ' + Math.round(c.vendor_score * 100) + '%</span>');
        const thumb = c.is_image
          ? '<img src="/docs/upload/' + c.doc_id + '/raw" loading="lazy" style="width:40px;height:40px;object-fit:cover;border-radius:4px;flex-shrink:0">'
          : '<span style="width:40px;height:40px;display:flex;align-items:center;justify-content:center;background:#1a1a1a;border-radius:4px;flex-shrink:0">📄</span>';
        return '<div onclick="_actualsLinkReceipt(' + c.doc_id + ')" '
          + 'style="display:flex;align-items:center;gap:10px;padding:7px 9px;border:1px solid var(--border);border-radius:6px;cursor:pointer;background:var(--bg-input)" '
          + 'onmouseover="this.style.borderColor=\'#2a5a3a\'" onmouseout="this.style.borderColor=\'var(--border)\'">'
          + thumb
          + '<div style="flex:1;min-width:0"><div style="font-size:.82rem;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + _frEsc(c.vendor || '(no vendor)') + '</div>'
          + '<div style="font-size:.7rem;color:var(--text-muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + _frEsc(c.filename) + '</div></div>'
          + '<div style="text-align:right;flex-shrink:0"><div style="font-size:.82rem;font-weight:600">' + _frFmt(c.amount) + '</div>'
          + '<div style="font-size:.66rem;color:var(--text-muted)">' + _frEsc(c.doc_date || '') + '</div></div>'
          + '<div style="font-size:.62rem;text-align:right;flex-shrink:0;min-width:88px;line-height:1.5">' + badges.join('<br>') + '</div></div>';
      }).join('');
    }
    window.actualsFindReceiptFilter = _renderFindRcpt;
    window.actualsCloseFindReceipt = function () { document.getElementById('findRcptOverlay').style.display = 'none'; _findRcptTid = null; };
    window._actualsLinkReceipt = async function (docId) {
      if (!_findRcptTid) return;
      try {
        const r = await fetch('/projects/' + PROJ_ID + '/actuals/transaction/' + _findRcptTid + '/link-doc', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ doc_upload_id: docId }) });
        const d = await r.json();
        if (!r.ok) { if (typeof _actualsToast === 'function') _actualsToast('Link failed: ' + (d.error || r.status), 'red'); return; }
        if (typeof _actualsToast === 'function') _actualsToast('Linked receipt ↔ charge', 'green');
        actualsCloseFindReceipt();
        setTimeout(() => location.reload(), 600);
      } catch (e) { if (typeof _actualsToast === 'function') _actualsToast('Error: ' + e.message, 'red'); }
    };

    // ── Unmatch a receipt + Undo last match (AJAX, no reload) ──
    function _actualsRowToUnlinked(tid) {
      const row = document.querySelector('.actuals-txn-row[data-tid="' + tid + '"]');
      if (!row) return;
      row.dataset.hasDoc = '0';
      row.dataset.matchStatus = 'unmatched';
      // Remove the filled doc badge; the Document cell's dashed empty
      // placeholder (Find / Add) auto-reveals via CSS now that
      // data-has-doc="0", so no button needs to be re-created here.
      row.querySelectorAll('.actuals-doc-badge').forEach(e => e.remove());
      const banner = document.querySelector('.actuals-suggested-banner[data-tid="' + tid + '"]');
      if (banner) banner.remove();
      if (typeof _actualsRecountStats === 'function') _actualsRecountStats();
      if (typeof _actualsRefreshReviewBtn === 'function') _actualsRefreshReviewBtn();
      if (typeof window._actualsSectionize === 'function') window._actualsSectionize();  // re-bucket
    }
    window.actualsUnmatch = async function (tid) {
      if (!confirm('Unlink this receipt from the charge? The receipt returns to the pool; the charge stays.')) return;
      try {
        const r = await fetch('/projects/' + PROJ_ID + '/actuals/transaction/' + tid + '/unmatch',
          { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' });
        const d = await r.json();
        if (!r.ok) { if (typeof _actualsToast === 'function') _actualsToast('Unmatch failed: ' + (d.error || r.status), 'red'); return; }
        _actualsRowToUnlinked(tid);
        if (typeof _actualsToast === 'function') _actualsToast('Unlinked' + (d.restored_receipt ? ' · receipt back in pool' : ''), 'green');
      } catch (e) { if (typeof _actualsToast === 'function') _actualsToast('Error: ' + e.message, 'red'); }
    };
    window.actualsAutoCode = async function () {
      try {
        const dry = await (await fetch('/projects/' + PROJ_ID + '/actuals/code-suggested-bulk',
          { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' })).json();
        const n = dry.would_code || 0;
        if (!n) { if (typeof _actualsToast === 'function') _actualsToast('No high-confidence line suggestions to apply', 'yellow'); return; }
        if (!confirm('Auto-code ' + n + ' uncoded charge' + (n !== 1 ? 's' : '') + ' to their suggested budget lines? (high-confidence only — you can re-code any individually.)')) return;
        const d = await (await fetch('/projects/' + PROJ_ID + '/actuals/code-suggested-bulk?apply=1',
          { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' })).json();
        if (typeof _actualsToast === 'function') _actualsToast('Coded ' + (d.coded || 0) + (d.failed ? (' · ' + d.failed + ' failed') : '') + '. Reloading…', 'green');
        setTimeout(() => location.reload(), 900);
      } catch (e) { if (typeof _actualsToast === 'function') _actualsToast('Auto-code error: ' + e.message, 'red'); }
    };

    window.actualsUndoLastMatch = async function () {
      try {
        const r = await fetch('/projects/' + PROJ_ID + '/actuals/undo-last-match',
          { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' });
        const d = await r.json();
        if (!r.ok) { if (typeof _actualsToast === 'function') _actualsToast('Undo failed: ' + (d.error || r.status), 'red'); return; }
        if (!d.undone) { if (typeof _actualsToast === 'function') _actualsToast(d.message || 'No recent match to undo', 'yellow'); return; }
        if (d.transaction_id) _actualsRowToUnlinked(d.transaction_id);
        if (typeof _actualsToast === 'function') _actualsToast('Undid match: ' + (d.vendor || 'charge') + (d.amount != null ? (' · $' + Number(d.amount).toFixed(2)) : ''), 'green');
      } catch (e) { if (typeof _actualsToast === 'function') _actualsToast('Error: ' + e.message, 'red'); }
    };

    // ── Grouped 3-section Match view (Needs receipt / Receipts to place / Linked) ──
    window._actualsSectionize = function () {
      const list = document.getElementById('actuals-txn-list');
      if (!list) return;
      let wrap = document.getElementById('actuals-sections');
      if (!wrap) {
        wrap = document.createElement('div');
        wrap.id = 'actuals-sections';
        list.parentNode.insertBefore(wrap, list);
      }
      // Idempotency: a prior run moved every row OUT of the flat list into
      // these section bodies. Re-bucket callers (unlink / dismiss / code)
      // invoke us again — so first reclaim those rows back into the flat
      // list, otherwise we'd re-read an empty list and wipe everything.
      wrap.querySelectorAll('.actuals-txn-row, .actuals-suggested-banner')
          .forEach(n => list.appendChild(n));
      wrap.innerHTML = '';
      const defs = [
        { key: 'needs',    title: '⚡ Needs receipt',     color: '#e0a040', desc: 'charges with no proof yet', collapsed: false },
        { key: 'receipts', title: '🧾 Receipts — waiting (not counted yet)', color: '#7ba6e8', desc: 'each needs one of: 🔗 match to a charge · 📎 backup for an invoice · ⚡ activate as its own charge (cash / reimbursement) by picking a budget line', collapsed: false },
        { key: 'linked',   title: '🔗 Linked',            color: '#74c69d', desc: 'matched pairs (suggested + confirmed)', collapsed: true },
      ];
      const bodies = {};
      defs.forEach(d => {
        const sec = document.createElement('div'); sec.style.cssText = 'margin-bottom:14px';
        const hdr = document.createElement('div');
        hdr.style.cssText = 'display:flex;align-items:center;gap:8px;padding:9px 12px;cursor:pointer;background:var(--bg-card);border:1px solid var(--border);border-left:3px solid ' + d.color + ';border-radius:8px 8px 0 0';
        hdr.innerHTML = '<span style="font-weight:600;font-size:.88rem">' + d.title + '</span>'
          + '<span class="sec-count" id="ax-seccount-' + d.key + '" style="font-size:.78rem;color:' + d.color + ';font-weight:700"></span>'
          + '<span class="muted" style="font-size:.72rem;margin-left:6px">— ' + d.desc + '</span>'
          + '<span class="sec-chev" style="margin-left:auto;color:var(--text-muted);font-size:.8rem">' + (d.collapsed ? '▸' : '▾') + '</span>';
        const body = document.createElement('div');
        body.id = 'ax-secbody-' + d.key;
        body.style.cssText = 'border:1px solid var(--border);border-top:none;border-radius:0 0 8px 8px;padding:8px;display:flex;flex-direction:column;gap:0' + (d.collapsed ? ';display:none' : '');
        hdr.addEventListener('click', () => {
          const hidden = body.style.display === 'none';
          body.style.display = hidden ? 'flex' : 'none';
          hdr.querySelector('.sec-chev').textContent = hidden ? '▾' : '▸';
        });
        sec.appendChild(hdr); sec.appendChild(body); wrap.appendChild(sec);
        bodies[d.key] = body;
      });
      // Perf: index banners once (avoids a querySelector per row) and build
      // each section into a DocumentFragment off-DOM, then append once — turns
      // ~2,000 live-DOM moves/reflows into 3. (User 2026-06-03.)
      const counts = { needs: 0, receipts: 0, linked: 0 };
      const frags = { needs: document.createDocumentFragment(),
                      receipts: document.createDocumentFragment(),
                      linked: document.createDocumentFragment() };
      const bannerByTid = {};
      list.querySelectorAll('.actuals-suggested-banner').forEach(b => { bannerByTid[b.dataset.tid] = b; });
      Array.from(list.children).forEach(node => {
        if (!node.classList || !node.classList.contains('actuals-txn-row')) return;
        if (node.dataset.claimedElsewhere === '1') return;                 // leave hidden admin rows out
        const src = node.dataset.source, hasDoc = node.dataset.hasDoc;
        const key = (src === 'doc_upload') ? 'receipts' : (hasDoc === '1' ? 'linked' : 'needs');
        const banner = bannerByTid[node.dataset.tid];
        if (banner) frags[key].appendChild(banner);
        frags[key].appendChild(node);
        counts[key]++;
      });
      ['needs', 'receipts', 'linked'].forEach(k => bodies[k].appendChild(frags[k]));
      ['needs', 'receipts', 'linked'].forEach((k, i) => {
        const el = wrap.querySelectorAll('.sec-count')[i];
        if (el) el.textContent = ' (' + counts[k] + ')';
      });
      list.style.display = 'none';   // original flat list now empty
    };

    // ── Side-by-side Match Review (confirm/dismiss in place, no reload) ──
    const _mrEsc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    const _mrFmt = n => (n == null || isNaN(n)) ? '—' : ('$' + Math.abs(Number(n)).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}));
    let _mrQueue = [], _mrIdx = 0;
    // Receipt preview zoom: 'fit' (whole page/image) or a percentage.
    let _mrZoom = 'fit', _mrCur = null;
    const _MR_ZOOMS = [50, 75, 100, 150, 200, 300];
    function _mrRenderPreview() {
      const wrap = document.getElementById('mrPreviewWrap');
      if (!wrap || !_mrCur) return;
      const { src, isPdf } = _mrCur;
      if (isPdf) {
        // Chrome PDF viewer params: page-fit shows the WHOLE page (fixes the
        // page-width over-zoom on photo-in-PDF receipts); a number = zoom %.
        const z = (_mrZoom === 'fit') ? 'page-fit' : _mrZoom;
        wrap.style.alignItems = 'stretch';
        wrap.innerHTML = '<iframe src="' + src + '#toolbar=0&navpanes=0&zoom=' + z
          + '" style="width:100%;height:100%;border:none;background:#fff"></iframe>';
      } else if (_mrZoom === 'fit') {
        wrap.style.alignItems = 'center';
        wrap.innerHTML = '<img src="' + src + '" style="max-width:100%;max-height:100%;object-fit:contain">';
      } else {
        wrap.style.alignItems = 'flex-start';
        wrap.innerHTML = '<img src="' + src + '" style="width:' + _mrZoom + '%;height:auto;display:block;margin:auto">';
      }
      const lbl = document.getElementById('mrZoomLbl');
      if (lbl) lbl.textContent = (_mrZoom === 'fit') ? 'Fit' : (_mrZoom + '%');
    }
    window._mrZoomSet = function (z) {
      if (z === 'fit') {
        _mrZoom = 'fit';
      } else if (z === 'in' || z === 'out') {
        const cur = (_mrZoom === 'fit') ? 100 : _mrZoom;
        let idx = _MR_ZOOMS.indexOf(cur);
        if (idx < 0) idx = _MR_ZOOMS.reduce((bi, v, i) => Math.abs(v - cur) < Math.abs(_MR_ZOOMS[bi] - cur) ? i : bi, 0);
        idx = Math.max(0, Math.min(_MR_ZOOMS.length - 1, idx + (z === 'in' ? 1 : -1)));
        _mrZoom = _MR_ZOOMS[idx];
      } else {
        _mrZoom = z;
      }
      _mrRenderPreview();
    };
    function _mrSyncCounts() {
      const banners = document.querySelectorAll('.actuals-suggested-banner');
      if (typeof _actualsRefreshReviewBtn === 'function') _actualsRefreshReviewBtn();
      const rc = document.getElementById('actuals-review-count'); if (rc) rc.textContent = banners.length;
      const hc = document.getElementById('actuals-review-hc');
      if (hc) hc.textContent = [...banners].filter(b => parseFloat(b.dataset.confidence || 0) >= 0.9).length;
    }
    window.actualsReviewOneByOne = function (startTid) {
      _mrQueue = [...document.querySelectorAll('.actuals-suggested-banner')].map(b => ({
        tid: b.dataset.tid, docId: b.dataset.docId, conf: parseFloat(b.dataset.confidence || 0), banner: b }));
      if (!_mrQueue.length) { if (typeof _actualsToast === 'function') _actualsToast('No suggested matches to review', 'red'); return; }
      _mrIdx = 0;
      if (startTid) { const i = _mrQueue.findIndex(q => String(q.tid) === String(startTid)); if (i >= 0) _mrIdx = i; }
      document.getElementById('matchReviewOverlay').style.display = 'flex';
      _mrRender();
    };
    async function _mrRender() {
      if (!_mrQueue.length) { actualsMRClose(); return; }
      if (_mrIdx < 0) _mrIdx = 0;
      if (_mrIdx >= _mrQueue.length) _mrIdx = _mrQueue.length - 1;
      const q = _mrQueue[_mrIdx];
      document.getElementById('mrProgress').textContent = 'Match ' + (_mrIdx + 1) + ' of ' + _mrQueue.length;
      const confEl = document.getElementById('mrConf'), hc = q.conf >= 0.9;
      confEl.textContent = Math.round(q.conf * 100) + '%';
      confEl.style.cssText = 'font-size:.74rem;padding:2px 8px;border-radius:10px;background:' + (hc ? '#13251a' : '#221d0c') + ';color:' + (hc ? '#74c69d' : '#e0a040') + ';border:1px solid ' + (hc ? '#2a5a3a' : '#4a3a1a');
      const row = document.querySelector('.actuals-txn-row[data-tid="' + q.tid + '"]');
      const tVendor = (row && row.querySelector('.actuals-txn-vendor')?.textContent.trim()) || '(no vendor)';
      const tAmt = parseFloat(row ? row.dataset.amount : NaN);
      const tDate = row ? row.dataset.txnDate : '';
      const tCard = row ? row.dataset.sortCard4 : '';
      const tNote = row ? row.dataset.note : '';
      document.getElementById('mrTxn').innerHTML =
        '<div style="font-size:.7rem;text-transform:uppercase;letter-spacing:.06em;color:#7ba6e8;margin-bottom:8px">🏦 Bank charge</div>'
        + '<div style="font-size:1.05rem;font-weight:700;margin-bottom:6px;word-break:break-word">' + _mrEsc(tVendor) + '</div>'
        + '<div style="font-size:1.7rem;font-weight:700;font-variant-numeric:tabular-nums;margin-bottom:10px">' + _mrFmt(tAmt) + '</div>'
        + '<div style="font-size:.82rem;line-height:1.9;color:var(--text-muted)">'
        + '<div>Date: <span style="color:var(--text)">' + _mrEsc(tDate || '—') + '</span></div>'
        + (tCard ? '<div>Card: <span style="color:var(--text)">••' + _mrEsc(tCard) + '</span></div>' : '')
        + (tNote ? '<div>Note: <span style="color:var(--text)">' + _mrEsc(tNote) + '</span></div>' : '')
        + '</div>';
      const docEl = document.getElementById('mrDoc');
      docEl.innerHTML = '<div style="font-size:.7rem;text-transform:uppercase;letter-spacing:.06em;color:#f0c060">🧾 Receipt</div><div class="muted" style="padding:20px">Loading…</div>';
      document.getElementById('mrAmtMatch').textContent = '';
      try {
        const meta = await (await fetch('/docs/upload/' + q.docId + '/meta')).json();
        const src = '/docs/upload/' + q.docId + '/raw';
        _mrCur = { src: src, isPdf: !!meta.is_pdf };
        _mrZoom = 'fit';
        docEl.innerHTML =
          '<div style="display:flex;align-items:center;gap:8px;flex-shrink:0">'
          + '<span style="font-size:.7rem;text-transform:uppercase;letter-spacing:.06em;color:#f0c060">🧾 Receipt</span>'
          + '<span style="margin-left:auto;display:inline-flex;align-items:center;gap:4px">'
          +   '<button type="button" class="btn btn-xs" style="padding:2px 8px;font-size:.7rem" onclick="_mrZoomSet(\'fit\')">Fit</button>'
          +   '<button type="button" class="btn btn-xs" style="padding:2px 9px;font-size:.85rem" onclick="_mrZoomSet(\'out\')" title="Zoom out (−)">−</button>'
          +   '<span id="mrZoomLbl" style="font-size:.7rem;color:var(--text-muted);min-width:40px;text-align:center">Fit</span>'
          +   '<button type="button" class="btn btn-xs" style="padding:2px 9px;font-size:.85rem" onclick="_mrZoomSet(\'in\')" title="Zoom in (+)">+</button>'
          + '</span></div>'
          + '<div id="mrPreviewWrap" style="flex:1;min-height:0;overflow:auto;display:flex;align-items:center;justify-content:center;background:#0a0d12;border:1px solid var(--border);border-radius:6px"></div>'
          + '<div style="font-size:.82rem;line-height:1.6;flex-shrink:0"><div style="font-weight:600">' + _mrEsc(meta.vendor || meta.filename || 'Receipt') + '</div>'
          + '<div style="color:var(--text-muted)">Amount: <span style="color:var(--text);font-weight:600">' + _mrFmt(meta.amount) + '</span> · Date: <span style="color:var(--text)">' + _mrEsc(meta.doc_date || '—') + '</span></div>'
          + '<div style="color:var(--text-muted);font-size:.72rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="' + _mrEsc(meta.filename) + '">📎 ' + _mrEsc(meta.filename) + '</div></div>'
          + '<a href="' + src + '" target="_blank" rel="noopener" style="font-size:.72rem;color:#8fb4ff;text-decoration:none;flex-shrink:0">↗ Open full receipt</a>';
        _mrRenderPreview();
        if (meta.amount != null && !isNaN(tAmt)) {
          const same = Math.abs(Math.abs(meta.amount) - Math.abs(tAmt)) <= 0.01;
          document.getElementById('mrAmtMatch').innerHTML = same
            ? '<span style="color:#74c69d">✓ amounts match</span>'
            : '<span style="color:#e08080">⚠ differ: ' + _mrFmt(tAmt) + ' vs ' + _mrFmt(meta.amount) + '</span>';
        }
      } catch (e) { docEl.innerHTML += '<div style="color:#e08080;padding:10px">Could not load receipt.</div>'; }
    }
    window.actualsMRStep = function (dir) { _mrIdx += dir; _mrRender(); };
    window.actualsMRClose = function () { document.getElementById('matchReviewOverlay').style.display = 'none'; };
    async function _mrAct(kind) {
      const q = _mrQueue[_mrIdx]; if (!q) return;
      const url = (kind === 'confirm')
        ? '/actuals/transaction/' + q.tid + '/confirm-match'
        : '/actuals/transaction/' + q.tid + '/dismiss-suggestion';
      try {
        const r = await fetch('/projects/' + PROJ_ID + url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' });
        const d = await r.json();
        if (!r.ok) { if (typeof _actualsToast === 'function') _actualsToast((kind === 'confirm' ? 'Confirm' : 'Dismiss') + ' failed: ' + (d.error || r.status), 'red'); return; }
        if (q.banner && q.banner.remove) q.banner.remove();   // drop from list, no reload
        _mrQueue.splice(_mrIdx, 1);
        _mrSyncCounts();
        if (!_mrQueue.length) { if (typeof _actualsToast === 'function') _actualsToast('All caught up 🎉', 'green'); actualsMRClose(); return; }
        if (_mrIdx >= _mrQueue.length) _mrIdx = _mrQueue.length - 1;
        _mrRender();
      } catch (e) { if (typeof _actualsToast === 'function') _actualsToast('Error: ' + e.message, 'red'); }
    }
    window.actualsMRConfirm = function () { _mrAct('confirm'); };
    window.actualsMRDismiss = function () { _mrAct('dismiss'); };
    document.addEventListener('keydown', e => {
      const ov = document.getElementById('matchReviewOverlay');
      if (!ov || ov.style.display === 'none') return;
      if (e.key === 'Escape') actualsMRClose();
      else if (e.key === 'ArrowRight') actualsMRStep(1);
      else if (e.key === 'ArrowLeft') actualsMRStep(-1);
      else if (e.key === 'c' || e.key === 'C') actualsMRConfirm();
      else if (e.key === 'x' || e.key === 'X') actualsMRDismiss();
      else if (e.key === '+' || e.key === '=') { _mrZoomSet('in'); e.preventDefault(); }
      else if (e.key === '-' || e.key === '_') { _mrZoomSet('out'); e.preventDefault(); }
      else if (e.key === '0' || e.key === 'f' || e.key === 'F') { _mrZoomSet('fit'); }
    });

    // ── Reconcile sub-tab: side-by-side unmatched txns ↔ receipts ──
    // Drag a receipt onto a transaction to fire /link-doc and pair them.
    // Smart-match highlight: when a receipt's amount matches a txn's
    // amount and the doc_date is within 7 days of the txn_date, both
    // cards get a subtle green outline.
    (function reconcileIIFE() {
      let _recState = { txns: [], docs: [] };
      // Defensive: coerce to string so numbers / null / undefined / dates
      // don't blow up .replace. Reconcile receipts include numeric ids
      // and amount fields that flow through here when rendering search
      // hits on filename containing the amount.
      const _esc = s => String(s == null ? '' : s).replace(/[<>&"]/g, c => ({'<':'&lt;','>':'&gt;','&':'&amp;','"':'&quot;'}[c]));
      const _fmt$ = a => (a == null) ? '—' : ('$' + Math.abs(a).toFixed(2) + (a < 0 ? '' : ''));
      const _fmtDate = iso => {
        if (!iso) return '—';
        const d = new Date(iso + 'T12:00:00');
        return isNaN(d) ? iso : d.toLocaleDateString('en-US', {month:'short',day:'numeric',year:'2-digit'});
      };
      // Two cards "match" if amounts equal (within 1 cent) AND dates
      // are within 7 days (or one date is missing, accept on amount).
      function _isMatch(t, d) {
        if (t.amount == null || d.amount == null) return false;
        if (Math.abs(Math.abs(t.amount) - Math.abs(d.amount)) > 0.01) return false;
        if (!t.txn_date || !d.doc_date) return true;
        const dt = (new Date(t.txn_date) - new Date(d.doc_date)) / 86400000;
        return Math.abs(dt) <= 7;
      }
      function _txnSuggests(t) { return _recState.docs.filter(d => _isMatch(t, d)); }
      function _docSuggests(d) { return _recState.txns.filter(t => _isMatch(t, d)); }

      function _renderTxn(t) {
        const suggestCount = _txnSuggests(t).length;
        const matchGlow = suggestCount > 0 ? 'box-shadow:0 0 0 1px #22c55e44' : '';
        const coa = t.account_code
          ? `<span style="font-size:10px;color:#a78bfa;background:rgba(168,85,247,.1);padding:1px 6px;border-radius:8px">${_esc(t.account_code)} ${_esc(t.account_code_name)}</span>`
          : '';
        const sourceBadge = t.source === 'qbo'
          ? '<span style="font-size:9px;color:#5b8af9;text-transform:uppercase;letter-spacing:.05em">QBO</span>'
          : '';
        return `<div class="reconcile-txn-card" draggable="true"
                     data-txn-id="${t.id}" data-amount="${t.amount||0}" data-date="${_esc(t.txn_date||'')}"
                     style="padding:8px 10px;background:var(--bg-input);border:1px solid var(--border);
                            border-radius:6px;cursor:grab;${matchGlow}"
                     title="Drag onto a receipt — or drop a receipt onto me — to pair">
          <div style="display:flex;align-items:baseline;gap:8px">
            <span style="font-weight:600;font-size:13px;color:var(--text)">${_esc(t.vendor || 'No vendor')}</span>
            <span style="margin-left:auto;font-weight:700;font-variant-numeric:tabular-nums;color:#fb7185">${_fmt$(t.amount)}</span>
          </div>
          <div style="display:flex;gap:8px;margin-top:3px;font-size:11px;color:var(--text-muted)">
            <span>${_fmtDate(t.txn_date)}</span>
            ${sourceBadge}
            ${coa}
            ${suggestCount > 0 ? `<span style="margin-left:auto;color:#22c55e">↔ ${suggestCount} match${suggestCount>1?'es':''}</span>` : ''}
          </div>
          ${t.note ? `<div style="font-size:10.5px;color:var(--text-muted);margin-top:2px;font-style:italic">${_esc(t.note)}</div>` : ''}
        </div>`;
      }
      function _renderDoc(d) {
        const suggestCount = _docSuggests(d).length;
        const matchGlow = suggestCount > 0 ? 'box-shadow:0 0 0 1px #22c55e44' : '';
        const cat = d.veryfi_category
          ? `<span style="font-size:10px;color:#c084fc;background:rgba(168,85,247,.1);padding:1px 6px;border-radius:8px">💡 ${_esc(d.veryfi_category)}</span>`
          : '';
        return `<div class="reconcile-doc-card" draggable="true"
                     data-doc-id="${d.id}" data-amount="${d.amount||0}" data-date="${_esc(d.doc_date||'')}"
                     style="padding:8px 10px;background:var(--bg-input);border:1px solid var(--border);
                            border-radius:6px;cursor:grab;${matchGlow}"
                     title="Drag onto a transaction to link. Click to preview.">
          <div style="display:flex;align-items:baseline;gap:8px">
            <span style="font-weight:600;font-size:13px;color:var(--text)">📄 ${_esc(d.vendor || d.filename || 'Receipt')}</span>
            <span style="margin-left:auto;font-weight:700;font-variant-numeric:tabular-nums;color:var(--text)">${_fmt$(d.amount)}</span>
          </div>
          <div style="display:flex;gap:8px;margin-top:3px;font-size:11px;color:var(--text-muted)">
            <span>${_fmtDate(d.doc_date)}</span>
            ${cat}
            ${suggestCount > 0 ? `<span style="margin-left:auto;color:#22c55e">↔ ${suggestCount} match${suggestCount>1?'es':''}</span>` : ''}
          </div>
          <div style="font-size:10.5px;color:var(--text-muted);margin-top:2px;
                      white-space:nowrap;overflow:hidden;text-overflow:ellipsis"
               title="${_esc(d.filename)}">📎 ${_esc(d.filename)}</div>
        </div>`;
      }
      function _applyFilters() {
        const txnQ = (document.getElementById('reconcile-txn-search')?.value || '').toLowerCase();
        const docQ = (document.getElementById('reconcile-doc-search')?.value || '').toLowerCase();
        const suggestOnly = document.getElementById('reconcile-suggest-only')?.checked;
        let txns = _recState.txns.slice();
        let docs = _recState.docs.slice();
        if (txnQ) txns = txns.filter(t =>
          (t.vendor||'').toLowerCase().includes(txnQ) ||
          (t.note||'').toLowerCase().includes(txnQ) ||
          String(t.amount||'').includes(txnQ));
        if (docQ) docs = docs.filter(d =>
          (d.vendor||'').toLowerCase().includes(docQ) ||
          (d.filename||'').toLowerCase().includes(docQ) ||
          (d.veryfi_category||'').toLowerCase().includes(docQ) ||
          String(d.amount||'').includes(docQ));
        if (suggestOnly) {
          txns = txns.filter(t => _txnSuggests(t).length > 0);
          docs = docs.filter(d => _docSuggests(d).length > 0);
        }
        const txnCol = document.getElementById('reconcile-txn-col');
        const docCol = document.getElementById('reconcile-doc-col');
        if (txnCol) txnCol.innerHTML = txns.length
          ? txns.map(_renderTxn).join('')
          : '<div style="color:var(--text-muted);text-align:center;padding:1rem;font-style:italic;font-size:.82rem">No unmatched transactions.</div>';
        if (docCol) docCol.innerHTML = docs.length
          ? docs.map(_renderDoc).join('')
          : '<div style="color:var(--text-muted);text-align:center;padding:1rem;font-style:italic;font-size:.82rem">No unlinked receipts.</div>';
        const tc = document.getElementById('reconcile-txn-count'); if (tc) tc.textContent = txns.length;
        const dc = document.getElementById('reconcile-doc-count'); if (dc) dc.textContent = docs.length;
        const empty = document.getElementById('reconcile-empty');
        if (empty) empty.style.display = (_recState.txns.length === 0 && _recState.docs.length === 0) ? 'block' : 'none';
        _wireDnD();
      }
      function _wireDnD() {
        const txnCol = document.getElementById('reconcile-txn-col');
        const docCol = document.getElementById('reconcile-doc-col');
        if (!txnCol || !docCol) return;
        // Drag a doc card → drop on a txn card → /link-doc
        // Drag a txn card → drop on a doc card → same operation
        document.querySelectorAll('.reconcile-doc-card, .reconcile-txn-card').forEach(card => {
          card.addEventListener('dragstart', e => {
            const isDoc = card.classList.contains('reconcile-doc-card');
            e.dataTransfer.setData('text/plain', JSON.stringify({
              kind: isDoc ? 'doc' : 'txn',
              id: isDoc ? card.dataset.docId : card.dataset.txnId,
            }));
            card.style.opacity = '.4';
          });
          card.addEventListener('dragend', () => { card.style.opacity = ''; });
          card.addEventListener('dragover', e => {
            // Only accept the opposite kind.
            const tx = e.dataTransfer.types.includes('text/plain');
            if (tx) { e.preventDefault(); card.style.outline = '2px dashed #22c55e'; }
          });
          card.addEventListener('dragleave', () => { card.style.outline = ''; });
          card.addEventListener('drop', async e => {
            e.preventDefault(); card.style.outline = '';
            let payload;
            try { payload = JSON.parse(e.dataTransfer.getData('text/plain')); } catch { return; }
            const isDocCard = card.classList.contains('reconcile-doc-card');
            // Source must be opposite kind.
            if (isDocCard && payload.kind !== 'txn') return;
            if (!isDocCard && payload.kind !== 'doc') return;
            const txnId = isDocCard ? payload.id : card.dataset.txnId;
            const docId = isDocCard ? card.dataset.docId : payload.id;
            try {
              const r = await fetch(`/projects/${PID}/actuals/transaction/${txnId}/link-doc`, {
                method: 'POST', headers: {'Content-Type':'application/json'},
                body: JSON.stringify({ doc_upload_id: parseInt(docId) }),
              });
              const j = await r.json();
              if (!r.ok) { alert(j.error || 'Link failed'); return; }
              if (typeof _actualsToast === 'function') _actualsToast(`Linked: ${j.filed_filename || 'receipt'} ↔ txn #${txnId}`, 'green');
              // Remove both rows from local state (the doc may stay if
              // we ever support multi-link from this view; for now drop
              // it since both belong to the "fully matched" world after
              // this drop).
              _recState.txns = _recState.txns.filter(t => String(t.id) !== String(txnId));
              _recState.docs = _recState.docs.filter(d => String(d.id) !== String(docId));
              _applyFilters();
            } catch (err) {
              alert('Link failed: ' + err.message);
            }
          });
          // Click-to-preview on receipt cards (added per user 2026-05-01)
          if (card.classList.contains('reconcile-doc-card')) {
            card.addEventListener('click', e => {
              // Don't preview during drag.
              if (card.style.opacity === '.4') return;
              const docId = card.dataset.docId;
              if (typeof window.openDocDetail === 'function') {
                window.openDocDetail(null, docId);
              }
            });
          }
          // Click a CHARGE → shortlist its likely receipts (user 2026-06-02).
          if (card.classList.contains('reconcile-txn-card')) {
            card.addEventListener('click', e => {
              if (card.style.opacity === '.4') return;   // mid-drag
              window._recSelectTxn(card.dataset.txnId);
            });
          }
        });
      }

      // ── Click-to-shortlist: pick a charge → rank its likely receipts ──
      let _recSelTxn = null;
      function _vsim(a, b) {
        a = (a || '').toLowerCase().replace(/[^a-z0-9 ]/g, ' ').trim();
        b = (b || '').toLowerCase().replace(/[^a-z0-9 ]/g, ' ').trim();
        if (!a || !b) return 0;
        if (a === b) return 1;
        if (a.includes(b) || b.includes(a)) return 0.85;
        const at = new Set(a.split(/\s+/)), bt = new Set(b.split(/\s+/));
        const inter = [...at].filter(x => bt.has(x)).length;
        const uni = new Set([...at, ...bt]).size;
        return uni ? inter / uni : 0;
      }
      const _cents = x => (x == null) ? null : Math.round((Math.abs(x) - Math.trunc(Math.abs(x))) * 100);
      function _recScore(t, d) {
        const ta = t.amount == null ? null : Math.abs(t.amount);
        const da = d.amount == null ? null : Math.abs(d.amount);
        const exact = ta != null && da != null && Math.abs(da - ta) <= 0.01;
        const close = !exact && ta && da != null && Math.abs(da - ta) <= Math.max(2, ta * 0.03);
        const centsMatch = !exact && ta != null && da != null && _cents(ta) === _cents(da);
        let s = 0;
        if (exact) s += 0.5;
        else if (close) s += 0.28;
        else if (centsMatch) s += 0.18;
        const v = _vsim(t.vendor, d.vendor); s += 0.3 * v;
        let gap = null;
        if (t.txn_date && d.doc_date) { gap = Math.abs((new Date(t.txn_date) - new Date(d.doc_date)) / 86400000); s += 0.2 * Math.max(0, 1 - gap / 14); }
        return { score: s, exact, close, centsMatch, gap: gap == null ? null : Math.round(gap), v };
      }
      window._recSelectTxn = function (txnId) {
        const t = _recState.txns.find(x => String(x.id) === String(txnId));
        if (!t) return;
        _recSelTxn = t;
        document.querySelectorAll('.reconcile-txn-card').forEach(c => {
          c.style.boxShadow = (String(c.dataset.txnId) === String(txnId)) ? 'inset 0 0 0 2px #22c55e' : '';
        });
        const docCol = document.getElementById('reconcile-doc-col');
        if (!docCol) return;
        const _ta = t.amount == null ? null : Math.abs(t.amount);
        const _exactPool = (_ta == null) ? 0 : _recState.docs.filter(d => d.amount != null && Math.abs(Math.abs(d.amount) - _ta) <= 0.01).length;
        const ranked = _recState.docs.map(d => {
          const r = _recScore(t, d);
          r.onlyExact = (r.exact && _exactPool === 1);
          if (r.onlyExact) r.score += 0.25;
          return { d, ...r };
        }).sort((a, b) => b.score - a.score);
        const head = '<div style="display:flex;align-items:center;gap:8px;padding:6px 8px;margin-bottom:6px;'
          + 'background:#13251a;border:1px solid #2a5a3a;border-radius:6px;font-size:.74rem">'
          + '<span style="color:#74c69d">Receipts likely matching <strong>' + _esc(t.vendor || 'this charge') + '</strong> · ' + _fmt$(t.amount) + '</span>'
          + '<button type="button" onclick="_recClearSel()" style="margin-left:auto;font-size:.7rem;padding:2px 8px;background:none;border:1px solid #2a5a3a;border-radius:4px;color:#74c69d;cursor:pointer">✕ Clear</button></div>';
        const cards = ranked.slice(0, 25).map(({ d, score, exact, close, centsMatch, onlyExact, gap, v }) => {
          const badges = [];
          if (onlyExact) badges.push('<span style="color:#f0c060">⭐ only $ match</span>');
          if (exact) badges.push('<span style="color:#74c69d">✓ exact $</span>');
          else if (close) badges.push('<span style="color:#e0a040">~ close $</span>');
          else if (centsMatch) badges.push('<span style="color:#9aa4b2">¢ cents</span>');
          if (gap != null) badges.push('<span style="color:#bba070">' + gap + 'd</span>');
          if (v >= 0.45) badges.push('<span style="color:#7ba6e8">' + Math.round(v * 100) + '%</span>');
          const strong = score >= 0.5;
          const bd = strong ? '#2a5a3a' : 'var(--border)';
          return '<div onclick="_recLinkSel(' + d.id + ')" '
            + 'style="padding:8px 10px;margin-bottom:6px;background:var(--bg-input);border:1px solid ' + bd + ';border-radius:6px;cursor:pointer" '
            + 'onmouseover="this.style.borderColor=\'#22c55e\'" onmouseout="this.style.borderColor=\'' + bd + '\'">'
            + '<div style="display:flex;align-items:baseline;gap:8px"><span style="font-weight:600;font-size:13px">📄 ' + _esc(d.vendor || d.filename || 'Receipt') + '</span>'
            + '<span style="margin-left:auto;font-weight:700;font-variant-numeric:tabular-nums">' + _fmt$(d.amount) + '</span></div>'
            + '<div style="display:flex;gap:8px;margin-top:3px;font-size:11px;color:var(--text-muted)"><span>' + _fmtDate(d.doc_date) + '</span>'
            + '<span style="margin-left:auto">' + badges.join(' · ') + '</span></div>'
            + '<div style="font-size:10.5px;color:var(--text-muted);margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">📎 ' + _esc(d.filename) + '</div></div>';
        }).join('');
        docCol.innerHTML = head + (cards || '<div style="color:var(--text-muted);padding:1rem;font-style:italic;font-size:.82rem">No receipts to suggest.</div>');
        const dc = document.getElementById('reconcile-doc-count'); if (dc) dc.textContent = ranked.length;
      };
      window._recClearSel = function () {
        _recSelTxn = null;
        document.querySelectorAll('.reconcile-txn-card').forEach(c => c.style.boxShadow = '');
        _applyFilters();
      };
      window._recLinkSel = async function (docId) {
        if (!_recSelTxn) return;
        const txnId = _recSelTxn.id, vend = _recSelTxn.vendor || 'charge';
        try {
          const r = await fetch('/projects/' + PID + '/actuals/transaction/' + txnId + '/link-doc', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ doc_upload_id: parseInt(docId) }) });
          const j = await r.json();
          if (!r.ok) { if (typeof _actualsToast === 'function') _actualsToast(j.error || 'Link failed', 'red'); return; }
          if (typeof _actualsToast === 'function') _actualsToast('Linked ↔ ' + vend, 'green');
          _recState.txns = _recState.txns.filter(t => String(t.id) !== String(txnId));
          _recState.docs = _recState.docs.filter(d => String(d.id) !== String(docId));
          _recSelTxn = null;
          _applyFilters();
        } catch (e) { if (typeof _actualsToast === 'function') _actualsToast('Link failed: ' + e.message, 'red'); }
      };
      window._reconcileRender = async function() {
        try {
          const r = await fetch(`/projects/${PID}/actuals/reconcile.json`);
          const j = await r.json();
          if (!r.ok) throw new Error(j.error || 'fetch failed');
          _recState.txns = j.transactions || [];
          _recState.docs = j.receipts || [];
          _applyFilters();
        } catch (e) {
          const txnCol = document.getElementById('reconcile-txn-col');
          if (txnCol) txnCol.innerHTML = `<div style="color:#ef4444;text-align:center;padding:1rem">Failed to load: ${e.message}</div>`;
        }
      };
      ['reconcile-txn-search','reconcile-doc-search','reconcile-suggest-only'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.addEventListener('input', _applyFilters);
        if (el) el.addEventListener('change', _applyFilters);
      });
      const refresh = document.getElementById('reconcile-refresh');
      if (refresh) refresh.addEventListener('click', () => window._reconcileRender());
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

    // Undo-capable toast for smart vendor-rename propagation.
    function _vendorRenameToast(prop, oldVendor, newVendor) {
      const t = document.createElement('div');
      t.style.cssText = `position:fixed;bottom:24px;left:50%;transform:translateX(-50%);
        background:#1a3a2a;color:#74c69d;border:1px solid #2a5a3a;border-radius:6px;
        padding:10px 16px;font-size:.85rem;z-index:9999;box-shadow:0 6px 24px rgba(0,0,0,.5);
        max-width:560px;display:flex;align-items:center;gap:14px`;
      const msg = document.createElement('span');
      msg.textContent = `Saved — also renamed ${prop.count} other item${prop.count===1?'':'s'} ` +
        `to “${newVendor}”.`;
      const undo = document.createElement('button');
      undo.textContent = 'Undo';
      undo.style.cssText = `background:none;border:1px solid #2a5a3a;color:#9fe0b6;
        border-radius:5px;padding:3px 12px;font-size:.8rem;cursor:pointer;flex:none`;
      undo.onclick = async () => {
        undo.disabled = true; undo.textContent = 'Undoing…';
        try {
          const r = await fetch(`/projects/${PROJ_ID}/actuals/vendor-rename/undo`, {
            method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({items: prop.items, canon: prop.canon})});
          const d = await r.json();
          if (r.ok) {
            (prop.items || []).forEach(it => {
              if (it.type !== 'txn') return;
              const r2 = document.querySelector(`.actuals-txn-row[data-tid="${it.id}"]`);
              if (r2) { const v2 = r2.querySelector('.actuals-txn-vendor');
                if (v2) { v2.textContent = it.old || '— vendor unknown —'; v2.title = it.old || ''; } }
            });
            t.remove();
            _actualsToast(`Reverted ${d.reverted} item${d.reverted===1?'':'s'}.`, 'yellow');
          } else { _actualsToast('Undo failed: ' + (d.error || r.status), 'red'); }
        } catch (e) { _actualsToast('Undo error: ' + e.message, 'red'); }
      };
      t.appendChild(msg); t.appendChild(undo);
      document.body.appendChild(t);
      setTimeout(() => t.remove(), 9000);
    }

    // ── Multi-select (Cmd/Shift click) + bulk drag ────────────────────
    const _selectedTids = new Set();
    let _lastClickedTid = null;  // anchor for shift-range

    function _renderSelection() {
      document.querySelectorAll('.actuals-txn-row').forEach(r => {
        const tid = parseInt(r.dataset.tid);
        const selected = _selectedTids.has(tid);
        r.style.boxShadow = selected ? 'inset 0 0 0 2px var(--green)' : '';
        r.dataset.selected = selected ? '1' : '0';
      });
      const bar = document.getElementById('actuals-bulk-bar');
      const cnt = document.getElementById('actuals-bulk-count');
      if (_selectedTids.size > 0) {
        if (bar) bar.style.display = 'flex';
        if (cnt) cnt.textContent = `${_selectedTids.size} selected`;
      } else {
        if (bar) bar.style.display = 'none';
      }
      // Merge button is only valid for exactly 2 selected rows.
      const mb = document.getElementById('actuals-bulk-merge-btn');
      if (mb) {
        const canMerge = (_selectedTids.size === 2);
        mb.disabled = !canMerge;
        mb.style.opacity = canMerge ? '1' : '.45';
        mb.style.cursor  = canMerge ? 'pointer' : 'not-allowed';
      }
    }

    window.actualsBulkClear = function () {
      _selectedTids.clear();
      _lastClickedTid = null;
      _renderSelection();
    };

    // Batch-assign every selected row to one budget line (or section) in a
    // single request — replaces firing one /set-line POST per row. (User 2026-06-03.)
    window.actualsBulkAssignLine = async function () {
      const sel = document.getElementById('actuals-bulk-line');
      const value = sel && sel.value;
      if (!value) { _actualsToast('Pick a budget line first.', 'yellow'); return; }
      if (_selectedTids.size === 0) { _actualsToast('No rows selected.', 'yellow'); return; }
      const tids = Array.from(_selectedTids);
      const payload = value.startsWith('section:')
        ? { transaction_ids: tids, account_code: value.slice(8) }
        : { transaction_ids: tids, budget_line_id: value };
      const lbl = (sel.options[sel.selectedIndex] || {}).text || 'line';
      try {
        const r = await fetch(`/projects/${PROJ_ID}/actuals/transactions/set-line-bulk`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
        const d = await r.json();
        if (!r.ok) { _actualsToast('Assign failed: ' + (d.error || r.status), 'red'); return; }
        // Update each selected row in place — no reload.
        tids.forEach(tid => {
          const row = document.querySelector(`.actuals-txn-row[data-tid="${tid}"]`);
          if (!row) return;
          row.dataset.coded = '1';
          const sb = row.querySelector('.actuals-suggest-btn'); if (sb) sb.remove();
          const p = row.querySelector('.actuals-line-picker');
          if (p) {
            p.dataset.current = value;
            if (p.dataset.populated === '1') { p.value = value; }
            else { p.innerHTML = '<option value="' + value + '" selected>' + (d.line_label || lbl) + '</option>'; }
          }
        });
        _actualsToast(`Assigned ${d.updated || tids.length} to ${d.line_label || lbl}` +
                      (d.failed ? ` · ${d.failed} failed` : ''), 'green');
        actualsBulkClear();
        if (typeof _actualsRecountStats === 'function') _actualsRecountStats();
        if (!window._actualsInReview && typeof actualsApplyFilter === 'function') {
          actualsApplyFilter(_actualsActiveFilter);
        }
      } catch (e) {
        _actualsToast('Assign error: ' + e.message, 'red');
      }
    };

    window.actualsBulkMerge = async function () {
      if (_selectedTids.size !== 2) {
        _actualsToast('Select exactly two rows to merge.', 'yellow');
        return;
      }
      const tids = Array.from(_selectedTids);
      // Pull readable info for the confirm dialog by reading row text.
      const rowInfo = tids.map(tid => {
        const r = document.querySelector(`.actuals-txn-row[data-tid="${tid}"]`);
        if (!r) return `#${tid}`;
        const cells = r.querySelectorAll('td');
        const date   = cells[1] ? (cells[1].textContent || '').trim() : '';
        const vendor = cells[2] ? (cells[2].textContent || '').trim() : '';
        const amount = cells[cells.length - 2]
                          ? (cells[cells.length - 2].textContent || '').trim() : '';
        return `  • #${tid} — ${date} ${vendor} ${amount}`;
      }).join('\n');
      if (!confirm(
        'Merge these two transactions into one?\n\n' +
        rowInfo + '\n\n' +
        'The receipt, budget-line link, and any user-coded fields from the ' +
        'losing row will be transferred onto the surviving row, then the ' +
        'losing row is deleted. The system picks the surviving row by ' +
        'priority (QBO charge first, then user-coded, then oldest). ' +
        'Activity log records the merge.'
      )) return;
      try {
        const r = await fetch(`/projects/${PROJ_ID}/actuals/transaction/merge`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ tids }),
        });
        const j = await r.json();
        if (!r.ok) {
          _actualsToast(j.error || 'Merge failed.', 'red');
          return;
        }
        const tr = (j.transferred || []).join(', ') || 'no changes';
        const warn = j.doc_conflict
          ? ' Both rows had different receipts — only the surviving row\'s receipt was kept.'
          : '';
        _actualsToast(
          `Merged #${j.deleted_id} into #${j.canonical_id} (${tr}).${warn}`,
          j.doc_conflict ? 'yellow' : 'green'
        );
        setTimeout(() => window.location.reload(), 800);
      } catch (e) {
        _actualsToast('Merge failed: ' + e.message, 'red');
      }
    };

    window.actualsBulkMarkNotProject = async function () {
      if (_selectedTids.size === 0) return;
      if (!confirm(`Mark ${_selectedTids.size} transaction${_selectedTids.size !== 1 ? 's' : ''} as not project expense?`)) return;
      const tids = Array.from(_selectedTids);
      let ok = 0, fail = 0;
      for (const tid of tids) {
        try {
          const r = await fetch(`/projects/${PROJ_ID}/actuals/transaction/${tid}/mark-not-project`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}',
          });
          if (r.ok) ok++; else fail++;
        } catch (e) { fail++; }
      }
      _actualsToast(`Marked ${ok} as not project${fail ? ` (${fail} failed)` : ''}.`, fail ? 'yellow' : 'green');
      setTimeout(() => window.location.reload(), 600);
    };

    // Click handler for selection — click-only model per user request:
    //   plain click  → toggle THIS row in/out of the selection
    //   Shift+click  → range from last-clicked tid through here
    //   Cmd/Ctrl+click → toggle (same as plain click)
    // Drag still works normally because mousedown+move triggers
    // dragstart instead of click.
    // Skip clicks on inner inputs / buttons / selects / dropdown.
    document.querySelectorAll('.actuals-txn-row').forEach(row => {
      row.addEventListener('click', e => {
        if (e.target.closest('select, input, textarea, button, a, .actuals-line-picker')) return;
        const tid = parseInt(row.dataset.tid);
        if (e.shiftKey && _lastClickedTid !== null && _lastClickedTid !== tid) {
          // Range select over visible rows.
          const visible = Array.from(document.querySelectorAll('.actuals-txn-row'))
            .filter(r => r.style.display !== 'none')
            .map(r => parseInt(r.dataset.tid));
          const i = visible.indexOf(_lastClickedTid);
          const j = visible.indexOf(tid);
          if (i >= 0 && j >= 0) {
            const [a, b] = i < j ? [i, j] : [j, i];
            for (let k = a; k <= b; k++) _selectedTids.add(visible[k]);
          }
        } else {
          // Plain click + Cmd/Ctrl click both toggle this single row.
          if (_selectedTids.has(tid)) _selectedTids.delete(tid);
          else _selectedTids.add(tid);
        }
        _lastClickedTid = tid;
        _renderSelection();
      });
    });

    // Cmd/Ctrl+A inside the Actuals tab selects all VISIBLE rows.
    document.addEventListener('keydown', e => {
      if (!(e.metaKey || e.ctrlKey)) return;
      if (e.key !== 'a' && e.key !== 'A') return;
      const tab = document.getElementById('tab-actuals');
      // Tabs toggle via the 'active' class, not inline display — the old
      // style.display check was always false, so Cmd+A was intercepted on
      // EVERY tab and silently selected hidden Actuals rows. (Review 2026-06-04.)
      if (!tab || !tab.classList.contains('active')) return;
      // Only intercept when focus is in the tab (not in a text input).
      const ae = document.activeElement;
      if (ae && (ae.tagName === 'INPUT' || ae.tagName === 'TEXTAREA' || ae.tagName === 'SELECT')) return;
      e.preventDefault();
      document.querySelectorAll('.actuals-txn-row').forEach(r => {
        if (r.style.display !== 'none') _selectedTids.add(parseInt(r.dataset.tid));
      });
      _renderSelection();
    });

    // ── Filter bar wiring ─────────────────────────────────────────────
    // Text/number inputs are DEBOUNCED (each keystroke used to run the full
    // ~1,200-row resolver synchronously — the "slow and clunky" typing feel);
    // selects/dates apply immediately. (User 2026-06-11.)
    const _AXF_FIELD_IDS = ['actuals-filter-card','actuals-filter-date-from','actuals-filter-date-to',
      'actuals-filter-vendor','actuals-filter-section','actuals-filter-receipt',
      'actuals-filter-uploader','actuals-filter-amount-min','actuals-filter-amount-max'];

    // Persist the whole view (stat-card filter + every field + sort) so a
    // reload — e.g. after uploading a receipt to a row — comes back to the
    // EXACT same filtered view instead of resetting. (User 2026-06-11.)
    const _AXF_STORE_KEY = 'fpb-actuals-view-' + PROJ_ID;
    window._actualsSaveFilterState = function () {
      try {
        const fields = {};
        _AXF_FIELD_IDS.forEach(id => { const el = document.getElementById(id); if (el && el.value) fields[id] = el.value; });
        sessionStorage.setItem(_AXF_STORE_KEY, JSON.stringify({
          f: _actualsActiveFilter || 'all',
          sort: (document.getElementById('actuals-sort-by') || {}).value || 'date-desc',
          fields: fields }));
      } catch (e) { /* storage unavailable — non-fatal */ }
    };
    function _actualsRestoreFilterState() {
      try {
        const raw = sessionStorage.getItem(_AXF_STORE_KEY);
        if (!raw) return null;
        const st = JSON.parse(raw);
        Object.entries(st.fields || {}).forEach(([id, v]) => {
          const el = document.getElementById(id); if (el) el.value = v;
        });
        const s = document.getElementById('actuals-sort-by');
        if (s && st.sort) s.value = st.sort;
        return st;
      } catch (e) { return null; }
    }

    // Build the 💳 Card dropdown from the rows actually on the page.
    function _actualsPopulateCardFilter() {
      const sel = document.getElementById('actuals-filter-card');
      if (!sel) return;
      const counts = {};
      document.querySelectorAll('.actuals-txn-row').forEach(r => {
        const c = r.dataset.sortCard4 || '';
        if (c) counts[c] = (counts[c] || 0) + 1;
      });
      const keep = sel.value;
      sel.innerHTML = '<option value="">Any card</option>' +
        Object.keys(counts).sort().map(c =>
          '<option value="' + c + '">••' + c + ' (' + counts[c] + ')</option>').join('');
      if (keep) sel.value = keep;
    }
    window._actualsPopulateCardFilter = _actualsPopulateCardFilter;

    window.actualsClearFilters = function () {
      _AXF_FIELD_IDS.forEach(id => {
        const el = document.getElementById(id);
        if (el) el.value = '';
      });
      // Reset sort to the default newest-first.
      const s = document.getElementById('actuals-sort-by');
      if (s) s.value = 'date-desc';
      try { sessionStorage.removeItem(_AXF_STORE_KEY); } catch (e) {}
      _actualsResolveFilters();
      _actualsApplySort();   // sort decoupled from filtering — apply explicitly
    };

    let _axfDebounceT = null;
    function _axfDebouncedResolve() {
      clearTimeout(_axfDebounceT);
      _axfDebounceT = setTimeout(_actualsResolveFilters, 180);
    }
    // Typed fields → debounced; pickers/dates → immediate on change.
    ['actuals-filter-vendor','actuals-filter-amount-min','actuals-filter-amount-max'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.addEventListener('input', _axfDebouncedResolve);
      if (el) el.addEventListener('change', _actualsResolveFilters);
    });
    ['actuals-filter-card','actuals-filter-date-from','actuals-filter-date-to',
     'actuals-filter-section','actuals-filter-receipt','actuals-filter-uploader'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.addEventListener('change', _actualsResolveFilters);
    });
    // Sort is decoupled from filtering — only re-order when the sort selection
    // itself changes (filtering never changes order). (User 2026-06-03.)
    const _sortSel = document.getElementById('actuals-sort-by');
    if (_sortSel) _sortSel.addEventListener('change', function () { _actualsApplySort(); _actualsSaveFilterState(); });

    // ── Three-bucket fulfillment state (user 2026-06-02) ──────────────
    // Each row works toward three boxes — a transaction, a document, and a
    // budget line. Derive data-fulfill (0..3) from the row's state attrs so
    // the Progress sort + the green "fully reconciled" stripe stay correct.
    // A MutationObserver recomputes it whenever those attrs change, so every
    // existing match/unmatch/code action keeps it current without touching
    // the ~30 sites that mutate them. (The cells themselves recolor purely
    // via CSS attribute selectors — this only maintains the derived total.)
    function _actualsComputeFulfill(row) {
      if (!row || !row.dataset) return;
      const hasTxn   = row.dataset.hasTxn === '1';
      const hasDoc   = row.dataset.hasDoc === '1';
      const coded    = row.dataset.coded === '1';
      const excluded = row.dataset.notProject === '1' || row.dataset.claimedElsewhere === '1';
      const n = excluded ? 0 : ((hasTxn ? 1 : 0) + (hasDoc ? 1 : 0) + (coded ? 1 : 0));
      row.dataset.fulfill = String(n);
    }
    (function () {
      const list = document.getElementById('actuals-txn-list');
      if (!list) return;
      list.querySelectorAll('.actuals-txn-row').forEach(_actualsComputeFulfill);
      const obs = new MutationObserver(muts => {
        const seen = new Set();
        muts.forEach(m => {
          const row = m.target.closest && m.target.closest('.actuals-txn-row');
          if (row && !seen.has(row)) { seen.add(row); _actualsComputeFulfill(row); }
        });
        // Restack live when sorting by progress, so a just-matched item
        // animates up/down into its new tier. Other sorts are unaffected.
        if (seen.size) {
          const sortBy = (document.getElementById('actuals-sort-by') || {}).value || '';
          if (sortBy.startsWith('progress') && typeof _actualsApplySort === 'function') {
            _actualsApplySort();
          }
        }
      });
      // Observe the common parent (not just the flat list): the grouped
      // view moves rows into a sibling #actuals-sections, so watching the
      // parent keeps data-fulfill (the 3/3 stripe + progress sort) live in
      // both flat and grouped layouts.
      obs.observe(list.parentNode || list, {
        subtree: true,
        attributes: true,
        attributeFilter: ['data-coded', 'data-has-doc', 'data-has-txn',
                          'data-not-project', 'data-claimed-elsewhere'],
      });
    })();

    // ── Hover preview on doc badges ───────────────────────────────────
    // Float a small thumbnail next to the badge while hovering. Image
    // receipts render as <img>; PDFs/other types show an icon. The
    // preview lazy-loads via /docs/upload/<uid>/raw the first time.
    let _hoverPreviewEl = null;
    function _ensureHoverPreviewEl() {
      if (_hoverPreviewEl) return _hoverPreviewEl;
      const el = document.createElement('div');
      el.id = '_actuals-doc-hover-preview';
      el.style.cssText = `
        position:fixed; z-index:10000; display:none; pointer-events:none;
        background:var(--bg-card); border:1px solid var(--border);
        border-radius:8px; box-shadow:0 12px 36px rgba(0,0,0,.6);
        padding:6px; max-width:340px; max-height:340px; overflow:hidden;
      `;
      document.body.appendChild(el);
      _hoverPreviewEl = el;
      return el;
    }
    document.querySelectorAll('.actuals-doc-badge').forEach(badge => {
      let hoverTimer = null;
      badge.addEventListener('mouseenter', e => {
        // 200ms delay so a quick mouse pass-through doesn't load.
        hoverTimer = setTimeout(() => {
          const docId = badge.dataset.docId;
          if (!docId) return;
          const el = _ensureHoverPreviewEl();
          el.innerHTML = `<div style="font-size:.7rem;color:var(--text-muted);padding:6px 4px">Loading preview…</div>`;
          // Fetch metadata to know whether it's an image or PDF.
          fetch(`/docs/upload/${docId}/preview-link`, { credentials: 'same-origin' })
            .then(r => r.json()).catch(() => ({}))
            .then(d => {
              const ct = (d && d.content_type ? d.content_type : '').toLowerCase();
              const fn = (d && d.filename ? d.filename : '').toLowerCase();
              const isImg = ct.startsWith('image/') || /\.(jpe?g|png|gif|heic|heif|webp|tiff?|bmp)$/i.test(fn);
              const isPdf = ct.includes('pdf') || fn.endsWith('.pdf');
              const rawUrl = `/docs/upload/${docId}/raw`;
              if (isImg) {
                el.innerHTML = `<img src="${rawUrl}" alt="" style="max-width:330px;max-height:330px;display:block;border-radius:5px">`;
              } else if (isPdf) {
                el.innerHTML = `
                  <div style="display:flex;align-items:center;gap:10px;padding:10px 14px;font-size:.82rem;color:var(--text)">
                    <span style="font-size:2rem">📄</span>
                    <div>
                      <div style="font-weight:600">PDF</div>
                      <div style="font-size:.72rem;color:var(--text-muted)">Click the badge to open the full preview.</div>
                    </div>
                  </div>`;
              } else {
                el.innerHTML = `
                  <div style="padding:10px 14px;font-size:.78rem;color:var(--text-muted)">
                    Click the badge to open this document.
                  </div>`;
              }
            });
          // Position the preview panel near the badge but inside the
          // viewport. Updated on mousemove for smooth follow.
          const rect = badge.getBoundingClientRect();
          el.style.left = `${Math.min(window.innerWidth - 360, rect.right + 12)}px`;
          el.style.top  = `${Math.min(window.innerHeight - 360, rect.top - 8)}px`;
          el.style.display = 'block';
        }, 200);
      });
      badge.addEventListener('mouseleave', () => {
        if (hoverTimer) { clearTimeout(hoverTimer); hoverTimer = null; }
        if (_hoverPreviewEl) _hoverPreviewEl.style.display = 'none';
      });
    });

    // ── Upload-receipt: dropzone modal (drag-or-browse) ───────────────
    // Click 📤 → opens a small modal with a visible drop zone PLUS a
    // "browse" affordance. Either path POSTs to the existing
    // /actuals/transaction/<tid>/upload-receipt endpoint.
    function _ensureUploadModal() {
      let modal = document.getElementById('_actuals-upload-modal');
      if (modal) return modal;
      modal = document.createElement('div');
      modal.id = '_actuals-upload-modal';
      modal.style.cssText = `
        display:none; position:fixed; top:0; left:0; right:0; bottom:0;
        z-index:10000; background:rgba(0,0,0,.78);
        align-items:flex-start; justify-content:center; padding-top:6vh;
      `;
      modal.innerHTML = `
        <div onclick="event.stopPropagation()"
             style="background:var(--bg-card, #1a1d27);border:1px solid var(--border);
                    border-radius:10px;padding:18px 22px;width:560px;max-width:96vw;
                    max-height:88vh;display:flex;flex-direction:column;
                    box-shadow:0 20px 60px rgba(0,0,0,.5)">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">
            <h3 style="margin:0;font-size:1rem">📤 Attach receipt</h3>
            <button type="button" class="btn btn-sm btn-ghost"
                    onclick="document.getElementById('_actuals-upload-modal').style.display='none'">✕</button>
          </div>
          <div id="_actuals-upload-target" style="font-size:.78rem;color:var(--text-muted);margin-bottom:10px"></div>
          <div style="display:flex;gap:0;border-bottom:1px solid var(--border);margin-bottom:14px">
            <button type="button" id="_actuals-upload-tab-new"
                    onclick="_actualsUploadShowTab('new')"
                    style="padding:8px 14px;background:none;border:none;border-bottom:2px solid var(--blue);color:var(--text);cursor:pointer;font-size:.84rem">
              New file
            </button>
            <button type="button" id="_actuals-upload-tab-existing"
                    onclick="_actualsUploadShowTab('existing')"
                    style="padding:8px 14px;background:none;border:none;border-bottom:2px solid transparent;color:var(--text-muted);cursor:pointer;font-size:.84rem">
              Link existing receipt
            </button>
          </div>
          <div id="_actuals-upload-pane-new">
            <div id="_actuals-upload-drop"
                 style="border:2px dashed var(--border);border-radius:10px;padding:30px 20px;
                        text-align:center;cursor:pointer;transition:border-color .15s,background .15s">
              <div style="font-size:2rem;margin-bottom:8px">📎</div>
              <div style="font-size:.88rem;color:var(--text);margin-bottom:4px">
                <strong>Drop a file here</strong>
              </div>
              <div style="font-size:.74rem;color:var(--text-muted)">
                or <a href="#" id="_actuals-upload-browse" style="color:var(--blue);text-decoration:underline">click to browse</a>
              </div>
              <div style="font-size:.7rem;color:var(--text-muted);margin-top:8px">
                PDF · JPG · PNG · HEIC
              </div>
            </div>
          </div>
          <div id="_actuals-upload-pane-existing" style="display:none;flex-direction:column">
            <div style="font-size:.74rem;color:var(--text-muted);margin-bottom:8px">
              Pick an existing receipt from this project's docs. Same receipt can back multiple transactions (e.g. a Lyft week-rollup).
            </div>
            <input type="text" id="_actuals-upload-existing-search" placeholder="Search vendor, amount, or filename…"
                   style="font-size:.78rem;padding:6px 10px;background:var(--bg-input);color:var(--text);border:1px solid var(--border);border-radius:5px;margin-bottom:8px;flex-shrink:0">
            <div id="_actuals-upload-existing-list"
                 style="height:380px;overflow-y:auto;border:1px solid var(--border);border-radius:5px;padding:4px;flex-shrink:0">
              <div class="muted" style="font-size:.78rem;padding:10px;text-align:center">Loading…</div>
            </div>
          </div>
          <div id="_actuals-upload-status" style="margin-top:12px;font-size:.78rem;text-align:center;min-height:18px"></div>
        </div>
      `;
      // Click outside the inner box closes the modal.
      modal.addEventListener('click', e => {
        if (e.target === modal) modal.style.display = 'none';
      });
      document.body.appendChild(modal);
      return modal;
    }

    async function _actualsDoUpload(tid, file) {
      const status = document.getElementById('_actuals-upload-status');
      if (status) {
        status.textContent = `⏳ Uploading "${file.name}" …`;
        status.style.color = 'var(--text-muted)';
      }
      const fd = new FormData();
      fd.append('file', file);
      try {
        const r = await fetch(`/projects/${PROJ_ID}/actuals/transaction/${tid}/upload-receipt`, {
          method: 'POST', body: fd,
        });
        const d = await r.json();
        if (!r.ok) {
          if (status) {
            status.textContent = '✕ ' + (d.error || ('HTTP ' + r.status));
            status.style.color = '#e08080';
          }
          return;
        }
        if (status) {
          status.textContent = `✓ Uploaded: ${d.filed_filename || file.name}`;
          status.style.color = 'var(--green)';
        }
        setTimeout(() => {
          document.getElementById('_actuals-upload-modal').style.display = 'none';
          window.location.reload();
        }, 700);
      } catch (e) {
        if (status) { status.textContent = '✕ ' + e.message; status.style.color = '#e08080'; }
      }
    }

    // Tab switcher inside the upload modal — toggles between "New file"
    // (drag/browse) and "Link existing receipt" (pick from project docs).
    let _actualsUploadCurrentTid = null;
    let _actualsExistingDocs     = null;  // cached doc list for the picker
    window._actualsUploadShowTab = function (which) {
      const tabNew = document.getElementById('_actuals-upload-tab-new');
      const tabEx  = document.getElementById('_actuals-upload-tab-existing');
      const paneNew = document.getElementById('_actuals-upload-pane-new');
      const paneEx  = document.getElementById('_actuals-upload-pane-existing');
      if (which === 'new') {
        if (tabNew) { tabNew.style.borderBottomColor = 'var(--blue)'; tabNew.style.color = 'var(--text)'; }
        if (tabEx)  { tabEx.style.borderBottomColor = 'transparent'; tabEx.style.color = 'var(--text-muted)'; }
        if (paneNew) paneNew.style.display = '';
        if (paneEx)  paneEx.style.display = 'none';
      } else {
        if (tabEx)  { tabEx.style.borderBottomColor = 'var(--blue)'; tabEx.style.color = 'var(--text)'; }
        if (tabNew) { tabNew.style.borderBottomColor = 'transparent'; tabNew.style.color = 'var(--text-muted)'; }
        if (paneNew) paneNew.style.display = 'none';
        if (paneEx)  paneEx.style.display = 'flex';
        _loadExistingDocs();
      }
    };

    async function _loadExistingDocs() {
      const list = document.getElementById('_actuals-upload-existing-list');
      if (!list) return;
      if (_actualsExistingDocs === null) {
        list.innerHTML = '<div class="muted" style="font-size:.78rem;padding:10px;text-align:center">Loading…</div>';
        try {
          const r = await fetch(`/projects/${PROJ_ID}/actuals/docs.json`);
          const d = await r.json();
          _actualsExistingDocs = (d && d.ok) ? (d.docs || []) : [];
        } catch (e) { _actualsExistingDocs = []; }
      }
      _renderExistingDocs();
    }

    function _renderExistingDocs() {
      const list = document.getElementById('_actuals-upload-existing-list');
      const search = (document.getElementById('_actuals-upload-existing-search')?.value || '').toLowerCase().trim();
      if (!list) return;
      const docs = _actualsExistingDocs || [];
      if (!docs.length) {
        list.innerHTML = '<div class="muted" style="font-size:.78rem;padding:14px;text-align:center">No existing receipts on this project yet.</div>';
        return;
      }
      const filtered = docs.filter(d => {
        if (!search) return true;
        // Searchable: label/vendor/category + amount (both signed and absolute,
        // so "107.17" matches a -107.17 refund). (User 2026-06-17.)
        const amtStr = (d.amount != null) ? (Number(d.amount).toFixed(2) + ' ' + Math.abs(Number(d.amount)).toFixed(2)) : '';
        return ((d.label || '') + ' ' + (d.vendor || '') + ' ' + (d.category || '') + ' ' + amtStr).toLowerCase().includes(search);
      });
      if (!filtered.length) {
        list.innerHTML = `<div class="muted" style="font-size:.78rem;padding:14px;text-align:center">No matches for "${search}".</div>`;
        return;
      }
      const _esc = s => String(s||'').replace(/[&<>"']/g, c =>
        ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
      list.innerHTML = filtered.map(d => `
        <div class="_actuals-existing-doc" data-doc-id="${d.id}"
             onclick="_actualsPreviewExistingDoc(${d.id}, event)"
             title="Click to preview · Link button to attach"
             style="display:flex;align-items:center;gap:8px;padding:7px 9px;margin-bottom:3px;
                    border:1px solid var(--border);border-radius:5px;background:var(--bg-input);
                    cursor:pointer;font-size:.78rem">
          <span style="font-size:.92rem;flex-shrink:0">${d.has_image ? '🖼' : '📄'}</span>
          <div style="flex:1;min-width:0;overflow:hidden">
            <div style="font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${_esc(d.label)}</div>
            <div style="font-size:.7rem;color:var(--text-muted)">
              ${d.vendor ? _esc(d.vendor) : '—'}${d.doc_date ? ' · ' + d.doc_date : ''}${d.amount ? ' · $' + d.amount.toFixed(2) : ''}${d.category ? ' · ' + _esc(d.category) : ''}
            </div>
          </div>
          <button type="button" class="btn btn-xs"
                  style="font-size:.7rem;padding:3px 9px;background:#1e293b;border:1px solid #334155;color:#cbd5e1;cursor:pointer;border-radius:4px"
                  onclick="event.stopPropagation();_actualsPreviewExistingDoc(${d.id})">👁 Preview</button>
          <button type="button" class="btn btn-xs"
                  style="font-size:.7rem;padding:3px 9px;background:#1a3a2a;border:1px solid #2a5a3a;color:#74c69d;cursor:pointer;border-radius:4px"
                  onclick="event.stopPropagation();_actualsLinkExistingDoc(${d.id})">Link</button>
        </div>
      `).join('');
    }

    // Open the doc-detail modal as a read-only preview from inside the
    // Attach Receipt picker. The detail modal already supports being
    // opened with row=null (it fetches metadata via /docs/upload/<uid>/
    // status), so we just call openDocDetail with the id.
    window._actualsPreviewExistingDoc = function (docId, ev) {
      if (ev) ev.stopPropagation();
      if (typeof window.openDocDetail === 'function') {
        window.openDocDetail(null, docId);
      }
    };

    window._actualsLinkExistingDoc = async function (docId) {
      const tid = _actualsUploadCurrentTid;
      if (!tid) return;
      const status = document.getElementById('_actuals-upload-status');
      if (status) { status.textContent = '⏳ Linking…'; status.style.color = 'var(--text-muted)'; }
      try {
        const r = await fetch(`/projects/${PROJ_ID}/actuals/transaction/${tid}/link-doc`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ doc_upload_id: docId }),
        });
        const d = await r.json();
        if (!r.ok) {
          if (status) { status.textContent = '✕ ' + (d.error || ('HTTP ' + r.status)); status.style.color = '#e08080'; }
          return;
        }
        if (status) { status.textContent = `✓ Linked: ${d.filed_filename}`; status.style.color = 'var(--green)'; }
        setTimeout(() => {
          document.getElementById('_actuals-upload-modal').style.display = 'none';
          window.location.reload();
        }, 700);
      } catch (e) {
        if (status) { status.textContent = '✕ ' + e.message; status.style.color = '#e08080'; }
      }
    };

    document.querySelectorAll('.actuals-upload-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        const tid = parseInt(btn.dataset.tid);
        _actualsUploadCurrentTid = tid;
        const modal = _ensureUploadModal();
        // Re-find the row → vendor / amount for the modal subtitle.
        const row = document.querySelector(`.actuals-txn-row[data-tid="${tid}"]`);
        const target = document.getElementById('_actuals-upload-target');
        if (target) {
          const vendor = row ? (row.querySelector('.actuals-txn-vendor')?.textContent.trim() || '') : '';
          const date   = row ? (row.dataset.txnDate || '') : '';
          const amt    = row ? parseFloat(row.dataset.amount || 0) : 0;
          // Build with textContent — vendor comes from OCR/QBO, not trusted
          // for innerHTML. (Review CR-5, 2026-06-04.)
          target.textContent = 'Linking to: ';
          const _vb = document.createElement('strong');
          _vb.textContent = vendor || '— no vendor —';
          target.appendChild(_vb);
          target.appendChild(document.createTextNode(
            (date ? ' · ' + date : '') + (amt ? ' · $' + amt.toFixed(2) : '')));
        }
        const status = document.getElementById('_actuals-upload-status');
        if (status) status.textContent = '';
        // Wire the dropzone for THIS row each time the modal opens.
        const drop   = document.getElementById('_actuals-upload-drop');
        const browse = document.getElementById('_actuals-upload-browse');
        let depth = 0;
        drop.ondragenter = e => {
          if (!Array.from(e.dataTransfer.types).includes('Files')) return;
          e.preventDefault(); depth++;
          drop.style.borderColor = 'var(--blue)';
          drop.style.background = 'rgba(91,138,249,.06)';
        };
        drop.ondragover = e => {
          if (!Array.from(e.dataTransfer.types).includes('Files')) return;
          e.preventDefault();
          e.dataTransfer.dropEffect = 'copy';
        };
        drop.ondragleave = () => {
          depth = Math.max(0, depth - 1);
          if (depth === 0) {
            drop.style.borderColor = 'var(--border)';
            drop.style.background = '';
          }
        };
        drop.ondrop = e => {
          e.preventDefault(); depth = 0;
          drop.style.borderColor = 'var(--border)';
          drop.style.background = '';
          if (!e.dataTransfer.files || !e.dataTransfer.files.length) return;
          if (e.dataTransfer.files.length > 1) {
            const status = document.getElementById('_actuals-upload-status');
            if (status) { status.textContent = 'Drop one file at a time.'; status.style.color = 'var(--yellow)'; }
            return;
          }
          _actualsDoUpload(tid, e.dataTransfer.files[0]);
        };
        // Click-to-browse opens the OS file picker.
        const openPicker = () => {
          let inp = document.getElementById('_actuals-upload-file');
          if (!inp) {
            inp = document.createElement('input');
            inp.type = 'file';
            inp.id = '_actuals-upload-file';
            inp.accept = 'image/*,application/pdf';
            inp.style.display = 'none';
            document.body.appendChild(inp);
          }
          inp.value = '';
          inp.onchange = () => {
            if (!inp.files || !inp.files.length) return;
            _actualsDoUpload(tid, inp.files[0]);
          };
          inp.click();
        };
        drop.onclick   = (e) => { if (e.target.id !== '_actuals-upload-browse') openPicker(); };
        browse.onclick = (e) => { e.preventDefault(); openPicker(); };
        // Reset to "New file" tab on every open + clear the existing-
        // doc cache so the picker re-fetches if the user switches tabs.
        _actualsUploadShowTab('new');
        _actualsExistingDocs = null;
        const search = document.getElementById('_actuals-upload-existing-search');
        if (search) {
          search.value = '';
          search.oninput = _renderExistingDocs;
        }
        modal.style.display = 'flex';
      });
    });

    // ── Drop a file onto a transaction row → upload + link ────────────
    // For users who have a paper receipt or a manually-found PDF and
    // want to attach it to a specific transaction. Drops POST to
    // /actuals/transaction/<tid>/upload-receipt which runs the full
    // analyzer pipeline + sets doc_upload_id WITHOUT creating a
    // duplicate doc-source Transaction.
    // File-drag highlight that STAYS while hovering a row (User 2026-06-16).
    // The old dragenter/dragleave depth-counting flickered the dashed box off
    // as the cursor crossed the row's child cells. Instead, re-assert the
    // outline on every dragover (fires continuously) and keep ONE highlighted
    // row; clear it on drop / dragend.
    let _fileHLRow = null;
    function _setFileHL(row) {
      if (_fileHLRow && _fileHLRow !== row) { _fileHLRow.style.outline = ''; _fileHLRow.style.outlineOffset = ''; }
      row.style.outline = '2px dashed var(--blue)'; row.style.outlineOffset = '-2px';
      _fileHLRow = row;
    }
    function _clearFileHL() { if (_fileHLRow) { _fileHLRow.style.outline = ''; _fileHLRow.style.outlineOffset = ''; _fileHLRow = null; } }
    document.addEventListener('dragend', _clearFileHL);
    const _isFileDrag = e => !!(e.dataTransfer && e.dataTransfer.types && Array.from(e.dataTransfer.types).includes('Files'));
    document.querySelectorAll('.actuals-txn-row').forEach(row => {
      row.addEventListener('dragenter', e => { if (!_isFileDrag(e)) return; e.preventDefault(); _setFileHL(row); });
      row.addEventListener('dragover', e => {
        if (!_isFileDrag(e)) return;
        e.preventDefault();
        e.dataTransfer.dropEffect = 'copy';
        _setFileHL(row);   // re-assert every frame so it never flickers off
      });
      row.addEventListener('drop', async e => {
        if (!e.dataTransfer || !e.dataTransfer.files || !e.dataTransfer.files.length) return;
        e.preventDefault();
        e.stopPropagation();
        _clearFileHL();
        const tid  = parseInt(row.dataset.tid);
        const file = e.dataTransfer.files[0];
        if (e.dataTransfer.files.length > 1) {
          _actualsToast('Drop one file at a time onto a transaction row.', 'yellow');
          return;
        }
        // If the row already has a receipt, refuse — the user should
        // explicitly clear/replace via the docs modal.
        if (row.dataset.hasDoc === '1') {
          _actualsToast('This transaction already has a receipt linked. Open it to replace.', 'yellow');
          return;
        }
        // Clear, unmistakable progress on the row: pulsing outline + an
        // inline "uploading / reading receipt" badge. (User 2026-06-16 —
        // the faint tint wasn't visible enough; "I didn't know it was working".)
        const prevBg = row.style.background, prevOutline = row.style.outline;
        row.style.background = '#15203a';
        row.style.outline = '2px solid var(--blue)';
        row.style.outlineOffset = '-2px';
        const badge = document.createElement('div');
        badge.textContent = '⏳ Uploading & reading receipt…';
        badge.style.cssText = 'position:absolute;top:2px;right:6px;z-index:5;font-size:.66rem;'
          + 'background:#15203a;border:1px solid var(--blue);color:#cfe0ff;padding:2px 8px;border-radius:10px';
        const _prevPos = row.style.position; row.style.position = 'relative';
        row.appendChild(badge);
        if (typeof _actualsToast === 'function') _actualsToast('⏳ Uploading receipt — reading it with OCR…', 'yellow');
        const _restore = () => { row.style.background = prevBg; row.style.outline = prevOutline; row.style.position = _prevPos; badge.remove(); };
        const fd = new FormData();
        fd.append('file', file);
        try {
          const r = await fetch(`/projects/${PROJ_ID}/actuals/transaction/${tid}/upload-receipt`, {
            method: 'POST', body: fd,
          });
          const d = await r.json();
          if (!r.ok) {
            _restore();
            _actualsToast('Upload failed: ' + (d.error || r.status), 'red');
            return;
          }
          // ── In-place update — no full page reload (User 2026-06-17) ──
          _restore();                                   // clear progress badge + styling
          const _coded = row.dataset.coded === '1';
          row.dataset.hasDoc = '1';
          row.dataset.docId = String(d.upload_id || '');
          if (row.dataset.matchStatus !== 'confirmed') row.dataset.matchStatus = 'suggested';
          row.dataset.fulfill = _coded ? '3' : '2';     // txn+doc(+code) — drives stripes via CSS
          // Fill the 📎 receipt cell with a clickable badge.
          const _filled = row.querySelector('.ax-cell-doc .ax-filled');
          if (_filled) {
            const _cat = d.doc_type || 'receipt';
            const _nm  = (d.filed_filename || file.name || '');
            const _short = _nm.length > 16 ? (_nm.slice(0, 16) + '…') : _nm;
            _filled.innerHTML = '<span class="ax-ic">🧾</span>'
              + '<a href="#" class="actuals-doc-badge ax-c-txt" data-doc-id="' + (d.upload_id || '') + '" '
              + 'onclick="openDocDetail(' + (d.upload_id || 0) + ', null);return false" '
              + 'style="color:#7ba6e8;text-decoration:none">' + _mrEsc(_cat) + ' · ' + _mrEsc(_short) + '</a>';
          }
          // Move the row (+ its suggestion banner) into the 🔗 Linked group and
          // refresh the section counts — no reload.
          const _linkedBody = document.getElementById('ax-secbody-linked');
          if (_linkedBody) {
            const _bn = document.querySelector('.actuals-suggested-banner[data-tid="' + tid + '"]');
            if (_bn) _linkedBody.appendChild(_bn);
            _linkedBody.appendChild(row);
            ['needs', 'receipts', 'linked'].forEach(k => {
              const b = document.getElementById('ax-secbody-' + k);
              const c = document.getElementById('ax-seccount-' + k);
              if (b && c) c.textContent = ' (' + b.querySelectorAll('.actuals-txn-row').length + ')';
            });
          }
          if (typeof _actualsRecountStats === 'function') _actualsRecountStats();
          _actualsToast(_coded
            ? `Receipt linked: ${d.filed_filename || file.name}. Moved to Linked.`
            : `Receipt linked: ${d.filed_filename || file.name}. Moved to Linked — still flagged “needs coding” (amber).`,
            'green');
        } catch (err) {
          _restore();
          _actualsToast('Upload error: ' + err.message, 'red');
        }
      });
    });

    // ── Reverse drag: row → COA tree (slice 3) ────────────────────────
    // Drag a transaction row over the sidebar. Hovering a section for
    // ~350ms expands its line children. Drop on the section header
    // → quick-code (account_code only). Drop on a specific line
    // → full link via /set-line, which auto-clones Working → Actual on
    // first link.
    let _draggingTxn = null;
    let _hoverExpandTimer = null;

    document.querySelectorAll('.actuals-txn-row').forEach(row => {
      row.addEventListener('dragstart', e => {
        // Don't hijack drags that started on a select/input/button.
        if (e.target.closest('select, input, textarea, button, a')) {
          e.preventDefault();
          return;
        }
        const tid = parseInt(row.dataset.tid);
        // Bulk drag: if the dragged row is in the selection, drag the
        // whole selection. Otherwise drag only this row (and clear any
        // selection so the user isn't surprised).
        let tids;
        if (_selectedTids.has(tid)) {
          tids = Array.from(_selectedTids);
        } else {
          tids = [tid];
        }
        _draggingTxn = { tids };
        // Visual cue: dim every dragged row.
        tids.forEach(_t => {
          const r = document.querySelector(`.actuals-txn-row[data-tid="${_t}"]`);
          if (r) r.style.opacity = '.55';
        });
        e.dataTransfer.effectAllowed = 'move';
        e.dataTransfer.setData('text/plain', tids.join(','));
      });
      row.addEventListener('dragend', () => {
        if (_draggingTxn && _draggingTxn.tids) {
          _draggingTxn.tids.forEach(_t => {
            const r = document.querySelector(`.actuals-txn-row[data-tid="${_t}"]`);
            if (r) r.style.opacity = '';
          });
        }
        _draggingTxn = null;
        document.querySelectorAll('.coa-lines').forEach(el => { el.style.display = 'none'; });
        document.querySelectorAll('.coa-chevron').forEach(c => { c.textContent = '▸'; });
      });
    });

    // Single-highlight guarantee (User 2026-06-16): per-element dragleave is
    // unreliable (it fires when crossing child nodes), so drop-target boxes
    // piled up. Clear EVERY COA highlight before lighting the current target,
    // and wipe them all when the drag ends.
    window._coaClearHighlights = function () {
      // Class-based (User 2026-06-17): just drop the highlight class. The old
      // version set el.style.background/borderColor/color = '' on every box,
      // but those properties hold each box's INLINE base look — wiping them
      // turned all boxes white on every dragover. Removing a class leaves the
      // inline base untouched.
      document.querySelectorAll('.coa-hl').forEach(el => el.classList.remove('coa-hl'));
      // Recently-used tiles highlight via inline bg (not .coa-item/.coa-line),
      // so reset them to their base explicitly.
      document.querySelectorAll('#coa-recent .coa-recent-item').forEach(el => {
        el.style.background = 'var(--bg-card)'; el.style.borderColor = 'var(--border)';
      });
    };
    document.addEventListener('dragend', () => window._coaClearHighlights());

    // ── Recently-used coding targets (User 2026-06-16) ─────────────────────
    // The last few lines/sections you dropped onto, pinned at the top of the
    // COA sidebar as drop targets — re-code a run of similar charges without
    // re-expanding the tree. Stored per-project in localStorage.
    const _recentKey = 'fpRecentCoa.' + PROJ_ID;
    function _recentLoad() { try { return JSON.parse(localStorage.getItem(_recentKey) || '[]'); } catch (e) { return []; } }
    window._recordRecent = function (entry) {
      if (!entry || (entry.code === 'not_project')) return;
      let list = _recentLoad();
      const key = o => o.kind + ':' + (o.lineId != null ? ('L' + o.lineId) : ('S' + o.code));
      list = list.filter(x => key(x) !== key(entry));
      list.unshift(entry);
      try { localStorage.setItem(_recentKey, JSON.stringify(list.slice(0, 5))); } catch (e) {}
      _renderRecent();
    };
    async function _coaApplyDrop(opts) {
      const tids = (_draggingTxn && _draggingTxn.tids) || [];
      if (!tids.length) return;
      let ok = 0, fail = 0;
      for (const tid of tids) {
        try {
          const r = (opts.lineId != null)
            ? await fetch(`/projects/${PROJ_ID}/actuals/transaction/${tid}/set-line`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ budget_line_id: parseInt(opts.lineId) }) })
            : await fetch(`/projects/${PROJ_ID}/actuals/transaction/${tid}/set-coa`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ account_code: opts.code, account_code_name: opts.name || '' }) });
          if (r.ok) { ok++; if (typeof _refreshRowAfterCode === 'function') _refreshRowAfterCode(tid, opts.code !== 'not_project', opts.code === 'not_project'); } else fail++;
        } catch (e) { fail++; }
      }
      if (typeof _actualsToast === 'function') _actualsToast(`${ok} coded${fail ? ` (${fail} failed)` : ''}.`, fail ? 'yellow' : 'green');
      if (typeof actualsBulkClear === 'function') actualsBulkClear();
    }
    function _renderRecent() {
      const host = document.getElementById('coa-recent');
      if (!host) return;
      const list = _recentLoad();
      if (!list.length) { host.style.display = 'none'; host.innerHTML = ''; return; }
      host.style.display = '';
      host.innerHTML = '<div style="font-size:.62rem;text-transform:uppercase;letter-spacing:.06em;color:var(--text-muted);margin-bottom:4px;display:flex;align-items:center">★ Recently used<a href="#" id="coa-recent-clear" style="margin-left:auto;color:var(--text-muted);font-size:.6rem;text-decoration:none">clear</a></div>';
      list.forEach(e => {
        const isLine = e.kind === 'line';
        const el = document.createElement('div');
        el.className = 'coa-recent-item';
        el.style.cssText = 'padding:6px 9px;margin-bottom:3px;border-radius:5px;border:1px solid var(--border);background:var(--bg-card);font-size:.78rem;display:flex;align-items:center;gap:6px';
        el.innerHTML = '<span style="color:var(--text-muted);flex-shrink:0;font-size:.7rem">' + (isLine ? '🏷' : '📂') + '</span>'
          + '<span style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--text)">' + ((e.label || e.name || e.code || '') + '').replace(/</g, '&lt;') + '</span>'
          + '<span style="color:var(--text-muted);flex-shrink:0;font-size:.66rem">' + (e.code || '') + '</span>';
        el.addEventListener('dragover', ev => {
          if (!_draggingTxn) return;
          ev.preventDefault(); ev.stopPropagation(); ev.dataTransfer.dropEffect = 'move';
          window._coaClearHighlights();
          el.style.background = '#1a3a2a'; el.style.borderColor = '#2a5a3a';
        });
        el.addEventListener('dragleave', () => { el.style.background = 'var(--bg-card)'; el.style.borderColor = 'var(--border)'; });
        el.addEventListener('drop', async ev => {
          if (!_draggingTxn) return;
          ev.preventDefault(); ev.stopPropagation();
          el.style.background = 'var(--bg-card)'; el.style.borderColor = 'var(--border)';
          await _coaApplyDrop(isLine ? { lineId: e.lineId, code: e.code } : { code: e.code, name: e.name });
          _recordRecent(e);   // bump it back to the top
        });
        host.appendChild(el);
      });
      const clr = document.getElementById('coa-recent-clear');
      if (clr) clr.addEventListener('click', ev => { ev.preventDefault(); try { localStorage.removeItem(_recentKey); } catch (e) {} _renderRecent(); });
    }
    _renderRecent();

    // Section drop target: hover-to-expand + drop = quick-code.
    document.querySelectorAll('.coa-section').forEach(section => {
      const header = section.querySelector('.coa-item[data-droptarget="section"]');
      const lines  = section.querySelector('.coa-lines');
      const chev   = section.querySelector('.coa-chevron');
      if (!header) return;

      section.addEventListener('dragover', e => {
        if (!_draggingTxn) return;
        e.preventDefault();
        e.dataTransfer.dropEffect = 'move';
        window._coaClearHighlights();
        header.classList.add('coa-hl');
        // Schedule expand if we have lines and they're collapsed.
        if (lines && lines.style.display === 'none') {
          if (_hoverExpandTimer) return;
          _hoverExpandTimer = setTimeout(() => {
            lines.style.display = 'block';
            if (chev) chev.textContent = '▾';
            _hoverExpandTimer = null;
          }, 350);
        }
      });
      section.addEventListener('dragleave', e => {
        // Only clear when leaving the section entirely, not when moving
        // between header / lines / line children inside it.
        if (!section.contains(e.relatedTarget)) {
          header.classList.remove('coa-hl');
          if (_hoverExpandTimer) {
            clearTimeout(_hoverExpandTimer);
            _hoverExpandTimer = null;
          }
        }
      });
      header.addEventListener('drop', async e => {
        if (!_draggingTxn) return;
        e.preventDefault();
        e.stopPropagation();
        header.classList.remove('coa-hl');
        const tids = _draggingTxn.tids || [];
        const code = header.dataset.code;
        const name = header.dataset.name || '';
        let ok = 0, fail = 0;
        for (const tid of tids) {
          try {
            const r = await fetch(`/projects/${PROJ_ID}/actuals/transaction/${tid}/set-coa`, {
              method:  'POST',
              headers: { 'Content-Type': 'application/json' },
              body:    JSON.stringify({ account_code: code, account_code_name: name }),
            });
            if (r.ok) {
              ok++;
              _refreshRowAfterCode(tid, code !== 'not_project', code === 'not_project');
            } else { fail++; }
          } catch (err) { fail++; }
        }
        if (code === 'not_project') {
          _actualsToast(`${ok} marked not project${fail ? ` (${fail} failed)` : ''}.`, fail ? 'yellow' : 'green');
        } else {
          _actualsToast(`${ok} coded to ${code} · ${name}${fail ? ` (${fail} failed)` : ''}.`, fail ? 'yellow' : 'green');
          if (ok) _recordRecent({ kind: 'section', code: code, name: name, label: name });
        }
        actualsBulkClear();
      });
    });

    // Line drop target: drop on a specific budget line = full link.
    document.querySelectorAll('.coa-line').forEach(lineEl => {
      lineEl.addEventListener('dragover', e => {
        if (!_draggingTxn) return;
        e.preventDefault();
        e.stopPropagation();
        e.dataTransfer.dropEffect = 'move';
        window._coaClearHighlights();
        lineEl.classList.add('coa-hl');
      });
      lineEl.addEventListener('dragleave', () => {
        lineEl.classList.remove('coa-hl');
      });
      lineEl.addEventListener('drop', async e => {
        if (!_draggingTxn) return;
        e.preventDefault();
        e.stopPropagation();
        lineEl.classList.remove('coa-hl');
        const tids   = _draggingTxn.tids || [];
        const lineId = parseInt(lineEl.dataset.lineId);
        let ok = 0, fail = 0;
        let actualJustMade = false, workingJustMade = false;
        for (const tid of tids) {
          try {
            const r = await fetch(`/projects/${PROJ_ID}/actuals/transaction/${tid}/set-line`, {
              method:  'POST',
              headers: { 'Content-Type': 'application/json' },
              body:    JSON.stringify({ budget_line_id: lineId }),
            });
            const d = await r.json();
            if (r.ok) {
              ok++;
              if (d.working_was_just_created) workingJustMade = true;
              if (d.actual_was_just_created)  actualJustMade  = true;
              _refreshRowAfterCode(tid, true, false);
              const row = document.querySelector(`.actuals-txn-row[data-tid="${tid}"]`);
              if (row) {
                const sel = row.querySelector('.actuals-line-picker');
                if (sel) {
                  // Set the dropdown to the dropped line. If the option
                  // doesn't exist (e.g. a row's option list was filtered
                  // for some reason, or the working budget was just
                  // cloned and option ids drifted), inject one inline so
                  // the dropdown reflects the assignment instead of
                  // staying on "Pick budget line". 2026-05-06 — fixes
                  // multi-select drag where only the first row's
                  // dropdown updated.
                  const wantId = String(lineId);
                  let found = false;
                  for (const opt of sel.options) {
                    if (opt.value === wantId) { sel.value = wantId; found = true; break; }
                  }
                  if (!found) {
                    const newOpt = document.createElement('option');
                    newOpt.value = wantId;
                    newOpt.textContent = (lineEl.dataset.code || '') + ' · ' +
                                         (lineEl.dataset.lineDesc || lineEl.dataset.name || 'Line ' + wantId);
                    sel.appendChild(newOpt);
                    sel.value = wantId;
                  }
                  // Without this, the lazy picker's focus-fill resets value to
                  // the STALE dataset.current and visually reverts the
                  // assignment. (Review 2026-06-04.)
                  sel.dataset.current = wantId;
                }
              }
            } else { fail++; }
          } catch (err) { fail++; }
        }
        if (workingJustMade && actualJustMade) {
          _actualsToast(`Working budget initialized + Actual budget started. ${ok} linked${fail ? ` (${fail} failed)` : ''}. Reloading…`, 'green');
        } else if (actualJustMade) {
          _actualsToast(`Actual budget started. ${ok} linked${fail ? ` (${fail} failed)` : ''}. Reloading…`, 'green');
        } else {
          _actualsToast(`${ok} linked${fail ? ` (${fail} failed)` : ''}.`, fail ? 'yellow' : 'green');
        }
        if (ok) _recordRecent({ kind: 'line', lineId: lineEl.dataset.lineId, code: lineEl.dataset.code,
                                name: lineEl.dataset.name, label: lineEl.dataset.lineDesc || lineEl.dataset.name });
        actualsBulkClear();
        // After auto-init the dropdown options are stale (they were
        // built from Estimated lines before Working/Actual existed).
        // Force a reload so the post-auto-init state is rendered with
        // matching ids. For non-auto-init bulk drops, the row's
        // dropdown was already updated client-side in the loop above.
        if (workingJustMade || actualJustMade) {
          setTimeout(() => window.location.reload(), 1000);
        }
      });
    });

    // Helper: refresh row visual + stat counts + reapply filter after
    // a coding change.
    function _refreshRowAfterCode(tid, isCoded, isNotProject) {
      const row = document.querySelector(`.actuals-txn-row[data-tid="${tid}"]`);
      if (!row) return;
      if (isNotProject) {
        row.dataset.notProject = '1';
        row.dataset.coded      = '0';
        row.style.opacity      = '.65';
        row.style.borderColor  = '#3a2a2a';
      } else {
        row.dataset.notProject = '0';
        row.dataset.coded      = isCoded ? '1' : '0';
        row.style.opacity      = '';
        row.style.borderColor  = isCoded ? '#1a3a2a' : 'var(--border)';
        row.style.background   = isCoded ? '#0d1a14' : 'var(--bg-card)';
      }
      _actualsRecountStats();
      // Auto-leave the active filter view if the row no longer matches.
      if (typeof actualsApplyFilter === 'function') {
        actualsApplyFilter(_actualsActiveFilter);
      }
    }

    // ── Drag-drop coding (slice 2) ─────────────────────────────────────
    // Drag a COA item from the sidebar onto a transaction row. Drop
    // sets the section's account_code on the txn (does NOT pick a
    // specific budget line — that's the dropdown's job, since it
    // triggers the Working→Actual auto-clone).
    let _draggingCOA = null;
    document.querySelectorAll('.coa-item').forEach(item => {
      item.addEventListener('dragstart', e => {
        _draggingCOA = {
          code: item.dataset.code,
          name: item.dataset.name || '',
          lineId: null,
        };
        item.style.opacity = '.5';
        e.dataTransfer.effectAllowed = 'copy';
        e.dataTransfer.setData('text/plain', item.dataset.code);
      });
      item.addEventListener('dragend', () => {
        item.style.opacity = '';
        _draggingCOA = null;
      });
    });
    // Click section header to expand/collapse its sub-lines (separate
    // from the hover-during-drag expansion). Lets the user keep a
    // section open and drag the specific child line they want.
    document.querySelectorAll('.coa-section').forEach(section => {
      const header = section.querySelector('.coa-item[data-droptarget="section"]');
      const lines  = section.querySelector('.coa-lines');
      const chev   = section.querySelector('.coa-chevron');
      if (!header || !lines) return;
      header.addEventListener('click', e => {
        // Don't trigger on drag — only on a real click. Drag fires
        // dragstart, not click, so this is safe.
        const open = lines.style.display !== 'none';
        lines.style.display = open ? 'none' : 'block';
        if (chev) chev.textContent = open ? '▸' : '▾';
      });
    });
    // Make every COA child line draggable too — drop on a transaction
    // = full /set-line link (auto-clones Working → Actual on first
    // link). Distinct from section drag (account_code only).
    document.querySelectorAll('.coa-line').forEach(line => {
      line.addEventListener('dragstart', e => {
        _draggingCOA = {
          code:   line.dataset.code,
          name:   line.dataset.name || line.dataset.lineDesc || '',
          lineId: parseInt(line.dataset.lineId),
        };
        line.style.opacity = '.5';
        e.dataTransfer.effectAllowed = 'copy';
        e.dataTransfer.setData('text/plain', 'line:' + line.dataset.lineId);
        e.stopPropagation();  // don't bubble to section
      });
      line.addEventListener('dragend', () => {
        line.style.opacity = '';
        _draggingCOA = null;
      });
    });
    document.querySelectorAll('.actuals-txn-row').forEach(row => {
      row.addEventListener('dragover', e => {
        if (!_draggingCOA) return;
        e.preventDefault();  // allow drop
        e.dataTransfer.dropEffect = 'copy';
        row.style.outline = '2px solid var(--blue)';
        row.style.outlineOffset = '-2px';
      });
      row.addEventListener('dragleave', () => {
        row.style.outline = '';
      });
      row.addEventListener('drop', async e => {
        e.preventDefault();
        row.style.outline = '';
        if (!_draggingCOA) return;
        const tid    = parseInt(row.dataset.tid);
        const code   = _draggingCOA.code;
        const name   = _draggingCOA.name;
        const lineId = _draggingCOA.lineId;
        try {
          // Specific line dragged → /set-line (full link, triggers
          // Working→Actual auto-clone on first link).
          // Section dragged → /set-coa (account_code only).
          let url, body;
          if (lineId) {
            url  = `/projects/${PROJ_ID}/actuals/transaction/${tid}/set-line`;
            body = JSON.stringify({ budget_line_id: lineId });
          } else {
            url  = `/projects/${PROJ_ID}/actuals/transaction/${tid}/set-coa`;
            body = JSON.stringify({ account_code: code, account_code_name: name });
          }
          const r = await fetch(url, {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body,
          });
          const d = await r.json();
          if (!r.ok) {
            _actualsToast('Could not code: ' + (d.error || r.status), 'red');
            return;
          }
          if (code === 'not_project') {
            _actualsToast('Marked as not a project expense.', 'yellow');
            row.dataset.notProject = '1';
            row.dataset.coded      = '0';
            row.style.opacity = '.65';
          } else if (lineId) {
            // Auto-init toasts mirror actualsSetLine.
            if (d.working_was_just_created && d.actual_was_just_created) {
              _actualsToast('Working budget initialized from Estimated. Actual budget started for live tracking.');
            } else if (d.actual_was_just_created) {
              _actualsToast('Actual budget started — Working was cloned for live tracking.');
            } else {
              _actualsToast(`Linked to ${code} · ${name}`, 'green');
            }
            row.dataset.coded     = '1';
            row.style.background  = '#0d1a14';
            row.style.borderColor = '#1a3a2a';
            // Sync the dropdown to reflect the new line. The select's
            // value uses the Working line id, not the Actual id, so we
            // can't always set it perfectly without a server hint, but
            // try anyway — the dropdown options only carry working ids.
            const sel = row.querySelector('.actuals-line-picker');
            if (sel) {
              const wantId = String(lineId);
              let _ddFound = false;
              for (const opt of sel.options) {
                if (opt.value === wantId) { sel.value = wantId; _ddFound = true; break; }
              }
              if (!_ddFound) {
                const _ddOpt = document.createElement('option');
                _ddOpt.value = wantId;
                _ddOpt.textContent = 'Assigned line';
                sel.appendChild(_ddOpt);
                sel.value = wantId;
              }
              // Prevent the focus-fill from reverting to stale dataset.current.
              // (Review 2026-06-04.)
              sel.dataset.current = wantId;
            }
          } else {
            // Section-only drag — surface auto-create signals from the
            // server. budget_line_auto_created means we just inserted
            // a placeholder line under this section in the Working
            // (and Actual, if extant) budget so the section will now
            // appear in the Budget view.
            if (d.working_was_just_created && d.budget_line_auto_created) {
              _actualsToast(`Working budget initialized + ${code} ${name} added.`, 'green');
            } else if (d.budget_line_auto_created) {
              _actualsToast(`Coded to ${code} · ${name} (added to budget)`, 'green');
            } else {
              _actualsToast(`Coded to ${code} · ${name}`, 'green');
            }
            row.dataset.coded     = '1';
            row.style.borderColor = '#3a5a3a';
            // Sync the dropdown so the user sees the section selection.
            // The dropdown's section-only option uses the value
            // 'section:<code>' (see actualsSetLine). If the option
            // doesn't exist (auto-added section that wasn't in the
            // server-rendered options), inject it inline rather than
            // forcing a page reload — a reload nukes the user's filter
            // state, which made it look like the row "disappeared"
            // from the Finished filter when they were just reassigning.
            const sel = row.querySelector('.actuals-line-picker');
            if (sel) {
              const want = 'section:' + code;
              let found = false;
              for (const opt of sel.options) {
                if (opt.value === want) { sel.value = want; found = true; break; }
              }
              if (!found) {
                const newOpt = document.createElement('option');
                newOpt.value = want;
                newOpt.textContent = `All ${name} (section-only, no specific line)`;
                sel.appendChild(newOpt);
                sel.value = want;
              }
            }
            // Match-status was already 'confirmed' if the row was
            // Finished — /set-coa doesn't touch it, so the row stays
            // in the Finished bucket. Defensive: ensure the dataset
            // matches the server's behavior.
            if (!row.dataset.matchStatus) {
              row.dataset.matchStatus = 'unmatched';
            }
          }
          _actualsRecountStats();
        } catch (err) {
          _actualsToast('Drop failed: ' + err.message, 'red');
        }
      });
    });

    // ── Suggested-match banner: Confirm / Override ─────────────────────
    window.actualsConfirmMatch = async function (tid) {
      try {
        const r = await fetch(`/projects/${PROJ_ID}/actuals/transaction/${tid}/confirm-match`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}',
        });
        const d = await r.json();
        if (!r.ok) { _actualsToast('Confirm failed: ' + (d.error || r.status), 'red'); return; }
        // Handle JUST this line — no full reload, view & filter preserved.
        const banner = document.querySelector(`.actuals-suggested-banner[data-tid="${tid}"]`);
        if (banner) banner.remove();
        const row = document.querySelector(`.actuals-txn-row[data-tid="${tid}"]`);
        if (row) {
          row.dataset.matchStatus = 'confirmed';
          row.style.transition = 'box-shadow .3s';
          row.style.boxShadow = 'inset 0 0 0 2px #2a5a3a';
          setTimeout(() => { row.style.boxShadow = ''; }, 1400);
        }
        // The receipt's own placeholder row was merged away on confirm — drop it.
        if (d.merged_doc_txn) {
          const sister = document.querySelector(`.actuals-txn-row[data-tid="${d.merged_doc_txn}"]`);
          if (sister) sister.remove();
        }
        if (typeof _actualsRefreshReviewBtn === 'function') _actualsRefreshReviewBtn();
        if (typeof _actualsRecountStats === 'function') _actualsRecountStats();
        const rc = document.getElementById('actuals-review-count');
        if (rc) rc.textContent = document.querySelectorAll('.actuals-suggested-banner').length;
        const hc = document.getElementById('actuals-review-hc');
        if (hc) hc.textContent = [...document.querySelectorAll('.actuals-suggested-banner')].filter(b => parseFloat(b.dataset.confidence || 0) >= 0.9).length;
        _actualsToast('Match confirmed', 'green');
      } catch (e) {
        _actualsToast('Confirm error: ' + e.message, 'red');
      }
    };
    window.actualsDismissSuggestion = async function (tid) {
      try {
        const r = await fetch(`/projects/${PROJ_ID}/actuals/transaction/${tid}/dismiss-suggestion`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}',
        });
        if (!r.ok) {
          const d = await r.json().catch(() => ({}));
          _actualsToast('Could not dismiss: ' + (d.error || r.status), 'red');
          return;
        }
        // Hide the banner inline — no full reload needed.
        const banner = document.querySelector(`.actuals-suggested-banner[data-tid="${tid}"]`);
        if (banner) banner.remove();
        const row = document.querySelector(`.actuals-txn-row[data-tid="${tid}"]`);
        if (row) {
          row.dataset.matchStatus = 'unmatched';
          row.dataset.hasDoc = '0';
          // Remove the doc badge too — leaving it meant later doc edits
          // patched a row that no longer carries that doc. (Review 2026-06-04.)
          const _bdg = row.querySelector('.actuals-doc-badge'); if (_bdg) _bdg.remove();
        }
        _actualsRecountStats();
        // Keep the Review-mode counters in sync (Confirm already did this;
        // Dismiss didn't). (Review 2026-06-04.)
        if (typeof _actualsRefreshReviewBtn === 'function') _actualsRefreshReviewBtn();
        const _rc = document.getElementById('actuals-review-count');
        if (_rc) _rc.textContent = document.querySelectorAll('.actuals-suggested-banner').length;
        const _hc = document.getElementById('actuals-review-hc');
        if (_hc) _hc.textContent = [...document.querySelectorAll('.actuals-suggested-banner')].filter(b => parseFloat(b.dataset.confidence || 0) >= 0.9).length;
        _actualsToast('Marked “not a match” — separated; won’t be suggested again', 'yellow');
      } catch (e) {
        _actualsToast('Error: ' + e.message, 'red');
      }
    };

    // ── Auto-Match button ──────────────────────────────────────────────
    window.actualsRunAutoMatch = async function () {
      const btn = document.getElementById('actualsAutoMatchBtn');
      if (btn) { btn.disabled = true; btn.textContent = 'Matching…'; }
      try {
        const r = await fetch(`/projects/${PROJ_ID}/actuals/auto-match`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}',
        });
        const d = await r.json();
        if (!r.ok) {
          _actualsToast('Auto-match failed: ' + (d.error || r.status), 'red');
          return;
        }
        if (d.suggestions === 0) {
          _actualsToast(`Auto-match ran — no candidate pairings found (${d.qbo_unmatched} unmatched QBO × ${d.doc_open} open docs).`, 'yellow');
        } else {
          _actualsToast(`Auto-match found ${d.suggestions} suggestion${d.suggestions !== 1 ? 's' : ''}. Reloading…`, 'green');
          setTimeout(() => window.location.reload(), 800);
        }
      } catch (e) {
        _actualsToast('Auto-match error: ' + e.message, 'red');
      } finally {
        if (btn) { btn.disabled = false; btn.textContent = '⚡ Auto-Match'; }
      }
    };

    // ── Find matches: configurable candidate review (User 2026-06-16) ─────
    // Adjustable amount/date/vendor → per-charge shortlists with confidence.
    // Suggests the top when ≥80% & not a near-tie; always shows alternatives;
    // confirm or switch. Confirm = link-doc then confirm-match (merge).
    window._fmcBuild = function () {
      let m = document.getElementById('match-review-modal');
      if (m) return m;
      m = document.createElement('div');
      m.id = 'match-review-modal';
      m.style.cssText = 'display:none;position:fixed;inset:0;z-index:10000;background:rgba(0,0,0,.8)';
      m.innerHTML = `
        <style>
          .mr-chip{font-size:.62rem;padding:1px 6px;border-radius:4px;white-space:nowrap}
          .mr-chip.ok{background:#10231a;color:#5fd0a0;border:1px solid #1f6f4a}
          .mr-chip.warn{background:#2a2414;color:#e0c060;border:1px solid #5a4520}
          .mr-chip.mut{background:var(--bg-input,#22263a);color:var(--text-muted)}
        </style>
        <div style="position:absolute;top:3vh;left:50%;transform:translateX(-50%);width:880px;max-width:96vw;
                    height:94vh;display:flex;flex-direction:column;background:var(--bg-card,#1a1d27);
                    border:1px solid var(--border);border-radius:12px;overflow:hidden">
          <div style="display:flex;align-items:center;gap:12px;padding:12px 16px;border-bottom:1px solid var(--border)">
            <h3 style="margin:0;font-size:1rem">🎯 Find matches</h3>
            <span id="mr-count" style="font-size:.8rem;color:var(--text-muted)"></span>
            <span style="margin-left:auto"></span>
            <button type="button" class="btn btn-sm btn-primary" id="mr-confirm-hc" style="display:none"
                    title="Confirm every ⚡ suggested match at 80%+ confidence in one go" onclick="mrConfirmAllHigh()">✓ Confirm all high-confidence (<span id="mr-hc-n">0</span>)</button>
            <button type="button" class="btn btn-sm btn-ghost" onclick="document.getElementById('match-review-modal').style.display='none'">✕ Close</button>
          </div>
          <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;padding:10px 16px;border-bottom:1px solid var(--border);background:var(--bg-input,#22263a)">
            <span style="font-size:.78rem;color:var(--text-muted)">💲 Amount</span><span id="mr-amt"></span>
            <input type="number" id="mr-amt-custom" value="25" step="1" style="width:68px;display:none;font-size:.76rem;padding:3px 6px;background:var(--bg-card,#1a1d27);color:var(--text);border:1px solid var(--border);border-radius:5px">
            <span style="font-size:.78rem;color:var(--text-muted)">📅 Date</span><span id="mr-date"></span>
            <label style="font-size:.78rem;display:flex;align-items:center;gap:5px;cursor:pointer"><input type="checkbox" id="mr-vendor"> require vendor</label>
            <span style="font-size:.78rem;color:var(--text-muted)">💳 Card</span>
            <select id="mr-card" onchange="mrFind()" style="font-size:.76rem;padding:4px 6px;background:var(--bg-card,#1a1d27);color:var(--text);border:1px solid var(--border);border-radius:5px"><option value="">Any card</option></select>
            <button type="button" class="btn btn-sm btn-primary" id="mr-find" onclick="mrFind()" style="margin-left:auto">Find</button>
          </div>
          <div id="mr-results" style="flex:1;overflow-y:auto;padding:12px 16px"></div>
        </div>`;
      document.body.appendChild(m);
      m.addEventListener('click', e => { if (e.target === m) m.style.display = 'none'; });
      const mkSeg = (host, opts, def) => {
        host.style.cssText = 'display:inline-flex;border:1px solid var(--border);border-radius:6px;overflow:hidden';
        host.innerHTML = opts.map(o => `<button type="button" data-v="${o.v}" class="${o.v===def?'on':''}" style="font-size:.74rem;padding:4px 9px;background:${o.v===def?'var(--blue)':'transparent'};color:${o.v===def?'#fff':'var(--text-muted)'};border:none;border-right:1px solid var(--border);cursor:pointer">${o.l}</button>`).join('');
        host.querySelectorAll('button').forEach(b => b.onclick = () => {
          host.querySelectorAll('button').forEach(x => { x.classList.remove('on'); x.style.background='transparent'; x.style.color='var(--text-muted)'; });
          b.classList.add('on'); b.style.background='var(--blue)'; b.style.color='#fff';
          if (host.id === 'mr-amt') document.getElementById('mr-amt-custom').style.display = (b.dataset.v==='custom'?'':'none');
        });
      };
      mkSeg(m.querySelector('#mr-amt'), [{v:'0',l:'Exact'},{v:'1',l:'±$1'},{v:'5',l:'±$5'},{v:'10',l:'±$10'},{v:'custom',l:'Custom'}], '0');
      mkSeg(m.querySelector('#mr-date'), [{v:'off',l:'Any'},{v:'1',l:'±1d'},{v:'3',l:'±3d'},{v:'5',l:'±5d'},{v:'7',l:'±7d'},{v:'30',l:'±30d'}], 'off');
      return m;
    };
    window.openMatchReview = function () { const m = window._fmcBuild(); m.style.display = ''; mrFind(); };
    function _fmcSeg(id){ const b=document.querySelector('#'+id+' button.on'); return b?b.dataset.v:null; }
    function _fmcFmt(n){ return Number(n).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}); }
    window.mrFind = async function () {
      const amtSel=_fmcSeg('mr-amt');
      const amount_tol = amtSel==='custom' ? (parseFloat(document.getElementById('mr-amt-custom').value)||0) : parseFloat(amtSel);
      const dSel=_fmcSeg('mr-date');
      const date_window = dSel==='off' ? 'off' : parseInt(dSel);
      const use_vendor = document.getElementById('mr-vendor').checked;
      const card = (document.getElementById('mr-card')||{}).value || '';
      const host = document.getElementById('mr-results');
      host.innerHTML = '<div class="muted" style="padding:20px;text-align:center">Searching…</div>';
      try {
        // Run the 1:1 candidate search and the split detector together.
        const [r, sr] = await Promise.all([
          fetch(`/projects/${PROJ_ID}/actuals/match-candidates`, {
            method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({ amount_tol, date_window, use_vendor, card }) }),
          fetch(`/projects/${PROJ_ID}/actuals/split-candidates`, {
            method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({ date_window: (date_window==='off'?14:date_window), card }) }),
        ]);
        const d = await r.json();
        if (!r.ok) { host.innerHTML = '<div style="padding:20px;color:#e08080">Error: '+(d.error||r.status)+'</div>'; return; }
        let splits = [];
        try { const sd = await sr.json(); if (sr.ok) splits = sd.splits || []; } catch (e) {}
        // Populate the card dropdown from the available cards (keep selection).
        const cardSel = document.getElementById('mr-card');
        if (cardSel && d.available_cards) {
          const cur = cardSel.value;
          cardSel.innerHTML = '<option value="">Any card</option>' +
            d.available_cards.map(c => `<option value="${c}"${c===cur?' selected':''}>•• ${c}</option>`).join('');
          cardSel.value = cur;
        }
        mrRender(d, splits);
      } catch (e) { host.innerHTML = '<div style="padding:20px;color:#e08080">Error: '+e.message+'</div>'; }
    };
    window._mrSplits = {};
    window.mrRender = function (d, splits) {
      const host = document.getElementById('mr-results');
      splits = splits || [];
      window._mrSplits = {};
      document.getElementById('mr-count').textContent = (d.charges_with_candidates||0) + ' charges with candidates'
        + (splits.length ? ` · ${splits.length} possible split${splits.length!==1?'s':''}` : '');
      const esc = s => (s||'').toString().replace(/</g,'&lt;');
      // ── Possible splits (one receipt = sum of several charges) ──
      let splitHtml = '';
      if (splits.length) {
        splitHtml = '<div style="font-size:.72rem;text-transform:uppercase;letter-spacing:.05em;color:#9bc0ff;margin:0 0 8px">📑 Possible splits — one receipt backs several charges</div>'
          + splits.map(s => {
            window._mrSplits[s.doc.id] = s.charges.map(c => c.tid);
            const thumb = s.doc.is_image ? `<img loading="lazy" src="/docs/upload/${s.doc.id}/raw" style="width:38px;height:48px;object-fit:cover;border-radius:4px">` : '<span style="width:38px;height:48px;display:inline-flex;align-items:center;justify-content:center;background:var(--bg-input,#22263a);border-radius:4px">🧾</span>';
            const rows = s.charges.map(c => `<div style="display:flex;gap:8px;font-size:.74rem;padding:2px 0"><span style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(c.vendor)} · ${c.date||''}${c.card?' · ••'+c.card:''}</span><span style="font-variant-numeric:tabular-nums">$${_fmcFmt(c.amount)}</span>${c.same_card?'<span class="mr-chip ok">card ✓</span>':''}</div>`).join('');
            return `<div class="mr-card" data-split="${s.doc.id}" style="border:1px solid #2d4a7a;border-radius:8px;padding:10px 12px;margin-bottom:10px;background:#0f1830">
              <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">${thumb}
                <div style="flex:1;min-width:0">
                  <div style="font-weight:600">${esc(s.doc.vendor)||'Receipt'} · $${_fmcFmt(s.total != null ? s.total : s.doc.amount)}<span class="mr-chip ok" style="margin-left:8px">= sum of ${s.n} charges ✓</span></div>
                  <div style="font-size:.72rem;color:var(--text-muted)">${esc(s.doc.file)}${s.doc.card?' · ••'+s.doc.card:''} · ${s.doc.date||''}</div>
                </div>
                <span style="display:flex;gap:6px">
                  <button type="button" class="btn btn-xs btn-primary" onclick="mrAcceptSplit(${s.doc.id}, this)">✓ Link as split</button>
                  <button type="button" class="btn btn-xs btn-ghost" onclick="mrSkipSplit(${s.doc.id})">Skip</button>
                </span>
              </div>
              <div style="border-top:1px solid var(--border);padding-top:6px">${rows}</div></div>`;
          }).join('') + (d.results && d.results.length ? '<div style="height:8px;border-bottom:1px solid var(--border);margin-bottom:12px"></div>' : '');
      }
      if ((!d.results || !d.results.length) && !splits.length) {
        host.innerHTML = '<div class="muted" style="padding:24px;text-align:center">No candidates within these criteria — loosen the amount or date.</div>';
        const hcBtn0 = document.getElementById('mr-confirm-hc'); if (hcBtn0) hcBtn0.style.display = 'none';
        return;
      }
      // High-confidence first so the slam-dunks are right at the top.
      const HIGH = 0.8;
      const results = (d.results || []).slice().sort((a, b) => (b.top_confidence || 0) - (a.top_confidence || 0));
      host.innerHTML = splitHtml + results.map(res => {
        const c = res.charge; const conf = Math.round((res.top_confidence||0)*100);
        const cands = res.candidates.map((cd,i) => {
          const sel = res.suggested_doc_id ? cd.doc_upload_id===res.suggested_doc_id : i===0;
          const amtChip = cd.amount_delta<0.005 ? '<span class="mr-chip ok">exact $</span>' : '<span class="mr-chip warn">$'+_fmcFmt(cd.amount_delta)+' off</span>';
          const dateChip = cd.day_gap===0 ? '<span class="mr-chip ok">same day</span>' : '<span class="mr-chip mut">'+cd.day_gap+'d apart</span>';
          const vChip = cd.vendor_match ? '<span class="mr-chip ok">vendor ✓</span>' : '<span class="mr-chip mut">diff vendor</span>';
          const refundChip = cd.opposite_sign ? '<span class="mr-chip warn" title="Receipt and charge have opposite signs — likely a refund/credit">↩ refund?</span>' : '';
          const thumb = cd.is_image ? `<img loading="lazy" src="/docs/upload/${cd.doc_upload_id}/raw" style="width:38px;height:48px;object-fit:cover;border-radius:4px">` : '<span style="width:38px;height:48px;display:inline-flex;align-items:center;justify-content:center;background:var(--bg-input,#22263a);border-radius:4px">🧾</span>';
          return `<label style="display:flex;align-items:center;gap:10px;padding:7px 8px;border-radius:6px;cursor:pointer;${sel?'background:#10231a;border:1px solid #1f6f4a':'border:1px solid var(--border)'}">
            <input type="radio" name="mr-${c.id}" value="${cd.doc_upload_id}" ${sel?'checked':''}>${thumb}
            <span style="flex:1;min-width:0">
              <span style="font-size:.78rem;font-weight:500;display:block;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(cd.file)||'receipt'}</span>
              <span style="font-size:.72rem;color:var(--text-muted)">${esc(cd.vendor)} · $${_fmcFmt(cd.amount)} · ${cd.date||''}${cd.card?' · ••'+cd.card:''}</span>
              <span style="display:flex;gap:5px;margin-top:2px">${amtChip}${dateChip}${vChip}${refundChip}</span>
            </span>
            <a href="#" onclick="event.preventDefault();window.open('/docs/upload/${cd.doc_upload_id}/raw','_blank')" style="font-size:.7rem;color:#5b8af9">open</a></label>`;
        }).join('');
        const badge = res.suggested_doc_id ? `<span class="mr-chip ok" style="font-size:.7rem">⚡ suggested · ${conf}%</span>`
                                           : `<span class="mr-chip warn" style="font-size:.7rem">pick one · top ${conf}%</span>`;
        return `<div class="mr-card" data-cid="${c.id}" data-conf="${res.top_confidence||0}" data-suggested="${res.suggested_doc_id||''}" style="border:1px solid var(--border);border-radius:8px;padding:10px 12px;margin-bottom:10px;background:var(--bg-card,#1a1d27)">
          <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;flex-wrap:wrap">
            <span style="font-weight:600">${esc(c.vendor)||'Unknown'}</span>
            <span style="font-size:.78rem;color:var(--text-muted)">$${_fmcFmt(c.amount)} · ${c.date||''}${c.card4?' · ••'+c.card4:''}</span>${badge}
            <span style="margin-left:auto;display:flex;gap:6px">
              <button type="button" class="btn btn-xs btn-primary" onclick="mrConfirm(${c.id}, this)">✓ Confirm</button>
              <button type="button" class="btn btn-xs btn-ghost" onclick="mrSkip(${c.id})">Skip</button></span>
          </div>
          <div style="display:flex;flex-direction:column;gap:6px">${cands}</div></div>`;
      }).join('');
      // Surface the bulk "confirm all high-confidence" button + count.
      const hcN = results.filter(r => r.suggested_doc_id && (r.top_confidence || 0) >= HIGH).length;
      const hcBtn = document.getElementById('mr-confirm-hc');
      if (hcBtn) {
        hcBtn.style.display = hcN ? '' : 'none';
        const nEl = document.getElementById('mr-hc-n'); if (nEl) nEl.textContent = hcN;
      }
    };
    // Confirm every ⚡ suggested match at ≥80% in one pass (sequential to avoid
    // hammering the server). Each uses the pre-selected (suggested) receipt. (User 2026-06-17.)
    window.mrConfirmAllHigh = async function () {
      const HIGH = 0.8;
      const cards = [...document.querySelectorAll('.mr-card[data-cid]')]
        .filter(card => card.dataset.suggested && parseFloat(card.dataset.conf || 0) >= HIGH);
      if (!cards.length) { _actualsToast('No high-confidence matches to confirm.', 'yellow'); return; }
      if (!confirm(`Confirm ${cards.length} high-confidence match${cards.length !== 1 ? 'es' : ''}? Each links its ⚡ suggested receipt — you can still undo any from the Activity tab.`)) return;
      const hcBtn = document.getElementById('mr-confirm-hc');
      if (hcBtn) { hcBtn.disabled = true; }
      let ok = 0, fail = 0;
      for (const card of cards) {
        const cid = card.dataset.cid;
        if (hcBtn) hcBtn.innerHTML = `Confirming… ${ok + fail + 1}/${cards.length}`;
        const sel = card.querySelector('input[name="mr-' + cid + '"]:checked')
                 || card.querySelector('input[name="mr-' + cid + '"]');
        if (!sel) { fail++; continue; }
        try {
          const r = await fetch(`/projects/${PROJ_ID}/actuals/transaction/${cid}/link-doc`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ doc_upload_id: parseInt(sel.value) }) });
          if (!r.ok) { fail++; continue; }
          await fetch(`/projects/${PROJ_ID}/actuals/transaction/${cid}/confirm-match`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' });
          ok++;
          card.style.transition = 'opacity .25s'; card.style.opacity = '0';
          setTimeout(() => card.remove(), 250);
        } catch (e) { fail++; }
      }
      _actualsToast(`Confirmed ${ok} match${ok !== 1 ? 'es' : ''}${fail ? ` · ${fail} failed` : ''}.`, fail ? 'yellow' : 'green');
      if (hcBtn) { hcBtn.disabled = false; }
      mrFind();   // refresh counts + remaining candidates
    };
    window.mrConfirm = async function (cid, btn) {
      const card = document.querySelector('.mr-card[data-cid="'+cid+'"]');
      const sel = card && card.querySelector('input[name="mr-'+cid+'"]:checked');
      if (!sel) { _actualsToast('Pick a receipt first.','yellow'); return; }
      const docId = parseInt(sel.value);
      btn.disabled = true; btn.textContent = '…';
      try {
        let r = await fetch(`/projects/${PROJ_ID}/actuals/transaction/${cid}/link-doc`, {
          method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({doc_upload_id: docId}) });
        if (!r.ok) { const j=await r.json().catch(()=>({})); _actualsToast('Link failed: '+(j.error||r.status),'red'); btn.disabled=false; btn.textContent='✓ Confirm'; return; }
        await fetch(`/projects/${PROJ_ID}/actuals/transaction/${cid}/confirm-match`, {
          method:'POST', headers:{'Content-Type':'application/json'}, body:'{}' });
        card.style.transition='opacity .3s'; card.style.opacity='0'; setTimeout(()=>card.remove(),300);
        _actualsToast('Matched ✓','green');
      } catch (e) { _actualsToast('Error: '+e.message,'red'); btn.disabled=false; btn.textContent='✓ Confirm'; }
    };
    window.mrSkip = function (cid) { const card=document.querySelector('.mr-card[data-cid="'+cid+'"]'); if (card) card.remove(); };
    window.mrSkipSplit = function (docId) { const card=document.querySelector('.mr-card[data-split="'+docId+'"]'); if (card) card.remove(); };
    window.mrAcceptSplit = async function (docId, btn) {
      const tids = (window._mrSplits || {})[docId] || [];
      if (tids.length < 2) { _actualsToast('Split needs at least two charges.', 'yellow'); return; }
      btn.disabled = true; btn.textContent = '…';
      try {
        const r = await fetch(`/projects/${PROJ_ID}/actuals/link-split`, {
          method:'POST', headers:{'Content-Type':'application/json'},
          body: JSON.stringify({ keeper_doc_id: docId, charge_tids: tids }) });
        const j = await r.json().catch(()=>({}));
        if (!r.ok) { _actualsToast('Split failed: '+(j.error||r.status),'red'); btn.disabled=false; btn.textContent='✓ Link as split'; return; }
        const card = document.querySelector('.mr-card[data-split="'+docId+'"]');
        if (card) { card.style.transition='opacity .3s'; card.style.opacity='0'; setTimeout(()=>card.remove(),300); }
        _actualsToast(`Split linked — 1 receipt → ${j.linked} charges ✓`,'green');
      } catch (e) { _actualsToast('Error: '+e.message,'red'); btn.disabled=false; btn.textContent='✓ Link as split'; }
    };

    // ── Duplicate-transaction review (User 2026-06-17) ──────────────────
    // Scans for likely-duplicate charges (QBO re-imports / double-counts),
    // shows each cluster, and lets the user remove the extra(s) or confirm
    // they're genuinely separate. Mirrors the docs duplicate finder.
    window._dtxBuild = function () {
      let m = document.getElementById('dup-txn-modal');
      if (m) return m;
      m = document.createElement('div');
      m.id = 'dup-txn-modal';
      m.style.cssText = 'display:none;position:fixed;inset:0;z-index:10000;background:rgba(0,0,0,.8)';
      m.innerHTML = `
        <style>
          #dup-txn-modal .mr-chip{font-size:.62rem;padding:1px 6px;border-radius:4px;white-space:nowrap}
          #dup-txn-modal .mr-chip.ok{background:#10231a;color:#5fd0a0;border:1px solid #1f6f4a}
          #dup-txn-modal .mr-chip.warn{background:#2a2414;color:#e0c060;border:1px solid #5a4520}
          #dup-txn-modal .mr-chip.mut{background:var(--bg-input,#22263a);color:var(--text-muted)}
        </style>
        <div style="position:absolute;top:3vh;left:50%;transform:translateX(-50%);width:860px;max-width:96vw;height:94vh;display:flex;flex-direction:column;background:var(--bg-card,#1a1d27);border:1px solid var(--border);border-radius:12px;overflow:hidden">
          <div style="display:flex;align-items:center;gap:12px;padding:12px 16px;border-bottom:1px solid var(--border)">
            <h3 style="margin:0;font-size:1rem">🔁 Duplicate transactions</h3>
            <span id="dtx-count" style="font-size:.8rem;color:var(--text-muted)"></span>
            <button type="button" class="btn btn-sm btn-ghost" style="margin-left:auto" onclick="document.getElementById('dup-txn-modal').style.display='none'">✕ Close</button>
          </div>
          <div id="dtx-body" style="flex:1;overflow-y:auto;padding:12px 16px"></div>
        </div>`;
      document.body.appendChild(m);
      m.addEventListener('click', e => { if (e.target === m) m.style.display = 'none'; });
      return m;
    };
    window.openDupTxnReview = async function () {
      const m = window._dtxBuild(); m.style.display = '';
      const body = document.getElementById('dtx-body');
      body.innerHTML = '<div class="muted" style="padding:24px;text-align:center">Scanning for duplicate transactions…</div>';
      try {
        const d = await (await fetch(`/projects/${PROJ_ID}/actuals/scan-dup-transactions`, {cache:'no-store'})).json();
        window._dtxData = d;
        dtxRender(d);
      } catch (e) { body.innerHTML = '<div style="padding:20px;color:#e08080">Scan failed: ' + e.message + '</div>'; }
    };
    window.dtxRender = function (d) {
      const body = document.getElementById('dtx-body');
      const esc = s => (s || '').toString().replace(/</g, '&lt;');
      const money = a => a == null ? '$?' : ('$' + Number(Math.abs(a)).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2}));
      const clusters = d.clusters || [];
      document.getElementById('dtx-count').textContent = clusters.length
        ? `${clusters.length} suspected set${clusters.length !== 1 ? 's' : ''} · ~${money(d.potential_overcount)} potential double-count`
        : '';
      if (!clusters.length) { body.innerHTML = '<div class="muted" style="padding:28px;text-align:center">✓ No duplicate transactions found.</div>'; return; }
      const kindBadge = k => k === 'reimport' ? '<span class="mr-chip warn">♻ re-import</span>'
                          : k === 'matchable' ? '<span class="mr-chip ok">🔗 should be matched</span>'
                          : k === 'phantom'   ? '<span class="mr-chip warn">♻ duplicate ledger row</span>'
                          : k === 'review'    ? '<span class="mr-chip mut">👀 review manually</span>'
                          : '<span class="mr-chip warn">⚠ possible duplicate</span>';
      // Pick a sensible default keeper: a coded electronic charge, else any
      // electronic charge, else the first row. (Phantom doc_upload rows are
      // the ones we want to drop.)
      const defaultKeep = c => {
        const elec = c.rows.filter(r => r.source !== 'doc_upload');
        return ((elec.find(r => r.coded) || elec[0] || c.rows[0]) || {}).tid;
      };
      body.innerHTML = clusters.map((c, ci) => {
        const keepTid = defaultKeep(c);
        const rows = c.rows.map((r) => `
          <label style="display:flex;align-items:center;gap:9px;padding:6px 8px;border-radius:6px;border:1px solid var(--border);margin-bottom:5px;font-size:.76rem;cursor:pointer">
            <input type="radio" name="dtx-keep-${ci}" value="${r.tid}" ${r.tid === keepTid ? 'checked' : ''} title="Keep this one">
            <span style="flex:1;min-width:0">
              <span style="font-weight:600">${esc(r.vendor) || '—'}</span> · ${money(r.amount)} · ${r.date || ''}${r.card ? (' · ••' + r.card) : ''}
              <span style="color:var(--text-muted)"> · ${esc(r.source)}${r.coded ? (' · ' + esc(r.coded)) : ' · uncoded'}${r.doc_upload_id ? ' · 📎 receipt' : ''}</span>
            </span>
            ${r.phantom ? '<span class="mr-chip warn" title="Redundant — its receipt is already on the charge">phantom</span>'
                        : (r.deletable ? '' : '<span class="mr-chip mut" title="Receipt-linked — keepable but not removable here">protected</span>')}
          </label>`).join('');
        const dismissBtn = `<button class="btn btn-xs btn-ghost" onclick="dtxDismiss('${c.key}',${ci})">Keep all — separate</button>`;
        let actions, note = '';
        if (c.kind === 'matchable') {
          actions = `<button class="btn btn-xs btn-primary" onclick="document.getElementById('dup-txn-modal').style.display='none';openMatchReview()">🔗 Match in Find-matches</button>${dismissBtn}`;
          note = 'An unmatched charge + a receipt for the same spend — link them in Find-matches.';
        } else if (c.kind === 'phantom') {
          actions = `<button class="btn btn-xs btn-primary" onclick="dtxRemoveOthers(${ci})">🗑 Remove the redundant row</button>${dismissBtn}`;
          note = 'The receipt is already attached to the bank charge — the extra doc_upload row double-counts it. Keep the charge, drop the extra.';
        } else if (c.kind === 'review') {
          actions = dismissBtn;
          note = 'These are already matched/coded in different ways — review them on the Actuals list. Nothing safe to auto-remove here.';
        } else {
          actions = `<button class="btn btn-xs btn-primary" onclick="dtxRemoveOthers(${ci})">🗑 Remove the others</button>${dismissBtn}`;
        }
        return `<div class="mr-card" data-dtx="${ci}" style="border:1px solid var(--border);border-radius:8px;padding:10px 12px;margin-bottom:12px;background:var(--bg-card,#1a1d27)">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px;flex-wrap:wrap">
            ${kindBadge(c.kind)} <span style="font-weight:600">${money(c.amount)}</span>
            <span style="color:var(--text-muted);font-size:.74rem">× ${c.rows.length} copies</span>
            <span style="margin-left:auto;display:flex;gap:6px">${actions}</span>
          </div>
          ${rows}
          ${note ? ('<div style="font-size:.7rem;color:var(--text-muted);margin-top:4px">' + note + '</div>') : ''}
        </div>`;
      }).join('');
    };
    window.dtxDismiss = async function (key, ci) {
      try {
        await fetch(`/projects/${PROJ_ID}/actuals/dup-transactions/dismiss`, {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({dup_key:key})});
        const card = document.querySelector('.mr-card[data-dtx="' + ci + '"]'); if (card) card.remove();
        _actualsToast('Marked as separate ✓', 'green');
      } catch (e) { _actualsToast('Error: ' + e.message, 'red'); }
    };
    window.dtxRemoveOthers = async function (ci) {
      const card = document.querySelector('.mr-card[data-dtx="' + ci + '"]');
      const keep = card && card.querySelector('input[name="dtx-keep-' + ci + '"]:checked');
      if (!keep) { _actualsToast('Pick which one to keep.', 'yellow'); return; }
      const keepTid = parseInt(keep.value);
      const cluster = (window._dtxData.clusters || [])[ci] || {rows:[]};
      const toDelete = cluster.rows.filter(r => r.tid !== keepTid && r.deletable);
      if (!toDelete.length) {
        _actualsToast('The other copies are receipt-linked — unmatch them first, or match instead.', 'yellow'); return;
      }
      if (!confirm(`Remove ${toDelete.length} duplicate transaction${toDelete.length > 1 ? 's' : ''} (keeping the selected one)? This deletes the extra ledger row${toDelete.length > 1 ? 's' : ''} — logged in Activity.`)) return;
      let ok = 0, fail = 0;
      for (const r of toDelete) {
        try {
          const resp = await fetch(`/projects/${PROJ_ID}/actuals/transaction/${r.tid}/delete-duplicate`, {method:'POST', headers:{'Content-Type':'application/json'}, body:'{}'});
          if (resp.ok) ok++; else fail++;
        } catch (e) { fail++; }
      }
      _actualsToast(`Removed ${ok} duplicate${ok !== 1 ? 's' : ''}${fail ? (' · ' + fail + ' failed') : ''}.`, fail ? 'yellow' : 'green');
      if (card) card.remove();
      if (ok && !fail) setTimeout(() => location.reload(), 900);
    };

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
      const esc = s => (s || '').toString().replace(/</g, '&lt;');
      const money = a => a == null ? '$?' : ('$' + Number(Math.abs(a)).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2}));
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
        // Compare the WHOLE set (all receipts in this cluster), not just the
        // file-hash-identical ones. (User 2026-06-17.)
        const recIds = c.receipts.map(r => r.doc_id).join(',');
        const _thumb = r => r.is_image
          ? `<img loading="lazy" src="/docs/upload/${r.doc_id}/raw" title="Click to view / compare all" onclick="event.preventDefault();event.stopPropagation();docsOpenCompareSet([${recIds}])" style="width:30px;height:38px;object-fit:cover;border-radius:4px;cursor:zoom-in;flex-shrink:0">`
          : `<span title="Click to view / compare all" onclick="event.preventDefault();event.stopPropagation();docsOpenCompareSet([${recIds}])" style="width:30px;height:38px;display:inline-flex;align-items:center;justify-content:center;background:var(--bg-input,#22263a);border-radius:4px;cursor:zoom-in;flex-shrink:0">📄</span>`;
        const recRows = c.receipts.map(r => `
          <label class="rc-row">
            ${c.receipts.length > 1 ? `<input type="radio" name="rc-rec-${ci}" value="${r.doc_id}" ${r.doc_id === recKeep ? 'checked' : ''}>` : '<span style="width:13px"></span>'}
            ${_thumb(r)}
            <span style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(r.file) || ('Receipt #' + r.doc_id)} <span style="color:var(--text-muted)">· ${money(r.amount)}</span></span>
            ${r.dup ? '<span class="mr-chip warn">identical copy</span>' : ''}${r.on_charge ? '<span class="mr-chip ok">✓ attached to a charge</span>' : ''}
          </label>`).join('');
        const recAction = c.receipts.length > 1
          ? `<button class="btn btn-xs btn-ghost" onclick="docsOpenCompareSet([${recIds}])" title="Open all of these side by side">⇆ Compare</button>
             <button class="btn btn-xs btn-primary" onclick="rcMergeReceipts(${ci})">Keep selected · merge the rest</button>` : '';
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

    // ── Reprocess unmatched receipts (background re-OCR) ────────────────
    // Re-OCRs every receipt not yet paired to a charge. Resumable + DB-backed
    // (survives a server worker recycle): we poll row-count progress and
    // re-kick if a worker dies mid-run. Uses Veryfi credits. (User 2026-06-15.)
    window.actualsReprocessReceipts = async function () {
      const btn = document.getElementById('actualsReprocessBtn');
      const prog = document.getElementById('actualsReprocessProg');
      if (btn && btn.dataset.running === '1') return;   // already polling
      if (!confirm('Re-run OCR on all unmatched receipts?\n\nThis refreshes vendor / amount / date, converts foreign currency to USD, then re-matches. It uses Veryfi credits (one per receipt) and runs in the background — you can keep working. Already-processed receipts are skipped, so it’s safe to run again.')) return;
      const start = async (reset) => {
        const r = await fetch(`/admin/docs/project/${PROJ_ID}/reprocess-unpaired`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ limit: 1000, reset: reset ? 1 : 0 }),
        });
        return r.json().catch(() => ({}));
      };
      const status = async () => {
        const r = await fetch(`/admin/docs/project/${PROJ_ID}/reprocess-unpaired-status`, { cache: 'no-store' });
        return r.json().catch(() => ({}));
      };
      if (btn) { btn.dataset.running = '1'; btn.disabled = true; }
      try {
        await start(false);
        _actualsToast('Reprocessing receipts in the background…', 'green');
        let stalls = 0, lastProcessed = -1, firstSuggestions = null;
        // Poll until the queue drains. Survives the status hitting any worker.
        for (let i = 0; i < 1200; i++) {        // ~80 min ceiling at 4s
          await new Promise(res => setTimeout(res, 4000));
          const s = await status();
          if (s && typeof s.total === 'number' && s.total > 0) {
            if (prog) prog.textContent = `${s.processed}/${s.total}`;
            if (firstSuggestions === null && s.auto_match_suggestions != null) firstSuggestions = s.auto_match_suggestions;
            if (s.remaining === 0) {            // done
              if (prog) prog.textContent = '';
              const made = (s.auto_match_suggestions != null) ? s.auto_match_suggestions : null;
              _actualsToast('Reprocess complete — ' + s.processed + ' receipts re-OCR’d'
                + (made != null ? `, ${made} new suggestion${made !== 1 ? 's' : ''}.` : '.') + ' Reloading…', 'green');
              setTimeout(() => window.location.reload(), 1400);
              return;
            }
            // Stall detection: no forward progress AND not active → a worker
            // likely recycled. Re-kick to resume the remainder (idempotent).
            if (s.processed === lastProcessed && !s.active) {
              if (++stalls >= 3) { await start(false); stalls = 0; _actualsToast('Resuming reprocess…', 'yellow'); }
            } else { stalls = 0; }
            lastProcessed = s.processed;
          }
        }
        _actualsToast('Reprocess still running — check back shortly.', 'yellow');
      } catch (e) {
        _actualsToast('Reprocess error: ' + e.message, 'red');
      } finally {
        if (btn) { btn.dataset.running = '0'; btn.disabled = false; }
      }
    };

    // ── Edit Transaction modal ─────────────────────────────────────────
    // If the row has a linked DocUpload, open the existing Docs detail
    // modal (with image preview + zoom + per-type fields). Otherwise
    // fall back to the plain text edit modal for non-doc transactions
    // (manual entries, QBO-only without a receipt).
    let _editTxnId = null;
    window.actualsOpenEditTxn = function (tid, docId) {
      if (docId && typeof window.openDocDetail === 'function') {
        window.openDocDetail(docId, null);
        return;
      }
      const row = document.querySelector(`.actuals-txn-row[data-tid="${tid}"]`);
      if (!row) return;
      _editTxnId = tid;
      // Pre-populate from the row's current cell text. Cleaner than
      // round-tripping a GET — the row already has the canonical state.
      document.getElementById('editTxnVendor').value = row.querySelector('.actuals-txn-vendor')?.textContent.trim() || '';
      document.getElementById('editTxnDate').value   = row.dataset.txnDate || '';
      document.getElementById('editTxnAmount').value = row.dataset.amount || '';
      document.getElementById('editTxnNote').value   = row.dataset.note || '';
      document.getElementById('editTxnOverlay').style.display = 'flex';
    };
    window.actualsCloseEditTxn = function () {
      document.getElementById('editTxnOverlay').style.display = 'none';
      _editTxnId = null;
    };
    window.actualsSaveEditTxn = async function () {
      if (!_editTxnId) return;
      const payload = {
        vendor:   document.getElementById('editTxnVendor').value,
        txn_date: document.getElementById('editTxnDate').value,
        amount:   document.getElementById('editTxnAmount').value,
        note:     document.getElementById('editTxnNote').value,
      };
      const btn = document.getElementById('editTxnSaveBtn');
      btn.disabled = true; btn.textContent = 'Saving…';
      try {
        const r = await fetch(`/projects/${PROJ_ID}/actuals/transaction/${_editTxnId}/edit`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload),
        });
        const d = await r.json();
        if (!r.ok) {
          _actualsToast('Save failed: ' + (d.error || r.status), 'red');
          return;
        }
        // Update the row in place — no full reload (was slow + lost the view).
        // (User 2026-06-03.)
        const row = document.querySelector(`.actuals-txn-row[data-tid="${_editTxnId}"]`);
        if (row) {
          const vEl = row.querySelector('.actuals-txn-vendor');
          if (vEl) { vEl.textContent = payload.vendor || '— vendor unknown —'; vEl.title = payload.vendor || ''; }
          const dEl = row.querySelector('.actuals-txn-date');
          if (dEl) dEl.textContent = payload.txn_date || '—';
          row.dataset.txnDate = payload.txn_date || '';
          row.dataset.note = payload.note || '';
          if (payload.amount !== '' && payload.amount != null) {
            row.dataset.amount = payload.amount;
            const amtEl = row.querySelector('.actuals-txn-amt');
            if (amtEl) {
              const neg = amtEl.textContent.trim().charAt(0) === '−';
              amtEl.textContent = (neg ? '−' : '+') + '$' +
                Number(payload.amount || 0).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2});
            }
          }
        }
        // Smart vendor rename: reflect any propagated siblings in the grid and
        // offer a one-click Undo. (User 2026-06-22.)
        const prop = d.vendor_propagation || {count: 0, items: []};
        if (prop.count > 0) {
          (prop.items || []).forEach(it => {
            if (it.type !== 'txn') return;
            const r2 = document.querySelector(`.actuals-txn-row[data-tid="${it.id}"]`);
            if (r2) {
              const v2 = r2.querySelector('.actuals-txn-vendor');
              if (v2) { v2.textContent = payload.vendor || '— vendor unknown —'; v2.title = payload.vendor || ''; }
            }
          });
          _vendorRenameToast(prop, d.old_vendor, payload.vendor);
        } else {
          _actualsToast('Transaction saved.', 'green');
        }
        actualsCloseEditTxn();
      } catch (e) {
        _actualsToast('Save error: ' + e.message, 'red');
      } finally {
        btn.disabled = false; btn.textContent = 'Save';
      }
    };
  })();
  