// Extracted from a templates/budget.html inline block (audit M9, 2026-07-20).
// Jinja values arrive via window.__BJ (set by the inline preamble before this
// script tag). Classic script, document order — semantics identical to inline.

// Cleanup B (2026-11-02): the in-page Docs list (#tab-docs — drop zone, upload
// queue, history, duplicate review, trash) is gone; documents live in the Docs
// inbox (/projects/<pid>/docs, static/docs-inbox.js). This file now carries
// only the budget page's document editor MODAL glue: openDocDetail /
// closeDocDetail, the itemize overrides, and the DocEditor mount.

(function(){
  const PROJ_ID = window.__BJ["b03_PROJ_ID"];

  // ── Document detail modal ────────────────────────────────────────────
  // Opened from a line's receipt link, the People-tab packets, the Dashboard
  // review queue and the Line Ledger (openDocDetail). Loads a Dropbox
  // temporary link and the editable metadata.
  let _docDetailUid = null;

  // Receipt-side transaction matcher, log-expense and re-OCR now live in
  // static/doc-editor.js (window.DocEditor) — ONE editor for both doors
  // (DOCUMENTS_HANDOFF Phase 2). This file keeps the budget-page glue only.

  // Prev/next sequence: the modal used to page through the Docs / Actuals
  // list it was opened from. Those lists left the budget page (Cleanup B,
  // 2026-11-02), so the sequence is always empty here and the buttons stay
  // disabled; docDetailNav is kept because _doc_editor.html binds it.
  let _docNavSeq = [];   // [{uid, row}]
  let _docNavIdx = -1;
  // Dirty tracking lives in DocEditor (the open-time signature); these
  // wrappers keep the nav code below readable.
  function _docDetailDirty() { return window.DocEditor.dirty(); }
  // PO actions: DocEditor pre-saves the document and hands us the
  // context; we close the modal and open the existing PO flows with a
  // synthesized button element carrying the row's data attributes.
  function _docPoCreateGlue(ctx) {
    const fakeBtn = { dataset: { uid: String(ctx.uid), vendor: ctx.vendor || '', amount: ctx.amount || '',
                                 filename: ctx.filename || '', docDate: ctx.docDate || '' } };
    closeDocDetail();
    if (typeof window.openCreatePoFromDoc === 'function') window.openCreatePoFromDoc(fakeBtn);
  }
  function _docPoAddToGlue(ctx) {
    const fakeBtn = { dataset: { uid: String(ctx.uid), vendor: ctx.vendor || '', amount: ctx.amount || '',
                                 filename: ctx.filename || '' } };
    closeDocDetail();
    if (typeof window.openAddToPoFromDoc === 'function') window.openAddToPoFromDoc(fakeBtn);
  }

  // ── Modal prev/next navigation ───────────────────────────────────────
  // No list on this page → nothing to page through (see above).
  function _docCaptureNavSeq(uid) {
    _docNavSeq = [];
    _docNavIdx = -1;
  }
  function _docDetailUpdateNav() {
    const pos  = document.getElementById('docDetailNavPos');
    const prev = document.getElementById('docDetailPrevBtn');
    const nxt  = document.getElementById('docDetailNextBtn');
    const setBtn = (b, disabled) => {
      if (!b) return;
      b.disabled = disabled;
      b.style.opacity = disabled ? '.4' : '';
      b.style.cursor  = disabled ? 'not-allowed' : 'pointer';
    };
    const n = _docNavSeq.length;
    if (_docNavIdx === -1 || n === 0) {
      if (pos) pos.textContent = n ? '—' : '';
      // Not part of a list (e.g. opened from the Action Center) → no paging.
      setBtn(prev, true); setBtn(nxt, true);
      return;
    }
    if (pos) pos.textContent = (_docNavIdx + 1) + ' / ' + n;
    setBtn(prev, _docNavIdx <= 0);
    setBtn(nxt, _docNavIdx >= n - 1);
  }
  window.docDetailNav = async function (delta) {
    // Step through the sequence captured at open time — stable across saves that
    // re-render/re-file the list, so prev/next always know where we are and the
    // buttons never dead-end. (User 2026-06-22.)
    if (!_docNavSeq.length || _docNavIdx < 0) { _docDetailUpdateNav(); return; }
    const nextIdx = _docNavIdx + delta;
    const target = _docNavSeq[nextIdx] || null;   // null = at an end
    // Auto-save current edits before paging away — for BOTH prev AND next.
    if (_docDetailDirty()) {
      const pos = document.getElementById('docDetailNavPos');
      if (pos) pos.textContent = 'Saving…';
      try { await _docDetailCommit(); } catch (e) { /* keep paging */ }
    }
    if (!target) { _docDetailUpdateNav(); return; }   // boundary — stay, restore counter
    _docNavIdx = nextIdx;
    openDocDetail(target.uid, target.row || null, true);   // internalNav=true → keep the sequence
  };
  // Keyboard: ←/→ page through, Esc closes — but never while typing in a
  // field (so caret movement / text entry isn't hijacked).
  document.addEventListener('keydown', (e) => {
    const overlay = document.getElementById('docDetailOverlay');
    if (!overlay || overlay.style.display === 'none') return;
    const tag = (document.activeElement && document.activeElement.tagName) || '';
    if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') {
      if (e.key === 'Escape') document.activeElement.blur();
      return;
    }
    if (e.key === 'ArrowLeft')      { e.preventDefault(); docDetailNav(-1); }
    else if (e.key === 'ArrowRight'){ e.preventDefault(); docDetailNav(1); }
    else if (e.key === 'Escape')    { closeDocDetail(); }
  });

  // ✈️ Travel panel: DocEditor.loadTravel / extractTravel / applyTravel.

  window.openDocDetail = async function (uid, row, internalNav) {
    // Tolerate the legacy (null, docId) call order used by a few inline
    // links (by-dept detail rows, dynamically-rendered doc lists).
    if ((uid == null) && (typeof row === 'number')) { uid = row; row = null; }
    _docDetailUid = uid;
    window._docDetailCurrentUid = uid;
    if (!internalNav) _docCaptureNavSeq(uid);
    _docDetailUpdateNav();   // refresh prev/next position + button states
    // The modal is a top-level, position:fixed element (it sat inside the old
    // Docs pane and was hoisted on first open; hoisting is kept, idempotent).
    const overlay = document.getElementById('docDetailOverlay');
    if (overlay && overlay.parentElement !== document.body) {
      document.body.appendChild(overlay);
    }
    overlay.style.display = 'flex';
    // Instant fill from a row's dataset when a caller passes one (none do on
    // this page any more — kept for the (uid, row) signature); DocEditor
    // fetches /status for the authoritative row and fills again.
    const ds = (row && row.dataset) ? row.dataset : null;
    const seed = ds ? {
      filed_filename: ds.sortFilename || '', vendor: ds.sortVendor || '',
      amount: ds.sortAmount || '', original_amount: ds.originalAmount || '',
      original_currency: ds.originalCurrency || '', doc_date: ds.docDate || '',
      category: (ds.sortType || '').toLowerCase(), note: ds.note || '',
      doc_number: ds.docNum || '', card_last4: ds.sortCard4 || '',
      owner_payee_id: ds.crewMemberId || '', location_id: ds.locationId || '',
    } : null;
    await window.DocEditor.open(uid, seed);
  };

  // ── Itemize into budget lines ─────────────────────────────────────────────
  // The grid (_itemAddRow / _itemUpdateTotal / load / pull / save) lives in
  // DocEditor; the budget page only overrides the subline pick below (it
  // knows the ＋ New-line flow) and keeps the payroll parse handler.
  // Subline budget-line pick: handles the ＋ New-line option, marks dirty, and
  // refreshes the peel-off math (coded vs uncoded). (2026-07-20.)
  window._ditemLineChanged = function (sel) {
    if (sel.value === '__newline__') {
      sel.value = '';
      window._newBudgetLineFlow(function (nl) {
        window._pickerInsertNewLine(nl);
        sel.value = String(nl.line_id);
        window._docItemizeDirty = true;
        _itemUpdateTotal();
      });
      return;
    }
    window._docItemizeDirty = true;
    _itemUpdateTotal();
  };
  // ⚡ Payroll package → per-person pre-coded itemize rows (owner 2026-09-15:
  // "tens of employees… uploaded as one big package… hard to sort through
  // fifty or a hundred line items"). Server AI-extracts {name, amount} rows
  // and matches each to the person's budget line; rows land pre-selected.
  window.docItemizeParsePeople = async function () {
    const uid = _docDetailUid;
    if (!uid) return;
    const btn = document.getElementById('docItemizePeopleBtn');
    const st = document.getElementById('docItemizeStatus');
    const orig = btn ? btn.textContent : '';
    if (btn) { btn.disabled = true; btn.textContent = '⚡ Reading document…'; }
    if (st) { st.style.color = 'var(--text-muted)'; st.textContent = 'AI is parsing the payroll package — this can take up to a minute for big documents.'; }
    try {
      const r = await fetch(`/docs/upload/${uid}/parse-people`, { method: 'POST' });
      const j = await r.json().catch(() => ({}));
      if (!r.ok || !j.ok) {
        if (st) { st.style.color = '#e0a13a'; st.textContent = j.error || ('Parse failed (' + r.status + ')'); }
        return;
      }
      if (!j.rows || !j.rows.length) {
        if (st) { st.style.color = '#e0a13a'; st.textContent = 'No per-person rows found in this document.'; }
        return;
      }
      const money = v => '$' + Number(v).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2});
      j.rows.forEach(row => {
        _itemAddRow(row.desc, row.amount, row.line_id || '', row.crew_member_id);
        // Tie-out delta (Phase 4): the parts did not sum to the person's
        // total; the difference sits on this row — yellow flag, never a
        // silent merge.
        if (row.delta) {
          const last = document.querySelector('#docItemizeRows .ditem-row:last-child');
          const inp = last && last.querySelector('.ditem-amt');
          if (inp) {
            inp.style.borderColor = '#e0a13a';
            inp.title = 'Document parts did not tie out by ' + money(Math.abs(row.delta)) +
              ' — the difference is booked here. Check the person\'s wages.';
          }
        }
      });
      window._docItemizeDirty = true;
      const blk = document.getElementById('docDetailItemizeBlock');
      if (blk) blk.open = true;
      const unmatched = j.total_rows - j.matched;
      const nDelta = (j.deltas || []).length;
      if (st) {
        st.style.color = (unmatched || nDelta) ? '#e0a13a' : '#5fd0a0';
        // Name where every part went — the fringe lines are per
        // department in 6500 ("Payroll Fringes — Production Staff"), WC
        // goes to 6000 Workers' Comp, the platform fee to 6500 Payroll
        // Service Fee. (Phase 4; the old text always claimed 6500.)
        const where = [];
        if (j.fringes_total) where.push(money(j.fringes_total) + ' employer fringes → ' +
          ((j.fringe_lines && j.fringe_lines.length) ? j.fringe_lines.join(', ') : '6500 Payroll Fringes'));
        if (j.workers_comp_total) where.push(money(j.workers_comp_total) + " workers' comp → 6000 Workers' Comp");
        if (j.platform_fee_total) where.push(money(j.platform_fee_total) + ' platform fee → 6500 Payroll Service Fee');
        st.textContent = `${j.total_rows} people parsed (${money(j.sum)}), ` +
          `${j.matched} matched to budget lines` +
          (where.length ? ` — gross wages on each person's line; ${where.join('; ')}` : '') +
          (unmatched ? ` — ${unmatched} need a line picked (amber)` : '') +
          (nDelta ? ` — ${nDelta} row${nDelta !== 1 ? 's' : ''} did not tie out (yellow amount; the difference is on the person's line)` : '') +
          '. Review, then Save itemization.';
      }
    } catch (e) {
      if (st) { st.style.color = '#e0a13a'; st.textContent = 'Parse failed: ' + e.message; }
    } finally {
      if (btn) { btn.disabled = false; btn.textContent = orig; }
    }
  };

  // 📎 Backups and budget-line coding: DocEditor.backupsLoad / loadCoding
  // (the main-line pick commits through actualsSetLine via init()).

  window.closeDocDetail = function (ev) {
    if (ev && ev.target && ev.target.id !== 'docDetailOverlay') return;   // click inside the panel
    // Backdrop click: don't silently discard unsaved itemized lines. (User 2026-07.)
    if (ev && ev.target && ev.target.id === 'docDetailOverlay' && window._docItemizeDirty) {
      const st = document.getElementById('docItemizeStatus');
      if (st) { st.textContent = 'You have unsaved itemized lines — Save, or use the Close/Cancel button to discard.'; st.style.color = '#e0a13a'; }
      const blk = document.getElementById('docDetailItemizeBlock'); if (blk) blk.open = true;
      return;
    }
    window._docItemizeDirty = false;
    document.getElementById('docDetailOverlay').style.display = 'none';
    _docDetailUid = null;
    window.DocEditor.reset();
    // Reset transient state so a stale message/button doesn't carry into the
    // next doc the user opens. (User 2026-06-22: the re-OCR conversion note and
    // the 'Deleting…' button persisted until a page refresh.)
    const _reocrSt = document.getElementById('docDetailReocrStatus');
    if (_reocrSt) { _reocrSt.textContent = ''; _reocrSt.style.color = 'var(--text-muted)'; }
    const _delBtn = document.getElementById('docDetailDeleteBtn');
    if (_delBtn) { _delBtn.disabled = false; _delBtn.textContent = '🗑 Delete document'; }
    const _reocrBtn = document.getElementById('docDetailReocrBtn');
    if (_reocrBtn) { _reocrBtn.disabled = false; _reocrBtn.textContent = '🔄 Re-run OCR'; }
  };

  // ── After a delete (DocEditor.delete → onDeleted) ─────────────────────
  function _docDeletedGlue(uid) {
    closeDocDetail();
  }

  // Persist the current modal's edits and patch the affected list row(s)
  // in place. Returns the /update response (or null on failure). Shared by
  // prev/next auto-save; the Save button is bound by DocEditor (afterSave
  // closes the modal). Does NOT close the modal — callers decide.
  window._docDetailCommit = async function () {
    if (!_docDetailUid) return null;
    return await window.DocEditor.save();
  };
  // After any successful save (Save button, prev/next): keep the People-tab
  // packet completeness in lockstep — without a reload.
  function _docSavedGlue(uid, d) {
    window._packetDocsCache = null;   // the People-tab views re-fetch the doc list
    if (typeof _buildPacketView === 'function') _buildPacketView();
    if (typeof _buildPeopleSummary === 'function') _buildPeopleSummary().catch(()=>{});
    if (typeof _buildLocationDocs === 'function') _buildLocationDocs();
    if (typeof _buildVendorView === 'function') _buildVendorView();
  }

  // Mount THE editor on this page: the line picker fills from the shared
  // #actualsLineOptsTpl options template and commits through actualsSetLine
  // (both in b04.js); PO actions open the page's PO modals; a save refreshes
  // the People-tab packet views; a delete closes the modal. (DOCUMENTS_HANDOFF
  // Phase 2.)
  if (window.DocEditor) {
    window.DocEditor.init({
      pid: (typeof PROJ_ID !== 'undefined' ? PROJ_ID : null),
      fillPicker: sel => { if (typeof window._actualsFillPicker === 'function') window._actualsFillPicker(sel); },
      setLine: sel => { if (typeof window.actualsSetLine === 'function') window.actualsSetLine(sel); },
      onSaved: _docSavedGlue,
      onDeleted: _docDeletedGlue,
      afterSave: () => closeDocDetail(),
      poCreate: _docPoCreateGlue,
      poAddTo: _docPoAddToGlue,
    });
  }

})();
