/* DocEditor — THE one document editor (DOCUMENTS_HANDOFF Phase 2, 2026-11-01).
 *
 * Drives the form rendered by templates/_doc_editor.html on BOTH doors:
 * the budget page's doc-detail modal (b03.js mounts it) and the standalone
 * /projects/<pid>/docs/<uid>/editor page (doc_editor.html mounts it), plus
 * the project-less /docs/<uid>/editor for Payee-page paperwork. Every
 * action lives here — fields, preview + zoom, save, payee (owner) with its
 * suggestion chip, duplicate Replace / Keep both, budget-line coding,
 * itemize (+ the Parse-payroll button), backups, travel, PO actions, match
 * to transactions, log expense, re-OCR, delete — so nothing depends on
 * which door the person came in through. Page-specific glue (prev/next
 * lists, patching list rows, opening the PO modals, b03's payroll status
 * text) stays in b03.js / doc_editor.html and is handed in through init().
 *
 * Classic script, no modules. Load order: doc-form.js, then this, then the
 * page's glue (b03.js or the inline block in doc_editor.html).
 *
 * window.DocEditor.init({
 *   pid,            // project id or null (project-less paperwork)
 *   fillPicker(sel) // populate a budget-line <select> (clone the page's options)
 *   setLine(sel)    // commit a main-line pick (default: generic set-line/set-coa POST)
 *   onSaved(uid, d) // after a successful /update (patch list rows, etc.)
 *   onDeleted(uid)  // after a successful delete (navigate / remove rows)
 *   afterSave(d)    // after the Save button's save (modal closes, page stays)
 *   afterOpen(uid, d)
 *   poCreate(ctx), poAddTo(ctx)   // present → PO actions shown
 *   onDirtyReset()  // page may re-capture its own dirty signature
 * })
 * window.DocEditor.open(uid, seed)  // seed: optional status-shaped dict for an
 *                                   // instant fill; /status is fetched regardless
 */
(function () {
  'use strict';
  const $ = id => document.getElementById(id);
  const esc = FP.esc;                        // shared rule (fp-utils.js)
  const money = v => FP.money(v || 0);
  function fmtMoney(v) {
    if (v === '' || v == null) return '';
    const n = parseFloat(String(v).replace(/,/g, ''));
    return isFinite(n) ? n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : '';
  }
  function parseMoney(v) {
    if (v == null) return '';
    const c = String(v).replace(/[$,\s]/g, '').trim();
    if (c === '' || c === '-') return '';
    const n = parseFloat(c);
    return isFinite(n) ? String(n) : '';
  }
  const fmtSize = b => !b ? '' : (b < 1048576 ? (b / 1024).toFixed(1) + ' KB' : (b / 1048576).toFixed(1) + ' MB');
  const CAT_ALIAS = { quote: 'estimate', sow: 'contract' };

  const E = {
    opts: { pid: null },
    uid: null,
    status: null,          // the /status payload of the open document
    openSig: '',
    _itemDocTotal: null,
    _itemVeryfi: [],
    _bk: null,
    _travel: { uid: null, data: null, people: [], bid: null },
    _txnCands: [], _txnDoc: {},
  };

  function init(opts) {
    E.opts = Object.assign({ pid: null }, opts || {});
    if (!E.opts.fillPicker) E.opts.fillPicker = fillPickerFromTemplate;
    if (!E.opts.setLine) E.opts.setLine = setLineGeneric;
    const sb = $('docDetailSaveBtn');
    if (sb && !sb._deBound) {
      sb._deBound = true;
      sb.addEventListener('click', async () => {
        if (!E.uid) return;
        sb.disabled = true; sb.textContent = 'Saving…';
        try {
          const d = await save();
          if (d && E.opts.afterSave) E.opts.afterSave(d);
        } catch (e) { alert('Save error: ' + e.message); }
        finally { sb.disabled = false; sb.textContent = 'Save'; }
      });
    }
    (function wireAmount() {
      const el = $('docDetailAmount'); if (!el || el._deBound) return;
      el._deBound = true;
      el.addEventListener('focus', () => { el.value = String(el.value).replace(/,/g, ''); });
      el.addEventListener('blur',  () => { el.value = fmtMoney(el.value); });
    })();
    return E;
  }

  // ── dirty signature ──────────────────────────────────────────────────
  function sig() {
    return ['docDetailVendor', 'docDetailDocSubtype', 'docDetailAmount', 'docDetailOrigAmount',
            'docDetailOrigCurrency', 'docDetailDocDate', 'docDetailCategory', 'docDetailNote',
            'docDetailDocNum', 'docDetailCard4', 'docDetailCrewId', 'docDetailLocationId',
            'docDetailTaxKind', 'docDetailReceivedOn', 'docDetailExpiresOn']
      .map(id => ($(id) ? ($(id).value || '') : '')).join('|');
  }
  function dirty() { return sig() !== E.openSig; }
  function markClean() { E.openSig = sig(); if (E.opts.onDirtyReset) E.opts.onDirtyReset(); }

  // ── type-driven UI ───────────────────────────────────────────────────
  function applyTypeUI(type) {
    const spec = window.DocForm.applyTypeUI(type, {
      uid: E.uid,
      uidStillCurrent: uid => uid === E.uid,
      onAutoApplied: () => markClean(),
    });
    // PO actions: any commitment-shaped doc can create OR join a PO; a
    // receipt can only be attached to an existing PO. Only on a page that
    // can open the PO modals (the budget page). (Owner 2026-09-25.)
    const po = $('docDetailPoActions');
    if (po) {
      const t = (type || '').toLowerCase();
      const canCreate = ['estimate', 'quote', 'contract', 'sow', 'purchase_order', 'invoice'].includes(t);
      const canAttach = canCreate || t === 'receipt';
      const canAny = !!(E.opts.poCreate || E.opts.poAddTo) && !!E.opts.pid;
      po.style.display = (canAny && canAttach) ? '' : 'none';
      const cb = $('docDetailCreatePoBtn'); if (cb) cb.style.display = (canCreate && E.opts.poCreate) ? '' : 'none';
      const ab = $('docDetailAddToPoBtn'); if (ab) ab.style.display = E.opts.poAddTo ? '' : 'none';
    }
    // Project-bound blocks hide on a project-less document.
    const projectOnly = ['docDetailTxnFind', 'docDetailTravelBlock'];
    projectOnly.forEach(id => { const el = $(id); if (el && !E.opts.pid) el.style.display = 'none'; });
    highlightMissing(spec);
    return spec;
  }
  // For a row still in Review, outline the required-but-blank fields.
  function highlightMissing(spec) {
    if (!spec) return;
    const isReview = !!(E.status && E.status.status === 'review');
    const hl = (id, needed) => {
      const el = $(id); if (!el) return;
      const empty = !((el.value || '').trim());
      const on = isReview && needed && empty;
      el.style.outline = on ? '1px solid #d97706' : '';
      el.style.outlineOffset = on ? '2px' : '';
    };
    hl('docDetailVendor', !!spec.vendor);
    hl('docDetailDocDate', true);
    hl('docDetailAmount', !!spec.amount);
    hl('docDetailDocNum', !!spec.docnum);
    hl('docDetailCrewId', !!spec.taxform || !!spec.subtype);
  }

  // ── fill from a status-shaped dict ────────────────────────────────────
  function fill(d) {
    d = d || {};
    const sv = (id, v) => { const el = $(id); if (el) el.value = (v == null ? '' : v); };
    const t = $('docDetailTitle');
    if (t) t.textContent = d.filed_filename || d.original_filename || d.filename || 'Document';
    sv('docDetailVendor', d.vendor || '');
    const amt = parseFloat(d.amount);
    sv('docDetailAmount', (Number.isFinite(amt) && amt !== 0) ? fmtMoney(amt) : '');
    const oamt = parseFloat(d.original_amount);
    sv('docDetailOrigAmount', (Number.isFinite(oamt) && oamt !== 0) ? fmtMoney(oamt) : '');
    sv('docDetailOrigCurrency', d.original_currency || '');
    sv('docDetailDocDate', d.doc_date || '');
    const raw = (d.category || '').toLowerCase();
    sv('docDetailCategory', CAT_ALIAS[raw] || raw);
    sv('docDetailNote', d.note || '');
    sv('docDetailDocNum', d.doc_number || '');
    sv('docDetailCard4', d.card_last4 || '');
    const owner = (d.owner_payee_id != null ? d.owner_payee_id : d.crew_member_id);
    sv('docDetailCrewId', owner != null && owner !== '' ? String(owner) : '');
    sv('docDetailLocationId', d.location_id != null ? String(d.location_id) : '');
    if (window.DocForm) { window.DocForm.setPaperwork(d); window.DocForm.newCrewHide(); }
  }

  // ── open ──────────────────────────────────────────────────────────────
  async function open(uid, seed) {
    E.uid = uid;
    window._docDetailCurrentUid = uid;
    E.status = null;
    // Reset transient state from the previous document.
    const res = $('docDetailTxnResults'); if (res) res.innerHTML = '';
    const incl = $('docDetailInclMatched'); if (incl) incl.checked = false;
    const rs = $('docDetailReocrStatus'); if (rs) { rs.textContent = ''; rs.style.color = 'var(--text-muted)'; }
    const db_ = $('docDetailDeleteBtn'); if (db_) { db_.disabled = false; db_.textContent = '🗑 Delete document'; }
    const rb = $('docDetailReocrBtn'); if (rb) { rb.disabled = false; rb.textContent = '🔄 Re-run OCR'; }
    const ss = $('docDetailSaveStatus'); if (ss) ss.textContent = '';
    const dupB = $('docDetailDupBanner'); if (dupB) dupB.style.display = 'none';
    if (window.DocForm) window.DocForm.resetSuggest();
    const fe = $('docDetailFullEditor');
    if (fe) fe.href = E.opts.pid ? ('/projects/' + E.opts.pid + '/docs/' + uid + '/editor') : ('/docs/' + uid + '/editor');
    if (seed) fill(seed);
    const cat = $('docDetailCategory');
    if (cat) {
      cat.onchange = e => applyTypeUI((e.target.value || '').toLowerCase());
      ['docDetailVendor', 'docDetailAmount', 'docDetailDocDate', 'docDetailDocNum', 'docDetailCrewId'].forEach(id => {
        const el = $(id); if (el) el.oninput = () => highlightMissing(window.DocForm.FIELD_SPEC[(cat.value || '').toLowerCase()] || window.DocForm.FIELD_SPEC.misc);
      });
    }
    const meta = $('docDetailMeta'); if (meta) meta.textContent = 'Upload ID #' + uid;
    const vEl = $('docDetailVerify'); if (vEl) vEl.style.display = 'none';
    const pv = $('docDetailPreview'); if (pv) pv.innerHTML = '<p class="muted" style="padding:1rem">Loading preview…</p>';

    // Authoritative fields (paperwork columns, owner, dup state) + verify +
    // preview type, in parallel.
    let st = null, verifyData = null, linkData = null;
    try {
      const [sR, vR, lR] = await Promise.all([
        fetch('/docs/upload/' + uid + '/status', { credentials: 'same-origin' }),
        fetch('/docs/upload/' + uid + '/verify', { credentials: 'same-origin' }),
        fetch('/docs/upload/' + uid + '/preview-link', { credentials: 'same-origin' }),
      ]);
      st = sR.ok ? await sR.json().catch(() => null) : null;
      verifyData = await vR.json().catch(() => null);
      linkData = await lR.json().catch(() => null);
    } catch (e) { /* fall through */ }
    if (uid !== E.uid) return;          // user moved on while we were loading
    if (st) { E.status = st; fill(st); }
    applyTypeUI(($('docDetailCategory') || {}).value || '');
    markClean();
    renderDupBanner();
    renderVerify(verifyData);
    renderPreview(uid, linkData);
    loadDropboxLink(uid);
    if (E.opts.pid) {
      loadCoding(uid);
      itemizeLoad(uid);
      backupsLoad(uid);
      try { loadTravel(uid); } catch (e) {}
    } else {
      ['docDetailCodingBlock', 'docDetailItemizeBlock', 'docDetailBackupsBlock',
       'docDetailTravelBlock', 'docDetailPoActions', 'docDetailTxnFind'].forEach(id => {
        const el = $(id); if (el) el.style.display = 'none';
      });
      // Receipt detail (subtotal/tax/tip) still loads on a project-less doc.
      fetch('/docs/upload/' + uid + '/line-items', { credentials: 'same-origin' })
        .then(r => r.ok ? r.json() : null).then(d => { if (d) fillReceiptDetail(d.receipt_detail || {}); }).catch(() => {});
    }
    if (E.opts.afterOpen) E.opts.afterOpen(uid, st);
  }

  function renderVerify(verifyData) {
    const vEl = $('docDetailVerify'); if (!vEl) return;
    if (verifyData && verifyData.ok && verifyData.verified) {
      vEl.style.cssText = 'font-size:11px;padding:6px 9px;border-radius:6px;margin-bottom:12px;background:#14291e;color:#22c55e;border:1px solid #1a4228;';
      vEl.textContent = '✓ Verified in Dropbox' + (verifyData.size ? (' · ' + fmtSize(verifyData.size)) : '');
      vEl.style.display = 'block';
    } else if (verifyData && verifyData.ok && verifyData.verified === false) {
      vEl.style.cssText = 'font-size:11px;padding:6px 9px;border-radius:6px;margin-bottom:12px;background:#2a1414;color:#ef4444;border:1px solid #4a2020;font-weight:600;';
      vEl.textContent = '⚠ MISSING from Dropbox — ' + (verifyData.reason || 'file not found');
      vEl.style.display = 'block';
    }
  }

  // ── duplicate banner: Replace / Keep both / It's a duplicate ──────────
  function renderDupBanner() {
    const b = $('docDetailDupBanner'); if (!b) return;
    const st = E.status || {};
    const pending = !!(st.dup_pending || (st.is_duplicate && st.status !== 'duplicate'));
    if (!pending) { b.style.display = 'none'; return; }
    const peer = st.duplicate_of_id;
    const paper = (st.doc_class === 'paperwork');
    b.innerHTML =
      '<div style="font-weight:600;margin-bottom:4px">⚠ Possible duplicate' + (peer ? ' of #' + esc(peer) : '') + '</div>'
      + '<div style="font-size:11px;color:var(--text-muted);margin-bottom:6px">' + esc(st.note || (paper
        ? 'This payee already has this kind of document on file.'
        : 'Another document with the same contents is already filed.')) + '</div>'
      + '<div style="display:flex;gap:6px;flex-wrap:wrap">'
      + (paper ? '<button type="button" onclick="DocEditor.resolveDup(\'replace\')" style="padding:5px 11px;border-radius:6px;background:#1f7a4d;border:none;color:#fff;font-size:12px;cursor:pointer">Replace the older one</button>' : '')
      + '<button type="button" onclick="DocEditor.resolveDup(\'keep\')" style="padding:5px 11px;border-radius:6px;background:#1a2540;border:1px solid #2d4070;color:#8fb4ff;font-size:12px;cursor:pointer">Keep both</button>'
      + '<button type="button" onclick="DocEditor.resolveDup(\'confirm\')" style="padding:5px 11px;border-radius:6px;background:#2a1414;border:1px solid #4a2020;color:#ef4444;font-size:12px;cursor:pointer">It’s a duplicate — bury this copy</button>'
      + '<span id="docDetailDupStatus" style="font-size:11px;color:var(--text-muted);align-self:center"></span>'
      + '</div>';
    b.style.display = '';
  }
  async function resolveDup(action, force) {
    const uid = E.uid; if (!uid) return;
    const st = $('docDetailDupStatus');
    if (action === 'replace' && !confirm('Replace the older document with this one?\n\nThe older copy moves to the Trash (restorable) and this one becomes the document on file.')) return;
    if (st) st.textContent = 'Working…';
    try {
      const r = await fetch('/docs/upload/' + uid + '/resolve-duplicate', {
        method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: action, force: !!force }) });
      const d = await r.json().catch(() => ({}));
      if (r.status === 409 && d.needs_force) {
        if (confirm('This copy is coded or linked to a charge (' + (d.coded_to || 'linked') + '). Bury it anyway? Linked charges are unlinked, never deleted.')) return resolveDup(action, true);
        if (st) st.textContent = ''; return;
      }
      if (!r.ok) { if (st) st.textContent = d.error || ('Failed (' + r.status + ')'); return; }
      const b = $('docDetailDupBanner'); if (b) b.style.display = 'none';
      // Re-read the authoritative state and let the page patch its rows.
      const sR = await fetch('/docs/upload/' + uid + '/status', { credentials: 'same-origin' });
      const s2 = sR.ok ? await sR.json().catch(() => null) : null;
      if (s2 && uid === E.uid) { E.status = s2; fill(s2); markClean(); }
      if (E.opts.onSaved) E.opts.onSaved(uid, Object.assign({ resolved: action }, s2 || {}));
    } catch (e) { if (st) st.textContent = 'Error: ' + e.message; }
  }

  // ── preview ──────────────────────────────────────────────────────────
  function renderPreview(uid, linkData) {
    const previewEl = $('docDetailPreview'); if (!previewEl) return;
    const rawUrl = '/docs/upload/' + uid + '/raw';
    const fn = (linkData && linkData.filename ? linkData.filename : '').toLowerCase();
    const ct = (linkData && linkData.content_type ? linkData.content_type : '').toLowerCase();
    const isImage = ct.indexOf('image/') === 0 || /\.(jpe?g|png|gif|heic|heif|webp|bmp|tiff?)$/i.test(fn);
    const zoomBar = $('docDetailZoomBar');
    if (zoomBar) zoomBar.style.display = isImage ? 'flex' : 'none';
    if (isImage) {
      previewEl.style.alignItems = 'flex-start';
      previewEl.style.justifyContent = 'flex-start';
      previewEl.innerHTML =
        '<img id="docDetailImg" src="' + rawUrl + '" alt="preview" ' +
        'style="display:block;max-width:none;max-height:none;transform-origin:0 0;transition:transform .08s linear;user-select:none;-webkit-user-drag:none;cursor:grab;">';
      initImageControls();
    } else {
      // Real PDFs AND images misnamed ".pdf": /raw sniffs the bytes and sends
      // the right Content-Type, an iframe honours it. (User 2026-06-01.)
      previewEl.style.alignItems = 'stretch';
      previewEl.style.justifyContent = 'stretch';
      previewEl.style.position = 'relative';
      previewEl.innerHTML =
        '<iframe src="' + rawUrl + '" title="document preview" style="border:0;width:100%;height:100%;min-height:540px;display:block;background:#fff"></iframe>' +
        '<a href="' + rawUrl + '" target="_blank" rel="noopener" style="position:absolute;top:8px;right:8px;font-size:11px;background:#1a2540;border:1px solid #2d4070;color:#8fb4ff;padding:3px 9px;border-radius:5px;text-decoration:none">↗ Open in new tab</a>';
    }
  }
  function initImageControls() {
    const wrap = $('docDetailPreview'), img = $('docDetailImg'), lbl = $('docDetailZoomLabel');
    if (!wrap || !img) return;
    const z = { scale: 1, fitScale: 1, naturalW: 0, naturalH: 0 };
    function applyScale() {
      img.style.transform = 'scale(' + z.scale + ')';
      img.style.width = z.naturalW + 'px'; img.style.height = z.naturalH + 'px';
      if (lbl) lbl.textContent = Math.round(z.scale * 100) + '%';
      img.style.cursor = z.scale > z.fitScale + 0.001 ? 'grab' : 'default';
    }
    function fit() {
      const cw = wrap.clientWidth, ch = wrap.clientHeight;
      if (!z.naturalW || !z.naturalH || !cw || !ch) return;
      z.fitScale = Math.min(cw / z.naturalW, ch / z.naturalH, 1);
      z.scale = z.fitScale; applyScale();
    }
    img.onload = () => { z.naturalW = img.naturalWidth; z.naturalH = img.naturalHeight; fit(); };
    if (img.complete && img.naturalWidth) { z.naturalW = img.naturalWidth; z.naturalH = img.naturalHeight; fit(); }
    function zoomBy(factor, anchor) {
      const before = z.scale, after = Math.max(0.1, Math.min(8, before * factor));
      if (after === before) return;
      const sl = wrap.scrollLeft, st = wrap.scrollTop;
      const ax = anchor ? anchor.x : wrap.clientWidth / 2, ay = anchor ? anchor.y : wrap.clientHeight / 2;
      z.scale = after; applyScale();
      const ratio = after / before;
      wrap.scrollLeft = (sl + ax) * ratio - ax; wrap.scrollTop = (st + ay) * ratio - ay;
    }
    const b = id => $(id);
    if (b('docDetailZoomIn'))  b('docDetailZoomIn').onclick  = () => zoomBy(1.25);
    if (b('docDetailZoomOut')) b('docDetailZoomOut').onclick = () => zoomBy(1 / 1.25);
    if (b('docDetailZoomFit')) b('docDetailZoomFit').onclick = () => fit();
    if (b('docDetailZoom100')) b('docDetailZoom100').onclick = () => { z.scale = 1; applyScale(); };
    wrap.onwheel = e => {
      if (!(e.ctrlKey || e.metaKey)) return;
      e.preventDefault();
      const rect = wrap.getBoundingClientRect();
      zoomBy(e.deltaY < 0 ? 1.1 : 1 / 1.1, { x: e.clientX - rect.left, y: e.clientY - rect.top });
    };
    let dragging = false, sx = 0, sy = 0, sl0 = 0, st0 = 0;
    img.onmousedown = e => {
      if (z.scale <= z.fitScale + 0.001) return;
      dragging = true; sx = e.clientX; sy = e.clientY; sl0 = wrap.scrollLeft; st0 = wrap.scrollTop;
      img.style.cursor = 'grabbing'; e.preventDefault();
    };
    window.addEventListener('mousemove', e => { if (!dragging) return; wrap.scrollLeft = sl0 - (e.clientX - sx); wrap.scrollTop = st0 - (e.clientY - sy); });
    window.addEventListener('mouseup', () => { if (!dragging) return; dragging = false; img.style.cursor = z.scale > z.fitScale + 0.001 ? 'grab' : 'default'; });
  }
  function loadDropboxLink(uid) {
    const dbxBtn = $('docDetailDropboxLink');
    if (dbxBtn) { dbxBtn.href = '#'; dbxBtn.style.opacity = '0.5'; dbxBtn.textContent = 'Loading link…'; }
    fetch('/docs/upload/' + uid + '/dropbox-link', { credentials: 'same-origin' })
      .then(r => r.json())
      .then(d => {
        if (uid !== E.uid) return;
        if (dbxBtn) {
          if (d && d.ok && d.url) { dbxBtn.href = d.url; dbxBtn.style.opacity = '1'; dbxBtn.textContent = 'Open in Dropbox ↗'; }
          else { dbxBtn.style.opacity = '0.4'; dbxBtn.textContent = 'Open in Dropbox (unavailable)'; dbxBtn.title = (d && d.error) || 'Could not generate link'; }
        }
        const srcBtn = $('docDetailArchiveLink');
        if (srcBtn) {
          if (d && d.archive_url) { srcBtn.href = d.archive_url; srcBtn.style.display = 'inline-block'; srcBtn.title = 'Source archive: ' + (d.archive_path || ''); }
          else srcBtn.style.display = 'none';
        }
      })
      .catch(() => { if (dbxBtn) { dbxBtn.style.opacity = '0.4'; dbxBtn.textContent = 'Open in Dropbox (network error)'; } });
  }

  // ── save ─────────────────────────────────────────────────────────────
  function collect() {
    const g = id => ($(id) ? ($(id).value || '') : '');
    const pw = window.DocForm.paperworkValues();
    return {
      vendor:   window.DocForm.vendorValue(),
      amount:   parseMoney(g('docDetailAmount')),
      original_amount:   parseMoney(g('docDetailOrigAmount')),
      original_currency: g('docDetailOrigCurrency'),
      doc_date: g('docDetailDocDate'),
      category: g('docDetailCategory'),
      note:     g('docDetailNote'),
      doc_number: g('docDetailDocNum'),
      card_last4: g('docDetailCard4'),
      subtotal: parseMoney(g('docDetailSubtotal')),
      tax:      parseMoney(g('docDetailTax')),
      tip:      parseMoney(g('docDetailTip')),
      merchant_address: g('docDetailMerchAddr'),
      merchant_phone:   g('docDetailMerchPhone'),
      owner_payee_id: pw.owner_payee_id,
      crew_member_id: pw.owner_payee_id,        // alias column, same value
      location_id:    g('docDetailLocationId') || null,
      doc_subtype:    pw.doc_subtype,
      tax_form_kind:  pw.tax_form_kind,
      received_on:    pw.received_on,
      expires_on:     pw.expires_on,
    };
  }
  // Persist the open document's edits. Returns the /update response, or
  // null on failure (the error is shown). Does NOT close anything.
  async function save() {
    if (!E.uid) return null;
    const uid = E.uid;
    const st = $('docDetailSaveStatus');
    if (st) { st.textContent = ''; st.style.color = 'var(--text-muted)'; }
    const r = await fetch('/docs/upload/' + uid + '/update', {
      method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(collect()),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) {
      if (st) { st.textContent = 'Save failed: ' + (d.error || r.status); st.style.color = '#e0a13a'; }
      else alert('Save failed: ' + (d.error || r.status));
      return null;
    }
    if (uid === E.uid) {
      E.status = Object.assign({}, E.status || {}, d);
      markClean();
      renderDupBanner();
    }
    if (st) { st.textContent = '✓ Saved'; st.style.color = '#5fd0a0'; }
    if (E.opts.onSaved) E.opts.onSaved(uid, d);
    return d;
  }

  // ── budget-line coding ───────────────────────────────────────────────
  function fillPickerFromTemplate(sel) {
    if (!sel || sel.dataset.populated === '1') return;
    const tpl = $('docDetailLineOptsTpl'); if (!tpl) return;
    sel.innerHTML = tpl.innerHTML;
    sel.dataset.populated = '1';
    sel.value = sel.dataset.current || '';
  }
  // Generic main-line commit: line id → set-line; 'section:N' → set-coa;
  // 'not_project' → set-coa {not_project}; '__clear__' → set-line {null}.
  function setLineGeneric(selectEl) {
    const tid = parseInt(selectEl.dataset.tid), value = selectEl.value, st = $('docDetailCodingStatus');
    if (!value || !E.opts.pid) return;
    const prev = selectEl.dataset.current || '';
    let url, body;
    const base = '/projects/' + E.opts.pid + '/actuals/transaction/' + tid;
    if (value === '__clear__') { url = base + '/set-line'; body = { budget_line_id: null }; }
    else if (value.indexOf('section:') === 0) { url = base + '/set-coa'; body = { account_code: value.slice(8) }; }
    else if (value === 'not_project') { url = base + '/set-coa'; body = { account_code: 'not_project' }; }
    else { url = base + '/set-line'; body = { budget_line_id: value }; }
    if (st) { st.textContent = 'Saving…'; st.style.color = 'var(--text-muted)'; }
    fetch(url, { method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
      .then(r => r.json().catch(() => ({})).then(d => ({ ok: r.ok, status: r.status, d })))
      .then(res => {
        if (!res.ok) { selectEl.value = prev; if (st) { st.textContent = res.d.error || ('Could not save: ' + res.status); st.style.color = '#e0a13a'; } return; }
        selectEl.dataset.current = (value === '__clear__' || value === 'not_project') ? '' : value;
        if (st) {
          const o = selectEl.options[selectEl.selectedIndex];
          st.textContent = value === '__clear__' ? '✓ Cleared.' : value === 'not_project' ? '✓ Marked “not a project expense.”' : ('✓ Saved — ' + (o ? o.text.trim() : value));
          st.style.color = '#5fd0a0';
        }
        if (value === '__clear__' || value === 'not_project') selectEl.value = '';
        itemUpdateTotal();
      })
      .catch(e => { selectEl.value = prev; if (st) { st.textContent = 'Save failed: ' + e.message; st.style.color = '#e0a13a'; } });
  }
  function fillPicker(sel) { return E.opts.fillPicker(sel); }
  function setLine(sel) { return E.opts.setLine(sel); }
  async function loadCoding(uid) {
    const block = $('docDetailCodingBlock'), picker = $('docDetailLinePicker'), sugEl = $('docDetailSuggest'), statusEl = $('docDetailCodingStatus');
    if (!block || !picker) return;
    block.style.display = 'none';
    if (sugEl) { sugEl.style.display = 'none'; sugEl.innerHTML = ''; }
    picker.dataset.populated = '0';
    picker.innerHTML = '<option value="">— pick budget line —</option>';
    picker.dataset.tid = ''; picker.dataset.current = ''; picker.disabled = false;
    if (statusEl) statusEl.textContent = '';
    try {
      const r = await fetch('/docs/upload/' + uid + '/coding', { credentials: 'same-origin' });
      if (!r.ok || uid !== E.uid) return;
      const d = await r.json();
      if (!d.txn_id) return;                 // non-ledger doc — no line coding
      picker.dataset.tid = d.txn_id;
      picker.dataset.current = d.current || '';
      fillPicker(picker);
      block.style.display = '';
      if (d.not_project) { picker.disabled = true; if (statusEl) statusEl.textContent = 'Marked “not a project expense.”'; return; }
      const sel = picker.options[picker.selectedIndex];
      const ph = Array.from(picker.options).find(o => o.value === '');
      if (ph) ph.text = d.current ? 'Change budget line' : '＋ Create expense — pick budget line';
      if (statusEl) statusEl.textContent = d.current && sel ? ('Currently: ' + sel.text.trim())
        : 'Not an expense yet — picking a line creates it (the doc rides along as backup).';
      if (d.suggestion && !d.current && sugEl) {
        const s = d.suggestion;
        sugEl.style.display = '';
        sugEl.innerHTML = '<button type="button" id="docDetailSuggestBtn" style="font-size:11.5px;padding:4px 9px;background:#10231a;border:1px solid #1f6f4a;border-radius:5px;color:#5fd0a0;cursor:pointer;max-width:100%;white-space:normal;text-align:left;line-height:1.3">💡 Suggested: ' + esc(String(s.code)) + ' · ' + esc(String(s.label)) + '</button>';
        $('docDetailSuggestBtn').onclick = () => {
          picker.value = String(s.line_id);
          if (picker.value === String(s.line_id)) { sugEl.style.display = 'none'; if (statusEl) statusEl.textContent = 'Coding…'; setLine(picker); }
        };
      }
    } catch (e) { /* coding is best-effort */ }
  }

  // ── itemize ──────────────────────────────────────────────────────────
  function itemAddRow(desc, amount, lineId, crewMemberId) {
    const host = $('docItemizeRows'); if (!host) return;
    const row = document.createElement('div');
    row.className = 'ditem-row';
    // Payee behind the row (Documents Phase 3): the payroll parse names the
    // person; the save sends it so the child transaction attributes to them.
    if (crewMemberId) row.dataset.crewMemberId = String(crewMemberId);
    row.style.cssText = 'display:flex;gap:6px;margin-bottom:5px;align-items:center';
    row.innerHTML =
      '<input class="ditem-desc" placeholder="description" value="' + esc(desc || '') + '" oninput="window._docItemizeDirty=true" style="flex:1.5;min-width:0;font-size:12px;padding:4px 6px;background:var(--bg-input);color:var(--text);border:1px solid var(--border);border-radius:5px">' +
      '<input class="ditem-amt" type="number" step="0.01" placeholder="0.00" value="' + (amount != null && amount !== '' ? amount : '') + '" oninput="_itemUpdateTotal();window._docItemizeDirty=true" style="width:88px;font-size:12px;padding:4px 6px;background:var(--bg-input);color:var(--text);border:1px solid var(--border);border-radius:5px;text-align:right">' +
      '<select class="ditem-line actuals-line-picker" onfocus="DocEditor.fillPicker(this)" onchange="_ditemLineChanged(this)" style="flex:1.3;min-width:0;font-size:12px;padding:4px 6px;background:var(--bg-input);color:var(--text);border:1px solid var(--border);border-radius:5px"><option value="">— budget line —</option></select>' +
      '<button type="button" title="Remove line" onclick="this.closest(\'.ditem-row\').remove();_itemUpdateTotal();window._docItemizeDirty=true" style="background:none;border:none;color:var(--text-muted);cursor:pointer;font-size:14px">✕</button>';
    host.appendChild(row);
    const sel = row.querySelector('.ditem-line');
    if (lineId) sel.dataset.current = String(lineId);
    // Populate at creation, not lazily on focus — mutating 300+ options while
    // the native dropdown opens stalled the first pick. (Review fix 2026-07.)
    fillPicker(sel);
    itemUpdateTotal();
  }
  function itemUpdateTotal() {
    const rows = document.querySelectorAll('#docItemizeRows .ditem-row');
    const hdr = $('docItemizeHdr'); if (hdr) hdr.style.display = rows.length ? 'flex' : 'none';
    // Only lines WITH a budget line peel off; lines with an amount but no line
    // stay on the main line (amber border flags them). (User 2026-07-20.)
    let sub = 0, uncSub = 0, uncN = 0;
    rows.forEach(r => {
      const a = parseFloat((r.querySelector('.ditem-amt') || {}).value || 0) || 0;
      const sel = r.querySelector('.ditem-line');
      const hasLine = !!(sel && sel.value);
      if (a && !hasLine) { uncSub += a; uncN++; if (sel) sel.style.borderColor = '#e0a13a'; }
      else { sub += a; if (sel) sel.style.borderColor = ''; }
    });
    sub = Math.round(sub * 100) / 100; uncSub = Math.round(uncSub * 100) / 100;
    window._itemUncodedN = uncN; window._itemUncodedSub = uncSub;
    const el = $('docItemizeTotal'); if (!el) return;
    const tot = E._itemDocTotal;
    let mainLabel = 'main line';
    const mp = $('docDetailLinePicker');
    if (mp && mp.value && mp.selectedIndex >= 0 && mp.options[mp.selectedIndex]) {
      const t = (mp.options[mp.selectedIndex].text || '').trim();
      if (t && t !== '— pick budget line —') mainLabel = t.length > 30 ? t.slice(0, 30) + '…' : t;
    }
    if (tot != null) {
      const remainder = Math.round((tot - sub) * 100) / 100;
      const over = remainder < -0.01;
      const uncNote = uncN ? ' <span style="color:#e0a13a">(incl. ' + money(uncSub) + ' from ' + uncN + ' line' + (uncN !== 1 ? 's' : '') + ' with no budget line)</span>' : '';
      el.innerHTML = 'Peeling off <b>' + money(sub) + '</b> · ' +
        (over ? '<span style="color:#e0a13a">⚠ ' + money(-remainder) + ' over the doc total</span>'
              : '→ <b style="color:#5fd0a0">' + money(remainder) + '</b> to ' + (mp && mp.value ? '<b>' + mainLabel + '</b>' : '<span style="color:#e0a13a">main line (pick one above)</span>') + uncNote);
    } else el.innerHTML = 'Sublines: <b>' + money(sub) + '</b>';
  }
  function ditemLineChanged(sel) { window._docItemizeDirty = true; itemUpdateTotal(); }
  function itemSetOverride(n) {
    const label = document.querySelector('#docDetailCodingBlock label');
    const status = $('docDetailCodingStatus');
    if (label) label.textContent = n > 0 ? 'Main budget line (gets the remainder)' : 'Budget line (Actuals)';
    if (n > 0 && status) { status.innerHTML = '↳ plus ' + n + ' subline' + (n !== 1 ? 's' : '') + ' peeled off to other lines (below).'; status.style.color = 'var(--text-muted)'; }
    else if (status) status.style.color = '';
    itemUpdateTotal();
  }
  function fillReceiptDetail(rd) {
    const sv = (id, v) => { const el = $(id); if (el) el.value = (v != null ? v : ''); };
    sv('docDetailSubtotal', rd.subtotal); sv('docDetailTax', rd.tax); sv('docDetailTip', rd.tip);
    sv('docDetailMerchAddr', rd.merchant_address); sv('docDetailMerchPhone', rd.merchant_phone);
  }
  function itemizePull() {
    const items = E._itemVeryfi || [];
    const st = $('docItemizeStatus');
    if (!items.length) { if (st) st.textContent = 'No OCR line items found on this document.'; return; }
    $('docItemizeRows').innerHTML = '';
    items.forEach(it => itemAddRow(it.description, it.amount, ''));
    window._docItemizeDirty = true;
  }
  async function itemizeLoad(uid) {
    E._itemDocTotal = null; E._itemVeryfi = []; window._itemVeryfi = [];
    const block = $('docDetailItemizeBlock'), rows = $('docItemizeRows'), st = $('docItemizeStatus'), cnt = $('docItemizeCount');
    if (!block || !rows) return;
    block.style.display = 'none'; rows.innerHTML = ''; if (st) st.textContent = ''; if (cnt) cnt.textContent = '';
    try {
      const r = await fetch('/docs/upload/' + uid + '/line-items', { credentials: 'same-origin' });
      if (!r.ok || uid !== E.uid) return;
      const d = await r.json();
      fillReceiptDetail(d.receipt_detail || {});
      if (!d.ok || d.parent_txn_id == null) return;      // no transaction to itemize
      block.style.display = '';
      E._itemDocTotal = d.doc_total; E._itemVeryfi = d.veryfi_items || []; window._itemVeryfi = E._itemVeryfi;
      const pullBtn = $('docItemizePullBtn'); if (pullBtn) pullBtn.style.display = (E._itemVeryfi.length ? '' : 'none');
      if (d.main_line_id) {
        const mp = $('docDetailLinePicker');
        if (mp) { mp.dataset.current = String(d.main_line_id); fillPicker(mp); mp.value = String(d.main_line_id); }
      }
      if (d.existing && d.existing.length) {
        block.open = true;
        if (cnt) cnt.textContent = '· ' + d.existing.length + ' subline' + (d.existing.length !== 1 ? 's' : '');
        d.existing.forEach(x => itemAddRow(x.description, x.amount, x.budget_line_id, x.payee_id));
      } else itemUpdateTotal();
      itemSetOverride(d.existing ? d.existing.length : 0);
      // ONE split mechanism (2026-11-04): a document attached to a charge
      // splits THE CHARGE; the one document backs every piece.
      E._itemWrites = d.writes || 'itemize';
      if (st && d.writes === 'split') { st.textContent = 'Attached to a charge: saving splits the charge by these lines, and this document backs every piece.'; st.style.color = ''; }
      window._docItemizeDirty = false;
    } catch (e) {}
  }
  async function itemizeSave() {
    const uid = E.uid; if (!uid) return false;
    const st = $('docItemizeStatus'), btn = $('docItemizeSaveBtn');
    const rows = []; let uncoded = 0, uncodedAmt = 0;
    document.querySelectorAll('#docItemizeRows .ditem-row').forEach(r => {
      const amt = parseFloat((r.querySelector('.ditem-amt') || {}).value || 0);
      const line = (r.querySelector('.ditem-line') || {}).value || '';
      const desc = (r.querySelector('.ditem-desc') || {}).value || '';
      const cm = r.dataset.crewMemberId || null;
      if (amt && line) rows.push({ amount: amt, budget_line_id: line, description: desc, crew_member_id: cm });
      else if (amt) { uncoded++; uncodedAmt += amt; }
    });
    const mainLine = ($('docDetailLinePicker') || {}).value || '';
    if (!rows.length && !mainLine) {
      if (st) { st.textContent = uncoded ? 'No line has a budget line yet — pick budget lines on the rows, or pick a main budget line above.' : 'Nothing to save — add sublines or pick a main budget line above.'; st.style.color = '#e0a13a'; }
      return false;
    }
    if (uncoded && !mainLine) {
      if (st) { st.textContent = uncoded + ' line' + (uncoded !== 1 ? 's have' : ' has') + ' no budget line (' + money(uncodedAmt) + ') — pick lines for them, or pick a main budget line above to receive that amount.'; st.style.color = '#e0a13a'; }
      return false;
    }
    if (btn) { btn.disabled = true; btn.textContent = 'Saving…'; }
    try {
      const r = await fetch('/docs/upload/' + uid + '/itemize', {
        method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ rows, main_line_id: mainLine }) });
      const d = await r.json().catch(() => ({}));
      if (r.ok && d.ok) {
        if (st) {
          if (d.writes === 'split') st.textContent = d.mode === 'split'
            ? '✓ Saved as a split on the charge — ' + d.pieces + ' pieces; this document backs every piece.'
            : (d.mode === 'cleared' ? 'Coding cleared on the charge.' : '✓ Saved — the charge is coded to one line.');
          else if (d.cleared) st.textContent = 'Itemization cleared.';
          else if (d.over) st.textContent = '⚠ Saved, but sublines are ' + money(-d.remainder) + ' over the doc total — check the amounts.';
          else st.textContent = '✓ Saved — ' + money(d.sublines_total) + ' peeled off, ' + money(d.main_line_amount) + ' stays on the main line'
            + (d.uncoded_n ? ' (incl. ' + money(d.uncoded_total) + ' from ' + d.uncoded_n + ' line' + (d.uncoded_n !== 1 ? 's' : '') + ' without a budget line)' : '') + '.';
          st.style.color = d.over ? '#e0a13a' : '#5fd0a0';
        }
        const nsub = rows.filter(x => x.amount).length;
        const cnt = $('docItemizeCount'); if (cnt) cnt.textContent = nsub ? ('· ' + nsub + ' subline' + (nsub !== 1 ? 's' : '')) : '';
        itemSetOverride(nsub);
        window._docItemizeDirty = false;
        return true;
      }
      // LOUD failure — a whole re-itemization was lost once. (User 2026-07.)
      const msg = 'Itemization NOT saved: ' + (d.error || ('server error ' + r.status));
      if (st) { st.textContent = msg; st.style.color = '#fb7185'; }
      alert(msg + '\n\nYour rows are still here — fix the issue and press Save again.');
      return false;
    } catch (e) {
      const msg = 'Itemization NOT saved (network): ' + e.message;
      if (st) { st.textContent = msg; st.style.color = '#fb7185'; }
      alert(msg + '\n\nYour rows are still here — check your connection and press Save again.');
      return false;
    } finally { if (btn) { btn.disabled = false; btn.textContent = 'Save itemization'; } }
  }
  // ⚡ Parse payroll package: the budget page's b03.js owns the full
  // handler (status text and all — Actuals Phase 4 edits it there) and
  // overrides window.docItemizeParsePeople; this is the standalone page's
  // wiring to the SAME endpoint.
  async function parsePayrollGeneric() {
    const uid = E.uid; if (!uid) return;
    const btn = $('docItemizePeopleBtn'), st = $('docItemizeStatus');
    const orig = btn ? btn.textContent : '';
    if (btn) { btn.disabled = true; btn.textContent = '⚡ Reading document…'; }
    if (st) { st.style.color = 'var(--text-muted)'; st.textContent = 'AI is parsing the payroll package — this can take up to a minute for big documents.'; }
    try {
      const r = await fetch('/docs/upload/' + uid + '/parse-people', { method: 'POST', credentials: 'same-origin' });
      const j = await r.json().catch(() => ({}));
      if (!r.ok || !j.ok) { if (st) { st.style.color = '#e0a13a'; st.textContent = j.error || ('Parse failed (' + r.status + ')'); } return; }
      if (!j.rows || !j.rows.length) { if (st) { st.style.color = '#e0a13a'; st.textContent = 'No per-person rows found in this document.'; } return; }
      j.rows.forEach(row => itemAddRow(row.desc, row.amount, row.line_id || '', row.crew_member_id));
      window._docItemizeDirty = true;
      const blk = $('docDetailItemizeBlock'); if (blk) blk.open = true;
      const unmatched = (j.total_rows || 0) - (j.matched || 0);
      if (st) {
        st.style.color = unmatched ? '#e0a13a' : '#5fd0a0';
        st.textContent = (j.total_rows || 0) + ' people parsed (' + money(j.sum) + '), ' + (j.matched || 0) + ' matched to budget lines'
          + (unmatched ? ' — ' + unmatched + ' need a line picked (amber)' : '') + '. Review, then Save itemization.';
      }
    } catch (e) { if (st) { st.style.color = '#e0a13a'; st.textContent = 'Parse failed: ' + e.message; } }
    finally { if (btn) { btn.disabled = false; btn.textContent = orig; } }
  }

  // ── backups ──────────────────────────────────────────────────────────
  const bkMoney = v => v == null ? '—' : money(v);
  async function backupsLoad(uid) {
    E._bk = null;
    const block = $('docDetailBackupsBlock'); if (!block) return;
    block.style.display = 'none';
    const list = $('docBackupsList'), cand = $('docBackupsCandidates'), st = $('docBackupsStatus'), cnt = $('docBackupsCount');
    if (list) list.innerHTML = ''; if (cand) { cand.style.display = 'none'; cand.innerHTML = ''; }
    if (st) st.textContent = ''; if (cnt) cnt.textContent = '';
    try {
      const r = await fetch('/docs/upload/' + uid + '/backups', { credentials: 'same-origin' });
      if (!r.ok || uid !== E.uid) return;
      const d = await r.json();
      if (!d.ok || !d.targets || !d.targets.length) return;
      E._bk = d;
      block.style.display = '';
      const sel = $('docBackupsTarget');
      if (sel) sel.innerHTML = d.targets.map(t => '<option value="' + t.txn_id + '">' + (t.is_parent ? '' : '↳ ') + esc(t.label) + (t.amount != null ? ' · ' + bkMoney(t.amount) : '') + '</option>').join('');
      let n = 0, html = '';
      d.targets.forEach(t => {
        const bs = d.backups[String(t.txn_id)] || [];
        if (!bs.length) return;
        html += '<div style="margin-top:6px;font-size:10.5px;color:var(--text-muted);text-transform:uppercase;letter-spacing:.04em">' + (t.is_parent ? '' : '↳ ') + esc(t.label) + (t.amount != null ? ' · ' + bkMoney(t.amount) : '') + '</div>';
        bs.forEach(b => {
          n++;
          html += '<div style="display:flex;gap:8px;align-items:center;padding:4px 6px;border:1px solid var(--border);border-radius:6px;margin-top:3px">'
            + '<a href="#" onclick="DocEditor.openOther(' + b.doc_id + ');return false" style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#8fb4ff;text-decoration:none" title="Open this backup document">📎 ' + esc(b.vendor || b.filename) + '</a>'
            + '<span style="color:var(--text-muted);font-size:.78rem;white-space:nowrap">' + esc(b.doc_date || '') + '</span>'
            + '<b style="white-space:nowrap">' + bkMoney(b.amount) + '</b>'
            + '<button type="button" title="Detach this backup" onclick="docBackupsDetach(' + b.doc_id + ',' + t.txn_id + ')" style="background:none;border:none;color:var(--text-muted);cursor:pointer;font-size:13px">✕</button></div>';
        });
      });
      if (list) list.innerHTML = html || '<div style="color:var(--text-muted)">No backups attached yet — use <b>+ Attach backup…</b> below, or open a receipt in the Actuals queue and mark it 📎 Backup from there.</div>';
      if (cnt) cnt.textContent = n ? ('· ' + n) : '';
      if (n) block.open = true;
    } catch (e) {}
  }
  function openOther(docId) {
    if (typeof window.openDocDetail === 'function') return window.openDocDetail(docId, null);
    location.href = (E.opts.pid ? '/projects/' + E.opts.pid + '/docs/' + docId + '/editor' : '/docs/' + docId + '/editor');
  }
  function backupsShowCandidates() {
    const cand = $('docBackupsCandidates'); if (!cand || !E._bk) return;
    if (cand.style.display !== 'none') { cand.style.display = 'none'; return; }
    const cs = E._bk.candidates || [];
    cand.innerHTML = cs.length
      ? ('<div style="font-size:10.5px;color:var(--text-muted);margin-bottom:4px">Pick the document that backs up the target chosen above (closest amounts first):</div>'
        + cs.map(c => '<div style="display:flex;gap:8px;align-items:center;padding:4px 6px;border:1px solid var(--border);border-radius:6px;margin-top:3px">'
          + '<span style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + esc(c.vendor || c.filename) + (c.category ? ' <span style="color:var(--text-muted);font-size:.75rem">' + esc(c.category) + '</span>' : '') + '</span>'
          + '<span style="color:var(--text-muted);font-size:.78rem;white-space:nowrap">' + esc(c.doc_date || '') + '</span>'
          + '<b style="white-space:nowrap">' + bkMoney(c.amount) + '</b>'
          + '<button type="button" onclick="docBackupsAttach(' + c.doc_id + ')" title="' + (c.will_absorb ? 'Attach — its own charge stops counting (it becomes documentation)' : 'Attach as backup documentation') + '" style="padding:3px 10px;border-radius:5px;background:#1f7a4d;border:none;color:#fff;font-size:11px;cursor:pointer">Attach</button></div>').join(''))
      : '<div style="color:var(--text-muted);font-size:.85rem">No other documents on this project to attach.</div>';
    cand.style.display = '';
  }
  async function backupsAttach(docId) {
    const uid = E.uid; if (!uid) return;
    const sel = $('docBackupsTarget'), st = $('docBackupsStatus');
    const target = sel ? parseInt(sel.value) : null; if (!target) return;
    if (st) { st.style.color = 'var(--text-muted)'; st.textContent = 'Attaching…'; }
    try {
      const r = await fetch('/docs/upload/' + uid + '/attach-backup', { method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ doc_id: docId, target_txn_id: target }) });
      const j = await r.json();
      if (r.ok && j.ok) { if (st) { st.style.color = '#5fd0a0'; st.textContent = j.absorbed ? '📎 Attached — that document’s own charge no longer counts.' : '📎 Attached as backup documentation.'; } backupsLoad(uid); }
      else if (st) { st.style.color = '#e0a13a'; st.textContent = j.error || ('Failed (' + r.status + ')'); }
    } catch (e) { if (st) { st.style.color = '#e0a13a'; st.textContent = 'Error: ' + e.message; } }
  }
  async function backupsDetach(docId, targetId) {
    const uid = E.uid; if (!uid) return;
    const st = $('docBackupsStatus');
    try {
      const r = await fetch('/docs/upload/' + uid + '/detach-backup', { method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ doc_id: docId, target_txn_id: targetId }) });
      const j = await r.json();
      if (r.ok && j.ok) { if (st) { st.style.color = 'var(--text-muted)'; st.textContent = j.restored ? 'Detached — the document’s charge is back in the queue.' : 'Detached.'; } backupsLoad(uid); }
      else if (st) { st.style.color = '#e0a13a'; st.textContent = j.error || ('Failed (' + r.status + ')'); }
    } catch (e) { if (st) { st.style.color = '#e0a13a'; st.textContent = 'Error: ' + e.message; } }
  }

  // ── travel ───────────────────────────────────────────────────────────
  function travelCollect() {
    const g = id => { const el = $(id); return el ? (el.value || '').trim() : undefined; };
    const d = Object.assign({}, E._travel.data || {});
    const map = { ddTrvKind: 'kind', ddTrvConf: 'confirmation_no', ddTrvAirline: 'airline', ddTrvFlightNo: 'flight_no',
      ddTrvDepAir: 'depart_airport', ddTrvArrAir: 'arrive_airport', ddTrvHotel: 'hotel_name', ddTrvCheckIn: 'check_in',
      ddTrvCheckOut: 'check_out', ddTrvRoom: 'room_type', ddTrvRental: 'rental_co', ddTrvPickup: 'pickup_location',
      ddTrvDropoff: 'dropoff_location', ddTrvPhone: 'contact_phone' };
    Object.keys(map).forEach(id => { const v = g(id); if (v !== undefined) d[map[id]] = v; });
    E._travel.data = d;
    return d;
  }
  function travelRenderFields() {
    const t = E._travel.data || {}, f = $('docDetailTravelFields'); if (!f) return;
    const kind = t.kind || 'flight';
    const row = (id, label, val) => '<div style="margin-bottom:5px"><label style="font-size:10px;color:var(--text-muted)">' + label + '</label><input id="' + id + '" value="' + esc(val || '') + '" style="width:100%;font-size:12px"></div>';
    let html = '';
    if (!t.confirmation_no && !t.kind) html += '<div style="color:var(--text-muted);font-size:11px;margin-bottom:6px">No travel details yet — click <b>Pull from document</b> to read this document.</div>';
    html += '<div style="margin-bottom:5px"><label style="font-size:10px;color:var(--text-muted)">Type</label><select id="ddTrvKind" style="width:100%;font-size:12px">'
      + '<option value="flight"' + (kind === 'flight' ? ' selected' : '') + '>✈️ Flight</option>'
      + '<option value="hotel"' + (kind === 'hotel' ? ' selected' : '') + '>🏨 Hotel</option>'
      + '<option value="car_rental"' + (kind === 'car_rental' ? ' selected' : '') + '>🚗 Car rental</option>'
      + '<option value="car_service"' + (kind === 'car_service' ? ' selected' : '') + '>🚐 Car service</option></select></div>';
    html += row('ddTrvConf', 'Confirmation #', t.confirmation_no);
    if (kind === 'flight') html += row('ddTrvAirline', 'Airline', t.airline) + row('ddTrvFlightNo', 'Flight #', t.flight_no) + row('ddTrvDepAir', 'From (IATA)', t.depart_airport) + row('ddTrvArrAir', 'To (IATA)', t.arrive_airport);
    else if (kind === 'hotel') html += row('ddTrvHotel', 'Hotel', t.hotel_name) + row('ddTrvCheckIn', 'Check-in (YYYY-MM-DD)', t.check_in) + row('ddTrvCheckOut', 'Check-out (YYYY-MM-DD)', t.check_out) + row('ddTrvRoom', 'Room type', t.room_type);
    else if (kind === 'car_rental') html += row('ddTrvRental', 'Rental company', t.rental_co) + row('ddTrvPickup', 'Pickup location', t.pickup_location);
    else if (kind === 'car_service') html += row('ddTrvRental', 'Company', t.rental_co) + row('ddTrvPickup', 'Pickup location', t.pickup_location) + row('ddTrvDropoff', 'Drop-off location', t.dropoff_location) + row('ddTrvPhone', 'Contact phone', t.contact_phone);
    f.innerHTML = html;
    const ks = $('ddTrvKind'); if (ks) ks.onchange = () => { travelCollect(); travelRenderFields(); };
  }
  async function loadTravel(uid) {
    const block = $('docDetailTravelBlock'); if (!block || !E.opts.pid) return;
    E._travel = { uid, data: null, people: [], bid: null };
    const stEl = $('docDetailTravelStatus'); if (stEl) stEl.textContent = '';
    const t = (E.status && E.status.travel) || null;
    const cat = (($('docDetailCategory') || {}).value || '').toLowerCase();
    const vendor = (($('docDetailVendor') || {}).value || '').toLowerCase();
    const looksTravel = /hotel|air|flight|travel|lodging|car ?rental|rental/.test(cat + ' ' + vendor);
    const suspected = (t && t.is_travel) || looksTravel;
    block.open = !!suspected;
    const hint = $('docDetailTravelHint');
    if (hint) hint.textContent = (t && t.is_travel) ? '· extracted ✓' : (suspected ? '· looks like travel — Pull to read' : '· not detected — expand to Pull');
    E._travel.data = (t && t.is_travel) ? t : {};
    travelRenderFields();
    try {
      if (!window._ddRosterCache) window._ddRosterCache = await (await fetch('/projects/' + E.opts.pid + '/travel/people', { credentials: 'same-origin' })).json();
      const pr = window._ddRosterCache;
      E._travel.people = pr.people || []; E._travel.bid = pr.bid;
      const sel = $('docDetailTravelPerson');
      if (sel) sel.innerHTML = '<option value="">— pick person —</option>' + E._travel.people.map((p, i) => '<option value="' + i + '">' + esc(p.person) + (p.role ? (' — ' + esc(p.role)) : '') + '</option>').join('');
      let pidx = -1;
      const mp = t && t.matched_person;
      if (mp && mp.line_id != null) pidx = E._travel.people.findIndex(p => p.line_id === mp.line_id && (p.instance || 1) === (mp.instance || 1));
      if (pidx < 0 && t && t.traveler_name) {
        const nm = t.traveler_name.toLowerCase(), toks = nm.split(/[^a-z]+/).filter(x => x.length > 1);
        pidx = E._travel.people.findIndex(p => {
          const pn = (p.person || '').toLowerCase(); if (!pn) return false;
          if (pn.includes(nm) || nm.includes(pn)) return true;
          const pt = pn.split(/[^a-z]+/).filter(x => x.length > 1);
          const shared = toks.filter(x => pt.includes(x)).length;
          return shared >= 2 || (shared >= 1 && (toks.every(x => pt.includes(x)) || pt.every(x => toks.includes(x))));
        });
      }
      if (pidx >= 0 && sel) { sel.value = String(pidx); if (stEl) stEl.textContent = '✓ Auto-matched to ' + (E._travel.people[pidx].person || 'a crew member') + ' — pick the day and Apply.'; }
      if (!E._travel.bid && stEl) stEl.textContent = 'No Working/Estimated budget found to attach travel to.';
    } catch (e) {}
    const di = $('docDetailTravelDate');
    const pre = (t && (t.check_in || (t.depart_at || '').slice(0, 10))) || (($('docDetailDocDate') || {}).value || '');
    if (di) di.value = (pre || '').slice(0, 10);
  }
  async function extractTravel() {
    const uid = E._travel.uid; if (!uid) return;
    const btn = $('docDetailTravelExtractBtn'), st = $('docDetailTravelStatus');
    if (btn) { btn.disabled = true; btn.textContent = 'Reading…'; }
    if (st) st.textContent = 'Reading the document…';
    try {
      const r = await fetch('/docs/upload/' + uid + '/extract-travel', { method: 'POST', credentials: 'same-origin' });
      const d = await r.json();
      if (r.ok && d.travel) {
        E._travel.data = d.travel.is_travel ? d.travel : (d.travel || {});
        travelRenderFields();
        const hint = $('docDetailTravelHint'); if (hint) hint.textContent = d.travel.is_travel ? '· extracted ✓' : '· no travel details found';
        if (st) st.textContent = d.travel.is_travel ? 'Extracted ✓ — review and Apply.' : 'No travel details found in this document.';
        const mp = d.travel && d.travel.matched_person, sel = $('docDetailTravelPerson');
        if (mp && mp.line_id != null && sel && E._travel.people) {
          const i = E._travel.people.findIndex(p => p.line_id === mp.line_id && (p.instance || 1) === (mp.instance || 1));
          if (i >= 0) { sel.value = String(i); if (st) st.textContent = '✓ Auto-matched to ' + (E._travel.people[i].person || 'a crew member') + ' — pick the day and Apply.'; }
        }
      } else if (st) st.textContent = 'Extract failed.';
    } catch (e) { if (st) st.textContent = 'Extract error: ' + e.message; }
    finally { if (btn) { btn.disabled = false; btn.textContent = '⤓ Pull from document'; } }
  }
  async function applyTravel() {
    const uid = E._travel.uid; if (!uid) return;
    const st = $('docDetailTravelStatus');
    const pidx = ($('docDetailTravelPerson') || {}).value, date = ($('docDetailTravelDate') || {}).value;
    if (pidx === '' || pidx == null || !E._travel.people[pidx]) { if (st) st.textContent = 'Pick a person first.'; return; }
    if (!date) { if (st) st.textContent = 'Pick a travel day.'; return; }
    if (!E._travel.bid) { if (st) st.textContent = 'No budget to attach travel to.'; return; }
    const person = E._travel.people[pidx], t = travelCollect();
    const payload = Object.assign({ doc_upload_id: uid, line_id: person.line_id, instance: person.instance, date, kind: t.kind || 'flight' }, t);
    const btn = $('docDetailTravelApplyBtn'); if (btn) { btn.disabled = true; btn.textContent = 'Applying…'; }
    try {
      const r = await fetch('/projects/' + E.opts.pid + '/budget/' + E._travel.bid + '/travel/apply-doc', { method: 'POST', headers: { 'Content-Type': 'application/json' }, credentials: 'same-origin', body: JSON.stringify(payload) });
      const d = await r.json();
      if (r.ok && d.ok) {
        if (st) st.textContent = '✓ Added to ' + esc(person.person) + "'s travel (" + d.kind + ').';
        if (person.crew_member_id) { const cs = $('docDetailCrewId'); if (cs) cs.value = String(person.crew_member_id); }
      } else if (st) st.textContent = 'Apply failed: ' + (d.error || r.status);
    } catch (e) { if (st) st.textContent = 'Apply error: ' + e.message; }
    finally { if (btn) { btn.disabled = false; btn.textContent = 'Apply to Travel tab'; } }
  }

  // ── match to transactions / log expense ──────────────────────────────
  async function logExpense() {
    const uid = E.uid; if (!uid) return;
    const status = $('docDetailLogExpenseStatus');
    if (!confirm('Log this receipt as a standalone expense?\n\nUse this when the purchase was cash / check / petty cash and no card or bank charge will ever appear to match it. The expense will show on the Actuals tab, ready to code to a budget line.')) return;
    if (status) { status.textContent = 'Logging…'; status.style.color = ''; }
    try {
      const r = await fetch('/docs/upload/' + uid + '/log-expense', { method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' }, body: '{}' });
      const d = await r.json();
      if (!r.ok) { if (status) { status.textContent = (d && d.error) || ('Failed (' + r.status + ')'); status.style.color = '#e08080'; } return; }
      if (status) { status.textContent = d.already_activated ? 'Already an expense — marked resolved (no charge to match).' : '✓ Expense logged — code it to a line on the Actuals tab.'; status.style.color = '#74c69d'; }
      loadCoding(uid);
    } catch (e) { if (status) { status.textContent = 'Failed: ' + e.message; status.style.color = '#e08080'; } }
  }
  async function findTxns() {
    const uid = E.uid; if (!uid || !E.opts.pid) return;
    const host = $('docDetailTxnResults');
    const inclMatched = ($('docDetailInclMatched') || {}).checked;
    host.innerHTML = '<div style="font-size:11px;color:var(--text-muted)">Searching…</div>';
    try {
      const d = await (await fetch('/projects/' + E.opts.pid + '/docs/upload/' + uid + '/transaction-candidates?include_matched=' + (inclMatched ? 1 : 0), { credentials: 'same-origin' })).json();
      E._txnDoc = d.doc || {}; E._txnCands = d.candidates || [];
      renderTxns();
    } catch (e) { host.innerHTML = '<div style="color:#e08080;font-size:11px">Error: ' + e.message + '</div>'; }
  }
  function renderTxns() {
    const host = $('docDetailTxnResults'), cands = E._txnCands || [], doc = E._txnDoc || {};
    const moneyAbs = a => FP.money(a, {abs: true});   // shared rule (fp-utils.js)
    const signTag = a => { const n = Number(a); if (!isFinite(n)) return '<span style="color:var(--text-muted)">$?</span>';
      return n < 0 ? '<span style="color:#5fd0a0;font-weight:600">+' + moneyAbs(n) + '</span> <span style="color:#5fd0a0">refund</span>' : '<span style="font-weight:600">−' + moneyAbs(n) + '</span> <span style="color:var(--text-muted)">charge</span>'; };
    if (!cands.length) { host.innerHTML = '<div style="font-size:11px;color:var(--text-muted)">No candidate charges' + (($('docDetailInclMatched') || {}).checked ? '' : ' — try “include already-matched”') + '.</div>'; return; }
    const docSign = (doc.total_signed != null && Number(doc.total_signed) !== 0) ? Math.sign(Number(doc.total_signed)) : 0;
    const docKind = docSign < 0 ? 'refund' : (docSign > 0 ? 'charge' : null);
    const rows = cands.map((c, i) => {
      const cSign = (c.amount != null && Number(c.amount) !== 0) ? Math.sign(Number(c.amount)) : 0;
      const mismatch = (docSign !== 0 && cSign !== 0 && docSign !== cSign);
      const matched = (c.match_status !== 'unmatched' || c.doc_upload_id);
      const details = '<div id="dd-txn-det-' + i + '" style="display:none;margin-top:5px;padding:6px 8px;background:var(--bg-subtle,rgba(255,255,255,.03));border-radius:6px;font-size:11px;line-height:1.6;color:var(--text-muted)">'
        + '<div><b style="color:var(--text)">' + (esc(c.vendor) || 'charge') + '</b></div>'
        + '<div>Amount: ' + signTag(c.amount) + ' &nbsp;<span style="opacity:.7">(raw ' + Number(c.amount).toFixed(2) + ')</span></div>'
        + '<div>Date: ' + (c.date || '—') + (c.day_gap != null ? ' · ' + c.day_gap + 'd from receipt' : '') + '</div>'
        + '<div>Card: ' + (c.card ? ('••' + c.card) : '—') + (c.same_card ? ' <span style="color:#5fd0a0">· same card ✓</span>' : '') + '</div>'
        + '<div>Coded: ' + (c.coded ? esc(c.coded) : '<span style="opacity:.7">uncoded</span>') + '</div>'
        + '<div>Vendor match: ' + (c.vendor_match ? '<span style="color:#5fd0a0">yes</span>' : 'weak') + ' · Kind: ' + c.kind + '</div>'
        + '<div>Status: ' + esc(c.match_status || 'unmatched') + (matched ? ' <span style="color:#e0c060">· already linked to a receipt</span>' : '') + '</div>'
        + (mismatch ? '<div style="color:#e08a6a;margin-top:3px">⚠ This is a <b>' + (cSign < 0 ? 'refund' : 'charge') + '</b> but the receipt is a <b>' + docKind + '</b> — signs disagree.</div>' : '') + '</div>';
      return '<div style="border:1px solid ' + (mismatch ? '#7a4a3a' : 'var(--border)') + ';border-radius:6px;margin-bottom:5px;padding:6px 8px;font-size:11.5px">'
        + '<label style="display:flex;align-items:flex-start;gap:8px;cursor:pointer">'
        + '<input type="checkbox" class="dd-txn-cb" value="' + c.tid + '" data-amt="' + c.amount + '" onchange="ddTxnSelChanged()" style="margin-top:2px">'
        + '<span style="flex:1;min-width:0"><span style="font-weight:600">' + (esc(c.vendor) || 'charge') + '</span>'
        + (c.kind === 'exact' ? '<span style="color:#5fd0a0;margin-left:5px">exact</span>' : '<span style="color:var(--text-muted);margin-left:5px">part</span>')
        + (matched ? '<span style="color:#e0c060;margin-left:5px" title="already linked to a receipt">matched</span>' : '')
        + (mismatch ? '<span style="color:#e08a6a;margin-left:5px" title="sign mismatch">⚠ sign</span>' : '')
        + '<br><span style="display:inline-block;margin-top:2px">' + signTag(c.amount) + '<span style="color:var(--text-muted)"> · ' + (c.date || '—') + (c.card ? (' · ••' + c.card) : '') + (c.coded ? (' · ' + esc(c.coded)) : '') + '</span></span></span>'
        + '<button type="button" onclick="event.preventDefault();event.stopPropagation();ddTxnToggleDet(' + i + ')" style="flex:none;padding:1px 7px;border:1px solid var(--border);border-radius:5px;background:transparent;color:var(--text-muted);font-size:11px;cursor:pointer">details</button>'
        + '</label>' + details + '</div>';
    }).join('');
    const docHdr = docKind ? '<div style="font-size:11px;color:var(--text-muted);margin-bottom:6px">This receipt is a <b style="color:' + (docSign < 0 ? '#5fd0a0' : 'var(--text)') + '">' + docKind + '</b> for ' + moneyAbs(doc.total_signed) + '. Matching charges should also be a <b>' + docKind + '</b>.</div>' : '';
    host.innerHTML = docHdr + rows
      + '<div id="dd-txn-sum" style="font-size:11px;color:var(--text-muted);margin:4px 0"></div>'
      + '<button type="button" id="dd-txn-link" onclick="ddLinkTxns()" disabled style="padding:5px 11px;border-radius:6px;background:#1f6f4a;border:1px solid #2a8a5e;color:#fff;font-size:12px;cursor:pointer;opacity:.5">Link selected</button>';
    txnSelChanged();
  }
  function txnToggleDet(i) { const el = $('dd-txn-det-' + i); if (el) el.style.display = (el.style.display === 'none' || !el.style.display) ? 'block' : 'none'; }
  function txnSelChanged() {
    const cbs = [...document.querySelectorAll('.dd-txn-cb:checked')];
    const sum = cbs.reduce((a, cb) => a + Math.abs(parseFloat(cb.dataset.amt) || 0), 0);
    const total = Math.abs((E._txnDoc || {}).total || 0);
    const sumEl = $('dd-txn-sum'), btn = $('dd-txn-link');
    if (!cbs.length) { if (sumEl) sumEl.textContent = ''; if (btn) { btn.disabled = true; btn.style.opacity = '.5'; btn.textContent = 'Link selected'; } return; }
    const match = total && Math.abs(sum - total) <= 0.02;
    if (sumEl) sumEl.innerHTML = cbs.length + ' selected · sum $' + sum.toFixed(2) + ' / receipt $' + total.toFixed(2) + (match ? ' <span style="color:#5fd0a0">✓ matches</span>' : ' <span style="color:#e0c060">≠ total</span>');
    if (btn) { const ok = (cbs.length === 1) || match; btn.disabled = !ok; btn.style.opacity = ok ? '1' : '.5'; btn.textContent = cbs.length >= 2 ? ('Link as split (' + cbs.length + ')') : 'Link selected'; }
  }
  async function linkTxns() {
    const uid = E.uid, tids = [...document.querySelectorAll('.dd-txn-cb:checked')].map(cb => parseInt(cb.value));
    if (!uid || !tids.length || !E.opts.pid) return;
    const btn = $('dd-txn-link'); if (btn) { btn.disabled = true; btn.textContent = '…'; }
    const base = '/projects/' + E.opts.pid + '/actuals';
    try {
      if (tids.length >= 2) {
        const r = await fetch(base + '/link-split', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ keeper_doc_id: parseInt(uid), charge_tids: tids }) });
        const j = await r.json().catch(() => ({}));
        if (!r.ok) { alert('Split link failed: ' + (j.error || r.status)); if (btn) btn.disabled = false; return; }
        alert('Linked this receipt to ' + (j.linked || tids.length) + ' charges as a split ✓');
      } else {
        const tid = tids[0];
        const r = await fetch(base + '/transaction/' + tid + '/link-doc', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ doc_upload_id: parseInt(uid) }) });
        if (!r.ok) { const j = await r.json().catch(() => ({})); alert('Link failed: ' + (j.error || r.status)); if (btn) btn.disabled = false; return; }
        await fetch(base + '/transaction/' + tid + '/confirm-match', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' });
        alert('Linked ✓');
      }
      findTxns();
      loadCoding(uid);
    } catch (e) { alert('Error: ' + e.message); if (btn) btn.disabled = false; }
  }

  // ── re-OCR ───────────────────────────────────────────────────────────
  async function reocr() {
    const uid = E.uid; if (!uid) return;
    const btn = $('docDetailReocrBtn'), st = $('docDetailReocrStatus');
    if (btn) { btn.disabled = true; btn.textContent = '🔄 Re-reading…'; }
    if (st) { st.textContent = 'Sending to OCR…'; st.style.color = 'var(--text-muted)'; }
    try {
      const r = await fetch('/docs/upload/' + uid + '/reocr', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) { if (st) { st.textContent = '✕ ' + (d.error || ('HTTP ' + r.status)); st.style.color = '#e08080'; } return; }
      const setIf = (id, val) => { if (val != null && val !== '') { const el = $(id); if (el) el.value = val; } };
      if (d.vendor) setIf('docDetailVendor', d.vendor);
      if (d.amount != null) { const el = $('docDetailAmount'); if (el) el.value = fmtMoney(d.amount); }
      if (d.original_amount != null) { const el = $('docDetailOrigAmount'); if (el) el.value = fmtMoney(d.original_amount); }
      setIf('docDetailOrigCurrency', d.original_currency); setIf('docDetailDocDate', d.doc_date); setIf('docDetailCard4', d.card_last4);
      if (st) { st.textContent = d.fx_note ? ('✓ ' + d.fx_note + ' — review & Save') : '✓ Re-read — review the fields, then Save'; st.style.color = '#5fd0a0'; }
    } catch (e) { if (st) { st.textContent = '✕ ' + e.message; st.style.color = '#e08080'; } }
    finally { if (btn) { btn.disabled = false; btn.textContent = '🔄 Re-run OCR'; } }
  }

  // ── delete ───────────────────────────────────────────────────────────
  async function del() {
    const uid = E.uid; if (!uid) return;
    const titleEl = $('docDetailTitle');
    const name = (titleEl && titleEl.textContent) ? titleEl.textContent.trim() : ('Upload #' + uid);
    if (!confirm('Move "' + name + '" to the Trash?\n\nThis will:\n  • Remove it from the Docs list and Actuals\n  • Unlink it from any matched bank charge (the charge keeps its coding)\n  • Move the Dropbox copies to /_TRASH/\n\nYou can restore it from Docs → 🗑 Trash (files auto-purge after 30 days).')) return;
    const btn = $('docDetailDeleteBtn'); if (btn) { btn.disabled = true; btn.textContent = 'Deleting…'; }
    try {
      const r = await fetch('/docs/upload/' + uid + '/delete', { method: 'POST', credentials: 'same-origin' });
      const j = await r.json().catch(() => ({}));
      if (!r.ok || (j && j.error)) { alert('Delete failed: ' + (j.error || r.status)); if (btn) { btn.disabled = false; btn.textContent = '🗑 Delete document'; } return; }
      window._docItemizeDirty = false;
      if (E.opts.onDeleted) E.opts.onDeleted(uid);
    } catch (e) { alert('Delete failed: ' + e.message); if (btn) { btn.disabled = false; btn.textContent = '🗑 Delete document'; } }
  }

  // ── PO actions (page provides the modals) ────────────────────────────
  function poContext() {
    return { uid: E.uid, vendor: ($('docDetailVendor') || {}).value || '', amount: parseMoney(($('docDetailAmount') || {}).value) || '',
             filename: ($('docDetailTitle') || {}).textContent || '', docDate: ($('docDetailDocDate') || {}).value || '' };
  }
  async function createPo() {
    if (!E.uid) { alert('No document loaded'); return; }
    // Persist pending edits first so a type change (invoice → estimate)
    // survives the trip and the file re-files to the right folder.
    try { await save(); } catch (e) { console.warn('[DocEditor] pre-save before Create PO failed', e); }
    if (E.opts.poCreate) E.opts.poCreate(poContext());
  }
  function addToPo() {
    if (!E.uid) { alert('No document loaded'); return; }
    if (E.opts.poAddTo) E.opts.poAddTo(poContext());
  }

  function reset() { E.uid = null; window._docDetailCurrentUid = null; window._docItemizeDirty = false; }

  // Inline-onclick globals used by templates/_doc_editor.html. b03.js may
  // override docItemizeParsePeople and _ditemLineChanged with the budget
  // page's richer versions (it loads after this file).
  window._itemAddRow = itemAddRow;
  window._itemUpdateTotal = itemUpdateTotal;
  window._itemSetOverride = itemSetOverride;
  window._ditemLineChanged = ditemLineChanged;
  window.docItemizePull = itemizePull;
  window.docItemizeLoad = itemizeLoad;
  window.docItemizeSave = itemizeSave;
  window.docItemizeParsePeople = parsePayrollGeneric;
  window.docBackupsLoad = backupsLoad;
  window.docBackupsShowCandidates = backupsShowCandidates;
  window.docBackupsAttach = backupsAttach;
  window.docBackupsDetach = backupsDetach;
  window.docDetailExtractTravel = extractTravel;
  window.docDetailApplyTravel = applyTravel;
  window.docDetailLogExpense = logExpense;
  window.docDetailFindTxns = findTxns;
  window.ddTxnToggleDet = txnToggleDet;
  window.ddTxnSelChanged = txnSelChanged;
  window.ddLinkTxns = linkTxns;
  window.docDetailReocr = reocr;
  window.docDetailDelete = del;
  window.docDetailCreatePo = createPo;
  window.docDetailAddToPo = addToPo;

  window.DocEditor = {
    init, open, fill, save, collect, sig, dirty, markClean, reset, applyTypeUI,
    loadCoding, setLine, fillPicker, fillPickerFromTemplate, setLineGeneric,
    itemAddRow, itemUpdateTotal, itemizeLoad, itemizeSave, itemizePull, itemSetOverride,
    backupsLoad, loadTravel, findTxns, logExpense, reocr, delete: del, resolveDup, openOther,
    get uid() { return E.uid; }, get status() { return E.status; }, get state() { return E; },
  };
})();
