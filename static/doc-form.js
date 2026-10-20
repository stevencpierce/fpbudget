/* DocForm — THE shared document-metadata form logic (owner 2026-10-20:
 * "do the deeper unification — you open up a document you should have the
 * same document editor").
 *
 * One copy of everything that used to drift between the budget-page
 * doc-detail MODAL (b03.js) and the standalone doc EDITOR page
 * (doc_editor.html): the per-type field spec, type-driven field
 * visibility, the supporting-doc subtype control, the person/vendor
 * quick-add, and the OCR person auto-suggest. Both surfaces render the
 * SAME markup (templates/_doc_fields.html, docDetail* ids) and delegate
 * here; page-specific glue (preview, itemize, coding, navigation) stays
 * in each page.
 *
 * Load order: this file BEFORE b03.js on the budget page, and in
 * doc_editor.html's script block. No dependencies.
 */
(function () {
  'use strict';
  const $ = id => document.getElementById(id);

  // Per-type field spec. vendor = label for the top field; vendorPh =
  // placeholder; amount = show Amount + Card last-4; date = date label;
  // docnum = Document-# label (null hides it); crew/location = show the
  // Person-Vendor / Location pickers.
  const FIELD_SPEC = {
    receipt:        { vendor: 'Vendor',       amount: true,  date: 'Receipt Date',    docnum: null },
    invoice:        { vendor: 'Vendor',       amount: true,  date: 'Invoice Date',    docnum: 'Invoice #', crew: true,  location: false },
    estimate:       { vendor: 'Vendor',       amount: true,  date: 'Estimate Date',   docnum: 'Estimate #',  crew: false, location: false },
    quote:          { vendor: 'Vendor',       amount: true,  date: 'Quote Date',      docnum: 'Quote #',     crew: false, location: false },
    purchase_order: { vendor: 'Vendor',       amount: true,  date: 'PO Date',         docnum: 'PO #',        crew: false, location: false },
    contract:       { vendor: 'Counterparty', amount: false, date: 'Effective Date',  docnum: 'Contract #',  crew: true,  location: false },
    insurance:      { vendor: 'Insured / Carrier', amount: false, date: 'Expiration Date', docnum: 'Policy #', crew: false, location: true },
    tax_form:       { vendor: 'Entity / Filer', amount: false, date: 'Tax Year / Date', docnum: 'Tax ID (EIN/SSN)', crew: true, location: false },
    payroll:        { vendor: 'Vendor / Employee', amount: true, date: 'Pay Date',    docnum: 'Check / Ref #', crew: true, location: false },
    timecard:       { vendor: 'Employee',     amount: false, date: 'Week Ending',     docnum: 'Ref #',       crew: true,  location: false },
    legal:          { vendor: 'Counterparty', amount: false, date: 'Doc Date',        docnum: 'Reference #', crew: false, location: false },
    release:        { vendor: 'Talent / Location', amount: false, date: 'Signed Date', docnum: null,         crew: true,  location: true },
    employee_vendor_doc: { vendor: 'Document Type', vendorPh: 'e.g. Direct Deposit, W-9, ID, DTR…', amount: false, date: 'Doc Date', docnum: 'Doc # / ID', crew: true, location: false },
    misc:           { vendor: 'Vendor',       amount: true,  date: 'Doc Date',        docnum: null,          crew: false, location: false },
  };

  // The "vendor" value a save should persist: the controlled Document-Type
  // dropdown for supporting docs, the free-text input otherwise.
  function vendorValue() {
    const cat = (($('docDetailCategory') || {}).value || '').toLowerCase();
    if (cat === 'employee_vendor_doc') {
      const s = $('docDetailDocSubtype');
      return s ? (s.value || '') : '';
    }
    const i = $('docDetailVendor');
    return i ? (i.value || '') : '';
  }

  // Select the matching dropdown option for an existing supporting-doc
  // value; legacy free-text not in the list is injected so it's kept.
  function setSubtype(val) {
    const s = $('docDetailDocSubtype');
    if (!s) return;
    const v = (val || '').trim();
    const inj = s.querySelector('option[data-injected="1"]');
    if (inj) inj.remove();
    let matched = false;
    Array.from(s.options).forEach(o => {
      if (o.value && o.value.toLowerCase() === v.toLowerCase()) matched = true;
    });
    if (v && !matched) {
      const opt = document.createElement('option');
      opt.textContent = v; opt.value = v; opt.dataset.injected = '1';
      s.insertBefore(opt, s.options[1] || null);
    }
    s.value = v || '';
  }

  // Type-driven field visibility. hooks: { uid, onAutoApplied } — uid
  // enables the person auto-suggest; onAutoApplied fires after a
  // confident suggestion pre-selects the picker (the modal uses it to
  // re-capture its dirty signature). Returns the spec so callers can run
  // page-specific extras (PO actions, missing-field highlight).
  function applyTypeUI(type, hooks) {
    hooks = hooks || {};
    const spec = FIELD_SPEC[(type || '').toLowerCase()] || FIELD_SPEC.misc;
    const vL = $('docDetailVendorLabel');
    if (vL) vL.textContent = spec.vendor || 'Vendor';
    const vIn = $('docDetailVendor');
    if (vIn) vIn.placeholder = spec.vendorPh || 'Vendor name…';
    const subSel = $('docDetailDocSubtype');
    const isEvd = (type || '').toLowerCase() === 'employee_vendor_doc';
    if (subSel && vIn) {
      subSel.style.display = isEvd ? '' : 'none';
      vIn.style.display    = isEvd ? 'none' : '';
      if (isEvd && !subSel.value && vIn.value) setSubtype(vIn.value);
    }
    const dL = $('docDetailDocDateLabel');
    if (dL) dL.textContent = spec.date || 'Doc Date';
    const amtField = $('docDetailAmountField');
    if (amtField) amtField.style.display = spec.amount ? '' : 'none';
    const card4Field = $('docDetailCard4Field');
    if (card4Field) card4Field.style.display = spec.amount ? '' : 'none';
    const dnField = $('docDetailDocNumField');
    const dnLabel = $('docDetailDocNumLabel');
    if (spec.docnum) {
      if (dnField) dnField.style.display = '';
      if (dnLabel) dnLabel.textContent = spec.docnum;
    } else if (dnField) {
      dnField.style.display = 'none';
    }
    const crewField = $('docDetailCrewField');
    if (crewField) crewField.style.display = spec.crew ? '' : 'none';
    const locField = $('docDetailLocationField');
    if (locField) locField.style.display = spec.location ? '' : 'none';
    if (spec.crew && hooks.uid) crewSuggestLoad(hooks.uid, hooks);
    else { const sb = $('docDetailCrewSuggest'); if (sb) sb.style.display = 'none'; }
    const t = (type || '').toLowerCase();
    const rdLabel = $('docDetailReceiptDetailLabel');
    if (rdLabel) {
      rdLabel.textContent = t === 'invoice' ? 'Invoice detail'
        : t === 'receipt' ? 'Receipt detail'
        : 'Document detail';
    }
    const tipWrap = $('docDetailTipWrap');
    if (tipWrap) tipWrap.style.display = (t === 'invoice') ? 'none' : '';
    return spec;
  }

  // ── Person auto-suggest (owner 2026-10-20) ──────────────────────────
  // One fetch per doc; a confident match (≥0.9) pre-selects the picker
  // ("Save to confirm"), a weaker one shows a one-click attach chip.
  // Never overrides a value already set.
  let _sugUid = null;
  function crewSuggestLoad(uid, hooks) {
    hooks = hooks || {};
    const sel = $('docDetailCrewId');
    const box = $('docDetailCrewSuggest');
    if (!uid || !sel || !box) return;
    if (sel.value) { box.style.display = 'none'; return; }
    if (_sugUid === uid) return;
    _sugUid = uid;
    fetch('/docs/upload/' + uid + '/crew-suggest', { credentials: 'same-origin' })
      .then(r => (r.ok ? r.json() : null))
      .then(d => {
        const s = d && d.suggestion;
        if (!s || sel.value) return;
        if (hooks.uidStillCurrent && !hooks.uidStillCurrent(uid)) return;
        const esc = t => String(t).replace(/</g, '&lt;');
        const opt = Array.from(sel.options).find(o => o.value === String(s.crew_member_id));
        if (!opt) return;
        if (s.confidence >= 0.9) {
          sel.value = String(s.crew_member_id);
          box.innerHTML = '👤 Auto-matched <b>' + esc(s.name) + '</b> from the '
            + esc(s.matched_on || 'document') + (s.on_project ? ' (on this project)' : '')
            + ' — Save to confirm · <a href="#" style="color:#e0a13a">clear</a>';
          box.querySelector('a').onclick = e => { e.preventDefault(); sel.value = ''; box.style.display = 'none'; };
          if (hooks.onAutoApplied) hooks.onAutoApplied(s);
        } else {
          box.innerHTML = '👤 Looks like <b>' + esc(s.name) + '</b>'
            + (s.on_project ? ' (on this project)' : '')
            + ' — <a href="#" style="color:#5fd0a0">attach</a>';
          box.querySelector('a').onclick = e => { e.preventDefault(); sel.value = String(s.crew_member_id); box.style.display = 'none'; };
        }
        box.style.color = 'var(--text-muted)';
        box.style.display = '';
      })
      .catch(() => {});
  }
  function resetSuggest() { _sugUid = null; const b = $('docDetailCrewSuggest'); if (b) b.style.display = 'none'; }

  // ── Quick-add person/vendor (shared /crew/new endpoint) ─────────────
  function newCrewShow() {
    const d = $('docDetailNewCrew');
    if (d) d.style.display = '';
    const n = $('docDetailNewCrewName');
    if (n) n.focus();
  }
  function newCrewHide() {
    const d = $('docDetailNewCrew');
    if (d) d.style.display = 'none';
    const n = $('docDetailNewCrewName');
    if (n) n.value = '';
    const st = $('docDetailNewCrewStatus');
    if (st) st.textContent = '';
  }
  async function newCrewSave() {
    const name = (($('docDetailNewCrewName') || {}).value || '').trim();
    const isVendor = !!($('docDetailNewCrewVendor') || {}).checked;
    const status = $('docDetailNewCrewStatus');
    if (!name) { if (status) status.textContent = 'Name required'; return; }
    if (status) status.textContent = 'Adding…';
    try {
      const r = await fetch('/crew/new?fmt=json', {
        method: 'POST', credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, is_vendor: isVendor }),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok || !d.id) { if (status) status.textContent = 'Failed: ' + (d.error || r.status); return; }
      const sel = $('docDetailCrewId');
      let opt = Array.from(sel.options).find(o => o.value === String(d.id));
      if (!opt) {
        opt = document.createElement('option');
        opt.value = String(d.id);
        opt.textContent = d.name + (isVendor ? ' (vendor)' : '');
        sel.appendChild(opt);
      }
      sel.value = String(d.id);
      newCrewHide();
    } catch (e) {
      if (status) status.textContent = 'Error: ' + e.message;
    }
  }

  window.DocForm = {
    FIELD_SPEC, applyTypeUI, vendorValue, setSubtype,
    crewSuggestLoad, resetSuggest,
    newCrewShow, newCrewHide, newCrewSave,
  };
})();
