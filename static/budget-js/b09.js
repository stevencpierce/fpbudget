// Extracted from a templates/budget.html inline block (audit M9, 2026-07-20).
// Jinja values arrive via window.__BJ (set by the inline preamble before this
// script tag). Classic script, document order — semantics identical to inline.

// ── Direct Contact URLs ─────────────────────────────────────────────────────
const DIRECT_CONTACT_ADD_URL = window.__BJ["b09_DIRECT_CONTACT_ADD_URL"];
const DIRECT_CONTACT_DEL_BASE = window.__BJ["b09_DIRECT_CONTACT_DEL_BASE"];

// ── Crew Edit Modal ─────────────────────────────────────────────────────────
function openCrewEditModal(cid, name) {
  document.getElementById('crew-edit-modal').classList.remove('hidden');
  document.getElementById('crew-edit-id').value = cid;
  fetch(`/crew/${cid}/json`)
    .then(r => r.json())
    .then(d => {
      document.getElementById('crew-edit-name').value = d.name || '';
      document.getElementById('crew-edit-phone').value = d.phone || '';
      document.getElementById('crew-edit-email').value = d.email || '';
      document.getElementById('crew-edit-company').value = d.company || '';
      document.getElementById('crew-edit-department').value = d.department || '';
      // Vendor / loan-out + required docs.
      const vcb = document.getElementById('crew-edit-is-vendor');
      if (vcb) vcb.checked = !!d.is_vendor;
      const lo = document.getElementById('crew-edit-loanout');
      if (lo) lo.value = d.loan_out_vendor_id ? String(d.loan_out_vendor_id) : '';
      const req = (d.required_docs ? String(d.required_docs).split(',') : []).map(s => s.trim());
      document.querySelectorAll('.crew-req-cb').forEach(cb => { cb.checked = req.includes(cb.value); });
      _crewVendorToggle();
    });
}
// Show the loan-out picker for a PERSON, the required-docs checklist for a VENDOR.
function _crewVendorToggle() {
  const isV = document.getElementById('crew-edit-is-vendor')?.checked;
  const lo  = document.getElementById('crew-edit-loanout-field');
  const rq  = document.getElementById('crew-edit-reqdocs-field');
  if (lo) lo.style.display = isV ? 'none' : '';
  if (rq) rq.style.display = isV ? '' : 'none';
}
function closeCrewEditModal() {
  document.getElementById('crew-edit-modal').classList.add('hidden');
}
function saveCrewEdit() {
  const cid = document.getElementById('crew-edit-id').value;
  const emailInput = document.getElementById('crew-edit-email');
  if (emailInput && !validateEmailField(emailInput)) return;
  const data = {
    name: document.getElementById('crew-edit-name').value.trim(),
    phone: document.getElementById('crew-edit-phone').value.trim(),
    email: document.getElementById('crew-edit-email').value.trim(),
    company: document.getElementById('crew-edit-company').value.trim(),
    department: document.getElementById('crew-edit-department').value.trim(),
    is_vendor: document.getElementById('crew-edit-is-vendor')?.checked || false,
    loan_out_vendor_id: document.getElementById('crew-edit-loanout')?.value || null,
    required_docs: Array.from(document.querySelectorAll('.crew-req-cb:checked')).map(cb => cb.value),
  };
  if (!data.name) { alert('Name is required'); return; }
  fetch(`/crew/${cid}/edit?fmt=json`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(data)
  })
  .then(r => r.json())
  .then(d => {
    if (d.ok) {
      closeCrewEditModal();
      reloadWithTab();
    } else {
      alert(d.error || 'Save failed');
    }
  });
}

// ── Add Direct Contact / Vendor Modal ───────────────────────────────────────
// vendorMode=true opens it as "Add Vendor": pre-checks the vendor flag and
// shows the required-docs checklist. (User 2026-06-01.)
function openAddContactModal(vendorMode) {
  document.getElementById('add-contact-modal').classList.remove('hidden');
  document.getElementById('add-contact-name').value = '';
  document.getElementById('add-contact-role').value = '';
  document.getElementById('add-contact-phone').value = '';
  document.getElementById('add-contact-email').value = '';
  document.getElementById('add-contact-company').value = '';
  document.querySelectorAll('.add-contact-req-cb').forEach(cb => cb.checked = false);
  const lcb = document.getElementById('add-contact-is-loanout');
  if (lcb) lcb.checked = false;
  const lsel = document.getElementById('add-contact-loanout');
  if (lsel) lsel.value = '';
  const vcb = document.getElementById('add-contact-is-vendor');
  if (vcb) vcb.checked = !!vendorMode;
  _addContactVendorToggle();
  _addContactLoanoutToggle();
}
// Reveal the vendor dropdown when "this person is a loan-out" is ticked.
function _addContactLoanoutToggle() {
  const on  = document.getElementById('add-contact-is-loanout')?.checked;
  const sel = document.getElementById('add-contact-loanout');
  if (sel) sel.style.display = on ? '' : 'none';
}
// Vendor mode hides Role (vendors don't have a crew role) and shows the
// required-docs checklist; the title + button update to read "Vendor".
function _addContactVendorToggle() {
  const isV = document.getElementById('add-contact-is-vendor')?.checked;
  const roleField = document.getElementById('add-contact-role-field');
  const reqField  = document.getElementById('add-contact-reqdocs-field');
  const loField   = document.getElementById('add-contact-loanout-field');
  const title     = document.getElementById('add-contact-title');
  const nameLbl   = document.getElementById('add-contact-name-label');
  const saveBtn   = document.getElementById('add-contact-save-btn');
  if (roleField) roleField.style.display = isV ? 'none' : '';
  if (reqField)  reqField.style.display  = isV ? '' : 'none';
  if (loField)   loField.style.display   = isV ? 'none' : '';   // loan-out is for people
  if (title)   title.textContent   = isV ? 'Add Vendor' : 'Add Contact';
  if (nameLbl) nameLbl.textContent = isV ? 'Vendor / Company Name *' : 'Name *';
  if (saveBtn) saveBtn.textContent = isV ? 'Add Vendor' : 'Add';
}
function closeAddContactModal() {
  document.getElementById('add-contact-modal').classList.add('hidden');
}
function saveNewContact() {
  const emailInput = document.getElementById('add-contact-email');
  if (emailInput && !validateEmailField(emailInput)) return;
  const name = document.getElementById('add-contact-name').value.trim();
  if (!name) { alert('Name is required'); return; }
  const isVendor = document.getElementById('add-contact-is-vendor')?.checked || false;
  fetch(DIRECT_CONTACT_ADD_URL, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      name,
      role: document.getElementById('add-contact-role').value.trim(),
      phone: document.getElementById('add-contact-phone').value.trim(),
      email: document.getElementById('add-contact-email').value.trim(),
      company: document.getElementById('add-contact-company').value.trim(),
      is_vendor: isVendor,
      required_docs: Array.from(document.querySelectorAll('.add-contact-req-cb:checked')).map(cb => cb.value),
      loan_out_vendor_id: (!isVendor && document.getElementById('add-contact-is-loanout')?.checked)
        ? (document.getElementById('add-contact-loanout')?.value || null) : null,
    })
  })
  .then(r => r.json())
  .then(d => {
    if (d.ok) { closeAddContactModal(); reloadWithTab(); }
    else { alert(d.error || 'Failed to add contact'); }
  });
}
function removeDirectContact(dcId) {
  if (!confirm('Remove this contact from the project?')) return;
  fetch(DIRECT_CONTACT_DEL_BASE + '/' + dcId, { method: 'POST' })
    .then(() => reloadWithTab());
}

// ── Representation / Support Contacts Modal ──────────────────────────────────
let _repCrewId = null;
let _repCrewName = '';

async function showSupportModal(crewId, crewName) {
  _repCrewId   = crewId;
  _repCrewName = crewName;
  document.getElementById('rep-modal-title').textContent = `Representation — ${crewName}`;
  document.getElementById('rep-modal').classList.remove('hidden');
  document.getElementById('rep-form').classList.add('hidden');
  await loadSupportContacts(crewId);
}

function closeRepModal() {
  document.getElementById('rep-modal').classList.add('hidden');
  _repCrewId = null;
  // ONE page refresh on close (if anything changed) — the old per-save
  // reloadWithTab() rebuilt the whole page while the modal was still open,
  // wiping whatever the user had begun typing for the NEXT contact ("won't
  // let me add more than three", 2026-07-13).
  if (_repDirty) { _repDirty = false; reloadWithTab(); }
}
var _repDirty = false;

async function loadSupportContacts(crewId) {
  const list = document.getElementById('rep-list');
  list.innerHTML = '<p class="muted" style="font-size:.85rem">Loading…</p>';
  const res = await fetch(`/crew/${crewId}/support`);
  const contacts = await res.json();
  if (!contacts.length) {
    list.innerHTML = '<p class="muted" style="font-size:.85rem">No representation on file.</p>';
    return;
  }
  // Escape all data-derived strings (XSS — this list previously interpolated
  // name/company/phone/email raw into innerHTML; review CR-5, 2026-06-04).
  const _repEsc = s => String(s == null ? '' : s).replace(/[&<>"']/g,
      c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  window._repContactsById = {};
  list.innerHTML = contacts.map(sc => {
    window._repContactsById[sc.id] = sc;
    const feeLabel = sc.fee_pct != null
      ? `<span class="rep-fee-badge">${_repEsc(sc.fee_pct)}% ${sc.fee_type === 'on_top' ? '(on top)' : sc.fee_type === 'inclusive' ? '(inclusive)' : ''}</span>`
      : '';
    return `
    <div class="rep-item" data-id="${_repEsc(sc.id)}">
      <div class="rep-item-header">
        <span class="rep-role-badge">${_repEsc(sc.role_type)}</span>
        <strong>${_repEsc(sc.name)}</strong>
        ${sc.company ? `<span class="muted">${_repEsc(sc.company)}</span>` : ''}
        ${feeLabel}
        <div style="margin-left:auto;display:flex;gap:.25rem">
          <button class="btn btn-xs btn-ghost" onclick="editSupportById(${parseInt(sc.id, 10) || 0})">✎</button>
          <button class="btn btn-xs btn-ghost" onclick="deleteSupport(${parseInt(sc.id, 10) || 0})">✕</button>
        </div>
      </div>
      ${sc.phone ? `<div style="font-size:.8rem"><a href="tel:${encodeURIComponent(sc.phone)}" class="contact-link">${_repEsc(sc.phone)}</a></div>` : ''}
      ${sc.email ? `<div style="font-size:.8rem"><a href="mailto:${encodeURIComponent(sc.email)}" class="contact-link">${_repEsc(sc.email)}</a></div>` : ''}
    </div>
  `;}).join('');
}

// Data-id indirection for the edit button (replaces JSON.stringify-in-onclick,
// which both broke on quotes and was an XSS vector). (Review CR-5.)
function editSupportById(id) {
  const sc = (window._repContactsById || {})[id];
  if (sc && typeof editSupport === 'function') editSupport(sc);
}

function openRepForm(data) {
  const form = document.getElementById('rep-form');
  form.classList.remove('hidden');
  document.getElementById('rep-sc-id').value        = data ? data.id    : '';
  document.getElementById('rep-sc-role').value      = data ? data.role_type : 'agent';
  document.getElementById('rep-sc-name').value      = data ? data.name  : '';
  document.getElementById('rep-sc-company').value   = data ? (data.company||'') : '';
  document.getElementById('rep-sc-phone').value     = data ? (data.phone||'') : '';
  document.getElementById('rep-sc-email').value     = data ? (data.email||'') : '';
  document.getElementById('rep-sc-fee-pct').value   = data ? (data.fee_pct != null ? data.fee_pct : '') : '';
  document.getElementById('rep-sc-fee-type').value  = data ? (data.fee_type||'') : '';
}

function editSupport(sc) { openRepForm(sc); }

async function saveRepContact() {
  const id   = document.getElementById('rep-sc-id').value;
  const name = document.getElementById('rep-sc-name').value.trim();
  if (!name) { alert('Contact name is required.'); return; }
  if (!_repCrewId) { alert('Modal lost its crew member — close and reopen it.'); return; }
  const feePctRaw = document.getElementById('rep-sc-fee-pct').value;
  const payload = {
    name,
    role_type: document.getElementById('rep-sc-role').value,
    company:   document.getElementById('rep-sc-company').value.trim() || null,
    phone:     document.getElementById('rep-sc-phone').value.trim() || null,
    email:     document.getElementById('rep-sc-email').value.trim() || null,
    fee_pct:   feePctRaw !== '' ? parseFloat(feePctRaw) : null,
    fee_type:  document.getElementById('rep-sc-fee-type').value || null,
  };
  if (id) payload.id = parseInt(id);
  let body;
  try {
    const res = await fetch(`/crew/${_repCrewId}/support/save`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload),
    });
    body = await res.json();  // an HTML 500 throws here → caught below
  } catch (e) {
    alert('Could not save contact — server error. Try again or reload the page.');
    return;
  }
  if (body.ok) {
    document.getElementById('rep-form').classList.add('hidden');
    await loadSupportContacts(_repCrewId);
    _repDirty = true;  // Agent column refreshes ONCE on modal close
  } else {
    alert(body.error || 'Failed to save');
  }
}

async function deleteSupport(sid) {
  if (!confirm('Remove this contact?')) return;
  await fetch(`/crew/${_repCrewId}/support/${sid}/delete`, { method: 'POST' });
  await loadSupportContacts(_repCrewId);
  _repDirty = true;
}

// ── Top Sheet Collapsible Sections ──────────────────────────────────────────
function toggleTsSection(code, btn) {
  const rows = document.querySelectorAll(`[data-ts-section="${code}"]`);
  const isExpanded = btn.textContent === '▾';
  rows.forEach(row => {
    row.style.display = isExpanded ? 'none' : '';
  });
  btn.textContent = isExpanded ? '▸' : '▾';
}

// ── % Edit Popup ─────────────────────────────────────────────────────────────
let _pctEditType = null;
function editAutoLinePct(event, type) {
  event.preventDefault();
  _pctEditType = type;
  const popup = document.getElementById('pct-edit-popup');
  popup.classList.remove('hidden');
  popup.style.left = Math.min(event.clientX, window.innerWidth - 260) + 'px';
  popup.style.top = (event.clientY + 8) + 'px';
  if (type === 'workers_comp') {
    document.getElementById('pct-edit-label').textContent = "Workers' Comp %";
    document.getElementById('pct-edit-note').textContent = "% of gross labor wages → Insurance";
    document.getElementById('pct-edit-value').value = parseFloat(document.getElementById('set-workers-comp')?.value || 3.0).toFixed(2);
  } else {
    document.getElementById('pct-edit-label').textContent = "Payroll Service Fee %";
    document.getElementById('pct-edit-note').textContent = "% of gross labor wages → Administrative";
    document.getElementById('pct-edit-value').value = parseFloat(document.getElementById('set-payroll-fee')?.value || 1.75).toFixed(2);
  }
  setTimeout(() => document.getElementById('pct-edit-value').focus(), 50);
}
function closePctPopup() {
  document.getElementById('pct-edit-popup').classList.add('hidden');
  _pctEditType = null;
}
function savePctEdit() {
  const val = parseFloat(document.getElementById('pct-edit-value').value);
  if (isNaN(val)) { closePctPopup(); return; }
  if (_pctEditType === 'workers_comp') {
    const el = document.getElementById('set-workers-comp');
    if (el) { el.value = val; el.dispatchEvent(new Event('change')); }
  } else {
    const el = document.getElementById('set-payroll-fee');
    if (el) { el.value = val; el.dispatchEvent(new Event('change')); }
  }
  closePctPopup();
  if (typeof saveSettings === 'function') saveSettings();
}
document.addEventListener('click', e => {
  if (!e.target.closest('#pct-edit-popup')) closePctPopup();
});

// ── Working Budget Search/Filter ──────────────────────────────────────────
function filterWorkingBudget(query) {
  const q = (query || '').toLowerCase().trim();
  const sections = document.querySelectorAll('#working-budget-wrap .section-block');
  sections.forEach(sec => {
    let secVisible = false;
    sec.querySelectorAll('tbody .line-row').forEach(row => {
      if (!q) {
        row.style.display = '';
        secVisible = true;
      } else {
        const desc = (row.querySelector('[data-field="description"]')?.textContent || '').toLowerCase();
        const acc  = (row.dataset.codeName || '').toLowerCase();
        const match = desc.includes(q) || acc.includes(q);
        row.style.display = match ? '' : 'none';
        if (match) secVisible = true;
      }
    });
    sec.style.display = (!q || secVisible) ? '' : 'none';
  });
}

// ── Budget Mode Safety ────────────────────────────────────────────────────
function dismissModeSafetyBanner() {
  const el = document.getElementById('mode-safety-banner');
  if (el) el.style.display = 'none';
}

// ── Company settings: load on page init, save with budget settings ──────────
(function loadCompanySettings() {
  fetch('/settings/company')
    .then(r => r.json())
    .then(d => {
      const f = id => document.getElementById(id);
      if (f('cs-company-name'))  f('cs-company-name').value  = d.company_name  || '';
      if (f('cs-address-line1')) f('cs-address-line1').value = d.address_line1 || '';
      if (f('cs-address-line2')) f('cs-address-line2').value = d.address_line2 || '';
      if (f('cs-city'))          f('cs-city').value          = d.city          || '';
      if (f('cs-state'))         f('cs-state').value         = d.state         || '';
      if (f('cs-zip'))           f('cs-zip').value           = d.zip_code      || '';
      if (f('cs-phone'))         f('cs-phone').value         = d.phone         || '';
      if (f('cs-email'))         f('cs-email').value         = d.email         || '';
      if (f('cs-website'))       f('cs-website').value       = d.website       || '';
    }).catch(() => {});
})();

async function saveCompanySettings() {
  const f = id => document.getElementById(id)?.value || '';
  await fetch('/settings/company', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      company_name:  f('cs-company-name'),
      address_line1: f('cs-address-line1'),
      address_line2: f('cs-address-line2'),
      city:          f('cs-city'),
      state:         f('cs-state'),
      zip_code:      f('cs-zip'),
      phone:         f('cs-phone'),
      email:         f('cs-email'),
      website:       f('cs-website'),
    })
  });
}

// ═══════════════════════════════════════════════════════════════════════════
// QUICK ENTRY — full item library keyed by COA code
// Each item: { label, isLabor, rate, qty, days, kit, fringe, comp, unit }
// ═══════════════════════════════════════════════════════════════════════════
// QE_CATEGORIES is the runtime Quick Entry catalog. The array below is the
// FALLBACK used if /api/catalog fetch fails — normally openCascade() replaces
// this with live DB data from the CatalogItem table so edits made in
// /admin/catalog show up immediately without a deploy.
let QE_CATEGORIES = [
  { code:1000,  name:'Development Rights & Story', items:[
    { label:'Story Rights / Option',         isLabor:false, rate:5000, qty:1, days:1, comp:'expense',  unit:'flat' },
    { label:'Research',                      isLabor:false, rate:1000, qty:1, days:1, comp:'expense',  unit:'flat' },
    { label:'Pitch Deck',                    isLabor:false, rate:1500, qty:1, days:1, comp:'expense',  unit:'flat' },
    { label:'Sizzle / Proof-of-Concept Edit',isLabor:false, rate:3000, qty:1, days:1, comp:'expense',  unit:'flat' },
    { label:'Legal (Development)',           isLabor:false, rate:1500, qty:1, days:1, comp:'expense',  unit:'flat' },
    { label:'Development Office / Admin',    isLabor:false, rate:1000, qty:1, days:1, comp:'expense',  unit:'month'},
    { label:'Treatment / Outline',           isLabor:false, rate:500,  qty:1, days:1, comp:'expense',  unit:'flat' },
    { label:'Script Copies (Dev)',           isLabor:false, rate:100,  qty:1, days:1, comp:'purchase', unit:'flat' },
  ]},
  { code:1100,  name:'Development Labor', items:[
    { label:'Development Executive Producer',isLabor:true,  rate:1500, qty:1, days:1, kit:0, fringe:'N', comp:'labor' },
    { label:'Development Producer',          isLabor:true,  rate:1200, qty:1, days:1, kit:0, fringe:'N', comp:'labor' },
    { label:'Showrunner',                    isLabor:true,  rate:1500, qty:1, days:1, kit:0, fringe:'N', comp:'labor' },
    { label:'Writer (Development)',          isLabor:true,  rate:1200, qty:1, days:1, kit:0, fringe:'N', comp:'labor' },
    { label:'Creative Director (Dev)',       isLabor:true,  rate:1200, qty:1, days:1, kit:0, fringe:'N', comp:'labor' },
    { label:'Story Consultant',              isLabor:true,  rate:800,  qty:1, days:1, kit:0, fringe:'N', comp:'labor' },
  ]},
  { code:2100,  name:'Talent', items:[
    { label:'Principal Talent',  isLabor:true,  rate:825,  qty:1, days:1, kit:0, fringe:'N', unionFringe:'S', agent_pct:10, comp:'labor' },
    { label:'Host',              isLabor:true,  rate:1000, qty:1, days:1, kit:0, fringe:'N', unionFringe:'S', agent_pct:10, comp:'labor' },
    { label:'Stunt Performer',   isLabor:true,  rate:825,  qty:1, days:1, kit:0, fringe:'N', unionFringe:'S', agent_pct:10, comp:'labor' },
    { label:'Extra / Background',isLabor:true,  rate:200,  qty:1, days:1, kit:0, fringe:'N', unionFringe:'S', agent_pct:0,  comp:'labor' },
    { label:'Voice Over Talent', isLabor:false, rate:500,  qty:1, days:1, comp:'expense', unit:'session' },
  ]},
  { code:2300,  name:'Rehearsal', items:[
    { label:'Rehearsal Space',   isLabor:false, rate:500,  qty:1, days:1, comp:'expense', unit:'day'  },
    { label:'Choreographer',     isLabor:true,  rate:800,  qty:1, days:1, kit:0, fringe:'N', comp:'labor' },
    { label:'Table Read',        isLabor:false, rate:300,  qty:1, days:1, comp:'expense', unit:'flat' },
  ]},
  { code:2200,  name:'Casting', items:[
    { label:'Casting Director',  isLabor:true,  rate:900,  qty:1, days:1, kit:0, fringe:'N', comp:'labor' },
    { label:'Casting Associate', isLabor:true,  rate:650,  qty:1, days:1, kit:0, fringe:'N', comp:'labor' },
    { label:'Casting Space',     isLabor:false, rate:500,  qty:1, days:1, comp:'expense', unit:'day'  },
    { label:'Casting Tapes',     isLabor:false, rate:200,  qty:1, days:1, comp:'expense', unit:'flat' },
  ]},
  { code:2000,  name:'Production Staff', items:[
    // ATL Executives
    { label:'Director',                   group:'Executives',     isLabor:true, rate:1500, qty:1, days:1, kit:0, fringe:'N', unionFringe:'D', comp:'labor' },
    { label:'Executive Producer',         group:'Executives',     isLabor:true, rate:1200, qty:1, days:1, kit:0, fringe:'N', unionFringe:'N', comp:'labor' },
    { label:'Producer',                   group:'Executives',     isLabor:true, rate:1000, qty:1, days:1, kit:0, fringe:'N', unionFringe:'N', comp:'labor' },
    { label:'Creative Director',          group:'Executives',     isLabor:true, rate:1200, qty:1, days:1, kit:0, fringe:'N', unionFringe:'N', comp:'labor' },
    { label:'Writer Fee',                 group:'Executives',     isLabor:false,rate:0,    qty:1, days:1, comp:'expense', unit:'flat' },
    // Production Management
    { label:'Line Producer',              group:'Production',     isLabor:true, rate:1200, qty:1, days:1, kit:0,   fringe:'N', unionFringe:'D', comp:'labor' },
    { label:'UPM',                        group:'Production',     isLabor:true, rate:1000, qty:1, days:1, kit:0,   fringe:'N', unionFringe:'D', comp:'labor' },
    { label:'Supervising Producer',       group:'Production',     isLabor:true, rate:800,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'N', comp:'labor' },
    { label:'Production Supervisor',      group:'Production',     isLabor:true, rate:900,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'N', comp:'labor' },
    { label:'Production Coordinator',     group:'Production',     isLabor:true, rate:750,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'N', comp:'labor' },
    { label:'APOC',                       group:'Production',     isLabor:true, rate:800,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'N', comp:'labor' },
    { label:'Production Accountant',      group:'Production',     isLabor:true, rate:900,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'N', comp:'labor' },
    { label:'Payroll Coordinator',        group:'Production',     isLabor:true, rate:650,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'N', comp:'labor' },
    { label:'Travel Coordinator',         group:'Production',     isLabor:true, rate:650,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'N', comp:'labor' },
    { label:'Production Secretary',       group:'Production',     isLabor:true, rate:650,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'N', comp:'labor' },
    // Direction / AD
    { label:'Live Director',              group:'Direction / AD', isLabor:true, rate:700,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'D', comp:'labor' },
    { label:'1st AD',                     group:'Direction / AD', isLabor:true, rate:900,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'D', comp:'labor' },
    { label:'2nd AD',                     group:'Direction / AD', isLabor:true, rate:750,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'D', comp:'labor' },
    { label:'2nd 2nd AD',                 group:'Direction / AD', isLabor:true, rate:650,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'D', comp:'labor' },
    { label:'Script Supervisor',          group:'Direction / AD', isLabor:true, rate:750,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Key PA',                     group:'Direction / AD', isLabor:true, rate:350,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'N', comp:'labor' },
    { label:'Set PA',                     group:'Direction / AD', isLabor:true, rate:300,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'N', comp:'labor' },
    { label:'Office PA',                  group:'Direction / AD', isLabor:true, rate:300,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'N', comp:'labor' },
    { label:'Second Unit Director',       group:'Direction / AD', isLabor:true, rate:1200, qty:1, days:1, kit:0, fringe:'N', unionFringe:'D', comp:'labor' },
    // Camera
    { label:'Director of Photography',    group:'Camera',         isLabor:true, rate:1200, qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Camera Operator',            group:'Camera',         isLabor:true, rate:900,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Robotic Camera Operator',    group:'Camera',         isLabor:true, rate:500,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'1st AC',                     group:'Camera',         isLabor:true, rate:800,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'2nd AC',                     group:'Camera',         isLabor:true, rate:650,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'DIT',                        group:'Camera',         isLabor:true, rate:850,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Steadicam Operator',         group:'Camera',         isLabor:true, rate:900,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Data Wrangler',              group:'Camera',         isLabor:true, rate:700,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Second Unit DP',             group:'Camera',         isLabor:true, rate:1000, qty:1, days:1, kit:0, fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Video Engineer',             group:'Camera',         isLabor:true, rate:750,  qty:1, days:1, kit:0, fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'VTR Operator',               group:'Camera',         isLabor:true, rate:650,  qty:1, days:1, kit:0, fringe:'N', unionFringe:'I', comp:'labor' },
    // Grip & Electric
    { label:'Lighting Designer',          group:'Grip & Electric',isLabor:true, rate:1000, qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Gaffer',                     group:'Grip & Electric',isLabor:true, rate:825,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Key Grip',                   group:'Grip & Electric',isLabor:true, rate:825,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Best Boy Electric',          group:'Grip & Electric',isLabor:true, rate:800,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Best Boy Grip',              group:'Grip & Electric',isLabor:true, rate:800,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Electric',                   group:'Grip & Electric',isLabor:true, rate:775,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Grip',                       group:'Grip & Electric',isLabor:true, rate:775,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Generator Operator',         group:'Grip & Electric',isLabor:true, rate:775,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Swing (Electric)',           group:'Grip & Electric',isLabor:true, rate:750,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Swing (Grip)',              group:'Grip & Electric',isLabor:true, rate:750,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    // Sound
    { label:'Sound Mixer',                group:'Sound',          isLabor:true, rate:900,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Boom Operator',              group:'Sound',          isLabor:true, rate:650,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Utility Sound',              group:'Sound',          isLabor:true, rate:750,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    // Art Department
    { label:'Production Designer',        group:'Art',            isLabor:true, rate:1000, qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Art Director',               group:'Art',            isLabor:true, rate:825,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Set Dresser',                group:'Art',            isLabor:true, rate:775,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Props Master',               group:'Art',            isLabor:true, rate:825,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Props Assistant',            group:'Art',            isLabor:true, rate:650,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    // Hair & Makeup
    { label:'Key Makeup Artist',          group:'Hair & Makeup',  isLabor:true, rate:825,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Makeup Artist',              group:'Hair & Makeup',  isLabor:true, rate:775,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Hair Stylist',               group:'Hair & Makeup',  isLabor:true, rate:825,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'HMU (Hair & Makeup)',        group:'Hair & Makeup',  isLabor:true, rate:800,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'SFX Makeup Artist',          group:'Hair & Makeup',  isLabor:true, rate:900,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    // Wardrobe
    { label:'Wardrobe Stylist',           group:'Wardrobe',       isLabor:true, rate:825,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Wardrobe Assistant',         group:'Wardrobe',       isLabor:true, rate:650,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    // Locations
    { label:'Location Manager',           group:'Locations',      isLabor:true, rate:900,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Location Assistant',         group:'Locations',      isLabor:true, rate:650,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'N', comp:'labor' },
    // Transportation
    { label:'Transportation Coordinator', group:'Transportation', isLabor:true, rate:800,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'N', comp:'labor' },
    { label:'Driver (Captain)',           group:'Transportation', isLabor:true, rate:700,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'N', comp:'labor' },
    { label:'Driver',                     group:'Transportation', isLabor:true, rate:600,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'N', comp:'labor' },
    // Control Room
    { label:'Technical Producer',         group:'Control Room',   isLabor:true, rate:1000, qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Technical Director',         group:'Control Room',   isLabor:true, rate:900,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Graphics and Playback',      group:'Control Room',   isLabor:true, rate:500,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Switcher Operator',          group:'Control Room',   isLabor:true, rate:750,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    // EPK / BTS
    { label:'EPK Videographer',           group:'EPK / BTS',      isLabor:true, rate:800,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'EPK Photographer',           group:'EPK / BTS',      isLabor:true, rate:750,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'I', comp:'labor' },
    // Craft Services
    { label:'Craft Services Coordinator', group:'Craft Services', isLabor:true, rate:600,  qty:1, days:1, kit:0,   fringe:'N', unionFringe:'N', comp:'labor' },
  ]},
  { code:4000,  name:'Post-Production Staff', items:[
    { label:'Editor',                    isLabor:true, rate:900,  qty:1, days:1, kit:0, fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Assistant Editor',          isLabor:true, rate:650,  qty:1, days:1, kit:0, fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Post Production Supervisor',isLabor:true, rate:900,  qty:1, days:1, kit:0, fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Colorist',                  isLabor:true, rate:900,  qty:1, days:1, kit:0, fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'VFX Supervisor',            isLabor:true, rate:1000, qty:1, days:1, kit:0, fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Motion Graphics Designer',  isLabor:true, rate:850,  qty:1, days:1, kit:0, fringe:'N', unionFringe:'I', comp:'labor' },
    { label:'Sound Designer',            isLabor:true, rate:900,  qty:1, days:1, kit:0, fringe:'N', unionFringe:'I', comp:'labor' },
  ]},
  { code:2600,  name:'Camera Equipment', items:[
    { label:'Camera Package Rental',    isLabor:false, rate:1500, qty:1, days:1, comp:'rental',  unit:'day'  },
    { label:'Lens Kit Rental',          isLabor:false, rate:500,  qty:1, days:1, comp:'rental',  unit:'day'  },
    { label:'Monitor Rental',           isLabor:false, rate:150,  qty:1, days:1, comp:'rental',  unit:'day'  },
    { label:'Gimbal / Stabilizer',      isLabor:false, rate:200,  qty:1, days:1, comp:'rental',  unit:'day'  },
    { label:'Drone Package',            isLabor:false, rate:800,  qty:1, days:1, comp:'rental',  unit:'day'  },
    { label:'Teleprompter Rental',      isLabor:false, rate:300,  qty:1, days:1, comp:'rental',  unit:'day'  },
    { label:'Media Cards / Hard Drives',isLabor:false, rate:300,  qty:1, days:1, comp:'purchase',unit:'flat' },
    { label:'Camera Expendables',       isLabor:false, rate:100,  qty:1, days:1, comp:'purchase',unit:'flat' },
  ]},
  { code:2700,  name:'Grip & Electric Equipment', items:[
    { label:'Lighting Package',         isLabor:false, rate:1500, qty:1, days:1, comp:'rental',  unit:'day'  },
    { label:'Grip Package',             isLabor:false, rate:800,  qty:1, days:1, comp:'rental',  unit:'day'  },
    { label:'Generator Rental',         isLabor:false, rate:400,  qty:1, days:1, comp:'rental',  unit:'day'  },
    { label:'Additional Fixtures',      isLabor:false, rate:300,  qty:1, days:1, comp:'rental',  unit:'day'  },
    { label:'Expendables (Gels/Tape)',  isLabor:false, rate:300,  qty:1, days:1, comp:'purchase',unit:'flat' },
    { label:'Extension Cords / Stingers',isLabor:false,rate:100, qty:1, days:1, comp:'purchase',unit:'flat' },
  ]},
  { code:5000,  name:'Processing & Lab', items:[
    { label:'Hard Drives (Shoot)',        isLabor:false, rate:150,  qty:2, days:1, comp:'purchase',unit:'each' },
    { label:'RAID / Backup System',       isLabor:false, rate:500,  qty:1, days:1, comp:'rental',  unit:'flat' },
    { label:'Cloud Delivery / Transfer',  isLabor:false, rate:200,  qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'LTO Archival',               isLabor:false, rate:400,  qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Encoder / Decoder Unit',     isLabor:false, rate:600,  qty:1, days:1, comp:'rental',  unit:'day'  },
    { label:'Video Processor',            isLabor:false, rate:400,  qty:1, days:1, comp:'rental',  unit:'day'  },
    { label:'Audio Processor',            isLabor:false, rate:300,  qty:1, days:1, comp:'rental',  unit:'day'  },
    { label:'Signal Conversion Gear',     isLabor:false, rate:400,  qty:1, days:1, comp:'rental',  unit:'day'  },
    { label:'Frame Sync / Converter',     isLabor:false, rate:300,  qty:1, days:1, comp:'rental',  unit:'day'  },
    { label:'SDI Distribution Amp',       isLabor:false, rate:200,  qty:1, days:1, comp:'rental',  unit:'day'  },
    { label:'Fiber / Transport System',   isLabor:false, rate:500,  qty:1, days:1, comp:'rental',  unit:'day'  },
    { label:'Playback / Replay System',   isLabor:false, rate:800,  qty:1, days:1, comp:'rental',  unit:'day'  },
    { label:'Processing Expendables',     isLabor:false, rate:100,  qty:1, days:1, comp:'purchase',unit:'flat' },
  ]},
  { code:2900,  name:'Control Room Equipment', items:[
    { label:'Control Room Rental',       isLabor:false, rate:2000, qty:1, days:1, comp:'rental',  unit:'day' },
    { label:'Video Playback System',     isLabor:false, rate:500,  qty:1, days:1, comp:'rental',  unit:'day' },
    { label:'Switcher / Mixer Rental',   isLabor:false, rate:400,  qty:1, days:1, comp:'rental',  unit:'day' },
    { label:'Broadcast Equipment',       isLabor:false, rate:1000, qty:1, days:1, comp:'rental',  unit:'day' },
  ]},
  { code:2800,  name:'Sound Equipment', items:[
    { label:'Sound Package Rental',      isLabor:false, rate:600,  qty:1, days:1, comp:'rental',  unit:'day' },
    { label:'Wireless Mic Kit',          isLabor:false, rate:200,  qty:1, days:1, comp:'rental',  unit:'day' },
    { label:'Walkie Talkies',            isLabor:false, rate:200,  qty:1, days:1, comp:'rental',  unit:'day' },
    { label:'IFB System',                isLabor:false, rate:150,  qty:1, days:1, comp:'rental',  unit:'day' },
    { label:'Audio Playback System',     isLabor:false, rate:400,  qty:1, days:1, comp:'rental',  unit:'day' },
    { label:'Sound Expendables',         isLabor:false, rate:100,  qty:1, days:1, comp:'purchase',unit:'flat' },
  ]},
  { code:3000,  name:'Art & Sets Costs', items:[
    { label:'Props Purchase',           isLabor:false, rate:500,  qty:1, days:1, comp:'purchase',unit:'flat' },
    { label:'Props Rental',             isLabor:false, rate:300,  qty:1, days:1, comp:'rental',  unit:'flat' },
    { label:'Set Dressing / Décor',     isLabor:false, rate:500,  qty:1, days:1, comp:'purchase',unit:'flat' },
    { label:'Fabrication / Build',      isLabor:false, rate:1000, qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Printing / Signage',       isLabor:false, rate:300,  qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Art Supplies',             isLabor:false, rate:200,  qty:1, days:1, comp:'purchase',unit:'flat' },
    { label:'Floral / Greenery',        isLabor:false, rate:400,  qty:1, days:1, comp:'purchase',unit:'flat' },
  ]},
  { code:3100,  name:'Hair & Makeup Costs', items:[
    { label:'Makeup Supplies / Kit',    isLabor:false, rate:200,  qty:1, days:1, comp:'purchase',unit:'flat' },
    { label:'Hair Supplies / Kit',      isLabor:false, rate:150,  qty:1, days:1, comp:'purchase',unit:'flat' },
    { label:'SFX Makeup Supplies',      isLabor:false, rate:400,  qty:1, days:1, comp:'purchase',unit:'flat' },
    { label:'Touch-Up Supplies',        isLabor:false, rate:100,  qty:1, days:1, comp:'purchase',unit:'flat' },
  ]},
  { code:3200,  name:'Wardrobe Costs', items:[
    { label:'Wardrobe Purchase',        isLabor:false, rate:500,  qty:1, days:1, comp:'purchase',unit:'flat' },
    { label:'Wardrobe Rental',          isLabor:false, rate:300,  qty:1, days:1, comp:'rental',  unit:'flat' },
    { label:'Alterations / Tailoring',  isLabor:false, rate:200,  qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Laundry & Cleaning',       isLabor:false, rate:150,  qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Accessories',              isLabor:false, rate:200,  qty:1, days:1, comp:'purchase',unit:'flat' },
  ]},
  { code:3400,  name:'Transportation', items:[
    { label:'Cargo Van Rental',         isLabor:false, rate:150,  qty:1, days:1, comp:'rental',  unit:'day'  },
    { label:'Cube Truck Rental',        isLabor:false, rate:250,  qty:1, days:1, comp:'rental',  unit:'day'  },
    { label:'Passenger Van Rental',     isLabor:false, rate:150,  qty:1, days:1, comp:'rental',  unit:'day'  },
    { label:'Production Car',           isLabor:false, rate:100,  qty:1, days:1, comp:'rental',  unit:'day'  },
    { label:'Fuel',                     isLabor:false, rate:100,  qty:1, days:1, comp:'expense', unit:'day'  },
    { label:'Parking',                  isLabor:false, rate:50,   qty:1, days:1, comp:'expense', unit:'day'  },
    { label:'Tolls',                    isLabor:false, rate:30,   qty:1, days:1, comp:'expense', unit:'day'  },
    { label:'Mileage Reimbursement',    isLabor:false, rate:200,  qty:1, days:1, comp:'expense', unit:'flat' },
  ]},
  { code:3500,  name:'Travel', items:[
    { label:'Airfare',               isLabor:false, rate:600,  qty:1, days:1, comp:'expense', unit:'each'  },
    { label:'Hotel',                 isLabor:false, rate:200,  qty:1, days:1, comp:'expense', unit:'night' },
    { label:'Per Diem',              isLabor:false, rate:75,   qty:1, days:1, comp:'expense', unit:'day'   },
    { label:'Ground Transportation', isLabor:false, rate:100,  qty:1, days:1, comp:'expense', unit:'day'   },
    { label:'Car Service',           isLabor:false, rate:150,  qty:1, days:1, comp:'expense', unit:'trip'  },
    { label:'Baggage Fees',          isLabor:false, rate:60,   qty:1, days:1, comp:'expense', unit:'each'  },
    { label:'Travel Agent Fee',      isLabor:false, rate:200,  qty:1, days:1, comp:'expense', unit:'flat'  },
  ]},
  { code:3600,  name:'Shipping', items:[
    { label:'Equipment Shipping',    isLabor:false, rate:500,  qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Courier / Messenger',   isLabor:false, rate:100,  qty:1, days:1, comp:'expense', unit:'each' },
    { label:'Overnight Shipping',    isLabor:false, rate:75,   qty:1, days:1, comp:'expense', unit:'each' },
    { label:'Packaging Supplies',    isLabor:false, rate:100,  qty:1, days:1, comp:'purchase',unit:'flat' },
  ]},
  { code:3700,  name:'Production Meals & Craft Services', items:[
    { label:'Catering (Breakfast)',     isLabor:false, rate:20,   qty:30, days:1, comp:'expense', unit:'person/day' },
    { label:'Catering (Lunch)',         isLabor:false, rate:25,   qty:30, days:1, comp:'expense', unit:'person/day' },
    { label:'Craft Services (daily)',   isLabor:false, rate:300,  qty:1,  days:1, comp:'expense', unit:'day'        },
    { label:'Craft Services Supplies',  isLabor:false, rate:150,  qty:1,  days:1, comp:'purchase',unit:'day'        },
    { label:'Coffee Service',           isLabor:false, rate:100,  qty:1,  days:1, comp:'expense', unit:'day'        },
    { label:'Catering Gratuity',        isLabor:false, rate:100,  qty:1,  days:1, comp:'expense', unit:'day'        },
  ]},
  { code:3800,  name:'Sanitation', items:[
    { label:'Portable Toilets',      isLabor:false, rate:300,  qty:1, days:1, comp:'rental',  unit:'day'  },
    { label:'Cleaning Crew',         isLabor:false, rate:400,  qty:1, days:1, comp:'expense', unit:'day'  },
    { label:'Trash Removal',         isLabor:false, rate:200,  qty:1, days:1, comp:'expense', unit:'day'  },
    { label:'Cleaning Supplies',     isLabor:false, rate:100,  qty:1, days:1, comp:'purchase',unit:'flat' },
  ]},
  { code:3300,  name:'Locations', items:[
    // Pre-production
    { label:'Location Scout Day',       isLabor:false, rate:500,  qty:1, days:1, comp:'expense', unit:'day'  },
    { label:'Tech Scout',               isLabor:false, rate:500,  qty:1, days:1, comp:'expense', unit:'day'  },
    { label:'Pre-Production Office',    isLabor:false, rate:2000, qty:1, days:1, comp:'expense', unit:'week' },
    { label:'Script Copies / Printing', isLabor:false, rate:100,  qty:1, days:1, comp:'purchase',unit:'flat' },
    { label:'Office Supplies (Pre-Pro)',isLabor:false, rate:200,  qty:1, days:1, comp:'purchase',unit:'flat' },
    // On-shoot
    { label:'Location Fee',             isLabor:false, rate:2000, qty:1, days:1, comp:'expense', unit:'day'  },
    { label:'Permit Fee',               isLabor:false, rate:500,  qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Police / Fire Support',    isLabor:false, rate:800,  qty:1, days:1, comp:'expense', unit:'day'  },
    { label:'Location Hold Fee',        isLabor:false, rate:1000, qty:1, days:1, comp:'expense', unit:'day'  },
    { label:'Location Cleaning',        isLabor:false, rate:300,  qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Damage Deposit',           isLabor:false, rate:1000, qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Generator (Location)',     isLabor:false, rate:400,  qty:1, days:1, comp:'rental',  unit:'day'  },
  ]},
  { code:4500,  name:'Post-Production Equipment', items:[
    { label:'Edit System Rental',    isLabor:false, rate:200,  qty:1, days:1, comp:'rental',  unit:'day'  },
    { label:'External Hard Drives',  isLabor:false, rate:150,  qty:2, days:1, comp:'purchase',unit:'each' },
    { label:'RAID Array',            isLabor:false, rate:500,  qty:1, days:1, comp:'rental',  unit:'flat' },
    { label:'Software Licenses',     isLabor:false, rate:300,  qty:1, days:1, comp:'expense', unit:'flat' },
  ]},
  { code:4600,  name:'Post-Production Facilities', items:[
    { label:'Edit Suite Rental',     isLabor:false, rate:1000, qty:1, days:1, comp:'rental', unit:'day' },
    { label:'Color Suite Rental',    isLabor:false, rate:1500, qty:1, days:1, comp:'rental', unit:'day' },
    { label:'Mix Studio Rental',     isLabor:false, rate:1200, qty:1, days:1, comp:'rental', unit:'day' },
    { label:'Screening Room',        isLabor:false, rate:800,  qty:1, days:1, comp:'rental', unit:'day' },
  ]},
  { code:4700,  name:'Post-Production Services', items:[
    { label:'Color Grading',         isLabor:false, rate:2000, qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Audio Mix',             isLabor:false, rate:1500, qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Transcription',         isLabor:false, rate:300,  qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Closed Captions',       isLabor:false, rate:400,  qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Subtitles / Translation',isLabor:false,rate:500,  qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'VFX Work',              isLabor:false, rate:0,    qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Mastering / Encoding',  isLabor:false, rate:500,  qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Delivery / Output',     isLabor:false, rate:300,  qty:1, days:1, comp:'expense', unit:'flat' },
  ]},
  { code:6100,  name:'Licensing', items:[
    { label:'Music License',         isLabor:false, rate:1000, qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Stock Footage License', isLabor:false, rate:500,  qty:1, days:1, comp:'expense', unit:'each' },
    { label:'Photo License',         isLabor:false, rate:200,  qty:1, days:1, comp:'expense', unit:'each' },
    { label:'Archival Rights',       isLabor:false, rate:500,  qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Clearance Research',    isLabor:false, rate:500,  qty:1, days:1, comp:'expense', unit:'flat' },
  ]},
  { code:4800,  name:'Music & Composition', items:[
    { label:'Composer Fee',          isLabor:false, rate:2000, qty:1, days:1, comp:'expense', unit:'flat'    },
    { label:'Music Recording Session',isLabor:false,rate:1500, qty:1, days:1, comp:'expense', unit:'session' },
    { label:'Studio Time',           isLabor:false, rate:1000, qty:1, days:1, comp:'rental',  unit:'day'     },
    { label:'Mastering Fee',         isLabor:false, rate:500,  qty:1, days:1, comp:'expense', unit:'flat'    },
  ]},
  { code:6200,  name:'Distribution', items:[
    { label:'Festival Submission Fees',isLabor:false,rate:100, qty:5, days:1, comp:'expense', unit:'each' },
    { label:'Distribution Platform Fee',isLabor:false,rate:500,qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'DCP (Digital Cinema Pkg)',isLabor:false,rate:800, qty:1, days:1, comp:'expense', unit:'each' },
    { label:'Screener Copies',       isLabor:false, rate:200,  qty:1, days:1, comp:'expense', unit:'flat' },
  ]},
  { code:6400,  name:'Software & Digital Tools', items:[
    { label:'Production Software',   isLabor:false, rate:500,  qty:1, days:1, comp:'expense', unit:'flat'  },
    { label:'Cloud Storage',         isLabor:false, rate:100,  qty:1, days:1, comp:'expense', unit:'month' },
    { label:'Office Supplies',       isLabor:false, rate:200,  qty:1, days:1, comp:'purchase',unit:'flat'  },
    { label:'Printer Ink / Paper',   isLabor:false, rate:100,  qty:1, days:1, comp:'purchase',unit:'flat'  },
    { label:'Phone & Internet',      isLabor:false, rate:150,  qty:1, days:1, comp:'expense', unit:'month' },
  ]},
  { code:6000,  name:'Insurance', items:[
    { label:'Production Insurance',       isLabor:false, rate:2000, qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Equipment Insurance',        isLabor:false, rate:500,  qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'E&O Insurance',              isLabor:false, rate:3000, qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Workers Comp (% of labor)',  isLabor:false, rate:0,    qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'General Liability',          isLabor:false, rate:1500, qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Auto / Vehicle Insurance',   isLabor:false, rate:400,  qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Umbrella / Excess',          isLabor:false, rate:500,  qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'COI Fee',                    isLabor:false, rate:100,  qty:1, days:1, comp:'expense', unit:'each' },
  ]},
  { code:6500,  name:'Administrative', items:[
    { label:'Accounting / Bookkeeping',isLabor:false,rate:500, qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Legal Fees',            isLabor:false, rate:500,  qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Bank / Wire Fees',      isLabor:false, rate:50,   qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Contract Review',       isLabor:false, rate:300,  qty:1, days:1, comp:'expense', unit:'flat' },
  ]},
  { code:6300,  name:'Marketing & EPK', items:[
    { label:'Graphic Design',        isLabor:false, rate:1000, qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Social Media Content',  isLabor:false, rate:500,  qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Press Materials',       isLabor:false, rate:300,  qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Advertising Placement', isLabor:false, rate:0,    qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Trailer / Sizzle Edit', isLabor:false, rate:1500, qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'BTS Edit',              isLabor:false, rate:1000, qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Still Photos (license)',isLabor:false, rate:300,  qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'EPK Package',           isLabor:false, rate:2000, qty:1, days:1, comp:'expense', unit:'flat' },
  ]},
  { code:4900,  name:'Title Sequence', items:[
    { label:'Title Design',          isLabor:false, rate:2000, qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Animation / Motion Graphics',isLabor:false,rate:2500,qty:1,days:1,comp:'expense',unit:'flat'},
    { label:'Render Time',           isLabor:false, rate:200,  qty:1, days:1, comp:'expense', unit:'flat' },
  ]},
  { code:6600,  name:'Residuals', items:[
    { label:'SAG Residuals',         isLabor:false, rate:0, qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'WGA Residuals',         isLabor:false, rate:0, qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'DGA Residuals',         isLabor:false, rate:0, qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Residual Administration',isLabor:false,rate:0, qty:1, days:1, comp:'expense', unit:'flat' },
  ]},
  { code:6700,  name:'Miscellaneous', items:[
    { label:'Petty Cash',            isLabor:false, rate:500, qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Contingency',           isLabor:false, rate:0,   qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Rush Charges',          isLabor:false, rate:0,   qty:1, days:1, comp:'expense', unit:'flat' },
    { label:'Miscellaneous',         isLabor:false, rate:0,   qty:1, days:1, comp:'expense', unit:'flat' },
  ]},
];

// ── ROLE_LIBRARY kept for backward compat with any remaining references ──
const ROLE_LIBRARY = QE_CATEGORIES.flatMap(cat =>
  cat.items.map(item => ({
    dept: cat.name, coa: cat.code, coa_name: cat.name, role: item.label,
    rate: item.rate, kit: item.kit||0, fringe: item.fringe||'N', comp: item.comp
  }))
);

const UNION_FRINGE  = { 'N':'I', 'E':'E', 'S':'S', 'L':'L', 'U':'U', 'I':'I' };
const UNION_RATE_MULT = 1.20;

// per-category union flags (code → boolean)
// Categories with union implications: 2100 (Talent/SAG), 4000 (Post Staff/IATSE).
// Production Staff (2000) intentionally has NO union toggle — crew union status
// is tracked per-role via the unionFringe field on individual items instead.
const QE_UNION_LABELS = {
  2100: { nonunion: 'Non-Union', union: 'SAG' },
  4000: { nonunion: 'Non-Union', union: 'IATSE' },
};
let _qeUnionByCode = {}; // code → true (union) / false (non-union)

// Persistent cart: key → { cat, item, qty, days, rate, kit, fringe, comp, unit, agent_pct }
const _qeCart = new Map();
let _qeCatCode = null; // currently displayed category code

// Load the live Quick Entry catalog from the server. Transforms the
// /api/catalog response shape (snake_case, grouped by category) into the
// QE_CATEGORIES shape (camelCase, array of {code,name,items}). On failure,
// silently leaves QE_CATEGORIES at its hardcoded fallback.
async function loadQeCatalog() {
  try {
    const r = await fetch('/api/catalog', { credentials: 'same-origin' });
    if (!r.ok) return;                     // HTTP error → leave fallback
    const data = await r.json();
    if (!data || !Array.isArray(data.categories)) return;  // malformed → leave fallback
    // An EMPTY catalog is a valid state (user explicitly cleared it) —
    // replace QE_CATEGORIES with the empty array so QE shows "no items".
    QE_CATEGORIES = data.categories
      .slice()
      .sort((a, b) => a.code - b.code)
      .map(cat => ({
        code: cat.code,
        name: cat.name,
        items: (cat.items || []).map(it => ({
          label:       it.label,
          isLabor:     !!it.is_labor,
          rate:        Number(it.rate) || 0,
          qty:         Number(it.qty)  || 1,
          days:        Number(it.days) || 1,
          kit:         Number(it.kit_fee) || 0,
          fringe:      it.fringe || 'N',
          unionFringe: it.union_fringe || undefined,
          agent_pct:   Number(it.agent_pct) || 0,
          comp:        it.comp || (it.is_labor ? 'labor' : 'expense'),
          unit:        it.unit || (it.is_labor ? 'day' : 'flat'),
          group:       it.group_name || undefined,
        })),
      }));
  } catch (e) {
    console.warn('QE: /api/catalog fetch failed, using fallback list', e);
  }
}

// Quick Entry ALWAYS reads from the Global Quick Entry Catalog (the
// catalog_item DB table, exposed via /api/catalog). Super admin's
// /admin/catalog page is the single source of truth for QE content.
// When the DB is empty, the QE panel shows zero items (the user is
// expected to populate the catalog manually). DO NOT flip back to
// false without an explicit instruction from the user.
const QE_USE_DB_CATALOG = true;

async function openCascade() {
  _qeUnionByCode = {};
  _qeCart.clear();
  _qeCatCode = null;
  document.getElementById('qe-search').value = '';
  document.getElementById('cascade-panel').classList.remove('hidden');
  document.getElementById('cascade-overlay').classList.remove('hidden');
  if (QE_USE_DB_CATALOG) {
    await loadQeCatalog();
  }
  buildQeSidebar();
  renderQeCategory(null);
  updateQeUI();
}

function closeCascade() {
  document.getElementById('cascade-panel').classList.add('hidden');
  document.getElementById('cascade-overlay').classList.add('hidden');
  if (_qeCart.size > 0) reloadWithTab();
}

// per-category union toggle
function setCatUnion(code, isUnion) {
  _qeUnionByCode[code] = isUnion;
  const btnNU = document.getElementById(`qe-union-nu-${code}`);
  const btnU  = document.getElementById(`qe-union-u-${code}`);
  if (btnNU) btnNU.classList.toggle('active', !isUnion);
  if (btnU)  btnU.classList.toggle('active', isUnion);
  if (_qeCatCode === code) renderQeCategory(code);
}

// ── Sidebar ────────────────────────────────────────────────────────────────
function buildQeSidebar() {
  const sidebar = document.getElementById('qe-sidebar');
  sidebar.innerHTML = QE_CATEGORIES.map(cat => `
    <button class="qe-cat-btn" data-code="${cat.code}" onclick="selectQeCategory(${cat.code})">
      <span class="qe-cat-code">${cat.code}</span>
      <span class="qe-cat-name">${cat.name}</span>
      <span class="qe-cat-sel" id="qe-sel-${cat.code}"></span>
    </button>
  `).join('');
}

function selectQeCategory(code) {
  _qeCatCode = code;
  document.querySelectorAll('.qe-cat-btn').forEach(b => {
    b.classList.toggle('active', parseInt(b.dataset.code) === code);
  });
  document.getElementById('qe-search').value = '';
  renderQeCategory(code);
}

function renderQeCategory(code, searchQ) {
  const mainBody = document.getElementById('qe-main-body');
  const titleEl  = document.getElementById('qe-cat-title');
  const codeEl   = document.getElementById('qe-cat-code');

  if (code === null) {
    titleEl.textContent = 'Select a category';
    codeEl.textContent  = '';
    mainBody.innerHTML  = `<div class="qe-welcome">
      <p>Select a category from the left to see items, or type in the search box to search across <strong>every department</strong>.</p>
      <p class="muted" style="margin-top:.4rem;font-size:.8rem">Selections persist as you move between categories.</p>
    </div>`;
    return;
  }

  const cat = QE_CATEGORIES.find(c => c.code === code);
  if (!cat) return;

  titleEl.textContent = cat.name;
  codeEl.textContent  = cat.code;

  // per-category union toggle UI
  const unionLabel = QE_UNION_LABELS[code];
  const isUnion    = _qeUnionByCode[code] || false;
  const unionToggleHtml = unionLabel ? `
    <div class="qe-union-toggle" style="margin-left:.75rem">
      <button class="qe-union-btn ${!isUnion?'active':''}" id="qe-union-nu-${code}"
              onclick="setCatUnion(${code}, false)">${unionLabel.nonunion}</button>
      <button class="qe-union-btn ${isUnion?'active':''}" id="qe-union-u-${code}"
              onclick="setCatUnion(${code}, true)">${unionLabel.union}</button>
    </div>` : '';

  // show agent% column only for Talent (code 700)
  const showAgent = (code === 2100);  // Talent

  let items = cat.items;
  if (searchQ) {
    const q = searchQ.toLowerCase();
    items = items.filter(i => i.label.toLowerCase().includes(q));
  }

  // Build rows with optional group headers
  let lastGroup = null;
  const colCount = showAgent ? 9 : 8;
  const rows = items.map((item, idx) => {
    const key  = qeKey(code, item.label);
    const cart = _qeCart.get(key);
    const checked = !!cart;
    const effRate    = cart ? cart.rate    : (isUnion && item.isLabor ? Math.round(item.rate * UNION_RATE_MULT) : item.rate);
    const effFringe  = cart ? cart.fringe  : (isUnion && item.isLabor
      ? (item.unionFringe || UNION_FRINGE[item.fringe||'N'] || 'I')
      : (item.fringe||'N'));
    const effQty     = cart ? cart.qty     : item.qty;
    const effDays    = cart ? cart.days    : item.days;
    const effKit     = cart ? cart.kit     : (item.kit || 0);
    const effUnit    = cart ? cart.unit    : (item.unit || 'day');
    const effComp    = cart ? cart.comp    : item.comp;
    // agent_pct (stored as %, e.g. 10 means 10%)
    const effAgent   = cart ? (cart.agent_pct || 0) : (item.agent_pct || 0);

    const agentCell = showAgent && item.isLabor
      ? `<td class="qe-td-agent"><input type="number" class="qe-input qe-agent" value="${effAgent}" min="0" step="5" max="100"></td>`
      : (showAgent ? `<td class="qe-td-agent"><span class="muted" style="font-size:.75rem">—</span></td>` : '');

    const laborFields = item.isLabor ? `
      <td class="qe-td-qty">
        <input type="number" class="qe-input qe-qty" value="${effQty}" min="1" step="1"
               title="People — values above 1 create separate lines (A, B, C...)">
      </td>
      <td class="qe-td-days"><input type="number" class="qe-input qe-days" value="${effDays}" min="0.5" step="0.5"></td>
      <td class="qe-td-rate"><div class="qe-rate-wrap"><span class="qe-dollar">$</span>
        <input type="number" class="qe-input qe-rate" value="${effRate}" min="0" step="25">
        <span class="qe-rate-lbl">/day</span></div></td>
      <td class="qe-td-kit"><div class="qe-rate-wrap"><span class="qe-dollar">$</span>
        <input type="number" class="qe-input qe-kit" value="${effKit}" min="0" step="25">
        <span class="qe-rate-lbl">/day</span></div></td>
      <td class="qe-td-fringe">
        <select class="qe-select qe-fringe">
          ${['E','N','L','U','S','I','D'].map(f=>`<option value="${f}" ${effFringe===f?'selected':''}>${f}</option>`).join('')}
        </select>
      </td>
      ${agentCell}
      <td class="qe-td-comp"><span class="qe-comp-badge labor">Labor</span></td>
    ` : `
      <td class="qe-td-qty"><input type="number" class="qe-input qe-qty" value="${effQty}" min="1" step="1"></td>
      <td class="qe-td-days"><input type="number" class="qe-input qe-days" value="${effDays}" min="0.5" step="0.5"></td>
      <td class="qe-td-rate"><div class="qe-rate-wrap"><span class="qe-dollar">$</span>
        <input type="number" class="qe-input qe-rate" value="${effRate}" min="0" step="25">
        <span class="qe-rate-lbl">/${effUnit}</span></div></td>
      <td class="qe-td-kit"><span class="muted" style="font-size:.75rem">—</span></td>
      <td class="qe-td-fringe"><span class="muted" style="font-size:.75rem">—</span></td>
      ${agentCell}
      <td class="qe-td-comp"><span class="qe-comp-badge ${effComp}">${effComp}</span></td>
    `;

    // Group header row
    let groupHeader = '';
    if (item.group && item.group !== lastGroup) {
      lastGroup = item.group;
      groupHeader = `<tr class="qe-group-header"><td colspan="${colCount}"><span class="qe-group-label">${item.group}</span></td></tr>`;
    }

    return groupHeader + `<tr class="qe-item-row ${checked?'qe-row-checked':''}" data-key="${key}" data-cat="${code}" data-idx="${idx}">
      <td class="qe-td-check"><input type="checkbox" class="qe-cb" ${checked?'checked':''}></td>
      <td class="qe-td-label"><span class="qe-item-label">${item.label}</span></td>
      ${laborFields}
    </tr>`;
  }).join('');

  const agentHeader = showAgent ? `<th class="qe-th-agent">Agent%</th>` : '';

  mainBody.innerHTML = `
    <div style="display:flex;align-items:center;margin-bottom:.5rem">
      ${unionToggleHtml}
    </div>
    <table class="qe-table">
      <thead>
        <tr>
          <th class="qe-th-check"><input type="checkbox" id="qe-cat-check-all" onchange="qeSelectAllInCategory(this.checked)" title="Select all in category"></th>
          <th class="qe-th-label">Item</th>
          <th class="qe-th-qty">Qty</th>
          <th class="qe-th-days">Days</th>
          <th class="qe-th-rate">Rate</th>
          <th class="qe-th-kit">Kit Fee</th>
          <th class="qe-th-fringe">Fringe</th>
          ${agentHeader}
          <th class="qe-th-comp">Type</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>`;

  // Attach checkbox listeners
  mainBody.querySelectorAll('.qe-item-row').forEach(row => {
    const cb = row.querySelector('.qe-cb');
    cb.addEventListener('change', () => {
      syncRowToCart(row, cb.checked);
    });
    // Sync fields to cart on any input change. If the row isn't checked
    // yet, auto-check it — editing a qty/rate/days means the user intends
    // to add this item, so don't make them check the box separately.
    row.querySelectorAll('input[type="number"], select').forEach(inp => {
      inp.addEventListener('change', () => {
        if (!cb.checked) cb.checked = true;
        syncRowToCart(row, true);
      });
      // Also fire on 'input' (as-you-type) for number fields so the check
      // appears instantly, not only after blur.
      if (inp.type === 'number') {
        inp.addEventListener('input', () => {
          if (!cb.checked) {
            cb.checked = true;
            syncRowToCart(row, true);
          }
        });
      }
    });
  });
}

function qeKey(code, label) { return `${code}::${label}`; }

function syncRowToCart(row, checked) {
  const key   = row.dataset.key;
  const code  = parseInt(row.dataset.cat);
  const cat   = QE_CATEGORIES.find(c => c.code === code);
  const label = key.slice(key.indexOf('::') + 2);
  const item  = cat.items.find(i => i.label === label);

  row.classList.toggle('qe-row-checked', checked);

  if (!checked) {
    _qeCart.delete(key);
  } else {
    const qty       = parseFloat(row.querySelector('.qe-qty')?.value   || item.qty);
    const days      = parseFloat(row.querySelector('.qe-days')?.value  || item.days);
    const rate      = parseFloat(row.querySelector('.qe-rate')?.value  || item.rate);
    const kit       = parseFloat(row.querySelector('.qe-kit')?.value   || 0);
    const fringe    = row.querySelector('.qe-fringe')?.value || item.fringe || 'N';
    const comp      = item.comp;
    const unit      = item.unit || 'day';
    // read agent% if column present
    const agentEl   = row.querySelector('.qe-agent');
    const agent_pct = agentEl ? parseFloat(agentEl.value || 0) : (item.agent_pct || 0);
    // Union labor defaults to 8-hour day rate type
    const isUnion   = _qeUnionByCode[code] || false;
    const rate_type = item.isLabor ? (isUnion ? 'day_8' : 'day_10') : 'day_10';
    _qeCart.set(key, { code, coa_name: cat.name, item, qty, days, rate, kit, fringe, comp, unit, agent_pct, rate_type });
  }
  updateQeUI();
}

function updateQeUI() {
  const n = _qeCart.size;
  document.getElementById('qe-selected-count').textContent = `${n} selected`;
  document.getElementById('qe-count-badge').textContent = n;
  document.getElementById('btn-add-selected').disabled = n === 0;

  // Update sidebar badges
  const countsByCode = {};
  for (const [, v] of _qeCart) {
    countsByCode[v.code] = (countsByCode[v.code] || 0) + 1;
  }
  QE_CATEGORIES.forEach(cat => {
    const el = document.getElementById(`qe-sel-${cat.code}`);
    if (!el) return;
    const n = countsByCode[cat.code] || 0;
    el.textContent = n > 0 ? n : '';
    el.className = n > 0 ? 'qe-cat-sel has-sel' : 'qe-cat-sel';
  });

  // Update tray
  const chips = document.getElementById('qe-tray-chips');
  if (_qeCart.size === 0) {
    chips.innerHTML = `<span class="muted" style="font-size:.8rem;padding:.2rem .4rem">No items selected yet</span>`;
  } else {
    chips.innerHTML = [..._qeCart.entries()].map(([key, v]) => `
      <span class="qe-chip">
        <span class="qe-chip-name">${v.item.label}</span>
        <span class="qe-chip-code">${v.code}</span>
        <button class="qe-chip-remove" onclick="qeRemoveFromCart('${key.replace(/'/g,"\\'")}')">✕</button>
      </span>
    `).join('');
  }
}

function qeRemoveFromCart(key) {
  _qeCart.delete(key);
  // Uncheck in current view if visible
  const row = document.querySelector(`.qe-item-row[data-key="${CSS.escape(key)}"]`);
  if (row) {
    const cb = row.querySelector('.qe-cb');
    if (cb) cb.checked = false;
    row.classList.remove('qe-row-checked');
  }
  updateQeUI();
}

function qeSelectAllInCategory(checked) {
  // Acts on every visible .qe-item-row — works for both single-category
  // view and the global search view (where _qeCatCode may be null).
  const rows = document.querySelectorAll('.qe-item-row');
  if (!rows.length) return;
  rows.forEach(row => {
    const cb = row.querySelector('.qe-cb');
    if (cb) {
      cb.checked = checked;
      syncRowToCart(row, checked);
    }
  });
}

function qeClearAll() {
  _qeCart.clear();
  document.querySelectorAll('.qe-cb').forEach(cb => cb.checked = false);
  document.querySelectorAll('.qe-item-row').forEach(r => r.classList.remove('qe-row-checked'));
  updateQeUI();
}

function qeSearch(q) {
  // Empty query → restore the prior view (category if one was open, else
  // welcome screen). Non-empty → search across EVERY department (per user
  // 2026-05-28: "if it could search across all departments rather than
  // just the open department, would be really helpful.").
  const trimmed = (q || '').trim();
  if (!trimmed) {
    renderQeCategory(_qeCatCode);
    return;
  }
  renderQeGlobalSearch(trimmed);
}

// Cross-department search results. Mirrors renderQeCategory's row markup
// so the existing checkbox / input / cart-sync wiring works unchanged —
// the only diff is the rows come from every QE_CATEGORIES bucket and are
// grouped under a section header per department so the user can see which
// department each match belongs to. The Agent% column is always present
// (Talent items use it; others render "—") to avoid varying column counts
// across groups.
function renderQeGlobalSearch(q) {
  const mainBody = document.getElementById('qe-main-body');
  const titleEl  = document.getElementById('qe-cat-title');
  const codeEl   = document.getElementById('qe-cat-code');
  const qLower   = q.toLowerCase();

  // Collect matches by category, preserving QE_CATEGORIES order.
  const groups = [];
  let totalMatches = 0;
  QE_CATEGORIES.forEach(cat => {
    const matches = (cat.items || []).filter(i =>
      (i.label || '').toLowerCase().includes(qLower)
    );
    if (matches.length) {
      groups.push({ cat, matches });
      totalMatches += matches.length;
    }
  });

  // Highlight every category in the sidebar that has at least one match
  // so the user has spatial context for where results live.
  document.querySelectorAll('.qe-cat-btn').forEach(b => {
    b.classList.remove('active');
    b.classList.remove('qe-cat-has-match');
  });
  groups.forEach(({ cat }) => {
    const btn = document.querySelector(`.qe-cat-btn[data-code="${cat.code}"]`);
    if (btn) btn.classList.add('qe-cat-has-match');
  });

  titleEl.textContent = `Search: "${q}"`;
  codeEl.textContent  = totalMatches
    ? `${totalMatches} result${totalMatches !== 1 ? 's' : ''} across ${groups.length} department${groups.length !== 1 ? 's' : ''}`
    : 'No results';

  if (!groups.length) {
    mainBody.innerHTML = `<div class="qe-welcome">
      <p>No items match &ldquo;<strong>${_qeHtmlEscape(q)}</strong>&rdquo;.</p>
      <p class="muted" style="margin-top:.4rem;font-size:.8rem">Try a different keyword or clear the search box.</p>
    </div>`;
    return;
  }

  // Always reserve the Agent column in global search so Talent rows line
  // up with the rest. colCount = 9 = check + label + qty + days + rate +
  // kit + fringe + agent + comp.
  const colCount = 9;

  let html = '';
  groups.forEach(({ cat, matches }) => {
    const isUnion = _qeUnionByCode[cat.code] || false;
    // Department divider row.
    html += `<tr class="qe-group-header qe-dept-divider" data-cat="${cat.code}">
      <td colspan="${colCount}" style="background:var(--bg-3);padding:.4rem .55rem;border-top:1px solid var(--border)">
        <span class="qe-group-label" style="color:var(--accent)">${cat.code} · ${_qeHtmlEscape(cat.name)}</span>
        <span class="muted" style="font-size:.7rem;margin-left:.5rem">${matches.length} match${matches.length !== 1 ? 'es' : ''}</span>
      </td>
    </tr>`;

    matches.forEach((item, idx) => {
      const code  = cat.code;
      const key   = qeKey(code, item.label);
      const cart  = _qeCart.get(key);
      const checked   = !!cart;
      const effRate   = cart ? cart.rate
                              : (isUnion && item.isLabor ? Math.round(item.rate * UNION_RATE_MULT) : item.rate);
      const effFringe = cart ? cart.fringe
                              : (isUnion && item.isLabor
                                  ? (item.unionFringe || UNION_FRINGE[item.fringe||'N'] || 'I')
                                  : (item.fringe||'N'));
      const effQty    = cart ? cart.qty    : item.qty;
      const effDays   = cart ? cart.days   : item.days;
      const effKit    = cart ? cart.kit    : (item.kit || 0);
      const effUnit   = cart ? cart.unit   : (item.unit || 'day');
      const effComp   = cart ? cart.comp   : item.comp;
      const effAgent  = cart ? (cart.agent_pct || 0) : (item.agent_pct || 0);

      // Agent input only meaningful for Talent labor; placeholder cell
      // everywhere else keeps column alignment.
      const agentCell = (code === 2100 && item.isLabor)
        ? `<td class="qe-td-agent"><input type="number" class="qe-input qe-agent" value="${effAgent}" min="0" step="5" max="100"></td>`
        : `<td class="qe-td-agent"><span class="muted" style="font-size:.75rem">—</span></td>`;

      const laborFields = item.isLabor ? `
        <td class="qe-td-qty">
          <input type="number" class="qe-input qe-qty" value="${effQty}" min="1" step="1"
                 title="People — values above 1 create separate lines (A, B, C...)">
        </td>
        <td class="qe-td-days"><input type="number" class="qe-input qe-days" value="${effDays}" min="0.5" step="0.5"></td>
        <td class="qe-td-rate"><div class="qe-rate-wrap"><span class="qe-dollar">$</span>
          <input type="number" class="qe-input qe-rate" value="${effRate}" min="0" step="25">
          <span class="qe-rate-lbl">/day</span></div></td>
        <td class="qe-td-kit"><div class="qe-rate-wrap"><span class="qe-dollar">$</span>
          <input type="number" class="qe-input qe-kit" value="${effKit}" min="0" step="25">
          <span class="qe-rate-lbl">/day</span></div></td>
        <td class="qe-td-fringe">
          <select class="qe-select qe-fringe">
            ${['E','N','L','U','S','I','D'].map(f=>`<option value="${f}" ${effFringe===f?'selected':''}>${f}</option>`).join('')}
          </select>
        </td>
        ${agentCell}
        <td class="qe-td-comp"><span class="qe-comp-badge labor">Labor</span></td>
      ` : `
        <td class="qe-td-qty"><input type="number" class="qe-input qe-qty" value="${effQty}" min="1" step="1"></td>
        <td class="qe-td-days"><input type="number" class="qe-input qe-days" value="${effDays}" min="0.5" step="0.5"></td>
        <td class="qe-td-rate"><div class="qe-rate-wrap"><span class="qe-dollar">$</span>
          <input type="number" class="qe-input qe-rate" value="${effRate}" min="0" step="25">
          <span class="qe-rate-lbl">/${effUnit}</span></div></td>
        <td class="qe-td-kit"><span class="muted" style="font-size:.75rem">—</span></td>
        <td class="qe-td-fringe"><span class="muted" style="font-size:.75rem">—</span></td>
        ${agentCell}
        <td class="qe-td-comp"><span class="qe-comp-badge ${effComp}">${effComp}</span></td>
      `;

      html += `<tr class="qe-item-row ${checked?'qe-row-checked':''}" data-key="${key}" data-cat="${code}" data-idx="${idx}">
        <td class="qe-td-check"><input type="checkbox" class="qe-cb" ${checked?'checked':''}></td>
        <td class="qe-td-label"><span class="qe-item-label">${_qeHtmlEscape(item.label)}</span></td>
        ${laborFields}
      </tr>`;
    });
  });

  mainBody.innerHTML = `
    <table class="qe-table">
      <thead>
        <tr>
          <th class="qe-th-check"><input type="checkbox" id="qe-cat-check-all" onchange="qeSelectAllInCategory(this.checked)" title="Select all visible"></th>
          <th class="qe-th-label">Item</th>
          <th class="qe-th-qty">Qty</th>
          <th class="qe-th-days">Days</th>
          <th class="qe-th-rate">Rate</th>
          <th class="qe-th-kit">Kit Fee</th>
          <th class="qe-th-fringe">Fringe</th>
          <th class="qe-th-agent">Agent%</th>
          <th class="qe-th-comp">Type</th>
        </tr>
      </thead>
      <tbody>${html}</tbody>
    </table>`;

  // Same row-wiring renderQeCategory uses — checkbox toggle + auto-check
  // on any input change.
  mainBody.querySelectorAll('.qe-item-row').forEach(row => {
    const cb = row.querySelector('.qe-cb');
    cb.addEventListener('change', () => {
      syncRowToCart(row, cb.checked);
    });
    row.querySelectorAll('input[type="number"], select').forEach(inp => {
      inp.addEventListener('change', () => {
        if (!cb.checked) cb.checked = true;
        syncRowToCart(row, true);
      });
      if (inp.type === 'number') {
        inp.addEventListener('input', () => {
          if (!cb.checked) {
            cb.checked = true;
            syncRowToCart(row, true);
          }
        });
      }
    });
  });
}

function _qeHtmlEscape(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

// Convert 0 → A, 1 → B, ..., 25 → Z, 26 → AA, 27 → AB, ...
function _seqSuffix(n) {
  let s = '';
  n = Math.max(0, n | 0);
  do {
    s = String.fromCharCode(65 + (n % 26)) + s;
    n = Math.floor(n / 26) - 1;
  } while (n >= 0);
  return s;
}

async function qeAddSelected() {
  if (_qeCart.size === 0) {
    console.warn('[QE] Add Selected clicked but cart is empty');
    return;
  }
  const btn = document.getElementById('btn-add-selected');
  btn.disabled = true;
  btn.textContent = 'Adding…';
  console.log('[QE] Adding', _qeCart.size, 'cart entries');

  // Track the LAST section we added to so the post-reload focus handler
  // can scroll there (instead of jumping the user back to the top of the
  // budget). When a single QE add touches multiple sections we land on
  // the last one — the user can scroll up from there.
  let _lastAddedCode = null;

  // Save sequentially so kit fee rows are inserted immediately after their parent
  try {
  for (const [, v] of _qeCart) {
    const { code, coa_name, item, qty, days, rate, kit, fringe, comp, agent_pct, rate_type } = v;
    if (!item) {
      console.error('[QE] cart entry has no item — skipping', v);
      continue;
    }
    const isLabor = item.isLabor;
    _lastAddedCode = code;

    // Bottom-of-section append: don't pass a sort_order. The server's
    // upsert_line handler now computes max(sort_order)+10 within the
    // (budget, account_code) bucket whenever the client omits the
    // field, so QE adds always land at the end of their section. The
    // role_group clustering on the rendering side still keeps lines
    // grouped under the right sub-department header.

    // Labor lines with qty > 1 are SPLIT into that many separate named
    // lines with letter suffixes (A, B, C ...). Each resulting line has
    // quantity=1 and can be independently scheduled + assigned to a person.
    // Non-labor items keep qty as a multiplier on a single line.
    // (Users can also click "Duplicate Row" on the budget to copy a line
    // later, but setting qty up-front in QE is faster for bulk adds.)
    const shouldSplit = isLabor && qty > 1;
    const numLines    = shouldSplit ? qty : 1;
    const perLineQty  = shouldSplit ? 1 : qty;

    for (let i = 0; i < numLines; i++) {
      const suffix      = shouldSplit ? ' ' + _seqSuffix(i) : '';
      const description = item.label + suffix;

      const resp = await fetch(SAVE_URL, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          account_code:    code,
          account_name:    coa_name,
          description:     description,
          is_labor:        isLabor,
          // sort_order intentionally omitted — server appends to bottom of section.
          role_group:      item.group || null,
          quantity:        perLineQty,
          days:            days,
          // Non-labor rows must persist the user-entered rate so the
          // visible row shows the right rate/qty/days after save. Backend
          // auto-recomputes estimated_total = rate * qty * days for
          // non-labor lines, so sending both is safe.
          rate:            rate,
          rate_type:       isLabor ? (rate_type || 'day_10') : 'day_10',
          fringe_type:     isLabor ? fringe : 'N',
          agent_pct:       isLabor ? (agent_pct || 0) / 100 : 0,
          estimated_total: isLabor ? 0 : rate * perLineQty * days,
          // Task 2: link line back to the CatalogItem for exports. The
          // JS QE_CATEGORIES currently doesn't carry DB ids (Phase 2
          // replaces this array with /api/catalog data). Forward the id
          // when present; backend tolerates null/undefined.
          catalog_item_id: (item.id != null ? item.id : null),
          // Bypass the "estimated edit protection" 409 — Quick Entry
          // adds NEW lines, never edits an existing Estimated. Without
          // this, a project that already has a Working budget refuses
          // every QE add with 409 estimated_protected and the JS hangs
          // on resp.json() because the response carries no .id.
          override_estimated: true,
        })
      });
      if (!resp.ok) {
        const errText = await resp.text().catch(()=>'');
        console.error('[QE] save failed', resp.status, errText);
        alert('Quick Entry save failed: HTTP ' + resp.status + '\n' + errText.slice(0, 300));
        btn.disabled = false; btn.textContent = 'Add Selected';
        return;
      }

      if (isLabor && kit > 0) {
        // Kit fee per person — one per split line
        const parentData = await resp.json();
        if (parentData && parentData.id) {
          await fetch(`/projects/${PID}/budget/${BID}/line/${parentData.id}/kit-fee`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
              rate:     kit,
              quantity: 1,
              days:     days,
            })
          });
        }
      }
    }
  }
  } catch (err) {
    console.error('[QE] qeAddSelected crashed', err);
    alert('Quick Entry hit an error: ' + (err && err.message || err) + '\nCheck the browser console for details.');
    btn.disabled = false; btn.textContent = 'Add Selected';
    return;
  }
  closeCascade();
  // Reload so the new lines render server-side, but pass the LAST
  // section we touched so the post-reload handler scrolls back there
  // and highlights the new row instead of dumping the user at the top.
  if (_lastAddedCode) {
    reloadWithTab(_lastAddedCode);
  } else {
    reloadWithTab();
  }
}
