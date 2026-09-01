// Extracted from a templates/budget.html inline block (audit M9, 2026-07-20).
// Jinja values arrive via window.__BJ (set by the inline preamble before this
// script tag). Classic script, document order — semantics identical to inline.

(function() {
  const _pid = window.__BJ["b06__pid"];
  const _bid = window.__BJ["b06__bid"];
  // _ctxLineId and _ctxLineRow are declared globally so allInRecalc/allInApply can access them

  const menu = document.getElementById('budget-row-ctx-menu');

  function _showCtxMenuAt(row, x, y) {
    _ctxLineId  = parseInt(row.dataset.id);
    _ctxLineRow = row;
    // Change Group… only applies to role-grouped sections (2000), matching
    // the non-labor menu.
    const _cgBtn = document.getElementById('ctx-labor-change-group');
    if (_cgBtn) _cgBtn.style.display = (parseInt(row.dataset.code || '0') === 2000) ? '' : 'none';
    menu.style.visibility = 'hidden';
    menu.style.left = '0px';
    menu.style.top  = '0px';
    menu.classList.remove('hidden');
    requestAnimationFrame(() => {
      const mw = menu.offsetWidth, mh = menu.offsetHeight;
      menu.style.left = Math.max(4, Math.min(x + 4, window.innerWidth  - mw - 12)) + 'px';
      menu.style.top  = Math.max(4, Math.min(y + 4, window.innerHeight - mh - 12)) + 'px';
      menu.style.visibility = '';
    });
  }

  // Right-click on any labor row opens the context menu
  document.addEventListener('contextmenu', function(e) {
    const row = e.target.closest('.labor-line');
    if (!row) { menu.classList.add('hidden'); return; }
    e.preventDefault();
    _showCtxMenuAt(row, e.clientX, e.clientY);
  });

  // Insert / Spacer / Sub-header / Move / Duplicate parity with non-labor
  // rows. These reuse the shared window.lineInsert/lineDuplicate/move
  // helpers (which the ⋮ button already drives on labor rows); we just
  // point them at the right-clicked row first. 2026-05-20.
  menu.querySelectorAll('.labor-rowop').forEach(function(btn) {
    btn.addEventListener('click', function() {
      menu.classList.add('hidden');
      if (!_ctxLineId) return;
      const code = _ctxLineRow ? (_ctxLineRow.dataset.code || '') : '';
      if (window._setLineMenuTarget) window._setLineMenuTarget(_ctxLineId, code);
      switch (this.dataset.op) {
        case 'insert-above':  window.lineInsert('above'); break;
        case 'insert-below':  window.lineInsert('below'); break;
        case 'duplicate':     window.lineDuplicate(); break;
        case 'spacer-above':  window.lineInsert('above', 'spacer'); break;
        case 'spacer-below':  window.lineInsert('below', 'spacer'); break;
        case 'header-above':  window.lineInsert('above', 'header'); break;
        case 'header-below':  window.lineInsert('below', 'header'); break;
        case 'move':          window.openLineMoveModal(); break;
        case 'change-group':  window.openChangeGroupModal(); break;
      }
    });
  });

  // Long-press (500ms) on touch devices — triggers context menu without right-click
  let _lpTimer = null, _lpRow = null, _lpX = 0, _lpY = 0;
  document.addEventListener('touchstart', function(e) {
    const row = e.target.closest('.labor-line');
    if (!row || e.target.closest('.editable') || e.target.closest('select')) return;
    const t = e.touches[0];
    _lpX = t.clientX; _lpY = t.clientY; _lpRow = row;
    _lpTimer = setTimeout(() => {
      if (_lpRow) {
        _showCtxMenuAt(_lpRow, _lpX, _lpY);
      }
    }, 500);
  }, { passive: true });
  document.addEventListener('touchend',   () => { clearTimeout(_lpTimer); _lpTimer = null; });
  document.addEventListener('touchmove',  () => { clearTimeout(_lpTimer); _lpTimer = null; });

  // Close on outside click
  document.addEventListener('click', function(e) {
    if (!e.target.closest('#budget-row-ctx-menu')) {
      menu.classList.add('hidden');
    }
  });

  // Add Kit Fee / Custom Fee — one centered modal, two flavors (owner
  // 2026-09-01: "right click as we do for a kit fee… wardrobe fees…
  // custom fees"). _kitFeeTag rides the POST so the server stamps the
  // right line_tag + default description.
  let _kitFeeTag = 'kit_fee';
  function _openFeeModal(tag) {
    menu.classList.add('hidden');
    if (!_ctxLineId || !_ctxLineRow) return;
    _kitFeeTag = tag;
    const isKit = tag === 'kit_fee';
    const parentDesc = (_ctxLineRow.querySelector('.editable[data-field="description"]')?.textContent || '').trim();
    const title = document.querySelector('#kit-fee-modal h2');
    if (title) title.textContent = isKit ? '🔧 Add Kit Fee' : '👔 Add Custom Fee';
    const sub = document.getElementById('kit-fee-modal-subtitle');
    if (sub) {
      sub.textContent = isKit
        ? `Adds a child kit-fee line under "${parentDesc}". Kit fees are never labor.`
        : `Adds a child fee line under "${parentDesc}" — wardrobe fee, styling fee, any custom payment. Fees are never labor.`;
    }
    const descEl = document.getElementById('kit-fee-description');
    descEl.value = '';
    descEl.placeholder = isKit ? 'Kit Fee' : 'e.g. Wardrobe Fee';
    document.getElementById('kit-fee-qty').value = '1';
    document.getElementById('kit-fee-days').value = '1';
    document.getElementById('kit-fee-type').value = isKit ? 'days' : 'flat';
    document.getElementById('kit-fee-rate').value = '';
    document.getElementById('kit-fee-preview').style.display = 'none';
    document.getElementById('kit-fee-modal').classList.remove('hidden');
    setTimeout(() => document.getElementById('kit-fee-rate').focus(), 50);
  }
  document.getElementById('ctx-add-kit-fee').addEventListener('click', () => _openFeeModal('kit_fee'));
  const _customFeeBtn = document.getElementById('ctx-add-custom-fee');
  if (_customFeeBtn) _customFeeBtn.addEventListener('click', () => _openFeeModal('custom_fee'));

  // Agent / Rep fee (owner 2026-09-01): usually a % of the pre-tax/fringe
  // subtotal (rides the line's agent_pct so it tracks rate changes live),
  // or a flat child line when they're paid an odd fixed amount.
  const _agentFeeBtn = document.getElementById('ctx-add-agent-fee');
  if (_agentFeeBtn) _agentFeeBtn.addEventListener('click', function() {
    menu.classList.add('hidden');
    if (!_ctxLineId || !_ctxLineRow) return;
    window._agentFeeLineId = _ctxLineId;
    const parentDesc = (_ctxLineRow.querySelector('.editable[data-field="description"]')?.textContent || '').trim();
    const sub = document.getElementById('agent-fee-modal-subtitle');
    if (sub) sub.textContent = `Agent / representative fee for "${parentDesc}".`;
    // Wage subtotal (before fringes/taxes) read from the row — the % fee
    // is computed against it and added as a CHILD line (owner 2026-09-08:
    // "get rid of the column, just use the right click — it'll be a lower
    // down line, like a kit fee. That's much better.").
    const subTxt = (_ctxLineRow.querySelector('.line-subtotal')?.textContent || '').replace(/[$,]/g, '');
    window._agentFeeSubtotal = parseFloat(subTxt) || 0;
    document.getElementById('agent-fee-pct').value = '10';
    document.getElementById('agent-fee-flat').value = '';
    document.querySelectorAll('input[name="agent-fee-mode"]').forEach(r => { r.checked = (r.value === 'pct'); });
    if (typeof window._agentFeeModeSync === 'function') window._agentFeeModeSync();
    if (typeof window._agentFeePreview === 'function') window._agentFeePreview();
    document.getElementById('agent-fee-modal').classList.remove('hidden');
    setTimeout(() => document.getElementById('agent-fee-pct').focus(), 50);
  });

  window._agentFeePreview = function() {
    const el = document.getElementById('agent-fee-preview');
    if (!el) return;
    const pct = parseFloat(document.getElementById('agent-fee-pct').value || '0') || 0;
    const sub = window._agentFeeSubtotal || 0;
    if (pct > 0 && sub > 0) {
      const amt = sub * pct / 100;
      const money = n => '$' + n.toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2});
      el.textContent = `${pct}% of ${money(sub)} = ${money(amt)}`;
    } else {
      el.textContent = '';
    }
  };

  window._agentFeeModeSync = function() {
    const mode = document.querySelector('input[name="agent-fee-mode"]:checked')?.value || 'pct';
    document.getElementById('agent-fee-pct-wrap').style.display  = mode === 'pct'  ? '' : 'none';
    document.getElementById('agent-fee-flat-wrap').style.display = mode === 'flat' ? '' : 'none';
  };

  window.applyAgentFee = async function() {
    const lid = window._agentFeeLineId;
    if (!lid) return;
    const mode = document.querySelector('input[name="agent-fee-mode"]:checked')?.value || 'pct';
    const btn = document.getElementById('agent-fee-apply-btn');
    btn.disabled = true; btn.textContent = 'Adding…';
    try {
      // Both modes create a CHILD LINE (2026-09-08) — the earlier % path
      // patched the parent's agent_pct, which bounced off the
      // Estimated-protection 409 ("Failed to add agent fee") and hid the
      // fee in a column nobody wanted. Child lines behave like kit fees.
      let amt, desc;
      if (mode === 'pct') {
        const pct = parseFloat(document.getElementById('agent-fee-pct').value || '0') || 0;
        amt = Math.round((window._agentFeeSubtotal || 0) * pct) / 100;
        desc = `Agent Fee (${pct}%)`;
        if (!(amt > 0)) {
          alert('Enter a percentage — the line needs a wage subtotal to calculate from.');
          btn.disabled = false; btn.textContent = 'Add Fee';
          return;
        }
      } else {
        amt = parseFloat(document.getElementById('agent-fee-flat').value || '0') || 0;
        desc = 'Agent Fee';
      }
      const r = await fetch(`/projects/${_pid}/budget/${_bid}/line/${lid}/kit-fee`, {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ tag: 'agent_fee', description: desc,
                               rate: amt, quantity: 1, days: 1, days_unit: 'flat' }),
      });
      const j = await r.json().catch(() => ({}));
      if (r.ok) {
        document.getElementById('agent-fee-modal').classList.add('hidden');
        reloadWithTab();
      } else {
        alert(j.error || 'Failed to add agent fee.');
        btn.disabled = false; btn.textContent = 'Add Fee';
      }
    } catch (e) {
      alert('Failed: ' + e.message);
      btn.disabled = false; btn.textContent = 'Add Fee';
    }
  };

  // Live preview as the user types — qty × days × rate.
  function _kitFeeRecalc() {
    const qty  = parseFloat(document.getElementById('kit-fee-qty').value || '0') || 0;
    const days = parseFloat(document.getElementById('kit-fee-days').value || '0') || 0;
    const rate = parseFloat(document.getElementById('kit-fee-rate').value || '0') || 0;
    const preview = document.getElementById('kit-fee-preview');
    if (rate > 0 && qty > 0 && days > 0) {
      const total = qty * days * rate;
      document.getElementById('kit-fee-preview-total').textContent =
        '$' + total.toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2});
      const _formatNum = n => (n === Math.floor(n)) ? String(Math.floor(n)) : String(n);
      document.getElementById('kit-fee-preview-formula').textContent =
        `${_formatNum(qty)} × ${_formatNum(days)} × $${_formatNum(rate)}`;
      preview.style.display = '';
    } else {
      preview.style.display = 'none';
    }
  }
  ['kit-fee-qty','kit-fee-days','kit-fee-rate'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.addEventListener('input', _kitFeeRecalc);
  });

  window.applyKitFee = async function() {
    const lid = _ctxLineId;
    if (!lid) return;
    const btn = document.getElementById('kit-fee-apply-btn');
    btn.disabled = true; btn.textContent = 'Adding…';
    const body = {
      description: (document.getElementById('kit-fee-description').value || '').trim(),
      rate:        parseFloat(document.getElementById('kit-fee-rate').value || '0') || 0,
      quantity:    parseFloat(document.getElementById('kit-fee-qty').value  || '1') || 1,
      days:        parseFloat(document.getElementById('kit-fee-days').value || '1') || 1,
      days_unit:   document.getElementById('kit-fee-type').value || 'days',
      tag:         _kitFeeTag,
    };
    try {
      const r = await fetch(`/projects/${_pid}/budget/${_bid}/line/${lid}/kit-fee`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(body),
      });
      if (r.ok) {
        document.getElementById('kit-fee-modal').classList.add('hidden');
        reloadWithTab();
      } else {
        alert('Failed to add kit fee.');
        btn.disabled = false; btn.textContent = 'Add Kit Fee';
      }
    } catch (e) {
      alert('Failed to add kit fee: ' + e.message);
      btn.disabled = false; btn.textContent = 'Add Kit Fee';
    }
  };

  // ── All-in Total Calculator ──────────────────────────────────────────────
  // All-in Total is PAUSED (owner 2026-09-01: "not fully vetted — remove
  // that function until we're ready"). The menu button is gone from the
  // template; the modal + handlers stay dormant behind this null guard so
  // re-enabling is a one-line template change.
  const _allInBtn = document.getElementById('ctx-all-in-total');
  if (_allInBtn) _allInBtn.addEventListener('click', function() {
    menu.classList.add('hidden');
    if (!_ctxLineId || !_ctxLineRow) return;
    // Reset modal
    document.getElementById('all-in-amount').value = '';
    document.getElementById('all-in-result').style.display = 'none';
    document.getElementById('all-in-apply-btn').disabled = true;
    document.getElementById('all-in-modal').classList.remove('hidden');
    setTimeout(() => document.getElementById('all-in-amount').focus(), 50);
  });

  // ── Days-unit RIGHT-CLICK menu on each row ───────────────────────────────
  // Previously was a click-to-cycle D/W toggle. Now opens a popup on
  // right-click with a selectable list of denominations (Days, Weeks, X,
  // Per, Allow, EST, LB, Hrs, Flat, Mo, Mi, Ea) plus a Custom… option
  // that prompts for a free-form label. Left-click is a no-op (easier
  // to right-click without accidentally changing it).
  const _UNIT_OPTIONS = [
    { code: 'days',  label: 'Days',   short: 'D'   },
    { code: 'weeks', label: 'Weeks',  short: 'W'   },
    { code: 'hrs',   label: 'Hours',  short: 'Hrs' },
    { code: 'flat',  label: 'Flat',   short: 'Flat'},
    { code: 'x',     label: 'X',      short: 'X'   },
    { code: 'per',   label: 'Per',    short: 'Per' },
    { code: 'allow', label: 'Allow',  short: 'Allow'},
    { code: 'est',   label: 'EST',    short: 'EST' },
    { code: 'lb',    label: 'LB',     short: 'LB'  },
    { code: 'mo',    label: 'Month',  short: 'Mo'  },
    { code: 'mi',    label: 'Mile',   short: 'Mi'  },
    { code: 'ea',    label: 'Each',   short: 'Ea'  },
  ];

  function _shortForUnit(u) {
    if (!u) return 'D';
    const hit = _UNIT_OPTIONS.find(o => o.code === u);
    if (hit) return hit.short;
    // Custom value: show first 4 chars, uppercase
    return String(u).slice(0,4).toUpperCase();
  }

  // Build menu element once
  let _unitMenu = document.getElementById('days-unit-menu');
  if (!_unitMenu) {
    _unitMenu = document.createElement('div');
    _unitMenu.id = 'days-unit-menu';
    _unitMenu.className = 'ctx-menu hidden';
    _unitMenu.style.cssText = 'position:fixed;z-index:9999;background:var(--bg-2);'
      + 'color:var(--text);border:1px solid var(--border);border-radius:6px;'
      + 'box-shadow:var(--shadow);padding:4px 0;min-width:160px;font-size:.85rem';
    _unitMenu.innerHTML = _UNIT_OPTIONS.map(o =>
      `<div class="ctx-menu-item" data-unit="${o.code}"
             style="padding:6px 14px;cursor:pointer"
             onmouseover="this.style.background='var(--bg-3)'"
             onmouseout="this.style.background='transparent'">${o.label}</div>`
    ).join('')
      + '<div style="height:1px;background:var(--border,#333);margin:4px 0"></div>'
      + `<div class="ctx-menu-item" data-unit="__custom__"
             style="padding:6px 14px;cursor:pointer;font-style:italic"
             onmouseover="this.style.background='var(--bg-3)'"
             onmouseout="this.style.background='transparent'">Custom…</div>`;
    document.body.appendChild(_unitMenu);
  }

  let _unitBtnActive = null;

  function _hideUnitMenu() {
    _unitMenu.classList.add('hidden');
    _unitBtnActive = null;
  }

  async function _saveUnit(btn, newUnit) {
    const lid = parseInt(btn.dataset.id);
    const oldUnit = btn.dataset.unit || 'days';
    btn.dataset.unit = newUnit;
    btn.textContent  = _shortForUnit(newUnit);
    btn.classList.toggle('active', newUnit === 'weeks');
    // Calendar-duration preservation: when flipping Days → Weeks,
    // divide the duration field by 5 (or whatever days_per_week is) so
    // "10 days" becomes "2 weeks" — same calendar span, same subtotal.
    // Without this, _effective_days() server-side multiplies by 5 and
    // the subtotal explodes 5×. Per user 2026-05-01 — duration unit
    // toggles should NEVER change the dollar total, only the label.
    let convertBody = {};
    try {
      const row = document.querySelector(`tr.line-row[data-id="${lid}"]`);
      const dpwAttr = row?.dataset.daysPerWeek || row?.dataset.dpw || '5';
      const dpw = Math.max(parseFloat(dpwAttr) || 5, 1);
      const daysInput = row?.querySelector('[data-field="days"]');
      const curDays = parseFloat(daysInput?.value || daysInput?.textContent || row?.dataset.days || '1');
      if (!isNaN(curDays) && curDays > 0) {
        let newDays = curDays;
        if (oldUnit !== 'weeks' && newUnit === 'weeks')      newDays = +(curDays / dpw).toFixed(2);
        else if (oldUnit === 'weeks' && newUnit !== 'weeks') newDays = +(curDays * dpw).toFixed(2);
        if (newDays !== curDays) {
          convertBody.days = newDays;
          // Reflect in the visible cell so the user sees the swap.
          if (daysInput) {
            if ('value' in daysInput) daysInput.value = newDays;
            else daysInput.textContent = newDays;
          }
          if (row) row.dataset.days = String(newDays);
        }
      }
    } catch(_) {}
    try {
      const r = await fetch(`/projects/${_pid}/budget/${_bid}/line`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ id: lid, days_unit: newUnit, ...convertBody })
      });
      if (r.ok) {
        const data = await r.json();
        if (typeof refreshLineRow === 'function') refreshLineRow(lid, data);
      }
    } catch(_) {}
  }

  // Build the menu HTML, optionally including any custom unit values
  // already in use anywhere on this budget so they auto-populate as
  // dropdown choices for the rest of the project.
  function _renderUnitMenu() {
    const stdCodes = new Set(_UNIT_OPTIONS.map(o => o.code));
    const customs = new Set();
    document.querySelectorAll('.days-unit-toggle').forEach(b => {
      const u = (b.dataset.unit || '').trim();
      if (u && !stdCodes.has(u)) customs.add(u);
    });
    let html = _UNIT_OPTIONS.map(o =>
      `<div class="ctx-menu-item" data-unit="${o.code}"
             style="padding:6px 14px;cursor:pointer"
             onmouseover="this.style.background='var(--bg-3)'"
             onmouseout="this.style.background='transparent'">${o.label}</div>`
    ).join('');
    if (customs.size > 0) {
      html += '<div style="height:1px;background:var(--border,#333);margin:4px 0"></div>'
            + '<div style="padding:2px 14px;font-size:.7rem;color:var(--text-muted,#888);'
            + 'text-transform:uppercase;letter-spacing:.05em">This project</div>';
      [...customs].sort().forEach(u => {
        const safe = u.replace(/"/g,'&quot;').replace(/</g,'&lt;');
        html += `<div class="ctx-menu-item" data-unit="${safe}"
                  style="padding:6px 14px;cursor:pointer"
                  onmouseover="this.style.background='var(--bg-3)'"
                  onmouseout="this.style.background='transparent'">${safe.charAt(0).toUpperCase() + safe.slice(1)}</div>`;
      });
    }
    html += '<div style="height:1px;background:var(--border,#333);margin:4px 0"></div>'
          + `<div class="ctx-menu-item" data-unit="__custom__"
              style="padding:6px 14px;cursor:pointer;font-style:italic"
              onmouseover="this.style.background='var(--bg-3)'"
              onmouseout="this.style.background='transparent'">Custom…</div>`;
    _unitMenu.innerHTML = html;
  }

  // Non-labor <select> change — save new unit the same way as the menu.
  document.addEventListener('change', function(e) {
    const sel = e.target.closest('.days-unit-select');
    if (!sel) return;
    const oldUnit = sel.dataset.unit || 'days';
    const newUnit = sel.value;
    // "Custom…" option opens a prompt for free-text entry.
    if (newUnit === '__custom__') {
      const prev = sel.dataset.unit || 'days';
      const custom = prompt('Custom unit label (e.g. "Rolls", "Pack", "Shoot"):', '');
      if (custom === null) {
        // Restore previous selection
        sel.value = prev;
        return;
      }
      const cleaned = String(custom).trim().slice(0, 20);
      if (!cleaned) { sel.value = prev; return; }
      const newCode = cleaned.toLowerCase();
      const label = cleaned.charAt(0).toUpperCase() + cleaned.slice(1);
      // Project-wide propagation (per user 2026-05-04): inject the new
      // custom option into EVERY .days-unit-select on the page, not just
      // this one. So when someone types "Rolls" on a Camera Equipment
      // line, every other line's dropdown gets "Rolls" available too.
      // The server picks it up on next page load via project_custom_units.
      document.querySelectorAll('.days-unit-select').forEach(other => {
        if (Array.from(other.options).find(o => o.value === newCode)) return;
        const opt = document.createElement('option');
        opt.value = newCode;
        opt.textContent = label;
        const customOpt = Array.from(other.options).find(o => o.value === '__custom__');
        if (customOpt) other.insertBefore(opt, customOpt);
        else other.appendChild(opt);
      });
      sel.value = newCode;
      sel.dataset.unit = newCode;
      _saveSelectUnit(sel, newCode, oldUnit);
      return;
    }
    sel.dataset.unit = newUnit;
    _saveSelectUnit(sel, newUnit, oldUnit);
  });

  async function _saveSelectUnit(sel, newUnit, oldUnit) {
    const lid = parseInt(sel.dataset.id);
    // Same calendar-duration preservation as _saveUnit (labor toggle).
    // Non-labor lines are calc'd qty × days × rate (no _effective_days
    // multiplier) so changing the unit alone wouldn't blow up the
    // subtotal — but for consistency the displayed value should still
    // swap so "10 days" doesn't look like "10 rolls" with the same number.
    let convertBody = {};
    try {
      const _prev = oldUnit || sel.dataset.prevUnit || 'days';
      const row = document.querySelector(`tr.line-row[data-id="${lid}"]`);
      const dpwAttr = row?.dataset.daysPerWeek || row?.dataset.dpw || '5';
      const dpw = Math.max(parseFloat(dpwAttr) || 5, 1);
      const daysInput = row?.querySelector('[data-field="days"]');
      const curDays = parseFloat(daysInput?.value || daysInput?.textContent || row?.dataset.days || '1');
      if (!isNaN(curDays) && curDays > 0) {
        let newDays = curDays;
        if (_prev !== 'weeks' && newUnit === 'weeks')      newDays = +(curDays / dpw).toFixed(2);
        else if (_prev === 'weeks' && newUnit !== 'weeks') newDays = +(curDays * dpw).toFixed(2);
        if (newDays !== curDays) {
          convertBody.days = newDays;
          if (daysInput) {
            if ('value' in daysInput) daysInput.value = newDays;
            else daysInput.textContent = newDays;
          }
          if (row) row.dataset.days = String(newDays);
        }
      }
      sel.dataset.prevUnit = newUnit;
    } catch(_) {}
    try {
      const r = await fetch(`/projects/${_pid}/budget/${_bid}/line`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ id: lid, days_unit: newUnit, ...convertBody })
      });
      if (r.ok) {
        const data = await r.json();
        if (typeof refreshLineRow === 'function') refreshLineRow(lid, data);
      }
    } catch(_) {}
  }

  // Right-click on the unit button → open menu
  document.addEventListener('contextmenu', function(e) {
    const btn = e.target.closest('.days-unit-toggle');
    if (!btn) return;
    e.preventDefault();
    e.stopPropagation();
    _unitBtnActive = btn;
    _renderUnitMenu();
    // Position: snap inside viewport
    _unitMenu.classList.remove('hidden');
    const mw = _unitMenu.offsetWidth || 160;
    const mh = _unitMenu.offsetHeight || 260;
    const x = Math.min(e.clientX, window.innerWidth  - mw - 8);
    const y = Math.min(e.clientY, window.innerHeight - mh - 8);
    _unitMenu.style.left = x + 'px';
    _unitMenu.style.top  = y + 'px';
  });

  // Swallow left-click on the unit button so it doesn't do anything surprising
  document.addEventListener('click', function(e) {
    const btn = e.target.closest('.days-unit-toggle');
    if (btn) { e.stopPropagation(); return; }
    // Click outside menu closes it
    if (!e.target.closest('#days-unit-menu')) _hideUnitMenu();
  });

  // Menu item click
  _unitMenu.addEventListener('click', function(e) {
    const item = e.target.closest('.ctx-menu-item');
    if (!item || !_unitBtnActive) return;
    const btn = _unitBtnActive;
    const choice = item.dataset.unit;
    _hideUnitMenu();
    if (choice === '__custom__') {
      const custom = prompt('Custom unit label (e.g. "Rolls", "Pack", "Shoot"):',
                            btn.dataset.unit && !_UNIT_OPTIONS.find(o => o.code === btn.dataset.unit)
                              ? btn.dataset.unit : '');
      if (custom === null) return;
      const cleaned = String(custom).trim().slice(0, 20);
      if (!cleaned) return;
      _saveUnit(btn, cleaned.toLowerCase());
    } else {
      _saveUnit(btn, choice);
    }
  });

  // Escape closes
  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') _hideUnitMenu();
  });

})();
