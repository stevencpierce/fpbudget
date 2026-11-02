// Extracted from a templates/budget.html inline block (audit M9, 2026-07-20).
// Jinja values arrive via window.__BJ (set by the inline preamble before this
// script tag). Classic script, document order — semantics identical to inline.

(function() {
  const _pid = window.__BJ["b12__pid"];
  const _bid = window.__BJ["b12__bid"];

  let _menuLineId  = null;
  let _menuCode    = null;

  // Allow the labor right-click menu (a separate IIFE) to target the same
  // row before invoking the shared lineInsert/lineDuplicate/move helpers,
  // so labor rows get the same Insert/Spacer/Header/Move actions as
  // non-labor rows. 2026-05-20.
  window._setLineMenuTarget = function(id, code) {
    _menuLineId = parseInt(id) || null;
    _menuCode   = (code !== undefined && code !== null && code !== '') ? parseInt(code) : null;
  };
  const _menu = document.getElementById('line-row-menu');
  const _addMenu = document.getElementById('line-add-menu');
  let _menuOpener = null;   // the button that opened the current menu (focus returns here)

  // Position a menu under its button, right-aligned to it (the artboard's
  // "+" / "⋯" menus hang off the row's right end), clamped to the viewport.
  function _placeMenu(menu, btn) {
    requestAnimationFrame(() => {
      const rect = btn.getBoundingClientRect();
      const mw = menu.offsetWidth;
      const mh = menu.offsetHeight;
      menu.style.top  = Math.min(rect.bottom + 4, window.innerHeight - mh - 8) + 'px';
      menu.style.left = Math.max(4, Math.min(rect.right - mw, window.innerWidth - mw - 8)) + 'px';
      const first = menu.querySelector('[role="menuitem"]:not([style*="display: none"]):not([style*="display:none"])');
      if (first) first.focus();
    });
  }
  function _closeMenus(returnFocus) {
    const wasOpen = !_menu.classList.contains('hidden') || (_addMenu && !_addMenu.classList.contains('hidden'));
    _menu.classList.add('hidden');
    if (_addMenu) _addMenu.classList.add('hidden');
    document.querySelectorAll('.line-act-btn[aria-expanded="true"]').forEach(b => b.setAttribute('aria-expanded', 'false'));
    if (returnFocus && wasOpen && _menuOpener) { try { _menuOpener.focus(); } catch (e) {} }
  }
  window._closeLineMenus = _closeMenus;

  function _target(btn) {
    _menuLineId = parseInt(btn.dataset.id);
    _menuCode   = parseInt(btn.dataset.code);
    _menuOpener = btn;
  }

  // The row's "+" menu: Line below / Sub-header / Spacer / Duplicate.
  window.openLineAddMenu = function(btn) {
    _closeMenus(false);
    _target(btn);
    const kind = btn.dataset.kind || (btn.closest('tr') && btn.closest('tr').dataset.tag) || '';
    _addMenu.querySelectorAll('.line-menu-line-only').forEach(el => {
      el.style.display = (kind === 'spacer' || kind === 'header') ? 'none' : '';
    });
    _addMenu.classList.remove('hidden');
    btn.setAttribute('aria-expanded', 'true');
    _placeMenu(_addMenu, btn);
  };

  // The row's "⋯" menu: Flag / Note / Line.
  window.openLineRowMenu = function(btn) {
    _closeMenus(false);
    _target(btn);
    const kind = btn.dataset.kind || (btn.closest('tr') && btn.closest('tr').dataset.tag) || '';
    const structural = (kind === 'spacer' || kind === 'header');
    _menu.querySelectorAll('.line-menu-line-only').forEach(el => { el.style.display = structural ? 'none' : ''; });
    // Show the "Change group…" option only for sections that use role_group
    // sub-headers (currently just 2000 Production Staff).
    const cgBtn = document.getElementById('line-menu-change-group-btn');
    if (cgBtn) cgBtn.style.display = (_menuCode === 2000 && !structural) ? '' : 'none';
    const poLbl = document.getElementById('line-menu-po-label');
    if (poLbl) poLbl.textContent = (btn.dataset.hasPo === '1') ? 'Change PO…' : 'Attach PO';
    const lvBtn = document.getElementById('line-menu-lever-btn');
    if (lvBtn) lvBtn.querySelector('.line-menu-sub').textContent =
      (btn.dataset.hasLever === '1') ? 'Already a lever — open Assumptions to edit' : 'A cost the client can choose';
    _menu.classList.remove('hidden');
    btn.setAttribute('aria-expanded', 'true');
    _placeMenu(_menu, btn);
  };

  // Esc closes an open menu and returns focus to its button; arrow keys
  // walk the items.
  document.addEventListener('keydown', e => {
    const open = [_menu, _addMenu].find(m => m && !m.classList.contains('hidden'));
    if (!open) return;
    if (e.key === 'Escape') { e.preventDefault(); _closeMenus(true); return; }
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      const items = Array.from(open.querySelectorAll('[role="menuitem"]')).filter(el => el.offsetParent !== null);
      if (!items.length) return;
      e.preventDefault();
      const i = items.indexOf(document.activeElement);
      const next = e.key === 'ArrowDown' ? items[(i + 1) % items.length] : items[(i - 1 + items.length) % items.length];
      next.focus();
    }
  });

  // Right-click on any budget line row opens the same context menu as the
  // ⋮ button — just faster. Position the menu at the cursor instead of the
  // button. Per user 2026-05-04: SKIP labor rows here — they have their
  // own right-click menu (Add Kit Fee / All-in Total / Sync omit) which
  // is more important for the labor workflow. Insert/Duplicate/Move are
  // still reachable on labor rows via the ⋮ hamburger button.
  document.addEventListener('contextmenu', function(e) {
    const row = e.target.closest('.line-row');
    if (!row) return;
    // Kit-fee child rows follow their parent — no independent menu.
    if (row.classList.contains('kit-fee-row')) return;
    // Labor rows: defer to the kit-fee context menu (registered earlier).
    if (row.classList.contains('labor-line')) return;
    const id   = parseInt(row.dataset.id || '0');
    const code = parseInt(row.dataset.code || '0');
    if (!id) return;
    e.preventDefault();
    // Same menu as the row's ⋯ button, placed at the cursor.
    const btn = row.querySelector('.line-row-menu-btn');
    if (btn) { window.openLineRowMenu(btn); }
    else { _menuLineId = id; _menuCode = code; _menu.classList.remove('hidden'); }
    requestAnimationFrame(() => {
      const mw = _menu.offsetWidth;
      const mh = _menu.offsetHeight;
      _menu.style.top  = Math.min(e.clientY, window.innerHeight - mh - 8) + 'px';
      _menu.style.left = Math.min(e.clientX, window.innerWidth - mw - 8) + 'px';
    });
  });

  window.openChangeGroupModal = function() {
    _closeMenus(false);
    if (!_menuLineId || !_menuCode) return;
    const modal = document.getElementById('change-group-modal');
    const list  = document.getElementById('change-group-list');
    list.innerHTML = '';

    // Gather candidate groups: every distinct group name from QE_CATEGORIES
    // for this category code, plus any role_group values already in use on
    // existing rows (in case admin added custom groups).
    const fromQE = new Set();
    const _cat = (QE_CATEGORIES || []).find(c => c.code === _menuCode);
    if (_cat) (_cat.items || []).forEach(it => { if (it.group) fromQE.add(it.group); });
    document.querySelectorAll(`.line-row[data-code="${_menuCode}"]`).forEach(r => {
      // data-group attr isn't rendered today; skip — QE list is canonical.
    });
    const groups = Array.from(fromQE).sort();

    if (groups.length === 0) {
      list.innerHTML = '<div class="muted" style="padding:8px">No sub-groups defined for this section.</div>';
    } else {
      groups.forEach(g => {
        const b = document.createElement('button');
        b.type = 'button';
        b.className = 'btn btn-sm btn-outline';
        b.style.cssText = 'text-align:left;width:100%';
        b.textContent = g;
        b.onclick = () => applyChangeGroup(g);
        list.appendChild(b);
      });
    }
    modal.classList.remove('hidden');
  };

  window.closeChangeGroupModal = function() {
    document.getElementById('change-group-modal').classList.add('hidden');
  };

  async function applyChangeGroup(group) {
    if (!_menuLineId) return;
    const res = await fetch(`/projects/${_pid}/budget/${_bid}/line/${_menuLineId}/set-group`, {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ role_group: group })
    });
    if (res.ok) {
      closeChangeGroupModal();
      reloadWithTab();
    } else {
      alert('Change group failed');
    }
  }

  // Close on outside click
  document.addEventListener('click', e => {
    if (!e.target.closest('#line-row-menu') && !e.target.closest('#line-add-menu')
        && !e.target.closest('.line-row-menu-btn') && !e.target.closest('.line-add-btn')) {
      _closeMenus(false);
    }
  });

  window.lineInsert = async function(position, kind) {
    _closeMenus(false);
    if (!_menuLineId) return;
    let initialDescription = '';
    // Header: prompt for the label up front so empty placeholders never
    // get persisted in the DB (per user 2026-05-04: "make sure that the
    // temp text never appears if no one uses it"). Cancel or blank
    // aborts the insert entirely.
    if (kind === 'header') {
      const label = prompt('Sub-header label (e.g. "Pre-production", "On-set crew"):', '');
      if (label === null) return;  // canceled
      const trimmed = String(label).trim();
      if (!trimmed) return;        // blank — same as cancel
      initialDescription = trimmed.slice(0, 200);
    }
    const res = await fetch(`/projects/${_pid}/budget/${_bid}/line/insert`, {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        reference_id: _menuLineId,
        position,
        line_kind: kind || 'normal',
        description: initialDescription,
      })
    });
    if (res.ok) reloadWithTab();
    else alert('Insert failed');
  };

  window.lineDuplicate = async function() {
    _closeMenus(false);
    if (!_menuLineId) return;
    // Only labor and location lines have schedules worth asking about.
    // Equipment / expense / admin lines get duplicated silently — there's
    // no schedule to copy or skip, so no dialog.
    const row = document.querySelector(`.line-row[data-id="${_menuLineId}"]`);
    const isLabor = !!(row && row.classList.contains('labor-line'));
    const codeAttr = row && row.dataset && row.dataset.code;
    const isLocation = (parseInt(codeAttr || '0') === 3300);
    const promptsSchedule = isLabor || isLocation;

    let dupeSched = false;
    if (promptsSchedule) {
      // In-page Yes / No / Cancel modal (was window.confirm; per user
      // 2026-05-28 the native popup "feels like an error" because it
      // renders as browser chrome at the top of the window).
      const modal = document.getElementById('line-duplicate-confirm-modal');
      if (!modal) {
        // Defensive fallback — shouldn't happen, but don't strand the click.
        dupeSched = window.confirm('Also duplicate the schedule days?');
      } else {
        const choice = await new Promise((resolve) => {
          window._lineDupResolve = (val) => {
            modal.classList.add('hidden');
            window._lineDupResolve = null;
            resolve(val);
          };
          modal.classList.remove('hidden');
        });
        if (choice === null) return;   // user cancelled — no duplicate at all
        dupeSched = !!choice;
      }
    }

    const srcId = _menuLineId;
    // If the source was schedule-driven and the user picked "No", the
    // duplicate lands in a different mode (use_schedule=false, no days,
    // not checked in the schedule grid). The in-place clone would carry
    // the .schedule-driven class + checked grid boxes from the source,
    // so force a full reload in that case to pull faithful server state.
    const srcWasSched = !!(row && row.classList.contains('schedule-driven'));
    const modeChanged = promptsSchedule && srcWasSched && !dupeSched;

    const res = await fetch(`/projects/${_pid}/budget/${_bid}/line/${srcId}/duplicate`, {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ duplicate_schedule: !!dupeSched })
    });
    if (res.ok) {
      if (modeChanged) { reloadWithTab(); return; }
      // In-place insert (per user 2026-05-04: avoid the full-page
      // reload "jump" feel). Clone the source row's DOM, retarget all
      // data-id refs to the new line id, drop in immediately after the
      // source, flash green so the user sees the new row land. Most
      // interactive handlers (edits, selects, drag-drop) use document-
      // level event delegation so the clone wires up automatically.
      // If anything looks off, a manual refresh restores the canonical
      // server-rendered state.
      try {
        const j = await res.clone().json().catch(() => null);
        const newId = j && j.id;
        const srcRow = document.querySelector(`.line-row[data-id="${srcId}"]`);
        if (newId && srcRow) {
          const clone = srcRow.cloneNode(true);
          clone.setAttribute('data-id', String(newId));
          // Retarget every data-id attribute on inner controls so per-
          // row server calls (rate edit, line/<id>/* endpoints) hit
          // the new row's id, not the source's.
          clone.querySelectorAll('[data-id]').forEach(el => {
            if (el.getAttribute('data-id') === String(srcId)) {
              el.setAttribute('data-id', String(newId));
            }
          });
          // cloneNode(true) doesn't carry live form state (select.value,
          // input.value) — it copies the DOM tree only. So a select the
          // user changed from "Days" to "Weeks" reverts to the default
          // option in the clone. Walk paired controls and mirror state.
          // Per user 2026-05-04: every element should duplicate, including
          // the Type column dropdown and any other form controls.
          const srcSelects = srcRow.querySelectorAll('select');
          const dstSelects = clone.querySelectorAll('select');
          srcSelects.forEach((s, i) => {
            const d = dstSelects[i];
            if (d) {
              d.value = s.value;
              if (d.dataset && s.dataset && s.dataset.unit) d.dataset.unit = s.dataset.unit;
            }
          });
          const srcInputs = srcRow.querySelectorAll('input, textarea');
          const dstInputs = clone.querySelectorAll('input, textarea');
          srcInputs.forEach((s, i) => {
            const d = dstInputs[i];
            if (d) {
              d.value = s.value;
              if (s.type === 'checkbox' || s.type === 'radio') d.checked = s.checked;
            }
          });
          // Don't carry expand-state from a possibly-open source row.
          clone.style.background = 'rgba(34,197,94,.12)';
          srcRow.parentNode.insertBefore(clone, srcRow.nextSibling);
          // Fade the highlight back out.
          setTimeout(() => { clone.style.transition = 'background .8s ease'; clone.style.background = ''; }, 50);
          // Bump the line-number text for the clone + every row after
          // it in the same section so the visual numbering stays
          // sequential without a full reload.
          const code = srcRow.dataset.code;
          if (code) {
            const peers = Array.from(document.querySelectorAll(`.line-row[data-code="${code}"]`));
            peers.forEach((row, i) => {
              const ln = row.querySelector('.line-number');
              if (ln) {
                const padded = String(i + 1).padStart(2, '0');
                ln.textContent = `${code}-${padded}`;
              }
            });
          }
          return;
        }
      } catch (e) { /* fall through to reload */ }
      reloadWithTab();
    }
    else {
      let msg = 'Duplicate failed';
      try { const j = await res.json(); if (j && j.error) msg += ': ' + j.error; } catch(e){}
      alert(msg);
    }
  };

  // Open the per-line Reconcile panel (tiles + merge/unlink/uncode) for the
  // line the context menu is targeting. (User 2026-06-26.)
  window.reconcileMenuLine = function() {
    _closeMenus(false);
    if (!_menuLineId) return;
    if (typeof window.openReconcileLine === 'function') window.openReconcileLine(_menuLineId);
  };

  window.lineLedgerMenuLine = function() {
    _closeMenus(false);
    if (!_menuLineId) return;
    if (typeof window.openLineLedger === 'function') window.openLineLedger(_menuLineId);
  };

  // 🚩 Discussion note — flag this row as a talking point (owner
  // 2026-09-10: "highlight a specific row… this is a discussion row…
  // attach a note"). editDiscussionNote lives in the budget.html inline
  // block (needs patchLine).
  window.discussionNoteMenuLine = function() {
    _closeMenus(false);
    if (!_menuLineId) return;
    if (typeof window.editDiscussionNote === 'function') window.editDiscussionNote(_menuLineId);
  };

  // ⋯ → Flag → Lever: build a lever on this row (title = the row, delta =
  // −line total, scope = its discussion note) through the existing
  // levers/save endpoint, then show the Lever chip. A row that already
  // carries a lever opens Assumptions to edit it. (Cleanup C.)
  window.leverMenuLine = async function() {
    _closeMenus(false);
    if (!_menuLineId) return;
    const row = document.querySelector(`.line-row[data-id="${_menuLineId}"]`);
    if (!row) return;
    if (row.querySelector('.line-chip-lever')) {
      document.querySelector('.tab-btn[data-tab="assumptions"]')?.click();
      return;
    }
    const desc = row.querySelector('.editable[data-field="description"]');
    const title = (desc && desc.textContent.trim()) || ('Line ' + _menuLineId);
    const totalCell = row.querySelector('.line-total-cell, .col-working');
    let amt = 0;
    try {
      const raw = row.dataset.subtotal || (totalCell && totalCell.textContent) || '0';
      amt = parseFloat(String(raw).replace(/[^0-9.\-]/g, '')) || 0;
    } catch (e) {}
    const payload = { title, delta_lines: -Math.abs(amt), fee_applied: true,
                      scope: row.dataset.discussion || '', consequence: '', line_ids: [_menuLineId] };
    const r = await fetch(`/projects/${_pid}/budget/${_bid}/levers/save`, {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)});
    const j = await r.json().catch(() => ({}));
    if (!(r.ok && j.ok)) { alert(j.error || 'Could not create the lever'); return; }
    const host = row.querySelector('.col-desc');
    const flag = host && host.querySelector('.discussion-flag');
    if (host) {
      const chip = document.createElement('span');
      chip.className = 'line-chip line-chip-lever';
      chip.setAttribute('role', 'button'); chip.tabIndex = 0;
      chip.title = 'A lever is built on this line — open Assumptions to edit it';
      chip.innerHTML = '<svg aria-hidden="true" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 21v-7M4 10V3M12 21v-9M12 8V3M20 21v-5M20 12V3M1 14h6M9 8h6M17 16h6"></path></svg>Lever';
      chip.onclick = () => document.querySelector('.tab-btn[data-tab="assumptions"]')?.click();
      if (flag && flag.nextSibling) host.insertBefore(chip, flag.nextSibling); else host.appendChild(chip);
    }
    const mb = row.querySelector('.line-row-menu-btn'); if (mb) mb.dataset.hasLever = '1';
    if (typeof _lineSaveToast === 'function') _lineSaveToast('✓ Lever created — adjust the delta or wording in Assumptions.');
    if (typeof loadLevers === 'function') loadLevers();
  };

  // ⋯ → Note → Internal note: the line's comment drawer (existing feature).
  window.noteMenuLine = function() {
    _closeMenus(false);
    if (!_menuLineId) return;
    const row = document.querySelector(`.line-row[data-id="${_menuLineId}"]`);
    const desc = row && row.querySelector('.editable[data-field="description"]');
    const label = (desc && desc.textContent.trim()) || 'line';
    if (typeof window.openComments === 'function') window.openComments(_menuLineId, label.slice(0, 40));
  };

  // ⋯ → Line → Attach PO: the existing PO picker.
  window.attachPoMenuLine = function() {
    _closeMenus(false);
    if (!_menuLineId) return;
    if (typeof window.openLinePoPicker === 'function') window.openLinePoPicker(_menuLineId);
  };

  // ⋯ → Line → Add to sub-budget: the existing multi-select picker.
  window.subBudgetMenuLine = function() {
    _closeMenus(false);
    if (!_menuLineId) return;
    const row = document.querySelector(`.line-row[data-id="${_menuLineId}"]`);
    const badge = row && row.querySelector('.line-sb-badge');
    if (badge && typeof window.openLineSubBudgetPicker === 'function') window.openLineSubBudgetPicker(badge);
  };

  // ⋯ → Line → Delete line (with confirm; the same flow as before).
  window.deleteMenuLine = function() {
    _closeMenus(false);
    if (!_menuLineId) return;
    const row = document.querySelector(`.line-row[data-id="${_menuLineId}"]`);
    const desc = row && row.querySelector('.editable[data-field="description"]');
    const label = (desc && desc.textContent.trim()) || '';
    if (typeof window.deleteLineWithConfirm === 'function') window.deleteLineWithConfirm(_menuLineId, label);
  };

  window.openLineMoveModal = function() {
    _closeMenus(false);
    if (!_menuLineId || !_menuCode) return;

    // Build the Section selector — every COA section that's present on
    // this budget page (so you can move across sections per user
    // 2026-05-04 — e.g. Camera Equipment → Grip & Electric).
    const secSel = document.getElementById('line-move-section-select');
    if (secSel && !secSel.dataset.populated) {
      const seen = new Set();
      const opts = [];
      document.querySelectorAll('.section-block').forEach(block => {
        const code = block.dataset.section;
        if (!code || seen.has(code)) return;
        seen.add(code);
        // Pull the readable name from the section header.
        const nameEl = block.querySelector('.section-name');
        const name = (nameEl && nameEl.textContent.trim()) || code;
        opts.push({ code, name });
      });
      // Keep server-side COA order (already enforced by sections list).
      secSel.innerHTML = opts.map(o =>
        `<option value="${o.code}">${o.code} — ${o.name}</option>`).join('');
      secSel.dataset.populated = '1';
    }
    if (secSel) secSel.value = String(_menuCode);

    _renderMoveList(secSel ? secSel.value : String(_menuCode));
    document.getElementById('line-move-modal').classList.remove('hidden');
  };

  function _renderMoveList(sectionCode) {
    const list = document.getElementById('line-move-list');
    if (!list) return;
    list.innerHTML = '';
    // "Move to top of section" — works for both same-section and cross-
    // section moves (the latter passes target_section_code along).
    const topBtn = document.createElement('button');
    topBtn.className = 'btn btn-sm btn-outline';
    topBtn.style.cssText = 'text-align:left;width:100%';
    const isCross = String(sectionCode) !== String(_menuCode);
    topBtn.textContent = isCross
      ? `↑ Move to top of section ${sectionCode}`
      : '↑ Move to top of section';
    topBtn.onclick = () => doMove(null, isCross ? sectionCode : null);
    list.appendChild(topBtn);

    const rows = Array.from(document.querySelectorAll(`.line-row[data-code="${sectionCode}"]`));
    rows.forEach(row => {
      const id = parseInt(row.dataset.id);
      if (id === _menuLineId) return;  // skip self when same-section
      const desc = row.querySelector('.editable[data-field="description"]');
      const label = (desc && desc.textContent.trim()) || `Line #${id}`;
      const btn = document.createElement('button');
      btn.className = 'btn btn-sm btn-outline';
      btn.style.cssText = 'text-align:left;width:100%';
      btn.textContent = `After: ${label}`;
      btn.onclick = () => doMove(id, isCross ? sectionCode : null);
      list.appendChild(btn);
    });
  }

  // Re-render the row list when the user changes the target section.
  document.addEventListener('change', e => {
    if (e.target && e.target.id === 'line-move-section-select') {
      _renderMoveList(e.target.value);
    }
  });

  window.closeLineMoveModal = function() {
    document.getElementById('line-move-modal').classList.add('hidden');
  };

  async function doMove(afterId, targetSectionCode) {
    closeLineMoveModal();
    const body = { line_id: _menuLineId, after_id: afterId };
    if (targetSectionCode) body.target_section_code = targetSectionCode;
    const res = await fetch(`/projects/${_pid}/budget/${_bid}/line/reorder`, {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify(body)
    });
    if (res.ok) reloadWithTab();
    else alert('Move failed');
  }
})();
