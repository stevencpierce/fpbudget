// Extracted from a templates/budget.html inline block (audit M9, 2026-07-20).
// Jinja values arrive via window.__BJ (set by the inline preamble before this
// script tag). Classic script, document order — semantics identical to inline.

// ── The ONE row menu (2026-11-03) ───────────────────────────────────────────
// Every budget row — section header, sub-header, spacer, line — has a single
// "⋯" button and a single menu, #line-row-menu. Right-click (or a long press
// on touch) on a row opens the same menu at the cursor. The former "+" menu,
// the labor right-click menu (b06) and the schedule-line right-click menu
// (b07) are folded in here; items show / hide by row kind:
//   .line-menu-line-only     plain lines only (not header / spacer / section)
//   .line-menu-has-line      rows backed by a BudgetLine (not the section header)
//   .line-menu-section-only  the section header only
//   .line-menu-header-only   sub-headers only
//   .line-menu-labor-only    labor lines only
//   .line-menu-sync-only     schedule-driven (auto-calc) non-labor lines only
(function() {
  const _pid = window.__BJ["b12__pid"];
  const _bid = window.__BJ["b12__bid"];

  let _menuLineId  = null;   // the BudgetLine the menu targets (null on a section header)
  let _menuCode    = null;   // its account section code
  let _menuKind    = 'line'; // 'line' | 'header' | 'spacer' | 'section'
  let _menuSecName = '';     // the section's name (section-header menu → Add line modal)
  let _menuSecCode = null;   // the COA section block the row sits in (.section-block[data-section])

  // Lets another module target a row before calling the shared helpers
  // (lineInsert / lineDuplicate / openLineMoveModal).
  window._setLineMenuTarget = function(id, code) {
    _menuLineId = parseInt(id) || null;
    _menuCode   = (code !== undefined && code !== null && code !== '') ? parseInt(code) : null;
  };
  const _menu = document.getElementById('line-row-menu');
  let _menuOpener = null;   // the button that opened the current menu (focus returns here)

  function _visibleItems() {
    return Array.from(_menu.querySelectorAll('[role="menuitem"]')).filter(el => el.offsetParent !== null);
  }
  // Position the menu under its button, right-aligned to it, clamped to the
  // viewport; or at a point (right-click / long press).
  function _placeMenu(btn, at) {
    requestAnimationFrame(() => {
      const mw = _menu.offsetWidth;
      const mh = _menu.offsetHeight;
      let top, left;
      if (at) {
        top  = Math.min(at.y, window.innerHeight - mh - 8);
        left = Math.min(at.x, window.innerWidth - mw - 8);
      } else {
        const rect = btn.getBoundingClientRect();
        top  = Math.min(rect.bottom + 4, window.innerHeight - mh - 8);
        left = Math.max(4, Math.min(rect.right - mw, window.innerWidth - mw - 8));
      }
      _menu.style.top  = Math.max(4, top) + 'px';
      _menu.style.left = Math.max(4, left) + 'px';
      const first = _visibleItems()[0];
      if (first && !at) first.focus();
    });
  }
  function _closeMenus(returnFocus) {
    const wasOpen = !_menu.classList.contains('hidden');
    _menu.classList.add('hidden');
    document.querySelectorAll('.line-act-btn[aria-expanded="true"]').forEach(b => b.setAttribute('aria-expanded', 'false'));
    if (returnFocus && wasOpen && _menuOpener) { try { _menuOpener.focus(); } catch (e) {} }
  }
  window._closeLineMenus = _closeMenus;

  // Show / hide the menu's items for one row kind. `row` is the <tr> (or
  // the .section-header div for a section).
  function _applyKind(kind, row, btn) {
    const isLine    = (kind === 'line');
    const hasLine   = (kind !== 'section');
    const isLabor   = isLine && !!(row && row.classList.contains('labor-line'));
    const isSync    = isLine && !isLabor && !!(row && row.classList.contains('sync-line'));
    const show = (sel, on) => _menu.querySelectorAll(sel).forEach(el => { el.style.display = on ? '' : 'none'; });
    show('.line-menu-line-only',    isLine);
    show('.line-menu-has-line',     hasLine);
    show('.line-menu-section-only', kind === 'section');
    show('.line-menu-header-only',  kind === 'header');
    show('.line-menu-labor-only',   isLabor);
    show('.line-menu-sync-only',    isSync);
    // Per-row wording.
    const cgBtn = document.getElementById('line-menu-change-group-btn');
    if (cgBtn) cgBtn.style.display = (_menuCode === 2000 && isLine) ? '' : 'none';
    const poLbl = document.getElementById('line-menu-po-label');
    if (poLbl && btn) poLbl.textContent = (btn.dataset.hasPo === '1') ? 'Change PO…' : 'Attach PO';
    const lvBtn = document.getElementById('line-menu-lever-btn');
    if (lvBtn && btn) lvBtn.querySelector('.line-menu-sub').textContent =
      (btn.dataset.hasLever === '1') ? 'Already a lever — open Assumptions to edit' : 'A cost the client can choose';
    const syncLbl = document.getElementById('line-menu-sync-label');
    if (syncLbl && row) syncLbl.textContent =
      (row.dataset.syncOmit === '1') ? 'Re-enable auto-calc' : 'Omit from auto-calc';
    const dupLbl = document.getElementById('line-menu-dup-label');
    if (dupLbl) dupLbl.textContent =
      kind === 'header' ? 'Duplicate sub-header + its lines'
      : kind === 'section' ? 'Duplicate section'
      : kind === 'spacer' ? 'Duplicate spacer' : 'Duplicate line';
    const delLbl = document.getElementById('line-menu-delete-label');
    if (delLbl) delLbl.textContent =
      kind === 'header' ? 'Delete sub-header' : kind === 'spacer' ? 'Delete spacer' : 'Delete line';
    // A section with no rows yet has nothing to insert next to / duplicate.
    const emptySection = (kind === 'section' && !_menuLineId);
    show('.line-menu-needs-row', !emptySection);
    // A group whose every item is hidden hides with them (its label too).
    _menu.querySelectorAll('.line-menu-group').forEach(g => {
      const any = Array.from(g.querySelectorAll('[role="menuitem"]')).some(el => el.style.display !== 'none');
      g.style.display = any ? '' : 'none';
    });
  }

  // Open the row menu from its "⋯" button (or at a point for right-click).
  window.openLineRowMenu = function(btn, at) {
    _closeMenus(false);
    const row  = btn.closest('tr.line-row') || btn.closest('.section-header');
    const kind = btn.dataset.kind || (row && row.dataset.tag) || 'line';
    _menuKind    = kind;
    _menuLineId  = parseInt(btn.dataset.id) || null;
    _menuCode    = parseInt(btn.dataset.code) || null;
    _menuSecName = btn.dataset.secName || '';
    const block  = btn.closest('.section-block');
    _menuSecCode = (block && parseInt(block.dataset.section)) || _menuCode;
    _menuOpener  = btn;
    _menu.dataset.kind = kind;
    _applyKind(kind, row, btn);
    _menu.classList.remove('hidden');
    btn.setAttribute('aria-expanded', 'true');
    _placeMenu(btn, at);
  };

  // Esc closes the menu and returns focus to its button; arrow keys walk
  // the items.
  document.addEventListener('keydown', e => {
    if (_menu.classList.contains('hidden')) return;
    if (e.key === 'Escape') { e.preventDefault(); _closeMenus(true); return; }
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      const items = _visibleItems();
      if (!items.length) return;
      e.preventDefault();
      const i = items.indexOf(document.activeElement);
      const next = e.key === 'ArrowDown' ? items[(i + 1) % items.length] : items[(i - 1 + items.length) % items.length];
      next.focus();
    }
  });

  // Right-click on any row (line, sub-header, spacer, section header) opens
  // the same menu at the cursor. Cell-level pickers (the Duration unit
  // button) and kit-fee child rows keep their own behaviour.
  function _rowMenuButtonFor(target) {
    if (target.closest('.days-unit-toggle') || target.closest('#line-row-menu')) return null;
    const row = target.closest('tr.line-row') || target.closest('.section-header');
    if (!row) return null;
    if (row.classList && row.classList.contains('kit-fee-row')) return null;
    if (row.classList && row.classList.contains('work-section-only-row')) return null;
    return row.querySelector('.line-row-menu-btn');
  }
  document.addEventListener('contextmenu', function(e) {
    const btn = _rowMenuButtonFor(e.target);
    if (!btn) return;
    e.preventDefault();
    window.openLineRowMenu(btn, { x: e.clientX, y: e.clientY });
  });
  // Long-press (500ms) on touch devices — the same menu without a right-click.
  let _lpTimer = null;
  document.addEventListener('touchstart', function(e) {
    if (e.target.closest('.editable') || e.target.closest('select') || e.target.closest('input')) return;
    const btn = _rowMenuButtonFor(e.target);
    if (!btn) return;
    const t = e.touches[0];
    const at = { x: t.clientX, y: t.clientY };
    _lpTimer = setTimeout(() => { window.openLineRowMenu(btn, at); }, 500);
  }, { passive: true });
  document.addEventListener('touchend',  () => { clearTimeout(_lpTimer); _lpTimer = null; });
  document.addEventListener('touchmove', () => { clearTimeout(_lpTimer); _lpTimer = null; });

  // Close on outside click
  document.addEventListener('click', e => {
    if (!e.target.closest('#line-row-menu') && !e.target.closest('.line-row-menu-btn')) {
      _closeMenus(false);
    }
  });

  // ── Add ───────────────────────────────────────────────────────────────
  // Section header → Add → Line: the section's add-line modal.
  window.addLineMenuSection = function() {
    _closeMenus(false);
    if (!_menuCode) return;
    if (typeof window.addLineInSection === 'function') window.addLineInSection(_menuCode, _menuSecName);
  };

  // Insert next to the targeted row. From the section header the reference
  // row is the section's first row and the new row goes ABOVE it (top of
  // the section).
  window.lineInsert = async function(position, kind) {
    _closeMenus(false);
    if (!_menuLineId) return;
    if (_menuKind === 'section') position = 'above';
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

  // ── Fees (labor rows) / auto-calc (schedule-driven rows) ─────────────
  window.feeMenuLine = function(tag) {
    _closeMenus(false);
    if (!_menuLineId) return;
    if (tag === 'agent_fee') {
      if (typeof window.openAgentFeeModalFor === 'function') window.openAgentFeeModalFor(_menuLineId);
    } else if (typeof window.openFeeModalFor === 'function') {
      window.openFeeModalFor(_menuLineId, tag);
    }
  };
  window.syncOmitMenuLine = function() {
    _closeMenus(false);
    if (!_menuLineId) return;
    if (typeof window.toggleSyncOmitFor === 'function') window.toggleSyncOmitFor(_menuLineId);
  };

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

  // The rows a duplicate would copy, from the DOM — used to decide whether
  // to ask about schedule days. 'line' → the row; 'block' → a sub-header +
  // the rows under it down to the next sub-header / spacer; 'section' →
  // every row of the section.
  function _dupRows(scope) {
    const row = document.querySelector(`.line-row[data-id="${_menuLineId}"]`);
    if (scope === 'section') {
      return Array.from(document.querySelectorAll(`.section-block[data-section="${_menuSecCode}"] tr.line-row`));
    }
    if (!row) return [];
    if (scope !== 'block') return [row];
    const out = [row];
    let r = row.nextElementSibling;
    while (r) {
      if (r.classList.contains('line-row')) {
        if (r.classList.contains('header-row') || r.classList.contains('spacer-row')) break;
        out.push(r);
      }
      r = r.nextElementSibling;
    }
    return out;
  }

  // Duplicate the targeted row (scope 'line'), a sub-header and its lines
  // ('block'), or the whole section ('section') — one endpoint, one
  // transaction server-side. Internal notes copy; Discussion / Lever
  // flags do not.
  window.lineDuplicate = async function(scope) {
    _closeMenus(false);
    if (!_menuLineId) return;
    scope = scope || (_menuKind === 'header' ? 'block' : _menuKind === 'section' ? 'section' : 'line');
    const rows = _dupRows(scope);
    // Only labor and location lines have schedules worth asking about.
    // Equipment / expense / admin lines get duplicated silently — there's
    // no schedule to copy or skip, so no dialog.
    const promptsSchedule = rows.some(r =>
      r.classList.contains('labor-line') || parseInt(r.dataset.code || '0') === 3300);

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
        const title = document.getElementById('line-duplicate-confirm-title');
        if (title) title.textContent =
          scope === 'block' ? 'Duplicate Sub-header + Lines' : scope === 'section' ? 'Duplicate Section' : 'Duplicate Line';
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
    const row = rows[0];
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
      body: JSON.stringify({ duplicate_schedule: !!dupeSched, scope })
    });
    if (res.ok) {
      // Block / section copies: several rows, re-seated by the server —
      // reload on the section so the numbering and totals are canonical.
      if (scope !== 'line' || modeChanged) { reloadWithTab(_menuSecCode || undefined); return; }
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
          // Flags do not copy: drop the Discussion / Lever chips and the
          // discussion text from the clone (the server left them off).
          clone.classList.remove('discussion-row');
          clone.dataset.discussion = '';
          clone.querySelectorAll('.line-chip-lever, .discussion-note-text').forEach(el => el.remove());
          const dflag = clone.querySelector('.discussion-flag');
          if (dflag) dflag.style.display = 'none';
          const mb = clone.querySelector('.line-row-menu-btn');
          if (mb) { mb.dataset.hasLever = '0'; mb.dataset.id = String(newId); }
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
    const label = (desc && desc.textContent.trim())
      || (_menuKind === 'header' ? 'sub-header' : _menuKind === 'spacer' ? 'spacer' : 'line');
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
