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

  window.openLineRowMenu = function(btn) {
    _menuLineId = parseInt(btn.dataset.id);
    _menuCode   = parseInt(btn.dataset.code);
    _menu.classList.remove('hidden');
    // Show the "Change Group…" option only for sections that use role_group
    // sub-headers (currently just 2000 Production Staff). Keeps the menu
    // compact for sections where sub-grouping isn't in use.
    const cgBtn = document.getElementById('line-menu-change-group-btn');
    if (cgBtn) cgBtn.style.display = (_menuCode === 2000) ? '' : 'none';
    // Position with fixed coords (viewport-relative)
    requestAnimationFrame(() => {
      const rect = btn.getBoundingClientRect();
      const mw = _menu.offsetWidth;
      const mh = _menu.offsetHeight;
      _menu.style.top  = Math.min(rect.bottom + 4, window.innerHeight - mh - 8) + 'px';
      _menu.style.left = Math.max(4, Math.min(rect.left, window.innerWidth - mw - 8)) + 'px';
    });
  };

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
    _menuLineId = id;
    _menuCode   = code;
    _menu.classList.remove('hidden');
    const cgBtn = document.getElementById('line-menu-change-group-btn');
    if (cgBtn) cgBtn.style.display = (code === 2000) ? '' : 'none';
    requestAnimationFrame(() => {
      const mw = _menu.offsetWidth;
      const mh = _menu.offsetHeight;
      _menu.style.top  = Math.min(e.clientY, window.innerHeight - mh - 8) + 'px';
      _menu.style.left = Math.min(e.clientX, window.innerWidth - mw - 8) + 'px';
    });
  });

  window.openChangeGroupModal = function() {
    _menu.classList.add('hidden');
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
    if (!e.target.closest('#line-row-menu') && !e.target.closest('.line-row-menu-btn')) {
      _menu.classList.add('hidden');
    }
  });

  window.lineInsert = async function(position, kind) {
    _menu.classList.add('hidden');
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
    _menu.classList.add('hidden');
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
    _menu.classList.add('hidden');
    if (!_menuLineId) return;
    if (typeof window.openReconcileLine === 'function') window.openReconcileLine(_menuLineId);
  };

  window.lineLedgerMenuLine = function() {
    _menu.classList.add('hidden');
    if (!_menuLineId) return;
    if (typeof window.openLineLedger === 'function') window.openLineLedger(_menuLineId);
  };

  window.openLineMoveModal = function() {
    _menu.classList.add('hidden');
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
