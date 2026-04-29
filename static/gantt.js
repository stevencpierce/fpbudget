/**
 * FPBudget Gantt controller
 *
 * Primary click:  single-click toggles Work ↔ Off  (fast path)
 * Secondary:      right-click opens context menu for Travel/Hold/Half/Kill/OT/Notes/Flags
 * Select mode:    Shift key toggles on/off; Cmd/Ctrl+click also enters it
 *                 In select mode: click to select cells, Cmd+C copy, Cmd+V paste
 */

// ── Primary click cycle: Work ↔ Off only (fast path) ──────────────────────────
const DAY_CYCLE = ['work', 'off'];

// All known day types (for class-cleanup purposes)
const ALL_DAY_TYPES = ['work', 'travel', 'travel_half', 'travel_unpaid', 'hold', 'half', 'kill_fee', 'off', 'custom'];

let _pid, _bid, _activeProfileId;
let _dragging   = false;
let _dragType   = null;        // legacy paint-mode (kept for compatibility paths)
let _dragAnchor = null;        // anchor cell for drag-select rectangle

// ── Select Mode ───────────────────────────────────────────────────────────────
let _selectMode   = false;
let _selection    = new Set();   // Set of "lineId:instance:date" strings
let _lastClickedCell = null;     // for shift-range selection
let _clipboard    = null;        // { grid, rows, cols } — NOT cleared on exit

// Prevents handleCellClick from double-toggling after mousedown already acted
let _mousedownDidAct = false;

// ── Undo Stack ────────────────────────────────────────────────────────────────
const _undoStack = [];
const MAX_UNDO   = 50;

// ── Scroll persistence ────────────────────────────────────────────────────────
function _scrollKey()     { return `gantt_scrollX_${_bid}`; }
function _rangeKey()      { return `gantt_range_${_bid}`; }

// ─────────────────────────────────────────────────────────────────────────────
// INIT
// ─────────────────────────────────────────────────────────────────────────────
function initGantt(pid, bid, activeProfileId) {
  _pid             = pid;
  _bid             = bid;
  _activeProfileId = activeProfileId;

  // Restore horizontal scroll
  const wrap = document.getElementById('gantt-scroll-wrap');
  if (wrap) {
    const saved = sessionStorage.getItem(_scrollKey());
    if (saved) wrap.scrollLeft = parseInt(saved, 10);
    wrap.addEventListener('scroll', () => {
      sessionStorage.setItem(_scrollKey(), wrap.scrollLeft);
    }, { passive: true });
  }

  // Restore last date range if the page loaded without URL params (navigated back)
  const urlParams = new URL(window.location.href).searchParams;
  if (!urlParams.has('gantt_start') && !urlParams.has('gantt_end')) {
    const savedRange = sessionStorage.getItem(_rangeKey());
    if (savedRange) {
      try {
        const { start, end } = JSON.parse(savedRange);
        if (start && end) {
          const url = new URL(window.location.href);
          url.searchParams.set('gantt_start', start);
          url.searchParams.set('gantt_end', end);
          window.location.replace(url.toString());
          return; // stop init — page will reload with the saved range
        }
      } catch (_) {}
    }
  }
  // Save current range to sessionStorage (covers direct loads with URL params)
  if (urlParams.has('gantt_start')) {
    sessionStorage.setItem(_rangeKey(), JSON.stringify({
      start: urlParams.get('gantt_start'),
      end:   urlParams.get('gantt_end'),
    }));
  }

  // Right-click → context menu via event delegation (covers dynamically added cells too)
  document.addEventListener('contextmenu', e => {
    const cell = e.target.closest('.gantt-cell');
    if (!cell) return;
    e.preventDefault();
    // If the right-click landed on a cell that's part of a multi-cell
    // selection, show the bulk action menu. Otherwise show the
    // single-cell picker for that cell.
    if (_selection.size > 1 && _selection.has(cellKey(cell))) {
      showSelectActionMenu(e);
    } else {
      showPicker(e, cell);
    }
  });

  // ── Inject ▾ dropdown button into each cell ──────────────────────────────
  // Per user 2026-04-28 redesign: cell body click = select (Excel-style),
  // dropdown button click = open picker. Two distinct interaction targets
  // so the cell is safe to click for selection/copy without accidentally
  // editing day type — especially important on iPad/iPhone where touch
  // gestures were too sensitive before.
  function _injectCellDropdowns() {
    document.querySelectorAll('.gantt-cell').forEach(cell => {
      if (cell.querySelector('.gantt-cell-dropdown')) return;
      // Skip cells that aren't editable day cells (e.g. meal-row cells use
      // data-meal, not the standard line/instance/date triplet — those
      // already have their own click behavior via data-meal attr).
      if (cell.dataset.meal) return;
      if (!cell.dataset.line || !cell.dataset.date) return;
      const btn = document.createElement('button');
      btn.className = 'gantt-cell-dropdown';
      btn.type = 'button';
      btn.tabIndex = -1;
      btn.title = 'Open day-type / flag picker';
      btn.innerHTML = '▾';
      cell.appendChild(btn);
    });
  }
  _injectCellDropdowns();
  // Re-inject if the gantt re-renders (e.g. after zoom or date-range change).
  // MutationObserver on the scroll wrap watches for new .gantt-cell children.
  const _gantWrap = document.getElementById('gantt-scroll-wrap');
  if (_gantWrap) {
    new MutationObserver(_injectCellDropdowns).observe(_gantWrap, {
      childList: true, subtree: true,
    });
  }

  // ── Close crew picker on outside mousedown ────────────────────────────────
  document.addEventListener('mousedown', e => {
    if (!e.target.closest('#crew-picker-popover') &&
        !e.target.closest('.gantt-crew-chip')) {
      closeCrewPicker();
    }
  }, true);  // capture phase so it fires before any element handlers

  // ── Mousedown: selection-only model (rewritten 2026-04-28) ──────────────
  // Plain click on cell body = highlight that one cell, clear prior
  // selection. Shift+click = rectangle select from anchor to clicked cell
  // (Excel-style). Cmd/Ctrl+click = toggle add to selection. Click on the
  // ▾ dropdown button = open the picker (no selection). Drag from cell
  // body = drag-select rectangle (no painting). Right-click still opens
  // the picker as a parallel route.
  document.addEventListener('mousedown', e => {
    if (e.button !== 0) return;
    const dropdown = e.target.closest('.gantt-cell-dropdown');
    const cell = e.target.closest('.gantt-cell');
    if (!cell) return;

    if (dropdown) {
      // Dropdown button clicked → open picker for this cell, don't select.
      e.stopPropagation();
      e.preventDefault();
      showPicker(e, cell);
      _mousedownDidAct = true;
      return;
    }
    // Skip non-editable meal-row cells (their own click handler runs)
    if (cell.dataset.meal) return;

    const isMac = navigator.platform.toUpperCase().includes('MAC');
    const ctrl  = isMac ? e.metaKey : e.ctrlKey;

    if (e.shiftKey && _lastClickedCell) {
      // Range-select from anchor to this cell. Doesn't clear prior
      // selection so you can extend after a Cmd-click pick.
      _selectRectangle(_lastClickedCell, cell);
    } else if (ctrl) {
      // Toggle add/remove. Anchor moves to this cell.
      toggleSelectCell(cell);
    } else {
      // Plain click = single-cell select. Replace any prior selection.
      clearSelection();
      toggleSelectCell(cell, true);
    }
    _dragging   = true;
    _dragAnchor = cell;
    _mousedownDidAct = true;
  });

  document.addEventListener('mouseover', e => {
    if (!_dragging || !_dragAnchor) return;
    const cell = e.target.closest('.gantt-cell');
    if (!cell || cell.dataset.meal || !cell.dataset.date) return;
    // Drag = extend the rectangle from the original anchor. We don't
    // OR-into existing selection here because the anchor was already
    // chosen at mousedown (with the right modifier semantics handled
    // there). This keeps drag-select intuitive: "what's between anchor
    // and current cursor".
    _selectRectangle(_dragAnchor, cell);
  });

  document.addEventListener('mouseup', e => {
    _dragging   = false;
    _dragAnchor = null;
    setTimeout(() => { _mousedownDidAct = false; }, 0);
  });

  // ── Touch: long-press opens picker, swipe selects ───────────────────────
  // iPad/iPhone-friendly. Default tap = select (same as desktop). Hold
  // for ≥500ms without moving = open the picker. Swiping during the
  // hold cancels the timer and treats the gesture as drag-select.
  let _touchTimer = null, _touchStart = null, _touchAnchor = null;
  document.addEventListener('touchstart', e => {
    const dropdown = e.target.closest('.gantt-cell-dropdown');
    const cell = e.target.closest('.gantt-cell');
    if (!cell) return;
    if (dropdown) {
      // Tap on the ▾ button — open picker. touchstart is fine; we don't
      // need long-press here because the button itself is the explicit
      // intent.
      const t = e.touches[0];
      showPicker({ clientX: t.clientX, clientY: t.clientY }, cell);
      e.preventDefault();
      return;
    }
    if (cell.dataset.meal) return;
    const t = e.touches[0];
    _touchStart  = { x: t.clientX, y: t.clientY, time: Date.now() };
    _touchAnchor = cell;
    _touchTimer = setTimeout(() => {
      if (!_touchAnchor) return;
      // Long-press: open picker for the held cell.
      showPicker({ clientX: _touchStart.x, clientY: _touchStart.y }, _touchAnchor);
      _touchTimer = null;
      _touchAnchor = null;
    }, 500);
  }, { passive: true });
  document.addEventListener('touchmove', e => {
    if (!_touchStart) return;
    const t = e.touches[0];
    const dx = t.clientX - _touchStart.x, dy = t.clientY - _touchStart.y;
    if (Math.sqrt(dx*dx + dy*dy) > 12) {
      // User started swiping — cancel long-press, treat as drag-select.
      if (_touchTimer) { clearTimeout(_touchTimer); _touchTimer = null; }
      const cell = document.elementFromPoint(t.clientX, t.clientY);
      const target = cell && cell.closest && cell.closest('.gantt-cell');
      if (target && _touchAnchor && target !== _touchAnchor) {
        _selectRectangle(_touchAnchor, target);
      }
    }
  }, { passive: true });
  document.addEventListener('touchend', e => {
    if (_touchTimer) { clearTimeout(_touchTimer); _touchTimer = null; }
    if (_touchAnchor && _touchStart && (Date.now() - _touchStart.time) < 300) {
      // Quick tap — single-cell select.
      clearSelection();
      toggleSelectCell(_touchAnchor, true);
    }
    _touchStart  = null;
    _touchAnchor = null;
  });

  // ── Use-schedule checkbox ─────────────────────────────────────────────────
  document.querySelectorAll('.use-sched-cb').forEach(cb => {
    cb.addEventListener('change', async function() {
      const res = await fetch(`/projects/${_pid}/budget/${_bid}/line`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({id: parseInt(this.dataset.id), use_schedule: this.checked})
      });
      if (!res.ok) { alert('Save failed'); this.checked = !this.checked; }
    });
  });

  // ── Context menu (day picker) buttons ────────────────────────────────────
  document.querySelectorAll('.day-pick-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const picker = document.getElementById('day-picker');
      const cell   = picker._targetCell;
      if (!cell) return;

      if (btn.dataset.type === '__note__') {
        const note = prompt('Note for this day:', cell.title || '');
        if (note !== null) saveDay(cell, cell.dataset.type, note);
      } else if (btn.dataset.type === '__ot__') {
        const current = parseFloat(cell.dataset.otHours || 0);
        const raw = prompt(
          `OT hours for this day (0.25 increments):\nCurrent: ${current > 0 ? current + 'h' : 'None'}`,
          current > 0 ? current : ''
        );
        if (raw !== null) {
          const hrs = Math.round(parseFloat(raw || 0) * 4) / 4;
          cell.dataset.otHours = hrs;
          updateOtBadge(cell, hrs);
          saveDay(cell, cell.dataset.type, null, hrs);
        }
      } else if (btn.dataset.type === '__copy__') {
        // Make sure this cell is in the selection so copySelection grabs it.
        if (!_selection.has(cellKey(cell))) {
          clearSelection();
          toggleSelectCell(cell, true);
        }
        copySelection();
      } else if (btn.dataset.type === '__paste__') {
        if (!_selection.has(cellKey(cell))) {
          clearSelection();
          toggleSelectCell(cell, true);
        }
        pasteSelection();
      } else if (btn.dataset.type === '__paste_special__') {
        if (!_selection.has(cellKey(cell))) {
          clearSelection();
          toggleSelectCell(cell, true);
        }
        window.openPasteSpecial();
      } else {
        const newType = btn.dataset.type;
        const prev    = cell.dataset.type || 'off';
        paintCell(cell, newType);
        saveDay(cell, newType);
        pushUndo([{ lineId: cell.dataset.line, instance: parseInt(cell.dataset.instance||1),
                    date: cell.dataset.date, prevType: prev, newType }]);
      }
      picker.classList.add('hidden');
    });
  });

  // Travel flag toggles in picker
  document.querySelectorAll('.flag-toggle-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const picker = document.getElementById('day-picker');
      const cell   = picker._targetCell;
      if (!cell) return;
      toggleCellFlag(cell, btn.dataset.flag);
    });
  });

  // Select action menu buttons
  document.querySelectorAll('.select-action-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const action = btn.dataset.action;
      const value  = btn.dataset.value;
      document.getElementById('select-action-menu').classList.add('hidden');
      if (action === 'type') {
        applyTypeToSelection(value);
      } else if (action === 'flag') {
        applyFlagToSelection(value);
      } else if (action === 'delete-all') {
        deleteSelection();
      } else if (action === 'copy') {
        copySelection();
      } else if (action === 'paste') {
        pasteSelection();
      } else if (action === 'paste-special') {
        window.openPasteSpecial();
      }
    });
  });

  // Close picker on outside click
  document.addEventListener('click', e => {
    if (!e.target.closest('#day-picker') && !e.target.closest('.gantt-cell')) {
      document.getElementById('day-picker').classList.add('hidden');
    }
    if (!e.target.closest('#select-action-menu')) {
      document.getElementById('select-action-menu').classList.add('hidden');
    }
    // Close payroll info popup on outside click
    if (!e.target.closest('#payroll-info-popup') && !e.target.closest('#payroll-info-btn')) {
      hidePayrollInfo();
    }
  });

  // ── Keyboard shortcuts (selection-always-on model) ──────────────────────
  // Per user 2026-04-29: dropped the Shift-to-toggle-select-mode flow.
  // Selection is always on. Cmd+C / Cmd+V / Delete fire whenever cells
  // are selected. Arrow keys move the active cell with optional Shift
  // (extend) and Cmd (jump-to-edge) modifiers, Excel-style.
  document.addEventListener('keydown', e => {
    const isMac = navigator.platform.toUpperCase().includes('MAC');
    const ctrl  = isMac ? e.metaKey : e.ctrlKey;
    const tag   = document.activeElement && document.activeElement.tagName;
    const isTyping = (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT'
                      || (document.activeElement && document.activeElement.isContentEditable));
    // ? toggles the hotkey help overlay
    if (e.key === '?' && !isTyping) {
      e.preventDefault();
      window.toggleHotkeyHelp && window.toggleHotkeyHelp();
      return;
    }

    // Use e.code (layout/modifier-independent) for letter keys when
    // a modifier might transform e.key. On macOS, Option+V produces
    // "√" via the character map — checking e.key === 'v' never
    // matches with Alt held, which broke the Cmd+Option+V (Paste
    // Special) shortcut. Per user 2026-04-29.
    const codeIs = (c) => e.code === c;
    if (ctrl && codeIs('KeyZ') && !isTyping) { e.preventDefault(); undoLast(); return; }

    // Cmd+Option+V — paste special. Must come BEFORE plain Cmd+V.
    if (ctrl && e.altKey && codeIs('KeyV') && _clipboard && _selection.size > 0) {
      e.preventDefault();
      window.openPasteSpecial && window.openPasteSpecial();
      return;
    }
    if (ctrl && codeIs('KeyC') && _selection.size > 0 && !isTyping) {
      e.preventDefault();
      copySelection();
      return;
    }
    if (ctrl && codeIs('KeyV') && _clipboard && _selection.size > 0 && !isTyping) {
      e.preventDefault();
      pasteSelection();
      return;
    }
    if ((e.key === 'Delete' || e.key === 'Backspace') && _selection.size > 0 && !isTyping) {
      e.preventDefault();
      deleteSelection();
      return;
    }

    // Enter on a selected cell → open the picker. Equivalent to clicking
    // the ▾ button on the active cell (last-clicked / arrow-navigated).
    if (e.key === 'Enter' && !isTyping && _lastClickedCell) {
      // Don't intercept Enter inside the picker itself
      if (e.target.closest && e.target.closest('#day-picker')) return;
      e.preventDefault();
      const rect = _lastClickedCell.getBoundingClientRect();
      showPicker({ clientX: rect.right - 8, clientY: rect.bottom - 8 }, _lastClickedCell);
      return;
    }

    // Arrow-key navigation
    const arrows = { ArrowLeft: [0,-1], ArrowRight: [0,1], ArrowUp: [-1,0], ArrowDown: [1,0] };
    if (arrows[e.key] && !isTyping) {
      e.preventDefault();
      _arrowMove(e.key, e.shiftKey, ctrl);
      return;
    }

    if (e.key === 'Escape') {
      clearSelection();
      document.getElementById('day-picker').classList.add('hidden');
      document.getElementById('select-action-menu').classList.add('hidden');
      const psm = document.getElementById('paste-special-modal');
      if (psm) psm.classList.add('hidden');
      const help = document.getElementById('hotkey-help-overlay');
      if (help) help.classList.add('hidden');
      hidePayrollInfo();
      closeCrewPicker();
      _showPickerNewForm(false);
    }
  });
}

// ── Arrow-key navigation helper ─────────────────────────────────────────
// Builds a row × column matrix of editable cells (skipping meal rows etc.)
// then moves the active cell by (drow, dcol). Cmd+Arrow jumps to the row
// or column edge. Shift+Arrow extends the selection rectangle from the
// existing anchor instead of replacing it.
function _arrowMove(key, shift, jumpEdge) {
  const arrows = { ArrowLeft: [0,-1], ArrowRight: [0,1], ArrowUp: [-1,0], ArrowDown: [1,0] };
  const [dr, dc] = arrows[key];
  // Build active cell list grouped by .gantt-row
  const rows = Array.from(document.querySelectorAll('tr.gantt-row'));
  if (!rows.length) return;
  // Each row's editable cells (data-line + data-date, no data-meal).
  const rowCells = rows.map(r =>
    Array.from(r.querySelectorAll('.gantt-cell'))
      .filter(c => c.dataset.line && c.dataset.date && !c.dataset.meal));
  const nonEmpty = rowCells.map((cs, i) => cs.length ? i : -1).filter(i => i >= 0);
  if (!nonEmpty.length) return;

  // Find current anchor's row/col index
  let curRowIdx = -1, curColIdx = 0;
  if (_lastClickedCell) {
    for (let i = 0; i < rowCells.length; i++) {
      const idx = rowCells[i].indexOf(_lastClickedCell);
      if (idx >= 0) { curRowIdx = i; curColIdx = idx; break; }
    }
  }
  if (curRowIdx < 0) {
    // No active cell yet — start at the first cell of the first non-empty row.
    curRowIdx = nonEmpty[0];
    curColIdx = 0;
  }

  // Compute target. Cmd jumps to edge. Plain arrow moves by 1.
  let newRow = curRowIdx, newCol = curColIdx;
  if (dr !== 0) {
    if (jumpEdge) {
      newRow = dr > 0 ? nonEmpty[nonEmpty.length - 1] : nonEmpty[0];
    } else {
      // step to next non-empty row in the direction
      const order = dr > 0 ? nonEmpty : [...nonEmpty].reverse();
      const here = order.indexOf(curRowIdx);
      newRow = here >= 0 && here + 1 < order.length ? order[here + 1] : curRowIdx;
    }
    // Clamp column to that row's cell count
    const maxCol = Math.max(0, rowCells[newRow].length - 1);
    newCol = Math.min(curColIdx, maxCol);
  } else if (dc !== 0) {
    const maxCol = rowCells[curRowIdx].length - 1;
    if (jumpEdge) {
      newCol = dc > 0 ? maxCol : 0;
    } else {
      newCol = Math.max(0, Math.min(maxCol, curColIdx + dc));
    }
  }

  const targetCell = rowCells[newRow][newCol];
  if (!targetCell) return;

  if (shift && _lastClickedCell) {
    // Capture the anchor BEFORE calling _selectRectangle — that
    // function mutates _lastClickedCell to point at the target. If we
    // save the anchor afterward, it's already the moved-to cell and
    // every subsequent rect call collapses to that single cell, which
    // looks to the user like the selection "jumped" forward by one
    // instead of growing from the existing cell. Per user 2026-04-29.
    if (!_shiftAnchor) _shiftAnchor = _lastClickedCell;
    _selectRectangle(_shiftAnchor, targetCell);
  } else {
    _shiftAnchor = null;
    clearSelection();
    toggleSelectCell(targetCell, true);
  }
  // Scroll target into view if it's clipped by the gantt scroll wrap
  try { targetCell.scrollIntoView({ block: 'nearest', inline: 'nearest' }); } catch (_) {}
}
let _shiftAnchor = null;

// ─────────────────────────────────────────────────────────────────────────────
// SELECT MODE
// ─────────────────────────────────────────────────────────────────────────────

// _selectMode is dead (selection is always-on now) but a few call sites
// still reference these helpers — keep them as no-ops so we don't have
// to chase down every old hook. Per user 2026-04-29: toolbar select
// button + select-mode-badge removed; selection now happens on every
// click without an explicit mode.
function _activateSelectMode()   { _selectMode = false; }
function _deactivateSelectMode() { _selectMode = false; clearSelection(); }
function toggleSelectMode()      { /* no-op — selection always on */ }

// ── Hotkey help overlay ──────────────────────────────────────────────────
window.toggleHotkeyHelp = function() {
  const ov = document.getElementById('hotkey-help-overlay');
  if (!ov) return;
  ov.classList.toggle('hidden');
};

// ── Paste Special ────────────────────────────────────────────────────────
// Modal lets the user pick which elements (day type / flags / OT / notes)
// from the clipboard to apply on paste. Without it, ⌘V applies all four.
window.openPasteSpecial = function() {
  if (!_clipboard) { alert('Nothing on the clipboard. Copy some cells first (⌘C).'); return; }
  if (_selection.size === 0) { alert('Select target cells first.'); return; }
  const m = document.getElementById('paste-special-modal');
  if (m) m.classList.remove('hidden');
};
window.closePasteSpecial = function() {
  const m = document.getElementById('paste-special-modal');
  if (m) m.classList.add('hidden');
};
window.applyPasteSpecial = async function() {
  const opts = {
    dayType: document.getElementById('ps-day-type').checked,
    flags:   document.getElementById('ps-flags').checked,
    ot:      document.getElementById('ps-ot').checked,
    notes:   document.getElementById('ps-notes').checked,
  };
  if (!opts.dayType && !opts.flags && !opts.ot && !opts.notes) {
    alert('Pick at least one element to paste.');
    return;
  }
  window.closePasteSpecial();
  await pasteSelection(opts);
};

// ─────────────────────────────────────────────────────────────────────────────
// CELL INTERACTION
// ─────────────────────────────────────────────────────────────────────────────

function handleCellClick(event, cell) {
  if (_selectMode) {
    if (event.shiftKey && _lastClickedCell) {
      rangeSelect(cell);
    } else if (!_mousedownDidAct) {
      toggleSelectCell(cell);
    }
  }
  // Normal mode: paint handled entirely by mousedown (paintAndSave) + mouseover drag.
}

function nextDayType(current) {
  // Primary cycle: Work ↔ Off
  return (current === 'work') ? 'off' : 'work';
}

let _dragBatch = null;  // accumulate drag-paint cells for single undo push

function paintAndSave(cell, dayType, isStart) {
  if (isStart) {
    _dragBatch = [];
  }
  const prev = cell.dataset.type || 'off';
  if (prev === dayType) return;  // no change

  if (_dragBatch !== null) {
    _dragBatch.push({
      lineId:   cell.dataset.line,
      instance: parseInt(cell.dataset.instance || 1),
      date:     cell.dataset.date,
      prevType: prev,
      newType:  dayType,
    });
    if (isStart) pushUndo(_dragBatch);  // push reference; array fills during drag
  }
  paintCell(cell, dayType);
  saveDay(cell, dayType);
}

function paintCell(cell, dayType) {
  cell.dataset.prevType = cell.dataset.type || 'off';
  ALL_DAY_TYPES.forEach(dt => cell.classList.remove('day-' + dt));
  cell.classList.add('day-' + dayType);
  cell.dataset.type = dayType;

  // Preserve existing flag dots when repainting
  const flagHTML = Array.from(cell.querySelectorAll('.flag-dot')).map(f => f.outerHTML).join('');

  if (dayType === 'off') {
    cell.innerHTML = flagHTML;
  } else {
    // Two-char labels for compact cell rendering. Most day types use
    // first two letters; the new travel-half / travel-unpaid variants
    // get distinct glyphs so they don't collide with plain TRavel.
    const labelMap = {
      work: 'WK', travel: 'TR',
      travel_half: 'T½', travel_unpaid: 'T0',
      hold: 'HO', half: 'HF', kill_fee: 'KF', custom: 'CS',
    };
    const label = labelMap[dayType] || dayType.substring(0, 2).toUpperCase();
    cell.innerHTML = `<span class="cell-label">${label}</span>${flagHTML}`;
  }
  // OT/DT classes are refreshed by the debounced fetchTotals call after each save
}

async function saveDay(cell, dayType, note, estOtHours) {
  const lineId   = cell.dataset.line;
  const dateStr  = cell.dataset.date;
  const instance = parseInt(cell.dataset.instance || 1);

  // Mark this cell as locally edited so the collab live-patch skips it for 12s
  if (typeof window._ganttMarkCell === 'function') {
    window._ganttMarkCell(`${lineId}:${instance}:${dateStr}`);
  }

  if (dayType === 'off') {
    const r = await fetch(`/projects/${_pid}/budget/${_bid}/gantt/day`, {
      method: 'DELETE',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({line_id: parseInt(lineId), date: dateStr, crew_instance: instance})
    });
    if (!r.ok) console.error('Gantt day delete failed', r.status);
    else scheduleTotalsRefresh();
    return;
  }

  const payload = {
    line_id:       parseInt(lineId),
    date:          dateStr,
    day_type:      dayType,
    note:          note  !== undefined ? note  : null,
    crew_instance: instance,
  };
  if (estOtHours !== undefined && estOtHours !== null) {
    payload.est_ot_hours = estOtHours;
  }

  const r = await fetch(`/projects/${_pid}/budget/${_bid}/gantt/day`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  });
  if (!r.ok) {
    console.error('Gantt day save failed', r.status);
    // Revert visually
    const prev = cell.dataset.prevType || 'off';
    paintCell(cell, prev);
  } else {
    // Auto-enable use_schedule on this line if it isn't already checked.
    // Also zeroes est_ot so legacy manual OT doesn't carry into schedule mode.
    // Use querySelectorAll so all instances of a multi-qty line get ticked.
    const cbs = document.querySelectorAll(`.use-sched-cb[data-id="${lineId}"]`);
    const anyUnchecked = Array.from(cbs).some(cb => !cb.checked);
    cbs.forEach(cb => { cb.checked = true; });
    if (anyUnchecked) {
      fetch(`/projects/${_pid}/budget/${_bid}/line`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({id: parseInt(lineId), use_schedule: true, est_ot: 0})
      });
    }
    scheduleTotalsRefresh();
  }
}

function updateOtBadge(cell, hrs) {
  let badge = cell.querySelector('.cell-ot-badge');
  if (hrs > 0) {
    if (!badge) {
      badge = document.createElement('span');
      badge.className = 'cell-ot-badge';
      cell.appendChild(badge);
    }
    badge.textContent = `+${hrs}h`;
  } else if (badge) {
    badge.remove();
  }
}

function showPicker(e, cell) {
  const picker = document.getElementById('day-picker');
  picker._targetCell = cell;

  // Sync flag button active states before showing
  const flags = _getCellFlags(cell);
  document.querySelectorAll('.flag-toggle-btn').forEach(btn => {
    btn.classList.toggle('flag-active', !!flags[btn.dataset.flag]);
  });

  // Highlight current day type (work / travel / hold / etc.) so the
  // user sees what's already on this cell when they right-click. Also
  // marks the Add Note + OT Hours buttons when the cell carries that
  // metadata. Without this, the menu always looked the same regardless
  // of cell state — only flags showed their state.
  const curType = cell.dataset.type || 'off';
  const curNote = (cell.title || '').trim();
  const curOt   = parseFloat(cell.dataset.otHours || 0);
  document.querySelectorAll('#day-picker .day-pick-btn').forEach(btn => {
    const t = btn.dataset.type;
    if (t === '__note__') {
      btn.classList.toggle('flag-active', !!curNote);
    } else if (t === '__ot__') {
      btn.classList.toggle('flag-active', curOt > 0);
    } else {
      btn.classList.toggle('flag-active', t === curType);
    }
  });

  // Show off-screen so the browser can reflow and we can read true dimensions
  picker.style.visibility = 'hidden';
  picker.style.left = '0px';
  picker.style.top  = '0px';
  picker.classList.remove('hidden');

  // Read actual dimensions after reflow, then clamp to viewport
  requestAnimationFrame(() => {
    const pw   = picker.offsetWidth;
    const ph   = picker.offsetHeight;
    const maxL = window.innerWidth  - pw  - 12;
    const maxT = window.innerHeight - ph  - 12;
    // Use clientX/clientY (viewport coords) since picker is position:fixed
    picker.style.left = Math.max(4, Math.min(e.clientX + 4, maxL)) + 'px';
    picker.style.top  = Math.max(4, Math.min(e.clientY + 4, maxT)) + 'px';
    picker.style.visibility = '';
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// CELL FLAGS (travel indicators)
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// SELECT MODE BULK ACTIONS
// ─────────────────────────────────────────────────────────────────────────────

function showSelectActionMenu(e) {
  const menu = document.getElementById('select-action-menu');
  if (!menu) return;
  const count = document.getElementById('select-action-count');
  if (count) count.textContent = `${_selection.size} cell${_selection.size !== 1 ? 's' : ''} selected`;

  // Highlight unanimous state across the selection. A button gets
  // .flag-active when ALL selected cells share that day type / flag.
  // Mixed selections show no highlight on the relevant button so the
  // user knows clicking will set rather than toggle off.
  const cells = _getSelectedCells();
  if (cells.length) {
    const types = new Set(cells.map(c => c.dataset.type || 'off'));
    const unanimousType = types.size === 1 ? cells[0].dataset.type || 'off' : null;
    menu.querySelectorAll('.select-action-btn[data-action="type"]').forEach(btn => {
      btn.classList.toggle('flag-active', btn.dataset.value === unanimousType);
    });
    // Per flag: count how many cells already have it on. Highlight when
    // all do; half-highlight class for partial coverage.
    const flagBtns = menu.querySelectorAll('.select-action-btn[data-action="flag"]');
    flagBtns.forEach(btn => {
      const f = btn.dataset.value;
      let on = 0;
      cells.forEach(c => { if (_getCellFlags(c)[f]) on++; });
      btn.classList.toggle('flag-active',  on === cells.length);
      btn.classList.toggle('flag-partial', on > 0 && on < cells.length);
    });
  } else {
    menu.querySelectorAll('.flag-active, .flag-partial').forEach(b =>
      b.classList.remove('flag-active', 'flag-partial'));
  }

  menu.style.visibility = 'hidden';
  menu.style.left = '0px';
  menu.style.top  = '0px';
  menu.classList.remove('hidden');

  requestAnimationFrame(() => {
    const mw   = menu.offsetWidth;
    const mh   = menu.offsetHeight;
    const maxL = window.innerWidth  - mw - 12;
    const maxT = window.innerHeight - mh - 12;
    // Use clientX/clientY (viewport coords) since menu is position:fixed
    menu.style.left = Math.max(4, Math.min(e.clientX + 4, maxL)) + 'px';
    menu.style.top  = Math.max(4, Math.min(e.clientY + 4, maxT)) + 'px';
    menu.style.visibility = '';
  });
}

async function applyTypeToSelection(dayType) {
  const cells = _getSelectedCells();
  if (!cells.length) return;
  const batch = [];
  for (const cell of cells) {
    const prev = cell.dataset.type || 'off';
    paintCell(cell, dayType);
    await saveDay(cell, dayType);
    batch.push({ lineId: cell.dataset.line, instance: parseInt(cell.dataset.instance || 1),
                 date: cell.dataset.date, prevType: prev, newType: dayType });
  }
  if (batch.length) pushUndo(batch);
  clearSelection();
}

async function applyFlagToSelection(flag) {
  const cells = _getSelectedCells();
  if (!cells.length) return;
  // Determine target state: if majority are on, turn all off; otherwise turn all on
  let onCount = 0;
  cells.forEach(c => { const f = _getCellFlags(c); if (f[flag]) onCount++; });
  const targetOn = onCount < cells.length / 2;  // majority-off → turn on
  for (const cell of cells) {
    if (cell.dataset.type === 'off' || !cell.dataset.type) continue;  // skip unscheduled
    const flags = _getCellFlags(cell);
    if (targetOn) {
      flags[flag] = true;
    } else {
      delete flags[flag];
    }
    cell.dataset.flags = JSON.stringify(flags);
    _renderFlagDots(cell, flags);
    await fetch(`/projects/${_pid}/budget/${_bid}/gantt/day`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        line_id: parseInt(cell.dataset.line),
        date: cell.dataset.date,
        day_type: cell.dataset.type,
        crew_instance: parseInt(cell.dataset.instance || 1),
        cell_flags: flags,
      })
    });
  }
  // Don't clear selection after flag apply so user can apply multiple flags
}

function _getSelectedCells() {
  return Array.from(_selection).map(key => {
    const [lineId, instance, date] = key.split(':');
    return document.querySelector(
      `.gantt-cell[data-line="${lineId}"][data-instance="${instance}"][data-date="${date}"]`
    );
  }).filter(Boolean);
}


function _getCellFlags(cell) {
  try { return JSON.parse(cell.dataset.flags || '{}'); }
  catch(e) { return {}; }
}

function _renderFlagDots(cell, flags) {
  cell.querySelectorAll('.flag-dot').forEach(f => f.remove());
  if (flags.flight) {
    const s = document.createElement('span');
    s.className = 'flag-dot flag-flight'; s.title = 'Flight';
    cell.appendChild(s);
  }
  if (flags.mileage) {
    const s = document.createElement('span');
    s.className = 'flag-dot flag-mileage'; s.title = 'Mileage';
    cell.appendChild(s);
  }
  if (flags.car_rental) {
    const s = document.createElement('span');
    s.className = 'flag-dot flag-car-rental'; s.title = 'Car Rental';
    cell.appendChild(s);
  }
  if (flags.hotel) {
    const sl = document.createElement('span');
    sl.className = 'flag-dot flag-hotel-l'; sl.title = 'Hotel';
    const sr = document.createElement('span');
    sr.className = 'flag-dot flag-hotel-r'; sr.title = 'Hotel';
    cell.appendChild(sl);
    cell.appendChild(sr);
  }
  if (flags.working_meal) {
    const s = document.createElement('span');
    s.className = 'flag-dot flag-working-meal'; s.title = 'Working Meal';
    cell.appendChild(s);
  }
  // Per diem: four variants, each with its own dot color so the schedule
  // view shows at-a-glance whether the day is a full-day per diem or a
  // partial one. Legacy `per_diem` flag → full-day dot for back-compat.
  if (flags.per_diem_full || flags.per_diem) {
    const s = document.createElement('span');
    s.className = 'flag-dot flag-per-diem'; s.title = 'Per Diem — Full Day';
    cell.appendChild(s);
  }
  if (flags.per_diem_breakfast) {
    const s = document.createElement('span');
    s.className = 'flag-dot flag-per-diem-b'; s.title = 'Per Diem — Breakfast';
    cell.appendChild(s);
  }
  if (flags.per_diem_lunch) {
    const s = document.createElement('span');
    s.className = 'flag-dot flag-per-diem-l'; s.title = 'Per Diem — Lunch';
    cell.appendChild(s);
  }
  if (flags.per_diem_dinner) {
    const s = document.createElement('span');
    s.className = 'flag-dot flag-per-diem-d'; s.title = 'Per Diem — Dinner';
    cell.appendChild(s);
  }
}

async function toggleCellFlag(cell, flag) {
  const lineId   = cell.dataset.line;
  const dateStr  = cell.dataset.date;
  const instance = parseInt(cell.dataset.instance || 1);
  const dayType  = cell.dataset.type || 'off';
  if (dayType === 'off') return;

  const flags = _getCellFlags(cell);
  flags[flag] = !flags[flag];
  if (!flags[flag]) delete flags[flag];

  cell.dataset.flags = JSON.stringify(flags);
  _renderFlagDots(cell, flags);

  document.querySelectorAll('.flag-toggle-btn').forEach(btn => {
    if (btn.dataset.flag === flag) btn.classList.toggle('flag-active', !!flags[flag]);
  });

  const r = await fetch(`/projects/${_pid}/budget/${_bid}/gantt/day`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      line_id: parseInt(lineId), date: dateStr,
      day_type: dayType, crew_instance: instance, cell_flags: flags,
    })
  });
  if (r.ok) scheduleTotalsRefresh();
}

// ─────────────────────────────────────────────────────────────────────────────
// MEAL ROW TOGGLES
// ─────────────────────────────────────────────────────────────────────────────

async function toggleMeal(cell) {
  const dateStr = cell.dataset.date;
  const field   = cell.dataset.meal;
  const active  = cell.classList.contains('meal-active');
  const newVal  = !active;

  cell.classList.toggle('meal-active', newVal);
  // Production-day rows use a green star (⭐) marker; meal rows use the
  // standard purple dot (●). Pick the symbol based on the field name.
  const isProductionDay = field === 'is_production_day';
  const symbol = isProductionDay ? '⭐' : '●';
  cell.classList.toggle('production-day-active', newVal && isProductionDay);
  const dot = cell.querySelector('.meal-dot');
  if (newVal && !dot) {
    const s = document.createElement('span');
    s.className = 'meal-dot';
    s.textContent = symbol;
    if (isProductionDay) s.style.color = '#22c55e';
    cell.appendChild(s);
  } else if (!newVal && dot) {
    dot.remove();
  }

  const r = await fetch(`/projects/${_pid}/budget/${_bid}/gantt/meal`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ date: dateStr, field, value: newVal })
  });
  if (!r.ok) { cell.classList.toggle('meal-active', active); }
  else scheduleTotalsRefresh();
}

// ─────────────────────────────────────────────────────────────────────────────
// WINDOW NAVIGATION
// ─────────────────────────────────────────────────────────────────────────────

function shiftWindow(days) {
  const headers = document.querySelectorAll('#gantt-table th.gantt-date-col');
  if (!headers.length) return;

  const firstDate = headers[0].dataset.date;
  const lastDate  = headers[headers.length - 1].dataset.date;
  const start = new Date(firstDate + 'T00:00:00');
  const end   = new Date(lastDate  + 'T00:00:00');
  start.setDate(start.getDate() + days);
  end.setDate(end.getDate()   + days);
  navigateTo(start.toISOString().slice(0, 10), end.toISOString().slice(0, 10));
}

function showWeeks(n) {
  const headers = document.querySelectorAll('#gantt-table th.gantt-date-col');
  const startDate = headers.length
    ? new Date(headers[0].dataset.date + 'T00:00:00')
    : new Date();
  const end = new Date(startDate);
  end.setDate(end.getDate() + (n * 7) - 1);
  navigateTo(startDate.toISOString().slice(0, 10), end.toISOString().slice(0, 10));
}

function applyDateRange() {
  const start = document.getElementById('gantt-start-input').value;
  const end   = document.getElementById('gantt-end-input').value;
  if (start && end && start <= end) navigateTo(start, end);
}

function navigateTo(start, end) {
  const wrap = document.getElementById('gantt-scroll-wrap');
  if (wrap) sessionStorage.setItem(_scrollKey(), wrap.scrollLeft);
  sessionStorage.setItem(_rangeKey(), JSON.stringify({ start, end }));
  const url = new URL(window.location.href);
  url.searchParams.set('gantt_start', start);
  url.searchParams.set('gantt_end',   end);
  window.location.href = url.toString();
}

// ─────────────────────────────────────────────────────────────────────────────
// SELECT MODE HELPERS
// ─────────────────────────────────────────────────────────────────────────────

function cellKey(cell) {
  return `${cell.dataset.line}:${cell.dataset.instance || 1}:${cell.dataset.date}`;
}

function toggleSelectCell(cell, forceAdd) {
  const key = cellKey(cell);
  if (forceAdd) {
    _selection.add(key);
    cell.classList.add('selected');
  } else {
    if (_selection.has(key)) {
      _selection.delete(key);
      cell.classList.remove('selected');
    } else {
      _selection.add(key);
      cell.classList.add('selected');
    }
  }
  _lastClickedCell = cell;
}

function rangeSelect(toCell) {
  if (!_lastClickedCell) { toggleSelectCell(toCell); return; }
  const allCells = Array.from(document.querySelectorAll('.gantt-cell'));
  const fromIdx  = allCells.indexOf(_lastClickedCell);
  const toIdx    = allCells.indexOf(toCell);
  if (fromIdx < 0 || toIdx < 0) { toggleSelectCell(toCell); return; }
  const minIdx = Math.min(fromIdx, toIdx);
  const maxIdx = Math.max(fromIdx, toIdx);
  allCells.slice(minIdx, maxIdx + 1).forEach(c => {
    _selection.add(cellKey(c));
    c.classList.add('selected');
  });
  _lastClickedCell = toCell;
}

// Excel-style rectangle range select: from anchor cell to target cell,
// includes every cell whose row is between the anchor's row and the
// target's row AND whose date column is between the anchor's date and
// the target's date. Used by both shift-click and drag-select.
function _selectRectangle(anchorCell, toCell) {
  if (!anchorCell || !toCell) return;
  // Wipe prior selection — drag-select replaces it. (Cmd modifier
  // semantics already happened at mousedown if the user wanted to
  // accumulate.)
  document.querySelectorAll('.gantt-cell.selected').forEach(c =>
    c.classList.remove('selected'));
  _selection.clear();
  // Resolve row ordering by DOM position of each cell's parent <tr>.
  const rows = Array.from(document.querySelectorAll('tr.gantt-row'));
  const aRow = anchorCell.closest('tr.gantt-row');
  const tRow = toCell.closest('tr.gantt-row');
  if (!aRow || !tRow) {
    // Anchor or target isn't inside a labor row — fall back to flat range
    rangeSelect(toCell);
    _lastClickedCell = toCell;
    return;
  }
  const aRowIdx = rows.indexOf(aRow);
  const tRowIdx = rows.indexOf(tRow);
  const minRow  = Math.min(aRowIdx, tRowIdx);
  const maxRow  = Math.max(aRowIdx, tRowIdx);
  // Date axis: lex order on YYYY-MM-DD works as date order.
  const minDate = anchorCell.dataset.date < toCell.dataset.date
                ? anchorCell.dataset.date : toCell.dataset.date;
  const maxDate = anchorCell.dataset.date > toCell.dataset.date
                ? anchorCell.dataset.date : toCell.dataset.date;
  for (let i = minRow; i <= maxRow; i++) {
    const r = rows[i];
    if (!r) continue;
    r.querySelectorAll('.gantt-cell').forEach(c => {
      if (c.dataset.meal) return;
      const d = c.dataset.date;
      if (!d) return;
      if (d >= minDate && d <= maxDate) {
        _selection.add(cellKey(c));
        c.classList.add('selected');
      }
    });
  }
  _lastClickedCell = toCell;
}

function clearSelection() {
  _selection.clear();
  document.querySelectorAll('.gantt-cell.selected').forEach(c => c.classList.remove('selected'));
  document.querySelectorAll('.gantt-cell.copied').forEach(c => c.classList.remove('copied'));
  _lastClickedCell = null;
  const wrap = document.getElementById('gantt-scroll-wrap');
  if (wrap) wrap.classList.remove('has-copy');
}

// ─────────────────────────────────────────────────────────────────────────────
// COPY / PASTE
// ─────────────────────────────────────────────────────────────────────────────

function copySelection() {
  if (_selection.size === 0) return;

  const items = [];
  _selection.forEach(key => {
    const [lineId, instance, date] = key.split(':');
    const cell = document.querySelector(
      `.gantt-cell[data-line="${lineId}"][data-instance="${instance}"][data-date="${date}"]`
    );
    if (cell) items.push({ lineId, instance: parseInt(instance), date,
                           dayType: cell.dataset.type || 'off', note: cell.title || '',
                           flags: _getCellFlags(cell),
                           estOtHours: cell.dataset.estOtHours ? parseFloat(cell.dataset.estOtHours) : null });
  });
  if (items.length === 0) return;

  const rowKeys = [...new Set(items.map(i => `${i.lineId}:${i.instance}`))].sort();
  const colKeys = [...new Set(items.map(i => i.date))].sort();

  const grid = rowKeys.map(rk =>
    colKeys.map(date => {
      const item = items.find(i => `${i.lineId}:${i.instance}` === rk && i.date === date);
      return item ? { dayType: item.dayType, note: item.note, flags: item.flags || {}, estOtHours: item.estOtHours }
                  : { dayType: 'off', note: '', flags: {}, estOtHours: null };
    })
  );

  _clipboard = { grid, rows: rowKeys.length, cols: colKeys.length };

  // Visual feedback: mark copied cells
  document.querySelectorAll('.gantt-cell.copied').forEach(c => c.classList.remove('copied'));
  items.forEach(i => {
    const cell = document.querySelector(
      `.gantt-cell[data-line="${i.lineId}"][data-instance="${i.instance}"][data-date="${i.date}"]`
    );
    if (cell) cell.classList.add('copied');
  });
  const wrap = document.getElementById('gantt-scroll-wrap');
  if (wrap) wrap.classList.add('has-copy');

  _scheduleToast(`✓ Copied ${items.length} cell${items.length > 1 ? 's' : ''} — select target, then ⌘V to paste (⌘⌥V for Paste Special)`);
}

function _scheduleToast(msg) {
  let el = document.getElementById('schedule-toast');
  if (!el) {
    el = document.createElement('div');
    el.id = 'schedule-toast';
    el.style.cssText = 'position:fixed;bottom:24px;left:50%;transform:translateX(-50%);background:rgba(15,23,42,.95);color:#fff;padding:10px 18px;border-radius:8px;font-size:.85rem;box-shadow:0 4px 16px rgba(0,0,0,.4);z-index:3000;pointer-events:none;transition:opacity .25s;opacity:0';
    document.body.appendChild(el);
  }
  el.textContent = msg;
  el.style.opacity = '1';
  clearTimeout(el._t);
  el._t = setTimeout(() => { el.style.opacity = '0'; }, 2500);
}

// Delete: wipe day_type + flags + OT + note on every selected cell.
// One bulk action so the user can sweep large rectangles clean. Saved
// to the server cell-by-cell using the existing /gantt/day DELETE
// endpoint (full row removal). Pushed onto the undo stack so a single
// Cmd+Z restores everything.
async function deleteSelection() {
  if (_selection.size === 0) return;
  const cells = _getSelectedCells();
  if (!cells.length) return;
  if (!confirm(`Delete ${cells.length} cell${cells.length !== 1 ? 's' : ''}? This clears day type, flags, OT, and notes.`)) return;

  // Snapshot for undo BEFORE we wipe.
  const undoBatch = cells.map(cell => ({
    lineId:   cell.dataset.line,
    instance: parseInt(cell.dataset.instance || 1),
    date:     cell.dataset.date,
    prevType: cell.dataset.type || 'off',
    prevFlags: cell.dataset.flags || '{}',
    prevOt:   parseFloat(cell.dataset.otHours || 0),
    prevNote: cell.title || '',
  }));

  for (const cell of cells) {
    cell.dataset.type    = 'off';
    cell.dataset.flags   = '{}';
    cell.dataset.otHours = '0';
    cell.title = '';
    paintCell(cell, 'off');
    _renderFlagDots(cell, {});
    updateOtBadge(cell, 0);
    try {
      await fetch(`/projects/${_pid}/budget/${_bid}/gantt/day`, {
        method: 'DELETE',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          line_id: parseInt(cell.dataset.line),
          date: cell.dataset.date,
          crew_instance: parseInt(cell.dataset.instance || 1),
        })
      });
    } catch (e) {
      console.warn('Delete cell failed', e);
    }
  }
  pushUndo(undoBatch);
  scheduleTotalsRefresh();
}

async function pasteSelection(opts) {
  if (!_clipboard) return;
  // opts (Paste Special): {dayType, flags, ot, notes} — all default true
  // when omitted (legacy plain-paste path).
  const _opts = Object.assign({ dayType: true, flags: true, ot: true, notes: true }, opts || {});

  // Determine paste target
  if (_selection.size === 0) return;

  const selectedItems = [];
  _selection.forEach(key => {
    const [lineId, instance, date] = key.split(':');
    selectedItems.push({ lineId, instance: parseInt(instance), date });
  });

  const targetRows = [...new Set(selectedItems.map(i => `${i.lineId}:${i.instance}`))].sort();
  const targetCols = [...new Set(selectedItems.map(i => i.date))].sort();
  const undoBatch  = [];
  const payload    = [];

  // Phase 1: paint all cells locally for instant visual feedback, collect batch payload
  for (let ri = 0; ri < targetRows.length; ri++) {
    const [lineId, instance] = targetRows[ri].split(':');
    for (let ci = 0; ci < targetCols.length; ci++) {
      const date   = targetCols[ci];
      const srcRow = ri % _clipboard.rows;
      const srcCol = ci % _clipboard.cols;
      const src    = _clipboard.grid[srcRow][srcCol];

      const cell = document.querySelector(
        `.gantt-cell[data-line="${lineId}"][data-instance="${instance}"][data-date="${date}"]`
      );
      if (!cell) continue;

      const prev = cell.dataset.type || 'off';
      // Filter by opts. Day type only changes if opts.dayType true; else
      // keep the existing day type.
      const newType = _opts.dayType ? src.dayType : prev;
      undoBatch.push({ lineId, instance: parseInt(instance), date,
                       prevType: prev, newType });

      if (_opts.dayType) paintCell(cell, src.dayType);
      if (_opts.flags) {
        if (src.flags && Object.keys(src.flags).length > 0 && newType !== 'off') {
          cell.dataset.flags = JSON.stringify(src.flags);
          _renderFlagDots(cell, src.flags);
        } else if (newType === 'off') {
          cell.dataset.flags = '';
        }
      }

      const spec = {
        line_id:       parseInt(lineId),
        date,
        day_type:      newType,
        crew_instance: parseInt(instance),
      };
      if (_opts.notes && src.note !== undefined && src.note !== null)               spec.note         = src.note;
      if (_opts.ot    && src.estOtHours !== undefined && src.estOtHours !== null)   spec.est_ot_hours = src.estOtHours;
      if (_opts.flags && src.flags && Object.keys(src.flags).length > 0)            spec.cell_flags   = src.flags;
      payload.push(spec);
    }
  }

  if (payload.length === 0) return;

  // Phase 2: single batched network call
  try {
    const r = await fetch(`/projects/${_pid}/budget/${_bid}/gantt/days`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ days: payload })
    });
    if (!r.ok) {
      console.error('Gantt batch paste failed', r.status);
      // Revert all painted cells
      for (const entry of undoBatch) {
        const cell = document.querySelector(
          `.gantt-cell[data-line="${entry.lineId}"][data-instance="${entry.instance}"][data-date="${entry.date}"]`
        );
        if (cell) paintCell(cell, entry.prevType);
      }
      return;
    }
    const data = await r.json().catch(() => ({}));
    // Reflect server-side use_schedule auto-toggles in the UI
    const toggled = data.use_schedule_toggled_lines || [];
    toggled.forEach(lineId => {
      document.querySelectorAll(`.use-sched-cb[data-id="${lineId}"]`)
        .forEach(cb => { cb.checked = true; });
    });
  } catch (err) {
    console.error('Gantt batch paste error', err);
    return;
  }

  if (undoBatch.length > 0) pushUndo(undoBatch);
  scheduleTotalsRefresh();
}

// ─────────────────────────────────────────────────────────────────────────────
// UNDO
// ─────────────────────────────────────────────────────────────────────────────

function pushUndo(batch) {
  if (!batch || batch.length === 0) return;
  _undoStack.push(batch);
  if (_undoStack.length > MAX_UNDO) _undoStack.shift();
}

async function undoLast() {
  if (_undoStack.length === 0) return;
  const batch = _undoStack.pop();
  for (const entry of batch) {
    const cell = document.querySelector(
      `.gantt-cell[data-line="${entry.lineId}"][data-instance="${entry.instance}"][data-date="${entry.date}"]`
    );
    if (!cell) continue;
    paintCell(cell, entry.prevType);
    await saveDay(cell, entry.prevType, entry.prevNote || null);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// LIVE TOTALS PANEL
// ─────────────────────────────────────────────────────────────────────────────

let _totalsTimer = null;
function scheduleTotalsRefresh() {
  clearTimeout(_totalsTimer);
  _totalsTimer = setTimeout(fetchTotals, 800);
}

// Refresh totals (and float bar) once on load in case server snapshot is stale
fetchTotals();

async function fetchTotals() {
  try {
    const res = await fetch(`/projects/${_pid}/budget/${_bid}/gantt/totals`);
    if (!res.ok) return;
    const data = await res.json();
    if (Array.isArray(data)) {
      renderTotals(data, {});
    } else {
      renderTotals(data.sections || [], data.ot_cells || {});
      updateGanttFloatBar(data);
    }
  } catch(e) { console.error('Totals fetch failed', e); }
}

function updateGanttFloatBar(data) {
  const fmt = v => '$' + parseFloat(v || 0).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2});
  const subEl   = document.getElementById('float-subtotal');
  const feeEl   = document.getElementById('float-fee');
  const grandEl = document.getElementById('float-grand');
  if (subEl)   subEl.textContent   = fmt(data.subtotal);
  if (feeEl)   feeEl.textContent   = fmt(data.fee);
  if (grandEl) grandEl.textContent = fmt(data.grand);
}

function renderTotals(sections, otCells) {
  const tbody = document.getElementById('gantt-totals-tbody');
  if (!tbody) return;
  const fmt = v => '$' + parseFloat(v||0).toLocaleString('en-US',
    {minimumFractionDigits:0, maximumFractionDigits:0});
  tbody.innerHTML = '';
  if (!sections || sections.length === 0) {
    tbody.innerHTML = '<tr><td colspan="5" class="muted" style="padding:.4rem .5rem;font-size:.8rem">No schedule data yet.</td></tr>';
  } else {
    sections.forEach(s => {
      const tr = document.createElement('tr');
      tr.dataset.section = s.code;
      tr.innerHTML = `
        <td class="totals-section-name">${s.name}</td>
        <td class="col-num totals-st">${fmt(s.st)}</td>
        <td class="col-num totals-ot ${s.ot>0?'has-ot':''}">${fmt(s.ot)}</td>
        <td class="col-num totals-dt ${s.dt>0?'has-dt':''}">${fmt(s.dt)}</td>
        <td class="col-num totals-total"><strong>${fmt(s.total)}</strong></td>`;
      tbody.appendChild(tr);
    });
  }

  // Apply per-cell OT/DT highlighting without touching cell content
  // First clear all existing OT/DT classes on visible cells
  document.querySelectorAll('.gantt-cell.day-has-ot, .gantt-cell.day-has-dt').forEach(c => {
    c.classList.remove('day-has-ot', 'day-has-dt');
  });
  // Then apply fresh status from server
  if (otCells) {
    Object.entries(otCells).forEach(([key, dates]) => {
      const [lineId, instance] = key.split(':');
      Object.entries(dates).forEach(([date, status]) => {
        const cell = document.querySelector(
          `.gantt-cell[data-line="${lineId}"][data-date="${date}"][data-instance="${instance}"]`
        );
        if (cell) {
          cell.classList.add(status === 'dt' ? 'day-has-dt' : 'day-has-ot');
        }
      });
    });
  }
}

function toggleTotalsPanel() {
  const body = document.getElementById('gantt-totals-body');
  const icon = document.getElementById('totals-toggle-icon');
  if (!body) return;
  const collapsed = body.style.display === 'none';
  body.style.display = collapsed ? '' : 'none';
  if (icon) icon.textContent = collapsed ? '▼' : '▶';
}

// ─────────────────────────────────────────────────────────────────────────────
// PAYROLL PROFILE
// ─────────────────────────────────────────────────────────────────────────────

async function changePayrollProfile(value) {
  const r = await fetch(`/projects/${_pid}/budget/${_bid}/settings`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ payroll_profile_id: value ? parseInt(value) : null })
  });
  if (r.ok) window.location.reload();
  else alert('Failed to save payroll profile');
}

// ─────────────────────────────────────────────────────────────────────────────
// PAYROLL INFO POPUP
// ─────────────────────────────────────────────────────────────────────────────

function togglePayrollInfo(e) {
  e.stopPropagation();
  const popup = document.getElementById('payroll-info-popup');
  if (popup.classList.contains('hidden')) {
    showPayrollInfo();
  } else {
    hidePayrollInfo();
  }
}

function showPayrollInfo() {
  const popup = document.getElementById('payroll-info-popup');
  const nameEl = document.getElementById('payroll-info-name');
  const bodyEl = document.getElementById('payroll-info-body');
  if (!popup) return;

  // Find the currently selected profile
  const sel = document.getElementById('payroll-profile-select');
  const selectedId = sel ? parseInt(sel.value) : null;
  const profile = (PAYROLL_PROFILES || []).find(p => p.id === selectedId);

  if (!profile) {
    nameEl.textContent = 'No payroll profile selected';
    bodyEl.innerHTML = `
      <p>With no profile selected, all days are calculated as flat day rates.</p>
      <p>No OT or DT will be computed regardless of hours worked.</p>`;
  } else {
    nameEl.textContent = profile.name;
    bodyEl.innerHTML = buildProfileDescription(profile);
  }

  // Also show a legend of all profiles for comparison
  const allProfiles = (PAYROLL_PROFILES || []);
  if (allProfiles.length > 0) {
    let legend = '<div class="payroll-info-all"><strong>All profiles:</strong><ul>';
    allProfiles.forEach(p => {
      legend += `<li><em>${p.name}</em> — ${p.description}</li>`;
    });
    legend += '</ul></div>';
    bodyEl.innerHTML += legend;
  }

  popup.classList.remove('hidden');
}

function hidePayrollInfo() {
  const popup = document.getElementById('payroll-info-popup');
  if (popup) popup.classList.add('hidden');
}

// ── Schedule row label inline editing ────────────────────────────────────────
function startLabelEdit(span) {
  if (span.querySelector('input')) return; // already editing
  const lineId   = parseInt(span.dataset.line);
  const instance = parseInt(span.dataset.instance);
  const current  = span.textContent.trim();
  const input    = document.createElement('input');
  input.type     = 'text';
  input.value    = current;
  input.className = 'gantt-label-input';
  input.style.cssText = 'width:100%;font:inherit;padding:1px 4px;border:1px solid var(--accent);border-radius:3px;background:var(--bg-input,var(--bg-2));color:inherit;';
  span.textContent = '';
  span.appendChild(input);
  input.focus();
  input.select();

  const baseLabel = span.dataset.baseLabel || current;

  async function commit() {
    const newLabel = input.value.trim();
    // Rebuild span: custom label text + original name span
    span.textContent = newLabel || baseLabel;
    if (newLabel && newLabel !== baseLabel) {
      span.classList.add('gantt-label-custom');
      const origSpan = document.createElement('span');
      origSpan.className = 'gantt-label-original';
      origSpan.textContent = `(${baseLabel})`;
      span.appendChild(origSpan);
    } else {
      span.classList.remove('gantt-label-custom');
    }
    if (newLabel !== current) {
      await fetch(`/projects/${_pid}/budget/${_bid}/line/${lineId}/schedule-label`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ instance, label: newLabel }),
      });
    }
  }

  let _cancelled = false;
  input.addEventListener('blur', () => { if (!_cancelled) commit(); });
  input.addEventListener('keydown', e => {
    if (e.key === 'Enter') { e.preventDefault(); input.blur(); }
    if (e.key === 'Escape') {
      _cancelled = true;
      // Restore original display without saving
      span.textContent = current;
      if (span.dataset.baseLabel && current !== span.dataset.baseLabel) {
        span.classList.add('gantt-label-custom');
        const origSpan = document.createElement('span');
        origSpan.className = 'gantt-label-original';
        origSpan.textContent = `(${baseLabel})`;
        span.appendChild(origSpan);
      }
    }
  });
}

function jumpOverflow(e) {
  e.stopPropagation();
  const targetDate = e.currentTarget.dataset.date; // YYYY-MM-DD
  if (!targetDate) return;

  // Preserve the current window size
  const headers = document.querySelectorAll('#gantt-table th.gantt-date-col');
  const windowDays = headers.length > 0 ? headers.length : 14;

  const d = new Date(targetDate + 'T00:00:00');
  // Put the target date near the start (offset by 1 day so context is visible)
  const start = new Date(d);
  start.setDate(start.getDate() - 1);
  const end = new Date(start);
  end.setDate(end.getDate() + windowDays - 1);
  navigateTo(start.toISOString().slice(0, 10), end.toISOString().slice(0, 10));
}

// ─────────────────────────────────────────────────────────────────────────────
// CREW PICKER (per schedule row)
// ─────────────────────────────────────────────────────────────────────────────

let _crewPickerTarget = null;  // { lineId, instance }

async function removeCrewFromRow(lineId, instance, chip) {
  if (chip) {
    chip.innerHTML = '+ Assign';
    chip.title = 'Click to assign crew member';
    chip.classList.add('unassigned');
  }
  const r = await fetch(`/projects/${_pid}/budget/${_bid}/gantt/assign`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ line_id: lineId, instance, crew_member_id: null }),
  });
  if (!r.ok) {
    if (chip) { chip.innerHTML = '! Error'; chip.title = 'Remove failed — refresh to retry'; }
  }
}

function closeCrewPicker() {
  const cp = document.getElementById('crew-picker-popover');
  if (cp) cp.classList.add('hidden');
  _crewPickerTarget = null;
}

function openCrewPicker(lineId, instance, el) {
  _crewPickerTarget = { lineId, instance };
  const popover = document.getElementById('crew-picker-popover');
  const rect    = el.getBoundingClientRect();
  popover.style.left = Math.min(rect.left, window.innerWidth - 260) + 'px';
  popover.style.top  = (rect.bottom + window.scrollY + 4) + 'px';
  const searchInput = document.getElementById('crew-picker-search');
  searchInput.value = '';
  // Reset inline add form
  _showPickerNewForm(false);
  ['cpf-name','cpf-phone','cpf-email','cpf-dept'].forEach(id => {
    const el2 = document.getElementById(id); if (el2) el2.value = '';
  });
  const errEl = document.getElementById('cpf-error');
  if (errEl) errEl.style.display = 'none';
  filterCrewPicker('');
  popover.classList.remove('hidden');
  setTimeout(() => searchInput.focus(), 50);
}

function filterCrewPicker(query) {
  const q = query.trim().toLowerCase();
  const filtered = (typeof ALL_CREW !== 'undefined' ? ALL_CREW : [])
    .filter(c => !q || c.name.toLowerCase().includes(q) ||
                       (c.company && c.company.toLowerCase().includes(q)));
  const list = document.getElementById('crew-picker-list');
  if (!filtered.length) {
    list.innerHTML = '<div class="crew-picker-empty">No match — add them below.</div>';
    // Auto-show the inline add form and pre-fill name from search
    _showPickerNewForm(true, query.trim());
    return;
  }
  list.innerHTML = filtered.slice(0, 30).map(c => `
    <div class="crew-picker-item" data-id="${c.id}" data-name="${c.name.replace(/"/g, '&quot;')}">
      <span class="crew-picker-name">${c.name}</span>
      ${c.department ? `<span class="crew-picker-dept">${c.department}</span>` : ''}
    </div>`).join('');
  list.querySelectorAll('.crew-picker-item').forEach(el => {
    const crew = filtered.find(c => c.id === parseInt(el.dataset.id));
    el.addEventListener('click', () => assignCrewToRow(parseInt(el.dataset.id), el.dataset.name, crew));
  });
}

function _showPickerNewForm(show, prefillName) {
  const form = document.getElementById('crew-picker-new-form');
  const btn  = document.getElementById('cpf-toggle-btn');
  if (!form) return;
  form.style.display = show ? 'block' : 'none';
  if (btn) btn.textContent = show ? '− New Person' : '+ New Person';
  if (show && prefillName) {
    const nameEl = document.getElementById('cpf-name');
    if (nameEl && !nameEl.value) nameEl.value = prefillName;
  }
  if (show) {
    const nameEl = document.getElementById('cpf-name');
    if (nameEl) setTimeout(() => nameEl.focus(), 50);
  }
}

function togglePickerNewForm() {
  const form = document.getElementById('crew-picker-new-form');
  const open = form && form.style.display !== 'none';
  const q = (document.getElementById('crew-picker-search') || {}).value || '';
  _showPickerNewForm(!open, q.trim());
}

async function assignCrewToRow(crewId, crewName, crewObj) {
  if (!_crewPickerTarget) return;
  const { lineId, instance } = _crewPickerTarget;

  if (typeof window._ganttMarkCell === 'function') {
    window._ganttMarkCell(`crew:${lineId}:${instance || 1}`);
  }

  // Close and clear immediately — don't wait for the network
  document.getElementById('crew-picker-popover').classList.add('hidden');
  _crewPickerTarget = null;

  // Update chip optimistically
  const chip = document.querySelector(
    `.gantt-crew-chip[data-line="${lineId}"][data-instance="${instance}"]`
  );
  if (chip) {
    if (crewId && crewName) {
      chip.innerHTML = `${crewName}<span class="crew-chip-remove" title="Remove">✕</span>`;
      chip.title       = crewName;
      chip.classList.remove('unassigned');
      chip.querySelector('.crew-chip-remove').addEventListener('click', e => {
        e.stopPropagation();
        removeCrewFromRow(lineId, instance, chip);
      });
    } else {
      chip.innerHTML = '+ Assign';
      chip.title     = 'Click to assign crew member';
      chip.classList.add('unassigned');
    }
  }

  const r = await fetch(`/projects/${_pid}/budget/${_bid}/gantt/assign`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ line_id: lineId, instance, crew_member_id: crewId }),
  });
  if (!r.ok) {
    if (chip) { chip.innerHTML = '! Error'; chip.title = 'Save failed — refresh to retry'; }
    console.error('Failed to save crew assignment', r.status);
    return;
  }

  // Prompt for default rate if crew has one
  if (crewId && crewObj && crewObj.default_rate) {
    const rtLabel = {'day_10':'10hr Day','day_8':'8hr Day','day_12':'12hr Day',
                     'flat_day':'Flat Day','flat_project':'Flat Project',
                     'hourly':'Hourly','week':'Weekly'}[crewObj.default_rate_type] || crewObj.default_rate_type;
    const apply = confirm(`${crewName} has a default rate of $${crewObj.default_rate.toLocaleString()} (${rtLabel}) — apply it to this line?`);
    if (apply) {
      await fetch(`/projects/${_pid}/budget/${_bid}/line/${lineId}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ rate: crewObj.default_rate, rate_type: crewObj.default_rate_type }),
      });
      // Refresh gantt section totals
      if (typeof updateTotalsFromServer === 'function') updateTotalsFromServer();
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// INLINE ADD CREW (inside picker popover)
// ─────────────────────────────────────────────────────────────────────────────

async function submitPickerNewPerson() {
  const name = (document.getElementById('cpf-name') || {}).value?.trim();
  const errEl = document.getElementById('cpf-error');
  if (!name) {
    errEl.textContent = 'Name is required.';
    errEl.style.display = 'block';
    return;
  }
  errEl.style.display = 'none';

  const payload = {
    name,
    phone:      (document.getElementById('cpf-phone') || {}).value?.trim() || '',
    email:      (document.getElementById('cpf-email') || {}).value?.trim() || '',
    department: (document.getElementById('cpf-dept')  || {}).value?.trim() || '',
  };

  const btn = document.querySelector('#crew-picker-new-form .btn-primary');
  if (btn) { btn.disabled = true; btn.textContent = 'Saving…'; }

  try {
    const r = await fetch('/crew/new', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!r.ok) {
      errEl.textContent = 'Failed to create crew member. Please try again.';
      errEl.style.display = 'block';
      return;
    }
    const data = await r.json();
    // Add to local ALL_CREW so future searches find them
    if (typeof ALL_CREW !== 'undefined') {
      ALL_CREW.push({ id: data.id, name: data.name,
                      department: data.department || '', company: data.company || '' });
    }
    // Assign to targeted row
    if (_crewPickerTarget) {
      await assignCrewToRow(data.id, data.name);
    } else {
      closeCrewPicker();
    }
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = 'Save & Assign'; }
  }
}

function buildProfileDescription(p) {
  const lines = [];

  if (!p.daily_st_hours && !p.weekly_st_hours) {
    lines.push('<strong>Flat rate only</strong> — no OT or DT calculated.');
    return `<ul>${lines.map(l=>`<li>${l}</li>`).join('')}</ul>`;
  }

  // Daily thresholds
  if (p.daily_st_hours) {
    lines.push(`Daily ST threshold: <strong>${p.daily_st_hours} hrs</strong> straight time per day`);
    if (p.daily_dt_hours) {
      lines.push(`Daily DT threshold: OT from ${p.daily_st_hours}–${p.daily_dt_hours} hrs, DT after <strong>${p.daily_dt_hours} hrs</strong>`);
    } else {
      lines.push(`All hours beyond ${p.daily_st_hours} hrs/day are OT (<strong>${p.ot_multiplier}×</strong>)`);
    }
  } else {
    lines.push('No daily hour thresholds — daily hours tracked but no daily OT trigger');
  }

  // Weekly threshold
  if (p.weekly_st_hours) {
    lines.push(`Weekly OT threshold: OT after <strong>${p.weekly_st_hours} hrs/week</strong> (${p.weekly_ot_multiplier}×)`);
  }

  // Multipliers
  lines.push(`OT rate: <strong>${p.ot_multiplier}×</strong> | DT rate: <strong>${p.dt_multiplier}×</strong>`);

  // 7th day rule
  if (p.seventh_day_rule === 'ot_all') {
    lines.push('7th consecutive workday: <strong>all hours at OT (first 8) then DT</strong>');
  }

  // Exempt note
  lines.push('<em style="color:var(--text-muted)">Note: lines marked Exempt fringe skip OT regardless of this profile.</em>');

  return `<ul>${lines.map(l=>`<li>${l}</li>`).join('')}</ul>`;
}
