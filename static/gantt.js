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
const ALL_DAY_TYPES = ['work', 'travel', 'hold', 'half', 'kill_fee', 'off', 'custom'];

let _pid, _bid, _activeProfileId;
let _dragging   = false;
let _dragType   = null;

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
    if (_selectMode && _selection.size > 0) {
      showSelectActionMenu(e);
    } else {
      showPicker(e, cell);
    }
  });

  // ── Close crew picker on outside mousedown ────────────────────────────────
  document.addEventListener('mousedown', e => {
    if (!e.target.closest('#crew-picker-popover') &&
        !e.target.closest('.gantt-crew-chip') &&
        !e.target.closest('#add-crew-modal')) {
      closeCrewPicker();
    }
  }, true);  // capture phase so it fires before any element handlers

  // ── Mousedown: primary paint or select ───────────────────────────────────
  document.addEventListener('mousedown', e => {
    const cell = e.target.closest('.gantt-cell');
    if (!cell || e.button !== 0) return;
    const ctrl = e.metaKey || e.ctrlKey;

    if (ctrl && !_selectMode) {
      _activateSelectMode();
      _mousedownDidAct = false;
      return;
    }

    if (_selectMode) {
      _dragging = true;
      if (!e.shiftKey) {
        toggleSelectCell(cell);
        _mousedownDidAct = true;
      }
    } else {
      // Normal paint mode
      _dragging = true;
      _dragType = nextDayType(cell.dataset.type);
      paintAndSave(cell, _dragType, true);
      _mousedownDidAct = true;
    }
  });

  document.addEventListener('mouseover', e => {
    if (!_dragging) return;
    const cell = e.target.closest('.gantt-cell');
    if (!cell) return;
    if (_selectMode) {
      toggleSelectCell(cell, true);  // drag-select: always add
    } else if (_dragType) {
      paintAndSave(cell, _dragType, false);  // drag-paint: no undo push per cell
    }
  });

  document.addEventListener('mouseup', e => {
    if (!_selectMode && _dragging && _dragType) {
      // Collect all cells that were drag-painted and push a single undo entry
      // (individual undos already pushed per-cell via paintAndSave batch tracking)
    }
    _dragging = false;
    _dragType = null;
    setTimeout(() => { _mousedownDidAct = false; }, 0);
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

  // ── Keyboard shortcuts ────────────────────────────────────────────────────
  document.addEventListener('keydown', e => {
    const isMac = navigator.platform.toUpperCase().includes('MAC');
    const ctrl  = isMac ? e.metaKey : e.ctrlKey;

    // Shift toggles select mode — skip if user is typing in any input/textarea
    if (e.key === 'Shift' && !e.repeat) {
      const tag = document.activeElement && document.activeElement.tagName;
      if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;
      e.preventDefault();
      if (_selectMode) {
        _deactivateSelectMode();
      } else {
        _activateSelectMode();
      }
      return;
    }

    if (ctrl && e.key === 'z') { e.preventDefault(); undoLast(); }

    if (_selectMode) {
      if (ctrl && e.key === 'c' && _selection.size > 0) {
        e.preventDefault();
        copySelection();
      }
      if (ctrl && e.key === 'v' && _clipboard) {
        e.preventDefault();
        pasteSelection();
      }
    }

    if (e.key === 'Escape') {
      clearSelection();
      if (_selectMode) _deactivateSelectMode();
      document.getElementById('day-picker').classList.add('hidden');
      document.getElementById('select-action-menu').classList.add('hidden');
      hidePayrollInfo();
      closeCrewPicker();
      closeAddCrewModal();
    }
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// SELECT MODE
// ─────────────────────────────────────────────────────────────────────────────

function _activateSelectMode() {
  _selectMode = true;
  const btn  = document.getElementById('btn-select-mode');
  const wrap = document.getElementById('gantt-scroll-wrap');
  const badge = document.getElementById('select-mode-badge');
  if (btn)  { btn.classList.add('active'); btn.textContent = '✓ Select ON'; }
  if (wrap) wrap.classList.add('select-mode-active');
  if (badge) badge.classList.remove('hidden');
  document.body.classList.add('gantt-select-mode');
}

function _deactivateSelectMode() {
  _selectMode = false;
  clearSelection();
  const btn  = document.getElementById('btn-select-mode');
  const wrap = document.getElementById('gantt-scroll-wrap');
  const badge = document.getElementById('select-mode-badge');
  if (btn)  { btn.classList.remove('active'); btn.textContent = '☐ Select'; }
  if (wrap) wrap.classList.remove('select-mode-active');
  if (badge) badge.classList.add('hidden');
  document.body.classList.remove('gantt-select-mode');
  // NOTE: _clipboard is intentionally NOT cleared here so paste still works
  //       after toggling select mode off and back on.
}

function toggleSelectMode() {
  if (_selectMode) _deactivateSelectMode();
  else             _activateSelectMode();
}

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
    const label = dayType === 'work' ? 'WK' : dayType.substring(0, 2).toUpperCase();
    cell.innerHTML = `<span class="cell-label">${label}</span>${flagHTML}`;
  }
  // OT/DT classes are refreshed by the debounced fetchTotals call after each save
}

async function saveDay(cell, dayType, note, estOtHours) {
  const lineId   = cell.dataset.line;
  const dateStr  = cell.dataset.date;
  const instance = parseInt(cell.dataset.instance || 1);

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
  if (flags.per_diem) {
    const s = document.createElement('span');
    s.className = 'flag-dot flag-per-diem'; s.title = 'Per Diem';
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

  await fetch(`/projects/${_pid}/budget/${_bid}/gantt/day`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      line_id: parseInt(lineId), date: dateStr,
      day_type: dayType, crew_instance: instance, cell_flags: flags,
    })
  });
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
  const dot = cell.querySelector('.meal-dot');
  if (newVal && !dot) {
    const s = document.createElement('span');
    s.className = 'meal-dot'; s.textContent = '●';
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

  // Brief flash on badge
  const badge = document.getElementById('select-mode-badge');
  if (badge) {
    const orig = badge.textContent;
    badge.textContent = `✓ Copied ${items.length} cell${items.length > 1 ? 's' : ''} — select target then Cmd+V to paste`;
    setTimeout(() => { badge.textContent = orig; }, 2500);
  }
}

async function pasteSelection() {
  if (!_clipboard) return;

  // Determine paste target: use current selection, or if empty, re-enter select mode and wait
  if (_selection.size === 0) {
    const badge = document.getElementById('select-mode-badge');
    if (badge) badge.textContent = 'Select target cells, then Cmd+V to paste';
    return;
  }

  const selectedItems = [];
  _selection.forEach(key => {
    const [lineId, instance, date] = key.split(':');
    selectedItems.push({ lineId, instance: parseInt(instance), date });
  });

  const targetRows = [...new Set(selectedItems.map(i => `${i.lineId}:${i.instance}`))].sort();
  const targetCols = [...new Set(selectedItems.map(i => i.date))].sort();
  const undoBatch  = [];

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
      undoBatch.push({ lineId, instance: parseInt(instance), date,
                       prevType: prev, newType: src.dayType });
      paintCell(cell, src.dayType);
      await saveDay(cell, src.dayType, src.note || null, src.estOtHours ?? undefined);
      // Paste flags (saved separately after day type is set)
      if (src.flags && Object.keys(src.flags).length > 0 && src.dayType !== 'off') {
        cell.dataset.flags = JSON.stringify(src.flags);
        _renderFlagDots(cell, src.flags);
        await fetch(`/projects/${_pid}/budget/${_bid}/gantt/day`, {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({
            line_id: parseInt(lineId), date, day_type: src.dayType,
            crew_instance: parseInt(instance), cell_flags: src.flags,
          })
        });
      }
    }
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
    list.innerHTML = '<div class="crew-picker-empty">No crew found</div>';
    return;
  }
  list.innerHTML = filtered.slice(0, 30).map(c => `
    <div class="crew-picker-item" data-id="${c.id}" data-name="${c.name.replace(/"/g, '&quot;')}">
      <span class="crew-picker-name">${c.name}</span>
      ${c.department ? `<span class="crew-picker-dept">${c.department}</span>` : ''}
    </div>`).join('');
  // Attach click handlers after DOM insertion to avoid quote-escaping issues
  list.querySelectorAll('.crew-picker-item').forEach(el => {
    el.addEventListener('click', () => assignCrewToRow(parseInt(el.dataset.id), el.dataset.name));
  });
}

async function assignCrewToRow(crewId, crewName) {
  if (!_crewPickerTarget) return;
  const { lineId, instance } = _crewPickerTarget;

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
      // re-attach remove listener
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
    // Revert chip on failure
    if (chip) { chip.innerHTML = '! Error'; chip.title = 'Save failed — refresh to retry'; }
    console.error('Failed to save crew assignment', r.status);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// ADD CREW MODAL
// ─────────────────────────────────────────────────────────────────────────────

function openAddCrewModal() {
  // Hide the picker but keep _crewPickerTarget so we can assign after creation
  document.getElementById('crew-picker-popover').classList.add('hidden');
  // Clear form
  ['ac-name','ac-phone','ac-email','ac-company','ac-department'].forEach(id => {
    document.getElementById(id).value = '';
  });
  const err = document.getElementById('ac-error');
  err.textContent = '';
  err.classList.add('hidden');
  document.getElementById('add-crew-modal').classList.remove('hidden');
  setTimeout(() => document.getElementById('ac-name').focus(), 50);
}

function closeAddCrewModal() {
  document.getElementById('add-crew-modal').classList.add('hidden');
}

async function submitAddCrew() {
  const name = document.getElementById('ac-name').value.trim();
  if (!name) {
    const err = document.getElementById('ac-error');
    err.textContent = 'Name is required.';
    err.classList.remove('hidden');
    return;
  }

  const payload = {
    name,
    phone:      document.getElementById('ac-phone').value.trim(),
    email:      document.getElementById('ac-email').value.trim(),
    company:    document.getElementById('ac-company').value.trim(),
    department: document.getElementById('ac-department').value.trim(),
  };

  const r = await fetch('/crew/new', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });

  if (!r.ok) {
    const err = document.getElementById('ac-error');
    err.textContent = 'Failed to create crew member. Please try again.';
    err.classList.remove('hidden');
    return;
  }

  const data = await r.json();
  closeAddCrewModal();

  // Add to local ALL_CREW array so future picker searches find them
  if (typeof ALL_CREW !== 'undefined') {
    ALL_CREW.push({ id: data.id, name: data.name,
                    department: data.department || '', company: data.company || '' });
  }

  // Assign them to the row that was targeted before opening the modal
  if (_crewPickerTarget) {
    await assignCrewToRow(data.id, data.name);
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
