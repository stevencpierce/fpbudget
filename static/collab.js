// ── FPBudget Real-Time Collaboration (Socket.IO) ────────────────────────────
// Loaded as external script to avoid CSP inline-script restrictions.
// Expects global variables: BID, MY_ID, MY_NAME, MY_COLOR, PID
// Expects global functions: refreshLineRow, refreshSectionTotals, _collabPingPresence, reloadWithTab

(function() {
  'use strict';

  // ── Top Sheet live refresh ──────────────────────────────────────────────────
  // Sums line totals from Budget tab DOM, updates Top Sheet section rows + footer
  function _refreshTopSheet() {
    var fmt = function(v) {
      return '$' + parseFloat(v || 0).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2});
    };
    // Collect totals by section code from budget line rows
    var sectionTotals = {};
    document.querySelectorAll('tr[data-code]').forEach(function(row) {
      var code = row.getAttribute('data-code');
      if (!code) return;
      // Find the section start (e.g. code 601 belongs to section 600)
      // Budget lines store account_code; the section is the parent code
      // from the section-block's data-section attribute
      var block = row.closest('.section-block');
      var sec = block ? block.getAttribute('data-section') : code;
      if (!sec) sec = code;
      // Read est_total from the line-total or line-working strong element
      var strong = row.querySelector('.line-working strong') || row.querySelector('.line-total strong');
      var base = 0;
      if (strong) {
        base = parseFloat(strong.dataset.base !== undefined ? strong.dataset.base : strong.textContent.replace(/[$,]/g, '')) || 0;
      }
      sectionTotals[sec] = (sectionTotals[sec] || 0) + base;
    });

    // Update Top Sheet section rows
    var grandEstimated = 0;
    document.querySelectorAll('tr[data-ts-code]').forEach(function(tsRow) {
      var code = tsRow.getAttribute('data-ts-code');
      var total = sectionTotals[code] || 0;
      grandEstimated += total;
      var cells = tsRow.querySelectorAll('td.col-num');
      // cells[0] = Estimated, cells[1] = Working, cells[2] = Actual, cells[3] = Variance
      if (cells[0]) cells[0].textContent = fmt(total);
    });

    // Update Subtotal row
    var subRow = document.querySelector('tr[data-ts-row="subtotal"]');
    if (subRow) {
      var subCells = subRow.querySelectorAll('td.col-num strong');
      if (subCells[0]) subCells[0].textContent = fmt(grandEstimated);
    }

    // Update Fee row
    var feeRow = document.querySelector('tr[data-ts-row="fee"]');
    if (feeRow) {
      var feePct = parseFloat(feeRow.getAttribute('data-fee-pct')) || 0;
      var dispersed = feeRow.getAttribute('data-dispersed') === 'true';
      var feeAmount = dispersed ? 0 : grandEstimated * feePct;
      var feeCells = feeRow.querySelectorAll('td.col-num');
      if (feeCells[0]) feeCells[0].textContent = fmt(feeAmount);
    }

    // Update Grand Total row
    var grandRow = document.querySelector('tr[data-ts-row="grand"]');
    if (grandRow) {
      var feeForGrand = 0;
      if (feeRow) {
        var fp = parseFloat(feeRow.getAttribute('data-fee-pct')) || 0;
        var disp = feeRow.getAttribute('data-dispersed') === 'true';
        feeForGrand = disp ? 0 : grandEstimated * fp;
      }
      var grandTotal = grandEstimated + feeForGrand;
      var grandCells = grandRow.querySelectorAll('td.col-num strong');
      if (grandCells[0]) grandCells[0].textContent = fmt(grandTotal);
    }
  }

  // Expose for external use
  window._refreshTopSheet = _refreshTopSheet;

  if (typeof io === 'undefined') {
    console.warn('[WS] Socket.IO client not loaded');
    return;
  }

  console.log('[WS] collab.js init, BID=' + BID + ', MY_ID=' + MY_ID);

  var socket;
  try {
    socket = io();
  } catch(e) {
    console.error('[WS] io() threw:', e);
    return;
  }

  // Expose globally so polling fallback can check connection state
  window._socket = socket;
  console.log('[WS] Socket created, connecting...');

  socket.on('connect', function() {
    console.log('[WS] Connected:', socket.id);
    socket.emit('join_budget', {
      budget_id: BID,
      user_id: MY_ID,
      user_name: MY_NAME,
    });
  });

  socket.on('connect_error', function(err) {
    console.warn('[WS] Connect error:', err.message);
  });

  socket.on('disconnect', function(reason) {
    console.log('[WS] Disconnected:', reason);
  });

  // ── Presence ────────────────────────────────────────────────────────────────

  var _pendingViewers = null;
  var _leaveTimer = null;

  socket.on('presence_update', function(data) {
    _pendingViewers = data.viewers || [];
    if (!_leaveTimer && typeof _renderViewers === 'function') {
      _renderViewers(_pendingViewers);
    }
  });

  socket.on('user_joined', function(data) {
    var hint = document.getElementById('collab-edit-hint');
    if (hint) hint.textContent = data.user_name + ' joined';
    setTimeout(function() { if (hint) hint.textContent = ''; }, 3000);
  });

  socket.on('user_left', function(data) {
    var hint = document.getElementById('collab-edit-hint');
    if (hint) hint.textContent = data.user_name + ' left';
    clearTimeout(_leaveTimer);
    _leaveTimer = setTimeout(function() {
      _leaveTimer = null;
      if (_pendingViewers && typeof _renderViewers === 'function') {
        _renderViewers(_pendingViewers);
      }
      if (hint) hint.textContent = '';
    }, 5000);
  });

  // ── Field changes ───────────────────────────────────────────────────────────

  socket.on('field_change', function(data) {
    if (!data.line_id || !data.data) return;
    if (data.user_id === MY_ID) return;
    if (typeof _myEditedLines !== 'undefined' && _myEditedLines.has(data.line_id)) return;

    var row = document.querySelector('tr[data-id="' + data.line_id + '"]');
    if (!row) {
      console.warn('[WS] field_change: no row for line_id=' + data.line_id);
      return;
    }

    console.log('[WS] field_change: updating line', data.line_id, data.data);

    // Try the full refresh function first, then direct DOM update as fallback
    var refreshed = false;
    try {
      if (typeof refreshLineRow === 'function') {
        refreshLineRow(data.line_id, data.data);
        refreshed = true;
      }
    } catch(e) {
      console.warn('[WS] refreshLineRow threw:', e);
    }

    // Direct DOM update as fallback (or supplement)
    var d = data.data;
    var fmt = function(v) {
      return '$' + parseFloat(v || 0).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2});
    };

    // Subtotal cell
    var subEl = row.querySelector('.line-subtotal');
    if (subEl && d.subtotal !== undefined) subEl.textContent = fmt(d.subtotal);

    // Estimated total cell
    var totEl = row.querySelector('.line-total strong');
    if (totEl && d.est_total !== undefined) totEl.textContent = fmt(d.est_total);

    // Working total cell
    var workEl = row.querySelector('.line-working strong');
    if (workEl && d.est_total !== undefined) workEl.textContent = fmt(d.est_total);

    // Flash updated cells
    var cells = [subEl, totEl, workEl].filter(Boolean);
    cells.forEach(function(el) {
      el.style.transition = 'background .3s';
      el.style.background = 'rgba(37,99,235,.25)';
      setTimeout(function() { el.style.background = ''; }, 1500);
    });

    // Flash row
    row.style.transition = 'background .3s';
    row.style.background = 'rgba(37,99,235,.08)';
    setTimeout(function() { row.style.background = ''; }, 1200);

    // Refresh section totals (Budget tab)
    try {
      if (typeof refreshSectionTotals === 'function') {
        document.querySelectorAll('.line-table').forEach(function(t) {
          refreshSectionTotals(t);
        });
      }
    } catch(e) { /* ignore */ }

    // Refresh Top Sheet section rollups + grand total
    try { _refreshTopSheet(); } catch(e) { /* ignore */ }

    if (typeof _flashSync === 'function') _flashSync();

    var hint = document.getElementById('collab-edit-hint');
    if (hint) hint.textContent = data.user_name + ' edited just now';
  });

  // ── Conflict override ───────────────────────────────────────────────────────

  socket.on('conflict_override', function(data) {
    if (typeof _showConflictToast === 'function') {
      _showConflictToast(data.field, data.winner_name);
    }
  });

  // ── Cursor presence ─────────────────────────────────────────────────────────

  socket.on('editing_start', function(data) {
    var row = document.querySelector('tr[data-id="' + data.line_id + '"]');
    if (!row) return;
    row.style.borderLeft = '3px solid ' + (data.color || '#2563eb');
    row.dataset.editingBy = data.user_name;
    var badge = row.querySelector('.edit-badge');
    if (!badge) {
      badge = document.createElement('span');
      badge.className = 'edit-badge';
      var firstTd = row.querySelector('td');
      if (firstTd) firstTd.prepend(badge);
    }
    var initials = (data.user_name || '').split(' ').map(function(w) { return w[0]; }).join('').slice(0, 2).toUpperCase();
    badge.textContent = initials;
    badge.style.background = data.color || '#2563eb';
    badge.title = data.user_name + ' is editing';
  });

  socket.on('editing_stop', function(data) {
    var row = document.querySelector('tr[data-id="' + data.line_id + '"]');
    if (!row) return;
    row.style.borderLeft = '';
    delete row.dataset.editingBy;
    var badge = row.querySelector('.edit-badge');
    if (badge) badge.remove();
  });

})();
