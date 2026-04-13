// ── FPBudget Real-Time Collaboration (Socket.IO) ────────────────────────────
// Loaded as external script to avoid CSP inline-script restrictions.
// Expects global variables: BID, MY_ID, MY_NAME, MY_COLOR, PID
// Expects global functions: refreshLineRow, refreshSectionTotals, _collabPingPresence, reloadWithTab

(function() {
  'use strict';

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

    var row = document.querySelector('.line-row[data-id="' + data.line_id + '"]');
    if (!row) return;

    if (typeof refreshLineRow === 'function') {
      refreshLineRow(data.line_id, data.data);
    }

    // Flash row blue
    row.style.transition = 'background .3s';
    row.style.background = 'rgba(37,99,235,.15)';
    setTimeout(function() { row.style.background = ''; }, 1200);

    // Refresh section totals
    if (typeof refreshSectionTotals === 'function') {
      document.querySelectorAll('.line-table').forEach(function(t) {
        refreshSectionTotals(t);
      });
    }

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
    var row = document.querySelector('.line-row[data-id="' + data.line_id + '"]');
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
    var row = document.querySelector('.line-row[data-id="' + data.line_id + '"]');
    if (!row) return;
    row.style.borderLeft = '';
    delete row.dataset.editingBy;
    var badge = row.querySelector('.edit-badge');
    if (badge) badge.remove();
  });

})();
