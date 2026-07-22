// Extracted from a templates/budget.html inline block (audit M9, 2026-07-20).
// Jinja values arrive via window.__BJ (set by the inline preamble before this
// script tag). Classic script, document order — semantics identical to inline.


// ── Conflict toast helper ─────────────────────────────────────────────────
function _showConflictToast(field, winnerName) {
  const existing = document.querySelector('.conflict-toast');
  if (existing) existing.remove();
  const toast = document.createElement('div');
  toast.className = 'conflict-toast';
  // winnerName is another collaborator's USER-SETTABLE display name — escape it
  // (stored-XSS sink, security audit C3 2026-07-20).
  const _ctEsc = s => String(s == null ? '' : s).replace(/[&<>"']/g,
    c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const label = _ctEsc((field || '').replace(/_/g, ' '));
  toast.innerHTML = `Your change to <strong>${label}</strong> was overridden by <strong>${_ctEsc(winnerName)}</strong>.`
    + `<button onclick="this.parentElement.remove()" style="background:none;border:none;color:var(--text-muted);cursor:pointer;margin-left:8px;font-size:.85rem">✕</button>`;
  document.body.appendChild(toast);
  setTimeout(() => { if (toast.parentElement) toast.remove(); }, 4000);
}

// ── Fallback: polling for structural changes (lines added/removed) ───────────
function _collabPingPresence() {
  if (window._socket && window._socket.connected) return; // SocketIO handles presence
  fetch(_PRESENCE_URL, { method: 'POST' })
    .then(r => r.json())
    .then(d => {
      const viewers = (d.viewers || []).map(v => ({
        user_id: v.id, user_name: v.name, color: '#2563eb'
      }));
      _renderViewers(viewers);
      const hint = document.getElementById('collab-edit-hint');
      if (hint && d.last_edit) {
        hint.textContent = `${d.last_edit.name} edited ${_timeAgo(new Date(d.last_edit.at))}`;
      }
    })
    .catch(() => {});
}

function _collabLivePatch() {
  fetch(_LIVE_URL)
    .then(r => r.json())
    .then(d => {
      if (!d.lines) return;

      // Initialise known line IDs on first poll
      if (_knownLineIds === null) {
        _knownLineIds = d.line_ids || [];
        return;
      }

      // Detect structural changes (lines added or removed)
      const incoming = d.line_ids || [];
      const added   = incoming.filter(id => !_knownLineIds.includes(id));
      const removed = _knownLineIds.filter(id => !incoming.includes(id));
      if ((added.length || removed.length) && !_structToastShown) {
        _structToastShown = true;
        const activeTag = document.activeElement && document.activeElement.tagName;
        const isEditing = (activeTag === 'INPUT' || activeTag === 'TEXTAREA' || activeTag === 'SELECT');
        if (!isEditing) {
          reloadWithTab();
          return;
        }
        const parts = [];
        if (added.length)   parts.push(`${added.length} line${added.length > 1 ? 's' : ''} added`);
        if (removed.length) parts.push(`${removed.length} line${removed.length > 1 ? 's' : ''} removed`);
        document.getElementById('collab-struct-msg').textContent = `Budget updated: ${parts.join(', ')}`;
        document.getElementById('collab-struct-toast').style.display = 'flex';
        setTimeout(() => {
          document.getElementById('collab-struct-toast').style.display = 'none';
          _structToastShown = false;
          _knownLineIds = incoming;
        }, 20000);
      }

      // Belt-and-suspenders: always poll for field changes even when socket is
      // connected. If socket emit was missed (proxy / WS upgrade issue / race),
      // polling catches up. Skipping polling when socket connects caused stale
      // data for other clients.

      // Silently patch changed lines. Per user 2026-04-28: previously this
      // only refreshed when subtotal differed, which missed pure-text edits
      // like payroll_co or fringe_type changes. Now also diffs the field
      // values returned by /live so non-financial edits sync too.
      let patched = 0;
      for (const [idStr, res] of Object.entries(d.lines)) {
        const id = parseInt(idStr);
        if (_myEditedLines.has(id)) continue;
        const row = document.querySelector(`.line-row[data-id="${id}"]`);
        if (!row) continue;
        const subEl = row.querySelector('.line-subtotal');
        const currentSub = subEl ? parseFloat(subEl.textContent.replace(/[$,]/g, '')) || 0 : -1;
        let needsRefresh = Math.abs(currentSub - res.subtotal) >= 0.005;
        if (!needsRefresh && res.fields) {
          // Compare each editable field's current DOM value to what /live
          // returned. Any mismatch flags the row for refresh.
          const f = res.fields;
          const _curText = (sel) => {
            const el = row.querySelector(sel);
            return el ? (el.textContent || '').trim() : '';
          };
          const _curSelect = (sel) => {
            const el = row.querySelector(sel);
            return el ? el.value : '';
          };
          const checks = [
            [String(parseFloat(f.rate || 0)),     _curText('.editable[data-field="rate"]').replace(/[^\d.-]/g, '')],
            [(f.payroll_co || ''),                _curText('.editable[data-field="payroll_co"]')],
            [(f.description || ''),               _curText('.editable[data-field="description"]')],
            [f.fringe_type || '',                 _curSelect('select[data-field="fringe_type"]')],
            [f.rate_type   || '',                 _curSelect('select[data-field="rate_type"]')],
          ];
          for (const [want, got] of checks) {
            // Permit empty-equivalent comparisons (—, blank).
            if ((want || '') !== (got || '')) {
              needsRefresh = true; break;
            }
          }
        }
        if (!needsRefresh) continue;
        refreshLineRow(id, res);
        patched++;
      }
      if (patched > 0) {
        document.querySelectorAll('.line-table').forEach(t => refreshSectionTotals(t));
        _flashSync();
      }
    })
    .catch(() => {});
}

// Mark lines I edit so they're not overwritten by remote patches during
// the brief window after save. Auto-clear after 4s so polling can catch up
// if another user edited the same line afterward.
document.addEventListener('change', e => {
  const row = e.target.closest('.line-row');
  if (!row) return;
  const id = parseInt(row.dataset.id);
  _myEditedLines.add(id);
  setTimeout(() => _myEditedLines.delete(id), 4000);
});

// Always run field-change polling as a backup — catches anything the socket
// emits miss (Render free tier / proxy issues / serialization failures).
// Presence: still socket-preferred; poll only when no socket connected.
setTimeout(function() {
  // Visibility gate (2026-05-29 perf): skip polls while the tab is hidden,
  // so background tabs (the user often has several budgets open) stop
  // hammering /live + /presence. On return to the tab, refresh immediately.
  const _vis = () => !document.hidden;
  if (!window._socket || !window._socket.connected) {
    console.log('[WS] No socket — using polling fallback');
    _collabPingPresence();
    setInterval(() => { if (_vis()) _collabPingPresence(); }, 20000);
  }
  // Poll regardless of socket — reliability net — but only when visible.
  _collabLivePatch();
  setInterval(() => { if (_vis()) _collabLivePatch(); }, 10000);
  document.addEventListener('visibilitychange', () => {
    if (_vis()) { _collabLivePatch(); if (!window._socket || !window._socket.connected) _collabPingPresence(); }
  });
}, 2000);
