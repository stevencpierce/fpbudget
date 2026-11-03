// Extracted from a templates/budget.html inline block (audit M9, 2026-07-20).
// Jinja values arrive via window.__BJ (set by the inline preamble before this
// script tag). Classic script, document order — semantics identical to inline.

// ── Schedule-driven line: omit from / re-enable auto-calc ───────────────────
// The separate right-click menu this used to own (#sync-line-ctx-menu) is
// gone (2026-11-03): the action is an item of the ONE row menu
// (#line-row-menu, b12.js), shown on schedule-driven non-labor rows only.
(function() {
  const _pid = window.__BJ["b07__pid"];
  const _bid = window.__BJ["b07__bid"];

  window.toggleSyncOmitFor = async function(lineId) {
    const row = document.querySelector(`.line-row[data-id="${lineId}"]`);
    if (!row) return;
    const res = await fetch(`/projects/${_pid}/budget/${_bid}/line/${lineId}/toggle-sync-omit`, { method: 'POST' });
    if (!res.ok) { alert('Failed to update line'); return; }
    const data = await res.json();
    row.dataset.syncOmit = data.sync_omit ? '1' : '0';
    row.classList.toggle('sync-omitted', data.sync_omit);
    // Reload to reflect recalculated totals
    window.location.reload();
  };
})();
