// Extracted from a templates/budget.html inline block (audit M9, 2026-07-20).
// Jinja values arrive via window.__BJ (set by the inline preamble before this
// script tag). Classic script, document order — semantics identical to inline.

// ── Schedule-driven line right-click (Craft Services, meals, etc.) ──────────
(function() {
  const _pid = window.__BJ["b07__pid"];
  const _bid = window.__BJ["b07__bid"];
  const menu = document.getElementById('sync-line-ctx-menu');
  const btn  = document.getElementById('ctx-toggle-sync-omit');
  let _row = null;

  function _showAt(row, x, y) {
    _row = row;
    const omitted = row.dataset.syncOmit === '1';
    btn.textContent = omitted ? '↺ Re-enable auto-calc' : '⊘ Omit from auto-calc';
    menu.style.visibility = 'hidden';
    menu.style.left = '0px'; menu.style.top = '0px';
    menu.classList.remove('hidden');
    requestAnimationFrame(() => {
      const mw = menu.offsetWidth, mh = menu.offsetHeight;
      menu.style.left = Math.max(4, Math.min(x + 4, window.innerWidth  - mw - 12)) + 'px';
      menu.style.top  = Math.max(4, Math.min(y + 4, window.innerHeight - mh - 12)) + 'px';
      menu.style.visibility = '';
    });
  }

  document.addEventListener('contextmenu', function(e) {
    const row = e.target.closest('.sync-line');
    if (!row || row.classList.contains('labor-line')) { menu.classList.add('hidden'); return; }
    e.preventDefault();
    _showAt(row, e.clientX, e.clientY);
  });

  // Long-press for touch
  let _lpTimer = null, _lpRow = null, _lpX = 0, _lpY = 0;
  document.addEventListener('touchstart', function(e) {
    const row = e.target.closest('.sync-line');
    if (!row || row.classList.contains('labor-line') || e.target.closest('.editable')) return;
    const t = e.touches[0]; _lpX = t.clientX; _lpY = t.clientY; _lpRow = row;
    _lpTimer = setTimeout(() => { if (_lpRow) _showAt(_lpRow, _lpX, _lpY); }, 500);
  }, { passive: true });
  document.addEventListener('touchend',  () => { clearTimeout(_lpTimer); _lpTimer = null; });
  document.addEventListener('touchmove', () => { clearTimeout(_lpTimer); _lpTimer = null; });

  document.addEventListener('click', function(e) {
    if (!e.target.closest('#sync-line-ctx-menu')) menu.classList.add('hidden');
  });

  btn.addEventListener('click', async function() {
    menu.classList.add('hidden');
    if (!_row) return;
    const lid = parseInt(_row.dataset.id);
    const res = await fetch(`/projects/${_pid}/budget/${_bid}/line/${lid}/toggle-sync-omit`, { method: 'POST' });
    if (!res.ok) { alert('Failed to update line'); return; }
    const data = await res.json();
    _row.dataset.syncOmit = data.sync_omit ? '1' : '0';
    _row.classList.toggle('sync-omitted', data.sync_omit);
    // Reload to reflect recalculated totals
    window.location.reload();
  });
})();
