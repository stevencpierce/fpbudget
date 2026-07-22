// Extracted from a templates/budget.html inline block (audit M9, 2026-07-20).
// Jinja values arrive via window.__BJ (set by the inline preamble before this
// script tag). Classic script, document order — semantics identical to inline.

// ── Realtime Collaboration ─────────────────────────────────────────────────
const _PRESENCE_URL = `/projects/${PID}/budget/${BID}/presence`;
const _LIVE_URL     = `/projects/${PID}/budget/${BID}/live`;

// IDs of lines I edited this session — skip patching these so my own edits aren't overwritten
const _myEditedLines = new Set();

// Snapshot of line_ids at page load (for structural-change detection)
let _knownLineIds = null;
let _structToastShown = false;

// ── Avatar helpers ───────────────────────────────────────────────────────────
function _renderViewers(viewers) {
  const el = document.getElementById('collab-viewers');
  if (!el) return;
  const myId = window.__BJ["b10_myId"];
  const others = viewers.filter(v => v.user_id !== myId);
  const MAX_SHOW = 5;
  const visible = others.slice(0, MAX_SHOW);
  const overflow = others.length - MAX_SHOW;
  let html = visible.map(v => {
    const initials = v.user_name.split(' ').map(w => w[0]).join('').slice(0, 2).toUpperCase();
    return `<span class="collab-avatar" title="${v.user_name} is viewing" style="background:${v.color || '#2563eb'} !important">${initials}</span>`;
  }).join('');
  if (overflow > 0) {
    html += `<span class="collab-avatar collab-overflow" title="${overflow} more viewer${overflow > 1 ? 's' : ''}">+${overflow}</span>`;
  }
  el.innerHTML = html;
}

function _flashSync() {
  const flash = document.getElementById('collab-sync-flash');
  if (flash) {
    flash.textContent = '● synced';
    flash.style.opacity = '1';
    setTimeout(() => { flash.style.opacity = '0'; }, 1800);
  }
}

function _timeAgo(d) {
  const s = Math.floor((Date.now() - d) / 1000);
  if (s < 10) return 'just now';
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s/60)}m ago`;
  return `${Math.floor(s/3600)}h ago`;
}

// window._socket is set by collab.js
