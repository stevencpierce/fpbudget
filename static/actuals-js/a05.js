/* FPBudget Actuals workspace — a05: resizable panes and columns
   (2026-11-04).

   Drag the divider between the transaction list and the budget pane; drag
   a column edge in the list's header row. Widths persist per user in
   localStorage (every read / write in try/catch — a private window or
   blocked storage just means the defaults). Double-click a divider or a
   column edge to reset it. Keyboard: the divider is a separator — arrow
   keys move it 24 px, Home resets. */
(function () {
  'use strict';
  const AW = window.AW, AJ = window.__AJ || {};
  const $ = AW.$;
  const KEY = 'fpb.aw.' + (AJ.uid || 'anon') + '.';
  const store = {
    get(k) { try { const v = localStorage.getItem(KEY + k); return v ? JSON.parse(v) : null; } catch (e) { return null; } },
    set(k, v) { try { localStorage.setItem(KEY + k, JSON.stringify(v)); } catch (e) {} },
    del(k) { try { localStorage.removeItem(KEY + k); } catch (e) {} },
  };
  const root = $('aw');
  const right = document.querySelector('.aw-right');
  const divider = $('aw-divider');

  // ── Pane divider ─────────────────────────────────────────────────────
  const MIN_R = 300, MIN_L = 420;
  function applyPane(w) {
    if (!right) return;
    if (!w) { right.style.flex = ''; right.style.maxWidth = ''; right.style.width = ''; return; }
    const max = Math.max(MIN_R, window.innerWidth - MIN_L);
    w = Math.max(MIN_R, Math.min(max, Math.round(w)));
    right.style.flex = `0 0 ${w}px`; right.style.maxWidth = 'none'; right.style.width = w + 'px';
    if (divider) divider.setAttribute('aria-valuenow', String(w));
    return w;
  }
  const stacked = () => window.matchMedia('(max-width: 1100px)').matches;
  if (divider && right) {
    const saved = store.get('pane');
    if (saved && !stacked()) applyPane(saved);
    let startX = 0, startW = 0, dragging = false;
    divider.addEventListener('pointerdown', e => {
      if (stacked()) return;
      dragging = true; startX = e.clientX; startW = right.getBoundingClientRect().width;
      divider.setPointerCapture(e.pointerId); divider.classList.add('drag'); document.body.classList.add('aw-resizing');
      e.preventDefault();
    });
    divider.addEventListener('pointermove', e => { if (dragging) applyPane(startW - (e.clientX - startX)); });
    const end = () => {
      if (!dragging) return;
      dragging = false; divider.classList.remove('drag'); document.body.classList.remove('aw-resizing');
      store.set('pane', Math.round(right.getBoundingClientRect().width));
    };
    divider.addEventListener('pointerup', end);
    divider.addEventListener('pointercancel', end);
    divider.addEventListener('dblclick', () => { applyPane(null); store.del('pane'); });
    divider.addEventListener('keydown', e => {
      const w = right.getBoundingClientRect().width;
      if (e.key === 'ArrowLeft') { e.preventDefault(); store.set('pane', applyPane(w + 24)); }
      else if (e.key === 'ArrowRight') { e.preventDefault(); store.set('pane', applyPane(w - 24)); }
      else if (e.key === 'Home') { e.preventDefault(); applyPane(null); store.del('pane'); }
    });
    window.addEventListener('resize', () => { const s = store.get('pane'); if (s && !stacked()) applyPane(s); else if (stacked()) applyPane(null); });
  }

  // ── Column edges ─────────────────────────────────────────────────────
  // Each table kind keeps its own widths: the charge views share --aw-cols,
  // Receipts has --aw-rcols, Not project --aw-ncols.
  function kindOf(table) {
    if (table.classList.contains('receipts')) return 'rcols';
    if (table.classList.contains('not_project')) return 'ncols';
    if (table.classList.contains('duplicates') || table.classList.contains('sync')) return null;
    return 'cols';
  }
  function applyCols(kind, widths) {
    if (!kind) return;
    if (!widths) { root.style.removeProperty('--aw-' + kind); return; }
    root.style.setProperty('--aw-' + kind, widths.map((w, i) => i === widths.length - 1 ? `minmax(${w}px, 1fr)` : w + 'px').join(' '));
  }
  ['cols', 'rcols', 'ncols'].forEach(k => { const w = store.get(k); if (Array.isArray(w)) applyCols(k, w); });

  function decorate() {
    const table = $('aw-table'); if (!table) return;
    const head = table.querySelector('.aw-hrow'); if (!head || head.dataset.grips) return;
    const kind = kindOf(table); if (!kind) return;
    head.dataset.grips = '1';
    const cells = Array.from(head.children);
    cells.forEach((cell, i) => {
      if (i === cells.length - 1 || i === 0) return;       // the checkbox and the last column flex
      const g = document.createElement('span');
      g.className = 'aw-colgrip'; g.setAttribute('aria-hidden', 'true');
      g.title = 'Drag to resize · double-click to reset';
      cell.style.position = 'relative';
      cell.appendChild(g);
      let sx = 0, widths = null, dragging = false;
      g.addEventListener('pointerdown', e => {
        dragging = true; sx = e.clientX;
        widths = cells.map(c => Math.round(c.getBoundingClientRect().width));
        g.setPointerCapture(e.pointerId); document.body.classList.add('aw-resizing');
        e.preventDefault(); e.stopPropagation();
      });
      g.addEventListener('pointermove', e => {
        if (!dragging) return;
        const w = widths.slice(); w[i] = Math.max(48, widths[i] + (e.clientX - sx));
        applyCols(kind, w); g._w = w;
      });
      const end = () => { if (!dragging) return; dragging = false; document.body.classList.remove('aw-resizing'); if (g._w) store.set(kind, g._w); };
      g.addEventListener('pointerup', end);
      g.addEventListener('pointercancel', end);
      g.addEventListener('click', e => e.stopPropagation());
      g.addEventListener('dblclick', e => { e.stopPropagation(); applyCols(kind, null); store.del(kind); });
    });
  }
  const table = $('aw-table');
  if (table) new MutationObserver(decorate).observe(table, { childList: true });
  decorate();
  AW.resetLayout = () => { applyPane(null); store.del('pane'); ['cols', 'rcols', 'ncols'].forEach(k => { applyCols(k, null); store.del(k); }); };
})();
