/* FPBudget shared page utilities — the ONE copy (Cleanup D, 2026-11-02).
 *
 * Loaded by base.html before every other script. Every inline block and
 * static module that used to carry its own `esc` / `money` keeps its local
 * name but aliases it to these, so one rule renders an HTML string safely
 * and one rule formats a dollar amount.
 *
 *   FP.esc(s)              HTML-escape for text AND attribute contexts:
 *                          & < > " ' (three of the old copies escaped only
 *                          '<', which is unsafe inside a quoted attribute).
 *   FP.money(v, opts)      '$1,234.56'; a negative amount is '-$1,234.56'
 *                          (sign before the dollar). opts:
 *                            empty    what null / '' / NaN render as ('—')
 *                            abs      format the absolute value
 *                            decimals fraction digits (2)
 *   FP.whole(v)            '1,234' with the sign, no dollar, no decimals.
 */
(function () {
  'use strict';
  var MAP = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) { return MAP[c]; });
  }

  function money(v, opts) {
    opts = opts || {};
    var empty = ('empty' in opts) ? opts.empty : '—';
    if (v == null || v === '') return empty;
    var n = Number(v);
    if (isNaN(n)) return empty;
    if (opts.abs) n = Math.abs(n);
    var dec = (opts.decimals == null) ? 2 : opts.decimals;
    var body = Math.abs(n).toLocaleString('en-US', { minimumFractionDigits: dec, maximumFractionDigits: dec });
    return (n < 0 ? '-' : '') + '$' + body;
  }

  function whole(v) {
    var n = Math.round(Number(v || 0));
    if (isNaN(n)) n = 0;
    return (n < 0 ? '-' : '') + Math.abs(n).toLocaleString('en-US');
  }

  window.FP = window.FP || {};
  window.FP.esc = esc;
  window.FP.money = money;
  window.FP.whole = whole;

  // The "AI Agent" badge (2026-11-03): every place that shows WHO did
  // something appends FP.aiBadge(flag) after the name, where flag is the
  // server's *_ai_agent boolean. Server-rendered pages use the Jinja
  // ai_badge(user) global — same markup, same .ai-agent-badge class.
  function aiBadge(flag) {
    if (!flag) return '';
    return '<span class="ai-agent-badge" title="Action taken by the AI assistant (Claude)">'
      + '<span aria-hidden="true">\u2726</span> AI Agent</span>';
  }
  window.FP.aiBadge = aiBadge;
})();

/* ── Project navigation: the "More ▾" menu (templates/_project_nav.html) ──
 * One controller for every page that carries the strip. The menu is
 * positioned FIXED under its button on open, so the strip's sideways
 * scroll container (iPad widths) never clips it. On the budget page the
 * menu items are in-page <button data-tab> tabs; when one of them is the
 * active pane the More button lights up and reads "More · <pane>".
 */
(function () {
  'use strict';
  function initTabMore() {
    var btn  = document.getElementById('tab-more-btn');
    var menu = document.getElementById('tab-more-menu');
    if (!btn || !menu || btn.dataset.fpBound) return;
    btn.dataset.fpBound = '1';
    var label = btn.querySelector('.tab-more-label');
    function sync() {
      var active = menu.querySelector('.tab-btn.active');
      btn.classList.toggle('active', !!active);
      if (label) label.textContent = active ? ('More · ' + active.textContent.replace(/\d+$/, '').trim()) : 'More';
    }
    function place() {
      var r = btn.getBoundingClientRect();
      menu.style.position = 'fixed';
      menu.style.top = (r.bottom + 2) + 'px';
      var w = menu.offsetWidth || 420;
      menu.style.left = Math.max(8, Math.min(r.left, window.innerWidth - w - 8)) + 'px';
    }
    function close() { menu.hidden = true; btn.setAttribute('aria-expanded', 'false'); }
    function open() {
      menu.hidden = false; btn.setAttribute('aria-expanded', 'true'); place();
      var first = menu.querySelector('[role="menuitem"]'); if (first) first.focus();
    }
    btn.addEventListener('click', function (e) { e.stopPropagation(); if (menu.hidden) open(); else close(); });
    document.addEventListener('click', function (e) { if (!e.target.closest('.tab-more-wrap')) close(); });
    window.addEventListener('resize', close);
    window.addEventListener('scroll', close, true);
    document.addEventListener('keydown', function (e) {
      if (menu.hidden) return;
      if (e.key === 'Escape') { close(); btn.focus(); return; }
      if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
        var items = Array.prototype.slice.call(menu.querySelectorAll('[role="menuitem"]'));
        var i = items.indexOf(document.activeElement);
        e.preventDefault();
        (e.key === 'ArrowDown' ? items[(i + 1) % items.length] : items[(i - 1 + items.length) % items.length]).focus();
      }
    });
    Array.prototype.forEach.call(document.querySelectorAll('.project-nav .tab-btn[data-tab]'), function (b) {
      b.addEventListener('click', function () { close(); setTimeout(sync, 0); });
    });
    setTimeout(sync, 0);
  }
  window.FP = window.FP || {};
  window.FP.initTabMore = initTabMore;
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', initTabMore);
  else initTabMore();
})();

/* ── Folder-aware file drops (owner 2026-11-03: "if I drag in a folder or
 * multiple folders … brings all the images in") ─────────────────────────
 *
 *   FP.collectDroppedFiles(dataTransfer) -> Promise<File[]>
 *     Walks dropped folders recursively (webkitGetAsEntry), falls back to
 *     dataTransfer.files, skips hidden / system files and anything the
 *     server would reject (FP.DOC_EXTS mirrors app.py _UPLOAD_ALLOW
 *     ['document']). Each File carries .fpRelativePath ("Day 1/a.jpg").
 *   FP.filterDocFiles(files)  the same filter for a FileList / array (used
 *     for the "Choose folder…" <input webkitdirectory> picker).
 *   FP.isDocFile(name)        one name → accepted or not.
 */
(function () {
  'use strict';
  var DOC_EXTS = ['pdf', 'jpg', 'jpeg', 'png', 'heic', 'heif', 'webp', 'tif', 'tiff',
                  'bmp', 'gif', 'docx', 'xlsx', 'csv', 'txt', 'eml'];
  var HIDDEN = /^(\.|~\$|thumbs\.db$|desktop\.ini$)/i;

  function isDocFile(name) {
    name = String(name || '');
    var base = name.split('/').pop();
    if (!base || HIDDEN.test(base)) return false;
    var ext = base.indexOf('.') >= 0 ? base.split('.').pop().toLowerCase() : '';
    return DOC_EXTS.indexOf(ext) >= 0;
  }

  function filterDocFiles(list) {
    var out = [];
    Array.prototype.forEach.call(list || [], function (f) {
      if (!f || !f.name) return;
      var rel = f.fpRelativePath || f.webkitRelativePath || f.name;
      // A folder picker reports hidden files inside hidden folders too.
      var parts = String(rel).split('/');
      for (var i = 0; i < parts.length; i++) { if (HIDDEN.test(parts[i])) return; }
      if (!isDocFile(f.name)) return;
      try { f.fpRelativePath = rel; } catch (e) { /* read-only File in some engines */ }
      out.push(f);
    });
    return out;
  }

  function readEntries(reader) {
    // readEntries returns at most ~100 entries per call; drain it.
    return new Promise(function (resolve) {
      var all = [];
      (function step() {
        reader.readEntries(function (batch) {
          if (!batch || !batch.length) { resolve(all); return; }
          all = all.concat(Array.prototype.slice.call(batch));
          step();
        }, function () { resolve(all); });
      })();
    });
  }

  function walkEntry(entry, prefix, out) {
    if (!entry) return Promise.resolve();
    if (HIDDEN.test(entry.name || '')) return Promise.resolve();
    if (entry.isFile) {
      return new Promise(function (resolve) {
        entry.file(function (f) {
          try { f.fpRelativePath = prefix + f.name; } catch (e) {}
          out.push(f); resolve();
        }, function () { resolve(); });
      });
    }
    if (entry.isDirectory) {
      return readEntries(entry.createReader()).then(function (kids) {
        var p = Promise.resolve();
        kids.forEach(function (k) { p = p.then(function () { return walkEntry(k, prefix + entry.name + '/', out); }); });
        return p;
      });
    }
    return Promise.resolve();
  }

  function collectDroppedFiles(dt) {
    if (!dt) return Promise.resolve([]);
    var items = dt.items ? Array.prototype.slice.call(dt.items) : [];
    var entries = [];
    items.forEach(function (it) {
      if (it.kind !== 'file') return;
      var en = null;
      try { en = it.webkitGetAsEntry ? it.webkitGetAsEntry() : (it.getAsEntry ? it.getAsEntry() : null); } catch (e) { en = null; }
      if (en) entries.push(en);
    });
    if (!entries.length) return Promise.resolve(filterDocFiles(dt.files));
    var out = [];
    var p = Promise.resolve();
    entries.forEach(function (en) { p = p.then(function () { return walkEntry(en, '', out); }); });
    return p.then(function () {
      // A browser that handed us entries but no files (older Safari) still
      // has the flat list; never come back empty when something was dropped.
      if (!out.length && dt.files && dt.files.length) return filterDocFiles(dt.files);
      return filterDocFiles(out);
    });
  }

  window.FP = window.FP || {};
  window.FP.DOC_EXTS = DOC_EXTS;
  window.FP.isDocFile = isDocFile;
  window.FP.filterDocFiles = filterDocFiles;
  window.FP.collectDroppedFiles = collectDroppedFiles;
})();
