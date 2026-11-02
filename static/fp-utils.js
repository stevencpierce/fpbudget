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
})();
