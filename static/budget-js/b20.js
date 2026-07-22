// Extracted from a templates/budget.html inline block (audit M9, 2026-07-20).
// Jinja values arrive via window.__BJ (set by the inline preamble before this
// script tag). Classic script, document order — semantics identical to inline.

(function () {
  var orig = window.openLineLedger;
  if (typeof orig === 'function') {
    window.openLineLedger = function (lid) { window._llLineId = lid; return orig(lid); };
  }
  document.addEventListener('click', function (ev) {
    var td = ev.target.closest ? ev.target.closest('td.line-actual[data-acell-line]') : null;
    if (!td) return;
    if (ev.target.closest('a,button,select,input,[contenteditable="true"]')) return;
    var lid = parseInt(td.getAttribute('data-acell-line'));
    if (lid && typeof window.openLineLedger === 'function') window.openLineLedger(lid);
  });
  document.querySelectorAll('td.line-actual[data-acell-line]').forEach(function (td) {
    if (!td.title) td.title = 'Click: open this line\u2019s ledger (every charge coded here, with receipts)';
  });
})();
