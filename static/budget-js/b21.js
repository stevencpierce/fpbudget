// The ONE float-bar updater (owner 2026-11-04). Classic script, document
// order — loaded right after #budget-float-bar in templates/budget.html.
//
// The bar shows, always and labeled, the three grand totals (Estimated ·
// Working · Actual), the viewed budget's Subtotal + Prod. Fee breakdown, and
// ONE Variance item labeled with the basis chosen at the top of the Budget
// tab. Every writer — the line editors' updateFloatBar, applyFeeDisplay,
// refreshActualCells, the variance control — calls
//
//     FP.budgetFloatBar.update({ estimated, working, actual, subtotal, fee,
//                                basis, view, feeText })
//
// with only the keys it knows, and this module renders. Nothing else writes
// the bar's spans, so the inline writers cannot diverge. The initial values
// come from the data-* attributes the template renders (the same numbers the
// Top Sheet tfoot shows); the variance math is window.computeVariance — the
// ONE rule the Top Sheet and the grid use (Cleanup C) — never a private copy.
(function () {
  'use strict';
  var FP = window.FP = window.FP || {};

  // The control's exact wording (Cleanup C keys).
  var BASIS_LABEL = {
    vs_estimated:   'Actual vs Estimated',
    vs_working:     'Actual vs Working',
    est_to_working: 'Estimated → Working'
  };
  var VIEW_LABEL = { estimated: 'Estimated', working: 'Working' };

  var state = {
    estimated: 0,
    working: null,      // null = no Working budget exists
    actual: 0,
    subtotal: 0,
    fee: 0,
    feeText: '',        // "10.0% · Dispersed" — the viewed budget's fee setting
    basis: null,        // vs_estimated | vs_working | est_to_working
    view: 'estimated',  // estimated | working — the VIEWING switch
    frozen: false
  };

  function num(v) { var n = parseFloat(v); return isNaN(n) ? 0 : n; }
  function money(v) { return (FP.money ? FP.money(v) : String(v)); }
  function el(id) { return document.getElementById(id); }
  function bar() { return el('budget-float-bar'); }

  function init() {
    var b = bar();
    if (!b) return;
    var d = b.dataset;
    state.estimated = num(d.estimated);
    state.working   = d.hasWorking === '1' ? num(d.working) : null;
    state.actual    = num(d.actual);
    state.subtotal  = num(d.subtotal);
    state.fee       = num(d.fee);
    state.feeText   = d.feeText || '';
    state.view      = d.view === 'working' ? 'working' : 'estimated';
    state.frozen    = d.frozen === '1';
    state.basis     = d.basis || null;
    render();
  }

  function variance() {
    // null = cannot be computed (no basis yet, or the basis needs a Working
    // that does not exist).
    if (!state.basis) return null;
    if (state.working == null && state.basis !== 'vs_estimated') return null;
    var compute = window.computeVariance;
    if (typeof compute !== 'function') return null;
    return compute(state.basis, state.estimated, state.working || 0, state.actual);
  }

  function render() {
    var b = bar();
    if (!b) return;
    b.dataset.view = state.view;
    if (state.basis) b.dataset.basis = state.basis;
    var viewName = VIEW_LABEL[state.view] || 'Estimated';

    var subLbl = el('float-subtotal-label');
    if (subLbl) subLbl.textContent = 'Subtotal · ' + viewName;
    var subEl = el('float-subtotal');
    if (subEl) subEl.textContent = money(state.subtotal);

    var feeLbl = el('float-fee-label');
    if (feeLbl) feeLbl.textContent = 'Prod. Fee · ' + viewName + (state.feeText ? ' (' + state.feeText + ')' : '');
    var feeEl = el('float-fee');
    if (feeEl) feeEl.textContent = money(state.fee);

    var estEl = el('float-estimated');
    if (estEl) estEl.textContent = money(state.estimated);
    var wEl = el('float-working');
    var wItem = el('float-total-working');
    if (wEl) wEl.textContent = state.working == null ? '— no Working' : money(state.working);
    if (wItem) wItem.classList.toggle('fb-empty', state.working == null);
    var aEl = el('float-actual');
    if (aEl) aEl.textContent = money(state.actual);

    // Highlight the total that matches the VIEWING switch.
    b.querySelectorAll('.float-bar-total[data-mode]').forEach(function (item) {
      item.classList.toggle('fb-current', item.dataset.mode === state.view);
    });

    // Variance, labeled with the basis currently chosen at the top.
    var vLbl = el('float-variance-label');
    if (vLbl) vLbl.textContent = 'Variance' + (state.basis && BASIS_LABEL[state.basis] ? ' · ' + BASIS_LABEL[state.basis] : '');
    var vEl = el('float-variance');
    if (vEl) {
      var v = variance();
      vEl.classList.remove('fb-var-pos', 'fb-var-neg', 'fb-var-zero', 'fb-var-na');
      if (v == null) {
        vEl.textContent = '—';
        vEl.classList.add('fb-var-na');
        vEl.title = (state.working == null && state.basis && state.basis !== 'vs_estimated') ? 'No Working budget to compare against' : '';
      } else {
        vEl.textContent = money(v);
        vEl.classList.add(Math.abs(v) < 0.005 ? 'fb-var-zero' : (v > 0 ? 'fb-var-pos' : 'fb-var-neg'));
        vEl.title = '';
      }
    }
    syncBodyPadding();
  }

  // The bar may wrap to two rows on narrower screens (iPad 1024): keep the
  // page's bottom padding equal to the bar's real height so the last table
  // rows are never covered.
  function syncBodyPadding() {
    var b = bar();
    if (!b) return;
    if (b.classList.contains('hidden')) { document.body.style.paddingBottom = ''; return; }
    var h = b.offsetHeight || 56;
    document.body.style.paddingBottom = (h + 12) + 'px';
  }

  function update(patch) {
    if (patch && typeof patch === 'object') {
      Object.keys(patch).forEach(function (k) {
        if (!(k in state)) return;
        var v = patch[k];
        if (k === 'working') state.working = (v == null ? null : num(v));
        else if (k === 'basis' || k === 'view' || k === 'feeText') state[k] = v;
        else if (k === 'frozen') state.frozen = !!v;
        else state[k] = num(v);
      });
    }
    render();
  }

  FP.budgetFloatBar = {
    update: update,
    render: render,
    syncBodyPadding: syncBodyPadding,
    get: function () { return Object.assign({}, state); }
  };

  if (typeof ResizeObserver === 'function' && bar()) {
    try { new ResizeObserver(syncBodyPadding).observe(bar()); } catch (_) {}
  }
  window.addEventListener('resize', syncBodyPadding);

  init();
})();
