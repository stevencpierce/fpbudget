// Extracted from a templates/budget.html inline block (audit M9, 2026-07-20).
// Jinja values arrive via window.__BJ (set by the inline preamble before this
// script tag). Classic script, document order — semantics identical to inline.

// Expose ctx line refs for all-in modal (set by the IIFE context menu handler)
let _ctxLineId  = null;
let _ctxLineRow = null;

// ── All-in Total Reverse Calculator ───────────────────────────────────────
// OT multiplier for a given rate_type: returns the factor by which a day rate
// must be divided to find the ST-only rate that produces a given all-in total.
// Formula: all_in_per_day = day_rate × OT_FACTOR
const _OT_FACTORS = {
  day_10:       1 + (2/8 * 1.5),        // 8 ST + 2 OT@1.5x  → 1.375
  day_8:        1.0,                     // 8 ST only          → 1.0
  day_12:       1 + (4/8 * 1.5),        // 8 ST + 4 OT@1.5x  → 1.75
  flat_day:     1.0,
  flat_project: 1.0,
  hourly:       1.0,
  custom:       1.0,
};
const _OT_LABELS = {
  day_10:       '8 ST hrs + 2 OT hrs @1.5×',
  day_8:        '8 ST hrs, no OT',
  day_12:       '8 ST hrs + 4 OT hrs @1.5×',
  flat_day:     'Flat rate, no OT',
  flat_project: 'Flat project rate',
  hourly:       'Hourly rate',
  custom:       'Custom rate',
};

let _allInComputedRate = null;

function allInRecalc() {
  const amountEl = document.getElementById('all-in-amount');
  const typeEl   = document.getElementById('all-in-type');
  const resultEl = document.getElementById('all-in-result');
  const applyBtn = document.getElementById('all-in-apply-btn');

  const amount = parseFloat(amountEl.value);
  if (!amount || amount <= 0 || !_ctxLineRow) {
    resultEl.style.display = 'none';
    applyBtn.disabled = true;
    return;
  }

  const rateType = _ctxLineRow.dataset.rateType || 'day_10';
  const qty      = parseFloat(_ctxLineRow.dataset.qty)  || 1;
  const days     = parseFloat(_ctxLineRow.dataset.days) || 1;
  const factor   = _OT_FACTORS[rateType] ?? 1.0;
  const totalType = typeEl.value;

  let perDayTotal, computedRate;
  if (totalType === 'day') {
    perDayTotal  = amount;
    computedRate = amount / factor;
  } else {
    // Project total: all_in = rate × qty × days × factor
    perDayTotal  = amount / (qty * days);
    computedRate = perDayTotal / factor;
  }

  _allInComputedRate = Math.round(computedRate * 100) / 100;

  const fmt = n => '$' + n.toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2});
  const otLabel = _OT_LABELS[rateType] || rateType;

  document.getElementById('all-in-rate-display').textContent = fmt(_allInComputedRate);
  document.getElementById('all-in-formula').textContent =
    `Rate type: ${rateType} (${otLabel}) · OT factor: ${factor.toFixed(4)}`;

  let breakdown = `Day rate ${fmt(_allInComputedRate)} × ${factor.toFixed(3)} = ${fmt(perDayTotal)}/day`;
  if (totalType === 'project') {
    breakdown += ` × ${qty} × ${days} days = ${fmt(amount)} total`;
  }
  document.getElementById('all-in-breakdown').textContent = breakdown;

  resultEl.style.display = 'block';
  applyBtn.disabled = false;
}

async function allInApply() {
  if (_allInComputedRate == null || !_ctxLineId) return;
  const applyBtn = document.getElementById('all-in-apply-btn');
  applyBtn.disabled = true;
  applyBtn.textContent = 'Applying…';

  const r = await fetch(SAVE_URL, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ id: _ctxLineId, rate: _allInComputedRate })
  });
  if (r.ok) {
    const d = await r.json();
    refreshLineRow(_ctxLineId, d);
    refreshSectionTotals(_ctxLineRow.closest('table'));
    document.getElementById('all-in-modal').classList.add('hidden');
  } else {
    applyBtn.disabled = false;
    applyBtn.textContent = 'Apply Rate';
    alert('Failed to save rate.');
  }
}
