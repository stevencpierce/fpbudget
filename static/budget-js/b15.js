// Extracted from a templates/budget.html inline block (audit M9, 2026-07-20).
// Jinja values arrive via window.__BJ (set by the inline preamble before this
// script tag). Classic script, document order — semantics identical to inline.

/* ── Budget Gut Check panel ──────────────────────────────────────────────
   Fetches the read-only /audit.json diagnostic and renders Summary / Lines /
   Sections / Drift tabs. Super-admin only; the trigger + markup are gated in
   the template so this script no-ops for everyone else. */
(function () {
  const trigger = document.getElementById('gc-trigger');
  const overlay = document.getElementById('gc-overlay');
  if (!trigger || !overlay) return;

  const content = document.getElementById('gc-content');
  const statusEl = document.getElementById('gc-status');
  let DATA = null;
  let TAB = 'summary';

  const fmtMoney = (n) => {
    const v = Number(n || 0);
    return (v < 0 ? '-$' : '$') + Math.abs(v).toLocaleString('en-US',
      { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  };
  const esc = (s) => String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const within = (d, tol) => Math.abs(Number(d || 0)) < (tol || 0.01);

  function open() {
    overlay.classList.add('gc-open');
    if (!DATA) run();
  }
  function close() { overlay.classList.remove('gc-open'); }

  trigger.addEventListener('click', open);
  document.getElementById('gc-close').addEventListener('click', close);
  overlay.addEventListener('click', (e) => { if (e.target === overlay) close(); });
  document.querySelectorAll('.gc-tab').forEach((b) => {
    b.addEventListener('click', () => {
      TAB = b.dataset.tab;
      document.querySelectorAll('.gc-tab').forEach((x) =>
        x.classList.toggle('gc-active', x === b));
      render();
    });
  });

  async function run() {
    content.innerHTML = '<p class="gc-note">Running audit…</p>';
    statusEl.textContent = '';
    statusEl.className = 'gc-status';
    try {
      const res = await fetch(`/projects/${PID}/budget/${BID}/audit.json`,
        { headers: { 'Accept': 'application/json' } });
      if (!res.ok) throw new Error('Server returned ' + res.status);
      DATA = await res.json();
    } catch (e) {
      content.innerHTML = '<p class="gc-note" style="color:#ef4444">Audit failed: '
        + esc(e.message) + '</p>';
      return;
    }
    const s = DATA.summary;
    statusEl.textContent = s.status === 'PASS' ? '● ALL CHECKS PASS' : '● ERRORS FOUND';
    statusEl.className = 'gc-status ' + (s.status === 'PASS' ? 'gc-pass' : 'gc-fail');
    render();
  }

  function stat(label, value, color) {
    return '<div class="gc-stat"><div class="gc-l">' + esc(label) + '</div>'
      + '<div class="gc-v"' + (color ? ' style="color:' + color + '"' : '') + '>'
      + value + '</div></div>';
  }

  function render() {
    if (!DATA) return;
    if (TAB === 'summary') return renderSummary();
    if (TAB === 'lines') return renderLines();
    if (TAB === 'sections') return renderSections();
    if (TAB === 'drift') return renderDrift();
  }

  function renderSummary() {
    const s = DATA.summary, g = s.grand, tol = DATA.tolerance;
    const ec = s.error_counts, wc = s.warning_counts;
    const totalWarn = Object.values(wc).reduce((a, b) => a + b, 0);
    let h = '';
    h += '<div class="gc-note">' + esc(DATA.budget_name) + ' · mode <b>'
      + esc(DATA.budget_mode) + '</b>' + (DATA.is_actual ? ' · actual' : '')
      + ' · tolerance ' + fmtMoney(tol) + '</div>';
    h += '<div class="gc-grid">';
    h += stat('Total Lines', s.total_lines);
    h += stat('Clean', s.lines_ok, '#22c55e');
    h += stat('Lines w/ Errors', s.lines_with_errors, s.lines_with_errors ? '#ef4444' : null);
    h += stat('Lines w/ Warnings', s.lines_with_warnings, s.lines_with_warnings ? '#f59e0b' : null);
    h += stat('Grand (engine)', fmtMoney(g.engine_grand));
    h += stat('Grand (recomputed)', fmtMoney(g.indep_grand));
    h += stat('Grand Δ', fmtMoney(g.grand_delta), within(g.grand_delta, tol) ? '#22c55e' : '#ef4444');
    h += stat('Section Errors', s.section_errors, s.section_errors ? '#ef4444' : null);
    h += '</div>';

    h += '<table class="gc-table"><thead><tr><th>Roll-up reconciliation</th>'
      + '<th class="gc-r">Engine</th><th class="gc-r">Recomputed</th>'
      + '<th class="gc-r">Δ</th><th></th></tr></thead><tbody>';
    const recon = [
      ['Section subtotal', g.engine_subtotal, g.indep_subtotal, g.subtotal_delta],
      ['Prod. company fee (' + (g.fee_mode === 'flat' ? 'flat' : (g.fee_pct * 100).toFixed(1) + '%') + ')',
        g.engine_fee, g.indep_fee, g.fee_delta],
      ['Grand total', g.engine_grand, g.indep_grand, g.grand_delta],
    ];
    recon.forEach((r) => {
      const ok = within(r[3], tol);
      h += '<tr class="' + (ok ? '' : 'gc-row-fail') + '"><td>' + esc(r[0]) + '</td>'
        + '<td class="gc-r">' + fmtMoney(r[1]) + '</td>'
        + '<td class="gc-r">' + fmtMoney(r[2]) + '</td>'
        + '<td class="gc-r">' + fmtMoney(r[3]) + '</td>'
        + '<td><span class="gc-badge ' + (ok ? 'ok' : 'fail') + '">'
        + (ok ? '✓' : '✗') + '</span></td></tr>';
    });
    h += '<tr><td class="gc-sub">Gross labor wages</td><td class="gc-r gc-sub" colspan="4">'
      + fmtMoney(g.gross_labor_wages) + ' · WC ' + fmtMoney(g.workers_comp_amount)
      + ' · Payroll fee ' + fmtMoney(g.payroll_fee_amount)
      + ' · Prod. insurance ' + fmtMoney(g.production_insurance_amount) + '</td></tr>';
    h += '</tbody></table>';

    const errTotal = Object.values(ec).reduce((a, b) => a + b, 0);
    if (errTotal) {
      h += '<div class="gc-warn-box" style="background:#2a1414;border-color:#ef4444;color:#fca5a5">'
        + '⚠️ Math errors: ' + errLabels(ec) + '</div>';
    }
    if (totalWarn) {
      h += '<div class="gc-warn-box">⚠️ ' + totalWarn
        + ' warning(s): ' + errLabels(wc) + '. See "Drift &amp; Flags".</div>';
    }
    if (!errTotal && !totalWarn && within(g.grand_delta, tol)) {
      h += '<div class="gc-warn-box" style="background:#052e16;border-color:#15803d;color:#4ade80">'
        + '✓ Every line, section, and the grand total reconcile within tolerance.</div>';
    }
    content.innerHTML = h;
  }

  function errLabels(obj) {
    return Object.entries(obj).filter(([, v]) => v > 0)
      .map(([k, v]) => v + '× ' + k.replace(/_/g, ' ')).join(', ');
  }

  function checkCell(line) {
    const failed = line.checks.filter((c) => !c.pass);
    if (failed.length) {
      return '<span class="gc-badge fail">✗ ' + failed.map((c) => esc(c.name)).join(', ')
        + '</span>';
    }
    return '<span class="gc-badge ok">✓</span>';
  }

  function renderLines() {
    const rows = DATA.lines;
    let h = '<div class="gc-note">' + rows.length + ' lines · '
      + 'engine output vs independent recompute. Failing checks highlighted.</div>';
    h += '<table class="gc-table"><thead><tr>'
      + '<th>Acct</th><th>Description</th><th>Type</th>'
      + '<th class="gc-r">Subtotal</th><th class="gc-r">Fringe</th>'
      + '<th class="gc-r">Agent</th><th class="gc-r">Total</th>'
      + '<th>Checks</th></tr></thead><tbody>';
    rows.forEach((l) => {
      const cls = !l.ok ? 'gc-row-fail' : (l.has_warnings ? 'gc-row-warn' : '');
      const type = l.is_labor ? (l.used_schedule ? 'labor·sched' : 'labor') : 'non-labor';
      h += '<tr class="' + cls + '">'
        + '<td>' + esc(l.account_code) + '</td>'
        + '<td>' + esc(l.description || l.account_name) + '</td>'
        + '<td class="gc-sub">' + type + '</td>'
        + '<td class="gc-r">' + fmtMoney(l.subtotal) + '</td>'
        + '<td class="gc-r">' + fmtMoney(l.fringe_amount) + '</td>'
        + '<td class="gc-r">' + fmtMoney(l.agent_amount) + '</td>'
        + '<td class="gc-r">' + fmtMoney(l.total) + '</td>'
        + '<td>' + checkCell(l) + '</td></tr>';
      const failed = l.checks.filter((c) => !c.pass);
      failed.forEach((c) => {
        h += '<tr class="gc-row-fail"><td></td><td colspan="7" class="gc-sub">'
          + '↳ ' + esc(c.label) + ': expected ' + fmtMoney(c.expected)
          + ', got ' + fmtMoney(c.got) + ' (Δ ' + fmtMoney(c.delta) + ')</td></tr>';
      });
    });
    h += '</tbody></table>';
    content.innerHTML = h;
  }

  function renderSections() {
    const rows = DATA.sections;
    let h = '<div class="gc-note">Independent sum of line totals (plus injected '
      + 'WC / payroll fee / insurance) vs the Top Sheet section totals.</div>';
    h += '<table class="gc-table"><thead><tr>'
      + '<th>Code</th><th>Section</th><th class="gc-r">Top Sheet</th>'
      + '<th class="gc-r">Recomputed</th><th class="gc-r">Δ</th>'
      + '<th>Fee?</th><th></th></tr></thead><tbody>';
    rows.forEach((r) => {
      h += '<tr class="' + (r.pass ? '' : 'gc-row-fail') + '">'
        + '<td>' + esc(r.code) + '</td><td>' + esc(r.name) + '</td>'
        + '<td class="gc-r">' + fmtMoney(r.engine_raw) + '</td>'
        + '<td class="gc-r">' + fmtMoney(r.indep_total) + '</td>'
        + '<td class="gc-r">' + fmtMoney(r.delta) + '</td>'
        + '<td class="gc-sub">' + (r.fee_applies ? 'yes' : 'exempt') + '</td>'
        + '<td><span class="gc-badge ' + (r.pass ? 'ok' : 'fail') + '">'
        + (r.pass ? '✓' : '✗') + '</span></td></tr>';
    });
    h += '</tbody></table>';
    content.innerHTML = h;
  }

  function renderDrift() {
    const flagged = DATA.lines.filter((l) => l.warnings && l.warnings.length);
    if (!flagged.length) {
      content.innerHTML = '<p class="gc-note">No drift or structural flags. '
        + 'Stored snapshots match the live calc and no anomalies found.</p>';
      return;
    }
    let h = '<div class="gc-note">' + flagged.length
      + ' line(s) with snapshot drift or structural flags. '
      + 'Drift = live calc moved away from a stored snapshot (not necessarily a bug).</div>';
    h += '<table class="gc-table"><thead><tr>'
      + '<th>Acct</th><th>Description</th><th>Flag</th>'
      + '<th class="gc-r">Stored</th><th class="gc-r">Live</th>'
      + '<th class="gc-r">Δ</th></tr></thead><tbody>';
    flagged.forEach((l) => {
      l.warnings.forEach((w, i) => {
        const hasNums = (w.stored != null);
        h += '<tr class="gc-row-warn">'
          + '<td>' + (i === 0 ? esc(l.account_code) : '') + '</td>'
          + '<td>' + (i === 0 ? esc(l.description || l.account_name) : '') + '</td>'
          + '<td><span class="gc-badge warn">' + esc(w.type.replace(/_/g, ' ')) + '</span>'
          + (w.detail ? ' <span class="gc-sub">' + esc(w.detail) + '</span>' : '') + '</td>'
          + '<td class="gc-r">' + (hasNums ? fmtMoney(w.stored) : '') + '</td>'
          + '<td class="gc-r">' + (hasNums ? fmtMoney(w.computed) : '') + '</td>'
          + '<td class="gc-r">' + (w.delta != null ? fmtMoney(w.delta) : '') + '</td></tr>';
      });
    });
    h += '</tbody></table>';
    content.innerHTML = h;
  }
})();
