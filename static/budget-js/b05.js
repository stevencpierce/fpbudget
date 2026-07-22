// Extracted from a templates/budget.html inline block (audit M9, 2026-07-20).
// Jinja values arrive via window.__BJ (set by the inline preamble before this
// script tag). Classic script, document order — semantics identical to inline.

(function() {
  // Wire up all ⓘ buttons
  document.addEventListener('click', function(e) {
    const btn = e.target.closest('.calc-detail-btn');
    if (!btn) return;
    const pid = btn.dataset.pid, bid = btn.dataset.bid, lid = btn.dataset.lid;
    openCalcDetail(pid, bid, lid);
  });

  // Schedule Detail button — same panel chrome, different endpoint.
  // Reuses the OT-detail panel + overlay and renders a who-where-when
  // breakdown for schedule-driven non-labor lines (per diem, meals,
  // hotel, flight, mileage, etc).
  document.addEventListener('click', function(e) {
    const btn = e.target.closest('.schedule-detail-btn');
    if (!btn) return;
    const pid = btn.dataset.pid, bid = btn.dataset.bid, lid = btn.dataset.lid;
    openScheduleDetail(pid, bid, lid);
  });

  // Close on Escape
  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') closeCalcDetail();
  });
})();

function openCalcDetail(pid, bid, lid) {
  const panel   = document.getElementById('calc-detail-panel');
  const overlay = document.getElementById('calc-detail-overlay');
  const body    = document.getElementById('calc-detail-body');
  document.getElementById('calc-detail-title').textContent = 'Payroll Calc';
  body.innerHTML = '<div class="calc-detail-loading">Loading…</div>';
  panel.classList.remove('hidden');
  panel.classList.add('visible');
  overlay.classList.remove('hidden');
  overlay.classList.add('visible');

  fetch(`/projects/${pid}/budget/${bid}/line/${lid}/calc`)
    .then(r => r.json())
    .then(d => renderCalcDetail(d))
    .catch(() => { body.innerHTML = '<p class="muted">Failed to load.</p>'; });
}

function closeCalcDetail() {
  const panel = document.getElementById('calc-detail-panel');
  const overlay = document.getElementById('calc-detail-overlay');
  panel.classList.remove('visible');
  panel.classList.add('hidden');
  overlay.classList.remove('visible');
  overlay.classList.add('hidden');
}

// ── Schedule Detail (per-day, per-person audit for schedule-driven lines) ──
function openScheduleDetail(pid, bid, lid) {
  const panel   = document.getElementById('calc-detail-panel');
  const overlay = document.getElementById('calc-detail-overlay');
  const body    = document.getElementById('calc-detail-body');
  document.getElementById('calc-detail-title').textContent = 'Schedule Detail';
  body.innerHTML = '<div class="calc-detail-loading">Loading…</div>';
  panel.classList.remove('hidden');
  panel.classList.add('visible');
  overlay.classList.remove('hidden');
  overlay.classList.add('visible');

  fetch(`/projects/${pid}/budget/${bid}/line/${lid}/schedule-detail`)
    .then(r => r.json())
    .then(d => renderScheduleDetail(d))
    .catch(() => { body.innerHTML = '<p class="muted">Failed to load.</p>'; });
}

function renderScheduleDetail(d) {
  if (d.error) {
    document.getElementById('calc-detail-body').innerHTML =
      `<p class="muted">${d.error}</p>`;
    return;
  }
  document.getElementById('calc-detail-title').textContent =
    `${d.label} — Schedule Detail`;

  const fmt = v => v == null ? '—' : '$' + parseFloat(v).toLocaleString('en-US',
    {minimumFractionDigits:2, maximumFractionDigits:2});
  const _esc = s => String(s == null ? '' : s)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  const _dayBadge = dt => {
    const colors = {
      work:'#22c55e', travel:'#3498db', hold:'#a78bfa',
      half:'#f59e0b', kill_fee:'#ef4444', custom:'#9b59b6'
    };
    const c = colors[dt] || 'var(--text-muted)';
    return `<span style="font-size:10px;padding:1px 6px;border-radius:8px;background:${c}22;color:${c};border:1px solid ${c}55;text-transform:uppercase;letter-spacing:.4px">${_esc(dt)}</span>`;
  };

  // Sanity-check banner: stored line total should equal the sum of all
  // (date × person × rate) breakdown rows. If they disagree, it means
  // someone manually edited the line's qty/days/rate and the breakdown
  // is now informational rather than authoritative — call that out so
  // the user knows to re-sync if they want them re-aligned.
  const computed = parseFloat(d.total_amount || 0);
  const stored   = parseFloat(d.stored_total || 0);
  const drift    = Math.abs(computed - stored);
  let banner = '';
  if (drift > 0.5) {
    // When sync_omit is on AND there's drift, give the user an actual
    // "fix it" button. The previous version of this banner just told
    // them what the symptom was without a way out — user report
    // 2026-05-19: "the top sheet is not calculating correctly for
    // working" → root cause was a sync-omitted Flights line stuck at
    // its old qty=1, days=2, $400 even though the schedule had 0
    // flight flags. One click resets sync_omit=False + re-runs sync
    // → days drop to 1, total to $0, line either deletes or stands
    // by ready to refill from new flags.
    const omittedNote = d.sync_omit
      ? `<div style="font-size:.78rem;margin-top:6px;color:#fbbf24">
           This line has <strong>manual override on</strong> — sync skips it
           every render, so deleting schedule flags doesn't zero it out.
         </div>`
      : '';
    const resyncBtn = d.sync_omit
      ? `<button type="button" id="calc-resync-btn"
                 data-line-id="${d.line_id}" data-line-tag="${d.line_tag}"
                 style="margin-top:10px;padding:5px 12px;border-radius:5px;
                        background:#1a2540;border:1px solid #2d4070;
                        color:#5b8af9;font-size:.8rem;cursor:pointer">
           ↻ Reset to auto-sync
         </button>`
      : '';
    banner = `
      <div style="padding:8px 12px;border-radius:6px;margin-bottom:12px;
                  background:#2a2414;color:#fbbf24;border:1px solid #4a3a1a;font-size:.82rem">
        ⚠ Line total ${fmt(stored)} differs from breakdown sum ${fmt(computed)}.
        ${d.sync_omit ? '' : 'Manually edited qty / days / rate? '}The breakdown shows what the
        schedule WOULD compute — not what's stored.
        ${omittedNote}
        ${resyncBtn}
      </div>`;
  }

  let html = `
    ${banner}
    <div class="calc-meta">
      <span><strong>Rate:</strong> ${fmt(d.rate)} <span class="muted">(${_esc(d.rate_unit || 'per unit')})</span></span>
      <span><strong>Days flagged:</strong> ${d.total_days}</span>
      <span><strong>Total instances:</strong> ${d.total_count}</span>
      <span><strong>Total:</strong> <strong>${fmt(d.total_amount)}</strong></span>
    </div>`;

  if (!d.days || d.days.length === 0) {
    html += `<p class="muted" style="margin-top:1rem">No schedule data found for this line. ` +
            `Check that the matching cell flag is set on the schedule.</p>`;
  } else {
    html += `<table class="calc-weeks-table" style="margin-top:.75rem">
      <thead><tr>
        <th>Date</th><th>Role</th><th>Person</th><th>Day type</th><th style="text-align:right">Amount</th>
      </tr></thead><tbody>`;
    for (const day of d.days) {
      // First row of the day group prints the date + day subtotal.
      // Subsequent rows in the same date leave date column blank for clarity.
      day.items.forEach((item, idx) => {
        const dateCell = idx === 0
          ? `<td rowspan="${day.items.length}" style="font-weight:600;vertical-align:top">
               ${_esc(day.date)}
               <div class="muted" style="font-size:.7rem;font-weight:400;margin-top:2px">
                 ${day.count} · ${fmt(day.subtotal)}
               </div>
             </td>` : '';
        html += `<tr>
          ${dateCell}
          <td>${_esc(item.role)}</td>
          <td>${_esc(item.person)}</td>
          <td>${_dayBadge(item.day_type)}</td>
          <td style="text-align:right">${fmt(item.amount)}</td>
        </tr>`;
      });
    }
    html += `</tbody>
      <tfoot><tr>
        <td colspan="4" style="text-align:right;font-weight:600">Total</td>
        <td style="text-align:right;font-weight:600">${fmt(d.total_amount)}</td>
      </tr></tfoot>
    </table>`;
  }

  document.getElementById('calc-detail-body').innerHTML = html;

  // Wire the Reset-to-auto button when present. POSTs to the
  // toggle-sync-omit endpoint (which flips False → not False = True;
  // so when current state IS True, one toggle clears it). Then reloads
  // so calc_top_sheet + sync_schedule_driven_lines re-render with the
  // line back under auto-control.
  const resyncBtn = document.getElementById('calc-resync-btn');
  if (resyncBtn) {
    resyncBtn.addEventListener('click', async () => {
      const lineId = resyncBtn.dataset.lineId;
      if (!confirm('Reset this line to auto-sync? Its qty / days / rate / total will be ' +
                   'recomputed from the schedule on the next render. Any value you typed in ' +
                   'manually will be overwritten.')) return;
      resyncBtn.disabled = true;
      resyncBtn.textContent = 'Resetting…';
      try {
        const r = await fetch(`/projects/${PID}/budget/${BID}/line/${lineId}/toggle-sync-omit`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
        });
        if (!r.ok) {
          const j = await r.json().catch(() => ({}));
          alert(j.error || `Reset failed (HTTP ${r.status}).`);
          resyncBtn.disabled = false;
          resyncBtn.textContent = '↻ Reset to auto-sync';
          return;
        }
        // Reload — calc_top_sheet + sync_schedule_driven_lines run
        // on page load and will normalize the line.
        window.location.reload();
      } catch (e) {
        alert('Reset failed: ' + e.message);
        resyncBtn.disabled = false;
        resyncBtn.textContent = '↻ Reset to auto-sync';
      }
    });
  }
}

function renderCalcDetail(d) {
  document.getElementById('calc-detail-title').textContent =
    `${d.label} — Payroll Calc`;

  const fmt  = v => v == null ? '—' : '$' + parseFloat(v).toLocaleString('en-US',
    {minimumFractionDigits:2, maximumFractionDigits:2});
  const fmtH = v => v == null ? '—' : parseFloat(v).toFixed(1) + 'h';
  const fmtR = v => v == null ? null : '$' + parseFloat(v).toLocaleString('en-US',
    {minimumFractionDigits:2, maximumFractionDigits:2}) + '/hr';
  const hasHours = d.total_st_hours > 0 || d.total_ot_hours > 0 || d.total_dt_hours > 0;

  const hourlyRates = d.st_hourly != null
    ? `<span><strong>ST hrly:</strong> ${fmtR(d.st_hourly)}</span>
       ${d.ot_hourly != null ? `<span class="ot-val"><strong>OT hrly:</strong> ${fmtR(d.ot_hourly)}</span>` : ''}
       ${d.dt_hourly != null ? `<span class="dt-val"><strong>DT hrly:</strong> ${fmtR(d.dt_hourly)}</span>` : ''}`
    : '';

  let html = `
    <div class="calc-meta">
      <span><strong>Rate:</strong> ${fmt(d.rate)} / ${RATE_TYPE_LABELS[d.rate_type] || d.rate_type}</span>
      ${d.hours_per_day ? `<span><strong>Hours/day:</strong> ${d.hours_per_day}</span>` : ''}
      ${hourlyRates}
      <span><strong>Qty:</strong> ${d.qty}</span>
      <span><strong>Fringe:</strong> ${d.fringe_type}${!d.ot_applies ? ' <em class="no-ot">(OT exempt)</em>' : ''}</span>
    </div>
    <div class="calc-rule">📐 ${d.rule}</div>`;

  if (d.weeks && d.weeks.length > 0) {
    html += `<table class="calc-weeks-table">
      <thead><tr>
        <th>Week of</th><th>Days</th>
        ${hasHours ? '<th>ST hrs</th><th>OT hrs</th><th>DT hrs</th>' : ''}
        <th>ST cost</th><th>OT cost</th><th>DT cost</th>
      </tr></thead><tbody>`;
    d.weeks.forEach(w => {
      html += `<tr>
        <td>${w.week_of}</td>
        <td class="num">${w.days}</td>
        ${hasHours ? `<td class="num">${fmtH(w.st_hours)}</td>
          <td class="num ${w.ot_hours > 0 ? 'ot-val' : ''}">${fmtH(w.ot_hours)}</td>
          <td class="num ${w.dt_hours > 0 ? 'dt-val' : ''}">${fmtH(w.dt_hours)}</td>` : ''}
        <td class="num">${fmt(w.st_cost)}</td>
        <td class="num ${w.ot_cost > 0 ? 'ot-val' : ''}">${fmt(w.ot_cost)}</td>
        <td class="num ${w.dt_cost > 0 ? 'dt-val' : ''}">${fmt(w.dt_cost)}</td>
      </tr>`;
    });
    html += `</tbody></table>`;
  } else {
    html += `<p class="muted" style="font-size:.8rem;padding:.4rem 0">No schedule days — manual estimate only.</p>`;
  }

  if (hasHours) {
    html += `<div class="calc-hours-total">
      Total: <strong>${fmtH(d.total_st_hours)} ST</strong>
      ${d.total_ot_hours > 0 ? ` + <strong class="ot-val">${fmtH(d.total_ot_hours)} OT</strong>` : ''}
      ${d.total_dt_hours > 0 ? ` + <strong class="dt-val">${fmtH(d.total_dt_hours)} DT</strong>` : ''}
      &nbsp;(${d.total_days} day${d.total_days !== 1 ? 's' : ''})
    </div>`;
  }

  html += `<div class="calc-cost-summary">
    <div class="calc-cost-row"><span>ST wages</span><span>${fmt(d.st_cost)}</span></div>
    ${d.ot_cost > 0 ? `<div class="calc-cost-row ot-val"><span>OT wages</span><span>${fmt(d.ot_cost)}</span></div>` : ''}
    ${d.dt_cost > 0 ? `<div class="calc-cost-row dt-val"><span>DT wages</span><span>${fmt(d.dt_cost)}</span></div>` : ''}
    <div class="calc-cost-row"><span>Fringe</span><span>${fmt(d.fringe_cost)}</span></div>
    ${d.agent_cost > 0 ? `<div class="calc-cost-row"><span>Agent %</span><span>${fmt(d.agent_cost)}</span></div>` : ''}
    <div class="calc-cost-row calc-cost-total"><span><strong>Total</strong></span><span><strong>${fmt(d.total_cost)}</strong></span></div>
  </div>`;

  document.getElementById('calc-detail-body').innerHTML = html;
}
