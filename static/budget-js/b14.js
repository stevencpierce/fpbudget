// Extracted from a templates/budget.html inline block (audit M9, 2026-07-20).
// Jinja values arrive via window.__BJ (set by the inline preamble before this
// script tag). Classic script, document order — semantics identical to inline.
(function() {
  try {
  if (typeof PID === 'undefined' || typeof BID === 'undefined') {
    console.error('[Travel/Catering] PID/BID not defined');
    return;
  }
  console.log('[Travel/Catering] dedicated tag starting');
  const TRAVEL_GRID_URL    = `/projects/${PID}/budget/${BID}/travel/grid`;
  const TRAVEL_TOGGLE_URL  = `/projects/${PID}/budget/${BID}/travel/toggle`;
  const TRAVEL_DETAIL_URL  = `/projects/${PID}/budget/${BID}/travel/detail`;
  const TRAVEL_ADD_URL     = `/projects/${PID}/budget/${BID}/travel/add`;
  const CATERING_GRID_URL  = `/projects/${PID}/budget/${BID}/catering/grid`;
  const CATERING_MEAL_URL  = `/projects/${PID}/budget/${BID}/catering/meal-toggle`;
  const CATERING_BILL_URL  = `/projects/${PID}/budget/${BID}/catering/bill`;

  let _travelData   = null;
  let _cateringData = null;
  let _detailContext = null;  // { sd_id, kind, current_detail }

  const _esc = s => String(s == null ? '' : s)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  const _fmt = n => '$' + parseFloat(n || 0).toLocaleString('en-US',
    {minimumFractionDigits:2, maximumFractionDigits:2});

  // ── Travel grid ────────────────────────────────────────────────────────
  async function loadTravelGrid() {
    const onlyFlagged = document.getElementById('travel-only-flagged')?.checked;
    const showAll = onlyFlagged ? '0' : '1';
    document.getElementById('travel-totals').innerHTML = '<span class="muted">Loading…</span>';
    document.getElementById('travel-days').innerHTML =
      '<div class="muted" style="padding:1rem;text-align:center">Loading…</div>';
    try {
      const r = await fetch(TRAVEL_GRID_URL + '?show_all=' + showAll, { credentials: 'same-origin' });
      if (!r.ok) {
        const txt = await r.text().catch(() => '');
        document.getElementById('travel-days').innerHTML =
          `<div style="padding:1rem;color:#ef4444">Server returned ${r.status}: ${_esc(txt.slice(0,200))}</div>`;
        document.getElementById('travel-totals').innerHTML =
          `<span style="color:#ef4444">Failed (HTTP ${r.status})</span>`;
        return;
      }
      _travelData = await r.json();
      renderTravelGrid();
    } catch (err) {
      console.error('loadTravelGrid failed', err);
      document.getElementById('travel-days').innerHTML =
        `<div style="padding:1rem;color:#ef4444">Error: ${_esc(err.message || err)}</div>`;
      document.getElementById('travel-totals').innerHTML =
        `<span style="color:#ef4444">Error: ${_esc(err.message || err)}</span>`;
    }
  }

  function renderTravelGrid() {
    if (!_travelData) return;
    const rows = _travelData.rows || [];
    const rates = _travelData.rates || {};

    // Running totals (computed on ALL rows, displayed in the top summary bar).
    let flightCount=0, hotelCount=0, carCount=0, svcCount=0, mileageMiles=0;
    const pdCounts = {full:0, breakfast:0, lunch:0, dinner:0};
    rows.forEach(r => {
      if (r.flags.flight) flightCount++;
      if (r.flags.hotel)  hotelCount++;
      if (r.flags.car_rental)  carCount++;
      if (r.flags.car_service) svcCount++;
      if (r.detail.mileage && r.detail.mileage.miles) mileageMiles += parseFloat(r.detail.mileage.miles);
      if (r.flags.per_diem) pdCounts[r.flags.per_diem] = (pdCounts[r.flags.per_diem]||0) + 1;
    });

    // Group rows by date so we can render each day as its own card.
    const byDate = {};
    rows.forEach(r => { (byDate[r.date] = byDate[r.date] || []).push(r); });
    const dates = Object.keys(byDate).sort();

    if (dates.length === 0) {
      document.getElementById('travel-days').innerHTML =
        '<div class="muted" style="padding:1.2rem;text-align:center;background:var(--bg-card);border:1px solid var(--border);border-radius:8px">' +
        'No travel days yet. Use "+ Add Travel Day" above to get started.' +
        '</div>';
      document.getElementById('travel-totals').innerHTML =
        '<span class="muted">No travel costs yet.</span>';
      return;
    }

    // Day-type editor: replaces the read-only pill with a small select
    // so users can change Work → Travel → Hold etc. directly from the
    // Travel tab without bouncing back to the Schedule. Same color
    // accent on the border so it still scans as a status pill.
    const daySelect = (r) => {
      const colors = {
        work:'#22c55e',
        travel:'#3498db',
        travel_half:'#5b9fd9',     // lighter blue for half-rate travel
        travel_unpaid:'#7d96aa',   // muted blue-grey for unpaid travel
        hold:'#a78bfa',
        half:'#f59e0b',
        kill_fee:'#ef4444',
        custom:'#9b59b6',
        off:'var(--text-muted)'
      };
      const c = colors[r.day_type] || 'var(--text-muted)';
      const handler = `travelSetDayType(${r.line_id}, ${r.instance}, '${r.date}', this.value)`;
      const opts = [
        ['work',          'Work'],
        ['travel',        'Travel'],
        ['travel_half',   'Travel — ½ rate'],
        ['travel_unpaid', 'Travel — unpaid'],
        ['hold',          'Hold'],
        ['half',          'Half'],
        ['kill_fee',      'Kill Fee'],
        ['custom',        'Custom'],
        ['off',           'Off'],
      ];
      return `<select onchange="${handler}" title="Change day type"
                      style="font-size:13px;padding:5px 10px;border-radius:6px;
                             background:${c}22;color:${c};border:1px solid ${c}55;
                             text-transform:uppercase;letter-spacing:.4px;font-weight:600;cursor:pointer">
        ${opts.map(([v,l]) => `<option value="${v}" ${r.day_type===v?'selected':''}>${l}</option>`).join('')}
      </select>`;
    };
    // Flag pill — just the checkbox that flips the cell flag on/off. The
    // per-entry reservation lines render below in entriesBlock().
    const cb = (r, flag, isOn, kind, label) => {
      const handler = `travelToggleFlag(${r.line_id}, ${r.instance}, '${r.date}', '${flag}')`;
      const list = kind ? ((r.details_list && r.details_list[kind]) || []) : [];
      const nEntries = list.length;
      const countBadge = (isOn && nEntries > 1)
        ? `<span style="font-size:10px;color:#8fa9d6;margin-left:4px" title="${nEntries} entries">×${nEntries}</span>`
        : '';
      const activeStyle = isOn
        ? 'background:#1a2540;color:#5b8af9;border:1px solid #2d4070'
        : 'background:transparent;color:var(--text-muted);border:1px solid var(--border)';
      return `<label style="display:inline-flex;align-items:center;gap:6px;padding:6px 12px;border-radius:6px;cursor:pointer;font-size:13px;font-weight:500;${activeStyle}" title="${label}">
                <input type="checkbox" ${isOn?'checked':''} onclick="${handler}" style="margin:0;width:14px;height:14px">
                ${label}${countBadge}
              </label>`;
    };

    // Icon + short kind label for entry rows.
    const kindMeta = {
      flight:      {icon:'✈️',  label:'Flight'},
      hotel:       {icon:'🏨',  label:'Hotel'},
      car_rental:  {icon:'🚗',  label:'Car rental'},
      car_service: {icon:'🚐',  label:'Car service'},
      mileage:     {icon:'🚙',  label:'Mileage'},
    };

    // One-line summary of a single travel entry (used in the entries list).
    const entrySummary = (kind, d) => {
      if (kind === 'flight') {
        let s = `${_esc(d.airline || 'Flight')}${d.flight_no ? ' ' + _esc(d.flight_no) : ''}`;
        if (d.depart_airport || d.arrive_airport) s += ` · ${_esc(d.depart_airport || '?')}→${_esc(d.arrive_airport || '?')}`;
        if (d.depart_at) s += ` · ${_esc((d.depart_at || '').slice(11,16))}`;
        return s;
      }
      if (kind === 'hotel') {
        let s = `${_esc(d.hotel_name || 'Hotel')}`;
        if (d.check_in || d.check_out) s += ` · ${_esc(d.check_in || '?')}–${_esc(d.check_out || '?')}`;
        return s;
      }
      if (kind === 'car_rental') {
        let s = `${_esc(d.rental_co || 'Car rental')}`;
        if (d.pickup_location) s += ` · ${_esc(d.pickup_location)}`;
        return s;
      }
      if (kind === 'car_service') {
        if (d.self_report) return `🚕 Rideshare (self-book) — keep your receipt`;
        let s = `${_esc(d.rental_co || 'Car service')}`;
        const t = (d.pickup_at || '').slice(11,16);
        if (d.pickup_location) s += ` · pickup ${t ? t + ' ' : ''}${_esc(d.pickup_location)}`;
        else if (t) s += ` · pickup ${t}`;
        if (d.dropoff_location) s += ` → ${_esc(d.dropoff_location)}`;
        if (d.contact_phone) s += ` · ☎ ${_esc(d.contact_phone)}`;
        return s;
      }
      if (kind === 'mileage') {
        let s = 'Mileage';
        if (d.route) s += ` · ${_esc(d.route)}`;
        if (d.miles) s += ` · ${_esc(d.miles)} mi`;
        return s;
      }
      return kind;
    };

    // The stacked list of entries for one kind on one person-day, with per-entry
    // Edit / Delete and an "+ Add another" button. Only shown when the flag is on.
    const entriesBlock = (r, kind, isOn) => {
      if (!isOn) return '';
      const meta = kindMeta[kind] || {icon:'📍', label:kind};
      const list = (r.details_list && r.details_list[kind]) || [];
      let lines = '';
      list.forEach(d => {
        const spanned = !!d.spanned;
        const editSd  = spanned && d.anchor_day
          ? `openTravelDetailByAnchor(${r.line_id}, ${r.instance}, '${_esc(d.anchor_day)}', '${kind}', ${d.id})`
          : `openTravelDetail(${r.schedule_day_id}, '${kind}', ${d.id})`;
        const conf = d.confirmation_no
          ? `<span style="font-size:11px;color:${spanned?'var(--text-muted)':'#8fa9d6'};margin-left:6px" title="Confirmation #">#${_esc(d.confirmation_no)}</span>` : '';
        const spanTag = spanned
          ? `<span style="font-size:10px;color:var(--text-muted);margin-left:5px;font-style:italic" title="Reservation spans this day (edit on ${_esc(d.anchor_day || '')})">mid-stay</span>` : '';
        const delBtn = spanned ? '' :
          `<button onclick="event.stopPropagation(); deleteTravelDetail(${r.schedule_day_id}, '${kind}', ${d.id})"
                   style="background:transparent;border:1px solid #4a2530;color:#c77;cursor:pointer;padding:2px 7px;font-size:11px;border-radius:5px;margin-left:4px" title="Delete this entry">✕</button>`;
        lines += `
          <div style="display:flex;align-items:center;gap:4px;padding:3px 0;flex-wrap:wrap">
            <span style="font-size:12px">${meta.icon}</span>
            <span style="font-size:12.5px;color:${spanned?'var(--text-muted)':'var(--text)'}">${entrySummary(kind, d)}</span>
            ${conf}${spanTag}
            <button onclick="event.stopPropagation(); ${editSd}"
                    style="background:#1a2540;border:1px solid #2d4070;color:#5b8af9;cursor:pointer;padding:2px 9px;font-size:11px;border-radius:5px;margin-left:auto" title="Edit this entry">Edit</button>
            ${delBtn}
          </div>`;
      });
      // Checkout morning note (hotels only) — informational, slot stays free.
      let checkoutNote = '';
      if (kind === 'hotel' && r.hotel_checkout) {
        checkoutNote = `<div style="font-size:10.5px;color:var(--text-muted);font-style:italic;padding:2px 0"
             title="Checking out of ${_esc(r.hotel_checkout.hotel_name || 'hotel')} this morning">◀ check-out${r.hotel_checkout.confirmation_no ? ' #' + _esc(r.hotel_checkout.confirmation_no) : ''}</div>`;
      }
      const addBtn = `<button onclick="event.stopPropagation(); openTravelDetail(${r.schedule_day_id}, '${kind}', null)"
                    style="background:transparent;border:1px dashed #2d4070;color:#5b8af9;cursor:pointer;padding:3px 10px;font-size:11.5px;border-radius:5px;margin-top:3px" title="Add another ${meta.label.toLowerCase()}">+ Add another ${meta.label.toLowerCase()}</button>`;
      return `<div style="width:100%;margin-top:5px;padding:6px 10px;border:1px solid var(--border);border-radius:7px;background:var(--bg-2)">
                <div style="font-size:10.5px;color:var(--text-muted);text-transform:uppercase;letter-spacing:.4px;font-weight:600;margin-bottom:2px">${meta.icon} ${meta.label}</div>
                ${lines || '<div style="font-size:11.5px;color:var(--text-muted);font-style:italic;padding:2px 0">No details yet.</div>'}
                ${checkoutNote}${addBtn}
              </div>`;
    };
    const pdSelect = (r) => `
      <select onchange="travelSetPerDiem(${r.line_id}, ${r.instance}, '${r.date}', this.value)"
              style="font-size:13px;padding:6px 10px;border-radius:6px;background:var(--bg-input);border:1px solid var(--border);color:var(--text);font-weight:500"
              title="Per Diem">
        <option value=""          ${r.flags.per_diem===''        ?' selected':''}>No Per Diem</option>
        <option value="full"      ${r.flags.per_diem==='full'    ?' selected':''}>Full Per Diem</option>
        <option value="breakfast" ${r.flags.per_diem==='breakfast'?' selected':''}>Breakfast</option>
        <option value="lunch"     ${r.flags.per_diem==='lunch'   ?' selected':''}>Lunch</option>
        <option value="dinner"    ${r.flags.per_diem==='dinner'  ?' selected':''}>Dinner</option>
      </select>`;

    // Pretty date heading: "Mon Jun 7, 2027" without timezone weirdness.
    const fmtDate = (iso) => {
      const [y,m,d] = iso.split('-').map(n => parseInt(n,10));
      const date = new Date(y, m-1, d);
      return date.toLocaleDateString(undefined, {weekday:'short', month:'short', day:'numeric', year:'numeric'});
    };

    let html = '';
    dates.forEach(date => {
      const dayRows = byDate[date];
      const flagged = dayRows.filter(r => r.flags.flight || r.flags.hotel || r.flags.car_rental || r.flags.car_service || r.flags.mileage || r.flags.per_diem).length;
      html += `
        <div class="travel-day-card" style="background:var(--bg-card);border:1px solid var(--border);border-radius:10px;overflow:hidden">
          <div class="travel-day-header"
               onclick="if (event.target.closest('button')) return; this.parentElement.querySelector('.travel-day-body').classList.toggle('travel-collapsed'); this.querySelector('.travel-chevron').textContent = this.parentElement.querySelector('.travel-day-body').classList.contains('travel-collapsed') ? '▸' : '▾';"
               style="display:flex;align-items:center;gap:14px;padding:14px 18px;background:var(--bg-2);border-bottom:1px solid var(--border);cursor:pointer;user-select:none">
            <span class="travel-chevron" style="color:var(--text-muted);font-size:18px;width:18px">▾</span>
            <strong style="font-size:16px">${_esc(fmtDate(date))}</strong>
            <span class="muted" style="font-size:13px">${dayRows.length} crew · ${flagged} flagged</span>
            <button class="btn btn-sm" onclick="event.stopPropagation(); openAddTravelForDate('${date}')"
                    style="margin-left:auto;font-size:13px;padding:6px 12px"
                    title="Add another crew member to this date">+ Add to this day</button>
          </div>
          <div class="travel-day-body" style="display:flex;flex-direction:column">
      `;
      dayRows.forEach((r, idx) => {
        const sep = idx > 0 ? 'border-top:1px solid var(--border)' : '';
        html += `
          <div style="display:grid;grid-template-columns:minmax(220px,300px) auto 1fr;gap:16px;padding:14px 18px;align-items:center;${sep}">
            <div style="min-width:0">
              <div style="font-size:14px"><strong>${_esc(r.person)}</strong></div>
              <div class="muted" style="font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-top:2px">${_esc(r.role)}</div>
            </div>
            <div>${daySelect(r)}</div>
            <div style="display:flex;flex-wrap:wrap;gap:8px;justify-content:flex-end;align-items:flex-start">
              <div style="display:flex;flex-wrap:wrap;gap:8px;justify-content:flex-end;align-items:center;width:100%">
                ${cb(r, 'flight',      r.flags.flight,      'flight',      'Flight')}
                ${cb(r, 'hotel',       r.flags.hotel,       'hotel',       'Hotel')}
                ${cb(r, 'car_rental',  r.flags.car_rental,  'car_rental',  'Car Rental')}
                ${cb(r, 'car_service', r.flags.car_service, 'car_service', 'Car Service')}
                ${cb(r, 'mileage',     r.flags.mileage,     'mileage',     'Mileage')}
                ${pdSelect(r)}
              </div>
              ${entriesBlock(r, 'flight',      r.flags.flight)}
              ${entriesBlock(r, 'hotel',       r.flags.hotel)}
              ${entriesBlock(r, 'car_rental',  r.flags.car_rental)}
              ${entriesBlock(r, 'car_service', r.flags.car_service)}
              ${entriesBlock(r, 'mileage',     r.flags.mileage)}
            </div>
          </div>
        `;
      });
      html += '</div></div>';
    });
    document.getElementById('travel-days').innerHTML = html;

    // Summary bar — instance counts and approximate $ totals using crew
    // rates as a default (the real auto-line uses role-group rates and
    // is the source of truth; this is just a quick-glance estimate).
    const flightTotal = flightCount * (rates.flight_crew || 0);
    const hotelTotal  = hotelCount  * (rates.hotel_crew  || 0);
    const carTotal    = carCount    * 50;  // car rental rate placeholder
    const mileageTotal = mileageMiles * 0.67;  // IRS standard rate
    const pdTotal = (pdCounts.full * (rates.per_diem_full||0)
                    + pdCounts.breakfast * (rates.per_diem_breakfast||0)
                    + pdCounts.lunch     * (rates.per_diem_lunch||0)
                    + pdCounts.dinner    * (rates.per_diem_dinner||0));
    document.getElementById('travel-totals').innerHTML = `
      <span><strong>${flightCount}</strong> flights ~ <strong>${_fmt(flightTotal)}</strong></span>
      <span><strong>${hotelCount}</strong> hotel nights ~ <strong>${_fmt(hotelTotal)}</strong></span>
      <span><strong>${carCount}</strong> car rentals</span>
      <span><strong>${svcCount}</strong> car services</span>
      <span><strong>${mileageMiles.toFixed(0)}</strong> miles</span>
      <span><strong>${pdCounts.full + pdCounts.breakfast + pdCounts.lunch + pdCounts.dinner}</strong> per-diem days ~ <strong>${_fmt(pdTotal)}</strong></span>
      <span class="muted" style="font-size:.78rem;margin-left:auto">Source of truth: budget auto-lines</span>
    `;
  }

  window.travelToggleFlag = async function(line_id, instance, date, flag) {
    const cb = event.target;
    const value = cb.checked;
    cb.disabled = true;
    try {
      const r = await fetch(TRAVEL_TOGGLE_URL, {
        method: 'POST', headers: {'Content-Type':'application/json'},
        body: JSON.stringify({line_id, instance, date, flag, value})
      });
      if (!r.ok) { alert('Toggle failed'); cb.checked = !value; return; }
      // Reload so the ✎ pencil + dot appear/disappear correctly.
      await loadTravelGrid();
    } finally {
      cb.disabled = false;
    }
  };

  // Change a cell's day_type (Work / Travel / Hold / Half / Kill Fee /
  // Custom / Off) directly from the Travel tab — saves a trip back to
  // the Schedule when the only change is "this person is now on travel
  // instead of work". Hits the existing /gantt/day endpoint so all
  // downstream sync (auto-line counts, schedule day totals) runs the
  // same as if the change came from the Gantt grid.
  window.travelSetDayType = async function(line_id, instance, date, day_type) {
    const r = await fetch(`/projects/${PID}/budget/${BID}/gantt/day`, {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        line_id: parseInt(line_id),
        crew_instance: parseInt(instance),
        date,
        day_type,
      }),
    });
    if (!r.ok) {
      const d = await r.json().catch(() => ({}));
      alert('Failed to update day type: ' + (d.error || r.status));
      return;
    }
    await loadTravelGrid();
  };

  window.travelSetPerDiem = async function(line_id, instance, date, mode) {
    const r = await fetch(TRAVEL_TOGGLE_URL, {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({line_id, instance, date, flag:'per_diem', per_diem_mode:mode, value:!!mode})
    });
    if (!r.ok) alert('Per diem update failed');
    await loadTravelGrid();
  };

  // ── Travel detail modal (flight/hotel/car/mileage entry) ───────────────
  // Open the editor for a specific entry (td_id). td_id=null → new blank entry.
  // Multi-entry: entries are looked up in row.details_list[kind] by id.
  window.openTravelDetail = function(sd_id, kind, td_id) {
    if (td_id === undefined) td_id = null;
    // Find the row in cached data so we can pre-fill the form.
    let row = _travelData?.rows?.find(r => r.schedule_day_id === sd_id);
    const _pick = (rw) => {
      const list = (rw && rw.details_list && rw.details_list[kind]) || [];
      if (td_id != null) return list.find(d => d.id === td_id) || {};
      return {};  // new blank entry
    };
    let detail = _pick(row);
    // A hotel reservation spans multiple days but lives on ONE anchor cell.
    // If the user clicks "Edit" on a spanned (mid-stay) entry, redirect the
    // edit to the anchor day so we update the single source-of-truth row
    // instead of creating a duplicate reservation on this day.
    if (detail && detail.spanned && detail.anchor_day) {
      const anchor = _travelData?.rows?.find(
        r => r.line_id === row.line_id && r.instance === row.instance
             && r.date === detail.anchor_day);
      if (anchor) {
        row = anchor;
        sd_id = anchor.schedule_day_id;
        detail = _pick(anchor);
      }
    }
    _detailContext = { sd_id, kind, td_id: (detail && detail.id) || td_id || null, detail };

    const titleMap = {flight:'Flight', hotel:'Hotel', car_rental:'Car Rental', car_service:'Car Service', mileage:'Mileage'};
    document.getElementById('travel-detail-title').textContent =
      `${titleMap[kind]} — ${row?.person || ''} ${row?.date || ''}`;

    // Form-field renderers — generous spacing + readable label sizes.
    // Inputs get explicit padding/font-size so browser defaults don't
    // squish the form on dark themes.
    const inputStyle = 'width:100%;font-size:14px;padding:9px 11px;border-radius:6px;background:var(--bg-input);border:1px solid var(--border);color:var(--text);box-sizing:border-box';
    const labelStyle = 'display:block;font-size:12px;color:var(--text-muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:.4px;font-weight:600';
    const fld = (label, name, type, value, extra) =>
      `<div style="margin-bottom:14px">
         <label style="${labelStyle}">${label}</label>
         <input type="${type}" data-fld="${name}" value="${_esc(value || '')}" style="${inputStyle}" ${extra || ''}>
       </div>`;
    const txt = (label, name, value) =>
      `<div style="margin-bottom:14px">
         <label style="${labelStyle}">${label}</label>
         <textarea data-fld="${name}" rows="3" style="${inputStyle};resize:vertical;font-family:inherit">${_esc(value || '')}</textarea>
       </div>`;
    const grid2 = (a, b) => `<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">${a}${b}</div>`;
    const grid12 = (a, b) => `<div style="display:grid;grid-template-columns:1fr 2fr;gap:12px">${a}${b}</div>`;

    let body = '';
    if (kind === 'flight') {
      body =
        grid2(fld('Airline',   'airline',   'text', detail.airline),
              fld('Flight #',  'flight_no', 'text', detail.flight_no))
      + grid2(fld('From (airport code)', 'depart_airport', 'text', detail.depart_airport, 'maxlength="10"'),
              fld('To (airport code)',   'arrive_airport', 'text', detail.arrive_airport, 'maxlength="10"'))
      + grid2(fld('Depart',    'depart_at', 'datetime-local', (detail.depart_at||'').slice(0,16)),
              fld('Arrive',    'arrive_at', 'datetime-local', (detail.arrive_at||'').slice(0,16)))
      + fld('Confirmation #', 'confirmation_no', 'text', detail.confirmation_no)
      + txt('Notes',          'notes', detail.notes);
    } else if (kind === 'hotel') {
      body =
        fld('Hotel name',     'hotel_name',     'text', detail.hotel_name)
      + fld('Address',        'hotel_address',  'text', detail.hotel_address)
      + grid2(fld('Check-in',  'check_in',  'date', detail.check_in),
              fld('Check-out', 'check_out', 'date', detail.check_out))
      + fld('Room type',      'room_type',      'text', detail.room_type)
      + fld('Confirmation #', 'confirmation_no','text', detail.confirmation_no)
      + txt('Notes',          'notes', detail.notes);
    } else if (kind === 'car_rental') {
      body =
        fld('Rental company', 'rental_co',       'text', detail.rental_co)
      + fld('Pickup location','pickup_location', 'text', detail.pickup_location)
      + grid2(fld('Pickup', 'pickup_at', 'datetime-local', (detail.pickup_at||'').slice(0,16)),
              fld('Return', 'return_at', 'datetime-local', (detail.return_at||'').slice(0,16)))
      + fld('Confirmation #', 'confirmation_no','text', detail.confirmation_no)
      + txt('Notes',          'notes', detail.notes);
    } else if (kind === 'car_service') {
      // Self-report rideshare toggle: when checked, company/conf/phone become
      // optional and the notes carry the "take an Uber, keep your receipt"
      // instruction that must land on the person's call sheet.
      const selfOn = !!detail.self_report;
      const selfBox =
        `<label style="display:flex;align-items:center;gap:9px;margin-bottom:14px;padding:10px 12px;border:1px solid var(--border);border-radius:7px;background:var(--bg-2);cursor:pointer;font-size:13.5px">
           <input type="checkbox" data-fld="self_report" id="trvSelfReport" ${selfOn?'checked':''}
                  onchange="_trvToggleSelfReport(this.checked)" style="width:16px;height:16px;margin:0">
           <span>🚕 <strong>Self-report (rideshare)</strong> — traveler books their own Uber/Lyft and expenses it. No confirmation needed.</span>
         </label>`;
      body =
        selfBox
      + `<div id="trvCarSvcFields">`
      +   fld('Company', 'rental_co', 'text', detail.rental_co)
      +   grid2(fld('Pickup', 'pickup_at', 'datetime-local', (detail.pickup_at||'').slice(0,16)),
                fld('Drop-off', 'return_at', 'datetime-local', (detail.return_at||'').slice(0,16)))
      +   fld('Pickup location',  'pickup_location',  'text', detail.pickup_location)
      +   fld('Drop-off location','dropoff_location', 'text', detail.dropoff_location)
      +   fld('Contact phone',    'contact_phone',    'text', detail.contact_phone)
      +   fld('Confirmation #',   'confirmation_no',  'text', detail.confirmation_no)
      + `</div>`
      + txt('Instructions / notes', 'notes',
            detail.notes || (selfOn ? 'Take an Uber/Lyft — keep your receipt and email it to production.' : ''));
    } else if (kind === 'mileage') {
      body =
        grid12(fld('Miles', 'miles', 'number', detail.miles, 'step="0.1" min="0"'),
               fld('Route', 'route', 'text',   detail.route))
      + txt('Notes', 'notes', detail.notes);
    }
    document.getElementById('travel-detail-body').innerHTML = body;
    document.getElementById('travel-detail-overlay').classList.remove('hidden');
  };

  window.closeTravelDetail = function(ev) {
    if (ev && ev.target && ev.target.id !== 'travel-detail-overlay') return;
    document.getElementById('travel-detail-overlay').classList.add('hidden');
    _detailContext = null;
  };

  // Edit a spanned (mid-stay) entry by routing to its anchor day's row.
  window.openTravelDetailByAnchor = function(line_id, instance, anchor_date, kind, td_id) {
    const anchor = _travelData?.rows?.find(
      r => r.line_id === line_id && r.instance === instance && r.date === anchor_date);
    if (anchor) openTravelDetail(anchor.schedule_day_id, kind, td_id);
  };

  // Self-report rideshare toggle inside the car_service editor: grey out the
  // booking fields and prefill the instruction note when turned on.
  window._trvToggleSelfReport = function(on) {
    const box = document.getElementById('trvCarSvcFields');
    if (box) box.style.opacity = on ? '0.45' : '1';
    const notes = document.querySelector('#travel-detail-body [data-fld="notes"]');
    if (on && notes && !notes.value.trim()) {
      notes.value = 'Take an Uber/Lyft — keep your receipt and email it to production.';
    }
  };

  // Delete a single travel entry (multi-entry aware — addressed by td_id).
  window.deleteTravelDetail = async function(sd_id, kind, td_id) {
    if (!confirm('Delete this travel entry?')) return;
    const r = await fetch(TRAVEL_DETAIL_URL, {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({schedule_day_id: sd_id, kind, td_id, delete: true}),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) { alert('Delete failed: ' + (d.error || r.status)); return; }
    await loadTravelGrid();
  };

  document.getElementById('travel-detail-save-btn')?.addEventListener('click', async () => {
    if (!_detailContext) return;
    // td_id present → update that row; absent → create a new entry.
    const payload = { schedule_day_id: _detailContext.sd_id, kind: _detailContext.kind };
    if (_detailContext.td_id != null) payload.td_id = _detailContext.td_id;
    document.querySelectorAll('#travel-detail-body [data-fld]').forEach(el => {
      payload[el.dataset.fld] = (el.type === 'checkbox') ? el.checked : el.value;
    });
    const btn = document.getElementById('travel-detail-save-btn');
    btn.disabled = true; btn.textContent = 'Saving…';
    try {
      const r = await fetch(TRAVEL_DETAIL_URL, {
        method: 'POST', headers: {'Content-Type':'application/json'},
        body: JSON.stringify(payload),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) { alert('Save failed: ' + (d.error || r.status)); return; }
      closeTravelDetail();
      await loadTravelGrid();
    } finally {
      btn.disabled = false; btn.textContent = 'Save';
    }
  });

  // ── Add Travel Day ─────────────────────────────────────────────────────
  function _populateAddTravelLines() {
    // Pull every labor line in the budget. For lines with quantity > 1
    // (multi-instance roles, e.g. 2 Camera Operators), expand into one
    // dropdown entry per instance so users can target a specific person.
    // Falls back to the budget's line ORM payload via a fetch if the
    // Budget tab DOM isn't rendered yet — fixes the bug where opening
    // Add Travel Day from the Travel tab showed a near-empty dropdown.
    const sel = document.getElementById('add-travel-line');
    sel.innerHTML = '<option value="">— select a person —</option>';

    let added = 0;
    document.querySelectorAll('#working-budget-wrap .line-row.labor-line').forEach(row => {
      const lid = row.dataset.id;
      const desc = row.querySelector('.editable[data-field="description"]')?.textContent.trim() || '';
      const crewNm = row.querySelector('.assigned-crew-name')?.textContent.trim() || '';
      const qty = Math.max(1, parseInt(row.dataset.qty || '1', 10) || 1);
      if (!lid || !desc) return;
      for (let inst = 1; inst <= qty; inst++) {
        const baseLabel = (crewNm && crewNm !== '+ Assign') ? `${desc} — ${crewNm}` : desc;
        const label = (qty > 1) ? `${baseLabel}  (instance ${inst})` : baseLabel;
        const opt = document.createElement('option');
        opt.value = lid + ':' + inst;
        opt.textContent = label;
        sel.appendChild(opt);
        added++;
      }
    });

    // Fallback: if the Budget tab DOM hasn't been built yet (user landed
    // on Travel tab from URL), fetch a lightweight list from the server.
    // Avoids the "dropdown empty until you visit Budget tab once" gotcha.
    if (added === 0) {
      const opt = document.createElement('option');
      opt.disabled = true;
      opt.textContent = '(loading crew list…)';
      sel.appendChild(opt);
      fetch(`/projects/${PID}/budget/${BID}/lines.json`, { credentials: 'same-origin' })
        .then(r => r.ok ? r.json() : null)
        .then(data => {
          if (!data || !data.lines) return;
          // Clear and rebuild
          sel.innerHTML = '<option value="">— select a person —</option>';
          data.lines.filter(ln => ln.is_labor).forEach(ln => {
            const qty = Math.max(1, parseInt(ln.quantity || 1, 10) || 1);
            const baseLabel = ln.assigned_crew ? `${ln.description} — ${ln.assigned_crew}` : ln.description;
            for (let inst = 1; inst <= qty; inst++) {
              const opt = document.createElement('option');
              opt.value = ln.id + ':' + inst;
              opt.textContent = (qty > 1) ? `${baseLabel}  (instance ${inst})` : baseLabel;
              sel.appendChild(opt);
            }
          });
        })
        .catch(() => { /* ignore — keep existing fallback option */ });
    }
  }

  document.getElementById('travel-add-day-btn')?.addEventListener('click', () => {
    _populateAddTravelLines();
    document.getElementById('add-travel-date').value = '';
    document.getElementById('travel-add-day-overlay').classList.remove('hidden');
  });

  // openAddTravelForDate — invoked from per-day card "+ Add to this day"
  // button. Same modal but with the date pre-filled so the user only
  // picks the person.
  window.openAddTravelForDate = function(dateIso) {
    _populateAddTravelLines();
    document.getElementById('add-travel-date').value = dateIso;
    document.getElementById('travel-add-day-overlay').classList.remove('hidden');
  };

  window.closeAddTravelDay = function(ev) {
    if (ev && ev.target && ev.target.id !== 'travel-add-day-overlay') return;
    document.getElementById('travel-add-day-overlay').classList.add('hidden');
  };

  document.getElementById('add-travel-save-btn')?.addEventListener('click', async () => {
    const sel  = document.getElementById('add-travel-line').value;
    const date = document.getElementById('add-travel-date').value;
    if (!sel || !date) { alert('Pick a person and a date.'); return; }
    const [line_id, instance] = sel.split(':');
    const r = await fetch(TRAVEL_ADD_URL, {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({line_id: parseInt(line_id), instance: parseInt(instance), date}),
    });
    if (!r.ok) { alert('Add failed'); return; }
    closeAddTravelDay();
    await loadTravelGrid();
  });

  document.getElementById('travel-only-flagged')?.addEventListener('change', loadTravelGrid);
  document.getElementById('travel-refresh-btn')?.addEventListener('click', loadTravelGrid);
  // Travel expand/collapse all controls.
  document.getElementById('travel-expand-all')?.addEventListener('click', () => {
    document.querySelectorAll('#travel-days .travel-day-body').forEach(el => el.classList.remove('travel-collapsed'));
    document.querySelectorAll('#travel-days .travel-chevron').forEach(el => el.textContent = '▾');
  });
  document.getElementById('travel-collapse-all')?.addEventListener('click', () => {
    document.querySelectorAll('#travel-days .travel-day-body').forEach(el => el.classList.add('travel-collapsed'));
    document.querySelectorAll('#travel-days .travel-chevron').forEach(el => el.textContent = '▸');
  });

  // ── Catering grid ──────────────────────────────────────────────────────
  async function loadCateringGrid() {
    // Targets `#catering-days` now (was `catering-tbody` before the
    // 2026-04-28 day-card rework). Pointing at the old id was leaving
    // "Loading…" stuck because innerHTML calls were no-ops on a
    // non-existent element AND any thrown error inside renderCatering*
    // never wrote the recovery banner.
    const daysEl   = document.getElementById('catering-days');
    const totalsEl = document.getElementById('catering-totals');
    totalsEl.innerHTML = '<span class="muted">Loading…</span>';
    daysEl.innerHTML   = '<div class="muted" style="padding:1rem;text-align:center">Loading…</div>';
    try {
      const r = await fetch(CATERING_GRID_URL, { credentials: 'same-origin' });
      if (!r.ok) {
        const txt = await r.text().catch(() => '');
        daysEl.innerHTML =
          `<div style="padding:1rem;color:#ef4444">Server returned ${r.status}: ${_esc(txt.slice(0,200))}</div>`;
        totalsEl.innerHTML = `<span style="color:#ef4444">Failed (HTTP ${r.status})</span>`;
        return;
      }
      _cateringData = await r.json();
      try { renderCateringGrid(); }
      catch (e1) { console.error('renderCateringGrid failed', e1);
                   daysEl.innerHTML = `<div style="padding:1rem;color:#ef4444">renderCateringGrid: ${_esc(e1.message||e1)}</div>`; }
      try { renderCateringBills(); }
      catch (e2) { console.error('renderCateringBills failed', e2); }
      try { renderCateringPersonRollups(); }
      catch (e3) { console.error('renderCateringPersonRollups failed', e3); }
    } catch (err) {
      console.error('loadCateringGrid failed', err);
      daysEl.innerHTML = `<div style="padding:1rem;color:#ef4444">Error: ${_esc(err.message || err)}</div>`;
      totalsEl.innerHTML = `<span style="color:#ef4444">Error: ${_esc(err.message || err)}</span>`;
    }
  }

  function renderCateringGrid() {
    if (!_cateringData) return;
    const days = _cateringData.days || [];
    const bills = _cateringData.bills || [];
    if (days.length === 0) {
      document.getElementById('catering-days').innerHTML =
        '<div class="muted" style="padding:1.2rem;text-align:center;background:var(--bg-card);border:1px solid var(--border);border-radius:8px">No scheduled days yet — add crew + day-types on the Schedule first.</div>';
      document.getElementById('catering-totals').innerHTML =
        '<span class="muted">No catering data yet.</span>';
      return;
    }

    // Map each date → its production-week index (1-based) using the
    // server's payroll-aware `weeks` array. Lets us show "WK 2" labels
    // alongside dates so the user can see the payroll cycle at a glance.
    const weeksList = _cateringData.weeks || [];
    const weekIdxOf = (dateIso) => {
      // weeks are sorted ISO strings of week-start dates. Find the
      // largest week-start that's <= dateIso.
      let idx = 0;
      for (let i = 0; i < weeksList.length; i++) {
        if (weeksList[i] <= dateIso) idx = i;
      }
      return idx + 1;
    };

    let totalExpected = 0, totalMeals = 0;
    let html = '';
    days.forEach((d, idx) => {
      totalExpected += d.expected_cost;
      const mealCount = (d.flags.courtesy_breakfast?d.working_count:0)
                      + (d.flags.first_meal?d.working_count:0)
                      + (d.flags.second_meal?d.working_count:0)
                      + d.working_meal_people.length;
      totalMeals += mealCount;
      const cb = (flag, isOn, label) => {
        const activeStyle = isOn
          ? 'background:#1a2540;color:#5b8af9;border:1px solid #2d4070'
          : 'background:transparent;color:var(--text-muted);border:1px solid var(--border)';
        return `<label style="display:inline-flex;align-items:center;gap:5px;padding:3px 9px;border-radius:5px;cursor:pointer;font-size:11px;${activeStyle}">
                  <input type="checkbox" ${isOn?'checked':''} onclick="cateringMealToggle('${d.date}','${flag}',this.checked)" style="margin:0">${label}
                </label>`;
      };

      // Pretty heading: "Mon · Jun 7, 2027 · Wk 2"
      const [yy,mm,dd] = d.date.split('-').map(n => parseInt(n,10));
      const dt = new Date(yy, mm-1, dd);
      const dow = dt.toLocaleDateString(undefined, {weekday:'short'});
      const longDate = dt.toLocaleDateString(undefined, {month:'short', day:'numeric', year:'numeric'});
      const weekIdx = weekIdxOf(d.date);
      const prodBadge = d.is_production_day
        ? '<span style="font-size:10px;padding:2px 8px;border-radius:6px;background:#14291e;color:#22c55e;border:1px solid #1a4228;text-transform:uppercase;letter-spacing:.4px;font-weight:600">Production Day</span>'
        : '';

      html += `
        <div class="catering-day-card" data-date="${d.date}" style="background:var(--bg-card);border:1px solid var(--border);border-radius:8px;overflow:hidden">
          <div class="catering-day-header"
               onclick="this.parentElement.querySelector('.catering-day-body').classList.toggle('catering-collapsed'); this.querySelector('.catering-chevron').textContent = this.parentElement.querySelector('.catering-day-body').classList.contains('catering-collapsed') ? '▸' : '▾';"
               style="display:flex;align-items:center;gap:12px;padding:10px 14px;background:var(--bg-2);border-bottom:1px solid var(--border);cursor:pointer;user-select:none">
            <span class="catering-chevron" style="color:var(--text-muted);font-size:14px;width:14px">▾</span>
            <strong style="font-size:14px">${_esc(dow)} · ${_esc(longDate)}</strong>
            <span class="muted" style="font-size:11px">Wk ${weekIdx}</span>
            ${prodBadge}
            <span class="muted" style="font-size:11px">${d.working_count} working · ${mealCount} meal${mealCount===1?'':'s'}</span>
            <span style="margin-left:auto;font-weight:600;color:#5b8af9">${_fmt(d.expected_cost)}</span>
          </div>
          <div class="catering-day-body" style="padding:12px 14px">
            <div style="display:flex;flex-wrap:wrap;gap:8px;margin-bottom:10px;align-items:center">
              <span class="muted" style="font-size:11px;text-transform:uppercase;letter-spacing:.4px">Meals served:</span>
              ${cb('courtesy_breakfast', d.flags.courtesy_breakfast, 'Courtesy Bkfst')}
              ${cb('first_meal',         d.flags.first_meal,         '1st Meal')}
              ${cb('second_meal',        d.flags.second_meal,        '2nd Meal')}
            </div>
            ${(() => {
              // Color-coded list helper: assigned person renders white,
              // unassigned slot falls back to the role label rendered in
              // amber italic so the user can scan at a glance for "this
              // slot has no human attached yet". Per user 2026-04-28:
              // working crew + working meal + per-diem all get this.
              const personCell = (p, suffix='') => {
                const name = (p.person || '').trim();
                const role = (p.role   || '').trim();
                if (name && name !== '—') {
                  return `<span style="color:var(--text)">${_esc(name)}</span>${suffix?' '+suffix:''}`;
                }
                return `<span style="color:#f59e0b;font-style:italic" title="No crew assigned to this role">${_esc(role || '—')}</span>${suffix?' '+suffix:''}`;
              };
              const list = (arr, fmt) => arr.map(fmt).join(', ');
              let out = '';
              if (d.working_count > 0) {
                out += `<div style="margin-bottom:.5rem;font-size:12px"><strong>Working crew (${d.working_count}):</strong> ${list(d.working_crew, p => personCell(p))}</div>`;
              }
              if (d.working_meal_people.length > 0) {
                out += `<div style="margin-bottom:.5rem;font-size:12px"><strong>Working meal opt-ins (${d.working_meal_people.length}):</strong> ${list(d.working_meal_people, p => personCell(p))}</div>`;
              }
              if (d.per_diem_people.length > 0) {
                out += `<div style="font-size:12px"><strong>Per Diem (${d.per_diem_people.length}):</strong> ${list(d.per_diem_people, p => personCell(p, '<span class="muted" style="font-size:10px">('+_esc(p.kind)+')</span>'))}</div>`;
              }
              return out;
            })()}
            ${d.working_count===0 && d.working_meal_people.length===0 && d.per_diem_people.length===0 ? '<span class="muted" style="font-size:12px">No working crew or meal opt-ins on this day.</span>' : ''}
          </div>
        </div>
      `;
    });
    document.getElementById('catering-days').innerHTML = html;

    const totalBilled = bills.reduce((acc, b) => acc + parseFloat(b.amount || 0), 0);
    const drift = totalBilled - totalExpected;
    document.getElementById('catering-totals').innerHTML = `
      <span><strong>${days.length}</strong> scheduled days · <strong>${totalMeals}</strong> meals served</span>
      <span>Expected: <strong>${_fmt(totalExpected)}</strong></span>
      <span>Caterer billed: <strong>${_fmt(totalBilled)}</strong></span>
      <span style="color:${drift>0.5?'#ef4444':drift<-0.5?'#22c55e':'var(--text-muted)'}">Drift: <strong>${_fmt(drift)}</strong></span>
    `;
  }

  // Catering expand/collapse all controls.
  document.getElementById('catering-expand-all')?.addEventListener('click', () => {
    document.querySelectorAll('#catering-days .catering-day-body').forEach(el => el.classList.remove('catering-collapsed'));
    document.querySelectorAll('#catering-days .catering-chevron').forEach(el => el.textContent = '▾');
  });
  document.getElementById('catering-collapse-all')?.addEventListener('click', () => {
    document.querySelectorAll('#catering-days .catering-day-body').forEach(el => el.classList.add('catering-collapsed'));
    document.querySelectorAll('#catering-days .catering-chevron').forEach(el => el.textContent = '▸');
  });

  // Per-person Per Diem + Working Meal weekly breakdown.
  // Server provides one row per (line, instance) with a `weeks` map
  // {weekStartIso: $amount} plus a project total. We render two
  // matrices (Per Diem table + Working Meal table) with one column
  // per payroll week + a Total column.
  function renderCateringPersonRollups() {
    if (!_cateringData) return;
    const fmtWeek = (iso) => {
      const [y,m,d] = iso.split('-').map(n => parseInt(n,10));
      const dt = new Date(y, m-1, d);
      return dt.toLocaleDateString(undefined, {month:'short', day:'numeric'});
    };

    function _renderTable(blockId, headId, tbodyId, footId, rows, total, label) {
      const block = document.getElementById(blockId);
      if (!rows || rows.length === 0) {
        if (block) block.style.display = 'none';
        return;
      }
      block.style.display = '';
      const weeks = _cateringData.weeks || [];
      const head = document.getElementById(headId);
      // Per-week column header includes a "Details" button (only on the
      // Per Diem table) so the user can drill into "everyone receiving
      // per diem this week" with one click.
      const isPerDiem = blockId === 'catering-perdiem-block';
      head.innerHTML =
        `<th style="text-align:left;padding:.45rem .7rem">Person</th>` +
        `<th style="text-align:left;padding:.45rem .6rem">Role</th>` +
        weeks.map((wk, i) => {
          const detailsBtn = isPerDiem
            ? `<div style="margin-top:3px"><button class="btn btn-xs btn-ghost" style="font-size:10px;padding:1px 6px" onclick="openPerDiemWeekDetails('${_esc(wk)}')" title="See everyone receiving per diem this week">Details</button></div>`
            : '';
          return `<th style="text-align:right;padding:.45rem .55rem;white-space:nowrap;vertical-align:top" title="Week of ${_esc(wk)}">Wk ${i+1}<br><span class="muted" style="font-weight:400;font-size:10px">${_esc(fmtWeek(wk))}</span>${detailsBtn}</th>`;
        }).join('') +
        `<th style="text-align:right;padding:.45rem .8rem;border-left:1px solid var(--border)">Total</th>` +
        `<th style="text-align:right;padding:.45rem .55rem" title="Total ${label} count for this person">Count</th>` +
        (isPerDiem ? `<th style="text-align:center;padding:.45rem .55rem" title="See per-day breakdown for each person"></th>` : '');

      const tbody = document.getElementById(tbodyId);
      tbody.innerHTML = rows.map(r => `
        <tr style="border-top:1px solid var(--border)" data-ident="${_esc(r.ident)}">
          <td style="padding:.4rem .7rem"><strong>${_esc(r.person)}</strong></td>
          <td class="muted" style="padding:.4rem .6rem">${_esc(r.role)}</td>
          ${weeks.map(wk => {
            const v = r.weeks[wk] || 0;
            const c = r.weekly_counts[wk] || 0;
            return `<td style="text-align:right;padding:.4rem .55rem">${v ? _fmt(v) : '<span class="muted">—</span>'}${c ? `<div class="muted" style="font-size:10px">${c} day${c===1?'':'s'}</div>` : ''}</td>`;
          }).join('')}
          <td style="text-align:right;padding:.4rem .8rem;border-left:1px solid var(--border);font-weight:600">${_fmt(r.total)}</td>
          <td style="text-align:right;padding:.4rem .55rem" class="muted">${r.count}</td>
          ${isPerDiem ? `<td style="text-align:center;padding:.4rem .55rem"><button class="btn btn-xs btn-ghost" style="font-size:10px;padding:1px 6px" onclick="openPerDiemPersonDetails('${_esc(r.ident)}')" title="See day-by-day breakdown for this person">Details</button></td>` : ''}
        </tr>
      `).join('');

      // Footer: weekly column totals + grand total
      const colTotals = weeks.map(wk =>
        rows.reduce((acc, r) => acc + (r.weeks[wk] || 0), 0)
      );
      const colCounts = weeks.map(wk =>
        rows.reduce((acc, r) => acc + (r.weekly_counts[wk] || 0), 0)
      );
      const grandCount = rows.reduce((acc, r) => acc + r.count, 0);
      document.getElementById(footId).innerHTML = `
        <tr style="background:var(--bg-2);font-weight:600;border-top:2px solid var(--border)">
          <td colspan="2" style="padding:.5rem .7rem">${label} TOTAL</td>
          ${weeks.map((wk, i) => `<td style="text-align:right;padding:.5rem .55rem">${_fmt(colTotals[i])}<div class="muted" style="font-size:10px;font-weight:400">${colCounts[i]} day${colCounts[i]===1?'':'s'}</div></td>`).join('')}
          <td style="text-align:right;padding:.5rem .8rem;border-left:1px solid var(--border);color:#5b8af9">${_fmt(total)}</td>
          <td style="text-align:right;padding:.5rem .55rem;font-weight:400" class="muted">${grandCount}</td>
          ${isPerDiem ? '<td></td>' : ''}
        </tr>
      `;
    }
    _renderTable('catering-perdiem-block', 'catering-perdiem-head',
                 'catering-perdiem-tbody', 'catering-perdiem-foot',
                 _cateringData.per_diem_by_person,
                 _cateringData.per_diem_total, 'Per Diem');
    _renderTable('catering-wmeal-block',  'catering-wmeal-head',
                 'catering-wmeal-tbody',   'catering-wmeal-foot',
                 _cateringData.working_meal_by_person,
                 _cateringData.working_meal_total, 'Working Meal');
  }

  // ── Per-diem details + export modals ──────────────────────────────
  // Lightweight in-memory drilldowns: all the data is already in
  // _cateringData.days[].per_diem_people, so each modal is just a
  // filter + render of that slice. No new endpoints needed for
  // viewing — the export modals POST to /catering/export with extra
  // filter params (people / meals / date range).
  const _PD_KIND_LABEL = { full: 'Full Day', breakfast: 'Breakfast', lunch: 'Lunch', dinner: 'Dinner' };

  function _formatYmd(iso) {
    const [y,m,d] = iso.split('-').map(n => parseInt(n,10));
    return new Date(y, m-1, d).toLocaleDateString(undefined,
      {weekday:'short', month:'short', day:'numeric', year:'numeric'});
  }

  function _showCateringModal(title, bodyHtml, footerHtml) {
    let modal = document.getElementById('catering-detail-modal');
    if (!modal) {
      modal = document.createElement('div');
      modal.id = 'catering-detail-modal';
      modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:2000;display:flex;align-items:center;justify-content:center;padding:24px';
      modal.innerHTML = `
        <div style="background:var(--bg-card);border:1px solid var(--border);border-radius:10px;width:min(900px,95vw);max-height:88vh;display:flex;flex-direction:column;overflow:hidden">
          <div id="cdm-header" style="padding:16px 20px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:12px">
            <h3 id="cdm-title" style="margin:0;font-size:1rem;flex:1"></h3>
            <button class="btn btn-xs btn-ghost" onclick="document.getElementById('catering-detail-modal').remove()">✕ Close</button>
          </div>
          <div id="cdm-body" style="padding:16px 20px;overflow-y:auto;flex:1;font-size:.86rem"></div>
          <div id="cdm-footer" style="padding:12px 20px;border-top:1px solid var(--border);display:flex;justify-content:flex-end;gap:10px"></div>
        </div>`;
      modal.addEventListener('click', e => { if (e.target === modal) modal.remove(); });
      document.body.appendChild(modal);
    }
    document.getElementById('cdm-title').textContent = title;
    document.getElementById('cdm-body').innerHTML = bodyHtml;
    document.getElementById('cdm-footer').innerHTML = footerHtml || '';
  }

  // (a) Per-person details — every per-diem date for one ident.
  window.openPerDiemPersonDetails = function(ident) {
    if (!_cateringData) return;
    const rates = _cateringData.rates || {};
    const rateFor = k => rates['per_diem_' + k] || 0;
    const rows = [];
    let total = 0;
    let personLabel = ident, roleLabel = '';
    (_cateringData.days || []).forEach(day => {
      (day.per_diem_people || []).forEach(p => {
        const id = `${p.line_id}:${p.instance}`;
        if (id !== ident) return;
        if (!personLabel || personLabel === ident) {
          personLabel = p.person || '(unassigned)';
          roleLabel = p.role || '';
        }
        const amt = rateFor(p.kind);
        total += amt;
        rows.push({ date: day.date, kind: p.kind, amount: amt });
      });
    });
    if (!rows.length) {
      _showCateringModal('Per Diem — Detail',
        `<p class="muted">No per diem entries found for this person.</p>`);
      return;
    }
    rows.sort((a,b) => a.date < b.date ? -1 : 1);
    let running = 0;
    const body = `
      <p style="margin:0 0 .8rem"><strong>${_esc(personLabel)}</strong>${roleLabel ? ` <span class="muted">— ${_esc(roleLabel)}</span>` : ''}</p>
      <table style="width:100%;border-collapse:collapse;font-size:.85rem">
        <thead><tr style="background:var(--bg-2);text-align:left">
          <th style="padding:.4rem .6rem">Date</th>
          <th style="padding:.4rem .6rem">Kind</th>
          <th style="padding:.4rem .6rem;text-align:right">Amount</th>
          <th style="padding:.4rem .6rem;text-align:right">Running Total</th>
        </tr></thead>
        <tbody>
          ${rows.map(r => { running += r.amount; return `
            <tr style="border-top:1px solid var(--border)">
              <td style="padding:.35rem .6rem">${_esc(_formatYmd(r.date))}</td>
              <td style="padding:.35rem .6rem">${_esc(_PD_KIND_LABEL[r.kind] || r.kind)}</td>
              <td style="padding:.35rem .6rem;text-align:right">${_fmt(r.amount)}</td>
              <td style="padding:.35rem .6rem;text-align:right" class="muted">${_fmt(running)}</td>
            </tr>`; }).join('')}
          <tr style="background:var(--bg-2);font-weight:600;border-top:2px solid var(--border)">
            <td colspan="2" style="padding:.5rem .6rem">${rows.length} day${rows.length===1?'':'s'}</td>
            <td style="padding:.5rem .6rem;text-align:right;color:#5b8af9">${_fmt(total)}</td>
            <td></td>
          </tr>
        </tbody>
      </table>`;
    _showCateringModal(`Per Diem — ${personLabel}`, body);
  };

  // (b) Per-week details — everyone receiving per diem in one week.
  window.openPerDiemWeekDetails = function(weekIso) {
    if (!_cateringData) return;
    const rates = _cateringData.rates || {};
    const rateFor = k => rates['per_diem_' + k] || 0;
    // Same week-key math the server uses (payroll_week_start). Because
    // _cateringData buckets by date, walk the days and bucket into the
    // matching payroll week here client-side.
    const pwStart = _cateringData.payroll_week_start || 0; // 0=Mon..6=Sun
    function _weekKeyOf(iso) {
      const [y,m,d] = iso.split('-').map(n => parseInt(n,10));
      const dt = new Date(y, m-1, d);
      // JS getDay: 0=Sun..6=Sat. Convert to Mon=0..Sun=6 to match Python weekday().
      const jsDow = dt.getDay();
      const dow = (jsDow + 6) % 7;
      const back = ((dow - pwStart) % 7 + 7) % 7;
      const wk = new Date(dt); wk.setDate(wk.getDate() - back);
      const yy = wk.getFullYear(), mm = String(wk.getMonth()+1).padStart(2,'0'), dd = String(wk.getDate()).padStart(2,'0');
      return `${yy}-${mm}-${dd}`;
    }
    const byPerson = {};   // ident → {person, role, dates: [{date, kind, amount}], total}
    let weekTotal = 0;
    (_cateringData.days || []).forEach(day => {
      if (_weekKeyOf(day.date) !== weekIso) return;
      (day.per_diem_people || []).forEach(p => {
        const id = `${p.line_id}:${p.instance}`;
        if (!byPerson[id]) byPerson[id] = {
          person: p.person || '(unassigned)', role: p.role || '',
          dates: [], total: 0,
        };
        const amt = rateFor(p.kind);
        byPerson[id].dates.push({ date: day.date, kind: p.kind, amount: amt });
        byPerson[id].total += amt;
        weekTotal += amt;
      });
    });
    const sorted = Object.entries(byPerson)
      .sort((a,b) => b[1].total - a[1].total || a[1].person.localeCompare(b[1].person));
    if (!sorted.length) {
      _showCateringModal(`Per Diem — Week of ${_formatYmd(weekIso)}`,
        `<p class="muted">No per diem entries this week.</p>`);
      return;
    }
    const body = `
      <p style="margin:0 0 .8rem">Week of <strong>${_esc(_formatYmd(weekIso))}</strong> — ${sorted.length} ${sorted.length===1?'person':'people'}, ${_fmt(weekTotal)} total</p>
      <table style="width:100%;border-collapse:collapse;font-size:.85rem">
        <thead><tr style="background:var(--bg-2);text-align:left">
          <th style="padding:.4rem .6rem">Person</th>
          <th style="padding:.4rem .6rem">Role</th>
          <th style="padding:.4rem .6rem">Days</th>
          <th style="padding:.4rem .6rem;text-align:right">Total</th>
        </tr></thead>
        <tbody>
          ${sorted.map(([id, p]) => `
            <tr style="border-top:1px solid var(--border)">
              <td style="padding:.35rem .6rem"><strong>${_esc(p.person)}</strong></td>
              <td style="padding:.35rem .6rem" class="muted">${_esc(p.role)}</td>
              <td style="padding:.35rem .6rem;font-size:.78rem">
                ${p.dates.map(d => `<div>${_esc(_formatYmd(d.date).replace(/, \d{4}$/,''))} — ${_esc(_PD_KIND_LABEL[d.kind] || d.kind)} (${_fmt(d.amount)})</div>`).join('')}
              </td>
              <td style="padding:.35rem .6rem;text-align:right;font-weight:600">${_fmt(p.total)}</td>
            </tr>`).join('')}
          <tr style="background:var(--bg-2);font-weight:600;border-top:2px solid var(--border)">
            <td colspan="3" style="padding:.5rem .6rem">WEEK TOTAL</td>
            <td style="padding:.5rem .6rem;text-align:right;color:#5b8af9">${_fmt(weekTotal)}</td>
          </tr>
        </tbody>
      </table>`;
    _showCateringModal(`Per Diem — Week of ${_formatYmd(weekIso)}`, body);
  };

  // (c) Per-diem report export modal — pick people + dates, opens the
  // catering-export endpoint with filtering params.
  window.openPerDiemExportModal = function() {
    if (!_cateringData) return;
    const people = (_cateringData.per_diem_by_person || []);
    const days = (_cateringData.days || []).filter(d =>
      (d.per_diem_people || []).length > 0);
    if (!people.length || !days.length) {
      alert('No per diem data to export.');
      return;
    }
    const body = `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:18px">
        <div>
          <div style="display:flex;align-items:center;gap:.5rem;margin-bottom:.4rem">
            <strong style="font-size:.85rem">People</strong>
            <button class="btn btn-xs btn-ghost" onclick="document.querySelectorAll('#pde-people input').forEach(c=>c.checked=true)">All</button>
            <button class="btn btn-xs btn-ghost" onclick="document.querySelectorAll('#pde-people input').forEach(c=>c.checked=false)">None</button>
          </div>
          <div id="pde-people" style="max-height:300px;overflow-y:auto;border:1px solid var(--border);border-radius:6px;padding:.4rem">
            ${people.map(p => `
              <label style="display:block;padding:.25rem .35rem;cursor:pointer;font-size:.83rem">
                <input type="checkbox" value="${_esc(p.ident)}" checked> ${_esc(p.person)} <span class="muted">— ${_esc(p.role)}</span>
              </label>`).join('')}
          </div>
        </div>
        <div>
          <div style="display:flex;align-items:center;gap:.5rem;margin-bottom:.4rem">
            <strong style="font-size:.85rem">Dates</strong>
            <button class="btn btn-xs btn-ghost" onclick="document.querySelectorAll('#pde-dates input').forEach(c=>c.checked=true)">All</button>
            <button class="btn btn-xs btn-ghost" onclick="document.querySelectorAll('#pde-dates input').forEach(c=>c.checked=false)">None</button>
          </div>
          <div id="pde-dates" style="max-height:300px;overflow-y:auto;border:1px solid var(--border);border-radius:6px;padding:.4rem">
            ${days.map(d => `
              <label style="display:block;padding:.25rem .35rem;cursor:pointer;font-size:.83rem">
                <input type="checkbox" value="${_esc(d.date)}" checked> ${_esc(_formatYmd(d.date))} <span class="muted">— ${(d.per_diem_people||[]).length} ppl</span>
              </label>`).join('')}
          </div>
        </div>
      </div>`;
    const footer = `
      <button class="btn btn-ghost" onclick="document.getElementById('catering-detail-modal').remove()">Cancel</button>
      <button class="btn btn-primary" onclick="_runPerDiemExport()">Open Report</button>`;
    _showCateringModal('Export Per Diem Report', body, footer);
  };

  window._runPerDiemExport = function() {
    const people = Array.from(document.querySelectorAll('#pde-people input:checked')).map(c => c.value);
    const dates  = Array.from(document.querySelectorAll('#pde-dates input:checked')).map(c => c.value);
    if (!people.length || !dates.length) {
      alert('Pick at least one person and one date.');
      return;
    }
    const url = `/projects/${PID}/budget/${BID}/catering/export?dates=${encodeURIComponent(dates.join(','))}&include=perdiem&people=${encodeURIComponent(people.join(','))}&recipient=upm`;
    window.open(url, '_blank');
    document.getElementById('catering-detail-modal').remove();
  };

  // (d) Catering breakdown export — meal types + date range, output is
  // one table per (meal × day).
  window.openCateringBreakdownExportModal = function() {
    if (!_cateringData) return;
    const days = _cateringData.days || [];
    if (!days.length) { alert('No catering days yet.'); return; }
    const minDate = days[0].date;
    const maxDate = days[days.length-1].date;
    const meals = [
      { key: 'courtesy_breakfast', label: '🥐 Courtesy Breakfast' },
      { key: 'first_meal',         label: '🍽 First Meal' },
      { key: 'second_meal',        label: '🍽 Second Meal' },
      { key: 'working_meal',       label: '🥪 Working Meal' },
      { key: 'per_diem',           label: '💰 Per Diem' },
      { key: 'craft_services',     label: '☕ Craft Services' },
    ];
    const body = `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:18px">
        <div>
          <strong style="font-size:.85rem">Meal Types</strong>
          <p class="muted" style="font-size:.76rem;margin:.25rem 0 .5rem">Pick which meals to include — separate caterers? export each one.</p>
          <div id="cbe-meals" style="border:1px solid var(--border);border-radius:6px;padding:.4rem">
            ${meals.map(m => `
              <label style="display:block;padding:.3rem .35rem;cursor:pointer;font-size:.85rem">
                <input type="checkbox" value="${m.key}" ${['first_meal','courtesy_breakfast','second_meal'].includes(m.key)?'checked':''}> ${m.label}
              </label>`).join('')}
          </div>
        </div>
        <div>
          <strong style="font-size:.85rem">Date Range</strong>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:.5rem;margin-top:.4rem">
            <div><label style="font-size:.76rem;color:var(--text-muted)">From</label>
              <input type="date" id="cbe-from" value="${minDate}" min="${minDate}" max="${maxDate}" style="width:100%"></div>
            <div><label style="font-size:.76rem;color:var(--text-muted)">To</label>
              <input type="date" id="cbe-to" value="${maxDate}" min="${minDate}" max="${maxDate}" style="width:100%"></div>
          </div>
          <p class="muted" style="font-size:.74rem;margin-top:.55rem">Output: one table per (meal × day). Empty days are skipped.</p>
        </div>
      </div>`;
    const footer = `
      <button class="btn btn-ghost" onclick="document.getElementById('catering-detail-modal').remove()">Cancel</button>
      <button class="btn btn-primary" onclick="_runCateringBreakdownExport()">Open Report</button>`;
    _showCateringModal('Export Catering Breakdown', body, footer);
  };

  window._runCateringBreakdownExport = function() {
    const meals = Array.from(document.querySelectorAll('#cbe-meals input:checked')).map(c => c.value);
    const from  = document.getElementById('cbe-from').value;
    const to    = document.getElementById('cbe-to').value;
    if (!meals.length) { alert('Pick at least one meal type.'); return; }
    if (!from || !to)  { alert('Pick a date range.'); return; }
    if (from > to)     { alert('From date must be before To date.'); return; }
    // Expand date range to a CSV. Server will skip empty days within it.
    const dates = [];
    const [fy,fm,fd] = from.split('-').map(n=>parseInt(n,10));
    const [ty,tm,td] = to.split('-').map(n=>parseInt(n,10));
    let cur = new Date(fy, fm-1, fd), end = new Date(ty, tm-1, td);
    while (cur <= end) {
      const yy=cur.getFullYear(), mm=String(cur.getMonth()+1).padStart(2,'0'), dd=String(cur.getDate()).padStart(2,'0');
      dates.push(`${yy}-${mm}-${dd}`);
      cur.setDate(cur.getDate()+1);
    }
    const url = `/projects/${PID}/budget/${BID}/catering/export?dates=${encodeURIComponent(dates.join(','))}&meals=${encodeURIComponent(meals.join(','))}&breakdown=1&recipient=catering`;
    window.open(url, '_blank');
    document.getElementById('catering-detail-modal').remove();
  };

  function renderCateringBills() {
    const bills = _cateringData?.bills || [];
    const tbody = document.getElementById('catering-bills-tbody');
    if (!bills.length) {
      tbody.innerHTML = '<tr><td colspan="5" class="muted" style="padding:.7rem 1rem;text-align:center;font-size:.82rem">No caterer bills entered yet.</td></tr>';
      return;
    }
    tbody.innerHTML = bills.map(b => `
      <tr>
        <td style="padding:.5rem .8rem"><strong>${_esc(b.period_start)}</strong> → ${_esc(b.period_end)}</td>
        <td>${_esc(b.vendor || '—')}</td>
        <td style="text-align:right;font-weight:600">${_fmt(b.amount)}</td>
        <td class="muted">${_esc(b.note || '')}</td>
        <td style="text-align:center">
          <button class="btn btn-xs btn-ghost" onclick='openCateringBill(${JSON.stringify(b).replace(/'/g, "&#39;")})'>Edit</button>
          <button class="btn btn-xs btn-ghost" style="color:#ef4444" onclick="deleteCateringBill(${b.id})">✕</button>
        </td>
      </tr>
    `).join('');
  }

  window.cateringMealToggle = async function(date, flag, value) {
    const r = await fetch(CATERING_MEAL_URL, {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({date, flag, value}),
    });
    if (!r.ok) alert('Meal toggle failed');
    await loadCateringGrid();
  };

  window.openCateringBill = function(bill) {
    document.getElementById('catering-bill-title').textContent = bill ? 'Edit Caterer Bill' : 'Add Caterer Bill';
    document.getElementById('cb-id').value     = bill?.id || '';
    document.getElementById('cb-start').value  = bill?.period_start || '';
    document.getElementById('cb-end').value    = bill?.period_end   || '';
    document.getElementById('cb-vendor').value = bill?.vendor || '';
    document.getElementById('cb-amount').value = bill?.amount || '';
    document.getElementById('cb-note').value   = bill?.note   || '';
    document.getElementById('catering-bill-overlay').classList.remove('hidden');
  };

  window.closeCateringBill = function(ev) {
    if (ev && ev.target && ev.target.id !== 'catering-bill-overlay') return;
    document.getElementById('catering-bill-overlay').classList.add('hidden');
  };

  document.getElementById('catering-add-bill-btn')?.addEventListener('click', () => openCateringBill(null));
  document.getElementById('catering-refresh-btn')?.addEventListener('click', loadCateringGrid);

  document.getElementById('cb-save-btn')?.addEventListener('click', async () => {
    const payload = {
      id:           document.getElementById('cb-id').value || null,
      period_start: document.getElementById('cb-start').value,
      period_end:   document.getElementById('cb-end').value,
      vendor:       document.getElementById('cb-vendor').value,
      amount:       document.getElementById('cb-amount').value,
      note:         document.getElementById('cb-note').value,
    };
    if (!payload.period_start || !payload.period_end) { alert('Set both period dates.'); return; }
    const r = await fetch(CATERING_BILL_URL, {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify(payload),
    });
    if (!r.ok) { alert('Save failed'); return; }
    closeCateringBill();
    await loadCateringGrid();
  });

  window.deleteCateringBill = async function(id) {
    if (!confirm('Delete this caterer bill?')) return;
    const r = await fetch(CATERING_BILL_URL + '/' + id, { method: 'DELETE' });
    if (!r.ok) { alert('Delete failed'); return; }
    await loadCateringGrid();
  };

  // ── Tab activation hooks: load data the FIRST time the panel becomes
  //    active, regardless of whether activation came from a real click,
  //    a programmatic btn.click() from the URL handler, or pre-active
  //    state at page load. MutationObserver on the panel's class list
  //    is the only reliable way to catch all three.
  let _travelLoaded = false, _cateringLoaded = false;
  function _watchPanel(tabName, loader, loadedFlag) {
    const panel = document.getElementById('tab-' + tabName);
    if (!panel) return;
    const fire = () => {
      if (!window['_'+tabName+'Loaded'] && panel.classList.contains('active')) {
        window['_'+tabName+'Loaded'] = true;
        loader();
      }
    };
    new MutationObserver(fire).observe(panel, { attributes: true, attributeFilter: ['class'] });
    fire();  // catch initial state if user landed directly on this tab
  }
  // Use module-scoped flags via closures instead of window globals
  document.querySelector('.tab-btn[data-tab="travel"]')?.addEventListener('click', () => {
    if (!_travelLoaded || window._travelStale) { _travelLoaded = true; window._travelStale = false; loadTravelGrid(); }
  });
  document.querySelector('.tab-btn[data-tab="catering"]')?.addEventListener('click', () => {
    if (!_cateringLoaded || window._cateringStale) { _cateringLoaded = true; window._cateringStale = false; loadCateringGrid(); }
  });
  // MutationObserver fallback for URL-driven activation (where the click
  // event fires before this IIFE registers the listener). Watches for the
  // .active class to land on the panel and triggers the loader once.
  const _travelPanel = document.getElementById('tab-travel');
  if (_travelPanel) {
    const fireT = () => {
      if ((!_travelLoaded || window._travelStale) && _travelPanel.classList.contains('active')) {
        _travelLoaded = true; window._travelStale = false; loadTravelGrid();
      }
    };
    new MutationObserver(fireT).observe(_travelPanel, { attributes: true, attributeFilter: ['class'] });
    fireT();
  }
  const _cateringPanel = document.getElementById('tab-catering');
  if (_cateringPanel) {
    const fireC = () => {
      if ((!_cateringLoaded || window._cateringStale) && _cateringPanel.classList.contains('active')) {
        _cateringLoaded = true; window._cateringStale = false; loadCateringGrid();
      }
    };
    new MutationObserver(fireC).observe(_cateringPanel, { attributes: true, attributeFilter: ['class'] });
    fireC();
  }
  console.log('[Travel/Catering] IIFE ready; observers attached');

  // ── Activity tab ───────────────────────────────────────────────
  // Loads the audit feed scoped to the user's role. One row per
  // mutation, with [Undo] for budget-line changes that haven't been
  // superseded. Filter chips: All / Mine / Today / Last 7 days.
  let _activityLoaded = false;
  let _activityFilter = 'all';
  let _activityEntity = 'all';
  // Color stripe + icon by entity-type bucket. Keeps a glance-level
  // separation between budget edits, doc activity, and ledger activity.
  const _ENTITY_BUCKET = {
    budget_line:'budget', budget_settings:'budget', budget_mode:'budget',
    tax_credit:'budget', schedule_day:'budget', schedule_batch:'budget',
    production_day:'budget', travel_flag:'budget', travel_detail:'budget',
    travel_day:'budget', catering_meal:'budget', catering_bill:'budget',
    project:'budget',
    doc_upload:'docs', doc_upload_bulk:'docs',
    transaction:'actuals', transaction_match:'actuals',
    qbo_sync:'qbo', qbo_accounts:'qbo',
  };
  const _BUCKET_STYLE = {
    budget:  {color:'#3b82f6', icon:'📊', label:'Budget'},
    docs:    {color:'#a855f7', icon:'📄', label:'Docs'},
    actuals: {color:'#10b981', icon:'💰', label:'Actuals'},
    qbo:     {color:'#f59e0b', icon:'🔄', label:'QBO'},
    other:   {color:'#6b7280', icon:'•',  label:''},
  };
  function _activityBucket(et) { return _ENTITY_BUCKET[et] || 'other'; }
  function _fmtActivityWhen(iso) {
    if (!iso) return '—';
    const d = new Date(iso);
    const now = new Date();
    const diffMin = Math.round((now - d) / 60000);
    if (diffMin < 1) return 'just now';
    if (diffMin < 60) return diffMin + 'm ago';
    if (diffMin < 24*60) return Math.round(diffMin/60) + 'h ago';
    return d.toLocaleDateString() + ' ' + d.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'});
  }
  function _fmtActivityDelta(d) {
    if (!d) return '';
    const sign = d > 0 ? '+' : '−';
    const abs  = Math.abs(d).toLocaleString('en-US', {minimumFractionDigits:0, maximumFractionDigits:0});
    const color = d > 0 ? '#f59e0b' : '#10b981';  // increase=warn, decrease=savings
    return `<span style="color:${color};font-weight:600">${sign}$${abs}</span>`;
  }
  // Second line under an activity row: what the receipt/charge actually is —
  // renamed-to filename, vendor, amount, date, code, linked receipt. (User 2026-06-17.)
  function _activityDetail(d) {
    if (!d) return '';
    const esc = s => (s == null ? '' : ('' + s)).replace(/[<>&]/g, c => ({'<':'&lt;','>':'&gt;','&':'&amp;'}[c]));
    const money = a => a == null ? '' : ('$' + Math.abs(a).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2}));
    const bits = [];
    if (d.kind === 'receipt') {
      if (d.filed && d.filed !== d.original) bits.push('renamed → <span style="color:var(--text)">' + esc(d.filed) + '</span>');
      const meta = [d.vendor && esc(d.vendor), d.amount != null && money(d.amount), d.date && esc(d.date), d.doc_type && esc(d.doc_type)].filter(Boolean);
      if (meta.length) bits.push(meta.join(' · '));
      if (d.status) bits.push('status: ' + esc(d.status));
    } else if (d.kind === 'charge') {
      const meta = [d.vendor && esc(d.vendor), d.amount != null && money(d.amount), d.date && esc(d.date)].filter(Boolean);
      if (meta.length) bits.push(meta.join(' · '));
      if (d.code) bits.push('code: ' + esc(d.code));
      if (d.linked_receipt) bits.push('📎 ' + esc(d.linked_receipt));
    }
    if (!bits.length) return '';
    return '<div style="font-size:.72rem;color:var(--text-muted);margin-top:2px;line-height:1.35">' + bits.join('<br>') + '</div>';
  }
  async function loadActivityFeed() {
    const tbody = document.getElementById('activity-tbody');
    const empty = document.getElementById('activity-empty');
    if (!tbody) return;
    tbody.innerHTML = '<tr><td colspan="5" style="color:var(--text-muted);text-align:center;padding:1rem">Loading…</td></tr>';
    try {
      const r = await fetch(`/projects/${PID}/budget/${BID}/activity?filter=${_activityFilter}&entity=${_activityEntity}`);
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const data = await r.json();
      if (!data.items || !data.items.length) {
        tbody.innerHTML = '';
        if (empty) empty.style.display = 'block';
        return;
      }
      if (empty) empty.style.display = 'none';
      tbody.innerHTML = data.items.map(it => {
        const undoBtn = (it.can_undo && !it.undone)
          ? `<input type="checkbox" class="act-sel" value="${it.id}" onchange="activitySelCount()" title="Select for bulk undo" style="margin-right:6px;vertical-align:middle"><button class="btn btn-xs" data-act-undo="${it.id}">Undo</button>`
          : (it.undone ? '<span class="muted" style="font-size:.75rem">undone</span>' : '');
        // Prefer the server-side `note` for richer summaries on docs/
        // actuals/qbo events. Fall back to `summary` for legacy budget
        // rows that don't carry a note.
        const rawText   = (it.note && it.note.trim()) ? it.note : (it.summary || '');
        const summaryEsc = rawText.replace(/[<>&]/g, c => ({'<':'&lt;','>':'&gt;','&':'&amp;'}[c]));
        const whoEsc    = (it.who || '—').replace(/[<>&]/g, c => ({'<':'&lt;','>':'&gt;','&':'&amp;'}[c]));
        const bucket    = _activityBucket(it.entity_type);
        const style     = _BUCKET_STYLE[bucket] || _BUCKET_STYLE.other;
        const stripe    = `border-left:3px solid ${style.color}`;
        const iconHtml  = `<span title="${style.label}" style="margin-right:.4rem">${style.icon}</span>`;
        return `<tr ${it.undone ? 'style="opacity:.55;'+stripe+'"' : 'style="'+stripe+'"'}>
          <td style="font-size:.85em;color:var(--text-muted);padding-left:.6rem">${_fmtActivityWhen(it.when)}</td>
          <td>${whoEsc}</td>
          <td>${iconHtml}${summaryEsc}${_activityDetail(it.detail)}</td>
          <td class="col-num">${_fmtActivityDelta(it.dollar_delta)}</td>
          <td>${undoBtn}</td>
        </tr>`;
      }).join('');
      // Wire undo buttons
      tbody.querySelectorAll('[data-act-undo]').forEach(btn => {
        btn.addEventListener('click', async () => {
          if (!confirm('Undo this change?')) return;
          const aid = btn.getAttribute('data-act-undo');
          btn.disabled = true; btn.textContent = '…';
          try {
            const r2 = await fetch(`/projects/${PID}/budget/${BID}/activity/${aid}/undo`, {method:'POST'});
            const j2 = await r2.json();
            if (!r2.ok) {
              alert(j2.message || j2.error || 'Undo failed');
              btn.disabled = false; btn.textContent = 'Undo';
              return;
            }
            // Reload activity feed; user can hard-refresh budget if needed
            loadActivityFeed();
          } catch (e) {
            alert('Undo failed: ' + e.message);
            btn.disabled = false; btn.textContent = 'Undo';
          }
        });
      });
      activitySelCount();   // reset the bulk-undo bar after a re-render
    } catch (e) {
      tbody.innerHTML = `<tr><td colspan="5" style="color:#ef4444;text-align:center;padding:1rem">Failed to load: ${e.message}</td></tr>`;
    }
  }
  // Bulk-undo: select multiple activity rows and undo them at once. Permissions
  // (own / admin) are enforced server-side per row. (User 2026-06-16.)
  window.activitySelCount = function () {
    const n = document.querySelectorAll('#activity-tbody .act-sel:checked').length;
    const c = document.getElementById('act-sel-count'); if (c) c.textContent = n;
    const btn = document.getElementById('activity-undo-selected'); if (btn) btn.style.display = n ? '' : 'none';
  };
  window.activityUndoSelected = async function () {
    const ids = [...document.querySelectorAll('#activity-tbody .act-sel:checked')].map(c => parseInt(c.value));
    if (!ids.length) return;
    if (!confirm(`Undo ${ids.length} selected change${ids.length !== 1 ? 's' : ''}?`)) return;
    const btn = document.getElementById('activity-undo-selected');
    if (btn) { btn.disabled = true; btn.textContent = 'Undoing…'; }
    try {
      const r = await fetch(`/projects/${PID}/budget/${BID}/activity/undo-batch`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ids }) });
      const j = await r.json().catch(() => ({}));
      if (!r.ok) { alert(j.error || 'Undo failed'); }
      else if (j.failed && j.failed.length) { alert(`Undid ${j.undone}. ${j.failed.length} couldn’t be undone (not yours, already undone, or superseded).`); }
      loadActivityFeed();
    } catch (e) { alert('Undo failed: ' + e.message); }
    finally { if (btn) { btn.disabled = false; btn.innerHTML = '↶ Undo selected (<span id="act-sel-count">0</span>)'; } }
  };
  // Filter chip clicks
  document.querySelectorAll('#activity-filters [data-act-filter]').forEach(btn => {
    btn.addEventListener('click', () => {
      _activityFilter = btn.getAttribute('data-act-filter');
      document.querySelectorAll('#activity-filters [data-act-filter]')
        .forEach(b => b.style.opacity = (b === btn ? '1' : '.7'));
      loadActivityFeed();
    });
  });
  // Entity-type chip clicks (Budget / Docs / Actuals / QBO / All)
  document.querySelectorAll('#activity-entity-filters [data-act-entity]').forEach(btn => {
    btn.addEventListener('click', () => {
      _activityEntity = btn.getAttribute('data-act-entity');
      document.querySelectorAll('#activity-entity-filters [data-act-entity]')
        .forEach(b => b.style.opacity = (b === btn ? '1' : '.7'));
      loadActivityFeed();
    });
  });
  document.getElementById('activity-refresh')?.addEventListener('click', loadActivityFeed);
  // Tab click + MutationObserver pattern (matches travel/catering)
  document.querySelector('.tab-btn[data-tab="activity"]')?.addEventListener('click', () => {
    _activityLoaded = true; loadActivityFeed();
  });
  const _activityPanel = document.getElementById('tab-activity');
  if (_activityPanel) {
    const fireA = () => {
      if (!_activityLoaded && _activityPanel.classList.contains('active')) {
        _activityLoaded = true; loadActivityFeed();
      }
    };
    new MutationObserver(fireA).observe(_activityPanel, { attributes: true, attributeFilter: ['class'] });
    fireA();
  }
  } catch (err) {
    console.error('[Travel/Catering] IIFE crashed:', err);
    // Show the error visibly so a screenshot tells us what broke.
    try {
      const tt = document.getElementById('travel-totals');
      if (tt) tt.innerHTML = '<span style="color:#ef4444">JS error: ' + (err.message || err) + '</span>';
      const ct = document.getElementById('catering-totals');
      if (ct) ct.innerHTML = '<span style="color:#ef4444">JS error: ' + (err.message || err) + '</span>';
      // Also pop a small banner at the bottom of the screen — visible
      // regardless of which tab is active.
      const banner = document.createElement('div');
      banner.style.cssText = 'position:fixed;bottom:16px;right:16px;background:#2a1414;color:#ef4444;border:1px solid #ef4444;padding:10px 14px;border-radius:6px;z-index:9999;max-width:480px;font-size:12px;font-family:monospace';
      banner.textContent = 'Travel/Catering JS error: ' + (err.message || err);
      document.body.appendChild(banner);
    } catch (_) { /* swallow */ }
  }
})();
