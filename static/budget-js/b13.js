// Extracted from a templates/budget.html inline block (audit M9, 2026-07-20).
// Jinja values arrive via window.__BJ (set by the inline preamble before this
// script tag). Classic script, document order — semantics identical to inline.

  function _epFmtUSD(n) {
    if (n == null || isNaN(n)) return '';
    return '$' + Number(n).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
  }
  function _epEscape(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }
  // ── Export Options modal ─────────────────────────────────────────────
  // Caller passes (event, format, variant) where:
  //   format   : 'pdf' | 'csv' | 'mmb' | 'showbiz'
  //   variant  : for pdf/csv only — 'topsheet' | 'detail' | 'working'
  // Modal collects suppress_zeros + fee-dispersed override, then builds
  // the right URL with query params and triggers the download.
  let _exportPending = null;
  window.openExportOptions = function (ev, format, variant) {
    if (ev && ev.preventDefault) ev.preventDefault();
    _exportPending = { format: format, variant: variant };
    const labelMap = {
      pdf:     (variant === 'detail' ? 'Full Detail PDF' : 'Top Sheet PDF'),
      csv:     (variant === 'working' ? 'Line Detail CSV' : 'Top Sheet CSV'),
      mmb:     'Movie Magic Budgeting (.txt)',
      showbiz: 'ShowBiz Budgeting (.txt)',
    };
    document.getElementById('export-opts-target').textContent =
      'Format: ' + (labelMap[format] || format);
    // Reset checkboxes/radios to defaults each open.
    document.getElementById('export-opt-suppress-zeros').checked = false;
    // Travel notes checkbox — only meaningful for PDF detail export.
    // Hide for non-PDF formats (and for PDF top-sheet, which doesn't
    // render per-line rows). Default off so the export stays compact.
    const _tnWrap = document.getElementById('export-opt-travel-notes-wrap');
    if (_tnWrap) {
      _tnWrap.style.display = (format === 'pdf' && variant === 'detail') ? '' : 'none';
    }
    const _tnBox = document.getElementById('export-opt-travel-notes');
    if (_tnBox) _tnBox.checked = false;
    document.querySelectorAll('input[name="export-opt-fee"]').forEach(r => {
      r.checked = (r.value === '');
    });
    // Show what the saved budget setting is in the "Use current" label.
    const _isDispNow = window.__BJ["b13__isDispNow"];
    document.getElementById('export-opt-fee-current').textContent =
      _isDispNow ? '(currently: dispersed)' : '(currently: separate line)';
    // Columns picker — PDF only; defaults follow the current view.
    const _colsWrap = document.getElementById('export-opt-cols-wrap');
    if (_colsWrap) {
      _colsWrap.style.display = (format === 'pdf') ? '' : 'none';
      // Default: working view shows Est+Work+Var; estimated view just
      // Est; actual view Est+Work+Act+Var with Work-V-Act basis.
      const _hasActual = window.__BJ["b13__hasActual"];
      const _isWorkView = window.__BJ["b13__isWorkView"];
      const _isActView = window.__BJ["b13__isActView"];
      document.getElementById('export-col-est').checked  = true;
      document.getElementById('export-col-work').checked = _isWorkView || _isActView;
      document.getElementById('export-col-act').checked  = _isActView || _hasActual;
      document.getElementById('export-col-var').checked  = _isWorkView || _isActView;
      document.getElementById('export-var-basis').value =
        _isActView ? 'work_v_act' : 'est_v_work';
      _updateVarBasisVisibility();
    }
    document.getElementById('export-options-overlay').classList.remove('hidden');
  };

  // Hide the variance-basis dropdown when Variance is unchecked or when
  // fewer than 2 value columns are checked (variance needs a pair).
  function _updateVarBasisVisibility() {
    const wrap = document.getElementById('export-var-basis-wrap');
    if (!wrap) return;
    const varChecked = document.getElementById('export-col-var')?.checked;
    const valueColsChecked =
      ['export-col-est','export-col-work','export-col-act']
        .filter(id => document.getElementById(id)?.checked).length;
    wrap.style.display = (varChecked && valueColsChecked >= 2) ? '' : 'none';
  }
  // Wire change events for live show/hide of the basis dropdown.
  ['export-col-est','export-col-work','export-col-act','export-col-var']
    .forEach(id => {
      const el = document.getElementById(id);
      if (el) el.addEventListener('change', _updateVarBasisVisibility);
    });

  window.closeExportOptions = function (ev) {
    if (ev && ev.target && ev.target.id !== 'export-options-overlay') return;
    document.getElementById('export-options-overlay').classList.add('hidden');
    _exportPending = null;
  };

  document.getElementById('btn-export-go')?.addEventListener('click', () => {
    if (!_exportPending) return;
    const { format, variant } = _exportPending;
    const params = new URLSearchParams();
    if (document.getElementById('export-opt-suppress-zeros').checked) {
      params.set('suppress_zeros', '1');
    }
    const feeRadio = document.querySelector('input[name="export-opt-fee"]:checked');
    if (feeRadio && feeRadio.value !== '') {
      params.set('fee_dispersed', feeRadio.value);
    }
    let url;
    if (format === 'pdf') {
      url = '/projects/' + PID + '/budget/' + BID + '/export.pdf';
      if (variant === 'detail') params.set('detail', '1');
      // Column picker — PDF only. Pass each checked column as a
      // separate flag so the server's URL parser is simple and the
      // template's column-visibility test is a single Jinja bool.
      if (document.getElementById('export-col-est')?.checked)  params.set('col_est',  '1');
      if (document.getElementById('export-col-work')?.checked) params.set('col_work', '1');
      if (document.getElementById('export-col-act')?.checked)  params.set('col_act',  '1');
      if (document.getElementById('export-col-var')?.checked)  params.set('col_var',  '1');
      const _vb = document.getElementById('export-var-basis')?.value;
      if (_vb) params.set('var_basis', _vb);
      // Per-person travel / per-diem mirror rows — PDF detail only.
      // Server reads ?travel_notes=1 to compute travel_mirror_by_line
      // and the template renders the indented "↪ Hotel — Crew /
      // Flights / Per Diem" rows under each labor person. 2026-05-19.
      if (document.getElementById('export-opt-travel-notes')?.checked) {
        params.set('travel_notes', '1');
      }
    } else if (format === 'csv') {
      url = '/projects/' + PID + '/budget/' + BID + '/export.csv';
      if (variant) params.set('type', variant);
    } else if (format === 'mmb') {
      url = '/projects/' + PID + '/budget/' + BID + '/export.mmb.txt';
    } else if (format === 'showbiz') {
      url = '/projects/' + PID + '/budget/' + BID + '/export.showbiz.txt';
    } else { return; }
    const qs = params.toString();
    const full = qs ? (url + '?' + qs) : url;
    // Open PDFs in a new tab so the user can see the result; keep
    // download-style files inline-navigated so the browser handles them.
    if (format === 'pdf') window.open(full, '_blank');
    else window.location.href = full;
    closeExportOptions();
  });

  function openExportPreview(ev, target) {
    if (ev && ev.preventDefault) ev.preventDefault();
    var title = (target === 'mmb') ? 'Preview as Movie Magic' : 'Preview as ShowBiz';
    document.getElementById('export-preview-title').textContent = title;
    document.getElementById('export-preview-body').innerHTML =
      '<p class="muted" style="padding:1rem">Loading…</p>';
    document.getElementById('export-preview-overlay').classList.add('is-open');
    document.getElementById('export-preview-drawer').classList.add('is-open');
    var url = '/projects/' + PID + '/budget/' + BID + '/preview/' + target;
    fetch(url).then(function (r) { return r.json(); }).then(function (data) {
      if (data.error) {
        document.getElementById('export-preview-body').innerHTML =
          '<p class="muted" style="padding:1rem">Error: ' + _epEscape(data.error) + '</p>';
        return;
      }
      var html = ['<table><thead><tr>',
        '<th style="width:80px">Acct</th>',
        '<th>Description</th>',
        '<th class="ep-num">Qty</th>',
        '<th class="ep-num">Rate</th>',
        '<th style="width:50px">Fringe</th>',
        '<th class="ep-num">Amount</th>',
        '</tr></thead><tbody>'];
      (data.sections || []).forEach(function (sec) {
        html.push('<tr class="ep-section-row"><td>' + _epEscape(sec.code) +
                  '</td><td colspan="4">' + _epEscape(sec.name) +
                  '</td><td class="ep-num">' + _epFmtUSD(sec.subtotal) + '</td></tr>');
        (sec.lines || []).forEach(function (ln) {
          html.push('<tr><td class="muted" style="font-size:.75rem">' + _epEscape(sec.code) +
                    '</td><td>' + _epEscape(ln.description) +
                    '</td><td class="ep-num">' + (ln.qty || '') +
                    '</td><td class="ep-num">' + _epFmtUSD(ln.rate) +
                    '</td><td>' + _epEscape(ln.fringe || '') +
                    '</td><td class="ep-num">' + _epFmtUSD(ln.amount) + '</td></tr>');
        });
      });
      html.push('<tr class="ep-grand-row"><td colspan="5">GRAND TOTAL</td>' +
                '<td class="ep-num">' + _epFmtUSD(data.grand_total) + '</td></tr>');
      html.push('</tbody></table>');
      document.getElementById('export-preview-body').innerHTML = html.join('');
    }).catch(function (e) {
      document.getElementById('export-preview-body').innerHTML =
        '<p class="muted" style="padding:1rem">Preview failed: ' + _epEscape(e.message || e) + '</p>';
    });
  }
  function closeExportPreview() {
    document.getElementById('export-preview-overlay').classList.remove('is-open');
    document.getElementById('export-preview-drawer').classList.remove('is-open');
  }
