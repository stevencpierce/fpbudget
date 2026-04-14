// ── Smart CSV Import (CSP-safe, no inline handlers) ────────────────────────
document.addEventListener('DOMContentLoaded', function() {
  var btn     = document.getElementById('import-csv-btn');
  var fileInp = document.getElementById('import-csv-file');
  var modal   = document.getElementById('import-csv-modal');
  var status  = document.getElementById('import-csv-status');
  if (!btn || !fileInp || !modal) return;

  var analyzeUrl = fileInp.dataset.analyzeUrl;
  var applyUrl   = fileInp.dataset.applyUrl;

  // Persisted state across analyze → modal → apply
  var _file            = null;
  var _rawPreview      = [];    // raw rows (first 20) for the header picker
  var _headerRowIndex  = 0;
  var _headers         = [];
  var _targetFields    = [];
  var _previewRows     = [];

  // ── Open file picker ──────────────────────────────────────────────────────
  btn.addEventListener('click', function() { fileInp.click(); });

  fileInp.addEventListener('change', function() {
    if (!fileInp.files.length) return;
    _file = fileInp.files[0];
    status.textContent = 'Analyzing ' + _file.name + '…';
    _analyze(null);  // let server auto-detect header
    fileInp.value = '';  // allow re-upload of same file
  });

  function _analyze(headerIdxOverride) {
    if (!_file) return;
    var fd = new FormData();
    fd.append('file', _file);
    if (headerIdxOverride !== null && headerIdxOverride !== undefined) {
      fd.append('header_row_index', headerIdxOverride);
    }
    fetch(analyzeUrl, { method: 'POST', body: fd })
      .then(function(r) { return r.json().then(function(b) { return {ok: r.ok, body: b}; }); })
      .then(function(res) {
        if (!res.ok) {
          status.textContent = 'Error: ' + (res.body.error || 'Upload failed');
          return;
        }
        status.textContent = '';
        _rawPreview     = res.body.raw_preview || [];
        _headerRowIndex = res.body.header_row_index || 0;
        _headers        = res.body.headers || [];
        _targetFields   = res.body.target_fields || [];
        _previewRows    = res.body.preview_rows || [];
        _renderHeaderPicker();
        _renderMappingTable(res.body.mappings || []);
        _renderPreviewTable(_previewRows);
        _setSummary(res.body.row_count, res.body.mappings || [], res.body.auto_detected_header_index);
        modal.classList.remove('hidden');
      })
      .catch(function(e) { status.textContent = 'Error: ' + e.message; });
  }

  // ── Modal controls ────────────────────────────────────────────────────────
  modal.querySelector('.modal-overlay').addEventListener('click', function() {
    modal.classList.add('hidden');
  });
  document.getElementById('import-csv-cancel').addEventListener('click', function() {
    modal.classList.add('hidden');
  });
  document.getElementById('import-csv-apply-btn').addEventListener('click', _applyImport);

  // ── Render header row picker (clickable raw preview) ──────────────────────
  function _renderHeaderPicker() {
    var container = document.getElementById('import-csv-header-picker');
    container.innerHTML = '';
    if (!_rawPreview.length) return;

    var table = document.createElement('table');
    table.className = 'import-rawpreview-table';

    // Column count = max across all rows
    var maxCols = 0;
    _rawPreview.forEach(function(r) { if (r.length > maxCols) maxCols = r.length; });

    _rawPreview.forEach(function(row, idx) {
      var tr = document.createElement('tr');
      tr.dataset.rowIdx = idx;
      tr.className = 'import-rawpreview-row';
      if (idx === _headerRowIndex) tr.classList.add('is-header');
      if (idx < _headerRowIndex) tr.classList.add('above-header');

      // Row number cell
      var tdNum = document.createElement('td');
      tdNum.className = 'import-rawpreview-num';
      tdNum.textContent = idx + 1;
      tr.appendChild(tdNum);

      for (var i = 0; i < maxCols; i++) {
        var td = document.createElement('td');
        var v = row[i] == null ? '' : String(row[i]);
        td.textContent = v.length > 30 ? v.slice(0, 30) + '…' : v;
        td.title = v;
        tr.appendChild(td);
      }

      tr.addEventListener('click', function() {
        var newIdx = parseInt(this.dataset.rowIdx);
        if (newIdx === _headerRowIndex) return;
        _headerRowIndex = newIdx;
        _analyze(newIdx);
      });
      table.appendChild(tr);
    });

    container.appendChild(table);
  }

  // ── Render mapping table ──────────────────────────────────────────────────
  function _renderMappingTable(mappings) {
    var tbody = document.querySelector('#import-csv-mapping tbody');
    tbody.innerHTML = '';
    mappings.forEach(function(m) {
      var tr = document.createElement('tr');

      var tdCol = document.createElement('td');
      tdCol.textContent = m.csv_col;
      tr.appendChild(tdCol);

      var tdSel = document.createElement('td');
      var sel = document.createElement('select');
      sel.className = 'import-target-sel';
      sel.dataset.csvCol = m.csv_col;
      _targetFields.forEach(function(tf) {
        var opt = document.createElement('option');
        opt.value = tf.value;
        opt.textContent = tf.label;
        if (tf.value === (m.target || '')) opt.selected = true;
        sel.appendChild(opt);
      });
      sel.addEventListener('change', _refreshPreview);
      tdSel.appendChild(sel);
      tr.appendChild(tdSel);

      var tdConf = document.createElement('td');
      var conf = m.confidence || 0;
      var badge = document.createElement('span');
      badge.className = 'import-confidence-badge ' + (
        conf >= 0.9 ? 'conf-high' : conf >= 0.6 ? 'conf-med' : 'conf-low'
      );
      badge.textContent = conf > 0 ? Math.round(conf * 100) + '%' : '—';
      tdConf.appendChild(badge);
      tr.appendChild(tdConf);

      var tdSample = document.createElement('td');
      var samples = _previewRows.slice(0, 3)
        .map(function(r) { return (r.csv_row || {})[m.csv_col]; })
        .filter(function(v) { return v !== undefined && v !== ''; })
        .slice(0, 3);
      tdSample.className = 'muted';
      tdSample.style.fontSize = '.8rem';
      tdSample.textContent = samples.join(' · ');
      tr.appendChild(tdSample);

      tbody.appendChild(tr);
    });
  }

  function _currentMapping() {
    var map = {};
    document.querySelectorAll('.import-target-sel').forEach(function(sel) {
      map[sel.dataset.csvCol] = sel.value || null;
    });
    return map;
  }

  function _refreshPreview() { _renderPreviewTable(_previewRows); }

  function _renderPreviewTable(rows) {
    var tbody = document.querySelector('#import-csv-preview tbody');
    tbody.innerHTML = '';
    var map = _currentMapping();
    var rev = {};
    Object.keys(map).forEach(function(k) { if (map[k]) rev[map[k]] = k; });

    rows.forEach(function(r, idx) {
      var row = r.csv_row || {};
      var tr = document.createElement('tr');
      if (r.duplicate_of_line_id) tr.classList.add('duplicate');
      if (r.is_junk) tr.classList.add('junk');

      var tdIdx = document.createElement('td');
      tdIdx.textContent = idx + 1;
      tr.appendChild(tdIdx);

      var tdAction = document.createElement('td');
      var actSel = document.createElement('select');
      actSel.className = 'import-action-sel';
      actSel.dataset.rowIdx = idx;
      actSel.dataset.dupId = r.duplicate_of_line_id || '';
      var opts = [
        { value: 'new',  label: '+ Add as new line' },
        { value: 'skip', label: '— Skip this row —' },
      ];
      if (r.duplicate_of_line_id) opts.splice(1, 0, { value: 'update', label: '↻ Update existing' });
      opts.forEach(function(o) {
        var opt = document.createElement('option');
        opt.value = o.value;
        opt.textContent = o.label;
        actSel.appendChild(opt);
      });
      // Default: skip if junk, update if duplicate, otherwise new
      if (r.is_junk) actSel.value = 'skip';
      else if (r.duplicate_of_line_id) actSel.value = 'update';
      else actSel.value = 'new';
      tdAction.appendChild(actSel);
      if (r.is_junk) {
        var junkLabel = document.createElement('div');
        junkLabel.className = 'muted';
        junkLabel.style.fontSize = '.7rem';
        junkLabel.textContent = 'Looks like total/empty/section';
        tdAction.appendChild(junkLabel);
      } else if (r.duplicate_of_line_id) {
        var dupLabel = document.createElement('div');
        dupLabel.className = 'muted';
        dupLabel.style.fontSize = '.7rem';
        dupLabel.textContent = 'Duplicate of line #' + r.duplicate_of_line_id;
        tdAction.appendChild(dupLabel);
      }
      tr.appendChild(tdAction);

      function cell(fieldName, fallback) {
        var csvCol = rev[fieldName];
        var v = csvCol ? row[csvCol] : fallback;
        var td = document.createElement('td');
        td.textContent = v == null ? '' : v;
        return td;
      }
      tr.appendChild(cell('account_code', r.resolved_code));
      tr.appendChild(cell('description', ''));
      tr.appendChild(cell('quantity', ''));
      tr.appendChild(cell('days', ''));
      tr.appendChild(cell('rate', ''));
      tr.appendChild(cell('estimated_total', ''));

      tbody.appendChild(tr);
    });
  }

  function _setSummary(total, mappings, autoIdx) {
    var mapped = mappings.filter(function(m) { return m.target; }).length;
    var el = document.getElementById('import-csv-summary');
    if (el) {
      var parts = [
        total + ' data row' + (total === 1 ? '' : 's') + ' after header',
        mapped + ' of ' + mappings.length + ' columns mapped',
        'Header auto-detected on row ' + ((autoIdx || 0) + 1),
      ];
      el.textContent = parts.join(' · ');
    }
  }

  function _applyImport() {
    if (!_file) return;
    var mapping = _currentMapping();

    var previewActions = [];
    document.querySelectorAll('.import-action-sel').forEach(function(sel) {
      var val = sel.value;
      if (val === 'update') previewActions.push({ update: parseInt(sel.dataset.dupId) });
      else previewActions.push(val);
    });

    var autoSkip = document.getElementById('import-csv-skip-junk').checked ? '1' : '0';

    var fd = new FormData();
    fd.append('file', _file);
    fd.append('mapping', JSON.stringify(mapping));
    fd.append('row_actions', JSON.stringify(previewActions));
    fd.append('header_row_index', _headerRowIndex);
    fd.append('auto_skip_junk', autoSkip);

    var applyBtn = document.getElementById('import-csv-apply-btn');
    applyBtn.disabled = true;
    applyBtn.textContent = 'Importing…';

    fetch(applyUrl, { method: 'POST', body: fd })
      .then(function(r) { return r.json().then(function(b) { return {ok: r.ok, body: b}; }); })
      .then(function(res) {
        applyBtn.disabled = false;
        applyBtn.textContent = 'Import Lines';
        if (!res.ok) {
          alert('Import failed: ' + (res.body.error || 'Unknown error'));
          return;
        }
        var b = res.body;
        var msg = 'Imported ' + b.added + ' new, ' + b.updated + ' updated, ' + b.skipped + ' skipped';
        if (b.auto_skipped_junk) msg += ', ' + b.auto_skipped_junk + ' auto-skipped (totals/empty/junk)';
        msg += '.';
        if (b.errors && b.errors.length) {
          msg += '\n\nErrors (first 10):\n' + b.errors.slice(0, 10).join('\n');
        }
        alert(msg);
        modal.classList.add('hidden');
        location.reload();
      })
      .catch(function(e) {
        applyBtn.disabled = false;
        applyBtn.textContent = 'Import Lines';
        alert('Import failed: ' + e.message);
      });
  }
});
