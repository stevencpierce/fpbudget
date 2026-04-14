// ── Smart CSV Import (CSP-safe, no inline handlers) ────────────────────────
document.addEventListener('DOMContentLoaded', function() {
  var btn      = document.getElementById('import-csv-btn');
  var fileInp  = document.getElementById('import-csv-file');
  var modal    = document.getElementById('import-csv-modal');
  var status   = document.getElementById('import-csv-status');
  if (!btn || !fileInp || !modal) return;

  var analyzeUrl = fileInp.dataset.analyzeUrl;
  var applyUrl   = fileInp.dataset.applyUrl;

  // Hold the selected file + analysis result across steps
  var _file         = null;
  var _headers      = [];
  var _targetFields = [];
  var _previewRows  = [];

  // ── Open file picker ──────────────────────────────────────────────────────
  btn.addEventListener('click', function() { fileInp.click(); });

  fileInp.addEventListener('change', function() {
    if (!fileInp.files.length) return;
    _file = fileInp.files[0];
    status.textContent = 'Analyzing ' + _file.name + '…';

    var fd = new FormData();
    fd.append('file', _file);
    fetch(analyzeUrl, { method: 'POST', body: fd })
      .then(function(r) { return r.json().then(function(body) { return {ok: r.ok, body: body}; }); })
      .then(function(res) {
        if (!res.ok) {
          status.textContent = 'Error: ' + (res.body.error || 'Upload failed');
          return;
        }
        status.textContent = '';
        _headers      = res.body.headers || [];
        _targetFields = res.body.target_fields || [];
        _previewRows  = res.body.preview_rows || [];
        _renderMappingTable(res.body.mappings || []);
        _renderPreviewTable(_previewRows);
        _setSummary(res.body.row_count, res.body.mappings || []);
        modal.classList.remove('hidden');
      })
      .catch(function(e) {
        status.textContent = 'Error: ' + e.message;
      });
    // reset input so same file can be re-uploaded
    fileInp.value = '';
  });

  // ── Modal controls ────────────────────────────────────────────────────────
  modal.querySelector('.modal-overlay').addEventListener('click', function() {
    modal.classList.add('hidden');
  });
  document.getElementById('import-csv-cancel').addEventListener('click', function() {
    modal.classList.add('hidden');
  });
  document.getElementById('import-csv-apply-btn').addEventListener('click', _applyImport);

  // ── Render mapping table ──────────────────────────────────────────────────
  function _renderMappingTable(mappings) {
    var tbody = document.querySelector('#import-csv-mapping tbody');
    tbody.innerHTML = '';
    mappings.forEach(function(m, idx) {
      var tr = document.createElement('tr');

      // CSV column name
      var tdCol = document.createElement('td');
      tdCol.textContent = m.csv_col;
      tr.appendChild(tdCol);

      // Target dropdown
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

      // Confidence badge
      var tdConf = document.createElement('td');
      var conf = m.confidence || 0;
      var badge = document.createElement('span');
      badge.className = 'import-confidence-badge ' + (
        conf >= 0.9 ? 'conf-high' : conf >= 0.6 ? 'conf-med' : 'conf-low'
      );
      badge.textContent = conf > 0 ? Math.round(conf * 100) + '%' : '—';
      tdConf.appendChild(badge);
      tr.appendChild(tdConf);

      // Sample values (up to 3)
      var tdSample = document.createElement('td');
      var samples = _previewRows.slice(0, 3)
        .map(function(r) { return (r.csv_row || {})[m.csv_col]; })
        .filter(function(v) { return v !== undefined && v !== ''; })
        .slice(0, 3);
      tdSample.className = 'muted';
      tdSample.style.fontSize = '.82rem';
      tdSample.textContent = samples.join(' · ');
      tr.appendChild(tdSample);

      tbody.appendChild(tr);
    });
  }

  // ── Compute current mapping from dropdowns ────────────────────────────────
  function _currentMapping() {
    var map = {};
    document.querySelectorAll('.import-target-sel').forEach(function(sel) {
      map[sel.dataset.csvCol] = sel.value || null;
    });
    return map;
  }

  // ── Refresh the preview table when a mapping dropdown changes ─────────────
  function _refreshPreview() {
    _renderPreviewTable(_previewRows);
  }

  // ── Render preview table with per-row action dropdowns ────────────────────
  function _renderPreviewTable(rows) {
    var tbody = document.querySelector('#import-csv-preview tbody');
    tbody.innerHTML = '';
    var map = _currentMapping();
    // Build reverse lookup: target -> csv_col
    var rev = {};
    Object.keys(map).forEach(function(k) { if (map[k]) rev[map[k]] = k; });

    rows.forEach(function(r, idx) {
      var row = r.csv_row || {};
      var tr = document.createElement('tr');
      if (r.duplicate_of_line_id) tr.classList.add('duplicate');

      // Index
      var tdIdx = document.createElement('td');
      tdIdx.textContent = idx + 1;
      tr.appendChild(tdIdx);

      // Action dropdown
      var tdAction = document.createElement('td');
      var actSel = document.createElement('select');
      actSel.className = 'import-action-sel';
      actSel.dataset.rowIdx = idx;
      actSel.dataset.dupId = r.duplicate_of_line_id || '';
      var opts = [
        { value: 'new', label: '+ Add as new line' },
        { value: 'skip', label: '— Skip this row —' },
      ];
      if (r.duplicate_of_line_id) {
        opts.splice(1, 0, { value: 'update', label: '↻ Update existing' });
      }
      opts.forEach(function(o) {
        var opt = document.createElement('option');
        opt.value = o.value;
        opt.textContent = o.label;
        actSel.appendChild(opt);
      });
      // Default to "update" if duplicate, else "new"
      actSel.value = r.duplicate_of_line_id ? 'update' : 'new';
      tdAction.appendChild(actSel);
      if (r.duplicate_of_line_id) {
        var dupLabel = document.createElement('div');
        dupLabel.className = 'muted';
        dupLabel.style.fontSize = '.72rem';
        dupLabel.textContent = 'Duplicate of line #' + r.duplicate_of_line_id;
        tdAction.appendChild(dupLabel);
      }
      tr.appendChild(tdAction);

      // Resolved fields
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

  function _setSummary(total, mappings) {
    var mapped = mappings.filter(function(m) { return m.target; }).length;
    var el = document.getElementById('import-csv-summary');
    if (el) {
      el.textContent = total + ' row' + (total === 1 ? '' : 's') + ' detected · '
                     + mapped + ' of ' + mappings.length + ' columns mapped';
    }
  }

  // ── Apply: POST mapping + actions + file ──────────────────────────────────
  function _applyImport() {
    if (!_file) return;
    var mapping = _currentMapping();

    // Build row_actions — for rows beyond the preview window, default to "new"
    var previewActions = [];
    document.querySelectorAll('.import-action-sel').forEach(function(sel) {
      var val = sel.value;
      if (val === 'update') {
        previewActions.push({ update: parseInt(sel.dataset.dupId) });
      } else {
        previewActions.push(val);  // "new" or "skip"
      }
    });

    var fd = new FormData();
    fd.append('file', _file);
    fd.append('mapping', JSON.stringify(mapping));
    fd.append('row_actions', JSON.stringify(previewActions));

    var applyBtn = document.getElementById('import-csv-apply-btn');
    applyBtn.disabled = true;
    applyBtn.textContent = 'Importing…';

    fetch(applyUrl, { method: 'POST', body: fd })
      .then(function(r) { return r.json().then(function(body) { return {ok: r.ok, body: body}; }); })
      .then(function(res) {
        applyBtn.disabled = false;
        applyBtn.textContent = 'Import Lines';
        if (!res.ok) {
          alert('Import failed: ' + (res.body.error || 'Unknown error'));
          return;
        }
        var b = res.body;
        var msg = 'Imported ' + b.added + ' new, ' + b.updated + ' updated, ' + b.skipped + ' skipped.';
        if (b.errors && b.errors.length) {
          msg += '\n\nErrors:\n' + b.errors.slice(0, 10).join('\n');
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
