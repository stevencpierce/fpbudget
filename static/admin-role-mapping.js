// admin-role-mapping.js — CSP-safe JS for the Role Tag Mapping editor.
// Debounced autosave on input change; filter + search across the table.

(function () {
  'use strict';

  var $tbody = document.getElementById('rtm-tbody');
  var $filterSection = document.getElementById('rtm-filter-section');
  var $filterSearch = document.getElementById('rtm-filter-search');
  var $save = document.getElementById('rtm-save');
  var $bulkForm = document.getElementById('rtm-bulk-form');
  var $bulkFile = document.getElementById('rtm-bulk-file');

  if (!$tbody) return;

  var _saveTimers = {}; // id -> setTimeout handle

  function setIndicator(cls, text) {
    $save.className = 'rtm-save-indicator ' + cls;
    $save.textContent = text;
    if (cls === 'saved' || cls === 'error') {
      setTimeout(function () {
        if ($save.className.indexOf(cls) !== -1) {
          $save.className = 'rtm-save-indicator';
          $save.textContent = '';
        }
      }, 2000);
    }
  }

  function saveRow(id, payload) {
    setIndicator('saving', 'Saving…');
    fetch('/admin/role-mapping/' + id, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    })
      .then(function (r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
      .then(function () { setIndicator('saved', 'Saved'); })
      .catch(function (e) { setIndicator('error', 'Save failed: ' + e.message); });
  }

  function onInputChange(ev) {
    var $inp = ev.target;
    if ($inp.tagName !== 'INPUT') return;
    var $tr = $inp.closest('tr');
    if (!$tr) return;
    var id = $tr.dataset.id;
    var field = $inp.dataset.field;
    if (!id || !field) return;
    clearTimeout(_saveTimers[id]);
    _saveTimers[id] = setTimeout(function () {
      var payload = {};
      payload[field] = $inp.value;
      saveRow(id, payload);
    }, 400);
  }

  $tbody.addEventListener('input', onInputChange);

  function applyFilters() {
    var sec = $filterSection.value;
    var q = ($filterSearch.value || '').toLowerCase().trim();
    var rows = $tbody.querySelectorAll('tr');
    for (var i = 0; i < rows.length; i++) {
      var r = rows[i];
      var show = true;
      if (sec && r.dataset.section !== sec) show = false;
      if (q && show) {
        if ((r.dataset.haystack || '').indexOf(q) === -1) show = false;
      }
      r.style.display = show ? '' : 'none';
    }
  }
  $filterSection.addEventListener('change', applyFilters);
  $filterSearch.addEventListener('input', applyFilters);

  // Bulk CSV import
  $bulkForm.addEventListener('submit', function (e) {
    e.preventDefault();
    if (!$bulkFile.files.length) return;
    var fd = new FormData();
    fd.append('file', $bulkFile.files[0]);
    setIndicator('saving', 'Uploading…');
    fetch('/admin/role-mapping/bulk-import', { method: 'POST', body: fd })
      .then(function (r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
      .then(function (data) {
        setIndicator('saved', 'Updated ' + (data.updated || 0) + ' rows — reloading…');
        setTimeout(function () { window.location.reload(); }, 800);
      })
      .catch(function (e) { setIndicator('error', 'Bulk upload failed: ' + e.message); });
  });
})();
