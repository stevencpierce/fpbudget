// ── Global Quick Entry Catalog Editor (CSP-safe) ──────────────────────────
(function() {
  'use strict';

  var items = [];
  var coaSections = [];
  try {
    items = JSON.parse(document.getElementById('cat-initial-items').textContent);
    coaSections = JSON.parse(document.getElementById('cat-coa-sections').textContent);
  } catch(e) { console.error('Bad initial JSON:', e); }

  var tbody     = document.getElementById('cat-tbody');
  var filterSel = document.getElementById('cat-filter');
  var searchInp = document.getElementById('cat-search');
  var showInact = document.getElementById('cat-show-inactive');
  var addBtn    = document.getElementById('cat-add-btn');

  var FRINGE_OPTS = [
    { v: '',  l: '— None —' },
    { v: 'E', l: 'E (Exempt)' },
    { v: 'N', l: 'N (Non-Union)' },
    { v: 'L', l: 'L (Loan-Out)' },
    { v: 'U', l: 'U (Union)' },
    { v: 'S', l: 'S (SAG)' },
    { v: 'I', l: 'I (IATSE)' },
    { v: 'D', l: 'D (DGA)' },
  ];
  var COMP_OPTS = [
    { v: 'labor',    l: 'Labor' },
    { v: 'expense',  l: 'Expense' },
    { v: 'rental',   l: 'Rental' },
    { v: 'purchase', l: 'Purchase' },
  ];
  var UNIT_OPTS = [
    { v: 'day',     l: 'Day' },
    { v: 'flat',    l: 'Flat' },
    { v: 'week',    l: 'Week' },
    { v: 'session', l: 'Session' },
    { v: 'night',   l: 'Night' },
    { v: 'each',    l: 'Each' },
  ];

  // ── Render ───────────────────────────────────────────────────────────────
  function render() {
    var q       = (searchInp.value || '').toLowerCase();
    var filter  = filterSel.value;
    var inactOk = showInact.checked;

    var filtered = items.filter(function(it) {
      if (!inactOk && !it.is_active) return false;
      if (filter && String(it.category_code) !== String(filter)) return false;
      if (q) {
        var hay = ((it.label || '') + ' ' + (it.group_name || '') + ' ' + (it.category_name || '')).toLowerCase();
        if (hay.indexOf(q) < 0) return false;
      }
      return true;
    });

    // Group by category for readability
    var byCat = {};
    filtered.forEach(function(it) {
      var k = it.category_code;
      (byCat[k] = byCat[k] || []).push(it);
    });
    var catCodes = Object.keys(byCat).map(Number).sort(function(a,b) { return a - b; });

    tbody.innerHTML = '';
    catCodes.forEach(function(code) {
      var group = byCat[code];
      // Category header row
      var hdr = document.createElement('tr');
      hdr.className = 'cat-group-hdr';
      var hdrTd = document.createElement('td');
      hdrTd.colSpan = 15;
      hdrTd.textContent = code + ' — ' + (group[0].category_name || '');
      hdr.appendChild(hdrTd);
      tbody.appendChild(hdr);

      // Items within this category
      group.forEach(function(it) { tbody.appendChild(_buildRow(it)); });
    });

    _initSortable();
  }

  function _buildRow(it) {
    var tr = document.createElement('tr');
    tr.dataset.id = it.id;
    if (!it.is_active) tr.classList.add('inactive');

    tr.appendChild(_td(function(td) {
      var h = document.createElement('span');
      h.className = 'cat-drag-handle';
      h.textContent = '⋮⋮';
      h.title = 'Drag to reorder within this category';
      td.appendChild(h);
    }));

    // Category dropdown
    tr.appendChild(_td(function(td) {
      var sel = document.createElement('select');
      coaSections.forEach(function(s) {
        var o = document.createElement('option');
        o.value = s[0];
        o.textContent = s[0] + ' — ' + s[1];
        if (s[0] === it.category_code) o.selected = true;
        sel.appendChild(o);
      });
      sel.addEventListener('change', function() {
        _patch(it.id, { category_code: parseInt(sel.value) });
      });
      td.appendChild(sel);
    }));

    tr.appendChild(_textTd(it, 'label', { minWidth: '180px' }));
    tr.appendChild(_textTd(it, 'group_name', { minWidth: '140px', placeholder: '—' }));

    // Labor checkbox
    tr.appendChild(_td(function(td) {
      var inp = document.createElement('input');
      inp.type = 'checkbox';
      inp.checked = !!it.is_labor;
      inp.addEventListener('change', function() {
        _patch(it.id, { is_labor: inp.checked });
      });
      td.style.textAlign = 'center';
      td.appendChild(inp);
    }));

    tr.appendChild(_numTd(it, 'rate', { className: 'col-rate' }));
    tr.appendChild(_numTd(it, 'qty',  { className: 'col-qty' }));
    tr.appendChild(_numTd(it, 'days', { className: 'col-days' }));
    tr.appendChild(_numTd(it, 'kit_fee', { className: 'col-kit' }));

    tr.appendChild(_selectTd(it, 'fringe', FRINGE_OPTS));
    tr.appendChild(_selectTd(it, 'union_fringe', FRINGE_OPTS));

    // Agent % — stored as fraction (0.10), display as percent
    tr.appendChild(_td(function(td) {
      var inp = document.createElement('input');
      inp.type = 'number';
      inp.step = '0.5';
      inp.value = ((it.agent_pct || 0) * 100).toFixed(1).replace(/\.0$/, '');
      inp.addEventListener('blur', function() {
        var v = parseFloat(inp.value) || 0;
        _patch(it.id, { agent_pct: v / 100 });
      });
      td.className = 'col-agent';
      td.appendChild(inp);
    }));

    tr.appendChild(_selectTd(it, 'comp', COMP_OPTS));
    tr.appendChild(_selectTd(it, 'unit', UNIT_OPTS));

    // Delete button
    tr.appendChild(_td(function(td) {
      var btn = document.createElement('button');
      btn.className = 'cat-delete-btn';
      btn.textContent = it.is_active ? '✕' : '↺';
      btn.title = it.is_active ? 'Soft-delete (hide from Quick Entry)' : 'Restore';
      btn.addEventListener('click', function() {
        if (it.is_active) {
          if (!confirm('Hide "' + it.label + '" from Quick Entry?')) return;
          _delete(it.id);
        } else {
          _patch(it.id, { is_active: true });
        }
      });
      td.appendChild(btn);
    }));

    return tr;
  }

  // ── Cell builders ───────────────────────────────────────────────────────
  function _td(fn) { var td = document.createElement('td'); fn(td); return td; }

  function _textTd(it, field, opts) {
    opts = opts || {};
    return _td(function(td) {
      var inp = document.createElement('input');
      inp.type = 'text';
      inp.value = it[field] || '';
      if (opts.placeholder) inp.placeholder = opts.placeholder;
      if (opts.minWidth) inp.style.minWidth = opts.minWidth;
      inp.addEventListener('blur', function() {
        var v = inp.value.trim();
        if (v !== (it[field] || '')) _patch(it.id, (function() { var o = {}; o[field] = v; return o; })());
      });
      td.appendChild(inp);
    });
  }

  function _numTd(it, field, opts) {
    opts = opts || {};
    return _td(function(td) {
      var inp = document.createElement('input');
      inp.type = 'number';
      inp.step = field === 'rate' ? '1' : '0.25';
      inp.value = it[field] || 0;
      inp.addEventListener('blur', function() {
        var v = parseFloat(inp.value) || 0;
        if (v !== parseFloat(it[field] || 0)) _patch(it.id, (function() { var o = {}; o[field] = v; return o; })());
      });
      if (opts.className) td.className = opts.className;
      td.appendChild(inp);
    });
  }

  function _selectTd(it, field, options) {
    return _td(function(td) {
      var sel = document.createElement('select');
      options.forEach(function(o) {
        var opt = document.createElement('option');
        opt.value = o.v;
        opt.textContent = o.l;
        if (String(o.v) === String(it[field] || '')) opt.selected = true;
        sel.appendChild(opt);
      });
      sel.addEventListener('change', function() {
        var upd = {};
        upd[field] = sel.value;
        _patch(it.id, upd);
      });
      td.appendChild(sel);
    });
  }

  // ── API calls ───────────────────────────────────────────────────────────
  function _patch(id, updates) {
    fetch('/admin/catalog/item/' + id, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(updates),
    }).then(function(r) { return r.json(); }).then(function(data) {
      if (data.error) { alert('Save failed: ' + data.error); return; }
      // Merge back into local list
      for (var i = 0; i < items.length; i++) {
        if (items[i].id === id) { items[i] = data; break; }
      }
      render();
    });
  }

  function _delete(id) {
    fetch('/admin/catalog/item/' + id + '/delete', { method: 'POST' })
      .then(function(r) { return r.json(); })
      .then(function() {
        for (var i = 0; i < items.length; i++) {
          if (items[i].id === id) { items[i].is_active = false; break; }
        }
        render();
      });
  }

  function _create(payload) {
    fetch('/admin/catalog/item', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    }).then(function(r) { return r.json(); }).then(function(data) {
      if (data.error) { alert('Create failed: ' + data.error); return; }
      items.push(data);
      render();
    });
  }

  // ── Sortable within each category ───────────────────────────────────────
  function _initSortable() {
    if (typeof Sortable === 'undefined') return;
    // One Sortable on the tbody; filter out cat-group-hdr rows
    if (tbody.dataset.sortInit === '1') return;
    tbody.dataset.sortInit = '1';
    new Sortable(tbody, {
      handle: '.cat-drag-handle',
      filter: '.cat-group-hdr',
      animation: 150,
      ghostClass: 'sortable-ghost',
      dragClass: 'sortable-drag',
      onEnd: function() {
        // Build ordered list of ids from current DOM
        var ids = Array.from(tbody.querySelectorAll('tr[data-id]')).map(function(tr) {
          return parseInt(tr.dataset.id);
        });
        fetch('/admin/catalog/reorder', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ order: ids }),
        });
        // Update local sort_order
        ids.forEach(function(id, idx) {
          var it = items.find(function(x) { return x.id === id; });
          if (it) it.sort_order = idx * 10;
        });
      },
    });
  }

  // ── Add Item modal ─────────────────────────────────────────────────────
  var modal       = document.getElementById('cat-add-modal');
  var modalBody   = {
    category: document.getElementById('cat-add-category'),
    label:    document.getElementById('cat-add-label'),
    group:    document.getElementById('cat-add-group'),
    comp:     document.getElementById('cat-add-comp'),
    unit:     document.getElementById('cat-add-unit'),
    rate:     document.getElementById('cat-add-rate'),
    qty:      document.getElementById('cat-add-qty'),
    days:     document.getElementById('cat-add-days'),
    fringe:   document.getElementById('cat-add-fringe'),
    union:    document.getElementById('cat-add-union'),
    agent:    document.getElementById('cat-add-agent'),
    labor:    document.getElementById('cat-add-labor'),
  };

  function _openAddModal() {
    // Reset form
    modalBody.category.value = '';
    modalBody.label.value    = '';
    modalBody.group.value    = '';
    modalBody.comp.value     = 'labor';
    modalBody.unit.value     = 'day';
    modalBody.rate.value     = '0';
    modalBody.qty.value      = '1';
    modalBody.days.value     = '1';
    modalBody.fringe.value   = 'N';
    modalBody.union.value    = '';
    modalBody.agent.value    = '0';
    modalBody.labor.checked  = true;
    modal.style.display = 'flex';
    setTimeout(function() { modalBody.category.focus(); }, 50);
  }

  function _closeAddModal() { modal.style.display = 'none'; }

  // Keep comp in sync with labor checkbox — labor line defaults to 'labor' comp,
  // non-labor defaults to 'expense'. User can still override.
  modalBody.labor.addEventListener('change', function() {
    if (modalBody.labor.checked) {
      modalBody.comp.value = 'labor';
      if (!modalBody.fringe.value) modalBody.fringe.value = 'N';
    } else {
      modalBody.comp.value = 'expense';
      modalBody.fringe.value = '';
      modalBody.union.value = '';
      modalBody.agent.value = '0';
    }
  });

  addBtn.addEventListener('click', _openAddModal);
  document.getElementById('cat-add-close').addEventListener('click', _closeAddModal);
  document.getElementById('cat-add-overlay').addEventListener('click', _closeAddModal);
  document.getElementById('cat-add-cancel').addEventListener('click', _closeAddModal);

  document.getElementById('cat-add-save').addEventListener('click', function() {
    var code = parseInt(modalBody.category.value);
    var label = modalBody.label.value.trim();
    if (!code) { alert('Please select a department.'); modalBody.category.focus(); return; }
    if (!label) { alert('Please enter a label.'); modalBody.label.focus(); return; }
    var payload = {
      category_code: code,
      label: label,
      group_name: modalBody.group.value.trim() || null,
      is_labor: modalBody.labor.checked,
      rate: parseFloat(modalBody.rate.value) || 0,
      qty: parseFloat(modalBody.qty.value) || 1,
      days: parseFloat(modalBody.days.value) || 1,
      fringe: modalBody.fringe.value || null,
      union_fringe: modalBody.union.value || null,
      agent_pct: (parseFloat(modalBody.agent.value) || 0) / 100,
      comp: modalBody.comp.value,
      unit: modalBody.unit.value,
    };
    _create(payload);
    _closeAddModal();
  });

  // Escape closes modal
  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape' && modal.style.display === 'flex') _closeAddModal();
  });

  // ── Filters ─────────────────────────────────────────────────────────────
  filterSel.addEventListener('change', render);
  searchInp.addEventListener('input', render);
  showInact.addEventListener('change', render);

  // Initial render
  render();
})();
