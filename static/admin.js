// ── Admin Panel JS (CSP-safe — no inline handlers) ──────────────────────────
document.addEventListener('DOMContentLoaded', function() {

  // ── Edit User Modal ─────────────────────────────────────────────────────────
  document.querySelectorAll('.edit-user-btn').forEach(function(btn) {
    btn.addEventListener('click', function() {
      var uid   = btn.dataset.uid;
      var name  = btn.dataset.name;
      var email = btn.dataset.email;
      var role  = btn.dataset.role;
      var dept  = btn.dataset.dept;
      document.getElementById('edit-form').action = '/admin/users/' + uid + '/edit';
      document.getElementById('en').value = name || '';
      document.getElementById('ee').value = email || '';
      var er = document.getElementById('er');
      for (var i = 0; i < er.options.length; i++) {
        er.options[i].selected = (er.options[i].value === role);
      }
      var d = document.getElementById('ed');
      if (d) for (var i = 0; i < d.options.length; i++) {
        d.options[i].selected = (String(d.options[i].value) === String(dept));
      }
      toggleEditDept();
      document.getElementById('edit-modal').classList.add('open');
    });
  });

  // Close modal on backdrop click
  var modal = document.getElementById('edit-modal');
  if (modal) {
    modal.addEventListener('click', function(e) {
      if (e.target === modal) modal.classList.remove('open');
    });
  }

  // Cancel button
  var cancelBtn = document.getElementById('edit-cancel-btn');
  if (cancelBtn) {
    cancelBtn.addEventListener('click', function() {
      document.getElementById('edit-modal').classList.remove('open');
    });
  }

  // ── Role → Dept toggle ──────────────────────────────────────────────────────
  var er = document.getElementById('er');
  if (er) er.addEventListener('change', toggleEditDept);

  var newRole = document.getElementById('new-role');
  if (newRole) newRole.addEventListener('change', toggleNewDept);

  // ── Confirm forms (reset pw, delete user, dropbox import) ───────────────────
  document.querySelectorAll('.confirm-form').forEach(function(form) {
    form.addEventListener('submit', function(e) {
      if (!confirm(form.dataset.msg)) e.preventDefault();
    });
  });

  // ── Add project access ──────────────────────────────────────────────────────
  document.querySelectorAll('.add-access-btn').forEach(function(btn) {
    btn.addEventListener('click', function() {
      addAccess(btn.dataset.pid);
    });
  });

  // ── Remove project access ───────────────────────────────────────────────────
  document.querySelectorAll('.remove-access-btn').forEach(function(btn) {
    btn.addEventListener('click', function() {
      removeAccess(btn.dataset.pid, btn.dataset.uid, btn.closest('.access-chip'));
    });
  });

  // ── One-time migration: split multi-qty labor lines ─────────────────────────
  var splitPreviewBtn = document.getElementById('split-labor-preview-btn');
  var splitRunBtn     = document.getElementById('split-labor-run-btn');
  var splitPreviewEl  = document.getElementById('split-labor-preview');
  var splitResultEl   = document.getElementById('split-labor-result');

  if (splitPreviewBtn) {
    splitPreviewBtn.addEventListener('click', function() {
      splitPreviewBtn.disabled = true;
      splitPreviewBtn.textContent = 'Scanning…';
      splitPreviewEl.innerHTML = '';
      splitResultEl.innerHTML = '';
      fetch('/admin/migrate/split-labor/preview')
        .then(function(r) { return r.json(); })
        .then(function(data) {
          splitPreviewBtn.disabled = false;
          splitPreviewBtn.textContent = 'Re-scan';
          if (data.error) {
            splitPreviewEl.innerHTML = '<span style="color:#f87171">Error: ' + data.error + '</span>';
            return;
          }
          if (!data.total_affected_lines) {
            splitPreviewEl.innerHTML = '<span style="color:#4ade80">✓ Nothing to split — all labor lines are already single-qty or already split.</span>';
            return;
          }
          var html = '<div style="margin-top:.4rem">';
          html += '<strong>' + data.total_affected_lines + ' line(s)</strong> across ';
          html += '<strong>' + data.budgets.length + ' budget(s)</strong> will be split into ';
          html += '<strong>' + (data.total_new_lines + data.total_affected_lines) + ' total lines</strong> ';
          html += '(' + data.total_new_lines + ' new).</div>';
          html += '<details style="margin-top:.4rem"><summary style="cursor:pointer;font-size:.78rem;color:var(--text-muted)">Show details</summary>';
          html += '<ul style="margin:.4rem 0 0;padding-left:1.2rem;font-size:.78rem;color:var(--text-muted)">';
          data.budgets.forEach(function(b) {
            html += '<li><strong>' + (b.budget_name || ('Budget #' + b.budget_id)) + '</strong>: ';
            html += b.line_count + ' line(s) → ' + (b.line_count + b.total_new_lines) + ' total';
            html += '<ul style="margin:.2rem 0;padding-left:1.2rem">';
            b.lines.slice(0, 10).forEach(function(l) {
              html += '<li>' + (l.description || '(no label)') + ' — qty ' + l.quantity + '</li>';
            });
            if (b.lines.length > 10) html += '<li>… and ' + (b.lines.length - 10) + ' more</li>';
            html += '</ul></li>';
          });
          html += '</ul></details>';
          splitPreviewEl.innerHTML = html;
          splitRunBtn.disabled = false;
        })
        .catch(function(e) {
          splitPreviewBtn.disabled = false;
          splitPreviewBtn.textContent = 'Preview';
          splitPreviewEl.innerHTML = '<span style="color:#f87171">Error: ' + e.message + '</span>';
        });
    });
  }

  // ── One-time: Resync schedule-driven lines for ALL budgets ─────────────────
  var resyncBtn     = document.getElementById('resync-all-btn');
  var resyncResult  = document.getElementById('resync-all-result');
  if (resyncBtn) {
    resyncBtn.addEventListener('click', function() {
      if (!confirm('Resync meals, flights, hotel, mileage, per diem, working meals, and craft services across EVERY budget? Safe to run — this rebuilds the auto-created lines from the current schedule.')) return;
      resyncBtn.disabled = true;
      resyncBtn.textContent = 'Running…';
      resyncResult.innerHTML = '';
      fetch('/admin/migrate/resync-all', { method: 'POST' })
        .then(function(r) { return r.json(); })
        .then(function(data) {
          resyncBtn.disabled = false;
          resyncBtn.textContent = 'Resync All Budgets';
          if (data.error) {
            resyncResult.innerHTML = '<span style="color:#f87171">Error: ' + data.error + '</span>';
            return;
          }
          var html = '<div style="background:rgba(22,163,74,.12);border:1px solid #16a34a;color:#4ade80;padding:.6rem .8rem;border-radius:5px">';
          html += '✓ Resynced <strong>' + data.resynced + '</strong> / ' + data.total_budgets + ' budget(s).';
          html += '</div>';
          if (data.errors && data.errors.length) {
            html += '<div style="margin-top:.4rem;font-size:.8rem;color:#fbbf24">⚠ ' + data.errors.length + ' error(s):<ul style="margin:.2rem 0;padding-left:1.2rem">';
            data.errors.forEach(function(e) { html += '<li>' + e + '</li>'; });
            html += '</ul></div>';
          }
          resyncResult.innerHTML = html;
        })
        .catch(function(e) {
          resyncBtn.disabled = false;
          resyncBtn.textContent = 'Resync All Budgets';
          resyncResult.innerHTML = '<span style="color:#f87171">Error: ' + e.message + '</span>';
        });
    });
  }

  if (splitRunBtn) {
    splitRunBtn.addEventListener('click', function() {
      if (!confirm('Split multi-qty labor lines into A/B/C rows across ALL budgets? This change cannot be automatically undone.')) return;
      splitRunBtn.disabled = true;
      splitRunBtn.textContent = 'Running…';
      splitResultEl.innerHTML = '';
      fetch('/admin/migrate/split-labor', { method: 'POST' })
        .then(function(r) { return r.json(); })
        .then(function(data) {
          splitRunBtn.textContent = 'Run Migration';
          if (data.error) {
            splitResultEl.innerHTML = '<span style="color:#f87171">Error: ' + data.error + '</span>';
            splitRunBtn.disabled = false;
            return;
          }
          var html = '<div style="background:rgba(22,163,74,.12);border:1px solid #16a34a;color:#4ade80;padding:.6rem .8rem;border-radius:5px">';
          html += '✓ Done. Split <strong>' + data.split_count + '</strong> line(s), created ';
          html += '<strong>' + data.new_lines + '</strong> new line(s) across ';
          html += '<strong>' + data.budgets_affected + '</strong> budget(s).';
          html += '</div>';
          if (data.errors && data.errors.length) {
            html += '<div style="margin-top:.4rem;font-size:.8rem;color:#fbbf24">⚠ Errors:<ul>';
            data.errors.forEach(function(e) { html += '<li>' + e + '</li>'; });
            html += '</ul></div>';
          }
          splitResultEl.innerHTML = html;
          // Disable further runs until a fresh preview confirms there's still work
          splitRunBtn.disabled = true;
          splitPreviewEl.innerHTML = '';
        })
        .catch(function(e) {
          splitRunBtn.disabled = false;
          splitResultEl.innerHTML = '<span style="color:#f87171">Error: ' + e.message + '</span>';
        });
    });
  }

});

// ── Helper functions ────────────────────────────────────────────────────────

function toggleEditDept() {
  var wrap = document.getElementById('edit-dept-wrap');
  var er = document.getElementById('er');
  if (wrap && er) wrap.style.display = er.value === 'dept_head' ? '' : 'none';
}

function toggleNewDept() {
  var field = document.getElementById('new-dept-field');
  var sel = document.getElementById('new-role');
  if (field && sel) field.style.display = sel.value === 'dept_head' ? '' : 'none';
}

async function addAccess(pid) {
  var uid = document.getElementById('au-' + pid).value;
  var role = document.getElementById('ar-' + pid).value;
  if (!uid) { alert('Select a user first.'); return; }
  var fd = new FormData();
  fd.append('user_id', uid);
  fd.append('role', role);
  var r = await fetch('/admin/projects/' + pid + '/access', { method: 'POST', body: fd });
  if (r.ok) location.reload(); else alert('Failed.');
}

async function removeAccess(pid, uid, chip) {
  if (!confirm('Remove this user from the project?')) return;
  var fd = new FormData();
  fd.append('user_id', uid);
  var r = await fetch('/admin/projects/' + pid + '/access/remove', { method: 'POST', body: fd });
  if (r.ok) chip.remove(); else alert('Failed.');
}
