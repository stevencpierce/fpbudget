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
