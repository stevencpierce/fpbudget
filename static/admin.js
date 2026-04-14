// ── Admin Panel JS ──────────────────────────────────────────────────────────
function openEdit(uid, name, email, role, dept) {
  document.getElementById('edit-form').action = '/admin/users/' + uid + '/edit';
  document.getElementById('en').value = name || '';
  document.getElementById('ee').value = email || '';
  var er = document.getElementById('er');
  for (var i = 0; i < er.options.length; i++) er.options[i].selected = (er.options[i].value === role);
  var d = document.getElementById('ed');
  if (d) for (var i = 0; i < d.options.length; i++) d.options[i].selected = (String(d.options[i].value) === String(dept));
  toggleEditDept();
  document.getElementById('edit-modal').classList.add('open');
}

function closeEdit() {
  document.getElementById('edit-modal').classList.remove('open');
}

function toggleEditDept() {
  document.getElementById('edit-dept-wrap').style.display =
    document.getElementById('er').value === 'dept_head' ? '' : 'none';
}

function toggleNewDept() {
  document.getElementById('new-dept-field').style.display =
    document.getElementById('new-role').value === 'dept_head' ? '' : 'none';
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
