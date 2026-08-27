// Extracted from a templates/budget.html inline block (audit M9, 2026-07-20).
// Jinja values arrive via window.__BJ (set by the inline preamble before this
// script tag). Classic script, document order — semantics identical to inline.

(function(){
  const PROJ_ID = window.__BJ["b03_PROJ_ID"];
  const UPLOAD_URL = window.__BJ["b03_UPLOAD_URL"];
  let docsQueue = [];
  let _docsUploading = false;   // true while an Upload All run is in flight

  function docsFmtSize(b){ return b<1048576?(b/1024).toFixed(1)+' KB':(b/1048576).toFixed(1)+' MB'; }

  function docsRenderQueue(){
    const el = document.getElementById('docsQueue');
    el.innerHTML = '';
    docsQueue.forEach((item,idx)=>{
      const statusMap = {pending:['var(--text-muted)','Ready'],uploading:['#f59e0b','Uploading…'],done:['#22c55e','Uploaded ✓'],review:['#fbbf24','⚠ Needs review'],review_dup:['#fbbf24','🔁 Possible duplicate — review below'],duplicate:['#a78bfa','Already uploaded'],error:['#ef4444', item.errorMsg ? ('Failed — ' + item.errorMsg) : 'Failed']};
      const [color, label] = statusMap[item.status] || statusMap.pending;
      const div = document.createElement('div');
      div.style.cssText = 'background:var(--bg-card);border:1px solid var(--border);border-radius:8px;padding:11px 13px;display:flex;align-items:center;gap:10px;';
      div.innerHTML = `
        <div style="width:40px;height:40px;border-radius:6px;background:var(--bg-input);display:flex;align-items:center;justify-content:center;font-size:18px;flex-shrink:0;overflow:hidden;">
          ${item.thumbUrl ? `<img src="${item.thumbUrl}" style="width:100%;height:100%;object-fit:cover;">` : (item.file.type.includes('pdf') ? '📄' : '🖼')}
        </div>
        <div style="flex:1;min-width:0;">
          <div style="font-size:13px;font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;"
               title="${item.file.name}">${item.filedName || item.file.name}</div>
          ${item.filedName && item.filedName !== item.file.name
            ? `<div style="font-size:11px;color:#6b7280;font-style:italic;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;" title="Original filename">📎 uploaded as: ${item.file.name}</div>`
            : ''}
          <div style="font-size:11px;color:${color};margin-top:2px;">${label}</div>
        </div>
        ${item.status!=='uploading'?`<button onclick="docsRemove(${idx})" style="background:none;border:none;color:var(--text-muted);font-size:18px;cursor:pointer;padding:4px;">×</button>`:''}`;
      el.appendChild(div);
    });
    _docsUpdateUploadBtn();
  }

  // Single source of truth for the Upload All button's look + enabled
  // state. Per user 2026-05-29: while an upload run is in flight the
  // button must look and behave as locked ("Uploading…", greyed, wait
  // cursor, disabled) so a user scrolled past the top progress banner
  // can't click it a second time and trigger phantom duplicate/failed
  // uploads. Outside a run, it's enabled only when there's pending work,
  // and shows the pending count so it's obvious what's queued.
  function _docsUpdateUploadBtn(){
    const btn = document.getElementById('docsBtnUpload');
    if(!btn) return;
    if(_docsUploading){
      btn.disabled = true;
      btn.textContent = '⏳ Uploading… please wait';
      btn.style.background = '#3a3a4a';
      btn.style.color = '#cfd2dc';
      btn.style.cursor = 'wait';
      return;
    }
    const pending = docsQueue.filter(i=>i.status==='pending').length;
    btn.disabled = pending === 0;
    btn.textContent = pending === 0 ? 'Upload All' : `Upload All (${pending})`;
    btn.style.background = btn.disabled ? 'var(--bg-input)' : '#5b8af9';
    btn.style.color      = btn.disabled ? 'var(--text-muted)' : '#fff';
    btn.style.cursor     = btn.disabled ? 'not-allowed' : 'pointer';
  }

  window.docsRemove = function(idx){ docsQueue.splice(idx,1); docsRenderQueue(); };

  function docsAddFiles(files){
    // Dedupe by name + size + lastModified so the same file dropped or
    // selected twice doesn't queue twice — that race used to slip past
    // the server-side dedupe and produce two filed copies of one file.
    const _key = f => `${f.name}::${f.size}::${f.lastModified || 0}`;
    const existing = new Set(docsQueue.map(it => _key(it.file)));
    let skipped = 0;
    for(const f of files){
      if(existing.has(_key(f))){ skipped++; continue; }
      existing.add(_key(f));
      const item={file:f,status:'pending',thumbUrl:null};
      if(f.type.startsWith('image/')){const r=new FileReader();r.onload=e=>{item.thumbUrl=e.target.result;docsRenderQueue();};r.readAsDataURL(f);}
      docsQueue.push(item);
    }
    if(skipped > 0){
      console.log(`[docs] Skipped ${skipped} duplicate file${skipped===1?'':'s'} from queue`);
    }
    docsRenderQueue();
  }

  // ── Folder-aware drag & drop (user 2026-05-29) ───────────────────────
  // Drop whole folders (e.g. per-vendor subfolders of invoices/contracts/
  // tax docs) and we recurse the directory tree, pull every supported file,
  // and queue them. Skips junk (.DS_Store, dotfiles) and unsupported types.
  const _DOC_EXTS = ['.pdf', '.jpg', '.jpeg', '.png', '.heic', '.heif', '.webp'];
  function _docsReadEntry(entry, out) {
    return new Promise(resolve => {
      if (!entry) return resolve();
      if (entry.isFile) {
        entry.file(f => { out.push(f); resolve(); }, () => resolve());
      } else if (entry.isDirectory) {
        const reader = entry.createReader();
        const collected = [];
        const readBatch = () => reader.readEntries(ents => {
          if (!ents.length) {
            Promise.all(collected.map(e => _docsReadEntry(e, out))).then(resolve);
          } else {
            collected.push(...ents);   // dirs return in ≤100-entry batches
            readBatch();
          }
        }, () => resolve());
        readBatch();
      } else { resolve(); }
    });
  }
  async function _docsFilesFromDrop(dt) {
    const items = dt && dt.items;
    if (items && items.length && items[0].webkitGetAsEntry) {
      const roots = [];
      for (const it of items) {
        const e = it.webkitGetAsEntry && it.webkitGetAsEntry();
        if (e) roots.push(e);
      }
      if (roots.length) {
        const out = [];
        await Promise.all(roots.map(e => _docsReadEntry(e, out)));
        return out;
      }
    }
    return Array.from((dt && dt.files) || []);
  }
  function _docsFilterSupported(files) {
    const accepted = [], skipped = [];
    for (const f of files) {
      const n = f.name || '';
      if (!n || n.startsWith('.')) continue;            // .DS_Store, dotfiles
      const lower = n.toLowerCase();
      if (_DOC_EXTS.some(x => lower.endsWith(x))) accepted.push(f);
      else skipped.push(n);
    }
    return { accepted, skipped };
  }

  const dz = document.getElementById('docsDropZone');
  const fi = document.getElementById('docsFileInput');
  dz.addEventListener('dragover',e=>{e.preventDefault();dz.style.borderColor='#5b8af9';});
  dz.addEventListener('dragleave',()=>{dz.style.borderColor='';});
  dz.addEventListener('drop', async e => {
    e.preventDefault(); dz.style.borderColor='';
    const all = await _docsFilesFromDrop(e.dataTransfer);
    const { accepted, skipped } = _docsFilterSupported(all);
    docsAddFiles(accepted);
    if (skipped.length) {
      console.log(`[docs] Skipped ${skipped.length} unsupported file(s): ` + skipped.slice(0,20).join(', '));
    }
    if (accepted.length) {
      const det = document.getElementById('docsUploadBanner-detail') || null;
      console.log(`[docs] Queued ${accepted.length} file(s) from drop`
        + (skipped.length ? ` (skipped ${skipped.length} unsupported)` : ''));
    } else if (skipped.length) {
      alert(`No supported documents found in what you dropped.\nSupported: PDF, JPG, PNG, HEIC.\nSkipped ${skipped.length} other file(s).`);
    }
  });
  fi.addEventListener('change',()=>{docsAddFiles(Array.from(fi.files));fi.value='';});

  // Optional "choose a folder" picker (sibling to the file picker) — same
  // recursive behavior without needing a drag. Wired if the element exists.
  const fdir = document.getElementById('docsFolderInput');
  if (fdir) {
    fdir.addEventListener('change', () => {
      const { accepted, skipped } = _docsFilterSupported(Array.from(fdir.files || []));
      docsAddFiles(accepted);
      if (skipped.length) console.log(`[docs] Folder pick skipped ${skipped.length} unsupported file(s)`);
      fdir.value='';
    });
  }

  window.docsOpenCamera  = ()=>document.getElementById('docsCameraInput').click();
  window.docsOpenGallery = ()=>document.getElementById('docsGalleryInput').click();
  document.getElementById('docsCameraInput').addEventListener('change',function(){docsAddFiles(Array.from(this.files));this.value='';});
  document.getElementById('docsGalleryInput').addEventListener('change',function(){docsAddFiles(Array.from(this.files));this.value='';});

  // ── Upload-in-progress safety: banner + beforeunload guard ─────────
  // Increments while any docsUploadOne is mid-flight; the banner shows
  // and beforeunload triggers the browser's native "Are you sure?"
  // dialog. Both clear the moment the count returns to 0. The guard
  // prevents the orphan-file case where the worker copies to Dropbox,
  // commits a DocUpload row, but the browser navigates away before the
  // response handler runs (so the queue row never flips to Done and
  // the user thinks the upload failed when it actually succeeded).
  let _docsActiveUploads = 0;
  function _docsBannerUpdate() {
    const banner = document.getElementById('docsUploadBanner');
    const detail = document.getElementById('docsUploadBannerDetail');
    if (!banner) return;
    if (_docsActiveUploads > 0) {
      banner.style.display = 'flex';
      const total = docsQueue.length;
      const done  = docsQueue.filter(i => i.status === 'done' ||
                                           i.status === 'review' ||
                                           i.status === 'duplicate' ||
                                           i.status === 'error').length;
      if (detail) {
        detail.textContent = `Uploading ${done + _docsActiveUploads} of ${total} files… (${done} processed, ${_docsActiveUploads} in flight)`;
      }
    } else {
      banner.style.display = 'none';
    }
  }
  function _docsBeforeUnload(e) {
    if (_docsActiveUploads > 0) {
      // Modern browsers ignore custom messages for security reasons but
      // still show the native "Leave site?" dialog when preventDefault
      // is called and returnValue is set.
      e.preventDefault();
      e.returnValue = 'Uploads are still in progress. Leaving now may cause files to be lost or duplicated.';
      return e.returnValue;
    }
  }
  window.addEventListener('beforeunload', _docsBeforeUnload);

  async function docsUploadOne(item){
    item.status='uploading'; docsRenderQueue();
    _docsActiveUploads += 1; _docsBannerUpdate();
    const fd=new FormData(); fd.append('file',item.file);
    try{
      const r=await fetch(UPLOAD_URL,{method:'POST',body:fd});
      const d=await r.json();
      // HTTP status -> item status:
      //   200 + {status:'duplicate'}  → duplicate badge
      //   201 (filed)                 → done
      //   202 (review)                → review
      //   4xx/5xx                     → error
      // Capture the Analyzer's renamed filename (date-prefixed,
      // vendor-tagged) so the queue row flips from the upload's raw
      // name to the final filed name once the server responds.
      if (d && d.new_filename) item.filedName = d.new_filename;
      if (d && d.status === 'duplicate') {
        item.status = 'duplicate';
      } else if (d && d.status === 'review_dup') {
        // Filed in place but flagged as a possible duplicate. Don't
        // prepend a buttonless row — flag for a one-shot reload after
        // the batch so the server-rendered row (with Keep both / It's a
        // dupe actions) appears. 2026-05-29.
        item.status = 'review_dup';
        _docsNeedsDupReload = true;
      } else if (r.status === 201) {
        item.status = 'done';
        docsPreppendHistory(item.file, d);
      } else if (r.status === 202) {
        item.status = 'review';
        docsPreppendHistory(item.file, d, /*review=*/true);
      } else {
        item.status = 'error';
        item.errorMsg = (d && d.error) || ('HTTP ' + r.status);
      }
    }catch(e){item.status='error'; item.errorMsg=e.message;}
    finally{
      _docsActiveUploads = Math.max(0, _docsActiveUploads - 1);
      _docsBannerUpdate();
    }
    docsRenderQueue();
  }

  let _docsNeedsDupReload = false;
  window.docsUploadAll = async function(){
    // Re-entrancy guard: ignore a second click (or Enter) while a run is
    // already going. Belt-and-suspenders alongside the disabled button —
    // covers the instant before the DOM repaints the locked state.
    if(_docsUploading) return;
    if(!docsQueue.some(i=>i.status==='pending')) return;
    _docsUploading = true;
    _docsUpdateUploadBtn();              // lock the button immediately
    try {
      for(const item of docsQueue.filter(i=>i.status==='pending')) await docsUploadOne(item);
    } finally {
      _docsUploading = false;
      _docsUpdateUploadBtn();            // unlock / reflect remaining work
    }
    // If any upload landed as a possible duplicate, reload once so the
    // server-rendered rows show the Keep both / It's a dupe buttons.
    if (_docsNeedsDupReload) {
      _docsNeedsDupReload = false;
      setTimeout(() => reloadWithTab(), 600);
    }
  };

  // Build a fully-populated history row for a just-completed upload.
  // Mirrors the server-rendered markup in the doc_uploads Jinja loop
  // so the sort toolbar's data-sort-* attributes keep working and
  // the metadata (uploader, doc type, vendor/amount, file size) is
  // visible without a page reload. Also swaps out the "No uploads yet"
  // empty-state block if it's still in the DOM.
  function _esc(s){ return String(s||'').replace(/[&<>"']/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
  function _fmtSize(b){ if(!b) return ''; return b<1048576 ? (b/1024).toFixed(1)+' KB' : (b/1048576).toFixed(1)+' MB'; }
  function _fmtMoney(n){ return Number(n).toFixed(2); }

  const CURRENT_USER_NAME = window.__BJ["b03_CURRENT_USER_NAME"];

  function docsPreppendHistory(file, d, review){
    const h = document.getElementById('docsHistory');
    // Replace empty-state placeholder the first time a row is added.
    const empty = h.querySelector('[data-empty]') || h.querySelector('div[style*="No uploads yet"]');
    if (empty) empty.remove();

    const uid       = d.upload_id;
    const isReview  = !!review || d.status === 'review';
    const isFiled   = !isReview;
    const filename  = file.name;
    const displayName = d.new_filename || filename;
    const docType   = d.doc_type || '';
    const conf      = d.confidence != null ? Math.round(d.confidence * 100) : null;
    const now       = new Date();
    const nowIso    = now.toISOString();
    const dateStr   = now.toLocaleString(undefined, {month:'short',day:'numeric',year:'numeric',hour:'numeric',minute:'2-digit'});
    const sizeStr   = _fmtSize(file.size);
    const isPdf     = (file.type || '').includes('pdf');
    const icon      = isPdf ? '📄' : '🧾';
    const uploader  = CURRENT_USER_NAME || '';

    const badgeCss = isFiled
      ? 'background:#14291e;color:#22c55e;border:1px solid #1a4228;'
      : 'background:#2a2414;color:#fbbf24;border:1px solid #4a3a1a;';
    const badgeLabel = isFiled ? 'Filed' : ('⚠ Review' + (conf != null ? ' ' + conf + '%' : ''));

    const div = document.createElement('div');
    div.className        = 'doc-row';
    div.dataset.uploadId = uid;
    div.dataset.status   = isFiled ? 'filed' : 'review';
    div.dataset.sortDate     = nowIso;
    div.dataset.sortUploader = uploader.toLowerCase();
    div.dataset.sortFilename = displayName.toLowerCase();
    div.dataset.sortOriginal = filename.toLowerCase();
    div.dataset.sortType     = docType.toLowerCase();
    // group-key drives the type subtab filter; matches doc_groups keys
    // (category value, or '_unsorted' when the analyzer returned no type).
    div.dataset.groupKey     = docType ? docType.toLowerCase() : '_unsorted';
    // Pull OCR-extracted vendor/amount/date from the upload response so
    // clicking the freshly-prepended row pre-fills the modal fields.
    div.dataset.sortVendor   = (d.vendor   || '').toString();
    div.dataset.sortAmount   = (d.amount   || 0).toString();
    div.dataset.docDate      = (d.doc_date || '').toString();
    div.dataset.docNum       = (d.doc_number || '').toString();
    div.dataset.sortCard4    = (d.card_last4 || '').toString();
    div.dataset.note         = '';
    div.dataset.sortSize     = file.size || 0;
    div.dataset.sortStatus   = isFiled ? 'filed' : 'review';
    div.style.cssText = 'background:var(--bg-card);border:1px solid var(--border);border-radius:8px;padding:12px 14px;display:flex;align-items:flex-start;gap:12px;';

    const wasRenamed = displayName && displayName !== filename;
    const renameRow  = wasRenamed
      ? `<div style="font-size:11px;color:#6b7280;font-style:italic;margin-top:1px;">📎 uploaded as: ${_esc(filename)}</div>`
      : '';

    div.innerHTML = `
      <div style="font-size:20px;width:32px;text-align:center;padding-top:2px;">${icon}</div>
      <div style="flex:1;min-width:0;">
        <div style="font-size:13px;font-weight:500;">
          <span class="doc-filename ${isFiled ? 'editable-filename' : ''}" data-id="${uid}" data-original="${_esc(displayName)}"
                ${isFiled ? 'title="Click to rename — Dropbox will update too" style="cursor:pointer;border-bottom:1px dashed transparent;padding:1px 2px;border-radius:2px;"' : ''}>${_esc(displayName)}</span>
          ${isFiled ? '<span class="rename-hint" style="font-size:10px;color:#6b7280;margin-left:4px;opacity:0;">✎</span>' : ''}
        </div>
        ${renameRow}
        <div style="font-size:11px;color:var(--text-muted);margin-top:3px;">
          ${uploader ? `👤 ${_esc(uploader)} · ` : ''}📅 ${_esc(dateStr)}${docType ? ` · 🏷 <strong>${_esc(docType)}</strong>` : ''}${sizeStr ? ` · 📦 ${sizeStr}` : ''}
        </div>
      </div>
      <span class="doc-status-badge" style="font-size:11px;padding:3px 9px;border-radius:20px;white-space:nowrap;${badgeCss}">${badgeLabel}</span>
      <button class="doc-delete-btn" title="Delete this document" style="background:none;border:none;color:#ef4444;cursor:pointer;padding:4px 8px;font-size:14px;">✕</button>
    `;
    h.prepend(div);

    // Re-apply the active subtab + search so the new row lands in the
    // right view (and the count reflects what's actually visible).
    if (typeof _docsApplyView === 'function') _docsApplyView();
  }

  // ── Delete + Retry Filing handlers (event-delegated) ─────────────────────
  document.addEventListener('click', async function(e) {
    const row = e.target.closest('.doc-row');
    if (!row) return;
    const uid = row.dataset.uploadId;
    if (!uid) return;

    if (e.target.closest('.doc-delete-btn')) {
      e.preventDefault();
      e.stopPropagation();
      const name = row.querySelector('div > div > div')?.textContent?.trim() || 'this document';
      if (!confirm(`Move ${name} to the Trash? You can restore it from Docs → 🗑 Trash.`)) return;
      const r = await fetch('/docs/upload/' + uid + '/delete', { method: 'POST' });
      if (r.ok) {
        row.style.transition = 'opacity .3s';
        row.style.opacity = '0';
        setTimeout(() => row.remove(), 300);
        // Mirror in the Actuals tab without a reload. (User 2026-06-11.)
        if (typeof _docRemoveActualsRowsForDoc === 'function') _docRemoveActualsRowsForDoc(uid);
      } else {
        const d = await r.json().catch(() => ({}));
        alert('Delete failed: ' + (d.error || r.status));
      }
      return;
    }

    if (e.target.closest('.doc-retry-btn')) {
      e.preventDefault();
      e.stopPropagation();
      const btn = e.target.closest('.doc-retry-btn');
      btn.disabled = true;
      btn.textContent = '⟳ Filing…';
      const r = await fetch('/docs/upload/' + uid + '/retry-filing', { method: 'POST' });
      const d = await r.json().catch(() => ({}));
      if (r.ok) {
        // Flip the badge to "Filed" and remove the retry button
        const badge = row.querySelector('.doc-status-badge');
        if (badge) {
          badge.textContent = 'Filed';
          badge.style.cssText = 'font-size:11px;padding:3px 9px;border-radius:20px;background:#14291e;color:#22c55e;border:1px solid #1a4228;';
        }
        btn.remove();
      } else {
        btn.disabled = false;
        btn.textContent = '↻ Retry';
        alert('Retry failed: ' + (d.error || r.status));
      }
    }
  });

  // ── Possible-duplicate review (Keep both / It's a dupe) ──────────────────
  // Resolves a doc the Analyzer flagged as a byte-identical match of an
  // earlier upload. "keep" clears the flag + creates the deferred Actuals
  // txn; "confirm" buries the file in /_DUPLICATES/. 2026-05-29.
  window.docsResolveDuplicate = async function(uid, action, btn) {
    const row = btn && btn.closest('.doc-row');
    // Disable both action buttons while the request is in flight.
    const cluster = btn && btn.parentElement;
    const dupBtns = cluster ? cluster.querySelectorAll('.doc-dup-keep-btn, .doc-dup-confirm-btn, .doc-dup-compare-btn') : [];
    dupBtns.forEach(b => { b.disabled = true; b.style.opacity = '.5'; });
    if (action === 'confirm' &&
        !confirm("Confirm this is a duplicate? The file will be moved to a /_DUPLICATES/ subfolder in Dropbox.")) {
      dupBtns.forEach(b => { b.disabled = false; b.style.opacity = ''; });
      return;
    }
    try {
      let r = await fetch('/docs/upload/' + uid + '/resolve-duplicate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action }),
      });
      let d = await r.json().catch(() => ({}));
      // Linked / coded duplicate: server returns 409 with what it's tied to so
      // we can ask the user how to handle it before burying. (User 2026-06-17.)
      if (r.status === 409 && d.needs_force) {
        const charges = d.linked_charges || [];
        const money = a => (a != null) ? ('$' + Number(a).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2})) : '$?';
        let link_mode = null;
        if (charges.length && d.keeper) {
          // Real choice: re-point the charge(s) to the kept original, or unlink.
          const list = charges.map(c => '  • ' + (c.vendor || 'charge') + ' ' + money(c.amount) +
                          (c.date ? (' · ' + c.date) : '') + (c.code ? (' · ' + c.code) : '')).join('\n');
          const transfer = confirm(
            'This duplicate is linked to ' + charges.length + ' charge' + (charges.length > 1 ? 's' : '') + ':\n' + list +
            '\n\nKept original: ' + (d.keeper.filed || ('#' + d.keeper.id)) +
            '\n\n[ OK ]  Re-point the charge' + (charges.length > 1 ? 's' : '') + ' to the kept original (keep the match — recommended)\n' +
            '[ Cancel ]  Unlink ' + (charges.length > 1 ? 'them' : 'it') + ' back to the unmatched pool');
          link_mode = transfer ? 'transfer' : 'unlink';
        } else {
          // Coded but no transferable charge/keeper — just confirm the un-code + bury.
          const amt = (d.amount != null) ? (' (' + money(d.amount) + ')') : '';
          if (!confirm('This duplicate' + amt + (d.coded_to ? (' is coded to ' + d.coded_to) : ' is linked') +
                       '.\n\nBurying it removes that duplicate transaction from Actuals (de-double-counting). The original copy keeps its coding.\n\nProceed?')) {
            dupBtns.forEach(b => { b.disabled = false; b.style.opacity = ''; });
            return;
          }
          link_mode = 'unlink';
        }
        r = await fetch('/docs/upload/' + uid + '/resolve-duplicate', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ action, force: true, link_mode }),
        });
        d = await r.json().catch(() => ({}));
        if (r.ok && d.transferred_charges) {
          alert('Re-pointed ' + d.transferred_charges + ' charge' + (d.transferred_charges > 1 ? 's' : '') +
                ' to the kept original. The duplicate was buried.');
        }
      }
      if (!r.ok) {
        alert('Could not resolve: ' + (d.error || r.status));
        dupBtns.forEach(b => { b.disabled = false; b.style.opacity = ''; });
        return;
      }
      if (!row) return;
      const badge = row.querySelector('.doc-status-badge');
      // Remove both review buttons either way.
      dupBtns.forEach(b => b.remove());
      row.dataset.dupPending = '';
      if (action === 'keep') {
        row.dataset.isDuplicate = '';
        row.dataset.duplicateOf = '';
        if (badge) {
          badge.textContent = '✓';
          badge.style.cssText = 'font-size:11px;padding:3px 9px;border-radius:20px;background:#14291e;color:#22c55e;border:1px solid #1a4228;';
        }
        row.style.transition = 'background .6s';
        row.style.background = 'rgba(34,197,94,.10)';
        setTimeout(() => { row.style.background = ''; }, 900);
      } else {
        // Confirmed duplicate — recolor to purple and fade the row out of
        // the clean view (it now lives in /_DUPLICATES/). It still shows
        // under the "Duplicates" filter chip after reload.
        if (badge) {
          badge.textContent = '🔁 Dup';
          badge.style.cssText = 'font-size:11px;padding:3px 9px;border-radius:20px;background:#1e1a2a;color:#a78bfa;border:1px solid #2d2a40;';
        }
        row.dataset.status = 'duplicate';
        row.style.transition = 'opacity .4s';
        row.style.opacity = '.45';
      }
      if (typeof _docsRefreshDupCounts === 'function') _docsRefreshDupCounts();
      if (typeof _docsApplyView === 'function') _docsApplyView();
    } catch (err) {
      alert('Network error: ' + (err && err.message || err));
      dupBtns.forEach(b => { b.disabled = false; b.style.opacity = ''; });
    }
  };

  // ── Duplicate GROUP review (side-by-side, per-item keep/dupe) ─────────
  // A "group" is every doc-row sharing the same file_hash (byte-identical
  // contents). The modal shows them all together; each gets a Keep /
  // Duplicate toggle (≥1 must be Kept). One Apply resolves the whole group
  // via the batch endpoint. Metadata is read straight from each row in the
  // DOM (rows exist even when filtered/hidden); files embed via /raw.
  // 2026-05-29 — replaces the old 2-pane pairwise compare.
  let _docDupGroupState = {};   // { uid: 'keep' | 'dupe' }

  function _docGroupMembers(uid) {
    const seed = document.querySelector('.doc-row[data-upload-id="' + uid + '"]');
    const hash = seed && seed.dataset.fileHash;
    let rows;
    if (hash) {
      rows = Array.from(document.querySelectorAll(
        '.doc-row[data-file-hash="' + hash + '"]'));
    } else {
      // No hash → fall back to seed + its recorded match.
      rows = seed ? [seed] : [];
      const orig = seed && seed.dataset.duplicateOf;
      if (orig) {
        const o = document.querySelector('.doc-row[data-upload-id="' + orig + '"]');
        if (o) rows.push(o);
      }
    }
    // Drop rows already confirmed (they live in /_DUPLICATES/ now) — the
    // review group is just the original + still-pending copies.
    rows = rows.filter(r => (r.dataset.status || '') !== 'duplicate');
    // Stable order: oldest upload id first (the original tends to be first).
    return rows.sort((a, b) => (+a.dataset.uploadId) - (+b.dataset.uploadId));
  }
  function _docMeta(row) {
    const fname = (row.querySelector('.doc-filename')?.textContent || '').trim();
    return {
      uid:    row.dataset.uploadId,
      vendor: (row.querySelector('.doc-cell-vendor')?.textContent || '').trim(),
      amount: (row.querySelector('.doc-cell-amount')?.textContent || '').trim(),
      fname,
      date:   row.dataset.docDate  || '',
      dtype:  row.dataset.sortType || '',
      status: (row.dataset.status || '').toLowerCase(),
      isDup:  row.dataset.isDuplicate === '1',
      isPdf:  /\.pdf\b/i.test(fname) || /\.pdf\b/i.test(row.dataset.sortFilename || ''),
    };
  }
  // Reference-only pane (the ORIGINAL a flagged copy duplicates) — preview +
  // metadata, no Keep/Duplicate actions, green-tinted "Original on file" tag.
  function _docRefPaneHTML(m) {
    const src = '/docs/upload/' + m.uid + '/raw';
    const preview = m.isPdf
      ? '<iframe src="' + src + '#zoom=page-width" style="width:100%;height:100%;border:1px solid var(--border);border-radius:6px;background:#fff"></iframe>'
      : '<img src="' + src + '" alt="original" style="width:100%;height:100%;object-fit:contain;border:1px solid var(--border);border-radius:6px;background:#0a0d12">';
    const mrow = (k, v) => '<span style="margin-right:14px;white-space:nowrap"><span style="color:var(--text-muted)">' + k + ':</span> ' + _esc(v || '—') + '</span>';
    return ''
      + '<div class="dup-pane" style="flex:1;min-width:260px;min-height:0;display:flex;flex-direction:column;gap:8px;'
      +      'border:1px solid #2a5a3a;border-radius:8px;padding:10px;background:var(--bg-card)">'
      +   '<div style="display:flex;align-items:center;gap:8px;flex-shrink:0">'
      +     '<span style="font-size:12px;font-weight:600;color:#74c69d">📌 Original on file · #' + m.uid + '</span>'
      +     '<a href="' + src + '" target="_blank" rel="noopener" style="margin-left:auto;background:#1a2540;'
      +        'border:1px solid #2d4070;color:#8fb4ff;padding:3px 9px;border-radius:5px;font-size:11px;text-decoration:none">↗ Full size</a>'
      +   '</div>'
      +   '<div class="dup-preview" style="flex:1;min-height:300px">' + preview + '</div>'
      +   '<div style="font-size:11.5px;line-height:1.5;word-break:break-word;flex-shrink:0">'
      +     mrow('Vendor', m.vendor) + mrow('Amount', m.amount) + mrow('Date', m.date) + mrow('Type', m.dtype)
      +     '<div style="margin-top:2px"><span style="color:var(--text-muted)">File:</span> ' + _esc(m.fname || '—') + '</div>'
      +   '</div>'
      + '</div>';
  }
  function _docGroupPaneHTML(m, single) {
    const src = '/docs/upload/' + m.uid + '/raw';
    // Preview FILLS the pane height (flex:1) so docs are big and readable
    // without fighting the embedded PDF zoom. Per-pane "Expand" makes one
    // fill the modal; "Full size" opens /raw in a new tab (native zoom,
    // can't close the modal). 2026-05-29.
    const preview = m.isPdf
      ? '<iframe src="' + src + '#zoom=page-width" style="width:100%;height:100%;border:1px solid var(--border);border-radius:6px;background:#fff"></iframe>'
      : '<img src="' + src + '" alt="preview" style="width:100%;height:100%;object-fit:contain;border:1px solid var(--border);border-radius:6px;background:#0a0d12">';
    const mrow = (k, v) => '<span style="margin-right:14px;white-space:nowrap"><span style="color:var(--text-muted)">' + k + ':</span> ' + _esc(v || '—') + '</span>';
    return ''
      + '<div class="dup-pane" data-uid="' + m.uid + '" '
      +      'style="flex:1;min-width:260px;min-height:0;display:flex;flex-direction:column;gap:8px;'
      +      'border:1px solid var(--border);border-radius:8px;padding:10px;background:var(--bg-card)">'
      +   '<div style="display:flex;align-items:center;gap:8px;flex-shrink:0">'
      +     '<span style="font-size:12px;font-weight:600;color:var(--text-muted)">#' + m.uid + '</span>'
      +     '<button type="button" onclick="docDupEditDoc(\'' + m.uid + '\')" '
      +        'title="Open this document to edit / reclassify (wrong doc type, vendor, etc.)" '
      +        'style="margin-left:auto;background:#241a2e;border:1px solid #3a2a4a;color:#c9a8ff;'
      +        'padding:3px 9px;border-radius:5px;cursor:pointer;font-size:11px">✎ Edit / reclassify</button>'
      +     '<button type="button" class="dup-expand-btn" onclick="docDupExpand(\'' + m.uid + '\')" '
      +        'style="background:#1a1f2a;border:1px solid var(--border);color:#cbd5e1;'
      +        'padding:3px 9px;border-radius:5px;cursor:pointer;font-size:11px">⤢ Expand</button>'
      +     '<a href="' + src + '" target="_blank" rel="noopener" '
      +        'style="background:#1a2540;border:1px solid #2d4070;color:#8fb4ff;'
      +        'padding:3px 9px;border-radius:5px;font-size:11px;text-decoration:none">↗ Full size</a>'
      +   '</div>'
      +   '<div class="dup-preview" style="flex:1;min-height:300px">' + preview + '</div>'
      +   '<div style="font-size:11.5px;line-height:1.5;word-break:break-word;flex-shrink:0">'
      +     mrow('Vendor', m.vendor) + mrow('Amount', m.amount) + mrow('Date', m.date) + mrow('Type', m.dtype)
      +     '<div style="margin-top:2px"><span style="color:var(--text-muted)">File:</span> ' + _esc(m.fname || '—') + '</div>'
      +   '</div>'
      // Single-member groups (the only copy still in review — its match was
      // already resolved or isn't loaded) resolve IMMEDIATELY via the
      // single-doc endpoint, so the ≥1-keep rule that needs two items can't
      // strand the user. (User 2026-06-01.)
      + (single
        ? ('<div class="dup-toggle" style="display:flex;gap:6px;flex-shrink:0">'
          +   '<button type="button" onclick="docDupResolveSingle(\'' + m.uid + '\',\'keep\')" '
          +      'style="flex:1;padding:7px;border-radius:6px;cursor:pointer;font-size:12px;'
          +      'border:1px solid #1a4228;background:#14291e;color:#22c55e">✓ Keep — not a duplicate</button>'
          +   '<button type="button" onclick="docDupResolveSingle(\'' + m.uid + '\',\'confirm\')" '
          +      'style="flex:1;padding:7px;border-radius:6px;cursor:pointer;font-size:12px;'
          +      'border:1px solid #2d2a40;background:#1e1a2a;color:#a78bfa">🔁 It\'s a duplicate</button>'
          + '</div>')
        : ('<div class="dup-toggle" style="display:flex;gap:6px;flex-shrink:0">'
          +   '<button type="button" class="dup-keep" onclick="docDupSetChoice(\'' + m.uid + '\',\'keep\')" '
          +      'style="flex:1;padding:7px;border-radius:6px;cursor:pointer;font-size:12px;'
          +      'border:1px solid #1a4228;background:#14291e;color:#22c55e">Keep</button>'
          +   '<button type="button" class="dup-dupe" onclick="docDupSetChoice(\'' + m.uid + '\',\'dupe\')" '
          +      'style="flex:1;padding:7px;border-radius:6px;cursor:pointer;font-size:12px;'
          +      'border:1px solid #2d2a40;background:#1e1a2a;color:#a78bfa">Duplicate</button>'
          + '</div>'))
      + '</div>';
  }
  // Per-copy linkage banner for the compare modal: shows the charge(s) a copy
  // is matched to (and/or what it's coded to) so you can see the connection
  // before burying it. (User 2026-06-17.)
  window._dupGroupLinks = {};
  function _docRenderDupLink(info) {
    if (!info) return '';
    const charges = info.linked_charges || [];
    if (!charges.length && !info.coded_to) return '';
    const money = a => (a != null) ? ('$' + Number(a).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2})) : '$?';
    let html = '<div class="dup-link-note" style="flex-shrink:0;font-size:11px;background:#1a1426;border:1px solid #2d2440;border-radius:6px;padding:6px 9px;color:#c9a8ff;line-height:1.5">';
    if (charges.length) {
      html += '🔗 Linked to ' + charges.length + ' charge' + (charges.length > 1 ? 's' : '') + ':<br>' +
        charges.map(c => '&nbsp;• ' + _esc(c.vendor || 'charge') + ' ' + money(c.amount) +
          (c.date ? (' · ' + _esc(c.date)) : '') + (c.code ? (' · ' + _esc(c.code)) : '')).join('<br>');
      if (info.keeper) html += '<br><span style="color:var(--text-muted)">On bury: re-point to the kept original, or unlink.</span>';
    } else if (info.coded_to) {
      html += '🏷 Coded to ' + _esc(info.coded_to);
    }
    html += '</div>';
    return html;
  }
  // Fetch linkage for the group's members and inject the banners into each pane.
  function _docLoadDupLinks(members) {
    const body = document.getElementById('docDupCompareBody');
    if (!body || !members.length) return;
    const uids = members.map(r => parseInt(r.dataset ? r.dataset.uploadId : r)).filter(Boolean);
    fetch('/docs/' + PROJ_ID + '/dup-links', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ids: uids }),
    }).then(r => r.ok ? r.json() : null).then(j => {
      window._dupGroupLinks = (j && j.links) || {};
      uids.forEach(uid => {
        const note = _docRenderDupLink(window._dupGroupLinks[uid]);
        if (!note) return;
        const pane = body.querySelector('.dup-pane[data-uid="' + uid + '"]');
        const toggle = pane && pane.querySelector('.dup-toggle');
        if (pane && toggle) toggle.insertAdjacentHTML('beforebegin', note);
      });
    }).catch(() => {});
  }
  // Jump from the compare view into the full doc-detail modal so a flagged
  // duplicate that's actually MISLABELED can be reclassified / edited — the
  // compare modal only offers keep/duplicate. (User 2026-06-01: "review and
  // compare do the same thing, I can't reclassify.")
  window.docDupEditDoc = function(uid) {
    if (typeof closeDupCompare === 'function') closeDupCompare();
    const row = document.querySelector('.doc-row[data-upload-id="' + uid + '"]');
    if (typeof openDocDetail === 'function') openDocDetail(parseInt(uid), row || null);
  };
  // Expand one pane to fill the modal (hide siblings); click again to restore.
  window.docDupExpand = function(uid) {
    const body  = document.getElementById('docDupCompareBody');
    if (!body) return;
    const panes = body.querySelectorAll('.dup-pane');
    const isOn  = body.dataset.expanded === String(uid);
    body.dataset.expanded = isOn ? '' : String(uid);
    panes.forEach(p => {
      p.style.display = (isOn || p.dataset.uid === String(uid)) ? '' : 'none';
      const btn = p.querySelector('.dup-expand-btn');
      if (btn) btn.textContent = (!isOn && p.dataset.uid === String(uid)) ? '⤡ Collapse' : '⤢ Expand';
    });
  };
  window.docDupSetChoice = function(uid, choice) {
    _docDupGroupState[uid] = choice;
    _docDupRefreshChoices();
  };
  function _docDupRefreshChoices() {
    const panes = document.querySelectorAll('#docDupCompareBody .dup-pane');
    let keeps = 0, dupes = 0;
    panes.forEach(p => {
      const choice = _docDupGroupState[p.dataset.uid] || 'keep';
      const keepBtn = p.querySelector('.dup-keep');
      const dupeBtn = p.querySelector('.dup-dupe');
      const on  = (b) => { b.style.outline = '2px solid currentColor'; b.style.fontWeight = '700'; };
      const off = (b) => { b.style.outline = 'none'; b.style.fontWeight = '400'; };
      if (choice === 'dupe') { on(dupeBtn); off(keepBtn); dupes++; }
      else                   { on(keepBtn); off(dupeBtn); keeps++; }
    });
    const sum   = document.getElementById('docDupApplySummary');
    const apply = document.getElementById('docDupApplyBtn');
    if (keeps === 0) {
      if (sum) { sum.textContent = '⚠ At least one document must be kept.'; sum.style.color = '#fbbf24'; }
      if (apply) { apply.disabled = true; apply.style.opacity = '.5'; apply.style.cursor = 'not-allowed'; }
    } else {
      if (sum) { sum.textContent = keeps + ' keep · ' + dupes + ' duplicate'; sum.style.color = 'var(--text-muted)'; }
      if (apply) { apply.disabled = false; apply.style.opacity = ''; apply.style.cursor = 'pointer'; }
    }
  }

  // ── Compare-queue prev/next over the Docs-Review duplicate groups ──────────
  // (User 2026-06-22.) Page through duplicate groups without leaving the modal.
  // If a keep/duplicate decision was made, paging SAVES it + drops the group;
  // if nothing was touched, paging just moves on and the group stays in queue.
  let _docDupNav = { queue: [], curKey: null, idx: -1 };
  let _docDupOpenSig = '';
  function _docGroupKey(uid) {
    const r = document.querySelector('.doc-row[data-upload-id="' + uid + '"]');
    if (!r) return null;
    return r.dataset.fileHash || ('dof:' + (r.dataset.duplicateOf || uid));
  }
  function _docDupQueue() {
    const rows = Array.from(document.querySelectorAll('#docsHistory .doc-row'))
      .filter(r => r.dataset.isDuplicate === '1' && (r.dataset.status || '') !== 'duplicate');
    const seen = new Set(), out = [];
    rows.forEach(r => {
      const key = r.dataset.fileHash || ('dof:' + (r.dataset.duplicateOf || r.dataset.uploadId));
      if (seen.has(key)) return;
      seen.add(key);
      out.push({ uid: parseInt(r.dataset.uploadId), key });
    });
    return out;
  }
  function _docDupUpdateNavButtons() {
    const q = _docDupNav.queue;
    let idx = q.findIndex(g => g.key === _docDupNav.curKey);
    if (idx === -1) idx = _docDupNav.idx;
    const prev = document.getElementById('docDupPrevBtn');
    const nxt  = document.getElementById('docDupNextBtn');
    const pos  = document.getElementById('docDupNavPos');
    const set = (b, dis) => { if (!b) return; b.disabled = dis; b.style.opacity = dis ? '.4' : ''; b.style.cursor = dis ? 'not-allowed' : 'pointer'; };
    if (!q.length || idx === -1) { set(prev, true); set(nxt, true); if (pos) pos.textContent = ''; return; }
    if (pos) pos.textContent = (idx + 1) + ' / ' + q.length;
    set(prev, idx <= 0);
    set(nxt, idx >= q.length - 1);
  }
  // Capture the queue + position when the compare modal opens for `uid`.
  window._docDupCaptureNav = function (uid) {
    _docDupNav.queue = _docDupQueue();
    _docDupNav.curKey = _docGroupKey(uid);
    _docDupNav.idx = _docDupNav.queue.findIndex(g => g.key === _docDupNav.curKey);
    _docDupOpenSig = JSON.stringify(_docDupGroupState || {});
    _docDupUpdateNavButtons();
  };
  window.docDupNav = async function (delta) {
    const q = _docDupNav.queue;
    let idx = q.findIndex(g => g.key === _docDupNav.curKey);
    if (idx === -1) idx = _docDupNav.idx;
    const neighbor = (idx >= 0) ? (q[idx + delta] || null) : null;
    // Intervention = the user actually changed a keep/duplicate choice.
    const changed = JSON.stringify(_docDupGroupState || {}) !== _docDupOpenSig;
    if (changed) {
      const applyBtn = document.getElementById('docDupApplyBtn');
      if (applyBtn && !applyBtn.disabled) {
        await docDupApplyGroup();   // resolves the current group (+ removes it + closes)
      }
    }
    // Move to the neighbor group (it wasn't the one we just resolved). If there's
    // no neighbor, close — and if we just saved, say so.
    if (neighbor) {
      docsOpenDupGroup(neighbor.uid);
    } else {
      if (typeof closeDupCompare === 'function') closeDupCompare();
      if (changed && typeof _actualsToast === 'function') _actualsToast('Saved — no more groups this way.', 'green');
    }
  };
  // Public entry — the row's "⇆ Compare" button calls this.
  window.docsOpenDupGroup = function(uid) {
    // Hoist the overlay to <body> so it shows even when opened from a tab
    // whose panel is display:none (e.g. the Reconcile modal in Actuals).
    // Idempotent. (User 2026-06-17.)
    const _ov = document.getElementById('docDupCompareOverlay');
    if (_ov && _ov.parentElement !== document.body) document.body.appendChild(_ov);
    const members = _docGroupMembers(uid);
    const body    = document.getElementById('docDupCompareBody');
    if (!body || !members.length) {
      if (typeof _actualsToast === 'function') _actualsToast('Could not load that receipt to compare.', 'yellow');
      return;
    }
    _docDupGroupState = {};
    const applyBtn = document.getElementById('docDupApplyBtn');
    const sumEl    = document.getElementById('docDupApplySummary');
    // Degenerate group: only ONE copy is still in review (its match was
    // already resolved, deleted, or isn't in the loaded list). The
    // side-by-side compare + ≥1-keep gating can't resolve this, so offer a
    // direct keep / confirm on the single doc instead. (User 2026-06-01.)
    if (members.length === 1) {
      const seed   = members[0];
      const origId = seed.dataset.duplicateOf;
      const origInDom = origId && document.querySelector('.doc-row[data-upload-id="' + origId + '"]');
      if (applyBtn) applyBtn.style.display = 'none';   // actions are inline
      // If we know which doc it duplicates and that original isn't loaded in
      // the Review list, FETCH it so the user can actually compare instead of
      // staring at a lone receipt. (User 2026-06-02.)
      if (origId && !origInDom) {
        body.innerHTML = '<div style="padding:18px;color:var(--text-muted)">Loading the original to compare…</div>';
        document.getElementById('docDupCompareOverlay').style.display = 'flex';
        fetch('/docs/upload/' + origId + '/meta')
          .then(r => r.ok ? r.json() : null)
          .then(orig => {
            const flagged = _docMeta(seed);
            if (orig) {
              const origMeta = {
                uid: String(orig.id), vendor: orig.vendor,
                amount: (orig.amount != null) ? ('$' + Number(orig.amount).toFixed(2)) : '',
                fname: orig.filename, date: orig.doc_date || '',
                dtype: orig.category || '', isPdf: orig.is_pdf,
              };
              body.innerHTML = _docRefPaneHTML(origMeta) + _docGroupPaneHTML(flagged, true);
              if (sumEl) { sumEl.textContent = 'Left = the original already on file (#' + orig.id + '). Right = the new copy flagged as a duplicate — Keep it if it’s genuinely different, or confirm it’s a dupe.'; sumEl.style.color = 'var(--text-muted)'; }
            } else {
              body.innerHTML = _docGroupPaneHTML(flagged, true);
              if (sumEl) { sumEl.textContent = 'Couldn’t load the original (#' + origId + ') — it may have been deleted. Keep this copy, or confirm it as a duplicate.'; sumEl.style.color = 'var(--text-muted)'; }
            }
          })
          .catch(() => { body.innerHTML = _docGroupPaneHTML(_docMeta(seed), true); });
        return;
      }
      body.innerHTML = _docGroupPaneHTML(_docMeta(seed), true);
      if (sumEl) {
        sumEl.textContent = 'This is the only copy still in review — keep it, or confirm it as a duplicate.';
        sumEl.style.color = 'var(--text-muted)';
      }
      document.getElementById('docDupCompareOverlay').style.display = 'flex';
      return;
    }
    if (applyBtn) applyBtn.style.display = '';          // restore for multi
    // Default: a CONFIRMED dup (already filed away) defaults to Duplicate;
    // everything else defaults to Keep. So the unflagged original stays kept
    // and the flagged copies start as Duplicate only if already confirmed —
    // otherwise the user makes the call. To make the common case one-click,
    // default flagged-pending copies to Duplicate and the lowest-id (likely
    // original, unflagged) to Keep.
    members.forEach(row => {
      const m = _docMeta(row);
      _docDupGroupState[m.uid] = (m.isDup) ? 'dupe' : 'keep';
    });
    body.innerHTML = members.map(row => _docGroupPaneHTML(_docMeta(row))).join('');
    _docDupRefreshChoices();
    _docLoadDupLinks(members);   // show each copy's linked charge inline
    document.getElementById('docDupCompareOverlay').style.display = 'flex';
  };
  // Backwards-compat alias (older inline onclick may still reference it).
  window.docsOpenDupCompare = window.docsOpenDupGroup;

  // Open the compare overlay for an EXPLICIT set of doc ids — used by Reconcile,
  // where a cluster's receipts share amount/date/vendor but may NOT be
  // byte-identical (so file_hash grouping would show only one). (User 2026-06-17.)
  window.docsOpenCompareSet = function(docIds) {
    docIds = (docIds || []).map(Number).filter(Boolean);
    const _ov = document.getElementById('docDupCompareOverlay');
    if (_ov && _ov.parentElement !== document.body) document.body.appendChild(_ov);
    const body = document.getElementById('docDupCompareBody');
    if (!body) return;
    // One id → reuse the normal opener (handles the "fetch the original" case).
    if (docIds.length <= 1) { window.docsOpenDupGroup(docIds[0]); return; }
    const members = docIds
      .map(id => document.querySelector('.doc-row[data-upload-id="' + id + '"]'))
      .filter(Boolean);
    if (!members.length) {
      if (typeof _actualsToast === 'function') _actualsToast('Could not load those receipts to compare.', 'yellow');
      return;
    }
    _docDupGroupState = {};
    const applyBtn = document.getElementById('docDupApplyBtn');
    const sumEl    = document.getElementById('docDupApplySummary');
    if (applyBtn) applyBtn.style.display = '';
    members.forEach(row => { const m = _docMeta(row); _docDupGroupState[m.uid] = (m.isDup) ? 'dupe' : 'keep'; });
    body.innerHTML = members.map(row => _docGroupPaneHTML(_docMeta(row))).join('');
    _docDupRefreshChoices();
    _docLoadDupLinks(members);
    if (sumEl) { sumEl.textContent = members.length + ' receipts for the same spend — compare, then keep the right one.'; sumEl.style.color = 'var(--text-muted)'; }
    document.getElementById('docDupCompareOverlay').style.display = 'flex';
  };

  // Wrap the two compare openers so prev/next captures the queue + position on
  // every open (after each sets up its keep/duplicate state). Re-pointing the
  // legacy alias too. (User 2026-06-22.)
  (function () {
    const _origOpenDupGroup = window.docsOpenDupGroup;
    window.docsOpenDupGroup = function (uid) {
      const r = _origOpenDupGroup.apply(this, arguments);
      try { window._docDupCaptureNav(uid); } catch (e) {}
      return r;
    };
    window.docsOpenDupCompare = window.docsOpenDupGroup;
    const _origOpenCompareSet = window.docsOpenCompareSet;
    if (_origOpenCompareSet) {
      window.docsOpenCompareSet = function (docIds) {
        const r = _origOpenCompareSet.apply(this, arguments);
        try { window._docDupCaptureNav((docIds || [])[0]); } catch (e) {}
        return r;
      };
    }
  })();

  // Resolve a SINGLE-member review group (keep | confirm) via the single-doc
  // endpoint — used when the compare modal has only one copy left to act on.
  // (User 2026-06-01: "compare group opens with one document and I can't keep
  // or flag it.") Patches the row in place and closes the modal. 2026-06-01.
  window.docDupResolveSingle = async function(uid, action) {
    if (action === 'confirm' &&
        !confirm("Confirm this is a duplicate? The file will be moved to a /_DUPLICATES/ subfolder in Dropbox.")) {
      return;
    }
    const body = document.getElementById('docDupCompareBody');
    if (body) body.querySelectorAll('button').forEach(b => { b.disabled = true; b.style.opacity = '.6'; });
    try {
      const r = await fetch('/docs/upload/' + uid + '/resolve-duplicate', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action }),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) {
        alert('Could not resolve: ' + (d.error || r.status));
        if (body) body.querySelectorAll('button').forEach(b => { b.disabled = false; b.style.opacity = ''; });
        return;
      }
      const row = document.querySelector('.doc-row[data-upload-id="' + uid + '"]');
      if (row) {
        row.dataset.dupPending = '';
        const badge = row.querySelector('.doc-status-badge');
        if (action === 'keep') {
          row.dataset.isDuplicate = '';
          row.dataset.duplicateOf = '';
          if (badge) {
            badge.textContent = '✓';
            badge.style.cssText = 'font-size:11px;padding:3px 9px;border-radius:20px;background:#14291e;color:#22c55e;border:1px solid #1a4228;';
          }
        } else {
          if (badge) {
            badge.textContent = '🔁 Dup';
            badge.style.cssText = 'font-size:11px;padding:3px 9px;border-radius:20px;background:#1e1a2a;color:#a78bfa;border:1px solid #2d2a40;';
          }
          row.dataset.status = 'duplicate';
          row.style.transition = 'opacity .4s';
          row.style.opacity = '.45';
        }
        row.querySelectorAll('.doc-dup-keep-btn, .doc-dup-confirm-btn, .doc-dup-compare-btn')
           .forEach(b => b.remove());
      }
      closeDupCompare();
      if (typeof _docsRefreshDupCounts === 'function') _docsRefreshDupCounts();
      if (typeof _docsApplyView === 'function') _docsApplyView();
    } catch (e) {
      alert('Network error: ' + (e && e.message || e));
      if (body) body.querySelectorAll('button').forEach(b => { b.disabled = false; b.style.opacity = ''; });
    }
  };

  window.docDupApplyGroup = async function() {
    const keep = [], confirm = [];
    Object.keys(_docDupGroupState).forEach(uid => {
      (_docDupGroupState[uid] === 'dupe' ? confirm : keep).push(parseInt(uid));
    });
    if (!keep.length) { alert('At least one document must be kept.'); return; }
    // If any copy being buried is linked to a charge (or coded), ask how to
    // handle it — re-point to the kept original, or unlink. (User 2026-06-17.)
    const links = window._dupGroupLinks || {};
    const linkedConfirms = confirm.filter(uid => {
      const i = links[uid]; return i && ((i.linked_charges && i.linked_charges.length) || i.coded_to);
    });
    let force = false, link_mode = null;
    if (linkedConfirms.length) {
      force = true;
      const transferable = linkedConfirms.some(uid => {
        const i = links[uid]; return i.keeper && i.linked_charges && i.linked_charges.length;
      });
      if (transferable) {
        const lines = linkedConfirms.flatMap(uid => (links[uid].linked_charges || [])
          .map(c => '  • ' + (c.vendor || 'charge') + ' $' + (c.amount != null ? Number(c.amount).toFixed(2) : '?') +
                    (c.date ? (' · ' + c.date) : '') + (c.code ? (' · ' + c.code) : ''))).join('\n');
        const t = window.confirm(
          'Some duplicates you\'re burying are matched to charges:\n' + lines +
          '\n\n[ OK ]  Re-point the charge(s) to the kept original (keep the match)\n' +
          '[ Cancel ]  Unlink them back to the unmatched pool');
        link_mode = t ? 'transfer' : 'unlink';
      } else {
        link_mode = 'unlink';
      }
    }
    const apply = document.getElementById('docDupApplyBtn');
    if (apply) { apply.disabled = true; apply.textContent = 'Applying…'; }
    try {
      const r = await fetch('/docs/' + PROJ_ID + '/duplicates/resolve-batch', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ keep, confirm, force, link_mode }),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) { alert('Apply failed: ' + (d.error || r.status)); return; }
      // Reflect results into the rows without a reload.
      (d.results || []).forEach(res => _docApplyResolvedRow(res.upload_id, res.resolved));
      closeDupCompare();
      if (typeof _docsApplyView === 'function') _docsApplyView();
      _docsRefreshDupCounts();
    } catch (e) {
      alert('Apply error: ' + (e && e.message || e));
    } finally {
      if (apply) { apply.textContent = 'Apply decisions'; }
    }
  };

  // Scan for person-doc duplicates (same crew member + same sub-type) and flag
  // them for the Compare/dup review. (User 2026-06-02.)
  window.docsScanPersonDuplicates = async function() {
    try {
      const dry = await (await fetch('/docs/' + PROJ_ID + '/scan-person-duplicates',
        { method:'POST', headers:{'Content-Type':'application/json'}, body:'{}' })).json();
      const n = dry.flagged || 0;
      if (!n) { alert('No duplicate person docs found (same person + same type).'); return; }
      if (!confirm('Found ' + n + ' duplicate person doc' + (n!==1?'s':'') + ' (a person already has that type). Flag them for review? You can then Compare and file each as a duplicate or keep both.')) return;
      const d = await (await fetch('/docs/' + PROJ_ID + '/scan-person-duplicates?apply=1',
        { method:'POST', headers:{'Content-Type':'application/json'}, body:'{}' })).json();
      alert('Flagged ' + (d.flagged||0) + ' for review. Reloading the Docs Review tab…');
      location.reload();
    } catch(e) { alert('Scan failed: ' + e.message); }
  };

  // Bulk: confirm EVERY still-pending flagged duplicate at once (the
  // unflagged original of each group is kept automatically). The "move all
  // → /_DUPLICATES/" button in the Review toolbar. 2026-05-29.
  window.docsConfirmAllDuplicates = async function() {
    const pending = Array.from(document.querySelectorAll('#docsHistory .doc-row'))
      .filter(r => r.dataset.isDuplicate === '1' && (r.dataset.status || '') !== 'duplicate')
      .map(r => parseInt(r.dataset.uploadId));
    if (!pending.length) { alert('No flagged duplicates to move.'); return; }
    if (!confirm('Move ' + pending.length + ' flagged duplicate' + (pending.length === 1 ? '' : 's')
                 + ' to /_DUPLICATES/?\n\nThe original copy of each is kept automatically. '
                 + 'You can still un-flag any from the Duplicates tab afterward.')) return;
    try {
      const r = await fetch('/docs/' + PROJ_ID + '/duplicates/resolve-batch', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ keep: [], confirm: pending }),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) { alert('Failed: ' + (d.error || r.status)); return; }
      (d.results || []).forEach(res => _docApplyResolvedRow(res.upload_id, res.resolved));
      _docsRefreshDupCounts();
      if (typeof _docsApplyView === 'function') _docsApplyView();
    } catch (e) {
      alert('Error: ' + (e && e.message || e));
    }
  };

  // Apply a resolution outcome to a single row's DOM state.
  function _docApplyResolvedRow(uid, resolved) {
    const row = document.querySelector('.doc-row[data-upload-id="' + uid + '"]');
    if (!row) return;
    const badge   = row.querySelector('.doc-status-badge');
    const cluster = badge ? badge.parentElement : null;
    if (cluster) cluster.querySelectorAll(
      '.doc-dup-keep-btn, .doc-dup-confirm-btn, .doc-dup-compare-btn').forEach(b => b.remove());
    if (resolved === 'keep') {
      row.dataset.isDuplicate = '';
      row.dataset.dupPending  = '';
      if (badge) {
        badge.textContent = '✓';
        badge.style.cssText = 'font-size:11px;padding:3px 9px;border-radius:20px;background:#14291e;color:#22c55e;border:1px solid #1a4228;';
      }
    } else {  // confirm
      row.dataset.status      = 'duplicate';
      row.dataset.dupPending  = '';
      if (badge) {
        badge.textContent = '🔁 Dup';
        badge.style.cssText = 'font-size:11px;padding:3px 9px;border-radius:20px;background:#1e1a2a;color:#a78bfa;border:1px solid #2d2a40;';
      }
    }
  }

  // Recompute the Review + Duplicates subtab counts after a resolve, and
  // hide the Review toolbar / re-home the user if Review just emptied out.
  function _docsRefreshDupCounts() {
    const rows = Array.from(document.querySelectorAll('#docsHistory .doc-row'));
    let reviewN = 0, pendingDup = 0, confirmedDup = 0;
    rows.forEach(r => {
      const st = (r.dataset.status || '').toLowerCase();
      const dup = r.dataset.isDuplicate === '1';
      const pend = dup && st !== 'duplicate';
      if (pend) pendingDup++;
      if (st === 'duplicate') confirmedDup++;
      if (st === 'review' || pend) reviewN++;
    });
    const setCount = (tab, n, wrapId) => {
      const btn = document.querySelector('.docs-subtab[data-tab="' + tab + '"]');
      if (!btn) return;
      const c = btn.querySelector('.docs-subtab-count');
      if (c) c.textContent = n;
      // Toggle visibility on the wrapper when one exists (Duplicates tab),
      // otherwise on the button itself (Review tab).
      const el = wrapId ? document.getElementById(wrapId) : btn;
      if (el) el.style.display = (n > 0) ? '' : 'none';
    };
    setCount('review', reviewN);
    setCount('duplicate', confirmedDup, 'docs-dup-tab-wrap');
    const tb = document.getElementById('docs-review-toolbar');
    if (tb && _docsActiveTab === 'review') tb.style.display = pendingDup > 0 ? 'flex' : 'none';
    // If the Review tab just emptied while the user is on it, send them to
    // All so they're not staring at a now-hidden, empty tab.
    if (reviewN === 0 && _docsActiveTab === 'review') {
      _docsSelectTab('all');
    }
  }

  window.closeDupCompare = function(ev) {
    if (ev && ev.target && ev.target.id !== 'docDupCompareOverlay') return;
    const o = document.getElementById('docDupCompareOverlay');
    if (o) o.style.display = 'none';
    const b = document.getElementById('docDupCompareBody');
    if (b) { b.innerHTML = ''; b.dataset.expanded = ''; }   // stop iframe/img loads + reset expand
    _docDupGroupState = {};
  };
  // Esc closes the group-review modal (only when it's open).
  document.addEventListener('keydown', (e) => {
    if (e.key !== 'Escape') return;
    const o = document.getElementById('docDupCompareOverlay');
    if (o && o.style.display !== 'none') { e.preventDefault(); closeDupCompare(); }
  });

  // ── Inline filename rename (click filed filename) ─────────────────────
  document.getElementById('docsHistory').addEventListener('click', (e) => {
    const span = e.target.closest('.editable-filename');
    if (!span || span.classList.contains('editing')) return;
    _docsStartRename(span);
  });

  function _docsStartRename(span) {
    const uploadId = parseInt(span.dataset.id);
    const currentName = span.textContent.trim();
    span.classList.add('editing');
    span.contentEditable = 'plaintext-only';
    span.focus();
    // Select name portion before extension so typing replaces readable
    // text but keeps the extension intact.
    const range = document.createRange();
    const sel = window.getSelection();
    const dot = currentName.lastIndexOf('.');
    const textNode = span.firstChild;
    if (textNode && dot > 0) {
      range.setStart(textNode, 0);
      range.setEnd(textNode, dot);
    } else if (textNode) {
      range.selectNodeContents(textNode);
    }
    sel.removeAllRanges();
    sel.addRange(range);

    const commit = async () => {
      span.contentEditable = 'false';
      span.classList.remove('editing');
      const newName = span.textContent.trim();
      if (!newName || newName === currentName) {
        span.textContent = currentName;
        return;
      }
      if (newName.includes('/') || newName.includes('\\') || newName.length > 200) {
        alert('Invalid filename — no slashes, ≤ 200 chars.');
        span.textContent = currentName;
        return;
      }
      try {
        const r = await fetch('/docs/upload/' + uploadId + '/rename', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ new_filename: newName })
        });
        const d = await r.json();
        if (!r.ok) {
          alert('Rename failed: ' + (d.error || 'unknown'));
          span.textContent = currentName;
          return;
        }
        const finalName = d.new_filename || newName;
        span.textContent = finalName;
        span.dataset.original = finalName;
      } catch (err) {
        alert('Rename request failed: ' + err.message);
        span.textContent = currentName;
      }
    };
    const cancel = () => {
      span.contentEditable = 'false';
      span.classList.remove('editing');
      span.textContent = currentName;
    };
    const onKey = (ev) => {
      if (ev.key === 'Enter') { ev.preventDefault(); span.removeEventListener('keydown', onKey); span.removeEventListener('blur', onBlur); commit(); }
      else if (ev.key === 'Escape') { ev.preventDefault(); span.removeEventListener('keydown', onKey); span.removeEventListener('blur', onBlur); cancel(); }
    };
    const onBlur = () => {
      span.removeEventListener('keydown', onKey);
      span.removeEventListener('blur', onBlur);
      commit();
    };
    span.addEventListener('keydown', onKey);
    span.addEventListener('blur', onBlur);
  }

  // ── Client-side sort of the uploaded-docs list ─────────────────────────
  // Reads data-sort-* attributes on each .doc-row and reorders the DOM
  // WITHIN each group (so receipts stay with receipts, etc.). Without
  // this, the old "appendChild every row to the bottom" approach moved
  // every row past the group headers and broke the grouped layout.
  function _docsSort() {
    const by  = document.getElementById('docs-sort-by');
    const dir = document.getElementById('docs-sort-dir');
    if (!by || !dir) return;
    const key    = by.value;
    const desc   = dir.dataset.dir === 'desc';
    const NUMERIC = new Set(['amount', 'size', 'confidence']);
    // ISO-string keys: lexicographic sort gives correct chronological order.
    const ISO_DATE = new Set(['date', 'doc-date']);
    const list   = document.getElementById('docsHistory');
    if (!list) return;

    // Convert kebab-case sort key to the camelCased dataset property.
    // 'doc-date'  → 'sortDocDate'   (data-sort-doc-date)
    // 'vendor'    → 'sortVendor'    (data-sort-vendor)
    function _datasetKey(k) {
      const camel = k.split('-').map((p, i) =>
        i === 0 ? p : p.charAt(0).toUpperCase() + p.slice(1)).join('');
      return 'sort' + camel.charAt(0).toUpperCase() + camel.slice(1);
    }

    function _cmp(a, b) {
      const dsKey = _datasetKey(key);
      const av = a.dataset[dsKey] || '';
      const bv = b.dataset[dsKey] || '';
      let c;
      if (NUMERIC.has(key))           c = (parseFloat(av) || 0) - (parseFloat(bv) || 0);
      else if (ISO_DATE.has(key))     c = av < bv ? -1 : av > bv ? 1 : 0;
      else                            c = av.localeCompare(bv);
      return desc ? -c : c;
    }

    // Walk the children, partitioning into groups by .doc-group-header
    // boundaries. Each group sorts its own rows and re-appends in order.
    const groups = [];
    let current = { header: null, rows: [] };
    Array.from(list.children).forEach(node => {
      if (node.classList && node.classList.contains('doc-group-header')) {
        if (current.header || current.rows.length) groups.push(current);
        current = { header: node, rows: [] };
      } else if (node.classList && node.classList.contains('doc-row')) {
        current.rows.push(node);
      }
    });
    if (current.header || current.rows.length) groups.push(current);

    groups.forEach(g => g.rows.sort(_cmp));
    // Re-append in group order: header, then its sorted rows.
    groups.forEach(g => {
      if (g.header) list.appendChild(g.header);
      g.rows.forEach(r => list.appendChild(r));
    });
  }

  // ── Collapsible group sections ─────────────────────────────────────
  // Click any .doc-group-header to toggle its rows. Rows belonging to
  // a header are every .doc-row sibling that follows, up to the next
  // .doc-group-header.
  function _docsBindCollapsibleHeaders() {
    const list = document.getElementById('docsHistory');
    if (!list) return;
    list.querySelectorAll('.doc-group-header').forEach(h => {
      // Inject a chevron once.
      if (!h.querySelector('.doc-group-chevron')) {
        const chev = document.createElement('span');
        chev.className = 'doc-group-chevron';
        chev.style.cssText = 'margin-left:auto;font-size:12px;color:var(--text-muted);transition:transform .12s;user-select:none;';
        chev.textContent = '▾';
        h.appendChild(chev);
        h.style.cursor = 'pointer';
        h.title = 'Click to collapse / expand this section';
      }
      h.onclick = () => {
        const collapsed = h.dataset.collapsed === '1';
        h.dataset.collapsed = collapsed ? '' : '1';
        const chev = h.querySelector('.doc-group-chevron');
        if (chev) chev.style.transform = collapsed ? '' : 'rotate(-90deg)';
        // Defer actual row visibility to _docsApplyView so collapse state
        // composes with the active subtab + search instead of overriding
        // them (a collapsed group inside a filtered view must stay hidden).
        if (typeof _docsApplyView === 'function') _docsApplyView();
      };
    });
  }
  // Bind on initial render.
  _docsBindCollapsibleHeaders();

  // Wire up the sort dropdown + direction toggle.
  const _sortBy  = document.getElementById('docs-sort-by');
  const _sortDir = document.getElementById('docs-sort-dir');
  if (_sortBy && _sortDir) {
    // Labels shown on the direction toggle per column (makes intent clear —
    // "Newest first" is way more scannable than "↓ desc" for date).
    const DIR_LABELS = {
      vendor:     { desc: '↓ Z → A',          asc: '↑ A → Z' },
      'doc-date': { desc: '↓ Newest first',  asc: '↑ Oldest first' },
      amount:     { desc: '↓ Largest first', asc: '↑ Smallest first' },
      confidence: { desc: '↓ Highest first',  asc: '↑ Lowest first' },
      date:       { desc: '↓ Newest first',  asc: '↑ Oldest first' },
      uploader:   { desc: '↓ Z → A',          asc: '↑ A → Z' },
      filename:   { desc: '↓ Z → A',          asc: '↑ A → Z' },
      original:   { desc: '↓ Z → A',          asc: '↑ A → Z' },
      type:       { desc: '↓ Z → A',          asc: '↑ A → Z' },
      size:       { desc: '↓ Largest first', asc: '↑ Smallest first' },
      status:     { desc: '↓ Z → A',          asc: '↑ A → Z' },
    };
    function _updateDirLabel() {
      const l = DIR_LABELS[_sortBy.value] || { desc: '↓ Desc', asc: '↑ Asc' };
      _sortDir.textContent = _sortDir.dataset.dir === 'desc' ? l.desc : l.asc;
    }
    _sortBy.addEventListener('change', () => { _updateDirLabel(); _docsSort(); });
    _sortDir.addEventListener('click', () => {
      _sortDir.dataset.dir = _sortDir.dataset.dir === 'desc' ? 'asc' : 'desc';
      _updateDirLabel();
      _docsSort();
    });
    // Initial sort on page load (default: date desc — matches the server
    // order so the page doesn't visually flicker).
    _updateDirLabel();
    _docsSort();
  }

  // ── Subtabs + search (status/type tabs combined with text filter) ──────
  // Replaces the old flat status chips (per user 2026-05-29). A row is
  // visible when it matches BOTH the active subtab AND the search query;
  // group headers auto-hide when none of their rows match. Collapse state
  // (set on a header by clicking it) is also honored. This single function
  // is the authority on row/header visibility.
  let _docsActiveTab   = 'all';
  let _docsSearchQuery = '';

  function _docsRowMatchesTab(row) {
    const tab = _docsActiveTab;
    // Confirmed duplicates live ONLY in the Duplicates tab — never in 'all' or a
    // category group (user 2026-06-22: a marked dup kept showing in Invoices).
    if ((row.dataset.status || '').toLowerCase() === 'duplicate') return tab === 'duplicate';
    if (tab === 'all') return true;
    const st  = (row.dataset.status || '').toLowerCase();
    const dup = row.dataset.isDuplicate === '1';
    const pendingDup = dup && st !== 'duplicate';   // flagged but not yet confirmed
    // Combined Review tab: low-confidence OCR + still-pending duplicates.
    if (tab === 'review')     return st === 'review' || pendingDup;
    if (tab === 'processing') return st === 'processing';
    if (tab === 'error')      return st === 'error';
    // Demoted Duplicates tab: only CONFIRMED dupes (already in /_DUPLICATES).
    if (tab === 'duplicate')  return st === 'duplicate';
    if (tab.indexOf('type:') === 0) return (row.dataset.groupKey || '') === tab.slice(5);
    return true;
  }
  function _docsRowMatchesSearch(row) {
    if (!_docsSearchQuery) return true;
    const hay = [
      row.dataset.sortVendor   || '',
      row.dataset.sortFilename || '',
      row.dataset.sortOriginal || '',
      (row.dataset.note    || '').toLowerCase(),
      (row.dataset.docNum  || '').toLowerCase(),
      (row.dataset.sortCard4 || '').toLowerCase(),
    ].join(' ');
    return hay.indexOf(_docsSearchQuery) !== -1;
  }
  // Collapse byte-identical duplicate sets into ONE row + an expander, so a
  // receipt uploaded 3× reads as a single item with its copies tucked under it
  // instead of N separate rows. Expand-only: each copy keeps its own Keep /
  // It's-a-dupe actions. Review tab only. (User 2026-06-17.)
  let _dupGroupsExpanded = {};   // file_hash -> bool
  function _docsCollapseDupGroups() {
    const list = document.getElementById('docsHistory');
    if (!list) return;
    list.querySelectorAll('.dup-group-bar').forEach(n => n.remove());
    if (_docsActiveTab !== 'review') return;
    const byHash = {};
    Array.from(list.querySelectorAll('.doc-row')).forEach(r => {
      const h = r.dataset.fileHash; if (h) (byHash[h] = byHash[h] || []).push(r);
    });
    Object.keys(byHash).forEach(h => {
      const members = byHash[h];
      if (members.length < 2) return;                                   // not a set
      if (!members.some(m => m.dataset.dupPending === '1')) return;     // no pending dup
      if (!members.some(m => m.style.display !== 'none')) return;       // nothing visible
      const rep = members.find(m => m.dataset.isDuplicate !== '1') || members[0];  // original heads the set
      const others = members.filter(m => m !== rep);
      const expanded = !!_dupGroupsExpanded[h];
      rep.style.display = '';
      others.forEach(m => { m.style.display = expanded ? '' : 'none'; });
      const v = (rep.dataset.sortVendor || 'receipt').replace(/</g, '&lt;');
      const a = rep.dataset.sortAmount;
      const bar = document.createElement('div');
      bar.className = 'dup-group-bar';
      bar.style.cssText = 'display:flex;align-items:center;gap:8px;margin:6px 0 0;padding:5px 11px;font-size:11px;color:#a78bfa;background:#1a1426;border:1px solid #2d2440;border-radius:6px;cursor:pointer';
      bar.innerHTML = '<span>📑 Duplicate set · ' + members.length + ' identical copies · ' + v +
        (a ? (' · $' + (+a).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2})) : '') +
        '</span><span style="margin-left:auto">' + (expanded ? '▾ collapse' : '▸ show all ' + members.length) + '</span>';
      bar.onclick = () => { _dupGroupsExpanded[h] = !_dupGroupsExpanded[h]; _docsApplyView(); };
      rep.parentNode.insertBefore(bar, rep);
    });
  }
  function _docsApplyView() {
    const list = document.getElementById('docsHistory');
    if (!list) return;
    let curHeader = null, curCollapsed = false, curMatched = 0, visible = 0;
    const finalizeHeader = () => {
      // A header shows whenever ≥1 of its rows matches (even if collapsed,
      // so the user can still expand it). Hide it when its whole group is
      // filtered out — e.g. on a single-type subtab every other header.
      if (curHeader) curHeader.style.display = curMatched > 0 ? '' : 'none';
    };
    Array.from(list.children).forEach(node => {
      if (node.classList && node.classList.contains('doc-group-header')) {
        finalizeHeader();
        curHeader    = node;
        curCollapsed = node.dataset.collapsed === '1';
        curMatched   = 0;
      } else if (node.classList && node.classList.contains('doc-row')) {
        const m = _docsRowMatchesTab(node) && _docsRowMatchesSearch(node);
        if (m) { curMatched++; visible++; }
        node.style.display = (m && !curCollapsed) ? '' : 'none';
        if (!m) {
          const cb = node.querySelector('.doc-select-cb');
          if (cb && cb.checked) cb.checked = false;
        }
      }
    });
    finalizeHeader();
    _docsCollapseDupGroups();   // fold duplicate sets into one expandable row
    const count = document.getElementById('docs-count');
    if (count) {
      const vis = Array.from(list.querySelectorAll('.doc-row')).filter(r => r.style.display !== 'none').length;
      count.textContent = `${vis} item${vis === 1 ? '' : 's'}`;
    }
    _docsUpdateBulkBar();
  }
  function _docsSelectTab(tab) {
    _docsActiveTab = tab;
    document.querySelectorAll('.docs-subtab').forEach(btn => {
      const active = btn.dataset.tab === tab;
      btn.style.borderBottomColor = active ? '#5b8af9' : 'transparent';
      btn.style.color             = active ? 'var(--text)' : 'var(--text-muted)';
      btn.style.fontWeight        = active ? '600' : 'normal';
      const c = btn.querySelector('.docs-subtab-count');
      if (c) {
        c.style.background = active ? 'rgba(91,138,249,.18)' : 'var(--bg-input)';
        c.style.color      = active ? '#5b8af9' : 'var(--text-muted)';
      }
    });
    // Review-tab bulk toolbar only shows on the Review tab.
    const rt = document.getElementById('docs-review-toolbar');
    if (rt) rt.style.display = (tab === 'review') ? 'flex' : 'none';
    _docsApplyView();
  }
  window._docsOnSearch = function(q) {
    _docsSearchQuery = (q || '').trim().toLowerCase();
    _docsApplyView();
  };
  document.querySelectorAll('.docs-subtab').forEach(btn => {
    btn.addEventListener('click', () => _docsSelectTab(btn.dataset.tab));
  });
  // Landing tab: Needs Review when it exists, else All. Mirrors the
  // server-side "review tab only rendered when count>0" rule.
  //
  // BUT if the user just came back from the standalone doc editor (or closed
  // it), restore the exact list view they left — subtab, search text, sort,
  // and scroll position — from sessionStorage. (User 2026-07 — "take me back
  // to exactly where I was in the list before with the same filtering.") The
  // state is only honored when we're actually on the docs tab, is fresh
  // (<10 min), then consumed (removed) so a later plain visit lands normally.
  (function _docsInitTab(){
    if (typeof _docsRefreshDupCounts === 'function') _docsRefreshDupCounts();
    let restored = false;
    try {
      const onDocsTab = new URLSearchParams(location.search).get('tab') === 'docs';
      const raw = sessionStorage.getItem('fpDocsListState');
      if (onDocsTab && raw) {
        const s = JSON.parse(raw);
        if (s && s.ts && (Date.now() - s.ts) < 600000) {   // 10 minutes
          restored = true;
          // 1) Subtab — only if the button still exists (review tab may be gone).
          if (s.tab && document.querySelector('.docs-subtab[data-tab="' + s.tab + '"]')) {
            _docsSelectTab(s.tab);
          } else {
            const hasReview = !!document.querySelector('.docs-subtab[data-tab="review"]');
            _docsSelectTab(hasReview ? 'review' : 'all');
          }
          // 2) Search text — set the input and run the same filter path.
          const searchEl = document.getElementById('docs-search');
          if (searchEl) { searchEl.value = s.search || ''; }
          if (typeof _docsOnSearch === 'function') _docsOnSearch(s.search || '');
          // 3) Sort — restore column + direction, then re-run the client sort.
          const sortBy  = document.getElementById('docs-sort-by');
          const sortDir = document.getElementById('docs-sort-dir');
          if (s.sort && sortBy && sortDir) {
            if (s.sort.by)  sortBy.value = s.sort.by;
            if (s.sort.dir) sortDir.dataset.dir = s.sort.dir;
            sortBy.dispatchEvent(new Event('change'));   // updates label + sorts
          }
          // 4) Scroll — after layout settles.
          const y = s.scroll || 0;
          setTimeout(() => { try { window.scrollTo(0, y); } catch (e) {} }, 60);
        }
      }
      // Consume the key so a normal (non-return) visit lands on the default tab.
      if (raw) sessionStorage.removeItem('fpDocsListState');
    } catch (e) { restored = false; }
    if (!restored) {
      const hasReview = !!document.querySelector('.docs-subtab[data-tab="review"]');
      _docsSelectTab(hasReview ? 'review' : 'all');
    }
  })();

  // ── Bulk-select ────────────────────────────────────────────────────
  function _docsUpdateBulkBar() {
    const checked = Array.from(document.querySelectorAll('.doc-select-cb:checked'));
    const bar     = document.getElementById('docs-bulk-bar');
    const count   = document.getElementById('docs-bulk-count');
    if (!bar) return;
    if (checked.length) {
      bar.style.display = 'flex';
      count.textContent = `${checked.length} selected`;
    } else {
      bar.style.display = 'none';
    }
    // Sync the master checkbox state — checked = all visible selected.
    const visibleCbs = Array.from(document.querySelectorAll('#docsHistory .doc-row'))
      .filter(r => r.style.display !== 'none')
      .map(r => r.querySelector('.doc-select-cb'))
      .filter(Boolean);
    const master = document.getElementById('docs-select-all');
    if (master && visibleCbs.length) {
      const allChecked = visibleCbs.every(cb => cb.checked);
      const someChecked = visibleCbs.some(cb => cb.checked);
      master.checked = allChecked;
      master.indeterminate = !allChecked && someChecked;
    }
  }
  document.querySelectorAll('.doc-select-cb').forEach(cb =>
    cb.addEventListener('change', _docsUpdateBulkBar));
  const _docsMasterCb = document.getElementById('docs-select-all');
  if (_docsMasterCb) {
    _docsMasterCb.addEventListener('change', () => {
      const visible = Array.from(document.querySelectorAll('#docsHistory .doc-row'))
        .filter(r => r.style.display !== 'none');
      visible.forEach(row => {
        const cb = row.querySelector('.doc-select-cb');
        if (cb) cb.checked = _docsMasterCb.checked;
      });
      _docsUpdateBulkBar();
    });
  }
  window.docsBulkClear = function() {
    document.querySelectorAll('.doc-select-cb').forEach(cb => cb.checked = false);
    _docsUpdateBulkBar();
  };
  window.docsBulkDelete = async function() {
    const checked = Array.from(document.querySelectorAll('.doc-select-cb:checked'));
    if (!checked.length) return;
    const ids = checked.map(cb => parseInt(cb.dataset.id));
    if (!confirm(`Delete ${ids.length} document${ids.length === 1 ? '' : 's'}?\nThis removes them from the project AND moves them to _TRASH in Dropbox.`)) return;
    try {
      const r = await fetch(`/docs/${PROJ_ID}/bulk-delete`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ ids })
      });
      const data = await r.json();
      if (!r.ok) {
        alert('Bulk delete failed: ' + (data.error || `HTTP ${r.status}`));
        return;
      }
      // Remove deleted rows from the DOM. Skipped rows (not yours) stay.
      ids.slice(0, data.deleted).forEach(id => {
        const row = document.querySelector(`.doc-row[data-upload-id="${id}"]`);
        if (row) row.remove();
      });
      docsBulkClear();
      // Update count
      const count = document.getElementById('docs-count');
      const remaining = document.querySelectorAll('#docsHistory .doc-row').length;
      if (count) count.textContent = `${remaining} item${remaining === 1 ? '' : 's'}`;
      if (data.skipped) {
        alert(`Deleted ${data.deleted}. ${data.skipped} skipped (uploaded by someone else — admin required).`);
      }
    } catch (e) {
      alert('Bulk delete failed: ' + (e.message || e));
    }
  };

  // ── Super-admin wipe button ──────────────────────────────────────────
  const _wipeBtn = document.getElementById('btn-wipe-docs');
  if (_wipeBtn) {
    _wipeBtn.addEventListener('click', async () => {
      if (!confirm("Delete EVERY receipt/doc on this project?\n\n" +
                   "• All DocUpload rows removed from the database\n" +
                   "• Filed Dropbox files moved to /_TRASH/\n\n" +
                   "This is a testing tool. Proceed?")) return;
      if (!confirm("Really sure? Only affects THIS project's docs.")) return;
      _wipeBtn.disabled = true;
      _wipeBtn.textContent = 'Wiping…';
      try {
        const r = await fetch('/admin/docs/project/' + PROJ_ID + '/wipe', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' }
        });
        const d = await r.json();
        if (!r.ok) { alert('Wipe failed: ' + (d.error || 'unknown')); return; }
        let msg = 'Wipe complete.\n\n';
        msg += '• DocUpload rows deleted: ' + d.deleted_count + '\n';
        msg += '• Dropbox files moved to trash: ' + d.moved_count + '\n';
        if (d.error_count) msg += '\nErrors: ' + d.error_count;
        if (d.trash_folder) msg += '\n\nTrash folder: ' + d.trash_folder;
        alert(msg);
        location.reload();
      } catch (e) {
        alert('Request failed: ' + e.message);
      } finally {
        _wipeBtn.disabled = false;
        _wipeBtn.textContent = '🗑 Wipe all receipts (test)';
      }
    });
  }

  // ── Super-admin scan-Dropbox button ────────────────────────────────────
  // Walks the project's Dropbox doc folders and creates DocUpload rows for
  // any files present on Dropbox but missing from the DB. Catches
  // interrupted-upload cases (tab closed before the response landed) and
  // any legacy manually-filed docs. Server-side at
  // /admin/docs/project/<pid>/reconcile; returns counts + sample list.
  const _scanBtn = document.getElementById('btn-scan-docs');
  if (_scanBtn) {
    _scanBtn.addEventListener('click', async () => {
      _scanBtn.disabled = true;
      _scanBtn.textContent = '⏳ Scanning…';
      try {
        const r = await fetch('/admin/docs/project/' + PROJ_ID + '/reconcile', { method: 'POST' });
        const d = await r.json();
        if (!r.ok) { alert('Scan failed: ' + (d.error || 'unknown')); return; }
        let msg = 'Dropbox scan complete.\n\n';
        msg += '• Files scanned: '    + d.scanned  + '\n';
        msg += '• Already tracked: '  + d.existing + '\n';
        msg += '• New rows created: ' + d.created  + '\n';
        if (d.errors && d.errors.length) msg += '\nWarnings: ' + d.errors.length;
        if (d.sample && d.sample.length) {
          msg += '\n\nSample:';
          for (const s of d.sample.slice(0, 8)) msg += '\n  • ' + s.filename + ' (' + (s.doc_type||'?') + ')';
        }
        alert(msg);
        if (d.created > 0) location.reload();
      } catch (e) {
        alert('Request failed: ' + e.message);
      } finally {
        _scanBtn.disabled = false;
        _scanBtn.textContent = '🔍 Scan Dropbox';
      }
    });
  }

  // ── LAZY verification of Filed rows ───────────────────────────────────
  // Previously fired /verify for EVERY filed doc on every page load — on a
  // project with hundreds of docs that's hundreds of Dropbox round-trips,
  // which made the page slow (user 2026-06-01). Now each row is verified
  // against Dropbox only when it scrolls into view on the Docs tab (and at
  // most once). The detail modal still does its own verify on open, so the
  // "missing file" safety net is intact for anything you actually look at.
  async function _verifyOne(row) {
    const uid = row.dataset.uploadId;
    if (!uid || row.dataset.verifyChecked) return;
    row.dataset.verifyChecked = '1';
    try {
      const r = await fetch('/docs/upload/' + uid + '/verify', { credentials: 'same-origin' });
      const d = await r.json().catch(() => ({}));
      if (!r.ok || !d.ok) { delete row.dataset.verifyChecked; return; }  // allow retry
      if (d.verified) {
        const badge = row.querySelector('.doc-status-badge');
        if (badge && !badge.dataset.verifiedMarked) {
          badge.dataset.verifiedMarked = '1';
          badge.title = (badge.title || '') + ' · Verified in Dropbox';
          const dot = document.createElement('span');
          dot.textContent = ' ✓';
          dot.style.cssText = 'opacity:.85;margin-left:2px';
          badge.appendChild(dot);
        }
        row.dataset.verified = '1';
      } else {
        // FILE IS MISSING. Make this LOUD so the user can't miss it.
        row.style.border = '1px solid #ef4444';
        row.style.background = '#2a0d0d';
        row.dataset.verified = '0';
        const badge = row.querySelector('.doc-status-badge');
        if (badge) {
          badge.textContent = '⚠ Missing from Dropbox';
          badge.style.cssText = 'font-size:11px;padding:3px 9px;border-radius:20px;white-space:nowrap;background:#2a1414;color:#ef4444;border:1px solid #ef4444;font-weight:600;';
          badge.title = 'This file is not at the recorded Dropbox path. Possible causes: ' +
                        'Dropbox upload silently failed, file was moved/deleted in Dropbox, ' +
                        'or the recorded path is stale. ' + (d.reason || '');
        }
      }
    } catch (e) {
      delete row.dataset.verifyChecked;   // network failure — allow retry later
    }
  }

  let _docsVerifyObserver = null;
  function _docsLazyVerifyInit() {
    const rows = Array.from(document.querySelectorAll('#docsHistory .doc-row')).filter(r => {
      const s = (r.dataset.status || '').toLowerCase();
      return (s === 'filed' || s === 'done') && !r.dataset.verifyChecked && !r.dataset.verifyObserved;
    });
    if (!rows.length) return;
    if (!('IntersectionObserver' in window)) {
      // No IO support — just verify the rows that are actually on screen now.
      rows.filter(r => r.offsetParent !== null).slice(0, 40).forEach(_verifyOne);
      return;
    }
    if (!_docsVerifyObserver) {
      _docsVerifyObserver = new IntersectionObserver((entries) => {
        entries.forEach(e => {
          if (e.isIntersecting) {
            _docsVerifyObserver.unobserve(e.target);
            _verifyOne(e.target);
          }
        });
      }, { rootMargin: '250px' });
    }
    rows.forEach(r => { r.dataset.verifyObserved = '1'; _docsVerifyObserver.observe(r); });
  }
  // Kick off ONLY when the Docs tab is opened (not on every page load), and
  // re-run after view changes so rows revealed by a subtab/search get picked
  // up. Each call only observes still-unobserved filed rows (idempotent).
  window._docsLazyVerifyInit = _docsLazyVerifyInit;
  (function _wireDocsLazyVerify() {
    const btn = document.querySelector('[data-tab="docs"]');
    if (btn) btn.addEventListener('click', () => setTimeout(_docsLazyVerifyInit, 250));
    const initIfActive = () => {
      const panel = document.getElementById('tab-docs');
      if (panel && panel.classList.contains('active')) setTimeout(_docsLazyVerifyInit, 600);
    };
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', initIfActive);
    else initIfActive();
  })();

  // ── Document detail modal ────────────────────────────────────────────
  // Click any non-edit, non-delete area of a doc-row to open. Loads a
  // Dropbox temporary link and the editable metadata.
  let _docDetailUid = null;

  // ── Receipt-side transaction matcher (User 2026-06-17) ──────────────
  // From the open receipt, find the charge(s) it backs — one 1:1, or several
  // that sum to the total (a split). Reuses link-doc/confirm-match + link-split.
  window.docDetailFindTxns = async function () {
    const uid = _docDetailUid;
    if (!uid) return;
    const host = document.getElementById('docDetailTxnResults');
    const inclMatched = document.getElementById('docDetailInclMatched').checked;
    host.innerHTML = '<div style="font-size:11px;color:var(--text-muted)">Searching…</div>';
    try {
      const d = await (await fetch(`/projects/${PROJ_ID}/docs/upload/${uid}/transaction-candidates?include_matched=${inclMatched ? 1 : 0}`)).json();
      window._ddTxnDoc = d.doc || {}; window._ddTxnCands = d.candidates || [];
      ddRenderTxns();
    } catch (e) { host.innerHTML = '<div style="color:#e08080;font-size:11px">Error: ' + e.message + '</div>'; }
  };
  function ddRenderTxns() {
    const host = document.getElementById('docDetailTxnResults');
    const cands = window._ddTxnCands || [];
    const doc = window._ddTxnDoc || {};
    const esc = s => (s || '').toString().replace(/</g, '&lt;');
    // Signed display: refund/credit (negative) shows green "+$x · refund",
    // a charge (positive) shows "−$x · charge", so the user can verify they're
    // matching a refund to a refund (not a refund receipt to a positive charge).
    const moneyAbs = a => '$' + Math.abs(Number(a)).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2});
    const signTag = a => {
      const n = Number(a);
      if (!isFinite(n)) return '<span style="color:var(--text-muted)">$?</span>';
      return n < 0
        ? '<span style="color:#5fd0a0;font-weight:600">+' + moneyAbs(n) + '</span> <span style="color:#5fd0a0">refund</span>'
        : '<span style="font-weight:600">−' + moneyAbs(n) + '</span> <span style="color:var(--text-muted)">charge</span>';
    };
    if (!cands.length) {
      host.innerHTML = '<div style="font-size:11px;color:var(--text-muted)">No candidate charges' +
        (document.getElementById('docDetailInclMatched').checked ? '' : ' — try “include already-matched”') + '.</div>';
      return;
    }
    // This receipt's own sign — refund (<0) vs charge (>0) — to flag mismatches.
    const docSign = (doc.total_signed != null && Number(doc.total_signed) !== 0)
      ? Math.sign(Number(doc.total_signed)) : 0;
    const docKind = docSign < 0 ? 'refund' : (docSign > 0 ? 'charge' : null);
    const rows = cands.map((c, i) => {
      const cSign = (c.amount != null && Number(c.amount) !== 0) ? Math.sign(Number(c.amount)) : 0;
      const mismatch = (docSign !== 0 && cSign !== 0 && docSign !== cSign);
      const matched = (c.match_status !== 'unmatched' || c.doc_upload_id);
      const details =
        '<div id="dd-txn-det-' + i + '" style="display:none;margin-top:5px;padding:6px 8px;background:var(--bg-subtle,rgba(255,255,255,.03));border-radius:6px;font-size:11px;line-height:1.6;color:var(--text-muted)">'
        + '<div><b style="color:var(--text)">' + (esc(c.vendor) || 'charge') + '</b></div>'
        + '<div>Amount: ' + signTag(c.amount) + ' &nbsp;<span style="opacity:.7">(raw ' + Number(c.amount).toFixed(2) + ')</span></div>'
        + '<div>Date: ' + (c.date || '—') + (c.day_gap != null ? ' · ' + c.day_gap + 'd from receipt' : '') + '</div>'
        + '<div>Card: ' + (c.card ? ('••' + c.card) : '—') + (c.same_card ? ' <span style="color:#5fd0a0">· same card ✓</span>' : '') + '</div>'
        + '<div>Coded: ' + (c.coded ? esc(c.coded) : '<span style="opacity:.7">uncoded</span>') + '</div>'
        + '<div>Vendor match: ' + (c.vendor_match ? '<span style="color:#5fd0a0">yes</span>' : 'weak') + ' · Kind: ' + c.kind + '</div>'
        + '<div>Status: ' + esc(c.match_status || 'unmatched') + (matched ? ' <span style="color:#e0c060">· already linked to a receipt</span>' : '') + '</div>'
        + (mismatch ? '<div style="color:#e08a6a;margin-top:3px">⚠ This is a <b>' + (cSign < 0 ? 'refund' : 'charge') + '</b> but the receipt is a <b>' + docKind + '</b> — signs disagree.</div>' : '')
        + '</div>';
      return `
      <div style="border:1px solid ${mismatch ? '#7a4a3a' : 'var(--border)'};border-radius:6px;margin-bottom:5px;padding:6px 8px;font-size:11.5px">
        <label style="display:flex;align-items:flex-start;gap:8px;cursor:pointer">
          <input type="checkbox" class="dd-txn-cb" value="${c.tid}" data-amt="${c.amount}" onchange="ddTxnSelChanged()" style="margin-top:2px">
          <span style="flex:1;min-width:0">
            <span style="font-weight:600">${esc(c.vendor) || 'charge'}</span>
            ${c.kind === 'exact' ? '<span style="color:#5fd0a0;margin-left:5px">exact</span>' : '<span style="color:var(--text-muted);margin-left:5px">part</span>'}
            ${matched ? '<span style="color:#e0c060;margin-left:5px" title="already linked to a receipt">matched</span>' : ''}
            ${mismatch ? '<span style="color:#e08a6a;margin-left:5px" title="sign mismatch">⚠ sign</span>' : ''}
            <br>
            <span style="display:inline-block;margin-top:2px">${signTag(c.amount)}<span style="color:var(--text-muted)"> · ${c.date || '—'}${c.card ? (' · ••' + c.card) : ''}${c.coded ? (' · ' + esc(c.coded)) : ''}</span></span>
          </span>
          <button type="button" onclick="event.preventDefault();event.stopPropagation();ddTxnToggleDet(${i})" style="flex:none;padding:1px 7px;border:1px solid var(--border);border-radius:5px;background:transparent;color:var(--text-muted);font-size:11px;cursor:pointer">details</button>
        </label>
        ${details}
      </div>`;
    }).join('');
    const docHdr = docKind
      ? '<div style="font-size:11px;color:var(--text-muted);margin-bottom:6px">This receipt is a <b style="color:' + (docSign < 0 ? '#5fd0a0' : 'var(--text)') + '">' + docKind + '</b> for ' + moneyAbs(doc.total_signed) + '. Matching charges should also be a <b>' + docKind + '</b>.</div>'
      : '';
    host.innerHTML = docHdr + rows
      + '<div id="dd-txn-sum" style="font-size:11px;color:var(--text-muted);margin:4px 0"></div>'
      + '<button type="button" id="dd-txn-link" onclick="ddLinkTxns()" disabled style="padding:5px 11px;border-radius:6px;background:#1f6f4a;border:1px solid #2a8a5e;color:#fff;font-size:12px;cursor:pointer;opacity:.5">Link selected</button>';
    ddTxnSelChanged();
  }
  window.ddTxnToggleDet = function (i) {
    const el = document.getElementById('dd-txn-det-' + i);
    if (el) el.style.display = (el.style.display === 'none' || !el.style.display) ? 'block' : 'none';
  };
  window.ddTxnSelChanged = function () {
    const cbs = [...document.querySelectorAll('.dd-txn-cb:checked')];
    const sum = cbs.reduce((a, cb) => a + Math.abs(parseFloat(cb.dataset.amt) || 0), 0);
    const total = Math.abs((window._ddTxnDoc || {}).total || 0);
    const sumEl = document.getElementById('dd-txn-sum');
    const btn = document.getElementById('dd-txn-link');
    if (!cbs.length) { if (sumEl) sumEl.textContent = ''; if (btn) { btn.disabled = true; btn.style.opacity = '.5'; btn.textContent = 'Link selected'; } return; }
    const match = total && Math.abs(sum - total) <= 0.02;
    if (sumEl) sumEl.innerHTML = cbs.length + ' selected · sum $' + sum.toFixed(2) + ' / receipt $' + total.toFixed(2) +
      (match ? ' <span style="color:#5fd0a0">✓ matches</span>' : ' <span style="color:#e0c060">≠ total</span>');
    if (btn) {
      const ok = (cbs.length === 1) || match;   // single = 1:1; multiple must sum to the total
      btn.disabled = !ok; btn.style.opacity = ok ? '1' : '.5';
      btn.textContent = cbs.length >= 2 ? ('Link as split (' + cbs.length + ')') : 'Link selected';
    }
  };
  window.ddLinkTxns = async function () {
    const uid = _docDetailUid;
    const tids = [...document.querySelectorAll('.dd-txn-cb:checked')].map(cb => parseInt(cb.value));
    if (!uid || !tids.length) return;
    const btn = document.getElementById('dd-txn-link'); if (btn) { btn.disabled = true; btn.textContent = '…'; }
    try {
      if (tids.length >= 2) {
        const r = await fetch(`/projects/${PROJ_ID}/actuals/link-split`, {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({keeper_doc_id: parseInt(uid), charge_tids: tids})});
        const j = await r.json().catch(() => ({}));
        if (!r.ok) { alert('Split link failed: ' + (j.error || r.status)); if (btn) { btn.disabled = false; } return; }
        alert('Linked this receipt to ' + (j.linked || tids.length) + ' charges as a split ✓');
      } else {
        const tid = tids[0];
        let r = await fetch(`/projects/${PROJ_ID}/actuals/transaction/${tid}/link-doc`, {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({doc_upload_id: parseInt(uid)})});
        if (!r.ok) { const j = await r.json().catch(() => ({})); alert('Link failed: ' + (j.error || r.status)); if (btn) { btn.disabled = false; } return; }
        await fetch(`/projects/${PROJ_ID}/actuals/transaction/${tid}/confirm-match`, {method:'POST', headers:{'Content-Type':'application/json'}, body:'{}'});
        alert('Linked ✓');
      }
      docDetailFindTxns();   // refresh the candidate list
    } catch (e) { alert('Error: ' + e.message); if (btn) { btn.disabled = false; } }
  };

  // Re-run OCR on the open document on demand — re-reads vendor/amount/date/
  // currency (incl. foreign → USD) and FILLS the fields for review. Does NOT
  // save; the user clicks Save to keep them. (User 2026-06-17.)
  window.docDetailReocr = async function () {
    const uid = _docDetailUid; if (!uid) return;
    const btn = document.getElementById('docDetailReocrBtn');
    const st  = document.getElementById('docDetailReocrStatus');
    if (btn) { btn.disabled = true; btn.textContent = '🔄 Re-reading…'; }
    if (st)  { st.textContent = 'Sending to OCR…'; st.style.color = 'var(--text-muted)'; }
    try {
      const r = await fetch('/docs/upload/' + uid + '/reocr', {method:'POST', headers:{'Content-Type':'application/json'}, body:'{}'});
      const d = await r.json().catch(() => ({}));
      if (!r.ok) { if (st) { st.textContent = '✕ ' + (d.error || ('HTTP ' + r.status)); st.style.color = '#e08080'; } return; }
      const setIf = (id, val) => { if (val != null && val !== '') { const el = document.getElementById(id); if (el) el.value = val; } };
      if (d.vendor) setIf('docDetailVendor', d.vendor);
      if (d.amount != null) { const el = document.getElementById('docDetailAmount'); if (el) el.value = _fmtMoney(d.amount); }
      if (d.original_amount != null) { const el = document.getElementById('docDetailOrigAmount'); if (el) el.value = _fmtMoney(d.original_amount); }
      setIf('docDetailOrigCurrency', d.original_currency);
      setIf('docDetailDocDate', d.doc_date);
      setIf('docDetailCard4', d.card_last4);
      if (st) {
        st.textContent = d.fx_note ? ('✓ ' + d.fx_note + ' — review & Save') : '✓ Re-read — review the fields, then Save';
        st.style.color = '#5fd0a0';
      }
    } catch (e) { if (st) { st.textContent = '✕ ' + e.message; st.style.color = '#e08080'; } }
    finally { if (btn) { btn.disabled = false; btn.textContent = '🔄 Re-run OCR'; } }
  };

  // Which list the modal was opened from — drives prev/next paging and
  // which row(s) get patched on save ('docs' = Docs tab, 'actuals' =
  // Actuals tab). Set in openDocDetail. (User 2026-05-30.)
  let _docDetailContext = 'docs';
  // The ordered nav sequence + current index, CAPTURED when the modal is opened
  // from a list (not recomputed every click) — so a save that re-renders/re-files
  // the list can't lose our place or dead-end the prev/next buttons. (User
  // 2026-06-22 — prev was tripping out and graying both buttons.)
  let _docNavSeq = [];   // [{uid, row}] in the on-screen order at open time
  let _docNavIdx = -1;
  // Doc categories that are NOT spend events — switching to one of these
  // deletes the linked Actuals transaction server-side, so the row must
  // drop out of the Actuals list on save. Mirrors app.py _NON_LEDGER_CATS.
  const _DOC_NON_LEDGER_CATS = new Set(['tax_form','contract','release','legal',
    'insurance','misc','employee_vendor_doc','estimate','quote','purchase_order']);
  // Snapshot of the editable fields when the modal opened — lets prev/next
  // auto-save only when something actually changed.
  let _docDetailOpenSig = '';
  function _docDetailSig() {
    return ['docDetailVendor','docDetailDocSubtype','docDetailAmount','docDetailOrigAmount',
            'docDetailOrigCurrency','docDetailDocDate','docDetailCategory',
            'docDetailNote','docDetailDocNum','docDetailCard4','docDetailCrewId','docDetailLocationId']
      .map(id => (document.getElementById(id)?.value || '')).join('');
  }
  function _docDetailDirty() { return _docDetailSig() !== _docDetailOpenSig; }

  function _docDetailFmtSize(b) { if (!b) return ''; return b<1048576?(b/1024).toFixed(1)+' KB':(b/1048576).toFixed(1)+' MB'; }

  // Per-type field set: different docs need different metadata. Tax
  // forms have no amount; COIs care about policy# + expiration; AP
  // docs have invoice/PO numbers. We swap labels and toggle visibility
  // on the same physical inputs so the modal stays compact and the
  // server's /update endpoint doesn't need a separate per-type schema.
  // The "Document #" input is stored on the row's `note` field with a
  // type-prefix (e.g. "invoice#: 12345") so no schema change is needed.
  const _DOC_TYPE_FIELD_SPEC = {
    receipt:        { vendor:'Vendor',       amount:true,  date:'Receipt Date',    docnum:null },
    invoice:        { vendor:'Vendor',       amount:true,  date:'Invoice Date',    docnum:'Invoice #', crew:true, location:false },
    // Per-spec flags: crew = shows the Crew Member picker (tax forms,
    // contracts, releases, payroll → backed by a person). location =
    // shows the Location picker (COIs, location releases → backed by
    // a place). Both nullable; user picks if relevant.
    estimate:       { vendor:'Vendor',       amount:true,  date:'Estimate Date',   docnum:'Estimate #',  crew:false, location:false },
    quote:          { vendor:'Vendor',       amount:true,  date:'Quote Date',      docnum:'Quote #',     crew:false, location:false },
    purchase_order: { vendor:'Vendor',       amount:true,  date:'PO Date',         docnum:'PO #',        crew:false, location:false },
    contract:       { vendor:'Counterparty', amount:false, date:'Effective Date',  docnum:'Contract #',  crew:true,  location:false },
    insurance:      { vendor:'Insured / Carrier', amount:false, date:'Expiration Date', docnum:'Policy #', crew:false, location:true  },
    tax_form:       { vendor:'Entity / Filer', amount:false, date:'Tax Year / Date', docnum:'Tax ID (EIN/SSN)', crew:true, location:false },
    payroll:        { vendor:'Vendor / Employee', amount:true, date:'Pay Date',     docnum:'Check / Ref #', crew:true, location:false },
    legal:          { vendor:'Counterparty', amount:false, date:'Doc Date',        docnum:'Reference #', crew:false, location:false },
    release:        { vendor:'Talent / Location', amount:false, date:'Signed Date', docnum:null,         crew:true,  location:true  },
    // Employee/Vendor supporting docs (DTR, ID, W-9…): the PERSON is set via
    // the Crew Member dropdown, so the top text field describes the document
    // itself (its kind/title) rather than a vendor. (2026-05-29.)
    employee_vendor_doc: { vendor:'Document Type', vendorPh:'e.g. Direct Deposit, W-9, ID, DTR…', amount:false, date:'Doc Date', docnum:'Doc # / ID', crew:true, location:false },
    misc:           { vendor:'Vendor',       amount:true,  date:'Doc Date',        docnum:null,          crew:false, location:false },
  };
  // The "Document Type / Vendor" value comes from whichever control is live:
  // the controlled dropdown for supporting docs, else the free-text input.
  function _docDetailVendorValue() {
    const cat = (document.getElementById('docDetailCategory').value || '').toLowerCase();
    if (cat === 'employee_vendor_doc') {
      const s = document.getElementById('docDetailDocSubtype');
      return s ? (s.value || '') : '';
    }
    const i = document.getElementById('docDetailVendor');
    return i ? (i.value || '') : '';
  }
  // Select the matching dropdown option for an existing supporting-doc value;
  // if it's legacy free-text that isn't in the list, inject it so it's kept.
  function _docDetailSetSubtype(val) {
    const s = document.getElementById('docDetailDocSubtype');
    if (!s) return;
    const v = (val || '').trim();
    const inj = s.querySelector('option[data-injected="1"]');
    if (inj) inj.remove();
    let matched = false;
    Array.from(s.options).forEach(o => {
      if (o.value && o.value.toLowerCase() === v.toLowerCase()) matched = true;
    });
    if (v && !matched) {
      const opt = document.createElement('option');
      opt.textContent = v; opt.value = v; opt.dataset.injected = '1';
      s.insertBefore(opt, s.options[1] || null);
    }
    s.value = v || '';
  }
  function _docDetailApplyTypeUI(type) {
    const spec = _DOC_TYPE_FIELD_SPEC[(type || '').toLowerCase()] || _DOC_TYPE_FIELD_SPEC.misc;
    const vL = document.getElementById('docDetailVendorLabel');
    if (vL) vL.textContent = spec.vendor || 'Vendor';
    const vIn = document.getElementById('docDetailVendor');
    if (vIn) vIn.placeholder = spec.vendorPh || 'Vendor name…';
    // Supporting docs use a controlled Document-Type dropdown instead of the
    // free-text vendor input. Toggle which control is visible. The visible
    // one is the source of truth on save (see _docDetailVendorValue).
    const subSel = document.getElementById('docDetailDocSubtype');
    const isEvd = (type || '').toLowerCase() === 'employee_vendor_doc';
    if (subSel && vIn) {
      subSel.style.display = isEvd ? '' : 'none';
      vIn.style.display    = isEvd ? 'none' : '';
    }
    const dL = document.getElementById('docDetailDocDateLabel');
    if (dL) dL.textContent = spec.date || 'Doc Date';
    const amtField = document.getElementById('docDetailAmountField');
    if (amtField) amtField.style.display = spec.amount ? '' : 'none';
    // Card/account last-4 tracks the amount field — a charge has a card.
    const card4Field = document.getElementById('docDetailCard4Field');
    if (card4Field) card4Field.style.display = spec.amount ? '' : 'none';
    const dnField = document.getElementById('docDetailDocNumField');
    const dnLabel = document.getElementById('docDetailDocNumLabel');
    if (spec.docnum) {
      if (dnField) dnField.style.display = '';
      if (dnLabel) dnLabel.textContent = spec.docnum;
    } else {
      if (dnField) dnField.style.display = 'none';
    }
    // Crew + Location pickers — show only when this doc type backs a
    // person or a place. Hidden by default for receipts / invoices.
    const crewField = document.getElementById('docDetailCrewField');
    if (crewField) crewField.style.display = spec.crew ? '' : 'none';
    const locField  = document.getElementById('docDetailLocationField');
    if (locField)  locField.style.display = spec.location ? '' : 'none';
    // PO actions visible only when type is estimate / quote / SOW / PO.
    // Per user 2026-05-05.
    const poActions = document.getElementById('docDetailPoActions');
    if (poActions) {
      const isPoCandidate = ['estimate', 'quote', 'purchase_order'].includes((type || '').toLowerCase());
      poActions.style.display = isPoCandidate ? '' : 'none';
    }
    // Detail-section heading tracks the doc type — an invoice isn't a
    // "receipt". (User 2026-07 — "'receipt details' is confusing because this
    // is an invoice.") Also hide the Tip field for invoices: tipping is a
    // receipt concept, not an invoice one.
    const _t = (type || '').toLowerCase();
    const rdLabel = document.getElementById('docDetailReceiptDetailLabel');
    if (rdLabel) {
      rdLabel.textContent = _t === 'invoice' ? 'Invoice detail'
        : _t === 'receipt' ? 'Receipt detail'
        : 'Document detail';
    }
    const tipWrap = document.getElementById('docDetailTipWrap');
    if (tipWrap) tipWrap.style.display = (_t === 'invoice') ? 'none' : '';
    _docDetailHighlightMissing(spec);
  }
  // Amount field helpers: display with thousand-separators (95,541.47)
  // while keeping the bound value parseable. Stripped on focus so the
  // user can edit the raw number; reformatted on blur. All reads of
  // .value go through _parseMoney before being sent to the server.
  function _fmtMoney(v) {
    if (v === '' || v == null) return '';
    const n = parseFloat(String(v).replace(/,/g, ''));
    if (!isFinite(n)) return '';
    return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }
  function _parseMoney(v) {
    if (v == null) return '';
    const cleaned = String(v).replace(/,/g, '').trim();
    if (cleaned === '') return '';
    const n = parseFloat(cleaned);
    return isFinite(n) ? String(n) : '';
  }
  (function _wireAmountFormatting() {
    const el = document.getElementById('docDetailAmount');
    if (!el) return;
    el.addEventListener('focus', () => { el.value = String(el.value).replace(/,/g, ''); });
    el.addEventListener('blur',  () => { el.value = _fmtMoney(el.value); });
  })();
  // Find the currently-open detail row and route through the existing
  // openCreatePoFromDoc / openAddToPoFromDoc handlers using a synthesized
  // button element with the row's data attributes.
  window.docDetailCreatePo = async function() {
    const uid = window._docDetailCurrentUid;
    if (!uid) { alert('No document loaded'); return; }
    // Persist any pending edits in the modal (vendor / amount / date /
    // category / note) BEFORE opening the create-PO flow, so manual
    // category changes (e.g. user just switched type from "invoice" to
    // "estimate") survive the trip and the file gets re-filed to the
    // right Dropbox folder. Without this, clicking "Create PO" would
    // open the next modal while the doc is still saved as an invoice.
    try {
      const payload = {
        vendor:     document.getElementById('docDetailVendor').value,
        amount:     _parseMoney(document.getElementById('docDetailAmount').value),
        doc_date:   document.getElementById('docDetailDocDate').value,
        category:   document.getElementById('docDetailCategory').value,
        note:       document.getElementById('docDetailNote').value,
        doc_number: document.getElementById('docDetailDocNum').value,
      };
      const r = await fetch('/docs/upload/' + uid + '/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      if (!r.ok) {
        const d = await r.json().catch(() => ({}));
        alert('Could not save document changes: ' + (d.error || r.status));
        return;
      }
    } catch (e) {
      // Non-fatal — proceed to PO modal even if the save fails so the
      // user isn't stuck. They'll see the error in the console.
      console.warn('[docDetailCreatePo] pre-save failed', e);
    }
    const fakeBtn = {
      dataset: {
        uid:     String(uid),
        vendor:  document.getElementById('docDetailVendor').value || '',
        amount:  _parseMoney(document.getElementById('docDetailAmount').value) || '',
        filename: document.getElementById('docDetailTitle')?.textContent || '',
        docDate: document.getElementById('docDetailDocDate').value || '',
      }
    };
    closeDocDetail();
    if (typeof window.openCreatePoFromDoc === 'function') {
      window.openCreatePoFromDoc(fakeBtn);
    }
  };
  window.docDetailAddToPo = function() {
    const uid = window._docDetailCurrentUid;
    if (!uid) { alert('No document loaded'); return; }
    const fakeBtn = {
      dataset: {
        uid:     String(uid),
        vendor:  document.getElementById('docDetailVendor').value || '',
        amount:  _parseMoney(document.getElementById('docDetailAmount').value) || '',
        filename: document.getElementById('docDetailTitle')?.textContent || '',
      }
    };
    closeDocDetail();
    if (typeof window.openAddToPoFromDoc === 'function') {
      window.openAddToPoFromDoc(fakeBtn);
    }
  };

  // For a row that's still in Review status, highlight the required
  // fields that are blank so the user knows what to fill in to push it
  // out of Review. Required = the fields the spec says this type uses
  // (vendor + date always; amount when applicable).
  function _docDetailHighlightMissing(spec) {
    if (!spec) return;
    const row = document.querySelector('.doc-row[data-upload-id="' + _docDetailUid + '"]');
    const isReview = row && row.dataset.status === 'review';
    const yellow = '1px solid #d97706';
    const normal = '';
    const _hl = (id, needed) => {
      const el = document.getElementById(id);
      if (!el) return;
      const empty = !((el.value || '').trim());
      el.style.outline = (isReview && needed && empty) ? yellow : normal;
      el.style.outlineOffset = (isReview && needed && empty) ? '2px' : '';
    };
    _hl('docDetailVendor', true);
    _hl('docDetailDocDate', true);
    _hl('docDetailAmount', !!spec.amount);
    _hl('docDetailDocNum', !!spec.docnum);
  }

  // ── Image preview zoom + pan ───────────────────────────────────────
  // Receipts are unreadable when shrunk to fit a sidebar — this gives
  // the user real zoom controls (− / + / Fit / 1:1), wheel-zoom anchored
  // at the cursor, and click-drag panning when zoomed in. State lives
  // on a single _imgZoom object that's reset every modal open.
  let _imgZoom = null;
  function _initImagePreviewControls() {
    const wrap = document.getElementById('docDetailPreview');
    const img  = document.getElementById('docDetailImg');
    const lbl  = document.getElementById('docDetailZoomLabel');
    if (!wrap || !img) return;
    _imgZoom = { scale: 1, fitScale: 1, naturalW: 0, naturalH: 0 };

    function applyScale() {
      img.style.transform = 'scale(' + _imgZoom.scale + ')';
      img.style.width  = _imgZoom.naturalW + 'px';
      img.style.height = _imgZoom.naturalH + 'px';
      if (lbl) lbl.textContent = Math.round(_imgZoom.scale * 100) + '%';
      img.style.cursor = _imgZoom.scale > _imgZoom.fitScale + 0.001 ? 'grab' : 'default';
    }

    function fit() {
      const cw = wrap.clientWidth, ch = wrap.clientHeight;
      if (!_imgZoom.naturalW || !_imgZoom.naturalH || !cw || !ch) return;
      const sx = cw / _imgZoom.naturalW, sy = ch / _imgZoom.naturalH;
      _imgZoom.fitScale = Math.min(sx, sy, 1);  // never upscale on fit
      _imgZoom.scale = _imgZoom.fitScale;
      applyScale();
    }

    img.onload = function () {
      _imgZoom.naturalW = img.naturalWidth;
      _imgZoom.naturalH = img.naturalHeight;
      fit();
    };
    if (img.complete && img.naturalWidth) {
      _imgZoom.naturalW = img.naturalWidth;
      _imgZoom.naturalH = img.naturalHeight;
      fit();
    }

    // Toolbar buttons.
    function zoomBy(factor, anchor) {
      const before = _imgZoom.scale;
      const after  = Math.max(0.1, Math.min(8, before * factor));
      if (after === before) return;
      // Anchor zoom at the cursor (or center if not provided) so the
      // pixel under the mouse stays put.
      const sl = wrap.scrollLeft, st = wrap.scrollTop;
      const ax = anchor ? anchor.x : wrap.clientWidth / 2;
      const ay = anchor ? anchor.y : wrap.clientHeight / 2;
      _imgZoom.scale = after;
      applyScale();
      const ratio = after / before;
      wrap.scrollLeft = (sl + ax) * ratio - ax;
      wrap.scrollTop  = (st + ay) * ratio - ay;
    }
    document.getElementById('docDetailZoomIn').onclick   = () => zoomBy(1.25);
    document.getElementById('docDetailZoomOut').onclick  = () => zoomBy(1/1.25);
    document.getElementById('docDetailZoomFit').onclick  = () => fit();
    document.getElementById('docDetailZoom100').onclick  = () => {
      _imgZoom.scale = 1; applyScale();
    };

    // Wheel-zoom (Ctrl/Cmd+wheel, or pinch on trackpads — both surface
    // as wheel events with ctrlKey set on macOS).
    wrap.onwheel = function (e) {
      if (!(e.ctrlKey || e.metaKey)) return;  // let plain wheel scroll
      e.preventDefault();
      const rect = wrap.getBoundingClientRect();
      const anchor = { x: e.clientX - rect.left, y: e.clientY - rect.top };
      zoomBy(e.deltaY < 0 ? 1.1 : 1/1.1, anchor);
    };

    // Click-drag panning when zoomed in beyond fit-scale.
    let dragging = false, startX = 0, startY = 0, startSL = 0, startST = 0;
    img.onmousedown = function (e) {
      if (_imgZoom.scale <= _imgZoom.fitScale + 0.001) return;
      dragging = true;
      startX = e.clientX; startY = e.clientY;
      startSL = wrap.scrollLeft; startST = wrap.scrollTop;
      img.style.cursor = 'grabbing';
      e.preventDefault();
    };
    window.addEventListener('mousemove', function (e) {
      if (!dragging) return;
      wrap.scrollLeft = startSL - (e.clientX - startX);
      wrap.scrollTop  = startST - (e.clientY - startY);
    });
    window.addEventListener('mouseup', function () {
      if (!dragging) return;
      dragging = false;
      img.style.cursor = _imgZoom.scale > _imgZoom.fitScale + 0.001 ? 'grab' : 'default';
    });
  }

  // ── Modal prev/next navigation ───────────────────────────────────────
  // Walks the visible .doc-row elements in DOM order — which already
  // reflects the active subtab, search query, and sort — so paging matches
  // exactly what the user filtered to. Collapsed groups (display:none) are
  // naturally skipped. 2026-05-29.
  // The ordered, currently-visible list the modal pages through —
  // normalised to {uid, row}. In the Docs tab that's the filtered
  // .doc-row list; in the Actuals tab it's every visible transaction row
  // that has a doc badge (so prev/next walks the receipts you can edit).
  function _docNavList() {
    if (_docDetailContext === 'actuals') {
      // Rows live in #actuals-sections after sectionize (the default grouped
      // view) — the old #actuals-txn-list-only selector returned [] and froze
      // prev/next. (Review 2026-06-04.)
      return Array.from(document.querySelectorAll('#actuals-sections .actuals-txn-row, #actuals-txn-list .actuals-txn-row'))
        .filter(r => r.style.display !== 'none')
        .map(r => {
          // A doc id can come from the badge (charge rows linked to a receipt) OR
          // the row itself (doc-source / 'receipt to place' rows whose badge is
          // absent). Without the row fallback, opening a receipt to file it left
          // it out of the nav list → prev/next dead-ended. (User 2026-06-22.)
          const b = r.querySelector('.actuals-doc-badge');
          const id = (b && parseInt(b.dataset.docId)) || (r.dataset.docId ? parseInt(r.dataset.docId) : 0);
          return id ? { uid: id, row: r } : null;
        })
        .filter(Boolean);
    }
    return Array.from(document.querySelectorAll('#docsHistory .doc-row'))
      .filter(r => r.style.display !== 'none')
      .map(r => ({ uid: parseInt(r.dataset.uploadId), row: r }));
  }
  // (Re)capture the ordered nav sequence from the current on-screen list and
  // locate the given uid in it. Called when the user opens a doc from a list.
  function _docCaptureNavSeq(uid) {
    _docNavSeq = _docNavList();
    _docNavIdx = _docNavSeq.findIndex(x => x.uid === uid);
  }
  function _docDetailUpdateNav() {
    const pos  = document.getElementById('docDetailNavPos');
    const prev = document.getElementById('docDetailPrevBtn');
    const nxt  = document.getElementById('docDetailNextBtn');
    const setBtn = (b, disabled) => {
      if (!b) return;
      b.disabled = disabled;
      b.style.opacity = disabled ? '.4' : '';
      b.style.cursor  = disabled ? 'not-allowed' : 'pointer';
    };
    const n = _docNavSeq.length;
    if (_docNavIdx === -1 || n === 0) {
      if (pos) pos.textContent = n ? '—' : '';
      // Not part of a list (e.g. opened from the Action Center) → no paging.
      setBtn(prev, true); setBtn(nxt, true);
      return;
    }
    if (pos) pos.textContent = (_docNavIdx + 1) + ' / ' + n;
    setBtn(prev, _docNavIdx <= 0);
    setBtn(nxt, _docNavIdx >= n - 1);
  }
  window.docDetailNav = async function (delta) {
    // Step through the sequence captured at open time — stable across saves that
    // re-render/re-file the list, so prev/next always know where we are and the
    // buttons never dead-end. (User 2026-06-22.)
    if (!_docNavSeq.length || _docNavIdx < 0) { _docDetailUpdateNav(); return; }
    const nextIdx = _docNavIdx + delta;
    const target = _docNavSeq[nextIdx] || null;   // null = at an end
    // Auto-save current edits before paging away — for BOTH prev AND next.
    if (_docDetailDirty()) {
      const pos = document.getElementById('docDetailNavPos');
      if (pos) pos.textContent = 'Saving…';
      try { await _docDetailCommit(); } catch (e) { /* keep paging */ }
    }
    if (!target) { _docDetailUpdateNav(); return; }   // boundary — stay, restore counter
    _docNavIdx = nextIdx;
    // Re-resolve the live row by uid (a save may have replaced the element);
    // fall back to the captured one. Actuals passes null → server fetch.
    let liveRow = null;
    if (_docDetailContext !== 'actuals') {
      liveRow = document.querySelector('.doc-row[data-upload-id="' + target.uid + '"]') || target.row;
    }
    openDocDetail(target.uid, liveRow, true);   // internalNav=true → keep the sequence
  };
  // Keyboard: ←/→ page through, Esc closes — but never while typing in a
  // field (so caret movement / text entry isn't hijacked).
  document.addEventListener('keydown', (e) => {
    const overlay = document.getElementById('docDetailOverlay');
    if (!overlay || overlay.style.display === 'none') return;
    const tag = (document.activeElement && document.activeElement.tagName) || '';
    if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') {
      if (e.key === 'Escape') document.activeElement.blur();
      return;
    }
    if (e.key === 'ArrowLeft')      { e.preventDefault(); docDetailNav(-1); }
    else if (e.key === 'ArrowRight'){ e.preventDefault(); docDetailNav(1); }
    else if (e.key === 'Escape')    { closeDocDetail(); }
  });

  // ── ✈️ Travel intelligence in the doc-detail modal (2026-06-22) ──────────
  let _ddTravel = { uid: null, data: null, people: [], bid: null };

  function _ddTravelCollect() {
    // Pull whatever travel inputs are currently rendered into _ddTravel.data.
    const g = id => { const el = document.getElementById(id); return el ? (el.value || '').trim() : undefined; };
    const d = Object.assign({}, _ddTravel.data || {});
    const k = g('ddTrvKind'); if (k !== undefined) d.kind = k;
    const conf = g('ddTrvConf'); if (conf !== undefined) d.confirmation_no = conf;
    if (g('ddTrvAirline')  !== undefined) d.airline        = g('ddTrvAirline');
    if (g('ddTrvFlightNo') !== undefined) d.flight_no      = g('ddTrvFlightNo');
    if (g('ddTrvDepAir')   !== undefined) d.depart_airport = g('ddTrvDepAir');
    if (g('ddTrvArrAir')   !== undefined) d.arrive_airport = g('ddTrvArrAir');
    if (g('ddTrvHotel')    !== undefined) d.hotel_name     = g('ddTrvHotel');
    if (g('ddTrvCheckIn')  !== undefined) d.check_in       = g('ddTrvCheckIn');
    if (g('ddTrvCheckOut') !== undefined) d.check_out      = g('ddTrvCheckOut');
    if (g('ddTrvRoom')     !== undefined) d.room_type      = g('ddTrvRoom');
    if (g('ddTrvRental')   !== undefined) d.rental_co      = g('ddTrvRental');
    if (g('ddTrvPickup')   !== undefined) d.pickup_location = g('ddTrvPickup');
    if (g('ddTrvDropoff')  !== undefined) d.dropoff_location = g('ddTrvDropoff');
    if (g('ddTrvPhone')    !== undefined) d.contact_phone   = g('ddTrvPhone');
    _ddTravel.data = d;
    return d;
  }

  function _ddTravelRenderFields() {
    const t = _ddTravel.data || {};
    const f = document.getElementById('docDetailTravelFields');
    if (!f) return;
    const kind = t.kind || 'flight';
    const row = (id, label, val) =>
      `<div style="margin-bottom:5px"><label style="font-size:10px;color:var(--text-muted)">${label}</label>` +
      `<input id="${id}" value="${_esc(val || '')}" style="width:100%;font-size:12px"></div>`;
    let html = '';
    if (!t.confirmation_no && !t.kind) {
      html += '<div style="color:var(--text-muted);font-size:11px;margin-bottom:6px">No travel details yet — click <b>Re-extract</b> to read this document.</div>';
    }
    html += `<div style="margin-bottom:5px"><label style="font-size:10px;color:var(--text-muted)">Type</label>` +
      `<select id="ddTrvKind" style="width:100%;font-size:12px">` +
      `<option value="flight"${kind==='flight'?' selected':''}>✈️ Flight</option>` +
      `<option value="hotel"${kind==='hotel'?' selected':''}>🏨 Hotel</option>` +
      `<option value="car_rental"${kind==='car_rental'?' selected':''}>🚗 Car rental</option>` +
      `<option value="car_service"${kind==='car_service'?' selected':''}>🚐 Car service</option></select></div>`;
    html += row('ddTrvConf', 'Confirmation #', t.confirmation_no);
    if (kind === 'flight') {
      html += row('ddTrvAirline','Airline',t.airline) + row('ddTrvFlightNo','Flight #',t.flight_no) +
              row('ddTrvDepAir','From (IATA)',t.depart_airport) + row('ddTrvArrAir','To (IATA)',t.arrive_airport);
    } else if (kind === 'hotel') {
      html += row('ddTrvHotel','Hotel',t.hotel_name) + row('ddTrvCheckIn','Check-in (YYYY-MM-DD)',t.check_in) +
              row('ddTrvCheckOut','Check-out (YYYY-MM-DD)',t.check_out) + row('ddTrvRoom','Room type',t.room_type);
    } else if (kind === 'car_rental') {
      html += row('ddTrvRental','Rental company',t.rental_co) + row('ddTrvPickup','Pickup location',t.pickup_location);
    } else if (kind === 'car_service') {
      html += row('ddTrvRental','Company',t.rental_co) + row('ddTrvPickup','Pickup location',t.pickup_location) +
              row('ddTrvDropoff','Drop-off location',t.dropoff_location) + row('ddTrvPhone','Contact phone',t.contact_phone);
    }
    f.innerHTML = html;
    const ks = document.getElementById('ddTrvKind');
    if (ks) ks.onchange = () => { _ddTravelCollect(); _ddTravelRenderFields(); };
  }

  async function _docDetailLoadTravel(uid) {
    const block = document.getElementById('docDetailTravelBlock');
    if (!block) return;
    _ddTravel = { uid, data: null, people: [], bid: null };
    document.getElementById('docDetailTravelStatus').textContent = '';
    let d = {};
    try { d = await (await fetch(`/docs/upload/${uid}/status`, {credentials:'same-origin'})).json(); } catch (e) {}
    const t = d.travel;
    const cat = (document.getElementById('docDetailCategory').value || '').toLowerCase();
    const vendor = (document.getElementById('docDetailVendor').value || '').toLowerCase();
    const looksTravel = /hotel|air|flight|travel|lodging|car ?rental|rental/.test(cat + ' ' + vendor);
    const suspected = (t && t.is_travel) || looksTravel;
    // The panel is ALWAYS present now — auto-open when we suspect travel, otherwise
    // collapsed so the user can expand it and Pull on demand. (User 2026-06-22.)
    block.open = !!suspected;
    const hint = document.getElementById('docDetailTravelHint');
    if (hint) hint.textContent = (t && t.is_travel)
      ? '· extracted ✓'
      : (suspected ? '· looks like travel — Pull to read' : '· not detected — expand to Pull');
    _ddTravel.data = (t && t.is_travel) ? t : {};
    _ddTravelRenderFields();
    // Person roster + the budget the schedule lives on (cached per page load —
    // the panel now loads on every doc open, so don't refetch each time).
    try {
      if (!window._ddRosterCache) {
        window._ddRosterCache = await (await fetch(`/projects/${PROJ_ID}/travel/people`, {credentials:'same-origin'})).json();
      }
      const pr = window._ddRosterCache;
      _ddTravel.people = pr.people || []; _ddTravel.bid = pr.bid;
      const sel = document.getElementById('docDetailTravelPerson');
      sel.innerHTML = '<option value="">— pick person —</option>' +
        _ddTravel.people.map((p,i) => `<option value="${i}">${_esc(p.person)}${p.role?(' — '+_esc(p.role)):''}</option>`).join('');
      // Pre-select the traveler. Prefer the server's robust traveler→person
      // match (handles "ROE/ELIZABETH", middle names, etc.); fall back to a
      // token overlap on the name. (User 2026-06-26.)
      let _pidx = -1;
      const _mp = t && t.matched_person;
      if (_mp && _mp.line_id != null) {
        _pidx = _ddTravel.people.findIndex(p =>
          p.line_id === _mp.line_id && (p.instance||1) === (_mp.instance||1));
      }
      if (_pidx < 0 && t && t.traveler_name) {
        const nm = t.traveler_name.toLowerCase();
        const toks = nm.split(/[^a-z]+/).filter(x => x.length > 1);
        _pidx = _ddTravel.people.findIndex(p => {
          const pn = (p.person||'').toLowerCase();
          if (!pn) return false;
          if (pn.includes(nm) || nm.includes(pn)) return true;
          const pt = pn.split(/[^a-z]+/).filter(x => x.length > 1);
          const shared = toks.filter(x => pt.includes(x)).length;
          return shared >= 2 || (shared >= 1 && (toks.every(x=>pt.includes(x)) || pt.every(x=>toks.includes(x))));
        });
      }
      if (_pidx >= 0) {
        sel.value = String(_pidx);
        const _stp = document.getElementById('docDetailTravelStatus');
        if (_stp) _stp.textContent = '✓ Auto-matched to ' + (_ddTravel.people[_pidx].person || 'a crew member') + ' — pick the day and Apply.';
      }
      if (!_ddTravel.bid) document.getElementById('docDetailTravelStatus').textContent =
        'No Working/Estimated budget found to attach travel to.';
    } catch (e) {}
    // Prefill the travel day from check-in / departure / doc date.
    const di = document.getElementById('docDetailTravelDate');
    const pre = (t && (t.check_in || (t.depart_at || '').slice(0,10))) ||
                document.getElementById('docDetailDocDate').value || '';
    di.value = (pre || '').slice(0,10);
  }

  window.docDetailExtractTravel = async function () {
    const uid = _ddTravel.uid; if (!uid) return;
    const btn = document.getElementById('docDetailTravelExtractBtn');
    const st = document.getElementById('docDetailTravelStatus');
    if (btn) { btn.disabled = true; btn.textContent = 'Reading…'; }
    if (st) st.textContent = 'Reading the document…';
    try {
      const r = await fetch(`/docs/upload/${uid}/extract-travel`, {method:'POST', credentials:'same-origin'});
      const d = await r.json();
      if (r.ok && d.travel) {
        _ddTravel.data = d.travel.is_travel ? d.travel : (d.travel || {});
        _ddTravelRenderFields();
        const hint = document.getElementById('docDetailTravelHint');
        if (hint) hint.textContent = d.travel.is_travel ? '· extracted ✓' : '· no travel details found';
        if (st) st.textContent = d.travel.is_travel ? 'Extracted ✓ — review and Apply.' : 'No travel details found in this document.';
        // Auto-select the matched traveler (server resolved it during extract).
        const _mp = d.travel && d.travel.matched_person;
        const _sel = document.getElementById('docDetailTravelPerson');
        if (_mp && _mp.line_id != null && _sel && _ddTravel.people) {
          const _i = _ddTravel.people.findIndex(p => p.line_id === _mp.line_id && (p.instance||1) === (_mp.instance||1));
          if (_i >= 0) { _sel.value = String(_i);
            if (st) st.textContent = '✓ Auto-matched to ' + (_ddTravel.people[_i].person||'a crew member') + ' — pick the day and Apply.'; }
        }
      } else { if (st) st.textContent = 'Extract failed.'; }
    } catch (e) { if (st) st.textContent = 'Extract error: ' + e.message; }
    finally { if (btn) { btn.disabled = false; btn.textContent = '⤓ Pull from document'; } }
  };

  window.docDetailApplyTravel = async function () {
    const uid = _ddTravel.uid; if (!uid) return;
    const st = document.getElementById('docDetailTravelStatus');
    const pidx = document.getElementById('docDetailTravelPerson').value;
    const date = document.getElementById('docDetailTravelDate').value;
    if (pidx === '' || !_ddTravel.people[pidx]) { if (st) st.textContent = 'Pick a person first.'; return; }
    if (!date) { if (st) st.textContent = 'Pick a travel day.'; return; }
    if (!_ddTravel.bid) { if (st) st.textContent = 'No budget to attach travel to.'; return; }
    const person = _ddTravel.people[pidx];
    const t = _ddTravelCollect();
    const payload = Object.assign({
      doc_upload_id: uid, line_id: person.line_id, instance: person.instance,
      date, kind: t.kind || 'flight',
    }, t);
    const btn = document.getElementById('docDetailTravelApplyBtn');
    if (btn) { btn.disabled = true; btn.textContent = 'Applying…'; }
    try {
      const r = await fetch(`/projects/${PROJ_ID}/budget/${_ddTravel.bid}/travel/apply-doc`, {
        method:'POST', headers:{'Content-Type':'application/json'},
        credentials:'same-origin', body: JSON.stringify(payload)});
      const d = await r.json();
      if (r.ok && d.ok) {
        if (st) st.textContent = `✓ Added to ${_esc(person.person)}'s travel (${d.kind}).`;
        // Reflect the crew link in the person picker above.
        if (person.crew_member_id) {
          const cs = document.getElementById('docDetailCrewId');
          if (cs) cs.value = String(person.crew_member_id);
        }
      } else { if (st) st.textContent = 'Apply failed: ' + (d.error || r.status); }
    } catch (e) { if (st) st.textContent = 'Apply error: ' + e.message; }
    finally { if (btn) { btn.disabled = false; btn.textContent = 'Apply to Travel tab'; } }
  };

  window.openDocDetail = async function (uid, row, internalNav) {
    // Tolerate the legacy (null, docId) call order used by a few inline
    // links (by-dept detail rows, dynamically-rendered doc lists).
    if ((uid == null) && (typeof row === 'number')) { uid = row; row = null; }
    _docDetailUid = uid;
    window._docDetailCurrentUid = uid;
    // Reset the receipt-side transaction search for the newly-opened doc.
    const _ddTxnRes = document.getElementById('docDetailTxnResults');
    if (_ddTxnRes) _ddTxnRes.innerHTML = '';
    const _ddIncl = document.getElementById('docDetailInclMatched');
    if (_ddIncl) _ddIncl.checked = false;
    // Decide which list we're paging/patching against from the active tab.
    const _actPanel = document.getElementById('tab-actuals');
    _docDetailContext = (_actPanel && _actPanel.classList.contains('active')) ? 'actuals' : 'docs';
    // Capture the nav sequence ONCE per user-initiated open. Internal prev/next
    // calls (internalNav=true) keep the captured sequence so the list re-render
    // on save can't lose our place. (User 2026-06-22.)
    if (!internalNav) _docCaptureNavSeq(uid);
    _docDetailUpdateNav();   // refresh prev/next position + button states
    // Hoist the modal out of the Docs tab panel on first open so the
    // user can open it from any tab (Actuals, etc.) — without this,
    // the parent tab-docs div's display:none cascades and hides the
    // modal even though its own display is :flex. Idempotent: once
    // moved to body, subsequent opens just toggle visibility.
    const overlay = document.getElementById('docDetailOverlay');
    if (overlay && overlay.parentElement !== document.body) {
      document.body.appendChild(overlay);
    }
    overlay.style.display = 'flex';
    // Point the ⧉ Full editor link at the standalone editor for THIS doc.
    // PROJ_ID is in scope in this script block. (User 2026-07.)
    { const _fe = document.getElementById('docDetailFullEditor'); if (_fe) _fe.href = '/projects/' + PROJ_ID + '/docs/' + uid + '/editor'; }
    // Clear transient state from the previously-viewed doc (covers prev/next nav,
    // which reuses the open overlay): stale re-OCR conversion note + button text.
    // (User 2026-06-22.)
    const _reocrSt0 = document.getElementById('docDetailReocrStatus');
    if (_reocrSt0) { _reocrSt0.textContent = ''; _reocrSt0.style.color = 'var(--text-muted)'; }
    const _delBtn0 = document.getElementById('docDetailDeleteBtn');
    if (_delBtn0) { _delBtn0.disabled = false; _delBtn0.textContent = '🗑 Delete document'; }
    const _reocrBtn0 = document.getElementById('docDetailReocrBtn');
    if (_reocrBtn0) { _reocrBtn0.disabled = false; _reocrBtn0.textContent = '🔄 Re-run OCR'; }
    // When called from a tab that doesn't have the Docs row dataset
    // (e.g. Actuals tab), `row` is null and the field-prepopulate
    // below would leave everything blank. Fetch the canonical
    // metadata from the server and synthesize a row-like dataset
    // proxy so the rest of this function still works unchanged.
    if (!row) {
      try {
        const r = await fetch(`/docs/upload/${uid}/status`, { credentials: 'same-origin' });
        const d = r.ok ? await r.json() : {};
        row = { dataset: {
          sortFilename: d.filed_filename || d.original_filename || '',
          sortVendor:   d.vendor || '',
          sortAmount:   d.amount != null ? String(d.amount) : '',
          sortType:     (d.category || '').toLowerCase(),
          docDate:      d.doc_date || '',
          docNum:       d.doc_number || '',
          sortCard4:    d.card_last4 || '',
          note:         d.note || '',
          crewMemberId: d.crew_member_id != null ? String(d.crew_member_id) : '',
          locationId:   d.location_id != null ? String(d.location_id) : '',
        } };
      } catch (e) {
        row = { dataset: {} };
      }
    }
    document.getElementById('docDetailTitle').textContent =
      row?.dataset?.sortFilename ? row.dataset.sortFilename : 'Document';
    // Reset form fields to whatever the row's dataset carries — server
    // will overwrite when /preview-link comes back, but this avoids
    // a flash of stale data from a previous open.
    // Pre-populate from the row's dataset so the form shows the OCR-
    // extracted vendor/amount/doc_date/category immediately on open
    // (no flash of empty fields while /verify and /preview-link load).
    // The dataset uses lowercase HTML attribute names — note the
    // camelCase access (data-doc-date → dataset.docDate).
    document.getElementById('docDetailVendor').value   = row?.dataset?.sortVendor || '';
    _docDetailSetSubtype(row?.dataset?.sortVendor || '');   // supporting-doc dropdown
    // Amount of "0" is meaningless to a user — leave blank so they see the
    // placeholder. But a REFUND is stored as a negative amount, so show any
    // non-zero value (was `> 0`, which blanked refunds on reopen and made it
    // look like the edit didn't save). (User 2026-06-17.)
    const _amt = parseFloat(row?.dataset?.sortAmount || 0);
    document.getElementById('docDetailAmount').value   = (Number.isFinite(_amt) && _amt !== 0) ? _fmtMoney(_amt) : '';
    // Foreign-currency original figure (blank unless previously set).
    const _oamt = parseFloat(row?.dataset?.originalAmount || 0);
    document.getElementById('docDetailOrigAmount').value   = (Number.isFinite(_oamt) && _oamt !== 0) ? _fmtMoney(_oamt) : '';
    document.getElementById('docDetailOrigCurrency').value = row?.dataset?.originalCurrency || '';
    document.getElementById('docDetailDocDate').value  = row?.dataset?.docDate || '';
    // Category alias for the 2026-05-11 taxonomy collapse: legacy rows
    // tagged 'quote' or 'sow' have no matching <option> after the merge,
    // so map them to the merged keys before assigning. A subsequent Save
    // by the user will rewrite the row to the merged key.
    const _categoryAlias = { quote: 'estimate', sow: 'contract' };
    const _rawCat = (row?.dataset?.sortType || '').toLowerCase();
    document.getElementById('docDetailCategory').value = _categoryAlias[_rawCat] || _rawCat;
    document.getElementById('docDetailNote').value     = row?.dataset?.note || '';
    document.getElementById('docDetailDocNum').value   = row?.dataset?.docNum || '';
    document.getElementById('docDetailCard4').value    = row?.dataset?.sortCard4 || '';
    document.getElementById('docDetailCrewId').value     = row?.dataset?.crewMemberId || '';
    if (typeof docDetailNewCrewHide === 'function') docDetailNewCrewHide();   // reset inline creator
    document.getElementById('docDetailLocationId').value = row?.dataset?.locationId || '';
    _docDetailApplyTypeUI(row?.dataset?.sortType || '');
    try { _docDetailLoadTravel(uid); } catch (e) {}
    // Re-apply when the user changes the type dropdown so labels /
    // field visibility track the selection live.
    document.getElementById('docDetailCategory').onchange = (e) =>
      _docDetailApplyTypeUI((e.target.value || '').toLowerCase());
    // Re-evaluate the missing-field highlight on every keystroke so the
    // yellow outline clears as soon as the user fills the field in.
    ['docDetailVendor','docDetailAmount','docDetailDocDate','docDetailDocNum']
      .forEach(id => {
        const el = document.getElementById(id);
        if (el) el.oninput = () => _docDetailApplyTypeUI(
          (document.getElementById('docDetailCategory').value || '').toLowerCase());
      });
    // Snapshot the just-populated fields so prev/next can tell whether the
    // user changed anything (→ auto-save) vs. just browsed (→ skip save).
    _docDetailOpenSig = _docDetailSig();
    document.getElementById('docDetailVerify').style.display = 'none';
    const previewEl = document.getElementById('docDetailPreview');
    previewEl.innerHTML = '<p class="muted" style="padding:1rem">Loading preview…</p>';

    // Pull doc metadata in parallel with the verify check. The actual
    // file bytes are streamed by the browser when it loads the
    // /raw URL we set as the img/iframe src — no AJAX needed for
    // those, since the proxy endpoint sets the correct Content-Type
    // and inline disposition for the browser to render directly.
    let verifyData = null, linkData = null;
    try {
      const [verifyR, linkR] = await Promise.all([
        fetch('/docs/upload/' + uid + '/verify',       { credentials: 'same-origin' }),
        fetch('/docs/upload/' + uid + '/preview-link', { credentials: 'same-origin' }),
      ]);
      verifyData = await verifyR.json().catch(() => null);
      linkData   = await linkR.json().catch(() => null);
    } catch (e) { /* fall through */ }

    // Use OUR /raw proxy URL — Dropbox content URLs send
    // X-Frame-Options: DENY (breaks iframe) and Content-Disposition:
    // attachment (download instead of preview). Routing through the
    // server lets us serve the bytes inline with the right Content-Type.
    const rawUrl = '/docs/upload/' + uid + '/raw';
    const fn = (linkData && linkData.filename ? linkData.filename : '').toLowerCase();
    const ct = (linkData && linkData.content_type ? linkData.content_type : '').toLowerCase();
    const isPdf   = ct.includes('pdf')   || fn.endsWith('.pdf');
    const isImage = ct.startsWith('image/')
                    || /\.(jpe?g|png|gif|heic|heif|webp|bmp|tiff?)$/i.test(fn);

    // Reset zoom toolbar visibility — only images get it.
    const zoomBar = document.getElementById('docDetailZoomBar');
    if (zoomBar) zoomBar.style.display = isImage ? 'flex' : 'none';

    if (isImage) {
      // Image preview: large by default (fit-to-window), with zoom + pan.
      // Wraps in a positioned container so we can transform the <img>
      // and let the parent's overflow:auto provide the panning behavior.
      previewEl.style.alignItems = 'flex-start';
      previewEl.style.justifyContent = 'flex-start';
      previewEl.innerHTML =
        '<img id="docDetailImg" src="' + rawUrl + '" alt="preview" ' +
              'style="display:block;max-width:none;max-height:none;' +
              'transform-origin:0 0;transition:transform .08s linear;' +
              'user-select:none;-webkit-user-drag:none;cursor:grab;">';
      _initImagePreviewControls();
    } else {
      // Everything that isn't a confident image (real PDFs, AND images that
      // were misnamed ".pdf" by the filing pipeline) renders in an <iframe>
      // pointed at /raw. /raw now sniffs the real bytes and serves the
      // correct Content-Type, so the browser shows a PDF as a PDF and a
      // JPEG-named-".pdf" as an image — instead of the PDF viewer throwing
      // "Failed to load PDF document" on image bytes. (User 2026-06-01.)
      // An iframe (vs <embed type="application/pdf">) honours whatever type
      // the server sends, which is the whole point. Same-origin, so no
      // X-Frame-Options issue. A small "open in new tab" link covers any
      // exotic type the browser still can't render inline.
      previewEl.style.alignItems = 'stretch';
      previewEl.style.justifyContent = 'stretch';
      previewEl.style.position = 'relative';
      previewEl.innerHTML =
        '<iframe src="' + rawUrl + '" title="document preview" ' +
                'style="border:0;width:100%;height:100%;min-height:540px;display:block;background:#fff"></iframe>' +
        '<a href="' + rawUrl + '" target="_blank" rel="noopener" ' +
           'style="position:absolute;top:8px;right:8px;font-size:11px;background:#1a2540;' +
           'border:1px solid #2d4070;color:#8fb4ff;padding:3px 9px;border-radius:5px;text-decoration:none">↗ Open in new tab</a>';
    }

    // Verification banner — green on success, red if Dropbox doesn't
    // have the file. Mirrors what the row badge says, but inside the
    // modal it's much harder to miss.
    const vEl = document.getElementById('docDetailVerify');
    if (verifyData && verifyData.ok && verifyData.verified) {
      vEl.style.cssText = 'font-size:11px;padding:6px 9px;border-radius:6px;margin-bottom:12px;background:#14291e;color:#22c55e;border:1px solid #1a4228;';
      vEl.textContent = '✓ Verified in Dropbox' +
        (verifyData.size ? (' · ' + _docDetailFmtSize(verifyData.size)) : '');
      vEl.style.display = 'block';
    } else if (verifyData && verifyData.ok && verifyData.verified === false) {
      vEl.style.cssText = 'font-size:11px;padding:6px 9px;border-radius:6px;margin-bottom:12px;background:#2a1414;color:#ef4444;border:1px solid #4a2020;font-weight:600;';
      vEl.textContent = '⚠ MISSING from Dropbox — ' + (verifyData.reason || 'file not found');
      vEl.style.display = 'block';
    }

    // Fetch a REAL Dropbox shared-link URL for the "Open in Dropbox"
    // button. The temp link from /preview-link is a content URL that
    // browsers download instead of preview, so we hit /dropbox-link
    // which calls sharing_create_shared_link_with_settings to produce
    // a dropbox.com URL that opens the web preview. Done in parallel
    // with the temp link, but exposed via a separate button so the
    // image/iframe doesn't have to wait on the shared-link round-trip.
    const dbxBtn = document.getElementById('docDetailDropboxLink');
    dbxBtn.href = '#';
    dbxBtn.style.opacity = '0.5';
    dbxBtn.textContent = 'Loading link…';
    fetch('/docs/upload/' + uid + '/dropbox-link', { credentials: 'same-origin' })
      .then(r => r.json())
      .then(d => {
        if (d && d.ok && d.url) {
          dbxBtn.href = d.url;
          dbxBtn.style.opacity = '1';
          dbxBtn.textContent = 'Open in Dropbox ↗';
        } else {
          dbxBtn.style.opacity = '0.4';
          dbxBtn.textContent = 'Open in Dropbox (unavailable)';
          dbxBtn.title = (d && d.error) || 'Could not generate link';
        }
        // The /dropbox-link endpoint also reports whether a separate
        // source archive copy exists. If yes, expose a download link
        // so the user can grab the original uploaded bytes regardless
        // of what happened to the processed copy.
        const srcBtn = document.getElementById('docDetailArchiveLink');
        if (srcBtn) {
          if (d && d.archive_url) {
            srcBtn.href = d.archive_url;
            srcBtn.style.display = 'inline-block';
            srcBtn.title = 'Source archive: ' + (d.archive_path || '');
          } else {
            srcBtn.style.display = 'none';
          }
        }
      })
      .catch(() => {
        dbxBtn.style.opacity = '0.4';
        dbxBtn.textContent = 'Open in Dropbox (network error)';
      });

    // Pull the saved metadata (vendor / amount / doc_date / category /
    // note) by hitting /update with no body — actually safer to re-pull
    // via /verify since it carries the row, OR ask the server. Easiest:
    // pre-populate from dataset, let /verify add path; for full fields
    // we already have them server-rendered in dataset attrs. The
    // detail modal is editing in-place — a no-op load is fine.
    document.getElementById('docDetailMeta').textContent =
      'Upload ID #' + uid;
    _docDetailLoadCoding(uid);
    docItemizeLoad(uid);
    docBackupsLoad(uid);
  };

  // ── Itemize into budget lines (QuickBooks-style line-item split) ──────────
  let _itemUid = null, _itemDocTotal = null;
  window._itemAddRow = function (desc, amount, lineId) {
    const host = document.getElementById('docItemizeRows');
    if (!host) return;
    const row = document.createElement('div');
    row.className = 'ditem-row';
    row.style.cssText = 'display:flex;gap:6px;margin-bottom:5px;align-items:center';
    row.innerHTML =
      '<input class="ditem-desc" placeholder="description" value="' + _esc(desc || '') + '" oninput="window._docItemizeDirty=true" style="flex:1.5;min-width:0;font-size:12px;padding:4px 6px;background:var(--bg-input);color:var(--text);border:1px solid var(--border);border-radius:5px">' +
      '<input class="ditem-amt" type="number" step="0.01" placeholder="0.00" value="' + (amount != null && amount !== '' ? amount : '') + '" oninput="_itemUpdateTotal();window._docItemizeDirty=true" style="width:88px;font-size:12px;padding:4px 6px;background:var(--bg-input);color:var(--text);border:1px solid var(--border);border-radius:5px;text-align:right">' +
      '<select class="ditem-line actuals-line-picker" onfocus="_actualsFillPicker(this)" onchange="_ditemLineChanged(this)" style="flex:1.3;min-width:0;font-size:12px;padding:4px 6px;background:var(--bg-input);color:var(--text);border:1px solid var(--border);border-radius:5px"><option value="">— budget line —</option></select>' +
      '<button type="button" title="Remove line" onclick="this.closest(\'.ditem-row\').remove();_itemUpdateTotal();window._docItemizeDirty=true" style="background:none;border:none;color:var(--text-muted);cursor:pointer;font-size:14px">✕</button>';
    host.appendChild(row);
    if (lineId) { const sel = row.querySelector('.ditem-line'); sel.dataset.current = String(lineId); if (typeof window._actualsFillPicker === 'function') window._actualsFillPicker(sel); }
    _itemUpdateTotal();
  };
  window._itemUpdateTotal = function () {
    const _n = document.querySelectorAll('#docItemizeRows .ditem-row').length;
    const _hdr = document.getElementById('docItemizeHdr'); if (_hdr) _hdr.style.display = _n ? 'flex' : 'none';
    // Only lines WITH a budget line peel off; lines with an amount but no line
    // stay on the main line (amber border flags them). (User 2026-07-20.)
    let sub = 0, uncSub = 0, uncN = 0;
    document.querySelectorAll('#docItemizeRows .ditem-row').forEach(r => {
      const a = parseFloat((r.querySelector('.ditem-amt') || {}).value || 0) || 0;
      const sel = r.querySelector('.ditem-line');
      const hasLine = !!(sel && sel.value);
      if (a && !hasLine) { uncSub += a; uncN++; if (sel) sel.style.borderColor = '#e0a13a'; }
      else { sub += a; if (sel) sel.style.borderColor = ''; }
    });
    sub = Math.round(sub * 100) / 100; uncSub = Math.round(uncSub * 100) / 100;
    window._itemUncodedN = uncN; window._itemUncodedSub = uncSub;
    const el = document.getElementById('docItemizeTotal'); if (!el) return;
    const tot = _itemDocTotal;
    const fmt = v => '$' + Number(v).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2});
    // Main-line label (the whole-doc coding picker above).
    let mainLabel = 'main line';
    const mp = document.getElementById('docDetailLinePicker');
    if (mp && mp.value && mp.selectedIndex >= 0 && mp.options[mp.selectedIndex]) {
      const t = (mp.options[mp.selectedIndex].text || '').trim();
      if (t && t !== '— pick budget line —') mainLabel = t.length > 30 ? t.slice(0,30)+'…' : t;
    }
    if (tot != null) {
      const remainder = Math.round((tot - sub) * 100) / 100;
      const over = remainder < -0.01;
      const uncNote = uncN
        ? ' <span style="color:#e0a13a">(incl. ' + fmt(uncSub) + ' from ' + uncN + ' line' + (uncN !== 1 ? 's' : '') + ' with no budget line)</span>'
        : '';
      el.innerHTML = 'Peeling off <b>' + fmt(sub) + '</b> · ' +
        (over
          ? '<span style="color:#e0a13a">⚠ ' + fmt(-remainder) + ' over the doc total</span>'
          : '→ <b style="color:#5fd0a0">' + fmt(remainder) + '</b> to ' + (mp && mp.value ? '<b>'+mainLabel+'</b>' : '<span style="color:#e0a13a">main line (pick one above)</span>') + uncNote);
    } else { el.innerHTML = 'Sublines: <b>' + fmt(sub) + '</b>'; }
  };
  // Subline budget-line pick: handles the ＋ New-line option, marks dirty, and
  // refreshes the peel-off math (coded vs uncoded). (2026-07-20.)
  window._ditemLineChanged = function (sel) {
    if (sel.value === '__newline__') {
      sel.value = '';
      window._newBudgetLineFlow(function (nl) {
        window._pickerInsertNewLine(nl);
        sel.value = String(nl.line_id);
        window._docItemizeDirty = true;
        _itemUpdateTotal();
      });
      return;
    }
    window._docItemizeDirty = true;
    _itemUpdateTotal();
  };
  window.docItemizePull = function () {
    const items = window._itemVeryfi || [];
    if (!items.length) { document.getElementById('docItemizeStatus').textContent = 'No OCR line items found on this document.'; return; }
    document.getElementById('docItemizeRows').innerHTML = '';
    items.forEach(it => _itemAddRow(it.description, it.amount, ''));
    window._docItemizeDirty = true;
  };
  window.docItemizeLoad = async function (uid) {
    _itemUid = uid; _itemDocTotal = null; window._itemVeryfi = [];
    const block = document.getElementById('docDetailItemizeBlock');
    const rows = document.getElementById('docItemizeRows');
    const st = document.getElementById('docItemizeStatus');
    const cnt = document.getElementById('docItemizeCount');
    if (!block || !rows) return;
    block.style.display = 'none'; rows.innerHTML = ''; if (st) st.textContent = ''; if (cnt) cnt.textContent = '';
    try {
      const r = await fetch('/docs/upload/' + uid + '/line-items', { credentials:'same-origin' });
      if (!r.ok) return;
      const d = await r.json();
      // Receipt detail (subtotal/tax/tip/address) — populate regardless of whether
      // the doc has a transaction to itemize.
      const rd = d.receipt_detail || {};
      const _sv = (id, v) => { const el = document.getElementById(id); if (el) el.value = (v != null ? v : ''); };
      _sv('docDetailSubtotal', rd.subtotal); _sv('docDetailTax', rd.tax); _sv('docDetailTip', rd.tip);
      _sv('docDetailMerchAddr', rd.merchant_address); _sv('docDetailMerchPhone', rd.merchant_phone);
      if (!d.ok || d.parent_txn_id == null) return;      // no transaction to itemize
      block.style.display = '';
      _itemDocTotal = d.doc_total; window._itemVeryfi = d.veryfi_items || [];
      const pullBtn = document.getElementById('docItemizePullBtn');
      if (pullBtn) pullBtn.style.display = (window._itemVeryfi.length ? '' : 'none');
      // Reflect the saved main line on the picker (the remainder child's line).
      if (d.main_line_id) {
        const mp = document.getElementById('docDetailLinePicker');
        if (mp) { mp.dataset.current = String(d.main_line_id); if (window._actualsFillPicker) window._actualsFillPicker(mp); mp.value = String(d.main_line_id); }
      }
      if (d.existing && d.existing.length) {
        block.open = true;
        cnt.textContent = '· ' + d.existing.length + ' subline' + (d.existing.length!==1?'s':'');
        d.existing.forEach(x => _itemAddRow(x.description, x.amount, x.budget_line_id));
      } else {
        _itemUpdateTotal();
      }
      _itemSetOverride(d.existing ? d.existing.length : 0);
      window._docItemizeDirty = false;
    } catch (e) {}
  };
  // When a doc is itemized, the whole-document (single line) coding is OVERRIDDEN
  // by the line items — reflect that on the picker so it's not ambiguous. (User 2026-07.)
  window._itemSetOverride = function (n) {
    // n = number of sublines (peel-offs). The picker stays enabled as the MAIN
    // line; when split, it just receives the remainder. (User 2026-07.)
    const label = document.querySelector('#docDetailCodingBlock label');
    const status = document.getElementById('docDetailCodingStatus');
    if (label) label.textContent = n > 0 ? 'Main budget line (gets the remainder)' : 'Budget line (Actuals)';
    if (n > 0 && status) { status.innerHTML = '↳ plus ' + n + ' subline' + (n !== 1 ? 's' : '') + ' peeled off to other lines (below).'; status.style.color = 'var(--text-muted)'; }
    else if (status) { status.style.color = ''; }
    _itemUpdateTotal();
  };
  window.docItemizeSave = async function () {
    if (!_itemUid) return;
    const st = document.getElementById('docItemizeStatus');
    const btn = document.getElementById('docItemizeSaveBtn');
    // Only rows WITH a budget line peel off; rows without one stay on the main
    // line (their dollars ride the remainder). (User 2026-07-20.)
    const rows = []; let uncoded = 0, uncodedAmt = 0;
    document.querySelectorAll('#docItemizeRows .ditem-row').forEach(r => {
      const amt = parseFloat((r.querySelector('.ditem-amt')||{}).value || 0);
      const line = (r.querySelector('.ditem-line')||{}).value || '';
      const desc = (r.querySelector('.ditem-desc')||{}).value || '';
      if (amt && line) rows.push({ amount: amt, budget_line_id: line, description: desc });
      else if (amt) { uncoded++; uncodedAmt += amt; }
    });
    const mainLine = (document.getElementById('docDetailLinePicker')||{}).value || '';
    // Guard: no sublines AND no main line would silently UN-code the doc — make
    // the user pick something instead of losing coding on a stray click. (2026-07.)
    if (!rows.length && !mainLine) {
      if (st) { st.textContent = uncoded
        ? 'No line has a budget line yet — pick budget lines on the rows, or pick a main budget line above.'
        : 'Nothing to save — add sublines or pick a main budget line above.'; st.style.color = '#e0a13a'; }
      return;
    }
    // Uncoded rows need a main line to land on — otherwise their dollars vanish.
    if (uncoded && !mainLine) {
      if (st) {
        const money = v => '$'+Number(v||0).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});
        st.textContent = uncoded + ' line' + (uncoded!==1?'s have':' has') + ' no budget line ('
          + money(uncodedAmt) + ') — pick lines for them, or pick a main budget line above to receive that amount.';
        st.style.color = '#e0a13a';
      }
      return;
    }
    if (btn) { btn.disabled = true; btn.textContent = 'Saving…'; }
    try {
      const r = await fetch('/docs/upload/' + _itemUid + '/itemize', {
        method:'POST', credentials:'same-origin', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({ rows, main_line_id: mainLine })
      });
      const d = await r.json();
      if (r.ok && d.ok) {
        if (st) {
          const money = v => '$'+Number(v||0).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});
          if (d.cleared) st.textContent = 'Itemization cleared.';
          else if (d.over) st.textContent = '⚠ Saved, but sublines are '+money(-d.remainder)+' over the doc total — check the amounts.';
          else st.textContent = '✓ Saved — '+money(d.sublines_total)+' peeled off, '+money(d.main_line_amount)+' stays on the main line'
            + (d.uncoded_n ? ' (incl. '+money(d.uncoded_total)+' from '+d.uncoded_n+' line'+(d.uncoded_n!==1?'s':'')+' without a budget line)' : '') + '.';
          st.style.color = d.over ? '#e0a13a' : '';
        }
        const nsub = rows.filter(x=>x.amount).length;
        const cnt = document.getElementById('docItemizeCount');
        if (cnt) cnt.textContent = nsub ? ('· '+nsub+' subline'+(nsub!==1?'s':'')) : '';
        _itemSetOverride(nsub);
        window._docItemizeDirty = false;
      } else { if (st) st.textContent = 'Save failed: ' + (d.error || r.status); }
    } catch (e) { if (st) st.textContent = 'Error: ' + e.message; }
    finally { if (btn) { btn.disabled = false; btn.textContent = 'Save itemization'; } }
  };

  // ── 📎 Invoice-side backups (2026-07-22) ──────────────────────────────────
  // View + attach backup documents FROM the invoice: for the invoice total or
  // any itemized subline. Mirror of the receipt-side mark-backup flow.
  let _bkUid = null, _bkData = null;
  const _bkMoney = v => v == null ? '—' :
    '$' + Number(v).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  window.docBackupsLoad = async function (uid) {
    _bkUid = uid; _bkData = null;
    const block = document.getElementById('docDetailBackupsBlock');
    if (!block) return;
    block.style.display = 'none';
    const list = document.getElementById('docBackupsList');
    const cand = document.getElementById('docBackupsCandidates');
    const st = document.getElementById('docBackupsStatus');
    const cnt = document.getElementById('docBackupsCount');
    if (list) list.innerHTML = '';
    if (cand) { cand.style.display = 'none'; cand.innerHTML = ''; }
    if (st) st.textContent = '';
    if (cnt) cnt.textContent = '';
    try {
      const r = await fetch('/docs/upload/' + uid + '/backups', { credentials: 'same-origin' });
      if (!r.ok) return;
      const d = await r.json();
      if (!d.ok || !d.targets || !d.targets.length) return;   // no expense yet — panel hidden
      _bkData = d;
      block.style.display = '';
      const sel = document.getElementById('docBackupsTarget');
      if (sel) sel.innerHTML = d.targets.map(t =>
        '<option value="' + t.txn_id + '">' + (t.is_parent ? '' : '↳ ') + _esc(t.label)
        + (t.amount != null ? ' · ' + _bkMoney(t.amount) : '') + '</option>').join('');
      let n = 0, html = '';
      d.targets.forEach(t => {
        const bs = d.backups[String(t.txn_id)] || [];
        if (!bs.length) return;
        html += '<div style="margin-top:6px;font-size:10.5px;color:var(--text-muted);text-transform:uppercase;letter-spacing:.04em">'
          + (t.is_parent ? '' : '↳ ') + _esc(t.label)
          + (t.amount != null ? ' · ' + _bkMoney(t.amount) : '') + '</div>';
        bs.forEach(b => {
          n++;
          html += '<div style="display:flex;gap:8px;align-items:center;padding:4px 6px;border:1px solid var(--border);border-radius:6px;margin-top:3px">'
            + '<a href="#" onclick="openDocDetail(' + b.doc_id + ', null);return false" '
            + 'style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#8fb4ff;text-decoration:none" '
            + 'title="Open this backup document">📎 ' + _esc(b.vendor || b.filename) + '</a>'
            + '<span style="color:var(--text-muted);font-size:.78rem;white-space:nowrap">' + _esc(b.doc_date || '') + '</span>'
            + '<b style="white-space:nowrap">' + _bkMoney(b.amount) + '</b>'
            + '<button type="button" title="Detach this backup" onclick="docBackupsDetach(' + b.doc_id + ',' + t.txn_id + ')" '
            + 'style="background:none;border:none;color:var(--text-muted);cursor:pointer;font-size:13px">✕</button>'
            + '</div>';
        });
      });
      if (list) list.innerHTML = html ||
        '<div style="color:var(--text-muted)">No backups attached yet — use <b>+ Attach backup…</b> below, or open a receipt in the Actuals queue and mark it 📎 Backup from there.</div>';
      if (cnt) cnt.textContent = n ? ('· ' + n) : '';
      if (n) block.open = true;
    } catch (e) {}
  };
  window.docBackupsShowCandidates = function () {
    const cand = document.getElementById('docBackupsCandidates');
    if (!cand || !_bkData) return;
    if (cand.style.display !== 'none') { cand.style.display = 'none'; return; }
    const cs = _bkData.candidates || [];
    cand.innerHTML = cs.length
      ? ('<div style="font-size:10.5px;color:var(--text-muted);margin-bottom:4px">Pick the document that backs up the target chosen above (closest amounts first):</div>'
        + cs.map(c =>
          '<div style="display:flex;gap:8px;align-items:center;padding:4px 6px;border:1px solid var(--border);border-radius:6px;margin-top:3px">'
          + '<span style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + _esc(c.vendor || c.filename)
          + (c.category ? ' <span style="color:var(--text-muted);font-size:.75rem">' + _esc(c.category) + '</span>' : '') + '</span>'
          + '<span style="color:var(--text-muted);font-size:.78rem;white-space:nowrap">' + _esc(c.doc_date || '') + '</span>'
          + '<b style="white-space:nowrap">' + _bkMoney(c.amount) + '</b>'
          + '<button type="button" onclick="docBackupsAttach(' + c.doc_id + ')" '
          + 'title="' + (c.will_absorb ? 'Attach — its own charge stops counting (it becomes documentation)' : 'Attach as backup documentation') + '" '
          + 'style="padding:3px 10px;border-radius:5px;background:#1f7a4d;border:none;color:#fff;font-size:11px;cursor:pointer">Attach</button>'
          + '</div>').join(''))
      : '<div style="color:var(--text-muted);font-size:.85rem">No other documents on this project to attach.</div>';
    cand.style.display = '';
  };
  window.docBackupsAttach = async function (docId) {
    if (!_bkUid) return;
    const sel = document.getElementById('docBackupsTarget');
    const st = document.getElementById('docBackupsStatus');
    const target = sel ? parseInt(sel.value) : null;
    if (!target) return;
    if (st) { st.style.color = 'var(--text-muted)'; st.textContent = 'Attaching…'; }
    try {
      const r = await fetch('/docs/upload/' + _bkUid + '/attach-backup', {
        method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ doc_id: docId, target_txn_id: target }) });
      const j = await r.json();
      if (r.ok && j.ok) {
        if (st) { st.style.color = '#5fd0a0'; st.textContent = j.absorbed
          ? '📎 Attached — that document’s own charge no longer counts.'
          : '📎 Attached as backup documentation.'; }
        docBackupsLoad(_bkUid);
      } else if (st) { st.style.color = '#e0a13a'; st.textContent = j.error || ('Failed (' + r.status + ')'); }
    } catch (e) { if (st) { st.style.color = '#e0a13a'; st.textContent = 'Error: ' + e.message; } }
  };
  window.docBackupsDetach = async function (docId, targetId) {
    if (!_bkUid) return;
    const st = document.getElementById('docBackupsStatus');
    try {
      const r = await fetch('/docs/upload/' + _bkUid + '/detach-backup', {
        method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ doc_id: docId, target_txn_id: targetId }) });
      const j = await r.json();
      if (r.ok && j.ok) {
        if (st) { st.style.color = 'var(--text-muted)'; st.textContent = j.restored
          ? 'Detached — the document’s charge is back in the queue.' : 'Detached.'; }
        docBackupsLoad(_bkUid);
      } else if (st) { st.style.color = '#e0a13a'; st.textContent = j.error || ('Failed (' + r.status + ')'); }
    } catch (e) { if (st) { st.style.color = '#e0a13a'; st.textContent = 'Error: ' + e.message; } }
  };

  // Load the budget-line coding context for the open doc and wire the
  // in-modal picker + smart suggestion. Reuses the Actuals machinery
  // (window.actualsSetLine / window._actualsFillPicker). 2026-05-29.
  async function _docDetailLoadCoding(uid) {
    const block  = document.getElementById('docDetailCodingBlock');
    const picker = document.getElementById('docDetailLinePicker');
    const sugEl  = document.getElementById('docDetailSuggest');
    const statusEl = document.getElementById('docDetailCodingStatus');
    if (!block || !picker) return;
    // reset
    block.style.display = 'none';
    sugEl.style.display = 'none'; sugEl.innerHTML = '';
    picker.dataset.populated = '0';
    picker.innerHTML = '<option value="">— pick budget line —</option>';
    picker.dataset.tid = ''; picker.dataset.current = ''; statusEl.textContent = '';
    if (typeof window._actualsFillPicker !== 'function') return;
    try {
      const r = await fetch('/docs/upload/' + uid + '/coding', { credentials: 'same-origin' });
      if (!r.ok) return;
      const d = await r.json();
      if (!d.txn_id) return;                 // non-ledger doc — no line coding
      picker.dataset.tid = d.txn_id;
      picker.dataset.current = d.current || '';
      window._actualsFillPicker(picker);      // clone options + select current
      block.style.display = '';
      if (d.not_project) { picker.disabled = true; statusEl.textContent = 'Marked “not a project expense.”'; return; }
      const sel = picker.options[picker.selectedIndex];
      // Same language as the Actuals row (2026-08-19 parity pass): picking a
      // line on an uncoded document IS the create-expense step.
      const _ph = Array.from(picker.options).find(o => o.value === '');
      if (_ph) _ph.text = d.current ? 'Change budget line' : '＋ Create expense — pick budget line';
      statusEl.textContent = d.current && sel
        ? ('Currently: ' + sel.text.trim())
        : 'Not an expense yet — picking a line creates it (the doc rides along as backup).';
      // Smart suggestion chip — only when not already coded.
      if (d.suggestion && !d.current) {
        const s = d.suggestion;
        sugEl.style.display = '';
        sugEl.innerHTML = '<button type="button" id="docDetailSuggestBtn" '
          + 'style="font-size:11.5px;padding:4px 9px;background:#10231a;border:1px solid #1f6f4a;'
          + 'border-radius:5px;color:#5fd0a0;cursor:pointer;max-width:100%;white-space:normal;text-align:left;line-height:1.3">'
          + '💡 Suggested: ' + _esc(String(s.code)) + ' · ' + _esc(String(s.label)) + '</button>';
        document.getElementById('docDetailSuggestBtn').onclick = () => {
          picker.value = String(s.line_id);
          if (picker.value === String(s.line_id)) {
            sugEl.style.display = 'none';
            statusEl.textContent = 'Coding…';
            actualsSetLine(picker);
          }
        };
      }
    } catch (e) { /* silent — coding is best-effort */ }
  }

  window.closeDocDetail = function (ev) {
    if (ev && ev.target && ev.target.id !== 'docDetailOverlay') return;   // click inside the panel
    // Backdrop click: don't silently discard unsaved itemized lines. (User 2026-07.)
    if (ev && ev.target && ev.target.id === 'docDetailOverlay' && window._docItemizeDirty) {
      const st = document.getElementById('docItemizeStatus');
      if (st) { st.textContent = 'You have unsaved itemized lines — Save, or use the Close/Cancel button to discard.'; st.style.color = '#e0a13a'; }
      const blk = document.getElementById('docDetailItemizeBlock'); if (blk) blk.open = true;
      return;
    }
    window._docItemizeDirty = false;
    document.getElementById('docDetailOverlay').style.display = 'none';
    _docDetailUid = null;
    // Reset transient state so a stale message/button doesn't carry into the
    // next doc the user opens. (User 2026-06-22: the re-OCR conversion note and
    // the 'Deleting…' button persisted until a page refresh.)
    const _reocrSt = document.getElementById('docDetailReocrStatus');
    if (_reocrSt) { _reocrSt.textContent = ''; _reocrSt.style.color = 'var(--text-muted)'; }
    const _delBtn = document.getElementById('docDetailDeleteBtn');
    if (_delBtn) { _delBtn.disabled = false; _delBtn.textContent = '🗑 Delete document'; }
    const _reocrBtn = document.getElementById('docDetailReocrBtn');
    if (_reocrBtn) { _reocrBtn.disabled = false; _reocrBtn.textContent = '🔄 Re-run OCR'; }
  };

  // ── Create a new person / vendor from inside the doc popup ───────────
  // For when a doc arrives for someone not in the crew DB yet. Creates the
  // crew record (optionally a vendor), adds the option, and selects it.
  window.docDetailNewCrewShow = function () {
    const f = document.getElementById('docDetailNewCrew');
    if (f) { f.style.display = 'block'; document.getElementById('docDetailNewCrewName').focus(); }
  };
  window.docDetailNewCrewHide = function () {
    const f = document.getElementById('docDetailNewCrew');
    if (!f) return;
    f.style.display = 'none';
    document.getElementById('docDetailNewCrewName').value = '';
    document.getElementById('docDetailNewCrewVendor').checked = false;
    document.getElementById('docDetailNewCrewStatus').textContent = '';
  };
  window.docDetailNewCrewSave = async function () {
    const name = (document.getElementById('docDetailNewCrewName').value || '').trim();
    const isVendor = document.getElementById('docDetailNewCrewVendor').checked;
    const status = document.getElementById('docDetailNewCrewStatus');
    if (!name) { status.textContent = 'Name required'; return; }
    status.textContent = 'Adding…';
    try {
      const r = await fetch('/crew/new?fmt=json', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, is_vendor: isVendor }),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok || !d.id) { status.textContent = 'Failed: ' + (d.error || r.status); return; }
      const sel = document.getElementById('docDetailCrewId');
      let opt = Array.from(sel.options).find(o => o.value === String(d.id));
      if (!opt) {
        opt = document.createElement('option');
        opt.value = String(d.id);
        opt.textContent = d.name + (isVendor ? ' (vendor)' : '');
        sel.appendChild(opt);
      }
      sel.value = String(d.id);
      docDetailNewCrewHide();
    } catch (e) {
      status.textContent = 'Error: ' + (e && e.message || e);
    }
  };

  // ── Delete the currently-open document ───────────────────────────────
  // Calls the existing /docs/upload/<uid>/delete endpoint, which trashes
  // both Dropbox copies, unlinks PO source-doc + attachments, and
  // cascades delete to the linked Transaction row(s). Reloads on
  // success so the Actuals tab / Docs tab refresh. Per user 2026-05-14.
  window.docDetailDelete = async function () {
    const uid = _docDetailUid || window._docDetailCurrentUid;
    if (!uid) return;
    const titleEl = document.getElementById('docDetailTitle');
    const name = (titleEl && titleEl.textContent) ? titleEl.textContent.trim() : `Upload #${uid}`;
    if (!confirm(
      `Move "${name}" to the Trash?\n\n` +
      `This will:\n` +
      `  • Remove it from the Docs list and Actuals\n` +
      `  • Unlink it from any matched bank charge (the charge keeps its coding)\n` +
      `  • Move the Dropbox copies to /_TRASH/\n\n` +
      `You can restore it from Docs → 🗑 Trash (files auto-purge after 30 days).`
    )) return;
    const btn = document.getElementById('docDetailDeleteBtn');
    if (btn) { btn.disabled = true; btn.textContent = 'Deleting…'; }
    try {
      const r = await fetch(`/docs/upload/${uid}/delete`, {
        method: 'POST', credentials: 'same-origin',
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok) {
        alert('Delete failed: ' + (j.error || r.status));
        if (btn) { btn.disabled = false; btn.textContent = '🗑 Delete document'; }
        return;
      }
      // Remove the row IN PLACE (no reload) so the user keeps their
      // subtab / search / sort / scroll. Fade it out so it's obvious the
      // delete worked (user 2026-05-29: "it reloads… I don't know if it
      // deleted or not").
      const row = document.querySelector('.doc-row[data-upload-id="' + uid + '"]');
      closeDocDetail();
      if (row) {
        row.style.transition = 'opacity .3s';
        row.style.opacity = '0';
        setTimeout(() => {
          row.remove();
          if (typeof _docsApplyView === 'function') _docsApplyView();
        }, 300);
      } else if (typeof _docsApplyView === 'function') {
        _docsApplyView();
      }
      // Also update the ACTUALS tab in place — the doc's own row disappears,
      // a matched bank row reverts to "needs receipt". (User 2026-06-11.)
      _docRemoveActualsRowsForDoc(uid);
    } catch (e) {
      alert('Delete failed: ' + e.message);
      if (btn) { btn.disabled = false; btn.textContent = '🗑 Delete document'; }
    }
  };

  // Patch the matching Docs-tab row IN PLACE from a /update response so
  // the list reflects the edit without a full reload (preserves subtab /
  // search / sort / scroll). 2026-05-29.
  function _docPatchDocsRow(uid, d) {
    const row = document.querySelector('.doc-row[data-upload-id="' + uid + '"]');
    if (!row) return;
    row.dataset.sortVendor  = (d.vendor || '').toLowerCase();
    row.dataset.sortAmount  = d.amount || 0;
    row.dataset.sortType    = (d.category || '').toLowerCase();
    row.dataset.groupKey    = d.category ? d.category.toLowerCase() : '_unsorted';
    row.dataset.sortDocDate = d.doc_date || '';
    row.dataset.docDate     = d.doc_date || '';
    row.dataset.docNum      = d.doc_number || '';
    row.dataset.sortCard4   = d.card_last4 || '';
    row.dataset.note        = d.note || '';
    row.dataset.originalAmount   = d.original_amount || 0;
    row.dataset.originalCurrency = d.original_currency || '';
    const vEl = row.querySelector('.doc-cell-vendor');
    if (vEl) vEl.innerHTML = d.vendor
      ? ('🏢 ' + _esc(d.vendor))
      : '<span style="color:var(--text-muted);font-style:italic">No vendor</span>';
    const aEl = row.querySelector('.doc-cell-amount');
    if (aEl) {
      aEl.innerHTML = (d.amount != null && +d.amount)
        ? ('$' + Number(d.amount).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}))
        : '<span style="color:var(--text-muted);font-style:italic;font-weight:400;font-size:11px">—</span>';
      if (d.original_amount && +d.original_amount && d.original_currency) {
        aEl.innerHTML += '<div style="font-size:10px;color:var(--text-muted);font-weight:400">'
          + Number(d.original_amount).toLocaleString() + ' ' + _esc(d.original_currency) + '</div>';
      }
    }
    const dEl = row.querySelector('.doc-cell-date');
    if (dEl) dEl.innerHTML = _fmtDocDateCell(d.doc_date);
    const c4El = row.querySelector('.doc-cell-card4');
    if (c4El) c4El.textContent = d.card_last4 ? (' · 💳 ••' + d.card_last4) : '';
    if (d.filed_path) {
      const nm = d.filed_path.split('/').pop();
      const fn = row.querySelector('.doc-filename');
      if (fn && nm) { fn.textContent = '📎 ' + nm; fn.dataset.original = nm; }
    }
  }

  // Patch any Actuals-tab transaction row(s) showing this doc. If the new
  // category is non-ledger the server deleted the linked transaction, so
  // the row must drop out of the Actuals list; otherwise just refresh the
  // visible vendor / badge so it doesn't sit there in its old form.
  // (User 2026-05-30: edits from the Actuals tab didn't update the row,
  // and a doc recategorised out of actuals stayed in the list.)
  function _docPatchActualsRows(uid, d) {
    const nonLedger = _DOC_NON_LEDGER_CATS.has((d.category || '').toLowerCase());
    let removedAny = false;
    // Collect target rows BOTH via the doc badge (matched bank rows) AND via
    // the row-level data-doc-id (doc-source rows whose badge may be absent or
    // shaped differently) — patching only by badge missed rows, so edits from
    // the popup didn't show without a refresh. (User 2026-06-11.)
    const _targets = new Map();
    document.querySelectorAll('.actuals-doc-badge[data-doc-id="' + uid + '"]').forEach(b => {
      const tr = b.closest('.actuals-txn-row');
      if (tr) _targets.set(tr, b);
    });
    document.querySelectorAll('.actuals-txn-row[data-doc-id="' + uid + '"]').forEach(tr => {
      if (!_targets.has(tr)) _targets.set(tr, tr.querySelector('.actuals-doc-badge'));
    });
    _targets.forEach((badge, trow) => {
      if (!trow) return;
      if (nonLedger) {
        removedAny = true;
        trow.style.transition = 'opacity .3s';
        trow.style.opacity = '0';
        setTimeout(() => { trow.remove(); if (typeof _actualsRecountStats === 'function') _actualsRecountStats(); }, 300);
      } else {
        const vEl = trow.querySelector('.actuals-txn-vendor');
        if (vEl && d.vendor) { vEl.textContent = d.vendor; vEl.title = d.vendor; }
        trow.dataset.note    = d.note || '';
        trow.dataset.txnDate = d.doc_date || trow.dataset.txnDate;
        // Update the VISIBLE date cell too — previously only the data attr was
        // patched, so an edited receipt date didn't show until a page refresh.
        // (User 2026-06-03.)
        if (d.doc_date) {
          const dEl = trow.querySelector('.actuals-txn-date');
          if (dEl) dEl.textContent = d.doc_date;
        }
        if (d.amount != null) {
          trow.dataset.amount = d.amount || 0;
          const amtEl = trow.querySelector('.actuals-txn-amt');
          if (amtEl) {
            const neg = amtEl.textContent.trim().charAt(0) === '−';
            amtEl.textContent = (neg ? '−' : '+') + '$' +
              Number(d.amount || 0).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2});
          }
        }
        trow.dataset.sortCard4 = d.card_last4 || '';
        const c4El = trow.querySelector('.actuals-txn-card4');
        if (c4El) {
          c4El.textContent = d.card_last4 ? ('••' + d.card_last4) : '';
          c4El.style.display = d.card_last4 ? '' : 'none';
        }
        if (d.filed_path && badge) {
          const nm = (d.filed_path.split('/').pop() || '');
          badge.textContent = '📎 ' + (d.category || 'doc') + ' · '
            + (nm.length > 14 ? nm.slice(0, 14) + '…' : nm);
        }
      }
    });
    if (removedAny && typeof _actualsRecountStats === 'function') _actualsRecountStats();
  }

  // Remove/unlink the Actuals rows backed by a doc that was just deleted —
  // mirrors what the server did: the doc's own row disappears, a matched bank
  // row is unlinked (keeps its coding) and reverts to "needs receipt". No
  // reload. (User 2026-06-11.)
  function _docRemoveActualsRowsForDoc(uid) {
    const seen = new Set();
    const handle = (trow) => {
      if (!trow || seen.has(trow)) return;
      seen.add(trow);
      if ((trow.dataset.source || '') === 'doc_upload') {
        const banner = document.querySelector('.actuals-suggested-banner[data-tid="' + trow.dataset.tid + '"]');
        if (banner) banner.remove();
        trow.style.transition = 'opacity .3s';
        trow.style.opacity = '0';
        setTimeout(() => trow.remove(), 300);
      } else {
        trow.querySelectorAll('.actuals-doc-badge').forEach(b => b.remove());
        trow.dataset.hasDoc = '0';
        trow.dataset.matchStatus = 'unmatched';
        trow.dataset.docId = '';
        const banner = document.querySelector('.actuals-suggested-banner[data-tid="' + trow.dataset.tid + '"]');
        if (banner) banner.remove();
      }
    };
    document.querySelectorAll('.actuals-txn-row[data-doc-id="' + uid + '"]').forEach(handle);
    document.querySelectorAll('.actuals-doc-badge[data-doc-id="' + uid + '"]').forEach(b => handle(b.closest('.actuals-txn-row')));
    setTimeout(() => { if (typeof window._actualsRecountStats === 'function') window._actualsRecountStats(); }, 350);
  }

  // ── Docs Trash modal (restore / purge soft-deleted docs) ──────────────
  window.openDocsTrash = function () {
    document.getElementById('docs-trash-overlay').classList.remove('hidden');
    _loadDocsTrash();
  };
  window.closeDocsTrash = function (e) {
    if (e && e.target && e.target.id !== 'docs-trash-overlay') return;
    document.getElementById('docs-trash-overlay').classList.add('hidden');
  };
  function _trashEsc(s) {
    return String(s == null ? '' : s).replace(/[&<>"]/g,
      c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
  }
  async function _loadDocsTrash() {
    const body = document.getElementById('docs-trash-body');
    body.innerHTML = '<div style="padding:24px;text-align:center;color:var(--text-muted)">Loading…</div>';
    try {
      const d = await (await fetch('/docs/' + PROJ_ID + '/trash', {cache: 'no-store'})).json();
      const rows = d.trash || [];
      if (!rows.length) {
        body.innerHTML = '<div style="padding:24px;text-align:center;color:var(--text-muted)">Trash is empty.</div>';
        return;
      }
      body.innerHTML = rows.map(r => {
        const amt = (typeof r.amount === 'number')
          ? ('$' + r.amount.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})) : '';
        const when = r.deleted_at ? new Date(r.deleted_at).toLocaleString(undefined,
          {month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit'}) : '';
        return '<div style="border:1px solid var(--border);border-radius:10px;padding:10px 13px;margin-bottom:8px;display:flex;gap:10px;align-items:center;flex-wrap:wrap">'
          + '<div style="min-width:0;flex:1">'
          +   '<div style="font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + _trashEsc(r.filename || ('Upload #' + r.id)) + '</div>'
          +   '<div style="font-size:.72rem;color:var(--text-muted)">'
          +     _trashEsc(r.category || 'doc') + (r.vendor ? (' · ' + _trashEsc(r.vendor)) : '')
          +     (amt ? (' · ' + amt) : '') + (when ? (' · deleted ' + when) : '') + '</div>'
          + '</div>'
          + '<button type="button" class="btn btn-xs btn-primary" onclick="docsTrashRestore(' + r.id + ')">↩ Restore</button>'
          + '<button type="button" class="btn btn-xs" style="color:#e05555" onclick="docsTrashPurge(' + r.id + ')">Delete forever</button>'
          + '</div>';
      }).join('');
    } catch (err) {
      body.innerHTML = '<div style="padding:24px;color:#e05555">Could not load the Trash: ' + _trashEsc(err.message) + '</div>';
    }
  }
  window.docsTrashRestore = async function (uid) {
    try {
      const r = await fetch('/docs/upload/' + uid + '/restore', {method: 'POST'});
      const j = await r.json().catch(() => ({}));
      if (!r.ok) { alert('Restore failed: ' + (j.error || r.status)); return; }
      _loadDocsTrash();
      alert('Restored.' + (j.txn_recreated ? ' Its Actuals row was recreated.' : '')
            + ' Reload the page to see it back in the lists.');
    } catch (e) { alert('Restore error: ' + e.message); }
  };
  window.docsTrashPurge = async function (uid) {
    if (!confirm('Permanently delete this document record? This cannot be undone. (The Dropbox copies stay in /_TRASH/ until the 30-day purge.)')) return;
    try {
      const r = await fetch('/docs/upload/' + uid + '/purge', {method: 'POST'});
      const j = await r.json().catch(() => ({}));
      if (!r.ok) { alert('Purge failed: ' + (j.error || r.status)); return; }
      _loadDocsTrash();
    } catch (e) { alert('Purge error: ' + e.message); }
  };

  // Persist the current modal's edits and patch the affected list row(s)
  // in place. Returns the /update response (or null on failure). Shared by
  // the Save button AND prev/next auto-save. Does NOT close the modal —
  // callers decide. 2026-05-30.
  window._docDetailCommit = async function () {
    if (!_docDetailUid) return null;
    const uid = _docDetailUid;   // capture — nav may change _docDetailUid after
    const payload = {
      vendor:   _docDetailVendorValue(),
      amount:   _parseMoney(document.getElementById('docDetailAmount').value),
      original_amount:   _parseMoney(document.getElementById('docDetailOrigAmount').value),
      original_currency: document.getElementById('docDetailOrigCurrency').value,
      doc_date: document.getElementById('docDetailDocDate').value,
      category: document.getElementById('docDetailCategory').value,
      note:     document.getElementById('docDetailNote').value,
      doc_number: document.getElementById('docDetailDocNum').value,
      card_last4: document.getElementById('docDetailCard4').value,
      subtotal: _parseMoney(document.getElementById('docDetailSubtotal').value),
      tax:      _parseMoney(document.getElementById('docDetailTax').value),
      tip:      _parseMoney(document.getElementById('docDetailTip').value),
      merchant_address: document.getElementById('docDetailMerchAddr').value,
      merchant_phone:   document.getElementById('docDetailMerchPhone').value,
      crew_member_id: document.getElementById('docDetailCrewId').value || null,
      location_id:    document.getElementById('docDetailLocationId').value || null,
    };
    const r = await fetch('/docs/upload/' + uid + '/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(payload),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) { alert('Save failed: ' + (d.error || r.status)); return null; }
    // Mark the form clean (so an immediate prev/next won't re-save).
    if (uid === _docDetailUid) _docDetailOpenSig = _docDetailSig();
    _docPatchDocsRow(uid, d);
    _docPatchActualsRows(uid, d);
    // Re-apply Docs view + sort so the edited row re-files into the right
    // place under the CURRENT subtab/search/sort — without a reload.
    if (typeof _docsApplyView === 'function') _docsApplyView();
    if (typeof _docsSort === 'function') _docsSort();
    // Keep the People-tab packet completeness in lockstep (crew link / type
    // changes can complete or break a packet). (User 2026-06-01.)
    if (typeof _buildPacketView === 'function') _buildPacketView();
    if (typeof _buildPeopleSummary === 'function') _buildPeopleSummary().catch(()=>{});
    if (typeof _buildLocationDocs === 'function') _buildLocationDocs();
    if (typeof _buildVendorView === 'function') _buildVendorView();
    return d;
  };

  document.getElementById('docDetailSaveBtn')?.addEventListener('click', async () => {
    if (!_docDetailUid) return;
    const btn = document.getElementById('docDetailSaveBtn');
    btn.disabled = true; btn.textContent = 'Saving…';
    try {
      const d = await _docDetailCommit();
      if (d) closeDocDetail();
    } catch (e) {
      alert('Save error: ' + e.message);
    } finally {
      btn.disabled = false; btn.textContent = 'Save';
    }
  });

  // Render the two-line "Mon D / YYYY" date cell (or em-dash) from an
  // ISO yyyy-mm-dd string, parsed as a local date to avoid TZ drift.
  function _fmtDocDateCell(iso) {
    if (!iso) return '<span style="color:var(--text-muted);font-style:italic;font-size:11px">—</span>';
    const p = String(iso).split('-').map(Number);
    if (p.length < 3 || !p[0]) return '<span style="color:var(--text-muted);font-style:italic;font-size:11px">—</span>';
    const dt = new Date(p[0], (p[1]||1)-1, p[2]||1);
    const md = dt.toLocaleDateString(undefined, {month:'short', day:'numeric'});
    return '<div style="color:var(--text);font-weight:500">' + md + '</div>'
         + '<div style="color:var(--text-muted);font-size:11px">' + p[0] + '</div>';
  }

  // Stash the exact docs-list view state (subtab, search, sort, scroll) so
  // that when the standalone editor sends the user back to ?tab=docs we can
  // drop them right where they were. (User 2026-07 — "take me back to exactly
  // where I was in the list.") Fail-open: any DOM gap just stores blanks.
  function _docsStashListState() {
    try {
      const sortBy  = document.getElementById('docs-sort-by');
      const sortDir = document.getElementById('docs-sort-dir');
      const search  = document.getElementById('docs-search');
      sessionStorage.setItem('fpDocsListState', JSON.stringify({
        tab:    (typeof _docsActiveTab !== 'undefined' ? _docsActiveTab : 'all'),
        search: search ? search.value : '',
        sort:   { by: sortBy ? sortBy.value : '', dir: (sortDir ? sortDir.dataset.dir : '') || 'desc' },
        scroll: window.scrollY || 0,
        ts:     Date.now(),
      }));
    } catch (e) { /* sessionStorage unavailable — skip, editor still opens */ }
  }

  // Click-to-open binding: the row is the click target unless the user
  // clicked one of the inline buttons (delete, retry) or the editable
  // filename span (which has its own contenteditable handler).
  document.getElementById('docsHistory')?.addEventListener('click', (e) => {
    if (e.target.closest('.doc-delete-btn')) return;
    if (e.target.closest('.doc-retry-btn')) return;
    if (e.target.closest('.doc-dup-keep-btn')) return;
    if (e.target.closest('.doc-dup-confirm-btn')) return;
    if (e.target.closest('.doc-dup-compare-btn')) return;
    if (e.target.closest('.editable-filename')) return;
    // The small "quick view" icon keeps the old popup for users who prefer
    // it; let its own onclick handle it and stop here. (User 2026-07.)
    if (e.target.closest('.doc-quickview-btn')) return;
    const row = e.target.closest('.doc-row');
    if (!row) return;
    const uid = parseInt(row.dataset.uploadId);
    if (!uid) return;
    // For a flagged (pending) duplicate, the whole row opens the side-by-side
    // Compare group — that's the review action you're reaching for, and it
    // removes the "missed the small button → opened the doc" misclick.
    // (User 2026-05-29.) The detail panel is still reachable from inside it.
    if (row.dataset.dupPending === '1' && typeof docsOpenDupGroup === 'function') {
      docsOpenDupGroup(uid);
      return;
    }
    // PRIMARY interface is now the standalone editor, not the popup. (User
    // 2026-07 — "this should be our primary interface not a secondary click.")
    // Stash list state first so Save/close returns here with the same filter,
    // sort and scroll position. PROJ_ID is baked in globally; fall back to the
    // path if it's ever missing.
    _docsStashListState();
    let pid = (typeof PROJ_ID !== 'undefined' && PROJ_ID) ? PROJ_ID : null;
    if (!pid) { const m = location.pathname.match(/\/projects\/(\d+)/); pid = m ? m[1] : ''; }
    location.href = '/projects/' + pid + '/docs/' + uid + '/editor';
  });
})();
