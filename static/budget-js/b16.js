// Extracted from a templates/budget.html inline block (audit M9, 2026-07-20).
// Jinja values arrive via window.__BJ (set by the inline preamble before this
// script tag). Classic script, document order — semantics identical to inline.

(function(){
  const PIDr = window.__BJ["b16_PIDr"];
  let _reconLine = null, _reconTxns = [], _reconSel = new Set(), _reconBusy = false;

  function rToast(msg, color){
    const t = document.getElementById('recon-toast');
    if(!t) return; t.textContent = msg; t.style.background = color || '#16a34a';
    t.style.display='block'; clearTimeout(t._h); t._h = setTimeout(()=>{t.style.display='none';}, 3200);
  }
  const esc = s => String(s==null?'':s).replace(/[<>&"]/g,c=>({'<':'&lt;','>':'&gt;','&':'&amp;','"':'&quot;'}[c]));
  const money = v => (v==null?'':'$'+Number(v).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2}));
  const SRC_LABEL = {reconciled:'QBO ✓', qbo_sync:'QBO', csv_import:'CSV', doc_upload:'Receipt', manual_entry:'Manual'};

  window.openReconcileLine = async function(lid){
    if(!lid) return;
    const modal = document.getElementById('reconcileModal');
    modal.classList.add('open'); modal.setAttribute('aria-hidden','false');
    document.getElementById('recon-tiles').innerHTML = '<div class="recon-empty">Loading…</div>';
    document.getElementById('recon-line-name').textContent = 'Reconcile line';
    document.getElementById('recon-line-sub').textContent = '';
    _reconSel.clear();
    try {
      const r = await fetch('/projects/'+PIDr+'/actuals/line/'+lid+'/transactions.json', {credentials:'same-origin'});
      const j = await r.json();
      if(!j.ok) throw new Error(j.error||'load failed');
      _reconLine = j.line; _reconTxns = j.transactions || [];
      renderRecon();
    } catch(e){
      document.getElementById('recon-tiles').innerHTML = '<div class="recon-empty">Could not load: '+esc(e.message)+'</div>';
    }
  };
  window.closeReconcile = function(){
    const m = document.getElementById('reconcileModal');
    m.classList.remove('open'); m.setAttribute('aria-hidden','true');
  };

  function renderRecon(){
    const L = _reconLine || {};
    document.getElementById('recon-line-name').textContent = (L.name||'Line');
    const total = _reconTxns.reduce((a,t)=>a+(t.is_expense===false?-(t.amount||0):(t.amount||0)),0);
    document.getElementById('recon-line-sub').textContent =
      (L.section_name||'')+' · code '+(L.code||'')+' · '+_reconTxns.length+' item'+(_reconTxns.length!==1?'s':'')+' · '+money(total);
    const host = document.getElementById('recon-tiles');
    if(!_reconTxns.length){ host.innerHTML = '<div class="recon-empty">Nothing is coded to this line.</div>'; updateMergeZone(); return; }
    host.innerHTML = _reconTxns.map(function(t){
      var thumb;
      if(t.doc_upload_id && t.has_image){
        thumb = '<img loading="lazy" src="/docs/upload/'+t.doc_upload_id+'/raw" onerror="this.parentNode.innerHTML=&quot;<div class=\\&quot;ph\\&quot;>📄</div>&quot;">';
      } else if(t.doc_upload_id){
        thumb = '<div class="ph">📄<small>'+esc((t.doc_category||'doc').toUpperCase())+'</small></div>';
      } else {
        thumb = '<div class="ph">🏦<small>NO RECEIPT</small></div>';
      }
      var openable = t.doc_upload_id ? 'onclick="window.open(\'/docs/upload/'+t.doc_upload_id+'/raw\',\'_blank\')"' : '';
      var hasReceipt = !!t.doc_upload_id && t.source!=='doc_upload';
      return '<div class="recon-tile" data-tid="'+t.id+'">'
        + '<div class="recon-thumb" '+openable+'>'
        +   '<input type="checkbox" class="recon-chk" data-tid="'+t.id+'" onclick="event.stopPropagation();reconToggle('+t.id+',this.checked)">'
        +   '<span class="recon-srcbadge">'+esc(SRC_LABEL[t.source]||t.source||'')+'</span>'+thumb
        + '</div>'
        + '<div class="recon-body">'
        +   '<div class="recon-vend">'+esc(t.vendor||'(no vendor)')+'</div>'
        +   '<div class="recon-amt">'+(t.is_expense===false?'<span style="color:#22c55e">+</span>':'')+money(t.amount)+'</div>'
        +   '<div class="recon-meta">'+esc(t.date||'')+(t.qbo_txn_id?' · QBO #'+esc(t.qbo_txn_id):'')+'</div>'
        +   (t.doc_filename?'<div class="recon-meta" title="'+esc(t.doc_filename)+'">📎 '+esc(t.doc_filename.slice(0,34))+'</div>':'')
        + '</div>'
        + '<div class="recon-acts">'
        +   (hasReceipt?'<button onclick="reconUnmatch('+t.id+')" title="Detach the receipt and send it back to the matching queue. The charge stays.">↩ Unmatch</button>':'')
        +   '<button onclick="reconUncode('+t.id+')" title="Remove this from the line entirely (back to Needs coding).">⤺ Uncode</button>'
        + '</div>'
        + '</div>';
    }).join('');
    updateMergeZone();
  }

  window.reconToggle = function(tid, on){
    if(on){ _reconSel.add(tid); if(_reconSel.size>2){ var first=[..._reconSel][0]; _reconSel.delete(first); } }
    else _reconSel.delete(tid);
    document.querySelectorAll('#recon-tiles .recon-tile').forEach(function(el){
      var id=+el.dataset.tid; el.classList.toggle('sel', _reconSel.has(id));
      var cb=el.querySelector('.recon-chk'); if(cb) cb.checked=_reconSel.has(id);
    });
    updateMergeZone();
  };

  function updateMergeZone(){
    var z = document.getElementById('recon-merge-zone');
    var info = document.getElementById('recon-selinfo');
    if(_reconSel.size===2){
      var arr=[..._reconSel];
      info.textContent = 'Merge keeps the QBO/coded row and attaches the other’s receipt, then removes the duplicate.';
      z.innerHTML = '<button class="recon-btn primary" onclick="reconMerge('+arr[0]+','+arr[1]+')">⛓ Merge these 2</button>';
    } else {
      info.textContent = 'Tick two tiles to merge duplicates · click a thumbnail to open the doc';
      z.innerHTML = '';
    }
  }

  async function reconPost(url, body){
    if(_reconBusy) return null; _reconBusy = true;
    try{
      var r = await fetch(url,{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},body:JSON.stringify(body||{})});
      var j = await r.json().catch(function(){return {};});
      if(!r.ok || j.error){ rToast(j.error||('Failed ('+r.status+')'),'#b91c1c'); return null; }
      return j;
    } finally { _reconBusy = false; }
  }

  window.reconMerge = async function(a,b){
    var j = await reconPost('/projects/'+PIDr+'/actuals/transaction/merge', {tids:[a,b]});
    if(!j) return;
    _reconSel.clear(); rToast('Merged — receipt attached, duplicate removed.');
    await window.openReconcileLine(_reconLine.id);
  };
  window.reconUnmatch = async function(tid){
    var j = await reconPost('/projects/'+PIDr+'/actuals/transaction/'+tid+'/unmatch', {});
    if(!j) return;
    rToast('Receipt unmatched — back in the queue.');
    await window.openReconcileLine(_reconLine.id);
  };
  window.reconUncode = async function(tid){
    var j = await reconPost('/projects/'+PIDr+'/actuals/transaction/'+tid+'/set-coa', {account_code:''});
    if(!j) return;
    rToast('Uncoded — moved to Needs coding.');
    await window.openReconcileLine(_reconLine.id);
  };

  document.addEventListener('keydown', function(e){ if(e.key==='Escape' && document.getElementById('reconcileModal').classList.contains('open')) closeReconcile(); });
})();
