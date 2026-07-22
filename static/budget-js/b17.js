// Extracted from a templates/budget.html inline block (audit M9, 2026-07-20).
// Jinja values arrive via window.__BJ (set by the inline preamble before this
// script tag). Classic script, document order — semantics identical to inline.

(function(){
  const PIDp = window.__BJ["b17_PIDp"];
  let _pp = null, _ppBusy = false;
  const esc = s => String(s==null?'':s).replace(/[<>&"]/g,c=>({'<':'&lt;','>':'&gt;','&':'&amp;','"':'&quot;'}[c]));
  const money = v => (v==null?'—':'$'+Number(v).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2}));
  const RT = {day_10:'10hr day',day_8:'8hr day',day_12:'12hr day',flat_day:'flat/day',flat_project:'flat',hourly:'hr',custom:'custom'};

  window.openPersonProfile = async function(cmid){
    if(!cmid) return;
    const m = document.getElementById('personPanel');
    m.classList.add('open'); m.setAttribute('aria-hidden','false');
    document.getElementById('ppMain').innerHTML = '<div style="color:var(--text-muted);padding:30px">Loading…</div>';
    document.getElementById('ppSide').innerHTML = '';
    try {
      const d = await fetch('/projects/'+PIDp+'/person/'+cmid+'/profile',{credentials:'same-origin'}).then(r=>r.json());
      if(!d.ok) throw new Error(d.error||'load failed');
      _pp = d; _pp._cmid = cmid; renderPP();
    } catch(e){ document.getElementById('ppMain').innerHTML = '<div style="color:#e0a13a;padding:30px">Could not load: '+esc(e.message)+'</div>'; }
  };
  window.closePersonPanel = function(){ const m=document.getElementById('personPanel'); m.classList.remove('open'); m.setAttribute('aria-hidden','true'); };

  function renderPP(){
    const id = _pp.identity||{}, deal=_pp.deal||[], docs=_pp.documents||[], comp=_pp.completeness||{}, pays=_pp.payments||[], reps=_pp.representation||[];
    const initials = (id.name||'?').split(/\s+/).map(x=>x[0]).join('').slice(0,2).toUpperCase();
    document.getElementById('ppAvatar').textContent = initials;
    document.getElementById('ppName').textContent = id.name||'Person';
    document.getElementById('ppSub').textContent = _ppSubText(id) || (deal[0]?deal[0].role:'');

    // ── MAIN: Deal + Documents + Payments ──
    let h = '';
    h += '<div class="pp-sec"><h4>Deal &amp; pay</h4>';
    if(!deal.length){ h+='<div style="color:var(--text-muted);font-size:.82rem">Not assigned to a budget line on the current budget.</div>'; }
    deal.forEach(x=>{
      h += '<div class="pp-deal"><div class="role">'+esc(x.role)+' <span style="font-weight:400;color:var(--text-muted);font-size:.75rem">· '+esc(x.account_code)+' '+esc(x.account_name)+'</span></div>'
        + '<div class="r"><span>Rate</span><b>'+money(x.rate)+' <span style="font-weight:400;color:var(--text-muted)">'+(RT[x.rate_type]||esc(x.rate_type||''))+'</span></b></div>'
        + '<div class="r"><span>Days</span><span>'+(x.days||0)+'</span></div>'
        + (x.kit_total?'<div class="r"><span>Kit fee</span><span>'+money(x.kit_total)+'</span></div>':'')
        + '<div class="r"><span>Budgeted</span><b>'+money(x.budgeted)+'</b></div>'
        + '<div class="r"><span>Actual paid</span><b>'+money(x.actual)+'</b></div>'
        + (x.mismatch?'<div class="pp-mis">⚠ Over budget by '+money(x.mismatch.delta)+' (budget '+money(x.mismatch.budgeted)+', actual '+money(x.mismatch.actual)+')</div>':'')
        + '</div>';
    });
    h += '</div>';

    h += '<div class="pp-sec"><h4>Documents</h4><div class="pp-badges">';
    [['contract','Contract'],['tax_form','Tax form'],['photo_id','ID'],['invoice','Invoice']].forEach(([k,lbl])=>{
      h += '<span class="pp-badge '+(comp[k]?'ok':'no')+'">'+(comp[k]?'✓ ':'○ ')+lbl+'</span>';
    });
    h += '</div>';
    h += '<button class="pp-btn" onclick="_ppUpload()">⤒ Upload document</button> <span style="font-size:.72rem;color:var(--text-muted)">or drag files anywhere onto this panel</span>';
    docs.forEach(dc=>{
      h += '<div class="pp-doc" onclick="closePersonPanel();window.openDocDetail('+dc.id+',null)">'
        + '<span class="cat">'+esc(dc.category||'doc')+'</span>'
        + '<span style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+esc(dc.vendor||dc.filename||('Doc #'+dc.id))+'</span>'
        + '<span style="color:var(--text-muted)">'+(dc.doc_date||'')+'</span>'
        + '<b>'+(dc.amount!=null?money(dc.amount):'')+'</b></div>';
    });
    if(!docs.length) h += '<div style="color:var(--text-muted);font-size:.8rem;margin-top:8px">No documents yet.</div>';
    h += '</div>';

    // Timecards slice 1 (User 2026-07.): timecards + invoices are BOTH
    // first-class. Which one this person is EXPECTED to submit depends on
    // their per-project employment_type: 'employee' → timecard first, THEN the
    // payments/invoices list; anyone else → invoices first, timecards after
    // (still shown, so loan-outs who happen to have timecards see them).
    // The timecards block is an empty container patched after an async fetch.
    const isEmployee = (id.employment_type === 'employee');
    const tcBlock = '<div class="pp-sec"><h4>Timecards</h4><div id="ppTcBox">'
      + '<div style="color:var(--text-muted);font-size:.8rem">Loading…</div></div></div>';
    let payBlock = '<div class="pp-sec"><h4>Timecards &amp; invoices <span style="font-weight:400;text-transform:none;color:var(--text-muted)">· payments total '+money(_pp.total_paid)+'</span></h4>';
    pays.forEach(p=>{
      payBlock += '<div class="pp-pay"'+(p.doc_upload_id?' onclick="closePersonPanel();window.openDocDetail('+p.doc_upload_id+',null)"':'')+'>'
        + '<span style="color:var(--text-muted)">'+(p.date||'')+'</span>'
        + '<span style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+esc(p.vendor||'')+'</span>'
        + '<span class="cat" style="font-size:.62rem;padding:1px 6px;border-radius:8px;background:var(--bg-input);color:var(--text-muted)">'+esc(p.source||'')+'</span>'
        + '<b>'+money(p.amount)+'</b></div>';
    });
    if(!pays.length) payBlock += '<div style="color:var(--text-muted);font-size:.8rem;margin-top:8px">No payments recorded.</div>';
    payBlock += '</div>';
    h += isEmployee ? (tcBlock + payBlock) : (payBlock + tcBlock);
    document.getElementById('ppMain').innerHTML = h;
    _ppLoadTimecards();   // async fetch → patches #ppTcBox

    // ── SIDE: personal info + classification + representation ──
    let s = '<div class="pp-sec"><h4>Details</h4>';
    s += '<div style="font-size:.68rem;color:var(--text-muted);margin-bottom:8px">Employment &amp; union are <b>per project</b>; email/phone/company travel with the person.</div>';
    s += '<div class="pp-field"><label>Employment type <span style="text-transform:none;color:var(--text-muted)">· this project</span></label><select id="ppEmp" onchange="_ppSaveField(\'employment_type\',this.value)"><option value="">—</option>'
      + ['loan_out:Loan-out','employee:Employee','vendor:Vendor'].map(o=>{const[v,l]=o.split(':');return '<option value="'+v+'"'+(id.employment_type===v?' selected':'')+'>'+l+'</option>';}).join('')+'</select></div>';
    s += '<div class="pp-field"><label>Union status</label><select id="ppUnion" onchange="_ppSaveField(\'union_status\',this.value)"><option value="">—</option>'
      + ['union:Union','non_union:Non-Union'].map(o=>{const[v,l]=o.split(':');return '<option value="'+v+'"'+(id.union_status===v?' selected':'')+'>'+l+'</option>';}).join('')+'</select></div>';
    s += '<div class="pp-field"><label>Email</label><input id="ppEmail" value="'+esc(id.email||'')+'" onblur="_ppSaveField(\'email\',this.value)"></div>';
    s += '<div class="pp-field"><label>Phone</label><input id="ppPhone" value="'+esc(id.phone||'')+'" onblur="_ppSaveField(\'phone\',this.value)"></div>';
    // Loan-out vendor · this project (User 2026-07.). A real-vendor dropdown that
    // replaces the old free-text "company/loan-out" field. Render synchronously
    // with just the current value + '—' + '➕ New vendor…'; the full vendor list
    // is patched in when the (lazy, cached) fetch lands — so the side panel never
    // waits on the network to paint. onchange handled by _ppVendorChange.
    s += '<div class="pp-field"><label>Loan-out vendor <span style="text-transform:none;color:var(--text-muted)">· this project</span></label>'
      + '<select id="ppLoanVendor" onchange="_ppVendorChange(this)">'
      + _ppVendorOptions(id.loan_out_vendor_id, id.loan_out_vendor_name)
      + '</select></div>';
    s += '<div class="pp-field"><label>Company <span style="text-transform:none;color:var(--text-muted)">· note</span></label><input id="ppCompany" value="'+esc(id.company||'')+'" onblur="_ppSaveField(\'company\',this.value)"></div>';
    s += '<div id="ppSaveStatus" style="font-size:.7rem;color:#4ade80;height:12px"></div></div>';
    if(reps.length){
      s += '<div class="pp-sec"><h4>Representation</h4>';
      reps.forEach(r=>{ s += '<div style="font-size:.82rem;margin-bottom:6px"><b>'+esc(r.name)+'</b> <span style="color:var(--text-muted)">'+esc(r.role_type||'')+'</span>'+(r.email?'<br><span style="color:var(--text-muted);font-size:.75rem">'+esc(r.email)+'</span>':'')+'</div>'; });
      s += '</div>';
    }
    document.getElementById('ppSide').innerHTML = s;
    _ppLoadVendors();   // async → patches the full vendor <option> list into #ppLoanVendor
  }

  // ── Header subtitle (User 2026-07.) ──────────────────────────────────────
  // Maps employment_type + union_status to the drawer subtitle. When the person
  // is a loan-out AND a vendor is linked, show 'Loan-out — <vendor> · <union>'.
  function _ppSubText(id){
    id = id || {};
    const et = {loan_out:'Loan-out',employee:'Employee',vendor:'Vendor'}[id.employment_type]||'';
    const us = {union:'Union',non_union:'Non-Union'}[id.union_status]||'';
    let lead = et;
    if(id.employment_type==='loan_out' && id.loan_out_vendor_name)
      lead = 'Loan-out — '+id.loan_out_vendor_name;
    return [lead,us].filter(Boolean).join(' · ');
  }

  // ── Loan-out vendor dropdown (User 2026-07.) ─────────────────────────────
  // Build the <select> option markup: '—', then every cached vendor (or, until
  // the fetch lands, just the currently-linked one so the value is preserved),
  // then '➕ New vendor…'. selId/selName = the person's current per-project link.
  function _ppVendorOptions(selId, selName){
    selId = (selId!=null ? String(selId) : '');
    let o = '<option value=""'+(selId?'':' selected')+'>—</option>';
    const cache = window._ppVendorsCache;
    if(Array.isArray(cache) && cache.length){
      cache.forEach(v=>{ const vid=String(v.id);
        o += '<option value="'+vid+'"'+(vid===selId?' selected':'')+'>'+esc(v.name)+'</option>'; });
      // Linked vendor missing from the list (inactive / not is_vendor) → keep it.
      if(selId && !cache.some(v=>String(v.id)===selId))
        o += '<option value="'+esc(selId)+'" selected>'+esc(selName||('Vendor #'+selId))+'</option>';
    } else if(selId){
      // Pre-fetch: preserve the current selection so the value isn't lost.
      o += '<option value="'+esc(selId)+'" selected>'+esc(selName||('Vendor #'+selId))+'</option>';
    }
    o += '<option value="__new__">➕ New vendor…</option>';
    return o;
  }
  // Lazily fetch + cache the vendor list, then patch the full option set into the
  // live #ppLoanVendor without losing its current value. pid derived from PIDp.
  async function _ppLoadVendors(){
    const sel = document.getElementById('ppLoanVendor');
    if(!sel) return;
    if(!Array.isArray(window._ppVendorsCache)){
      try{
        const d = await fetch('/projects/'+PIDp+'/vendors.json',{credentials:'same-origin'}).then(r=>r.json());
        window._ppVendorsCache = (d && d.ok && Array.isArray(d.vendors)) ? d.vendors : [];
      }catch(e){ window._ppVendorsCache = []; }
    }
    const s2 = document.getElementById('ppLoanVendor');   // may have re-rendered
    if(!s2) return;
    const cur = (_pp && _pp.identity) ? _pp.identity.loan_out_vendor_id : null;
    const curName = (_pp && _pp.identity) ? _pp.identity.loan_out_vendor_name : null;
    s2.innerHTML = _ppVendorOptions(cur, curName);
  }
  // Dropdown onchange: '__new__' → prompt + create-and-link; '' → clear; id → link.
  window._ppVendorChange = async function(sel){
    if(!_pp || !sel) return;
    const st = document.getElementById('ppSaveStatus');
    if(sel.value === '__new__'){
      const nm = (prompt('New vendor name') || '').trim();
      if(!nm){ // revert to previous selection
        sel.value = (_pp.identity && _pp.identity.loan_out_vendor_id!=null)
          ? String(_pp.identity.loan_out_vendor_id) : '';
        return;
      }
      if(st) st.textContent='Saving…';
      try{
        const r = await fetch('/projects/'+PIDp+'/person/'+_pp._cmid+'/update',
          {method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},
           body:JSON.stringify({new_vendor_name:nm})});
        const j = await r.json();
        const nv = j && j.loan_out_vendor;
        if(!nv) throw new Error('create failed');
        // Insert into the cache + a live <option>, then select it.
        if(!Array.isArray(window._ppVendorsCache)) window._ppVendorsCache = [];
        window._ppVendorsCache.push({id:nv.id, name:nv.name});
        window._ppVendorsCache.sort((a,b)=>(a.name||'').toLowerCase().localeCompare((b.name||'').toLowerCase()));
        if(_pp.identity){ _pp.identity.loan_out_vendor_id=nv.id; _pp.identity.loan_out_vendor_name=nv.name; }
        sel.innerHTML = _ppVendorOptions(nv.id, nv.name);
        document.getElementById('ppSub').textContent = _ppSubText(_pp.identity)||'';
        if(st){ st.textContent='Saved ✓'; setTimeout(()=>st.textContent='',1500); }
      }catch(e){ if(st) st.textContent='Save failed';
        sel.value = (_pp.identity && _pp.identity.loan_out_vendor_id!=null)
          ? String(_pp.identity.loan_out_vendor_id) : ''; }
      return;
    }
    // Normal id or '' (clear) → same endpoint _ppSaveField uses.
    const v = sel.value || '';
    if(st) st.textContent='Saving…';
    try{
      await fetch('/projects/'+PIDp+'/person/'+_pp._cmid+'/update',
        {method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},
         body:JSON.stringify({loan_out_vendor_id:v})});
      if(_pp.identity){
        _pp.identity.loan_out_vendor_id = v ? parseInt(v,10) : null;
        const hit = (window._ppVendorsCache||[]).find(x=>String(x.id)===v);
        _pp.identity.loan_out_vendor_name = v ? (hit ? hit.name : _pp.identity.loan_out_vendor_name) : null;
      }
      document.getElementById('ppSub').textContent = _ppSubText(_pp.identity)||'';
      if(st){ st.textContent='Saved ✓'; setTimeout(()=>st.textContent='',1500); }
    }catch(e){ if(st) st.textContent='Save failed'; }
  };

  // ── Timecards slice 1 (User 2026-07.) ────────────────────────────────────
  // Fetch this person's timecards + expected (missing) schedule weeks, then
  // render into the #ppTcBox placeholder. Fail-quiet: a fetch error just shows
  // a muted note — timecards are advisory, never block the drawer.
  async function _ppLoadTimecards(){
    if(!_pp) return;
    const box = document.getElementById('ppTcBox');
    if(!box) return;
    try{
      const d = await fetch('/projects/'+PIDp+'/person/'+_pp._cmid+'/timecards',{credentials:'same-origin'}).then(r=>r.json());
      if(!d.ok) throw new Error(d.error||'load failed');
      _ppRenderTimecards(d);
    }catch(e){ box.innerHTML = '<div style="color:var(--text-muted);font-size:.78rem">Could not load timecards.</div>'; }
  }
  const _TCST = {draft:'Draft',submitted:'Submitted',approved:'Approved'};
  function _ppRenderTimecards(d){
    const box = document.getElementById('ppTcBox');
    if(!box) return;
    const cards = d.timecards||[], expected = d.expected||[];
    let x = '';
    // Existing timecards — click shows a per-day breakdown from days_json.
    cards.forEach(t=>{
      const st = (t.status||'draft');
      x += '<div class="pp-tc" onclick="_ppShowTimecard('+t.id+')" title="Click for day breakdown">'
        + '<span style="flex:1;min-width:0">wk ending '+esc(t.week_ending)+' · '+(t.days_count||0)+' day'+((t.days_count===1)?'':'s')+'</span>'
        + '<b>'+money(t.gross)+'</b>'
        + '<span class="chip '+st+'">'+(_TCST[st]||st)+'</span></div>';
    });
    // Expected weeks with no timecard yet — amber "needs timecard" + Generate.
    expected.forEach(w=>{
      x += '<div class="pp-tc-need">'
        + '<span class="pp-tc-badge">⚠ Needs timecard (wk of '+esc(w.week_ending)+')</span>'
        + '<span style="color:var(--text-muted);font-size:.72rem">'+(w.days_count||0)+' day'+((w.days_count===1)?'':'s')+'</span>'
        + '<button class="pp-tc-gen" onclick="_ppGenTimecard(\''+esc(w.week_ending)+'\')">Generate</button></div>';
    });
    if(!cards.length && !expected.length)
      x = '<div style="color:var(--text-muted);font-size:.8rem">No timecards or scheduled weeks.</div>';
    box.innerHTML = x;
    // Stash the loaded cards so _ppShowTimecard can read their days_json.
    _pp._timecards = cards;
  }
  window._ppGenTimecard = async function(weekEnding){
    if(!_pp || _ppBusy) return;
    _ppBusy = true;
    const box = document.getElementById('ppTcBox');
    if(box) box.style.opacity = '.5';
    try{
      await fetch('/projects/'+PIDp+'/person/'+_pp._cmid+'/timecards/generate',
        {method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},
         body:JSON.stringify({week_ending:weekEnding})});
    }catch(e){ /* fail-quiet */ }
    finally{ _ppBusy = false; if(box) box.style.opacity = ''; }
    _ppLoadTimecards();   // re-fetch + re-render the block
  };
  window._ppShowTimecard = function(tid){
    const t = (_pp && _pp._timecards || []).find(c=>c.id===tid);
    if(!t){ return; }
    // Slice 1: no grid editor yet — just alert the day breakdown. We only have
    // day_count/gross client-side here, so re-fetch the full card for its days.
    fetch('/projects/'+PIDp+'/timecards/'+tid+'/update',
      {method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},body:'{}'})
      .then(r=>r.json()).then(d=>{
        if(!d.ok || !d.timecard){ alert('Timecard #'+tid); return; }
        const tc = d.timecard;
        const lines = (tc.days||[]).map(x=>'  '+x.date+'  '+(x.day_type||'work')+'  ×'+x.mult
          + (x.ot_amount?('  +$'+x.ot_amount+' OT'):'')).join('\n');
        alert('Timecard — wk ending '+tc.week_ending+' ('+(tc.status||'draft')+')\n'
          + 'Rate: '+money(tc.rate)+'/day\n\n'+(lines||'  (no days)')+'\n\nGross: '+money(tc.gross));
      }).catch(()=>alert('Timecard #'+tid));
  };

  window._ppSaveField = async function(field, value){
    if(!_pp) return;
    const st = document.getElementById('ppSaveStatus'); if(st) st.textContent='Saving…';
    try{
      await fetch('/projects/'+PIDp+'/person/'+_pp._cmid+'/update',{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},body:JSON.stringify({[field]:value})});
      if(_pp.identity) _pp.identity[field]=value;
      document.getElementById('ppSub').textContent=_ppSubText(_pp.identity)||'';
      if(st){ st.textContent='Saved ✓'; setTimeout(()=>st.textContent='',1500); }
    }catch(e){ if(st) st.textContent='Save failed'; }
  };

  async function _ppUploadFiles(files){
    if(!_pp || !files || !files.length || _ppBusy) return;
    _ppBusy=true;
    const st=document.getElementById('ppSaveStatus');
    let done=0;
    try{
      for(const f of files){
        if(st){ st.textContent='Uploading '+(done+1)+'/'+files.length+'… '+(f.name||''); st.style.color='var(--text-muted)'; }
        const fd = new FormData(); fd.append('file', f);
        const r = await fetch('/projects/'+PIDp+'/docs/upload',{method:'POST',credentials:'same-origin',body:fd});
        const j = await r.json().catch(()=>({}));
        const uid = j.id || j.uid || j.upload_id || (j.upload&&j.upload.id);
        if(uid) await fetch('/docs/upload/'+uid+'/update',{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},body:JSON.stringify({crew_member_id:_pp._cmid})});
        done++;
      }
    } finally { _ppBusy=false; }
    window.openPersonProfile(_pp._cmid);   // refresh
  }
  window._ppUpload = function(){
    if(!_pp) return;
    const inp = document.getElementById('ppUploadInput');
    inp.onchange = () => { const files=[...inp.files]; inp.value=''; _ppUploadFiles(files); };
    inp.click();
  };
  // Drag-and-drop upload onto the whole drawer → files attach to this person.
  (function(){
    const panel = document.getElementById('personPanel');
    const drawer = panel && panel.querySelector('.pp-drawer');
    if(!drawer) return;
    let _dragDepth=0;
    const showDrop = on => { drawer.style.outline = on ? '3px dashed #7c3aed' : ''; drawer.style.outlineOffset = on?'-6px':''; };
    drawer.addEventListener('dragenter', e=>{ if(e.dataTransfer && [...e.dataTransfer.types].includes('Files')){ e.preventDefault(); _dragDepth++; showDrop(true);} });
    drawer.addEventListener('dragover', e=>{ if(e.dataTransfer && [...e.dataTransfer.types].includes('Files')) e.preventDefault(); });
    drawer.addEventListener('dragleave', e=>{ _dragDepth--; if(_dragDepth<=0){ _dragDepth=0; showDrop(false);} });
    drawer.addEventListener('drop', e=>{
      if(!(e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files.length)) return;
      e.preventDefault(); _dragDepth=0; showDrop(false);
      _ppUploadFiles([...e.dataTransfer.files]);
    });
  })();

  // Entry point: click a person's NAME in the People tab.
  document.addEventListener('click', function(e){
    const nameCell = e.target.closest('#tab-contacts .cs-field[data-field="name"]');
    if(!nameCell) return;
    if(e.target.closest('.cs-docs-toggle') || e.target.closest('.cs-omit-badge')) return; // don't hijack existing controls
    const docsEl = nameCell.querySelector('.cs-docs[data-crew-member-id]');
    const cmid = docsEl && docsEl.dataset.crewMemberId;
    if(cmid){ e.preventDefault(); window.openPersonProfile(cmid); }
  });
  document.addEventListener('keydown', function(e){ if(e.key==='Escape' && document.getElementById('personPanel').classList.contains('open')) closePersonPanel(); });
})();
