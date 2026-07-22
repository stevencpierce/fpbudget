// Extracted from a templates/budget.html inline block (audit M9, 2026-07-20).
// Jinja values arrive via window.__BJ (set by the inline preamble before this
// script tag). Classic script, document order — semantics identical to inline.

(function(){
  const PIDm = window.__BJ["b18_PIDm"];
  const fmt = v => '$' + Number(v||0).toLocaleString('en-US',{minimumFractionDigits:0,maximumFractionDigits:0});
  async function loadMismatches(){
    try{
      const d = await fetch('/projects/'+PIDm+'/budget/'+BIDm+'/mismatches',{credentials:'same-origin'}).then(r=>r.json());
      if(!d || !d.ok) return;
      (d.mismatches||[]).forEach(m=>{
        const row = document.querySelector('.line-row[data-id="'+m.line_id+'"]');
        if(!row) return;
        const cell = row.querySelector('.col-desc') || row.querySelector('td');
        if(!cell || cell.querySelector('.line-mismatch-badge')) return;
        const b = document.createElement('span');
        b.className = 'line-mismatch-badge';
        b.textContent = '⚠';
        b.title = 'Over budget: actual '+fmt(m.actual)+' vs budget '+fmt(m.budgeted)
                + (m.person?(' — '+m.person):'') + ' (Δ '+fmt(m.delta)+'). Click for detail.';
        b.style.cssText = 'cursor:pointer;color:#e0a13a;margin-left:6px;font-size:.85rem;font-weight:700';
        b.addEventListener('click', function(e){
          e.stopPropagation();
          if(m.crew_member_id && typeof window.openPersonProfile==='function') window.openPersonProfile(m.crew_member_id);
        });
        cell.appendChild(b);
      });
    }catch(e){}
  }
  if(document.readyState !== 'loading') setTimeout(loadMismatches, 700);
  else document.addEventListener('DOMContentLoaded', ()=>setTimeout(loadMismatches, 700));
})();
