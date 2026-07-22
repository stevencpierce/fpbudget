// Extracted from templates/budget.html inline block 1 (audit M9, 2026-07-20).
// Jinja values arrive via window.__BJ (set by the inline preamble before this
// script tag). Classic script, document order — semantics identical to inline.

  function openApprovalHistory(){ var o=document.getElementById('appr-history-overlay'); if(o) o.style.display='flex'; }
  function closeApprovalHistory(){ var o=document.getElementById('appr-history-overlay'); if(o) o.style.display='none'; }
  document.addEventListener('keydown', function(e){ if(e.key==='Escape') closeApprovalHistory(); });
