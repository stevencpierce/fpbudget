// Extracted from a templates/budget.html inline block (audit M9, 2026-07-20).
// Jinja values arrive via window.__BJ (set by the inline preamble before this
// script tag). Classic script, document order — semantics identical to inline.

      // Show the classic-list note unless the user dismissed it before.
      (function(){
        try {
          if (localStorage.getItem('fpDocsClassicNoteDismissed') !== '1') {
            var _n = document.getElementById('docsClassicNote');
            if (_n) _n.style.display = 'flex';
          }
        } catch(e) {
          var _n2 = document.getElementById('docsClassicNote');
          if (_n2) _n2.style.display = 'flex';
        }
      })();
    