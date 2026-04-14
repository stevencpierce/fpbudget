// ── Drag-drop reorder for budget lines (CSP-safe) ──────────────────────────
// Requires Sortable.min.js loaded before this file.
// Uses the existing /line/reorder endpoint (scoped to same account_code).

(function() {
  'use strict';

  if (typeof Sortable === 'undefined') {
    console.warn('[DragReorder] Sortable.js not loaded');
    return;
  }

  if (typeof PID === 'undefined' || typeof BID === 'undefined') {
    console.warn('[DragReorder] PID/BID not defined on this page');
    return;
  }

  var REORDER_URL = '/projects/' + PID + '/budget/' + BID + '/line/reorder';

  function initSortables() {
    // One Sortable instance per section tbody
    document.querySelectorAll('.line-table tbody').forEach(function(tbody) {
      // Skip if already initialized
      if (tbody.dataset.sortableInit === '1') return;
      tbody.dataset.sortableInit = '1';

      new Sortable(tbody, {
        handle: '.line-number',
        animation: 150,
        draggable: '.line-row',
        filter: '.kit-fee-row, .sec-subtotal-row, .sec-total-row, tfoot tr',
        // Kit fees belong to their parent labor line — moving them independently is not supported
        // Also prevent dragging into footer / subtotal areas
        ghostClass: 'sortable-ghost',
        dragClass: 'sortable-drag',
        chosenClass: 'sortable-chosen',

        onEnd: function(evt) {
          var row = evt.item;
          // Kit-fee rows should never be draggable; if one slipped through, bail
          if (row.classList.contains('kit-fee-row')) return;

          // Skip if position didn't change
          if (evt.oldIndex === evt.newIndex) return;

          var id = parseInt(row.dataset.id);
          if (!id) return;

          // Find the .line-row immediately before this one (skipping kit-fee children)
          var prev = row.previousElementSibling;
          while (prev && (prev.classList.contains('kit-fee-row') || !prev.classList.contains('line-row'))) {
            prev = prev.previousElementSibling;
          }
          var afterId = prev ? parseInt(prev.dataset.id) : null;

          // Track this edit so socket/polling doesn't clobber it
          if (typeof _myEditedLines !== 'undefined') _myEditedLines.add(id);

          fetch(REORDER_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ line_id: id, after_id: afterId }),
          }).then(function(r) {
            if (!r.ok) {
              console.error('[DragReorder] Reorder failed, reloading');
              location.reload();
              return;
            }
            // Move any kit-fee children that were separated from their parent
            _reattachKitFees(row);
            // Flash the moved row briefly
            row.style.transition = 'background .3s';
            row.style.background = 'rgba(37, 99, 235, .15)';
            setTimeout(function() { row.style.background = ''; }, 1000);
          }).catch(function(e) {
            console.error('[DragReorder] Network error:', e);
            location.reload();
          });
        },
      });
    });
  }

  // Kit fee rows follow their parent labor line. After a drag, any kit-fee-row
  // whose previous sibling is no longer its parent (by data-parent-id) moves
  // back next to the parent.
  function _reattachKitFees(movedRow) {
    var parentId = movedRow.dataset.id;
    // Find kit fees that reference this parent — move them right after the parent
    var kitFees = document.querySelectorAll('.kit-fee-row[data-parent-id="' + parentId + '"]');
    var anchor = movedRow;
    kitFees.forEach(function(kf) {
      if (kf.previousElementSibling !== anchor) {
        anchor.parentNode.insertBefore(kf, anchor.nextSibling);
      }
      anchor = kf;
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initSortables);
  } else {
    initSortables();
  }
})();
