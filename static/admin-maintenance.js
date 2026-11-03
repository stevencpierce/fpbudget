// Admin → Maintenance (Cleanup E, 2026-11-02). One generic runner for every
// card: builds the tool's URL from the card's inputs, previews (dry run) or
// runs it, and shows the response under the card. Destructive tools confirm
// first. The registry arrives as window.__MAINT_TOOLS (routes/admin_maintenance.py).
(function () {
  var TOOLS = {};
  (window.__MAINT_TOOLS || []).forEach(function (t) { TOOLS[t.id] = t; });

  // Plain confirm forms (the Dropbox scan) — same behaviour as admin.js.
  document.querySelectorAll('.confirm-form').forEach(function (form) {
    form.addEventListener('submit', function (ev) {
      if (!confirm(form.dataset.msg || 'Proceed?')) ev.preventDefault();
    });
  });

  function params(card, t) {
    var out = {}, missing = [];
    (t.params || []).forEach(function (p) {
      var el = card.querySelector('[name="' + p.name + '"]');
      var v = el ? (el.value || '').trim() : '';
      var required = p.kind === 'project' || p.kind === 'int' || p.kind === 'text';
      if (required && !v) missing.push(p.label);
      if (v) out[p.name] = v;
    });
    return { values: out, missing: missing };
  }

  function buildUrl(base, values, extraQuery) {
    var url = base, q = [];
    Object.keys(values).forEach(function (k) {
      var ph = '{' + k + '}';
      if (url.indexOf(ph) >= 0) url = url.replace(ph, encodeURIComponent(values[k]));
      else q.push(encodeURIComponent(k) + '=' + encodeURIComponent(values[k]));
    });
    if (extraQuery) q.push(extraQuery);
    if (q.length) url += (url.indexOf('?') >= 0 ? '&' : '?') + q.join('&');
    return url;
  }

  function show(card, text, isErr) {
    var out = card.querySelector('[data-role="out"]');
    out.textContent = text;
    out.classList.add('show');
    out.classList.toggle('err', !!isErr);
  }

  function setStatus(card, msg) {
    var s = card.querySelector('[data-role="status"]');
    if (s) s.textContent = msg || '';
  }

  async function call(card, method, url) {
    setStatus(card, 'Running…');
    var buttons = card.querySelectorAll('button[data-act]');
    buttons.forEach(function (b) { b.disabled = true; });
    try {
      var init = { method: method, credentials: 'same-origin', headers: { 'Accept': 'application/json' } };
      if (method === 'POST') { init.headers['Content-Type'] = 'application/json'; init.body = '{}'; }
      var r = await fetch(url, init);
      var ct = (r.headers.get('content-type') || '');
      var body;
      if (ct.indexOf('application/json') >= 0) {
        body = JSON.stringify(await r.json(), null, 2);
      } else {
        var txt = await r.text();
        // HTML diagnostics: strip tags so the gist is readable in the pane.
        body = txt.replace(/<style[\s\S]*?<\/style>/gi, '').replace(/<script[\s\S]*?<\/script>/gi, '')
                  .replace(/<[^>]+>/g, ' ').replace(/[ \t]+/g, ' ').replace(/\n\s*\n+/g, '\n').trim();
      }
      show(card, body, !r.ok);
      setStatus(card, r.ok ? ('Done · ' + new Date().toLocaleTimeString()) : ('Failed · HTTP ' + r.status));
    } catch (e) {
      show(card, 'Request failed: ' + e.message, true);
      setStatus(card, 'Failed');
    } finally {
      buttons.forEach(function (b) { b.disabled = false; });
    }
  }

  document.querySelectorAll('.adm-card[data-tool]').forEach(function (card) {
    var t = TOOLS[card.dataset.tool];
    if (!t) return;
    card.querySelectorAll('button[data-act]').forEach(function (btn) {
      btn.addEventListener('click', function () {
        var p = params(card, t);
        if (p.missing.length) { setStatus(card, 'Fill in: ' + p.missing.join(', ')); return; }
        var act = btn.dataset.act;
        if (act === 'preview') {
          var pv = t.preview || {};
          var purl = buildUrl(pv.url || t.url, p.values, pv.query || '');
          if (t.open === 'tab') { window.open(purl, '_blank'); return; }
          call(card, pv.method || 'GET', purl);
          return;
        }
        if (t.confirm) {
          if (!confirm(t.confirm)) return;
          if (t.confirm2 && !confirm(t.confirm2)) return;
        }
        var url = buildUrl(t.url, p.values, t.apply || '');
        if (t.open === 'tab') { window.open(url, '_blank'); return; }
        call(card, t.method || 'GET', url);
      });
    });
  });
})();
