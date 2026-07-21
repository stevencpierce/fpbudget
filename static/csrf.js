// CSRF shim (audit H8, 2026-07-20). Reads the per-session token from the
// <meta name="csrf-token"> tag and (a) patches window.fetch to attach it as
// X-CSRFToken on every same-origin mutating request, (b) injects a hidden
// csrf_token input into any POST form at submit time. Covers every existing
// call site without touching them.
(function () {
  function tok() {
    var m = document.querySelector('meta[name="csrf-token"]');
    return m ? m.content : '';
  }
  var origFetch = window.fetch;
  window.fetch = function (input, init) {
    try {
      var url = (typeof input === 'string') ? input : ((input && input.url) || '');
      var method = (((init && init.method) || (input && input.method) || 'GET') + '').toUpperCase();
      var sameOrigin = !/^https?:\/\//i.test(url) || url.indexOf(location.origin) === 0;
      if (sameOrigin && method !== 'GET' && method !== 'HEAD' && tok()) {
        init = init || {};
        var h = init.headers;
        if (h && typeof Headers !== 'undefined' && h instanceof Headers) { h.set('X-CSRFToken', tok()); }
        else { init.headers = Object.assign({}, h || {}, { 'X-CSRFToken': tok() }); }
      }
    } catch (e) { /* never break a request over the shim */ }
    return origFetch.call(this, input, init);
  };
  document.addEventListener('submit', function (ev) {
    var f = ev.target;
    if (!f || !f.method || f.method.toLowerCase() !== 'post') return;
    if (f.querySelector('input[name="csrf_token"]') || !tok()) return;
    var i = document.createElement('input');
    i.type = 'hidden'; i.name = 'csrf_token'; i.value = tok();
    f.appendChild(i);
  }, true);
})();
