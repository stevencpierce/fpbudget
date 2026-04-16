"""Custom gunicorn worker that skips gevent's ssl monkey-patch.

Why: gevent's monkey-patched ssl module breaks outbound HTTPS on Python
3.12+ in two ways:

  1. SSLContext property setters (options, verify_mode, etc.) recurse
     forever because Python's new super()-based setters re-resolve
     `SSLContext` from ssl.py's module globals, which gevent has rebound.
  2. SSLSocket.__init__ hits `super(type, obj)` type-check failures
     because gevent's SSLSocket isn't in the MRO stdlib expects.

These manifested as RecursionError and "super(type, obj): obj ... is not
an instance or subtype" errors on every boto3/Dropbox upload.

We don't actually need gevent's cooperative ssl here: Render terminates
TLS at its load balancer, so our gunicorn worker receives plain HTTP
inbound. gevent's ssl patch only affects *outbound* HTTPS (Dropbox,
Anthropic, R2). Those calls are fine running synchronously — they block
a greenlet for <1s and we're I/O-bound on websocket traffic anyway.

Usage (render.yaml):
    startCommand: gunicorn -k gunicorn_workers.GeventWebSocketNoSSLWorker ...
"""
from gevent import monkey

# Patch everything EXCEPT ssl and subprocess. Must run before any stdlib
# module that would be patched is imported by the worker. gunicorn imports
# this module before instantiating workers, so this runs early enough.
if not monkey.is_module_patched('socket'):
    monkey.patch_all(ssl=False, subprocess=False)

from geventwebsocket.gunicorn.workers import GeventWebSocketWorker


class GeventWebSocketNoSSLWorker(GeventWebSocketWorker):
    """GeventWebSocketWorker variant that does NOT monkey-patch ssl.

    The parent class would call monkey.patch_all() in its patch() method
    and re-patch ssl. We override to skip it.
    """

    def patch(self):
        # Re-apply the same selective patch in case the parent class (or
        # gunicorn) ran any intermediate code that un-patched something.
        monkey.patch_all(ssl=False, subprocess=False)
