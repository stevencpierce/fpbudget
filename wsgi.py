# WSGI entrypoint. gunicorn's GeventWebSocketWorker handles gevent
# monkey-patching at worker boot — doing it here too caused a silent
# startup hang (double-patching + psycopg2). The R2 recursion bug that
# originally prompted this file was actually botocore 1.36's default
# flexible checksums; see _r2_client in app.py.
from app import app  # noqa: F401
