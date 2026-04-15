# WSGI entrypoint — MUST monkey-patch gevent before any other imports.
# Without this, boto3/botocore's signer recurses on SSL reads under
# gunicorn's GeventWebSocketWorker (worker-level patching happens too
# late, after app.py has already imported boto3/ssl/urllib3).
from gevent import monkey
monkey.patch_all()

from app import app  # noqa: E402
