"""Serve the mobile app's browser build at /app — M1-style slice.

mobile_web_dist/ is the committed Expo web export of mobile/ (the same
code the phones run, compiled for browsers). It's a static single-page app
that logs in through the same /api/v1 bearer-token endpoints, so serving it
from Flask costs no new infrastructure and keeps API calls same-origin.

Rebuild after changing mobile/ source:
    cd mobile && npx expo export --platform web --output-dir ../mobile_web_dist
"""
import os

from flask import send_from_directory

from app import app

_DIST = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                     "mobile_web_dist")


@app.route("/app")
@app.route("/app/")
def mobile_app_index():
    # index.html references content-hashed bundles, so it must never be
    # cached — otherwise users keep booting an old bundle after a deploy.
    return send_from_directory(_DIST, "index.html", max_age=0)


@app.route("/app/<path:filename>")
def mobile_app_assets(filename):
    # Bundle/asset filenames are content-hashed → long cache is safe.
    return send_from_directory(_DIST, filename, max_age=86400)
