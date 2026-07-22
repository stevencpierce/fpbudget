"""Route modules extracted from the app.py monolith (audit M1, 2026-07-20).

Pattern: each module registers on the SAME `app` object with @app.route, so
every endpoint name and URL stays byte-identical (no blueprint renaming —
url_for(...) calls keep working unchanged). app.py imports these modules at
its very BOTTOM, after all shared helpers exist. Each new slice must keep the
route map identical — verify with scripts/route_snapshot.py before/after.
"""
