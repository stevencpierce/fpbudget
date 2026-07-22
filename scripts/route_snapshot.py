#!/usr/bin/env python3
"""Route-map snapshot/compare for M1 monolith-split slices (2026-07-20).

Usage:
  python scripts/route_snapshot.py save before.json     # before a cut
  python scripts/route_snapshot.py diff before.json     # after the cut
A slice is CORRECT only when diff reports IDENTICAL — same rules, endpoint
names, and methods. Requires app to be importable (deps installed; DATABASE_URL
may be unset — sqlite fallback is fine, boot tasks are skipped).
"""
import json
import sys

import app as _app


def _rules():
    return sorted((r.rule, r.endpoint, ','.join(sorted(r.methods)))
                  for r in _app.app.url_map.iter_rules())


cmd, path = sys.argv[1], sys.argv[2]
if cmd == 'save':
    json.dump(_rules(), open(path, 'w'))
    print(f"saved {len(_rules())} routes → {path}")
elif cmd == 'diff':
    before = [tuple(x) for x in json.load(open(path))]
    now = _rules()
    if now == before:
        print(f"IDENTICAL: {len(now)} routes")
    else:
        print("missing:", sorted(set(before) - set(now)))
        print("added:  ", sorted(set(now) - set(before)))
        sys.exit(1)
