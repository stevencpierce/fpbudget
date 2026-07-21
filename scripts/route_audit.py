#!/usr/bin/env python3
"""Route-binding audit: every <converter:name> placeholder in an @app.route
decorator must appear in the parameters of the next `def`.

Catches "route stealing" — a helper def inserted between the decorator and the
real handler binds the route to the wrong function (commit ed2aeca: every
single-cell schedule save 500'd for 4 days). Run in CI; exits 1 on failure.
"""
import re
import sys

FILES = sys.argv[1:] or ["app.py"]
bad = []
for path in FILES:
    src = open(path).read().splitlines()
    for i, line in enumerate(src):
        m = re.match(r'\s*@app\.route\(\s*"([^"]+)"', line)
        if not m:
            continue
        placeholders = re.findall(r"<(?:[a-z_]+:)?([a-zA-Z_]+)>", m.group(1))
        if not placeholders:
            continue
        for j in range(i + 1, min(i + 8, len(src))):
            dm = re.match(r"\s*def\s+(\w+)\(([^)]*)\)", src[j])
            if dm:
                params = dm.group(2)
                for p in placeholders:
                    if not re.search(r"\b" + re.escape(p) + r"\b", params):
                        bad.append(f"{path}:{i+1} route {m.group(1)!r} → "
                                   f"def {dm.group(1)}() is missing param {p!r}")
                break
        else:
            bad.append(f"{path}:{i+1} route {m.group(1)!r} has no def within 8 lines")

if bad:
    print("ROUTE AUDIT FAILED:")
    for b in bad:
        print(" ", b)
    sys.exit(1)
print(f"route audit OK ({', '.join(FILES)})")
