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
# Duplicate-URL guard (2026-10-30): the same rule registered twice means
# Flask silently serves the FIRST one and the second handler is dead — two
# handlers shared /projects/<pid>/actuals/reconcile.json for months and the
# Reconcile sub-tab rendered empty. Key on (rule, method) so GET and POST
# handlers on one url (a legitimate pattern) are not flagged.
seen = {}   # (rule, method) -> "path:line"
for path in FILES:
    src = open(path).read().splitlines()
    for i, line in enumerate(src):
        m = re.match(r'\s*@app\.route\(\s*"([^"]+)"', line)
        if not m:
            continue
        rule = m.group(1)
        # Methods may sit on this line or a continuation line of the decorator.
        deco = line
        j = i
        while not deco.rstrip().endswith(")") and j + 1 < len(src) and j - i < 4:
            j += 1
            deco += " " + src[j]
        mm = re.search(r"methods\s*=\s*\[([^\]]*)\]", deco)
        methods = (sorted(x.strip().strip("'\"").upper() for x in mm.group(1).split(",") if x.strip())
                   if mm else ["GET"])
        for meth in methods:
            key = (rule, meth)
            if key in seen:
                bad.append(f"{path}:{i+1} route {rule!r} [{meth}] already registered at "
                           f"{seen[key]} — the second handler is dead")
            else:
                seen[key] = f"{path}:{i+1}"
        placeholders = re.findall(r"<(?:[a-z_]+:)?([a-zA-Z_]+)>", rule)
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
