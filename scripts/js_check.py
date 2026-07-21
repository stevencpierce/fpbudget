#!/usr/bin/env python3
"""Syntax-check the inline <script> blocks in Jinja templates with `node --check`.

Strips HTML comments FIRST (a `<script` inside an HTML comment used to produce
the budget.html "block 23" false positive), then Jinja comments/statements, and
neutralizes {{ expr }} to `null`. Run in CI; exits 1 on any syntax error.
"""
import os
import re
import subprocess
import sys
import tempfile

FILES = sys.argv[1:] or sorted(
    os.path.join("templates", f) for f in os.listdir("templates") if f.endswith(".html")
)
failed = 0
for path in FILES:
    text = open(path).read()
    text = re.sub(r"<!--.*?-->", "", text, flags=re.S)      # HTML comments first
    text = re.sub(r"\{#.*?#\}", "", text, flags=re.S)        # Jinja comments
    text = re.sub(r"\{%.*?%\}", "", text, flags=re.S)        # Jinja statements
    text = re.sub(r"\{\{.*?\}\}", " null ", text, flags=re.S)  # Jinja expressions
    blocks = re.findall(r"<script\b(?![^>]*\bsrc=)[^>]*>(.*?)</script>", text, flags=re.S)
    for i, block in enumerate(blocks):
        if not block.strip():
            continue
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
            f.write(block)
            tmp = f.name
        r = subprocess.run(["node", "--check", tmp], capture_output=True, text=True)
        os.unlink(tmp)
        if r.returncode != 0:
            failed += 1
            first = (r.stderr.strip().splitlines() or ["?"])
            msg = next((l for l in first if "Error" in l), first[-1])
            print(f"JS ERROR {path} block {i}: {msg}")
if failed:
    print(f"JS CHECK FAILED: {failed} block(s)")
    sys.exit(1)
print(f"js check OK ({len(FILES)} template(s))")
