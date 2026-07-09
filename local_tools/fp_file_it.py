#!/usr/bin/env python3
"""fp_file_it — the FP Document Analyzer, at the Finder level.

Point it at any file(s), anywhere on disk. Each file is sent to FPBudget's
analysis-only API (/api/analyze — OCR, doc-type detection, naming), then
filed LOCALLY, right next to where it already lives:

    ~/Taxes/2026/
        scan001.pdf                    ← you had this
    ~/Taxes/2026/                      ← after: fp_file_it scan001.pdf
        PROCESSED DOCUMENTS/
            2026-03-12_RECEIPT_Home Depot_142.55.pdf
        SOURCE DOCUMENTS/
            scan001.pdf                ← original, untouched, archived

Nothing is uploaded to Dropbox and nothing lands in any FPBudget project —
the server only reads the bytes, answers with the analysis, and forgets.
Safe for personal documents.

Usage:
    fp_file_it.py --setup                 one-time: save server/email,
                                          password goes in macOS Keychain
    fp_file_it.py FILE [FILE ...]         analyze + file each one
    fp_file_it.py --dry-run FILE ...      show what would happen
    fp_file_it.py --notify FILE ...       also post a macOS notification
                                          (for Finder Quick Actions)

Config: ~/.config/fpbudget/analyzer.json  {"url": ..., "email": ...}
Password lookup order: macOS Keychain (service "fpbudget.analyzer") →
$FPBUDGET_PASSWORD → interactive prompt.

Stdlib only — no pip installs needed. Python 3.8+.
"""

import argparse
import base64
import getpass
import json
import os
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
import uuid

CONFIG_PATH = os.path.expanduser("~/.config/fpbudget/analyzer.json")
KEYCHAIN_SERVICE = "fpbudget.analyzer"
DEFAULT_URL = "https://fp-budget.onrender.com"

PROCESSED_DIR = "PROCESSED DOCUMENTS"
SOURCE_DIR = "SOURCE DOCUMENTS"
REVIEW_DIR = "_NEEDS_REVIEW"  # low-confidence results, inside PROCESSED
# Never re-file something that's already inside one of our folders.
SKIP_PARENTS = {PROCESSED_DIR.lower(), SOURCE_DIR.lower(), REVIEW_DIR.lower()}

ANALYZABLE_EXTS = {".pdf", ".jpg", ".jpeg", ".png", ".heic", ".heif"}


# ── config / credentials ────────────────────────────────────────────────────

def load_config():
    try:
        with open(CONFIG_PATH) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def save_config(cfg):
    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)
    os.chmod(CONFIG_PATH, 0o600)


def keychain_get(email):
    """macOS Keychain lookup; returns None on non-Mac or missing entry."""
    try:
        out = subprocess.run(
            ["security", "find-generic-password",
             "-s", KEYCHAIN_SERVICE, "-a", email, "-w"],
            capture_output=True, text=True, timeout=10)
        return out.stdout.strip() or None if out.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired):
        return None


def keychain_set(email, password):
    try:
        subprocess.run(
            ["security", "add-generic-password", "-U",
             "-s", KEYCHAIN_SERVICE, "-a", email, "-w", password],
            capture_output=True, timeout=10, check=True)
        return True
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return False


def setup():
    cfg = load_config()
    url = input(f"FPBudget URL [{cfg.get('url', DEFAULT_URL)}]: ").strip() \
        or cfg.get("url", DEFAULT_URL)
    email = input(f"Login email [{cfg.get('email', '')}]: ").strip() \
        or cfg.get("email", "")
    if not email:
        sys.exit("An email is required.")
    save_config({"url": url.rstrip("/"), "email": email})
    pw = getpass.getpass("Password (stored in macOS Keychain): ")
    if pw:
        if keychain_set(email, pw):
            print("✓ Saved. Password is in your Keychain "
                  f"(service: {KEYCHAIN_SERVICE}).")
        else:
            print("⚠ Couldn't write to Keychain (not macOS?). Set "
                  "$FPBUDGET_PASSWORD instead, or you'll be prompted each run.")
    print(f"✓ Config written to {CONFIG_PATH}")


def get_credentials():
    cfg = load_config()
    url = os.environ.get("FPBUDGET_URL") or cfg.get("url") or DEFAULT_URL
    email = os.environ.get("FPBUDGET_EMAIL") or cfg.get("email")
    if not email:
        sys.exit("No account configured — run with --setup first.")
    password = keychain_get(email) or os.environ.get("FPBUDGET_PASSWORD")
    if not password:
        password = getpass.getpass(f"FPBudget password for {email}: ")
    return url.rstrip("/"), email, password


# ── API call ────────────────────────────────────────────────────────────────

def analyze(url, email, password, path):
    """POST one file to /api/analyze; returns the parsed JSON response."""
    with open(path, "rb") as f:
        data = f.read()
    boundary = uuid.uuid4().hex
    fname = os.path.basename(path).replace('"', "'")
    body = (
        (f"--{boundary}\r\n"
         f'Content-Disposition: form-data; name="file"; filename="{fname}"\r\n'
         f"Content-Type: application/octet-stream\r\n\r\n").encode("utf-8")
        + data
        + f"\r\n--{boundary}--\r\n".encode("utf-8")
    )
    req = urllib.request.Request(url + "/api/analyze", data=body, method="POST")
    req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")
    creds = base64.b64encode(f"{email}:{password}".encode("utf-8")).decode("ascii")
    req.add_header("Authorization", "Basic " + creds)
    # OCR takes 5–20s per doc; Render cold-start can add ~60s on the first hit.
    with urllib.request.urlopen(req, timeout=180) as r:
        return json.load(r)


# ── local filing ────────────────────────────────────────────────────────────

def unique_path(directory, name):
    """foo.pdf → foo (1).pdf → foo (2).pdf until it doesn't collide."""
    candidate = os.path.join(directory, name)
    if not os.path.exists(candidate):
        return candidate
    stem, ext = os.path.splitext(name)
    n = 1
    while True:
        candidate = os.path.join(directory, f"{stem} ({n}){ext}")
        if not os.path.exists(candidate):
            return candidate
        n += 1


def file_locally(path, result, dry_run=False):
    """Create PROCESSED/SOURCE folders next to `path` and file into them.
    Returns a one-line human summary."""
    parent = os.path.dirname(os.path.abspath(path))
    original = os.path.basename(path)

    needs_review = bool(result.get("needs_review"))
    new_name = result.get("new_filename")
    if not new_name:
        # No doc type → keep the original name, but if the server transcoded
        # the bytes (HEIC→JPEG) the extension must follow the bytes.
        new_name = original
        if result.get("converted_ext"):
            new_name = os.path.splitext(original)[0] + result["converted_ext"]

    processed_dir = os.path.join(parent, PROCESSED_DIR)
    if needs_review:
        processed_dir = os.path.join(processed_dir, REVIEW_DIR)
    source_dir = os.path.join(parent, SOURCE_DIR)

    processed_dest = unique_path(processed_dir, new_name)
    source_dest = unique_path(source_dir, original)

    conf = int(round((result.get("confidence") or 0) * 100))
    tag = (result.get("doc_type") or "unknown") + f" {conf}%"
    line = (f"{'⚠' if needs_review else '✓'} {original} → "
            f"{os.path.relpath(processed_dest, parent)}  [{tag}]"
            + ("  (low confidence — double-check the name)" if needs_review else ""))
    if dry_run:
        return "DRY RUN " + line

    os.makedirs(processed_dir, exist_ok=True)
    os.makedirs(source_dir, exist_ok=True)

    # Processed copy: the transcoded bytes if the server converted (HEIC),
    # otherwise a byte-identical copy of the original under the new name.
    if result.get("converted_b64"):
        with open(processed_dest, "wb") as f:
            f.write(base64.b64decode(result["converted_b64"]))
    else:
        shutil.copy2(path, processed_dest)

    # Archive the original LAST, so a failure above never strands the file.
    shutil.move(path, source_dest)
    return line


def notify_mac(title, message):
    try:
        subprocess.run(
            ["osascript", "-e",
             'display notification "{}" with title "{}"'.format(
                 message.replace('"', "'"), title.replace('"', "'"))],
            capture_output=True, timeout=10)
    except OSError:
        pass


# ── main ────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="Analyze documents via FPBudget and file them in place "
                    "(PROCESSED DOCUMENTS / SOURCE DOCUMENTS next to each file).")
    ap.add_argument("files", nargs="*", help="files to analyze & file")
    ap.add_argument("--setup", action="store_true",
                    help="save server URL, email, and Keychain password")
    ap.add_argument("--dry-run", action="store_true",
                    help="analyze and show the plan, but move nothing")
    ap.add_argument("--notify", action="store_true",
                    help="post a macOS notification with the outcome")
    args = ap.parse_args()

    if args.setup:
        setup()
        return

    if not args.files:
        ap.print_help()
        sys.exit(2)

    url, email, password = get_credentials()

    ok = review = failed = 0
    lines = []
    for path in args.files:
        if not os.path.isfile(path):
            lines.append(f"✕ {path}: not a file, skipped")
            failed += 1
            continue
        parent_name = os.path.basename(os.path.dirname(os.path.abspath(path)))
        if parent_name.lower() in SKIP_PARENTS:
            lines.append(f"– {os.path.basename(path)}: already filed, skipped")
            continue
        ext = os.path.splitext(path)[1].lower()
        if ext not in ANALYZABLE_EXTS:
            lines.append(f"– {os.path.basename(path)}: {ext or 'no extension'} "
                         f"isn't analyzable (PDF/JPG/PNG/HEIC), skipped")
            continue
        try:
            result = analyze(url, email, password, path)
        except urllib.error.HTTPError as e:
            if e.code == 401:
                sys.exit("✕ Authentication failed — check your email/password "
                         "(re-run with --setup).")
            try:
                detail = json.load(e).get("error", "")
            except Exception:
                detail = ""
            lines.append(f"✕ {os.path.basename(path)}: server error "
                         f"{e.code} {detail}".rstrip())
            failed += 1
            continue
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            lines.append(f"✕ {os.path.basename(path)}: {e}")
            failed += 1
            continue

        try:
            lines.append(file_locally(path, result, dry_run=args.dry_run))
        except OSError as e:
            lines.append(f"✕ {os.path.basename(path)}: filing failed: {e}")
            failed += 1
            continue
        if result.get("needs_review"):
            review += 1
        else:
            ok += 1

    print("\n".join(lines))
    summary = f"{ok} filed" + (f", {review} to review" if review else "") \
              + (f", {failed} failed" if failed else "")
    print(summary)
    if args.notify:
        notify_mac("FP Document Analyzer", summary)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
