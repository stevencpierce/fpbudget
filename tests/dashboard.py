#!/usr/bin/env python3
"""
FPBudget Test Runner Dashboard

A local web UI for running and viewing Playwright test results.
Launch:  python tests/dashboard.py
Open:    http://localhost:5005
"""

from __future__ import annotations

import ast
import atexit
import glob
import json
import os
import signal
import subprocess
import sys
import threading
import time

from flask import Flask, Response, jsonify, render_template_string, request, send_from_directory
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TESTS_DIR = os.path.join(PROJECT_ROOT, "tests")
ALLURE_DIR = os.path.join(PROJECT_ROOT, "allure-results")
REPORT_PATH = os.path.join(PROJECT_ROOT, ".report.json")

load_dotenv(os.path.join(TESTS_DIR, ".env"))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Global run state
# ---------------------------------------------------------------------------

_run_state = {
    "status": "idle",       # idle | running | done
    "process": None,
    "log_lines": [],
    "report": None,
    "started_at": None,
    "finished_at": None,
    "module": None,
    "headed": True,
}

_state_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Test discovery
# ---------------------------------------------------------------------------

def discover_modules() -> list[dict]:
    modules = []
    for path in sorted(glob.glob(os.path.join(TESTS_DIR, "test_*.py"))):
        with open(path) as f:
            tree = ast.parse(f.read())
        count = sum(
            1 for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name.startswith("test_")
        )
        name = os.path.basename(path).replace(".py", "")
        label = name.replace("test_", "").replace("_", " ").title()
        modules.append({"name": name, "file": os.path.basename(path), "test_count": count, "label": label})
    return modules


# ---------------------------------------------------------------------------
# Allure screenshot correlation
# ---------------------------------------------------------------------------

def get_failure_screenshots() -> dict[str, str]:
    """Map test fullName -> screenshot filename from allure-results."""
    screenshots = {}
    for path in glob.glob(os.path.join(ALLURE_DIR, "*-result.json")):
        try:
            with open(path) as f:
                result = json.load(f)
            if result.get("status") == "failed" and result.get("attachments"):
                for att in result["attachments"]:
                    if att.get("type", "").startswith("image/"):
                        full_name = result.get("fullName", result.get("name", ""))
                        screenshots[full_name] = att["source"]
        except (json.JSONDecodeError, KeyError):
            continue
    return screenshots


# ---------------------------------------------------------------------------
# Background reader thread
# ---------------------------------------------------------------------------

def _reader_thread(proc: subprocess.Popen):
    try:
        for line in proc.stdout:
            with _state_lock:
                _run_state["log_lines"].append(line.rstrip("\n"))
        proc.wait()
    except Exception:
        pass
    finally:
        with _state_lock:
            _run_state["status"] = "done"
            _run_state["finished_at"] = time.time()
            _run_state["process"] = None
        # Load report
        try:
            with open(REPORT_PATH) as f:
                _run_state["report"] = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            _run_state["report"] = None


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def _cleanup():
    proc = _run_state.get("process")
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()

atexit.register(_cleanup)
signal.signal(signal.SIGTERM, lambda *_: (_cleanup(), sys.exit(0)))


# ---------------------------------------------------------------------------
# Routes — API
# ---------------------------------------------------------------------------

ENV_FILE = os.path.join(TESTS_DIR, ".env")


def _load_credentials() -> dict:
    """Read current credentials from env vars."""
    return {
        "base_url": os.getenv("BASE_URL", "https://fp-budget.onrender.com"),
        "admin_email": os.getenv("ADMIN_EMAIL", "steven@thefp.tv"),
        "admin_password": os.getenv("ADMIN_PASSWORD", ""),
        "test_email": os.getenv("TEST_EMAIL", "claudes-test@thefp.tv"),
        "test_password": os.getenv("TEST_PASSWORD", ""),
    }


def _save_credentials(creds: dict):
    """Write credentials to .env file AND update os.environ in-place."""
    lines = []
    key_map = {
        "base_url": "BASE_URL",
        "admin_email": "ADMIN_EMAIL",
        "admin_password": "ADMIN_PASSWORD",
        "test_email": "TEST_EMAIL",
        "test_password": "TEST_PASSWORD",
    }
    for field, env_key in key_map.items():
        val = creds.get(field, "")
        if val:
            os.environ[env_key] = val
            lines.append(f'{env_key}="{val}"')
        elif env_key in os.environ:
            del os.environ[env_key]
    with open(ENV_FILE, "w") as f:
        f.write("\n".join(lines) + "\n")


@app.route("/api/credentials", methods=["GET"])
def api_credentials_get():
    creds = _load_credentials()
    # Mask passwords for display — send last 4 chars only
    return jsonify({
        "base_url": creds["base_url"],
        "admin_email": creds["admin_email"],
        "admin_password_set": bool(creds["admin_password"]),
        "admin_password_hint": ("***" + creds["admin_password"][-4:]) if len(creds["admin_password"]) >= 4 else ("*" * len(creds["admin_password"])),
        "test_email": creds["test_email"],
        "test_password_set": bool(creds["test_password"]),
        "test_password_hint": ("***" + creds["test_password"][-4:]) if len(creds["test_password"]) >= 4 else ("*" * len(creds["test_password"])),
    })


@app.route("/api/credentials", methods=["POST"])
def api_credentials_save():
    data = request.get_json(silent=True) or {}
    current = _load_credentials()
    # Only update fields that were actually sent (non-empty)
    for field in ["base_url", "admin_email", "admin_password", "test_email", "test_password"]:
        if field in data and data[field]:
            current[field] = data[field]
    _save_credentials(current)
    return jsonify({"ok": True, "message": "Credentials saved"})


@app.route("/api/env-check")
def api_env_check():
    return jsonify({
        "admin_password": bool(os.getenv("ADMIN_PASSWORD")),
        "test_password": bool(os.getenv("TEST_PASSWORD")),
        "base_url": os.getenv("BASE_URL", "https://fp-budget.onrender.com"),
    })


# ---------------------------------------------------------------------------
# Test-project cleanup — scan FPBudget for projects with test-name prefixes
# and delete them via the bulk-delete API.
# ---------------------------------------------------------------------------

TEST_PROJECT_PREFIXES = (
    "AutoTest-", "BudgetTest-", "ExportTest-", "CollabTest-",
    "TopSheetTest-", "CrewTest-", "DEBUG-",
)


def _find_and_delete_test_projects(dry_run: bool = False) -> dict:
    """
    Log in as admin, scrape the projects page for any project whose name
    starts with a known test prefix, then POST to /projects/bulk-delete
    with all their IDs. Returns a result dict.
    """
    from playwright.sync_api import sync_playwright

    creds = _load_credentials()
    base_url = creds["base_url"].rstrip("/")
    admin_email = creds["admin_email"]
    admin_password = creds["admin_password"]
    if not (admin_email and admin_password):
        return {"ok": False, "error": "Admin credentials not set"}

    result = {"ok": True, "found": [], "deleted_count": 0, "dry_run": dry_run}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        try:
            page.goto(f"{base_url}/login", wait_until="networkidle", timeout=60_000)
            page.fill('input[type="email"]', admin_email)
            page.fill('input[type="password"]', admin_password)
            page.click('button[type="submit"]')
            page.wait_for_load_state("networkidle", timeout=30_000)
            if "/login" in page.url:
                result["ok"] = False
                result["error"] = "Login failed"
                return result

            # Go through each status filter to catch active/wrapped/archived
            for status in ("active", "wrapped", "archived"):
                page.goto(f"{base_url}/", wait_until="networkidle", timeout=30_000)
                try:
                    page.locator("#proj-status-filter").select_option(status)
                    page.wait_for_timeout(500)
                except Exception:
                    pass
                cards = page.evaluate("""() => {
                    const out = [];
                    document.querySelectorAll('.project-card').forEach(c => {
                        if (c.style.display === 'none') return;
                        const nameEl = c.querySelector('.project-card-name');
                        out.push({
                            id: c.dataset.id,
                            name: nameEl ? nameEl.textContent.trim() : '',
                            status: c.dataset.status,
                        });
                    });
                    return out;
                }""")
                for card in cards:
                    name = card.get("name", "")
                    if any(name.startswith(pfx) for pfx in TEST_PROJECT_PREFIXES):
                        if not any(c["id"] == card["id"] for c in result["found"]):
                            result["found"].append(card)

            if not result["found"]:
                result["message"] = "No test projects found."
                return result

            if dry_run:
                result["message"] = f"Would delete {len(result['found'])} test projects."
                return result

            # Submit bulk-delete form via the authenticated session.
            # Use page.request to reuse cookies automatically.
            ids = [c["id"] for c in result["found"]]
            form_data = "&".join(f"project_ids={i}" for i in ids)
            resp = context.request.post(
                f"{base_url}/projects/bulk-delete",
                data=form_data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            result["deleted_count"] = len(ids)
            result["status_code"] = resp.status
            result["message"] = (
                f"Deleted {len(ids)} test projects (HTTP {resp.status})."
            )
        finally:
            browser.close()

    return result


@app.route("/api/cleanup-test-projects", methods=["POST"])
def api_cleanup_test_projects():
    data = request.get_json(silent=True) or {}
    dry_run = bool(data.get("dry_run", False))
    try:
        result = _find_and_delete_test_projects(dry_run=dry_run)
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/cleanup-test-projects", methods=["GET"])
def api_cleanup_preview():
    """Preview what would be deleted without deleting."""
    try:
        result = _find_and_delete_test_projects(dry_run=True)
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/modules")
def api_modules():
    return jsonify(discover_modules())


@app.route("/api/status")
def api_status():
    with _state_lock:
        return jsonify({
            "status": _run_state["status"],
            "module": _run_state["module"],
            "started_at": _run_state["started_at"],
            "finished_at": _run_state["finished_at"],
            "line_count": len(_run_state["log_lines"]),
        })


@app.route("/api/run", methods=["POST"])
def api_run():
    with _state_lock:
        if _run_state["status"] == "running":
            return jsonify({"error": "A test run is already in progress"}), 409

    data = request.get_json(silent=True) or {}
    module = data.get("module", "all")
    headed = data.get("headed", True)

    # Clear allure results
    os.makedirs(ALLURE_DIR, exist_ok=True)
    for f in glob.glob(os.path.join(ALLURE_DIR, "*")):
        try:
            os.remove(f)
        except OSError:
            pass

    # Remove old report
    if os.path.exists(REPORT_PATH):
        os.remove(REPORT_PATH)

    # Build command
    cmd = [
        sys.executable, "-m", "pytest",
        "-v", "--tb=short",
        "--json-report", "--json-report-file", REPORT_PATH,
        "--alluredir", ALLURE_DIR,
        "--browser", "chromium",
    ]
    if headed:
        cmd.append("--headed")
    if module != "all":
        cmd.append(os.path.join("tests", f"{module}.py"))

    # Environment — pass current credentials to subprocess
    env = os.environ.copy()
    creds = _load_credentials()
    env["BASE_URL"] = creds["base_url"]
    env["ADMIN_EMAIL"] = creds["admin_email"]
    env["ADMIN_PASSWORD"] = creds["admin_password"]
    env["TEST_EMAIL"] = creds["test_email"]
    env["TEST_PASSWORD"] = creds["test_password"]

    # Launch
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, cwd=PROJECT_ROOT, env=env, bufsize=1,
    )

    with _state_lock:
        _run_state.update({
            "status": "running",
            "process": proc,
            "log_lines": [],
            "report": None,
            "started_at": time.time(),
            "finished_at": None,
            "module": module,
            "headed": headed,
        })

    thread = threading.Thread(target=_reader_thread, args=(proc,), daemon=True)
    thread.start()

    return jsonify({"ok": True, "module": module, "pid": proc.pid})


@app.route("/api/stop", methods=["POST"])
def api_stop():
    with _state_lock:
        proc = _run_state.get("process")
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            _run_state["status"] = "done"
            _run_state["finished_at"] = time.time()
            _run_state["process"] = None
            return jsonify({"ok": True, "message": "Test run stopped"})
    return jsonify({"ok": False, "message": "No running test to stop"})


@app.route("/api/stream")
def api_stream():
    def generate():
        idx = 0
        while True:
            with _state_lock:
                lines = _run_state["log_lines"]
                status = _run_state["status"]
            while idx < len(lines):
                yield f"data: {json.dumps({'type': 'line', 'text': lines[idx]})}\n\n"
                idx += 1
            if status == "done":
                yield f"data: {json.dumps({'type': 'done'})}\n\n"
                break
            if status == "idle":
                yield f"data: {json.dumps({'type': 'idle'})}\n\n"
                break
            time.sleep(0.15)
    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/report")
def api_report():
    report = _run_state.get("report")
    if not report:
        return jsonify({"error": "No report available"}), 404
    screenshots = get_failure_screenshots()
    return jsonify({"report": report, "screenshots": screenshots})


@app.route("/screenshots/<path:filename>")
def serve_screenshot(filename):
    return send_from_directory(ALLURE_DIR, filename)


# ---------------------------------------------------------------------------
# Route — Dashboard UI
# ---------------------------------------------------------------------------

DASHBOARD_HTML = r"""
<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>FPBudget Test Dashboard</title>
<style>
:root {
  --bg: #0f1117; --bg-2: #1a1d27; --bg-3: #22263a;
  --text: #e2e6f0; --text-dim: #8a8fa8;
  --accent: #4f7ef8; --accent-hover: #6b93ff;
  --green: #2ecc71; --red: #e74c3c; --yellow: #f39c12;
  --border: #2a2e3e; --radius: 10px;
  --font: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  --mono: 'SF Mono', 'Fira Code', 'Consolas', monospace;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: var(--font); background: var(--bg); color: var(--text); min-height: 100vh; }
a { color: var(--accent); text-decoration: none; }

/* Header */
.header { background: var(--bg-2); border-bottom: 1px solid var(--border); padding: 16px 24px; display: flex; align-items: center; gap: 16px; }
.header h1 { font-size: 20px; font-weight: 600; }
.header h1 span { color: var(--accent); }
.badge { padding: 4px 12px; border-radius: 20px; font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; }
.badge-idle { background: var(--bg-3); color: var(--text-dim); }
.badge-running { background: rgba(79,126,248,0.15); color: var(--accent); animation: pulse 1.5s infinite; }
.badge-done { background: rgba(46,204,113,0.15); color: var(--green); }
.badge-failed { background: rgba(231,76,60,0.15); color: var(--red); }
@keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.5; } }
.timer { margin-left: auto; font-family: var(--mono); font-size: 14px; color: var(--text-dim); }

/* Layout */
.container { max-width: 1400px; margin: 0 auto; padding: 24px; }
.grid { display: grid; grid-template-columns: 340px 1fr; gap: 24px; }
@media (max-width: 900px) { .grid { grid-template-columns: 1fr; } }

/* Panels */
.panel { background: var(--bg-2); border: 1px solid var(--border); border-radius: var(--radius); overflow: hidden; }
.panel-header { padding: 14px 18px; border-bottom: 1px solid var(--border); font-weight: 600; font-size: 14px; display: flex; align-items: center; justify-content: space-between; }

/* Env check */
.env-row { padding: 10px 18px; display: flex; align-items: center; gap: 10px; font-size: 13px; border-bottom: 1px solid var(--border); }
.env-row:last-child { border-bottom: none; }
.dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
.dot-ok { background: var(--green); }
.dot-bad { background: var(--red); }

/* Settings form */
.settings-form { padding: 14px 18px; }
.field-group { margin-bottom: 12px; }
.field-group label { display: block; font-size: 11px; color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px; }
.field-row { display: flex; gap: 8px; }
.field-row input { flex: 1; }
input.field { width: 100%; padding: 8px 10px; background: var(--bg); border: 1px solid var(--border); border-radius: 6px; color: var(--text); font-family: var(--mono); font-size: 12px; outline: none; }
input.field:focus { border-color: var(--accent); }
input.field::placeholder { color: var(--text-dim); opacity: 0.5; }
.save-row { display: flex; gap: 8px; align-items: center; margin-top: 14px; }
.save-msg { font-size: 12px; color: var(--green); opacity: 0; transition: opacity 0.3s; }
.save-msg.show { opacity: 1; }
.settings-toggle { cursor: pointer; font-size: 12px; color: var(--accent); padding: 2px 0; }
.settings-toggle:hover { color: var(--accent-hover); }
.settings-body { overflow: hidden; transition: max-height 0.3s ease; max-height: 0; }
.settings-body.open { max-height: 600px; }

/* Module cards */
.module-list { padding: 8px; }
.module-card { display: flex; align-items: center; padding: 12px 14px; border-radius: 8px; cursor: pointer; transition: background 0.15s; margin-bottom: 4px; }
.module-card:hover { background: var(--bg-3); }
.module-card .name { flex: 1; font-size: 14px; font-weight: 500; }
.module-card .count { font-size: 12px; color: var(--text-dim); margin-right: 12px; }
.module-card .run-btn { padding: 5px 14px; border-radius: 6px; border: 1px solid var(--border); background: transparent; color: var(--accent); font-size: 12px; cursor: pointer; font-weight: 600; transition: all 0.15s; }
.module-card .run-btn:hover { background: var(--accent); color: #fff; border-color: var(--accent); }

/* Run All / Stop */
.actions { padding: 12px 18px; display: flex; gap: 10px; }
.btn { padding: 10px 20px; border-radius: 8px; border: none; font-size: 14px; font-weight: 600; cursor: pointer; transition: all 0.15s; }
.btn-primary { background: var(--accent); color: #fff; flex: 1; }
.btn-primary:hover { background: var(--accent-hover); }
.btn-danger { background: transparent; border: 1px solid var(--red); color: var(--red); }
.btn-danger:hover { background: var(--red); color: #fff; }
.btn:disabled { opacity: 0.4; cursor: not-allowed; }

/* Headed toggle */
.toggle-row { padding: 8px 18px 4px; display: flex; align-items: center; gap: 8px; font-size: 13px; color: var(--text-dim); }
.toggle-row input { accent-color: var(--accent); }

/* Output */
.output-wrap { position: relative; }
.output { background: #0a0c10; padding: 16px; font-family: var(--mono); font-size: 12px; line-height: 1.7; height: 500px; overflow-y: auto; white-space: pre-wrap; word-break: break-all; color: var(--text-dim); }
.output .pass { color: var(--green); }
.output .fail { color: var(--red); font-weight: 600; }
.output .skip { color: var(--yellow); }
.output .info { color: var(--accent); }

/* Results */
.results { padding: 20px; }
.result-summary { display: flex; gap: 24px; margin-bottom: 20px; flex-wrap: wrap; }
.stat { text-align: center; padding: 16px 24px; border-radius: var(--radius); background: var(--bg-3); min-width: 100px; }
.stat .num { font-size: 32px; font-weight: 700; font-family: var(--mono); }
.stat .label { font-size: 11px; color: var(--text-dim); text-transform: uppercase; letter-spacing: 1px; margin-top: 4px; }
.stat-pass .num { color: var(--green); }
.stat-fail .num { color: var(--red); }
.stat-skip .num { color: var(--yellow); }
.stat-total .num { color: var(--text); }
.stat-time .num { color: var(--accent); font-size: 20px; }

/* Failures table */
.failures-title { font-size: 15px; font-weight: 600; margin: 20px 0 12px; color: var(--red); }
.failure-row { background: var(--bg-3); border-radius: 8px; margin-bottom: 8px; overflow: hidden; }
.failure-header { padding: 12px 16px; cursor: pointer; display: flex; align-items: center; gap: 10px; }
.failure-header:hover { background: rgba(231,76,60,0.08); }
.failure-header .arrow { transition: transform 0.2s; }
.failure-header.open .arrow { transform: rotate(90deg); }
.failure-name { font-family: var(--mono); font-size: 13px; flex: 1; }
.failure-detail { padding: 0 16px 16px; display: none; }
.failure-detail.open { display: block; }
.failure-message { background: #0a0c10; padding: 12px; border-radius: 6px; font-family: var(--mono); font-size: 11px; color: var(--red); white-space: pre-wrap; max-height: 300px; overflow-y: auto; }
.failure-screenshot { margin-top: 12px; }
.failure-screenshot img { max-width: 100%; border-radius: 6px; border: 1px solid var(--border); cursor: pointer; }
.failure-screenshot img:hover { border-color: var(--accent); }

/* Lightbox */
.lightbox { display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.9); z-index: 1000; align-items: center; justify-content: center; cursor: zoom-out; }
.lightbox.open { display: flex; }
.lightbox img { max-width: 95vw; max-height: 95vh; border-radius: 8px; }

/* Empty state */
.empty { padding: 40px; text-align: center; color: var(--text-dim); font-size: 14px; }
</style>
</head>
<body>

<!-- Header -->
<div class="header">
  <h1><span>FPBudget</span> Test Dashboard</h1>
  <span id="statusBadge" class="badge badge-idle">Idle</span>
  <span id="timer" class="timer"></span>
</div>

<div class="container">
<div class="grid">

<!-- Left sidebar -->
<div>
  <!-- Settings / Credentials -->
  <div class="panel" style="margin-bottom:16px">
    <div class="panel-header">
      Settings
      <span class="settings-toggle" id="settingsToggle" onclick="toggleSettings()">Edit</span>
    </div>
    <!-- Status summary (always visible) -->
    <div id="envCheck">
      <div class="env-row"><div class="dot dot-bad"></div>Loading...</div>
    </div>
    <!-- Editable form (collapsible) -->
    <div class="settings-body" id="settingsBody">
      <div class="settings-form">
        <div class="field-group">
          <label>Target URL</label>
          <input class="field" id="cfgBaseUrl" placeholder="https://fp-budget.onrender.com">
        </div>
        <div class="field-group">
          <label>Admin Account</label>
          <div class="field-row">
            <input class="field" id="cfgAdminEmail" placeholder="Email">
            <input class="field" id="cfgAdminPw" type="password" placeholder="Password">
          </div>
        </div>
        <div class="field-group">
          <label>Test User Account</label>
          <div class="field-row">
            <input class="field" id="cfgTestEmail" placeholder="Email">
            <input class="field" id="cfgTestPw" type="password" placeholder="Password">
          </div>
        </div>
        <div class="save-row">
          <button class="btn btn-primary" style="flex:none;padding:8px 20px;font-size:13px" onclick="saveCredentials()">Save</button>
          <span class="save-msg" id="saveMsg">Saved!</span>
        </div>
      </div>
    </div>
  </div>

  <!-- Module list -->
  <div class="panel">
    <div class="panel-header">Test Modules <span id="totalTests" style="color:var(--text-dim);font-weight:400"></span></div>
    <div class="toggle-row">
      <input type="checkbox" id="headedToggle" checked>
      <label for="headedToggle">Run headed (show browser)</label>
    </div>
    <div class="actions">
      <button class="btn btn-primary" id="runAllBtn" onclick="runTests('all')">Run All Tests</button>
      <button class="btn btn-danger" id="stopBtn" onclick="stopTests()" disabled>Stop</button>
    </div>
    <div id="moduleList" class="module-list">
      <div class="empty">Loading modules...</div>
    </div>
  </div>

  <!-- Test Cleanup Panel -->
  <div class="panel" style="margin-top:16px">
    <div class="panel-header">Test Cleanup</div>
    <div style="padding:14px 18px;font-size:13px;color:var(--text-dim);line-height:1.5">
      Scans FPBudget for leftover projects with test-prefix names and deletes them.
      <div style="font-family:var(--mono);font-size:11px;margin-top:6px">
        Prefixes: AutoTest-, BudgetTest-, ExportTest-, CollabTest-, TopSheetTest-, DEBUG-
      </div>
    </div>
    <div class="actions">
      <button class="btn btn-primary" onclick="cleanupTestProjects(false)" id="cleanupBtn">
        Clean Up Test Projects
      </button>
      <button class="btn btn-danger" style="background:transparent;border:1px solid var(--border);color:var(--text-dim)"
              onclick="cleanupTestProjects(true)">
        Preview
      </button>
    </div>
    <div id="cleanupResult" style="padding:0 18px 14px;font-size:12px;font-family:var(--mono);color:var(--text-dim);max-height:200px;overflow-y:auto"></div>
  </div>
</div>

<!-- Right main area -->
<div>
  <!-- Output panel -->
  <div class="panel output-wrap" style="margin-bottom:16px">
    <div class="panel-header">
      Output
      <span id="lineCount" style="color:var(--text-dim);font-weight:400;font-size:12px"></span>
    </div>
    <div class="output" id="output">Click "Run" to start tests...</div>
  </div>

  <!-- Results panel -->
  <div class="panel" id="resultsPanel" style="display:none">
    <div class="panel-header">Results</div>
    <div class="results" id="results"></div>
  </div>
</div>

</div>
</div>

<!-- Lightbox -->
<div class="lightbox" id="lightbox" onclick="this.classList.remove('open')">
  <img id="lightboxImg" src="">
</div>

<script>
// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
let eventSource = null;
let timerInterval = null;
let startTime = null;

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
document.addEventListener('DOMContentLoaded', () => {
  loadCredentials();
  loadModules();
  checkRunningState();
});

// ---------------------------------------------------------------------------
// Credentials / Settings
// ---------------------------------------------------------------------------
async function loadCredentials() {
  try {
    const r = await fetch('/api/credentials');
    const d = await r.json();
    // Fill form fields
    document.getElementById('cfgBaseUrl').value = d.base_url || '';
    document.getElementById('cfgAdminEmail').value = d.admin_email || '';
    document.getElementById('cfgAdminPw').placeholder = d.admin_password_set ? d.admin_password_hint : 'Password';
    document.getElementById('cfgTestEmail').value = d.test_email || '';
    document.getElementById('cfgTestPw').placeholder = d.test_password_set ? d.test_password_hint : 'Password';
    // Update status dots
    const el = document.getElementById('envCheck');
    el.innerHTML = `
      <div class="env-row">
        <div class="dot ${d.admin_password_set ? 'dot-ok' : 'dot-bad'}"></div>
        Admin: <b>${d.admin_email || 'not set'}</b>
        ${d.admin_password_set ? '' : '<span style="color:var(--red);margin-left:4px">no password</span>'}
      </div>
      <div class="env-row">
        <div class="dot ${d.test_password_set ? 'dot-ok' : 'dot-bad'}"></div>
        Test: <b>${d.test_email || 'not set'}</b>
        ${d.test_password_set ? '' : '<span style="color:var(--red);margin-left:4px">no password</span>'}
      </div>
      <div class="env-row">
        <div class="dot dot-ok"></div>
        <span style="font-family:var(--mono);font-size:12px">${d.base_url}</span>
      </div>
    `;
    // Auto-open settings if passwords missing
    if (!d.admin_password_set || !d.test_password_set) {
      document.getElementById('settingsBody').classList.add('open');
      document.getElementById('settingsToggle').textContent = 'Close';
    }
  } catch (e) {
    console.error('Credentials load failed:', e);
  }
}

function toggleSettings() {
  const body = document.getElementById('settingsBody');
  const toggle = document.getElementById('settingsToggle');
  body.classList.toggle('open');
  toggle.textContent = body.classList.contains('open') ? 'Close' : 'Edit';
}

async function saveCredentials() {
  const payload = {};
  const baseUrl = document.getElementById('cfgBaseUrl').value.trim();
  const adminEmail = document.getElementById('cfgAdminEmail').value.trim();
  const adminPw = document.getElementById('cfgAdminPw').value;
  const testEmail = document.getElementById('cfgTestEmail').value.trim();
  const testPw = document.getElementById('cfgTestPw').value;

  if (baseUrl) payload.base_url = baseUrl;
  if (adminEmail) payload.admin_email = adminEmail;
  if (adminPw) payload.admin_password = adminPw;
  if (testEmail) payload.test_email = testEmail;
  if (testPw) payload.test_password = testPw;

  try {
    const r = await fetch('/api/credentials', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const d = await r.json();
    if (d.ok) {
      const msg = document.getElementById('saveMsg');
      msg.classList.add('show');
      setTimeout(() => msg.classList.remove('show'), 2000);
      // Clear password fields and reload status
      document.getElementById('cfgAdminPw').value = '';
      document.getElementById('cfgTestPw').value = '';
      loadCredentials();
    }
  } catch (e) {
    console.error('Save failed:', e);
  }
}

// ---------------------------------------------------------------------------
// Test project cleanup
// ---------------------------------------------------------------------------
async function cleanupTestProjects(dryRun) {
  const btn = document.getElementById('cleanupBtn');
  const resultEl = document.getElementById('cleanupResult');
  resultEl.innerHTML = `<span style="color:var(--accent)">${dryRun ? 'Scanning...' : 'Scanning and deleting...'}</span>`;
  btn.disabled = true;
  try {
    const r = await fetch('/api/cleanup-test-projects', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ dry_run: dryRun }),
    });
    const d = await r.json();
    if (!d.ok) {
      resultEl.innerHTML = `<span style="color:var(--red)">Error: ${d.error || 'unknown'}</span>`;
      return;
    }
    const found = d.found || [];
    if (found.length === 0) {
      resultEl.innerHTML = `<span style="color:var(--green)">${d.message || 'No test projects found.'}</span>`;
      return;
    }
    const listHtml = found.map(c =>
      `<div>${escHtml(c.name)} <span style="opacity:.5">(${c.status}, id=${c.id})</span></div>`
    ).join('');
    const header = dryRun
      ? `<span style="color:var(--yellow)">Would delete ${found.length} projects:</span>`
      : `<span style="color:var(--green)">${d.message}</span>`;
    resultEl.innerHTML = header + '<div style="margin-top:6px">' + listHtml + '</div>';
  } catch (e) {
    resultEl.innerHTML = `<span style="color:var(--red)">Request failed: ${e.message}</span>`;
  } finally {
    btn.disabled = false;
  }
}

// ---------------------------------------------------------------------------
// Module list
// ---------------------------------------------------------------------------
async function loadModules() {
  try {
    const r = await fetch('/api/modules');
    const modules = await r.json();
    const el = document.getElementById('moduleList');
    const total = modules.reduce((s, m) => s + m.test_count, 0);
    document.getElementById('totalTests').textContent = `(${total} tests)`;
    el.innerHTML = modules.map(m => `
      <div class="module-card">
        <span class="name">${m.label}</span>
        <span class="count">${m.test_count} tests</span>
        <button class="run-btn" onclick="event.stopPropagation(); runTests('${m.name}')">Run</button>
      </div>
    `).join('');
  } catch (e) {
    console.error('Module load failed:', e);
  }
}

// ---------------------------------------------------------------------------
// Check if a run is already in progress (page reload)
// ---------------------------------------------------------------------------
async function checkRunningState() {
  try {
    const r = await fetch('/api/status');
    const d = await r.json();
    if (d.status === 'running') {
      startTime = d.started_at * 1000;
      setRunning(d.module || 'all');
      connectStream();
    } else if (d.status === 'done') {
      setBadge('done');
      loadReport();
    }
  } catch (e) {}
}

// ---------------------------------------------------------------------------
// Run tests
// ---------------------------------------------------------------------------
async function runTests(module) {
  const headed = document.getElementById('headedToggle').checked;

  document.getElementById('output').innerHTML = '';
  document.getElementById('resultsPanel').style.display = 'none';
  setRunning(module);

  try {
    const r = await fetch('/api/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ module, headed }),
    });
    const d = await r.json();
    if (d.error) {
      appendLine(d.error, 'fail');
      setBadge('idle');
      return;
    }
    startTime = Date.now();
    startTimer();
    connectStream();
  } catch (e) {
    appendLine('Failed to start: ' + e.message, 'fail');
    setBadge('idle');
  }
}

async function stopTests() {
  try {
    await fetch('/api/stop', { method: 'POST' });
    appendLine('\n--- Run stopped by user ---', 'fail');
  } catch (e) {}
}

// ---------------------------------------------------------------------------
// SSE stream
// ---------------------------------------------------------------------------
function connectStream() {
  if (eventSource) eventSource.close();
  eventSource = new EventSource('/api/stream');

  eventSource.onmessage = (e) => {
    const data = JSON.parse(e.data);
    if (data.type === 'line') {
      appendLine(data.text);
      document.getElementById('lineCount').textContent = document.getElementById('output').childElementCount + ' lines';
    } else if (data.type === 'done') {
      eventSource.close();
      eventSource = null;
      onRunDone();
    } else if (data.type === 'idle') {
      eventSource.close();
      eventSource = null;
    }
  };

  eventSource.onerror = () => {
    eventSource.close();
    eventSource = null;
    onRunDone();
  };
}

function onRunDone() {
  stopTimer();
  document.getElementById('runAllBtn').disabled = false;
  document.getElementById('stopBtn').disabled = true;
  document.querySelectorAll('.run-btn').forEach(b => b.disabled = false);
  loadReport();
}

// ---------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------
function appendLine(text, cls) {
  const el = document.getElementById('output');
  const div = document.createElement('div');
  // Auto-detect class
  if (!cls) {
    if (/PASSED/.test(text)) cls = 'pass';
    else if (/FAILED|ERROR/.test(text)) cls = 'fail';
    else if (/SKIPPED|XFAIL/.test(text)) cls = 'skip';
    else if (/^(tests\/|=)/.test(text)) cls = 'info';
  }
  if (cls) div.className = cls;
  div.textContent = text;
  el.appendChild(div);
  el.scrollTop = el.scrollHeight;
}

// ---------------------------------------------------------------------------
// Report / Results
// ---------------------------------------------------------------------------
async function loadReport() {
  try {
    const r = await fetch('/api/report');
    if (!r.ok) { setBadge('done'); return; }
    const d = await r.json();
    renderResults(d.report, d.screenshots);
  } catch (e) {
    setBadge('done');
  }
}

function renderResults(report, screenshots) {
  if (!report || !report.summary) { setBadge('done'); return; }
  const s = report.summary;
  const passed = s.passed || 0;
  const failed = s.failed || 0;
  const skipped = (s.skipped || 0) + (s.xfailed || 0);
  const total = s.total || (passed + failed + skipped);
  const duration = s.duration ? s.duration.toFixed(1) : '?';

  setBadge(failed > 0 ? 'failed' : 'done');

  let html = `<div class="result-summary">
    <div class="stat stat-pass"><div class="num">${passed}</div><div class="label">Passed</div></div>
    <div class="stat stat-fail"><div class="num">${failed}</div><div class="label">Failed</div></div>
    <div class="stat stat-skip"><div class="num">${skipped}</div><div class="label">Skipped</div></div>
    <div class="stat stat-total"><div class="num">${total}</div><div class="label">Total</div></div>
    <div class="stat stat-time"><div class="num">${duration}s</div><div class="label">Duration</div></div>
  </div>`;

  // Failures
  const failures = (report.tests || []).filter(t => t.outcome === 'failed');
  if (failures.length > 0) {
    html += `<div class="failures-title">${failures.length} Failure${failures.length > 1 ? 's' : ''}</div>`;
    failures.forEach((t, i) => {
      const name = t.nodeid || t.name || 'Unknown test';
      const msg = t.call?.longrepr || t.call?.crash?.message || 'No details available';
      // Try to find screenshot
      let screenshotHtml = '';
      if (screenshots) {
        for (const [key, file] of Object.entries(screenshots)) {
          if (name.includes(key) || key.includes(name.split('::').pop())) {
            screenshotHtml = `<div class="failure-screenshot">
              <img src="/screenshots/${file}" alt="Failure screenshot"
                   onclick="event.stopPropagation(); openLightbox('/screenshots/${file}')">
            </div>`;
            break;
          }
        }
      }
      html += `
        <div class="failure-row">
          <div class="failure-header" onclick="toggleFailure(this)">
            <span class="arrow">&#9654;</span>
            <span class="failure-name">${escHtml(name)}</span>
          </div>
          <div class="failure-detail">
            <div class="failure-message">${escHtml(typeof msg === 'string' ? msg : JSON.stringify(msg, null, 2))}</div>
            ${screenshotHtml}
          </div>
        </div>`;
    });
  }

  const panel = document.getElementById('resultsPanel');
  panel.style.display = 'block';
  document.getElementById('results').innerHTML = html;
}

function toggleFailure(el) {
  el.classList.toggle('open');
  el.nextElementSibling.classList.toggle('open');
}

function openLightbox(src) {
  document.getElementById('lightboxImg').src = src;
  document.getElementById('lightbox').classList.add('open');
}

function escHtml(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

// ---------------------------------------------------------------------------
// UI helpers
// ---------------------------------------------------------------------------
function setRunning(module) {
  setBadge('running');
  document.getElementById('runAllBtn').disabled = true;
  document.getElementById('stopBtn').disabled = false;
  document.querySelectorAll('.run-btn').forEach(b => b.disabled = true);
  startTimer();
}

function setBadge(state) {
  const el = document.getElementById('statusBadge');
  el.className = 'badge badge-' + state;
  const labels = { idle: 'Idle', running: 'Running', done: 'Passed', failed: 'Failed' };
  el.textContent = labels[state] || state;
}

function startTimer() {
  if (!startTime) startTime = Date.now();
  stopTimer();
  const timerEl = document.getElementById('timer');
  timerInterval = setInterval(() => {
    const elapsed = Math.floor((Date.now() - startTime) / 1000);
    const m = Math.floor(elapsed / 60);
    const s = elapsed % 60;
    timerEl.textContent = `${m}:${s.toString().padStart(2, '0')}`;
  }, 1000);
}

function stopTimer() {
  if (timerInterval) { clearInterval(timerInterval); timerInterval = null; }
}
</script>
</body>
</html>
"""


@app.route("/")
def dashboard():
    return render_template_string(DASHBOARD_HTML)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    base_url = os.getenv("BASE_URL", "https://fp-budget.onrender.com")
    admin_set = bool(os.getenv("ADMIN_PASSWORD"))
    test_set = bool(os.getenv("TEST_PASSWORD"))

    print()
    print("  ======================================")
    print("   FPBudget Test Dashboard")
    print("  ======================================")
    print(f"   Dashboard:  http://localhost:5005")
    print(f"   Target:     {base_url}")
    print(f"   Admin pw:   {'SET' if admin_set else 'NOT SET'}")
    print(f"   Test pw:    {'SET' if test_set else 'NOT SET'}")
    print("  ======================================")
    print()

    if not admin_set or not test_set:
        print("  WARNING: Set ADMIN_PASSWORD and TEST_PASSWORD env vars")
        print("  before running tests.")
        print()

    app.run(port=5005, debug=True, threaded=True)
