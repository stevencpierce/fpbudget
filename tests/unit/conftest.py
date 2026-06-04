"""Headless unit-test harness for the budget/versioning logic.

Unlike tests/conftest.py (Playwright E2E against a running server), this
boots the real Flask app in-process against a throwaway SQLite database so
we can exercise _create_budget_from_source / sync_schedule_driven_lines /
_delete_budget_cascade / calc_top_sheet directly and assert on the numbers.

Heavy, irrelevant dependencies (weasyprint PDF rendering, the fp_analyzer
OCR/Dropbox pipeline) are stubbed before `import app` so the harness runs
without system libs. flask_socketio is already optional in app.py.
"""
import os
import sys
import types
import tempfile

import pytest

# ── Stub heavy modules BEFORE importing app ──────────────────────────────────
def _install_stubs():
    if 'weasyprint' not in sys.modules:
        wp = types.ModuleType('weasyprint')
        class _HTML:
            def __init__(self, *a, **k): pass
            def write_pdf(self, *a, **k): return b""
        wp.HTML = _HTML
        sys.modules['weasyprint'] = wp
    if 'fp_analyzer' not in sys.modules:
        fa = types.ModuleType('fp_analyzer')
        def _getattr(name):
            def _missing(*a, **k):
                raise RuntimeError(f"fp_analyzer.{name} unavailable in unit harness")
            return _missing
        fa.__getattr__ = _getattr
        sys.modules['fp_analyzer'] = fa


@pytest.fixture(scope="session")
def flask_app():
    _install_stubs()
    _dbfile = os.path.join(tempfile.gettempdir(), "fp_unit_test.db")
    if os.path.exists(_dbfile):
        os.remove(_dbfile)
    os.environ["DATABASE_URL"] = f"sqlite:///{_dbfile}"
    os.environ.setdefault("SESSION_COOKIE_SECURE", "0")
    os.environ.setdefault("REMEMBER_COOKIE_SECURE", "0")
    os.environ.setdefault("RUN_BOOT_TASKS", "0")

    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    import app as A
    from app import db
    with A.app.app_context():
        db.create_all()
    return A


@pytest.fixture
def ctx(flask_app):
    """Per-test app context with a clean schema."""
    from app import db
    with flask_app.app.app_context():
        db.drop_all()
        db.create_all()
        from budget_calc import seed_payroll_profiles
        try:
            seed_payroll_profiles(db.session)
            db.session.commit()
        except Exception:
            db.session.rollback()
        yield flask_app
        db.session.rollback()
