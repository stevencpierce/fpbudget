"""Dropbox paths after a project rename, and Maintenance → "Re-link
documents to Dropbox" — end to end on sqlite through the real routes
(fix 2026-11-04, project 307).

  1. renaming a project moves its folder (Dropbox mocked) AND rewrites every
     stored path inside it: DocUpload.filed_dropbox_path /
     source_archive_path, the trash_meta move sources, and the call-sheet
     send archive — logged on the rename's activity row
  2. the remap helper leaves another project's rows alone, even when they
     still point at the same old slug, and a failed folder move leaves
     every path untouched
  3. the re-link preview checks each live document against ONE recursive
     listing of the project folder and proposes: the slug remap when the
     same relative path exists under the current folder, else the one file
     with that basename (search), else "needs a person" (ambiguous / none);
     a present file is not listed; a deleted row is skipped
  4. Fix (apply=1) writes the proposed paths, logs one activity row per
     document with the old path in `before`, and the project's activity
     Undo puts the old path back
  5. preview-link / raw answer a path error with the plain
     "isn't at its recorded Dropbox location" message and missing: true
  6. the sweep's first-use branch never creates `_INBOX` for a stale slug:
     it re-reads the project's current slug and never creates a folder
     whose project root is not in Dropbox

Run: python -m pytest tests/unit/test_dropbox_relink_app.py -q --confcutdir=tests/unit -o addopts=""
"""
import json
import os
import sys
import tempfile
from datetime import date, datetime

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

_DB_DIR = tempfile.mkdtemp(prefix="fpb-relink-")
_PRESET = os.environ.get("DATABASE_URL", "")
if _PRESET and not _PRESET.startswith("sqlite:"):
    pytest.skip("DATABASE_URL is not sqlite; refusing to boot", allow_module_level=True)
os.environ.setdefault("DATABASE_URL", "sqlite:///" + os.path.join(_DB_DIR, "t.db"))
os.environ.setdefault("SECRET_KEY", "test")

try:
    _cwd = os.getcwd()
    os.chdir(ROOT)
    import app as fpapp                       # noqa: E402
    from models import (db, User, ProjectSheet, ProjectAccess, Budget,   # noqa: E402
                        DocUpload, CallSheetSend, ActivityLog)
    import routes.docs_relink as relink       # noqa: E402
    from dropbox.files import FileMetadata, FolderMetadata, GetMetadataError, LookupError as _DLookup  # noqa: E402
    from dropbox.exceptions import ApiError   # noqa: E402
    _IMPORT_ERR = None
except Exception as _e:                       # pragma: no cover
    _IMPORT_ERR = _e
finally:
    try:
        os.chdir(_cwd)
    except Exception:
        pass

pytestmark = pytest.mark.skipif(_IMPORT_ERR is not None,
                                reason=f"app.py not importable here: {_IMPORT_ERR}")

S = {}
OLD = "2026-08_GlobalIndustriaal_GlobalBrandVideosQ320264Commer"
OTHER_OLD = "2026-08_GlobalIndustriaal_SomeOtherShow"


def _root(slug):
    return fpapp._project_dbx_root(slug)


def _not_found():
    return ApiError("rid", GetMetadataError.path(_DLookup.not_found), None, None)


class FakeDbx:
    """A Dropbox that holds a set of FILE paths. Folders exist when a file
    (or a registered folder) lives under them. Records moves and creates."""

    def __init__(self, files=(), folders=(), fail_move=False):
        self.files = {p.lower(): p for p in files}
        self.folders = {f.lower() for f in folders}
        self.moves, self.created, self.meta_calls = [], [], []
        self.fail_move = fail_move

    def _folder_exists(self, path):
        pl = path.lower().rstrip("/")
        if pl in self.folders:
            return True
        return any(f.startswith(pl + "/") for f in list(self.files) + list(self.folders))

    def files_move_v2(self, src, dest, **kw):
        if self.fail_move:
            raise RuntimeError("move failed")
        self.moves.append((src, dest))
        sl = src.lower()
        for low, disp in list(self.files.items()):
            if low.startswith(sl + "/"):
                del self.files[low]
                nd = dest + disp[len(src):]
                self.files[nd.lower()] = nd
        self.folders = {(dest.lower() + f[len(sl):]) if f.startswith(sl + "/") or f == sl else f
                        for f in self.folders}
        return None

    def files_get_metadata(self, path):
        self.meta_calls.append(path)
        pl = path.lower()
        if pl in self.files:
            return FileMetadata(name=path.rsplit("/", 1)[-1], path_lower=pl, path_display=self.files[pl])
        if self._folder_exists(path):
            return FolderMetadata(name=path.rsplit("/", 1)[-1], path_lower=pl, path_display=path)
        raise _not_found()

    def files_get_temporary_link(self, path):
        if path.lower() in self.files:
            class _R: link = "https://dl.example/" + path.rsplit("/", 1)[-1]
            return _R()
        raise ApiError("rid", GetMetadataError.path(_DLookup.not_found), None, None)

    def files_download(self, path):
        raise _not_found()

    def files_list_folder(self, path, recursive=False, **kw):
        if not self._folder_exists(path):
            raise _not_found()
        pl = path.lower().rstrip("/")
        ents = [FileMetadata(name=d.rsplit("/", 1)[-1], path_lower=l, path_display=d)
                for l, d in self.files.items() if l.startswith(pl + "/")]
        class _Res:
            entries = ents
            has_more = False
            cursor = None
        return _Res()

    def files_create_folder_v2(self, path):
        self.created.append(path)
        self.folders.add(path.lower())


@pytest.fixture(scope="module")
def ctx():
    app = fpapp.app
    app.config["TESTING"] = True
    with app.app_context():
        db.create_all()
        u = User.query.filter_by(email="relink-admin@thefp.tv").first()
        if not u:
            u = User(email="relink-admin@thefp.tv", name="Relink Admin", role="super_admin")
            db.session.add(u)
        u.role = "super_admin"
        u.set_password("pw")
        p = ProjectSheet(name="Global Brand Videos Q3 2026 - 4 Commercials", client_name="Global Industriaal",
                         dropbox_folder=OLD, status="active")
        other = ProjectSheet(name="Some Other Show", client_name="Global Industriaal",
                             dropbox_folder=OTHER_OLD, status="active")
        db.session.add_all([p, other])
        db.session.flush()
        db.session.add(ProjectAccess(project_id=p.id, user_id=u.id, role="owner"))
        b = Budget(project_id=p.id, name="Global Brand Videos Q3 2026 - 4 Commercials v1",
                   budget_mode="estimated", version_status="current")
        db.session.add(b)
        db.session.flush()
        inv = f"{_root(OLD)}/01_ADMIN/PROCESSED DOCUMENTS/INVOICES/drop-folder/Elliemade/2026-09-03_INVOICE_Elliemade_1101_2800.00.pdf"
        arc = f"{_root(OLD)}/01_ADMIN/PROCESSED DOCUMENTS/_SOURCE_ARCHIVE/2026-09/batch/Elliemade_1101.pdf"
        d1 = DocUpload(project_id=p.id, uploader_id=u.id, original_filename="Elliemade_1101.pdf",
                       filed_filename="2026-09-03_INVOICE_Elliemade_1101_2800.00.pdf", status="done",
                       vendor="Elliemade", category="invoice", doc_date=date(2026, 9, 3),
                       filed_dropbox_path=inv, source_archive_path=arc, content_type="application/pdf")
        rcp = f"{_root(OLD)}/01_ADMIN/PROCESSED DOCUMENTS/RECEIPTS/2026-09-10_RECEIPT_Uber_18.40.pdf"
        d2 = DocUpload(project_id=p.id, uploader_id=u.id, original_filename="uber.pdf",
                       filed_filename="2026-09-10_RECEIPT_Uber_18.40.pdf", status="done",
                       vendor="Uber", category="receipt", filed_dropbox_path=rcp, content_type="application/pdf")
        trashed_src = f"{_root(OLD)}/01_ADMIN/PROCESSED DOCUMENTS/RECEIPTS/old-receipt.pdf"
        d3 = DocUpload(project_id=p.id, uploader_id=u.id, original_filename="old-receipt.pdf",
                       filed_filename="old-receipt.pdf", status="deleted", deleted_at=datetime(2026, 10, 1),
                       filed_dropbox_path=trashed_src,
                       trash_meta=json.dumps({"prior_status": "done",
                                              "moves": [{"src": trashed_src, "dest": "/_TRASH/2026-10-01/x_old-receipt.pdf"}]}))
        other_doc = DocUpload(project_id=other.id, uploader_id=u.id, original_filename="o.pdf",
                              filed_filename="o.pdf", status="done",
                              filed_dropbox_path=f"{_root(OLD)}/01_ADMIN/PROCESSED DOCUMENTS/RECEIPTS/misfiled.pdf")
        cs = CallSheetSend(budget_id=b.id, date=date(2026, 9, 5), schedule_mode="estimated", version_label="v1",
                           dropbox_path=f"{_root(OLD)}/01_ADMIN/CALL SHEETS/CallSheet_2026-09-05_v1.pdf")
        db.session.add_all([d1, d2, d3, other_doc, cs])
        db.session.commit()
        S.update(uid=u.id, pid=p.id, other_pid=other.id, bid=b.id, d1=d1.id, d2=d2.id, d3=d3.id,
                 other_doc=other_doc.id, cs=cs.id, inv=inv, arc=arc, rcp=rcp, trashed_src=trashed_src)
    c = app.test_client()
    with c.session_transaction() as sess:
        sess["_user_id"] = str(S["uid"])
        sess["_fresh"] = True
        sess["_csrf"] = "t"
    c.environ_base["HTTP_X_CSRFTOKEN"] = "t"
    return {"app": app, "client": c}


@pytest.fixture
def dbx(ctx, monkeypatch):
    """A fake Dropbox holding the project's files under the OLD slug."""
    fake = FakeDbx(files=[S["inv"], S["arc"], S["rcp"], S["trashed_src"].replace("RECEIPTS/", "RECEIPTS/x/")],
                   folders=[f"{_root(OLD)}/_INBOX", f"{_root(OTHER_OLD)}"])
    monkeypatch.setattr(fpapp, "_dbx_client", lambda: fake)
    monkeypatch.setattr(relink, "_dbx_client", lambda: fake)
    monkeypatch.setenv("DROPBOX_ACCESS_TOKEN", "x")
    monkeypatch.setattr(relink, "_META_SLEEP_S", 0)
    return fake


# ── 2 (first, so the rename test sees untouched rows): helper scope ────────

def test_02a_remap_helper_touches_only_the_named_project(ctx):
    with ctx["app"].app_context():
        # project 1's remap: its own 3 docs + the call sheet, never project 2's misfiled row
        counts = fpapp._remap_project_dropbox_paths(S["pid"], _root(OLD), _root("NEW_SLUG"))
        assert counts["docs"] == 3 and counts["callsheet_sends"] == 1
        assert DocUpload.query.get(S["other_doc"]).filed_dropbox_path.startswith(_root(OLD) + "/")
        db.session.rollback()
        # project 2's remap touches only project 2's row
        counts = fpapp._remap_project_dropbox_paths(S["other_pid"], _root(OLD), _root("NEW_SLUG"))
        assert counts["docs"] == 1 and counts["callsheet_sends"] == 0
        assert DocUpload.query.get(S["d1"]).filed_dropbox_path == S["inv"]
        db.session.rollback()
        assert DocUpload.query.get(S["other_doc"]).filed_dropbox_path.startswith(_root(OLD) + "/")
        # a slug that is a PREFIX of another is a different segment: untouched
        counts = fpapp._remap_project_dropbox_paths(S["pid"], _root(OLD[:-6]), _root("NEW_SLUG"))
        assert counts["docs"] == 0
        db.session.rollback()
        # the rule is a root PREFIX swap, case-insensitive, whole segment
        assert fpapp._remap_root_path(f"{_root(OLD)}/a/b.pdf", _root(OLD).upper(), "/_WRAPPED PROJECTS/2026-08-07_X") \
            == "/_WRAPPED PROJECTS/2026-08-07_X/a/b.pdf"
        assert fpapp._remap_root_path(f"{_root(OLD)}x/a.pdf", _root(OLD), "/n") == f"{_root(OLD)}x/a.pdf"


def test_02b_failed_folder_move_leaves_every_path_alone(ctx, monkeypatch):
    fake = FakeDbx(files=[S["inv"]], fail_move=True)
    monkeypatch.setattr(fpapp, "_dbx_client", lambda: fake)
    monkeypatch.setenv("DROPBOX_ACCESS_TOKEN", "x")
    c = ctx["client"]
    r = c.post(f"/projects/{S['pid']}/rename", data={"name": "Will Not Move"})
    assert r.status_code in (302, 303)
    with ctx["app"].app_context():
        p = ProjectSheet.query.get(S["pid"])
        assert p.name == "Will Not Move"
        assert p.dropbox_folder == OLD                       # slug stays with the folder
        assert DocUpload.query.get(S["d1"]).filed_dropbox_path == S["inv"]
        # put the name back for the real rename below
        p.name = "Global Brand Videos Q3 2026 - 4 Commercials"
        for b in Budget.query.filter_by(project_id=p.id).all():
            b.name = b.name.replace("Will Not Move", "Global Brand Videos Q3 2026 - 4 Commercials")
        db.session.commit()


# ── 1. the rename rewrites the paths ──────────────────────────────────────

def test_01_rename_moves_the_folder_and_rewrites_every_stored_path(ctx, dbx):
    c = ctx["client"]
    r = c.post(f"/projects/{S['pid']}/rename", data={"name": "Global Run Strong 2026 relink"})
    assert r.status_code in (302, 303), r.data[:300]
    assert len(dbx.moves) == 1
    src, dest = dbx.moves[0]
    assert src == _root(OLD)
    with ctx["app"].app_context():
        p = ProjectSheet.query.get(S["pid"])
        new = p.dropbox_folder
        assert new != OLD and new.startswith("2026-08_GlobalIndustriaal_GlobalRunStrong2026Relink")
        assert dest == _root(new)
        S["new"] = new
        d1, d2, d3 = DocUpload.query.get(S["d1"]), DocUpload.query.get(S["d2"]), DocUpload.query.get(S["d3"])
        assert d1.filed_dropbox_path == S["inv"].replace(f"/{OLD}/", f"/{new}/")
        assert d1.source_archive_path == S["arc"].replace(f"/{OLD}/", f"/{new}/")
        assert d2.filed_dropbox_path == S["rcp"].replace(f"/{OLD}/", f"/{new}/")
        # the trashed doc's restore target moves with the folder; its trash copy does not
        meta = json.loads(d3.trash_meta)
        assert meta["moves"][0]["src"] == S["trashed_src"].replace(f"/{OLD}/", f"/{new}/")
        assert meta["moves"][0]["dest"] == "/_TRASH/2026-10-01/x_old-receipt.pdf"
        assert d3.filed_dropbox_path == S["trashed_src"].replace(f"/{OLD}/", f"/{new}/")
        cs = CallSheetSend.query.get(S["cs"])
        assert cs.dropbox_path == f"{_root(new)}/01_ADMIN/CALL SHEETS/CallSheet_2026-09-05_v1.pdf"
        # the OTHER project's row still points at the old slug (not ours to touch)
        assert DocUpload.query.get(S["other_doc"]).filed_dropbox_path.startswith(_root(OLD) + "/")
        # the files in (fake) Dropbox are where the rows now say
        assert d1.filed_dropbox_path.lower() in dbx.files
        # logged on the rename's activity row
        row = (ActivityLog.query.filter_by(entity_type="project", entity_id=S["pid"], action="update")
               .order_by(ActivityLog.id.desc()).first())
        after = json.loads(row.after_json)
        assert after["dropbox_paths_remapped"]["docs"] == 3
        assert after["dropbox_paths_remapped"]["filed_dropbox_path"] == 3
        assert after["dropbox_paths_remapped"]["source_archive_path"] == 1
        assert after["dropbox_paths_remapped"]["trash_meta"] == 1
        assert after["dropbox_paths_remapped"]["callsheet_sends"] == 1
        assert "3 document path(s) updated" in row.note


# ── 3. the re-link preview ────────────────────────────────────────────────

def _break_rows(app_):
    """Put the rows back to the OLD slug (as if the rename had not rewritten
    them) plus one whose relative path differs and one ambiguous one."""
    with app_.app_context():
        new = S["new"]
        d1 = DocUpload.query.get(S["d1"])
        d1.filed_dropbox_path = S["inv"]                       # old slug, same rest → slug remap
        d1.source_archive_path = S["arc"]
        d2 = DocUpload.query.get(S["d2"])
        d2.filed_dropbox_path = f"{_root(OLD)}/01_ADMIN/SOMEWHERE ELSE/2026-09-10_RECEIPT_Uber_18.40.pdf"  # → search
        amb = DocUpload(project_id=S["pid"], uploader_id=S["uid"], original_filename="twice.pdf",
                        filed_filename="twice.pdf", status="done",
                        filed_dropbox_path=f"{_root(OLD)}/01_ADMIN/ELSEWHERE/twice.pdf")   # two by name, no exact match
        gone = DocUpload(project_id=S["pid"], uploader_id=S["uid"], original_filename="gone.pdf",
                         filed_filename="gone.pdf", status="done",
                         filed_dropbox_path=f"{_root(OLD)}/01_ADMIN/PROCESSED DOCUMENTS/RECEIPTS/gone.pdf")
        fine = DocUpload(project_id=S["pid"], uploader_id=S["uid"], original_filename="fine.pdf",
                         filed_filename="fine.pdf", status="done",
                         filed_dropbox_path=f"{_root(new)}/01_ADMIN/PROCESSED DOCUMENTS/RECEIPTS/fine.pdf")
        db.session.add_all([amb, gone, fine])
        db.session.commit()
        S.update(amb=amb.id, gone=gone.id, fine=fine.id)


def test_03_preview_proposes_slug_remap_then_search_and_flags_ambiguous(ctx, dbx):
    _break_rows(ctx["app"])
    new = S["new"]
    dbx.files = {}
    for p in [S["inv"].replace(f"/{OLD}/", f"/{new}/"), S["arc"].replace(f"/{OLD}/", f"/{new}/"),
              S["rcp"].replace(f"/{OLD}/", f"/{new}/"),
              f"{_root(new)}/01_ADMIN/PROCESSED DOCUMENTS/RECEIPTS/twice.pdf",
              f"{_root(new)}/01_ADMIN/PROCESSED DOCUMENTS/RECEIPTS/dup/twice.pdf",
              f"{_root(new)}/01_ADMIN/PROCESSED DOCUMENTS/RECEIPTS/fine.pdf"]:
        dbx.files[p.lower()] = p
    dbx.folders = set()
    c = ctx["client"]
    r = c.get(f"/admin/docs/relink?project={S['pid']}")
    assert r.status_code == 200, r.data[:300]
    pv = r.get_json()
    assert pv["dry_run"] is True and pv["ok"] is True
    by = {(x["doc_id"], x["field"]): x for x in pv["rows"]}
    # d1: both fields, slug remap
    a = by[(S["d1"], "filed_dropbox_path")]
    assert a["method"] == "slug" and a["new_path"] == S["inv"].replace(f"/{OLD}/", f"/{new}/")
    assert by[(S["d1"], "source_archive_path")]["method"] == "slug"
    # d2: the relative path changed → found by name
    b_ = by[(S["d2"], "filed_dropbox_path")]
    assert b_["method"] == "search" and b_["new_path"] == S["rcp"].replace(f"/{OLD}/", f"/{new}/")
    # ambiguous → no new_path, candidates listed
    amb = by[(S["amb"], "filed_dropbox_path")]
    assert amb["new_path"] is None and len(amb["candidates"]) == 2 and "needs a person" in amb["reason"]
    # nowhere → needs a person
    gone = by[(S["gone"], "filed_dropbox_path")]
    assert gone["new_path"] is None and "needs a person" in gone["reason"]
    # present and deleted rows are not listed
    assert (S["fine"], "filed_dropbox_path") not in by
    assert all(x["doc_id"] != S["d3"] for x in pv["rows"])
    assert pv["fixable"] == 3 and pv["needs_person"] == 2 and pv["missing"] == 5
    assert pv["present"] == 1
    # the old root is gone: ONE metadata call settled every old-slug path
    assert dbx.meta_calls == [_root(OLD)]
    # GET never writes
    with ctx["app"].app_context():
        assert DocUpload.query.get(S["d1"]).filed_dropbox_path == S["inv"]


# ── 4. Fix + Undo ─────────────────────────────────────────────────────────

def test_04_fix_writes_paths_logs_before_and_undo_restores(ctx, dbx):
    new = S["new"]
    dbx.files = {}
    for p in [S["inv"].replace(f"/{OLD}/", f"/{new}/"), S["arc"].replace(f"/{OLD}/", f"/{new}/"),
              S["rcp"].replace(f"/{OLD}/", f"/{new}/"),
              f"{_root(new)}/01_ADMIN/PROCESSED DOCUMENTS/RECEIPTS/twice.pdf",
              f"{_root(new)}/01_ADMIN/PROCESSED DOCUMENTS/RECEIPTS/dup/twice.pdf",
              f"{_root(new)}/01_ADMIN/PROCESSED DOCUMENTS/RECEIPTS/fine.pdf"]:
        dbx.files[p.lower()] = p
    dbx.folders = set()
    c = ctx["client"]
    r = c.post(f"/admin/docs/relink?project={S['pid']}&apply=1", json={})
    assert r.status_code == 200, r.data[:300]
    out = r.get_json()
    assert out["dry_run"] is False and out["fixed"] == 3 and out["needs_person"] == 2
    with ctx["app"].app_context():
        d1 = DocUpload.query.get(S["d1"])
        assert d1.filed_dropbox_path == S["inv"].replace(f"/{OLD}/", f"/{new}/")
        assert d1.source_archive_path == S["arc"].replace(f"/{OLD}/", f"/{new}/")
        assert DocUpload.query.get(S["d2"]).filed_dropbox_path == S["rcp"].replace(f"/{OLD}/", f"/{new}/")
        assert DocUpload.query.get(S["amb"]).filed_dropbox_path.startswith(_root(OLD) + "/")
        acts = {x["activity_id"]: x for x in out["rows"]}
        assert all(acts)
        for aid, x in acts.items():
            row = ActivityLog.query.get(aid)
            assert row.entity_type == "doc_upload" and row.entity_id == x["doc_id"]
            assert json.loads(row.before_json) == {x["field"]: x["old_path"]}
            assert json.loads(row.after_json)[x["field"]] == x["new_path"]
        aid_d2 = next(a for a, x in acts.items() if x["doc_id"] == S["d2"])
    # Undo from the project's activity log puts the old path back
    r = c.post(f"/projects/{S['pid']}/budget/{S['bid']}/activity/{aid_d2}/undo", json={})
    assert r.status_code == 200, r.data[:300]
    with ctx["app"].app_context():
        assert DocUpload.query.get(S["d2"]).filed_dropbox_path.startswith(_root(OLD) + "/")
        assert ActivityLog.query.get(aid_d2).undone_at is not None
    # a second preview finds nothing left to fix for d1
    r = c.get(f"/admin/docs/relink?project={S['pid']}")
    pv = r.get_json()
    assert all(x["doc_id"] != S["d1"] for x in pv["rows"])


# ── 5. the preview error says what happened ───────────────────────────────

def test_05_preview_link_and_raw_say_the_file_is_not_at_its_recorded_path(ctx, dbx):
    c = ctx["client"]
    with ctx["app"].app_context():
        gone = DocUpload.query.get(S["gone"])
        gone.r2_key = None
        db.session.commit()
        fine = DocUpload.query.get(S["fine"]).filed_dropbox_path
    dbx.files[fine.lower()] = fine
    r = c.get(f"/docs/upload/{S['gone']}/preview-link")
    assert r.status_code == 404
    j = r.get_json()
    assert j["missing"] is True and "isn't at its recorded Dropbox location" in j["error"]
    assert "Re-link documents" in j["error"]
    r = c.get(f"/docs/upload/{S['gone']}/raw")
    assert r.status_code == 404
    j = r.get_json()
    assert j["missing"] is True and "Re-link documents" in j["error"]
    # a present file still links
    r = c.get(f"/docs/upload/{S['fine']}/preview-link")
    assert r.status_code == 200 and r.get_json()["ok"] is True


# ── 6. the sweep never re-creates an old-slug _INBOX ──────────────────────

def test_06_sweep_uses_the_current_slug_and_never_creates_under_a_missing_root(ctx, dbx):
    new = S["new"]
    with ctx["app"].app_context():
        # the row a sweep loaded BEFORE the rename committed
        from types import SimpleNamespace
        stale = SimpleNamespace(id=S["pid"], dropbox_folder=OLD, name="x", status="active")
        dbx.folders = {f"{_root(new)}/_INBOX".lower()}
        dbx.created = []
        res = fpapp._sweep_project_drop_folder(stale, uploader=None)
        assert res["ok"] is True and res.get("created") is None
        assert res["path"] == f"{_root(new)}/_INBOX"
        assert dbx.created == []                          # nothing created under the old slug
        assert stale.dropbox_folder == new
        # the current folder has no _INBOX yet but the root exists → create it there
        dbx.folders = {_root(new).lower()}
        stale2 = SimpleNamespace(id=S["pid"], dropbox_folder=OLD, name="x", status="active")
        res = fpapp._sweep_project_drop_folder(stale2, uploader=None)
        assert res["ok"] is True and res["created"] is True
        assert dbx.created == [f"{_root(new)}/_INBOX", f"{_root(new)}/_INBOX/_imported"]
        # the project root is not in Dropbox at all → refuse, loudly, create nothing
        dbx.folders = set(); dbx.files = {}; dbx.created = []
        stale3 = SimpleNamespace(id=S["pid"], dropbox_folder=new, name="x", status="active")
        res = fpapp._sweep_project_drop_folder(stale3, uploader=None)
        assert res["ok"] is False and "not in Dropbox" in res["error"]
        assert dbx.created == []
        # a slug no project has any more → nothing created
        ghost = SimpleNamespace(id=999999, dropbox_folder=OLD, name="ghost", status="active")
        res = fpapp._sweep_project_drop_folder(ghost, uploader=None)
        assert res["ok"] is False and dbx.created == []
