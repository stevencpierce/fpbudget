"""Wrap / archive / reactivate move the Dropbox folder AND the stored paths,
and Re-link finds a folder that moved before that was true — end to end on
sqlite through the real routes (fix 2026-11-04, projects 285 / 288).

  1. wrap moves the folder to `_WRAPPED PROJECTS/<date>_<slug>`, records it
     in ProjectSheet.dropbox_root and rewrites every stored path; a rename
     while wrapped keeps the folder where it is (new slug, same date stamp);
     reactivate moves it back to the ops root from the recorded location,
     clears dropbox_root and rewrites the paths again
  2. archive does the same under `_ARCHIVED/`; a failed move changes nothing
  3. Re-link discovery: a project whose current folder is gone but whose
     `<date>_<slug>` folder sits under _WRAPPED PROJECTS → every document is
     proposed at the same place under it (method "wrapped") plus ONE
     project-level change; Fix records dropbox_root and the paths, logs
     both, and the project Undo puts dropbox_root back; two matching
     folders → needs a person, nothing proposed
  4. _project_root_for is the one rule (dropbox_root wins; else the slug)

Run: python -m pytest tests/unit/test_dropbox_wrapped_app.py -q --confcutdir=tests/unit -o addopts=""
"""
import json
import os
import sys
import tempfile
from datetime import date

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

_DB_DIR = tempfile.mkdtemp(prefix="fpb-wrapped-")
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
WRAP = fpapp._DBX_WRAP_ROOT
ARCH = fpapp._DBX_ARCHIVE_ROOT
SLUG_A = "2026-04_Ventana_2026Q2EtoroPressConferenceWT"
SLUG_B = "2026-05_VentanaProductions_VeoliaProvidenceWT"
SLUG_C = "2026-06_Cliburn_Cliburn25MundyTestWT"
SLUG_D = "2026-07_Ventana_TwoCopiesWT"


def _root(slug):
    return fpapp._project_dbx_root(slug)


def _not_found():
    return ApiError("rid", GetMetadataError.path(_DLookup.not_found), None, None)


class FakeDbx:
    """A Dropbox that holds FILE paths; a folder exists when something lives
    under it or it was registered. Moves rewrite the files under the
    source; listings are recursive or one level, like the real API."""

    def __init__(self, files=(), folders=(), fail_move=False):
        self.files = {p.lower(): p for p in files}
        self.folders = {f.lower(): f for f in folders}
        self.moves, self.created, self.uploads = [], [], []
        self.fail_move = fail_move

    def _folder_exists(self, path):
        pl = path.lower().rstrip("/")
        if pl in self.folders:
            return True
        return any(f.startswith(pl + "/") for f in list(self.files) + list(self.folders))

    class _Res:
        def __init__(self, meta): self.metadata = meta

    def files_move_v2(self, src, dest, **kw):
        if self.fail_move:
            raise RuntimeError("move failed")
        if not self._folder_exists(src) and src.lower() not in self.files:
            raise _not_found()
        self.moves.append((src, dest))
        sl = src.lower()
        for low, disp in list(self.files.items()):
            if low.startswith(sl + "/"):
                del self.files[low]
                nd = dest + disp[len(src):]
                self.files[nd.lower()] = nd
        for low, disp in list(self.folders.items()):
            if low == sl or low.startswith(sl + "/"):
                del self.folders[low]
                nd = dest + disp[len(src):]
                self.folders[nd.lower()] = nd
        return FakeDbx._Res(FolderMetadata(name=dest.rsplit("/", 1)[-1], path_lower=dest.lower(), path_display=dest))

    def files_get_metadata(self, path):
        pl = path.lower()
        if pl in self.files:
            return FileMetadata(name=path.rsplit("/", 1)[-1], path_lower=pl, path_display=self.files[pl])
        if self._folder_exists(path):
            return FolderMetadata(name=path.rsplit("/", 1)[-1], path_lower=pl, path_display=path)
        raise _not_found()

    def files_upload(self, data, path, **kw):
        self.uploads.append(path)
        self.files[path.lower()] = path

    def files_list_folder(self, path, recursive=False, **kw):
        if not self._folder_exists(path):
            raise _not_found()
        pl = path.lower().rstrip("/")
        ents = []
        seen = set()
        for low, disp in list(self.files.items()) + list(self.folders.items()):
            if not low.startswith(pl + "/"):
                continue
            rest = disp[len(pl) + 1:]
            if recursive:
                if low in self.files:
                    ents.append(FileMetadata(name=disp.rsplit("/", 1)[-1], path_lower=low, path_display=disp))
            else:
                first = rest.split("/", 1)[0]
                if "/" in rest or low in self.folders:
                    if first.lower() in seen:
                        continue
                    seen.add(first.lower())
                    fp = f"{disp[:len(pl)]}/{first}"
                    ents.append(FolderMetadata(name=first, path_lower=fp.lower(), path_display=fp))
                else:
                    ents.append(FileMetadata(name=first, path_lower=low, path_display=disp))

        class _R:
            entries = ents
            has_more = False
            cursor = None
        return _R()

    def files_create_folder_v2(self, path):
        self.created.append(path)
        self.folders[path.lower()] = path


def _files_for(root):
    return [f"{root}/01_ADMIN/PROCESSED DOCUMENTS/INVOICES/2026-06-01_INVOICE_Acme_100.00.pdf",
            f"{root}/01_ADMIN/PROCESSED DOCUMENTS/_SOURCE_ARCHIVE/2026-06/b/acme.pdf",
            f"{root}/01_ADMIN/PROCESSED DOCUMENTS/RECEIPTS/2026-06-02_RECEIPT_Lyft_22.10.pdf",
            f"{root}/01_ADMIN/CALL SHEETS/CallSheet_2026-06-03_v1.pdf"]


def _seed_project(u, name, slug, root=None, status="active", dropbox_root=None):
    p = ProjectSheet(name=name, client_name="Ventana", dropbox_folder=slug, status=status,
                     dropbox_root=dropbox_root)
    db.session.add(p)
    db.session.flush()
    db.session.add(ProjectAccess(project_id=p.id, user_id=u.id, role="owner"))
    b = Budget(project_id=p.id, name=f"{name} v1", budget_mode="estimated", version_status="current")
    db.session.add(b)
    db.session.flush()
    root = root or _root(slug)
    inv, arc, rcp, cs_path = _files_for(root)
    d1 = DocUpload(project_id=p.id, uploader_id=u.id, original_filename="acme.pdf",
                   filed_filename=inv.rsplit("/", 1)[-1], status="done", vendor="Acme",
                   category="invoice", filed_dropbox_path=inv, source_archive_path=arc,
                   content_type="application/pdf")
    d2 = DocUpload(project_id=p.id, uploader_id=u.id, original_filename="lyft.pdf",
                   filed_filename=rcp.rsplit("/", 1)[-1], status="done", vendor="Lyft",
                   category="receipt", filed_dropbox_path=rcp, content_type="application/pdf")
    cs = CallSheetSend(budget_id=b.id, date=date(2026, 6, 3), schedule_mode="estimated",
                       version_label="v1", dropbox_path=cs_path)
    db.session.add_all([d1, d2, cs])
    db.session.commit()
    return {"pid": p.id, "bid": b.id, "d1": d1.id, "d2": d2.id, "cs": cs.id,
            "inv": inv, "arc": arc, "rcp": rcp, "cs_path": cs_path}


@pytest.fixture(scope="module")
def ctx():
    app = fpapp.app
    app.config["TESTING"] = True
    with app.app_context():
        db.create_all()
        u = User.query.filter_by(email="wrapped-admin@thefp.tv").first()
        if not u:
            u = User(email="wrapped-admin@thefp.tv", name="Wrapped Admin", role="super_admin")
            db.session.add(u)
        u.role = "super_admin"
        u.set_password("pw")
        db.session.commit()
        S["uid"] = u.id
        S["A"] = _seed_project(u, "2026 Q2 Etoro Press Conference WT", SLUG_A)
        S["B"] = _seed_project(u, "Veolia Providence Event WT", SLUG_B)
        # C: wrapped before paths followed the folder — rows at the ops root,
        # folder under _WRAPPED PROJECTS, nothing recorded.
        S["C"] = _seed_project(u, "CLIBURN 25 MUNDY TEST WT", SLUG_C, status="wrapped")
        # D: two moved copies match (one wrapped, one archived) — ambiguous.
        S["D"] = _seed_project(u, "Two Copies WT", SLUG_D, status="wrapped")
    c = app.test_client()
    with c.session_transaction() as sess:
        sess["_user_id"] = str(S["uid"])
        sess["_fresh"] = True
        sess["_csrf"] = "t"
    c.environ_base["HTTP_X_CSRFTOKEN"] = "t"
    return {"app": app, "client": c}


@pytest.fixture
def dbx(ctx, monkeypatch):
    fake = FakeDbx(files=_files_for(_root(SLUG_A)) + _files_for(_root(SLUG_B))
                   + _files_for(f"{WRAP}/2026-08-07_{SLUG_C}")
                   + _files_for(f"{WRAP}/2026-07-08_{SLUG_D}") + _files_for(f"{ARCH}/2026-09-01_{SLUG_D}"),
                   folders=[WRAP, ARCH])
    monkeypatch.setattr(fpapp, "_dbx_client", lambda: fake)
    monkeypatch.setattr(relink, "_dbx_client", lambda: fake)
    monkeypatch.setenv("DROPBOX_ACCESS_TOKEN", "x")
    monkeypatch.setattr(relink, "_META_SLEEP_S", 0)
    return fake


def _paths(app_, key):
    with app_.app_context():
        d1 = DocUpload.query.get(S[key]["d1"])
        d2 = DocUpload.query.get(S[key]["d2"])
        cs = CallSheetSend.query.get(S[key]["cs"])
        p = ProjectSheet.query.get(S[key]["pid"])
        return {"inv": d1.filed_dropbox_path, "arc": d1.source_archive_path,
                "rcp": d2.filed_dropbox_path, "cs": cs.dropbox_path,
                "root": p.dropbox_root, "slug": p.dropbox_folder, "status": p.status}


# ── 4 (first, pure): the one root rule ───────────────────────────────────

def test_04_project_root_for_prefers_the_recorded_location(ctx):
    from types import SimpleNamespace as NS
    assert fpapp._project_root_for(NS(dropbox_folder="x", dropbox_root=None)) == _root("x")
    assert fpapp._project_root_for(NS(dropbox_folder="x", dropbox_root=f"{WRAP}/2026-08-07_x/")) \
        == f"{WRAP}/2026-08-07_x"
    assert fpapp._project_root_for(NS(dropbox_folder="x")) == _root("x")   # snapshot without the field
    # the relative-path rule knows both shapes
    assert relink._rel_path(f"{_root('x')}/01_ADMIN/a.pdf") == "01_ADMIN/a.pdf"
    assert relink._rel_path(f"{WRAP}/2026-08-07_x/01_ADMIN/a.pdf") == "01_ADMIN/a.pdf"
    assert relink._rel_path(f"{ARCH}/2026-08-07_x/01_ADMIN/b/c.pdf") == "01_ADMIN/b/c.pdf"
    assert relink._rel_path(f"{WRAP}/2026-08-07_x") is None


# ── 1. wrap → rename while wrapped → reactivate ───────────────────────────

def test_01_wrap_records_the_folder_and_rewrites_paths(ctx, dbx):
    c, A = ctx["client"], S["A"]
    r = c.post(f"/projects/{A['pid']}/wrap", data={})
    assert r.status_code in (302, 303), r.data[:300]
    stamp = date.today().strftime("%Y-%m-%d")
    dest = f"{WRAP}/{stamp}_{SLUG_A}"
    assert (_root(SLUG_A), dest) in dbx.moves
    got = _paths(ctx["app"], "A")
    assert got["status"] == "wrapped" and got["root"] == dest and got["slug"] == SLUG_A
    assert got["inv"] == A["inv"].replace(_root(SLUG_A), dest)
    assert got["arc"] == A["arc"].replace(_root(SLUG_A), dest)
    assert got["rcp"] == A["rcp"].replace(_root(SLUG_A), dest)
    assert got["cs"] == A["cs_path"].replace(_root(SLUG_A), dest)
    assert got["inv"].lower() in dbx.files                       # rows point where the files are
    with ctx["app"].app_context():
        row = (ActivityLog.query.filter_by(entity_type="project", entity_id=A["pid"])
               .order_by(ActivityLog.id.desc()).first())
        after = json.loads(row.after_json)
        assert after["status"] == "wrapped" and after["dropbox_root"] == dest
        assert after["dropbox_paths_remapped"]["docs"] == 2
        assert after["dropbox_paths_remapped"]["callsheet_sends"] == 1
        assert "folder moved to" in row.note
        # the app now reads the folder where it is
        p = ProjectSheet.query.get(A["pid"])
        assert fpapp._project_root_for(p) == dest
        assert fpapp._drop_folder_path(p) == f"{dest}/_INBOX"
    S["A_wrapped"] = dest


def test_01b_rename_while_wrapped_keeps_the_folder_where_it_is(ctx, dbx):
    c, A = ctx["client"], S["A"]
    wrapped = S["A_wrapped"]
    # the fake is fresh per test: put the files where the rows now say
    dbx.files = {p.lower(): p for p in _files_for(wrapped)}
    r = c.post(f"/projects/{A['pid']}/rename", data={"name": "Etoro Press Conference WT renamed"})
    assert r.status_code in (302, 303)
    got = _paths(ctx["app"], "A")
    new_slug = got["slug"]
    assert new_slug != SLUG_A and new_slug.startswith("2026-04_Ventana_EtoroPressConferenceWtRenamed")
    expect = wrapped[:-len(SLUG_A)] + new_slug                    # same date stamp, new slug
    assert dbx.moves == [(wrapped, expect)]
    assert got["root"] == expect
    assert got["inv"] == A["inv"].replace(_root(SLUG_A), expect)
    assert got["inv"].lower() in dbx.files
    S["A_wrapped"] = expect
    S["A_slug"] = new_slug


def test_01c_reactivate_moves_back_from_the_recorded_location(ctx, dbx):
    c, A = ctx["client"], S["A"]
    wrapped, slug = S["A_wrapped"], S["A_slug"]
    dbx.files = {p.lower(): p for p in _files_for(wrapped)}
    r = c.post(f"/projects/{A['pid']}/reactivate", data={})
    assert r.status_code in (302, 303)
    assert dbx.moves == [(wrapped, _root(slug))]
    got = _paths(ctx["app"], "A")
    assert got["status"] == "active" and got["root"] is None
    assert got["inv"] == A["inv"].replace(_root(SLUG_A), _root(slug))
    assert got["cs"] == A["cs_path"].replace(_root(SLUG_A), _root(slug))
    assert got["inv"].lower() in dbx.files


# ── 2. archive; a failed move changes nothing ─────────────────────────────

def test_02_archive_records_and_rewrites_and_a_failed_move_changes_nothing(ctx, dbx):
    c, B = ctx["client"], S["B"]
    dbx.fail_move = True
    r = c.post(f"/projects/{B['pid']}/archive", data={})
    assert r.status_code in (302, 303)
    got = _paths(ctx["app"], "B")
    assert got["status"] == "archived" and got["root"] is None and got["inv"] == B["inv"]
    with ctx["app"].app_context():
        row = (ActivityLog.query.filter_by(entity_type="project", entity_id=B["pid"])
               .order_by(ActivityLog.id.desc()).first())
        assert "NOT moved" in row.note
        p = ProjectSheet.query.get(B["pid"]); p.status = "active"; db.session.commit()
    dbx.fail_move = False
    r = c.post(f"/projects/{B['pid']}/archive", data={})
    assert r.status_code in (302, 303)
    dest = f"{ARCH}/{date.today().strftime('%Y-%m-%d')}_{SLUG_B}"
    got = _paths(ctx["app"], "B")
    assert got["status"] == "archived" and got["root"] == dest
    assert got["inv"] == B["inv"].replace(_root(SLUG_B), dest) and got["inv"].lower() in dbx.files
    assert got["arc"] == B["arc"].replace(_root(SLUG_B), dest)


# ── 3. Re-link discovery of a folder moved before paths followed it ───────

def test_03_relink_finds_the_wrapped_folder_and_records_it(ctx, dbx):
    c, C, D = ctx["client"], S["C"], S["D"]
    found = f"{WRAP}/2026-08-07_{SLUG_C}"
    r = c.get(f"/admin/docs/relink?project={C['pid']}")
    assert r.status_code == 200, r.data[:300]
    pv = r.get_json()
    assert pv["project_changes"] == [{"project_id": C["pid"], "project": "CLIBURN 25 MUNDY TEST WT",
                                      "field": "dropbox_root", "old": None, "new": found,
                                      "reason": f"the folder is not at {_root(SLUG_C)}; found at {found}"}]
    assert pv["missing"] == 3 and pv["fixable"] == 3 and pv["needs_person"] == 0
    for row in pv["rows"]:
        assert row["method"] == "wrapped", row
        assert row["new_path"] == row["old_path"].replace(_root(SLUG_C), found)
    # GET wrote nothing
    assert _paths(ctx["app"], "C")["root"] is None
    # the ambiguous project: two moved folders → needs a person, no change proposed
    r = c.get(f"/admin/docs/relink?project={D['pid']}")
    pv = r.get_json()
    assert pv["project_changes"] == []
    assert pv["fixable"] == 0 and pv["needs_person"] == 3
    assert len(pv["project_notes"]) == 1 and "2 moved folders match" in pv["project_notes"][0]["note"]
    assert sorted(pv["project_notes"][0]["candidates"]) == sorted([f"{WRAP}/2026-07-08_{SLUG_D}",
                                                                    f"{ARCH}/2026-09-01_{SLUG_D}"])
    assert all(r_["new_path"] is None for r_ in pv["rows"])


def test_03b_fix_applies_the_project_change_and_the_paths_and_undo_restores(ctx, dbx):
    c, C = ctx["client"], S["C"]
    found = f"{WRAP}/2026-08-07_{SLUG_C}"
    r = c.post(f"/admin/docs/relink?project={C['pid']}&apply=1", json={})
    assert r.status_code == 200, r.data[:300]
    out = r.get_json()
    assert out["fixed"] == 3 and out["stale"] == []
    assert len(out["project_changes_applied"]) == 1
    pc = out["project_changes_applied"][0]
    assert pc["new"] == found and pc["activity_id"]
    got = _paths(ctx["app"], "C")
    assert got["root"] == found
    assert got["inv"] == C["inv"].replace(_root(SLUG_C), found) and got["inv"].lower() in dbx.files
    assert got["arc"] == C["arc"].replace(_root(SLUG_C), found)
    with ctx["app"].app_context():
        row = ActivityLog.query.get(pc["activity_id"])
        assert row.entity_type == "project" and json.loads(row.before_json) == {"dropbox_root": None}
        assert json.loads(row.after_json) == {"dropbox_root": found, "relink": True}
        assert "nothing moved in Dropbox" in row.note
        # nothing was moved or created in Dropbox
    assert dbx.moves == [] and dbx.created == []
    # a second preview finds nothing left to do
    r = c.get(f"/admin/docs/relink?project={C['pid']}")
    pv = r.get_json()
    assert pv["missing"] == 0 and pv["project_changes"] == [] and pv["present"] == 3
    # Undo the project-level change from the project's activity log
    r = c.post(f"/projects/{C['pid']}/budget/{C['bid']}/activity/{pc['activity_id']}/undo", json={})
    assert r.status_code == 200, r.data[:300]
    assert _paths(ctx["app"], "C")["root"] is None
    # a wrap's own activity row is NOT undoable (it recorded a real move)
    with ctx["app"].app_context():
        wrap_row = (ActivityLog.query.filter_by(entity_type="project", entity_id=S["A"]["pid"])
                    .filter(ActivityLog.note.like('Wrapped project%')).order_by(ActivityLog.id.desc()).first())
    r = c.post(f"/projects/{S['A']['pid']}/budget/{S['A']['bid']}/activity/{wrap_row.id}/undo", json={})
    assert r.status_code == 400
