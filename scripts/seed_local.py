"""Boot FPBudget on a throw-away sqlite file with a small seeded project and
serve it locally. This is the fixture the browser (Playwright) suite runs
against, in CI and on a laptop — no Postgres, Dropbox, QuickBooks or Veryfi.

What it seeds
  * one super-admin (local-admin@thefp.tv / localpass-123) and a plain editor
  * the global fringe rates, payroll profiles and the "FP Standard" template
  * two active projects; the first has a Working budget with a few sections
    and lines (one fringe summary row), transactions in every Actuals state
    (unclaimed with a suggestion, duplicate pair, refund, cash paid by crew,
    assigned with / without backup, split, not-project, claimed elsewhere)
    and three receipts in the inbox

Usage (from the repo root)
  python scripts/seed_local.py --port 5055            # seed + serve
  python scripts/seed_local.py --db /tmp/x.db --keep  # reuse an existing file
  python scripts/seed_local.py --no-serve             # seed only

Cleanup F, 2026-11-02.
"""
import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

EMAIL, PW = "local-admin@thefp.tv", "localpass-123"
EDITOR_EMAIL, EDITOR_PW = "local-editor@thefp.tv", "localpass-123"


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--db", default=os.path.join(ROOT, "instance", "seed_local.db"),
                    help="sqlite file to boot against (default: instance/seed_local.db)")
    ap.add_argument("--port", type=int, default=int(os.environ.get("PORT", "5055")))
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--keep", action="store_true",
                    help="keep an existing --db file instead of starting fresh")
    ap.add_argument("--no-serve", action="store_true", help="seed and exit")
    return ap.parse_args(argv)


def configure_env(db_path):
    os.environ["DATABASE_URL"] = "sqlite:///" + os.path.abspath(db_path)
    os.environ.setdefault("SECRET_KEY", "local-dev-secret")
    # "0" is set (so the boot-time background threads — trash purge, drop
    # sweep, QBO auto-sync — stay off) but is not "1", so no in-process
    # boot tasks run either.
    os.environ.setdefault("RUN_BOOT_TASKS", "0")
    os.environ.setdefault("WTF_CSRF_ENABLED", "1")
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)


def seed(db, models, budget_calc):
    """Create the schema and the seed data. Returns the first project's id."""
    (User, ProjectSheet, Budget, BudgetLine, Transaction, DocUpload, CrewMember,
     TransactionSplit, ExpenseEvidence) = (
        models.User, models.ProjectSheet, models.Budget, models.BudgetLine,
        models.Transaction, models.DocUpload, models.CrewMember,
        models.TransactionSplit, models.ExpenseEvidence)

    db.create_all()
    if User.query.filter_by(email=EMAIL).first():
        return ProjectSheet.query.order_by(ProjectSheet.id).first().id

    budget_calc.seed_fringes(db.session)
    budget_calc.seed_payroll_profiles(db.session)
    budget_calc.seed_standard_template(db.session)

    u = User(email=EMAIL, name="Local Admin", role="super_admin")
    u.set_password(PW)
    ed = User(email=EDITOR_EMAIL, name="Local Editor", role="line_producer")
    ed.set_password(EDITOR_PW)
    db.session.add_all([u, ed])
    crew = CrewMember(name="James Allerdyce", department="Production", active=True)
    crew2 = CrewMember(name="Sam Rivera", department="Camera", active=True)
    db.session.add_all([crew, crew2])
    p = ProjectSheet(name="GrubHub Fee Breakdown", client_name="GrubHub", status="active",
                     qbo_account_ids=json.dumps(["41", "42"]), sync_through="2026-10-30",
                     last_synced=datetime.utcnow() - timedelta(hours=2))
    p2 = ProjectSheet(name="Ventana Live · Oct 4-7", client_name="Ventana", status="active",
                      qbo_account_ids=json.dumps(["41"]))
    db.session.add_all([p, p2])
    db.session.flush()
    b = Budget(project_id=p.id, name="v3", budget_mode="working", version_status="current",
               is_actual=False)
    db.session.add(b)
    db.session.flush()

    def line(code, name, desc, est, **kw):
        ln = BudgetLine(budget_id=b.id, account_code=code, account_name=name, description=desc,
                        estimated_total=Decimal(str(est)), rate=Decimal(str(est)), quantity=1,
                        days=1, sort_order=kw.pop("sort", 0), **kw)
        db.session.add(ln)
        db.session.flush()
        return ln

    line(2001, "Production Staff", "Director of Photography", 6000, is_labor=True, sort=1)
    line(2002, "Production Staff", "Camera Operator", 4400, is_labor=True, sort=2)
    ac1 = line(2003, "Production Staff", "1st Assistant Camera", 3200, is_labor=True, sort=3)
    line(2003, "Production Staff", "Kit fee", 300, parent_line_id=ac1.id, sort=4)
    line(2099, "Production Staff", "Payroll Fringes", 3565, line_tag="fringe_rollup",
         sync_omit=True, sort=9)
    pkg = line(2601, "Camera Equipment", "Camera package", 4200, sort=1)
    lens = line(2602, "Camera Equipment", "Lenses", 1200, sort=2)
    media = line(2603, "Camera Equipment", "Media & cards", 500, sort=3)
    meals = line(3701, "Production Meals & Craft Services", "Crew meals", 1800, sort=1)
    ride = line(3401, "Transportation", "Rideshare", 900, sort=1)
    line(3402, "Transportation", "Parking & tolls", 400, sort=2)
    line(6501, "Administrative", "Office supplies", 300, sort=1)

    now = datetime.utcnow()

    def txn(vendor, amount, d, **kw):
        t = Transaction(project_id=p.id, vendor=vendor, amount=Decimal(str(abs(amount))),
                        is_expense=amount >= 0, txn_date=d, created_at=now, updated_at=now, **kw)
        db.session.add(t)
        db.session.flush()
        return t

    # Unclaimed QBO charges (one with a suggestion, one duplicate pair, one refund)
    txn("Lensrentals", 912.00, "2026-10-22", source="qbo_sync", qbo_txn_id="18822",
        qbo_txn_type="Purchase", card_last4="4417", note="Lens rental week 2",
        suggested_budget_line_id=pkg.id, ai_code_confidence=Decimal("0.92"),
        ai_code_reason="Vendor coded to Camera package 4 times before")
    txn("B&H Photo Video", 372.50, "2026-10-21", source="qbo_sync", qbo_txn_id="18810",
        qbo_txn_type="Purchase", card_last4="4417", suggested_budget_line_id=media.id,
        ai_code_confidence=Decimal("0.71"))
    txn("Uber", 48.20, "2026-10-21", source="qbo_sync", qbo_txn_id="18801",
        qbo_txn_type="Purchase", card_last4="4417", suggested_budget_line_id=ride.id,
        ai_code_confidence=Decimal("0.88"))
    txn("Uber", 48.20, "2026-10-21", source="qbo_sync", qbo_txn_id="18802",
        qbo_txn_type="Purchase", card_last4="4417", suggested_budget_line_id=ride.id,
        ai_code_confidence=Decimal("0.88"))
    txn("Wrapbook Payroll", 6218.40, "2026-10-20", source="qbo_sync", qbo_txn_id="18790",
        qbo_txn_type="BillPayment", note="week ending Oct 18")
    # Phase 5 (Nov 3): the same $4,500 BillPayment that posted twice a day
    # apart (Ryan Mauskopf, Oct 19 / Oct 20) — the Duplicates view's case —
    # and a Lyft charge with its receipt waiting in the inbox (strong match).
    txn("Ryan Mauskopf", 4500.00, "2026-10-19", source="qbo_sync", qbo_txn_id="1300000319",
        qbo_txn_type="BillPayment", note="invoice 2026-10 · ACH")
    txn("Ryan Mauskopf", 4500.00, "2026-10-20", source="qbo_sync", qbo_txn_id="1300000322",
        qbo_txn_type="BillPayment", note="invoice 2026-10 · ACH")
    txn("LYFT *RIDE SUN 9PM", 88.44, "2026-09-28", source="qbo_sync", qbo_txn_id="18402",
        qbo_txn_type="Purchase", card_last4="4417")
    t6 = txn("Sweetgreen", 186.30, "2026-10-19", source="qbo_sync", qbo_txn_id="18780",
             qbo_txn_type="Purchase", card_last4="4417", suggested_budget_line_id=meals.id,
             ai_code_confidence=Decimal("0.95"))
    txn("The Home Depot", 64.11, "2026-10-18", source="qbo_sync", qbo_txn_id="18770",
        qbo_txn_type="Purchase", card_last4="9023")
    txn("Amazon", 129.47, "2026-10-16", source="qbo_sync", qbo_txn_id="18760",
        qbo_txn_type="Purchase", card_last4="4417")
    txn("Lensrentals", -120.00, "2026-09-26", source="qbo_sync", qbo_txn_id="2210",
        qbo_txn_type="CreditCardCredit", card_last4="4417")
    # Manual cash expense, paid by crew, unassigned
    txn("Parking — Seaport garage", 32.00, "2026-10-17", source="manual_entry",
        payment_method="cash", paid_by_crew_member_id=crew.id,
        no_charge_reason="cash — garage was cash only", activated_at=now,
        match_status="confirmed")
    # Assigned with receipt (green), assigned without backup (red),
    # assigned with a backup reason (yellow)
    t11 = txn("Lensrentals", 1480.00, "2026-10-09", source="qbo_sync", qbo_txn_id="18640",
              qbo_txn_type="Purchase", card_last4="4417", budget_line_id=pkg.id,
              account_code=2601, account_code_name="Camera package", match_status="confirmed")
    txn("Lensrentals", 640.00, "2026-09-26", source="qbo_sync", qbo_txn_id="18311",
        qbo_txn_type="Purchase", card_last4="4417", budget_line_id=lens.id,
        account_code=2602, account_code_name="Lenses")
    txn("Sweetgreen", 2046.00, "2026-10-12", source="qbo_sync", qbo_txn_id="18700",
        qbo_txn_type="Purchase", card_last4="4417", budget_line_id=meals.id,
        account_code=3701, account_code_name="Crew meals",
        no_charge_reason="vendor emails no receipts")
    txn("Lyft", 118.00, "2026-10-11", source="qbo_sync", qbo_txn_id="18690",
        qbo_txn_type="Purchase", card_last4="9023", budget_line_id=ride.id,
        account_code=3401, account_code_name="Rideshare")
    # Split parent across two lines
    t15 = txn("Lensrentals", 2310.00, "2026-09-12", source="qbo_sync", qbo_txn_id="17904",
              qbo_txn_type="Purchase", card_last4="4417", match_status="confirmed")
    db.session.add_all([
        TransactionSplit(transaction_id=t15.id, budget_line_id=pkg.id, project_id=p.id,
                         amount=Decimal("1500.00"), created_at=now, created_by_user_id=u.id),
        TransactionSplit(transaction_id=t15.id, budget_line_id=lens.id, project_id=p.id,
                         amount=Decimal("810.00"), created_at=now, created_by_user_id=u.id),
    ])
    # Not project
    txn("Lensrentals", 88.00, "2026-08-30", source="qbo_sync", qbo_txn_id="17522",
        qbo_txn_type="Purchase", card_last4="9023", not_project_expense=True,
        excluded_at=now - timedelta(days=8), excluded_by_user_id=u.id,
        excluded_reason="Personal rental · S. Pierce")
    txn("Netflix", 15.49, "2026-10-01", source="qbo_sync", qbo_txn_id="18500",
        qbo_txn_type="Purchase", card_last4="9023", not_project_expense=True,
        excluded_at=now - timedelta(days=2), excluded_by_user_id=u.id, excluded_reason=None)
    # The same QBO charge synced into project 2 and claimed there (claimed elsewhere)
    db.session.add(Transaction(
        project_id=p2.id, vendor="Adorama", amount=Decimal("455.00"), is_expense=True,
        txn_date="2026-10-05", source="qbo_sync", qbo_txn_id="18600", qbo_txn_type="Purchase",
        card_last4="4417", account_code=2601, account_code_name="Camera package",
        created_at=now, updated_at=now))
    txn("Adorama", 455.00, "2026-10-05", source="qbo_sync", qbo_txn_id="18600",
        qbo_txn_type="Purchase", card_last4="4417", not_project_expense=True,
        claimed_by_project_id=p2.id)
    # Receipts: one unmatched in the inbox, one confirmed on t11, one suggested on t6
    d1 = DocUpload(project_id=p.id, uploader_id=u.id, original_filename="IMG_4412.jpg",
                   filed_filename="2026-10-17_RECEIPT_Parking_Seaport_32.00.pdf", status="filed",
                   vendor="Seaport Garage", amount=Decimal("32.00"), doc_date=date(2026, 10, 17),
                   category="receipt", confidence=Decimal("94"), content_type="image/jpeg")
    d2 = DocUpload(project_id=p.id, uploader_id=u.id, original_filename="lensrentals-inv-18640.pdf",
                   filed_filename="2026-10-09_RECEIPT_Camera_Lensrentals_1480.00.pdf",
                   status="filed", vendor="Lensrentals", amount=Decimal("1480.00"),
                   doc_date=date(2026, 10, 9), category="receipt", content_type="application/pdf")
    d3 = DocUpload(project_id=p.id, uploader_id=u.id, original_filename="sweetgreen-oct19.jpg",
                   filed_filename="2026-10-19_RECEIPT_Meals_Sweetgreen_186.30.pdf", status="filed",
                   vendor="Sweetgreen", amount=Decimal("186.30"), doc_date=date(2026, 10, 19),
                   category="receipt", content_type="image/jpeg")
    d4 = DocUpload(project_id=p.id, uploader_id=u.id, original_filename="lyft-sep28.png",
                   filed_filename="2026-09-28_RECEIPT_Travel_Lyft_88.44.pdf", status="filed",
                   vendor="Lyft", amount=Decimal("88.44"), doc_date=date(2026, 9, 28),
                   category="receipt", content_type="image/png", card_last4="4417")
    db.session.add_all([d1, d2, d3, d4])
    db.session.flush()
    t11.doc_upload_id = d2.id
    db.session.add(ExpenseEvidence(transaction_id=t11.id, doc_upload_id=d2.id, kind="primary"))
    t6.doc_upload_id = d3.id
    t6.match_status = "suggested"
    t6.match_confidence = Decimal("0.90")
    db.session.commit()
    print(f"seeded project {p.id} budget {b.id}", flush=True)
    return p.id


def main(argv=None):
    args = parse_args(argv)
    if os.path.exists(args.db) and not args.keep:
        os.remove(args.db)
    configure_env(args.db)
    sys.path.insert(0, ROOT)
    os.chdir(ROOT)

    import app as appmod  # noqa: E402  (boots the Flask app against the sqlite file)
    import models  # noqa: E402
    import budget_calc  # noqa: E402

    app = appmod.app
    with app.app_context():
        pid = seed(models.db, models, budget_calc)
        with open(os.path.splitext(args.db)[0] + ".json", "w") as f:
            json.dump({"pid": pid, "email": EMAIL, "password": PW,
                       "editor_email": EDITOR_EMAIL, "editor_password": EDITOR_PW}, f)
    print(f"login {EMAIL} / {PW}  (editor {EDITOR_EMAIL} / {EDITOR_PW})", flush=True)

    if args.no_serve:
        return 0
    print(f"serving on http://{args.host}:{args.port}", flush=True)
    app.run(host=args.host, port=args.port, debug=False, use_reloader=False, threaded=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
