from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from datetime import datetime

db = SQLAlchemy()


class User(db.Model, UserMixin):
    __tablename__ = "users"
    id                   = db.Column(db.Integer, primary_key=True)
    email                = db.Column(db.String(200), unique=True, nullable=False)
    name                 = db.Column(db.String(200), nullable=True)
    password_hash        = db.Column(db.String(256), nullable=False)
    role                 = db.Column(db.String(20), default="line_producer", nullable=False)
    # "super_admin" | "admin" | "line_producer" | "dept_head"
    dept_code            = db.Column(db.Integer, nullable=True)
    # Only used when role == "dept_head". Stores the COA section code (e.g. 3000 for Grip & Electric)
    is_active            = db.Column(db.Boolean, default=True, nullable=False)
    must_change_password = db.Column(db.Boolean, default=False, nullable=False)
    created_at           = db.Column(db.DateTime, default=datetime.utcnow)
    reset_token          = db.Column(db.String(100), nullable=True)
    reset_token_expires  = db.Column(db.DateTime, nullable=True)
    phone                = db.Column(db.String(50), nullable=True)

    def set_password(self, pw):
        from werkzeug.security import generate_password_hash
        self.password_hash = generate_password_hash(pw)

    def check_password(self, pw):
        from werkzeug.security import check_password_hash
        return check_password_hash(self.password_hash, pw)

    @property
    def is_admin(self):
        return self.role in ('super_admin', 'admin')

    @property
    def is_docs_only(self):
        return self.role == 'docs_only'

    @property
    def display_role(self):
        return {
            'super_admin': 'Super Admin',
            'admin': 'Admin',
            'line_producer': 'Line Producer',
            'dept_head': 'Dept Head',
            'docs_only': 'Docs / Receipts',
        }.get(self.role, self.role)


class ProjectAccess(db.Model):
    __tablename__ = "project_access"
    id         = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.Integer, db.ForeignKey("project_sheet.id"), nullable=False)
    user_id    = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    role       = db.Column(db.String(20), default="editor")  # owner | editor | viewer | docs_only (legacy: collaborator→editor)
    __table_args__ = (db.UniqueConstraint("project_id", "user_id", name="uq_proj_user"),)

# ── Mirrored shared tables (read-only from FPBudget) ─────────────────────────

class ProjectSheet(db.Model):
    __tablename__ = "project_sheet"
    id             = db.Column(db.Integer, primary_key=True)
    name           = db.Column(db.String(200), nullable=False)
    dropbox_folder = db.Column(db.String(300), nullable=True)   # relative slug under ops root
    client_name    = db.Column(db.String(200), nullable=True)   # used for slug + display
    status         = db.Column(db.String(20), default='active', nullable=False)  # active | wrapped | archived

    # ── QBO sync state (added 2026-04-30 cutover) ──────────────────────
    # Per-project subset of QBOConnection.enabled_account_ids. Empty
    # array = sync nothing. JSON-encoded list of QBO account ids.
    qbo_account_ids = db.Column(db.Text, default='[]')
    # YYYY-MM-DD watermark — sync_project never advances this past
    # yesterday so the most recent day stays open for re-query (bank
    # feeds often arrive a day late).
    sync_through    = db.Column(db.String(10), nullable=True)
    last_synced     = db.Column(db.DateTime, nullable=True)
    # CDC (Change Data Capture) watermark — by MetaData.LastUpdatedTime
    # rather than TxnDate. Catches bank-feed items accepted into QBO
    # weeks after their transaction date.
    last_cdc_sync   = db.Column(db.DateTime, nullable=True)

    # Project-wide DEFAULT call-sheet logo arrangement (2026-07-09). JSON array
    # of {logo_id, x, w} — the same shape a per-day CallSheetData.data_json
    # 'logos' key holds. A call sheet with no 'logos' key of its own falls back
    # to this. Set via the "Set as project default" button on any sheet.
    logos_default   = db.Column(db.Text, nullable=True)


class Transaction(db.Model):
    """Single actuals row. The center of the three-legged stool:
       BudgetLine (planned) ← Transaction (actual) → DocUpload (backup).

    Three ingress paths set `source`:
      • qbo_sync     — pulled from QuickBooks Online
      • doc_upload   — auto-created from a receipt/invoice OCR
      • manual_entry — user-typed in the Actuals tab
      • invoice_split — child of a parent invoice transaction
    """
    __tablename__ = "transaction"
    id                  = db.Column(db.Integer, primary_key=True)
    project_id          = db.Column(db.Integer, db.ForeignKey("project_sheet.id"))
    account_code        = db.Column(db.Integer)
    account_code_name   = db.Column(db.String(100))
    amount              = db.Column(db.Numeric(12, 2))
    is_expense          = db.Column(db.Boolean, default=True)
    not_project_expense = db.Column(db.Boolean, default=False)
    vendor              = db.Column(db.String(300))
    txn_date            = db.Column(db.String(10))   # YYYY-MM-DD
    note                = db.Column(db.Text)
    # Last 4 of the card/account (added 2026-05-30). Synced from the linked
    # DocUpload's card_last4 so the Actuals tab can sort/group by card.
    card_last4          = db.Column(db.String(8), nullable=True)

    # ── Three-legged-stool linkage (added 2026-04-30) ─────────────────
    # FKs nullable so a freshly-ingested QBO txn (no doc, no line yet)
    # is still a valid row. The Actuals UI surfaces the missing legs
    # so the user can fill them in.
    budget_line_id        = db.Column(db.Integer, db.ForeignKey("budget_line.id"), nullable=True)
    doc_upload_id         = db.Column(db.Integer, db.ForeignKey("doc_upload.id"), nullable=True)
    parent_transaction_id = db.Column(db.Integer, db.ForeignKey("transaction.id"), nullable=True)
    # Backup-doc linkage (v2, 2026-07-20): this charge is DOCUMENTATION backing
    # another transaction (typically an invoice_split subline). Set → excluded
    # from rollups and shown 📎-attached to its target in the Line Ledger.
    backup_of_txn_id      = db.Column(db.Integer, db.ForeignKey("transaction.id"), nullable=True)
    # Actualizing 2.0 A1 (2026-07-20): documents are EVIDENCE, not expenses.
    # A doc-born row (source='doc_upload') may only be coded once the user has
    # explicitly CREATED an expense from it — stamped here. Imported rows
    # (qbo/csv) are expenses by definition and never need it.
    activated_at          = db.Column(db.DateTime, nullable=True)
    source                = db.Column(db.String(20), default='manual_entry')
    # match_status: unmatched | suggested | confirmed. Suggestions
    # written by the auto-matcher, never silently committed — user
    # must hit Confirm to flip suggested → confirmed.
    match_status      = db.Column(db.String(20), default='unmatched')
    match_confidence  = db.Column(db.Numeric(4, 3), nullable=True)
    suggested_budget_line_id = db.Column(db.Integer, db.ForeignKey("budget_line.id"), nullable=True)
    suggested_account_code   = db.Column(db.Integer, nullable=True)
    # Split receipt (2026-06-16): when ONE receipt backs several charges (e.g.
    # Turo posts a rental as two card charges), each of those charge rows shares
    # a split_group id and all point doc_upload_id at the SAME receipt. The
    # receipt total should equal the sum of the group's amounts. Marks the link
    # as intentional so the duplicate scan + auto-matcher don't re-flag it, and
    # lets exports show the shared source + allocation.
    split_group       = db.Column(db.String(40), nullable=True, index=True)

    # ── QBO ingestion fields (used when source='qbo_sync') ─────────────
    qbo_txn_id      = db.Column(db.String(50), nullable=True)
    qbo_txn_type    = db.Column(db.String(20), nullable=True)   # Purchase | Deposit
    qbo_account_id  = db.Column(db.String(50), nullable=True)
    qbo_category    = db.Column(db.String(200), nullable=True)

    # ── Cross-project claim (2026-05-07) ───────────────────────────────
    # An electronic transaction (qbo_txn_id present) can only be billed
    # to one project at a time. When project A codes it, every parallel
    # row across other projects gets this set to A's project_id so it
    # can be hidden from B's "uncoded" pile and shown to admins+ as
    # "claimed elsewhere → Project A · Account 2100". Releasing the
    # claim (clearing budget_line_id and account_code in A) clears it
    # everywhere. Self-heal DDL adds the column on every worker boot;
    # `nullable=True` so legacy rows are valid.
    claimed_by_project_id = db.Column(db.Integer,
                                      db.ForeignKey("project_sheet.id"),
                                      nullable=True)

    # ── AI auto-coding suggestion (2026-06-18) ─────────────────────────
    # Advisory only: ai_layer.categorize() (or a learned vendor→code mapping)
    # proposes a COA section for an UNCODED charge. Kept separate from
    # account_code so the Actuals UI can show a "✨ suggested" chip; the user
    # clicks Accept (→ set-coa) to confirm. Cleared the instant the row is
    # coded. NEVER auto-applied — AI is advisory, per the app's AI rules.
    ai_suggested_code      = db.Column(db.Integer, nullable=True)
    ai_suggested_code_name = db.Column(db.String(100), nullable=True)
    ai_code_confidence     = db.Column(db.Numeric(4, 3), nullable=True)
    ai_code_reason         = db.Column(db.String(300), nullable=True)
    # AI vendor-cleanup watermark for charges with no receipt (CSV/QBO). NULL =
    # not yet cleaned; lets batch cleanup resume. (2026-06-18.)
    ai_cleaned_at          = db.Column(db.DateTime, nullable=True)
    # AI matching: watermark (charge examined by the AI match pass — lets batches
    # resume + skip declined charges) + who proposed the current match.
    ai_match_checked_at    = db.Column(db.DateTime, nullable=True)
    match_source           = db.Column(db.String(12), nullable=True)  # heuristic | ai | manual

    # ── Line-ledger review (2026-07) ───────────────────────────────────
    # Per-transaction "reviewed" state for the Line Ledger side panel:
    # the user ticks a charge as checked-off against its budget line.
    # Purely advisory — does NOT affect totals or matching. Nullable so
    # legacy rows are unreviewed by default. Self-heal DDL adds these on
    # every worker boot.
    reviewed_at         = db.Column(db.DateTime, nullable=True)
    reviewed_by         = db.Column(db.String(120), nullable=True)

    # ── Provenance ──────────────────────────────────────────────────────
    created_via_user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class QBOConnection(db.Model):
    """OAuth tokens for QuickBooks Online. Single global row for now —
    multi-user QBO connections come later. Updated in place; the
    refresh_token rotates whenever Intuit sends a new one."""
    __tablename__ = "qbo_connection"
    id                  = db.Column(db.Integer, primary_key=True)
    realm_id            = db.Column(db.String(50))
    access_token        = db.Column(db.Text)
    refresh_token       = db.Column(db.Text)
    token_expiry        = db.Column(db.DateTime)
    enabled_account_ids = db.Column(db.Text, default="[]")  # JSON array of QBO account ids
    updated_at          = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class CategoryMapping(db.Model):
    """Learned mapping: QBO category (+ optional vendor) → FP COA code.
    Built up as the user confirms suggestions; the auto-matcher reads
    it back to predict the right account_code on subsequent imports."""
    __tablename__ = "category_mapping"
    id           = db.Column(db.Integer, primary_key=True)
    qbo_category = db.Column(db.String(200), nullable=False)
    vendor_name  = db.Column(db.String(300), nullable=True)
    coa_code     = db.Column(db.Integer, nullable=False)
    coa_name     = db.Column(db.String(100), nullable=False)
    usage_count  = db.Column(db.Integer, default=0)
    last_used    = db.Column(db.DateTime, default=datetime.utcnow)
    __table_args__ = (db.UniqueConstraint("qbo_category", "vendor_name",
                                           name="uq_category_vendor"),)


# ── Budget-specific tables ────────────────────────────────────────────────────

class Budget(db.Model):
    __tablename__ = "budget"
    id              = db.Column(db.Integer, primary_key=True)
    project_id      = db.Column(db.Integer, nullable=False)
    name            = db.Column(db.String(200), nullable=False)
    budget_mode     = db.Column(db.String(20), default="estimated")  # estimated | schedule | hybrid
    company_fee_pct       = db.Column(db.Numeric(6, 4), default=0.18)
    company_fee_dispersed = db.Column(db.Boolean, default=False, nullable=False)
    # Production Company Fee mode — 'pct' (default, fee = pct × eligible
    # subtotal) or 'flat' (fee = company_fee_flat, fixed dollar amount
    # regardless of subtotal). Added 2026-05-06 per user request to
    # support fixed-fee production deals where the producer's fee is
    # negotiated as a single number rather than a markup. Self-heal DDL
    # backfills existing rows to 'pct' so behavior is unchanged.
    company_fee_mode      = db.Column(db.String(10), default='pct',
                                      nullable=False, server_default='pct')
    company_fee_flat      = db.Column(db.Numeric(14, 2), default=0,
                                      server_default='0')
    # JSON-encoded array of COA section codes EXCLUDED from the
    # production-company fee base. NULL / empty = every section
    # contributes (default). Edited via budget Settings → "Sections
    # exempt from Prod Co Fee".  Column added manually via psql on
    # 2026-04-25 after the boot migration kept failing under the 5s
    # statement_timeout watchdog.
    fee_excluded_sections = db.Column(db.Text, nullable=True)
    # Industry standard: Production Company Fee does NOT compound on top
    # of fringes (P&W, P/H/W, etc.) — fringes are treated as a labor
    # pass-through cost. Default TRUE: every line's fringe_amount is
    # subtracted from the section total before computing the fee.
    # Column is guaranteed present by the per-worker essential-column
    # pass at the bottom of app.py, which runs IF-NOT-EXISTS on every
    # gunicorn web-worker boot. Safe to declare on the ORM again now.
    fee_exclude_fringes = db.Column(db.Boolean, default=True, nullable=False,
                                     server_default='1')
    # Production Insurance — modeled the same way as Workers' Comp and
    # Payroll Service Fee: an auto-calculated line that injects into the
    # Insurance (6000) section. Mode picker ('off' / 'pct' / 'flat')
    # gives the user the option to skip it entirely, charge it as a
    # percentage of labor wages, or as a flat dollar amount per project.
    # Default is 'off' so existing budgets are untouched.
    # Default ON at 1.5% of labor wages — industry-typical for general
    # production liability + E&O on a small-mid project. User can switch
    # to flat mode or off explicitly per project. New budgets get this
    # default; existing budgets are backfilled by the worker-boot
    # essential-cols pass below.
    production_insurance_mode = db.Column(db.String(10), default='pct',
                                           nullable=False, server_default='pct')
    production_insurance_pct  = db.Column(db.Numeric(8, 6), default=0.015,
                                           server_default='0.015')
    production_insurance_flat = db.Column(db.Numeric(12, 2), default=0)
    created_at      = db.Column(db.DateTime, default=datetime.utcnow)
    # Project settings
    start_date      = db.Column(db.Date, nullable=True)
    end_date        = db.Column(db.Date, nullable=True)
    target_budget   = db.Column(db.Numeric(14, 2), nullable=True)
    notes           = db.Column(db.Text, nullable=True)
    payroll_profile_id   = db.Column(db.Integer, db.ForeignKey("payroll_profile.id"), nullable=True)
    payroll_week_start   = db.Column(db.Integer, nullable=True)   # overrides profile default; NULL = use profile
    payroll_profile      = db.relationship("PayrollProfile", foreign_keys=[payroll_profile_id])
    working_initialized_at = db.Column(db.DateTime, nullable=True)
    # Display
    timezone        = db.Column(db.String(60), default='America/Los_Angeles', nullable=True)
    # Auto-calculated % line items
    workers_comp_pct = db.Column(db.Numeric(8, 6), default=0.03,   nullable=True)   # % of gross labor wages
    payroll_fee_pct  = db.Column(db.Numeric(8, 6), default=0.0175, nullable=True)   # % of gross labor wages
    # Per-budget production details (for exports/approvals)
    client_name       = db.Column(db.String(200), nullable=True)
    prepared_by       = db.Column(db.String(200), nullable=True)
    prepared_by_title = db.Column(db.String(100), nullable=True)
    prepared_by_email = db.Column(db.String(200), nullable=True)
    prepared_by_phone = db.Column(db.String(50),  nullable=True)
    # Version management
    updated_at      = db.Column(db.DateTime, default=datetime.utcnow, nullable=True)
    version_status  = db.Column(db.String(20), default='current', nullable=False)  # current | superseded | archived
    parent_budget_id = db.Column(db.Integer, db.ForeignKey('budget.id'), nullable=True)
    version_number  = db.Column(db.Integer, nullable=True)   # shared by Estimated + its Working pair

    # Marks an Actual budget — peer to Estimated/Working but represents
    # accounting reality, not planning intent. Auto-cloned from Working
    # the first time a Transaction is linked to a budget line in this
    # project. Lines on an actual budget point back at their Working
    # source via BudgetLine.source_line_id; transactions FK directly
    # to the actual lines (never to Working lines).
    is_actual       = db.Column(db.Boolean, default=False, nullable=False,
                                server_default='0')
    lines           = db.relationship("BudgetLine", backref="budget", lazy=True,
                                      cascade="all, delete-orphan")
    schedule_days   = db.relationship("ScheduleDay", backref="budget", lazy=True,
                                      cascade="all, delete-orphan")
    tax_credits     = db.relationship("TaxCredit", backref="budget", lazy=True,
                                      cascade="all, delete-orphan")


class BudgetLine(db.Model):
    __tablename__ = "budget_line"
    id              = db.Column(db.Integer, primary_key=True)
    budget_id       = db.Column(db.Integer, db.ForeignKey("budget.id"), nullable=False)
    account_code    = db.Column(db.Integer, nullable=False)
    account_name    = db.Column(db.String(100), nullable=False)
    description     = db.Column(db.String(300), nullable=True)
    is_labor        = db.Column(db.Boolean, default=False)
    sort_order      = db.Column(db.Integer, default=0)

    # Non-labor / flat estimate
    estimated_total = db.Column(db.Numeric(12, 2), default=0)

    # Labor fields
    payroll_co      = db.Column(db.String(50), nullable=True)
    quantity        = db.Column(db.Numeric(8, 2), default=1)
    days            = db.Column(db.Numeric(8, 2), default=1)
    rate            = db.Column(db.Numeric(12, 2), default=0)
    rate_type       = db.Column(db.String(20), default="day_10")
    # day_8 | day_10 | day_12 | flat_day | flat_project | hourly | custom
    est_ot          = db.Column(db.Numeric(12, 2), default=0)
    fringe_type     = db.Column(db.String(5), default="N")   # E|N|L|U|S|I
    agent_pct       = db.Column(db.Numeric(6, 4), default=0)
    note            = db.Column(db.String(300), nullable=True)
    use_schedule    = db.Column(db.Boolean, default=False)

    days_unit       = db.Column(db.String(10), default="days")  # "days" | "weeks"
    days_per_week   = db.Column(db.Numeric(4, 1), default=5.0)  # for schedule→weeks conversion

    # Parent linking (kit fees + other child rows)
    parent_line_id  = db.Column(db.Integer, db.ForeignKey("budget_line.id"), nullable=True)
    # Identifies auto-managed lines: 'kit_fee' | 'hotel_talent' | 'meal_first' | etc.
    line_tag        = db.Column(db.String(50), nullable=True)
    # If True, sync_schedule_driven_lines will not update this line (user opted out of auto-calc)
    sync_omit       = db.Column(db.Boolean, default=False)
    # Travel role group override: 'talent' | 'atl' | 'crew'  (falls back to COA code)
    role_group      = db.Column(db.String(20), nullable=True)
    # Per-unit rate used by schedule-driven non-labor lines (e.g. $25/meal, $150/night)
    unit_rate       = db.Column(db.Numeric(10, 2), nullable=True)
    assigned_crew_id = db.Column(db.Integer, db.ForeignKey("crew_member.id"), nullable=True)
    assigned_crew    = db.relationship("CrewMember", foreign_keys=[assigned_crew_id])

    # Link back to the CatalogItem row this line was created from (when added
    # via Quick Entry). Used by export logic to resolve role_tag → MMB /
    # ShowBiz target account via RoleTagMapping. NULL for legacy rows and
    # for lines added via free-text entry; exports fall back to fuzzy match
    # on (account_code, description) when NULL.
    catalog_item_id  = db.Column(db.Integer, db.ForeignKey("catalog_item.id"), nullable=True)

    # Purchase order assignment (added 2026-05-04 per user). Non-labor
    # rows can be tied to a PurchaseOrder so vendor commitments + cap
    # warnings flow through the row. Optional / nullable. Labor rows
    # don't typically use this — they go through CrewMember instead.
    po_id            = db.Column(db.Integer, db.ForeignKey("purchase_order.id"), nullable=True)

    # Three-phase system columns
    working_total   = db.Column(db.Numeric(14, 2), nullable=True)  # Working forecast (snapshot + evolving)
    manual_actual   = db.Column(db.Numeric(14, 2), nullable=True)  # Manual actual override per line

    # Per-instance custom schedule display labels: JSON {"1": "Hero Biker", "3": "Lead Double"}
    schedule_labels = db.Column(db.Text, nullable=True)

    crew_assignments = db.relationship("CrewAssignment", backref="line", lazy=True,
                                       cascade="all, delete-orphan")
    schedule_days    = db.relationship("ScheduleDay", backref="line", lazy=True,
                                       foreign_keys="ScheduleDay.budget_line_id")

    # ── Actuals linkage (added 2026-04-30) ─────────────────────────────
    # When a BudgetLine belongs to an Actual budget (Budget.is_actual=
    # True), source_line_id points back at the Working line it was
    # cloned from. Lets us:
    #   • Re-find the Actual equivalent when a user picks a Working line
    #   • Sync structural changes from Working → Actual on demand
    #   • Detect orphans (Working line was deleted while Actual line
    #     still has linked transactions — orphan_from_working flips on)
    source_line_id = db.Column(db.Integer, db.ForeignKey('budget_line.id'),
                                nullable=True)
    orphan_from_working = db.Column(db.Boolean, default=False, nullable=False,
                                    server_default='0')

    # ── Frozen per-line actual (Phase 4, 2026-07) ──────────────────────
    # The per-line actual total FROZEN at the moment this line's budget was
    # superseded by a newer Working version (create_working_from_estimated).
    # After the remap, coded transactions move to the NEW Working's lines, so
    # a live rollup on the OLD (superseded) Working line would go to $0. This
    # column preserves an immutable "this version showed these figures" record
    # so superseded Working budgets keep their per-line actuals history.
    # NULL = never frozen (current budgets, Estimated versions).
    actual_snapshot = db.Column(db.Numeric(14, 2), nullable=True)


class FringeConfig(db.Model):
    __tablename__ = "fringe_config"
    id           = db.Column(db.Integer, primary_key=True)
    project_id   = db.Column(db.Integer, nullable=True)   # NULL = global default
    fringe_type  = db.Column(db.String(5), nullable=False)
    label        = db.Column(db.String(50), nullable=False)
    rate         = db.Column(db.Numeric(8, 6), nullable=False)
    is_flat      = db.Column(db.Boolean, default=False)
    flat_amount  = db.Column(db.Numeric(10, 2), nullable=True)
    # When False, no OT/DT is ever calculated for lines using this fringe (e.g. Exempt)
    ot_applies   = db.Column(db.Boolean, default=True)
    __table_args__ = (db.UniqueConstraint("project_id", "fringe_type", name="uq_fringe_proj"),)


class CrewMember(db.Model):
    __tablename__ = "crew_member"
    id                  = db.Column(db.Integer, primary_key=True)
    name                = db.Column(db.String(200), nullable=False)
    department          = db.Column(db.String(100), nullable=True)
    default_rate        = db.Column(db.Numeric(12, 2), nullable=True)
    default_rate_type   = db.Column(db.String(20), default="day_10")
    default_fringe      = db.Column(db.String(5), default="N")
    default_agent_pct   = db.Column(db.Numeric(6, 4), default=0)
    email               = db.Column(db.String(200), nullable=True)
    phone               = db.Column(db.String(50), nullable=True)
    company             = db.Column(db.String(200), nullable=True)
    active              = db.Column(db.Boolean, default=True)
    # ── Vendor / loan-out (added 2026-06-01) ───────────────────────────
    # A vendor is a person-like entity (treated like a crew member). People
    # who provide services through a loan-out company point at that vendor
    # via loan_out_vendor_id, so the People tab can nest them under it.
    is_vendor           = db.Column(db.Boolean, default=False)
    loan_out_vendor_id  = db.Column(db.Integer, db.ForeignKey("crew_member.id"), nullable=True)
    # Per-vendor required-doc checklist (comma-separated keys), like Location.
    required_docs       = db.Column(db.Text, nullable=True)
    # ── Wrapbook-style employment classification (added 2026-07) ────────
    # Shown as the person's subtitle ("Loan Out · Non-Union") and used by the
    # person-profile panel + budget-mismatch checks. Free-ish text kept short.
    employment_type     = db.Column(db.String(20), nullable=True)   # loan_out | employee | vendor
    union_status        = db.Column(db.String(20), nullable=True)   # union | non_union
    support_contacts    = db.relationship("SupportContact", backref="crew_member",
                                          lazy=True, cascade="all, delete-orphan",
                                          foreign_keys="SupportContact.crew_member_id")


class CrewAssignment(db.Model):
    __tablename__ = "crew_assignment"
    id              = db.Column(db.Integer, primary_key=True)
    budget_line_id  = db.Column(db.Integer, db.ForeignKey("budget_line.id"), nullable=False)
    instance        = db.Column(db.Integer, default=1, nullable=False)   # which expanded row (1-based)
    crew_member_id  = db.Column(db.Integer, db.ForeignKey("crew_member.id"), nullable=True)
    crew_member     = db.relationship("CrewMember", foreign_keys=[crew_member_id])
    name_override   = db.Column(db.String(200), nullable=True)
    rate_override   = db.Column(db.Numeric(12, 2), nullable=True)
    fringe_override = db.Column(db.String(5), nullable=True)
    agent_override  = db.Column(db.Numeric(6, 4), nullable=True)
    omit_flags      = db.Column(db.Text, nullable=True)   # JSON: {"name":bool,"phone":bool,"email":bool}
    role_number     = db.Column(db.String(20),  nullable=True)   # Talent role number e.g. "1", "2A"
    __table_args__  = (db.UniqueConstraint("budget_line_id", "instance", name="uq_crew_assign_inst"),)


class ProjectCrewMember(db.Model):
    """Per-PROJECT overrides for a person (added 2026-07). Someone is a Camera Op
    on one show and a Gaffer on the next, and their union/employment status can
    differ per project — so those live here, keyed to (project, crew_member).
    Identity + persistent paperwork (ID, W-9, tax forms) stay GLOBAL on
    CrewMember. Falls back to the CrewMember global default when absent."""
    __tablename__ = "project_crew_member"
    id              = db.Column(db.Integer, primary_key=True)
    project_id      = db.Column(db.Integer, db.ForeignKey("project_sheet.id"), nullable=False, index=True)
    crew_member_id  = db.Column(db.Integer, db.ForeignKey("crew_member.id"), nullable=False, index=True)
    employment_type = db.Column(db.String(20), nullable=True)   # loan_out | employee | vendor
    union_status    = db.Column(db.String(20), nullable=True)   # union | non_union
    # ── Per-project loan-out vendor (User 2026-07.) ────────────────────────
    # Someone is a loan-out for "You Choose LLC" on this show but a W-2 employee
    # on the next — so the vendor link is PER-PROJECT here, pointing at a real
    # CrewMember (is_vendor=True) record. The GLOBAL CrewMember.loan_out_vendor_id
    # stays as legacy/global metadata (display-only fallback), untouched.
    loan_out_vendor_id = db.Column(db.Integer, db.ForeignKey("crew_member.id"), nullable=True)
    __table_args__  = (db.UniqueConstraint("project_id", "crew_member_id", name="uq_project_crew_member"),)


class ScheduleDay(db.Model):
    __tablename__ = "schedule_day"
    id              = db.Column(db.Integer, primary_key=True)
    budget_id       = db.Column(db.Integer, db.ForeignKey("budget.id"), nullable=False)
    budget_line_id  = db.Column(db.Integer, db.ForeignKey("budget_line.id"), nullable=True)
    crew_member_id  = db.Column(db.Integer, db.ForeignKey("crew_member.id"), nullable=True)
    date            = db.Column(db.Date, nullable=False)
    episode         = db.Column(db.String(50), nullable=True)
    day_type        = db.Column(db.String(20), default="work")
    # work | travel | hold | off | half | kill_fee | custom
    rate_multiplier = db.Column(db.Numeric(5, 3), default=1.0)
    note            = db.Column(db.String(200), nullable=True)
    crew_instance   = db.Column(db.Integer, default=1, nullable=False)
    est_ot_hours    = db.Column(db.Numeric(5, 2), nullable=True, default=0)
    cell_flags      = db.Column(db.Text, nullable=True)   # JSON: {"hotel":true,"flight":true,...}
    schedule_mode   = db.Column(db.String(20), default="estimated", nullable=False)  # estimated | working


class Timecard(db.Model):
    """Per-(project, person, payroll-week) timecard — Wrapbook mirror (added
    2026-07, Timecards slice 1). Which people are EXPECTED to submit a timecard
    (vs an invoice) is decided by their per-project ProjectCrewMember.
    employment_type: 'employee' → timecard, 'loan_out'/'vendor' → invoice.
    Timecards AUTO-GENERATE from the schedule: the app already knows each
    person's work days via ScheduleDay, so a draft is prefilled from that week's
    days. Wrapbook runs the actual payroll; this table is the planning mirror.

    days_json is a JSON list of per-day entries
    [{"date","day_type","mult","ot_amount"}] where `mult` is the pay multiplier
    from budget_calc.DAY_TYPE_MULTIPLIERS and `ot_amount` is FLAT DOLLARS of OT
    for that day (kept simple this slice — no hours×rate math). `rate` is the
    per-day rate captured at generate time (CrewAssignment.rate_override else the
    line's rate) so gross recomputes deterministically:
        gross = Σ (rate × mult + ot_amount).
    """
    __tablename__ = "timecard"
    id              = db.Column(db.Integer, primary_key=True)
    project_id      = db.Column(db.Integer, db.ForeignKey("project_sheet.id"), nullable=False, index=True)
    crew_member_id  = db.Column(db.Integer, db.ForeignKey("crew_member.id"), nullable=False, index=True)
    week_ending     = db.Column(db.Date, nullable=False)
    days_json       = db.Column(db.Text, nullable=True)   # JSON [{date, day_type, mult, ot_amount}]
    status          = db.Column(db.String(20), default="draft", nullable=False)  # draft | submitted | approved
    # Per-day rate captured at generate time so gross recomputes deterministically.
    rate            = db.Column(db.Numeric(12, 2), nullable=True)
    gross           = db.Column(db.Numeric(12, 2), nullable=True)
    note            = db.Column(db.String(300), nullable=True)
    created_at      = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at      = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    __table_args__  = (db.UniqueConstraint("project_id", "crew_member_id", "week_ending",
                                           name="uq_timecard_proj_crew_week"),)


class ProductionDay(db.Model):
    """Per-production-day flags: meals. Separate rows per schedule_mode (estimated/working)."""
    __tablename__ = "production_day"
    id                  = db.Column(db.Integer, primary_key=True)
    budget_id           = db.Column(db.Integer, db.ForeignKey("budget.id"), nullable=False)
    date                = db.Column(db.Date, nullable=False)
    schedule_mode       = db.Column(db.String(20), default="estimated", nullable=False)
    # Craft Services — explicit per-day toggle (added 2026-05-06).
    # Previously craft services was auto-counted from "every non-off
    # ScheduleDay row," which made the qty fluid and re-derived on every
    # sync, producing different numbers between Estimated and Working
    # clones. Promoting it to a user-controlled flag (parallel to
    # courtesy_breakfast / first_meal / second_meal) makes the line
    # reproducible and reproducible across clones.
    craft_services      = db.Column(db.Boolean, default=False, nullable=False,
                                     server_default=db.text('false'))
    courtesy_breakfast  = db.Column(db.Boolean, default=False)
    first_meal          = db.Column(db.Boolean, default=False)
    second_meal         = db.Column(db.Boolean, default=False)
    # is_production_day: marks days that are actual on-set production
    # days vs prep / remote / travel-only / etc. Per-user-request the
    # downstream calcs (insurance daily rate, location day count, certain
    # auto-injected lines) should only apply to true production days.
    # Set from the new "Production Day" row at the top of the Schedule.
    # server_default uses SQLA `text` so Postgres ('false') and SQLite
    # ('0') both accept the literal as a default expression.
    is_production_day   = db.Column(db.Boolean, default=False, nullable=False,
                                      server_default=db.text('false'))
    __table_args__      = (db.UniqueConstraint("budget_id", "date", "schedule_mode", name="uq_prod_day"),)


class TravelDetail(db.Model):
    """Per-(person, date, kind) travel detail — flight number,
    confirmations, hotel reservation info, etc. Lives alongside the
    ScheduleDay cell_flags: the flag determines IF travel applies
    on that cell, this row carries the WHAT (numbers, times, vendors).
    Users edit these from the new Travel tab; values flow into the
    call sheet email render so confirmations land in the crew's inbox.
    """
    __tablename__ = "travel_detail"
    id              = db.Column(db.Integer, primary_key=True)
    schedule_day_id = db.Column(db.Integer, db.ForeignKey("schedule_day.id"), nullable=False)
    kind            = db.Column(db.String(20), nullable=False)
    # flight | hotel | car_rental | car_service | mileage
    confirmation_no = db.Column(db.String(100), nullable=True)
    notes           = db.Column(db.Text,        nullable=True)
    # Flight-specific
    airline         = db.Column(db.String(100), nullable=True)
    flight_no       = db.Column(db.String(50),  nullable=True)
    depart_at       = db.Column(db.DateTime,    nullable=True)
    arrive_at       = db.Column(db.DateTime,    nullable=True)
    depart_airport  = db.Column(db.String(10),  nullable=True)  # IATA code
    arrive_airport  = db.Column(db.String(10),  nullable=True)
    # Hotel-specific
    hotel_name      = db.Column(db.String(200), nullable=True)
    hotel_address   = db.Column(db.String(300), nullable=True)
    check_in        = db.Column(db.Date,        nullable=True)
    check_out       = db.Column(db.Date,        nullable=True)
    room_type       = db.Column(db.String(100), nullable=True)
    # Car-rental-specific (also reused by car_service: rental_co=company,
    # pickup_at=pickup time, return_at=dropoff time, pickup_location).
    rental_co       = db.Column(db.String(100), nullable=True)
    pickup_at       = db.Column(db.DateTime,    nullable=True)
    return_at       = db.Column(db.DateTime,    nullable=True)
    pickup_location = db.Column(db.String(200), nullable=True)
    # Car-service-specific (chauffeur / black-car pickups): a dropoff address
    # (distinct from the pickup) and a driver/dispatch contact phone. The
    # self_report flag marks a "take an Uber/Lyft, keep your receipt" line —
    # no booked car, no confirmation required. (User 2026-07-09.)
    dropoff_location = db.Column(db.String(300), nullable=True)
    contact_phone    = db.Column(db.String(50),  nullable=True)
    self_report      = db.Column(db.Boolean, default=False)
    # Mileage-specific
    miles           = db.Column(db.Numeric(8, 2), nullable=True)
    route           = db.Column(db.String(300), nullable=True)
    updated_at      = db.Column(db.DateTime,    default=datetime.utcnow,
                                onupdate=datetime.utcnow)
    # NOTE: no (schedule_day_id, kind) uniqueness — a person can have MULTIPLE
    # entries of the same kind on one day (two flights, several car services).
    # Rows are addressed by id; see travel_detail_save. (User 2026-07-09.)


class CateringBill(db.Model):
    """Caterer bill entry — daily or weekly amounts the user gets from
    their catering vendor. The Catering tab shows expected meal cost
    (per-person × per-day × rate) alongside actual billed amount so
    the user can see drift between budget and reality.
    """
    __tablename__ = "catering_bill"
    id            = db.Column(db.Integer, primary_key=True)
    budget_id     = db.Column(db.Integer, db.ForeignKey("budget.id"), nullable=False)
    period_start  = db.Column(db.Date, nullable=False)
    period_end    = db.Column(db.Date, nullable=False)
    vendor        = db.Column(db.String(200), nullable=True)
    amount        = db.Column(db.Numeric(12, 2), nullable=False, default=0)
    note          = db.Column(db.Text, nullable=True)
    created_at    = db.Column(db.DateTime, default=datetime.utcnow)


class Location(db.Model):
    """Production location database. project_id=NULL = global library entry; project_id=N = project-specific."""
    __tablename__ = "location"
    id              = db.Column(db.Integer, primary_key=True)
    project_id      = db.Column(db.Integer, db.ForeignKey("project_sheet.id"), nullable=True)
    name            = db.Column(db.String(200), nullable=False)
    facility_name   = db.Column(db.String(200), nullable=True)   # business/venue name (e.g. "Public Storage – 42nd St")
    location_type   = db.Column(db.String(50),  nullable=True)   # stage | office | exterior | holding | parking | vendor | other
    address         = db.Column(db.String(500),  nullable=True)
    map_url         = db.Column(db.String(1000), nullable=True)  # stored Google Maps URL
    # Main / negotiating contact
    contact_name    = db.Column(db.String(200),  nullable=True)
    contact_email   = db.Column(db.String(200),  nullable=True)
    contact_phone   = db.Column(db.String(50),   nullable=True)
    # Day-of / on-site contact
    dayof_name      = db.Column(db.String(200),  nullable=True)
    dayof_email     = db.Column(db.String(200),  nullable=True)
    dayof_phone     = db.Column(db.String(50),   nullable=True)
    # Billing
    billing_type    = db.Column(db.String(20),   default="per_day")  # per_day | flat | per_week | info_only
    daily_rate      = db.Column(db.Numeric(10,2), nullable=True)
    # Link to budget line so schedule days drive budget quantities
    budget_line_id  = db.Column(db.Integer, db.ForeignKey("budget_line.id"), nullable=True)
    notes           = db.Column(db.Text,    nullable=True)
    active          = db.Column(db.Boolean, default=True)
    omit_flags      = db.Column(db.Text, nullable=True)   # JSON: {"main":{name,phone,email},"dayof":{name,phone,email}}
    # Per-location required-doc checklist (comma-separated keys, e.g.
    # "agreement,coi,permit"). User picks what THIS location requires; the
    # Locations tab badges measure attached docs against it. (2026-06-01.)
    required_docs   = db.Column(db.Text, nullable=True)
    days            = db.relationship("LocationDay", backref="location", lazy=True,
                                      cascade="all, delete-orphan")


class LocationDay(db.Model):
    """A location booked on a specific day for a specific budget."""
    __tablename__ = "location_day"
    id              = db.Column(db.Integer, primary_key=True)
    budget_id       = db.Column(db.Integer, db.ForeignKey("budget.id"), nullable=False)
    location_id     = db.Column(db.Integer, db.ForeignKey("location.id"), nullable=False)
    date            = db.Column(db.Date, nullable=False)
    day_type        = db.Column(db.String(20), default="use")   # use | scout | hold | strike
    note            = db.Column(db.String(200), nullable=True)
    __table_args__  = (db.UniqueConstraint("budget_id", "location_id", "date", name="uq_loc_day"),)


class BudgetTemplate(db.Model):
    __tablename__ = "budget_template"
    id          = db.Column(db.Integer, primary_key=True)
    name        = db.Column(db.String(200), nullable=False, unique=True)
    description = db.Column(db.String(500), nullable=True)
    created_at  = db.Column(db.DateTime, default=datetime.utcnow)
    lines       = db.relationship("BudgetTemplateLine", backref="template",
                                  lazy=True, cascade="all, delete-orphan")


class BudgetTemplateLine(db.Model):
    __tablename__ = "budget_template_line"
    id              = db.Column(db.Integer, primary_key=True)
    template_id     = db.Column(db.Integer, db.ForeignKey("budget_template.id"), nullable=False)
    account_code    = db.Column(db.Integer, nullable=False)
    account_name    = db.Column(db.String(100), nullable=False)
    description     = db.Column(db.String(300), nullable=True)
    is_labor        = db.Column(db.Boolean, default=False)
    quantity        = db.Column(db.Numeric(8, 2), default=1)
    days            = db.Column(db.Numeric(8, 2), default=1)
    rate            = db.Column(db.Numeric(12, 2), default=0)
    rate_type       = db.Column(db.String(20), default="day_10")
    fringe_type     = db.Column(db.String(5), default="N")
    agent_pct       = db.Column(db.Numeric(6, 4), default=0)
    estimated_total = db.Column(db.Numeric(12, 2), default=0)
    sort_order      = db.Column(db.Integer, default=0)


class CatalogItem(db.Model):
    """Global Quick Entry catalog — roles/items available when adding budget lines.
    Editable by super_admin via /admin/catalog. Seeded on first boot from
    FP_CATALOG_SEED in budget_calc.py."""
    __tablename__ = "catalog_item"
    id            = db.Column(db.Integer, primary_key=True)
    category_code = db.Column(db.Integer, nullable=False)          # COA section code
    category_name = db.Column(db.String(100), nullable=False)
    label         = db.Column(db.String(200), nullable=False)
    group_name    = db.Column(db.String(100), nullable=True)       # Sub-group: Production, Camera...
    is_labor      = db.Column(db.Boolean, default=False)
    rate          = db.Column(db.Numeric(12, 2), default=0)
    qty           = db.Column(db.Numeric(8, 2), default=1)
    days          = db.Column(db.Numeric(8, 2), default=1)
    kit_fee       = db.Column(db.Numeric(8, 2), default=0)
    fringe        = db.Column(db.String(5), nullable=True)          # non-union fringe (N, E, etc.)
    union_fringe  = db.Column(db.String(5), nullable=True)          # union variant (I, S, D, U)
    agent_pct     = db.Column(db.Numeric(6, 4), default=0)
    comp          = db.Column(db.String(20), default='labor')       # labor | expense | rental | purchase
    unit          = db.Column(db.String(20), default='day')         # day | flat | week | session...
    sort_order    = db.Column(db.Integer, default=0)
    is_active     = db.Column(db.Boolean, default=True)
    created_at    = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at    = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    # Stable opaque slug — e.g. 'director_of_photography'. Written once on
    # create (auto-generated from label if not provided); export logic keys
    # on this, so it MUST survive label edits.
    role_tag      = db.Column(db.String(80), nullable=True, unique=True)
    # Which production phase this row covers. ATL roles (Director,
    # Executive Producer, Producer, Writer) can appear three times — once
    # per phase — so {role_tag} alone isn't unique; {role_tag, phase} is.
    # Values: 'development' | 'production' | 'post' | NULL (no phase).
    phase         = db.Column(db.String(20), nullable=True)
    __table_args__ = (db.UniqueConstraint("category_code", "label", name="uq_catalog_item"),)


class RoleTagMapping(db.Model):
    """Translates internal role_tag → external budgeting-system account codes.
    One row per role_tag. Edited by super admin via /admin/role-mapping.
    Export logic (Task 3) reads this to route each line to the correct
    MMB / ShowBiz account on export.
    """
    __tablename__ = "role_tag_mapping"
    id                    = db.Column(db.Integer, primary_key=True)
    role_tag              = db.Column(db.String(80), nullable=False, unique=True)
    internal_account_code = db.Column(db.Integer, nullable=False)
    internal_account_name = db.Column(db.String(100), nullable=True)
    # MMB often uses decimal account codes (e.g. '2110.01'); keep as string.
    mmb_account_code      = db.Column(db.String(20), nullable=True)
    mmb_account_name      = db.Column(db.String(100), nullable=True)
    showbiz_account_code  = db.Column(db.String(20), nullable=True)
    showbiz_account_name  = db.Column(db.String(100), nullable=True)
    notes                 = db.Column(db.Text, nullable=True)
    updated_at            = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    updated_by_user_id    = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)


class CoaMigrationLog(db.Model):
    """One row per named COA migration that has been applied. Acts as the
    'migration already ran' guard so a restarted container doesn't re-apply
    the renumber. Immutable once a row exists."""
    __tablename__ = "coa_migration_log"
    id          = db.Column(db.Integer, primary_key=True)
    migration_key = db.Column(db.String(80), unique=True, nullable=False)
    applied_at  = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    applied_by_user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    notes       = db.Column(db.Text, nullable=True)


class CoaChangeLog(db.Model):
    """Immutable audit of every COA code change. Seeded with the 36 rows
    of the 2026-04 renumber; subsequent manual edits (via /admin/catalog)
    append here as well. General Rule: every change to the COA is logged
    with timestamp and user ID."""
    __tablename__ = "coa_change_log"
    id                 = db.Column(db.Integer, primary_key=True)
    account_code_old   = db.Column(db.Integer, nullable=True)
    account_code_new   = db.Column(db.Integer, nullable=False)
    account_name_old   = db.Column(db.String(100), nullable=True)
    account_name_new   = db.Column(db.String(100), nullable=True)
    changed_at         = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    changed_by_user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    change_reason      = db.Column(db.String(200), nullable=True)


class TaxCredit(db.Model):
    __tablename__ = "tax_credit"
    id           = db.Column(db.Integer, primary_key=True)
    budget_id    = db.Column(db.Integer, db.ForeignKey("budget.id"), nullable=False)
    name         = db.Column(db.String(200), nullable=False)   # "Georgia Film Tax Credit"
    jurisdiction = db.Column(db.String(100), nullable=True)    # "Georgia, USA"
    credit_rate  = db.Column(db.Numeric(8, 4), nullable=False, default=0)  # 0.30 = 30%
    applies_to   = db.Column(db.String(20), default="all")     # all|labor|nonlabor
    min_spend    = db.Column(db.Numeric(14, 2), nullable=True)  # minimum qualifying spend
    cap          = db.Column(db.Numeric(14, 2), nullable=True)  # max credit amount
    notes        = db.Column(db.Text, nullable=True)
    sort_order   = db.Column(db.Integer, default=0)


class PayrollProfile(db.Model):
    __tablename__ = "payroll_profile"
    id                   = db.Column(db.Integer, primary_key=True)
    name                 = db.Column(db.String(100), nullable=False)
    description          = db.Column(db.String(300), nullable=True)
    is_system            = db.Column(db.Boolean, default=False)
    # Daily thresholds (NULL = no daily OT rule)
    daily_st_hours       = db.Column(db.Numeric(5, 2), nullable=True)
    daily_dt_hours       = db.Column(db.Numeric(5, 2), nullable=True)
    ot_multiplier        = db.Column(db.Numeric(4, 3), default=1.5)
    dt_multiplier        = db.Column(db.Numeric(4, 3), default=2.0)
    # Weekly threshold (NULL = no weekly OT rule)
    weekly_st_hours      = db.Column(db.Numeric(5, 2), nullable=True)
    weekly_ot_multiplier = db.Column(db.Numeric(4, 3), default=1.5)
    # 7th day: None | 'ot_all'
    seventh_day_rule     = db.Column(db.String(20), nullable=True)
    # Default payroll week start: 0=Mon … 6=Sun
    payroll_week_start   = db.Column(db.Integer, default=0)
    sort_order           = db.Column(db.Integer, default=0)


class CallSheetData(db.Model):
    """Per-day call sheet editable overrides stored as a JSON blob."""
    __tablename__ = "callsheet_data"
    id            = db.Column(db.Integer, primary_key=True)
    budget_id     = db.Column(db.Integer, db.ForeignKey("budget.id"), nullable=False)
    date          = db.Column(db.Date, nullable=False)
    schedule_mode = db.Column(db.String(20), default='estimated', nullable=False)
    data_json     = db.Column(db.Text, nullable=True)
    updated_at    = db.Column(db.DateTime, default=datetime.utcnow)
    __table_args__ = (db.UniqueConstraint("budget_id", "date", "schedule_mode",
                                          name="uq_cs_day"),)


class ProjectLogo(db.Model):
    """A logo image (show / brand / client) uploaded to a project, stored in
    Postgres and reusable across that project's call sheets. (2026-07-09.)

    The bytes live in `data` (LargeBinary → BYTEA) — deliberately NOT Dropbox,
    to avoid serve latency + coupling. Each per-call-sheet arrangement stores
    only {logo_id, order, h} references (in CallSheetData.data_json['logos'] /
    ProjectSheet.logos_default), so one uploaded logo is placed on many sheets
    without duplicating bytes. width/height are the intrinsic pixel dimensions
    parsed at upload when cheap (Pillow for raster; null for SVG / on failure)."""
    __tablename__ = "project_logo"
    id           = db.Column(db.Integer, primary_key=True)
    project_id   = db.Column(db.Integer, db.ForeignKey("project_sheet.id"),
                             nullable=False, index=True)
    name         = db.Column(db.String(120), nullable=True)
    content_type = db.Column(db.String(50),  nullable=False)   # image/png|jpeg|svg+xml
    data         = db.Column(db.LargeBinary, nullable=False)
    width        = db.Column(db.Integer, nullable=True)        # intrinsic px (raster)
    height       = db.Column(db.Integer, nullable=True)
    created_at   = db.Column(db.DateTime, default=datetime.utcnow)


class SupportContact(db.Model):
    """Agent, manager, publicist, PA, attorney attached to a crew member."""
    __tablename__ = "support_contact"
    id             = db.Column(db.Integer, primary_key=True)
    crew_member_id = db.Column(db.Integer, db.ForeignKey("crew_member.id"), nullable=False)
    role_type      = db.Column(db.String(50),  nullable=False)   # agent|manager|publicist|pa|attorney|other
    name           = db.Column(db.String(200), nullable=False)
    email          = db.Column(db.String(200), nullable=True)
    phone          = db.Column(db.String(50),  nullable=True)
    company        = db.Column(db.String(200), nullable=True)
    notify_callsheet = db.Column(db.Boolean,   default=False)    # auto-CC on call sheet sends
    cc_by_default    = db.Column(db.Boolean,   default=False)
    active           = db.Column(db.Boolean,   default=True)
    visibility_flags = db.Column(db.Text, nullable=True)  # JSON: {"crew":bool,"talent":bool,"union":bool,"internal":bool,"client":bool}
    fee_pct          = db.Column(db.Numeric(6, 4), nullable=True)   # e.g. 0.10 = 10%
    fee_type         = db.Column(db.String(20), nullable=True)      # on_top | inclusive


class ProjectUnion(db.Model):
    """Union contact record scoped to a project."""
    __tablename__ = "project_union"
    id           = db.Column(db.Integer, primary_key=True)
    project_id   = db.Column(db.Integer, db.ForeignKey("project_sheet.id"), nullable=False)
    union_name   = db.Column(db.String(100), nullable=False)   # SAG-AFTRA | IATSE | DGA | Teamsters | Other
    contact_name = db.Column(db.String(200), nullable=True)
    email        = db.Column(db.String(200), nullable=True)
    phone        = db.Column(db.String(50),  nullable=True)
    receives_callsheet = db.Column(db.Boolean, default=False)
    sort_order   = db.Column(db.Integer, default=0)
    visibility_flags = db.Column(db.Text, nullable=True)


class ProjectPartner(db.Model):
    """Vendor / partner-company contact scoped to a project (User 2026-07-13).

    People at an outside company involved in a shoot — venue/ops staff, rental-
    house pickup & drop-off coordinators, recording-studio ops, etc. — who
    should RECEIVE the call sheet (crew view / full schedule) so they can vet
    timing on our plan. Distinct from '🏢 Vendors & Loan-Outs' (those are
    PAYMENT vendors/loan-out companies). Listed on the People tab + printed
    contact sheet, and surfaced in the call-sheet distribution panel under a
    '🚚 Vendors & Partners' group — always LISTED, NEVER pre-checked (a partner
    is emailed only when the user physically ticks the row for that send)."""
    __tablename__ = "project_partner"
    id           = db.Column(db.Integer, primary_key=True)
    project_id   = db.Column(db.Integer, db.ForeignKey("project_sheet.id"), nullable=False, index=True)
    name         = db.Column(db.String(200), nullable=False)
    role         = db.Column(db.String(120), nullable=True)   # free text: 'Ops Manager', 'Pickup Coordinator'
    company      = db.Column(db.String(200), nullable=True)
    email        = db.Column(db.String(200), nullable=True)
    phone        = db.Column(db.String(50),  nullable=True)
    notes        = db.Column(db.Text, nullable=True)
    sort_order   = db.Column(db.Integer, default=0)
    created_at   = db.Column(db.DateTime, default=datetime.utcnow)


class ProjectClient(db.Model):
    """Client contact scoped to a project — the single source of truth for both
    call-sheet clients (shown on call sheet page 1) and estimate recipients.

    Consolidated 2026-07-13: the former ProjectClientContact ('🤝 Clients',
    estimate recipients) was merged into this table. Rows added by an estimate
    send carry source='estimate_send' with show_on_callsheet/receives_callsheet
    = False (they are NOT call-sheet clients until a user ticks them on); rows
    added manually carry source='manual'. Email is the case-insensitive dedupe
    key per project where present (enforced in code, not by a DB constraint —
    legacy rows may collide)."""
    __tablename__ = "project_client"
    id           = db.Column(db.Integer, primary_key=True)
    project_id   = db.Column(db.Integer, db.ForeignKey("project_sheet.id"), nullable=False)
    name         = db.Column(db.String(200), nullable=False)
    title        = db.Column(db.String(100), nullable=True)
    company      = db.Column(db.String(200), nullable=True)
    email        = db.Column(db.String(200), nullable=True)
    phone        = db.Column(db.String(50),  nullable=True)
    show_on_callsheet    = db.Column(db.Boolean, default=True)
    receives_callsheet   = db.Column(db.Boolean, default=True)
    sort_order   = db.Column(db.Integer, default=0)
    visibility_flags = db.Column(db.Text, nullable=True)  # JSON: {"crew":bool,"talent":bool,"union":bool,"internal":bool,"client":bool}
    # Provenance of the row: 'manual' (added on the People tab / call-sheet
    # client modal) or 'estimate_send' (auto-added when an estimate was sent).
    source       = db.Column(db.String(20), default='manual')
    created_at   = db.Column(db.DateTime, nullable=True)


class CallSheetSend(db.Model):
    """Records a call sheet distribution event (foundation for future email send)."""
    __tablename__ = "callsheet_send"
    id            = db.Column(db.Integer, primary_key=True)
    budget_id     = db.Column(db.Integer, db.ForeignKey("budget.id"), nullable=False)
    date          = db.Column(db.Date, nullable=False)
    schedule_mode = db.Column(db.String(20), default='estimated', nullable=False)
    version_label = db.Column(db.String(50), nullable=True)    # e.g. "v1", "v2 REVISED"
    sent_at       = db.Column(db.DateTime, nullable=True)
    sent_by       = db.Column(db.String(200), nullable=True)
    notes         = db.Column(db.Text, nullable=True)
    # Archived artifact — the exact PDF that was emailed for THIS send, so any
    # version (even one sent to a single person) is downloadable/retrievable
    # later even if regeneration would differ. dropbox_path records where the
    # same PDF was mirrored into the project's admin folder (fail-open — may be
    # null if Dropbox was unavailable at send time). (User 2026-07-09.)
    pdf_data      = db.Column(db.LargeBinary, nullable=True)
    pdf_filename  = db.Column(db.String(300), nullable=True)
    dropbox_path  = db.Column(db.String(500), nullable=True)
    recipients    = db.relationship("CallSheetRecipient", backref="send",
                                    lazy=True, cascade="all, delete-orphan")


class CallSheetRecipient(db.Model):
    """Per-recipient record for a call sheet send — tracks confirmation."""
    __tablename__ = "callsheet_recipient"
    id            = db.Column(db.Integer, primary_key=True)
    send_id       = db.Column(db.Integer, db.ForeignKey("callsheet_send.id"), nullable=False)
    recipient_type = db.Column(db.String(30), nullable=False)  # crew|talent|client|union|support
    name          = db.Column(db.String(200), nullable=False)
    email         = db.Column(db.String(200), nullable=True)
    phone         = db.Column(db.String(50),  nullable=True)
    viewed_at     = db.Column(db.DateTime, nullable=True)
    confirmed_at  = db.Column(db.DateTime, nullable=True)
    confirm_token = db.Column(db.String(64), nullable=True, unique=True)
    # Status: pending | sent | viewed | confirmed | bounced
    status        = db.Column(db.String(20), default='pending', nullable=False)


class BudgetDirectContact(db.Model):
    """A person added directly to a budget's contact sheet (not via a budget line)."""
    __tablename__ = "budget_direct_contact"
    id             = db.Column(db.Integer, primary_key=True)
    budget_id      = db.Column(db.Integer, db.ForeignKey("budget.id"), nullable=False)
    crew_member_id = db.Column(db.Integer, db.ForeignKey("crew_member.id"), nullable=False)
    role           = db.Column(db.String(200), nullable=True)  # optional role for this project
    sort_order     = db.Column(db.Integer, default=0)
    crew_member    = db.relationship("CrewMember", foreign_keys=[crew_member_id])
    __table_args__ = (db.UniqueConstraint("budget_id", "crew_member_id", name="uq_direct_contact"),)


class CompanySettings(db.Model):
    """Global production company profile — singleton (always id=1)."""
    __tablename__ = "company_settings"
    id              = db.Column(db.Integer, primary_key=True)
    company_name    = db.Column(db.String(200), nullable=True)
    address_line1   = db.Column(db.String(200), nullable=True)
    address_line2   = db.Column(db.String(200), nullable=True)
    city            = db.Column(db.String(100), nullable=True)
    state           = db.Column(db.String(50),  nullable=True)
    zip_code        = db.Column(db.String(20),  nullable=True)
    phone           = db.Column(db.String(50),  nullable=True)
    email           = db.Column(db.String(200), nullable=True)
    website         = db.Column(db.String(200), nullable=True)


class DocUpload(db.Model):
    """A single document/receipt uploaded through the Docs module."""
    __tablename__ = "doc_upload"
    id               = db.Column(db.Integer, primary_key=True)
    project_id       = db.Column(db.Integer, db.ForeignKey("project_sheet.id"), nullable=False)
    uploader_id      = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    uploaded_at      = db.Column(db.DateTime, default=datetime.utcnow)

    # R2 storage
    r2_key           = db.Column(db.String(500), nullable=True)   # object key in R2 bucket
    original_filename = db.Column(db.String(300), nullable=True)
    file_size        = db.Column(db.Integer, nullable=True)       # bytes
    content_type     = db.Column(db.String(100), nullable=True)
    file_hash        = db.Column(db.String(64), nullable=True)    # SHA-256 for dedup

    # OCR / processing (Veryfi)
    status           = db.Column(db.String(20), default='pending')
    # pending | processing | review | done | error | duplicate
    veryfi_data      = db.Column(db.Text, nullable=True)          # raw JSON from Veryfi
    vendor           = db.Column(db.String(200), nullable=True)
    amount           = db.Column(db.Numeric(10, 2), nullable=True) # USD — reconciliation/budget value
    # Foreign-currency invoices (user 2026-05-29): `amount` always holds the
    # USD value that matches the bank charge (what reconciles + rolls into the
    # budget). When the source document was in another currency, the original
    # figure + ISO-ish code are preserved here for reference. Wider precision
    # since KRW/JPY amounts run large (₩1,500,000 etc.).
    original_amount   = db.Column(db.Numeric(16, 2), nullable=True)
    original_currency = db.Column(db.String(8), nullable=True)
    doc_date         = db.Column(db.Date, nullable=True)
    confidence       = db.Column(db.Numeric(5, 2), nullable=True) # 0-100
    category         = db.Column(db.String(100), nullable=True)   # doc type (receipt/invoice/...)
    # Veryfi expense category — free-text taxonomy from Veryfi (e.g.
    # "Meals & Entertainment", "Office Supplies & Software"). Denormalized
    # off veryfi_data so the Docs row can render it without loading the
    # full OCR JSON. Drives the "Suggested account" hint pill.
    veryfi_category  = db.Column(db.String(100), nullable=True)

    # Filing to Dropbox
    filed_filename   = db.Column(db.String(300), nullable=True)   # renamed file
    filed_dropbox_path = db.Column(db.String(500), nullable=True) # full Dropbox path
    filed_at         = db.Column(db.DateTime, nullable=True)
    is_duplicate     = db.Column(db.Boolean, default=False)
    # When an exact byte-identical match is detected on upload, this
    # points at the upload whose hash matched. Per user 2026-05-29 the
    # doc is NO LONGER auto-buried in /_DUPLICATES/ — it stays filed in
    # place and is flagged for review (is_duplicate=True, status='done').
    # The user then resolves it:
    #   • "Keep both"      → is_duplicate=False, duplicate_of_id=None
    #   • "It's a duplicate"→ status='duplicate', file moved to /_DUPLICATES/
    # So the pending-review state is (is_duplicate=True AND status!='duplicate').
    duplicate_of_id  = db.Column(db.Integer, db.ForeignKey("doc_upload.id"), nullable=True)

    # Mission-critical source archive (added 2026-04-30): every upload's
    # original bytes are persisted in Dropbox at _SOURCE_ARCHIVE/ and
    # this column points to that location. Even if filed_dropbox_path
    # is later renamed, deleted, or the processed copy is lost, the
    # archive copy is the durable source-of-truth and can be recovered.
    source_archive_path = db.Column(db.String(500), nullable=True)

    # Soft delete / Trash (2026-06-11): deleting a doc no longer destroys the
    # row — status flips to 'deleted', deleted_at stamps it, and trash_meta
    # records {prior_status, moves:[{src,dest}]} (the Dropbox files' trash
    # locations) so /restore can move everything back. Rows with
    # status='deleted' are excluded from the docs list, matching, and
    # duplicate checks; hard removal is the separate /purge action.
    deleted_at = db.Column(db.DateTime, nullable=True)
    trash_meta = db.Column(db.Text, nullable=True)

    # Background re-OCR bookkeeping (2026-06-15): the reprocess-unpaired job
    # stamps this each time it (re-)runs Veryfi on the doc — on success AND on
    # OCR failure/skip, so the queue always drains. The worker selects docs
    # where this is NULL, which makes the job resumable: a Render worker
    # recycle no longer restarts from zero, and progress is readable from any
    # worker (it's row state, not the old per-worker in-memory dict).
    reprocessed_at = db.Column(db.DateTime, nullable=True)
    # When the AI data-cleanup pass last ran on this doc (vendor normalization +
    # extraction sanity). NULL = not yet cleaned; lets batch cleanup resume.
    ai_cleaned_at  = db.Column(db.DateTime, nullable=True)

    # AI-extracted travel-reservation details (added 2026-06-22): JSON blob from
    # ai_layer.extract_travel for hotel/flight/car docs — {kind, confirmation_no,
    # airline, flight_no, depart_at, arrive_at, depart_airport, arrive_airport,
    # hotel_name, hotel_address, check_in, check_out, room_type, traveler_name,
    # confidence}. Suggestion only until the user applies it to a person + travel
    # day via the doc-detail Travel panel. Watermark in travel_extracted_at.
    travel_json          = db.Column(db.Text, nullable=True)
    travel_extracted_at  = db.Column(db.DateTime, nullable=True)

    # Type-specific identifier (added 2026-04-30): Invoice #, PO #, Tax
    # ID (EIN/SSN), Policy #, etc. Pre-populated from Veryfi's
    # invoice_number / purchase_order_number / tax_id fields when
    # available; user-editable in the doc-detail modal.
    doc_number = db.Column(db.String(100), nullable=True)

    # Last 4 digits of the card / bank account the purchase was made on
    # (added 2026-05-30). Pre-populated from Veryfi's payment.card_number /
    # account fields when available; user-editable in the doc-detail modal.
    # Stored as text to preserve any leading zeros. Sortable in Docs +
    # Actuals so a user can group every charge on one card/account.
    card_last4 = db.Column(db.String(8), nullable=True)

    # ── Receipt detail for tax-credit reporting (added 2026-07) ────────
    # Some tax-credit programs disallow tip and require the merchant name +
    # address on itemized receipts. Pre-populated from Veryfi (subtotal / tax /
    # tip / vendor.address / vendor.phone_number); user-editable in the modal.
    subtotal          = db.Column(db.Numeric(12, 2), nullable=True)
    tax               = db.Column(db.Numeric(10, 2), nullable=True)
    tip               = db.Column(db.Numeric(10, 2), nullable=True)
    merchant_address  = db.Column(db.String(300), nullable=True)
    merchant_phone    = db.Column(db.String(40), nullable=True)

    # ── People / location linkage (added 2026-04-30) ───────────────────
    # Some doc types back relationships, not spend events:
    #   tax_form / contract / release / payroll →  a CrewMember (or
    #     vendor — but our Vendor concept is just text on the row, so
    #     CrewMember is the only structured target today)
    #   insurance / release (location) →  a Location
    # Both nullable; the doc-detail modal surfaces the appropriate
    # picker based on doc type.
    crew_member_id = db.Column(db.Integer, db.ForeignKey("crew_member.id"),
                                nullable=True)
    location_id    = db.Column(db.Integer, db.ForeignKey("location.id"),
                                nullable=True)

    # User note
    note             = db.Column(db.String(500), nullable=True)

    uploader  = db.relationship("User",         foreign_keys=[uploader_id])
    project   = db.relationship("ProjectSheet", foreign_keys=[project_id])


class ActivityLog(db.Model):
    """Per-budget audit trail for the Activity tab. Each mutation that
    changes the budget's data emits one row. Visibility is scoped per-user
    (super_admin sees all; admin sees their projects; dept_head sees their
    dept_code; everyone else sees their own changes). The before/after
    JSON snapshot supports single-row Undo from the UI.

    Schema is intentionally append-only — `undone_at` flips when an entry
    is reverted, but the original row is never deleted, so the trail
    stays intact.
    """
    __tablename__ = "activity_log"
    id            = db.Column(db.Integer, primary_key=True)
    project_id    = db.Column(db.Integer, db.ForeignKey("project_sheet.id"), nullable=True)
    budget_id     = db.Column(db.Integer, db.ForeignKey("budget.id"),         nullable=True)
    user_id       = db.Column(db.Integer, db.ForeignKey("users.id"),          nullable=True)
    dept_code     = db.Column(db.Integer, nullable=True)            # cached from line for filtering
    action        = db.Column(db.String(20), nullable=False)        # create | update | delete | restore | sync
    entity_type   = db.Column(db.String(40), nullable=False)        # budget_line, schedule_day, crew_assignment, ...
    entity_id     = db.Column(db.Integer, nullable=True)            # PK of affected row (null for bulk ops)
    entity_label  = db.Column(db.String(300), nullable=True)        # human-readable label
    field_changes = db.Column(db.Text, nullable=True)               # JSON: {"field":[old,new], ...} for updates
    before_json   = db.Column(db.Text, nullable=True)               # full snapshot pre-change (for undo)
    after_json    = db.Column(db.Text, nullable=True)               # full snapshot post-change
    dollar_delta  = db.Column(db.Numeric(14, 2), default=0)         # signed change in line est_total
    note          = db.Column(db.String(500), nullable=True)
    created_at    = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    undone_at     = db.Column(db.DateTime, nullable=True)
    undone_by_id  = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)

    user      = db.relationship("User",         foreign_keys=[user_id])
    undone_by = db.relationship("User",         foreign_keys=[undone_by_id])


# ── PurchaseOrder (added 2026-05-04 per user) ─────────────────────────
# Tracks vendor commitments made against a project. A PO is project-
# scoped; lines on the budget can be assigned to a PO via BudgetLine.po_id
# so a row like "Camera Package Rental" books against a specific PO with
# Newton (or whoever). Receipts / transactions can also be tagged to a
# PO via DocUpload.po_id and Transaction.po_id (added later) so spend
# rolls up against committed totals automatically.
#
# total_committed is an explicit cap; the budget UI flags any line
# assigned to a PO whose summed budget exceeds the cap, and the PO list
# view shows budget-vs-cap variance + invoiced-vs-cap when actuals are
# in. status tracks the lifecycle the user manages manually.
class PurchaseOrder(db.Model):
    __tablename__ = "purchase_order"
    id              = db.Column(db.Integer, primary_key=True)
    project_id      = db.Column(db.Integer, db.ForeignKey("project_sheet.id"), nullable=False)
    po_number       = db.Column(db.String(80),  nullable=False)   # user-assigned, e.g. "PO-2026-001"
    vendor_name     = db.Column(db.String(200), nullable=False)
    vendor_contact  = db.Column(db.String(200), nullable=True)    # name or generic contact
    vendor_email    = db.Column(db.String(200), nullable=True)
    vendor_phone    = db.Column(db.String(50),  nullable=True)
    total_committed = db.Column(db.Numeric(12, 2), nullable=True) # cap; null = no cap
    status          = db.Column(db.String(20), default='open')    # open | sent | received | closed | cancelled
    issued_date     = db.Column(db.Date,        nullable=True)
    notes           = db.Column(db.Text,        nullable=True)
    created_at      = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    created_by_user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    updated_at      = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    archived        = db.Column(db.Boolean, default=False, nullable=False)

    # Source estimate / quote document (added 2026-05-05 per user). When
    # a PO is created from a vendor's estimate via the Docs tab, this
    # FK points at the originating DocUpload. The PO list page links
    # back to it. Additional supporting docs from "Add to PO" appends
    # are tracked in PoDocAttachment (junction). NULL = manually-created
    # PO with no source doc.
    source_doc_upload_id = db.Column(db.Integer, db.ForeignKey("doc_upload.id", ondelete="SET NULL"), nullable=True)

    project   = db.relationship("ProjectSheet", foreign_keys=[project_id])
    creator   = db.relationship("User",         foreign_keys=[created_by_user_id])
    source_doc = db.relationship("DocUpload",   foreign_keys=[source_doc_upload_id])

    __table_args__ = (
        db.Index('ix_po_project',         'project_id'),
        db.Index('ix_po_project_archived','project_id', 'archived'),
        db.UniqueConstraint('project_id', 'po_number', name='uq_po_project_number'),
    )


# ── Junction: PO ↔ DocUpload (added 2026-05-05) ─────────────────────────
# Per user: "Add to PO" should let multiple estimates/invoices stack
# under one PO. Source attachment lives in PurchaseOrder.source_doc_
# upload_id (the FIRST one); subsequent additions land here with
# their per-doc amount + optional note (e.g. "Revised quote — added $500").
class PoDocAttachment(db.Model):
    __tablename__ = "po_doc_attachment"
    id              = db.Column(db.Integer, primary_key=True)
    po_id           = db.Column(db.Integer, db.ForeignKey("purchase_order.id", ondelete="CASCADE"), nullable=False)
    doc_upload_id   = db.Column(db.Integer, db.ForeignKey("doc_upload.id", ondelete="CASCADE"), nullable=False)
    amount          = db.Column(db.Numeric(12, 2), nullable=True)   # snapshot of doc's amount at attach time
    note            = db.Column(db.String(300), nullable=True)
    role            = db.Column(db.String(20), default='additional')  # source | additional
    created_at      = db.Column(db.DateTime, default=datetime.utcnow)
    created_by_user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    # Supersede / replace history (2026-06-15): when an estimate is replaced
    # by an updated quote, the old attachment is NOT deleted — it's stamped
    # superseded_at (and superseded_by_att_id points at the replacement) so
    # the price history survives. Superseded attachments stay visible on the
    # PO (struck through) but are excluded from estimates_total + the cap.
    superseded_at        = db.Column(db.DateTime, nullable=True)
    superseded_by_att_id = db.Column(db.Integer, nullable=True)

    po           = db.relationship("PurchaseOrder", foreign_keys=[po_id])
    doc          = db.relationship("DocUpload",     foreign_keys=[doc_upload_id])

    __table_args__ = (
        db.Index('ix_po_doc_po',  'po_id'),
        db.Index('ix_po_doc_doc', 'doc_upload_id'),
        db.UniqueConstraint('po_id', 'doc_upload_id', name='uq_po_doc'),
    )


# ── Sub-Budget (2026-05-26) ─────────────────────────────────────────────
# User-defined slice of a project's budget for a client-facing or partial
# view. Functions parallel to PurchaseOrder — name + cap + rollup +
# actualized total — but the relationship to BudgetLines is MANY-to-MANY
# (a line can belong to a PO and to one or more sub-budgets at the same
# time). Use cases: "Day 2 Shoot Only" line slice for a co-producer,
# "Equipment Subset" for a vendor proposal, "Talent + Crew Subset" for
# a payroll handoff. Each sub-budget can be exported as its own PDF
# (filtered subset of the parent budget) so the client sees only the
# rows that pertain to them.
#
# Per user 2026-05-26: "be able to function a similar way to a PO …
# but this is more like being able to export these items specifically
# added to a sub budget for a client or simple view."
class SubBudget(db.Model):
    __tablename__ = "sub_budget"
    id              = db.Column(db.Integer, primary_key=True)
    project_id      = db.Column(db.Integer, db.ForeignKey("project_sheet.id"), nullable=False)
    # Required: short human label, e.g. "Day 2 Filming", "Equipment Quote"
    name            = db.Column(db.String(200), nullable=False)
    # Optional: longer note for the client / internal context
    description     = db.Column(db.Text,        nullable=True)
    # Optional: cap budget for over-spend warnings (parallel to PO cap)
    total_committed = db.Column(db.Numeric(12, 2), nullable=True)
    # Pinned to a specific budget version (Working by default).
    # Sub-budget always reflects this budget's lines — assignments are
    # to BudgetLine.id rows on this specific budget.
    budget_id       = db.Column(db.Integer, db.ForeignKey("budget.id"), nullable=True)
    notes           = db.Column(db.Text,        nullable=True)
    archived        = db.Column(db.Boolean, default=False, nullable=False)
    created_at      = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    created_by_user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    updated_at      = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    project = db.relationship("ProjectSheet", foreign_keys=[project_id])
    creator = db.relationship("User",         foreign_keys=[created_by_user_id])
    budget  = db.relationship("Budget",       foreign_keys=[budget_id])

    __table_args__ = (
        db.Index('ix_sub_budget_project', 'project_id'),
        db.Index('ix_sub_budget_project_archived', 'project_id', 'archived'),
    )


# ── Junction: SubBudget ↔ BudgetLine (many-to-many) ────────────────────
# A line can be in multiple sub-budgets (e.g. the Camera Operator A line
# might appear in both "Day 2 Filming" and "Crew-Only Quote"). Unique
# constraint prevents accidental double-assignment of the same line to
# the same sub-budget.
class SubBudgetLine(db.Model):
    __tablename__ = "sub_budget_line"
    id              = db.Column(db.Integer, primary_key=True)
    sub_budget_id   = db.Column(db.Integer, db.ForeignKey("sub_budget.id", ondelete="CASCADE"), nullable=False)
    budget_line_id  = db.Column(db.Integer, db.ForeignKey("budget_line.id", ondelete="CASCADE"), nullable=False)
    sort_order      = db.Column(db.Integer, default=0)   # let user reorder within the sub-budget
    note            = db.Column(db.String(300), nullable=True)   # per-assignment annotation
    created_at      = db.Column(db.DateTime, default=datetime.utcnow)

    sub_budget  = db.relationship("SubBudget",  foreign_keys=[sub_budget_id])
    budget_line = db.relationship("BudgetLine", foreign_keys=[budget_line_id])

    __table_args__ = (
        db.UniqueConstraint('sub_budget_id', 'budget_line_id', name='uq_sub_budget_line'),
        db.Index('ix_sbl_sub_budget', 'sub_budget_id'),
        db.Index('ix_sbl_budget_line', 'budget_line_id'),
    )


# ── Client estimate share / approval portal (2026-06-03) ───────────────
# One row per "send this budget to a client as an estimate" action. The
# client opens /e/<token> (no login) to review and approve. We freeze a
# SNAPSHOT of the totals + version at send time so the client always sees
# exactly what was sent and the approval is bound to that specific version,
# even if the budget is edited afterward.
class AnalyzerBatch(db.Model):
    """H7 (2026-07-20): durable mirror of fp_analyzer's in-memory batch state
    (_raw_pending / _pending). The staged FILES already live in Dropbox; this
    row preserves the metadata (which staged paths belong to the batch, OCR
    results, review state) across worker recycles — previously a recycle
    between staging and analysis silently dropped the whole upload batch."""
    __tablename__ = "analyzer_batch"
    batch_token  = db.Column(db.String(64), primary_key=True)
    raw_json     = db.Column(db.Text, nullable=True)   # _raw_pending items
    pending_json = db.Column(db.Text, nullable=True)   # _pending items
    created_at   = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at   = db.Column(db.DateTime, default=datetime.utcnow)


class EstimateShare(db.Model):
    __tablename__ = "estimate_share"
    id              = db.Column(db.Integer, primary_key=True)
    project_id      = db.Column(db.Integer, db.ForeignKey("project_sheet.id"), nullable=False)
    budget_id       = db.Column(db.Integer, db.ForeignKey("budget.id"), nullable=False)
    token           = db.Column(db.String(64), unique=True, index=True, nullable=False)
    client_name     = db.Column(db.String(200), nullable=True)
    client_email    = db.Column(db.String(200), nullable=True)
    # Per-send choice: full line-by-line detail vs. summary top-sheet only.
    detail_mode     = db.Column(db.Boolean, default=False, nullable=False)
    version_label   = db.Column(db.String(80), nullable=True)   # e.g. "Estimated v2"
    snapshot_json   = db.Column(db.Text, nullable=True)          # frozen totals at send time
    grand_total     = db.Column(db.Numeric(14, 2), nullable=True)
    # sent | viewed | approved | declined | revoked
    status          = db.Column(db.String(20), default='sent', nullable=False)
    created_at      = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    created_by_user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    sent_at         = db.Column(db.DateTime, nullable=True)
    emailed         = db.Column(db.Boolean, default=False, nullable=False)
    first_viewed_at = db.Column(db.DateTime, nullable=True)
    last_viewed_at  = db.Column(db.DateTime, nullable=True)
    view_count      = db.Column(db.Integer, default=0, nullable=False)
    responded_at    = db.Column(db.DateTime, nullable=True)
    approver_name   = db.Column(db.String(200), nullable=True)   # typed-name e-signature
    approver_note   = db.Column(db.Text, nullable=True)
    approver_ip     = db.Column(db.String(64), nullable=True)
    expires_at      = db.Column(db.DateTime, nullable=True)

    project = db.relationship("ProjectSheet", foreign_keys=[project_id])
    budget  = db.relationship("Budget",       foreign_keys=[budget_id])
    creator = db.relationship("User",         foreign_keys=[created_by_user_id])

    __table_args__ = (
        db.Index('ix_estimate_share_project', 'project_id'),
        db.Index('ix_estimate_share_budget', 'budget_id'),
    )


# ── Historical FX rate cache (2026-06-11) ──────────────────────────────
# Foreign-currency receipts (CNY/KRW/GBP/EUR/…) are converted to USD using
# the rate on the receipt's date, pulled from frankfurter.dev (ECB data,
# free, no key). Rates are immutable historical facts, so we cache them by
# (date, currency) and never re-fetch. usd_rate = USD value of 1 unit.
class FxRate(db.Model):
    __tablename__ = "fx_rate"
    id         = db.Column(db.Integer, primary_key=True)
    date       = db.Column(db.String(10), nullable=False)   # YYYY-MM-DD (rate date requested)
    currency   = db.Column(db.String(8),  nullable=False)   # ISO base, e.g. CNY
    usd_rate   = db.Column(db.Numeric(18, 8), nullable=False)
    fetched_at = db.Column(db.DateTime, default=datetime.utcnow)
    __table_args__ = (
        db.UniqueConstraint('date', 'currency', name='uq_fx_rate_date_ccy'),
    )


# ── Persistent "not a match" rejections (2026-06-15) ───────────────────
# When the user clicks "Not a match" on a suggested receipt↔charge pairing,
# we remember it here so run_auto_match never proposes that exact pair again.
# Without this the matcher re-suggested dismissed pairs on every run, which is
# why rejected matches kept reappearing. Keyed by (transaction_id of the bank
# charge, doc_upload_id of the receipt) — both stable identities.
class MatchRejection(db.Model):
    __tablename__ = "match_rejection"
    id             = db.Column(db.Integer, primary_key=True)
    project_id     = db.Column(db.Integer, db.ForeignKey("project_sheet.id"), nullable=False)
    transaction_id = db.Column(db.Integer, nullable=False)   # the bank/QBO charge
    doc_upload_id  = db.Column(db.Integer, nullable=False)    # the receipt
    created_at     = db.Column(db.DateTime, default=datetime.utcnow)
    created_by     = db.Column(db.Integer, nullable=True)
    __table_args__ = (
        db.UniqueConstraint('transaction_id', 'doc_upload_id', name='uq_match_rejection_pair'),
    )


class TransactionDupDismissal(db.Model):
    """A duplicate-transaction cluster the user reviewed and confirmed is
    genuinely SEPARATE (e.g. two real $40 Ubers same day). Keyed by the
    cluster's stable signature so the scan doesn't re-flag it. (User 2026-06-17.)"""
    __tablename__ = "transaction_dup_dismissal"
    id         = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.Integer, db.ForeignKey("project_sheet.id"), nullable=False)
    dup_key    = db.Column(db.String(200), nullable=False)   # qbo:<id> | attr:<amt>|<date>|<vendor>|<card>
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    created_by = db.Column(db.Integer, nullable=True)
    __table_args__ = (
        db.UniqueConstraint('project_id', 'dup_key', name='uq_txn_dup_dismissal'),
    )


# ── AI layer (FP Budget AI-Layer spec §8, 2026-06-17) ───────────────────────
class VendorCategoryMap(db.Model):
    """Learned vendor → category mapping (spec §B4). Each user correction
    writes/strengthens a row so future docs can short-circuit the LLM. Our
    taxonomy is the COA, so we store account_code / budget_line_id."""
    __tablename__ = "vendor_category_map"
    id                = db.Column(db.Integer, primary_key=True)
    project_id        = db.Column(db.Integer, nullable=True)   # null = global default
    vendor_canonical  = db.Column(db.String(200), nullable=False, index=True)
    account_code      = db.Column(db.Integer, nullable=True)
    budget_line_id    = db.Column(db.Integer, nullable=True)
    category_id       = db.Column(db.String(60), nullable=True)
    confirm_count     = db.Column(db.Integer, default=1)
    last_confirmed_at = db.Column(db.DateTime, default=datetime.utcnow)
    __table_args__ = (
        db.UniqueConstraint('project_id', 'vendor_canonical', name='uq_vendor_cat_map'),
    )


class AiEvent(db.Model):
    """Audit + tuning log for every model call (spec §8). Doubles as the dataset
    for prompt/threshold tuning and the provider A/B test."""
    __tablename__ = "ai_event"
    id                  = db.Column(db.Integer, primary_key=True)
    project_id          = db.Column(db.Integer, nullable=True)
    doc_upload_id       = db.Column(db.Integer, nullable=True)
    feature             = db.Column(db.String(20))   # 'categorize' | 'anomaly'
    provider            = db.Column(db.String(20))
    model               = db.Column(db.String(60))
    request_json        = db.Column(db.Text)
    response_json       = db.Column(db.Text)
    latency_ms          = db.Column(db.Integer, nullable=True)
    user_final_decision = db.Column(db.Text, nullable=True)
    created_at          = db.Column(db.DateTime, default=datetime.utcnow)


class AnomalyFlag(db.Model):
    """A surfaced anomaly / review-queue item (spec §8). Backs the Dashboard
    "Action Center". type: vendor_cleanup | data_issue | double_coded | duplicate.
    payload_json carries the proposed fix or duplicate-cluster members; dedup_key
    stops the same thing being re-flagged on every scan."""
    __tablename__ = "anomaly_flag"
    id             = db.Column(db.Integer, primary_key=True)
    project_id     = db.Column(db.Integer, nullable=True)
    doc_upload_id  = db.Column(db.Integer, nullable=True)
    transaction_id = db.Column(db.Integer, nullable=True)
    type           = db.Column(db.String(40))
    title          = db.Column(db.String(200), nullable=True)
    severity       = db.Column(db.String(10))
    explanation    = db.Column(db.Text)
    confidence     = db.Column(db.Numeric(4, 3), nullable=True)
    payload_json   = db.Column(db.Text, nullable=True)
    dedup_key      = db.Column(db.String(160), nullable=True, index=True)
    resolved       = db.Column(db.Boolean, default=False)
    resolution     = db.Column(db.String(40), nullable=True)
    resolved_by    = db.Column(db.Integer, nullable=True)
    resolved_at    = db.Column(db.DateTime, nullable=True)
    created_at     = db.Column(db.DateTime, default=datetime.utcnow)


class VendorAlias(db.Model):
    """Learned raw-vendor → clean display-name mapping (AI data cleanup,
    2026-06-18). Mirrors VendorCategoryMap: each confirmed/auto-applied cleanup
    writes a row so repeat vendors normalize consistently and for free (no LLM).
    Keyed on canon_vendor(raw)."""
    __tablename__ = "vendor_alias"
    id                = db.Column(db.Integer, primary_key=True)
    project_id        = db.Column(db.Integer, nullable=True)   # null = global default
    raw_canonical     = db.Column(db.String(200), nullable=False, index=True)
    clean_name        = db.Column(db.String(200), nullable=False)
    confirm_count     = db.Column(db.Integer, default=1)
    last_confirmed_at = db.Column(db.DateTime, default=datetime.utcnow)
    __table_args__ = (
        db.UniqueConstraint('project_id', 'raw_canonical', name='uq_vendor_alias'),
    )


class ProjectClientContact(db.Model):
    """RETIRED 2026-07-13 — merged into ProjectClient (see its docstring).

    This model + table are retained for data safety only; the app no longer
    reads or writes it. Estimate recipients and call-sheet clients are now one
    list on ProjectClient (source='estimate_send' | 'manual'). A one-time,
    idempotent boot migration (_migrate_client_contacts_into_clients in app.py)
    copied every row here into ProjectClient. Do NOT add new writers.

    Historical: was auto-populated when an estimate was sent
    (source='estimate_send') and manually addable on the People tab
    (source='manual'); deduped case-insensitively by (project_id, email);
    surfaced as the '🤝 Clients' group. (User 2026-07-08.)"""
    __tablename__ = "project_client_contact"
    id          = db.Column(db.Integer, primary_key=True)
    project_id  = db.Column(db.Integer, db.ForeignKey("project_sheet.id"), nullable=False, index=True)
    name        = db.Column(db.String(200), nullable=True)
    email       = db.Column(db.String(200), nullable=False, index=True)
    phone       = db.Column(db.String(50), nullable=True)
    company     = db.Column(db.String(200), nullable=True)
    source      = db.Column(db.String(20), default='manual', nullable=False)  # 'estimate_send' | 'manual'
    created_at  = db.Column(db.DateTime, default=datetime.utcnow)

    project = db.relationship("ProjectSheet", foreign_keys=[project_id])
