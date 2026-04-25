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


class Transaction(db.Model):
    __tablename__ = "transaction"
    id                  = db.Column(db.Integer, primary_key=True)
    project_id          = db.Column(db.Integer)
    account_code        = db.Column(db.Integer)
    account_code_name   = db.Column(db.String(100))
    amount              = db.Column(db.Numeric(12, 2))
    is_expense          = db.Column(db.Boolean)
    not_project_expense = db.Column(db.Boolean)
    vendor              = db.Column(db.String(300))
    txn_date            = db.Column(db.String(10))
    note                = db.Column(db.Text)


# ── Budget-specific tables ────────────────────────────────────────────────────

class Budget(db.Model):
    __tablename__ = "budget"
    id              = db.Column(db.Integer, primary_key=True)
    project_id      = db.Column(db.Integer, nullable=False)
    name            = db.Column(db.String(200), nullable=False)
    budget_mode     = db.Column(db.String(20), default="estimated")  # estimated | schedule | hybrid
    company_fee_pct       = db.Column(db.Numeric(6, 4), default=0.18)
    company_fee_dispersed = db.Column(db.Boolean, default=False, nullable=False)
    # JSON-encoded array of COA section codes EXCLUDED from the
    # production-company fee base. NULL / empty = every section
    # contributes (default). Edited via budget Settings → "Sections
    # exempt from Prod Co Fee".  Column added manually via psql on
    # 2026-04-25 after the boot migration kept failing under the 5s
    # statement_timeout watchdog.
    fee_excluded_sections = db.Column(db.Text, nullable=True)
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

    # Three-phase system columns
    working_total   = db.Column(db.Numeric(14, 2), nullable=True)  # Working forecast (snapshot + evolving)
    manual_actual   = db.Column(db.Numeric(14, 2), nullable=True)  # Manual actual override per line

    # Per-instance custom schedule display labels: JSON {"1": "Hero Biker", "3": "Lead Double"}
    schedule_labels = db.Column(db.Text, nullable=True)

    crew_assignments = db.relationship("CrewAssignment", backref="line", lazy=True,
                                       cascade="all, delete-orphan")
    schedule_days    = db.relationship("ScheduleDay", backref="line", lazy=True,
                                       foreign_keys="ScheduleDay.budget_line_id")


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


class ProductionDay(db.Model):
    """Per-production-day flags: meals. Separate rows per schedule_mode (estimated/working)."""
    __tablename__ = "production_day"
    id                  = db.Column(db.Integer, primary_key=True)
    budget_id           = db.Column(db.Integer, db.ForeignKey("budget.id"), nullable=False)
    date                = db.Column(db.Date, nullable=False)
    schedule_mode       = db.Column(db.String(20), default="estimated", nullable=False)
    courtesy_breakfast  = db.Column(db.Boolean, default=False)
    first_meal          = db.Column(db.Boolean, default=False)
    second_meal         = db.Column(db.Boolean, default=False)
    __table_args__      = (db.UniqueConstraint("budget_id", "date", "schedule_mode", name="uq_prod_day"),)



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


class ProjectClient(db.Model):
    """Client contact scoped to a project — shown on call sheet page 1."""
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
    amount           = db.Column(db.Numeric(10, 2), nullable=True)
    doc_date         = db.Column(db.Date, nullable=True)
    confidence       = db.Column(db.Numeric(5, 2), nullable=True) # 0-100
    category         = db.Column(db.String(100), nullable=True)   # predicted COA category

    # Filing to Dropbox
    filed_filename   = db.Column(db.String(300), nullable=True)   # renamed file
    filed_dropbox_path = db.Column(db.String(500), nullable=True) # full Dropbox path
    filed_at         = db.Column(db.DateTime, nullable=True)
    is_duplicate     = db.Column(db.Boolean, default=False)

    # User note
    note             = db.Column(db.String(500), nullable=True)

    uploader  = db.relationship("User",         foreign_keys=[uploader_id])
    project   = db.relationship("ProjectSheet", foreign_keys=[project_id])
