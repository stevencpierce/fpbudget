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
    is_admin             = db.Column(db.Boolean, default=False, nullable=False)
    is_active            = db.Column(db.Boolean, default=True, nullable=False)
    must_change_password = db.Column(db.Boolean, default=False, nullable=False)
    created_at           = db.Column(db.DateTime, default=datetime.utcnow)

    def set_password(self, pw):
        from werkzeug.security import generate_password_hash
        self.password_hash = generate_password_hash(pw)

    def check_password(self, pw):
        from werkzeug.security import check_password_hash
        return check_password_hash(self.password_hash, pw)


class ProjectAccess(db.Model):
    __tablename__ = "project_access"
    id         = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.Integer, db.ForeignKey("project_sheet.id"), nullable=False)
    user_id    = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    role       = db.Column(db.String(20), default="collaborator")  # "owner" or "collaborator"
    __table_args__ = (db.UniqueConstraint("project_id", "user_id", name="uq_proj_user"),)

# ── Mirrored shared tables (read-only from FPBudget) ─────────────────────────

class ProjectSheet(db.Model):
    __tablename__ = "project_sheet"
    id   = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False)


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
    # Travel role group override: 'talent' | 'atl' | 'crew'  (falls back to COA code)
    role_group      = db.Column(db.String(20), nullable=True)
    # Per-unit rate used by schedule-driven non-labor lines (e.g. $25/meal, $150/night)
    unit_rate       = db.Column(db.Numeric(10, 2), nullable=True)
    assigned_crew_id = db.Column(db.Integer, db.ForeignKey("crew_member.id"), nullable=True)
    assigned_crew    = db.relationship("CrewMember", foreign_keys=[assigned_crew_id])

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
    rate_type       = db.Column(db.String(20), default="day_10")
    fringe_type     = db.Column(db.String(5), default="N")
    agent_pct       = db.Column(db.Numeric(6, 4), default=0)
    estimated_total = db.Column(db.Numeric(12, 2), default=0)
    sort_order      = db.Column(db.Integer, default=0)
    __table_args__  = (db.UniqueConstraint("template_id", "account_code"),)


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
    confirmed_at  = db.Column(db.DateTime, nullable=True)
    confirm_token = db.Column(db.String(64), nullable=True, unique=True)
    # Status: pending | sent | confirmed | bounced
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
