import os, logging, json, csv, io, re
from flask_mail import Mail, Message as MailMessage
from datetime import date, datetime, timedelta
from flask import (Flask, render_template, redirect, url_for, request,
                   flash, jsonify, Response, abort)
from flask_login import (LoginManager, UserMixin, login_user, logout_user,
                         login_required, current_user)
from dotenv import load_dotenv
from sqlalchemy import func, text
from sqlalchemy.exc import IntegrityError
from weasyprint import HTML as WeasyprintHTML

from models import (db, User, ProjectAccess, ProjectSheet, Transaction, Budget, BudgetLine,
                    FringeConfig, CrewMember, CrewAssignment, ScheduleDay,
                    BudgetTemplate, BudgetTemplateLine, TaxCredit, PayrollProfile,
                    ProductionDay, Location, LocationDay, CallSheetData,
                    SupportContact, ProjectUnion, ProjectClient, CallSheetSend, CallSheetRecipient,
                    BudgetDirectContact, CompanySettings, DocUpload)
from budget_calc import (calc_line, calc_line_from_schedule, calc_top_sheet,
                         get_fringe_configs, seed_fringes, seed_standard_template,
                         seed_payroll_profiles, FP_COA_SECTIONS, DAY_TYPE_MULTIPLIERS,
                         calc_days_ot_status, _run_payroll_calc, calc_line_detail,
                         sync_schedule_driven_lines, SCHEDULE_LINE_DEFS, get_role_group,
                         _float as _bc_float)
try:
    import anthropic as _anthropic_sdk
    _HAS_ANTHROPIC = True
except ImportError:
    _HAS_ANTHROPIC = False

# ── Production Staff sub-department keyword lookup ───────────────────────────
# Ordered tuples: (keyword, sub_group). First match wins.
_PROD_STAFF_SUBGROUPS = [
    ("Line Producer",              "Production"),
    ("UPM",                        "Production"),
    ("Production Supervisor",      "Production"),
    ("Production Coordinator",     "Production"),
    ("Production Manager",         "Production"),
    ("Production Accountant",      "Production"),
    ("Payroll Coordinator",        "Production"),
    ("Travel Coordinator",         "Production"),
    ("APOC",                       "Production"),
    ("Production Secretary",       "Production"),
    ("2nd 2nd AD",                 "Direction / AD"),
    ("Second Unit Director",       "Direction / AD"),
    ("1st AD",                     "Direction / AD"),
    ("2nd AD",                     "Direction / AD"),
    ("Script Supervisor",          "Direction / AD"),
    ("Key PA",                     "Direction / AD"),
    ("Set PA",                     "Direction / AD"),
    ("Office PA",                  "Direction / AD"),
    (" PA",                        "Direction / AD"),
    ("Director of Photography",    "Camera"),
    ("Second Unit DP",             "Camera"),
    ("Camera Operator",            "Camera"),
    ("1st AC",                     "Camera"),
    ("2nd AC",                     "Camera"),
    ("DIT",                        "Camera"),
    ("Steadicam",                  "Camera"),
    ("Data Wrangler",              "Camera"),
    ("Video Engineer",             "Camera"),
    ("VTR Operator",               "Camera"),
    ("Gaffer",                     "Grip & Electric"),
    ("Key Grip",                   "Grip & Electric"),
    ("Best Boy Electric",          "Grip & Electric"),
    ("Best Boy Grip",              "Grip & Electric"),
    ("Generator Operator",         "Grip & Electric"),
    ("Swing (Electric)",           "Grip & Electric"),
    ("Swing (Grip)",               "Grip & Electric"),
    ("Electric",                   "Grip & Electric"),
    ("Grip",                       "Grip & Electric"),
    ("Sound Mixer",                "Sound"),
    ("Boom Operator",              "Sound"),
    ("Utility Sound",              "Sound"),
    ("Production Designer",        "Art"),
    ("Art Director",               "Art"),
    ("Set Dresser",                "Art"),
    ("Props Master",               "Art"),
    ("Props Assistant",            "Art"),
    ("Key Makeup",                 "Hair & Makeup"),
    ("HMU",                        "Hair & Makeup"),
    ("Makeup Artist",              "Hair & Makeup"),
    ("Hair Stylist",               "Hair & Makeup"),
    ("SFX Makeup",                 "Hair & Makeup"),
    ("Wardrobe Stylist",           "Wardrobe"),
    ("Wardrobe Assistant",         "Wardrobe"),
    ("Wardrobe",                   "Wardrobe"),
    ("Location Manager",           "Locations"),
    ("Location Assistant",         "Locations"),
    ("Location",                   "Locations"),
    ("Transportation Coordinator", "Transportation"),
    ("Driver",                     "Transportation"),
    ("Technical Director",         "Control Room"),
    ("Switcher Operator",          "Control Room"),
    ("EPK",                        "EPK / BTS"),
    ("Craft Services",             "Craft Services"),
]

def _get_prod_staff_subgroup(description):
    """Return sub-department label for a Production Staff line, or None."""
    if not description:
        return None
    desc_lower = description.lower()
    for keyword, group in _PROD_STAFF_SUBGROUPS:
        if keyword.lower() in desc_lower:
            return group
    return None

# ── Talent sub-department keyword lookup ──────────────────────────────────────
_TALENT_SUBGROUPS = [
    ("Host",          "Host / Presenter"),
    ("Co-Host",       "Host / Presenter"),
    ("Presenter",     "Host / Presenter"),
    ("Anchor",        "Host / Presenter"),
    ("Correspondent", "Host / Presenter"),
    ("Reporter",      "Host / Presenter"),
    ("Lead",          "Principal Cast"),
    ("Principal",     "Principal Cast"),
    ("Series Regular","Principal Cast"),
    ("Recurring",     "Supporting Cast"),
    ("Supporting",    "Supporting Cast"),
    ("Guest Star",    "Supporting Cast"),
    ("Co-Star",       "Supporting Cast"),
    ("Day Player",    "Day Players"),
    ("Featured",      "Day Players"),
    ("Under-5",       "Day Players"),
    ("Stunt",         "Stunts"),
    ("Stand-In",      "Background / Stand-Ins"),
    ("Background",    "Background / Stand-Ins"),
    ("Extra",         "Background / Stand-Ins"),
    ("Atmosphere",    "Background / Stand-Ins"),
    ("Voice",         "Voice-Over"),
    ("VO ",           "Voice-Over"),
    ("Spokesperson",  "Spokesperson"),
    ("Influencer",    "Spokesperson"),
]

def _get_talent_subgroup(description):
    """Return sub-department label for a Talent line, or None."""
    if not description:
        return None
    desc_lower = description.lower()
    for keyword, group in _TALENT_SUBGROUPS:
        if keyword.lower() in desc_lower:
            return group
    return None

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ── Contact data helpers ───────────────────────────────────────────────────────

def _normalize_phone(raw):
    """Normalize any phone string to +1-NXX-NXX-XXXX (US) or +CC-... (intl)."""
    if not raw:
        return None
    raw = str(raw).strip()
    digits = re.sub(r'\D', '', raw)
    n = len(digits)
    if n == 0:
        return None
    if n == 10:                          # US/Canada without country code
        return f"+1-{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    if n == 11 and digits[0] == '1':     # US/Canada with leading 1
        return f"+1-{digits[1:4]}-{digits[4:7]}-{digits[7:]}"
    if n == 11:                          # Other 11-digit international
        return f"+{digits[0]}-{digits[1:4]}-{digits[4:7]}-{digits[7:]}"
    if n == 12:
        return f"+{digits[:2]}-{digits[2:5]}-{digits[5:8]}-{digits[8:]}"
    if n >= 7:                           # Unknown length — keep digits with +
        return f"+{digits}"
    return raw                           # Too short to parse — return as-is


def _validate_email(email):
    """Return True if email passes a basic format check (or is empty)."""
    if not email:
        return True
    return bool(re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]{2,}$', email.strip()))

app = Flask(__name__)
app.config["SECRET_KEY"]                     = os.getenv("SECRET_KEY", "fpbudget-dev-secret")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

_db_url = os.getenv("DATABASE_URL", "sqlite:///fp_budget.db").replace("postgres://", "postgresql://")
if "postgresql" in _db_url:
    _sep = "&" if "?" in _db_url else "?"
    _db_url += f"{_sep}connect_timeout=10"
app.config["SQLALCHEMY_DATABASE_URI"]    = _db_url
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {"pool_pre_ping": True, "pool_recycle": 280}

db.init_app(app)

app.config['MAIL_SERVER']         = os.getenv('MAIL_SERVER', 'smtp.gmail.com')
app.config['MAIL_PORT']           = int(os.getenv('MAIL_PORT', 587))
app.config['MAIL_USE_TLS']        = True
app.config['MAIL_USERNAME']       = os.getenv('MAIL_USERNAME', '')
app.config['MAIL_PASSWORD']       = os.getenv('MAIL_PASSWORD', '')
app.config['MAIL_DEFAULT_SENDER'] = os.getenv('MAIL_USERNAME', '')
mail = Mail(app)

# ── Timezone filter ────────────────────────────────────────────────────────────
try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    def _to_tz(dt, tz_str):
        if not dt or not tz_str:
            return dt
        try:
            return dt.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo(tz_str))
        except (ZoneInfoNotFoundError, Exception):
            return dt
except ImportError:
    try:
        import pytz
        def _to_tz(dt, tz_str):
            if not dt or not tz_str:
                return dt
            try:
                return pytz.utc.localize(dt).astimezone(pytz.timezone(tz_str))
            except Exception:
                return dt
    except ImportError:
        def _to_tz(dt, tz_str):
            return dt

@app.template_filter("in_tz")
def in_tz_filter(dt, tz_str, fmt=None):
    if not dt:
        return ""
    local = _to_tz(dt, tz_str or "UTC")
    f = fmt or "%b %-d, %Y %-I:%M%p"
    return local.strftime(f).lower()

ADMIN_EMAIL        = os.getenv("ADMIN_EMAIL", "steven@thefp.tv")
ADMIN_PASSWORD     = os.getenv("ADMIN_PASSWORD", "changeme123")
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")

@app.context_processor
def inject_globals():
    return {"GOOGLE_MAPS_API_KEY": GOOGLE_MAPS_API_KEY}


def _fmt_local(dt, tz_name=None):
    """Format a UTC datetime in the given IANA timezone (defaults to America/New_York)."""
    if dt is None:
        return ''
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(tz_name or 'America/New_York')
        local_dt = dt.replace(tzinfo=ZoneInfo('UTC')).astimezone(tz)
        return local_dt.strftime('%-I:%M %p')
    except Exception:
        return dt.strftime('%-I:%M %p')


# ── Cloudflare R2 helpers ──────────────────────────────────────────────────────
_R2_ACCOUNT_ID  = os.getenv('R2_ACCOUNT_ID', '')
_R2_ACCESS_KEY  = os.getenv('R2_ACCESS_KEY_ID', '')
_R2_SECRET      = os.getenv('R2_SECRET_ACCESS_KEY', '')
_R2_BUCKET      = os.getenv('R2_BUCKET', 'fpbudget-docs')

def _r2_client():
    import boto3
    return boto3.client(
        's3',
        endpoint_url=f"https://{_R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=_R2_ACCESS_KEY,
        aws_secret_access_key=_R2_SECRET,
        region_name='auto',
    )

def _r2_upload(file_bytes, key, content_type='application/octet-stream'):
    """Upload bytes to R2. Returns True on success."""
    try:
        _r2_client().put_object(Bucket=_R2_BUCKET, Key=key, Body=file_bytes, ContentType=content_type)
        return True
    except Exception as e:
        logging.warning(f"R2 upload failed: {e}")
        return False

def _r2_presigned_url(key, expires=3600):
    """Return a presigned URL for a private R2 object (expires in `expires` seconds)."""
    try:
        return _r2_client().generate_presigned_url(
            'get_object', Params={'Bucket': _R2_BUCKET, 'Key': key}, ExpiresIn=expires)
    except Exception as e:
        logging.warning(f"R2 presigned URL failed: {e}")
        return None


# ── Dropbox helpers ────────────────────────────────────────────────────────────
_DBX_OPS_ROOT      = os.getenv('DROPBOX_OPERATIONS_PATH', '/Steven Pierce/_FP OPERATIONS FOLDER')
_DBX_TEMPLATE_NAME = os.getenv('DROPBOX_TEMPLATE_FOLDER', '!_PRODUCTION_PROJECT_TEMPLATE')

def _dbx_client():
    import dropbox as _dbx_mod
    return _dbx_mod.Dropbox(os.getenv('DROPBOX_ACCESS_TOKEN', ''))

def _provision_dropbox_folder(dropbox_folder):
    """Copy the project template tree to a new project folder. Returns path or None."""
    if not os.getenv('DROPBOX_ACCESS_TOKEN'):
        return None
    try:
        src  = f"{_DBX_OPS_ROOT}/{_DBX_TEMPLATE_NAME}"
        dest = f"{_DBX_OPS_ROOT}/{dropbox_folder}"
        _dbx_client().files_copy_v2(src, dest)
        return dest
    except Exception as e:
        logging.warning(f"Dropbox provision failed: {e}")
        return None


# ── Project folder slug generation ────────────────────────────────────────────
def _make_project_slug(project_name, client_name=None, dt=None):
    """Generate a Dropbox folder slug: YYYY-MM_ClientSlug_ProjectSlug"""
    import re
    from datetime import date as _date
    dt = dt or _date.today()

    def slugify(s, maxlen):
        s = re.sub(r"[^a-zA-Z0-9 ]", "", (s or "").strip())
        return "".join(w.capitalize() for w in s.split())[:maxlen]

    month     = dt.strftime("%Y-%m")
    c_slug    = slugify(client_name or "FP", 20)
    p_slug    = slugify(project_name, 30)
    return f"{month}_{c_slug}_{p_slug}"

def _unique_project_slug(project_name, client_name=None, exclude_id=None):
    """Return a slug guaranteed unique in the project_sheet table."""
    from models import ProjectSheet as _PS
    base = _make_project_slug(project_name, client_name)
    slug = base
    # Check DB for collision
    q = _PS.query.filter(_PS.dropbox_folder == slug)
    if exclude_id:
        q = q.filter(_PS.id != exclude_id)
    if not q.first():
        return slug
    # Fallback: append sequential number
    for n in range(2, 99):
        slug = f"{base}_{n}"
        q = _PS.query.filter(_PS.dropbox_folder == slug)
        if exclude_id:
            q = q.filter(_PS.id != exclude_id)
        if not q.first():
            return slug
    return base  # give up — extremely unlikely


def _send_email(to, subject, body, attachment_bytes=None, attachment_filename=None):
    """Send email — silently no-ops if mail not configured."""
    if not app.config.get('MAIL_USERNAME'):
        return False
    try:
        msg = MailMessage(subject, recipients=[to], body=body)
        if attachment_bytes and attachment_filename:
            msg.attach(attachment_filename, 'application/pdf', attachment_bytes)
        mail.send(msg)
        return True
    except Exception as e:
        logging.warning(f"Email send failed: {e}")
        return False


def _send_sms(to_phone, body):
    """Send SMS via Twilio — silently no-ops if not configured."""
    sid   = os.getenv('TWILIO_ACCOUNT_SID', '')
    token = os.getenv('TWILIO_AUTH_TOKEN', '')
    from_num = os.getenv('TWILIO_FROM_NUMBER', '')
    if not sid or not token or not from_num:
        return False
    # Normalize phone: strip everything except digits and leading +
    import re as _re
    digits = _re.sub(r'[^\d+]', '', to_phone.strip())
    if not digits:
        return False
    if not digits.startswith('+'):
        digits = '+1' + digits  # default to US
    try:
        from twilio.rest import Client as _TwilioClient
        _TwilioClient(sid, token).messages.create(body=body, from_=from_num, to=digits)
        return True
    except Exception as e:
        logging.warning(f"SMS send failed: {e}")
        return False


def _generate_callsheet_pdf(send_obj, project_name, date_display, cs_data, crew_rows, locations_today):
    """Generate a PDF of the call sheet using weasyprint. Returns bytes or None."""
    try:
        import weasyprint
        html_str = _render_callsheet_pdf_html(
            send_obj=send_obj,
            project_name=project_name,
            date_display=date_display,
            cs_data=cs_data,
            crew_rows=crew_rows,
            locations_today=locations_today,
        )
        pdf_bytes = weasyprint.HTML(string=html_str, base_url=None).write_pdf()
        return pdf_bytes
    except Exception as e:
        logging.warning(f"PDF generation failed: {e}")
        return None


def _render_callsheet_pdf_html(send_obj, project_name, date_display, cs_data, crew_rows, locations_today):
    """Render a lightweight HTML string suitable for PDF conversion."""
    import html as _html
    esc = _html.escape
    kp  = cs_data.get('key_personnel') or []
    cct = cs_data.get('crew_call_times') or {}
    dn  = cs_data.get('dept_notes') or []
    uc  = cs_data.get('useful_contacts') or []

    def row(label, val):
        if not val:
            return ''
        return f'<tr><td class="lbl">{esc(label)}</td><td>{esc(str(val))}</td></tr>'

    # Build location blocks
    loc_html = ''
    for loc in (locations_today or []):
        loc_html += f'<div class="loc-block"><strong>{esc(loc.name or "")}</strong>'
        if getattr(loc, 'facility_name', None):
            loc_html += f' — {esc(loc.facility_name)}'
        if getattr(loc, 'address', None):
            loc_html += f'<br><span class="sub">{esc(loc.address)}</span>'
        if getattr(loc, 'contact_name', None):
            loc_html += f'<br><span class="sub">Contact: {esc(loc.contact_name)}'
            if getattr(loc, 'contact_phone', None):
                loc_html += f' · {esc(loc.contact_phone)}'
            loc_html += '</span>'
        loc_html += '</div>'

    # Build crew call time rows grouped by section
    section_order = []
    section_rows = {}
    for key, t in cct.items():
        parts = key.split('||', 2)
        sec   = parts[0] if len(parts) > 0 else ''
        role  = parts[1] if len(parts) > 1 else ''
        name  = parts[2] if len(parts) > 2 else ''
        if sec not in section_rows:
            section_order.append(sec)
            section_rows[sec] = []
        section_rows[sec].append((role, name, t))

    crew_html = ''
    for sec in section_order:
        crew_html += f'<tr class="sec-hdr"><td colspan="3">{esc(sec)}</td></tr>'
        for role, name, t in section_rows[sec]:
            crew_html += f'<tr><td>{esc(role)}</td><td>{esc(name)}</td><td class="time">{esc(t)}</td></tr>'

    # Key personnel
    kp_html = ''
    for p in kp:
        if not isinstance(p, dict) or (not p.get('name') and not p.get('role')):
            continue
        kp_html += f'<tr><td>{esc(p.get("role",""))}</td><td>{esc(p.get("name",""))}</td><td>{esc(p.get("phone",""))}</td></tr>'

    # Dept notes
    dnotes_html = ''
    for d in dn:
        if isinstance(d, dict) and d.get('dept') and d.get('note'):
            dnotes_html += f'<div class="dnote"><strong>{esc(d["dept"])}:</strong> {esc(d["note"])}</div>'

    version_label = getattr(send_obj, 'version_label', '') or ''

    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<style>
  body {{ font-family: Arial, sans-serif; font-size: 9pt; margin: 0; padding: 0; color: #111; }}
  .page {{ padding: 18px 22px; }}
  h1 {{ font-size: 14pt; margin: 0 0 2px; }}
  .sub-head {{ font-size: 8pt; color: #555; margin-bottom: 10px; }}
  h2 {{ font-size: 10pt; border-bottom: 1px solid #bbb; margin: 12px 0 4px; padding-bottom: 2px; text-transform: uppercase; letter-spacing: .04em; }}
  table {{ width: 100%; border-collapse: collapse; margin-bottom: 8px; }}
  td {{ padding: 2px 6px; vertical-align: top; font-size: 8.5pt; }}
  .lbl {{ color: #555; width: 130px; white-space: nowrap; }}
  .time {{ font-weight: bold; text-align: right; }}
  .sec-hdr td {{ background: #eee; font-weight: bold; font-size: 8pt; padding: 3px 6px; }}
  .loc-block {{ margin-bottom: 8px; font-size: 8.5pt; }}
  .sub {{ color: #555; }}
  .dnote {{ margin-bottom: 4px; font-size: 8.5pt; }}
  .info-table td {{ border-bottom: 1px solid #f0f0f0; }}
  .footer {{ font-size: 7pt; color: #888; margin-top: 16px; border-top: 1px solid #ddd; padding-top: 6px; }}
</style>
</head><body><div class="page">

<h1>{esc(project_name)}</h1>
<div class="sub-head">Call Sheet · {esc(date_display)}{' · ' + esc(version_label) if version_label else ''}</div>

{"<h2>General</h2><table class='info-table'>" + "".join(filter(None, [row('General Crew Call', cs_data.get('general_crew_call','')), row('Est. Wrap', cs_data.get('estimated_wrap_time','')), row('Weather', cs_data.get('weather','')), row('Sunrise', cs_data.get('sunrise','')), row('Sunset', cs_data.get('sunset','')), row('Courtesy Breakfast', cs_data.get('courtesy_breakfast_time','')), row('First Meal', cs_data.get('first_meal_time','')), row('Second Meal', cs_data.get('second_meal_time',''))])) + "</table>" if any([cs_data.get('general_crew_call'), cs_data.get('estimated_wrap_time'), cs_data.get('first_meal_time'), cs_data.get('second_meal_time')]) else ''}

{'<h2>Locations</h2>' + loc_html if loc_html else ''}

{'<h2>Key Contacts</h2><table><tr><td class="lbl">Role</td><td>Name</td><td>Phone</td></tr>' + kp_html + '</table>' if kp_html else ''}

{'<h2>Crew Call Times</h2><table>' + crew_html + '</table>' if crew_html else ''}

{'<h2>Department Notes</h2>' + dnotes_html if dnotes_html else ''}

{'<p>' + esc(cs_data.get('additional_notes','')) + '</p>' if cs_data.get('additional_notes') else ''}

<div class="footer">Sent by Framework Productions · contact@thefp.tv</div>
</div></body></html>"""


# ── Auth ──────────────────────────────────────────────────────────────────────

from functools import wraps
import secrets

login_manager = LoginManager(app)
login_manager.login_view = "login"

@login_manager.user_loader
def load_user(uid):
    return User.query.get(int(uid))


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated or current_user.role not in ('super_admin', 'admin'):
            abort(403)
        return f(*args, **kwargs)
    return decorated


def super_admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated or current_user.role != 'super_admin':
            abort(403)
        return f(*args, **kwargs)
    return decorated


_FORCE_PW_ALLOWED = {"profile", "logout", "login", "static",
                     "reset_password", "forgot_password",
                     "callsheet_view_public", "callsheet_confirm_public",
                     "docs_dashboard", "docs_project", "docs_upload_post",
                     "docs_upload_status"}

@app.before_request
def enforce_password_change():
    """Redirect users who must change their password to the profile page."""
    if (current_user.is_authenticated
            and getattr(current_user, 'must_change_password', False)
            and request.endpoint not in _FORCE_PW_ALLOWED):
        flash("Please set a new password before continuing.", "warning")
        return redirect(url_for("profile"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("dashboard"))
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        user = User.query.filter_by(email=email).first()
        if user and user.is_active and user.check_password(password):
            login_user(user)
            if user.must_change_password:
                flash("Please set a new password to continue.", "warning")
                return redirect(url_for("profile"))
            if user.role == 'docs_only':
                return redirect(url_for("docs_dashboard"))
            return redirect(url_for("dashboard"))
        flash("Invalid credentials.", "error")
    return render_template("login.html")


@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))


@app.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    if current_user.is_authenticated:
        return redirect(url_for("dashboard"))
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        user = User.query.filter_by(email=email).first()
        # Always show the same message to prevent email enumeration
        if user and user.is_active:
            token = secrets.token_urlsafe(32)
            user.reset_token = token
            user.reset_token_expires = datetime.utcnow() + timedelta(hours=2)
            db.session.commit()
            reset_url = f"https://fp-budget.onrender.com/reset-password/{token}"
            sent = _send_email(
                user.email,
                "Reset your FPBudget password",
                f"""Hi {user.name or user.email},

A password reset was requested for your FPBudget account.

Click the link below to set a new password (valid for 2 hours):
{reset_url}

If you didn't request this, ignore this email — your password won't change.

— Framework Productions
"""
            )
            if not sent:
                # Mail not configured — show link directly (dev/fallback)
                flash(f"Email not configured. Reset link: {reset_url}", "warning")
                return redirect(url_for("forgot_password"))
        flash("If that email is in our system, a reset link has been sent.", "success")
        return redirect(url_for("login"))
    return render_template("forgot_password.html")


@app.route("/reset-password/<token>", methods=["GET", "POST"])
def reset_password(token):
    if current_user.is_authenticated:
        return redirect(url_for("dashboard"))
    user = User.query.filter_by(reset_token=token).first()
    if not user or not user.reset_token_expires or user.reset_token_expires < datetime.utcnow():
        flash("This reset link is invalid or has expired. Please request a new one.", "error")
        return redirect(url_for("forgot_password"))
    if request.method == "POST":
        pw = request.form.get("new_password", "")
        confirm = request.form.get("confirm_password", "")
        if len(pw) < 8:
            flash("Password must be at least 8 characters.", "error")
            return redirect(url_for("reset_password", token=token))
        if pw != confirm:
            flash("Passwords do not match.", "error")
            return redirect(url_for("reset_password", token=token))
        user.set_password(pw)
        user.reset_token = None
        user.reset_token_expires = None
        user.must_change_password = False
        db.session.commit()
        flash("Password updated. You can now log in.", "success")
        return redirect(url_for("login"))
    return render_template("reset_password.html", token=token)


@app.route("/health")
def health():
    return "ok"


# ── Realtime collaboration: in-memory presence store ─────────────────────────
# { bid: { user_id: {"name": str, "seen": datetime} } }
_presence = {}
_PRESENCE_TTL = 45   # seconds before a viewer is considered gone
_budget_last_editor = {}  # { bid: {"name": str, "at": datetime} }

def _presence_cleanup(bid):
    cutoff = datetime.utcnow() - timedelta(seconds=_PRESENCE_TTL)
    _presence.setdefault(bid, {})
    _presence[bid] = {uid: v for uid, v in _presence[bid].items() if v["seen"] > cutoff}

@app.route("/projects/<int:pid>/budget/<int:bid>/presence", methods=["POST"])
@login_required
def budget_presence(pid, bid):
    """Ping to register presence; returns list of current viewers."""
    Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    _presence.setdefault(bid, {})
    _presence[bid][current_user.id] = {
        "name": current_user.name or current_user.email.split("@")[0],
        "seen": datetime.utcnow(),
    }
    _presence_cleanup(bid)
    viewers = [
        {"id": uid, "name": v["name"]}
        for uid, v in _presence[bid].items()
        if uid != current_user.id
    ]
    editor = _budget_last_editor.get(bid)
    return jsonify({
        "viewers": viewers,
        "last_edit": {
            "name": editor["name"],
            "at": editor["at"].isoformat(),
        } if editor else None,
    })

@app.route("/projects/<int:pid>/budget/<int:bid>/poll")
@login_required
def budget_poll(pid, bid):
    """Return budget updated_at so clients can detect remote changes."""
    b = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    editor = _budget_last_editor.get(bid)
    return jsonify({
        "updated_at": b.updated_at.isoformat() if b.updated_at else None,
        "last_edit": {
            "name": editor["name"],
            "at": editor["at"].isoformat(),
        } if editor else None,
    })

@app.route("/projects/<int:pid>/budget/<int:bid>/live")
@login_required
def budget_live(pid, bid):
    """Return per-line calc results for silent real-time patching."""
    b = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    lines = BudgetLine.query.filter_by(budget_id=bid).order_by(BudgetLine.account_code, BudgetLine.sort_order).all()
    fringe_cfgs = get_fringe_configs(db.session)
    profile = b.payroll_profile
    pw_start = b.payroll_week_start if b.payroll_week_start is not None else (
        profile.payroll_week_start if profile else 6)
    sched_mode = 'working' if b.budget_mode in ('working', 'actual') else 'estimated'

    line_results = {}
    for ln in lines:
        if ln.use_schedule:
            sched = ScheduleDay.query.filter_by(budget_line_id=ln.id, schedule_mode=sched_mode).all()
            res = calc_line_from_schedule(ln, sched, fringe_cfgs, profile, pw_start)
        else:
            res = calc_line(ln, fringe_cfgs)
        line_results[str(ln.id)] = {
            "subtotal":     res["subtotal"],
            "est_total":    res["est_total"],
            "agent_amount": res.get("agent_amount", 0.0),
        }

    editor = _budget_last_editor.get(bid)
    return jsonify({
        "updated_at": b.updated_at.isoformat() if b.updated_at else None,
        "line_ids":   [ln.id for ln in lines],
        "lines":      line_results,
        "last_edit":  {"name": editor["name"], "at": editor["at"].isoformat()} if editor else None,
    })

# ── Dashboard ─────────────────────────────────────────────────────────────────

@app.route("/")
@login_required
def dashboard():
    if current_user.role in ('super_admin', 'admin'):
        projects = ProjectSheet.query.order_by(ProjectSheet.name).all()
    else:
        accessible_ids = [
            pa.project_id for pa in
            ProjectAccess.query.filter_by(user_id=current_user.id).all()
        ]
        projects = ProjectSheet.query.filter(
            ProjectSheet.id.in_(accessible_ids)
        ).order_by(ProjectSheet.name).all()
    budget_counts = {}
    for b in Budget.query.all():
        budget_counts[b.project_id] = budget_counts.get(b.project_id, 0) + 1
    all_templates = BudgetTemplate.query.order_by(BudgetTemplate.name).all()
    return render_template("dashboard.html", projects=projects, budget_counts=budget_counts,
                           all_templates=all_templates)


@app.route("/projects/new", methods=["POST"])
@login_required
def project_new():
    name = request.form.get("name", "").strip()
    if not name:
        flash("Project name required.", "error")
        return redirect(url_for("dashboard"))
    existing = ProjectSheet.query.filter_by(name=name).first()
    if existing:
        flash(f"Project '{name}' already exists.", "error")
        return redirect(url_for("dashboard"))
    template_id = request.form.get("template_id", type=int)
    client_name = request.form.get("client_name", "").strip() or None
    p = ProjectSheet(name=name, client_name=client_name)
    db.session.add(p)
    db.session.flush()
    # Generate unique Dropbox folder slug and provision the folder tree
    slug = _unique_project_slug(name, client_name, exclude_id=p.id)
    p.dropbox_folder = slug
    _provision_dropbox_folder(slug)
    # Auto-create a default budget
    _fed40 = PayrollProfile.query.filter(PayrollProfile.name.ilike('%federal%')).first()
    b = Budget(project_id=p.id, name=f"{name} Budget", payroll_profile_id=_fed40.id if _fed40 else None, payroll_week_start=6,)
    db.session.add(b)
    db.session.flush()
    # Apply template lines if one was selected
    if template_id:
        tmpl = BudgetTemplate.query.get(template_id)
        if tmpl:
            for i, tl in enumerate(sorted(tmpl.lines, key=lambda x: x.sort_order)):
                db.session.add(BudgetLine(
                    budget_id=b.id,
                    account_code=tl.account_code,
                    account_name=tl.account_name,
                    description=tl.description or "",
                    is_labor=tl.is_labor,
                    quantity=float(tl.quantity or 1),
                    days=float(tl.days or 1),
                    rate=float(tl.rate or 0),
                    rate_type=tl.rate_type,
                    fringe_type=tl.fringe_type,
                    agent_pct=tl.agent_pct,
                    estimated_total=float(tl.estimated_total or 0),
                    sort_order=i,
                ))
    # Grant the creating user owner access
    access = ProjectAccess(project_id=p.id, user_id=current_user.id, role="owner")
    db.session.add(access)
    db.session.commit()
    flash(f"Project '{name}' created.", "success")
    # Redirect to Working Budget with settings=1 so settings modal auto-opens
    return redirect(url_for("budget_view", pid=p.id, bid=b.id) + "?tab=working&setup=1")


@app.route("/projects/<int:pid>/delete", methods=["POST"])
@login_required
def project_delete(pid):
    p = ProjectSheet.query.get_or_404(pid)
    # Cascade-delete each budget and its FK-constrained children
    for b in Budget.query.filter_by(project_id=pid).all():
        _delete_budget_cascade(b.id)
    # Clean up project-level FK tables not on ORM cascade
    from models import Location
    Location.query.filter_by(project_id=pid).delete(synchronize_session=False)
    ProjectAccess.query.filter_by(project_id=pid).delete(synchronize_session=False)
    ProjectUnion.query.filter_by(project_id=pid).delete(synchronize_session=False)
    ProjectClient.query.filter_by(project_id=pid).delete(synchronize_session=False)
    db.session.delete(p)
    db.session.commit()
    flash(f"Project '{p.name}' deleted.", "success")
    return redirect(url_for("dashboard"))


# ── Budget ────────────────────────────────────────────────────────────────────

@app.route("/projects/<int:pid>/budget")
@login_required
def project_budget_redirect(pid):
    """Redirect to newest budget or create page."""
    project = ProjectSheet.query.get_or_404(pid)
    latest = Budget.query.filter_by(project_id=pid).order_by(Budget.created_at.desc()).first()
    if latest:
        return redirect(url_for("budget_view", pid=pid, bid=latest.id))
    all_templates = BudgetTemplate.query.order_by(BudgetTemplate.name).all()
    return render_template("budget_new.html", project=project, all_templates=all_templates)


def _next_version_name(project_id, project_name):
    """Return a unique auto-incremented version name for this project.

    Rules:
    - Version number is based on the highest vN among non-archived budgets (so
      deleted versions leave a gap that the next creation fills).
    - If the generated name already exists (e.g. an archived v2 is still on record),
      append a letter suffix: v2b, v2c, … to keep every name unique.
    """
    import re
    existing = Budget.query.filter_by(project_id=project_id).all()
    if not existing:
        return f"{project_name} Budget"

    all_names   = {b.name for b in existing}                     # every name, including archived
    live_names  = [b.name for b in existing if b.version_status != 'archived']

    max_v = 1
    base_name = None
    for n in live_names:
        m = re.search(r'^(.+?)\s+v(\d+)(?:[a-z])?$', n, re.IGNORECASE)
        if m:
            v = int(m.group(2))
            if v > max_v:
                max_v = v
                base_name = m.group(1)
        else:
            if base_name is None:
                base_name = n
    if base_name is None:
        # fall back: strip any trailing suffix from all_names
        for n in sorted(all_names):
            mm = re.search(r'^(.+?)\s+v\d+', n)
            base_name = mm.group(1) if mm else n
            break

    candidate = f"{base_name} v{max_v + 1}"

    # If that name is already taken (e.g. by an archived record), add b/c/d… suffix
    if candidate in all_names:
        for letter in 'bcdefghijklmnopqrstuvwxyz':
            alt = f"{base_name} v{max_v + 1}{letter}"
            if alt not in all_names:
                candidate = alt
                break

    return candidate


def _create_budget_from_source(pid, source, new_name, new_mode, parent_bid=None):
    """Create a new Budget record copied from source and return it (not yet committed)."""
    _fed40 = PayrollProfile.query.filter(PayrollProfile.name.ilike('%federal%')).first()
    b = Budget(
        project_id=pid,
        name=new_name,
        budget_mode=new_mode,
        company_fee_pct=source.company_fee_pct if source else 0.18,
        company_fee_dispersed=source.company_fee_dispersed if source else False,
        start_date=source.start_date if source else None,
        end_date=source.end_date if source else None,
        target_budget=source.target_budget if source else None,
        notes=source.notes if source else None,
        payroll_profile_id=_fed40.id if _fed40 else None,
        payroll_week_start=6,
        version_status='current',
        parent_budget_id=parent_bid,
        updated_at=datetime.utcnow(),
        workers_comp_pct=source.workers_comp_pct if source else 0.03,
        payroll_fee_pct=source.payroll_fee_pct if source else 0.0175,
    )
    db.session.add(b)
    db.session.flush()
    if source:
        line_id_map = _copy_budget_lines(source.id, b.id)
        db.session.flush()
        _copy_schedule_days(source.id, b.id, line_id_map, dest_mode=new_mode)
        db.session.flush()
        # Stamp working_total on every line so the Estimated column has a frozen
        # snapshot from the moment this working budget was created.
        if new_mode in ('working', 'actual'):
            _wfringe = get_fringe_configs(db.session)
            _wprofile  = b.payroll_profile
            _wpw_start = b.payroll_week_start if b.payroll_week_start is not None else (
                _wprofile.payroll_week_start if _wprofile else 6)
            for _wln in BudgetLine.query.filter_by(budget_id=b.id).all():
                if _wln.use_schedule:
                    _wsched = ScheduleDay.query.filter_by(
                        budget_line_id=_wln.id, schedule_mode='working').all()
                    _wres = calc_line_from_schedule(_wln, _wsched, _wfringe, _wprofile, _wpw_start)
                else:
                    _wres = calc_line(_wln, _wfringe)
                _wln.working_total = _wres['est_total']
            b.working_initialized_at = datetime.utcnow()
    else:
        db.session.flush()
    return b


@app.route("/projects/<int:pid>/budget/new", methods=["POST"])
@login_required
def budget_new(pid):
    """Create a new budget version copied from a chosen source version."""
    project    = ProjectSheet.query.get_or_404(pid)
    source_bid = request.form.get("source_bid", type=int)

    source = (Budget.query.filter_by(id=source_bid, project_id=pid).first()
              if source_bid else
              Budget.query.filter_by(project_id=pid).order_by(Budget.created_at.desc()).first())

    new_mode = source.budget_mode if source else "estimated"
    btype    = _budget_type(new_mode)

    _supersede_current(pid, btype)
    db.session.flush()

    new_name = _next_version_name(pid, project.name)
    b = _create_budget_from_source(pid, source, new_name, new_mode, parent_bid=source.id if source else None)
    db.session.commit()

    flash(f"Created {b.name} ({new_mode.capitalize()}) copied from {source.name if source else 'scratch'}.", "success")
    return redirect(url_for("budget_view", pid=pid, bid=b.id))


@app.route("/projects/<int:pid>/budget/<int:bid>/create-working", methods=["POST"])
@login_required
def create_working_from_estimated(pid, bid):
    """Create a new Working budget version from an existing Estimated budget."""
    project = ProjectSheet.query.get_or_404(pid)
    source  = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    try:
        _supersede_current(pid, 'working')
        db.session.flush()
        # Working budget keeps the same version name — version numbers only increment for new scope versions
        w_name = source.name
        w = _create_budget_from_source(pid, source, w_name, 'working', parent_bid=bid)
        # Stamp the source so has_working_budget and working_initialized_at stay consistent
        if not source.working_initialized_at:
            source.working_initialized_at = datetime.utcnow()
        db.session.commit()
        flash(f"Created Working budget: {w_name}", "success")
        return redirect(url_for("budget_view", pid=pid, bid=w.id))
    except Exception as e:
        db.session.rollback()
        logging.exception("create_working_from_estimated failed")
        flash(f"Could not create Working budget — database error: {e}", "error")
        return redirect(url_for("budget_view", pid=pid, bid=bid))


@app.route("/projects/<int:pid>/budget/<int:bid>/archive", methods=["POST"])
@login_required
def archive_budget(pid, bid):
    """Toggle a budget version between archived and superseded (previous)."""
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    if budget.version_status == 'current':
        return jsonify({"error": "Cannot archive the active version"}), 400
    budget.version_status = 'superseded' if budget.version_status == 'archived' else 'archived'
    db.session.commit()
    return jsonify({"ok": True, "version_status": budget.version_status})


@app.route("/projects/<int:pid>/budget/<int:bid>/set-active", methods=["POST"])
@login_required
def set_active_budget(pid, bid):
    """Promote a budget version to 'current', demoting the previous active of the same type."""
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    if budget.version_status != 'current':
        btype = _budget_type(budget.budget_mode)
        _supersede_current(pid, btype, exclude_id=bid)
        budget.version_status = 'current'
        db.session.commit()
    return jsonify({"ok": True, "redirect": url_for("budget_view", pid=pid, bid=bid)})


@app.route("/projects/<int:pid>/budget/<int:bid>/delete", methods=["POST"])
@login_required
def _delete_budget_cascade(bid):
    """Fully explicit cascade delete for a budget.
    Handles every FK-constrained child table in safe deletion order so
    Postgres never sees a dangling reference mid-transaction."""
    budget = Budget.query.get(bid)
    if not budget:
        return

    # Collect all budget_line IDs upfront
    line_ids = [r[0] for r in db.session.query(BudgetLine.id).filter_by(budget_id=bid).all()]

    if line_ids:
        # Null FK references to budget_lines from other tables before deleting lines
        BudgetLine.query.filter(BudgetLine.parent_line_id.in_(line_ids)).update(
            {"parent_line_id": None}, synchronize_session=False)
        Location.query.filter(Location.budget_line_id.in_(line_ids)).update(
            {"budget_line_id": None}, synchronize_session=False)
        # Delete children of budget_lines
        ScheduleDay.query.filter(ScheduleDay.budget_line_id.in_(line_ids)).delete(synchronize_session=False)
        CrewAssignment.query.filter(CrewAssignment.budget_line_id.in_(line_ids)).delete(synchronize_session=False)

    # Delete budget-level tables in safe order
    ScheduleDay.query.filter_by(budget_id=bid).delete(synchronize_session=False)
    TaxCredit.query.filter_by(budget_id=bid).delete(synchronize_session=False)
    send_ids = [r[0] for r in db.session.query(CallSheetSend.id).filter_by(budget_id=bid).all()]
    if send_ids:
        CallSheetRecipient.query.filter(CallSheetRecipient.send_id.in_(send_ids)).delete(synchronize_session=False)
    CallSheetSend.query.filter_by(budget_id=bid).delete(synchronize_session=False)
    ProductionDay.query.filter_by(budget_id=bid).delete(synchronize_session=False)
    LocationDay.query.filter_by(budget_id=bid).delete(synchronize_session=False)
    CallSheetData.query.filter_by(budget_id=bid).delete(synchronize_session=False)
    BudgetDirectContact.query.filter_by(budget_id=bid).delete(synchronize_session=False)
    BudgetLine.query.filter_by(budget_id=bid).delete(synchronize_session=False)

    db.session.delete(budget)


def delete_budget(pid, bid):
    """Permanently delete a budget version. Not allowed for the active version."""
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    remaining = Budget.query.filter(
        Budget.project_id == pid, Budget.id != bid
    ).order_by(Budget.created_at.desc()).first()
    if budget.version_status == 'current' and remaining:
        # Auto-promote the next version so nothing is left without a current
        remaining.version_status = 'current'

    _delete_budget_cascade(bid)
    db.session.commit()
    if remaining:
        return jsonify({"ok": True, "redirect": url_for("budget_view", pid=pid, bid=remaining.id)})
    return jsonify({"ok": True, "redirect": url_for("dashboard")})


@app.route("/projects/<int:pid>/budget/<int:bid>")
@login_required
def budget_view(pid, bid):
    project  = ProjectSheet.query.get_or_404(pid)
    budget   = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    # Auto-promote: viewing a version makes it the active one
    if budget.version_status != 'current':
        _supersede_current(pid, _budget_type(budget.budget_mode), exclude_id=bid)
        budget.version_status = 'current'
        db.session.commit()
    all_budgets = Budget.query.filter_by(project_id=pid).order_by(Budget.created_at.desc()).all()

    lines = BudgetLine.query.filter_by(budget_id=bid).order_by(
        BudgetLine.account_code, BudgetLine.sort_order).all()

    fringe_cfgs = get_fringe_configs(db.session)
    payroll_profiles = PayrollProfile.query.order_by(PayrollProfile.sort_order).all()
    profile = budget.payroll_profile  # may be None
    pw_start = budget.payroll_week_start if budget.payroll_week_start is not None else (
        profile.payroll_week_start if profile else 6)

    # One-time auto-init: stamp working_total on working budgets created before the
    # frozen-snapshot feature existed (working_initialized_at == None).
    if budget.budget_mode in ('working', 'actual') and budget.working_initialized_at is None and lines:
        _init_sm = 'working'
        for _ln in lines:
            if _ln.use_schedule:
                _isched = ScheduleDay.query.filter_by(
                    budget_line_id=_ln.id, schedule_mode=_init_sm).all()
                _ires = calc_line_from_schedule(_ln, _isched, fringe_cfgs, profile, pw_start)
            else:
                _ires = calc_line(_ln, fringe_cfgs)
            _ln.working_total = _ires['est_total']
        budget.working_initialized_at = datetime.utcnow()
        db.session.commit()

    # Compute per-line totals
    line_results = {}
    sched_mode = 'working' if budget.budget_mode in ('working', 'actual') else 'estimated'
    for ln in lines:
        if ln.use_schedule:
            sched = ScheduleDay.query.filter_by(budget_line_id=ln.id, schedule_mode=sched_mode).all()
            line_results[ln.id] = calc_line_from_schedule(ln, sched, fringe_cfgs, profile, pw_start)
        else:
            line_results[ln.id] = calc_line(ln, fringe_cfgs)

    # Actuals from Transaction table
    actuals_raw = db.session.query(
        Transaction.account_code, func.sum(Transaction.amount)
    ).filter(
        Transaction.project_id == pid,
        Transaction.is_expense == True,
        Transaction.not_project_expense == False,
        Transaction.account_code != None,
    ).group_by(Transaction.account_code).all()
    actuals_by_code = {r[0]: float(r[1]) for r in actuals_raw}

    top_sheet = calc_top_sheet(budget, lines, fringe_cfgs, actuals_by_code, profile, pw_start)

    # Group lines by COA section for Working Budget tab
    sections = []
    seen = {}
    for ln in lines:
        sec_key = _section_for_code(ln.account_code)
        if sec_key not in seen:
            seen[sec_key] = {"code": sec_key, "name": _section_name(sec_key), "lines": []}
            sections.append(seen[sec_key])
        seen[sec_key]["lines"].append(ln)
    # Reorder each section so kit fee / child rows appear directly under their parent
    for sec in sections:
        sec["lines"] = _order_lines_with_children(sec["lines"])

    # Sub-group lookup for department headers in Production Staff (1000) and Talent (700) sections.
    # Falls back to description-based keyword match for lines without role_group set.
    line_sub_groups = {}
    for ln in lines:
        if ln.account_code == 1000:
            line_sub_groups[ln.id] = ln.role_group or _get_prod_staff_subgroup(ln.description)
        elif ln.account_code == 700:
            line_sub_groups[ln.id] = ln.role_group or _get_talent_subgroup(ln.description)

    # Dept head filtering: restrict to their assigned dept_code only
    dept_filter = None
    if current_user.role == 'dept_head' and current_user.dept_code:
        dept_filter = current_user.dept_code
        sections = [s for s in sections if s['code'] == dept_filter]

    fringes      = FringeConfig.query.filter_by(project_id=None).order_by(FringeConfig.fringe_type).all()
    crew_members = CrewMember.query.filter_by(active=True).order_by(CrewMember.name).all()
    all_templates = BudgetTemplate.query.order_by(BudgetTemplate.name).all()

    # Per-line actuals for Compare tab
    actuals_by_code_full = {}
    for r in db.session.query(
        Transaction.account_code,
        Transaction.account_code_name,
        func.sum(Transaction.amount).label("total"),
        func.count(Transaction.id).label("count")
    ).filter(
        Transaction.project_id == pid,
        Transaction.is_expense == True,
        Transaction.not_project_expense == False,
        Transaction.account_code != None,
    ).group_by(Transaction.account_code, Transaction.account_code_name).all():
        actuals_by_code_full[r[0]] = {
            "name":  r[1] or "",
            "total": float(r[2]),
            "count": r[3],
        }

    # Tax Credits
    tax_credits = TaxCredit.query.filter_by(budget_id=bid).order_by(TaxCredit.sort_order, TaxCredit.id).all()

    # Compute qualifying spend totals for tax credit calculation
    total_labor    = sum(line_results[ln.id]["est_total"] for ln in lines if ln.is_labor)
    total_nonlabor = sum(line_results[ln.id]["est_total"] for ln in lines if not ln.is_labor)
    total_all      = total_labor + total_nonlabor

    tax_credit_totals = {}
    for tc in tax_credits:
        if tc.applies_to == "labor":
            qualifying = total_labor
        elif tc.applies_to == "nonlabor":
            qualifying = total_nonlabor
        else:
            qualifying = total_all
        min_spend = float(tc.min_spend) if tc.min_spend else 0
        if qualifying < min_spend:
            credit = 0.0
        else:
            credit = float(qualifying) * float(tc.credit_rate)
        if tc.cap:
            credit = min(credit, float(tc.cap))
        tax_credit_totals[tc.id] = credit

    # Working budget: sum working_total per COA section (fall back to est_total if unset)
    working_by_section = {}
    working_gross_labor = 0.0
    for ln in lines:
        sec_key = _section_for_code(ln.account_code)
        wt = float(ln.working_total) if ln.working_total is not None else line_results[ln.id]['est_total']
        working_by_section[sec_key] = working_by_section.get(sec_key, 0.0) + wt
        if ln.is_labor:
            working_gross_labor += line_results[ln.id].get('subtotal', 0.0)
    # Inject auto-calculated fee amounts (Workers' Comp, Payroll Service Fee) —
    # these are not BudgetLines so they must be added separately, same as calc_top_sheet does.
    _wc_pct = float(getattr(budget, 'workers_comp_pct', 0) or 0)
    _pf_pct = float(getattr(budget, 'payroll_fee_pct',  0) or 0)
    if _wc_pct:
        working_by_section[14000] = working_by_section.get(14000, 0.0) + round(working_gross_labor * _wc_pct, 2)
    if _pf_pct:
        working_by_section[15000] = working_by_section.get(15000, 0.0) + round(working_gross_labor * _pf_pct, 2)

    # Manual actuals sum per section (from BudgetLine.manual_actual)
    manual_by_section = {}
    for ln in lines:
        if ln.manual_actual is not None:
            sec_key = _section_for_code(ln.account_code)
            manual_by_section[sec_key] = manual_by_section.get(sec_key, 0.0) + float(ln.manual_actual)

    # Locations for this project (project-specific only)
    project_locations = Location.query.filter_by(
        project_id=pid, active=True
    ).order_by(Location.name).all()
    project_unions  = ProjectUnion.query.filter_by(project_id=pid).order_by(ProjectUnion.sort_order).all()
    project_clients = ProjectClient.query.filter_by(project_id=pid).order_by(ProjectClient.sort_order).all()
    direct_contacts = BudgetDirectContact.query.filter_by(budget_id=bid).order_by(BudgetDirectContact.sort_order).all()
    # Location days booked for this budget
    location_days = LocationDay.query.filter_by(budget_id=bid).all()
    loc_day_map = {}
    for ld in location_days:
        loc_day_map[(ld.location_id, ld.date.isoformat())] = ld

    # Build omit maps for contact sheet
    ca_ids = [ca.id for ln in lines for ca in ln.crew_assignments if ln.is_labor]
    ca_omit_map = {}
    if ca_ids:
        for ca in CrewAssignment.query.filter(CrewAssignment.id.in_(ca_ids)).all():
            ca_omit_map[ca.id] = json.loads(ca.omit_flags) if ca.omit_flags else {}
    loc_omit_map = {}
    for loc in project_locations:
        loc_omit_map[loc.id] = json.loads(loc.omit_flags) if loc.omit_flags else {}

    # Version management metadata
    has_working_budget = any(_budget_type(b.budget_mode) == 'working' and
                             b.version_status != 'archived' for b in all_budgets)
    current_working_bid = next(
        (b.id for b in all_budgets if _budget_type(b.budget_mode) == 'working' and b.version_status == 'current'),
        None
    ) or next(
        (b.id for b in all_budgets if _budget_type(b.budget_mode) == 'working' and b.version_status != 'archived'),
        None
    )
    current_estimated_bid = next(
        (b.id for b in all_budgets if _budget_type(b.budget_mode) == 'estimated' and b.version_status == 'current'),
        None
    ) or next(
        (b.id for b in all_budgets if _budget_type(b.budget_mode) == 'estimated' and b.version_status != 'archived'),
        None
    )
    # Build parent name lookup
    parent_names = {}
    for b in all_budgets:
        if b.parent_budget_id:
            parent = next((x for x in all_budgets if x.id == b.parent_budget_id), None)
            if parent:
                parent_names[b.id] = parent.name

    # Cross-reference: when viewing the Estimated budget, show working budget line totals
    # in the Working column. Lines matched by (account_code, sort_order).
    working_line_totals = {}
    if _budget_type(budget.budget_mode) == 'estimated' and current_working_bid:
        _wb = next((b for b in all_budgets if b.id == current_working_bid), None)
        _wblines = BudgetLine.query.filter_by(budget_id=current_working_bid).all()
        _wb_fringe = get_fringe_configs(db.session)
        _wb_profile  = _wb.payroll_profile if _wb else None
        _wb_pw_start = (_wb.payroll_week_start if (_wb and _wb.payroll_week_start is not None)
                        else (_wb_profile.payroll_week_start if _wb_profile else 6))
        for _wln in _wblines:
            try:
                if _wln.use_schedule:
                    _wb_sm = 'working' if (_wb and _wb.budget_mode in ('working', 'actual')) else 'estimated'
                    _wb_sched = ScheduleDay.query.filter_by(budget_line_id=_wln.id, schedule_mode=_wb_sm).all()
                    _wres = calc_line_from_schedule(_wln, _wb_sched, _wb_fringe, _wb_profile, _wb_pw_start)
                else:
                    _wres = calc_line(_wln, _wb_fringe)
                working_line_totals[(_wln.account_code, _wln.sort_order)] = _wres['est_total']
            except Exception:
                pass  # skip any line that fails to calc; column shows — for that line

    company_settings = CompanySettings.query.get(1) or CompanySettings()
    return render_template("budget.html",
        project=project,
        budget=budget,
        all_budgets=all_budgets,
        lines=lines,
        line_results=line_results,
        sections=sections,
        top_sheet=top_sheet,
        fringes=fringes,
        crew_members=crew_members,
        actuals_by_code=actuals_by_code_full,
        coa_sections=FP_COA_SECTIONS,
        all_templates=all_templates,
        tax_credits=tax_credits,
        tax_credit_totals=tax_credit_totals,
        payroll_profiles=payroll_profiles,
        working_by_section=working_by_section,
        working_line_totals=working_line_totals,
        manual_by_section=manual_by_section,
        project_locations=project_locations,
        loc_day_map=loc_day_map,
        ca_omit_map=ca_omit_map,
        loc_omit_map=loc_omit_map,
        has_working_budget=has_working_budget,
        current_working_bid=current_working_bid,
        current_estimated_bid=current_estimated_bid,
        parent_names=parent_names,
        project_unions=project_unions,
        project_clients=project_clients,
        direct_contacts=direct_contacts,
        company_settings=company_settings,
        dept_filter=dept_filter,
        line_sub_groups=line_sub_groups,
    )


@app.route("/projects/<int:pid>/budget/<int:bid>/line", methods=["POST"])
@login_required
def upsert_line(pid, bid):
    """AJAX: create or update a budget line. Returns updated line totals."""
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    data   = request.get_json(force=True)
    lid    = data.get("id")

    # Estimated edit protection: require explicit override when a Working budget exists
    if _budget_type(budget.budget_mode) == 'estimated' and not data.get('override_estimated'):
        has_working = Budget.query.filter(
            Budget.project_id == pid,
            Budget.version_status != 'archived',
        ).filter(
            Budget.budget_mode.in_(('working', 'actual'))
        ).first()
        if has_working:
            return jsonify({
                "estimated_protected": True,
                "message": "A Working budget exists. Editing Estimated will not affect Working. Confirm to proceed."
            }), 409

    if lid:
        ln = BudgetLine.query.filter_by(id=lid, budget_id=bid).first_or_404()
    else:
        ln = BudgetLine(budget_id=bid)
        db.session.add(ln)

    # Quantity-reduce conflict check: when reducing quantity on a scheduled labor line,
    # the caller must confirm which schedule instances to remove first.
    if lid and "quantity" in data and ln.is_labor and ln.use_schedule and not data.get('skip_qty_check'):
        new_qty = int(float(data["quantity"] or 1))
        old_qty = int(float(ln.quantity or 1))
        if new_qty < old_qty:
            _sm = 'working' if budget.budget_mode in ('working', 'actual') else 'estimated'
            _instances = db.session.query(ScheduleDay.crew_instance).filter_by(
                budget_line_id=ln.id, schedule_mode=_sm
            ).distinct().all()
            existing_instances = sorted(set(r[0] or 1 for r in _instances))
            if len(existing_instances) > new_qty:
                # Build instance info from crew assignments
                _cas = {ca.instance or 1: ca for ca in ln.crew_assignments}
                instance_info = []
                for inst in existing_instances:
                    ca = _cas.get(inst)
                    name = None
                    if ca:
                        if ca.crew_member:
                            name = ca.crew_member.name
                        elif ca.name_override:
                            name = ca.name_override
                    day_count = ScheduleDay.query.filter_by(
                        budget_line_id=ln.id, crew_instance=inst, schedule_mode=_sm
                    ).count()
                    instance_info.append({
                        "instance": inst,
                        "name": name or f"Instance {inst}",
                        "days": day_count,
                    })
                return jsonify({
                    "schedule_conflict": True,
                    "line_id": ln.id,
                    "old_qty": old_qty,
                    "new_qty": new_qty,
                    "instances": instance_info,
                    "remove_count": old_qty - new_qty,
                    "message": f"This line has {len(existing_instances)} schedule instances. Reducing to {new_qty} requires removing {old_qty - new_qty}. Select which to remove."
                }), 409

    # Allowed fields
    fields = ["account_code", "account_name", "description", "is_labor", "sort_order",
              "estimated_total", "payroll_co", "quantity", "days", "rate", "rate_type",
              "est_ot", "fringe_type", "agent_pct", "note", "use_schedule",
              "parent_line_id", "line_tag", "role_group", "unit_rate",
              "days_unit", "days_per_week",
              "working_total", "manual_actual"]
    for f in fields:
        if f in data:
            val = data[f]
            if val == "" or val is None:
                setattr(ln, f, None if f not in ("account_code", "sort_order") else 0)
            else:
                setattr(ln, f, val)

    # Auto-compute estimated_total for non-labor lines (rate × qty × days, less discount)
    if not ln.is_labor:
        r        = float(ln.rate or 0)
        q        = float(ln.quantity or 1)
        d        = float(ln.days or 1)
        discount = float(ln.agent_pct or 0)   # stored as fraction (0.15 = 15%)
        if r > 0:
            pre_discount = round(r * q * d, 2)
            ln.estimated_total = round(pre_discount * (1 - discount), 2)
        # If rate is 0 but we have a flat total and qty/days were just changed, back-derive unit rate
        elif float(ln.estimated_total or 0) > 0 and ("quantity" in data or "days" in data) and "rate" not in data:
            # Derive unit rate from existing total ÷ (new qty × new days)
            derived_rate = round(float(ln.estimated_total) / max(q * d, 1), 2)
            ln.rate = derived_rate
            ln.estimated_total = round(derived_rate * q * d * (1 - discount), 2)

    db.session.commit()
    _touch_budget(bid)
    db.session.commit()

    fringe_cfgs = get_fringe_configs(db.session)
    if ln.use_schedule:
        _sm = 'working' if budget.budget_mode in ('working', 'actual') else 'estimated'
        sched = ScheduleDay.query.filter_by(budget_line_id=ln.id, schedule_mode=_sm).all()
        result = calc_line_from_schedule(ln, sched, fringe_cfgs)
    else:
        result = calc_line(ln, fringe_cfgs)

    return jsonify({"id": ln.id, **result})


@app.route("/projects/<int:pid>/budget/<int:bid>/line/<int:lid>/kit-fee", methods=["POST"])
@login_required
def add_kit_fee(pid, bid, lid):
    """Add a kit fee child row to an existing labor line."""
    Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    parent = BudgetLine.query.filter_by(id=lid, budget_id=bid).first_or_404()
    data   = request.get_json(force=True) or {}

    desc      = data.get("description") or f"Kit Fee — {parent.description or parent.account_name}"
    rate      = _bc_float(data.get("rate", 0))
    qty       = _bc_float(data.get("quantity", 1), 1)
    days_val  = _bc_float(data.get("days", 1), 1)
    estimated = rate * qty * days_val if rate > 0 else 0

    ln = BudgetLine(
        budget_id      = bid,
        account_code   = parent.account_code,
        account_name   = parent.account_name,
        description    = desc,
        is_labor       = False,
        line_tag       = "kit_fee",
        parent_line_id = parent.id,
        sort_order     = parent.sort_order,
        quantity       = qty,
        days           = days_val,
        rate           = rate,
        unit_rate      = rate if rate > 0 else None,
        estimated_total= estimated,
    )
    db.session.add(ln)
    db.session.commit()
    return jsonify({"ok": True, "id": ln.id, "description": ln.description,
                    "estimated_total": float(ln.estimated_total or 0),
                    "account_code": ln.account_code})


@app.route("/projects/<int:pid>/budget/<int:bid>/line/insert", methods=["POST"])
@login_required
def line_insert(pid, bid):
    """Insert a blank line above or below a reference line within the same section."""
    Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    data     = request.get_json(force=True) or {}
    ref_id   = int(data.get("reference_id") or 0)
    position = data.get("position", "below")   # "above" | "below"

    ref = BudgetLine.query.filter_by(id=ref_id, budget_id=bid).first_or_404()

    # All lines in the same section, ordered by sort_order then id (stable tiebreak)
    section_lines = BudgetLine.query.filter_by(
        budget_id=bid, account_code=ref.account_code
    ).order_by(BudgetLine.sort_order, BudgetLine.id).all()

    ref_idx = next((i for i, ln in enumerate(section_lines) if ln.id == ref_id), 0)
    insert_idx = ref_idx if position == "above" else ref_idx + 1

    new_ln = BudgetLine(
        budget_id    = bid,
        account_code = ref.account_code,
        account_name = ref.account_name,
        description  = "",
        is_labor     = ref.is_labor,
        fringe_type  = ref.fringe_type if ref.is_labor else "N",
        quantity     = 1,
        days         = 1,
        rate         = 0,
        sort_order   = 0,
    )
    db.session.add(new_ln)
    db.session.flush()

    section_lines.insert(insert_idx, new_ln)
    for i, ln in enumerate(section_lines):
        ln.sort_order = i

    db.session.commit()
    _touch_budget(bid)
    db.session.commit()
    return jsonify({"ok": True, "id": new_ln.id})


@app.route("/projects/<int:pid>/budget/<int:bid>/line/reorder", methods=["POST"])
@login_required
def line_reorder(pid, bid):
    """Move a line to a new position within its section (same account_code)."""
    Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    data    = request.get_json(force=True) or {}
    line_id = int(data.get("line_id") or 0)
    after_id = data.get("after_id")   # None → first in section; int → move after this line

    ln = BudgetLine.query.filter_by(id=line_id, budget_id=bid).first_or_404()

    section_lines = BudgetLine.query.filter_by(
        budget_id=bid, account_code=ln.account_code
    ).filter(BudgetLine.id != line_id).order_by(BudgetLine.sort_order, BudgetLine.id).all()

    if after_id is None:
        section_lines.insert(0, ln)
    else:
        after_id = int(after_id)
        idx = next((i for i, sl in enumerate(section_lines) if sl.id == after_id),
                   len(section_lines) - 1)
        section_lines.insert(idx + 1, ln)

    for i, sl in enumerate(section_lines):
        sl.sort_order = i

    db.session.commit()
    _touch_budget(bid)
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/projects/<int:pid>/budget/<int:bid>/line/<int:lid>/remove-instance", methods=["POST"])
@login_required
def remove_schedule_instance(pid, bid, lid):
    """Remove all schedule days (and crew assignment) for a specific crew_instance on a line.
    Used when reducing quantity on a scheduled labor line."""
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    ln = BudgetLine.query.filter_by(id=lid, budget_id=bid).first_or_404()
    data = request.get_json(force=True) or {}
    instance = int(data.get("instance", 1))
    _sm = 'working' if budget.budget_mode in ('working', 'actual') else 'estimated'

    # Remove schedule days for this instance
    ScheduleDay.query.filter_by(
        budget_line_id=lid, crew_instance=instance, schedule_mode=_sm
    ).delete()

    # Remove crew assignment for this instance
    ca = next((c for c in ln.crew_assignments if (c.instance or 1) == instance), None)
    if ca:
        db.session.delete(ca)

    # Renumber remaining instances to fill the gap (keep them contiguous)
    remaining_sds = ScheduleDay.query.filter_by(
        budget_line_id=lid, schedule_mode=_sm
    ).filter(ScheduleDay.crew_instance > instance).all()
    for sd in remaining_sds:
        sd.crew_instance = (sd.crew_instance or 1) - 1

    remaining_cas = [c for c in ln.crew_assignments if (c.instance or 1) > instance]
    for c in remaining_cas:
        c.instance = (c.instance or 1) - 1

    db.session.commit()
    _touch_budget(bid)
    db.session.commit()
    return jsonify({"ok": True, "removed_instance": instance})


@app.route("/projects/<int:pid>/budget/<int:bid>/working/init", methods=["POST"])
@login_required
def init_working_budget(pid, bid):
    """
    Re-sync working_total on every BudgetLine to match the current estimated total.
    Used to reset the Working budget's line values back to the Estimated baseline.
    Idempotent and safe to call at any time.

    Note: Creating a Working budget version (separate Budget object) is done via
    create_working_from_estimated. This route only refreshes the working_total
    column on existing lines — it does not create new Budget objects.
    """
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    lines  = BudgetLine.query.filter_by(budget_id=bid).all()
    fringe_cfgs = get_fringe_configs(db.session)
    profile  = budget.payroll_profile
    pw_start = budget.payroll_week_start if budget.payroll_week_start is not None else (
        profile.payroll_week_start if profile else 6)
    _sched_mode = 'working' if budget.budget_mode in ('working', 'actual') else 'estimated'
    for ln in lines:
        if ln.use_schedule:
            sched = ScheduleDay.query.filter_by(budget_line_id=ln.id, schedule_mode=_sched_mode).all()
            res = calc_line_from_schedule(ln, sched, fringe_cfgs, profile, pw_start)
        else:
            res = calc_line(ln, fringe_cfgs)
        ln.working_total = res['est_total']

    if not budget.working_initialized_at:
        budget.working_initialized_at = datetime.utcnow()
    db.session.commit()
    return jsonify({"ok": True, "initialized_at": budget.working_initialized_at.isoformat()})


@app.route("/projects/<int:pid>/budget/<int:bid>/line/<int:lid>", methods=["DELETE"])
@login_required
def delete_line(pid, bid, lid):
    ln = BudgetLine.query.filter_by(id=lid, budget_id=bid).first_or_404()
    db.session.delete(ln)
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/projects/<int:pid>/budget/<int:bid>/mode", methods=["POST"])
@login_required
def set_budget_mode(pid, bid):
    """Change display mode on a budget.

    Allowed only for within-type changes (estimated ↔ hybrid,
    or working-family ↔ actual). Cross-type changes (estimated ↔ working) must go
    through create_working_from_estimated, which creates a separate Budget object.
    This prevents budget_mode from being flipped in a way that corrupts the
    current_estimated_bid / current_working_bid resolution logic.
    """
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    mode = request.form.get("mode", "estimated")
    allowed = ("estimated", "working", "actual", "schedule", "hybrid")
    if mode in allowed:
        # Guard: refuse cross-type changes
        if _budget_type(mode) != _budget_type(budget.budget_mode):
            flash("Switch between Estimated and Working using the mode buttons — not this route.", "error")
        else:
            budget.budget_mode = mode
            db.session.commit()
    return_to = request.form.get("return_to", "budget")
    if return_to == "gantt":
        return redirect(url_for("gantt_view", pid=pid, bid=bid))
    if return_to == "callsheet":
        return redirect(url_for("callsheet_view", pid=pid, bid=bid))
    tab = request.form.get("return_tab", "topsheet")
    return redirect(url_for("budget_view", pid=pid, bid=bid) + f"?tab={tab}")



@app.route("/projects/<int:pid>/budget/<int:bid>/export.csv")
@login_required
def export_csv(pid, bid):
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    mode   = request.args.get("type", "topsheet")   # topsheet | working
    lines  = BudgetLine.query.filter_by(budget_id=bid).order_by(
        BudgetLine.account_code, BudgetLine.sort_order).all()
    fringe_cfgs = get_fringe_configs(db.session)

    output = io.StringIO()
    w = csv.writer(output)

    if mode == "working":
        w.writerow(["code", "account", "description", "qty", "days", "rate",
                    "est_ot", "fringe", "subtotal", "agent_pct", "total"])
        for ln in lines:
            res = calc_line(ln, fringe_cfgs)
            w.writerow([ln.account_code, ln.account_name, ln.description or "",
                        float(ln.quantity or 0), float(ln.days or 0),
                        float(ln.rate or 0), float(ln.est_ot or 0),
                        ln.fringe_type, res["subtotal"],
                        float(ln.agent_pct or 0), res["total"]])
    else:
        actuals_raw = db.session.query(
            Transaction.account_code, func.sum(Transaction.amount)
        ).filter(Transaction.project_id == pid, Transaction.is_expense == True,
                 Transaction.not_project_expense == False,
                 Transaction.account_code != None
        ).group_by(Transaction.account_code).all()
        actuals_by_code = {r[0]: float(r[1]) for r in actuals_raw}
        ts = calc_top_sheet(budget, lines, fringe_cfgs, actuals_by_code)

        w.writerow(["code", "account", "estimated", "actual", "variance", "pct_used"])
        for row in ts["rows"]:
            pct = (row["actual"] / row["estimated"] * 100) if row["estimated"] else 0
            w.writerow([row["code"], row["account"],
                        row["estimated"], row["actual"], row["variance"], round(pct, 1)])
        if ts.get("workers_comp_amount", 0):
            w.writerow(["14000*", f"Workers' Comp ({ts['workers_comp_pct']*100:.2f}% of labor)",
                        ts["workers_comp_amount"], "", "", ""])
        if ts.get("payroll_fee_amount", 0):
            w.writerow(["15000*", f"Payroll Service Fee ({ts['payroll_fee_pct']*100:.2f}%)",
                        ts["payroll_fee_amount"], "", "", ""])
        if not ts.get("company_fee_dispersed"):
            w.writerow(["", "Company Fee", ts["company_fee"], "", "", ""])
        w.writerow(["", "GRAND TOTAL", ts["grand_total_estimated"],
                    ts["grand_total_actual"], ts["grand_variance"], ""])

    output.seek(0)
    fname = f"{budget.name.replace(' ', '_')}_{'working' if mode == 'working' else 'topsheet'}.csv"
    return Response(output.read(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={fname}"})


@app.route("/projects/<int:pid>/budget/<int:bid>/export.pdf")
@login_required
def export_pdf(pid, bid):
    budget  = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    project = ProjectSheet.query.get_or_404(pid)
    lines   = BudgetLine.query.filter_by(budget_id=bid).order_by(
        BudgetLine.account_code, BudgetLine.sort_order).all()
    fringe_cfgs = get_fringe_configs(db.session)
    profile  = budget.payroll_profile
    pw_start = budget.payroll_week_start if budget.payroll_week_start is not None else (
        profile.payroll_week_start if profile else 6)

    actuals_raw = db.session.query(
        Transaction.account_code, func.sum(Transaction.amount)
    ).filter(
        Transaction.project_id == pid,
        Transaction.is_expense == True,
        Transaction.not_project_expense == False,
        Transaction.account_code != None,
    ).group_by(Transaction.account_code).all()
    actuals_by_code = {r[0]: float(r[1]) for r in actuals_raw}

    top_sheet   = calc_top_sheet(budget, lines, fringe_cfgs, actuals_by_code, profile, pw_start)
    detail_mode = request.args.get("detail", "0") == "1"
    dispersed   = bool(budget.company_fee_dispersed)

    # Build section detail for full-detail export
    sched_mode = 'working' if budget.budget_mode in ('working', 'actual') else 'estimated'
    line_results = {}
    for ln in lines:
        if ln.use_schedule:
            sched = ScheduleDay.query.filter_by(budget_line_id=ln.id, schedule_mode=sched_mode).all()
            line_results[ln.id] = calc_line_from_schedule(ln, sched, fringe_cfgs, profile, pw_start)
        else:
            line_results[ln.id] = calc_line(ln, fringe_cfgs)

    # Group lines into sections
    from budget_calc import FP_COA_SECTIONS
    def _section_for_code(code):
        best = None
        for start, _ in FP_COA_SECTIONS:
            if code >= start:
                best = start
            else:
                break
        return best
    section_name_map = dict(FP_COA_SECTIONS)
    sections_detail = {}
    for ln in lines:
        sk = _section_for_code(ln.account_code)
        if sk not in sections_detail:
            sections_detail[sk] = {"code": sk, "name": section_name_map.get(sk, ""), "lines": []}
        sections_detail[sk]["lines"].append(ln)
    sections_ordered = [sections_detail[sk] for sk, _ in FP_COA_SECTIONS if sk in sections_detail]

    company_settings = CompanySettings.query.get(1) or CompanySettings()
    is_working_view  = budget.budget_mode in ('working', 'actual')

    fee_m = (1 + float(budget.company_fee_pct)) if dispersed else 1.0

    html_str = render_template("budget_pdf.html",
        project=project,
        budget=budget,
        top_sheet=top_sheet,
        detail_mode=detail_mode,
        dispersed=dispersed,
        company_settings=company_settings,
        is_working_view=is_working_view,
        sections_ordered=sections_ordered,
        line_results=line_results,
        fee_m=fee_m,
        today=date.today(),
    )

    pdf_bytes = WeasyprintHTML(string=html_str, base_url=request.host_url).write_pdf()
    mode_label = "Working" if is_working_view else "Estimated"
    detail_label = "_detail" if detail_mode else "_topsheet"
    fname = f"{project.name.replace(' ', '_')}_{budget.name.replace(' ', '_')}_{mode_label}{detail_label}.pdf"
    return Response(
        pdf_bytes,
        mimetype="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=\"{fname}\""}
    )


@app.route("/projects/<int:pid>/budget/<int:bid>/import", methods=["POST"])
@login_required
def import_csv(pid, bid):
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    f = request.files.get("file")
    if not f:
        flash("No file uploaded.", "error")
        return redirect(url_for("budget_view", pid=pid, bid=bid))

    reader = csv.DictReader(io.StringIO(f.read().decode("utf-8-sig")))
    added = 0
    for row in reader:
        try:
            ln = BudgetLine(
                budget_id=bid,
                account_code=int(row.get("account_code", 0)),
                account_name=row.get("account_name") or row.get("description", ""),
                description=row.get("description", ""),
                is_labor=str(row.get("is_labor", "")).lower() in ("true", "1", "yes"),
                quantity=float(row.get("quantity", 1) or 1),
                days=float(row.get("days", 1) or 1),
                rate=float(row.get("rate", 0) or 0),
                rate_type=row.get("rate_type", "day_10"),
                est_ot=float(row.get("est_ot", 0) or 0),
                fringe_type=row.get("fringe_type", "N"),
                agent_pct=float(row.get("agent_pct", 0) or 0),
                estimated_total=float(row.get("estimated_total", 0) or 0),
                note=row.get("note", ""),
            )
            db.session.add(ln)
            added += 1
        except Exception as e:
            logging.warning(f"CSV import row skip: {e}")
    db.session.commit()
    flash(f"Imported {added} lines.", "success")
    return redirect(url_for("budget_view", pid=pid, bid=bid))


@app.route("/projects/<int:pid>/budget/<int:bid>/from-template", methods=["POST"])
@login_required
def budget_from_template(pid, bid):
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    tid    = request.form.get("template_id", type=int)
    tmpl   = BudgetTemplate.query.get_or_404(tid)
    for i, tl in enumerate(sorted(tmpl.lines, key=lambda x: x.sort_order)):
        db.session.add(BudgetLine(
            budget_id=bid,
            account_code=tl.account_code,
            account_name=tl.account_name,
            description=tl.description or "",
            is_labor=tl.is_labor,
            quantity=float(tl.quantity or 1),
            days=float(tl.days or 1),
            rate=float(tl.rate or 0),
            rate_type=tl.rate_type,
            fringe_type=tl.fringe_type,
            agent_pct=tl.agent_pct,
            estimated_total=float(tl.estimated_total or 0),
            sort_order=i,
        ))
    db.session.commit()
    flash(f"Applied template '{tmpl.name}' — {len(tmpl.lines)} lines added.", "success")
    return redirect(url_for("budget_view", pid=pid, bid=bid))


@app.route("/projects/<int:pid>/budget/<int:bid>/save-as-template", methods=["POST"])
@login_required
def save_as_template(pid, bid):
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    name   = request.form.get("template_name", "").strip()
    if not name:
        flash("Template name required.", "error")
        return redirect(url_for("budget_view", pid=pid, bid=bid))

    existing = BudgetTemplate.query.filter_by(name=name).first()
    if existing:
        flash(f"Template '{name}' already exists.", "error")
        return redirect(url_for("budget_view", pid=pid, bid=bid))

    tmpl = BudgetTemplate(name=name, description=f"Saved from {budget.name}")
    db.session.add(tmpl)
    db.session.flush()

    lines = BudgetLine.query.filter_by(budget_id=bid).order_by(
        BudgetLine.account_code, BudgetLine.sort_order).all()
    for i, ln in enumerate(lines):
        if ln.line_tag == 'kit_fee':  # skip auto-managed kit fee rows
            continue
        db.session.add(BudgetTemplateLine(
            template_id=tmpl.id,
            account_code=ln.account_code,
            account_name=ln.account_name,
            description=ln.description,
            is_labor=ln.is_labor,
            quantity=float(ln.quantity or 1),
            days=float(ln.days or 1),
            rate=float(ln.rate or 0),
            rate_type=ln.rate_type,
            fringe_type=ln.fringe_type,
            agent_pct=ln.agent_pct,
            estimated_total=float(ln.estimated_total or 0),
            sort_order=i,
        ))
    db.session.commit()
    flash(f"Saved as template '{name}'.", "success")
    return redirect(url_for("budget_view", pid=pid, bid=bid))


# ── Project / Budget Settings ─────────────────────────────────────────────────

@app.route("/projects/<int:pid>/budget/<int:bid>/settings", methods=["POST"])
@login_required
def budget_settings(pid, bid):
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    data   = request.get_json(force=True)
    if "name" in data:
        budget.name = data["name"].strip() or budget.name
    if "company_fee_pct" in data:
        budget.company_fee_pct = float(data["company_fee_pct"]) / 100
    if "company_fee_dispersed" in data:
        budget.company_fee_dispersed = bool(data["company_fee_dispersed"])
    if "target_budget" in data:
        budget.target_budget = float(data["target_budget"]) if data["target_budget"] else None
    if "start_date" in data:
        try:
            budget.start_date = datetime.strptime(data["start_date"], "%Y-%m-%d").date() if data["start_date"] else None
        except ValueError:
            pass
    if "end_date" in data:
        try:
            budget.end_date = datetime.strptime(data["end_date"], "%Y-%m-%d").date() if data["end_date"] else None
        except ValueError:
            pass
    if "notes" in data:
        budget.notes = data["notes"] or None
    if "payroll_profile_id" in data:
        v = data.get("payroll_profile_id")
        budget.payroll_profile_id = int(v) if v else None
    if "payroll_week_start" in data:
        v = data.get("payroll_week_start")
        budget.payroll_week_start = int(v) if v is not None and v != '' else None
    if "timezone" in data:
        budget.timezone = data["timezone"] or "America/Los_Angeles"
    if "workers_comp_pct" in data:
        v = data.get("workers_comp_pct")
        budget.workers_comp_pct = float(v) / 100 if v is not None and v != '' else 0.03
    if "payroll_fee_pct" in data:
        v = data.get("payroll_fee_pct")
        budget.payroll_fee_pct = float(v) / 100 if v is not None and v != '' else 0.0175
    if "client_name" in data:
        budget.client_name = data["client_name"].strip() or None
    if "prepared_by" in data:
        budget.prepared_by = data["prepared_by"].strip() or None
    if "prepared_by_title" in data:
        budget.prepared_by_title = data["prepared_by_title"].strip() or None
    if "prepared_by_email" in data:
        budget.prepared_by_email = data["prepared_by_email"].strip() or None
    if "prepared_by_phone" in data:
        budget.prepared_by_phone = data["prepared_by_phone"].strip() or None
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/settings/company", methods=["GET"])
@login_required
def get_company_settings():
    cs = CompanySettings.query.get(1) or CompanySettings(id=1)
    return jsonify({
        "company_name":  cs.company_name  or "",
        "address_line1": cs.address_line1 or "",
        "address_line2": cs.address_line2 or "",
        "city":          cs.city          or "",
        "state":         cs.state         or "",
        "zip_code":      cs.zip_code      or "",
        "phone":         cs.phone         or "",
        "email":         cs.email         or "",
        "website":       cs.website       or "",
    })

@app.route("/settings/company", methods=["POST"])
@login_required
def save_company_settings():
    data = request.get_json(force=True)
    cs = CompanySettings.query.get(1)
    if not cs:
        cs = CompanySettings(id=1)
        db.session.add(cs)
    cs.company_name  = data.get("company_name",  "").strip() or None
    cs.address_line1 = data.get("address_line1", "").strip() or None
    cs.address_line2 = data.get("address_line2", "").strip() or None
    cs.city          = data.get("city",          "").strip() or None
    cs.state         = data.get("state",         "").strip() or None
    cs.zip_code      = data.get("zip_code",      "").strip() or None
    cs.phone         = data.get("phone",         "").strip() or None
    cs.email         = data.get("email",         "").strip() or None
    cs.website       = data.get("website",       "").strip() or None
    db.session.commit()
    return jsonify({"ok": True})


# ── Tax Credits ───────────────────────────────────────────────────────────────

@app.route("/projects/<int:pid>/budget/<int:bid>/tax-credit", methods=["POST"])
@login_required
def upsert_tax_credit(pid, bid):
    Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    data = request.get_json(force=True)
    tcid = data.get("id")
    if tcid:
        tc = TaxCredit.query.filter_by(id=tcid, budget_id=bid).first_or_404()
    else:
        tc = TaxCredit(budget_id=bid)
        db.session.add(tc)
    tc.name        = data.get("name", "").strip()
    tc.jurisdiction= data.get("jurisdiction", "") or None
    tc.credit_rate = float(data.get("credit_rate", 0) or 0)
    tc.applies_to  = data.get("applies_to", "all")
    tc.min_spend   = float(data.get("min_spend")) if data.get("min_spend") else None
    tc.cap         = float(data.get("cap")) if data.get("cap") else None
    tc.notes       = data.get("notes") or None
    tc.sort_order  = int(data.get("sort_order", 0) or 0)
    db.session.commit()
    return jsonify({"ok": True, "id": tc.id})


@app.route("/projects/<int:pid>/budget/<int:bid>/tax-credit/<int:tcid>", methods=["DELETE"])
@login_required
def delete_tax_credit(pid, bid, tcid):
    tc = TaxCredit.query.filter_by(id=tcid, budget_id=bid).first_or_404()
    db.session.delete(tc)
    db.session.commit()
    return jsonify({"ok": True})


# ── Gantt / Schedule ──────────────────────────────────────────────────────────

def _compute_gantt_section_totals(labor_lines, days, fringe_cfgs, profile, pw_start):
    """
    Compute ST/OT/DT cost totals per COA section for the live totals panel.
    Returns list of {code, name, st, ot, dt, total}.
    """
    _section_name_map = dict(FP_COA_SECTIONS)

    def section_for_code(code):
        best = None
        for start, _ in FP_COA_SECTIONS:
            if code >= start:
                best = start
            else:
                break
        return best

    # Group days by line_id
    days_by_line = {}
    for d in days:
        days_by_line.setdefault(d.budget_line_id, []).append(d)

    sec_totals = {}
    for ln in labor_lines:
        line_days = days_by_line.get(ln.id, [])
        qty = _bc_float(ln.quantity, 1.0)
        rate = _bc_float(ln.rate)

        cfg        = fringe_cfgs.get(ln.fringe_type) if fringe_cfgs else None
        ot_applies = getattr(cfg, 'ot_applies', True) if cfg else True

        if line_days and profile:
            st_b, ot_b, dt_b, _ = _run_payroll_calc(
                rate, ln.rate_type or 'day_10', qty, line_days, profile, pw_start,
                ot_applies=ot_applies
            )
        elif line_days:
            # Flat calc (no payroll profile)
            flat = sum(
                _bc_float(getattr(d, 'rate_multiplier', None), 1.0) if d.day_type == 'custom'
                else DAY_TYPE_MULTIPLIERS.get(d.day_type, 0.0)
                for d in line_days
            )
            st_b, ot_b, dt_b = rate * qty * flat, 0.0, 0.0
        else:
            continue

        sec = section_for_code(ln.account_code)
        if sec not in sec_totals:
            sec_totals[sec] = {"code": sec, "name": _section_name_map.get(sec, ""), "st": 0.0, "ot": 0.0, "dt": 0.0}
        sec_totals[sec]["st"] += st_b
        sec_totals[sec]["ot"] += ot_b
        sec_totals[sec]["dt"] += dt_b

    result = []
    for start, name in FP_COA_SECTIONS:
        if start in sec_totals:
            s = sec_totals[start]
            s["total"] = s["st"] + s["ot"] + s["dt"]
            s["st"]    = round(s["st"], 2)
            s["ot"]    = round(s["ot"], 2)
            s["dt"]    = round(s["dt"], 2)
            s["total"] = round(s["total"], 2)
            result.append(s)

    return result


@app.route("/projects/<int:pid>/budget/<int:bid>/gantt")
@login_required
def gantt_view(pid, bid):
    project = ProjectSheet.query.get_or_404(pid)
    budget  = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    # Auto-promote: viewing a version makes it the active one
    if budget.version_status != 'current':
        _supersede_current(pid, _budget_type(budget.budget_mode), exclude_id=bid)
        budget.version_status = 'current'
        db.session.commit()

    # All labor lines, ordered to match the Working Budget tab
    all_lines = BudgetLine.query.filter_by(budget_id=bid).order_by(
        BudgetLine.account_code, BudgetLine.sort_order).all()
    labor_lines = [ln for ln in all_lines if ln.is_labor]

    # Determine which schedule to show based on budget mode
    sched_mode = 'working' if budget.budget_mode in ('working', 'actual') else 'estimated'
    days = ScheduleDay.query.filter_by(
        budget_id=bid, schedule_mode=sched_mode
    ).order_by(ScheduleDay.date).all()
    profile   = budget.payroll_profile
    pw_start  = budget.payroll_week_start if budget.payroll_week_start is not None else (
        profile.payroll_week_start if profile else 6)

    # Build date range — URL params override, then DB data, then default week
    param_start = request.args.get("gantt_start")
    param_end   = request.args.get("gantt_end")
    try:
        start_date = datetime.strptime(param_start, "%Y-%m-%d").date() if param_start else None
        end_date   = datetime.strptime(param_end,   "%Y-%m-%d").date() if param_end   else None
    except ValueError:
        start_date = end_date = None

    if not start_date or not end_date:
        # Default: use budget.start_date if set, else current week
        if budget.start_date:
            default_start = budget.start_date
        else:
            today = date.today()
            days_to_sunday = (today.weekday() + 1) % 7
            default_start  = today - timedelta(days=days_to_sunday)
        default_end = default_start + timedelta(days=20)  # 3 weeks
        if not start_date:
            start_date = default_start
        if not end_date:
            end_date = default_end

    # Clamp to 28-day maximum window
    MAX_GANTT_DAYS = 28
    if (end_date - start_date).days >= MAX_GANTT_DAYS:
        end_date = start_date + timedelta(days=MAX_GANTT_DAYS - 1)

    date_range = []
    cur = start_date
    while cur <= end_date:
        date_range.append(cur)
        cur += timedelta(days=1)

    # Compute week boundary dates (first day of each payroll week visible)
    week_boundary_dates = set()
    for d in date_range:
        if d.weekday() == pw_start:
            week_boundary_dates.add(d.isoformat())

    # Load production days (meals) for this budget/schedule_mode
    prod_days = ProductionDay.query.filter_by(budget_id=bid, schedule_mode=sched_mode).all()
    meal_map = {pd.date.isoformat(): {
        "courtesy_breakfast": bool(pd.courtesy_breakfast),
        "first_meal":          bool(pd.first_meal),
        "second_meal":         bool(pd.second_meal),
    } for pd in prod_days}

    # Expand each labor line by its quantity into individual rows
    # Preserve the budget's sort order (account_code, sort_order) — no re-sort by label
    expanded_lines = []
    for ln in labor_lines:
        qty = int(ln.quantity or 1)
        base_label = ln.description or ln.account_name
        if ln.account_code == 1000:
            sub_group = ln.role_group or _get_prod_staff_subgroup(ln.description)
        elif ln.account_code == 700:
            sub_group = ln.role_group or _get_talent_subgroup(ln.description)
        else:
            sub_group = None
        sched_labels = json.loads(ln.schedule_labels) if ln.schedule_labels else {}
        if qty <= 1:
            custom = sched_labels.get("1") or sched_labels.get(1)
            expanded_lines.append({
                "id":           ln.id,
                "instance":     1,
                "label":        custom or base_label,
                "base_label":   base_label,
                "custom_label": custom or "",
                "account_code": ln.account_code,
                "account_name": ln.account_name,
                "sort_order":   ln.sort_order,
                "sub_group":    sub_group,
                "rate":         float(ln.rate or 0),
                "rate_type":    ln.rate_type or 'day_10',
                "use_schedule": ln.use_schedule,
                "fringe_type":  ln.fringe_type,
                "qty":          qty,
            })
        else:
            for n in range(1, qty + 1):
                custom = sched_labels.get(str(n)) or sched_labels.get(n)
                expanded_lines.append({
                    "id":           ln.id,
                    "instance":     n,
                    "label":        custom or f"{base_label} #{n}",
                    "base_label":   f"{base_label} #{n}",
                    "custom_label": custom or "",
                    "account_code": ln.account_code,
                    "account_name": ln.account_name,
                    "sort_order":   ln.sort_order,
                    "sub_group":    sub_group,
                    "rate":         float(ln.rate or 0),
                    "rate_type":    ln.rate_type or 'day_10',
                    "use_schedule": ln.use_schedule,
                    "fringe_type":  ln.fringe_type,
                    "qty":          qty,
                })

    # Build day lookup: (line_id, instance, date) → ScheduleDay
    day_map = {}
    for d in days:
        day_map[(d.budget_line_id, d.crew_instance or 1, d.date.isoformat())] = d

    # Load fringe configs once (needed for both ot_applies checks and section totals)
    fringe_cfgs = get_fringe_configs(db.session)

    # Build OT status map for cell highlighting: (line_id, instance, date_iso) → 'ot'|'dt'|None
    ot_status_map = {}
    if profile:
        # Group days by (line_id, instance)
        days_by_inst = {}
        for d in days:
            key = (d.budget_line_id, d.crew_instance or 1)
            days_by_inst.setdefault(key, []).append(d)

        for row in expanded_lines:
            key = (row['id'], row['instance'])
            inst_days = days_by_inst.get(key, [])
            if inst_days:
                cfg        = fringe_cfgs.get(row['fringe_type'])
                ot_applies = getattr(cfg, 'ot_applies', True) if cfg else True
                status = calc_days_ot_status(
                    row['rate_type'], inst_days, profile, pw_start,
                    ot_applies=ot_applies
                )
                for date_iso, s in status.items():
                    if s:  # only store non-None entries to keep dict small
                        ot_status_map[f"{row['id']}:{row['instance']}:{date_iso}"] = s

    # Compute initial section totals for the live totals panel
    gantt_section_totals = _compute_gantt_section_totals(
        labor_lines, days, fringe_cfgs, profile, pw_start
    )

    # Build per-row out-of-view overflow indicators
    overflow_before   = set()   # "line_id:instance" keys with days before start_date
    overflow_after    = set()   # "line_id:instance" keys with days after end_date
    overflow_earliest = {}      # key → earliest date before range (for click-to-jump)
    overflow_latest   = {}      # key → latest date after range (for click-to-jump)
    for d in days:
        if d.day_type == 'off':
            continue
        key = f"{d.budget_line_id}:{d.crew_instance or 1}"
        if d.date < start_date:
            overflow_before.add(key)
            if key not in overflow_earliest or d.date < overflow_earliest[key]:
                overflow_earliest[key] = d.date
        elif d.date > end_date:
            overflow_after.add(key)
            if key not in overflow_latest or d.date > overflow_latest[key]:
                overflow_latest[key] = d.date

    payroll_profiles = PayrollProfile.query.order_by(PayrollProfile.sort_order).all()

    # Serialize profiles for JS payroll info popup
    profiles_json = json.dumps([{
        'id':              p.id,
        'name':            p.name,
        'description':     p.description or '',
        'daily_st_hours':  float(p.daily_st_hours)  if p.daily_st_hours  else None,
        'daily_dt_hours':  float(p.daily_dt_hours)  if p.daily_dt_hours  else None,
        'ot_multiplier':   float(p.ot_multiplier),
        'dt_multiplier':   float(p.dt_multiplier),
        'weekly_st_hours': float(p.weekly_st_hours) if p.weekly_st_hours else None,
        'weekly_ot_multiplier': float(p.weekly_ot_multiplier),
        'seventh_day_rule': p.seventh_day_rule,
    } for p in payroll_profiles])

    # Locations for gantt (project-specific only)
    gantt_locations = Location.query.filter_by(
        project_id=pid, active=True
    ).order_by(Location.name).all()
    gantt_loc_days = LocationDay.query.filter_by(budget_id=bid).all()
    gantt_loc_day_map = {}
    for ld in gantt_loc_days:
        gantt_loc_day_map[(ld.location_id, ld.date.isoformat())] = ld.day_type

    # Crew assignments: (line_id, instance) → CrewAssignment
    line_ids = [ln.id for ln in labor_lines]
    crew_assignments_raw = CrewAssignment.query.filter(
        CrewAssignment.budget_line_id.in_(line_ids)
    ).all() if line_ids else []
    assignments_map = {(ca.budget_line_id, ca.instance or 1): ca for ca in crew_assignments_raw}

    # All active crew members for the picker
    all_crew = CrewMember.query.filter_by(active=True).order_by(CrewMember.name).all()
    crew_members_json = json.dumps([{
        "id": c.id, "name": c.name,
        "department": c.department or "",
        "company": c.company or "",
        "default_rate": float(c.default_rate) if c.default_rate else None,
        "default_rate_type": c.default_rate_type or "day_10",
        "default_agent_pct": float(c.default_agent_pct) if c.default_agent_pct else None,
    } for c in all_crew])

    # Top-sheet totals for floating budget bar
    actuals_by_code_gantt = {}
    gantt_top_sheet = calc_top_sheet(budget, all_lines, fringe_cfgs, actuals_by_code_gantt, profile, pw_start)

    all_budgets = Budget.query.filter_by(project_id=pid).order_by(Budget.created_at.desc()).all()
    parent_names = {}
    for b in all_budgets:
        if b.parent_budget_id:
            parent = next((x for x in all_budgets if x.id == b.parent_budget_id), None)
            if parent:
                parent_names[b.id] = parent.name

    current_working_bid = next(
        (b.id for b in all_budgets if _budget_type(b.budget_mode) == 'working' and b.version_status == 'current'), None
    ) or next(
        (b.id for b in all_budgets if _budget_type(b.budget_mode) == 'working' and b.version_status != 'archived'), None
    )
    current_estimated_bid = next(
        (b.id for b in all_budgets if _budget_type(b.budget_mode) == 'estimated' and b.version_status == 'current'), None
    ) or next(
        (b.id for b in all_budgets if _budget_type(b.budget_mode) == 'estimated' and b.version_status != 'archived'), None
    )

    return render_template("gantt.html",
        project=project, budget=budget,
        all_budgets=all_budgets,
        parent_names=parent_names,
        current_working_bid=current_working_bid,
        current_estimated_bid=current_estimated_bid,
        expanded_lines=expanded_lines,
        date_range=date_range, day_map=day_map,
        ot_status_map=ot_status_map,
        day_types=list(DAY_TYPE_MULTIPLIERS.keys()),
        payroll_profile=profile,
        payroll_profiles=payroll_profiles,
        profiles_json=profiles_json,
        gantt_section_totals=gantt_section_totals,
        pw_start=pw_start,
        week_boundary_dates=week_boundary_dates,
        meal_map=meal_map,
        gantt_start=start_date.isoformat(),
        gantt_end=end_date.isoformat(),
        overflow_before=overflow_before,
        overflow_after=overflow_after,
        overflow_earliest=overflow_earliest,
        overflow_latest=overflow_latest,
        gantt_locations=gantt_locations,
        gantt_loc_day_map=gantt_loc_day_map,
        assignments_map=assignments_map,
        crew_members_json=crew_members_json,
        sched_mode=sched_mode,
        gantt_top_sheet=gantt_top_sheet,
    )


@app.route("/projects/<int:pid>/budget/<int:bid>/gantt/day", methods=["POST"])
@login_required
def set_gantt_day(pid, bid):
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    data   = request.get_json(force=True)
    try:
        line_id = int(data.get("line_id") or 0) or None
    except (TypeError, ValueError):
        line_id = None
    date_str      = data.get("date")
    day_type      = data.get("day_type", "work")
    episode       = data.get("episode")
    note          = data.get("note")
    # CHANGE 2: accept crew_instance
    try:
        crew_instance = int(data.get("crew_instance") or 1)
    except (TypeError, ValueError):
        crew_instance = 1

    if not line_id or not date_str:
        return jsonify({"error": "line_id and date required"}), 400

    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"error": "invalid date"}), 400

    sched_mode = 'working' if budget.budget_mode in ('working', 'actual') else 'estimated'

    existing = ScheduleDay.query.filter_by(
        budget_id=bid, budget_line_id=line_id,
        crew_instance=crew_instance, date=d,
        schedule_mode=sched_mode).first()

    if day_type == "off" and existing:
        db.session.delete(existing)
        db.session.commit()
        return jsonify({"ok": True, "deleted": True})

    if not existing:
        existing = ScheduleDay(budget_id=bid, budget_line_id=line_id,
                               crew_instance=crew_instance, date=d,
                               schedule_mode=sched_mode)
        db.session.add(existing)

    existing.day_type = day_type
    if episode is not None:
        existing.episode = episode
    if note is not None:
        existing.note = note
    try:
        est_ot_hours = data.get("est_ot_hours")
        if est_ot_hours is not None:
            existing.est_ot_hours = float(est_ot_hours)
    except (TypeError, ValueError):
        pass

    cell_flags = data.get("cell_flags")
    if cell_flags is not None:
        existing.cell_flags = json.dumps(cell_flags) if isinstance(cell_flags, dict) else cell_flags

    # ── Primary commit — this is the one that MUST succeed ───────────────────
    db.session.commit()

    # ── Everything below is best-effort; failures must NOT cause a 500 ───────
    _post_save_error = None
    saved_id         = existing.id        # cache before any potential rollback
    saved_flags      = existing.cell_flags

    try:
        _touch_budget(bid)
        db.session.commit()
    except Exception as _te:
        import traceback as _tb
        _post_save_error = f"touch_budget: {_te}"
        app.logger.error("_touch_budget failed in set_gantt_day: %s\n%s", _te, _tb.format_exc())
        try:
            db.session.rollback()
        except Exception:
            pass

    try:
        sync_schedule_driven_lines(bid, db.session)
    except Exception as _sdl_err:
        import traceback as _tb
        _post_save_error = (_post_save_error or "") + f" | sync_lines: {_sdl_err}"
        app.logger.error("sync_schedule_driven_lines failed in set_gantt_day: %s\n%s",
                         _sdl_err, _tb.format_exc())
        try:
            db.session.rollback()
        except Exception:
            pass

    # Auto-enable use_schedule on the line when first day is added
    use_schedule_toggled = False
    try:
        if line_id and day_type != 'off':
            ln = BudgetLine.query.filter_by(id=line_id, budget_id=bid).first()
            if ln and ln.is_labor and not ln.use_schedule:
                day_count = ScheduleDay.query.filter_by(
                    budget_id=bid, budget_line_id=line_id, schedule_mode=sched_mode
                ).count()
                if day_count == 1:
                    ln.use_schedule = True
                    db.session.commit()
                    use_schedule_toggled = True
    except Exception as _ue:
        import traceback as _tb
        _post_save_error = (_post_save_error or "") + f" | use_sched: {_ue}"
        app.logger.error("use_schedule toggle failed in set_gantt_day: %s\n%s", _ue, _tb.format_exc())
        try:
            db.session.rollback()
        except Exception:
            pass

    resp = {"ok": True, "id": saved_id, "day_type": day_type,
            "cell_flags": saved_flags,
            "use_schedule_toggled": use_schedule_toggled}
    if _post_save_error:
        resp["_warn"] = _post_save_error  # visible in browser console, doesn't break JS
    return jsonify(resp)


@app.route("/projects/<int:pid>/budget/<int:bid>/gantt/day", methods=["DELETE"])
@login_required
def clear_gantt_day(pid, bid):
    budget   = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    data     = request.get_json(force=True)
    line_id  = data.get("line_id")
    date_str = data.get("date")
    crew_instance = int(data.get("crew_instance") or 1)
    sched_mode = 'working' if budget.budget_mode in ('working', 'actual') else 'estimated'
    if line_id and date_str:
        try:
            d = datetime.strptime(date_str, "%Y-%m-%d").date()
            existing = ScheduleDay.query.filter_by(
                budget_id=bid, budget_line_id=line_id,
                crew_instance=crew_instance, date=d,
                schedule_mode=sched_mode).first()
            if existing:
                db.session.delete(existing)
                db.session.commit()
            # Auto-disable use_schedule when last day is removed
            remaining = ScheduleDay.query.filter_by(
                budget_id=bid, budget_line_id=line_id, schedule_mode=sched_mode
            ).count()
            if remaining == 0:
                ln = BudgetLine.query.filter_by(id=line_id, budget_id=bid).first()
                if ln and ln.use_schedule:
                    ln.use_schedule = False
                    db.session.commit()
        except Exception:
            pass
    return jsonify({"ok": True})


@app.route("/projects/<int:pid>/budget/<int:bid>/gantt/assign", methods=["POST"])
@login_required
def gantt_assign_crew(pid, bid):
    """Assign (or clear) a crew member to a specific budget line + instance in the schedule."""
    Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    data     = request.get_json(force=True)
    line_id  = int(data.get("line_id", 0))
    instance = int(data.get("instance") or 1)
    crew_id  = data.get("crew_member_id")   # None = clear
    name_ov  = data.get("name_override") or None

    existing = CrewAssignment.query.filter_by(
        budget_line_id=line_id, instance=instance
    ).first()

    # Resolve display name BEFORE any delete/commit (avoids DetachedInstanceError)
    name = None
    if crew_id:
        cm = CrewMember.query.get(int(crew_id))
        name = cm.name if cm else None
    elif name_ov:
        name = name_ov

    if not crew_id and not name_ov:
        if existing:
            db.session.delete(existing)
    elif existing:
        existing.crew_member_id = int(crew_id) if crew_id else None
        existing.name_override  = name_ov
    else:
        db.session.add(CrewAssignment(
            budget_line_id=line_id, instance=instance,
            crew_member_id=int(crew_id) if crew_id else None,
            name_override=name_ov,
        ))

    # Mirror instance-1 assignment back to BudgetLine.assigned_crew_id so budget tab stays in sync
    if instance == 1:
        ln = BudgetLine.query.filter_by(id=line_id, budget_id=bid).first()
        if ln:
            ln.assigned_crew_id = int(crew_id) if crew_id else None

    _touch_budget(bid)
    db.session.commit()
    return jsonify({"ok": True, "name": name})


@app.route("/projects/<int:pid>/budget/<int:bid>/gantt/meal", methods=["POST"])
@login_required
def set_gantt_meal(pid, bid):
    """Toggle a meal flag on a production day (courtesy_breakfast, first_meal, second_meal)."""
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    data      = request.get_json(force=True)
    date_str  = data.get("date")
    field     = data.get("field")   # courtesy_breakfast | first_meal | second_meal
    value     = bool(data.get("value", False))
    sched_mode = 'working' if budget.budget_mode in ('working', 'actual') else 'estimated'

    if not date_str or field not in ("courtesy_breakfast", "first_meal", "second_meal"):
        return jsonify({"error": "date and valid field required"}), 400

    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"error": "invalid date"}), 400

    row = ProductionDay.query.filter_by(budget_id=bid, date=d, schedule_mode=sched_mode).first()
    if row is None:
        row = ProductionDay(budget_id=bid, date=d, schedule_mode=sched_mode)
        db.session.add(row)

    setattr(row, field, value)
    db.session.commit()
    try:
        sync_schedule_driven_lines(bid, db.session)
    except Exception as _sdl_err:
        import traceback
        app.logger.error("sync_schedule_driven_lines failed in set_gantt_meal: %s\n%s",
                         _sdl_err, traceback.format_exc())
        try:
            db.session.rollback()
        except Exception:
            pass
    return jsonify({"ok": True, "date": date_str, "field": field, "value": value})


@app.route("/projects/<int:pid>/budget/<int:bid>/gantt/expand", methods=["POST"])
@login_required
def expand_gantt(pid, bid):
    """Add days to the visible date range (prepend or append)."""
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    data   = request.get_json(force=True)
    direction = data.get("direction", "after")  # before | after
    anchor    = data.get("anchor")              # YYYY-MM-DD
    count     = int(data.get("count", 7))
    try:
        anchor_date = datetime.strptime(anchor, "%Y-%m-%d").date()
    except Exception:
        return jsonify({"error": "invalid anchor"}), 400

    new_dates = []
    for i in range(1, count + 1):
        delta = timedelta(days=i) if direction == "after" else timedelta(days=-i)
        new_dates.append((anchor_date + delta).isoformat())
    return jsonify({"dates": sorted(new_dates)})


@app.route("/projects/<int:pid>/budget/<int:bid>/gantt/live")
@login_required
def gantt_live(pid, bid):
    """Return compact schedule state for real-time collaborative patching."""
    b = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    sched_mode = 'working' if b.budget_mode in ('working', 'actual') else 'estimated'

    days = ScheduleDay.query.filter_by(budget_id=bid, schedule_mode=sched_mode).all()
    assignments = (CrewAssignment.query
                   .join(BudgetLine, CrewAssignment.budget_line_id == BudgetLine.id)
                   .filter(BudgetLine.budget_id == bid).all())

    cells = {}
    for d in days:
        key = f"{d.budget_line_id}:{d.crew_instance or 1}:{d.date.isoformat()}"
        cells[key] = {
            "day_type":     d.day_type,
            "est_ot_hours": float(d.est_ot_hours) if d.est_ot_hours else 0.0,
            "cell_flags":   d.cell_flags or "{}",
        }

    crew = {}
    for ca in assignments:
        key = f"{ca.budget_line_id}:{ca.instance or 1}"
        name = (ca.crew_member.name if ca.crew_member_id and ca.crew_member
                else ca.name_override or "")
        crew[key] = {"name": name, "crew_member_id": ca.crew_member_id}

    editor = _budget_last_editor.get(bid)
    return jsonify({
        "updated_at": b.updated_at.isoformat() if b.updated_at else None,
        "cells": cells,
        "crew":  crew,
        "last_edit": {"name": editor["name"], "at": editor["at"].isoformat()} if editor else None,
    })


@app.route("/projects/<int:pid>/budget/<int:bid>/gantt/totals")
@login_required
def gantt_totals(pid, bid):
    """Return per-section ST/OT/DT cost totals for the live totals panel."""
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    all_lines   = BudgetLine.query.filter_by(budget_id=bid).all()
    labor_lines = [ln for ln in all_lines if ln.is_labor]
    days        = ScheduleDay.query.filter_by(budget_id=bid).all()

    profile  = budget.payroll_profile
    pw_start = budget.payroll_week_start if budget.payroll_week_start is not None else (
        profile.payroll_week_start if profile else 6)

    fringe_cfgs = get_fringe_configs(db.session)
    totals = _compute_gantt_section_totals(labor_lines, days, fringe_cfgs, profile, pw_start)

    # Per-cell OT status — group days by (line_id, crew_instance) and run calc_days_ot_status
    ot_cells = {}
    if profile:
        days_by_line_inst = {}
        for d in days:
            key = (d.budget_line_id, d.crew_instance or 1)
            days_by_line_inst.setdefault(key, []).append(d)

        line_map = {ln.id: ln for ln in labor_lines}
        for (lid, inst), inst_days in days_by_line_inst.items():
            ln = line_map.get(lid)
            if not ln:
                continue
            cfg = fringe_cfgs.get(ln.fringe_type)
            ot_applies = getattr(cfg, 'ot_applies', True) if cfg else True
            status = calc_days_ot_status(ln.rate_type or 'day_10', inst_days,
                                         profile, pw_start, ot_applies=ot_applies)
            if any(v for v in status.values()):
                ot_cells[f"{lid}:{inst}"] = {k: v for k, v in status.items() if v}

    # Grand total for the floating budget bar (labor + non-labor + fee)
    actuals_empty = {}
    top = calc_top_sheet(budget, all_lines, fringe_cfgs, actuals_empty, profile, pw_start)

    return jsonify({
        "sections":   totals,
        "ot_cells":   ot_cells,
        "subtotal":   top["subtotal_estimated"],
        "fee":        top["company_fee"],
        "grand":      top["grand_total_estimated"],
        "dispersed":  bool(budget.company_fee_dispersed),
    })


@app.route("/projects/<int:pid>/budget/<int:bid>/line/<int:lid>/schedule-label", methods=["POST"])
@login_required
def set_schedule_label(pid, bid, lid):
    """Set or clear a custom display label for one instance of a schedule row."""
    BudgetLine.query.filter_by(budget_id=bid).first()  # verify budget access
    ln = BudgetLine.query.filter_by(id=lid, budget_id=bid).first_or_404()
    data     = request.get_json(force=True)
    instance = str(data.get("instance", 1))
    label    = (data.get("label") or "").strip()
    labels   = json.loads(ln.schedule_labels) if ln.schedule_labels else {}
    if label:
        labels[instance] = label
    else:
        labels.pop(instance, None)
    ln.schedule_labels = json.dumps(labels) if labels else None
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/projects/<int:pid>/budget/<int:bid>/line/<int:lid>/calc")
@login_required
def line_calc_detail(pid, bid, lid):
    """Return verbose payroll breakdown for a single labor line (View Calc popover)."""
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    line   = BudgetLine.query.filter_by(id=lid, budget_id=bid).first_or_404()
    if not line.is_labor:
        return jsonify({"error": "Not a labor line"}), 400

    _sm = 'working' if budget.budget_mode in ('working', 'actual') else 'estimated'
    sched_days  = ScheduleDay.query.filter_by(budget_id=bid, budget_line_id=lid, schedule_mode=_sm).all()
    profile     = budget.payroll_profile
    pw_start    = budget.payroll_week_start if budget.payroll_week_start is not None else (
                  profile.payroll_week_start if profile else 6)
    fringe_cfgs = get_fringe_configs(db.session)

    detail = calc_line_detail(line, sched_days, fringe_cfgs, profile, pw_start)
    return jsonify(detail)


# ── Locations ─────────────────────────────────────────────────────────────────

@app.route("/projects/<int:pid>/locations")
@login_required
def location_list(pid):
    project   = ProjectSheet.query.get_or_404(pid)
    locations = Location.query.filter_by(project_id=pid, active=True).order_by(Location.name).all()
    return jsonify([{
        "id": l.id, "name": l.name, "location_type": l.location_type or "",
        "address": l.address or "", "map_url": l.map_url or "",
        "contact_name": l.contact_name or "", "contact_email": l.contact_email or "",
        "contact_phone": l.contact_phone or "",
        "dayof_name": l.dayof_name or "", "dayof_email": l.dayof_email or "",
        "dayof_phone": l.dayof_phone or "",
        "billing_type": l.billing_type or "per_day",
        "daily_rate": float(l.daily_rate) if l.daily_rate else None,
        "notes": l.notes or "",
        "budget_line_id": l.budget_line_id,
    } for l in locations])


@app.route("/projects/<int:pid>/locations/save", methods=["POST"])
@login_required
def location_save(pid):
    """Create or update a project location. New locations are also saved to the global library."""
    ProjectSheet.query.get_or_404(pid)
    data = request.get_json(force=True)
    lid  = data.get("id")
    is_new = not lid
    if lid:
        loc = Location.query.filter_by(id=lid, project_id=pid).first_or_404()
    else:
        loc = Location(project_id=pid)
        db.session.add(loc)
    _phone_fields = {"contact_phone", "dayof_phone"}
    _email_fields = {"contact_email", "dayof_email"}
    _loc_fields = ["name", "facility_name", "location_type", "address", "map_url",
                   "contact_name", "contact_email", "contact_phone",
                   "dayof_name", "dayof_email", "dayof_phone",
                   "billing_type", "daily_rate", "notes", "budget_line_id"]
    for f in _loc_fields:
        if f in data:
            val = data[f] if data[f] != "" else None
            if f in _phone_fields:
                val = _normalize_phone(val)
            elif f in _email_fields and val and not _validate_email(val):
                return jsonify({"error": f"Invalid email: {val}"}), 400
            setattr(loc, f, val)

    # Auto-sync to global library on new location (unless user opted out)
    if is_new and data.get("save_to_library", True) and loc.name:
        existing_global = Location.query.filter_by(
            project_id=None, name=loc.name, active=True
        ).first()
        if not existing_global:
            lib = Location(project_id=None)
            db.session.add(lib)
            for f in _loc_fields:
                if f != "budget_line_id":
                    setattr(lib, f, getattr(loc, f))

    db.session.commit()
    return jsonify({"ok": True, "id": loc.id, "name": loc.name})


@app.route("/projects/<int:pid>/locations/library")
@login_required
def location_library(pid):
    """Return global library locations (project_id=NULL) as JSON for the import picker."""
    # Exclude names already in this project
    existing_names = {l.name for l in Location.query.filter_by(project_id=pid, active=True).all()}
    libs = Location.query.filter_by(project_id=None, active=True).order_by(Location.name).all()
    return jsonify([{
        "id": l.id, "name": l.name, "location_type": l.location_type or "",
        "address": l.address or "", "map_url": l.map_url or "",
        "contact_name": l.contact_name or "", "contact_email": l.contact_email or "",
        "contact_phone": l.contact_phone or "",
        "dayof_name": l.dayof_name or "", "dayof_email": l.dayof_email or "",
        "dayof_phone": l.dayof_phone or "",
        "billing_type": l.billing_type or "per_day",
        "daily_rate": float(l.daily_rate) if l.daily_rate else None,
        "notes": l.notes or "",
        "already_in_project": l.name in existing_names,
    } for l in libs])


@app.route("/projects/<int:pid>/locations/<int:lid>/delete", methods=["POST"])
@login_required
def location_delete(pid, lid):
    loc = Location.query.filter_by(id=lid, project_id=pid).first_or_404()
    loc.active = False
    db.session.commit()
    return jsonify({"ok": True})


# ── Global Locations Database ──────────────────────────────────────────────────

@app.route("/locations")
@login_required
def location_db_list():
    """Global locations database (project_id=NULL)."""
    locations = Location.query.filter_by(project_id=None, active=True).order_by(Location.name).all()
    # Build {location_id: [project_name, ...]} via LocationDay
    loc_projects = {}
    rows = (db.session.query(LocationDay.location_id, ProjectSheet.name)
            .join(Budget, Budget.id == LocationDay.budget_id)
            .join(ProjectSheet, ProjectSheet.id == Budget.project_id)
            .distinct().all())
    for loc_id, proj_name in rows:
        loc_projects.setdefault(loc_id, set()).add(proj_name)
    loc_projects = {k: sorted(v) for k, v in loc_projects.items()}
    return render_template("locations.html", locations=locations, loc_projects=loc_projects)


@app.route("/locations/save", methods=["POST"])
@login_required
def location_db_save():
    """Create or update a global library location (project_id=NULL)."""
    data = request.get_json(force=True)
    lid  = data.get("id")
    if lid:
        loc = Location.query.filter_by(id=lid, project_id=None).first_or_404()
    else:
        loc = Location(project_id=None)
        db.session.add(loc)
    _phone_fields = {"contact_phone", "dayof_phone"}
    _email_fields = {"contact_email", "dayof_email"}
    for f in ["name", "facility_name", "location_type", "address", "map_url",
              "contact_name", "contact_email", "contact_phone",
              "dayof_name", "dayof_email", "dayof_phone",
              "billing_type", "daily_rate", "notes"]:
        if f in data:
            val = data[f] if data[f] != "" else None
            if f in _phone_fields:
                val = _normalize_phone(val)
            elif f in _email_fields and val and not _validate_email(val):
                return jsonify({"error": f"Invalid email: {val}"}), 400
            setattr(loc, f, val)
    db.session.commit()
    return jsonify({"ok": True, "id": loc.id, "name": loc.name})


@app.route("/locations/<int:lid>/delete", methods=["POST"])
@login_required
def location_db_delete(lid):
    """Soft-delete a global library location."""
    loc = Location.query.filter_by(id=lid, project_id=None).first_or_404()
    loc.active = False
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/projects/<int:pid>/budget/<int:bid>/location-day", methods=["POST"])
@login_required
def set_location_day(pid, bid):
    """Set or clear a location day. AJAX."""
    Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    data    = request.get_json(force=True)
    loc_id  = data.get("location_id")
    date_s  = data.get("date")
    day_type= data.get("day_type", "")
    note    = data.get("note")
    from datetime import date as _date
    d = _date.fromisoformat(date_s)
    existing = LocationDay.query.filter_by(budget_id=bid, location_id=loc_id, date=d).first()
    if not day_type:
        if existing:
            db.session.delete(existing)
            db.session.commit()
        return jsonify({"ok": True, "day_type": ""})
    if existing:
        existing.day_type = day_type
        if note is not None:
            existing.note = note
    else:
        existing = LocationDay(budget_id=bid, location_id=loc_id, date=d,
                               day_type=day_type, note=note)
        db.session.add(existing)
    db.session.commit()
    # Sync budget line quantity if linked
    loc = Location.query.get(loc_id)
    if loc and loc.budget_line_id:
        count = LocationDay.query.filter_by(
            budget_id=bid, location_id=loc_id
        ).filter(LocationDay.day_type != 'strike').count()
        ln = BudgetLine.query.get(loc.budget_line_id)
        if ln:
            ln.days = count
            if loc.daily_rate:
                ln.rate = float(loc.daily_rate)
                ln.estimated_total = float(loc.daily_rate) * count
            db.session.commit()
    return jsonify({"ok": True, "day_type": day_type})


@app.route("/projects/<int:pid>/budget/<int:bid>/line/<int:lid>/assign-crew", methods=["POST"])
@login_required
def assign_crew(pid, bid, lid):
    """Assign a crew member as the primary person on a budget line.
    Auto-applies the crew member's default_agent_pct to the line when set."""
    BudgetLine.query.filter_by(id=lid, budget_id=bid).first_or_404()
    data   = request.get_json(force=True)
    cid    = data.get("crew_id")
    ln     = BudgetLine.query.get(lid)
    ln.assigned_crew_id = int(cid) if cid else None
    agent_pct_applied = None
    cm = CrewMember.query.get(int(cid)) if cid else None
    if cm and cm.default_agent_pct:
        ln.agent_pct = float(cm.default_agent_pct)
        agent_pct_applied = float(cm.default_agent_pct)
    # Mirror to CrewAssignment (instance 1) so the gantt stays in sync
    ca = CrewAssignment.query.filter_by(budget_line_id=lid, instance=1).first()
    if not cid:
        if ca:
            db.session.delete(ca)
    elif ca:
        ca.crew_member_id = int(cid)
        ca.name_override  = None
    else:
        db.session.add(CrewAssignment(budget_line_id=lid, instance=1,
                                      crew_member_id=int(cid), name_override=None))
    _touch_budget(bid)
    db.session.commit()
    name = ln.assigned_crew.name if ln.assigned_crew else None
    # Return calc results so the UI can refresh totals without a full reload
    fringe_cfgs = get_fringe_configs(db.session)
    budget = Budget.query.get(bid)
    if ln.use_schedule:
        sched_mode = budget.budget_mode if budget.budget_mode in ('working', 'actual') else 'estimated'
        sched = ScheduleDay.query.filter_by(budget_line_id=ln.id, schedule_mode=sched_mode).all()
        profile = budget.payroll_profile
        pw_start = budget.payroll_week_start if budget.payroll_week_start is not None else (
            profile.payroll_week_start if profile else 6)
        res = calc_line_from_schedule(ln, sched, fringe_cfgs, profile, pw_start)
    else:
        res = calc_line(ln, fringe_cfgs)
    default_rate      = float(cm.default_rate)      if cid and cm and cm.default_rate      else None
    default_rate_type = cm.default_rate_type or 'day_10' if cid and cm and cm.default_rate else None
    return jsonify({"ok": True, "crew_id": cid, "name": name,
                    "agent_pct": agent_pct_applied,
                    "default_rate": default_rate,
                    "default_rate_type": default_rate_type,
                    "subtotal": res["subtotal"],
                    "est_total": res["est_total"],
                    "agent_amount": res.get("agent_amount", 0.0)})


@app.route("/projects/<int:pid>/budget/<int:bid>/contacts/omit", methods=["POST"])
@login_required
def contacts_set_omit(pid, bid):
    """Set omit flags for one or more contact entries."""
    Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    data    = request.get_json(force=True)
    entries = data.get("entries", [])   # list of {type, id, contact?}
    field   = data.get("field")         # name | phone | email
    value   = bool(data.get("value", True))

    if field not in ("name", "phone", "email"):
        return jsonify({"error": "invalid field"}), 400

    for entry in entries:
        if entry.get("type") == "crew":
            ca = CrewAssignment.query.get(entry["id"])
            if ca:
                flags = json.loads(ca.omit_flags or "{}") if ca.omit_flags else {}
                flags[field] = value
                ca.omit_flags = json.dumps(flags)
        elif entry.get("type") == "location":
            loc = Location.query.get(entry["id"])
            if loc:
                flags = json.loads(loc.omit_flags or "{}") if loc.omit_flags else {}
                contact = entry.get("contact", "main")   # main | dayof
                if contact not in flags:
                    flags[contact] = {}
                flags[contact][field] = value
                loc.omit_flags = json.dumps(flags)

    db.session.commit()
    return jsonify({"ok": True})


# ── Crew ──────────────────────────────────────────────────────────────────────

@app.route("/crew")
@login_required
def crew_list():
    members = CrewMember.query.order_by(CrewMember.department, CrewMember.name).all()
    # Build {crew_member_id: [project_name, ...]} via CrewAssignment and assigned_crew_id
    crew_projects = {}
    rows = (db.session.query(CrewAssignment.crew_member_id, ProjectSheet.name)
            .join(BudgetLine, BudgetLine.id == CrewAssignment.budget_line_id)
            .join(Budget, Budget.id == BudgetLine.budget_id)
            .join(ProjectSheet, ProjectSheet.id == Budget.project_id)
            .filter(CrewAssignment.crew_member_id.isnot(None))
            .distinct().all())
    for crew_id, proj_name in rows:
        crew_projects.setdefault(crew_id, set()).add(proj_name)
    rows2 = (db.session.query(BudgetLine.assigned_crew_id, ProjectSheet.name)
             .join(Budget, Budget.id == BudgetLine.budget_id)
             .join(ProjectSheet, ProjectSheet.id == Budget.project_id)
             .filter(BudgetLine.assigned_crew_id.isnot(None))
             .distinct().all())
    for crew_id, proj_name in rows2:
        crew_projects.setdefault(crew_id, set()).add(proj_name)
    crew_projects = {k: sorted(v) for k, v in crew_projects.items()}
    return render_template("crew.html", members=members, crew_projects=crew_projects)


@app.route("/crew/new", methods=["POST"])
@login_required
def crew_new():
    want_json = request.is_json or request.args.get("fmt") == "json"
    name = (request.json or request.form).get("name", "").strip() if want_json else request.form.get("name", "").strip()
    if not name:
        if want_json:
            return jsonify({"error": "Name is required"}), 400
        flash("Name is required.", "error")
        return redirect(url_for("crew_list"))

    def _get(field, default=""):
        src = request.json if want_json else request.form
        return (src.get(field) or default)

    _email = _get("email").strip() or None
    if _email and not _validate_email(_email):
        if want_json:
            return jsonify({"error": f"Invalid email address: {_email}"}), 400
        flash(f"Invalid email address: {_email}", "error")
        return redirect(url_for("crew_list"))

    m = CrewMember(
        name=name,
        department=_get("department") or None,
        default_rate=_get("default_rate") or None,
        default_rate_type=_get("default_rate_type", "day_10"),
        default_fringe=_get("default_fringe", "N"),
        default_agent_pct=float(_get("default_agent_pct", 0) or 0) / 100,
        email=_email,
        phone=_normalize_phone(_get("phone")),
        company=_get("company") or None,
    )
    db.session.add(m)
    db.session.commit()

    if want_json:
        return jsonify({"ok": True, "id": m.id, "name": m.name,
                        "department": m.department or "", "company": m.company or ""})
    flash(f"Added {m.name}.", "success")
    return redirect(url_for("crew_list"))


@app.route("/crew/<int:cid>/edit", methods=["POST"])
@login_required
def crew_edit(cid):
    m = CrewMember.query.get_or_404(cid)
    want_json = request.is_json or request.args.get("fmt") == "json"

    def _get(field, default=""):
        src = request.json if want_json else request.form
        return (src or {}).get(field) or default

    m.name             = _get("name", m.name).strip() or m.name
    m.department       = _get("department", "").strip() or None
    m.default_rate     = _get("default_rate") or None
    m.default_rate_type= _get("default_rate_type", m.default_rate_type)
    m.default_fringe   = _get("default_fringe", m.default_fringe)
    m.default_agent_pct= float(_get("default_agent_pct", 0) or 0) / 100
    _email = _get("email", "").strip() or None
    if _email and not _validate_email(_email):
        if want_json:
            return jsonify({"error": f"Invalid email: {_email}"}), 400
        flash(f"Invalid email address: {_email}", "error")
        return redirect(url_for("crew_list"))
    m.email = _email
    m.phone = _normalize_phone(_get("phone", "").strip())
    m.company = _get("company", "").strip() or None
    if not want_json:
        m.active = request.form.get("active") == "1"
    db.session.commit()
    if want_json:
        return jsonify({"ok": True, "id": m.id, "name": m.name})
    flash(f"Updated {m.name}.", "success")
    return redirect(url_for("crew_list"))


@app.route("/crew/<int:cid>/json", methods=["GET"])
@login_required
def crew_get_json(cid):
    m = CrewMember.query.get_or_404(cid)
    return jsonify({
        "id": m.id, "name": m.name, "department": m.department or "",
        "email": m.email or "", "phone": m.phone or "", "company": m.company or "",
        "default_rate": float(m.default_rate) if m.default_rate else "",
        "default_rate_type": m.default_rate_type or "day_10",
        "default_fringe": m.default_fringe or "N",
        "default_agent_pct": float(m.default_agent_pct or 0) * 100,
    })


@app.route("/crew/<int:cid>/delete", methods=["POST"])
@login_required
def crew_delete(cid):
    m = CrewMember.query.get_or_404(cid)
    # Null out FK references so the delete doesn't fail on constraint violations
    BudgetLine.query.filter_by(assigned_crew_id=cid).update({"assigned_crew_id": None},
                                                             synchronize_session=False)
    CrewAssignment.query.filter_by(crew_member_id=cid).update({"crew_member_id": None},
                                                               synchronize_session=False)
    BudgetDirectContact.query.filter_by(crew_member_id=cid).delete(synchronize_session=False)
    db.session.delete(m)
    db.session.commit()
    flash("Crew member deleted.", "success")
    return redirect(url_for("crew_list"))


# ── Support Contacts (reps/agents/managers) ────────────────────────────────

@app.route("/crew/<int:cid>/support", methods=["GET"])
@login_required
def support_contacts_list(cid):
    CrewMember.query.get_or_404(cid)
    contacts = SupportContact.query.filter_by(crew_member_id=cid, active=True).all()
    return jsonify([{
        "id": s.id, "role_type": s.role_type, "name": s.name,
        "email": s.email or "", "phone": s.phone or "", "company": s.company or "",
        "notify_callsheet": bool(s.notify_callsheet),
        "cc_by_default": bool(s.cc_by_default),
        "fee_pct": float(s.fee_pct) * 100 if s.fee_pct else None,
        "fee_type": s.fee_type or None,
    } for s in contacts])


@app.route("/crew/<int:cid>/support/save", methods=["POST"])
@login_required
def support_contact_save(cid):
    CrewMember.query.get_or_404(cid)
    data = request.get_json(force=True)
    sid = data.get("id")
    if sid:
        s = SupportContact.query.filter_by(id=sid, crew_member_id=cid).first_or_404()
    else:
        s = SupportContact(crew_member_id=cid)
        db.session.add(s)
    s.role_type   = data.get("role_type", "other")
    s.name        = data.get("name", "").strip()
    s.email       = data.get("email", "").strip() or None
    s.phone       = _normalize_phone(data.get("phone", ""))
    s.company     = data.get("company", "").strip() or None
    s.notify_callsheet = bool(data.get("notify_callsheet", False))
    s.cc_by_default    = bool(data.get("cc_by_default", False))
    raw_fee = data.get("fee_pct")
    s.fee_pct  = float(raw_fee) / 100 if raw_fee is not None and raw_fee != '' else None
    s.fee_type = data.get("fee_type") or None
    if not s.name:
        return jsonify({"error": "Name required"}), 400
    db.session.commit()
    return jsonify({"ok": True, "id": s.id})


@app.route("/crew/<int:cid>/support/<int:sid>/delete", methods=["POST"])
@login_required
def support_contact_delete(cid, sid):
    s = SupportContact.query.filter_by(id=sid, crew_member_id=cid).first_or_404()
    s.active = False
    db.session.commit()
    return jsonify({"ok": True})


# ── Role number on crew assignment ─────────────────────────────────────────

@app.route("/projects/<int:pid>/budget/<int:bid>/assignment/<int:caid>/role-number", methods=["POST"])
@login_required
def set_role_number(pid, bid, caid):
    Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    ca = CrewAssignment.query.get_or_404(caid)
    data = request.get_json(force=True)
    ca.role_number = data.get("role_number", "").strip() or None
    db.session.commit()
    return jsonify({"ok": True, "role_number": ca.role_number})


# ── Direct Contacts ────────────────────────────────────────────────────────

@app.route("/projects/<int:pid>/budget/<int:bid>/direct-contacts/add", methods=["POST"])
@login_required
def direct_contact_add(pid, bid):
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    data = request.json or {}

    # Create or find the crew member
    cid = data.get("crew_member_id")
    if cid:
        cm = CrewMember.query.get_or_404(cid)
    else:
        name = (data.get("name") or "").strip()
        if not name:
            return jsonify({"error": "Name required"}), 400
        email = (data.get("email") or "").strip() or None
        if email and not _validate_email(email):
            return jsonify({"error": f"Invalid email: {email}"}), 400
        cm = CrewMember(
            name=name,
            phone=_normalize_phone(data.get("phone", "")),
            email=email,
            company=(data.get("company") or "").strip() or None,
            department=(data.get("department") or "").strip() or None,
        )
        db.session.add(cm)
        db.session.flush()

    # Add as direct contact if not already
    existing = BudgetDirectContact.query.filter_by(budget_id=bid, crew_member_id=cm.id).first()
    if not existing:
        dc = BudgetDirectContact(budget_id=bid, crew_member_id=cm.id, role=data.get("role", ""))
        db.session.add(dc)

    db.session.commit()
    return jsonify({"ok": True, "id": cm.id, "name": cm.name})


@app.route("/projects/<int:pid>/budget/<int:bid>/direct-contacts/<int:dcid>/delete", methods=["POST"])
@login_required
def direct_contact_delete(pid, bid, dcid):
    dc = BudgetDirectContact.query.filter_by(id=dcid, budget_id=bid).first_or_404()
    db.session.delete(dc)
    db.session.commit()
    return jsonify({"ok": True})


# ── Project Unions ─────────────────────────────────────────────────────────

@app.route("/projects/<int:pid>/unions", methods=["GET"])
@login_required
def project_unions(pid):
    ProjectSheet.query.get_or_404(pid)
    unions = ProjectUnion.query.filter_by(project_id=pid).order_by(ProjectUnion.sort_order, ProjectUnion.id).all()
    return jsonify([{
        "id": u.id, "union_name": u.union_name, "contact_name": u.contact_name or "",
        "email": u.email or "", "phone": u.phone or "",
        "receives_callsheet": bool(u.receives_callsheet), "sort_order": u.sort_order,
    } for u in unions])


@app.route("/projects/<int:pid>/unions/save", methods=["POST"])
@login_required
def project_union_save(pid):
    ProjectSheet.query.get_or_404(pid)
    data = request.get_json(force=True)
    uid = data.get("id")
    if uid:
        u = ProjectUnion.query.filter_by(id=uid, project_id=pid).first_or_404()
        # Partial update — only overwrite fields present in payload
        if "union_name" in data: u.union_name = data["union_name"].strip()
        if "contact_name" in data: u.contact_name = data["contact_name"].strip() or None
        if "email" in data: u.email = data["email"].strip() or None
        if "phone" in data: u.phone = _normalize_phone(data["phone"])
        if "receives_callsheet" in data: u.receives_callsheet = bool(data["receives_callsheet"])
        if "sort_order" in data: u.sort_order = int(data["sort_order"] or 0)
    else:
        u = ProjectUnion(project_id=pid)
        db.session.add(u)
        u.union_name   = data.get("union_name", "").strip()
        u.contact_name = data.get("contact_name", "").strip() or None
        u.email        = data.get("email", "").strip() or None
        u.phone        = _normalize_phone(data.get("phone", ""))
        u.receives_callsheet = bool(data.get("receives_callsheet", False))
        u.sort_order   = int(data.get("sort_order", 0) or 0)
        if not u.union_name:
            return jsonify({"error": "Union name required"}), 400
    db.session.commit()
    return jsonify({"ok": True, "id": u.id})


@app.route("/projects/<int:pid>/unions/<int:uid>/delete", methods=["POST"])
@login_required
def project_union_delete(pid, uid):
    u = ProjectUnion.query.filter_by(id=uid, project_id=pid).first_or_404()
    db.session.delete(u)
    db.session.commit()
    return jsonify({"ok": True})


# ── Project Clients ────────────────────────────────────────────────────────

@app.route("/projects/<int:pid>/clients", methods=["GET"])
@login_required
def project_clients(pid):
    ProjectSheet.query.get_or_404(pid)
    clients = ProjectClient.query.filter_by(project_id=pid).order_by(ProjectClient.sort_order, ProjectClient.id).all()
    return jsonify([{
        "id": c.id, "name": c.name, "title": c.title or "",
        "company": c.company or "", "email": c.email or "", "phone": c.phone or "",
        "show_on_callsheet": bool(c.show_on_callsheet),
        "receives_callsheet": bool(c.receives_callsheet),
        "sort_order": c.sort_order,
    } for c in clients])


@app.route("/projects/<int:pid>/clients/save", methods=["POST"])
@login_required
def project_client_save(pid):
    ProjectSheet.query.get_or_404(pid)
    data = request.get_json(force=True)
    cid = data.get("id")
    if cid:
        c = ProjectClient.query.filter_by(id=cid, project_id=pid).first_or_404()
        # Partial update — only overwrite fields present in payload
        if "name" in data: c.name = data["name"].strip()
        if "title" in data: c.title = data["title"].strip() or None
        if "company" in data: c.company = data["company"].strip() or None
        if "email" in data: c.email = data["email"].strip() or None
        if "phone" in data: c.phone = _normalize_phone(data["phone"])
        if "show_on_callsheet" in data: c.show_on_callsheet = bool(data["show_on_callsheet"])
        if "receives_callsheet" in data: c.receives_callsheet = bool(data["receives_callsheet"])
        if "sort_order" in data: c.sort_order = int(data["sort_order"] or 0)
    else:
        c = ProjectClient(project_id=pid)
        db.session.add(c)
        c.name              = data.get("name", "").strip()
        c.title             = data.get("title", "").strip() or None
        c.company           = data.get("company", "").strip() or None
        c.email             = data.get("email", "").strip() or None
        c.phone             = _normalize_phone(data.get("phone", ""))
        c.show_on_callsheet    = bool(data.get("show_on_callsheet", True))
        c.receives_callsheet   = bool(data.get("receives_callsheet", True))
        c.sort_order        = int(data.get("sort_order", 0) or 0)
        if not c.name:
            return jsonify({"error": "Client name required"}), 400
    db.session.commit()
    return jsonify({"ok": True, "id": c.id})


@app.route("/projects/<int:pid>/clients/<int:cid>/delete", methods=["POST"])
@login_required
def project_client_delete(pid, cid):
    c = ProjectClient.query.filter_by(id=cid, project_id=pid).first_or_404()
    db.session.delete(c)
    db.session.commit()
    return jsonify({"ok": True})


# ── Call Sheet Distribution (foundation) ──────────────────────────────────

@app.route("/projects/<int:pid>/budget/<int:bid>/callsheet/<date_str>/distribution")
@login_required
def callsheet_distribution(pid, bid, date_str):
    """Get send history + recipient list for this call sheet day."""
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    sched_mode = 'working' if budget.budget_mode in ('working', 'actual') else 'estimated'
    try:
        selected_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"error": "invalid date"}), 400
    sends = CallSheetSend.query.filter_by(
        budget_id=bid, date=selected_date, schedule_mode=sched_mode
    ).order_by(CallSheetSend.sent_at.desc()).all()
    return jsonify([{
        "id": s.id,
        "version_label": s.version_label or "",
        "sent_at": s.sent_at.isoformat() if s.sent_at else None,
        "sent_by": s.sent_by or "",
        "notes": s.notes or "",
        "recipients": [{
            "id": r.id, "name": r.name, "email": r.email or "",
            "type": r.recipient_type, "status": r.status,
            "viewed_at": r.viewed_at.isoformat() if r.viewed_at else None,
            "confirmed_at": r.confirmed_at.isoformat() if r.confirmed_at else None,
        } for r in s.recipients],
    } for s in sends])


@app.route("/projects/<int:pid>/budget/<int:bid>/callsheet/<date_str>/prepare-send", methods=["POST"])
@login_required
def callsheet_prepare_send(pid, bid, date_str):
    """Create a CallSheetSend record with recipients and send emails."""
    import secrets as _secrets_mod
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    project = ProjectSheet.query.get_or_404(pid)
    sched_mode = 'working' if budget.budget_mode in ('working', 'actual') else 'estimated'
    try:
        selected_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"error": "invalid date"}), 400
    data = request.get_json(force=True) or {}
    version_label = data.get("version_label", "v1")
    send = CallSheetSend(
        budget_id=bid,
        date=selected_date,
        schedule_mode=sched_mode,
        version_label=version_label,
        notes=data.get("notes", ""),
        sent_by=ADMIN_EMAIL,
        sent_at=datetime.utcnow(),
    )
    db.session.add(send)
    db.session.flush()
    recipients_in = data.get("recipients", [])
    created_recs = []
    for r in recipients_in:
        token = _secrets_mod.token_urlsafe(32)
        rec = CallSheetRecipient(
            send_id=send.id,
            recipient_type=r.get("type", "crew"),
            name=r.get("name", ""),
            email=r.get("email", ""),
            phone=r.get("phone", "") or None,
            confirm_token=token,
            status="pending",
        )
        db.session.add(rec)
        created_recs.append((rec, r.get("email", ""), r.get("phone", "")))
    db.session.commit()

    date_display = selected_date.strftime("%A, %B %-d, %Y")

    # Build call sheet data for PDF / SMS context
    cs_rec = CallSheetData.query.filter_by(
        budget_id=bid, date=selected_date, schedule_mode=sched_mode).first()
    cs_data_dict = json.loads(cs_rec.data_json) if cs_rec and cs_rec.data_json else {}

    # Build locations list for PDF
    from models import LocationDay, Location
    loc_day_ids = [ld.location_id for ld in LocationDay.query.filter_by(
        budget_id=bid, date=selected_date).all()]
    pdf_locations = Location.query.filter(Location.id.in_(loc_day_ids)).all() if loc_day_ids else []

    # Generate PDF (shared across all recipients for this send)
    pdf_bytes = _generate_callsheet_pdf(
        send_obj=send,
        project_name=project.name,
        date_display=date_display,
        cs_data=cs_data_dict,
        crew_rows=[],
        locations_today=pdf_locations,
    )
    pdf_filename = f"CallSheet_{project.name.replace(' ','_')}_{selected_date.isoformat()}_{version_label}.pdf"

    # Build first location summary for SMS
    first_loc = pdf_locations[0] if pdf_locations else None
    loc_sms_line = ""
    if first_loc:
        loc_sms_line = f"\nLocation: {first_loc.name or ''}"
        if getattr(first_loc, 'address', None):
            loc_sms_line += f"\n{first_loc.address}"

    cct = cs_data_dict.get('crew_call_times') or {}
    general_call = cs_data_dict.get('general_crew_call', '') or ''

    sent_email = 0
    sent_sms   = 0
    recipient_results = []
    for rec, email, phone in created_recs:
        # Find this person's individual call time
        personal_call = ''
        name_lower = rec.name.lower()
        for key, t in cct.items():
            if key.split('||')[-1].strip().lower() == name_lower:
                personal_call = t
                break
        call_display = personal_call or general_call or 'TBD'

        view_url = request.host_url.rstrip('/') + f"/callsheet/view/{rec.confirm_token}"
        subject  = f"Call Sheet — {project.name} — {date_display} ({version_label})"
        body = (
            f"Hi {rec.name},\n\n"
            f"Your call sheet for {project.name} on {date_display} is ready.\n"
            f"Your call time: {call_display}\n\n"
            f"View & confirm your call: {view_url}\n\n"
            f"Please confirm receipt by clicking the link above.\n\n"
            f"— Framework Productions · contact@thefp.tv\n"
        )
        email_ok = None
        sms_ok   = None
        if email:
            email_ok = _send_email(email, subject, body,
                                   attachment_bytes=pdf_bytes,
                                   attachment_filename=pdf_filename)
            if email_ok:
                rec.status = "sent"
                sent_email += 1

        if phone:
            sms_body = (
                f"{project.name} — Call Sheet {date_display}\n"
                f"Hi {rec.name}, your call: {call_display}"
                f"{loc_sms_line}\n"
                f"Confirm: {view_url}"
            )
            sms_ok = _send_sms(phone, sms_body)
            if sms_ok:
                sent_sms += 1
                if rec.status == "pending":
                    rec.status = "sent"

        recipient_results.append({
            "name":      rec.name,
            "email_ok":  email_ok,   # True/False/None (None = not attempted)
            "sms_ok":    sms_ok,     # True/False/None
            "version":   version_label,
        })

    db.session.commit()
    return jsonify({"ok": True, "send_id": send.id,
                    "sent_email": sent_email, "sent_sms": sent_sms,
                    "total": len(created_recs),
                    "recipients": recipient_results})


@app.route("/callsheet/view/<token>")
def callsheet_view_public(token):
    """Public link: mark as viewed, show confirm page."""
    rec = CallSheetRecipient.query.filter_by(confirm_token=token).first_or_404()
    if not rec.viewed_at:
        rec.viewed_at = datetime.utcnow()
        if rec.status == "sent":
            rec.status = "viewed"
        db.session.commit()
    send = CallSheetSend.query.get(rec.send_id)
    already_confirmed = rec.confirmed_at is not None
    date_display = send.date.strftime("%A, %B %-d, %Y") if send else ""
    budget = Budget.query.get(send.budget_id) if send else None
    project = ProjectSheet.query.get(budget.project_id) if budget else None
    project_name = project.name if project else ""
    # Fetch call sheet data to show on portal
    cs_portal_data = {}
    cs_locations = []
    if send and budget:
        sched_mode_p = 'working' if budget.budget_mode in ('working', 'actual') else 'estimated'
        cs_rec = CallSheetData.query.filter_by(
            budget_id=send.budget_id, date=send.date, schedule_mode=sched_mode_p).first()
        if cs_rec and cs_rec.data_json:
            try:
                cs_portal_data = json.loads(cs_rec.data_json)
            except Exception:
                pass
        from models import LocationDay, Location
        loc_day_ids = [ld.location_id for ld in LocationDay.query.filter_by(
            budget_id=send.budget_id, date=send.date).all()]
        cs_locations = Location.query.filter(Location.id.in_(loc_day_ids)).all() if loc_day_ids else []

    # Find this recipient's individual call time
    cct = cs_portal_data.get('crew_call_times') or {}
    personal_call = ''
    name_lower = rec.name.lower()
    for key, t in cct.items():
        if key.split('||')[-1].strip().lower() == name_lower:
            personal_call = t
            break
    personal_call = personal_call or cs_portal_data.get('general_crew_call', '') or ''

    tz_name = (project.timezone if project and project.timezone else 'America/New_York')
    return render_template(
        "callsheet_confirm.html",
        rec=rec,
        send=send,
        already_confirmed=already_confirmed,
        date_display=date_display,
        project_name=project_name,
        token=token,
        cs_data=cs_portal_data,
        cs_locations=cs_locations,
        personal_call=personal_call,
        confirmed_local=_fmt_local(rec.confirmed_at, tz_name),
        viewed_local=_fmt_local(rec.viewed_at, tz_name),
        tz_name=tz_name,
    )


@app.route("/callsheet/confirm/<token>", methods=["POST"])
def callsheet_confirm_public(token):
    """Public link: record confirmation."""
    rec = CallSheetRecipient.query.filter_by(confirm_token=token).first_or_404()
    if not rec.confirmed_at:
        rec.confirmed_at = datetime.utcnow()
        rec.status = "confirmed"
        if not rec.viewed_at:
            rec.viewed_at = rec.confirmed_at
        db.session.commit()
    send = CallSheetSend.query.get(rec.send_id)
    date_display = send.date.strftime("%A, %B %-d, %Y") if send else ""
    budget = Budget.query.get(send.budget_id) if send else None
    project = ProjectSheet.query.get(budget.project_id) if budget else None
    cs_portal_data = {}
    cs_locations = []
    if send and budget:
        sched_mode_p = 'working' if budget.budget_mode in ('working', 'actual') else 'estimated'
        cs_rec = CallSheetData.query.filter_by(
            budget_id=send.budget_id, date=send.date, schedule_mode=sched_mode_p).first()
        if cs_rec and cs_rec.data_json:
            try:
                cs_portal_data = json.loads(cs_rec.data_json)
            except Exception:
                pass
        from models import LocationDay, Location
        loc_day_ids = [ld.location_id for ld in LocationDay.query.filter_by(
            budget_id=send.budget_id, date=send.date).all()]
        cs_locations = Location.query.filter(Location.id.in_(loc_day_ids)).all() if loc_day_ids else []
    cct = cs_portal_data.get('crew_call_times') or {}
    personal_call = ''
    name_lower = rec.name.lower()
    for key, t in cct.items():
        if key.split('||')[-1].strip().lower() == name_lower:
            personal_call = t
            break
    personal_call = personal_call or cs_portal_data.get('general_crew_call', '') or ''
    tz_name = (project.timezone if project and project.timezone else 'America/New_York')
    return render_template(
        "callsheet_confirm.html",
        rec=rec,
        send=send,
        already_confirmed=True,
        date_display=date_display,
        project_name=project.name if project else "",
        token=token,
        cs_data=cs_portal_data,
        cs_locations=cs_locations,
        personal_call=personal_call,
        confirmed_local=_fmt_local(rec.confirmed_at, tz_name),
        viewed_local=_fmt_local(rec.viewed_at, tz_name),
        tz_name=tz_name,
    )


# ── Budget Templates ──────────────────────────────────────────────────────────

@app.route("/budget-templates")
@login_required
def template_list():
    templates = BudgetTemplate.query.order_by(BudgetTemplate.name).all()
    return render_template("templates.html", templates=templates)


@app.route("/budget-templates/new", methods=["POST"])
@login_required
def template_new():
    name = request.form.get("name", "").strip()
    if not name:
        flash("Name required.", "error")
        return redirect(url_for("template_list"))
    t = BudgetTemplate(
        name=name,
        description=request.form.get("description", "").strip() or None,
    )
    db.session.add(t)
    db.session.commit()
    return redirect(url_for("template_edit", tid=t.id))


@app.route("/budget-templates/<int:tid>")
@login_required
def template_edit(tid):
    t = BudgetTemplate.query.get_or_404(tid)
    lines = sorted(t.lines, key=lambda x: (x.account_code, x.sort_order))
    return render_template("template_edit.html", template=t, lines=lines,
                           coa_sections=FP_COA_SECTIONS)


@app.route("/budget-templates/<int:tid>/save", methods=["POST"])
@login_required
def template_save(tid):
    t = BudgetTemplate.query.get_or_404(tid)
    data = request.get_json(force=True)
    # Replace all lines
    for ln in list(t.lines):
        db.session.delete(ln)
    db.session.flush()
    for i, row in enumerate(data.get("lines", [])):
        db.session.add(BudgetTemplateLine(
            template_id=t.id,
            account_code=int(row["account_code"]),
            account_name=row.get("account_name", ""),
            description=row.get("description", ""),
            is_labor=bool(row.get("is_labor", False)),
            rate_type=row.get("rate_type", "day_10"),
            fringe_type=row.get("fringe_type", "N"),
            agent_pct=float(row.get("agent_pct", 0) or 0),
            estimated_total=float(row.get("estimated_total", 0) or 0),
            sort_order=i,
        ))
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/budget-templates/<int:tid>/delete", methods=["POST"])
@login_required
def template_delete(tid):
    t = BudgetTemplate.query.get_or_404(tid)
    db.session.delete(t)
    db.session.commit()
    flash(f"Template '{t.name}' deleted.", "success")
    return redirect(url_for("template_list"))


# ── Fringe Config ─────────────────────────────────────────────────────────────

@app.route("/fringe-config")
@login_required
def fringe_config():
    fringes = FringeConfig.query.filter_by(project_id=None).order_by(
        FringeConfig.fringe_type).all()
    return render_template("fringe_config.html", fringes=fringes)


@app.route("/fringe-config", methods=["POST"])
@login_required
def fringe_config_save():
    fid   = request.form.get("fringe_id", type=int)
    fringe = FringeConfig.query.get_or_404(fid)
    fringe.label      = request.form.get("label", fringe.label).strip()
    fringe.rate       = float(request.form.get("rate", float(fringe.rate)))
    fringe.is_flat    = request.form.get("is_flat") == "1"
    flat_amt          = request.form.get("flat_amount", "")
    fringe.flat_amount = float(flat_amt) if flat_amt else None
    db.session.commit()
    flash(f"Updated fringe '{fringe.fringe_type}'.", "success")
    return redirect(url_for("fringe_config"))


@app.route("/fringe-config/new", methods=["POST"])
@login_required
def fringe_config_new():
    fringe_type = request.form.get("fringe_type", "").strip().upper()[:5]
    label       = request.form.get("label", "").strip()
    if not fringe_type or not label:
        flash("Type code and label are required.", "error")
        return redirect(url_for("fringe_config"))
    # Check for duplicate
    existing = FringeConfig.query.filter_by(project_id=None, fringe_type=fringe_type).first()
    if existing:
        flash(f"Fringe type '{fringe_type}' already exists.", "error")
        return redirect(url_for("fringe_config"))
    rate     = float(request.form.get("rate", 0) or 0)
    is_flat  = request.form.get("is_flat") == "1"
    flat_amt = request.form.get("flat_amount", "")
    fc = FringeConfig(
        project_id=None,
        fringe_type=fringe_type,
        label=label,
        rate=rate,
        is_flat=is_flat,
        flat_amount=float(flat_amt) if flat_amt else None,
    )
    db.session.add(fc)
    db.session.commit()
    flash(f"Added fringe '{fringe_type}'.", "success")
    return redirect(url_for("fringe_config"))


@app.route("/fringe-config/<int:fid>/delete", methods=["POST"])
@login_required
def fringe_config_delete(fid):
    fringe = FringeConfig.query.get_or_404(fid)
    PROTECTED = {"E", "N", "L", "U", "S", "I", "D"}
    if fringe.fringe_type in PROTECTED:
        flash(f"Cannot delete default fringe '{fringe.fringe_type}'.", "error")
        return redirect(url_for("fringe_config"))
    db.session.delete(fringe)
    db.session.commit()
    flash(f"Deleted fringe '{fringe.fringe_type}'.", "success")
    return redirect(url_for("fringe_config"))


# ── Call Sheets ───────────────────────────────────────────────────────────────

_QUOTE_FALLBACK = [
    # ── Film ──────────────────────────────────────────────────────────────────
    ('"Nobody puts Baby in a corner."', 'Dirty Dancing', 'film'),
    ('"I\'m not even supposed to be here today."', 'Clerks', 'film'),
    ('"You\'re gonna need a bigger boat."', 'Jaws', 'film'),
    ('"I feel the need — the need for speed."', 'Top Gun', 'film'),
    ('"That rug really tied the room together."', 'The Big Lebowski', 'film'),
    ('"We accept the love we think we deserve."', 'The Perks of Being a Wallflower', 'film'),
    ('"Strange things are afoot at the Circle K."', 'Bill & Ted\'s Excellent Adventure', 'film'),
    ('"Roads? Where we\'re going we don\'t need roads."', 'Back to the Future', 'film'),
    ('"Lighten up, Francis."', 'Stripes', 'film'),
    ('"We\'re on a mission from God."', 'The Blues Brothers', 'film'),
    ('"Never rat on your friends and always keep your mouth shut."', 'Goodfellas', 'film'),
    ('"That\'s not a knife. THAT\'S a knife."', 'Crocodile Dundee', 'film'),
    ('"I am the Dude. So that\'s what you call me."', 'The Big Lebowski', 'film'),
    ('"Gentlemen, you can\'t fight in here — this is the War Room!"', 'Dr. Strangelove', 'film'),
    ('"The stuff that dreams are made of."', 'The Maltese Falcon', 'film'),
    # ── TV ────────────────────────────────────────────────────────────────────
    ('"I am not a good man, but I\'m not nothing."', 'The Wire', 'tv'),
    ('"How you doin\'?"', 'Friends — Joey Tribbiani', 'tv'),
    ('"Bears. Beets. Battlestar Galactica."', 'The Office', 'tv'),
    ('"What we do in the shadows is nobody\'s business but our own."', 'What We Do in the Shadows', 'tv'),
    ('"Cool, cool, cool, cool, cool."', 'Brooklyn Nine-Nine — Jake Peralta', 'tv'),
    ('"Nobody\'s walking out on this fun, old-fashioned family Christmas."', 'National Lampoon\'s Christmas Vacation', 'film'),
    ('"The truth is out there."', 'The X-Files', 'tv'),
    ('"We\'re all just walking each other home."', 'Dead to Me', 'tv'),
    ('"Treat yo\'self."', 'Parks and Recreation', 'tv'),
    ('"I am the danger."', 'Breaking Bad — Walter White', 'tv'),
    # ── Books & Literary ──────────────────────────────────────────────────────
    ('"I am not afraid of storms, for I am learning how to sail my ship."', 'Louisa May Alcott, Little Women', 'book'),
    ('"It\'s a funny thing about comin\' home. Looks the same, smells the same, feels the same."', 'Benjamin Button (F. Scott Fitzgerald)', 'book'),
    ('"We are all of us stars, and we deserve to twinkle."', 'Marilyn Monroe', 'person'),
    ('"The edge of the world is wherever you stop."', 'Terry Pratchett', 'book'),
    ('"There is no real ending. It\'s just the place where you stop the story."', 'Frank Herbert', 'book'),
    ('"Writing is easy. You just open a vein and bleed."', 'Walter Wellesley "Red" Smith', 'person'),
    ('"Never confuse movement with action."', 'Ernest Hemingway', 'person'),
    ('"An idea that is not dangerous is unworthy of being called an idea at all."', 'Oscar Wilde', 'person'),
    # ── People ────────────────────────────────────────────────────────────────
    ('"I\'ve never had a humble opinion. If you\'ve got an opinion, why be humble about it?"', 'Joan Baez', 'person'),
    ('"Everything is theoretically impossible, until it is done."', 'Robert A. Heinlein', 'person'),
    ('"You\'re only given a little spark of madness. You mustn\'t lose it."', 'Robin Williams', 'person'),
    ('"I\'d rather regret the things I\'ve done than the things I haven\'t."', 'Lucille Ball', 'person'),
    ('"Normal is nothing more than a cycle on a washing machine."', 'Whoopi Goldberg', 'person'),
    ('"You have to be odd to be number one."', 'Dr. Seuss', 'person'),
    ('"If you obey all the rules, you miss all the fun."', 'Katharine Hepburn', 'person'),
    ('"I figure if a girl wants to be a legend, she should go ahead and be one."', 'Calamity Jane', 'person'),
    ('"I never lose. I either win or learn."', 'Nelson Mandela', 'person'),
    ('"Do or do not. There is no try."', 'Yoda', 'film'),
    ('"If everything seems under control, you\'re not going fast enough."', 'Mario Andretti', 'person'),
    ('"The best time to plant a tree was 20 years ago. The second best time is now."', 'Chinese Proverb', 'book'),
]

@app.route("/api/quote")
@login_required
def get_quote():
    """Return a random inspirational quote. Uses Claude API if key is set, else fallback list."""
    import random
    source_type = request.args.get('type', 'random')

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if _HAS_ANTHROPIC and api_key:
        type_map = {
            'film':   'a famous film or movie (cite the film title)',
            'tv':     'a TV show or series (cite the show name)',
            'book':   'a book or literary work (cite title and author)',
            'person': 'a famous person — historical or contemporary',
            'random': 'a book, film, TV show, or famous person (choose at random)',
        }
        source_desc = type_map.get(source_type, type_map['random'])
        prompt = (
            f"Give me one short, clever, unexpected quote from {source_desc}. "
            "Avoid the most famous or overused quotes — pick something lesser-known, "
            "witty, or surprising that would make a film/TV production crew smile. "
            "It can be funny, dry, or unexpectedly profound. "
            "Format it exactly as:\n"
            "\"[quote text]\"\n"
            "— [Source / Author]\n\n"
            "Return ONLY the formatted quote. No explanation, no preamble."
        )
        try:
            client = _anthropic_sdk.Anthropic(api_key=api_key)
            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=200,
                messages=[{"role": "user", "content": prompt}]
            )
            quote_text = msg.content[0].text.strip()
            return jsonify({"quote": quote_text, "type": source_type, "source": "ai"})
        except Exception as e:
            logging.warning(f"Quote API call failed: {e}")
            # Fall through to fallback

    # Fallback: random from curated list, optionally filtered by type
    pool = _QUOTE_FALLBACK if source_type == 'random' else [q for q in _QUOTE_FALLBACK if q[2] == source_type]
    if not pool:
        pool = _QUOTE_FALLBACK
    text, attribution, qtype = random.choice(pool)
    return jsonify({"quote": f'{text}\n— {attribution}', "type": qtype, "source": "fallback"})


@app.route("/projects/<int:pid>/budget/<int:bid>/callsheet/<date_str>/save", methods=["POST"])
@login_required
def callsheet_save(pid, bid, date_str):
    """AJAX: save call sheet day data (editable overrides)."""
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    try:
        selected_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"error": "invalid date"}), 400
    sched_mode = 'working' if budget.budget_mode in ('working', 'actual') else 'estimated'
    payload = request.get_json(force=True, silent=True) or {}
    rec = CallSheetData.query.filter_by(
        budget_id=bid, date=selected_date, schedule_mode=sched_mode).first()
    if not rec:
        rec = CallSheetData(budget_id=bid, date=selected_date, schedule_mode=sched_mode)
        db.session.add(rec)
    rec.data_json = json.dumps(payload)
    rec.updated_at = datetime.utcnow()
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/projects/<int:pid>/budget/<int:bid>/callsheet/contacts")
@login_required
def callsheet_contacts_api(pid, bid):
    """Return all crew members for this budget for Key Personnel dropdowns."""
    Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    lines = BudgetLine.query.filter_by(budget_id=bid, is_labor=True).all()
    contacts = []
    seen_ids = set()
    for ln in lines:
        for ca in ln.crew_assignments:
            if ca.crew_member_id and ca.crew_member_id not in seen_ids:
                cm = ca.crew_member
                if cm:
                    contacts.append({
                        "id": cm.id,
                        "name": cm.name,
                        "role": ln.description or ln.account_name,
                        "phone": cm.phone or "",
                        "email": cm.email or "",
                    })
                    seen_ids.add(cm.id)
            elif ca.name_override and not ca.crew_member_id:
                contacts.append({
                    "id": None,
                    "name": ca.name_override,
                    "role": ln.description or ln.account_name,
                    "phone": "",
                    "email": "",
                })
    return jsonify(contacts)


@app.route("/projects/<int:pid>/budget/<int:bid>/callsheet")
@app.route("/projects/<int:pid>/budget/<int:bid>/callsheet/<date_str>")
@login_required
def callsheet_view(pid, bid, date_str=None):
    project = ProjectSheet.query.get_or_404(pid)
    budget  = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()

    sched_mode = 'working' if budget.budget_mode in ('working', 'actual') else 'estimated'

    # All scheduled (non-off) dates for this budget/mode
    date_rows = db.session.query(ScheduleDay.date).filter(
        ScheduleDay.budget_id == bid,
        ScheduleDay.schedule_mode == sched_mode,
        ScheduleDay.day_type != 'off',
    ).distinct().order_by(ScheduleDay.date).all()
    all_scheduled_dates = [r[0] for r in date_rows]

    # Resolve selected date — remember last viewed per budget
    _session_key = f"cs_last_date_{bid}"
    if date_str:
        try:
            selected_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            selected_date = all_scheduled_dates[0] if all_scheduled_dates else date.today()
        from flask import session as _session
        _session[_session_key] = selected_date.strftime("%Y-%m-%d")
    else:
        from flask import session as _session
        _saved = _session.get(_session_key)
        if _saved and all_scheduled_dates:
            try:
                _candidate = datetime.strptime(_saved, "%Y-%m-%d").date()
                selected_date = _candidate if _candidate in all_scheduled_dates else all_scheduled_dates[0]
            except ValueError:
                selected_date = all_scheduled_dates[0]
        else:
            selected_date = all_scheduled_dates[0] if all_scheduled_dates else date.today()

    shooting_day_num = None
    if selected_date in all_scheduled_dates:
        shooting_day_num = all_scheduled_dates.index(selected_date) + 1

    # Schedule days for selected date
    days_today = ScheduleDay.query.filter(
        ScheduleDay.budget_id == bid,
        ScheduleDay.schedule_mode == sched_mode,
        ScheduleDay.date == selected_date,
        ScheduleDay.day_type != 'off',
    ).order_by(ScheduleDay.budget_line_id, ScheduleDay.crew_instance).all()

    # Budget lines active today
    line_ids = list({d.budget_line_id for d in days_today if d.budget_line_id})
    lines_today = BudgetLine.query.filter(BudgetLine.id.in_(line_ids)).order_by(
        BudgetLine.account_code, BudgetLine.sort_order).all() if line_ids else []
    lines_by_id = {ln.id: ln for ln in lines_today}

    # Crew assignments for those lines
    assignments = CrewAssignment.query.filter(
        CrewAssignment.budget_line_id.in_(line_ids)
    ).all() if line_ids else []
    crew_ids = [a.crew_member_id for a in assignments if a.crew_member_id]
    crew_by_id = {c.id: c for c in CrewMember.query.filter(
        CrewMember.id.in_(crew_ids)).all()} if crew_ids else {}

    # Locations today
    location_days_today = LocationDay.query.filter_by(budget_id=bid, date=selected_date).all()
    loc_ids = [ld.location_id for ld in location_days_today]
    locations_today = Location.query.filter(Location.id.in_(loc_ids)).all() if loc_ids else []
    locations_by_id = {loc.id: loc for loc in locations_today}

    # Production day (meals)
    prod_day = ProductionDay.query.filter_by(
        budget_id=bid, date=selected_date, schedule_mode=sched_mode).first()

    # Build crew list grouped by COA section, with day type per instance
    days_by_line_inst = {}
    for d in days_today:
        days_by_line_inst[(d.budget_line_id, d.crew_instance or 1)] = d

    crew_rows = []  # {section_code, section_name, role, name, day_type, phone, email}
    for ln in lines_today:
        sec_code = _section_for_code(ln.account_code)
        sec_name = _section_name(sec_code)
        line_assignments = [a for a in assignments if a.budget_line_id == ln.id]
        if line_assignments:
            for a in line_assignments:
                inst = a.instance or 1
                sd = days_by_line_inst.get((ln.id, inst))
                day_type = sd.day_type if sd else 'work'
                cm = crew_by_id.get(a.crew_member_id) if a.crew_member_id else None
                name = a.name_override or (cm.name if cm else '') or '—'
                phone = cm.phone if cm else ''
                email = cm.email if cm else ''
                crew_rows.append({
                    'section_code': sec_code,
                    'section_name': sec_name,
                    'role': ln.description or ln.account_name,
                    'name': name,
                    'day_type': day_type,
                    'phone': phone or '',
                    'email': email or '',
                    'account_code': ln.account_code,
                })
        else:
            # Line has no assignment yet — show role with blank name
            for inst in range(1, int(ln.quantity or 1) + 1):
                sd = days_by_line_inst.get((ln.id, inst))
                day_type = sd.day_type if sd else 'work'
                crew_rows.append({
                    'section_code': sec_code,
                    'section_name': sec_name,
                    'role': ln.description or ln.account_name,
                    'name': '—',
                    'day_type': day_type,
                    'phone': '',
                    'email': '',
                    'account_code': ln.account_code,
                })

    # Separate ATL/Talent from crew
    atl_rows     = [r for r in crew_rows if r['account_code'] < 700]
    talent_rows  = [r for r in crew_rows if 700 <= r['account_code'] < 800]
    crew_rows_bg = [r for r in crew_rows if r['account_code'] >= 1000]

    # ── Build Page 2 unified crew list (ATL + Talent + Production Staff) ────
    _P2_SECTION_SORT = {
        "Above the Line": 0,
        "Talent": 1,
        "Direction / AD": 10,
        "Production": 11,
        "Camera": 20,
        "Grip & Electric": 30,
        "Sound": 40,
        "Art": 50,
        "Hair & Makeup": 60,
        "Wardrobe": 70,
        "Locations": 80,
        "Transportation": 90,
        "Control Room": 95,
        "EPK / BTS": 96,
        "Craft Services": 97,
    }

    def _p2_section_for_row(r):
        if r['account_code'] < 700:
            return "Above the Line"
        if 700 <= r['account_code'] < 800:
            return "Talent"
        # account_code >= 1000 — use subgroup or section_name
        sg = _get_prod_staff_subgroup(r['role'])
        return sg if sg else r['section_name']

    crew_p2_all = []
    for r in crew_rows:
        sect = _p2_section_for_row(r)
        sort_key = _P2_SECTION_SORT.get(sect, 99)
        crew_p2_all.append({**r, 'p2_section': sect, 'p2_sort': sort_key})

    crew_p2_all.sort(key=lambda x: (x['p2_sort'], x['role'], x['name']))

    # ── Load saved call sheet day data ─────────────────────────────────────
    cs_rec = CallSheetData.query.filter_by(
        budget_id=bid, date=selected_date, schedule_mode=sched_mode).first()
    cs_data = json.loads(cs_rec.data_json) if cs_rec and cs_rec.data_json else {}

    # Apply saved location ordering for this day
    saved_loc_order = cs_data.get('location_order', [])
    if saved_loc_order and location_days_today:
        ld_by_id = {ld.id: ld for ld in location_days_today}
        ordered = [ld_by_id[lid] for lid in saved_loc_order if lid in ld_by_id]
        remaining = [ld for ld in location_days_today if ld.id not in set(saved_loc_order)]
        location_days_today = ordered + remaining

    # Available contacts for Key Personnel dropdowns (all crew on this budget)
    _labor_lines = BudgetLine.query.filter_by(budget_id=bid, is_labor=True).all()
    available_contacts = []
    _seen_cm = set()
    for _ln in _labor_lines:
        for _ca in _ln.crew_assignments:
            if _ca.crew_member_id and _ca.crew_member_id not in _seen_cm:
                _cm = _ca.crew_member
                if _cm:
                    available_contacts.append({
                        "id": _cm.id,
                        "name": _cm.name,
                        "role": _ln.description or _ln.account_name,
                        "phone": _cm.phone or "",
                        "email": _cm.email or "",
                    })
                    _seen_cm.add(_ca.crew_member_id)
            elif _ca.name_override and not _ca.crew_member_id:
                available_contacts.append({
                    "id": None,
                    "name": _ca.name_override,
                    "role": _ln.description or _ln.account_name,
                    "phone": "",
                    "email": "",
                })

    # Clients for this project (shown on call sheet if show_on_callsheet)
    project_clients_cs = ProjectClient.query.filter_by(
        project_id=pid, show_on_callsheet=True
    ).order_by(ProjectClient.sort_order).all()

    # Unions for this project (for call sheet Page 2)
    project_unions_cs = ProjectUnion.query.filter_by(
        project_id=pid
    ).order_by(ProjectUnion.sort_order).all()

    # Representation contacts for crew on this budget
    rep_contacts = []
    _seen_cm_rep = set()
    for _ln in _labor_lines:
        for _ca in _ln.crew_assignments:
            if _ca.crew_member_id and _ca.crew_member_id not in _seen_cm_rep:
                _cm = _ca.crew_member
                if _cm and _cm.support_contacts:
                    for sc in _cm.support_contacts:
                        if sc.active:
                            rep_contacts.append({
                                'crew_name': _cm.name,
                                'crew_role': _ln.description or _ln.account_name,
                                'rep_name': sc.name,
                                'rep_role_type': sc.role_type,
                                'rep_company': sc.company or '',
                                'rep_phone': sc.phone or '',
                                'rep_email': sc.email or '',
                                'notify_callsheet': sc.notify_callsheet,
                            })
                _seen_cm_rep.add(_ca.crew_member_id)

    # Key contacts (ATL + Production Staff with known senior roles)
    KEY_ROLE_PRIORITY = ['Director', 'Executive Producer', 'Producer', 'Creative Director',
                         'Line Producer', 'UPM', 'Production Supervisor', '1st AD', 'Set Medic']
    key_contacts = []
    seen_roles = set()
    for priority_role in KEY_ROLE_PRIORITY:
        for r in crew_rows:
            if priority_role.lower() in r['role'].lower() and priority_role not in seen_roles:
                key_contacts.append(r)
                seen_roles.add(priority_role)
                break

    all_budgets = Budget.query.filter_by(project_id=pid).order_by(Budget.created_at.desc()).all()
    parent_names = {}
    for b in all_budgets:
        if b.parent_budget_id:
            parent = next((x for x in all_budgets if x.id == b.parent_budget_id), None)
            if parent:
                parent_names[b.id] = parent.name

    # Next scheduled day (for Advance Schedule section)
    next_day_date = None
    next_day_lines_preview = []
    if all_scheduled_dates and selected_date in all_scheduled_dates:
        cur_idx = all_scheduled_dates.index(selected_date)
        if cur_idx + 1 < len(all_scheduled_dates):
            next_day_date = all_scheduled_dates[cur_idx + 1]
            next_lines = db.session.query(ScheduleDay).filter(
                ScheduleDay.budget_id == bid,
                ScheduleDay.schedule_mode == sched_mode,
                ScheduleDay.date == next_day_date,
                ScheduleDay.day_type != 'off',
            ).order_by(ScheduleDay.budget_line_id).limit(20).all()
            next_line_ids = list({d.budget_line_id for d in next_lines if d.budget_line_id})
            if next_line_ids:
                next_day_lines_preview = BudgetLine.query.filter(
                    BudgetLine.id.in_(next_line_ids)
                ).order_by(BudgetLine.account_code, BudgetLine.sort_order).all()

    current_working_bid = next(
        (b.id for b in all_budgets if _budget_type(b.budget_mode) == 'working' and b.version_status == 'current'), None
    ) or next(
        (b.id for b in all_budgets if _budget_type(b.budget_mode) == 'working' and b.version_status != 'archived'), None
    )
    current_estimated_bid = next(
        (b.id for b in all_budgets if _budget_type(b.budget_mode) == 'estimated' and b.version_status == 'current'), None
    ) or next(
        (b.id for b in all_budgets if _budget_type(b.budget_mode) == 'estimated' and b.version_status != 'archived'), None
    )

    # ── Confirmation status map for crew list ────────────────────────────────
    # Most-recent send for this date; map name_lower → {status, viewed_at, confirmed_at}
    confirm_status = {}
    latest_send = CallSheetSend.query.filter_by(
        budget_id=bid, date=selected_date, schedule_mode=sched_mode
    ).order_by(CallSheetSend.sent_at.desc()).first()
    if latest_send:
        for rcp in latest_send.recipients:
            key = rcp.name.strip().lower()
            confirm_status[key] = {
                "status":       rcp.status,
                "viewed_at":    rcp.viewed_at.strftime("%-I:%M %p")  if rcp.viewed_at    else None,
                "confirmed_at": rcp.confirmed_at.strftime("%-I:%M %p") if rcp.confirmed_at else None,
            }

    return render_template("callsheet.html",
        project=project,
        budget=budget,
        all_budgets=all_budgets,
        parent_names=parent_names,
        selected_date=selected_date,
        all_scheduled_dates=all_scheduled_dates,
        shooting_day_num=shooting_day_num,
        days_today=days_today,
        lines_by_id=lines_by_id,
        assignments=assignments,
        crew_by_id=crew_by_id,
        location_days_today=location_days_today,
        locations_today=locations_today,
        locations_by_id=locations_by_id,
        prod_day=prod_day,
        sched_mode=sched_mode,
        key_contacts=key_contacts,
        atl_rows=atl_rows,
        talent_rows=talent_rows,
        crew_rows=crew_rows_bg,
        all_crew_rows=crew_rows,
        current_working_bid=current_working_bid,
        current_estimated_bid=current_estimated_bid,
        cs_data=cs_data,
        available_contacts=available_contacts,
        next_day_date=next_day_date,
        next_day_lines_preview=next_day_lines_preview,
        project_clients_cs=project_clients_cs,
        project_unions_cs=project_unions_cs,
        rep_contacts=rep_contacts,
        crew_p2_all=crew_p2_all,
        confirm_status=confirm_status,
    )


# ── Helpers ───────────────────────────────────────────────────────────────────


def _touch_budget(bid):
    """Update budget.updated_at to now. Call after any meaningful change."""
    now = datetime.utcnow()
    db.session.execute(text("UPDATE budget SET updated_at = :now WHERE id = :id"),
                       {"now": now, "id": bid})
    try:
        name = current_user.name or current_user.email.split("@")[0]
    except Exception:
        name = "someone"
    _budget_last_editor[bid] = {"name": name, "at": now}


def _budget_type(budget_mode):
    """Return 'working' for working/actual modes, else 'estimated'."""
    return 'working' if budget_mode in ('working', 'actual') else 'estimated'


def _supersede_current(project_id, budget_type, exclude_id=None):
    """Mark all 'current' budgets of the given type as 'superseded'."""
    budgets = Budget.query.filter_by(project_id=project_id, version_status='current').all()
    for b in budgets:
        if b.id == exclude_id:
            continue
        if _budget_type(b.budget_mode) == budget_type:
            b.version_status = 'superseded'


def _copy_budget_lines(source_id, dest_id):
    """Copy all BudgetLine rows from source budget to dest budget.
    Returns {old_line_id: new_line_id} mapping for use by _copy_schedule_days.

    Parent rows (parent_line_id=None) are processed first so that child rows
    (kit fees, etc.) can have their parent_line_id remapped correctly.
    """
    src_lines = BudgetLine.query.filter_by(budget_id=source_id).order_by(
        BudgetLine.account_code, BudgetLine.sort_order).all()

    # Pre-load all crew assignments in one query to avoid lazy-load inside flush loop
    src_line_ids = [ln.id for ln in src_lines]
    src_cas = CrewAssignment.query.filter(
        CrewAssignment.budget_line_id.in_(src_line_ids)
    ).all() if src_line_ids else []
    cas_by_line = {}
    for ca in src_cas:
        cas_by_line.setdefault(ca.budget_line_id, []).append(ca)

    # Two-pass: parents first, then children (so line_id_map has parent IDs ready)
    parents  = [ln for ln in src_lines if not ln.parent_line_id]
    children = [ln for ln in src_lines if ln.parent_line_id]

    line_id_map = {}
    for ln in parents + children:
        new_ln = BudgetLine(
            budget_id=dest_id,
            account_code=ln.account_code,
            account_name=ln.account_name,
            description=ln.description,
            is_labor=ln.is_labor,
            sort_order=ln.sort_order,
            estimated_total=ln.estimated_total,
            payroll_co=ln.payroll_co,
            quantity=ln.quantity,
            days=ln.days,
            rate=ln.rate,
            rate_type=ln.rate_type,
            est_ot=ln.est_ot,
            fringe_type=ln.fringe_type,
            agent_pct=ln.agent_pct,
            note=ln.note,
            use_schedule=ln.use_schedule,
            days_unit=ln.days_unit,
            days_per_week=ln.days_per_week,
            # Identification fields
            line_tag=ln.line_tag,
            role_group=ln.role_group,
            unit_rate=ln.unit_rate,
            assigned_crew_id=ln.assigned_crew_id,
            # Parent-child relationship (remapped to new IDs)
            parent_line_id=line_id_map.get(ln.parent_line_id) if ln.parent_line_id else None,
            # working_total and manual_actual intentionally NOT copied:
            # the new budget starts with no working override (None = falls back to est_total)
        )
        db.session.add(new_ln)
        db.session.flush()  # get new_ln.id
        line_id_map[ln.id] = new_ln.id

        # Copy crew assignments (pre-loaded above — no lazy-load inside flush loop)
        for ca in cas_by_line.get(ln.id, []):
            db.session.add(CrewAssignment(
                budget_line_id=new_ln.id,
                instance=ca.instance or 1,
                crew_member_id=ca.crew_member_id,
                name_override=ca.name_override,
                rate_override=ca.rate_override,
                fringe_override=ca.fringe_override,
                agent_override=ca.agent_override,
                omit_flags=ca.omit_flags,
                role_number=ca.role_number,
            ))

    return line_id_map


def _copy_schedule_days(source_bid, dest_bid, line_id_map, dest_mode=None):
    """Copy all ScheduleDay and ProductionDay rows from source to dest budget.

    dest_mode: the budget_mode of the destination budget. When it is 'working'
    or 'actual', all copied rows are stored with schedule_mode='working' so the
    Working gantt can find them immediately without needing a separate init step.
    """
    dest_sched_mode = 'working' if dest_mode in ('working', 'actual') else None
    src_days = ScheduleDay.query.filter_by(budget_id=source_bid).all()
    for d in src_days:
        new_lid = line_id_map.get(d.budget_line_id)
        if d.budget_line_id and not new_lid:
            continue  # skip orphaned days (line wasn't copied)
        db.session.add(ScheduleDay(
            budget_id=dest_bid,
            budget_line_id=new_lid,
            crew_member_id=d.crew_member_id,
            crew_instance=d.crew_instance,
            date=d.date,
            episode=d.episode,
            day_type=d.day_type,
            rate_multiplier=d.rate_multiplier,
            note=d.note,
            est_ot_hours=d.est_ot_hours,
            cell_flags=d.cell_flags,
            schedule_mode=dest_sched_mode or d.schedule_mode,
        ))
    # Clear any stale ProductionDay rows for dest_bid (e.g. from a previous failed
    # attempt where SQLite reused the same budget_id after a rollback).
    ProductionDay.query.filter_by(budget_id=dest_bid).delete()
    # Deduplicate by date before inserting — older DBs have UNIQUE(budget_id, date)
    # without schedule_mode, so we take the first row per date.
    seen_pd_dates = set()
    for pd in ProductionDay.query.filter_by(budget_id=source_bid).all():
        if pd.date in seen_pd_dates:
            continue
        seen_pd_dates.add(pd.date)
        db.session.add(ProductionDay(
            budget_id=dest_bid,
            date=pd.date,
            schedule_mode=dest_sched_mode or pd.schedule_mode,
            courtesy_breakfast=pd.courtesy_breakfast,
            first_meal=pd.first_meal,
            second_meal=pd.second_meal,
        ))



def _order_lines_with_children(lines):
    """Return lines reordered so kit-fee/child rows appear immediately after their parent."""
    children_by_parent = {}
    parents = []
    for ln in lines:
        pid = getattr(ln, 'parent_line_id', None)
        if pid:
            children_by_parent.setdefault(pid, []).append(ln)
        else:
            parents.append(ln)
    result = []
    for ln in parents:
        result.append(ln)
        result.extend(children_by_parent.get(ln.id, []))
    # Orphaned children (parent in different section) go at end
    all_parent_ids = {ln.id for ln in parents}
    for pid, kids in children_by_parent.items():
        if pid not in all_parent_ids:
            result.extend(kids)
    return result


def _section_for_code(code):
    best = FP_COA_SECTIONS[0][0]
    for start, _ in FP_COA_SECTIONS:
        if code >= start:
            best = start
        else:
            break
    return best


def _section_name(code):
    for start, name in FP_COA_SECTIONS:
        if start == code:
            return name
    return str(code)


# ── Template filters ──────────────────────────────────────────────────────────

@app.template_filter("currency")
def currency_filter(v):
    try:
        return f"${float(v):,.2f}"
    except (TypeError, ValueError):
        return "$0.00"


@app.template_filter("pct")
def pct_filter(v):
    try:
        return f"{float(v) * 100:.2f}%"
    except (TypeError, ValueError):
        return "0.00%"


@app.template_filter("fromjson")
def fromjson_filter(s):
    try:
        return json.loads(s) if s else {}
    except (ValueError, TypeError):
        return {}


# ── Admin routes ──────────────────────────────────────────────────────────────

@app.route("/admin", methods=["GET"])
@login_required
@admin_required
def admin_panel():
    users          = User.query.order_by(User.name).all()
    projects       = ProjectSheet.query.order_by(ProjectSheet.name).all()
    project_access = ProjectAccess.query.all()
    return render_template("admin.html",
                           users=users,
                           projects=projects,
                           project_access=project_access,
                           coa_sections=FP_COA_SECTIONS)


@app.route("/admin/users/create", methods=["POST"])
@login_required
@admin_required
def admin_user_create():
    email     = request.form.get("email", "").strip().lower()
    name      = request.form.get("name", "").strip()
    role      = request.form.get("role", "line_producer")
    dept_code = request.form.get("dept_code", type=int)

    # Admin cannot create super_admin
    if current_user.role == 'admin' and role == 'super_admin':
        flash("Admins cannot create Super Admin accounts.", "error")
        return redirect(url_for("admin_panel"))

    valid_roles = ('super_admin', 'admin', 'line_producer', 'dept_head')
    if role not in valid_roles:
        role = 'line_producer'

    if not email:
        flash("Email is required.", "error")
        return redirect(url_for("admin_panel"))
    if User.query.filter_by(email=email).first():
        flash(f"A user with email {email} already exists.", "error")
        return redirect(url_for("admin_panel"))

    temp_pw = secrets.token_urlsafe(8)
    u = User(
        email=email,
        name=name or None,
        role=role,
        dept_code=dept_code if role == 'dept_head' else None,
        must_change_password=True,
    )
    u.set_password(temp_pw)
    db.session.add(u)
    db.session.commit()
    sent = _send_email(
        email,
        "You've been invited to FPBudget",
        f"""Hi {name or email},

You've been invited to FPBudget by Framework Productions.

Login: https://fp-budget.onrender.com/login
Email: {email}
Temporary password: {temp_pw}

Please log in and change your password on your first visit.

— Framework Productions
"""
    )
    if sent:
        flash(f"User created and invitation sent to {email}. Temporary password: {temp_pw}", "temp_pw")
    else:
        flash(f"User created. Temporary password: {temp_pw} (no email configured — send manually)", "temp_pw")
    return redirect(url_for("admin_panel"))


@app.route("/admin/users/<int:uid>/edit", methods=["POST"])
@login_required
@admin_required
def admin_user_edit(uid):
    u = User.query.get_or_404(uid)
    # Admin cannot change a super_admin's role
    if current_user.role == 'admin' and u.role == 'super_admin':
        flash("Admins cannot edit Super Admin accounts.", "error")
        return redirect(url_for("admin_panel"))

    name      = request.form.get("name", "").strip()
    email     = request.form.get("email", "").strip().lower()
    role      = request.form.get("role", u.role)
    dept_code = request.form.get("dept_code", type=int)

    # Admin cannot assign super_admin role
    if current_user.role == 'admin' and role == 'super_admin':
        flash("Admins cannot assign the Super Admin role.", "error")
        return redirect(url_for("admin_panel"))

    valid_roles = ('super_admin', 'admin', 'line_producer', 'dept_head')
    if role not in valid_roles:
        role = u.role

    if email and email != u.email:
        existing = User.query.filter_by(email=email).first()
        if existing and existing.id != uid:
            flash(f"Email {email} is already in use.", "error")
            return redirect(url_for("admin_panel"))
        u.email = email

    u.name      = name or u.name
    u.role      = role
    u.dept_code = dept_code if role == 'dept_head' else None
    db.session.commit()
    flash(f"User {u.email} updated.", "success")
    return redirect(url_for("admin_panel"))


@app.route("/admin/users/<int:uid>/toggle-active", methods=["POST"])
@login_required
@admin_required
def admin_user_toggle_active(uid):
    if uid == current_user.id:
        flash("You cannot deactivate yourself.", "error")
        return redirect(url_for("admin_panel"))
    u = User.query.get_or_404(uid)
    u.is_active = not u.is_active
    db.session.commit()
    status = "activated" if u.is_active else "deactivated"
    flash(f"User {u.email} {status}.", "success")
    return redirect(url_for("admin_panel"))


@app.route("/admin/users/<int:uid>/reset-password", methods=["POST"])
@login_required
@admin_required
def admin_user_reset_password(uid):
    u = User.query.get_or_404(uid)
    temp_pw = secrets.token_urlsafe(8)
    u.set_password(temp_pw)
    u.must_change_password = True
    db.session.commit()
    sent = _send_email(
        u.email,
        "Your FPBudget password has been reset",
        f"""Hi {u.name or u.email},

Your FPBudget password has been reset by an administrator.

Temporary password: {temp_pw}

Please log in and change your password immediately:
https://fp-budget.onrender.com/login

— Framework Productions
"""
    )
    if sent:
        flash(f"Password reset. Temporary password: {temp_pw} (email sent to {u.email})", "temp_pw")
    else:
        flash(f"Password reset. Temporary password: {temp_pw} (no email — copy and send manually)", "temp_pw")
    return redirect(url_for("admin_panel"))


@app.route("/admin/users/<int:uid>/delete", methods=["POST"])
@login_required
@admin_required
def admin_user_delete(uid):
    if uid == current_user.id:
        flash("You cannot delete your own account.", "error")
        return redirect(url_for("admin_panel"))
    u = User.query.get_or_404(uid)
    # Admin can only delete line_producer and dept_head; super_admin can delete anyone except self
    if current_user.role == 'admin' and u.role in ('super_admin', 'admin'):
        flash("Admins cannot delete admin or super_admin accounts.", "error")
        return redirect(url_for("admin_panel"))
    ProjectAccess.query.filter_by(user_id=uid).delete()
    db.session.delete(u)
    db.session.commit()
    flash(f"User {u.email} deleted.", "success")
    return redirect(url_for("admin_panel"))


@app.route("/admin/projects/<int:pid>/access", methods=["POST"])
@login_required
@admin_required
def admin_project_access_add(pid):
    ProjectSheet.query.get_or_404(pid)
    user_id = request.form.get("user_id", type=int)
    role    = request.form.get("role", "collaborator")
    if not user_id:
        return jsonify({"error": "user_id required"}), 400
    existing = ProjectAccess.query.filter_by(project_id=pid, user_id=user_id).first()
    if not existing:
        pa = ProjectAccess(project_id=pid, user_id=user_id, role=role)
        db.session.add(pa)
        db.session.commit()
    return jsonify({"ok": True})


@app.route("/admin/projects/<int:pid>/access/remove", methods=["POST"])
@login_required
@admin_required
def admin_project_access_remove(pid):
    user_id = request.form.get("user_id", type=int)
    if user_id:
        ProjectAccess.query.filter_by(project_id=pid, user_id=user_id).delete()
        db.session.commit()
    return jsonify({"ok": True})


# ── Project sharing routes ─────────────────────────────────────────────────────

def _user_can_access_project(pid):
    """Return True if the current user may access project pid."""
    if current_user.role in ('super_admin', 'admin'):
        return True
    return ProjectAccess.query.filter_by(project_id=pid, user_id=current_user.id).first() is not None


@app.route("/projects/<int:pid>/share", methods=["GET", "POST"])
@login_required
def project_share(pid):
    if not _user_can_access_project(pid):
        abort(403)
    project = ProjectSheet.query.get_or_404(pid)
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        target = User.query.filter_by(email=email).first()
        if not target:
            return jsonify({"error": "No user with that email"})
        existing = ProjectAccess.query.filter_by(project_id=pid, user_id=target.id).first()
        if not existing:
            pa = ProjectAccess(project_id=pid, user_id=target.id, role="collaborator")
            db.session.add(pa)
            db.session.commit()
        _send_email(
            target.email,
            f"You've been added to a project on FPBudget",
            f"""Hi {target.name or target.email},

You've been given access to the project "{project.name}" on FPBudget.

View it here: https://fp-budget.onrender.com/

— Framework Productions
"""
        )
        return jsonify({"ok": True, "user": {"name": target.name or target.email, "email": target.email, "id": target.id}})
    # GET — return share page
    access_list = (db.session.query(ProjectAccess, User)
                   .join(User, User.id == ProjectAccess.user_id)
                   .filter(ProjectAccess.project_id == pid)
                   .all())
    return render_template("share.html", project=project, access_list=access_list)


@app.route("/projects/<int:pid>/share/remove", methods=["POST"])
@login_required
def project_share_remove(pid):
    if not _user_can_access_project(pid):
        abort(403)
    uid = request.form.get("user_id", type=int)
    if uid:
        ProjectAccess.query.filter_by(project_id=pid, user_id=uid).delete()
        db.session.commit()
    return redirect(url_for("project_share", pid=pid))


# ── Profile / password change route ───────────────────────────────────────────

@app.route("/profile", methods=["GET", "POST"])
@login_required
def profile():
    if request.method == "POST":
        action = request.form.get("action", "")
        if action == "profile":
            name  = request.form.get("name", "").strip()
            email = request.form.get("email", "").strip().lower()
            phone = request.form.get("phone", "").strip()
            if email and email != current_user.email:
                existing = User.query.filter_by(email=email).first()
                if existing and existing.id != current_user.id:
                    flash("That email address is already in use.", "error")
                    return redirect(url_for("profile"))
                current_user.email = email
            current_user.name  = name or current_user.name
            current_user.phone = phone or None
            db.session.commit()
            flash("Profile updated.", "success")
            return redirect(url_for("profile"))
        elif action == "password":
            current_pw = request.form.get("current_password", "")
            new_pw     = request.form.get("new_password", "")
            confirm_pw = request.form.get("confirm_password", "")
            if not current_user.check_password(current_pw):
                flash("Current password is incorrect.", "error")
                return redirect(url_for("profile"))
            if not new_pw or len(new_pw) < 8:
                flash("New password must be at least 8 characters.", "error")
                return redirect(url_for("profile"))
            if new_pw != confirm_pw:
                flash("New passwords do not match.", "error")
                return redirect(url_for("profile"))
            current_user.set_password(new_pw)
            current_user.must_change_password = False
            db.session.commit()
            flash("Password updated successfully.", "success")
            if current_user.is_admin:
                return redirect(url_for("admin_panel"))
            return redirect(url_for("dashboard"))
    return render_template("profile.html")


# ── Startup ───────────────────────────────────────────────────────────────────

with app.app_context():
    db.create_all()

    # ── Migrations (run before any seed that touches these tables) ────────────
    _migrations = [
        # schedule_day columns
        "ALTER TABLE schedule_day ADD COLUMN crew_instance INTEGER DEFAULT 1 NOT NULL",
        "ALTER TABLE schedule_day ADD COLUMN est_ot_hours NUMERIC(5,2) DEFAULT 0",
        "ALTER TABLE schedule_day ADD COLUMN cell_flags TEXT",
        "ALTER TABLE schedule_day ADD COLUMN schedule_mode TEXT DEFAULT 'estimated' NOT NULL",
        # budget columns
        "ALTER TABLE budget ADD COLUMN payroll_profile_id INTEGER REFERENCES payroll_profile(id)",
        "ALTER TABLE budget ADD COLUMN payroll_week_start INTEGER",
        # fringe_config columns — MUST run before seed_fringes
        "ALTER TABLE fringe_config ADD COLUMN ot_applies BOOLEAN DEFAULT 1",
        # budget_line columns for kit fees + schedule-driven lines
        "ALTER TABLE budget_line ADD COLUMN parent_line_id INTEGER REFERENCES budget_line(id)",
        "ALTER TABLE budget_line ADD COLUMN line_tag TEXT",
        "ALTER TABLE budget_line ADD COLUMN role_group TEXT",
        "ALTER TABLE budget_line ADD COLUMN unit_rate NUMERIC(10,2)",
        # budget_line columns for phase / estimator
        "ALTER TABLE budget_line ADD COLUMN phase_id INTEGER REFERENCES budget_phase(id)",
        "ALTER TABLE budget_line ADD COLUMN use_estimator BOOLEAN DEFAULT 0",
        "ALTER TABLE budget_line ADD COLUMN days_unit TEXT DEFAULT 'days'",
        "ALTER TABLE budget_line ADD COLUMN days_per_week NUMERIC(4,1) DEFAULT 5.0",
        "ALTER TABLE budget_line ADD COLUMN estimator_phases TEXT",
        # Three-phase system
        "ALTER TABLE budget ADD COLUMN working_initialized_at TIMESTAMP",
        "ALTER TABLE budget_line ADD COLUMN working_total NUMERIC(14,2)",
        "ALTER TABLE budget_line ADD COLUMN manual_actual NUMERIC(14,2)",
        # Location system
        "ALTER TABLE budget_line ADD COLUMN assigned_crew_id INTEGER REFERENCES crew_member(id)",
        # Per-instance crew assignments
        "ALTER TABLE crew_assignment ADD COLUMN instance INTEGER DEFAULT 1 NOT NULL",
        "ALTER TABLE crew_assignment ADD COLUMN omit_flags TEXT",
        "ALTER TABLE location ADD COLUMN omit_flags TEXT",
        # Version management
        "ALTER TABLE budget ADD COLUMN updated_at TIMESTAMP",
        "ALTER TABLE budget ADD COLUMN version_status TEXT DEFAULT 'current' NOT NULL",
        "ALTER TABLE budget ADD COLUMN parent_budget_id INTEGER REFERENCES budget(id)",
        "UPDATE budget SET updated_at = created_at WHERE updated_at IS NULL",
        "UPDATE budget SET version_status = 'current' WHERE version_status IS NULL",
        "ALTER TABLE budget ADD COLUMN timezone TEXT DEFAULT 'America/Los_Angeles'",
        # Meal isolation — production_day needs schedule_mode
        "ALTER TABLE production_day ADD COLUMN schedule_mode TEXT DEFAULT 'estimated' NOT NULL",
        # Auto-calculated % line items (Workers' Comp, Payroll Fee)
        "ALTER TABLE budget ADD COLUMN workers_comp_pct NUMERIC(8,6) DEFAULT 0.03",
        "ALTER TABLE budget ADD COLUMN payroll_fee_pct NUMERIC(8,6) DEFAULT 0.0175",
        "UPDATE budget SET workers_comp_pct = 0.03   WHERE workers_comp_pct IS NULL",
        "UPDATE budget SET payroll_fee_pct  = 0.0175 WHERE payroll_fee_pct  IS NULL",
        # Talent role numbers
        "ALTER TABLE crew_assignment ADD COLUMN role_number TEXT",
        # Company fee disperse option
        "ALTER TABLE budget ADD COLUMN company_fee_dispersed BOOLEAN DEFAULT 0 NOT NULL",
        # Location facility name
        "ALTER TABLE location ADD COLUMN facility_name TEXT",
        # Per-instance schedule display labels
        "ALTER TABLE budget_line ADD COLUMN schedule_labels TEXT",
        # Per-budget production details (for PDF exports)
        "ALTER TABLE budget ADD COLUMN client_name VARCHAR(200)",
        "ALTER TABLE budget ADD COLUMN prepared_by VARCHAR(200)",
        "ALTER TABLE budget ADD COLUMN prepared_by_title VARCHAR(100)",
        "ALTER TABLE budget ADD COLUMN prepared_by_email VARCHAR(200)",
        "ALTER TABLE budget ADD COLUMN prepared_by_phone VARCHAR(50)",
        "ALTER TABLE callsheet_recipient ADD COLUMN phone VARCHAR(50)",
        # Docs module: project_sheet new columns
        "ALTER TABLE project_sheet ADD COLUMN dropbox_folder VARCHAR(300)",
        "ALTER TABLE project_sheet ADD COLUMN client_name VARCHAR(200)",
    ]
    for _sql in _migrations:
        try:
            db.session.execute(text(_sql))
            db.session.commit()
        except Exception:
            db.session.rollback()

    # Make location.project_id nullable so global library entries (project_id=NULL) can be saved.
    # SQLite cannot ALTER COLUMN constraints, so we recreate the table if still NOT NULL.
    try:
        _loc_schema = db.session.execute(text("SELECT sql FROM sqlite_master WHERE type='table' AND name='location'")).scalar() or ''
        if 'project_id INTEGER NOT NULL' in _loc_schema:
            db.session.execute(text("""
                CREATE TABLE location_new (
                    id INTEGER NOT NULL PRIMARY KEY,
                    project_id INTEGER,
                    name VARCHAR(200) NOT NULL,
                    facility_name TEXT,
                    location_type VARCHAR(50),
                    address VARCHAR(500),
                    map_url VARCHAR(1000),
                    contact_name VARCHAR(200),
                    contact_email VARCHAR(200),
                    contact_phone VARCHAR(50),
                    dayof_name VARCHAR(200),
                    dayof_email VARCHAR(200),
                    dayof_phone VARCHAR(50),
                    billing_type VARCHAR(20),
                    daily_rate NUMERIC(10,2),
                    budget_line_id INTEGER REFERENCES budget_line(id),
                    notes TEXT,
                    active BOOLEAN,
                    omit_flags TEXT,
                    FOREIGN KEY(project_id) REFERENCES project_sheet(id)
                )
            """))
            db.session.execute(text("""
                INSERT INTO location_new (id, project_id, name, facility_name, location_type, address, map_url,
                    contact_name, contact_email, contact_phone,
                    dayof_name, dayof_email, dayof_phone,
                    billing_type, daily_rate, budget_line_id, notes, active, omit_flags)
                SELECT id, project_id, name, facility_name, location_type, address, map_url,
                    contact_name, contact_email, contact_phone,
                    dayof_name, dayof_email, dayof_phone,
                    billing_type, daily_rate, budget_line_id, notes, active, omit_flags
                FROM location
            """))
            db.session.execute(text("DROP TABLE location"))
            db.session.execute(text("ALTER TABLE location_new RENAME TO location"))
            db.session.commit()
    except Exception:
        db.session.rollback()

    # Production day table (DDL, not ALTER — safe to run after above)
    try:
        db.session.execute(text("""
            CREATE TABLE IF NOT EXISTS production_day (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                budget_id INTEGER NOT NULL REFERENCES budget(id),
                date DATE NOT NULL,
                courtesy_breakfast BOOLEAN DEFAULT 0,
                first_meal BOOLEAN DEFAULT 0,
                second_meal BOOLEAN DEFAULT 0,
                UNIQUE(budget_id, date)
            )
        """))
        db.session.commit()
    except Exception:
        db.session.rollback()

    try:
        db.session.execute(text("""
            CREATE TABLE IF NOT EXISTS callsheet_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                budget_id INTEGER NOT NULL REFERENCES budget(id),
                date DATE NOT NULL,
                schedule_mode TEXT NOT NULL DEFAULT 'estimated',
                data_json TEXT,
                updated_at TIMESTAMP,
                UNIQUE(budget_id, date, schedule_mode)
            )
        """))
        db.session.commit()
    except Exception:
        db.session.rollback()

    for _tbl_sql in [
        """CREATE TABLE IF NOT EXISTS support_contact (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            crew_member_id INTEGER NOT NULL REFERENCES crew_member(id),
            role_type TEXT NOT NULL DEFAULT 'other',
            name TEXT NOT NULL,
            email TEXT,
            phone TEXT,
            company TEXT,
            notify_callsheet INTEGER DEFAULT 0,
            cc_by_default INTEGER DEFAULT 0,
            active INTEGER DEFAULT 1
        )""",
        """CREATE TABLE IF NOT EXISTS project_union (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL REFERENCES project_sheet(id),
            union_name TEXT NOT NULL,
            contact_name TEXT,
            email TEXT,
            phone TEXT,
            receives_callsheet INTEGER DEFAULT 0,
            sort_order INTEGER DEFAULT 0
        )""",
        """CREATE TABLE IF NOT EXISTS project_client (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL REFERENCES project_sheet(id),
            name TEXT NOT NULL,
            title TEXT,
            company TEXT,
            email TEXT,
            phone TEXT,
            show_on_callsheet INTEGER DEFAULT 1,
            receives_callsheet INTEGER DEFAULT 1,
            sort_order INTEGER DEFAULT 0
        )""",
        """CREATE TABLE IF NOT EXISTS callsheet_send (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            budget_id INTEGER NOT NULL REFERENCES budget(id),
            date DATE NOT NULL,
            schedule_mode TEXT NOT NULL DEFAULT 'estimated',
            version_label TEXT,
            sent_at TIMESTAMP,
            sent_by TEXT,
            notes TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS callsheet_recipient (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            send_id INTEGER NOT NULL REFERENCES callsheet_send(id),
            recipient_type TEXT NOT NULL DEFAULT 'crew',
            name TEXT NOT NULL,
            email TEXT,
            confirmed_at TIMESTAMP,
            confirm_token TEXT UNIQUE,
            status TEXT NOT NULL DEFAULT 'pending'
        )""",
    ]:
        try:
            db.session.execute(text(_tbl_sql))
            db.session.commit()
        except Exception:
            db.session.rollback()

    try:
        db.session.execute(text("""
            CREATE TABLE IF NOT EXISTS budget_direct_contact (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                budget_id INTEGER NOT NULL REFERENCES budget(id),
                crew_member_id INTEGER NOT NULL REFERENCES crew_member(id),
                role TEXT,
                sort_order INTEGER DEFAULT 0,
                UNIQUE(budget_id, crew_member_id)
            )
        """))
        db.session.commit()
    except Exception:
        db.session.rollback()

    # ── visibility_flags column migrations ───────────────────────────────────
    for _vis_sql in [
        "ALTER TABLE project_client ADD COLUMN visibility_flags TEXT",
        "ALTER TABLE project_union ADD COLUMN visibility_flags TEXT",
        "ALTER TABLE support_contact ADD COLUMN visibility_flags TEXT",
    ]:
        try:
            db.session.execute(text(_vis_sql))
            db.session.commit()
        except Exception:
            db.session.rollback()

    # ── support_contact fee fields ────────────────────────────────────────────
    for _fee_sql in [
        "ALTER TABLE support_contact ADD COLUMN fee_pct NUMERIC(6,4)",
        "ALTER TABLE support_contact ADD COLUMN fee_type VARCHAR(20)",
    ]:
        try:
            db.session.execute(text(_fee_sql))
            db.session.commit()
        except Exception:
            db.session.rollback()

    # ── Auth tables ───────────────────────────────────────────────────────────
    for _auth_sql in [
        """CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            name TEXT,
            password_hash TEXT NOT NULL,
            is_admin INTEGER NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 1,
            must_change_password INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS project_access (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL REFERENCES project_sheet(id),
            user_id INTEGER NOT NULL REFERENCES users(id),
            role TEXT DEFAULT 'collaborator',
            UNIQUE(project_id, user_id)
        )""",
    ]:
        try:
            db.session.execute(text(_auth_sql))
            db.session.commit()
        except Exception:
            db.session.rollback()

    # ── Seeds (run after all migrations) ─────────────────────────────────────
    seed_fringes(db.session)
    seed_standard_template(db.session)
    seed_payroll_profiles(db.session)

    # ── User table role column migrations (replace is_admin with role) ────────
    for _role_sql in [
        "ALTER TABLE users ADD COLUMN role VARCHAR(20) NOT NULL DEFAULT 'line_producer'",
        "ALTER TABLE users ADD COLUMN dept_code INTEGER",
        "ALTER TABLE users ADD COLUMN reset_token VARCHAR(100)",
        "ALTER TABLE users ADD COLUMN reset_token_expires TIMESTAMP",
    ]:
        try:
            db.session.execute(text(_role_sql))
            db.session.commit()
        except Exception:
            db.session.rollback()

    # ── User phone column ─────────────────────────────────────────────────────
    try:
        db.session.execute(text("ALTER TABLE users ADD COLUMN phone VARCHAR(50)"))
        db.session.commit()
    except: db.session.rollback()
    # Migrate old is_admin=1 rows to role='admin'
    try:
        db.session.execute(text("UPDATE users SET role='admin' WHERE is_admin=1 AND role='line_producer'"))
        db.session.commit()
    except Exception:
        db.session.rollback()
    # Drop legacy is_admin column now that role replaces it
    try:
        db.session.execute(text("ALTER TABLE users DROP COLUMN is_admin"))
        db.session.commit()
    except Exception:
        db.session.rollback()

    # ── Seed admin user ───────────────────────────────────────────────────────
    def _seed_admin_user():
        if not User.query.filter_by(email=ADMIN_EMAIL).first():
            u = User(email=ADMIN_EMAIL, name="Admin", role='super_admin')
            u.set_password(ADMIN_PASSWORD)
            db.session.add(u)
            db.session.commit()
        else:
            # Upgrade existing admin seed to super_admin if still default
            existing = User.query.filter_by(email=ADMIN_EMAIL).first()
            if existing and existing.role == 'line_producer':
                existing.role = 'super_admin'
                db.session.commit()
    _seed_admin_user()

    # ── CallSheetRecipient: add viewed_at column ──────────────────────────────
    try:
        db.session.execute(text("ALTER TABLE callsheet_recipient ADD COLUMN viewed_at TIMESTAMP"))
        db.session.commit()
    except Exception:
        db.session.rollback()

    # ── BudgetTemplateLine: add qty/days/rate columns, drop unique constraint ──
    for _tmpl_col in [
        "ALTER TABLE budget_template_line ADD COLUMN quantity NUMERIC(8,2) DEFAULT 1",
        "ALTER TABLE budget_template_line ADD COLUMN days NUMERIC(8,2) DEFAULT 1",
        "ALTER TABLE budget_template_line ADD COLUMN rate NUMERIC(12,2) DEFAULT 0",
    ]:
        try:
            db.session.execute(text(_tmpl_col))
            db.session.commit()
        except Exception:
            db.session.rollback()

    # Drop the unique(template_id, account_code) constraint so templates can hold
    # multiple lines per COA section (e.g. multiple Production Staff / Camera lines).
    try:
        if 'postgresql' in str(db.engine.url).lower():
            rows = db.session.execute(text(
                "SELECT constraint_name FROM information_schema.table_constraints "
                "WHERE table_name='budget_template_line' AND constraint_type='UNIQUE'"
            )).fetchall()
            for r in rows:
                db.session.execute(text(
                    f'ALTER TABLE budget_template_line DROP CONSTRAINT IF EXISTS "{r[0]}"'
                ))
            db.session.commit()
    except Exception:
        db.session.rollback()

    # ── Seed pre-built production templates ───────────────────────────────────
    def _seed_production_templates():
        # ── QA / Dev Test Budget ──────────────────────────────────────────────
        _TEST = "QA Dev Test Budget"
        if not BudgetTemplate.query.filter_by(name=_TEST).first():
            tmpl_t = BudgetTemplate(name=_TEST,
                                    description="Multi-dept test template: ATL, Talent (multiples), Staff, Camera, G&E, Sound, Art, HMU, Transport, Meals")
            db.session.add(tmpl_t)
            db.session.flush()
            _test_lines = [
                # code,  name,                         desc,                         labor, qty, days, rate,   rt,           fringe, agent, sort
                # ── Above the Line ────────────────────────────────────────────
                (600,  "Above the Line",               "Director / DP",              True,  1,   5,    2000,   "day_10",     "E",    0,     0),
                (600,  "Above the Line",               "Executive Producer",          True,  1,   5,    1500,   "day_10",     "E",    0,     10),
                # ── Talent ────────────────────────────────────────────────────
                (700,  "Talent",                       "Principal Talent",            True,  2,   3,    1200,   "flat_day",   "S",    0.10,  20),
                (700,  "Talent",                       "Supporting Talent",           True,  4,   2,    600,    "flat_day",   "S",    0.10,  30),
                (700,  "Talent",                       "Voice Over Talent",           True,  1,   1,    2500,   "flat_project","N",   0.10,  40),
                # ── Production Staff ──────────────────────────────────────────
                (1000, "Production Staff",             "Line Producer",               True,  1,   5,    1200,   "day_10",     "N",    0,     50),
                (1000, "Production Staff",             "1st AD",                      True,  1,   5,    900,    "day_10",     "N",    0,     60),
                (1000, "Production Staff",             "2nd AD",                      True,  1,   5,    650,    "day_10",     "N",    0,     70),
                (1000, "Production Staff",             "Production Coordinator",      True,  1,   5,    600,    "day_10",     "N",    0,     80),
                (1000, "Production Staff",             "Production Assistant",        True,  3,   5,    300,    "day_10",     "N",    0,     90),
                (1000, "Production Staff",             "Camera Operator",             True,  2,   5,    900,    "day_10",     "N",    0,     100),
                (1000, "Production Staff",             "Gaffer",                      True,  1,   5,    850,    "day_10",     "I",    0,     110),
                (1000, "Production Staff",             "Sound Mixer",                 True,  1,   5,    950,    "day_10",     "I",    0,     120),
                (1200, "Post-Production Staff",        "Editor",                      True,  1,   10,   750,    "day_10",     "N",    0,     130),
                # ── Camera Equipment ──────────────────────────────────────────
                (2000, "Camera Equipment",             "Camera Package Rental",       False, 2,   5,    1500,   "day_10",     "N",    0,     140),
                (2000, "Camera Equipment",             "Media / Hard Drives",         False, 1,   1,    350,    "day_10",     "N",    0,     150),
                # ── Grip & Electric ───────────────────────────────────────────
                (3000, "Grip & Electric",              "Lighting Package",            False, 1,   5,    1200,   "day_10",     "N",    0,     160),
                (3000, "Grip & Electric",              "Grip Package",                False, 1,   5,    600,    "day_10",     "N",    0,     170),
                # ── Sound ─────────────────────────────────────────────────────
                (3300, "Sound",                        "Sound Package Rental",        False, 1,   5,    500,    "day_10",     "N",    0,     180),
                # ── Art ───────────────────────────────────────────────────────
                (4000, "Art",                          "Prop Rentals",                False, 1,   1,    800,    "day_10",     "N",    0,     190),
                (4000, "Art",                          "Set Dressing Materials",      False, 1,   1,    500,    "day_10",     "N",    0,     200),
                # ── Hair & Makeup ─────────────────────────────────────────────
                (4500, "Hair & Makeup",                "Hair Stylist",                True,  1,   3,    700,    "day_10",     "N",    0,     210),
                (4500, "Hair & Makeup",                "Makeup Artist",               True,  1,   3,    700,    "day_10",     "N",    0,     220),
                # ── Transportation ────────────────────────────────────────────
                (6000, "Transportation",               "15-Passenger Van Rental",     False, 1,   5,    200,    "day_10",     "N",    0,     230),
                (6000, "Transportation",               "Fuel & Parking",              False, 1,   5,    80,     "day_10",     "N",    0,     240),
                # ── Travel ────────────────────────────────────────────────────
                (7000, "Travel",                       "Hotel — Crew (est.)",         False, 6,   4,    150,    "day_10",     "N",    0,     250),
                # ── Production Meals / Craft Services ─────────────────────────
                # First Meal / Second Meal / Courtesy Breakfast auto-created by sync when schedule meals checked
                (8000, "Production Meals / Craft Services", "Craft Services",         False, 1,   5,    200,    "day_10",     "N",    0,     270),
                # ── Location ──────────────────────────────────────────────────
                (9000, "Location",                     "Studio / Stage Rental",       False, 1,   3,    2000,   "day_10",     "N",    0,     280),
                # ── Administrative ────────────────────────────────────────────
                (15000,"Administrative",               "Petty Cash / Miscellaneous",  False, 1,   1,    1000,   "day_10",     "N",    0,     290),
            ]
            for row in _test_lines:
                code, acct, desc, labor, qty, days, rate, rt, fringe, agent, sort = row
                est = 0 if labor else round(rate * qty * days, 2)
                db.session.add(BudgetTemplateLine(
                    template_id=tmpl_t.id,
                    account_code=code,
                    account_name=acct,
                    description=desc,
                    is_labor=labor,
                    quantity=qty,
                    days=days,
                    rate=rate,
                    rate_type=rt,
                    fringe_type=fringe,
                    agent_pct=agent,
                    estimated_total=est,
                    sort_order=sort,
                ))
            db.session.commit()
            logging.info(f"Seeded template: {_TEST}")

        # ── Small Live Production ─────────────────────────────────────────────
        _SMALL_LIVE = "Small Live Production"
        if BudgetTemplate.query.filter_by(name=_SMALL_LIVE).first():
            return
        tmpl = BudgetTemplate(name=_SMALL_LIVE,
                              description="Single-day live event: small crew, control room, no post")
        db.session.add(tmpl)
        db.session.flush()
        _lines = [
            # code, name,                         desc,                       labor, qty, days, rate,   rt,        fringe, agent, sort
            (100,  "Pre-Production Locations",    "Tech Scout",               False, 1,   1,    500,    "day_10",  "N",    0,     0),
            (600,  "Above the Line",              "Director",                 True,  1,   1,    1500,   "day_10",  "E",    0,     10),
            (700,  "Talent",                      "Host",                     True,  1,   1,    1000,   "day_10",  "N",    0.10,  20),
            (1000, "Production Staff",            "UPM",                      True,  1,   1,    1000,   "day_10",  "N",    0,     30),
            (1000, "Production Staff",            "Key PA",                   True,  1,   1,    350,    "day_10",  "N",    0,     40),
            (1000, "Production Staff",            "Camera Operator",          True,  2,   1,    900,    "day_10",  "N",    0,     50),
            (1000, "Production Staff",            "Video Engineer",            True,  1,   1,    750,    "day_10",  "N",    0,     60),
            (1000, "Production Staff",            "Sound Mixer",              True,  1,   1,    900,    "day_10",  "N",    0,     70),
            (2000, "Camera Equipment",            "Camera Package Rental",    False, 3,   1,    1500,   "day_10",  "N",    0,     80),
            (2000, "Camera Equipment",            "Lens Kit Rental",          False, 3,   1,    500,    "day_10",  "N",    0,     90),
            (2000, "Camera Equipment",            "Monitor Rental",           False, 4,   1,    150,    "day_10",  "N",    0,     100),
            (2000, "Camera Equipment",            "Media Cards / Hard Drives",False, 1,   1,    300,    "day_10",  "N",    0,     110),
            (2000, "Camera Equipment",            "Camera Expendables",       False, 1,   1,    100,    "day_10",  "N",    0,     120),
            (3000, "Grip & Electric",             "Lighting Package",         False, 1,   1,    1500,   "day_10",  "N",    0,     130),
            (3000, "Grip & Electric",             "Grip Package",             False, 1,   1,    800,    "day_10",  "N",    0,     140),
            (3100, "Processing",                  "SDI Distribution Amp",     False, 1,   1,    200,    "day_10",  "N",    0,     150),
            (3100, "Processing",                  "Encoder / Decoder Unit",   False, 1,   1,    600,    "day_10",  "N",    0,     160),
            (3200, "Control Room",                "Control Room Rental",      False, 1,   1,    2000,   "day_10",  "N",    0,     170),
            (3200, "Control Room",                "Video Playback System",    False, 1,   1,    500,    "day_10",  "N",    0,     180),
            (3200, "Control Room",                "Switcher / Mixer Rental",  False, 1,   1,    400,    "day_10",  "N",    0,     190),
            (3300, "Sound",                       "Sound Package Rental",     False, 1,   1,    600,    "day_10",  "N",    0,     200),
            (3300, "Sound",                       "Wireless Mic Kit",         False, 1,   1,    200,    "day_10",  "N",    0,     210),
            (6000, "Transportation",              "Production Car",           False, 1,   1,    100,    "day_10",  "N",    0,     220),
            (6000, "Transportation",              "Fuel",                     False, 1,   1,    100,    "day_10",  "N",    0,     230),
            (6000, "Transportation",              "Parking",                  False, 1,   1,    50,     "day_10",  "N",    0,     240),
            (6000, "Transportation",              "Mileage Reimbursement",    False, 1,   1,    200,    "day_10",  "N",    0,     250),
            (8000, "Production Meals / Craft Services", "Catering (Lunch)",   False, 30,  1,    25,     "day_10",  "N",    0,     260),
        ]
        for row in _lines:
            code, acct, desc, labor, qty, days, rate, rt, fringe, agent, sort = row
            est = 0 if labor else round(rate * qty * days, 2)
            db.session.add(BudgetTemplateLine(
                template_id=tmpl.id,
                account_code=code,
                account_name=acct,
                description=desc,
                is_labor=labor,
                quantity=qty,
                days=days,
                rate=rate,
                rate_type=rt,
                fringe_type=fringe,
                agent_pct=agent,
                estimated_total=est,
                sort_order=sort,
            ))
        db.session.commit()
        logging.info(f"Seeded template: {_SMALL_LIVE}")

    _seed_production_templates()


# ─────────────────────────────────────────────────────────────────────────────
# DOCS MODULE — Receipt / Document Upload
# ─────────────────────────────────────────────────────────────────────────────

def _docs_accessible_projects(user):
    """Return list of ProjectSheet rows visible to this user for docs."""
    if user.role in ('super_admin', 'admin'):
        return ProjectSheet.query.order_by(ProjectSheet.name).all()
    owned = (db.session.query(ProjectSheet)
             .join(ProjectAccess, ProjectAccess.project_id == ProjectSheet.id)
             .filter(ProjectAccess.user_id == user.id)
             .order_by(ProjectSheet.name).all())
    return owned


@app.route("/docs/")
@login_required
def docs_dashboard():
    projects = _docs_accessible_projects(current_user)
    # Attach upload counts
    counts = {}
    if projects:
        pids = [p.id for p in projects]
        rows = (db.session.query(DocUpload.project_id, func.count(DocUpload.id))
                .filter(DocUpload.project_id.in_(pids))
                .group_by(DocUpload.project_id).all())
        counts = {pid: cnt for pid, cnt in rows}
    return render_template("docs_dashboard.html", projects=projects, counts=counts)


@app.route("/docs/<int:pid>/")
@login_required
def docs_project(pid):
    project = ProjectSheet.query.get_or_404(pid)
    # Access check
    if current_user.role not in ('super_admin', 'admin'):
        access = ProjectAccess.query.filter_by(project_id=pid, user_id=current_user.id).first()
        if not access:
            abort(403)
    uploads = (DocUpload.query
               .filter_by(project_id=pid)
               .order_by(DocUpload.uploaded_at.desc()).all())
    return render_template("docs_upload.html", project=project, uploads=uploads)


@app.route("/docs/<int:pid>/upload", methods=["POST"])
@login_required
def docs_upload_post(pid):
    project = ProjectSheet.query.get_or_404(pid)
    if current_user.role not in ('super_admin', 'admin'):
        access = ProjectAccess.query.filter_by(project_id=pid, user_id=current_user.id).first()
        if not access:
            return jsonify({"error": "Forbidden"}), 403

    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify({"error": "No file provided"}), 400

    import hashlib, uuid as _uuid
    data = f.read()
    file_hash = hashlib.sha256(data).hexdigest()

    # Duplicate check within this project
    existing = DocUpload.query.filter_by(project_id=pid, file_hash=file_hash).first()
    if existing:
        return jsonify({
            "status": "duplicate",
            "upload_id": existing.id,
            "message": "This file has already been uploaded."
        }), 200

    content_type = f.content_type or "application/octet-stream"
    ext = os.path.splitext(f.filename)[1].lower() if f.filename else ""
    r2_key = f"docs/{pid}/{_uuid.uuid4().hex}{ext}"

    try:
        _r2_upload(r2_key, data, content_type)
    except Exception as e:
        logging.exception("R2 upload failed")
        return jsonify({"error": f"Upload failed: {str(e)}"}), 500

    upload = DocUpload(
        project_id=pid,
        uploader_id=current_user.id,
        r2_key=r2_key,
        original_filename=f.filename,
        file_size=len(data),
        content_type=content_type,
        file_hash=file_hash,
        status="pending",
    )
    db.session.add(upload)
    db.session.commit()

    return jsonify({"status": "ok", "upload_id": upload.id}), 201


@app.route("/docs/upload/<int:uid>/status")
@login_required
def docs_upload_status(uid):
    upload = DocUpload.query.get_or_404(uid)
    if current_user.role not in ('super_admin', 'admin'):
        access = ProjectAccess.query.filter_by(
            project_id=upload.project_id, user_id=current_user.id).first()
        if not access:
            return jsonify({"error": "Forbidden"}), 403
    return jsonify({
        "id": upload.id,
        "status": upload.status,
        "original_filename": upload.original_filename,
        "vendor": upload.vendor,
        "amount": float(upload.amount) if upload.amount else None,
        "doc_date": upload.doc_date.isoformat() if upload.doc_date else None,
        "confidence": float(upload.confidence) if upload.confidence else None,
        "category": upload.category,
        "is_duplicate": upload.is_duplicate,
        "filed_dropbox_path": upload.filed_dropbox_path,
    })


@app.route("/docs/upload/<int:uid>/delete", methods=["POST"])
@login_required
def docs_upload_delete(uid):
    upload = DocUpload.query.get_or_404(uid)
    if current_user.role not in ('super_admin', 'admin'):
        if upload.uploader_id != current_user.id:
            return jsonify({"error": "Forbidden"}), 403
    pid = upload.project_id
    # Remove from R2
    try:
        _r2_client().delete_object(Bucket=_R2_BUCKET, Key=upload.r2_key)
    except Exception:
        pass
    db.session.delete(upload)
    db.session.commit()
    return jsonify({"status": "deleted"})


if __name__ == "__main__":
    app.run(debug=True, port=5001)
