import os, logging, json, csv, io, re

# ── ssl.SSLContext property recursion fix ─────────────────────────────────────
# Python 3.12+ rewrote several SSLContext property setters (options,
# verify_mode, verify_flags, minimum_version, maximum_version) as
# Python-level getters/setters that do
# `super(SSLContext, SSLContext).<attr>.__set__(self, value)`. The
# `SSLContext` name is re-resolved from ssl.py's module globals at every
# call, so if ANY library (gevent's monkey-patch, truststore, etc.) has
# rebound `ssl.SSLContext` to a subclass, super() walks back to the original
# SSLContext whose setter does the same thing — infinite recursion. We
# observed this repeatedly in production on Python 3.13 (ssl.py:561 for
# `options`, ssl.py:679 for `verify_mode`) blowing up every boto3 client
# build and any urllib3-based HTTPS call (Dropbox SDK, Anthropic SDK, etc.).
#
# Fix: replace each broken Python-level property with one that forwards
# directly to the C-level `_ssl._SSLContext` slot descriptor. Cannot
# recurse because C-level setters aren't re-entering Python code.
#
# Must happen BEFORE boto3/urllib3/requests/dropbox/anthropic are imported.
import ssl as _ssl_mod
try:
    import _ssl as _c_ssl_mod
    _C_SSL = _c_ssl_mod._SSLContext
    def _make_passthrough_property(attr_name):
        desc = getattr(_C_SSL, attr_name)
        def _get(self): return desc.__get__(self, type(self))
        def _set(self, value): desc.__set__(self, value)
        return property(_get, _set)
    for _attr in ('options', 'verify_mode', 'verify_flags',
                  'minimum_version', 'maximum_version',
                  'check_hostname', 'post_handshake_auth',
                  'security_level'):
        try:
            # Only patch if the C-level descriptor actually exists (older
            # Python builds may not have all of these).
            getattr(_C_SSL, _attr)
        except AttributeError:
            continue
        try:
            setattr(_ssl_mod.SSLContext, _attr, _make_passthrough_property(_attr))
        except (TypeError, AttributeError) as _e:
            import sys as _sys
            print(f"[ssl-fix] could not patch SSLContext.{_attr}: {_e!r}",
                  file=_sys.stderr)
except Exception as _e:
    import sys as _sys
    print(f"[ssl-fix] fatal: could not set up SSLContext patches: {_e!r}",
          file=_sys.stderr)

# NOTE: SSLSocket._create patch attempted and reverted — caused the
# worker to hang during import on Render (no crash, just no port bind).
# Dropbox calls still fail with the super(SSLSocket, self) type-check
# error. Next approach will isolate Dropbox calls into a native thread
# pool so they run with the un-monkey-patched socket module.

# ── R2 / botocore checksum env vars ──────────────────────────────────────────
# MUST be set BEFORE boto3/botocore is ever imported. botocore reads these
# during module init and caches them. Cloudflare R2 rejects the new
# flexible-checksum trailers (Content-CRC32 etc.) introduced in botocore
# 1.36; boto3's retry path re-issues the request inside the same exception
# handler, which under the gevent monkey-patched threadlocals walks itself
# into "maximum recursion depth exceeded" instead of returning a normal
# error. Setting these env vars disables checksums regardless of the
# Config object that's passed to boto3.client (which we also still set as
# a belt-and-suspenders guard, because new botocore versions sometimes
# rename the kwargs and the env vars are the stable interface).
os.environ.setdefault('AWS_REQUEST_CHECKSUM_CALCULATION',  'when_required')
os.environ.setdefault('AWS_RESPONSE_CHECKSUM_VALIDATION',  'when_required')
# Some installs honour the older opt-in name; set both for safety.
os.environ.setdefault('AWS_S3_DISABLE_DEFAULT_CHECKSUMS',  'true')

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
                    BudgetDirectContact, CompanySettings, DocUpload, CatalogItem)
from budget_calc import (calc_line, calc_line_from_schedule, calc_top_sheet,
                         get_fringe_configs, seed_fringes, seed_standard_template,
                         seed_catalog, seed_payroll_profiles, FP_COA_SECTIONS, DAY_TYPE_MULTIPLIERS,
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
    ("Supervising Producer",       "Production"),
    ("Production Supervisor",      "Production"),
    ("Production Coordinator",     "Production"),
    ("Production Manager",         "Production"),
    ("Production Accountant",      "Production"),
    ("Payroll Coordinator",        "Production"),
    ("Travel Coordinator",         "Production"),
    ("APOC",                       "Production"),
    ("Production Secretary",       "Production"),
    ("Live Director",              "Direction / AD"),
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
    ("Robotic Camera Operator",    "Camera"),
    ("Camera Operator",            "Camera"),
    ("1st AC",                     "Camera"),
    ("2nd AC",                     "Camera"),
    ("DIT",                        "Camera"),
    ("Steadicam",                  "Camera"),
    ("Data Wrangler",              "Camera"),
    ("Video Engineer",             "Camera"),
    ("VTR Operator",               "Camera"),
    ("Lighting Designer",          "Grip & Electric"),
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
    ("Technical Producer",         "Control Room"),
    ("Technical Director",         "Control Room"),
    ("Graphics and Playback",      "Control Room"),
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


# ── COA section helpers ──────────────────────────────────────────────────────
# All business logic that used to pin integer account codes (700, 1000, 14000,
# 15000, ...) now goes through these helpers. The helpers resolve codes by
# section NAME from FP_COA_SECTIONS at startup, so any future renumber would
# only need the list updated — these helpers would still work.
#
# The 2026-04 renumber immutably fixed codes as follows:
#   2000 = Production Staff (crew + ATL roles per phase)
#   2100 = Talent
#   4000 = Post-Production Staff
#   6000 = Insurance      (Workers' Comp auto-inject target)
#   6500 = Administrative (Payroll Fee auto-inject target)
#   3500 = Travel
#   3700 = Production Meals & Craft Services

def _coa_code_by_name(name, default=None):
    for code, sec_name in FP_COA_SECTIONS:
        if sec_name == name:
            return code
    return default

# Module-level constants resolved at import time. If FP_COA_SECTIONS changes,
# restart required (same as before).
COA_CODE_PROD_STAFF = _coa_code_by_name("Production Staff", 2000)
COA_CODE_TALENT     = _coa_code_by_name("Talent", 2100)
COA_CODE_POST_STAFF = _coa_code_by_name("Post-Production Staff", 4000)
COA_CODE_INSURANCE  = _coa_code_by_name("Insurance", 6000)           # WC auto-inject
COA_CODE_ADMIN      = _coa_code_by_name("Administrative", 6500)       # Payroll Fee auto-inject
COA_CODE_TRAVEL     = _coa_code_by_name("Travel", 3500)
COA_CODE_MEALS      = _coa_code_by_name("Production Meals & Craft Services", 3700)
COA_CODE_LOCATIONS  = _coa_code_by_name("Locations", 3300)
COA_CODE_DEV_LABOR  = _coa_code_by_name("Development Labor", 1100)

# Known ATL role labels (used ONLY for display grouping — call sheets, PDF).
# Authoritative ATL classification lives on CatalogItem.group_name /
# RoleTagMapping once Task 2 lands; this set is the fallback for existing
# BudgetLines with no catalog link.
_ATL_ROLE_LABELS = {
    "director", "executive producer", "producer", "creative director",
    "writer", "co-producer", "co-executive producer", "line producer",
}

def _is_atl_line(ln):
    """True if a BudgetLine describes an ATL role (description-based fallback)."""
    desc = (getattr(ln, 'description', None) or '').strip().lower()
    if not desc:
        return False
    for token in _ATL_ROLE_LABELS:
        if token in desc:
            return True
    return False


# ── Best-guess MMB / ShowBiz target mappings ────────────────────────────────
# Used by the Task 2 seed to populate initial RoleTagMapping rows. Super
# admin refines in /admin/role-mapping. Numbers follow Movie Magic /
# ShowBiz industry conventions; they are DEFAULTS only — production
# budgets will override per their specific MMB/ShowBiz template.
#
# MMB structure reference (abridged):
#   1100-1899   Above-the-Line (1100=Writer, 1300=Producer, 1500=Director)
#   2000-2899   Below-the-Line Production (2000=Prod Staff, 2100=Extra Talent,
#                                          2300=Camera, 2500=Set Construction,
#                                          2800=Wardrobe, Makeup, etc.)
#   3000-3899   Post-Production (3000=Film Editing, 3200=Music, 3400=Sound)
#   4000-4899   Other (Insurance, Publicity, Legal, etc.)
#
# ShowBiz structure: very similar with slightly different numbering (e.g.
# 1100=Writers, 1300=Producers, 2000=Extras, 2200=Production Staff, etc.).
# Export/writer modules will document exact column layouts per format.

def _guess_mmb_target(ci):
    """Return (mmb_code:str, mmb_name:str) for a CatalogItem row."""
    code = int(getattr(ci, 'category_code', 0) or 0)
    label = (getattr(ci, 'label', '') or '').lower()
    # Check ATL labels FIRST — they should land in the MMB ATL range
    # regardless of which internal bucket they live in (could be 1100/2000/4000).
    if 'director' in label and 'asst' not in label and 'of photography' not in label:
        return ('1500-00', 'Director')
    if 'producer' in label:
        if 'executive' in label:
            return ('1310-00', 'Executive Producer')
        return ('1300-00', 'Producer')
    if 'writer' in label:
        return ('1100-00', 'Writer')
    # Internal COA → MMB target
    _INTERNAL_TO_MMB = {
        2000: ('2000-00', 'Production Staff'),
        2100: ('2100-00', 'Principal Talent'),
        2200: ('2200-00', 'Casting'),
        2300: ('2300-00', 'Extra Talent'),  # rehearsal-adjacent
        2600: ('2300-00', 'Camera Equipment'),
        2700: ('2500-00', 'Set Lighting / Grip'),
        2800: ('2600-00', 'Production Sound'),
        2900: ('2550-00', 'Technical / Control Room'),
        3000: ('2400-00', 'Art Department'),
        3100: ('2800-00', 'Hair & Makeup'),
        3200: ('2850-00', 'Wardrobe'),
        3300: ('2200-00', 'Locations'),
        3400: ('2900-00', 'Transportation'),
        3500: ('4000-00', 'Travel & Living'),
        3600: ('4100-00', 'Shipping'),
        3700: ('2700-00', 'Crafts / Food'),
        3800: ('2900-00', 'Sanitation / Other'),
        4000: ('3000-00', 'Post-Production Editorial'),
        4500: ('3100-00', 'Post-Production Equipment'),
        4600: ('3200-00', 'Post-Production Facilities'),
        4700: ('3300-00', 'Post-Production Services'),
        4800: ('3400-00', 'Music'),
        4900: ('3500-00', 'Titles / Stock Footage'),
        5000: ('3600-00', 'Lab / Processing'),
        6000: ('4500-00', 'Insurance'),
        6100: ('4400-00', 'Legal / Accounting'),
        6200: ('4600-00', 'Distribution'),
        6300: ('4700-00', 'Publicity'),
        6400: ('4300-00', 'Office / Admin'),
        6500: ('4300-00', 'General Administration'),
        6600: ('4200-00', 'Residuals'),
        6700: ('4800-00', 'Miscellaneous'),
        6800: ('4999-00', 'Producer Fee'),
        1000: ('5100-00', 'Development Costs'),
        1100: ('5100-00', 'Development Labor'),
    }
    return _INTERNAL_TO_MMB.get(code, ('', ''))


def _guess_showbiz_target(ci):
    """Return (showbiz_code:str, showbiz_name:str) for a CatalogItem row.
    ShowBiz numbering is close to MMB but not identical — using its own
    variant. Super admin refines in /admin/role-mapping.
    """
    code = int(getattr(ci, 'category_code', 0) or 0)
    label = (getattr(ci, 'label', '') or '').lower()
    if 'director' in label and 'asst' not in label and 'of photography' not in label:
        return ('1500', 'Director')
    if 'producer' in label:
        if 'executive' in label:
            return ('1310', 'Executive Producer')
        return ('1300', 'Producer')
    if 'writer' in label:
        return ('1100', 'Writer')
    _INTERNAL_TO_SHOWBIZ = {
        2000: ('2200', 'Production Staff'),
        2100: ('2000', 'Principal Talent'),
        2200: ('2050', 'Casting'),
        2300: ('2000', 'Extras & Rehearsal'),
        2600: ('2350', 'Camera'),
        2700: ('2500', 'Grip / Electric'),
        2800: ('2600', 'Sound'),
        2900: ('2700', 'Technical / Control Room'),
        3000: ('2300', 'Art Department'),
        3100: ('2800', 'Hair & Makeup'),
        3200: ('2850', 'Wardrobe'),
        3300: ('2250', 'Locations'),
        3400: ('2900', 'Transportation'),
        3500: ('4000', 'Travel & Living'),
        3600: ('4100', 'Shipping'),
        3700: ('2700', 'Food'),
        3800: ('2950', 'Sanitation / Other'),
        4000: ('3100', 'Editorial'),
        4500: ('3150', 'Post Equipment'),
        4600: ('3200', 'Post Facilities'),
        4700: ('3250', 'Post Services'),
        4800: ('3400', 'Music'),
        4900: ('3500', 'Titles / Stock'),
        5000: ('3600', 'Lab / Processing'),
        6000: ('4400', 'Insurance'),
        6100: ('4500', 'Legal / Accounting'),
        6200: ('4600', 'Distribution'),
        6300: ('4700', 'Publicity'),
        6400: ('4300', 'Office / Admin'),
        6500: ('4300', 'Administration'),
        6600: ('4200', 'Residuals'),
        6700: ('4800', 'Miscellaneous'),
        6800: ('4950', 'Producer Fee'),
        1000: ('5100', 'Development Costs'),
        1100: ('5100', 'Development Labor'),
    }
    return _INTERNAL_TO_SHOWBIZ.get(code, ('', ''))


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

# ── SocketIO ──────────────────────────────────────────────────────────────────
# async_mode='threading' (2026-04-20): we run under gunicorn's stock sync
# worker now (no gevent) so threading is the only async driver that works.
# This also forces the engineio layer to stop attempting websocket upgrades
# — all client traffic uses long-polling transport, which our sync worker
# handles fine. Gevent upgrades would 500 under the sync worker.
try:
    from flask_socketio import SocketIO, emit, join_room, leave_room
    socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")
    _HAS_SOCKETIO = True
except ImportError:
    socketio = None
    _HAS_SOCKETIO = False

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

_R2_VERSION_LOGGED = False
_R2_CLIENT_CACHE = None

def _r2_client():
    # Cache the client at module level. Building a boto3 client is expensive
    # (~100-500ms) and every build walks botocore's endpoint/SSL init — which
    # is exactly where the gevent ssl-monkey-patch recursion bug hits on
    # Python 3.12+. One build per worker, not per request.
    global _R2_CLIENT_CACHE
    if _R2_CLIENT_CACHE is not None:
        return _R2_CLIENT_CACHE
    import boto3, botocore
    from botocore.config import Config
    global _R2_VERSION_LOGGED
    if not _R2_VERSION_LOGGED:
        logging.warning(
            "[R2] boto3=%s botocore=%s checksum_env=%s",
            getattr(boto3, '__version__', '?'),
            getattr(botocore, '__version__', '?'),
            os.environ.get('AWS_REQUEST_CHECKSUM_CALCULATION'),
        )
        _R2_VERSION_LOGGED = True

    # Build Config kwargs incrementally so unknown kwargs on older botocore
    # don't blow away the entire config (the previous try/except wrapped
    # the WHOLE Config(), so any version mismatch silently fell back to a
    # config WITHOUT the checksum suppression and the recursion bug came
    # back). Try each new kwarg individually.
    base_kwargs = dict(
        signature_version='s3v4',
        s3={'addressing_style': 'path'},
        retries={'max_attempts': 3, 'mode': 'standard'},
    )
    for opt_kwarg in ('request_checksum_calculation', 'response_checksum_validation'):
        try:
            Config(**{**base_kwargs, opt_kwarg: 'when_required'})
            base_kwargs[opt_kwarg] = 'when_required'
        except TypeError:
            logging.warning("[R2] botocore Config does not accept %s — env var fallback active", opt_kwarg)

    cfg = Config(**base_kwargs)
    _R2_CLIENT_CACHE = boto3.client(
        's3',
        endpoint_url=f"https://{_R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=_R2_ACCESS_KEY,
        aws_secret_access_key=_R2_SECRET,
        region_name='auto',
        config=cfg,
    )
    return _R2_CLIENT_CACHE

def _r2_upload(file_bytes, key, content_type='application/octet-stream'):
    """Upload bytes to R2. Returns True on success."""
    try:
        _r2_client().put_object(Bucket=_R2_BUCKET, Key=key, Body=file_bytes, ContentType=content_type)
        return True
    except RecursionError:
        # botocore got into a retry loop (usually checksum-related on R2).
        # Surface the cause clearly so we don't waste another debugging
        # cycle thinking the env var fix is in place when it isn't.
        logging.exception("[R2] RecursionError — checksum suppression NOT effective. "
                          "Verify boto3/botocore version and AWS_REQUEST_CHECKSUM_CALCULATION env.")
        return False
    except Exception:
        logging.exception("R2 upload failed")
        return False


def _r2_download(key):
    """Download bytes from R2 by key. Returns (bytes, None) on success or
    (None, error_message) on failure — never raises, so callers don't have
    to deal with boto3's recursion/retry quirks on missing objects."""
    try:
        resp = _r2_client().get_object(Bucket=_R2_BUCKET, Key=key)
        return (resp['Body'].read(), None)
    except Exception as e:
        # Common cases: NoSuchKey (the file was never actually stored — happens
        # for rows created before the arg-swap bug fix), credentials missing,
        # or a genuine network error. Collapse all into a friendly string.
        msg = str(e).lower()
        if 'nosuchkey' in msg or 'not found' in msg or '404' in msg:
            return (None, "File data missing from storage. Delete this row and re-upload.")
        if 'recursion' in msg or 'endpoint' in msg:
            return (None, "R2 storage is unreachable (check R2 env vars on Render).")
        return (None, f"R2 fetch failed: {str(e)[:200]}")


def _r2_presigned_url(key, expires=3600):
    """Return a presigned URL for a private R2 object (expires in `expires` seconds)."""
    try:
        return _r2_client().generate_presigned_url(
            'get_object', Params={'Bucket': _R2_BUCKET, 'Key': key}, ExpiresIn=expires)
    except Exception as e:
        logging.warning(f"R2 presigned URL failed: {e}")
        return None


# ── Dropbox helpers ────────────────────────────────────────────────────────────
_DBX_TEMPLATE_NAME  = os.getenv('DROPBOX_TEMPLATE_FOLDER', '!_PRODUCTION_PROJECT_TEMPLATE')
_DBX_NAMESPACE_ID   = os.getenv('DROPBOX_NAMESPACE_ID', '')   # shared folder namespace ID
# Legacy path-based fallback (used only when namespace ID is not set).
# Paths are relative to the authenticated Dropbox user's root — if the app
# is connected as Steven, the prefix `/Steven Pierce/` would DUPLICATE and
# land everything under `Steven Pierce/Steven Pierce/...`. Keep it root-
# relative.
_DBX_OPS_ROOT       = os.getenv('DROPBOX_OPERATIONS_PATH', '/_FP OPERATIONS FOLDER')
# Archive and Wrap destinations live as SIBLINGS of _FP OPERATIONS FOLDER
# (not children). Both exist at the top of the Dropbox user root.
_DBX_ARCHIVE_ROOT   = os.getenv('DROPBOX_ARCHIVE_PATH',   '/_ARCHIVED')
_DBX_WRAP_ROOT      = os.getenv('DROPBOX_WRAP_PATH',      '/_WRAPPED PROJECTS')

def _dbx_client():
    import dropbox as _dbx_mod
    refresh_token = os.getenv('DROPBOX_REFRESH_TOKEN', '')
    app_key       = os.getenv('DROPBOX_APP_KEY', '')
    app_secret    = os.getenv('DROPBOX_APP_SECRET', '')
    if refresh_token and app_key and app_secret:
        dbx = _dbx_mod.Dropbox(
            oauth2_refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret,
        )
    else:
        dbx = _dbx_mod.Dropbox(os.getenv('DROPBOX_ACCESS_TOKEN', ''))
    # Route into the shared folder namespace so paths like /!_TEMPLATE work directly
    if _DBX_NAMESPACE_ID:
        from dropbox.common import PathRoot
        dbx = dbx.with_path_root(PathRoot.namespace_id(_DBX_NAMESPACE_ID))
    return dbx

def _dbx_paths(dropbox_folder):
    """Return (src, dest) paths for provision, adjusted for namespace mode."""
    if _DBX_NAMESPACE_ID:
        # Namespace root IS the ops folder — paths are relative to it
        src  = f"/{_DBX_TEMPLATE_NAME}"
        dest = f"/{dropbox_folder}"
    else:
        src  = f"{_DBX_OPS_ROOT}/{_DBX_TEMPLATE_NAME}"
        dest = f"{_DBX_OPS_ROOT}/{dropbox_folder}"
    return src, dest

def _provision_dropbox_folder(dropbox_folder):
    """Copy the project template tree to a new project folder. Returns path or None."""
    has_refresh = os.getenv('DROPBOX_REFRESH_TOKEN') and os.getenv('DROPBOX_APP_KEY')
    if not has_refresh and not os.getenv('DROPBOX_ACCESS_TOKEN'):
        logging.warning("[DBX PROVISION] skipped — no credentials set")
        return None
    try:
        src, dest = _dbx_paths(dropbox_folder)
        logging.warning(f"[DBX PROVISION] copying '{src}' → '{dest}' "
                        f"(OPS_ROOT={_DBX_OPS_ROOT!r}, TEMPLATE={_DBX_TEMPLATE_NAME!r}, "
                        f"NAMESPACE={_DBX_NAMESPACE_ID!r})")
        _dbx_client().files_copy_v2(src, dest)
        logging.warning(f"[DBX PROVISION] success → {dest}")
        return dest
    except Exception as e:
        # Log with full type + message so Render logs clearly show what broke.
        import traceback as _tb
        logging.error(f"[DBX PROVISION] FAILED for slug={dropbox_folder!r}: "
                      f"{type(e).__name__}: {e}\n{_tb.format_exc()}")
        return None


@app.route("/admin/dropbox/status")
@login_required
def admin_dropbox_status():
    # Super admin check inline — super_admin_required decorator is defined
    # further down in this module, can't apply it at import time here.
    if not current_user.is_authenticated or getattr(current_user, 'role', None) != 'super_admin':
        return jsonify({"error": "super admin only"}), 403
    """Diagnostic: show current Dropbox config + test connection + check
    the template folder exists. Super-admin only. Paste the JSON back to
    the engineer when project folders aren't being created."""
    out = {
        "env": {
            "DROPBOX_REFRESH_TOKEN_set": bool(os.getenv('DROPBOX_REFRESH_TOKEN')),
            "DROPBOX_APP_KEY_set":       bool(os.getenv('DROPBOX_APP_KEY')),
            "DROPBOX_ACCESS_TOKEN_set":  bool(os.getenv('DROPBOX_ACCESS_TOKEN')),
            "DROPBOX_OPERATIONS_PATH":   os.getenv('DROPBOX_OPERATIONS_PATH'),
            "DROPBOX_TEMPLATE_FOLDER":   os.getenv('DROPBOX_TEMPLATE_FOLDER'),
            "DROPBOX_NAMESPACE_ID":      os.getenv('DROPBOX_NAMESPACE_ID'),
            "DROPBOX_ARCHIVE_PATH":      os.getenv('DROPBOX_ARCHIVE_PATH'),
            "DROPBOX_WRAP_PATH":         os.getenv('DROPBOX_WRAP_PATH'),
        },
        "resolved": {
            "_DBX_OPS_ROOT":      _DBX_OPS_ROOT,
            "_DBX_TEMPLATE_NAME": _DBX_TEMPLATE_NAME,
            "_DBX_NAMESPACE_ID":  _DBX_NAMESPACE_ID,
            "_DBX_ARCHIVE_ROOT":  _DBX_ARCHIVE_ROOT,
            "template_source_path": (f"/{_DBX_TEMPLATE_NAME}" if _DBX_NAMESPACE_ID
                                     else f"{_DBX_OPS_ROOT}/{_DBX_TEMPLATE_NAME}"),
        },
        "connection": None,
        "template_folder_exists": None,
        "template_folder_error": None,
    }
    # Try authenticating + listing the template folder.
    has_refresh = os.getenv('DROPBOX_REFRESH_TOKEN') and os.getenv('DROPBOX_APP_KEY')
    if not has_refresh and not os.getenv('DROPBOX_ACCESS_TOKEN'):
        out["connection"] = "NO CREDENTIALS"
        return jsonify(out)
    try:
        dbx = _dbx_client()
        # Sanity: who are we authed as
        try:
            acct = dbx.users_get_current_account()
            out["connection"] = {
                "status": "connected",
                "account_email":  getattr(acct, 'email', None),
                "account_name":   getattr(getattr(acct, 'name', None), 'display_name', None),
            }
        except Exception as _ae:
            out["connection"] = f"auth failed: {type(_ae).__name__}: {_ae}"
            return jsonify(out)
        # Try to stat the template folder.
        template_path = out["resolved"]["template_source_path"]
        try:
            md = dbx.files_get_metadata(template_path)
            out["template_folder_exists"] = True
            out["template_folder_path_on_dropbox"] = getattr(md, 'path_display', None)
        except Exception as _te:
            out["template_folder_exists"] = False
            out["template_folder_error"] = f"{type(_te).__name__}: {_te}"
    except Exception as e:
        out["connection"] = f"unexpected: {type(e).__name__}: {e}"
    return jsonify(out)


@app.route("/admin/veryfi/status")
@login_required
def admin_veryfi_status():
    """Diagnostic: check Veryfi env vars + try a lightweight API call.
    Mirrors /admin/dropbox/status — paste the JSON back when uploads fail
    with 401/other Veryfi errors."""
    if not current_user.is_authenticated or getattr(current_user, 'role', None) != 'super_admin':
        return jsonify({"error": "super admin only"}), 403
    out = {
        "env": {
            "VERYFI_CLIENT_ID_set":     bool(os.getenv('VERYFI_CLIENT_ID')),
            "VERYFI_CLIENT_SECRET_set": bool(os.getenv('VERYFI_CLIENT_SECRET')),
            "VERYFI_USERNAME_set":      bool(os.getenv('VERYFI_USERNAME')),
            "VERYFI_API_KEY_set":       bool(os.getenv('VERYFI_API_KEY')),
        },
        "client_init": None,
        "test_call": None,
    }
    missing = [k for k, v in out["env"].items() if not v]
    if missing:
        out["client_init"] = f"MISSING ENV VARS: {', '.join(missing)}"
        return jsonify(out)
    try:
        import veryfi
        client = veryfi.Client(
            client_id=os.getenv("VERYFI_CLIENT_ID"),
            client_secret=os.getenv("VERYFI_CLIENT_SECRET"),
            username=os.getenv("VERYFI_USERNAME"),
            api_key=os.getenv("VERYFI_API_KEY"),
        )
        out["client_init"] = "ok"
        # Hit a lightweight endpoint that doesn't cost a document credit.
        # get_categories is a read-only listing call.
        try:
            cats = client.get_categories()
            # Some SDK versions return a list, others a dict. Normalize.
            if isinstance(cats, list):
                out["test_call"] = {
                    "status": "ok",
                    "category_count": len(cats),
                    "sample": cats[:3],
                }
            elif isinstance(cats, dict):
                out["test_call"] = {
                    "status": "ok",
                    "keys": list(cats.keys())[:10],
                }
            else:
                out["test_call"] = {"status": "ok", "type": str(type(cats))}
        except Exception as _te:
            out["test_call"] = f"{type(_te).__name__}: {_te}"
    except Exception as e:
        out["client_init"] = f"{type(e).__name__}: {e}"
    return jsonify(out)


@app.route("/admin/docs/project/<int:pid>/wipe", methods=["POST"])
@login_required
def admin_docs_wipe_project(pid):
    """Super admin testing tool: delete every DocUpload row for a project
    and move each filed Dropbox file to /_TRASH/{date}_{slug}/. Lets us
    do clean-start iteration while the Analyzer pipeline is still
    being refined. Dropbox files are MOVED (not deleted) so there's a
    recovery window if we trash something we shouldn't have.
    """
    if not current_user.is_authenticated or getattr(current_user, 'role', None) != 'super_admin':
        return jsonify({"error": "super admin only"}), 403

    project = ProjectSheet.query.get_or_404(pid)
    uploads = DocUpload.query.filter_by(project_id=pid).all()

    deleted_count = 0
    moved_count   = 0
    errors        = []
    trash_root    = None

    # Only bother with Dropbox if we have credentials.
    dbx = None
    has_refresh = os.getenv('DROPBOX_REFRESH_TOKEN') and os.getenv('DROPBOX_APP_KEY')
    if has_refresh or os.getenv('DROPBOX_ACCESS_TOKEN'):
        try:
            dbx = _dbx_client()
        except Exception as _de:
            logging.warning(f"[DOCS WIPE] dropbox client init failed: {_de}")
            dbx = None

    if dbx and uploads:
        # Build a dated trash folder specific to this wipe so we can find
        # the files again if a mistake was made.
        from datetime import datetime as _dt
        _stamp = _dt.utcnow().strftime("%Y-%m-%d_%H%M%S")
        _slug  = project.dropbox_folder or f"project-{pid}"
        trash_root = f"/_TRASH/{_stamp}_{_slug}"
        # Ensure trash folder exists (Dropbox auto-creates parent folders
        # on move, so this is best-effort).
        try:
            dbx.files_create_folder_v2(trash_root, autorename=False)
        except Exception:
            pass  # already exists or Dropbox handles it via move autocreate

    # When the app is in namespace mode, paths stored on DocUpload rows
    # may have been written by BOTH generations of fp_analyzer:
    #   (a) pre-namespace-fix: stored as '/Steven Pierce/_FP OPERATIONS FOLDER/...'
    #       (absolute user-root path). Passing this through the
    #       namespace-scoped client double-prepends → not_found.
    #   (b) post-namespace-fix: stored as '/{project}/...' (relative to
    #       the namespace root). Works as-is.
    # Strip the OPS prefix from (a)-style paths before calling move so
    # both generations end up at the same working path for the wipe.
    _ops_prefix_str = (_DBX_OPS_ROOT or "").rstrip("/") if _DBX_NAMESPACE_ID else ""

    def _normalize_for_namespace(p):
        """If p starts with the legacy ops prefix under namespace mode,
        strip it so the namespace client can locate the file."""
        if not p:
            return p
        if _ops_prefix_str and p.startswith(_ops_prefix_str + "/"):
            return p[len(_ops_prefix_str):]  # leave leading '/'
        return p

    for up in uploads:
        # Step 1: move the filed Dropbox file to trash, if present.
        if up.filed_dropbox_path and dbx and trash_root:
            original_src = up.filed_dropbox_path
            src = _normalize_for_namespace(original_src)
            _safe_path_frag = original_src.lstrip('/').replace('/', '_')
            dest = f"{trash_root}/{_safe_path_frag}"
            moved_ok = False
            # Try the normalized path first (covers both new and old uploads
            # after we've stripped the legacy prefix).
            try:
                dbx.files_move_v2(src, dest, autorename=True)
                moved_ok = True
            except Exception as _me1:
                err1 = str(_me1)
                # Fallback: try the original path verbatim. Covers edge
                # cases where the stored path is already correctly
                # namespace-relative and wasn't touched by the strip.
                if src != original_src:
                    try:
                        dbx.files_move_v2(original_src, dest, autorename=True)
                        moved_ok = True
                    except Exception as _me2:
                        errors.append(
                            f"upload {up.id} ({original_src}): "
                            f"both paths failed — normalized: {err1}; "
                            f"original: {_me2}"
                        )
                else:
                    errors.append(f"upload {up.id} ({original_src}): {err1}")
            if moved_ok:
                moved_count += 1
        # Step 2: delete the DB row regardless of Dropbox outcome.
        try:
            db.session.delete(up)
            deleted_count += 1
        except Exception as _re:
            errors.append(f"upload {up.id}: db delete failed: {_re}")

    try:
        db.session.commit()
    except Exception as _ce:
        db.session.rollback()
        errors.append(f"commit failed: {_ce}")
        return jsonify({
            "error": f"Commit failed, wipe partial: {_ce}",
            "deleted_count": 0,
            "moved_count": moved_count,
            "errors": errors[:20],
        }), 500

    return jsonify({
        "ok":            True,
        "deleted_count": deleted_count,
        "moved_count":   moved_count,
        "trash_folder":  trash_root,
        "error_count":   len(errors),
        "errors":        errors[:20],
    })


@app.route("/admin/docs/project/<int:pid>/reconcile", methods=["POST"])
@login_required
def admin_docs_reconcile_project(pid):
    """Walk the project's Dropbox doc folders and create DocUpload rows
    for any files present in Dropbox but missing from our DB.

    Motivation: if a user closes the browser tab mid-upload batch, the
    file can land in Dropbox (the Analyzer runs synchronously inside the
    request, but the browser may abort before the response is handled
    and the DocUpload row still commits — OR, in the legacy / pre-OCR
    pipeline, some files were filed manually). This tool gives us a way
    to reconcile the two sources of truth.

    Strategy:
      * For every subfolder in DOCUMENT_TYPES (+ the default receipts
        folder), list files recursively under the project root.
      * For each file, check whether a DocUpload row already references
        its path (filed_dropbox_path) for THIS project.
      * If not, create a status='filed' DocUpload row with whatever we
        can derive (filename, size, content-type, category from subfolder
        name). uploader_id = current user (best effort).

    Returns counts + sample orphan list. Does NOT move or re-OCR files.
    """
    if not current_user.is_authenticated or getattr(current_user, 'role', None) != 'super_admin':
        return jsonify({"error": "super admin only"}), 403

    project = ProjectSheet.query.get_or_404(pid)
    if not project.dropbox_folder:
        return jsonify({"error": "Project has no Dropbox folder configured"}), 400

    from fp_analyzer import DOCUMENT_TYPES
    try:
        dbx = _dbx_client()
    except Exception as e:
        return jsonify({"error": f"Dropbox client init failed: {e}"}), 500

    # Build the project root. Namespace vs legacy mode matters here —
    # mirrors the pattern used in docs_upload_retry_filing.
    proj_root = (f"/{project.dropbox_folder}"
                 if _DBX_NAMESPACE_ID
                 else f"{_DBX_OPS_ROOT}/{project.dropbox_folder}")

    # Collect the set of doc-type subfolders we care about. DOCUMENT_TYPES
    # gives us canonical paths; de-dupe because several types share a
    # folder (invoice + contract → CONTRACTS & INVOICES).
    subfolders = set(DOCUMENT_TYPES.values())
    subfolders.add("01_ADMIN/RECEIPTS FOLDER")  # Analyzer's default fallback

    # Pre-load existing filed paths so we can do set-membership checks
    # without re-hitting the DB per file.
    existing_paths = {
        (u.filed_dropbox_path or "").lower()
        for u in DocUpload.query.with_entities(DocUpload.filed_dropbox_path)
                                .filter_by(project_id=pid).all()
        if u.filed_dropbox_path
    }

    created = 0
    scanned = 0
    errors  = []
    sample  = []

    from datetime import datetime as _dt
    import mimetypes as _mt

    for sub in sorted(subfolders):
        folder_path = f"{proj_root}/{sub}"
        # Reverse-map subfolder → doc_type label for the category column.
        # Pick the first matching key (most DOCUMENT_TYPES values are
        # unique; shared ones collapse to whichever key sorts first).
        doc_type_for_folder = next(
            (k for k, v in DOCUMENT_TYPES.items() if v == sub),
            None,
        )

        try:
            res = dbx.files_list_folder(folder_path, recursive=True)
        except Exception as _le:
            _msg = str(_le)
            if "not_found" in _msg:
                continue  # folder doesn't exist for this project; skip silently
            errors.append(f"list {folder_path}: {_msg}")
            continue

        while True:
            for entry in res.entries:
                # Skip folders and deleted-metadata entries.
                if not hasattr(entry, 'size'):
                    continue
                scanned += 1
                path_lower_key = (entry.path_display or "").lower()
                if path_lower_key in existing_paths:
                    continue
                # Also check the namespace-normalized variant to handle
                # rows saved with the legacy ops prefix under namespace mode.
                if _DBX_NAMESPACE_ID and _DBX_OPS_ROOT:
                    alt = (_DBX_OPS_ROOT.rstrip('/') + (entry.path_display or "")).lower()
                    if alt in existing_paths:
                        continue

                filename = entry.name or ""
                ext = os.path.splitext(filename)[1].lower()
                guessed_ct = _mt.guess_type(filename)[0] or "application/octet-stream"

                try:
                    new_row = DocUpload(
                        project_id=pid,
                        uploader_id=current_user.id,
                        uploaded_at=getattr(entry, 'server_modified', None) or _dt.utcnow(),
                        r2_key=None,
                        original_filename=filename,
                        filed_filename=filename,
                        filed_dropbox_path=entry.path_display,
                        filed_at=getattr(entry, 'server_modified', None) or _dt.utcnow(),
                        file_size=getattr(entry, 'size', None),
                        content_type=guessed_ct,
                        status='filed',
                        category=doc_type_for_folder,
                        note="Reconciled from Dropbox scan",
                    )
                    db.session.add(new_row)
                    created += 1
                    if len(sample) < 15:
                        sample.append({
                            "filename": filename,
                            "path":     entry.path_display,
                            "doc_type": doc_type_for_folder,
                        })
                except Exception as _ce:
                    errors.append(f"create row for {entry.path_display}: {_ce}")

            if not res.has_more:
                break
            try:
                res = dbx.files_list_folder_continue(res.cursor)
            except Exception as _ce:
                errors.append(f"continue {folder_path}: {_ce}")
                break

    try:
        db.session.commit()
    except Exception as _ce:
        db.session.rollback()
        return jsonify({
            "error": f"Commit failed: {_ce}",
            "created": 0,
            "scanned": scanned,
            "errors": errors[:20],
        }), 500

    return jsonify({
        "ok":       True,
        "scanned":  scanned,
        "created":  created,
        "existing": len(existing_paths),
        "sample":   sample,
        "errors":   errors[:20],
        "proj_root": proj_root,
    })


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
            flash("Not authorized", "error")
            return redirect(url_for('dashboard'))
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

_DOCS_ONLY_ALLOWED = _FORCE_PW_ALLOWED | {"docs_upload_delete"}

@app.before_request
def enforce_password_change():
    """Redirect users who must change their password to the profile page."""
    if (current_user.is_authenticated
            and getattr(current_user, 'must_change_password', False)
            and request.endpoint not in _FORCE_PW_ALLOWED):
        flash("Please set a new password before continuing.", "warning")
        return redirect(url_for("profile"))


@app.before_request
def enforce_docs_only_role():
    """Restrict docs_only users to docs routes only."""
    if (current_user.is_authenticated
            and getattr(current_user, 'role', None) == 'docs_only'
            and request.endpoint
            and request.endpoint not in _DOCS_ONLY_ALLOWED
            and not request.endpoint.startswith('static')):
        return redirect(url_for("docs_dashboard"))


def _user_project_role(pid):
    """Return the current user's effective role for a project.

    Returns 'admin' for site admins, the ProjectAccess.role string for
    project members, or None if the user has no access.
    """
    if current_user.role in ('super_admin', 'admin'):
        return 'owner'
    pa = ProjectAccess.query.filter_by(project_id=pid, user_id=current_user.id).first()
    return pa.role if pa else None


def _require_project_role(pid, min_role='viewer'):
    """Abort 403 if user doesn't meet the minimum project role.

    Role hierarchy: owner > editor > viewer > docs_only
    """
    _HIERARCHY = ['docs_only', 'viewer', 'editor', 'owner',
                  'collaborator']  # legacy 'collaborator' treated as editor
    role = _user_project_role(pid)
    if role is None:
        abort(403)
    # Treat legacy 'collaborator' as 'editor'
    if role == 'collaborator':
        role = 'editor'
    min_idx = _HIERARCHY.index(min_role) if min_role in _HIERARCHY else 1
    cur_idx = _HIERARCHY.index(role) if role in _HIERARCHY else 0
    if cur_idx < min_idx:
        abort(403)


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
        flash("You're already logged in. Log out first to reset your password.", "info")
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


@app.route("/admin/dbx-ls")
@login_required
@admin_required
def dbx_ls():
    """Debug: list contents of the Dropbox ops root and find namespace IDs."""
    try:
        import dropbox as _dbx_mod
        dbx = _dbx_client()

        # Current config
        config = {
            "DROPBOX_NAMESPACE_ID": _DBX_NAMESPACE_ID or "(not set)",
            "DROPBOX_OPERATIONS_PATH": _DBX_OPS_ROOT,
            "path_mode": "namespace" if _DBX_NAMESPACE_ID else "legacy",
        }

        # Resolved path examples
        resolved_paths = {
            "sample_project_root": f"/SampleProject" if _DBX_NAMESPACE_ID else f"{_DBX_OPS_ROOT}/SampleProject",
            "archive_root": _DBX_ARCHIVE_ROOT,
            "wrap_root":    _DBX_WRAP_ROOT,
        }

        # List root to find shared namespaces
        root_result = dbx.files_list_folder("")
        root_entries = []
        for e in root_result.entries:
            info = {"name": e.name, "type": type(e).__name__}
            if hasattr(e, 'sharing_info') and e.sharing_info:
                info["namespace_id"] = getattr(e.sharing_info, 'shared_folder_id', None)
            root_entries.append(info)

        # Also try listing the ops root directly
        ops_entries = []
        list_path = "" if _DBX_NAMESPACE_ID else _DBX_OPS_ROOT
        try:
            ops_result = dbx.files_list_folder(list_path)
            for e in ops_result.entries:
                info = {"name": e.name, "type": type(e).__name__}
                if hasattr(e, 'sharing_info') and e.sharing_info:
                    info["namespace_id"] = getattr(e.sharing_info, 'shared_folder_id', None)
                ops_entries.append(info)
        except Exception as e2:
            ops_entries = [{"error": str(e2)}]

        # Get current user's namespace info
        account = dbx.users_get_current_account()
        namespace_info = {
            "account_id": account.account_id,
            "root_namespace_id": getattr(account.root_info, 'root_namespace_id', None) if hasattr(account, 'root_info') else None,
            "home_namespace_id": getattr(account.root_info, 'home_namespace_id', None) if hasattr(account, 'root_info') else None,
        }

        return jsonify({
            "config": config,
            "resolved_paths": resolved_paths,
            "namespace_info": namespace_info,
            "root_entries": root_entries,
            "ops_path": list_path or "(namespace root)",
            "ops_entries": ops_entries,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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

# ── SocketIO event handlers ───────────────────────────────────────────────────

# { sid: {"user_id": int, "user_name": str, "budget_id": int} }
_socket_sessions = {}
# Avatar color palette — consistent per user_id
_AVATAR_COLORS = [
    "#2563eb", "#dc2626", "#059669", "#d97706", "#7c3aed",
    "#db2777", "#0891b2", "#65a30d", "#c026d3", "#ea580c",
]
# Conflict detection: recent edits within a 2-second window
# { (bid, line_id, field): {"user_id": int, "user_name": str, "role": str, "at": datetime} }
_recent_edits = {}
_CONFLICT_WINDOW = 2  # seconds
_ROLE_PRIORITY = {
    'super_admin': 4, 'admin': 3, 'line_producer': 2,
    'dept_head': 1, 'docs_only': 0,
}

if _HAS_SOCKETIO:
    @socketio.on("connect")
    def _ws_connect():
        pass

    @socketio.on("disconnect")
    def _ws_disconnect():
        from flask import request as _req
        sid = _req.sid
        info = _socket_sessions.pop(sid, None)
        if info:
            bid = info["budget_id"]
            room = f"budget_{bid}"
            emit("user_left", {"user_id": info["user_id"], "user_name": info["user_name"]}, room=room)
            # Broadcast updated viewer list
            viewers = [
                {"user_id": v["user_id"], "user_name": v["user_name"],
                 "color": _AVATAR_COLORS[v["user_id"] % len(_AVATAR_COLORS)]}
                for s, v in _socket_sessions.items() if v["budget_id"] == bid
            ]
            emit("presence_update", {"viewers": viewers}, room=room)

    @socketio.on("join_budget")
    def _ws_join_budget(data):
        from flask import request as _req
        bid = data.get("budget_id")
        uid = data.get("user_id")
        uname = data.get("user_name", "")
        if not bid or not uid:
            return
        room = f"budget_{bid}"
        join_room(room)
        _socket_sessions[_req.sid] = {
            "user_id": uid, "user_name": uname, "budget_id": bid,
        }
        color = _AVATAR_COLORS[uid % len(_AVATAR_COLORS)]
        emit("user_joined", {"user_id": uid, "user_name": uname, "color": color}, room=room)
        # Send full viewer list
        viewers = [
            {"user_id": v["user_id"], "user_name": v["user_name"],
             "color": _AVATAR_COLORS[v["user_id"] % len(_AVATAR_COLORS)]}
            for s, v in _socket_sessions.items() if v["budget_id"] == bid
        ]
        emit("presence_update", {"viewers": viewers}, room=room)

    @socketio.on("editing_start")
    def _ws_editing_start(data):
        from flask import request as _req
        bid = data.get("budget_id")
        if not bid:
            return
        room = f"budget_{bid}"
        emit("editing_start", data, room=room, include_self=False)

    @socketio.on("editing_stop")
    def _ws_editing_stop(data):
        from flask import request as _req
        bid = data.get("budget_id")
        if not bid:
            return
        room = f"budget_{bid}"
        emit("editing_stop", data, room=room, include_self=False)


def _check_and_emit_conflicts(bid, line_id, data):
    """Detect near-simultaneous edits on the same field and notify the loser."""
    if not _HAS_SOCKETIO or not socketio:
        return
    try:
        my_id = current_user.id
        my_name = current_user.name or current_user.email.split("@")[0]
        my_role = getattr(current_user, 'role', 'line_producer')
    except Exception:
        return
    now = datetime.utcnow()
    cutoff = now - timedelta(seconds=_CONFLICT_WINDOW)
    editable_fields = {"rate", "quantity", "days", "estimated_total", "description",
                       "rate_type", "fringe_type", "agent_pct", "est_ot", "note",
                       "working_total", "manual_actual"}
    for field in editable_fields:
        if field not in data:
            continue
        key = (bid, line_id, field)
        prev = _recent_edits.get(key)
        if prev and prev["user_id"] != my_id and prev["at"] > cutoff:
            # Conflict detected — determine winner by role priority
            my_pri = _ROLE_PRIORITY.get(my_role, 1)
            prev_pri = _ROLE_PRIORITY.get(prev["role"], 1)
            if my_pri >= prev_pri:
                # I win (or equal = last-write-wins) — notify previous editor
                loser_sid = _find_sid_for_user(prev["user_id"], bid)
                winner_name = my_name
            else:
                # Previous editor had higher role — notify me
                loser_sid = _find_sid_for_user(my_id, bid)
                winner_name = prev["user_name"]
            if loser_sid:
                socketio.emit("conflict_override", {
                    "line_id": line_id,
                    "field": field,
                    "winner_name": winner_name,
                }, room=loser_sid, namespace="/")
        # Record this edit
        _recent_edits[key] = {
            "user_id": my_id, "user_name": my_name,
            "role": my_role, "at": now,
        }
    # Prune old entries periodically (every ~50 edits)
    if len(_recent_edits) > 200:
        stale = now - timedelta(seconds=30)
        for k in [k for k, v in _recent_edits.items() if v["at"] < stale]:
            del _recent_edits[k]


def _find_sid_for_user(user_id, bid):
    """Find the socket SID for a given user in a budget room."""
    for sid, info in _socket_sessions.items():
        if info["user_id"] == user_id and info["budget_id"] == bid:
            return sid
    return None


def _sanitize_for_json(obj):
    """Recursively convert Decimal/date values to JSON-safe types."""
    from decimal import Decimal as _Dec
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_for_json(v) for v in obj]
    if isinstance(obj, _Dec):
        return float(obj)
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    return obj


def _ws_emit_field_change(bid, line_id, result_data):
    """Broadcast a field change to all clients viewing this budget."""
    if not _HAS_SOCKETIO or not socketio:
        return
    try:
        user_id = current_user.id
        user_name = current_user.name or current_user.email.split("@")[0]
    except Exception:
        user_id = 0
        user_name = "someone"
    room = f"budget_{bid}"
    payload = {
        "line_id": line_id,
        "data": _sanitize_for_json(result_data),
        "user_id": user_id,
        "user_name": user_name,
    }
    try:
        socketio.emit("field_change", payload, room=room, namespace="/")
        app.logger.info("WS field_change → room=%s line=%s user=%s", room, line_id, user_name)
    except Exception as e:
        app.logger.error("WS field_change FAILED: %s", e)


# ── Dashboard ─────────────────────────────────────────────────────────────────

@app.route("/")
@login_required
def dashboard():
    if current_user.role in ('super_admin', 'admin'):
        all_projects = ProjectSheet.query.order_by(ProjectSheet.name).all()
    else:
        accessible_ids = [
            pa.project_id for pa in
            ProjectAccess.query.filter_by(user_id=current_user.id).all()
        ]
        all_projects = ProjectSheet.query.filter(
            ProjectSheet.id.in_(accessible_ids)
        ).order_by(ProjectSheet.name).all()
    projects         = [p for p in all_projects if getattr(p, 'status', 'active') == 'active']
    wrapped_projects = [p for p in all_projects if getattr(p, 'status', 'active') == 'wrapped']
    archived_projects= [p for p in all_projects if getattr(p, 'status', 'active') == 'archived']
    budget_counts = {}
    for b in Budget.query.all():
        budget_counts[b.project_id] = budget_counts.get(b.project_id, 0) + 1
    all_templates = BudgetTemplate.query.order_by(BudgetTemplate.name).all()
    return render_template("dashboard.html", projects=projects,
                           wrapped_projects=wrapped_projects,
                           archived_projects=archived_projects,
                           budget_counts=budget_counts,
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
    timezone    = request.form.get("timezone", "America/Los_Angeles").strip() or "America/Los_Angeles"
    # Optional start/end dates from the modal
    start_date_str = request.form.get("start_date", "").strip()
    end_date_str   = request.form.get("end_date", "").strip()
    start_date = None
    end_date   = None
    try:
        if start_date_str:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        if end_date_str:
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    except ValueError:
        pass
    p = ProjectSheet(name=name, client_name=client_name)
    db.session.add(p)
    db.session.flush()
    # Generate unique Dropbox folder slug and provision the folder tree
    slug = _unique_project_slug(name, client_name, exclude_id=p.id)
    p.dropbox_folder = slug
    _provision_dropbox_folder(slug)
    # Auto-create a default budget with user's device timezone
    _fed40 = PayrollProfile.query.filter(PayrollProfile.name.ilike('%federal%')).first()
    b = Budget(project_id=p.id, name=f"{name} Budget", payroll_profile_id=_fed40.id if _fed40 else None,
               payroll_week_start=6, timezone=timezone,
               start_date=start_date, end_date=end_date)
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


def _archive_project_dropbox(p):
    """Before deleting a project: move Dropbox folder to _ARCHIVED and upload budget PDFs."""
    try:
        has_refresh = os.getenv('DROPBOX_REFRESH_TOKEN') and os.getenv('DROPBOX_APP_KEY')
        if not has_refresh and not os.getenv('DROPBOX_ACCESS_TOKEN'):
            return
        dbx = _dbx_client()
        archive_root = _DBX_ARCHIVE_ROOT
        from datetime import date as _date
        stamp = _date.today().strftime("%Y-%m-%d")

        # Move the project folder into _ARCHIVED if it exists
        if p.dropbox_folder:
            src  = f"/{p.dropbox_folder}" if _DBX_NAMESPACE_ID else f"{_DBX_OPS_ROOT}/{p.dropbox_folder}"
            dest = f"{archive_root}/{stamp}_{p.dropbox_folder}"
            try:
                dbx.files_move_v2(src, dest, autorename=True)
                logging.info(f"Archived Dropbox folder: {src} → {dest}")
            except Exception as e:
                logging.warning(f"Could not move Dropbox folder on delete: {e}")
                # Still try to upload PDFs even if folder move failed
                dest = f"{archive_root}/{stamp}_{p.dropbox_folder}"

        else:
            dest = f"{archive_root}/{stamp}_{p.name}"

        # Export each budget version as a PDF and upload to archive
        budgets = Budget.query.filter_by(project_id=p.id).all()
        if budgets:
            fringe_cfgs = get_fringe_configs(db.session)
            for b in budgets:
                try:
                    b_lines = BudgetLine.query.filter_by(budget_id=b.id).order_by(
                        BudgetLine.account_code, BudgetLine.sort_order).all()
                    profile  = b.payroll_profile
                    pw_start = b.payroll_week_start if b.payroll_week_start is not None else (
                        profile.payroll_week_start if profile else 6)
                    top_sheet = calc_top_sheet(b, b_lines, fringe_cfgs, {}, profile, pw_start)
                    sched_mode = 'working' if b.budget_mode in ('working', 'actual') else 'estimated'
                    line_results = {}
                    for ln in b_lines:
                        if ln.use_schedule:
                            sched = ScheduleDay.query.filter_by(
                                budget_line_id=ln.id, schedule_mode=sched_mode).all()
                            line_results[ln.id] = calc_line_from_schedule(
                                ln, sched, fringe_cfgs, profile, pw_start)
                        else:
                            line_results[ln.id] = calc_line(ln, fringe_cfgs)
                    from budget_calc import FP_COA_SECTIONS as _SECS
                    def _sec(code):
                        best = None
                        for start, _ in _SECS:
                            if code >= start: best = start
                            else: break
                        return best
                    sec_map = dict(_SECS)
                    secs_detail = {}
                    for ln in b_lines:
                        sk = _sec(ln.account_code)
                        if sk not in secs_detail:
                            secs_detail[sk] = {"code": sk, "name": sec_map.get(sk, ""), "lines": []}
                        secs_detail[sk]["lines"].append(ln)
                    secs_ordered = [secs_detail[sk] for sk, _ in _SECS if sk in secs_detail]
                    company_settings = CompanySettings.query.get(1) or CompanySettings()
                    dispersed = bool(b.company_fee_dispersed)
                    fee_m = (1 + float(b.company_fee_pct)) if dispersed else 1.0
                    html_str = render_template("budget_pdf.html",
                        project=p, budget=b, top_sheet=top_sheet,
                        detail_mode=False, dispersed=dispersed,
                        company_settings=company_settings,
                        is_working_view=b.budget_mode in ('working', 'actual'),
                        sections_ordered=secs_ordered,
                        line_results=line_results, fee_m=fee_m, today=_date.today(),
                    )
                    pdf_bytes = WeasyprintHTML(string=html_str, base_url="http://localhost").write_pdf()
                    safe_name = re.sub(r"[^\w\-.]", "_", b.name)
                    mode_lbl  = "Working" if b.budget_mode in ('working','actual') else "Estimated"
                    pdf_key   = f"{dest}/01_ADMIN/BUDGET ESTIMATES/{safe_name}_{mode_lbl}.pdf"
                    dbx.files_upload(pdf_bytes, pdf_key, autorename=True)
                    logging.info(f"Uploaded budget PDF to archive: {pdf_key}")
                except Exception as e:
                    logging.warning(f"Budget PDF archive failed for budget {b.id}: {e}")
    except Exception as e:
        logging.error(f"_archive_project_dropbox failed: {e}")


@app.route("/projects/<int:pid>/delete", methods=["POST"])
@login_required
def project_delete(pid):
    # Only super_admin can permanently delete projects
    if getattr(current_user, 'role', None) != 'super_admin':
        flash("Only a super admin can permanently delete projects.", "error")
        return redirect(url_for("dashboard"))
    p = ProjectSheet.query.get_or_404(pid)
    # Archive to Dropbox before any DB deletion
    _archive_project_dropbox(p)
    # Null out parent_budget_id self-references so Postgres allows deletion
    Budget.query.filter_by(project_id=pid).update(
        {"parent_budget_id": None}, synchronize_session=False)
    db.session.flush()
    # Cascade-delete each budget and its FK-constrained children
    for b in Budget.query.filter_by(project_id=pid).all():
        _delete_budget_cascade(b.id)
    # Clean up project-level FK tables not on ORM cascade
    from models import Location
    DocUpload.query.filter_by(project_id=pid).delete(synchronize_session=False)
    Location.query.filter_by(project_id=pid).delete(synchronize_session=False)
    ProjectAccess.query.filter_by(project_id=pid).delete(synchronize_session=False)
    ProjectUnion.query.filter_by(project_id=pid).delete(synchronize_session=False)
    ProjectClient.query.filter_by(project_id=pid).delete(synchronize_session=False)
    db.session.delete(p)
    db.session.commit()
    flash(f"Project '{p.name}' deleted.", "success")
    return redirect(url_for("dashboard"))


@app.route("/projects/<int:pid>/rename", methods=["POST"])
@login_required
def project_rename(pid):
    """Rename a project. Admin or super_admin only."""
    if getattr(current_user, 'role', None) not in ('super_admin', 'admin'):
        flash("Only admins can rename projects.", "error")
        return redirect(url_for("dashboard"))
    p = ProjectSheet.query.get_or_404(pid)
    new_name = (request.form.get("name") or "").strip()
    if not new_name:
        flash("Project name cannot be empty.", "error")
        return redirect(url_for("dashboard"))
    # Uniqueness check (excluding this project)
    collision = ProjectSheet.query.filter(
        ProjectSheet.name == new_name, ProjectSheet.id != pid
    ).first()
    if collision:
        flash(f"A project named '{new_name}' already exists.", "error")
        return redirect(url_for("dashboard"))
    old_name = p.name
    p.name = new_name
    db.session.commit()
    flash(f"Renamed '{old_name}' → '{new_name}'.", "success")
    return redirect(url_for("dashboard"))


@app.route("/projects/<int:pid>/wrap", methods=["POST"])
@login_required
def project_wrap(pid):
    p = ProjectSheet.query.get_or_404(pid)
    if p.dropbox_folder:
        try:
            dbx = _dbx_client()
            src  = f"/{p.dropbox_folder}" if _DBX_NAMESPACE_ID else f"{_DBX_OPS_ROOT}/{p.dropbox_folder}"
            wrap_root = _DBX_WRAP_ROOT
            # Prefix destination folder with YYYY-MM-DD to match the _ARCHIVED
            # flow — easier chronological sort in Dropbox.
            from datetime import date as _date
            stamp = _date.today().strftime("%Y-%m-%d")
            dest = f"{wrap_root}/{stamp}_{p.dropbox_folder}"
            dbx.files_move_v2(src, dest, autorename=True)
            logging.info(f"Wrapped project Dropbox folder: {src} → {dest}")
        except Exception as e:
            logging.warning(f"Could not move Dropbox folder on wrap: {e}")
    p.status = 'wrapped'
    db.session.commit()
    flash(f"Project '{p.name}' wrapped.", "success")
    return redirect(url_for("dashboard"))


@app.route("/projects/<int:pid>/archive", methods=["POST"])
@login_required
def project_archive(pid):
    p = ProjectSheet.query.get_or_404(pid)
    if p.dropbox_folder:
        try:
            dbx = _dbx_client()
            src  = f"/{p.dropbox_folder}" if _DBX_NAMESPACE_ID else f"{_DBX_OPS_ROOT}/{p.dropbox_folder}"
            # _ARCHIVED lives as a SIBLING of _FP OPERATIONS FOLDER, not a child.
            from datetime import date as _date
            stamp = _date.today().strftime("%Y-%m-%d")
            dest = f"{_DBX_ARCHIVE_ROOT}/{stamp}_{p.dropbox_folder}"
            dbx.files_move_v2(src, dest, autorename=True)
            logging.info(f"Archived project Dropbox folder: {src} → {dest}")
        except Exception as e:
            logging.warning(f"Could not move Dropbox folder on archive: {e}")
    p.status = 'archived'
    db.session.commit()
    flash(f"Project '{p.name}' archived.", "success")
    return redirect(url_for("dashboard"))


@app.route("/projects/bulk-delete", methods=["POST"])
@login_required
def projects_bulk_delete():
    """Delete multiple projects at once. Expects form field 'project_ids' (repeatable)."""
    if getattr(current_user, 'role', None) != 'super_admin':
        flash("Only a super admin can permanently delete projects.", "error")
        return redirect(url_for("dashboard"))
    from models import Location
    ids = request.form.getlist("project_ids")
    if not ids:
        flash("No projects selected.", "warning")
        return redirect(url_for("dashboard"))
    deleted_names = []
    for raw in ids:
        try:
            pid = int(raw)
        except (TypeError, ValueError):
            continue
        p = ProjectSheet.query.get(pid)
        if not p:
            continue
        try:
            _archive_project_dropbox(p)
        except Exception as e:
            logging.warning(f"Dropbox archive on bulk delete failed for {p.name}: {e}")
        # Null out self-referencing parent_budget_id so Postgres allows deletion
        Budget.query.filter_by(project_id=pid).update(
            {"parent_budget_id": None}, synchronize_session=False)
        db.session.flush()
        for b in Budget.query.filter_by(project_id=pid).all():
            _delete_budget_cascade(b.id)
        DocUpload.query.filter_by(project_id=pid).delete(synchronize_session=False)
        Location.query.filter_by(project_id=pid).delete(synchronize_session=False)
        ProjectAccess.query.filter_by(project_id=pid).delete(synchronize_session=False)
        ProjectUnion.query.filter_by(project_id=pid).delete(synchronize_session=False)
        ProjectClient.query.filter_by(project_id=pid).delete(synchronize_session=False)
        deleted_names.append(p.name)
        db.session.delete(p)
    db.session.commit()
    n = len(deleted_names)
    flash(f"Deleted {n} project{'s' if n != 1 else ''}.", "success")
    return redirect(url_for("dashboard"))


@app.route("/projects/bulk-archive", methods=["POST"])
@login_required
def projects_bulk_archive():
    """Archive multiple projects at once. Expects form field 'project_ids' (repeatable)."""
    ids = request.form.getlist("project_ids")
    if not ids:
        flash("No projects selected.", "warning")
        return redirect(url_for("dashboard"))
    archived = 0
    for raw in ids:
        try:
            pid = int(raw)
        except (TypeError, ValueError):
            continue
        p = ProjectSheet.query.get(pid)
        if not p:
            continue
        if p.dropbox_folder:
            try:
                dbx = _dbx_client()
                src = f"/{p.dropbox_folder}" if _DBX_NAMESPACE_ID else f"{_DBX_OPS_ROOT}/{p.dropbox_folder}"
                from datetime import date as _date
                stamp = _date.today().strftime("%Y-%m-%d")
                dest = f"{_DBX_ARCHIVE_ROOT}/{stamp}_{p.dropbox_folder}"
                dbx.files_move_v2(src, dest, autorename=True)
            except Exception as e:
                logging.warning(f"Could not move Dropbox folder on bulk archive: {e}")
        p.status = 'archived'
        archived += 1
    db.session.commit()
    flash(f"Archived {archived} project{'s' if archived != 1 else ''}.", "success")
    return redirect(url_for("dashboard"))


@app.route("/projects/<int:pid>/reactivate", methods=["POST"])
@login_required
def project_reactivate(pid):
    """Move project back to active AND restore its Dropbox folder back to
    the ops root. Searches _ARCHIVED and _WRAPPED PROJECTS for an entry
    whose name ends with the project's slug (archives are prefixed with
    a YYYY-MM-DD date stamp like '2026-04-16_2026-04_Client_Project').
    Moves the most recent match back to the ops root, stripping the date
    prefix."""
    p = ProjectSheet.query.get_or_404(pid)
    p.status = 'active'
    db.session.commit()

    dbx_note = ""
    try:
        has_refresh = os.getenv('DROPBOX_REFRESH_TOKEN') and os.getenv('DROPBOX_APP_KEY')
        if (has_refresh or os.getenv('DROPBOX_ACCESS_TOKEN')) and p.dropbox_folder:
            dbx = _dbx_client()
            slug = p.dropbox_folder

            # Destination = ops root under the same slug name.
            dest = f"/{slug}" if _DBX_NAMESPACE_ID else f"{_DBX_OPS_ROOT}/{slug}"

            # Candidate source roots to search: _ARCHIVED first, then
            # _WRAPPED PROJECTS. Archived folders are named
            # '{YYYY-MM-DD}_{slug}' (or similar stamp prefix). We match on
            # anything ending with '_{slug}' OR exactly equal to slug.
            _wrap_root = os.getenv('DROPBOX_WRAP_PATH', '/_WRAPPED PROJECTS')
            search_roots = [_DBX_ARCHIVE_ROOT, _wrap_root]
            found_src = None
            found_from = None
            for root in search_roots:
                try:
                    res = dbx.files_list_folder(root)
                    entries = list(res.entries)
                    while getattr(res, 'has_more', False):
                        res = dbx.files_list_folder_continue(res.cursor)
                        entries.extend(res.entries)
                    # Match by suffix (date_prefix + underscore + slug) or exact name
                    candidates = [e for e in entries
                                  if getattr(e, 'name', '').endswith(f"_{slug}")
                                  or getattr(e, 'name', '') == slug]
                    # Pick the most recent (archives are date-prefixed so sort desc)
                    candidates.sort(key=lambda e: getattr(e, 'name', ''), reverse=True)
                    if candidates:
                        found_src = getattr(candidates[0], 'path_display', None) or f"{root}/{candidates[0].name}"
                        found_from = root
                        break
                except Exception as _le:
                    logging.warning(f"[DBX REACTIVATE] could not list {root}: {_le}")
                    continue

            if found_src:
                try:
                    dbx.files_move_v2(found_src, dest, autorename=True)
                    logging.warning(f"[DBX REACTIVATE] moved {found_src} → {dest}")
                    dbx_note = f" Dropbox folder restored from {found_from}."
                except Exception as _me:
                    logging.error(f"[DBX REACTIVATE] move failed {found_src} → {dest}: {_me}")
                    dbx_note = f" (Dropbox move failed — folder still in {found_from}. Error: {_me})"
            else:
                logging.warning(f"[DBX REACTIVATE] no archived/wrapped folder found for slug={slug!r}")
                dbx_note = (f" (no archived folder found matching '{slug}' — "
                            f"you may need to restore it manually in Dropbox.)")
    except Exception as _e:
        logging.exception(f"[DBX REACTIVATE] unexpected: {_e}")
        dbx_note = f" (Dropbox restore errored: {_e})"

    flash(f"Project '{p.name}' reactivated.{dbx_note}", "success")
    return redirect(url_for("dashboard"))


@app.route("/admin/import-dropbox", methods=["POST"])
@login_required
@admin_required
def admin_import_dropbox():
    """Scan Dropbox ops root for project folders and import any missing ones."""
    try:
        dbx = _dbx_client()
        list_path = "" if _DBX_NAMESPACE_ID else _DBX_OPS_ROOT
        result = dbx.files_list_folder(list_path)
        folders = []
        while True:
            for entry in result.entries:
                import dropbox as _dbx_mod
                if isinstance(entry, _dbx_mod.files.FolderMetadata):
                    fname = entry.name
                    # Skip template/special folders (start with _ or !)
                    if not fname.startswith('_') and not fname.startswith('!'):
                        folders.append(fname)
            if not result.has_more:
                break
            result = dbx.files_list_folder_continue(result.cursor)

        imported = 0
        skipped  = 0
        for fname in folders:
            existing = ProjectSheet.query.filter_by(dropbox_folder=fname).first()
            if existing:
                skipped += 1
                continue
            # Try to parse client/project from slug (YYYY-MM_Client_Project)
            parts = fname.split('_', 2)
            display_name = parts[2].replace('_', ' ') if len(parts) == 3 else fname
            p = ProjectSheet(name=display_name, dropbox_folder=fname, status='active')
            db.session.add(p)
            db.session.flush()
            # Grant importing user as owner
            db.session.add(ProjectAccess(project_id=p.id, user_id=current_user.id, role='owner'))
            imported += 1

        db.session.commit()
        flash(f"Dropbox import complete: {imported} imported, {skipped} already existed.", "success")
    except Exception as e:
        db.session.rollback()
        logging.error(f"admin_import_dropbox failed: {e}")
        flash(f"Import failed: {e}", "error")
    return redirect(url_for("admin_panel"))


# ── Budget ────────────────────────────────────────────────────────────────────

@app.route("/projects/<int:pid>/budget")
@login_required
def project_budget_redirect(pid):
    """Redirect to newest budget or create page."""
    project = ProjectSheet.query.get_or_404(pid)
    if not _user_can_access_project(pid):
        abort(403)
    # docs_only project role → go straight to docs
    if _user_project_role(pid) == 'docs_only':
        return redirect(url_for("docs_project", pid=pid))
    latest = Budget.query.filter_by(project_id=pid).order_by(Budget.created_at.desc()).first()
    if latest:
        return redirect(url_for("budget_view", pid=pid, bid=latest.id))
    all_templates = BudgetTemplate.query.order_by(BudgetTemplate.name).all()
    return render_template("budget_new.html", project=project, all_templates=all_templates)


def _project_base_name(project_id, project_name):
    """Return the base display name for version labels (strips trailing vN / Working vN)."""
    import re as _re
    existing = Budget.query.filter_by(project_id=project_id).all()
    for b in existing:
        m = _re.match(r'^(.+?)\s+(?:Working\s+)?v\d+$', b.name or '', _re.IGNORECASE)
        if m:
            return m.group(1)
    return project_name


def _next_version_number(project_id):
    """Return the next integer version number for a new Estimated budget in this project."""
    est_budgets = Budget.query.filter_by(project_id=project_id).filter(
        Budget.budget_mode == 'estimated'
    ).all()
    if not est_budgets:
        return 1
    return max((b.version_number or 1) for b in est_budgets) + 1


def _estimated_version_name(base_name, version_num):
    return f"{base_name} v{version_num}"


def _working_version_name(base_name, version_num):
    return f"{base_name} Working v{version_num}"


def _create_budget_from_source(pid, source, new_name, new_mode, parent_bid=None, version_number=None):
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
        version_number=version_number,
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
    """Create a new Estimated budget version (always Estimated — Working is tied to it via create_working_from_estimated)."""
    project    = ProjectSheet.query.get_or_404(pid)
    # Source: prefer the current Estimated so settings/lines carry forward
    source_bid = request.form.get("source_bid", type=int)
    source = (Budget.query.filter_by(id=source_bid, project_id=pid).first()
              if source_bid else
              Budget.query.filter(Budget.project_id == pid,
                                  Budget.budget_mode == 'estimated',
                                  Budget.version_status == 'current').first())
    if source is None:
        source = Budget.query.filter_by(project_id=pid).order_by(Budget.created_at.desc()).first()

    next_vnum = _next_version_number(pid)
    base_name = _project_base_name(pid, project.name)
    new_name  = _estimated_version_name(base_name, next_vnum)

    _supersede_current(pid, 'estimated')
    db.session.flush()

    b = _create_budget_from_source(pid, source, new_name, 'estimated',
                                   parent_bid=source.id if source else None,
                                   version_number=next_vnum)
    db.session.commit()

    flash(f"Created {b.name} — now add a Working budget when ready.", "success")
    return redirect(url_for("budget_view", pid=pid, bid=b.id))


@app.route("/projects/<int:pid>/budget/<int:bid>/create-working", methods=["POST"])
@login_required
def create_working_from_estimated(pid, bid):
    """Create a Working budget paired to this Estimated budget (shares its version_number)."""
    project = ProjectSheet.query.get_or_404(pid)
    source  = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    if _budget_type(source.budget_mode) != 'estimated':
        flash("Working budgets can only be created from an Estimated budget.", "error")
        return redirect(url_for("budget_view", pid=pid, bid=bid))
    try:
        vnum = source.version_number or 1
        # Guard: a Working budget for this version already exists
        existing_w = Budget.query.filter_by(project_id=pid, version_number=vnum).filter(
            Budget.budget_mode.in_(('working', 'actual')),
            Budget.version_status != 'archived'
        ).first()
        if existing_w:
            flash(f"A Working budget for v{vnum} already exists. Archive it first to create a new one.", "error")
            return redirect(url_for("budget_view", pid=pid, bid=bid))

        _supersede_current(pid, 'working')
        db.session.flush()

        base_name = _project_base_name(pid, project.name)
        w_name = _working_version_name(base_name, vnum)
        w = _create_budget_from_source(pid, source, w_name, 'working',
                                       parent_bid=bid, version_number=vnum)
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


@app.route("/projects/<int:pid>/budget/<int:bid>/delete", methods=["POST"])
@login_required
def delete_budget(pid, bid):
    """Permanently delete a budget version.
    Cannot delete the last Estimated budget for a project — it is the source of truth.
    Deleting a Working budget while an Estimated exists is always allowed.
    """
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()

    # Guard: never delete the last estimated budget
    if _budget_type(budget.budget_mode) == 'estimated':
        est_count = Budget.query.filter_by(project_id=pid).filter(
            Budget.budget_mode == 'estimated',
            Budget.id != bid
        ).count()
        if est_count == 0:
            return jsonify({"error": "Cannot delete the only Estimated budget. Delete the project instead."}), 400

    # Find best redirect target: prefer remaining budget of same type, then any
    remaining = (
        Budget.query.filter(Budget.project_id == pid, Budget.id != bid,
                            Budget.budget_mode == budget.budget_mode)
        .order_by(Budget.created_at.desc()).first()
        or Budget.query.filter(Budget.project_id == pid, Budget.id != bid)
        .order_by(Budget.created_at.desc()).first()
    )

    if budget.version_status == 'current' and remaining:
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
    # Access check — deny non-admins without project access
    if not _user_can_access_project(pid):
        abort(403)
    # docs_only project role → redirect to docs section
    proj_role = _user_project_role(pid)
    if proj_role == 'docs_only':
        return redirect(url_for("docs_project", pid=pid))
    # Auto-sync budget dates from schedule if not yet set
    if not budget.start_date:
        _sync_budget_dates_from_schedule(bid)
        db.session.commit()
    # Auto-promote: viewing a version makes it the active one
    if budget.version_status != 'current':
        _supersede_current(pid, _budget_type(budget.budget_mode), exclude_id=bid)
        budget.version_status = 'current'
        db.session.commit()

    # Catch-up: reconcile schedule-driven auto lines (meals, flights, hotel,
    # mileage, per diem) every time budget view loads. Ensures any flags/meals
    # set on the Gantt are reflected in the budget lines even if a prior sync
    # failed silently.
    try:
        sync_schedule_driven_lines(bid, db.session)
    except Exception as _se:
        app.logger.warning("budget_view sync_schedule_driven_lines failed: %s", _se)
        db.session.rollback()

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

    # Assigned-location lookup for Locations (3300) lines. Location has a
    # budget_line_id FK; we pull the reverse relation in one query so the
    # template can render the assigned location's name next to each 3300
    # line (same UX as assigned-crew on labor lines).
    line_assigned_location = {}
    _loc_line_ids = [ln.id for ln in lines if ln.account_code == COA_CODE_LOCATIONS]
    if _loc_line_ids:
        _locs = Location.query.filter(Location.budget_line_id.in_(_loc_line_ids)).all()
        for _lc in _locs:
            if _lc.budget_line_id:
                line_assigned_location[_lc.budget_line_id] = {
                    "id": _lc.id,
                    "name": _lc.name,
                    "facility_name": _lc.facility_name or "",
                }

    # Sub-group lookup for department headers in Production Staff (2000) and
    # Talent (2100) sections. Priority order:
    #   1. ln.role_group (explicitly set)
    #   2. Linked CatalogItem.group_name (for lines added via Quick Entry
    #      where role_group wasn't forwarded to the save payload)
    #   3. Keyword match on description (legacy lines with no catalog link)
    # Bulk-load catalog group names in one query to avoid per-line lookups.
    line_sub_groups = {}
    _cat_ids = [ln.catalog_item_id for ln in lines
                if getattr(ln, 'catalog_item_id', None)]
    _cat_group_by_id = {}
    if _cat_ids:
        _cat_rows = CatalogItem.query.filter(CatalogItem.id.in_(_cat_ids)).all()
        _cat_group_by_id = {c.id: c.group_name for c in _cat_rows if c.group_name}
    for ln in lines:
        if ln.account_code == COA_CODE_PROD_STAFF:
            sg = (ln.role_group or '').strip() or None
            if not sg and getattr(ln, 'catalog_item_id', None):
                sg = _cat_group_by_id.get(ln.catalog_item_id)
            if not sg:
                sg = _get_prod_staff_subgroup(ln.description)
            line_sub_groups[ln.id] = sg
        elif ln.account_code == COA_CODE_TALENT:
            sg = (ln.role_group or '').strip() or None
            if not sg and getattr(ln, 'catalog_item_id', None):
                sg = _cat_group_by_id.get(ln.catalog_item_id)
            if not sg:
                sg = _get_talent_subgroup(ln.description)
            line_sub_groups[ln.id] = sg

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
        working_by_section[COA_CODE_INSURANCE] = working_by_section.get(COA_CODE_INSURANCE, 0.0) + round(working_gross_labor * _wc_pct, 2)
    if _pf_pct:
        working_by_section[COA_CODE_ADMIN] = working_by_section.get(COA_CODE_ADMIN, 0.0) + round(working_gross_labor * _pf_pct, 2)

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

    # Build version groups: list of {version_number, estimated, working, is_current}
    # sorted descending so newest version is first.
    _vn_map = {}
    for _b in all_budgets:
        _vn = _b.version_number or 1
        if _vn not in _vn_map:
            _vn_map[_vn] = {'version_number': _vn, 'estimated': None, 'working': None}
        if _budget_type(_b.budget_mode) == 'estimated':
            # prefer current over superseded
            if _vn_map[_vn]['estimated'] is None or _b.version_status == 'current':
                _vn_map[_vn]['estimated'] = _b
        else:
            if _vn_map[_vn]['working'] is None or _b.version_status == 'current':
                _vn_map[_vn]['working'] = _b
    _max_vn = max(_vn_map.keys()) if _vn_map else 1
    for _vd in _vn_map.values():
        _vd['is_current'] = (_vd['version_number'] == _max_vn)
    version_groups = sorted(_vn_map.values(), key=lambda x: x['version_number'], reverse=True)

    # Mode-switcher peers: same-version Estimated and Working for the budget being viewed
    _cur_vn = budget.version_number or 1
    _vn_peer = _vn_map.get(_cur_vn, {})
    peer_estimated_bid = _vn_peer.get('estimated', {})
    peer_estimated_bid = peer_estimated_bid.id if peer_estimated_bid else current_estimated_bid
    peer_working_bid   = _vn_peer.get('working', {})
    peer_working_bid   = peer_working_bid.id if peer_working_bid else None

    company_settings = CompanySettings.query.get(1) or CompanySettings()
    doc_uploads = DocUpload.query.filter_by(project_id=pid).order_by(DocUpload.uploaded_at.desc()).all()
    return render_template("budget.html",
        project=project,
        budget=budget,
        all_budgets=all_budgets,
        version_groups=version_groups,
        peer_estimated_bid=peer_estimated_bid,
        peer_working_bid=peer_working_bid,
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
        line_assigned_location=line_assigned_location,
        doc_uploads=doc_uploads,
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
              "working_total", "manual_actual",
              # Task 2: catalog linkage for exports
              "catalog_item_id"]
    for f in fields:
        if f in data:
            val = data[f]
            if val == "" or val is None:
                setattr(ln, f, None if f not in ("account_code", "sort_order") else 0)
            else:
                setattr(ln, f, val)

    # Backfill role_group from the linked CatalogItem when the line has a
    # catalog_item_id but no role_group set. Fixes cases where QE adds a new
    # custom role (e.g. admin-added "Utility" under Grip & Electric) and the
    # client didn't forward group_name on the save payload — without this
    # backfill the budget page falls through to the keyword-based subgroup
    # guesser which doesn't know custom labels and places them under the
    # wrong sub-header (Utility would fall into Sound by proximity of sort
    # order). Safe: only runs when role_group is missing AND a catalog link
    # exists AND that catalog row has a non-empty group_name.
    try:
        if getattr(ln, 'catalog_item_id', None) and not (getattr(ln, 'role_group', None) or '').strip():
            _ci = db.session.get(CatalogItem, ln.catalog_item_id)
            if _ci and (_ci.group_name or '').strip():
                ln.role_group = _ci.group_name
    except Exception:
        # Don't break line save over a subgroup backfill lookup.
        pass

    # Auto-compute estimated_total for non-labor lines (rate × qty × days, less discount)
    if not ln.is_labor:
        r        = float(ln.rate or 0)
        q        = float(ln.quantity or 1)
        d        = float(ln.days or 1)
        discount = float(ln.agent_pct or 0)   # stored as fraction (0.15 = 15%)
        if "rate" in data:
            # User explicitly set the rate (including to 0) → always recompute total
            pre_discount = round(r * q * d, 2)
            ln.estimated_total = round(pre_discount * (1 - discount), 2)
        elif r > 0:
            # Rate wasn't in this payload but is still positive → recompute total
            # (qty / days / discount may have changed)
            pre_discount = round(r * q * d, 2)
            ln.estimated_total = round(pre_discount * (1 - discount), 2)
        elif float(ln.estimated_total or 0) > 0 and ("quantity" in data or "days" in data):
            # Rate is 0 but we have a flat total + qty/days changed → back-derive unit rate
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

    resp = {"id": ln.id, **result}
    # Conflict detection: check if another user edited the same field within 2 seconds
    _check_and_emit_conflicts(bid, ln.id, data)
    print(f"[WS] POST /line saved line={ln.id}, emitting to room budget_{bid}")
    _ws_emit_field_change(bid, ln.id, resp)
    return jsonify(resp)


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


@app.route("/projects/<int:pid>/budget/<int:bid>/line/<int:lid>/duplicate", methods=["POST"])
@login_required
def line_duplicate(pid, bid, lid):
    """Duplicate a budget line, placing the new row directly below the source.

    Copies all value fields (rate/days/fringe/agent/schedule config/etc.) but
    CLEARS individual assignment (assigned_crew_id=NULL, no CrewAssignment
    rows). The source line's ScheduleDay rows are copied only when the
    caller sets duplicate_schedule=true in the POST body (default). When
    false, the duplicate starts with no schedule days — the user can build
    a fresh schedule for it.
    """
    from models import ScheduleDay
    Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    src = BudgetLine.query.filter_by(id=lid, budget_id=bid).first_or_404()

    _req_body = request.get_json(silent=True) or {}
    # Default true for backward compat with callers that don't pass the flag.
    duplicate_schedule = bool(_req_body.get("duplicate_schedule", True))

    try:
        # Build the new row with every value field copied from src, but with
        # assigned_crew_id cleared and a fresh sort_order (placed just after src).
        new_ln = BudgetLine(
            budget_id        = src.budget_id,
            account_code     = src.account_code,
            account_name     = src.account_name,
            description      = src.description,
            is_labor         = src.is_labor,
            estimated_total  = src.estimated_total,
            payroll_co       = src.payroll_co,
            quantity         = src.quantity,
            days             = src.days,
            rate             = src.rate,
            rate_type        = src.rate_type,
            est_ot           = src.est_ot,
            fringe_type      = src.fringe_type,
            agent_pct        = src.agent_pct,
            note             = src.note,
            use_schedule     = src.use_schedule,
            days_unit        = src.days_unit,
            days_per_week    = src.days_per_week,
            parent_line_id   = src.parent_line_id,
            line_tag         = src.line_tag,
            sync_omit        = src.sync_omit,
            role_group       = src.role_group,
            unit_rate        = src.unit_rate,
            assigned_crew_id = None,                     # <-- cleared
            catalog_item_id  = src.catalog_item_id,
            working_total    = src.working_total,
            manual_actual    = src.manual_actual,
            schedule_labels  = src.schedule_labels,
            sort_order       = 0,                        # placeholder, reseated below
        )
        db.session.add(new_ln)
        db.session.flush()                               # populate new_ln.id

        # Re-seat sort_order so the copy sits immediately after the source in
        # its section. Fetch all section rows, re-index sequentially, inserting
        # the new line directly after the source.
        section_lines = BudgetLine.query.filter_by(
            budget_id=bid, account_code=src.account_code
        ).filter(BudgetLine.id != new_ln.id).order_by(
            BudgetLine.sort_order, BudgetLine.id
        ).all()
        src_idx = next((i for i, ln in enumerate(section_lines) if ln.id == src.id), 0)
        section_lines.insert(src_idx + 1, new_ln)
        for i, ln in enumerate(section_lines):
            ln.sort_order = i

        # Duplicate the source line's ScheduleDay rows when requested.
        # crew_member_id is cleared — the copy starts with no individual
        # attached even if the schedule days are copied.
        if duplicate_schedule:
            src_sched = ScheduleDay.query.filter_by(budget_line_id=src.id).all()
            for sd in src_sched:
                db.session.add(ScheduleDay(
                    budget_id       = sd.budget_id,
                    budget_line_id  = new_ln.id,
                    crew_member_id  = None,                  # <-- cleared
                    date            = sd.date,
                    episode         = sd.episode,
                    day_type        = sd.day_type,
                    rate_multiplier = sd.rate_multiplier,
                    note            = sd.note,
                    crew_instance   = sd.crew_instance,
                    est_ot_hours    = sd.est_ot_hours,
                    cell_flags      = sd.cell_flags,
                    schedule_mode   = sd.schedule_mode,
                ))

        # NOTE: CrewAssignment rows are intentionally NOT copied. The duplicate
        # is a fresh unassigned role — user fills it in via the normal flow.

        db.session.commit()
        _touch_budget(bid)
        db.session.commit()
        return jsonify({"ok": True, "id": new_ln.id})
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 400


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

    # Adopt role_group from the dropped position's immediate neighbors. This
    # ensures cross-group drags land cleanly — e.g. dragging "Executive
    # Producer" (role_group='Executives') into the middle of the
    # "Direction / AD" rows updates its group to 'Direction / AD' instead of
    # leaving a fragmented "Executives" header mid-section. Logic:
    #   - If the row IMMEDIATELY before and after the dropped row share the
    #     same role_group, the moved row adopts it.
    #   - If the row is at a section boundary (first or last), it adopts
    #     whichever single neighbor exists.
    #   - If neighbors disagree, leave role_group unchanged (the user is
    #     likely placing it on a group boundary intentionally).
    try:
        new_idx = next((i for i, sl in enumerate(section_lines) if sl.id == ln.id), None)
        if new_idx is not None:
            before = section_lines[new_idx - 1].role_group if new_idx > 0 else None
            after  = section_lines[new_idx + 1].role_group if new_idx + 1 < len(section_lines) else None
            target = None
            if before is not None and after is not None:
                if before == after:
                    target = before
            elif before is not None:
                target = before
            elif after is not None:
                target = after
            if target is not None and ln.role_group != target:
                ln.role_group = target
    except Exception:
        # If neighbor inference fails for any reason, silently keep the
        # existing role_group rather than breaking the reorder.
        pass

    db.session.commit()
    _touch_budget(bid)
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/projects/<int:pid>/budget/<int:bid>/line/<int:lid>/set-group", methods=["POST"])
@login_required
def line_set_group(pid, bid, lid):
    """Explicitly set a line's role_group and re-seat its sort_order so it
    lands at the bottom of that group cluster. Used by the "Change Group…"
    option in the line row context menu — lets users move rows into groups
    that currently have no members (and therefore no visible drop target)."""
    Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    data  = request.get_json(force=True) or {}
    group = (data.get("role_group") or "").strip() or None

    ln = BudgetLine.query.filter_by(id=lid, budget_id=bid).first_or_404()
    ln.role_group = group

    # Re-seat sort_order: place the line at the end of the chosen group's
    # cluster. If the group has no existing members, place the line at the
    # end of the section (so it picks up the group label visibly).
    section_lines = BudgetLine.query.filter_by(
        budget_id=bid, account_code=ln.account_code
    ).filter(BudgetLine.id != ln.id).order_by(
        BudgetLine.sort_order, BudgetLine.id
    ).all()

    # Find last index of a row with this role_group; insert after it.
    last_match = -1
    for i, sl in enumerate(section_lines):
        if sl.role_group == group:
            last_match = i
    if last_match >= 0:
        section_lines.insert(last_match + 1, ln)
    else:
        section_lines.append(ln)

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


@app.route("/projects/<int:pid>/budget/<int:bid>/line/<int:lid>/toggle-sync-omit", methods=["POST"])
@login_required
def toggle_sync_omit(pid, bid, lid):
    """Toggle sync_omit on a schedule-driven line. When True the line is excluded from auto-recalc."""
    ln = BudgetLine.query.filter_by(id=lid, budget_id=bid).first_or_404()
    ln.sync_omit = not bool(ln.sync_omit)
    if ln.sync_omit:
        # Zero the line out immediately so it stops contributing to section totals
        ln.estimated_total = 0
        ln.quantity = 0
    else:
        # Re-enable: trigger a sync so the line gets correct values back
        db.session.commit()
        try:
            sync_schedule_driven_lines(bid, db.session)
        except Exception:
            pass
        return jsonify({"ok": True, "sync_omit": False})
    db.session.commit()
    return jsonify({"ok": True, "sync_omit": ln.sync_omit})


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
            w.writerow([f"{COA_CODE_INSURANCE}*", f"Workers' Comp ({ts['workers_comp_pct']*100:.2f}% of labor)",
                        ts["workers_comp_amount"], "", "", ""])
        if ts.get("payroll_fee_amount", 0):
            w.writerow([f"{COA_CODE_ADMIN}*", f"Payroll Service Fee ({ts['payroll_fee_pct']*100:.2f}%)",
                        ts["payroll_fee_amount"], "", "", ""])
        if not ts.get("company_fee_dispersed"):
            w.writerow(["", "Company Fee", ts["company_fee"], "", "", ""])
        w.writerow(["", "GRAND TOTAL", ts["grand_total_estimated"],
                    ts["grand_total_actual"], ts["grand_variance"], ""])

    output.seek(0)
    fname = f"{budget.name.replace(' ', '_')}_{'working' if mode == 'working' else 'topsheet'}.csv"
    return Response(output.read(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={fname}"})


# ── Task 3: MMB / ShowBiz exports + preview ──────────────────────────────────

@app.route("/projects/<int:pid>/budget/<int:bid>/export.mmb.txt")
@login_required
def export_mmb(pid, bid):
    """Movie Magic Budgeting tab-delimited export. Import into MMB via
    'File → Import → From Tab-Delimited'."""
    _require_project_role(pid, 'editor')
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    from external_export import export_mmb_tab
    data = export_mmb_tab(budget)
    fname = f"{(budget.name or 'budget').replace(' ', '_')}_MMB.txt"
    return Response(data, mimetype="text/tab-separated-values",
                    headers={"Content-Disposition": f"attachment; filename={fname}"})


@app.route("/projects/<int:pid>/budget/<int:bid>/export.showbiz.txt")
@login_required
def export_showbiz(pid, bid):
    """ShowBiz Budgeting tab-delimited export."""
    _require_project_role(pid, 'editor')
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    from external_export import export_showbiz_tab
    data = export_showbiz_tab(budget)
    fname = f"{(budget.name or 'budget').replace(' ', '_')}_ShowBiz.txt"
    return Response(data, mimetype="text/tab-separated-values",
                    headers={"Content-Disposition": f"attachment; filename={fname}"})


@app.route("/projects/<int:pid>/budget/<int:bid>/preview/<target>")
@login_required
def export_preview(pid, bid, target):
    """Returns JSON for the right-drawer preview. target ∈ {mmb, showbiz}."""
    _require_project_role(pid, 'editor')
    if target not in ('mmb', 'showbiz'):
        return jsonify({"error": "Unknown target"}), 400
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    from external_export import preview_mmb, preview_showbiz
    data = preview_mmb(budget) if target == 'mmb' else preview_showbiz(budget)
    data["budget_name"] = budget.name
    data["target"] = target
    return jsonify(data)


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


# ── Smart CSV import ──────────────────────────────────────────────────────────
# Fuzzy synonym map: target_field -> list of header strings (lowercased) that should match
_CSV_FIELD_SYNONYMS = {
    'account_code':    ['account code', 'acct code', 'acct', 'coa', 'coa code', 'code', 'category code', 'chart code', 'section code'],
    'account_name':    ['account', 'account name', 'category', 'section', 'department', 'dept', 'coa name'],
    'description':     ['description', 'line item', 'item', 'line', 'detail', 'position', 'role'],
    'is_labor':        ['is labor', 'labor', 'is_labor'],
    'quantity':        ['qty', 'quantity', 'count', '#', 'units', 'people', 'num', 'number'],
    'days':            ['days', 'day', 'weeks', 'week'],
    'rate':            ['rate', 'daily', 'day rate', 'unit rate', 'price', 'amount', 'weekly rate', 'daily rate', '$'],
    'rate_type':       ['rate type', 'period', 'unit', 'rate_type'],
    'fringe_type':     ['fringe', 'fringe type', 'union', 'fringe_type'],
    'agent_pct':       ['agent', 'agent pct', 'agent %', 'commission', 'agent fee', 'agent_pct'],
    'est_ot':          ['ot', 'overtime', 'est ot', 'est_ot'],
    'estimated_total': ['total', 'estimated', 'flat', 'flat total', 'line total', 'budget', 'estimated total', 'subtotal'],
    'note':            ['note', 'notes', 'comment', 'comments', 'remark', 'remarks'],
}

_CSV_TARGET_FIELDS = list(_CSV_FIELD_SYNONYMS.keys())


def _fuzzy_suggest_target(header):
    """Return (best_target, confidence, [alternatives]) for a CSV header."""
    import difflib
    h = (header or '').strip().lower()
    if not h:
        return (None, 0.0, [])
    scores = []
    for target, synonyms in _CSV_FIELD_SYNONYMS.items():
        # Best match across all synonyms for this target
        best = 0.0
        for syn in synonyms:
            r = difflib.SequenceMatcher(None, h, syn).ratio()
            if r > best:
                best = r
        # Exact contains bonus
        if any(syn == h for syn in synonyms):
            best = 1.0
        elif any(syn in h or h in syn for syn in synonyms):
            best = max(best, 0.85)
        scores.append((target, best))
    scores.sort(key=lambda x: x[1], reverse=True)
    best_target, best_score = scores[0]
    if best_score < 0.55:
        return (None, best_score, [t for t, _ in scores[1:3]])
    alternatives = [t for t, s in scores[1:3] if s >= 0.4]
    return (best_target, round(best_score, 2), alternatives)


def _resolve_account_code(code_val, name_val):
    """Return a valid FP_COA section start code from a numeric code or section name.

    Handles multiple code conventions:
    - Raw FP code: 700 → 700 (Talent)
    - Showbiz-style decimal: 7.00 → 700 (x100 multiplier)
    - Short int: 20 → 2000 (x100 multiplier)
    - Section range snap: 712 → 700 (Talent)
    - Fuzzy section name match as last resort
    """
    section_codes = {c for c, _ in FP_COA_SECTIONS}
    sorted_codes = sorted(section_codes)

    if code_val is not None and str(code_val).strip():
        raw = None
        try:
            raw = float(str(code_val).replace(',', '').strip())
        except (ValueError, TypeError):
            pass
        if raw is not None and raw > 0:
            # Try exact match at multiple scales
            for candidate in [int(raw), int(raw * 100), int(raw * 1000)]:
                if candidate in section_codes:
                    return candidate
            # Range snap: find the section that contains this code
            # Use the scale that produces a sensible FP-range value (typically x100 if < 1000)
            candidate = int(raw * 100) if raw < 1000 else int(raw)
            best = None
            for c in sorted_codes:
                if candidate >= c:
                    best = c
                else:
                    break
            if best is not None and candidate - best <= 500:
                return best

    # Fall back to fuzzy-match section name
    if name_val:
        import difflib
        n = str(name_val).strip().lower()
        if n and not n.startswith('total'):
            best_code, best_score = None, 0.0
            for start, sec_name in FP_COA_SECTIONS:
                sl = sec_name.lower()
                r = difflib.SequenceMatcher(None, n, sl).ratio()
                if n in sl or sl in n:
                    r = max(r, 0.85)
                # Word-level overlap bonus
                n_words = set(w for w in n.split() if len(w) > 2)
                s_words = set(w for w in sl.split() if len(w) > 2)
                if n_words and s_words and n_words & s_words:
                    r = max(r, 0.7)
                if r > best_score:
                    best_score = r
                    best_code = start
            if best_score >= 0.55:
                return best_code
    return None


def _dedupe_headers(raw_headers):
    """Normalize CSV header row: strip, fill blanks, dedupe duplicates."""
    seen = {}
    out = []
    for i, h in enumerate(raw_headers):
        hh = (h or '').strip()
        if not hh:
            hh = f"Column {i + 1}"
        if hh in seen:
            seen[hh] += 1
            hh = f"{hh} ({seen[hh]})"
        else:
            seen[hh] = 1
        out.append(hh)
    return out


def _score_header_row(cells):
    """Count how many cells in a row look like recognizable field names."""
    score = 0
    for cell in cells:
        if not cell or not str(cell).strip():
            continue
        _, conf, _ = _fuzzy_suggest_target(str(cell))
        if conf >= 0.6:
            score += 1
    return score


@app.route("/projects/<int:pid>/budget/<int:bid>/import/analyze", methods=["POST"])
@login_required
def import_csv_analyze(pid, bid):
    """Analyze uploaded CSV: auto-detect header row, suggest column mappings,
    preview first 10 data rows."""
    Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "No file uploaded"}), 400

    try:
        content = f.read().decode("utf-8-sig")
    except UnicodeDecodeError:
        return jsonify({"error": "File must be UTF-8 encoded CSV"}), 400

    reader = csv.reader(io.StringIO(content))
    all_rows = list(reader)
    if not all_rows:
        return jsonify({"error": "Empty CSV"}), 400

    # Client may override auto-detection by passing header_row_index
    requested_header_idx = request.form.get("header_row_index", type=int)

    # Auto-detect header row: scan first 20 rows, pick the one with most field-like cells
    scan_rows = all_rows[: min(20, len(all_rows))]
    scores = [(idx, _score_header_row(row)) for idx, row in enumerate(scan_rows)]
    scores.sort(key=lambda x: x[1], reverse=True)
    best_score = scores[0][1] if scores else 0
    auto_header_idx = scores[0][0] if best_score >= 2 else 0

    header_idx = requested_header_idx if requested_header_idx is not None else auto_header_idx
    if header_idx < 0 or header_idx >= len(all_rows):
        header_idx = 0

    raw_headers = all_rows[header_idx]
    headers = _dedupe_headers(raw_headers)

    # Build fuzzy suggestions per header
    mappings = []
    for h in headers:
        target, confidence, alternatives = _fuzzy_suggest_target(h)
        mappings.append({
            "csv_col": h,
            "target": target,
            "confidence": confidence,
            "alternatives": alternatives,
        })

    # Pre-load existing lines for duplicate detection
    existing_lines = BudgetLine.query.filter_by(budget_id=bid).all()
    existing_index = {}
    for ln in existing_lines:
        key = (int(ln.account_code or 0), (ln.description or '').strip().lower())
        existing_index[key] = ln.id

    # Preview: first 10 data rows after the header
    data_rows = all_rows[header_idx + 1:]
    mapping_dict = {m["csv_col"]: m["target"] for m in mappings if m["target"]}
    preview = []
    for idx, row in enumerate(data_rows[:10]):
        row_dict = dict(zip(headers, row))
        parsed = {}
        for csv_col, target in mapping_dict.items():
            parsed[target] = row_dict.get(csv_col, '')
        resolved_code = _resolve_account_code(
            parsed.get('account_code'),
            parsed.get('account_name') or parsed.get('description'),
        )
        desc = (parsed.get('description') or '').strip().lower()
        dup_id = existing_index.get((resolved_code or 0, desc)) if resolved_code else None
        # Mark row as likely junk (total/empty/section) for UI hinting
        is_junk = _is_import_junk_row(parsed)
        preview.append({
            "row_index": idx,
            "csv_row": row_dict,
            "resolved_code": resolved_code,
            "duplicate_of_line_id": dup_id,
            "is_junk": is_junk,
        })

    # Return the first 20 raw rows for the header picker UI
    raw_preview = [list(r) for r in all_rows[: min(20, len(all_rows))]]

    return jsonify({
        "headers": headers,
        "mappings": mappings,
        "preview_rows": preview,
        "target_fields": [
            {"value": "", "label": "— skip this column —"},
            *[{"value": f, "label": f} for f in _CSV_TARGET_FIELDS],
        ],
        "row_count": len(data_rows),
        "coa_sections": [{"code": c, "name": n} for c, n in FP_COA_SECTIONS],
        "raw_preview": raw_preview,
        "header_row_index": header_idx,
        "auto_detected_header_index": auto_header_idx,
    })


def _is_import_junk_row(parsed):
    """Return True if a parsed row should be auto-skipped as junk
    (empty, total row, or section-header row)."""
    desc = (parsed.get('description') or '').strip()
    desc_low = desc.lower()
    code_raw = str(parsed.get('account_code') or '').strip()
    rate_raw = str(parsed.get('rate') or '').strip()
    total_raw = str(parsed.get('estimated_total') or '').strip()

    # Empty: no description and no rate and no total
    if not desc and not rate_raw and not total_raw:
        return True
    # Total rows (by convention: code "99" or "99.00", or description starts with "total")
    if code_raw in ('99', '99.0', '99.00'):
        return True
    if desc_low.startswith('total') or desc_low.startswith('subtotal'):
        return True
    # #REF!, #N/A, or similar error cells as description
    if desc_low.startswith('#'):
        return True
    return False


@app.route("/projects/<int:pid>/budget/<int:bid>/import/apply", methods=["POST"])
@login_required
def import_csv_apply(pid, bid):
    """Apply confirmed column mapping + per-row actions from a CSV upload."""
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    f = request.files.get("file")
    mapping_json = request.form.get("mapping", "{}")
    actions_json = request.form.get("row_actions", "[]")
    header_row_index = request.form.get("header_row_index", 0, type=int)
    auto_skip_junk = request.form.get("auto_skip_junk", "1") == "1"

    if not f:
        return jsonify({"error": "No file uploaded"}), 400

    try:
        mapping = json.loads(mapping_json)
        row_actions = json.loads(actions_json) if actions_json else []
    except (ValueError, TypeError):
        return jsonify({"error": "Invalid mapping or actions JSON"}), 400

    try:
        content = f.read().decode("utf-8-sig")
    except UnicodeDecodeError:
        return jsonify({"error": "File must be UTF-8 encoded CSV"}), 400

    reader = csv.reader(io.StringIO(content))
    all_rows = list(reader)
    if header_row_index < 0 or header_row_index >= len(all_rows):
        return jsonify({"error": "Header row index out of range"}), 400

    headers = _dedupe_headers(all_rows[header_row_index])
    data_rows = [dict(zip(headers, row)) for row in all_rows[header_row_index + 1:]]

    # Helper coercions
    def _f(v, default=0.0):
        try:
            if v in (None, ''):
                return default
            s = str(v).replace(',', '').replace('$', '').replace('%', '').strip()
            # Handle negatives in parentheses: (500) → -500
            if s.startswith('(') and s.endswith(')'):
                s = '-' + s[1:-1]
            return float(s)
        except (ValueError, TypeError):
            return default

    def _b(v):
        return str(v).strip().lower() in ("true", "1", "yes", "y", "labor")

    added = 0
    updated = 0
    skipped = 0
    auto_skipped_junk = 0
    errors = []

    for idx, row_dict in enumerate(data_rows):
        # Per-row action from preview (only covers first 10 rows; rest default to "new")
        action = row_actions[idx] if idx < len(row_actions) else "new"
        if action == "skip" or action is None:
            skipped += 1
            continue

        # Build field dict from mapping
        fields = {}
        for csv_col, target in mapping.items():
            if not target:
                continue
            fields[target] = row_dict.get(csv_col, '')

        # Auto-skip junk rows (totals, section headers, empties, #REF! rows)
        if auto_skip_junk and _is_import_junk_row(fields):
            auto_skipped_junk += 1
            continue

        # Resolve account_code
        code = _resolve_account_code(
            fields.get('account_code'),
            fields.get('account_name') or fields.get('description'),
        )
        if not code:
            errors.append(f"Row {idx + header_row_index + 2}: could not resolve account code")
            skipped += 1
            continue

        # Account name fallback to COA section name
        section_name_map = dict(FP_COA_SECTIONS)
        account_name = (fields.get('account_name') or '').strip() or section_name_map.get(code, '')

        values = {
            "account_code":    code,
            "account_name":    account_name,
            "description":     (fields.get('description') or '').strip(),
            "is_labor":        _b(fields.get('is_labor', '')),
            "quantity":        _f(fields.get('quantity', 1), 1) or 1,
            "days":            _f(fields.get('days', 1), 1) or 1,
            "rate":            _f(fields.get('rate', 0)),
            "rate_type":       (fields.get('rate_type') or 'day_10').strip() or 'day_10',
            "est_ot":          _f(fields.get('est_ot', 0)),
            "fringe_type":     (fields.get('fringe_type') or 'N').strip()[:1].upper() or 'N',
            "agent_pct":       _f(fields.get('agent_pct', 0)),
            "estimated_total": _f(fields.get('estimated_total', 0)),
            "note":            (fields.get('note') or '').strip(),
        }

        try:
            if isinstance(action, dict) and action.get('update'):
                ln = BudgetLine.query.filter_by(id=int(action['update']), budget_id=bid).first()
                if not ln:
                    errors.append(f"Row {idx + header_row_index + 2}: line to update not found")
                    skipped += 1
                    continue
                for k, v in values.items():
                    setattr(ln, k, v)
                updated += 1
            else:  # "new" or anything else
                db.session.add(BudgetLine(budget_id=bid, **values))
                added += 1
        except Exception as e:
            errors.append(f"Row {idx + header_row_index + 2}: {e}")
            skipped += 1

    try:
        db.session.commit()
        _touch_budget(bid)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Database error: {e}"}), 500

    return jsonify({
        "added": added,
        "updated": updated,
        "skipped": skipped,
        "auto_skipped_junk": auto_skipped_junk,
        "errors": errors[:50],  # cap error list
    })


# Legacy import route — kept for backward compatibility but redirects to the new UI
@app.route("/projects/<int:pid>/budget/<int:bid>/import", methods=["POST"])
@login_required
def import_csv(pid, bid):
    """Legacy naive import — redirects users to the new smart import UI."""
    flash("Please use the new Import CSV button to map columns.", "info")
    return redirect(url_for("budget_view", pid=pid, bid=bid) + "?tab=settings")


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
    if "fee_excluded_sections" in data:
        # Frontend sends an array of account-code ints (the sections the
        # user ticked as "exempt" in Settings). Empty array = fee applies
        # to every section. Written via raw SQL so the settings save
        # doesn't fail if the column migration hasn't landed yet — the
        # column is intentionally NOT declared on the Budget ORM model
        # for that reason (see models.py comment on company_fee_dispersed).
        raw = data.get("fee_excluded_sections") or []
        try:
            codes = sorted({int(c) for c in raw})
        except (TypeError, ValueError):
            codes = []
        import json as _j_fee
        _val = _j_fee.dumps(codes) if codes else None
        try:
            db.session.execute(
                text("UPDATE budget SET fee_excluded_sections = :v WHERE id = :i"),
                {"v": _val, "i": budget.id}
            )
        except Exception as _ue:
            # Column missing — log once and swallow so the rest of the
            # settings payload still commits. User will see exemptions
            # not persisted; they can retry once the column is added.
            logging.warning(f"[SETTINGS] fee_excluded_sections write failed ({_ue}); skipping")
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

    # All labor lines, ordered to match the Working Budget tab exactly.
    # Group by section, then cluster each section by sub-group so the Gantt
    # mirrors the budget layout (no duplicate Direction/AD sections, etc.)
    all_lines = BudgetLine.query.filter_by(budget_id=bid).order_by(
        BudgetLine.account_code, BudgetLine.sort_order).all()
    labor_lines_raw = [ln for ln in all_lines if ln.is_labor]
    # Cluster by sub-group within each account_code
    _by_section = {}
    _section_order = []
    for ln in labor_lines_raw:
        if ln.account_code not in _by_section:
            _by_section[ln.account_code] = []
            _section_order.append(ln.account_code)
        _by_section[ln.account_code].append(ln)
    labor_lines = []
    for code in _section_order:
        labor_lines.extend(_cluster_by_subgroup(_by_section[code]))

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
        if ln.account_code == COA_CODE_PROD_STAFF:
            sub_group = ln.role_group or _get_prod_staff_subgroup(ln.description)
        elif ln.account_code == COA_CODE_TALENT:
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

    # Auto-enable use_schedule on the line whenever a schedule day exists.
    # Previously only fired on the FIRST day — so if the flag got flipped off
    # for any reason, subsequent schedule edits silently stopped driving the
    # labor line total.
    use_schedule_toggled = False
    try:
        if line_id and day_type != 'off':
            ln = BudgetLine.query.filter_by(id=line_id, budget_id=bid).first()
            if ln and ln.is_labor and not ln.use_schedule:
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


@app.route("/projects/<int:pid>/budget/<int:bid>/gantt/days", methods=["POST"])
@login_required
def set_gantt_days_batch(pid, bid):
    """Batch upsert of ScheduleDay rows — one transaction, one totals sync.

    Used by the gantt paste flow to avoid N separate round-trips
    (each of which also triggered _touch_budget + sync_schedule_driven_lines).
    """
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    data   = request.get_json(force=True) or {}
    days   = data.get("days") or []
    if not isinstance(days, list) or not days:
        return jsonify({"error": "days array required"}), 400

    sched_mode        = 'working' if budget.budget_mode in ('working', 'actual') else 'estimated'
    affected_line_ids = set()
    applied           = 0

    for spec in days:
        try:
            line_id = int(spec.get("line_id") or 0) or None
        except (TypeError, ValueError):
            line_id = None
        date_str = spec.get("date")
        if not line_id or not date_str:
            continue
        try:
            d = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        day_type = spec.get("day_type", "work")
        try:
            crew_instance = int(spec.get("crew_instance") or 1)
        except (TypeError, ValueError):
            crew_instance = 1
        note       = spec.get("note")
        episode    = spec.get("episode")
        cell_flags = spec.get("cell_flags")

        existing = ScheduleDay.query.filter_by(
            budget_id=bid, budget_line_id=line_id,
            crew_instance=crew_instance, date=d,
            schedule_mode=sched_mode).first()

        if day_type == "off":
            if existing:
                db.session.delete(existing)
            affected_line_ids.add(line_id)
            applied += 1
            continue

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
            est_ot_hours = spec.get("est_ot_hours")
            if est_ot_hours is not None:
                existing.est_ot_hours = float(est_ot_hours)
        except (TypeError, ValueError):
            pass
        if cell_flags is not None:
            existing.cell_flags = json.dumps(cell_flags) if isinstance(cell_flags, dict) else cell_flags

        affected_line_ids.add(line_id)
        applied += 1

    # Primary commit — this is the one that MUST succeed
    db.session.commit()

    _post_save_error = None

    # One touch for the whole batch
    try:
        _touch_budget(bid)
        db.session.commit()
    except Exception as _te:
        import traceback as _tb
        _post_save_error = f"touch_budget: {_te}"
        app.logger.error("_touch_budget failed in set_gantt_days_batch: %s\n%s", _te, _tb.format_exc())
        try: db.session.rollback()
        except Exception: pass

    # One schedule-driven-line sync for the whole batch
    try:
        sync_schedule_driven_lines(bid, db.session)
    except Exception as _sdl_err:
        import traceback as _tb
        _post_save_error = (_post_save_error or "") + f" | sync_lines: {_sdl_err}"
        app.logger.error("sync_schedule_driven_lines failed in set_gantt_days_batch: %s\n%s",
                         _sdl_err, _tb.format_exc())
        try: db.session.rollback()
        except Exception: pass

    # Auto-enable use_schedule on any affected labor lines (mirror set_gantt_day behavior,
    # including zeroing est_ot so legacy manual OT doesn't carry into schedule mode).
    toggled_line_ids = []
    try:
        if affected_line_ids:
            lines = BudgetLine.query.filter(
                BudgetLine.id.in_(affected_line_ids),
                BudgetLine.budget_id == bid,
                BudgetLine.is_labor == True,
                BudgetLine.use_schedule == False,
            ).all()
            for ln in lines:
                ln.use_schedule = True
                ln.est_ot       = 0
                toggled_line_ids.append(ln.id)
            if toggled_line_ids:
                db.session.commit()
    except Exception as _ue:
        import traceback as _tb
        _post_save_error = (_post_save_error or "") + f" | use_sched: {_ue}"
        app.logger.error("use_schedule toggle failed in set_gantt_days_batch: %s\n%s", _ue, _tb.format_exc())
        try: db.session.rollback()
        except Exception: pass

    resp = {"ok": True, "applied": applied, "use_schedule_toggled_lines": toggled_line_ids}
    if _post_save_error:
        resp["_warn"] = _post_save_error
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
    _title_fields = {"name", "facility_name", "contact_name", "dayof_name"}
    for f in _loc_fields:
        if f in data:
            val = data[f] if data[f] != "" else None
            if f in _phone_fields:
                val = _normalize_phone(val)
            elif f in _email_fields and val and not _validate_email(val):
                return jsonify({"error": f"Invalid email: {val}"}), 400
            elif f in _title_fields and val:
                val = val.strip().title()
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


@app.route("/projects/<int:pid>/locations/picker-list")
@login_required
def location_picker_list(pid):
    """JSON list of candidate locations to assign to a budget line.
    Returns project-specific locations first (source='project') then
    global library locations (source='library'). Used by the Assign
    Location modal on 3300 budget lines."""
    ProjectSheet.query.get_or_404(pid)
    proj = Location.query.filter_by(project_id=pid, active=True).order_by(Location.name).all()
    libs = Location.query.filter_by(project_id=None, active=True).order_by(Location.name).all()

    def _row(l, source):
        return {
            "id": l.id,
            "name": l.name,
            "facility_name": l.facility_name or "",
            "location_type": l.location_type or "",
            "address": l.address or "",
            "source": source,
            "assigned_to_line_id": l.budget_line_id,
        }

    return jsonify({
        "project": [_row(l, "project") for l in proj],
        "library": [_row(l, "library") for l in libs],
    })


@app.route("/projects/<int:pid>/budget/<int:bid>/line/<int:lid>/assign-location", methods=["POST"])
@login_required
def line_assign_location(pid, bid, lid):
    """Assign a Location to a budget line. One location per line: if the
    line had a previous location assigned, its FK is cleared. Pass
    location_id=null to clear the current assignment without assigning
    a new one."""
    Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    ln = BudgetLine.query.filter_by(id=lid, budget_id=bid).first_or_404()
    data = request.get_json(force=True) or {}
    location_id = data.get("location_id")

    try:
        # Clear any existing assignment for this line.
        Location.query.filter_by(budget_line_id=lid).update({"budget_line_id": None})

        assigned = None
        if location_id is not None and location_id != "":
            loc = Location.query.get(int(location_id))
            if not loc:
                return jsonify({"error": f"location {location_id} not found"}), 404
            loc.budget_line_id = lid
            assigned = {"id": loc.id, "name": loc.name, "facility_name": loc.facility_name or ""}

        db.session.commit()
        _touch_budget(bid)
        db.session.commit()
        return jsonify({"ok": True, "assigned": assigned})
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 400


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
    default_fringe    = cm.default_fringe if cid and cm else None
    return jsonify({"ok": True, "crew_id": cid, "name": name,
                    "agent_pct": agent_pct_applied,
                    "default_rate": default_rate,
                    "default_rate_type": default_rate_type,
                    "default_fringe": default_fringe,
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
    # Primary agent per crew member (role_type='agent', active)
    agent_rows = SupportContact.query.filter_by(role_type='agent', active=True).all()
    agent_map = {}  # crew_member_id → first agent SupportContact
    for ag in agent_rows:
        if ag.crew_member_id not in agent_map:
            agent_map[ag.crew_member_id] = ag
    return render_template("crew.html", members=members, crew_projects=crew_projects,
                           agent_map=agent_map)


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
    # When the primary agent's fee is set, keep CrewMember.default_agent_pct in sync
    if s.role_type == 'agent' and s.fee_pct is not None:
        cm = CrewMember.query.get(cid)
        if cm:
            # Only update if this is the only/first active agent or matches current default
            other_agents = SupportContact.query.filter(
                SupportContact.crew_member_id == cid,
                SupportContact.role_type == 'agent',
                SupportContact.active == True,
                SupportContact.id != s.id,
            ).first()
            if not other_agents:
                cm.default_agent_pct = s.fee_pct
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

    # Determine day type for the selected date and compute shoot-day number
    # Only "work" days count toward the shoot day number; travel/hold/half get their own label
    _day_types_today = db.session.query(ScheduleDay.day_type).filter(
        ScheduleDay.budget_id == bid,
        ScheduleDay.schedule_mode == sched_mode,
        ScheduleDay.date == selected_date,
        ScheduleDay.day_type != 'off',
    ).distinct().all()
    _day_types_today = [r[0] for r in _day_types_today]
    primary_day_type = _day_types_today[0] if _day_types_today else None

    # Shoot day number = position among work days only
    shooting_day_num = None
    _work_dates = [r[0] for r in db.session.query(ScheduleDay.date).filter(
        ScheduleDay.budget_id == bid,
        ScheduleDay.schedule_mode == sched_mode,
        ScheduleDay.day_type == 'work',
    ).distinct().order_by(ScheduleDay.date).all()]
    _total_shoot_days = len(_work_dates)
    if selected_date in _work_dates:
        shooting_day_num = _work_dates.index(selected_date) + 1
    elif selected_date in all_scheduled_dates and primary_day_type:
        # Non-work day: show day-type label but no shoot day number
        shooting_day_num = None

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

    # Meal headcounts — use actual crew count on this shoot day, not budget line quantity.
    # days_today already contains non-off ScheduleDay rows for selected_date.
    _day_headcount = len([d for d in days_today if d.day_type != 'off'])
    meal_counts = {}
    if prod_day:
        if prod_day.courtesy_breakfast:
            meal_counts['courtesy_breakfast'] = _day_headcount
        if prod_day.first_meal:
            meal_counts['first_meal'] = _day_headcount
        if prod_day.second_meal:
            meal_counts['second_meal'] = _day_headcount
    # Craft services: always counts if anyone is scheduled
    if _day_headcount:
        meal_counts['craft_services'] = _day_headcount
    # Working meals: per-person cell flag count for this day
    def _parse_flags(cf):
        try: return json.loads(cf) if cf else {}
        except Exception: return {}
    _wm_count = sum(
        1 for d in days_today
        if d.day_type != 'off' and _parse_flags(d.cell_flags).get('working_meal')
    )
    if _wm_count:
        meal_counts['working_meal'] = _wm_count

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

    # Separate ATL/Talent from crew.
    # 2026-04 renumber: ATL no longer has its own section code — ATL roles
    # live inside Production Staff (COA_CODE_PROD_STAFF=2000). We identify
    # them by role-name match against _ATL_ROLE_LABELS. Talent is its own
    # section (COA_CODE_TALENT=2100). Everything else labor-like is BG crew.
    def _row_is_atl(r):
        if r['account_code'] != COA_CODE_PROD_STAFF:
            return False
        role = (r.get('role') or '').strip().lower()
        return any(tok in role for tok in _ATL_ROLE_LABELS)
    atl_rows     = [r for r in crew_rows if _row_is_atl(r)]
    talent_rows  = [r for r in crew_rows if r['account_code'] == COA_CODE_TALENT]
    crew_rows_bg = [r for r in crew_rows
                    if r['account_code'] != COA_CODE_TALENT and not _row_is_atl(r)]

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
        # 2026-04 renumber: "Above the Line" is no longer a section code;
        # ATL roles live in Production Staff. Detect by role name.
        if _row_is_atl(r):
            return "Above the Line"
        if r['account_code'] == COA_CODE_TALENT:
            return "Talent"
        # Production Staff or anything else — use subgroup or section_name
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
        total_shoot_days=_total_shoot_days,
        primary_day_type=primary_day_type,
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
        meal_counts=meal_counts,
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
    # Auto-sync budget start/end dates from schedule
    _sync_budget_dates_from_schedule(bid)


def _sync_budget_dates_from_schedule(bid):
    """Set budget.start_date / end_date from min/max of ScheduleDay dates."""
    try:
        row = db.session.query(
            func.min(ScheduleDay.date), func.max(ScheduleDay.date)
        ).filter(ScheduleDay.budget_id == bid).first()
        if row and row[0]:
            budget = Budget.query.get(bid)
            if budget:
                budget.start_date = row[0]
                budget.end_date = row[1]
    except Exception:
        pass


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



def _infer_line_subgroup(ln):
    """Infer a line's sub-group (Direction / AD, Camera, etc.) from its
    role_group field OR by keyword-matching the description against
    _PROD_STAFF_SUBGROUPS / _TALENT_SUBGROUPS. Returns None for sections
    that don't use sub-groups."""
    rg = getattr(ln, 'role_group', None)
    if rg:
        return rg
    code = int(getattr(ln, 'account_code', 0) or 0)
    if code == COA_CODE_PROD_STAFF:
        for kw, grp in _PROD_STAFF_SUBGROUPS:
            if kw.lower() in (ln.description or '').lower():
                return grp
    elif code == COA_CODE_TALENT:
        try:
            return _get_talent_subgroup(ln.description)
        except Exception:
            return None
    return None


def _cluster_by_subgroup(lines):
    """Cluster a list of lines by their sub-group, preserving first-appearance
    order of each group. Used to keep same-group lines together so we don't
    render duplicate sub-department headers when new lines are added later.
    Only clusters lines in sub-grouped sections (currently 1000 Production Staff).
    Other sections pass through unchanged."""
    is_subgrouped = any(
        int(getattr(ln, 'account_code', 0) or 0) == COA_CODE_PROD_STAFF for ln in lines
    )
    if not is_subgrouped:
        return list(lines)
    group_order = []
    buckets = {}
    for ln in lines:
        g = _infer_line_subgroup(ln) or ''
        if g not in buckets:
            buckets[g] = []
            group_order.append(g)
        buckets[g].append(ln)
    ordered = []
    for g in group_order:
        ordered.extend(sorted(buckets[g], key=lambda x: (x.sort_order or 0, x.id)))
    return ordered


def _order_lines_with_children(lines):
    """Return lines reordered so:
    1. Lines with the same role_group are clustered together (no duplicate
       sub-department headers when new Quick Entry lines are added later)
    2. Kit-fee/child rows appear immediately after their parent
    Ordering within each group is stable by (sort_order, id).
    """
    children_by_parent = {}
    parents = []
    for ln in lines:
        pid = getattr(ln, 'parent_line_id', None)
        if pid:
            children_by_parent.setdefault(pid, []).append(ln)
        else:
            parents.append(ln)

    ordered_parents = _cluster_by_subgroup(parents)

    result = []
    for ln in ordered_parents:
        result.append(ln)
        result.extend(children_by_parent.get(ln.id, []))

    # Orphaned children (parent in different section) go at end
    all_parent_ids = {ln.id for ln in ordered_parents}
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


_RATE_TYPE_LABELS = {
    'day_10':       '10hr Day',
    'day_8':        '8hr Day',
    'day_12':       '12hr Day',
    'flat_day':     'Flat Day',
    'flat_project': 'Flat Project',
    'hourly':       'Hourly',
    'custom':       'Custom',
}

@app.template_filter("rate_type_label")
def rate_type_label_filter(v):
    return _RATE_TYPE_LABELS.get(v, v or '—')


# ── Admin routes ──────────────────────────────────────────────────────────────

@app.route("/admin", methods=["GET"])
@login_required
@admin_required
def admin_panel():
    users          = User.query.order_by(User.name).all()
    projects       = ProjectSheet.query.filter(ProjectSheet.status != 'archived').order_by(ProjectSheet.name).all()
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

    valid_roles = ('super_admin', 'admin', 'line_producer', 'dept_head', 'docs_only')
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

    valid_roles = ('super_admin', 'admin', 'line_producer', 'dept_head', 'docs_only')
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


# ── Global Quick Entry Catalog (Super Admin only) ─────────────────────────────

def _catalog_item_to_dict(ci):
    return {
        "id": ci.id,
        "category_code": ci.category_code,
        "category_name": ci.category_name,
        "label": ci.label,
        "group_name": ci.group_name,
        "is_labor": bool(ci.is_labor),
        "rate": float(ci.rate or 0),
        "qty": float(ci.qty or 1),
        "days": float(ci.days or 1),
        "kit_fee": float(ci.kit_fee or 0),
        "fringe": ci.fringe,
        "union_fringe": ci.union_fringe,
        "agent_pct": float(ci.agent_pct or 0),
        "comp": ci.comp,
        "unit": ci.unit,
        "sort_order": ci.sort_order or 0,
        "is_active": bool(ci.is_active),
    }


@app.route("/api/catalog")
@login_required
def api_catalog():
    """Return all active catalog items grouped by category. Available to any
    logged-in user (Quick Entry on the budget page uses this)."""
    items = CatalogItem.query.filter_by(is_active=True).order_by(
        CatalogItem.category_code, CatalogItem.sort_order, CatalogItem.id
    ).all()
    by_cat = {}
    for ci in items:
        cat = by_cat.setdefault(ci.category_code, {
            "code": ci.category_code,
            "name": ci.category_name,
            "items": [],
        })
        cat["items"].append(_catalog_item_to_dict(ci))
    return jsonify({"categories": list(by_cat.values())})


@app.route("/admin/catalog")
@login_required
@super_admin_required
def admin_catalog_view():
    """Super admin catalog editor page."""
    items = CatalogItem.query.order_by(
        CatalogItem.category_code, CatalogItem.sort_order, CatalogItem.id
    ).all()
    return render_template("admin_catalog.html",
                           items=[_catalog_item_to_dict(i) for i in items],
                           coa_sections=FP_COA_SECTIONS)


@app.route("/admin/catalog/item", methods=["POST"])
@login_required
@super_admin_required
def admin_catalog_item_create():
    data = request.get_json(force=True) or {}
    try:
        code = int(data.get("category_code") or 0)
        label = (data.get("label") or "").strip()
        if not code or not label:
            return jsonify({"error": "category_code and label required"}), 400
        # Resolve category_name from COA
        cname = dict(FP_COA_SECTIONS).get(code, data.get("category_name") or "")
        group_name = (data.get("group_name") or None) or None

        # Auto-compute sort_order so the new item lands inside its existing
        # sub-group cluster. Previously we defaulted to 0, which placed every
        # new row at the TOP of the section, breaking the "Direction / AD"
        # cluster rendering (new row rendered its own group header instead of
        # joining the existing one). Logic:
        #   1. If caller provided sort_order explicitly, honor it.
        #   2. Else if any row already exists with the same (code, group_name),
        #      place the new row 10 after that cluster's MAX sort_order so it
        #      becomes the LAST member of that group.
        #   3. Else (first row in this group or ungrouped), append to the end
        #      of the whole section — 10 after the MAX sort_order at this code.
        if "sort_order" in data and data.get("sort_order") is not None:
            sort_order = int(data.get("sort_order") or 0)
        else:
            same_group_q = CatalogItem.query.filter_by(
                category_code=code, group_name=group_name
            ).order_by(CatalogItem.sort_order.desc()).first()
            if same_group_q:
                sort_order = int(same_group_q.sort_order or 0) + 10
            else:
                last_in_cat = CatalogItem.query.filter_by(
                    category_code=code
                ).order_by(CatalogItem.sort_order.desc()).first()
                sort_order = (int(last_in_cat.sort_order or 0) + 10) if last_in_cat else 0

        ci = CatalogItem(
            category_code=code,
            category_name=cname,
            label=label,
            group_name=group_name,
            is_labor=bool(data.get("is_labor", False)),
            rate=float(data.get("rate") or 0),
            qty=float(data.get("qty") or 1),
            days=float(data.get("days") or 1),
            kit_fee=float(data.get("kit_fee") or 0),
            fringe=(data.get("fringe") or None) or None,
            union_fringe=(data.get("union_fringe") or None) or None,
            agent_pct=float(data.get("agent_pct") or 0),
            comp=(data.get("comp") or "labor"),
            unit=(data.get("unit") or "day"),
            sort_order=sort_order,
            is_active=True,
        )
        db.session.add(ci)
        db.session.commit()
        return jsonify(_catalog_item_to_dict(ci))
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 400


@app.route("/admin/catalog/item/<int:iid>", methods=["POST"])
@login_required
@super_admin_required
def admin_catalog_item_update(iid):
    ci = CatalogItem.query.get_or_404(iid)
    data = request.get_json(force=True) or {}
    try:
        # Allow updating any editable field
        if "label" in data:
            ci.label = (data["label"] or "").strip() or ci.label
        if "category_code" in data:
            code = int(data["category_code"])
            ci.category_code = code
            ci.category_name = dict(FP_COA_SECTIONS).get(code, ci.category_name)
        if "group_name" in data:
            ci.group_name = (data["group_name"] or None) or None
        if "is_labor" in data:
            ci.is_labor = bool(data["is_labor"])
        for fld in ("rate", "qty", "days", "kit_fee", "agent_pct"):
            if fld in data:
                setattr(ci, fld, float(data[fld] or 0))
        for fld in ("fringe", "union_fringe", "comp", "unit"):
            if fld in data:
                setattr(ci, fld, (data[fld] or None) or None)
        if "sort_order" in data:
            ci.sort_order = int(data["sort_order"] or 0)
        if "is_active" in data:
            ci.is_active = bool(data["is_active"])
        db.session.commit()
        return jsonify(_catalog_item_to_dict(ci))
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 400


@app.route("/admin/catalog/item/<int:iid>/delete", methods=["POST"])
@login_required
@super_admin_required
def admin_catalog_item_delete(iid):
    """Soft delete via is_active=False. Preserves the row so it can be
    restored later via toggle_active. Use /purge for irreversible hard
    delete (e.g. cleaning up legacy pre-renumber rows)."""
    ci = CatalogItem.query.get_or_404(iid)
    ci.is_active = False
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/admin/catalog/item/<int:iid>/purge", methods=["POST"])
@login_required
@super_admin_required
def admin_catalog_item_purge(iid):
    """Hard delete — permanently remove the row from the DB. Irreversible.
    Any budget_line rows that reference this CatalogItem via catalog_item_id
    have that FK cleared (set to NULL) so existing budgets aren't orphaned
    or broken — exports fall back to fuzzy match on (account_code,
    description) when catalog_item_id is NULL."""
    ci = CatalogItem.query.get_or_404(iid)
    try:
        # Null out any FK references from budget_line so we don't violate the
        # FK constraint on delete.
        db.session.execute(
            text("UPDATE budget_line SET catalog_item_id = NULL WHERE catalog_item_id = :iid"),
            {"iid": iid}
        )
        db.session.delete(ci)
        db.session.commit()
        return jsonify({"ok": True})
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 400


@app.route("/admin/catalog/reorder", methods=["POST"])
@login_required
@super_admin_required
def admin_catalog_reorder():
    """Bulk sort_order update. Payload: {order: [id1, id2, id3, ...]}"""
    data = request.get_json(force=True) or {}
    order = data.get("order") or []
    for i, iid in enumerate(order):
        ci = CatalogItem.query.get(int(iid))
        if ci:
            ci.sort_order = i * 10
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/admin/catalog/reseed", methods=["POST"])
@login_required
@super_admin_required
def admin_catalog_reseed():
    """Force a re-seed from FP_CATALOG_SEED. Idempotent — only inserts
    items not already in the DB. Each row commits independently so one
    bad row doesn't block the rest. Returns a count and any failures."""
    from budget_calc import seed_catalog as _seed
    try:
        added, failed = _seed(db.session)
        return jsonify({
            "ok": True,
            "added": added,
            "failed_count": len(failed),
            "failures": [{"row": list(r[0]) if isinstance(r[0], tuple) else str(r[0]),
                          "error": r[1]} for r in failed[:20]],  # first 20
        })
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@app.route("/admin/catalog/stats")
@login_required
@super_admin_required
def admin_catalog_stats():
    """Diagnostic: return counts per category_code in the CatalogItem
    table, flagging any that are legacy (pre-renumber) codes. Useful
    for diagnosing why QE might render unexpected codes."""
    from budget_calc import COA_LEGACY_MAPPING, FP_COA_NAMES
    rows = db.session.execute(text(
        "SELECT category_code, category_name, COUNT(*) AS n "
        "FROM catalog_item "
        "GROUP BY category_code, category_name "
        "ORDER BY category_code"
    )).fetchall()
    legacy_codes = set(COA_LEGACY_MAPPING.keys())
    current_codes = set(FP_COA_NAMES.keys())
    stats = []
    for code, name, n in rows:
        code = int(code or 0)
        stats.append({
            "code": code,
            "name": name,
            "count": int(n),
            "is_legacy": code in legacy_codes,
            "is_current": code in current_codes,
            "expected_name": FP_COA_NAMES.get(code) if code in current_codes else None,
            "legacy_maps_to": COA_LEGACY_MAPPING.get(code) if code in legacy_codes else None,
        })
    return jsonify({
        "total_rows": sum(s["count"] for s in stats),
        "sections": stats,
        "current_codes": sorted(current_codes),
        "legacy_codes": sorted(legacy_codes),
    })


@app.route("/admin/catalog/purge-legacy-duplicates", methods=["POST"])
@login_required
@super_admin_required
def admin_catalog_purge_legacy_duplicates():
    """For each CatalogItem at a legacy (pre-renumber) code, if a row
    already exists at the mapped new code with the same label, HARD
    DELETE the legacy row. Leaves legacy-only rows (no new-code twin)
    alone — those are handled by /admin/catalog/repair-codes which
    remaps them. Returns per-row report.

    Uses case-insensitive, whitespace-trimmed label comparison so minor
    variance ("Director " vs "Director") still collapses."""
    from budget_calc import COA_LEGACY_MAPPING, FP_COA_NAMES

    # Build a lookup of (new_code, normalized_label) → id for fast collision
    # detection. Only considers rows currently at current codes.
    current_codes = set(FP_COA_NAMES.keys())

    def _norm(s):
        return (s or '').strip().lower()

    new_index = {}  # (new_code, norm_label) -> [ids]
    for ci in CatalogItem.query.all():
        code = int(ci.category_code or 0)
        if code in current_codes and code not in COA_LEGACY_MAPPING:
            new_index.setdefault((code, _norm(ci.label)), []).append(ci.id)
        elif code in current_codes:
            # Code is BOTH legacy and current (e.g. 1000, 2000, 3100) —
            # treat as current. Legacy-duplicate detection in that case
            # is still handled below because we walk legacy rows by
            # matching NEW code to same row's code.
            new_index.setdefault((code, _norm(ci.label)), []).append(ci.id)

    deleted = []
    errors = []

    for ci in list(CatalogItem.query.all()):
        code = int(ci.category_code or 0)
        if code not in COA_LEGACY_MAPPING:
            continue
        new_code = COA_LEGACY_MAPPING[code]
        key = (new_code, _norm(ci.label))
        # Collision = at least one row at the NEW code with the same label
        # that is NOT this row itself.
        peer_ids = [i for i in new_index.get(key, []) if i != ci.id]
        if not peer_ids:
            continue
        try:
            # Null out any FK refs from budget_line first.
            db.session.execute(
                text("UPDATE budget_line SET catalog_item_id = NULL WHERE catalog_item_id = :iid"),
                {"iid": ci.id}
            )
            db.session.delete(ci)
            db.session.commit()
            deleted.append({
                "id": ci.id,
                "legacy_code": code,
                "new_code": new_code,
                "label": ci.label,
                "kept_id": peer_ids[0],
            })
        except Exception as e:
            db.session.rollback()
            errors.append(f"delete {ci.id} ({code} {ci.label!r}): {e}")

    return jsonify({
        "ok": True,
        "deleted_count": len(deleted),
        "error_count": len(errors),
        "deleted": deleted[:50],  # first 50 for inspection
        "errors": errors[:20],
    })


@app.route("/admin/catalog/rehouse-staff-from/<int:from_code>", methods=["POST"])
@login_required
@super_admin_required
def admin_catalog_rehouse_staff(from_code):
    """Targeted one-shot: for every CatalogItem currently at `from_code`
    whose label canonically belongs at code 2000 (Production Staff) per
    FP_CATALOG_SEED, either MOVE it to 2000 or DELETE it if a (2000, label)
    row already exists.

    Used to clean up sections like 2600 Camera Equipment that got
    contaminated with staff roles (Camera Operator, DP, 1st AC, etc.) —
    those belong in 2000 Production Staff under the Camera sub-group.

    Pass ?dry_run=1 to preview without mutating. Default is apply.

    Label matching is case-insensitive + whitespace-trimmed. Only labels
    that EXIST in FP_CATALOG_SEED at code 2000 are considered — this
    protects unrelated custom labels at `from_code` from being swept up.
    """
    from budget_calc import FP_CATALOG_SEED, FP_COA_NAMES

    dry_run = request.args.get("dry_run") == "1" or (request.get_json(silent=True) or {}).get("dry_run")

    def _norm(s):
        return (s or '').strip().lower()

    # Build the canonical set of labels that belong at code 2000.
    staff_labels = set()
    # Preserve the seed's canonical group_name for each label so we can
    # populate role_group on moved rows (Camera, Sound, Art, etc.).
    staff_group_by_label = {}
    for tup in FP_CATALOG_SEED:
        try:
            code, _cname, label, group = int(tup[0]), tup[1], tup[2], tup[3]
        except Exception:
            continue
        if code == 2000:
            staff_labels.add(_norm(label))
            if group:
                staff_group_by_label[_norm(label)] = group

    # Index existing rows at code 2000 for collision check.
    existing_2000 = {_norm(ci.label): ci.id for ci in CatalogItem.query.filter_by(category_code=2000).all()}

    candidates = CatalogItem.query.filter_by(category_code=from_code).all()
    will_move = []
    will_delete = []
    will_skip = []

    for ci in candidates:
        nlabel = _norm(ci.label)
        if nlabel not in staff_labels:
            will_skip.append({"id": ci.id, "label": ci.label, "reason": "label is not a known 2000 role"})
            continue
        if nlabel in existing_2000 and existing_2000[nlabel] != ci.id:
            will_delete.append({
                "id": ci.id,
                "from_code": from_code,
                "label": ci.label,
                "kept_id_at_2000": existing_2000[nlabel],
            })
        else:
            will_move.append({
                "id": ci.id,
                "from_code": from_code,
                "label": ci.label,
                "new_group": staff_group_by_label.get(nlabel),
            })

    if dry_run:
        return jsonify({
            "ok": True,
            "dry_run": True,
            "from_code": from_code,
            "move_count": len(will_move),
            "delete_count": len(will_delete),
            "skip_count": len(will_skip),
            "will_move": will_move,
            "will_delete": will_delete,
            "will_skip": will_skip[:50],
        })

    # APPLY — per-row commits so one failure doesn't rollback the batch.
    moved_count = 0
    deleted_count = 0
    errors = []
    new_name_2000 = FP_COA_NAMES.get(2000, "Production Staff")

    for m in will_move:
        try:
            ci = CatalogItem.query.get(m["id"])
            if ci is None:
                continue
            ci.category_code = 2000
            ci.category_name = new_name_2000
            # Populate/overwrite group_name from the canonical seed group
            if m.get("new_group"):
                ci.group_name = m["new_group"]
            db.session.commit()
            moved_count += 1
        except Exception as e:
            db.session.rollback()
            errors.append(f"move {m['id']} {m['label']!r}: {e}")

    for d in will_delete:
        try:
            ci = CatalogItem.query.get(d["id"])
            if ci is None:
                continue
            # Null FK references before hard delete.
            db.session.execute(
                text("UPDATE budget_line SET catalog_item_id = NULL WHERE catalog_item_id = :iid"),
                {"iid": ci.id}
            )
            db.session.delete(ci)
            db.session.commit()
            deleted_count += 1
        except Exception as e:
            db.session.rollback()
            errors.append(f"delete {d['id']} {d['label']!r}: {e}")

    return jsonify({
        "ok": True,
        "dry_run": False,
        "from_code": from_code,
        "moved_count": moved_count,
        "deleted_count": deleted_count,
        "skip_count": len(will_skip),
        "error_count": len(errors),
        "errors": errors[:20],
        "moved": will_move[:50],
        "deleted": will_delete[:50],
    })


@app.route("/admin/catalog/wipe-and-reseed", methods=["POST"])
@login_required
@super_admin_required
def admin_catalog_wipe_and_reseed():
    """Destructive one-shot: DELETE every row in catalog_item, then reseed
    from FP_CATALOG_SEED (which is kept 1:1 with the hardcoded QE list).

    Exists so the user can fix catalog drift immediately without waiting
    on a boot migration. Any budget_line.catalog_item_id FKs are NULLed
    before the delete so existing budgets aren't broken — exports
    fall back to fuzzy (account_code, description) match when FK is NULL.
    """
    from budget_calc import seed_catalog as _seed
    try:
        # NULL FK references from budget_line so the DELETE doesn't violate
        # the FK constraint.
        _nulled = db.session.execute(text(
            "UPDATE budget_line SET catalog_item_id = NULL "
            "WHERE catalog_item_id IS NOT NULL"
        )).rowcount
        # Wipe.
        _deleted = db.session.execute(text("DELETE FROM catalog_item")).rowcount
        db.session.commit()

        # Reseed. Expansion error handling already per-row internally.
        _added, _failed = _seed(db.session)

        return jsonify({
            "ok": True,
            "deleted": _deleted,
            "fks_nulled": _nulled,
            "inserted": _added,
            "failed_count": len(_failed),
            "failures": [{"row": list(r[0]) if isinstance(r[0], tuple) else str(r[0]),
                          "error": r[1]} for r in _failed[:20]],
        })
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@app.route("/admin/catalog/bulk-move", methods=["POST"])
@login_required
@super_admin_required
def admin_catalog_bulk_move():
    """Move a list of CatalogItem ids to a different category_code in one
    request. Body: {ids: [...], to_code: 2000}. For each row:
      - If (to_code, label) already exists in DB → SKIP (returned in
        collisions list) so we don't violate the (code, label) unique
        constraint. User can decide to delete the duplicate via bulk
        delete if they really want.
      - Else → update category_code + category_name (synced from
        FP_COA_NAMES) + auto-compute sort_order to land at the end
        of the destination section.

    Does NOT touch group_name — moving across sections typically means
    the group label is still valid (e.g. Camera → Camera). User can
    edit the group cell inline on the admin table afterwards.
    """
    from budget_calc import FP_COA_NAMES
    data = request.get_json(force=True) or {}
    ids = data.get("ids") or []
    try:
        ids = [int(x) for x in ids]
        to_code = int(data.get("to_code"))
    except Exception:
        return jsonify({"error": "ids must be a list of integers and to_code required"}), 400
    if not ids:
        return jsonify({"ok": True, "moved": 0})
    if to_code not in FP_COA_NAMES:
        return jsonify({"error": f"to_code {to_code} is not a known COA section"}), 400

    new_name = FP_COA_NAMES[to_code]

    # Pre-index existing (to_code, label) to detect collisions.
    existing_at_target = {
        (ci.label or '').strip().lower(): ci.id
        for ci in CatalogItem.query.filter_by(category_code=to_code).all()
    }

    # Compute the starting sort_order for new arrivals (end of target section).
    last_in_target = CatalogItem.query.filter_by(
        category_code=to_code
    ).order_by(CatalogItem.sort_order.desc()).first()
    next_sort = (int(last_in_target.sort_order or 0) + 10) if last_in_target else 0

    moved = 0
    collisions = []
    errors = []

    for iid in ids:
        try:
            ci = CatalogItem.query.get(iid)
            if ci is None:
                continue
            if int(ci.category_code or 0) == to_code:
                continue  # already there
            norm_label = (ci.label or '').strip().lower()
            collide_id = existing_at_target.get(norm_label)
            if collide_id and collide_id != ci.id:
                collisions.append({
                    "id": ci.id,
                    "label": ci.label,
                    "from_code": int(ci.category_code or 0),
                    "existing_at_target_id": collide_id,
                })
                continue
            ci.category_code = to_code
            ci.category_name = new_name
            ci.sort_order = next_sort
            next_sort += 10
            db.session.commit()
            existing_at_target[norm_label] = ci.id
            moved += 1
        except Exception as e:
            db.session.rollback()
            errors.append(f"id={iid}: {e}")

    return jsonify({
        "ok": True,
        "moved": moved,
        "collision_count": len(collisions),
        "collisions": collisions[:50],
        "error_count": len(errors),
        "errors": errors[:20],
    })


@app.route("/admin/catalog/bulk-delete", methods=["POST"])
@login_required
@super_admin_required
def admin_catalog_bulk_delete():
    """Hard-delete a list of CatalogItem ids in one request. Body:
    {ids: [1, 2, 3, ...]}. Nulls out any budget_line.catalog_item_id FKs
    that reference the deleted rows (same behavior as single purge)."""
    data = request.get_json(force=True) or {}
    ids = data.get("ids") or []
    try:
        ids = [int(x) for x in ids]
    except Exception:
        return jsonify({"error": "ids must be a list of integers"}), 400
    if not ids:
        return jsonify({"ok": True, "deleted": 0})

    deleted = 0
    errors = []
    # Null FK references in one statement to avoid N separate updates.
    try:
        db.session.execute(
            text("UPDATE budget_line SET catalog_item_id = NULL "
                 "WHERE catalog_item_id = ANY(:ids)"),
            {"ids": ids}
        )
        db.session.commit()
    except Exception:
        # Non-PG fallback — loop per-id.
        db.session.rollback()
        for iid in ids:
            try:
                db.session.execute(
                    text("UPDATE budget_line SET catalog_item_id = NULL WHERE catalog_item_id = :iid"),
                    {"iid": iid}
                )
            except Exception:
                pass
        db.session.commit()

    for iid in ids:
        try:
            ci = CatalogItem.query.get(iid)
            if ci is None:
                continue
            db.session.delete(ci)
            db.session.commit()
            deleted += 1
        except Exception as e:
            db.session.rollback()
            errors.append(f"id={iid}: {e}")

    return jsonify({
        "ok": True,
        "deleted": deleted,
        "error_count": len(errors),
        "errors": errors[:20],
    })


@app.route("/admin/catalog/repair-codes", methods=["POST"])
@login_required
@super_admin_required
def admin_catalog_repair_codes():
    """Repair CatalogItem rows that are at legacy (pre-renumber) codes.
    Remaps each row's category_code/category_name to the current
    post-renumber value. If a (new_code, label) row already exists,
    the legacy duplicate is hard-deleted to avoid violating the unique
    constraint."""
    from budget_calc import COA_LEGACY_MAPPING, FP_COA_NAMES

    remapped = 0
    deleted_dupes = 0
    name_fixed = 0
    errors = []

    # Snapshot existing (code, label) pairs so we can detect target collisions.
    existing = {
        (int(ci.category_code), ci.label): ci.id
        for ci in CatalogItem.query.all()
    }

    for ci in list(CatalogItem.query.all()):
        old_code = int(ci.category_code or 0)
        label    = ci.label

        # Case A — legacy code. Remap to new code, update name.
        if old_code in COA_LEGACY_MAPPING:
            new_code = COA_LEGACY_MAPPING[old_code]
            new_name = FP_COA_NAMES.get(new_code, ci.category_name)
            # If a row already exists at (new_code, label), this legacy row
            # would collide on commit — delete it instead.
            target_id = existing.get((new_code, label))
            if target_id and target_id != ci.id:
                try:
                    db.session.delete(ci)
                    db.session.commit()
                    deleted_dupes += 1
                    existing.pop((old_code, label), None)
                except Exception as e:
                    db.session.rollback()
                    errors.append(f"delete {ci.id} ({old_code}, {label!r}): {e}")
                continue
            try:
                ci.category_code = new_code
                ci.category_name = new_name
                db.session.commit()
                existing.pop((old_code, label), None)
                existing[(new_code, label)] = ci.id
                remapped += 1
            except Exception as e:
                db.session.rollback()
                errors.append(f"remap {ci.id} {old_code}→{new_code} {label!r}: {e}")
            continue

        # Case B — current code but stale name. Sync category_name with
        # FP_COA_NAMES so "Hair, Makeup & Wardrobe Costs" becomes
        # "Hair & Makeup Costs" on 3100 rows, for example.
        if old_code in FP_COA_NAMES:
            expected_name = FP_COA_NAMES[old_code]
            if ci.category_name != expected_name:
                try:
                    ci.category_name = expected_name
                    db.session.commit()
                    name_fixed += 1
                except Exception as e:
                    db.session.rollback()
                    errors.append(f"rename {ci.id} ({old_code}): {e}")

    return jsonify({
        "ok": True,
        "remapped": remapped,
        "deleted_duplicates": deleted_dupes,
        "names_fixed": name_fixed,
        "error_count": len(errors),
        "errors": errors[:20],
    })


# ── Role Tag Mapping editor (Super Admin) ────────────────────────────────────
# Translates internal role_tag → MMB/ShowBiz target accounts. Super admin
# refines the seeded defaults here. Export routines (Task 3) read the same
# table.

def _role_mapping_to_dict(m):
    return {
        "id":                     m.id,
        "role_tag":               m.role_tag,
        "internal_account_code":  m.internal_account_code,
        "internal_account_name":  m.internal_account_name,
        "mmb_account_code":       m.mmb_account_code or "",
        "mmb_account_name":       m.mmb_account_name or "",
        "showbiz_account_code":   m.showbiz_account_code or "",
        "showbiz_account_name":   m.showbiz_account_name or "",
        "notes":                  m.notes or "",
        "updated_at":             m.updated_at.isoformat() if m.updated_at else None,
    }


@app.route("/admin/role-mapping")
@login_required
@super_admin_required
def admin_role_mapping_view():
    """Super admin page for editing role_tag → MMB/ShowBiz account mappings."""
    from models import RoleTagMapping as _RTM
    # Include label from CatalogItem via role_tag join for display.
    rows = db.session.query(_RTM, CatalogItem.label).outerjoin(
        CatalogItem, CatalogItem.role_tag == _RTM.role_tag
    ).order_by(_RTM.internal_account_code, _RTM.role_tag).all()
    mappings = []
    for m, label in rows:
        d = _role_mapping_to_dict(m)
        d["label"] = label or ""
        mappings.append(d)
    return render_template("admin_role_mapping.html",
                           mappings=mappings, coa_sections=FP_COA_SECTIONS)


@app.route("/api/role-mapping")
@login_required
def api_role_mapping():
    """JSON endpoint for export routines + admin editor. All logged-in users
    can READ (needed by budget.html export logic); only super admin writes."""
    from models import RoleTagMapping as _RTM
    rows = _RTM.query.order_by(_RTM.internal_account_code, _RTM.role_tag).all()
    return jsonify({"mappings": [_role_mapping_to_dict(m) for m in rows]})


@app.route("/admin/role-mapping/<int:mid>", methods=["POST"])
@login_required
@super_admin_required
def admin_role_mapping_update(mid):
    from models import RoleTagMapping as _RTM
    m = _RTM.query.get_or_404(mid)
    data = request.get_json(force=True) or {}
    for f in ("mmb_account_code", "mmb_account_name",
              "showbiz_account_code", "showbiz_account_name", "notes"):
        if f in data:
            setattr(m, f, (data[f] or None) if data[f] != "" else None)
    m.updated_by_user_id = current_user.id
    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 400
    return jsonify(_role_mapping_to_dict(m))


@app.route("/admin/reports/qe-audit.csv")
@login_required
@super_admin_required
def admin_qe_audit_csv():
    """Task 4: one-time audit comparing JS QE_CATEGORIES_FROZEN against
    CatalogItem rows in the DB. Produces a CSV the super admin can use to
    reconcile the two sources of truth before they're formally unified
    (Phase 2 will replace the JS array with a /api/catalog fetch).

    Statuses emitted:
      - 'Missing from catalog'       — in QE but not in CatalogItem
      - 'Missing from departments'   — in CatalogItem but not in QE
      - 'Duplicate within department'— (code, label) appears twice in QE
      - 'Duplicate across departments' — same label under two or more codes
    """
    from budget_calc import QE_CATEGORIES_FROZEN
    import csv as _csv, io as _io
    from collections import Counter, defaultdict

    # Build QE structures
    qe_pairs = [(int(c), lbl) for (c, _name, lbl) in QE_CATEGORIES_FROZEN]
    qe_dept_name = {int(c): name for (c, name, _lbl) in QE_CATEGORIES_FROZEN}
    qe_pair_counts = Counter(qe_pairs)
    qe_labels_by_code = defaultdict(set)
    for c, lbl in qe_pairs:
        qe_labels_by_code[c].add(lbl)
    qe_codes_by_label = defaultdict(set)
    for c, lbl in qe_pairs:
        qe_codes_by_label[lbl].add(c)

    # CatalogItem index
    ci_pairs = set()
    ci_by_pair = {}
    for ci in CatalogItem.query.all():
        key = (int(ci.category_code or 0), ci.label or "")
        ci_pairs.add(key)
        ci_by_pair[key] = ci

    # Build report rows
    report = []

    # 1. Missing from catalog
    for c, lbl in set(qe_pairs):
        if (c, lbl) not in ci_pairs:
            report.append({
                "Department": f"{c} — {qe_dept_name.get(c, '')}",
                "Quick Entry Item": lbl,
                "Status": "Missing from catalog",
                "Recommended Action": "Add to global catalog (/admin/catalog) or remove from QE_CATEGORIES",
            })

    # 2. Missing from departments
    qe_pair_set = set(qe_pairs)
    for key in ci_pairs:
        if key not in qe_pair_set:
            c, lbl = key
            report.append({
                "Department": f"{c} — {dict(FP_COA_SECTIONS).get(c, '')}",
                "Quick Entry Item": lbl,
                "Status": "Missing from departments",
                "Recommended Action": "Add to Quick Entry panel (templates/budget.html QE_CATEGORIES) or mark catalog row is_active=False",
            })

    # 3. Duplicate within department
    for (c, lbl), cnt in qe_pair_counts.items():
        if cnt > 1:
            report.append({
                "Department": f"{c} — {qe_dept_name.get(c, '')}",
                "Quick Entry Item": lbl,
                "Status": "Duplicate within department",
                "Recommended Action": f"Appears {cnt} times; remove duplicates — keep newest entry",
            })

    # 4. Duplicate across departments
    for lbl, codes in qe_codes_by_label.items():
        if len(codes) > 1:
            for c in sorted(codes):
                report.append({
                    "Department": f"{c} — {qe_dept_name.get(c, '')}",
                    "Quick Entry Item": lbl,
                    "Status": "Duplicate across departments",
                    "Recommended Action": f"Also appears under code(s) {sorted(codes - {c})}; confirm canonical section",
                })

    # Sort for easy review
    report.sort(key=lambda r: (r["Status"], r["Department"], r["Quick Entry Item"]))

    # Emit CSV
    out = _io.StringIO()
    w = _csv.DictWriter(out, fieldnames=["Department", "Quick Entry Item",
                                         "Status", "Recommended Action"])
    w.writeheader()
    for row in report:
        w.writerow(row)

    out.seek(0)
    return Response(out.read().encode('utf-8'), mimetype="text/csv",
                    headers={"Content-Disposition": "attachment; filename=qe_audit.csv"})


@app.route("/admin/role-mapping/bulk-import", methods=["POST"])
@login_required
@super_admin_required
def admin_role_mapping_bulk_import():
    """CSV bulk import. Columns: role_tag, mmb_account_code, mmb_account_name,
    showbiz_account_code, showbiz_account_name, notes. Missing role_tags are
    ignored (no new mappings created — admin creates those via the catalog
    editor which auto-seeds)."""
    from models import RoleTagMapping as _RTM
    import csv as _csv, io as _io
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "No CSV file provided"}), 400
    reader = _csv.DictReader(_io.StringIO(f.read().decode("utf-8-sig")))
    updated = 0
    for row in reader:
        rt = (row.get("role_tag") or "").strip()
        if not rt:
            continue
        m = _RTM.query.filter_by(role_tag=rt).first()
        if not m:
            continue
        for k in ("mmb_account_code", "mmb_account_name",
                  "showbiz_account_code", "showbiz_account_name", "notes"):
            if k in row:
                setattr(m, k, (row[k] or None))
        m.updated_by_user_id = current_user.id
        updated += 1
    db.session.commit()
    return jsonify({"ok": True, "updated": updated})


# ── One-time migrations (Super Admin) ─────────────────────────────────────────

def _seq_suffix(n):
    """0 → A, 1 → B, ..., 25 → Z, 26 → AA, ..."""
    s = ''
    n = max(0, int(n))
    while True:
        s = chr(65 + (n % 26)) + s
        n = (n // 26) - 1
        if n < 0:
            break
    return s


def _is_already_split(desc):
    """Return True if description looks like it was already split
    (ends with space + 1-3 uppercase letters, e.g. 'Camera Op A' or 'PA AB')."""
    if not desc:
        return False
    d = desc.rstrip()
    # Look at the last whitespace-delimited token
    parts = d.rsplit(' ', 1)
    if len(parts) != 2:
        return False
    tail = parts[1]
    if not (1 <= len(tail) <= 3):
        return False
    return tail.isupper() and tail.isalpha()


def _split_labor_line(line, db_session):
    """Split a single labor line with qty>1 into individual A/B/C lines.
    Moves ScheduleDay + CrewAssignment rows by crew_instance/instance so each
    split line owns its person's schedule. Returns number of NEW lines created."""
    from sqlalchemy import func as _func

    if not line.is_labor:
        return 0
    qty = int(float(line.quantity or 1))
    if qty <= 1:
        return 0
    if getattr(line, 'line_tag', None):
        return 0  # sync-driven auto lines
    base_desc = (line.description or '').rstrip()
    if not base_desc:
        return 0
    if _is_already_split(base_desc):
        return 0

    # How many splits — at least qty, or max crew_instance if schedule has drift
    max_inst = db_session.query(_func.max(ScheduleDay.crew_instance)).filter(
        ScheduleDay.budget_line_id == line.id
    ).scalar() or 1
    num_splits = max(qty, int(max_inst))

    # Rename original line to "{base} A" and collapse qty to 1
    line.description = base_desc + ' ' + _seq_suffix(0)
    line.quantity = 1
    # Recompute estimated_total for the now-single-qty line
    try:
        r = float(line.rate or 0)
        d = float(line.days or 1)
        disc = float(line.agent_pct or 0)
        if not line.is_labor:
            line.estimated_total = round(r * 1 * d * (1 - disc), 2)
    except Exception:
        pass

    new_count = 0
    for i in range(1, num_splits):
        new_line = BudgetLine(
            budget_id=line.budget_id,
            account_code=line.account_code,
            account_name=line.account_name,
            description=base_desc + ' ' + _seq_suffix(i),
            is_labor=True,
            quantity=1,
            days=line.days,
            rate=line.rate,
            rate_type=line.rate_type,
            est_ot=line.est_ot,
            fringe_type=line.fringe_type,
            agent_pct=line.agent_pct,
            estimated_total=line.estimated_total,
            note=line.note,
            payroll_co=line.payroll_co,
            use_schedule=line.use_schedule,
            role_group=line.role_group,
            unit_rate=line.unit_rate,
            days_unit=line.days_unit,
            days_per_week=line.days_per_week,
            working_total=line.working_total,
            manual_actual=line.manual_actual,
            sort_order=(line.sort_order or 0) + i,
        )
        db_session.add(new_line)
        db_session.flush()

        # Move ScheduleDay rows for crew_instance=(i+1) to the new line
        instance_to_move = i + 1
        sched_rows = db_session.query(ScheduleDay).filter(
            ScheduleDay.budget_line_id == line.id,
            ScheduleDay.crew_instance == instance_to_move,
        ).all()
        for sr in sched_rows:
            sr.budget_line_id = new_line.id
            sr.crew_instance = 1

        # Move CrewAssignment rows the same way
        ca_rows = db_session.query(CrewAssignment).filter(
            CrewAssignment.budget_line_id == line.id,
            CrewAssignment.instance == instance_to_move,
        ).all()
        for ca in ca_rows:
            ca.budget_line_id = new_line.id
            ca.instance = 1

        new_count += 1

    db_session.flush()
    return new_count


def _find_split_candidates():
    """Return list of (budget, line) tuples where line is eligible for split."""
    candidates = []
    all_lines = BudgetLine.query.filter(
        BudgetLine.is_labor == True,
        BudgetLine.quantity > 1,
    ).all()
    for ln in all_lines:
        if getattr(ln, 'line_tag', None):
            continue
        if _is_already_split(ln.description or ''):
            continue
        candidates.append(ln)
    return candidates


@app.route("/admin/migrate/split-labor/preview")
@login_required
@super_admin_required
def admin_migrate_split_labor_preview():
    """Count how many labor lines would be split (dry run)."""
    candidates = _find_split_candidates()
    # Group by budget for reporting
    by_budget = {}
    for ln in candidates:
        b = Budget.query.get(ln.budget_id)
        key = (ln.budget_id, b.name if b else f"budget#{ln.budget_id}")
        by_budget.setdefault(key, []).append({
            'line_id': ln.id,
            'description': ln.description,
            'quantity': int(float(ln.quantity or 1)),
            'account_code': ln.account_code,
        })
    summary = [
        {
            'budget_id': k[0],
            'budget_name': k[1],
            'line_count': len(v),
            'total_new_lines': sum(x['quantity'] - 1 for x in v),
            'lines': v,
        }
        for k, v in by_budget.items()
    ]
    summary.sort(key=lambda x: x['budget_name'].lower())
    total_lines = len(candidates)
    total_new = sum(int(float(ln.quantity or 1)) - 1 for ln in candidates)
    return jsonify({
        'total_affected_lines': total_lines,
        'total_new_lines': total_new,
        'budgets': summary,
    })


@app.route("/admin/migrate/resync-all", methods=["POST"])
@login_required
@super_admin_required
def admin_migrate_resync_all():
    """One-time: re-run sync_schedule_driven_lines for every budget in the
    system. Reconciles meals, flights, hotel, mileage, per diem, working meals,
    and craft services against the current schedule + production days."""
    total_budgets = Budget.query.count()
    resynced = 0
    errors = []
    for b in Budget.query.all():
        try:
            sync_schedule_driven_lines(b.id, db.session)
            resynced += 1
        except Exception as e:
            errors.append(f"bid={b.id} ({b.name}): {e}")
            try:
                db.session.rollback()
            except Exception:
                pass
    app.logger.info("[resync-all] resynced=%d total=%d errors=%d",
                    resynced, total_budgets, len(errors))
    return jsonify({
        'ok': True,
        'total_budgets': total_budgets,
        'resynced': resynced,
        'errors': errors[:20],
    })


@app.route("/admin/migrate/split-labor", methods=["POST"])
@login_required
@super_admin_required
def admin_migrate_split_labor_run():
    """Execute the one-time labor-line split migration."""
    candidates = _find_split_candidates()
    if not candidates:
        return jsonify({'ok': True, 'split_count': 0, 'new_lines': 0,
                        'message': 'Nothing to do.'})

    split_count = 0
    new_lines = 0
    errors = []
    affected_budgets = set()
    for ln in candidates:
        try:
            added = _split_labor_line(ln, db.session)
            if added > 0:
                split_count += 1
                new_lines += added
                affected_budgets.add(ln.budget_id)
        except Exception as e:
            errors.append(f"Line #{ln.id}: {e}")
            db.session.rollback()

    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': f'Commit failed: {e}'}), 500

    # Re-run schedule sync for affected budgets so meals/flights recalc
    # with the correct per-person counts
    sync_errors = []
    for bid in affected_budgets:
        try:
            sync_schedule_driven_lines(bid, db.session)
        except Exception as e:
            sync_errors.append(f"bid={bid}: {e}")
            db.session.rollback()

    app.logger.info("[labor-split] split=%d new_lines=%d budgets=%d errors=%d",
                    split_count, new_lines, len(affected_budgets), len(errors))

    return jsonify({
        'ok': True,
        'split_count': split_count,
        'new_lines': new_lines,
        'budgets_affected': len(affected_budgets),
        'errors': errors[:20],
        'sync_errors': sync_errors[:20],
    })


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
            return jsonify({"error": "No account found with that email. Make sure they've been invited first."})
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
#
# CRITICAL: Render uses zero-downtime deploys. The new gunicorn worker must
# bind its port within ~90 seconds or Render aborts the deploy. If ANYTHING
# in this block blocks (DB lock held by the old container, Dropbox token
# refresh, slow DNS, gevent+psycopg2 interactions), the port never binds and
# the deploy times out. We therefore run the entire boot block in a
# background thread so the worker binds its port immediately and the
# migrations/seeds happen async. Under gevent, threading.Thread is patched
# to a greenlet that yields on I/O so this is safe and lightweight.

def _run_boot_tasks():
    import sys as _sys, traceback as _tb
    _log = lambda msg: print(f"[BOOT] {msg}", file=_sys.stderr, flush=True)
    try:
        _log("starting migrations + seeds")
        _do_boot_work()
        _log("startup complete")
    except Exception:
        _log("FAILED:\n" + _tb.format_exc())


def _do_boot_work():
  # NOTE: body indented at 2 spaces so the original `with app.app_context():`
  # block below keeps its 4-space inner indent without a giant reformat.
  with app.app_context():
    # Install a per-connection statement_timeout via engine event so that
    # EVERY connection pulled from the pool during boot has a 5-second
    # ceiling. Previous approach (`SET statement_timeout` on session) was
    # ineffective: `SET` is connection-scoped, but each `db.session.commit()`
    # returned the connection to the pool, and the next `execute` could
    # land on a different pooled connection that still had timeout=0.
    # When the old container held a row lock during a zero-downtime deploy,
    # the new worker's ALTER TABLE hung forever → Render port-scan timeout.
    from sqlalchemy import event as _sa_event

    _is_pg = 'postgresql' in str(db.engine.url).lower()

    def _boot_set_timeout(dbapi_conn, _conn_record):
        if not _is_pg:
            return
        try:
            cur = dbapi_conn.cursor()
            cur.execute("SET statement_timeout = 5000")  # 5 seconds, in ms
            cur.close()
        except Exception:
            pass

    _sa_event.listen(db.engine, "connect", _boot_set_timeout)
    # Any already-pooled connections need the setting too. dispose() closes
    # them so they'll be re-opened through the listener.
    try:
        db.engine.dispose()
    except Exception:
        pass

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
        # Per-project section exclusions from the production-company fee
        # base. NULL / empty = every section contributes (default). User
        # edits from budget Settings → "Sections exempt from Prod Co Fee".
        "ALTER TABLE budget ADD COLUMN fee_excluded_sections TEXT",
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
        "ALTER TABLE project_sheet ADD COLUMN status VARCHAR(20) DEFAULT 'active' NOT NULL",
        # Budget versioning: shared version number for Estimated+Working pairs
        "ALTER TABLE budget ADD COLUMN version_number INTEGER",
        # Meals/craft services: allow user to opt a schedule-driven line out of auto-sync
        "ALTER TABLE budget_line ADD COLUMN sync_omit BOOLEAN DEFAULT false NOT NULL",
        # Task 2: role-tag + phase on catalog_item, catalog_item FK on budget_line.
        "ALTER TABLE catalog_item ADD COLUMN role_tag VARCHAR(80)",
        "ALTER TABLE catalog_item ADD COLUMN phase VARCHAR(20)",
        "ALTER TABLE budget_line ADD COLUMN catalog_item_id INTEGER REFERENCES catalog_item(id)",
    ]
    for _sql in _migrations:
        try:
            db.session.execute(text(_sql))
            db.session.commit()
        except Exception:
            db.session.rollback()

    # ── Essential-column healing pass ─────────────────────────────────────
    # The migrations above each run under the 5-second per-connection
    # statement_timeout installed for boot. In production we've seen the
    # timeout trip a single ALTER (likely an exclusive-lock stall from a
    # conflicting connection), silently skip it, and then every request
    # 500s because the ORM SELECTs the missing column.
    #
    # This block re-runs a small set of RECENT / CRITICAL column adds
    # using IF NOT EXISTS (Postgres 9.6+) on a DEDICATED connection with
    # a much longer timeout. Idempotent — safe to keep in place forever.
    _essential_cols = [
        # Prod Company Fee per-section exemptions (2026-04-24)
        "ALTER TABLE budget ADD COLUMN IF NOT EXISTS fee_excluded_sections TEXT",
        # Per-budget fee disperse
        "ALTER TABLE budget ADD COLUMN IF NOT EXISTS company_fee_dispersed BOOLEAN DEFAULT FALSE NOT NULL",
        # Workers' Comp / Payroll Fee percentages
        "ALTER TABLE budget ADD COLUMN IF NOT EXISTS workers_comp_pct NUMERIC(8,6) DEFAULT 0.03",
        "ALTER TABLE budget ADD COLUMN IF NOT EXISTS payroll_fee_pct NUMERIC(8,6) DEFAULT 0.0175",
    ]
    if _is_pg:
        try:
            # Raw connection so we can bump statement_timeout past the 5s
            # boot default for just these statements. Essential-column
            # healing must NOT be subject to the watchdog — a locked
            # deploy is better than a broken one.
            _raw_conn = db.engine.raw_connection()
            try:
                _raw_cur = _raw_conn.cursor()
                _raw_cur.execute("SET statement_timeout = 60000")  # 60s
                for _sql in _essential_cols:
                    try:
                        _raw_cur.execute(_sql)
                        _raw_conn.commit()
                        logging.warning(f"[BOOT] essential-col OK: {_sql}")
                    except Exception as _ee:
                        _raw_conn.rollback()
                        logging.error(f"[BOOT] essential-col FAILED: {_sql} → {_ee}")
                _raw_cur.close()
            finally:
                _raw_conn.close()
        except Exception as _ec:
            logging.error(f"[BOOT] essential-col pass could not open raw conn: {_ec}")

    # ── 2026-04 COA renumber ─────────────────────────────────────────────────
    # One-time remap of legacy COA codes to the new Movie Magic / ShowBiz-
    # aligned numbering. Guarded by CoaMigrationLog so rerunning boot is a
    # no-op. Runs in a single transaction — if ANY step fails, the entire
    # renumber rolls back and the deploy aborts (better than half-migrated).
    try:
        from models import CoaMigrationLog as _CML, CoaChangeLog as _CCL
        from budget_calc import COA_LEGACY_MAPPING, FP_COA_NAMES
        _COA_RENUMBER_KEY = '2026-04-renumber'
        _already_applied = _CML.query.filter_by(migration_key=_COA_RENUMBER_KEY).first()
        if not _already_applied:
            logging.warning("[COA] Running 2026-04 renumber migration…")
            # Build deterministic old→new mapping list for logging + UPDATE.
            _mapping_rows = sorted(COA_LEGACY_MAPPING.items())

            # STEP 1: Dedupe merge-collisions on catalog_item. For each
            # (new_code, label) that would collide, suffix the second row
            # with " (legacy)" so the unique constraint holds.
            _new_code_labels = {}  # (new_code, label) -> first catalog_item.id
            try:
                _rows = db.session.execute(text(
                    "SELECT id, category_code, label FROM catalog_item"
                )).fetchall()
                for _cid, _old_code, _label in _rows:
                    _new_code = COA_LEGACY_MAPPING.get(int(_old_code or 0), int(_old_code or 0))
                    _key = (_new_code, _label)
                    if _key in _new_code_labels:
                        _new_label = f"{_label} (legacy)"
                        db.session.execute(text(
                            "UPDATE catalog_item SET label = :nl WHERE id = :cid"
                        ), {"nl": _new_label, "cid": _cid})
                        logging.warning(f"[COA] Catalog merge collision: renamed id={_cid} '{_label}' → '{_new_label}'")
                    else:
                        _new_code_labels[_key] = _cid
            except Exception as _e:
                logging.warning(f"[COA] collision-dedupe skipped (catalog_item missing?): {_e}")

            # STEP 2: UPDATE all tables holding COA codes using a single
            # CASE expression so we don't double-migrate (e.g. old 1000 →
            # new 2000, then old 2000 → new 2600 would move things twice
            # if run sequentially).
            _tables_to_remap = [
                ("budget_line",           "account_code", "account_name"),
                ("budget_template_line",  "account_code", "account_name"),
                ("catalog_item",          "category_code", "category_name"),
                ("users",                 "dept_code",    None),
            ]
            for _tbl, _code_col, _name_col in _tables_to_remap:
                _case_code_parts = []
                _case_name_parts = []
                _in_params = {}
                for i, (_old, _new) in enumerate(_mapping_rows):
                    _case_code_parts.append(f"WHEN :old{i} THEN :new{i}")
                    _in_params[f"old{i}"] = _old
                    _in_params[f"new{i}"] = _new
                    if _name_col:
                        _case_name_parts.append(f"WHEN :old{i} THEN :nnm{i}")
                        _in_params[f"nnm{i}"] = FP_COA_NAMES.get(_new, None)
                _code_case = "CASE " + _code_col + " " + " ".join(_case_code_parts) + f" ELSE {_code_col} END"
                _in_list = ", ".join(f":old{i}" for i in range(len(_mapping_rows)))
                if _name_col:
                    _name_case = "CASE " + _code_col + " " + " ".join(_case_name_parts) + f" ELSE {_name_col} END"
                    _stmt = (f"UPDATE {_tbl} SET {_code_col} = {_code_case}, "
                             f"{_name_col} = {_name_case} "
                             f"WHERE {_code_col} IN ({_in_list})")
                else:
                    _stmt = (f"UPDATE {_tbl} SET {_code_col} = {_code_case} "
                             f"WHERE {_code_col} IN ({_in_list})")
                try:
                    _res = db.session.execute(text(_stmt), _in_params)
                    logging.warning(f"[COA] remapped {_tbl}.{_code_col}: rowcount={_res.rowcount}")
                except Exception as _e:
                    logging.error(f"[COA] FAILED to remap {_tbl}: {_e}")
                    raise

            # STEP 3: Log every old→new pair in coa_change_log for audit.
            for _old, _new in _mapping_rows:
                db.session.add(_CCL(
                    account_code_old=_old,
                    account_code_new=_new,
                    account_name_old=None,  # old names not tracked
                    account_name_new=FP_COA_NAMES.get(_new, None),
                    changed_by_user_id=None,  # automated migration
                    change_reason='2026-04 renumber (MMB/ShowBiz alignment)',
                ))

            # STEP 4: Insert migration-log row so this never re-runs.
            db.session.add(_CML(
                migration_key=_COA_RENUMBER_KEY,
                applied_by_user_id=None,
                notes=('Renumbered legacy 100-20500 COA to MMB/ShowBiz-aligned '
                       '1000-6800 structure. NOTE: transaction.account_code was '
                       'NOT touched — the external QBO sync app must apply the '
                       'same mapping before its next sync.'),
            ))
            db.session.commit()
            logging.warning("[COA] 2026-04 renumber migration COMPLETE")
        else:
            logging.info(f"[COA] renumber already applied at {_already_applied.applied_at}; skipping")
    except Exception as _e:
        logging.exception(f"[COA] renumber migration FAILED: {_e}")
        db.session.rollback()
        # Re-raise so the preDeployCommand exits non-zero and Render aborts
        # the deploy — we don't want a half-migrated production state.
        raise

    # 2026-04-17 Catalog resync migration REMOVED per user direction.
    # Previously this wiped and reseeded catalog_item on first boot after
    # that commit. No longer active — the catalog_item table is managed
    # manually by the user from this point forward.

    # Backfill version_number on existing budgets (one-time, skips already-set rows).
    try:
        import re as _re_vn
        _unfilled = Budget.query.filter(Budget.version_number == None).all()
        if _unfilled:
            for _bv in _unfilled:
                # Parse vN from name (e.g. "Project v2", "Project Working v3")
                _m = _re_vn.search(r'\bv(\d+)\b', _bv.name or '', _re_vn.IGNORECASE)
                if _m:
                    _bv.version_number = int(_m.group(1))
                elif _bv.parent_budget_id:
                    # Working budget with no parseable number — inherit from parent
                    _par = Budget.query.get(_bv.parent_budget_id)
                    if _par and _par.version_number:
                        _bv.version_number = _par.version_number
                    else:
                        _bv.version_number = 1
                else:
                    _bv.version_number = 1
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
    # Remove the boot-time statement_timeout listener and recycle the pool
    # so subsequent connections (seeds + request handlers) have the default
    # timeout (0 = unlimited). seed_catalog inserts 200+ rows and must not
    # be killed mid-flight.
    try:
        _sa_event.remove(db.engine, "connect", _boot_set_timeout)
    except Exception:
        pass
    try:
        db.engine.dispose()
    except Exception:
        pass
    try:
        seed_fringes(db.session)
    except Exception as _e:
        app.logger.warning("seed_fringes failed: %s", _e)
        db.session.rollback()
    try:
        seed_standard_template(db.session)
    except Exception as _e:
        app.logger.warning("seed_standard_template failed: %s", _e)
        db.session.rollback()
    try:
        seed_payroll_profiles(db.session)
    except Exception as _e:
        app.logger.warning("seed_payroll_profiles failed: %s", _e)
        db.session.rollback()
    # seed_catalog() DISABLED 2026-04-17 per user direction. They want the
    # Global Quick Entry Catalog table to stay EMPTY (or whatever they
    # explicitly leave in it). Quick Entry reads from the hardcoded
    # QE_CATEGORIES in templates/budget.html (QE_USE_DB_CATALOG = false),
    # so nothing needs to be in catalog_item for QE to work. Do NOT
    # re-enable this call without explicit user instruction.
    # try:
    #     seed_catalog(db.session)
    # except Exception as _e:
    #     app.logger.warning("seed_catalog failed: %s", _e)
    #     db.session.rollback()

    # ── Task 2: backfill role_tag on existing CatalogItem rows + seed
    # RoleTagMapping with best-guess MMB/ShowBiz targets for super admin. ────
    try:
        import re as _re_rt
        from models import RoleTagMapping as _RTM
        _ci_rows = CatalogItem.query.filter(CatalogItem.role_tag.is_(None)).all()
        if _ci_rows:
            logging.info(f"[role_tag backfill] generating tags for {len(_ci_rows)} CatalogItem rows")
            _seen = {c.role_tag for c in CatalogItem.query.filter(CatalogItem.role_tag.isnot(None)).all()}
            for _ci in _ci_rows:
                _slug_base = _re_rt.sub(r'[^a-z0-9]+', '_', (_ci.label or '').lower()).strip('_') or 'role'
                _slug = _slug_base
                _n = 2
                while _slug in _seen:
                    _slug = f"{_slug_base}_{_n}"
                    _n += 1
                _seen.add(_slug)
                _ci.role_tag = _slug[:80]
            db.session.commit()

        # Seed RoleTagMapping rows where missing. Best-guess MMB targets
        # follow the MMB account structure: 2000-series prod, 2100 talent,
        # 2500 equipment, 3000 post. Super admin refines via editor.
        _existing_mappings = {m.role_tag for m in _RTM.query.all()}
        _all_labor_items = CatalogItem.query.filter_by(is_labor=True).filter(
            CatalogItem.role_tag.isnot(None)
        ).all()
        _added = 0
        for _ci in _all_labor_items:
            if _ci.role_tag in _existing_mappings:
                continue
            # Best-guess MMB target based on internal section. Numbers
            # follow standard MMB examples; super admin refines.
            _mmb_code, _mmb_name = _guess_mmb_target(_ci)
            _sb_code, _sb_name = _guess_showbiz_target(_ci)
            db.session.add(_RTM(
                role_tag=_ci.role_tag,
                internal_account_code=_ci.category_code,
                internal_account_name=_ci.category_name,
                mmb_account_code=_mmb_code,
                mmb_account_name=_mmb_name,
                showbiz_account_code=_sb_code,
                showbiz_account_name=_sb_name,
                updated_by_user_id=None,
            ))
            _added += 1
        if _added:
            db.session.commit()
            logging.info(f"[role_tag mapping] seeded {_added} default RoleTagMapping rows")
    except Exception as _e:
        logging.exception(f"role_tag seed failed: {_e}")
        db.session.rollback()

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
                # code,  name,                             desc,                         labor, qty, days, rate,   rt,           fringe, agent, sort
                # ── ATL roles live in Production Staff (2000) per 2026-04 renumber ──
                (2000, "Production Staff",                 "Director / DP",              True,  1,   5,    2000,   "day_10",     "E",    0,     0),
                (2000, "Production Staff",                 "Executive Producer",         True,  1,   5,    1500,   "day_10",     "E",    0,     10),
                # ── Talent (2100) ────────────────────────────────────────────
                (2100, "Talent",                           "Principal Talent",           True,  2,   3,    1200,   "flat_day",   "S",    0.10,  20),
                (2100, "Talent",                           "Supporting Talent",          True,  4,   2,    600,    "flat_day",   "S",    0.10,  30),
                (2100, "Talent",                           "Voice Over Talent",          True,  1,   1,    2500,   "flat_project","N",   0.10,  40),
                # ── Production Staff (2000) ──────────────────────────────────
                (2000, "Production Staff",                 "Line Producer",              True,  1,   5,    1200,   "day_10",     "N",    0,     50),
                (2000, "Production Staff",                 "1st AD",                     True,  1,   5,    900,    "day_10",     "N",    0,     60),
                (2000, "Production Staff",                 "2nd AD",                     True,  1,   5,    650,    "day_10",     "N",    0,     70),
                (2000, "Production Staff",                 "Production Coordinator",     True,  1,   5,    600,    "day_10",     "N",    0,     80),
                (2000, "Production Staff",                 "Production Assistant",       True,  3,   5,    300,    "day_10",     "N",    0,     90),
                (2000, "Production Staff",                 "Camera Operator",            True,  2,   5,    900,    "day_10",     "N",    0,     100),
                (2000, "Production Staff",                 "Gaffer",                     True,  1,   5,    850,    "day_10",     "I",    0,     110),
                (2000, "Production Staff",                 "Sound Mixer",                True,  1,   5,    950,    "day_10",     "I",    0,     120),
                (4000, "Post-Production Staff",            "Editor",                     True,  1,   10,   750,    "day_10",     "N",    0,     130),
                # ── Camera Equipment (2600) ──────────────────────────────────
                (2600, "Camera Equipment",                 "Camera Package Rental",      False, 2,   5,    1500,   "day_10",     "N",    0,     140),
                (2600, "Camera Equipment",                 "Media / Hard Drives",        False, 1,   1,    350,    "day_10",     "N",    0,     150),
                # ── Grip & Electric Equipment (2700) ─────────────────────────
                (2700, "Grip & Electric Equipment",        "Lighting Package",           False, 1,   5,    1200,   "day_10",     "N",    0,     160),
                (2700, "Grip & Electric Equipment",        "Grip Package",               False, 1,   5,    600,    "day_10",     "N",    0,     170),
                # ── Sound Equipment (2800) ───────────────────────────────────
                (2800, "Sound Equipment",                  "Sound Package Rental",       False, 1,   5,    500,    "day_10",     "N",    0,     180),
                # ── Art & Sets Costs (3000) ──────────────────────────────────
                (3000, "Art & Sets Costs",                 "Prop Rentals",               False, 1,   1,    800,    "day_10",     "N",    0,     190),
                (3000, "Art & Sets Costs",                 "Set Dressing Materials",     False, 1,   1,    500,    "day_10",     "N",    0,     200),
                # ── Hair & Makeup (3100) ─────────────────────────────────────
                (3100, "Hair & Makeup Costs",              "Hair Stylist",               True,  1,   3,    700,    "day_10",     "N",    0,     210),
                (3100, "Hair & Makeup Costs",              "Makeup Artist",              True,  1,   3,    700,    "day_10",     "N",    0,     220),
                # ── Wardrobe (3200) ──────────────────────────────────────────
                (3200, "Wardrobe Costs",                   "Wardrobe Stylist",           True,  1,   3,    700,    "day_10",     "N",    0,     225),
                # ── Transportation (3400) ────────────────────────────────────
                (3400, "Transportation",                   "15-Passenger Van Rental",    False, 1,   5,    200,    "day_10",     "N",    0,     230),
                (3400, "Transportation",                   "Fuel & Parking",             False, 1,   5,    80,     "day_10",     "N",    0,     240),
                # ── Travel (3500) ────────────────────────────────────────────
                (3500, "Travel",                           "Hotel — Crew (est.)",        False, 6,   4,    150,    "day_10",     "N",    0,     250),
                # ── Production Meals & Craft Services (3700) ─────────────────
                (3700, "Production Meals & Craft Services","Craft Services",             False, 1,   5,    200,    "day_10",     "N",    0,     270),
                # ── Locations (3300) ─────────────────────────────────────────
                (3300, "Locations",                        "Studio / Stage Rental",      False, 1,   3,    2000,   "day_10",     "N",    0,     280),
                # ── Administrative (6500) ────────────────────────────────────
                (6500, "Administrative",                   "Petty Cash / Miscellaneous", False, 1,   1,    1000,   "day_10",     "N",    0,     290),
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
            # code, name,                              desc,                       labor, qty, days, rate,   rt,        fringe, agent, sort
            (3300, "Locations",                        "Tech Scout",               False, 1,   1,    500,    "day_10",  "N",    0,     0),
            (2000, "Production Staff",                 "Director",                 True,  1,   1,    1500,   "day_10",  "E",    0,     10),
            (2100, "Talent",                           "Host",                     True,  1,   1,    1000,   "day_10",  "N",    0.10,  20),
            (2000, "Production Staff",                 "UPM",                      True,  1,   1,    1000,   "day_10",  "N",    0,     30),
            (2000, "Production Staff",                 "Key PA",                   True,  1,   1,    350,    "day_10",  "N",    0,     40),
            (2000, "Production Staff",                 "Camera Operator",          True,  2,   1,    900,    "day_10",  "N",    0,     50),
            (2000, "Production Staff",                 "Video Engineer",           True,  1,   1,    750,    "day_10",  "N",    0,     60),
            (2000, "Production Staff",                 "Sound Mixer",              True,  1,   1,    900,    "day_10",  "N",    0,     70),
            (2600, "Camera Equipment",                 "Camera Package Rental",    False, 3,   1,    1500,   "day_10",  "N",    0,     80),
            (2600, "Camera Equipment",                 "Lens Kit Rental",          False, 3,   1,    500,    "day_10",  "N",    0,     90),
            (2600, "Camera Equipment",                 "Monitor Rental",           False, 4,   1,    150,    "day_10",  "N",    0,     100),
            (2600, "Camera Equipment",                 "Media Cards / Hard Drives",False, 1,   1,    300,    "day_10",  "N",    0,     110),
            (2600, "Camera Equipment",                 "Camera Expendables",       False, 1,   1,    100,    "day_10",  "N",    0,     120),
            (2700, "Grip & Electric Equipment",        "Lighting Package",         False, 1,   1,    1500,   "day_10",  "N",    0,     130),
            (2700, "Grip & Electric Equipment",        "Grip Package",             False, 1,   1,    800,    "day_10",  "N",    0,     140),
            (5000, "Processing & Lab",                 "SDI Distribution Amp",     False, 1,   1,    200,    "day_10",  "N",    0,     150),
            (5000, "Processing & Lab",                 "Encoder / Decoder Unit",   False, 1,   1,    600,    "day_10",  "N",    0,     160),
            (2900, "Control Room Equipment",           "Control Room Rental",      False, 1,   1,    2000,   "day_10",  "N",    0,     170),
            (2900, "Control Room Equipment",           "Video Playback System",    False, 1,   1,    500,    "day_10",  "N",    0,     180),
            (2900, "Control Room Equipment",           "Switcher / Mixer Rental",  False, 1,   1,    400,    "day_10",  "N",    0,     190),
            (2800, "Sound Equipment",                  "Sound Package Rental",     False, 1,   1,    600,    "day_10",  "N",    0,     200),
            (2800, "Sound Equipment",                  "Wireless Mic Kit",         False, 1,   1,    200,    "day_10",  "N",    0,     210),
            (3400, "Transportation",                   "Production Car",           False, 1,   1,    100,    "day_10",  "N",    0,     220),
            (3400, "Transportation",                   "Fuel",                     False, 1,   1,    100,    "day_10",  "N",    0,     230),
            (3400, "Transportation",                   "Parking",                  False, 1,   1,    50,     "day_10",  "N",    0,     240),
            (3400, "Transportation",                   "Mileage Reimbursement",    False, 1,   1,    200,    "day_10",  "N",    0,     250),
            (3700, "Production Meals & Craft Services","Catering (Lunch)",         False, 30,  1,    25,     "day_10",  "N",    0,     260),
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


# Boot tasks (migrations + seeds) used to run in the web worker on import.
# That fails under gevent because psycopg2 is NOT greenlet-aware: every DB
# call in _do_boot_work blocks the entire event loop, so gunicorn can't
# answer health checks and Render aborts the deploy. Even with the work
# inside threading.Thread (which is a greenlet under gevent), psycopg2 still
# blocks.
#
# Solution: run boot tasks in Render's preDeployCommand (separate container,
# runs to completion before the web container starts) and skip them entirely
# in the web worker. Set RUN_BOOT_TASKS=1 to force them in-process (e.g. for
# local dev or one-off shell sessions).
if os.environ.get('RUN_BOOT_TASKS') == '1':
    _run_boot_tasks()
else:
    logging.info("[BOOT] Skipping in-process boot tasks (RUN_BOOT_TASKS != 1). "
                 "Migrations should run via preDeployCommand.")


# ─────────────────────────────────────────────────────────────────────────────
# DOCS MODULE — Receipt / Document Upload
# ─────────────────────────────────────────────────────────────────────────────

def _docs_accessible_projects(user):
    """Return active ProjectSheet rows visible to this user for docs."""
    active_filter = (ProjectSheet.status == 'active')
    if user.role in ('super_admin', 'admin'):
        return ProjectSheet.query.filter(active_filter).order_by(ProjectSheet.name).all()
    owned = (db.session.query(ProjectSheet)
             .join(ProjectAccess, ProjectAccess.project_id == ProjectSheet.id)
             .filter(ProjectAccess.user_id == user.id, active_filter)
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
    # Pass latest budget for tab bar (docs_only users won't see budget tabs)
    budget = Budget.query.filter_by(project_id=pid).order_by(Budget.created_at.desc()).first()
    return render_template("docs_upload.html", project=project, uploads=uploads, budget=budget)


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
    # r2_key kept as a unique filing id (for DocUpload.r2_key column).
    # The actual R2 upload was removed — source of truth is Dropbox.
    r2_key = f"docs/{pid}/{_uuid.uuid4().hex}{ext}"

    # Project must have a Dropbox folder set up for the Analyzer to file
    # to. If it doesn't, bail early so we don't save a ghost upload row.
    if not project.dropbox_folder:
        return jsonify({
            "error": "Project has no Dropbox folder configured — cannot file."
        }), 500

    # ── FP Document Analyzer integration (2026-04-20) ────────────────────
    # Route every upload through the embedded fp_analyzer: Veryfi OCR →
    # doc-type detection → auto-file to the correct 01_ADMIN subfolder
    # when confidence ≥ threshold. Low-confidence docs come back as
    # status='review' and the user finishes filing via the review UI.
    import re as _re
    from datetime import datetime as _dt
    from fp_analyzer import analyze_and_file_single
    import json as _json

    _raw_user = (getattr(current_user, 'name', None)
                 or (current_user.email or '').split('@')[0]
                 or 'unknown')
    _safe_user = _re.sub(r"[^\w\- ]", "", _raw_user) or "unknown"

    try:
        result = analyze_and_file_single(
            file_bytes=data,
            filename=f.filename,
            project_name=project.dropbox_folder,   # Analyzer files under /{ops_root}/{project_name}/...
            user_name=_safe_user,
        )
    except Exception as _ae:
        logging.exception("Analyzer pipeline crashed")
        return jsonify({"error": f"Analyzer failed: {_ae}"}), 500

    status_map = {
        "filed":         "done",          # auto-filed to correct folder
        "needs_review":  "review",        # low-confidence → user picks in review UI
        "error":         "error",         # OCR / conversion failure
    }
    upload_status = status_map.get(result.get("status"), "error")

    # Extract structured OCR data from the Veryfi response if present so
    # we can display vendor/amount/date on the docs dashboard and wire
    # into the receipt-matching flow later.
    vr = {}
    try:
        # analyze_and_file_single doesn't return vr directly; needs_review
        # items keep it in _pending[batch_token]. Pull it out for storage.
        if result.get("needs_review") and result.get("batch_token") and result.get("item_id"):
            from fp_analyzer import _pending as _an_pending
            _bt   = result["batch_token"]
            _iid  = result["item_id"]
            items = _an_pending.get(_bt, [])
            item  = next((it for it in items if it.get("id") == _iid), None)
            if item:
                vr = item.get("vr") or {}
    except Exception:
        vr = {}

    vendor_name = None
    amount      = None
    doc_date    = None
    if vr:
        v = vr.get("vendor") or {}
        vendor_name = v.get("name") or v.get("raw_name")
        try:
            amount = float(vr.get("total")) if vr.get("total") is not None else None
        except Exception:
            amount = None
        try:
            from datetime import datetime as _dt_mod
            _d = vr.get("date") or ""
            doc_date = _dt_mod.strptime(_d[:10], "%Y-%m-%d").date() if _d else None
        except Exception:
            doc_date = None

    # Persist the upload row with whatever we got back from the Analyzer.
    upload = DocUpload(
        project_id=pid,
        uploader_id=current_user.id,
        r2_key=r2_key,
        original_filename=f.filename,
        file_size=len(data),
        content_type=content_type,
        file_hash=file_hash,
        status=upload_status,
        veryfi_data=_json.dumps(vr) if vr else None,
        vendor=vendor_name,
        amount=amount,
        doc_date=doc_date,
        # confidence column is 0-100; Analyzer returns 0-1
        confidence=round(float(result.get("confidence") or 0) * 100, 2),
        category=result.get("doc_type"),
        filed_filename=result.get("new_filename") or None,
        filed_dropbox_path=result.get("filed_path"),
        filed_at=_dt.utcnow() if result.get("filed_path") else None,
        is_duplicate=bool(result.get("duplicate")),
    )
    db.session.add(upload)
    db.session.commit()

    # Build a structured client response so the upload UI can show the
    # correct state (filed with path, or needs review, or error).
    if result.get("status") == "filed":
        return jsonify({
            "status":      "ok",
            "upload_id":   upload.id,
            "filed_path":  result.get("filed_path"),
            "doc_type":    result.get("doc_type"),
            "confidence":  result.get("confidence"),
            "duplicate":   bool(result.get("duplicate")),
            "message":     f"Filed as {result.get('doc_type')} ({int((result.get('confidence') or 0) * 100)}% confidence).",
        }), 201
    if result.get("status") == "needs_review":
        return jsonify({
            "status":      "review",
            "upload_id":   upload.id,
            "doc_type":    result.get("doc_type"),
            "confidence":  result.get("confidence"),
            "new_filename": result.get("new_filename"),
            "message":     f"OCR complete but confidence too low to auto-file "
                           f"({int((result.get('confidence') or 0) * 100)}%). Review required.",
        }), 202
    # status == 'error'
    return jsonify({
        "status":   "error",
        "upload_id": upload.id,
        "error":    result.get("error") or "Unknown analyzer error",
    }), 500


@app.route("/docs/upload/<int:uid>/retry-filing", methods=["POST"])
@login_required
def docs_upload_retry_filing(uid):
    """Retry filing an already-uploaded doc to Dropbox. Used when the initial
    upload succeeded to R2 but the Dropbox filing failed (common for legacy
    'pending' rows from before the current_user.username bug fix)."""
    upload = DocUpload.query.get_or_404(uid)
    # Access check
    if current_user.role not in ('super_admin', 'admin'):
        access = ProjectAccess.query.filter_by(
            project_id=upload.project_id, user_id=current_user.id).first()
        if not access:
            return jsonify({"error": "Forbidden"}), 403
    if upload.status == 'filed':
        return jsonify({"ok": True, "already_filed": True,
                        "path": upload.filed_dropbox_path})

    project = ProjectSheet.query.get(upload.project_id)
    if not project or not project.dropbox_folder:
        return jsonify({"error": "Project has no Dropbox folder configured"}), 400

    # Re-fetch bytes from R2
    data, err = _r2_download(upload.r2_key)
    if err or data is None:
        return jsonify({"error": err or "R2 fetch failed"}), 500

    try:
        import re as _re
        from datetime import datetime as _dt
        uploader = User.query.get(upload.uploader_id)
        _raw_user = (getattr(uploader, 'name', None)
                     or (uploader.email or '').split('@')[0]
                     or 'unknown') if uploader else 'unknown'
        _safe_user = _re.sub(r"[^\w\- ]", "", _raw_user) or "unknown"
        _proj_root = f"/{project.dropbox_folder}" if _DBX_NAMESPACE_ID else f"{_DBX_OPS_ROOT}/{project.dropbox_folder}"
        dbx_filing_path = f"{_proj_root}/01_ADMIN/PROCESSED DOCUMENTS/{_safe_user}/{upload.original_filename}"
        _dbx = _dbx_client()
        from dropbox.files import WriteMode as _WM
        _dbx.files_upload(data, dbx_filing_path, autorename=True, mode=_WM('add'))
        upload.status = 'filed'
        upload.filed_dropbox_path = dbx_filing_path
        upload.filed_at = _dt.utcnow()
        db.session.commit()
        return jsonify({"ok": True, "path": dbx_filing_path})
    except Exception as e:
        logging.exception("Retry filing failed")
        return jsonify({"error": str(e)}), 500


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


@app.route("/docs/upload/<int:uid>/rename", methods=["POST"])
@login_required
def docs_upload_rename(uid):
    """Rename a filed document. Updates DocUpload.filed_filename +
    filed_dropbox_path AND renames the file in Dropbox so the two stay
    in sync. Only works when the upload has been filed to Dropbox (has
    a filed_dropbox_path). Dropbox's files_move_v2 with autorename=True
    handles name collisions — we store whatever final name Dropbox used.

    Permission: project members + admins. Non-uploader non-admins
    cannot rename someone else's upload.
    """
    upload = DocUpload.query.get_or_404(uid)

    # Auth: admins can rename anything; regular users only their own.
    if current_user.role not in ('super_admin', 'admin'):
        if upload.uploader_id != current_user.id:
            return jsonify({"error": "Forbidden"}), 403

    if not upload.filed_dropbox_path:
        return jsonify({
            "error": "This document isn't filed in Dropbox yet — can't rename."
        }), 400

    data = request.get_json(force=True) or {}
    new_filename = (data.get("new_filename") or "").strip()
    if not new_filename:
        return jsonify({"error": "Filename required"}), 400
    # Sanity guards
    if "/" in new_filename or "\\" in new_filename:
        return jsonify({"error": "Filename cannot contain / or \\"}), 400
    if len(new_filename) > 200:
        return jsonify({"error": "Filename too long (>200 chars)"}), 400
    # Strip any leading/trailing whitespace and disallow control chars.
    if any(ord(c) < 32 for c in new_filename):
        return jsonify({"error": "Filename contains control characters"}), 400

    # Auto-append the original extension if the new name doesn't carry one.
    # e.g., user types 'VendorX_Receipt' on a '.pdf' file → 'VendorX_Receipt.pdf'.
    import os as _os
    old_path   = upload.filed_dropbox_path
    old_name   = _os.path.basename(old_path)
    parent_dir = _os.path.dirname(old_path) or "/"
    _, old_ext = _os.path.splitext(old_name)
    _, new_ext = _os.path.splitext(new_filename)
    if old_ext and not new_ext:
        new_filename = new_filename + old_ext

    if new_filename == old_name:
        return jsonify({
            "ok": True,
            "new_filename":  new_filename,
            "new_path":      old_path,
            "message":       "Filename unchanged — no move needed.",
        })

    new_path = f"{parent_dir}/{new_filename}"

    # Do the Dropbox move. autorename=True: if another file already has
    # this name in the same folder, Dropbox appends ' (2)' etc. — we
    # capture the final path it actually ended up at.
    try:
        dbx = _dbx_client()
        res = dbx.files_move_v2(old_path, new_path, autorename=True)
        final_path = getattr(getattr(res, 'metadata', None), 'path_display', None) or new_path
        final_name = _os.path.basename(final_path)
    except Exception as e:
        logging.exception(f"Rename failed: {old_path} → {new_path}")
        return jsonify({
            "error": f"Dropbox move failed: {type(e).__name__}: {e}"
        }), 500

    upload.filed_filename     = final_name
    upload.filed_dropbox_path = final_path
    from datetime import datetime as _dt
    # Touch filed_at so the row shows a "recently modified" sort if needed.
    upload.filed_at = _dt.utcnow()
    db.session.commit()

    logging.info(f"Renamed upload {uid}: {old_path} → {final_path}")
    return jsonify({
        "ok":           True,
        "new_filename": final_name,
        "new_path":     final_path,
        "message":      "Renamed." if final_name == new_filename
                        else f"Renamed (Dropbox autorenamed to avoid collision: {final_name}).",
    })


@app.errorhandler(404)
def page_not_found(e):
    return render_template("404.html"), 404


@app.route("/projects")
def projects_redirect():
    return redirect(url_for("dashboard"))


if __name__ == "__main__":
    if _HAS_SOCKETIO:
        socketio.run(app, debug=True, port=5001)
    else:
        app.run(debug=True, port=5001)
