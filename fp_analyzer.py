"""FP Document Analyzer — embedded copy.

This is a direct copy of FPReceiptRouter/processor.py so FPBudget can
run the Analyzer pipeline (OCR → doc-type detection → auto-filing to
Dropbox) in-process, without an inter-service HTTP call.

Keep this file in sync with the FPReceiptRouter upstream. When logic
changes upstream, copy back to here (or pull upstream changes into
the standalone Analyzer from here). Low-frequency syncing is fine at
current scale; long-term we can extract to a shared pip package.

Exposed entry points used by FPBudget (see bottom of file):
  analyze_and_file_single(file_bytes, filename, project_name, user_name)
    → runs the batch pipeline for exactly one file, synchronously,
      and returns a result dict usable by docs_upload_post.
"""
# ── fp_analyzer.py ══════════════════════════════════════════════════════════

import os, re, io, logging, tempfile, uuid, hashlib
from PIL import Image
import veryfi
import dropbox
from dropbox.exceptions import ApiError

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.expanduser("~/fp_receipt_router.log")),
        logging.StreamHandler(),
    ]
)

try:
    import pillow_heif
    pillow_heif.register_heif_opener()
except ImportError:
    pass

log = logging.getLogger(__name__)

SUPPORTED_EXTS = {".jpg", ".jpeg", ".png", ".pdf", ".heic"}

# Document types → Dropbox folder (relative to project root)
DOCUMENT_TYPES = {
    "receipt":   "01_ADMIN/PROCESSED DOCUMENTS",
    "invoice":   "01_ADMIN/CONTRACTS & INVOICES",
    "contract":  "01_ADMIN/CONTRACTS & INVOICES",
    "release":   "02_PRE-PRODUCTION/TALENT & RELEASES",
    "estimate":  "01_ADMIN/SOW",
    "insurance": "01_ADMIN/INSURANCE & COIs",
    "legal":     "01_ADMIN/LEGAL",
    "payroll":   "01_ADMIN/PAYROLL",
    "quote":     "01_ADMIN/QUOTES & MISC DOCS",
}

DOC_PREFIXES = {
    "receipt":   "RECEIPT",
    "invoice":   "INVOICE",
    "contract":  "CONTRACT",
    "release":   "RELEASE",
    "estimate":  "ESTIMATE",
    "insurance": "COI",
    "legal":     "LEGAL",
    "payroll":   "PAYROLL",
    "quote":     "QUOTE",
}

TOKEN_FIELDS = {
    "date":           lambda r: (r.get("date") or "Unknown").split(" ")[0].split("T")[0],
    "vendor":         lambda r: r.get("vendor", {}).get("name", "Unknown"),
    "total":          lambda r: f"{r['total']:.2f}" if r.get("total") is not None else "Unknown",
    "category":       lambda r: r.get("category", "Unknown"),
    "invoice_number": lambda r: r.get("invoice_number") or "Unknown",
}

DEFAULT_ORDER = ["date", "category", "vendor", "total"]
ORDER_BY_TYPE = {
    "invoice":  ["date", "vendor", "invoice_number", "total"],
    "contract": ["date", "vendor"],
}

# In-memory stores (safe with --workers 1)
# _raw_pending: files prepared (temp files saved) but not yet Veryfi'd
# _pending:     fully analyzed items awaiting user confirmation
_raw_pending: dict[str, list] = {}
_pending:     dict[str, list] = {}


def safe(text):
    text = str(text or "Unknown").strip()
    text = re.sub(r'[<>:"/\\|?*&]', '', text)
    text = re.sub(r'\s+', '_', text)
    text = re.sub(r'_+', '_', text).strip('_')
    return text or "Unknown"


def build_name(vr, doc_type, order=None):
    order = order or ORDER_BY_TYPE.get(doc_type, DEFAULT_ORDER)
    date_val = safe(TOKEN_FIELDS["date"](vr))
    rest = "_".join(
        safe(TOKEN_FIELDS.get(tok, lambda _: "Unknown")(vr))
        for tok in order if tok not in ("None", "date")
    ) or "untitled"
    prefix = DOC_PREFIXES.get(doc_type, "DOC")
    # Date-first so Finder sorts chronologically: 2025-06-18_RECEIPT_Vendor_42.50.pdf
    return f"{date_val}_{prefix}_{rest}.pdf"


def to_pdf_bytes(file_storage):
    ext = os.path.splitext(file_storage.filename)[1].lower()
    original_bytes = file_storage.read()
    if ext == ".pdf":
        return original_bytes, original_bytes
    img = Image.open(io.BytesIO(original_bytes)).convert("RGB")
    buf = io.BytesIO()
    img.save(buf, "PDF", resolution=100.0)
    return buf.getvalue(), original_bytes


def get_veryfi_client():
    return veryfi.Client(
        client_id=os.getenv("VERYFI_CLIENT_ID"),
        client_secret=os.getenv("VERYFI_CLIENT_SECRET"),
        username=os.getenv("VERYFI_USERNAME"),
        api_key=os.getenv("VERYFI_API_KEY"),
    )


IGNORED_PREFIXES = ("_", "!", ".")

def get_dropbox_client():
    refresh_token = os.getenv("DROPBOX_REFRESH_TOKEN")
    app_key       = os.getenv("DROPBOX_APP_KEY")
    app_secret    = os.getenv("DROPBOX_APP_SECRET")
    if refresh_token and app_key and app_secret:
        return dropbox.Dropbox(
            oauth2_refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret,
        )
    # Fallback to short-lived token
    token = (os.getenv("DROPBOX_ACCESS_TOKEN") or "").strip().replace("\n", "").replace(" ", "")
    return dropbox.Dropbox(token)


def list_projects():
    ops_path = os.getenv("DROPBOX_OPERATIONS_PATH", "").rstrip("/")
    try:
        dbx    = get_dropbox_client()
        result = dbx.files_list_folder(ops_path)
        names  = []
        while True:
            for entry in result.entries:
                if isinstance(entry, dropbox.files.FolderMetadata):
                    if not entry.name.startswith(IGNORED_PREFIXES):
                        names.append(entry.name)
            if not result.has_more:
                break
            result = dbx.files_list_folder_continue(result.cursor)
        return sorted(names)
    except Exception as e:
        log.error(f"Failed to list Dropbox projects: {e}")
        return []


def upload_to_dropbox(dbx, file_bytes, dropbox_path):
    try:
        meta = dbx.files_upload(
            file_bytes, dropbox_path,
            mode=dropbox.files.WriteMode.add,
            autorename=True,
        )
        return meta.path_display
    except ApiError as e:
        log.error(f"Dropbox upload failed for {dropbox_path}: {e}")
        raise


# ── Confidence scoring ────────────────────────────────────────────────────────

def _infer_type(vr):
    """Infer document type from Veryfi fields when document_type is null."""
    veryfi_type = (vr.get("document_type") or "").lower()
    if veryfi_type in DOCUMENT_TYPES:
        return veryfi_type, 1.0

    # Map Veryfi's own labels to ours
    veryfi_map = {
        "bill": "invoice", "expense": "receipt", "check": "receipt",
        "insurance": "insurance", "certificate_of_insurance": "insurance",
        "legal": "legal", "payroll": "payroll",
        "quote": "quote", "quotation": "quote",
    }
    if veryfi_type in veryfi_map:
        return veryfi_map[veryfi_type], 0.85

    # Infer from field presence
    has_due_date  = bool(vr.get("due_date"))
    has_bill_to   = bool((vr.get("bill_to") or {}).get("name"))
    has_total     = vr.get("total") is not None
    has_vendor    = bool((vr.get("vendor") or {}).get("name"))
    has_inv_num   = bool(vr.get("invoice_number"))

    if has_due_date or has_bill_to:
        return "invoice", 0.70
    if has_total and has_vendor and has_inv_num:
        return "invoice", 0.60
    if has_total and has_vendor:
        return "receipt", 0.55
    return None, 0.0


def assess_confidence(vr):
    """
    Returns (suggested_type, confidence, needs_review).
    confidence is 0.0–1.0.
    needs_review=True when user should confirm before filing.
    """
    ocr_score = (vr.get("meta") or {}).get("ocr_score", 0)

    has_vendor = bool((vr.get("vendor") or {}).get("name"))
    has_total  = vr.get("total") is not None
    has_date   = bool(vr.get("date"))
    field_score = sum([has_vendor, has_total, has_date]) / 3

    suggested_type, type_conf = _infer_type(vr)

    confidence = round((ocr_score * 0.4) + (field_score * 0.3) + (type_conf * 0.3), 3)
    # Only auto-file if Veryfi explicitly returned a known document_type
    veryfi_type = (vr.get("document_type") or "").lower()
    needs_review = confidence < 0.90 or suggested_type is None or veryfi_type not in DOCUMENT_TYPES

    log.debug(
        f"Confidence: ocr={ocr_score}, fields={field_score:.2f}, "
        f"type_conf={type_conf} → {suggested_type} @ {confidence} "
        f"({'REVIEW' if needs_review else 'AUTO'})"
    )
    return suggested_type, confidence, needs_review


# ── Phase 1a: Prepare (convert + save temp files) ─────────────────────────────

def prepare_files(file_storages, batch_token=None):
    """
    Convert uploaded files to temp PDFs and save originals.
    Appends to an existing batch if batch_token is supplied.
    Returns (batch_token, total_prepared_so_far, error_items_this_call).
    Does NOT call Veryfi.
    """
    if batch_token is None:
        batch_token = str(uuid.uuid4())

    items = _raw_pending.setdefault(batch_token, [])
    errors_this_call = []

    for fs in file_storages:
        if not fs.filename:
            continue

        item = {
            "id":                str(uuid.uuid4()),
            "original_filename": fs.filename,
            "pdf_path":          None,
            "original_path":     None,
            "file_hash":         None,
            "vr":                None,
            "suggested_type":    None,
            "confidence":        0.0,
            "needs_review":      True,
            "error":             None,
        }

        ext = os.path.splitext(fs.filename)[1].lower()
        if ext not in SUPPORTED_EXTS:
            item["error"] = f"Unsupported file type: {ext}"
            errors_this_call.append(item)
            items.append(item)
            continue

        try:
            pdf_bytes, original_bytes = to_pdf_bytes(fs)
            item["file_hash"] = hashlib.sha256(original_bytes).hexdigest()
            pdf_tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
            pdf_tmp.write(pdf_bytes); pdf_tmp.close()
            orig_tmp = tempfile.NamedTemporaryFile(suffix=ext, delete=False)
            orig_tmp.write(original_bytes); orig_tmp.close()
            item["pdf_path"]      = pdf_tmp.name
            item["original_path"] = orig_tmp.name
            items.append(item)
            log.info(f"Prepared {fs.filename} → {pdf_tmp.name}")
        except Exception as e:
            item["error"] = f"Conversion error: {e}"
            log.error(f"Conversion error for {fs.filename}: {e}", exc_info=True)
            errors_this_call.append(item)
            items.append(item)

    return batch_token, len(items), errors_this_call


# ── Duplicate detection ───────────────────────────────────────────────────────

def find_duplicate_groups(batch_token):
    """
    Returns a list of groups — each group is a list of 2+ items with the same
    file hash (identical content). Only looks at items already in _pending.
    """
    items = _pending.get(batch_token, [])
    by_hash = {}
    for item in items:
        h = item.get("file_hash")
        if h:
            by_hash.setdefault(h, []).append(item)
    return [group for group in by_hash.values() if len(group) > 1]


def remove_items_from_pending(batch_token, discard_ids):
    """Remove items by id from _pending[batch_token]. Cleans up their temp files."""
    items = _pending.get(batch_token, [])
    keep, discard = [], []
    for it in items:
        (discard if it["id"] in discard_ids else keep).append(it)
    for it in discard:
        for p in (it.get("pdf_path"), it.get("original_path")):
            if p and os.path.exists(p):
                try: os.unlink(p)
                except: pass
    _pending[batch_token] = keep


# ── Phase 1b: Analyze (Veryfi calls, parallel) ────────────────────────────────

def _call_veryfi(item):
    """Call Veryfi on one prepared item. Each thread gets its own client."""
    try:
        vc = get_veryfi_client()
        vr = vc.process_document(item["pdf_path"])
        item["vr"] = vr
        suggested, confidence, needs_review = assess_confidence(vr)
        item["suggested_type"] = suggested
        item["confidence"]     = confidence
        item["needs_review"]   = needs_review
        log.info(
            f"Analyzed {item['original_filename']}: type={suggested}, "
            f"confidence={confidence}, needs_review={needs_review}"
        )
    except Exception as e:
        item["error"] = f"Veryfi error: {e}"
        log.error(f"Veryfi error for {item['original_filename']}: {e}", exc_info=True)
    return item


def run_analysis(batch_token):
    """
    Run Veryfi sequentially on all prepared items for a batch.
    One file at a time prevents OOM on Render's 512 MB free tier.
    Moves results from _raw_pending → _pending.
    Returns list of analyzed items.
    """
    items   = _raw_pending.pop(batch_token, [])
    ready   = [it for it in items if it.get("pdf_path") and not it.get("error")]
    errored = [it for it in items if it.get("error") or not it.get("pdf_path")]

    for it in ready:
        _call_veryfi(it)

    id_order = {it["id"]: idx for idx, it in enumerate(items)}
    result   = sorted(ready + errored, key=lambda it: id_order[it["id"]])
    _pending[batch_token] = result
    return result


def analyze_files(file_storages):
    """Single-shot wrapper: prepare + analyze in one call (used by legacy routes)."""
    batch_token, _, _ = prepare_files(file_storages)
    items = run_analysis(batch_token)
    return batch_token, items


# ── Auto-filing helpers ───────────────────────────────────────────────────────

# Stores results for items that were auto-filed (duplicates + high-confidence)
# before the user reaches the review screen.
_auto_results: dict[str, list] = {}


def _build_result_skeleton(item):
    return {
        "original_filename": item["original_filename"],
        "success":           False,
        "filename":          item["original_filename"],
        "dest_path":         None,
        "duplicate":         False,
        "error":             item.get("error"),
        "confidence":        item["confidence"],
        "suggested_type":    item["suggested_type"],
        "auto_filed":        False,
        "file_hash":         item.get("file_hash"),
    }


def _file_item(item, dest_path, _unused, dbx):
    """Upload PDF version of one item to Dropbox. Returns (success, actual_path, error)."""
    try:
        pdf_bytes   = open(item["pdf_path"], "rb").read()
        actual_path = upload_to_dropbox(dbx, pdf_bytes, dest_path)
        return True, actual_path, None
    except Exception as e:
        return False, None, f"Dropbox error: {e}"
    finally:
        for p in (item.get("pdf_path"), item.get("original_path")):
            if p and os.path.exists(p):
                try: os.unlink(p)
                except: pass


def handle_duplicates_auto(batch_token, project_name, user_name):
    """
    For each duplicate group in the batch, keep only the first copy; move
    the rest to Processed Documents/Duplicate/ in Dropbox.
    Returns number of duplicates filed.
    """
    groups = find_duplicate_groups(batch_token)
    if not groups:
        return 0

    dbx          = get_dropbox_client()
    ops          = os.getenv("DROPBOX_OPERATIONS_PATH", "").rstrip("/")
    discard_ids  = set()
    dup_results  = []

    for group in groups:
        for item in group[1:]:           # keep group[0], discard the rest
            discard_ids.add(item["id"])
            result = _build_result_skeleton(item)
            result["duplicate"] = True
            dup_dest = (
                f"{ops}/{project_name}/01_ADMIN/PROCESSED DOCUMENTS"
                f"/Duplicate/{item['original_filename']}"
            )
            ok, path, err = _file_item(item, dup_dest, dup_dest, dbx)
            result["success"]   = ok
            result["dest_path"] = path
            result["error"]     = err
            if ok:
                log.info(f"Moved duplicate {item['original_filename']} → Duplicate folder")
            else:
                log.warning(f"Could not file duplicate {item['original_filename']}: {err}")
            dup_results.append(result)

    if dup_results:
        _auto_results.setdefault(batch_token, []).extend(dup_results)
    remove_items_from_pending(batch_token, discard_ids)
    return len(discard_ids)


def mark_known_dupes(batch_token, known_hashes, project_name, user_name):
    """
    Given a set of file_hash strings already in the filed-document store,
    auto-move matching pending items to the Duplicate folder.
    Returns count of items moved.
    """
    if not known_hashes:
        return 0
    items = _pending.get(batch_token, [])
    discard_ids = set()
    dup_results = []
    dbx = get_dropbox_client()
    ops = os.getenv("DROPBOX_OPERATIONS_PATH", "").rstrip("/")

    for item in items:
        if item.get("file_hash") not in known_hashes:
            continue
        discard_ids.add(item["id"])
        result = _build_result_skeleton(item)
        result["duplicate"] = True
        dup_dest = (
            f"{ops}/{project_name}/01_ADMIN/PROCESSED DOCUMENTS"
            f"/Duplicate/{item['original_filename']}"
        )
        ok, path, err = _file_item(item, dup_dest, dup_dest, dbx)
        result["success"]   = ok
        result["dest_path"] = path
        result["error"]     = err
        if ok:
            log.info(f"Cross-session duplicate {item['original_filename']} → Duplicate folder")
        else:
            log.warning(f"Could not move cross-session dup {item['original_filename']}: {err}")
        dup_results.append(result)

    if dup_results:
        _auto_results.setdefault(batch_token, []).extend(dup_results)
    remove_items_from_pending(batch_token, discard_ids)
    return len(discard_ids)


def auto_file_high_confidence(batch_token, project_name, user_name):
    """
    File all items whose needs_review=False directly to Dropbox without review.
    Returns number of items filed.
    """
    items     = _pending.get(batch_token, [])
    auto_ids  = {
        it["id"] for it in items
        if not it.get("needs_review") and not it.get("error") and it.get("vr")
    }
    if not auto_ids:
        return 0

    dbx        = get_dropbox_client()
    ops        = os.getenv("DROPBOX_OPERATIONS_PATH", "").rstrip("/")
    auto_res   = []
    remove_ids = set()

    for item in items:
        if item["id"] not in auto_ids:
            continue
        remove_ids.add(item["id"])
        result   = _build_result_skeleton(item)
        result["auto_filed"] = True

        doc_type = item["suggested_type"] or "receipt"
        folder   = DOCUMENT_TYPES.get(doc_type, "01_ADMIN/RECEIPTS FOLDER")
        new_name = build_name(item["vr"], doc_type)
        base     = f"{ops}/{project_name}/{folder}"

        if doc_type in ("invoice", "contract"):
            vendor_folder  = safe((item["vr"].get("vendor") or {}).get("name") or "Unknown_Vendor")
            processed_path = f"{base}/VENDOR AGREEMENTS/{vendor_folder}/{new_name}"
        elif doc_type in ("insurance",):
            vendor_folder  = safe((item["vr"].get("vendor") or {}).get("name") or "Unknown_Vendor")
            processed_path = f"{base}/{vendor_folder}/{new_name}"
        else:
            user_folder    = safe(user_name)
            processed_path = f"{base}/{user_folder}/{new_name}"

        ok, path, err = _file_item(item, processed_path, processed_path, dbx)
        actual_name        = os.path.basename(path) if path else new_name
        result["success"]  = ok
        result["filename"] = actual_name if ok else item["original_filename"]
        result["dest_path"]= path
        result["error"]    = err
        result["duplicate"]= ok and (actual_name != new_name)
        if ok:
            log.info(f"Auto-filed {item['original_filename']} → {path}")
        else:
            log.error(f"Auto-file error {item['original_filename']}: {err}")
        auto_res.append(result)

    if auto_res:
        _auto_results.setdefault(batch_token, []).extend(auto_res)
    remove_items_from_pending(batch_token, remove_ids)
    return len(remove_ids)


def has_review_items(batch_token):
    """True if any items still need manual review."""
    return any(
        it.get("needs_review") and not it.get("error")
        for it in _pending.get(batch_token, [])
    )


def flush_auto_results(batch_token):
    """Return all auto-filed results for a batch, clearing pending + auto state."""
    _pending.pop(batch_token, None)
    return _auto_results.pop(batch_token, [])


# ── Phase 2: File to Dropbox ──────────────────────────────────────────────────

def file_confirmed(batch_token, confirmations, project_name, user_name):
    """
    confirmations: dict of {item_id: doc_type}
    Files each pending item to Dropbox using the confirmed type.
    Cleans up temp files. Returns list of result dicts (including any auto-filed).
    """
    items   = _pending.pop(batch_token, [])
    dbx     = get_dropbox_client()
    ops     = os.getenv("DROPBOX_OPERATIONS_PATH", "").rstrip("/")
    results = []

    for item in items:
        result = {
            "original_filename": item["original_filename"],
            "success":           False,
            "filename":          item["original_filename"],
            "dest_path":         None,
            "duplicate":         False,
            "error":             item.get("error"),
            "confidence":        item["confidence"],
            "suggested_type":    item["suggested_type"],
            "file_hash":         item.get("file_hash"),
        }

        # Clean up temp files regardless of outcome
        def cleanup():
            for p in (item.get("pdf_path"), item.get("original_path")):
                if p and os.path.exists(p):
                    try: os.unlink(p)
                    except: pass

        if item.get("error") or item.get("vr") is None:
            cleanup()
            results.append(result)
            continue

        doc_type  = confirmations.get(item["id"], item["suggested_type"]) or "receipt"
        folder    = DOCUMENT_TYPES.get(doc_type, "01_ADMIN/RECEIPTS FOLDER")
        new_name  = build_name(item["vr"], doc_type)
        base      = f"{ops}/{project_name}/{folder}"

        if doc_type in ("invoice", "contract"):
            vendor_folder  = safe((item["vr"].get("vendor") or {}).get("name") or "Unknown_Vendor")
            processed_path = f"{base}/VENDOR AGREEMENTS/{vendor_folder}/{new_name}"
        elif doc_type in ("insurance",):
            vendor_folder  = safe((item["vr"].get("vendor") or {}).get("name") or "Unknown_Vendor")
            processed_path = f"{base}/{vendor_folder}/{new_name}"
        else:
            user_folder    = safe(user_name)
            processed_path = f"{base}/{user_folder}/{new_name}"

        try:
            pdf_bytes   = open(item["pdf_path"], "rb").read()
            actual_path = upload_to_dropbox(dbx, pdf_bytes, processed_path)

            actual_name = os.path.basename(actual_path)
            result["success"]   = True
            result["filename"]  = actual_name
            result["dest_path"] = actual_path
            result["duplicate"] = actual_name != new_name
            log.info(f"Filed {item['original_filename']} → {actual_path}")
        except Exception as e:
            result["error"] = f"Dropbox error: {e}"
            log.error(f"Dropbox error for {item['original_filename']}: {e}", exc_info=True)
        finally:
            cleanup()

        results.append(result)

    # Prepend any items that were auto-filed before review (high-confidence + duplicates)
    prior = _auto_results.pop(batch_token, [])
    return prior + results


# ═══════════════════════════════════════════════════════════════════════════
# ── FPBudget integration: single-file synchronous wrapper ──────────────────
# ═══════════════════════════════════════════════════════════════════════════
#
# The Analyzer's batch pipeline (prepare_files → run_analysis → auto_file_*)
# is designed for a browser-driven flow: user drops multiple files, a batch
# token threads through, a review UI opens for low-confidence items.
#
# FPBudget uploads come one file at a time from the /docs/<pid>/upload
# endpoint and we want a synchronous result (filed or needs_review) that
# fits on the existing DocUpload row. This wrapper runs the batch pipeline
# for exactly one file and returns a plain dict.

class _InMemoryFileStorage:
    """Minimal duck-type that mimics werkzeug FileStorage well enough for
    prepare_files → to_pdf_bytes. We only need `.filename`, `.read()`, and
    `.seek()`."""
    def __init__(self, data: bytes, filename: str):
        self._buf = io.BytesIO(data)
        self.filename = filename

    def read(self, *a, **kw):
        return self._buf.read(*a, **kw)

    def seek(self, *a, **kw):
        return self._buf.seek(*a, **kw)

    def save(self, dst):
        # Called by to_pdf_bytes on some paths. Dump buffer to destination.
        self._buf.seek(0)
        if hasattr(dst, 'write'):
            dst.write(self._buf.read())
        else:
            with open(dst, 'wb') as f:
                f.write(self._buf.read())
        self._buf.seek(0)


def analyze_and_file_single(file_bytes: bytes, filename: str,
                            project_name: str, user_name: str) -> dict:
    """Run the Analyzer pipeline on ONE file, synchronously. Returns:

        {
          "status":       "filed" | "needs_review" | "error",
          "doc_type":     "receipt" | "invoice" | ... | None,
          "filed_path":   str | None,       # Dropbox path if auto-filed
          "confidence":   float,            # 0.0–1.0
          "new_filename": str | None,       # normalized name per naming convention
          "original_filename": str,
          "duplicate":    bool,             # true if Dropbox autorenamed due to collision
          "error":        str | None,
          "needs_review": bool,             # true iff status == 'needs_review'
        }

    On 'needs_review' status, no Dropbox write happens — FPBudget should
    stash the file bytes + OCR result locally so the user can finish
    filing via a review UI.
    """
    fs = _InMemoryFileStorage(file_bytes, filename)

    # Phase 1: prepare (converts to PDF, saves temp files, computes hash)
    batch_token, _total, errs = prepare_files([fs])
    if errs:
        return {
            "status":            "error",
            "doc_type":          None,
            "filed_path":        None,
            "confidence":        0.0,
            "new_filename":      None,
            "original_filename": filename,
            "duplicate":         False,
            "error":             errs[0].get("error") or "File preparation failed",
            "needs_review":      False,
        }

    # Phase 2: OCR + classify (synchronous — blocks ~2-5s per Veryfi call)
    items = run_analysis(batch_token)
    if not items:
        return {
            "status":            "error",
            "doc_type":          None,
            "filed_path":        None,
            "confidence":        0.0,
            "new_filename":      None,
            "original_filename": filename,
            "duplicate":         False,
            "error":             "No items after analysis",
            "needs_review":      False,
        }
    item = items[0]
    if item.get("error"):
        # Clean up temp files before returning
        try:
            remove_items_from_pending(batch_token, [item["id"]])
        except Exception:
            pass
        return {
            "status":            "error",
            "doc_type":          item.get("suggested_type"),
            "filed_path":        None,
            "confidence":        float(item.get("confidence") or 0.0),
            "new_filename":      None,
            "original_filename": filename,
            "duplicate":         False,
            "error":             item.get("error"),
            "needs_review":      False,
        }

    confidence = float(item.get("confidence") or 0.0)
    doc_type   = item.get("suggested_type")
    needs_rev  = bool(item.get("needs_review"))

    # Phase 3a: high-confidence → auto-file to the correct Dropbox folder.
    # auto_file_high_confidence handles the filing + duplicate-rename.
    if not needs_rev:
        try:
            auto_file_high_confidence(batch_token, project_name, user_name)
        except Exception as e:
            log.error(f"auto_file_high_confidence error: {e}", exc_info=True)
            # fall through to 'needs_review' return below

        autos = _auto_results.pop(batch_token, [])
        remove_items_from_pending(batch_token, [item["id"]])
        if autos:
            r = autos[0]
            return {
                "status":            "filed" if r.get("success") else "error",
                "doc_type":          doc_type,
                "filed_path":        r.get("dest_path"),
                "confidence":        confidence,
                "new_filename":      r.get("filename"),
                "original_filename": filename,
                "duplicate":         bool(r.get("duplicate")),
                "error":             r.get("error"),
                "needs_review":      False,
            }

    # Phase 3b: low-confidence OR auto-file failed above → needs_review.
    # We leave the item in _pending (don't clean up temp files yet) so a
    # future review UI can finalize filing. Caller should stash what's
    # needed to drive a review later — e.g. the batch_token, item id, and
    # OCR result — in FPBudget's DocUpload row.
    return {
        "status":            "needs_review",
        "doc_type":          doc_type,
        "filed_path":        None,
        "confidence":        confidence,
        "new_filename":      build_name(item.get("vr") or {}, doc_type or "receipt") if doc_type else None,
        "original_filename": filename,
        "duplicate":         False,
        "error":             None,
        "needs_review":      True,
        "batch_token":       batch_token,   # for future review UI
        "item_id":           item["id"],
    }
