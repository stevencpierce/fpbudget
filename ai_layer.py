"""AI reasoning layer over Veryfi JSON — FP Budget AI-Layer spec (2026-06-17).

Veryfi EXTRACTS; this layer REASONS over the structured JSON (never the image).
Two tasks behind one swappable interface:
  • categorize()        — map a document to OUR taxonomy (COA / budget lines)
  • detect_anomalies()  — duplicate / anomaly judgment on ambiguous candidates

Single-provider (Claude) for now; an OpenAIProvider can drop in behind the same
interface later, routed by env (AI_ROUTE_CATEGORIZE / AI_ROUTE_ANOMALY).

Cardinal rule: ALWAYS fail open. On missing key / SDK / error / timeout, return
a neutral result so the caller saves the doc as needs_review — never blocks the
user. Output is forced through Claude tool-use so the backend never parses free
text.
"""
import os
import json
import logging

log = logging.getLogger(__name__)

try:
    import anthropic as _anthropic
    _HAS_ANTHROPIC = True
except ImportError:                       # pragma: no cover
    _HAS_ANTHROPIC = False

# Pinned model + timeout (override via env so output can't drift under us).
_CLAUDE_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
try:
    _TIMEOUT_S = max(2.0, float(os.getenv("AI_TIMEOUT_MS", "8000")) / 1000.0)
except (TypeError, ValueError):
    _TIMEOUT_S = 8.0


def _route(task, default="claude"):
    """Which provider serves a task — env-overridable, defaults to claude."""
    return (os.getenv("AI_ROUTE_" + task.upper()) or default).strip().lower()


# ── Provider interface ──────────────────────────────────────────────────────
class AIProvider:
    name = "base"

    def available(self):
        return False

    def categorize(self, payload):
        raise NotImplementedError

    def detect_anomalies(self, payload):
        raise NotImplementedError

    def clean_document(self, payload, image_url=None):
        raise NotImplementedError


class _NeutralProvider(AIProvider):
    """Fail-open stand-in when no real provider is configured/reachable."""
    name = "none"

    def available(self):
        return False

    def categorize(self, payload):
        return {"document_category_id": None, "confidence": 0.0, "line_items": [],
                "reasoning": "AI categorization unavailable (no provider/key)",
                "_provider": "none"}

    def detect_anomalies(self, payload):
        return {"is_duplicate": False, "duplicate_of_id": None, "confidence": 0.0,
                "anomalies": [], "recommended_action": "review", "_provider": "none"}

    def clean_document(self, payload, image_url=None):
        # No-op: keep whatever the OCR produced, confidence 0 so nothing is
        # auto-applied and nothing is queued as a "fix".
        return {"clean_vendor": None, "vendor_confidence": 0.0, "amount_ok": True,
                "suggested_amount": None, "date_ok": True, "suggested_date": None,
                "issues": [], "overall_confidence": 0.0, "_provider": "none"}


# Forced-output schemas (Claude tool-use) — see spec §3/§4.
_CATEGORIZE_TOOL = {
    "name": "categorize_document",
    "description": "Assign the document to exactly one category id from the provided taxonomy.",
    "input_schema": {
        "type": "object",
        "properties": {
            "document_category_id": {"type": "string",
                "description": "A category id from the taxonomy, or 'uncategorized'."},
            "confidence": {"type": "number", "description": "0.0–1.0"},
            "reasoning": {"type": "string", "description": "Short, internal."},
        },
        "required": ["document_category_id", "confidence"],
    },
}
_ANOMALY_TOOL = {
    "name": "review_document",
    "description": "Decide if the new document duplicates a match or shows anomalies.",
    "input_schema": {
        "type": "object",
        "properties": {
            "is_duplicate": {"type": "boolean"},
            "duplicate_of_id": {"type": ["integer", "null"]},
            "confidence": {"type": "number"},
            "anomalies": {"type": "array", "items": {
                "type": "object",
                "properties": {
                    "type": {"type": "string"},
                    "severity": {"type": "string", "enum": ["low", "medium", "high"]},
                    "explanation": {"type": "string"},
                },
            }},
            "recommended_action": {"type": "string", "enum": ["accept", "review", "reject"]},
            "explanation": {"type": "string",
                "description": "One short, user-facing sentence summarizing the verdict."},
        },
        "required": ["is_duplicate", "confidence", "recommended_action", "explanation"],
    },
}
_CLEAN_TOOL = {
    "name": "clean_document",
    "description": "Clean up the OCR-extracted fields of a document for human-facing display.",
    "input_schema": {
        "type": "object",
        "properties": {
            "clean_vendor": {"type": ["string", "null"],
                "description": "A short, human-readable merchant name (e.g. 'Amazon', "
                               "'United Airlines'). Prefer an exact match from KNOWN_VENDORS "
                               "when the raw name clearly refers to the same merchant, to keep "
                               "the project consistent. Null if you cannot tell."},
            "vendor_confidence": {"type": "number", "description": "0.0–1.0 for clean_vendor."},
            "amount_ok": {"type": "boolean",
                "description": "False if the amount looks wrong/implausible for this document."},
            "suggested_amount": {"type": ["number", "null"]},
            "date_ok": {"type": "boolean"},
            "suggested_date": {"type": ["string", "null"], "description": "YYYY-MM-DD or null."},
            "issues": {"type": "array", "items": {
                "type": "object",
                "properties": {
                    "field": {"type": "string"},
                    "severity": {"type": "string", "enum": ["low", "medium", "high"]},
                    "note": {"type": "string"},
                },
            }},
            "overall_confidence": {"type": "number",
                "description": "0.0–1.0 overall confidence the extracted data is correct."},
        },
        "required": ["clean_vendor", "vendor_confidence", "overall_confidence"],
    },
}


class ClaudeProvider(AIProvider):
    name = "claude"

    def __init__(self):
        self._key = os.getenv("ANTHROPIC_API_KEY")

    def available(self):
        return bool(_HAS_ANTHROPIC and self._key)

    def _call(self, system, user_obj, tool, image_url=None):
        client = _anthropic.Anthropic(api_key=self._key, timeout=_TIMEOUT_S)
        # When an image is supplied (low-confidence docs), attach it so the model
        # can read the actual receipt, not just the OCR JSON. (claude-haiku-4-5
        # supports vision.)
        content = []
        if image_url:
            content.append({"type": "image",
                            "source": {"type": "url", "url": image_url}})
        content.append({"type": "text", "text": json.dumps(user_obj, default=str)})
        msg = client.messages.create(
            model=_CLAUDE_MODEL,
            max_tokens=900,
            system=system,
            tools=[tool],
            tool_choice={"type": "tool", "name": tool["name"]},
            messages=[{"role": "user", "content": content}],
        )
        for block in msg.content:
            if getattr(block, "type", None) == "tool_use":
                return dict(block.input)
        raise ValueError("Claude returned no tool_use block")

    def categorize(self, payload):
        system = (
            "Assign the document to exactly one category id from the provided TAXONOMY. "
            "Use the vendor, Veryfi's suggested category, and line-item descriptions. "
            "Strongly prefer the user's PAST_CORRECTIONS for the same vendor when present. "
            "If nothing fits well, return 'uncategorized' with low confidence rather than "
            "guessing. Reason only from the data given."
        )
        out = self._call(system, payload, _CATEGORIZE_TOOL)
        out["_provider"] = "claude"
        out["_model"] = _CLAUDE_MODEL
        return out

    def detect_anomalies(self, payload):
        system = (
            "You are a financial-document reviewer for an expense/budget app. You receive "
            "a NEW_DOCUMENT and POSSIBLE_MATCHES, all structured JSON already extracted by "
            "OCR. Decide whether the new document is a duplicate of any match, or shows "
            "anomalies worth a human's attention. Reason only from the data given. Do not "
            "invent fields. Be conservative: when unsure, flag for review rather than "
            "auto-reject."
        )
        out = self._call(system, payload, _ANOMALY_TOOL)
        out["_provider"] = "claude"
        out["_model"] = _CLAUDE_MODEL
        return out

    def clean_document(self, payload, image_url=None):
        system = (
            "You clean up OCR-extracted fields of an expense document so they read well "
            "to a human. Produce a short, recognizable merchant name for clean_vendor "
            "(drop store/terminal numbers, ALL-CAPS noise, city/state codes, payment-"
            "processor cruft). If KNOWN_VENDORS contains an entry that clearly refers to "
            "the same merchant, return that exact string so the project stays consistent. "
            "Sanity-check amount and date; flag anything implausible in issues. "
            + ("An IMAGE of the document is attached — read it directly to verify and "
               "correct the vendor, amount, and date when the OCR text looks wrong or "
               "ambiguous, and raise your confidence accordingly. "
               if image_url else
               "Reason ONLY from the data given — never invent a vendor you cannot infer "
               "from the input. ")
            + "Be honest with confidence: low when the source is ambiguous."
        )
        out = self._call(system, payload, _CLEAN_TOOL, image_url=image_url)
        out["_provider"] = "claude"
        out["_model"] = _CLAUDE_MODEL
        out["_vision"] = bool(image_url)
        return out


def get_provider(task):
    """Provider for a task ('categorize'|'anomaly'), honoring AI_ROUTE_*. Falls
    back to the neutral fail-open provider when the chosen one isn't available."""
    want = _route(task)
    if want == "claude":
        p = ClaudeProvider()
        if p.available():
            return p
    # OpenAIProvider() would slot in here when multi-provider is enabled.
    return _NeutralProvider()


def status():
    """Lightweight introspection for an admin ping — no model call."""
    return {
        "has_sdk": _HAS_ANTHROPIC,
        "claude_key_set": bool(os.getenv("ANTHROPIC_API_KEY")),
        "model": _CLAUDE_MODEL,
        "timeout_s": _TIMEOUT_S,
        "route_categorize": _route("categorize"),
        "route_anomaly": _route("anomaly"),
        "route_clean": _route("clean"),
        "categorize_available": get_provider("categorize").name,
        "anomaly_available": get_provider("anomaly").name,
        "clean_available": get_provider("clean").name,
    }


def categorize(payload):
    """Fail-open categorize. payload: {taxonomy:[...], document:{...}, past_corrections:[...]}"""
    p = get_provider("categorize")
    try:
        return p.categorize(payload)
    except Exception as e:
        log.warning("[ai_layer] categorize failed (%s): %s", getattr(p, "name", "?"), e)
        return _NeutralProvider().categorize(payload)


def detect_anomalies(payload):
    """Fail-open anomaly detection. payload: {new_document:{...}, possible_matches:[...]}"""
    p = get_provider("anomaly")
    try:
        return p.detect_anomalies(payload)
    except Exception as e:
        log.warning("[ai_layer] detect_anomalies failed (%s): %s", getattr(p, "name", "?"), e)
        return _NeutralProvider().detect_anomalies(payload)


def clean_document(payload, image_url=None):
    """Fail-open OCR cleanup. payload: {document:{...}, known_vendors:[...]}.
    image_url: optional URL of the receipt image — passed to the model (vision)
    so it can read the actual document on low-confidence extractions."""
    p = get_provider("clean")
    try:
        return p.clean_document(payload, image_url=image_url)
    except Exception as e:
        log.warning("[ai_layer] clean_document failed (%s): %s", getattr(p, "name", "?"), e)
        return _NeutralProvider().clean_document(payload)
