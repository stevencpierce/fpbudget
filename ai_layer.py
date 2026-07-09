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

    def pick_match(self, payload):
        raise NotImplementedError

    def extract_travel(self, payload, image_url=None):
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

    def pick_match(self, payload):
        return {"best_doc_id": None, "confidence": 0.0,
                "reasoning": "AI matching unavailable (no provider/key)",
                "_provider": "none"}

    def extract_travel(self, payload, image_url=None):
        return {"is_travel": False, "kind": None, "confirmation_no": None,
                "confidence": 0.0, "_provider": "none"}


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
_MATCH_TOOL = {
    "name": "pick_match",
    "description": "Pick which candidate receipt (if any) documents this charge.",
    "input_schema": {
        "type": "object",
        "properties": {
            "best_doc_id": {"type": ["integer", "null"],
                "description": "doc_upload_id of the matching receipt from CANDIDATES, "
                               "or null if none is a confident match."},
            "confidence": {"type": "number", "description": "0.0–1.0"},
            "reasoning": {"type": "string", "description": "Short, internal."},
        },
        "required": ["best_doc_id", "confidence"],
    },
}
_TRAVEL_TOOL = {
    "name": "extract_travel",
    "description": "Extract structured travel-reservation details from a hotel, "
                   "flight/airline, or car-rental document.",
    "input_schema": {
        "type": "object",
        "properties": {
            "is_travel": {"type": "boolean",
                "description": "True only if this is a hotel, flight/airline, "
                               "car-rental, or car-service/black-car reservation/"
                               "receipt/itinerary."},
            "kind": {"type": ["string", "null"],
                "enum": ["flight", "hotel", "car_rental", "car_service", None],
                "description": "The travel type, or null if not travel. Use "
                               "car_service for a chauffeur/black-car/sedan/limo "
                               "pickup booking (a driver picks the traveler up); "
                               "use car_rental only for a rental-counter agreement "
                               "(Hertz/Avis/Enterprise — the traveler drives)."},
            "confirmation_no": {"type": ["string", "null"],
                "description": "Booking/reservation/confirmation number exactly as printed."},
            "traveler_name": {"type": ["string", "null"],
                "description": "Guest/passenger name on the reservation, if shown."},
            "airline": {"type": ["string", "null"]},
            "flight_no": {"type": ["string", "null"], "description": "e.g. 'UA1234'."},
            "depart_at": {"type": ["string", "null"], "description": "ISO 8601 local departure datetime, or null."},
            "arrive_at": {"type": ["string", "null"], "description": "ISO 8601 local arrival datetime, or null."},
            "depart_airport": {"type": ["string", "null"], "description": "IATA code, e.g. LAX."},
            "arrive_airport": {"type": ["string", "null"], "description": "IATA code."},
            "hotel_name": {"type": ["string", "null"]},
            "hotel_address": {"type": ["string", "null"]},
            "check_in": {"type": ["string", "null"], "description": "YYYY-MM-DD."},
            "check_out": {"type": ["string", "null"], "description": "YYYY-MM-DD."},
            "room_type": {"type": ["string", "null"]},
            "rental_co": {"type": ["string", "null"],
                "description": "Rental or car-service company name."},
            "pickup_at": {"type": ["string", "null"], "description": "ISO 8601, or null."},
            "return_at": {"type": ["string", "null"],
                "description": "ISO 8601 return/drop-off datetime, or null."},
            "pickup_location": {"type": ["string", "null"]},
            "dropoff_location": {"type": ["string", "null"],
                "description": "Car-service drop-off address, if distinct from pickup."},
            "contact_phone": {"type": ["string", "null"],
                "description": "Driver / dispatch contact phone, if shown (car service)."},
            "confidence": {"type": "number", "description": "0.0–1.0 overall confidence."},
        },
        "required": ["is_travel", "kind", "confidence"],
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

    def pick_match(self, payload):
        system = (
            "You match a bank/card CHARGE to the RECEIPT that documents it, choosing "
            "from CANDIDATES (each has a doc_id). A match means the same merchant AND "
            "the same amount — or a plausibly tip-adjusted / rounded amount — within a "
            "few days. A refund receipt pairs with a refund/credit charge of the same "
            "magnitude (opposite sign). Return the chosen candidate's doc_id and your "
            "confidence, or best_doc_id=null if none is a confident match. Be "
            "conservative: when unsure, return null rather than guessing. Reason only "
            "from the data given."
        )
        out = self._call(system, payload, _MATCH_TOOL)
        out["_provider"] = "claude"
        out["_model"] = _CLAUDE_MODEL
        return out

    def extract_travel(self, payload, image_url=None):
        system = (
            "You extract structured TRAVEL RESERVATION details from a document — a hotel "
            "folio/booking, an airline/flight itinerary, or a car-rental agreement. "
            + ("An IMAGE of the document is attached — read it directly. "
               if image_url else
               "Use the OCR text/fields provided in DOCUMENT. ")
            + "Return the confirmation/booking number EXACTLY as printed, the "
            "traveler/guest name if shown, and the kind-specific fields (flight: airline, "
            "flight number, depart/arrive datetimes in ISO 8601 and IATA airport codes; "
            "hotel: name, address, check-in/out as YYYY-MM-DD, room type; car rental: "
            "company, pickup/return datetimes and pickup location; car service / black "
            "car / chauffeur: company, pickup datetime + location, drop-off location, "
            "and driver/dispatch contact phone). A chauffeur/black-car/sedan/limo booking "
            "where a DRIVER picks the traveler up is kind=car_service; a rental-counter "
            "agreement (Hertz/Avis/Enterprise) is kind=car_rental. Set is_travel=false and "
            "kind=null when this is NOT a hotel/flight/car reservation. Reason ONLY from "
            "the document — never invent a confirmation number or dates. Be honest with "
            "confidence (low when the source is ambiguous)."
        )
        out = self._call(system, payload, _TRAVEL_TOOL, image_url=image_url)
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
        "route_match": _route("match"),
        "route_travel": _route("travel"),
        "categorize_available": get_provider("categorize").name,
        "anomaly_available": get_provider("anomaly").name,
        "clean_available": get_provider("clean").name,
        "match_available": get_provider("match").name,
        "travel_available": get_provider("travel").name,
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


def pick_match(payload):
    """Fail-open receipt↔charge matcher. payload: {charge:{...}, candidates:[{doc_id,...}]}.
    Returns {best_doc_id, confidence, reasoning}."""
    p = get_provider("match")
    try:
        return p.pick_match(payload)
    except Exception as e:
        log.warning("[ai_layer] pick_match failed (%s): %s", getattr(p, "name", "?"), e)
        return _NeutralProvider().pick_match(payload)


def extract_travel(payload, image_url=None):
    """Fail-open travel-reservation extractor. payload: {document:{...}}; image_url
    optional (vision). Returns {is_travel, kind, confirmation_no, ...fields, confidence}."""
    p = get_provider("travel")
    try:
        return p.extract_travel(payload, image_url=image_url)
    except Exception as e:
        log.warning("[ai_layer] extract_travel failed (%s): %s", getattr(p, "name", "?"), e)
        return _NeutralProvider().extract_travel(payload)
