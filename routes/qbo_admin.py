"""QuickBooks vendor Link screen (DOCUMENTS_HANDOFF Phase 3, 2026-11-02).

QuickBooks is the system of record for vendors. qbo_sync.sync_vendors
mirrors every QuickBooks Vendor into the qbo_vendor table and links the
ones whose DisplayName matches a Payee exactly; the rest wait here. This
screen lists the QuickBooks vendors no Payee carries and lets a
super-admin link each one to an existing Payee or create a Payee from it.
Nothing here writes to QuickBooks.

Routes (super-admin only):
  GET  /admin/qbo-vendors                           the screen
  GET  /admin/qbo-vendors.json                      its data
  POST /admin/qbo-vendors/sync                      run sync_vendors now
  POST /admin/qbo-vendors/<vid>/link   {payee_id}   link to an existing Payee
  POST /admin/qbo-vendors/<vid>/create-payee        Payee (kind=vendor) from it

See routes/__init__.py for the module pattern (plain @app.route on the
same app object, imported at the bottom of app.py).
"""
from datetime import datetime

from flask import render_template, request, jsonify, redirect, url_for, flash
from flask_login import login_required
from sqlalchemy import func

from app import app, super_admin_required, _log_activity
from models import db, CrewMember, QBOVendor, QBOConnection, Transaction
from actuals import attribute_txns_for_qbo_vendor
import qbo_sync


def _fmt_dt(d):
    if not d:
        return ''
    return f"{d.strftime('%b')} {d.day}, {d.year} {d.strftime('%H:%M')} UTC"


def _vendor_dict(v, suggestions, charge_counts):
    addr = ", ".join(x for x in (v.address_line1, v.address_line2,
                                 " ".join(y for y in (v.city, v.state, v.postal_code) if y)) if x)
    return {
        "id": v.qbo_vendor_id, "display_name": v.display_name or f"Vendor #{v.qbo_vendor_id}",
        "company_name": v.company_name, "email": v.email, "phone": v.phone,
        "address": addr, "vendor_1099": v.vendor_1099, "active": v.active,
        "synced_at": v.synced_at.isoformat() if v.synced_at else None,
        "charges": charge_counts.get(v.qbo_vendor_id, 0),
        "suggestions": suggestions.get(v.qbo_vendor_id, []),
    }


def _screen_data():
    conn = QBOConnection.query.first()
    linked_ids = {str(x) for (x,) in db.session.query(CrewMember.qbo_vendor_id)
                  .filter(CrewMember.qbo_vendor_id.isnot(None)).all()}
    vendors = QBOVendor.query.order_by(func.lower(QBOVendor.display_name)).all()
    unlinked = [v for v in vendors if v.qbo_vendor_id not in linked_ids]
    # Payees that can still take a vendor: no id yet, not W-2.
    candidates = (CrewMember.query
                  .filter(CrewMember.qbo_vendor_id.is_(None),
                          db.or_(CrewMember.is_w2_employee.is_(None),
                                 CrewMember.is_w2_employee == False))    # noqa: E712
                  .order_by(func.lower(CrewMember.name)).all())
    by_name = {}
    for m in candidates:
        for raw in (m.name, m.legal_name, m.company):
            k = (raw or '').strip().lower()
            if k and m.id not in [x.id for x in by_name.setdefault(k, [])]:
                by_name[k].append(m)
    # Advisory suggestions: exact name match (one or several — a person
    # decides), shown as chips; never applied here.
    suggestions = {}
    for v in unlinked:
        keys = {(v.display_name or '').strip().lower(), (v.company_name or '').strip().lower()}
        keys.discard('')
        hits = {}
        for k in keys:
            for m in by_name.get(k, []):
                hits[m.id] = m
        if hits:
            suggestions[v.qbo_vendor_id] = [{"id": m.id, "name": m.name, "kind": m.kind}
                                            for m in hits.values()]
    # Charges each unlinked vendor has in the pool (why linking matters).
    charge_counts = {}
    try:
        for ref, n in (db.session.query(Transaction.qbo_entity_ref, func.count(Transaction.id))
                       .filter(Transaction.qbo_entity_ref.isnot(None))
                       .group_by(Transaction.qbo_entity_ref).all()):
            charge_counts[str(ref)] = int(n or 0)
    except Exception:
        pass
    return {
        "connected": bool(conn and conn.refresh_token),
        "last_sync": (conn.vendor_cdc_since.isoformat() if conn and conn.vendor_cdc_since else None),
        "last_sync_label": _fmt_dt(conn.vendor_cdc_since) if conn and conn.vendor_cdc_since else '',
        "total": len(vendors), "linked": len(vendors) - len(unlinked),
        "unlinked": [_vendor_dict(v, suggestions, charge_counts) for v in unlinked],
        "payees": [{"id": m.id, "name": m.name, "kind": m.kind or 'person',
                    "legal_name": m.legal_name, "company": m.company} for m in candidates],
    }


@app.route("/admin/qbo-vendors")
@login_required
@super_admin_required
def admin_qbo_vendors():
    return render_template("admin_qbo_vendors.html", d=_screen_data())


@app.route("/admin/qbo-vendors.json")
@login_required
@super_admin_required
def admin_qbo_vendors_json():
    return jsonify({"ok": True, **_screen_data()})


@app.route("/admin/qbo-vendors/sync", methods=["POST"])
@login_required
@super_admin_required
def admin_qbo_vendors_sync():
    """Run the vendor mirror now (QuickBooks → FPBudget only)."""
    conn = QBOConnection.query.first()
    if not conn or not conn.refresh_token:
        msg = "QuickBooks is not connected."
        if request.is_json or request.args.get('fmt') == 'json':
            return jsonify({"ok": False, "error": msg}), 400
        flash(msg, "error")
        return redirect(url_for('admin_qbo_vendors'))
    try:
        r = qbo_sync.sync_vendors(conn, db)
    except Exception as e:
        db.session.rollback()
        msg = f"Vendor sync failed: {e}"
        if request.is_json or request.args.get('fmt') == 'json':
            return jsonify({"ok": False, "error": msg}), 502
        flash(msg, "error")
        return redirect(url_for('admin_qbo_vendors'))
    _log_activity(action='sync', entity_type='qbo_vendors', entity_label='QuickBooks vendors',
                  after={k: v for k, v in r.items() if k != 'server_time'},
                  note=f"QuickBooks vendor mirror ({r['mode']}): {r['fetched']} fetched, "
                       f"{r['linked']} linked by name, {r['unlinked']} unlinked")
    if request.is_json or request.args.get('fmt') == 'json':
        return jsonify({"ok": True, **{k: v for k, v in r.items() if k != 'server_time'}})
    flash(f"Synced {r['fetched']} QuickBooks vendors — {r['linked']} linked by name, "
          f"{r['unlinked']} still unlinked.", "success")
    return redirect(url_for('admin_qbo_vendors'))


def _want_json():
    return request.is_json or request.args.get('fmt') == 'json'


def _fail(msg, code=400):
    if _want_json():
        return jsonify({"ok": False, "error": msg}), code
    flash(msg, "error")
    return redirect(url_for('admin_qbo_vendors'))


@app.route("/admin/qbo-vendors/<vid>/link", methods=["POST"])
@login_required
@super_admin_required
def admin_qbo_vendor_link(vid):
    """Link a QuickBooks vendor to an existing Payee. Mirrors the vendor's
    fields onto the Payee (QuickBooks owns them from here) and attributes
    every synced charge carrying that vendor's id."""
    v = QBOVendor.query.get_or_404(str(vid))
    src = request.get_json(silent=True) if request.is_json else request.form
    try:
        payee_id = int((src or {}).get("payee_id") or 0)
    except (TypeError, ValueError):
        payee_id = 0
    cm = CrewMember.query.get(payee_id) if payee_id else None
    if cm is None:
        return _fail("Pick a payee to link.")
    if cm.qbo_vendor_id:
        return _fail(f"{cm.name} is already linked to QuickBooks vendor {cm.qbo_vendor_id}.", 409)
    if cm.is_w2_employee:
        return _fail(f"{cm.name} is a W-2 employee (Wrapbook) and cannot be a QuickBooks vendor.", 409)
    if CrewMember.query.filter(CrewMember.qbo_vendor_id == v.qbo_vendor_id).first():
        return _fail("That QuickBooks vendor is already linked to another payee.", 409)
    f = {c.name: getattr(v, c.name) for c in QBOVendor.__table__.columns}
    cm.qbo_vendor_id = v.qbo_vendor_id
    cm.qbo_origin = cm.qbo_origin or 'qbo'
    changed = qbo_sync.apply_vendor_to_payee(cm, f, datetime.utcnow())
    n = attribute_txns_for_qbo_vendor(v.qbo_vendor_id, cm.id)
    db.session.commit()
    _log_activity(action='update', entity_type='payee', entity_id=cm.id, entity_label=cm.name,
                  field_changes={"qbo_vendor_id": [None, v.qbo_vendor_id]},
                  after={"mirrored": changed, "charges_attributed": n},
                  note=f"Linked to QuickBooks vendor “{v.display_name}” (#{v.qbo_vendor_id}); "
                       f"{n} charge{'s' if n != 1 else ''} attributed")
    if _want_json():
        return jsonify({"ok": True, "payee_id": cm.id, "vendor_id": v.qbo_vendor_id,
                        "mirrored": changed, "charges_attributed": n})
    flash(f"Linked {v.display_name} to {cm.name}" + (f" — {n} charges attributed." if n else "."), "success")
    return redirect(url_for('admin_qbo_vendors'))


@app.route("/admin/qbo-vendors/<vid>/create-payee", methods=["POST"])
@login_required
@super_admin_required
def admin_qbo_vendor_create_payee(vid):
    """Create a Payee (kind=vendor) from a QuickBooks vendor. Nothing is
    written to QuickBooks; the Payee is born linked with qbo_origin='qbo'."""
    v = QBOVendor.query.get_or_404(str(vid))
    if CrewMember.query.filter(CrewMember.qbo_vendor_id == v.qbo_vendor_id).first():
        return _fail("That QuickBooks vendor is already linked to a payee.", 409)
    name = (v.display_name or v.company_name or f"QuickBooks vendor {v.qbo_vendor_id}").strip()[:200]
    cm = CrewMember(name=name, kind='vendor', is_vendor=True, active=(v.active is not False),
                    qbo_vendor_id=v.qbo_vendor_id, qbo_origin='qbo',
                    company=(v.company_name or None))
    db.session.add(cm)
    db.session.flush()
    f = {c.name: getattr(v, c.name) for c in QBOVendor.__table__.columns}
    qbo_sync.apply_vendor_to_payee(cm, f, datetime.utcnow())
    n = attribute_txns_for_qbo_vendor(v.qbo_vendor_id, cm.id)
    db.session.commit()
    _log_activity(action='create', entity_type='payee', entity_id=cm.id, entity_label=cm.name,
                  after={"kind": "vendor", "qbo_vendor_id": v.qbo_vendor_id,
                         "qbo_origin": "qbo", "charges_attributed": n},
                  note=f"Payee created from QuickBooks vendor “{v.display_name}” (#{v.qbo_vendor_id})")
    if _want_json():
        return jsonify({"ok": True, "payee_id": cm.id, "vendor_id": v.qbo_vendor_id,
                        "charges_attributed": n, "url": url_for('payee_page', payee_id=cm.id)})
    flash(f"Created payee {cm.name} from QuickBooks" + (f" — {n} charges attributed." if n else "."), "success")
    return redirect(url_for('admin_qbo_vendors'))
