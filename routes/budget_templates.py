"""Budget-template routes — first slice of the app.py split (audit M1).

See routes/__init__.py for the pattern. Endpoint names (template_list,
template_new, budget_template_view, template_save, template_delete) and URLs
are byte-identical to the monolith version; only the file moved.
"""
from flask import render_template, request, jsonify, flash, redirect, url_for
from flask_login import login_required

from app import app, _require_global_editor
from models import db, BudgetTemplate, BudgetTemplateLine

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
    _require_global_editor()
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
    _require_global_editor()
    t = BudgetTemplate.query.get_or_404(tid)
    db.session.delete(t)
    db.session.commit()
    flash(f"Template '{t.name}' deleted.", "success")
    return redirect(url_for("template_list"))
