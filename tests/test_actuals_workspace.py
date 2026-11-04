"""
Actuals workspace E2E (ACTUALS_HANDOFF Phase 3, 2026-11-01):
/projects/<pid>/actuals — the two-pane screen that replaced /register and
the legacy #tab-actuals (both routes are gone since Cleanup B).

Each test module run creates ONE throwaway project through the UI, seeds a
few hand-typed charges through POST .../actuals/manual-expense (the only
charge-creating route that needs no QuickBooks or Dropbox), and deletes the
project at the end. Anything that needs a receipt in the inbox (link-doc)
skips with a reason when the inbox is empty, because receipts can only be
made by the OCR + Dropbox upload pipeline.

Needs the usual conftest env (BASE_URL, ADMIN_EMAIL, ADMIN_PASSWORD; the
ALLOW_PROD_TESTS guard applies). The admin test needs ADMIN_EMAIL to be a
super_admin.
"""
import re
import time

import pytest
from playwright.sync_api import Page, expect

from tests.conftest import BASE_URL, DEFAULT_TIMEOUT
from tests.utils.helpers import (
    create_project, delete_project, go_to_projects, take_step_screenshot,
)

PROJECT_NAME = f"ActualsWS-{int(time.time()) % 100000}"
VIEWS = ["unclaimed", "assigned", "needs_backup", "receipts", "not_project", "sync"]


def _fetch(page: Page, url: str, method: str = "GET", body=None, redirect: str = "follow"):
    """Call an app route FROM the page (same session cookie, the csrf.js shim
    adds X-CSRFToken). Returns {status, headers, json|text}. Playwright's
    page.request would not carry the Secure session cookie over http."""
    return page.evaluate(
        """async ([url, method, body, redirect]) => {
            const init = { method, credentials: 'same-origin', redirect };
            if (body !== null) { init.headers = { 'Content-Type': 'application/json' }; init.body = JSON.stringify(body); }
            const r = await fetch(url, init);
            const text = await r.text();
            let json = null; try { json = JSON.parse(text); } catch (e) {}
            const headers = {}; r.headers.forEach((v, k) => { headers[k] = v; });
            return { status: r.status, type: r.type, headers, json, text: text.slice(0, 4000) };
        }""", [url, method, body, redirect])


def _post_json(page: Page, url: str, body: dict):
    r = _fetch(page, url, "POST", body)
    assert 200 <= r["status"] < 300, f"{url} -> {r['status']}: {r['text'][:300]}"
    return r["json"]


def _seed_charges(page: Page, pid: int, n: int = 3):
    """n unassigned company-card charges with distinct vendors."""
    ids = []
    for i in range(n):
        d = _post_json(page, f"/projects/{pid}/actuals/manual-expense", {
            "vendor": f"WS Vendor {i + 1}", "amount": 100 + i, "txn_date": "2026-10-2%d" % (i + 1),
            "payment_method": "company_card", "note": "e2e seed"})
        ids.append(d["transaction"]["id"])
    return ids


@pytest.fixture(scope="module")
def ws(browser):
    """One project + workspace page for the module. Yields (page, pid)."""
    import os
    from tests.conftest import ADMIN_EMAIL, ADMIN_PASSWORD, _login
    context = browser.new_context(base_url=BASE_URL, viewport={"width": 1440, "height": 960})
    context.set_default_timeout(DEFAULT_TIMEOUT)
    page = context.new_page()
    _login(page, ADMIN_EMAIL, ADMIN_PASSWORD)
    # ACTUALS_WS_PID=<id> runs against an existing project (one with receipts
    # in its inbox exercises the link-doc test) and leaves it in place.
    existing = os.getenv("ACTUALS_WS_PID")
    if existing:
        pid = int(existing)
    else:
        create_project(page, PROJECT_NAME, template="FP Standard")
        m = re.search(r"/projects/(\d+)/", page.url)
        assert m, f"no project id in {page.url}"
        pid = int(m.group(1))
    _seed_charges(page, pid, 3)
    page.goto(f"{BASE_URL}/projects/{pid}/actuals", wait_until="networkidle")
    page.wait_for_selector("#aw-table .aw-row[data-tid]", timeout=DEFAULT_TIMEOUT)
    yield page, pid
    try:
        if not existing:
            go_to_projects(page)
            delete_project(page, PROJECT_NAME)
    finally:
        context.close()


def _open_view(page: Page, view: str):
    page.click(f'#aw-views [data-view="{view}"]')
    page.wait_for_timeout(300)


def _reload_ws(page: Page, pid: int, view: str = "unclaimed"):
    page.goto(f"{BASE_URL}/projects/{pid}/actuals?view={view}", wait_until="networkidle")
    page.wait_for_selector("#aw-table", timeout=DEFAULT_TIMEOUT)
    page.wait_for_timeout(400)


def test_page_loads_with_six_views(ws):
    page, pid = ws
    for v in VIEWS:
        expect(page.locator(f'#aw-views [data-view="{v}"]')).to_be_visible()
    expect(page.locator('#aw-views [data-view="unclaimed"]')).to_have_attribute("aria-current", "page")
    expect(page.locator("#aw-budget .aw-line[data-target='line']").first).to_be_visible()
    assert page.locator("#aw-table .aw-row[data-tid]").count() >= 3
    take_step_screenshot(page, "actuals-ws-loaded")


def test_drag_two_rows_to_line(ws):
    page, pid = ws
    _reload_ws(page, pid)
    rows = page.locator("#aw-table .aw-row[data-tid]")
    # Company ledger (2026-11-03): Unclaimed also lists the unclaimed company
    # pool, so count relative to what is there, not to this project's seed.
    before = rows.count()
    assert before >= 2
    rows.nth(0).click()
    rows.nth(1).click(modifiers=["Shift"])
    expect(page.locator("#aw-actionbar")).to_be_visible()
    expect(page.locator("#aw-selinfo")).to_contain_text("2 selected")
    target = page.locator("#aw-budget .aw-line[data-target='line']").first
    label = target.get_attribute("data-label")
    rows.nth(0).drag_to(target)
    page.wait_for_timeout(1200)
    # Both rows left Unclaimed and sit on that line in Assigned.
    expect(page.locator("#aw-table .aw-row[data-tid]")).to_have_count(before - 2)
    _open_view(page, "assigned")
    chips = page.locator("#aw-table .aw-row[data-tid] .aw-chip.solid")
    expect(chips).to_have_count(2)
    for i in range(2):
        expect(chips.nth(i)).to_contain_text(label.split(" · ")[0])
    take_step_screenshot(page, "actuals-ws-dragged")


def test_not_project_then_recover(ws):
    page, pid = ws
    _reload_ws(page, pid)
    rows = page.locator("#aw-table .aw-row[data-tid]")
    assert rows.count() >= 1
    vendor = rows.nth(0).locator(".vendor").inner_text()
    rows.nth(0).click()
    page.keyboard.press("n")
    page.wait_for_selector("#aw-reason[open]")
    page.fill("#aw-reason-text", "e2e: personal")
    page.click("#aw-reason-save")
    page.wait_for_timeout(1000)
    _open_view(page, "not_project")
    row = page.locator("#aw-table .aw-row[data-tid]", has_text=vendor)
    expect(row).to_have_count(1)
    expect(row).to_contain_text("e2e: personal")
    row.locator('[data-act="recover"]').click()
    page.wait_for_timeout(1000)
    _open_view(page, "unclaimed")
    expect(page.locator("#aw-table .aw-row[data-tid]", has_text=vendor)).to_have_count(1)


def test_split_must_sum(ws):
    page, pid = ws
    _reload_ws(page, pid)
    rows = page.locator("#aw-table .aw-row[data-tid]")
    assert rows.count() >= 1
    amount_txt = rows.nth(0).locator(".amt").inner_text().replace("$", "").replace(",", "")
    amount = float(amount_txt)
    rows.nth(0).click()
    page.click('#aw-actionbar [data-act="split"]')
    page.wait_for_selector("#aw-split[open]")
    # The split rows use the SAME searchable picker as Assign (2026-11-04).
    pick_ids = []
    for i, nth in ((0, 0), (1, 1)):
        page.locator("#aw-split-rows .fplp-in").nth(i).click()
        opt = page.locator("#aw-split-rows .fplp").nth(i).locator(".fplp-opt.line").nth(nth)
        pick_ids.append(int(opt.get_attribute("data-value")))
        opt.click()
    inputs = page.locator("#aw-split-rows input.num")
    inputs.nth(0).fill(f"{amount - 10:.2f}")
    inputs.nth(1).fill("5.00")
    expect(page.locator("#aw-split-save")).to_be_disabled()
    expect(page.locator("#aw-split-remain-amt")).to_have_text("$5.00")
    # The server refuses too, even if the button were forced.
    tid = int(rows.nth(0).get_attribute("data-tid"))
    r = _fetch(page, f"/projects/{pid}/actuals/transaction/{tid}/split", "POST",
               {"splits": [{"budget_line_id": pick_ids[0], "amount": amount - 10},
                           {"budget_line_id": pick_ids[1], "amount": 5}]})
    assert r["status"] == 400, r
    assert abs(float((r["json"] or {}).get("delta", 0))) == pytest.approx(5.0)
    # Fix the second amount: Save turns on and the split lands.
    inputs.nth(1).fill("10.00")
    expect(page.locator("#aw-split-save")).to_be_enabled()
    page.click("#aw-split-save")
    page.wait_for_timeout(1000)
    _open_view(page, "assigned")
    expect(page.locator("#aw-table .aw-row[data-tid] .aw-chip.solid", has_text="Split")).to_have_count(1)


def test_needs_backup_clears_after_link_doc(ws):
    page, pid = ws
    _reload_ws(page, pid, "receipts")
    receipts = page.locator("#aw-table .aw-row[data-doc]")
    if receipts.count() == 0:
        pytest.skip("No receipt in this project's inbox — receipts need the OCR + Dropbox "
                    "upload pipeline, which this test cannot drive. Seed one (upload a receipt "
                    "to the project) to exercise link-doc.")
    doc_id = int(receipts.nth(0).get_attribute("data-doc"))
    _open_view(page, "needs_backup")
    rows = page.locator("#aw-table .aw-row[data-tid]")
    if rows.count() == 0:
        pytest.skip("Nothing in Needs backup to link a receipt to.")
    tid = int(rows.nth(0).get_attribute("data-tid"))
    expect(rows.nth(0).locator(".aw-flag.red")).to_contain_text("Needs backup")
    rows.nth(0).click()
    _open_view(page, "receipts")
    page.locator(f'#aw-table .aw-row[data-doc="{doc_id}"] [data-act="link-selected"]').click()
    page.wait_for_timeout(1200)
    _open_view(page, "needs_backup")
    expect(page.locator(f'#aw-table .aw-row[data-tid="{tid}"]')).to_have_count(0)
    _open_view(page, "assigned")
    row = page.locator(f'#aw-table .aw-row[data-tid="{tid}"]')
    expect(row).to_have_count(1)
    # Red is gone; the chip is green (receipt) or yellow when the receipt's
    # amount or date differs from the charge.
    expect(row.locator(".aw-flag.red")).to_have_count(0)
    expect(row.locator(".aw-flag.green, .aw-flag.yellow")).to_have_count(1)


@pytest.mark.admin
def test_admin_pool_shows_claimed_row(ws):
    page, pid = ws
    r = _fetch(page, "/admin/transactions?q=WS+Vendor")
    if r["status"] == 403:
        pytest.skip("ADMIN_EMAIL is not a super_admin — /admin/transactions is super-admin only.")
    page.goto(f"{BASE_URL}/admin/transactions?q=WS+Vendor&project_id={pid}", wait_until="networkidle")
    name = page.locator(f'#adm-project option[value="{pid}"]').inner_text().strip()
    claimed = page.locator(".adm-row.link", has_text=name)
    assert claimed.count() >= 1, "expected at least one row claimed by the test project"
    expect(claimed.first.locator("a", has_text=name)).to_be_visible()
    # Export carries the same filter.
    csv = _fetch(page, f"/admin/transactions.csv?q=WS+Vendor&project_id={pid}")
    assert csv["status"] == 200 and csv["headers"].get("content-type", "").startswith("text/csv")
    assert name in csv["text"]
    # A row click lands in the workspace with that charge focused.
    claimed.first.click()
    page.wait_for_url(re.compile(rf"/projects/{pid}/actuals"))
    # ?txn= selects and scrolls to that charge (the page then rewrites the
    # query string to the view it landed in).
    page.wait_for_selector("#aw-table .aw-row[aria-selected='true']", timeout=DEFAULT_TIMEOUT)
    expect(page.locator("#aw-table .aw-row[aria-selected='true']")).to_have_count(1)


# ═══ Phase 5 — matching, receipt linking, duplicates (2026-11-03) ═════════
# The duplicates tests run on the module's throwaway project (two
# hand-typed twins). The receipt tests need receipts, which only the OCR
# pipeline or scripts/seed_local.py can make, so they run on the SEEDED
# project ("GrubHub Fee Breakdown": a Lyft receipt with its charge, a
# Parking charge with its receipt, Sweetgreen suggested) and skip when it
# is not there.

SEEDED_PROJECT = "GrubHub Fee Breakdown"


def _seeded_pid(page: Page):
    go_to_projects(page)
    card = page.locator(f".project-card[data-name='{SEEDED_PROJECT.lower()}']")
    if card.count() == 0:
        return None
    return int(card.first.get_attribute("data-id"))


@pytest.fixture(scope="module")
def seeded(ws):
    page, _pid = ws
    pid = _seeded_pid(page)
    if pid is None:
        pytest.skip(f"The seeded project '{SEEDED_PROJECT}' (scripts/seed_local.py) is not on this app.")
    _reload_ws(page, pid)
    return page, pid


def test_duplicates_view_keep_both_and_delete_the_extra(ws):
    page, pid = ws
    # Two pairs of hand-typed twins (same vendor, amount and day).
    for v in ("WS Twin A", "WS Twin B"):
        for _ in range(2):
            _post_json(page, f"/projects/{pid}/actuals/manual-expense", {
                "vendor": v, "amount": 77.70, "txn_date": "2026-10-25",
                "payment_method": "company_card", "note": "e2e twin"})
    _reload_ws(page, pid, "duplicates")
    groups = page.locator("#aw-table .aw-dgrp[data-key]")
    assert groups.count() >= 2
    ga = page.locator("#aw-table .aw-dgrp[data-key]", has_text="WS Twin A")
    expect(ga).to_have_count(1)
    expect(ga).to_contain_text("keep the one first imported")
    key_a = ga.get_attribute("data-key")
    expect(page.locator(f"#aw-table .aw-drow.keeper", has_text="WS Twin A")).to_have_count(1)
    take_step_screenshot(page, "actuals-ws-duplicates")
    # Keep both: the group leaves and stays gone after a reload (remembered).
    ga.locator('[data-act="dup-keep-both"]').click()
    page.wait_for_timeout(1000)
    expect(page.locator("#aw-table .aw-dgrp[data-key]", has_text="WS Twin A")).to_have_count(0)
    _reload_ws(page, pid, "duplicates")
    expect(page.locator(f'#aw-table .aw-dgrp[data-key="{key_a}"]')).to_have_count(0)
    # Delete the extra on the other pair: the confirm dialog shows both rows.
    gb = page.locator("#aw-table .aw-dgrp[data-key]", has_text="WS Twin B")
    expect(gb).to_have_count(1)
    extra_row = page.locator("#aw-table .aw-drow:not(.keeper)", has_text="WS Twin B")
    extra_tid = int(extra_row.get_attribute("data-tid"))
    extra_row.locator('[data-act="dup-delete"]').click()
    page.wait_for_selector("#aw-dupdel[open]")
    expect(page.locator("#aw-dupdel-rows .r")).to_have_count(2)
    expect(page.locator("#aw-dupdel-rows .r.keep")).to_contain_text("WS Twin B")
    take_step_screenshot(page, "actuals-ws-delete-extra")
    page.click("#aw-dupdel-go")
    page.wait_for_timeout(1200)
    expect(page.locator("#aw-table .aw-dgrp[data-key]", has_text="WS Twin B")).to_have_count(0)
    _open_view(page, "unclaimed")
    expect(page.locator("#aw-table .aw-row[data-tid]", has_text="WS Twin B")).to_have_count(1)
    expect(page.locator(f'#aw-table .aw-row[data-tid="{extra_tid}"]')).to_have_count(0)
    # The twin rule is enforced server-side: a non-twin cannot be folded.
    other = int(page.locator("#aw-table .aw-row[data-tid]", has_text="WS Twin A").first.get_attribute("data-tid"))
    keeper_b = int(page.locator("#aw-table .aw-row[data-tid]", has_text="WS Twin B").first.get_attribute("data-tid"))
    r = _fetch(page, f"/projects/{pid}/actuals/transaction/{other}/merge-duplicate", "POST", {"keeper_id": keeper_b})
    assert r["status"] == 409, r


def test_receipt_candidate_panel_then_link(seeded):
    page, pid = seeded
    _reload_ws(page, pid, "receipts")
    row = page.locator("#aw-table .aw-row[data-doc]", has_text="Lyft")
    if row.count() == 0:
        pytest.skip("The seeded Lyft receipt is already linked on this app.")
    expect(row.locator('[data-act="link-selected"]')).to_be_disabled()
    expect(row.locator('[data-act="link-selected"]')).to_have_attribute("title", "Select a receipt and a charge")
    row.locator(".vendor").click()
    page.wait_for_selector("#aw-panel:not([hidden]) .aw-cand")
    expect(page.locator(".aw-right")).to_have_class(re.compile(r"panel-open"))
    cand = page.locator('#aw-panel .aw-cand[data-kind="charge"]').first
    expect(cand).to_contain_text("LYFT")
    expect(cand).to_contain_text("strong")
    take_step_screenshot(page, "actuals-ws-receipt-panel")
    cand.click()                                   # tick the charge
    link = row.locator('[data-act="link-selected"]')
    expect(link).to_be_enabled()
    expect(link).to_have_attribute("title", re.compile(r"^Link to LYFT"))
    link.click()
    page.wait_for_timeout(1200)
    expect(page.locator("#aw-table .aw-row[data-doc]", has_text="Lyft")).to_have_count(0)
    expect(page.locator("#aw-panel")).to_be_hidden()
    _open_view(page, "unclaimed")
    charge = page.locator("#aw-table .aw-row[data-tid]", has_text="LYFT *RIDE")
    expect(charge).to_have_count(1)
    expect(charge).to_have_attribute("data-has-doc", "1")


def test_strong_suggestion_approve_from_the_charge_row(seeded):
    page, pid = seeded
    _reload_ws(page, pid, "unclaimed")
    row = page.locator("#aw-table .aw-row[data-tid]", has_text="Seaport garage")
    if row.count() == 0 or row.locator('[data-act="confirm-match"]').count() == 0:
        pytest.skip("The seeded Parking charge has no receipt suggestion on this app (already approved?).")
    expect(row.locator(".aw-chip.info")).to_contain_text(re.compile(r"Receipt \d+%"))
    row.locator('[data-act="confirm-match"]').click()
    page.wait_for_timeout(1200)
    row = page.locator("#aw-table .aw-row[data-tid]", has_text="Seaport garage")
    expect(row).to_have_attribute("data-has-doc", "1")
    expect(row.locator('[data-act="confirm-match"]')).to_have_count(0)
    _open_view(page, "receipts")
    expect(page.locator("#aw-table .aw-row[data-doc]", has_text="Seaport Garage")).to_have_count(0)


def test_review_matches_approve_all_strong(seeded):
    page, pid = seeded
    _reload_ws(page, pid, "unclaimed")
    chip = page.locator("#aw-reviewchip")
    if chip.is_hidden():
        pytest.skip("No suggested matches left to review on the seeded project.")
    chip.click()
    page.wait_for_selector("#aw-review[open]")
    pairs = page.locator("#aw-review .aw-rpair")
    assert pairs.count() >= 1
    expect(pairs.first.locator(".why")).to_contain_text("%")
    take_step_screenshot(page, "actuals-ws-review-matches")
    strong = page.locator("#aw-review .aw-rpair.strong").count()
    btn = page.locator("#aw-review-allstrong")
    if strong == 0:
        expect(btn).to_be_disabled()
        pytest.skip("No strong pair to approve in bulk.")
    expect(btn).to_contain_text(f"({strong})")
    btn.click()
    page.wait_for_timeout(1500)
    expect(page.locator("#aw-review .aw-rpair.strong")).to_have_count(0)
    page.keyboard.press("Escape")
    r = _fetch(page, f"/projects/{pid}/actuals/workspace.json")
    assert r["json"]["counts"]["strong_matches"] == 0


# ── The detail window, the shared picker, section-coded spend (2026-11-04) ──

def test_detail_window_shows_the_chain_for_a_charge_split_by_its_invoice(seeded):
    page, pid = seeded
    _reload_ws(page, pid, "assigned")
    row = page.locator("#aw-table .aw-row[data-tid]", has_text="Gotham Sound")
    if row.count() == 0:
        pytest.skip("The seeded Gotham Sound charge is not on this app.")
    row.first.hover()
    row.first.locator('[data-act="detail"]').first.click()
    page.wait_for_selector("#fpd:not([hidden]) .fpd-chain")
    det = page.locator("#fpd")
    expect(det.locator(".fpd-src")).to_have_text("QuickBooks")
    expect(det.locator(".fpd-lock")).to_contain_text("the bank's figure")
    expect(det.locator(".fpd-doc")).to_have_count(1)
    expect(det.locator(".fpd-chain")).to_have_count(2)
    expect(det.locator(".fpd-chain").nth(0)).to_contain_text("$150.00")
    expect(det.locator(".fpd-chain .fpd-node.doc")).to_have_count(2)       # one document backs both pieces
    # edit the split in place with the shared picker, section option included
    det.locator('[data-fpd="split-edit"]').click()
    expect(det.locator(".fpd-srow")).to_have_count(2)
    det.locator(".fpd-srow .fplp-in").nth(1).click()
    det.locator(".fpd-srow .fplp-in").nth(1).fill("transport")
    expect(det.locator(".fpd-srow .fplp").nth(1).locator(".fplp-opt.section")).to_contain_text("All 3400")
    det.locator(".fpd-srow .fplp").nth(1).locator(".fplp-opt.section").first.click()
    det.locator('[data-fpd="split-save"]').click()
    page.wait_for_timeout(1200)
    expect(det.locator(".fpd-chain .fpd-node.sec")).to_contain_text("3400")
    expect(det.locator(".fpd-hist li").first).to_contain_text("Split")
    # undo the latest entry from History
    det.locator('.fpd-hist [data-fpd="undo"]').first.click()
    page.wait_for_timeout(1200)
    expect(det.locator(".fpd-chain .fpd-node.sec")).to_have_count(0)
    page.keyboard.press("Escape")
    expect(page.locator("#fpd")).to_be_hidden()


def test_section_coded_charge_shows_under_its_section_in_the_budget_pane(seeded):
    page, pid = seeded
    _reload_ws(page, pid, "assigned")
    sc = page.locator('#aw-budget .aw-seccoded[data-seccoded="4700"]')
    if sc.count() == 0:
        pytest.skip("The seeded Rare Medium section-coded charge is not on this app.")
    expect(sc).to_contain_text("Section-coded")
    expect(sc).to_contain_text("1,650")
    expect(sc).to_contain_text("(1)")
    sc.click()
    row = page.locator('#aw-budget .aw-seccoded-row', has_text="Rare Medium")
    expect(row).to_have_count(1)
    row.click()
    page.wait_for_selector("#fpd:not([hidden]) .fpd-chain")
    expect(page.locator("#fpd .fpd-node.sec")).to_contain_text("4700")
    page.keyboard.press("Escape")


def test_assign_picker_searches_and_offers_a_whole_section(ws):
    page, pid = ws
    _reload_ws(page, pid)
    rows = page.locator("#aw-table .aw-row[data-tid]")
    rows.nth(0).click()
    page.keyboard.press("a")
    page.wait_for_selector("#aw-picker[open]")
    page.fill("#aw-picker-q", "post-production services")
    expect(page.locator("#aw-picker-list .aw-popt.section").first).to_contain_text("All 4700")
    expect(page.locator("#aw-picker-list .aw-popt.new")).to_have_count(1)
    page.keyboard.press("Escape")


# ── Select all + quick date chips (owner reports, 2026-11-04) ─────────────

def _header_box(page: Page):
    return page.evaluate("(() => { const b = document.getElementById('aw-selall'); return { checked: b.checked, indeterminate: b.indeterminate, disabled: b.disabled }; })()")


def test_select_all_toggles_off_and_reflects_the_rows(ws):
    page, pid = ws
    _reload_ws(page, pid)
    rows = page.locator("#aw-table .aw-row[data-tid]")
    n = rows.count()
    assert n >= 2
    ticked = page.locator("#aw-table .aw-row[data-tid] input[data-sel]:checked")
    box = page.locator("#aw-selall")
    assert _header_box(page) == {"checked": False, "indeterminate": False, "disabled": False}
    # Check: every visible row is selected and the bar counts them.
    box.click()
    expect(ticked).to_have_count(n)
    expect(page.locator("#aw-selinfo")).to_contain_text(f"{n} selected")
    assert _header_box(page)["checked"] is True
    # Check again: everything is deselected — rows, bar and the header box.
    box.click()
    expect(ticked).to_have_count(0)
    expect(page.locator("#aw-actionbar")).to_be_hidden()
    expect(page.locator("#aw-table .aw-row[aria-selected='true']")).to_have_count(0)
    assert _header_box(page) == {"checked": False, "indeterminate": False, "disabled": False}
    # Select all, untick one row: indeterminate, and the count follows.
    box.click()
    rows.nth(0).locator("input[data-sel]").click()
    expect(page.locator("#aw-selinfo")).to_contain_text(f"{n - 1} selected")
    assert _header_box(page) == {"checked": False, "indeterminate": True, "disabled": False}
    # Tick it back by hand: the header box fills in on its own.
    rows.nth(0).locator("input[data-sel]").click()
    assert _header_box(page)["checked"] is True
    # Clear from the bar: rows and header box follow.
    page.locator('#aw-actionbar [data-act="clear"]').click()
    expect(ticked).to_have_count(0)
    assert _header_box(page) == {"checked": False, "indeterminate": False, "disabled": False}
    # A filter that empties the list disables the header box.
    page.fill("#aw-q", "zzz-nothing-matches-this")
    page.wait_for_timeout(300)
    expect(page.locator("#aw-table .aw-empty")).to_be_visible()
    page.fill("#aw-q", "")
    page.wait_for_timeout(300)
    expect(page.locator("#aw-table .aw-row[data-tid]")).to_have_count(n)
    take_step_screenshot(page, "actuals-ws-select-all")


def test_quick_date_chips_fill_the_inputs_and_filter(ws):
    page, pid = ws
    bid = _fetch(page, f"/projects/{pid}/actuals/workspace.json")["json"]["budget_id"]
    assert bid
    # Two charges a year apart from the seed, so a project window keeps one.
    for d in ("2025-03-10", "2025-03-20"):
        _post_json(page, f"/projects/{pid}/actuals/manual-expense", {
            "vendor": "QF Vendor", "amount": 42, "txn_date": d, "payment_method": "company_card", "note": "e2e quick"})
    # No project dates: the chip is disabled and points at Settings.
    _post_json(page, f"/projects/{pid}/budget/{bid}/settings", {"start_date": "", "end_date": ""})
    _reload_ws(page, pid)
    proj = page.locator('#aw-quick [data-quick="project"]')
    expect(proj).to_be_disabled()
    expect(proj).to_have_attribute("title", "Set the project dates in Settings")
    expect(page.locator("#aw-quick-set")).to_be_visible()
    assert "tab=settings" in page.locator("#aw-quick-set").get_attribute("href")
    expect(page.locator('#aw-quick [data-quick="all"]')).to_have_attribute("aria-pressed", "true")
    # This month fills BOTH inputs with the page's own month range and lights up.
    page.click('#aw-quick [data-quick="month"]')
    want = page.evaluate("AW.quickRange('month')")
    expect(page.locator("#aw-from")).to_have_value(want["from"])
    expect(page.locator("#aw-to")).to_have_value(want["to"])
    expect(page.locator('#aw-quick [data-quick="month"]')).to_have_attribute("aria-pressed", "true")
    expect(page.locator("#aw-table .aw-row[data-tid]", has_text="QF Vendor")).to_have_count(0)
    # Set the project dates on the Settings route the Settings tab saves through.
    _post_json(page, f"/projects/{pid}/budget/{bid}/settings", {"start_date": "2025-03-15", "end_date": "2025-03-31"})
    _reload_ws(page, pid)
    proj = page.locator('#aw-quick [data-quick="project"]')
    expect(proj).to_be_enabled()
    expect(page.locator("#aw-quick-set")).to_be_hidden()
    proj.click()
    expect(page.locator("#aw-from")).to_have_value("2025-03-15")
    expect(page.locator("#aw-to")).to_have_value("2025-03-31")
    expect(proj).to_have_attribute("aria-pressed", "true")
    qf = page.locator("#aw-table .aw-row[data-tid]", has_text="QF Vendor")
    expect(qf).to_have_count(1)
    expect(qf).to_contain_text("Mar 20")
    take_step_screenshot(page, "actuals-ws-quick-dates")
    # Remembered per project per person: a reload re-applies it.
    _reload_ws(page, pid)
    expect(page.locator("#aw-from")).to_have_value("2025-03-15")
    expect(page.locator('#aw-quick [data-quick="project"]')).to_have_attribute("aria-pressed", "true")
    expect(page.locator("#aw-table .aw-row[data-tid]", has_text="QF Vendor")).to_have_count(1)
    # A hand edit un-lights the chip and widens the range.
    page.fill("#aw-from", "2025-03-01")
    page.wait_for_timeout(300)
    expect(page.locator('#aw-quick [aria-pressed="true"]')).to_have_count(0)
    expect(page.locator("#aw-table .aw-row[data-tid]", has_text="QF Vendor")).to_have_count(2)
    # All clears both inputs and is remembered as the default again.
    page.click('#aw-quick [data-quick="all"]')
    expect(page.locator("#aw-from")).to_have_value("")
    expect(page.locator("#aw-to")).to_have_value("")
    expect(page.locator('#aw-quick [data-quick="all"]')).to_have_attribute("aria-pressed", "true")


def test_resizable_divider_persists_its_width(seeded):
    page, pid = seeded
    _reload_ws(page, pid)
    div = page.locator("#aw-divider")
    if not div.is_visible():
        pytest.skip("Narrow viewport: the panes stack, no divider.")
    box = div.bounding_box()
    before = page.locator(".aw-right").bounding_box()["width"]
    page.mouse.move(box["x"] + 3, box["y"] + 200)
    page.mouse.down()
    page.mouse.move(box["x"] - 120, box["y"] + 200, steps=6)
    page.mouse.up()
    after = page.locator(".aw-right").bounding_box()["width"]
    assert after > before + 80
    _reload_ws(page, pid)
    assert abs(page.locator(".aw-right").bounding_box()["width"] - after) < 4
    page.locator("#aw-divider").dblclick()
    assert abs(page.locator(".aw-right").bounding_box()["width"] - before) < 4


# ═══ Compare duplicates in place + loan-outs (2026-11-04) ═════════════════

def _dup_count(page: Page) -> int:
    return int(page.locator('#aw-views [data-count="duplicates"]').inner_text().strip() or 0)


def test_compare_from_the_unclaimed_chip_then_keep_one(ws):
    """Unclaimed → the yellow Possible duplicate chip opens Compare in place;
    keep one column (← / → then Enter, Enter) and the twin disappears, the
    chip goes from the kept row and the Duplicates count drops — no reload."""
    page, pid = ws
    for note in ("first entry", "second entry"):
        _post_json(page, f"/projects/{pid}/actuals/manual-expense", {
            "vendor": "WS Twin C", "amount": 91.15, "txn_date": "2026-10-26",
            "payment_method": "company_card", "note": note})
    _reload_ws(page, pid, "unclaimed")
    rows = page.locator("#aw-table .aw-row[data-tid]", has_text="WS Twin C")
    expect(rows).to_have_count(2)
    before = _dup_count(page)
    chip = rows.first.locator('.flagcell > [data-act="open-dup"]')
    expect(chip).to_have_text("Possible duplicate")
    # hovering hides the chip under the row actions, which offer Compare
    rows.first.hover()
    rows.first.locator('.aw-rowacts [data-act="open-dup"]').click()
    panel = page.locator("#fpc")
    expect(panel).to_be_visible()
    expect(panel.locator(".fpc-h")).to_have_count(2)
    expect(panel.locator(".fpc-h.keeper")).to_have_count(1)
    expect(panel.locator(".fpc-lab.diff", has_text="Memo")).to_have_count(1)
    take_step_screenshot(page, "actuals-compare-panel")
    # keyboard: → moves the column focus; Enter asks, Enter confirms
    page.keyboard.press("ArrowRight")
    focused = int(panel.locator(".fpc-h.focus").get_attribute("data-col"))
    page.keyboard.press("Enter")
    expect(panel.locator(".fpc-confirm")).to_have_count(1)
    page.keyboard.press("Enter")
    expect(panel).to_be_hidden()
    page.wait_for_timeout(800)
    rows = page.locator("#aw-table .aw-row[data-tid]", has_text="WS Twin C")
    expect(rows).to_have_count(1)
    expect(rows.first.locator('[data-act="open-dup"]')).to_have_count(0)
    assert _dup_count(page) == before - 1
    expect(page.locator("#aw-toasts .aw-toast", has_text="duplicate")).to_have_count(1)
    # the Duplicates tab no longer lists the group
    _open_view(page, "duplicates")
    expect(page.locator("#aw-table .aw-dgrp[data-key]", has_text="WS Twin C")).to_have_count(0)
    assert focused in (0, 1)


def test_compare_keep_both_from_the_duplicates_view(ws):
    page, pid = ws
    for _ in range(2):
        _post_json(page, f"/projects/{pid}/actuals/manual-expense", {
            "vendor": "WS Twin D", "amount": 12.34, "txn_date": "2026-10-27",
            "payment_method": "company_card", "note": "e2e twin"})
    _reload_ws(page, pid, "duplicates")
    g = page.locator("#aw-table .aw-dgrp[data-key]", has_text="WS Twin D")
    expect(g).to_have_count(1)
    g.locator('[data-act="dup-compare"]').click()
    expect(page.locator("#fpc")).to_be_visible()
    page.once("dialog", lambda d: d.accept())
    page.locator('#fpc [data-fpc="keep-both"]').click()
    expect(page.locator("#fpc")).to_be_hidden()
    page.wait_for_timeout(600)
    expect(page.locator("#aw-table .aw-dgrp[data-key]", has_text="WS Twin D")).to_have_count(0)
    _reload_ws(page, pid, "unclaimed")
    expect(page.locator("#aw-table .aw-row[data-tid]", has_text="WS Twin D").locator('[data-act="open-dup"]')).to_have_count(0)


def test_same_person_links_the_loan_out_from_the_detail_window(seeded):
    """Danielle Miller is Two Foot Shuffle LLC: the detail window's payee-
    differs box offers Same person; after it the header reads
    "Danielle Miller · via Two Foot Shuffle LLC"."""
    page, pid = seeded
    _reload_ws(page, pid, "unclaimed")
    page.wait_for_selector("#aw-table .aw-row[data-tid]")
    row = page.locator("#aw-table .aw-row[data-tid]", has_text="Two Foot Shuffle")
    if row.count() == 0:
        pytest.skip("The seeded Two Foot Shuffle LLC payment is not on this app.")
    row.first.hover()
    row.first.locator('[data-act="detail"]').click()
    det = page.locator("#fpd")
    expect(det.locator("#fpd-payee-differs")).to_be_visible()
    btn = det.locator('[data-fpd="same-person"]')
    if btn.count() == 0:
        pytest.skip("Already settled on this app.")
    expect(btn).to_contain_text("Two Foot Shuffle LLC is Danielle Miller's loan-out")
    page.once("dialog", lambda d: d.accept())
    btn.click()
    expect(det.locator("#fpd-payee-differs")).to_have_count(0)
    expect(det.locator(".fpd-kv")).to_contain_text("Danielle Miller · via Two Foot Shuffle LLC")
    page.keyboard.press("Escape")
    _reload_ws(page, pid)
    expect(page.locator("#aw-table .aw-row[data-tid]", has_text="Two Foot Shuffle").first).to_contain_text(
        "Danielle Miller · via Two Foot Shuffle LLC")
