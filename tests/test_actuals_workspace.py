"""
Actuals workspace E2E (ACTUALS_HANDOFF Phase 3, 2026-11-01):
/projects/<pid>/actuals — the two-pane screen that replaced /register and
the legacy #tab-actuals.

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

import allure
import pytest
from playwright.sync_api import Page, expect

from tests.conftest import BASE_URL, DEFAULT_TIMEOUT
from tests.utils.helpers import (
    create_project, delete_project, go_to_projects, take_step_screenshot,
)

pytestmark = allure.suite("Actuals workspace")

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


@allure.title("Workspace loads with the six views and the budget pane")
def test_page_loads_with_six_views(ws):
    page, pid = ws
    for v in VIEWS:
        expect(page.locator(f'#aw-views [data-view="{v}"]')).to_be_visible()
    expect(page.locator('#aw-views [data-view="unclaimed"]')).to_have_attribute("aria-current", "page")
    expect(page.locator("#aw-budget .aw-line[data-target='line']").first).to_be_visible()
    assert page.locator("#aw-table .aw-row[data-tid]").count() >= 3
    take_step_screenshot(page, "actuals-ws-loaded")


@allure.title("/register redirects to the workspace")
def test_register_redirects(ws):
    page, pid = ws
    r = _fetch(page, f"/projects/{pid}/register", "GET", None, "manual")
    # fetch() with redirect:'manual' yields an opaque redirect (status 0);
    # either way, following it must land on the workspace.
    assert r["status"] in (0, 302), r
    page.goto(f"{BASE_URL}/projects/{pid}/register", wait_until="networkidle")
    assert re.search(rf"/projects/{pid}/actuals$", page.url), page.url
    _reload_ws(page, pid)


@allure.title("Select two rows and drag them onto a line: both assigned")
def test_drag_two_rows_to_line(ws):
    page, pid = ws
    _reload_ws(page, pid)
    rows = page.locator("#aw-table .aw-row[data-tid]")
    assert rows.count() >= 2
    rows.nth(0).click()
    rows.nth(1).click(modifiers=["Shift"])
    expect(page.locator("#aw-actionbar")).to_be_visible()
    expect(page.locator("#aw-selinfo")).to_contain_text("2 selected")
    target = page.locator("#aw-budget .aw-line[data-target='line']").first
    label = target.get_attribute("data-label")
    rows.nth(0).drag_to(target)
    page.wait_for_timeout(1200)
    # Both rows left Unclaimed and sit on that line in Assigned.
    expect(page.locator("#aw-table .aw-row[data-tid]")).to_have_count(1)
    _open_view(page, "assigned")
    chips = page.locator("#aw-table .aw-row[data-tid] .aw-chip.solid")
    expect(chips).to_have_count(2)
    for i in range(2):
        expect(chips.nth(i)).to_contain_text(label.split(" · ")[0])
    take_step_screenshot(page, "actuals-ws-dragged")


@allure.title("Mark not project, see it under Not project, Recover it back to Unclaimed")
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


@allure.title("A split that does not sum is refused and the remainder shows")
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
    sel = page.locator("#aw-split-rows select")
    sel.nth(0).select_option(index=1)
    sel.nth(1).select_option(index=2)
    inputs = page.locator("#aw-split-rows input")
    inputs.nth(0).fill(f"{amount - 10:.2f}")
    inputs.nth(1).fill("5.00")
    expect(page.locator("#aw-split-save")).to_be_disabled()
    expect(page.locator("#aw-split-remain-amt")).to_have_text("$5.00")
    # The server refuses too, even if the button were forced.
    tid = int(rows.nth(0).get_attribute("data-tid"))
    r = _fetch(page, f"/projects/{pid}/actuals/transaction/{tid}/split", "POST",
               {"splits": [{"budget_line_id": int(sel.nth(0).input_value()), "amount": amount - 10},
                           {"budget_line_id": int(sel.nth(1).input_value()), "amount": 5}]})
    assert r["status"] == 400, r
    assert abs(float((r["json"] or {}).get("delta", 0))) == pytest.approx(5.0)
    # Fix the second amount: Save turns on and the split lands.
    inputs.nth(1).fill("10.00")
    expect(page.locator("#aw-split-save")).to_be_enabled()
    page.click("#aw-split-save")
    page.wait_for_timeout(1000)
    _open_view(page, "assigned")
    expect(page.locator("#aw-table .aw-row[data-tid] .aw-chip.solid", has_text="Split")).to_have_count(1)


@allure.title("Needs-backup flag clears after link-doc")
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


@allure.title("Admin pool shows a claimed row with the project name")
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
