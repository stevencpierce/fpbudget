"""
Collaboration tests: a project shared with the non-admin test user, two
browsers on the same budget, Socket.IO presence (collab.js → join_budget →
presence_update → #collab-viewers avatars), and the python-socketio client.

Needs TEST_EMAIL / TEST_PASSWORD (the seeded app has local-editor@thefp.tv).
"""

import re
import time

import pytest
from playwright.sync_api import Page, expect

from tests.conftest import BASE_URL, TEST_EMAIL
from tests.utils.helpers import (
    create_project, delete_project, find_project_card, go_to_projects,
    take_step_screenshot,
)
from tests.utils.socket_client import BudgetSocketClient

pytestmark = pytest.mark.collab

PROJECT_NAME = f"CollabTest-{int(time.time()) % 100000}"


@pytest.fixture
def shared_budget(collab_pages: tuple[Page, Page]):
    """Admin creates a project, shares it with the test user, both open it.
    Yields (admin_page, test_user_page, pid)."""
    admin_page, test_page = collab_pages
    create_project(admin_page, PROJECT_NAME, template="FP Standard")
    m = re.search(r"/projects/(\d+)/budget", admin_page.url)
    assert m, admin_page.url
    pid = int(m.group(1))

    go_to_projects(admin_page)
    card = find_project_card(admin_page, PROJECT_NAME)
    card.locator("text=Share").first.click()
    expect(admin_page.locator("#share-modal")).to_be_visible()
    admin_page.fill("#share-email-input", TEST_EMAIL)
    admin_page.click('#share-modal button:has-text("Add")')
    expect(admin_page.locator("#share-collaborators")).to_contain_text(TEST_EMAIL, timeout=10_000)
    admin_page.click('#share-modal button:has-text("Close")')

    for page in (admin_page, test_page):
        page.goto(f"{BASE_URL}/projects/{pid}/budget", wait_until="networkidle")
        expect(page.locator("#budget-name-heading")).to_be_visible(timeout=15_000)

    yield admin_page, test_page, pid

    go_to_projects(admin_page)
    delete_project(admin_page, PROJECT_NAME)


def test_both_users_see_budget(shared_budget):
    admin_page, test_page, pid = shared_budget
    for page in (admin_page, test_page):
        expect(page).to_have_url(re.compile(rf"/projects/{pid}/budget"))
        expect(page.locator("#budget-name-heading")).to_contain_text(PROJECT_NAME)
    take_step_screenshot(test_page, "collab-test-user-view")


def test_presence_shows_both_viewers(shared_budget):
    admin_page, test_page, pid = shared_budget
    # Each page joined the budget room on load; presence_update rebuilds the
    # avatar strip (OTHER viewers only), so each page shows one avatar — the
    # other user.
    for page in (admin_page, test_page):
        expect(page.locator("#collab-viewers .collab-avatar")).to_have_count(1, timeout=15_000)
    admin_sees = admin_page.locator("#collab-viewers .collab-avatar").first.get_attribute("title") or ""
    user_sees = test_page.locator("#collab-viewers .collab-avatar").first.get_attribute("title") or ""
    assert admin_sees.endswith("is viewing") and user_sees.endswith("is viewing"), (admin_sees, user_sees)
    assert admin_sees != user_sees, "each page should show the OTHER viewer"
    take_step_screenshot(admin_page, "collab-presence")


def test_socket_client_connects_and_joins(admin_page: Page):
    """The python-socketio client, with the browser's session cookie."""
    cookie = BudgetSocketClient.extract_session_cookie(admin_page)
    client = BudgetSocketClient(BASE_URL, session_cookie=cookie)
    try:
        client.connect(timeout=15.0)
        assert client.sio.connected, "Socket client failed to connect"
        client.sio.emit("join_budget", {"budget_id": 1, "user_id": 1, "user_name": "e2e"})
        evt = client.wait_for_event("presence_update", timeout=10.0)
        assert evt is not None, f"no presence_update after join_budget; got {client.get_events()}"
        assert any(v.get("user_name") == "e2e" for v in evt["data"].get("viewers", [])), evt
    finally:
        client.disconnect()
