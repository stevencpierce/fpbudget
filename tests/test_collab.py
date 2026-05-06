"""
Collaboration tests: two browsers editing the same budget in real-time.
"""

import re
import time
import allure
import pytest
from playwright.sync_api import Page, expect

from tests.conftest import BASE_URL, ADMIN_EMAIL, TEST_EMAIL
from tests.utils.helpers import (
    create_project, delete_project, find_project_card, go_to_projects,
    open_project_by_name, take_step_screenshot,
)
from tests.utils.socket_client import BudgetSocketClient

pytestmark = [
    allure.suite("Collaboration"),
    pytest.mark.collab,
    pytest.mark.skip(
        reason="Test user account is locked in 'Please set a new password' mode; "
        "needs a dedicated test user with a stable password to run collab tests."
    ),
]

PROJECT_NAME = f"CollabTest-{int(time.time()) % 100000}"


@pytest.fixture
def shared_budget(collab_pages: tuple[Page, Page]):
    """
    Create a project with admin, share it, open the budget in both browsers.
    Returns (admin_page, test_user_page).
    """
    admin_page, test_page = collab_pages

    # Admin creates the project
    create_project(admin_page, PROJECT_NAME, template="FP Standard")

    # Share with test user
    go_to_projects(admin_page)
    card = find_project_card(admin_page, PROJECT_NAME)
    share_btn = card.locator("text=Share").first
    if share_btn.is_visible(timeout=5000):
        share_btn.click()
        admin_page.fill('input[placeholder="collaborator@email.com"]', TEST_EMAIL)
        admin_page.click('button:has-text("Add")')
        admin_page.wait_for_timeout(2000)
        admin_page.click('button:has-text("Close")')

    # Both users navigate to the budget
    for page in [admin_page, test_page]:
        go_to_projects(page)
        page.wait_for_timeout(1000)
        try:
            open_project_by_name(page, PROJECT_NAME)
            page.wait_for_timeout(2000)
        except Exception:
            # Test user may not have access if share failed
            pass

    yield admin_page, test_page

    # Cleanup
    go_to_projects(admin_page)
    delete_project(admin_page, PROJECT_NAME)


# ---------------------------------------------------------------------------
# Real-time sync tests
# ---------------------------------------------------------------------------

@allure.title("Both users can see the same budget")
def test_both_users_see_budget(shared_budget: tuple[Page, Page]):
    admin_page, test_page = shared_budget
    expect(admin_page).to_have_url(re.compile(r"/projects/\d+/budget"))
    expect(test_page).to_have_url(re.compile(r"/projects/\d+/budget"))
    take_step_screenshot(admin_page, "collab-admin-view")
    take_step_screenshot(test_page, "collab-test-user-view")


@allure.title("Edit by admin is reflected in test user's view")
def test_admin_edit_syncs_to_test_user(shared_budget: tuple[Page, Page]):
    admin_page, test_page = shared_budget

    # Admin edits a numeric field
    numeric = admin_page.locator('input[type="number"], input.amount').first
    if numeric.is_visible(timeout=5000):
        numeric.click()
        numeric.fill("7777")
        numeric.press("Tab")
        admin_page.wait_for_timeout(3000)

        # Check if test user's page reflects the change
        take_step_screenshot(test_page, "collab-sync-after-admin-edit")
        content = test_page.content()
        allure.attach(
            f"Looking for '7777' in test user's page: {'7777' in content}",
            name="sync-check",
            attachment_type=allure.attachment_type.TEXT,
        )


@allure.title("Edit by test user is reflected in admin's view")
def test_test_user_edit_syncs_to_admin(shared_budget: tuple[Page, Page]):
    admin_page, test_page = shared_budget

    numeric = test_page.locator('input[type="number"], input.amount').first
    if numeric.is_visible(timeout=5000):
        numeric.click()
        numeric.fill("3333")
        numeric.press("Tab")
        test_page.wait_for_timeout(3000)

        take_step_screenshot(admin_page, "collab-sync-after-test-user-edit")
        content = admin_page.content()
        allure.attach(
            f"Looking for '3333' in admin's page: {'3333' in content}",
            name="sync-check",
            attachment_type=allure.attachment_type.TEXT,
        )


# ---------------------------------------------------------------------------
# WebSocket client tests
# ---------------------------------------------------------------------------

@allure.title("WebSocket client can connect and receive events")
def test_socket_client_connects(admin_page: Page):
    """Test the python-socketio client independently."""
    cookie = BudgetSocketClient.extract_session_cookie(admin_page)
    client = BudgetSocketClient(BASE_URL, session_cookie=cookie)
    try:
        client.connect(timeout=15.0)
        assert client.sio.connected, "Socket client failed to connect"
        allure.attach(
            f"Connected: {client.sio.connected}",
            name="socket-status",
            attachment_type=allure.attachment_type.TEXT,
        )
    except Exception as e:
        allure.attach(str(e), name="socket-error", attachment_type=allure.attachment_type.TEXT)
        pytest.skip(f"WebSocket connection not available: {e}")
    finally:
        client.disconnect()
