"""
python-socketio test client for FPBudget real-time collaboration tests.

Usage:
    client = BudgetSocketClient(base_url, session_cookie)
    client.connect()
    client.join_budget(budget_id)
    # ... wait for events ...
    client.disconnect()
"""

from __future__ import annotations

import os
import time
import threading
from typing import Any
import socketio

BASE_URL = os.getenv("BASE_URL", "https://fp-budget.onrender.com")


class BudgetSocketClient:
    """Thin wrapper around python-socketio for testing FPBudget WebSocket events."""

    def __init__(self, base_url: str = BASE_URL, session_cookie: str = ""):
        self.base_url = base_url.rstrip("/")
        self.session_cookie = session_cookie
        self.sio = socketio.Client(logger=False, engineio_logger=False)
        self.received_events: list[dict[str, Any]] = []
        self.connected = threading.Event()
        self._setup_handlers()

    def _setup_handlers(self):
        @self.sio.event
        def connect():
            self.connected.set()

        @self.sio.event
        def disconnect():
            self.connected.clear()

        # Catch-all for budget events
        @self.sio.on("*")
        def catch_all(event, data=None):
            self.received_events.append({"event": event, "data": data, "time": time.time()})

        # Specific known events
        for evt in [
            "budget_update", "line_item_update", "line_item_added",
            "line_item_deleted", "topsheet_update", "user_joined",
            "user_left", "budget_locked", "budget_unlocked",
        ]:
            self.sio.on(evt, lambda data, e=evt: self.received_events.append(
                {"event": e, "data": data, "time": time.time()}
            ))

    def connect(self, timeout: float = 15.0):
        """Connect to the SocketIO server with session cookie auth."""
        headers = {}
        if self.session_cookie:
            headers["Cookie"] = self.session_cookie
        self.sio.connect(
            self.base_url,
            headers=headers,
            transports=["websocket", "polling"],
            wait_timeout=timeout,
        )
        self.connected.wait(timeout=timeout)

    def disconnect(self):
        if self.sio.connected:
            self.sio.disconnect()

    def join_budget(self, budget_id: str | int):
        """Emit a join event for a specific budget room."""
        self.sio.emit("join_budget", {"budget_id": str(budget_id)})

    def leave_budget(self, budget_id: str | int):
        self.sio.emit("leave_budget", {"budget_id": str(budget_id)})

    def emit(self, event: str, data: Any = None):
        self.sio.emit(event, data)

    def wait_for_event(self, event_name: str, timeout: float = 10.0) -> dict | None:
        """Block until a specific event is received or timeout."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            for evt in self.received_events:
                if evt["event"] == event_name:
                    return evt
            time.sleep(0.1)
        return None

    def get_events(self, event_name: str | None = None) -> list[dict]:
        """Return all received events, optionally filtered by name."""
        if event_name is None:
            return list(self.received_events)
        return [e for e in self.received_events if e["event"] == event_name]

    def clear_events(self):
        self.received_events.clear()

    @staticmethod
    def extract_session_cookie(page) -> str:
        """Extract session cookie from a Playwright page for use with socketio."""
        cookies = page.context.cookies()
        session_cookies = [c for c in cookies if c["name"] == "session"]
        if session_cookies:
            return f"session={session_cookies[0]['value']}"
        # Fall back to all cookies
        return "; ".join(f"{c['name']}={c['value']}" for c in cookies)
