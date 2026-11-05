"""Client for `cascade serve`. Stdlib only."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Any

__all__ = ["CascadeClient", "CascadeError", "Run"]


class CascadeError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(f"{status}: {message}")
        self.status = status


class Run:
    def __init__(self, client: "CascadeClient", run_id: str):
        self.client = client
        self.run_id = run_id

    def describe(self) -> dict:
        return self.client._call("GET", f"/runs/{self.run_id}")

    def status(self) -> str:
        return self.describe()["status"]

    def history(self) -> list:
        return self.client._call("GET", f"/runs/{self.run_id}/history")

    def signal(self, name: str, value: Any = None) -> None:
        self.client._call("POST", f"/runs/{self.run_id}/signals/{name}", value)

    def cancel(self, reason: str | None = None) -> None:
        self.client._call("POST", f"/runs/{self.run_id}/cancel", {"reason": reason} if reason else None)

    def result(self, timeout: float = 60.0, poll: float = 0.25) -> Any:
        """Poll until the run ends. Raises CascadeError if it didn't complete."""
        deadline = time.monotonic() + timeout
        while True:
            d = self.describe()
            if d["status"] == "Completed":
                return d.get("result")
            if d["status"] != "Running":
                raise CascadeError(409, f"run {d['status'].lower()}: {d.get('error', '')}")
            if time.monotonic() > deadline:
                raise TimeoutError(f"run {self.run_id} still running after {timeout}s")
            time.sleep(poll)

    def __repr__(self) -> str:
        return f"Run({self.run_id!r})"


class CascadeClient:
    def __init__(self, endpoint: str = "http://127.0.0.1:7233", timeout: float = 10.0):
        self.endpoint = endpoint.rstrip("/")
        self.timeout = timeout

    def start(self, workflow_type: str, input: Any = None) -> Run:
        body = self._call("POST", f"/workflows/{workflow_type}", input)
        return Run(self, body["run_id"])

    def run(self, run_id: str) -> Run:
        return Run(self, run_id)

    def workflows(self) -> list[str]:
        return self._call("GET", "/workflows")

    def _call(self, method: str, path: str, body: Any = None) -> Any:
        data = None if body is None and method == "GET" else json.dumps(body).encode()
        req = urllib.request.Request(
            self.endpoint + path,
            data=data,
            method=method,
            headers={"content-type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                msg = json.loads(raw)["error"]
            except (ValueError, KeyError):
                msg = raw.decode(errors="replace")
            raise CascadeError(e.code, msg) from None
        return json.loads(raw) if raw else None
