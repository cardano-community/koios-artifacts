from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import requests

from .cases import TestCase


class TransportError(RuntimeError):
    pass


@dataclass
class PreparedCall:
    request: requests.PreparedRequest
    response: requests.Response | None = None
    attempts: int = 0


def _query_value(value: Any) -> Any:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list):
        return [_query_value(item) for item in value]
    return value


def _rate_interval(rate_limit: str) -> float:
    try:
        count_text, unit = rate_limit.split("/", 1)
        count = float(count_text)
        seconds = {"s": 1.0, "m": 60.0, "h": 3600.0}[unit]
    except (ValueError, KeyError, ZeroDivisionError) as exc:
        raise TransportError(f"Invalid rate limit: {rate_limit}") from exc
    return seconds / count


class HttpTransport:
    def __init__(
        self,
        base_url: str,
        token: str | None,
        timeout: float,
        retries: int,
        rate_limit: str,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self.minimum_interval = _rate_interval(rate_limit)
        self.last_request_at = 0.0
        self.session = requests.Session()
        self.token = token

    def prepare(self, case: TestCase) -> PreparedCall:
        path = case.operation.path
        for name, value in case.request.get("path", {}).items():
            path = path.replace("{" + name + "}", quote(str(value), safe=""))
        url = f"{self.base_url}{path}"
        headers = {str(key): str(value) for key, value in case.request.get("headers", {}).items()}
        headers.setdefault("Accept", "application/json")
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        media_type = case.request.get("media_type")
        if media_type:
            headers.setdefault("Content-Type", str(media_type))
        query = {
            key: _query_value(value)
            for key, value in case.request.get("query", {}).items()
        }
        request_kwargs: dict[str, Any] = {
            "method": case.operation.method.upper(),
            "url": url,
            "headers": headers,
            "params": query,
        }
        if "body_hex" in case.request:
            try:
                request_kwargs["data"] = bytes.fromhex(str(case.request["body_hex"]))
            except ValueError as exc:
                raise TransportError(f"Invalid hex body for {case.operation.operation_id}") from exc
        elif "body" in case.request:
            body = case.request["body"]
            if media_type and (media_type == "application/json" or media_type.endswith("+json")):
                request_kwargs["json"] = body
            elif isinstance(body, bytes):
                request_kwargs["data"] = body
            else:
                request_kwargs["data"] = str(body).encode()
        request = requests.Request(**request_kwargs)
        return PreparedCall(self.session.prepare_request(request))

    def send(self, call: PreparedCall) -> requests.Response:
        transient = {429, 502, 503, 504}
        for attempt in range(self.retries + 1):
            elapsed = time.monotonic() - self.last_request_at
            if elapsed < self.minimum_interval:
                time.sleep(self.minimum_interval - elapsed)
            self.last_request_at = time.monotonic()
            call.attempts = attempt + 1
            try:
                response = self.session.send(call.request, timeout=self.timeout)
            except requests.RequestException as exc:
                if attempt >= self.retries:
                    raise TransportError(str(exc)) from exc
                time.sleep(min(2**attempt, 5))
                continue
            call.response = response
            if response.status_code not in transient or attempt >= self.retries:
                return response
            retry_after = response.headers.get("Retry-After")
            try:
                delay = float(retry_after) if retry_after else min(2**attempt, 5)
            except ValueError:
                delay = min(2**attempt, 5)
            time.sleep(max(0.0, min(delay, 30.0)))
        raise TransportError("Request retry loop ended unexpectedly")
