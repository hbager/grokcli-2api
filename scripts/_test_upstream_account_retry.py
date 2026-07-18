#!/usr/bin/env python3
"""Regression coverage for configurable upstream account retries."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

_tmp = tempfile.mkdtemp(prefix="g2a-upstream-retry-test-")
os.environ["DATABASE_URL"] = ""
os.environ["GROK2API_DATABASE_URL"] = ""
os.environ["REDIS_URL"] = ""
os.environ["GROK2API_REDIS_URL"] = ""
os.environ["GROK2API_STORE_BACKEND"] = "file"
os.environ["GROK2API_DATA_DIR"] = _tmp

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from fastapi import Request  # noqa: E402
from grok2api import app as app_module  # noqa: E402
from grok2api.admin import settings_store  # noqa: E402
from grok2api.pool import account_pool as ap  # noqa: E402
from grok2api.pool.auth import GrokCredentials  # noqa: E402


def ok(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)
    print(f"  ok: {message}")


def assert_responses_sse_sequence(output: str, terminal: str) -> None:
    payloads = [
        json.loads(line.removeprefix("data: "))
        for line in output.splitlines()
        if line.startswith("data: {")
    ]
    sequence_numbers = [
        payload["sequence_number"]
        for payload in payloads
        if "sequence_number" in payload
    ]
    ok(
        sequence_numbers == list(range(len(sequence_numbers))),
        f"Responses sequence numbers are contiguous from zero: {sequence_numbers}",
    )
    ok(output.count("event: response.created") == 1, "response.created is emitted once")
    ok(output.count(f"event: {terminal}") == 1, f"{terminal} is emitted once")
    ok(
        output.count("event: response.completed") + output.count("event: response.failed") == 1,
        "exactly one Responses terminal is emitted",
    )


def retry_values(settings: dict[str, object]) -> tuple[int, int]:
    def get_value(key: str, default: object = None) -> object:
        return settings.get(key, default)

    ap._policy_cache.clear()
    with mock.patch.object(settings_store, "_get_setting_value", side_effect=get_value):
        retries = ap.upstream_retry_count()
        attempts = ap.max_failover_attempts()
    return retries, attempts


def test_new_retry_count_wins_including_zero() -> None:
    print("[new retry count wins]")
    retries, attempts = retry_values(
        {"upstream_retry_count": 0, "max_failover_attempts": 64}
    )
    ok(retries == 0, f"new zero preserved, got {retries}")
    ok(attempts == 1, f"zero retries means one attempt, got {attempts}")


def test_legacy_attempt_count_is_converted() -> None:
    print("[legacy total attempts converts to retries]")
    retries, attempts = retry_values({"max_failover_attempts": 4})
    ok(retries == 3, f"legacy 4 attempts becomes 3 retries, got {retries}")
    ok(attempts == 4, f"legacy total remains four attempts, got {attempts}")


def test_missing_settings_use_default() -> None:
    print("[missing settings use default]")
    retries, attempts = retry_values({})
    ok(retries == 3, f"default retries is 3, got {retries}")
    ok(attempts == 4, f"default attempts is 4, got {attempts}")


def credential(account_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        auth_key=account_id,
        user_id=account_id,
        expired=False,
        refresh_token=None,
        token=f"token-{account_id}",
        email=f"{account_id}@test.local",
    )


def test_sticky_chain_honors_zero_retries() -> None:
    print("[sticky chain honors zero retries]")
    sticky = credential("sticky")
    backup = credential("backup")
    state = {
        "sticky": {"enabled": True, "pool_status": "active"},
        "backup": {"enabled": True, "pool_status": "active"},
    }

    def get_value(key: str, default: object = None) -> object:
        return {"upstream_retry_count": 0}.get(key, default)

    ap._policy_cache.clear()
    with mock.patch.object(settings_store, "_get_setting_value", side_effect=get_value), \
         mock.patch.object(ap, "_ensure_multi_account_layout"), \
         mock.patch.object(ap, "peek_credentials_by_id", return_value=sticky), \
         mock.patch.object(ap, "get_account_pool_meta", return_value=state["sticky"]), \
         mock.patch.object(ap, "is_model_blocked", return_value=False), \
         mock.patch.object(ap, "get_cached_live_credentials", return_value=[sticky, backup]), \
         mock.patch.object(ap, "get_cached_account_pool_state", return_value=state):
        chain = ap.try_acquire_sequence(model="grok", prefer_account_id="sticky")

    ids = [item.auth_key for item in chain]
    ok(ids == ["sticky"], f"zero retries keeps only sticky account, got {ids}")


def test_sticky_chain_falls_back_when_warm_backups_are_incomplete() -> None:
    print("[sticky chain fills missing warm backups]")
    sticky = credential("sticky")
    backup_a = credential("backup-a")
    backup_b = credential("backup-b")
    state = {
        item.auth_key: {"enabled": True, "pool_status": "active"}
        for item in (sticky, backup_a, backup_b)
    }

    def get_value(key: str, default: object = None) -> object:
        return {"upstream_retry_count": 2}.get(key, default)

    ap._policy_cache.clear()
    with mock.patch.object(settings_store, "_get_setting_value", side_effect=get_value), \
         mock.patch.object(ap, "_ensure_multi_account_layout"), \
         mock.patch.object(ap, "peek_credentials_by_id", return_value=sticky), \
         mock.patch.object(ap, "get_account_pool_meta", return_value=state["sticky"]), \
         mock.patch.object(ap, "is_model_blocked", return_value=False), \
         mock.patch.object(ap, "get_cached_live_credentials", return_value=[sticky]), \
         mock.patch.object(ap, "list_live_credentials", return_value=[sticky, backup_a, backup_b]), \
         mock.patch.object(ap, "get_cached_account_pool_state", return_value=state):
        chain = ap.try_acquire_sequence(model="grok", prefer_account_id="sticky")

    ids = [item.auth_key for item in chain]
    ok(ids == ["sticky", "backup-a", "backup-b"], f"cold warm-cache falls back to full chain: {ids}")


def test_three_retries_select_four_distinct_accounts() -> None:
    print("[three retries select four distinct accounts]")
    accounts = [credential(f"account-{index}") for index in range(6)]
    state = {
        item.auth_key: {"enabled": True, "pool_status": "active"}
        for item in accounts
    }

    def get_value(key: str, default: object = None) -> object:
        return {"upstream_retry_count": 3}.get(key, default)

    ap._policy_cache.clear()
    with mock.patch.object(settings_store, "_get_setting_value", side_effect=get_value), \
         mock.patch.object(ap, "_ensure_multi_account_layout"), \
         mock.patch.object(ap, "list_live_credentials", return_value=accounts), \
         mock.patch.object(ap, "get_cached_account_pool_state", return_value=state), \
         mock.patch.object(ap, "is_model_blocked", return_value=False):
        chain = ap.try_acquire_sequence(model="grok")

    ids = [item.auth_key for item in chain]
    ok(len(ids) == 4, f"N=3 selects four total attempts: {ids}")
    ok(len(set(ids)) == 4, f"all selected accounts are distinct: {ids}")


def test_max_retry_count_selects_sixty_four_accounts() -> None:
    print("[maximum retry count selects sixty-four accounts]")
    accounts = [credential(f"account-{index}") for index in range(70)]
    state = {
        item.auth_key: {"enabled": True, "pool_status": "active"}
        for item in accounts
    }

    def get_value(key: str, default: object = None) -> object:
        return {"upstream_retry_count": 63}.get(key, default)

    ap._policy_cache.clear()
    with mock.patch.object(settings_store, "_get_setting_value", side_effect=get_value), \
         mock.patch.object(ap, "_ensure_multi_account_layout"), \
         mock.patch.object(ap, "list_live_credentials", return_value=accounts), \
         mock.patch.object(ap, "get_cached_account_pool_state", return_value=state), \
         mock.patch.object(ap, "is_model_blocked", return_value=False):
        chain = ap.try_acquire_sequence(model="grok")

    ids = [item.auth_key for item in chain]
    ok(len(ids) == 64, f"N=63 selects sixty-four total attempts, got {len(ids)}")
    ok(len(set(ids)) == 64, "maximum retry chain contains distinct accounts")


def test_all_upstream_http_errors_are_retryable() -> None:
    print("[all upstream HTTP errors retry]")
    for status in (300, 400, 404, 418, 429, 500, 502, 503):
        ok(app_module._retryable_status(status), f"status {status} retries")
    for status in (200, 204, 299):
        ok(not app_module._retryable_status(status), f"status {status} succeeds")


def fastapi_request(path: str, body: dict[str, object] | None = None) -> Request:
    async def receive():
        return {
            "type": "http.request",
            "body": json.dumps(body).encode() if body is not None else b"",
            "more_body": False,
        }

    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": path,
            "headers": [],
            "query_string": b"",
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
            "scheme": "http",
        },
        receive=receive,
    )


def test_chat_endpoint_retries_upstream_300() -> None:
    print("[chat endpoint retries upstream 300]")
    chain = [
        GrokCredentials(token="bad", auth_key="bad", email="bad@test.local"),
        GrokCredentials(token="ok", auth_key="ok", email="ok@test.local"),
    ]
    attempts: list[str] = []

    async def upstream_response(request):
        token = request.headers["Authorization"].removeprefix("Bearer ")
        attempts.append(token)
        if token == "bad":
            return app_module.httpx.Response(
                300,
                request=request,
                headers={"content-type": "text/event-stream"},
                text=(
                    'data: {"choices":[{"delta":{"content":"wrong"},'
                    '"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n'
                ),
            )
        return app_module.httpx.Response(
            200,
            request=request,
            headers={"content-type": "text/event-stream"},
            text=(
                'data: {"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}],'
                '"usage":{"completion_tokens":1}}\n\ndata: [DONE]\n\n'
            ),
        )

    client = app_module.httpx.AsyncClient(
        transport=app_module.httpx.MockTransport(upstream_response)
    )

    async def run():
        try:
            with mock.patch.object(app_module, "_resolve_conversation_affinity", return_value=(None, None, None, False)), \
                 mock.patch.object(app_module, "_pick_account_chain_timed", return_value=(chain, 0.0)), \
                 mock.patch.object(app_module, "build_upstream_body", return_value={"model": "grok", "messages": []}), \
                 mock.patch.object(app_module, "get_http_client", return_value=client), \
                 mock.patch.object(app_module.account_pool, "report_success"), \
                 mock.patch.object(app_module, "_report_upstream_failure"), \
                 mock.patch.object(app_module, "_record_usage_safe"), \
                 mock.patch.object(app_module, "_note_account_pick"):
                return await app_module.chat_completions(
                    app_module.ChatCompletionRequest(
                        model="grok",
                        messages=[{"role": "user", "content": "hi"}],
                    ),
                    fastapi_request("/v1/chat/completions"),
                    None,
                )
        finally:
            await client.aclose()

    response = asyncio.run(run())
    payload = json.loads(response.body)
    content = payload["choices"][0]["message"]["content"]
    ok(content == "ok", f"second account response returned: {content!r}")
    ok(attempts == ["bad", "ok"], f"distinct account attempts: {attempts}")


def test_chat_endpoint_exhaustion_is_standard_error() -> None:
    print("[chat endpoint exhaustion is standard error]")
    chain = [
        GrokCredentials(token="bad-a", auth_key="bad-a"),
        GrokCredentials(token="bad-b", auth_key="bad-b"),
    ]
    attempts: list[str] = []

    async def upstream_response(request):
        token = request.headers["Authorization"].removeprefix("Bearer ")
        attempts.append(token)
        return app_module.httpx.Response(
            404,
            request=request,
            text="sensitive upstream body",
            headers={"content-type": "application/json"},
        )

    client = app_module.httpx.AsyncClient(
        transport=app_module.httpx.MockTransport(upstream_response)
    )

    async def run():
        try:
            with mock.patch.object(app_module, "_resolve_conversation_affinity", return_value=(None, None, None, False)), \
                 mock.patch.object(app_module, "_pick_account_chain_timed", return_value=(chain, 0.0)), \
                 mock.patch.object(app_module, "build_upstream_body", return_value={"model": "grok", "messages": []}), \
                 mock.patch.object(app_module, "get_http_client", return_value=client), \
                 mock.patch.object(app_module, "_report_upstream_failure"), \
                 mock.patch.object(app_module, "_record_usage_safe"), \
                 mock.patch.object(app_module, "_note_account_pick"):
                return await app_module.chat_completions(
                    app_module.ChatCompletionRequest(
                        model="grok",
                        messages=[{"role": "user", "content": "hi"}],
                    ),
                    fastapi_request("/v1/chat/completions"),
                    None,
                )
        finally:
            await client.aclose()

    response = asyncio.run(run())
    payload = json.loads(response.body)
    error = payload["error"]
    ok(response.status_code == 503, f"status is 503, got {response.status_code}")
    ok(error["type"] == "upstream_error", f"type is upstream_error: {error}")
    ok(error["code"] == "all_accounts_failed", f"code is all_accounts_failed: {error}")
    ok("sensitive upstream body" not in response.body.decode(), "raw upstream body is hidden")
    ok(attempts == ["bad-a", "bad-b"], f"all distinct accounts attempted: {attempts}")


def test_messages_endpoint_exhaustion_is_standard_error() -> None:
    print("[messages endpoint exhaustion is standard error]")
    chain = [
        GrokCredentials(token="bad-a", auth_key="bad-a"),
        GrokCredentials(token="bad-b", auth_key="bad-b"),
    ]
    attempts: list[str] = []

    async def upstream_response(request):
        token = request.headers["Authorization"].removeprefix("Bearer ")
        attempts.append(token)
        return app_module.httpx.Response(
            404,
            request=request,
            text="sensitive anthropic upstream body",
            headers={"content-type": "application/json"},
        )

    client = app_module.httpx.AsyncClient(
        transport=app_module.httpx.MockTransport(upstream_response)
    )

    async def run():
        try:
            with mock.patch.object(app_module, "_resolve_anthropic_affinity", return_value=(None, None, None, False)), \
                 mock.patch.object(app_module, "_pick_account_chain_timed", return_value=(chain, 0.0)), \
                 mock.patch.object(app_module.anth, "build_openai_chat_body", return_value={"model": "grok", "messages": []}), \
                 mock.patch.object(app_module, "get_http_client", return_value=client), \
                 mock.patch.object(app_module, "_report_upstream_failure"), \
                 mock.patch.object(app_module, "_record_usage_safe"), \
                 mock.patch.object(app_module, "_note_account_pick"):
                return await app_module.anthropic_messages(
                    app_module.anth.AnthropicMessagesRequest(
                        model="grok",
                        messages=[{"role": "user", "content": "hi"}],
                        max_tokens=16,
                    ),
                    fastapi_request("/v1/messages"),
                    None,
                    None,
                )
        finally:
            await client.aclose()

    response = asyncio.run(run())
    payload = json.loads(response.body)
    error = payload["error"]
    ok(response.status_code == 503, f"status is 503, got {response.status_code}")
    ok(error["type"] == "api_error", f"type is api_error: {error}")
    ok(
        app_module.ALL_ACCOUNTS_FAILED_CODE in error["message"],
        f"message identifies all_accounts_failed: {error}",
    )
    ok(
        "sensitive anthropic upstream body" not in response.body.decode(),
        "raw upstream body is hidden",
    )
    ok(attempts == ["bad-a", "bad-b"], f"all distinct accounts attempted: {attempts}")


def test_responses_endpoint_exhaustion_is_standard_error() -> None:
    print("[responses endpoint exhaustion is standard error]")
    chain = [
        GrokCredentials(token="bad-a", auth_key="bad-a"),
        GrokCredentials(token="bad-b", auth_key="bad-b"),
    ]
    attempts: list[str] = []

    async def upstream_response(request):
        token = request.headers["Authorization"].removeprefix("Bearer ")
        attempts.append(token)
        return app_module.httpx.Response(
            404,
            request=request,
            text="sensitive responses upstream body",
            headers={"content-type": "application/json"},
        )

    client = app_module.httpx.AsyncClient(
        transport=app_module.httpx.MockTransport(upstream_response)
    )
    request_body = {"model": "grok", "input": "hi"}

    async def run():
        try:
            with mock.patch.object(app_module, "_responses_affinity", return_value=(None, None, None, None, False)), \
                 mock.patch.object(app_module, "_pick_account_chain_timed", return_value=(chain, 0.0)), \
                 mock.patch.object(app_module.oai_resp, "responses_request_to_chat_body", return_value={"model": "grok", "messages": [{"role": "user", "content": "hi"}]}), \
                 mock.patch.object(app_module, "get_http_client", return_value=client), \
                 mock.patch.object(app_module, "_report_upstream_failure"), \
                 mock.patch.object(app_module, "_record_usage_safe"), \
                 mock.patch.object(app_module, "_note_account_pick"):
                return await app_module.openai_responses(
                    fastapi_request("/v1/responses", request_body),
                    None,
                )
        finally:
            await client.aclose()

    response = asyncio.run(run())
    payload = json.loads(response.body)
    error = payload["error"]
    ok(response.status_code == 503, f"status is 503, got {response.status_code}")
    ok(error["type"] == "upstream_error", f"type is upstream_error: {error}")
    ok(error["code"] == "all_accounts_failed", f"code is all_accounts_failed: {error}")
    ok(
        "sensitive responses upstream body" not in response.body.decode(),
        "raw upstream body is hidden",
    )
    ok(attempts == ["bad-a", "bad-b"], f"all distinct accounts attempted: {attempts}")


class CancelledStream(app_module.httpx.AsyncByteStream):
    async def __aiter__(self):
        if False:
            yield b""
        raise asyncio.CancelledError()


class EventThenErrorStream(app_module.httpx.AsyncByteStream):
    def __init__(self, event: str) -> None:
        self.event = event

    async def __aiter__(self):
        yield self.event.encode()
        raise app_module.httpx.ReadError("scripted stream interruption")


async def collect_stream(iterator) -> str:
    return "".join([chunk async for chunk in iterator])


def test_messages_stream_retries_empty_before_model_output() -> None:
    print("[messages stream retries empty before model output]")
    chain = [
        GrokCredentials(token="empty", auth_key="empty"),
        GrokCredentials(token="ok", auth_key="ok"),
    ]
    attempts: list[str] = []

    async def upstream_response(request):
        token = request.headers["Authorization"].removeprefix("Bearer ")
        attempts.append(token)
        text = "data: [DONE]\n\n"
        if token == "ok":
            text = (
                'data: {"choices":[{"delta":{"content":"ok"},'
                '"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n'
            )
        return app_module.httpx.Response(
            200,
            request=request,
            text=text,
            headers={"content-type": "text/event-stream"},
        )

    client = app_module.httpx.AsyncClient(
        transport=app_module.httpx.MockTransport(upstream_response)
    )

    async def disconnected() -> bool:
        return False

    async def run() -> str:
        try:
            with mock.patch.object(app_module, "get_http_client", return_value=client), \
                 mock.patch.object(app_module.account_pool, "report_success"), \
                 mock.patch.object(app_module, "_report_upstream_failure"), \
                 mock.patch.object(app_module, "_record_usage_safe"), \
                 mock.patch.object(app_module, "_note_account_pick"):
                return await collect_stream(
                    app_module._stream_anthropic_with_failover_inner(
                        url="https://upstream.test/chat/completions",
                        body={"model": "grok", "messages": []},
                        chain=chain,
                        message_id="msg_test",
                        model="grok",
                        client_disconnected=disconnected,
                    )
                )
        finally:
            await client.aclose()

    output = asyncio.run(run())
    ok(attempts == ["empty", "ok"], f"empty account fails over once: {attempts}")
    ok('"text": "ok"' in output, "second account content is emitted")
    ok('event: error' not in output, "no error terminal is emitted after recovery")
    ok(output.count('"type": "message_start"') == 1, "envelope opens exactly once")


def test_responses_stream_retries_empty_before_model_output() -> None:
    print("[responses stream retries empty before model output]")
    chain = [
        GrokCredentials(token="empty", auth_key="empty"),
        GrokCredentials(token="ok", auth_key="ok"),
    ]
    attempts: list[str] = []

    async def upstream_response(request):
        token = request.headers["Authorization"].removeprefix("Bearer ")
        attempts.append(token)
        text = "data: [DONE]\n\n"
        if token == "ok":
            text = (
                'data: {"choices":[{"delta":{"content":"ok"},'
                '"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n'
            )
        return app_module.httpx.Response(
            200,
            request=request,
            text=text,
            headers={"content-type": "text/event-stream"},
        )

    client = app_module.httpx.AsyncClient(
        transport=app_module.httpx.MockTransport(upstream_response)
    )
    request_body = {"model": "grok", "input": "hi", "stream": True}

    async def run() -> str:
        try:
            with mock.patch.object(app_module, "_responses_affinity", return_value=(None, None, None, None, False)), \
                 mock.patch.object(app_module, "_pick_account_chain_timed", return_value=(chain, 0.0)), \
                 mock.patch.object(app_module.oai_resp, "responses_request_to_chat_body", return_value={"model": "grok", "messages": [{"role": "user", "content": "hi"}]}), \
                 mock.patch.object(app_module, "get_http_client", return_value=client), \
                 mock.patch.object(app_module.account_pool, "report_success"), \
                 mock.patch.object(app_module.conversation_affinity, "bind_response_chain"), \
                 mock.patch.object(app_module, "_report_upstream_failure"), \
                 mock.patch.object(app_module, "_record_usage_safe"), \
                 mock.patch.object(app_module, "_note_account_pick"):
                response = await app_module.openai_responses(
                    fastapi_request("/v1/responses", request_body),
                    None,
                )
                return await collect_stream(response.body_iterator)
        finally:
            await client.aclose()

    output = asyncio.run(run())
    ok(attempts == ["empty", "ok"], f"empty account fails over once: {attempts}")
    ok('"type": "response.output_text.delta"' in output, "second account content is emitted")
    ok('"delta": "ok"' in output, "second account text is preserved")
    ok('event: response.failed' not in output, "no failed terminal is emitted after recovery")
    ok('event: response.completed' in output, "successful terminal is emitted")
    assert_responses_sse_sequence(output, "response.completed")


def test_chat_stream_exhaustion_is_standard_error() -> None:
    print("[chat stream exhaustion is standard error]")
    chain = [
        GrokCredentials(token="bad-a", auth_key="bad-a"),
        GrokCredentials(token="bad-b", auth_key="bad-b"),
    ]
    attempts: list[str] = []

    async def upstream_response(request):
        token = request.headers["Authorization"].removeprefix("Bearer ")
        attempts.append(token)
        return app_module.httpx.Response(
            404,
            request=request,
            text="sensitive chat stream body",
            headers={"content-type": "application/json"},
        )

    client = app_module.httpx.AsyncClient(
        transport=app_module.httpx.MockTransport(upstream_response)
    )

    async def disconnected() -> bool:
        return False

    failure_reporter = mock.Mock()
    usage_recorder = mock.Mock()

    async def run() -> str:
        try:
            with mock.patch.object(app_module, "get_http_client", return_value=client), \
                 mock.patch.object(app_module, "_report_upstream_failure", new=failure_reporter), \
                 mock.patch.object(app_module, "_record_usage_safe", new=usage_recorder), \
                 mock.patch.object(app_module, "_note_account_pick"):
                return await collect_stream(
                    app_module._stream_proxy_with_failover_inner(
                        url="https://upstream.test/chat/completions",
                        body={"model": "grok", "messages": []},
                        chain=chain,
                        chat_id="chat_test",
                        model="grok",
                        created=1,
                        client_disconnected=disconnected,
                    )
                )
        finally:
            await client.aclose()

    output = asyncio.run(run())
    ok(attempts == ["bad-a", "bad-b"], f"all distinct accounts attempted: {attempts}")
    ok(app_module.ALL_ACCOUNTS_FAILED_CODE in output, "stream identifies all_accounts_failed")
    ok("data: [DONE]" in output, "stream emits DONE terminal")
    ok("sensitive chat stream body" not in output, "raw upstream body is hidden")
    observations = repr(failure_reporter.call_args_list) + repr(usage_recorder.call_args_list)
    ok("sensitive chat stream body" not in observations, "chat observations hide upstream body")


def test_messages_stream_exhaustion_is_standard_error() -> None:
    print("[messages stream exhaustion is standard error]")
    chain = [
        GrokCredentials(token="bad-a", auth_key="bad-a"),
        GrokCredentials(token="bad-b", auth_key="bad-b"),
    ]
    attempts: list[str] = []

    async def upstream_response(request):
        token = request.headers["Authorization"].removeprefix("Bearer ")
        attempts.append(token)
        return app_module.httpx.Response(
            404,
            request=request,
            text="sensitive messages stream body",
            headers={"content-type": "application/json"},
        )

    client = app_module.httpx.AsyncClient(
        transport=app_module.httpx.MockTransport(upstream_response)
    )

    async def disconnected() -> bool:
        return False

    failure_reporter = mock.Mock()
    usage_recorder = mock.Mock()

    async def run() -> str:
        try:
            with mock.patch.object(app_module, "get_http_client", return_value=client), \
                 mock.patch.object(app_module, "_report_upstream_failure", new=failure_reporter), \
                 mock.patch.object(app_module, "_record_usage_safe", new=usage_recorder), \
                 mock.patch.object(app_module, "_note_account_pick"):
                return await collect_stream(
                    app_module._stream_anthropic_with_failover_inner(
                        url="https://upstream.test/chat/completions",
                        body={"model": "grok", "messages": []},
                        chain=chain,
                        message_id="msg_test",
                        model="grok",
                        client_disconnected=disconnected,
                    )
                )
        finally:
            await client.aclose()

    output = asyncio.run(run())
    ok(attempts == ["bad-a", "bad-b"], f"all distinct accounts attempted: {attempts}")
    ok("event: error" in output, "stream emits Anthropic error terminal")
    ok(app_module.ALL_ACCOUNTS_FAILED_CODE in output, "stream identifies all_accounts_failed")
    ok("sensitive messages stream body" not in output, "raw upstream body is hidden")
    observations = repr(failure_reporter.call_args_list) + repr(usage_recorder.call_args_list)
    ok("sensitive messages stream body" not in observations, "Messages observations hide upstream body")


def test_responses_stream_exhaustion_is_standard_error() -> None:
    print("[responses stream exhaustion is standard error]")
    chain = [
        GrokCredentials(token="bad-a", auth_key="bad-a"),
        GrokCredentials(token="bad-b", auth_key="bad-b"),
    ]
    attempts: list[str] = []

    async def upstream_response(request):
        token = request.headers["Authorization"].removeprefix("Bearer ")
        attempts.append(token)
        return app_module.httpx.Response(
            404,
            request=request,
            text="sensitive responses stream body",
            headers={"content-type": "application/json"},
        )

    client = app_module.httpx.AsyncClient(
        transport=app_module.httpx.MockTransport(upstream_response)
    )
    request_body = {"model": "grok", "input": "hi", "stream": True}
    failure_reporter = mock.Mock()
    usage_recorder = mock.Mock()

    async def run() -> str:
        try:
            with mock.patch.object(app_module, "_responses_affinity", return_value=(None, None, None, None, False)), \
                 mock.patch.object(app_module, "_pick_account_chain_timed", return_value=(chain, 0.0)), \
                 mock.patch.object(app_module.oai_resp, "responses_request_to_chat_body", return_value={"model": "grok", "messages": [{"role": "user", "content": "hi"}]}), \
                 mock.patch.object(app_module, "get_http_client", return_value=client), \
                 mock.patch.object(app_module.conversation_affinity, "bind_response_chain"), \
                 mock.patch.object(app_module, "_report_upstream_failure", new=failure_reporter), \
                 mock.patch.object(app_module, "_record_usage_safe", new=usage_recorder), \
                 mock.patch.object(app_module, "_note_account_pick"):
                response = await app_module.openai_responses(
                    fastapi_request("/v1/responses", request_body),
                    None,
                )
                return await collect_stream(response.body_iterator)
        finally:
            await client.aclose()

    output = asyncio.run(run())
    ok(attempts == ["bad-a", "bad-b"], f"all distinct accounts attempted: {attempts}")
    ok("event: response.failed" in output, "stream emits Responses failed terminal")
    ok(app_module.ALL_ACCOUNTS_FAILED_CODE in output, "stream identifies all_accounts_failed")
    ok("data: [DONE]" in output, "stream emits DONE terminal")
    ok("sensitive responses stream body" not in output, "raw upstream body is hidden")
    observations = repr(failure_reporter.call_args_list) + repr(usage_recorder.call_args_list)
    ok("sensitive responses stream body" not in observations, "Responses observations hide upstream body")
    assert_responses_sse_sequence(output, "response.failed")


def test_chat_stream_does_not_retry_after_model_output() -> None:
    print("[chat stream does not retry after model output]")
    chain = [
        GrokCredentials(token="first", auth_key="first"),
        GrokCredentials(token="backup", auth_key="backup"),
    ]
    attempts: list[str] = []

    async def upstream_response(request):
        token = request.headers["Authorization"].removeprefix("Bearer ")
        attempts.append(token)
        return app_module.httpx.Response(
            200,
            request=request,
            headers={"content-type": "text/event-stream"},
            stream=EventThenErrorStream(
                'data: {"choices":[{"delta":{"content":"first"}}]}\n\n'
            ),
        )

    client = app_module.httpx.AsyncClient(
        transport=app_module.httpx.MockTransport(upstream_response)
    )

    async def disconnected() -> bool:
        return False

    async def run() -> str:
        try:
            with mock.patch.object(app_module, "get_http_client", return_value=client), \
                 mock.patch.object(app_module.account_pool, "report_success"), \
                 mock.patch.object(app_module, "_report_upstream_failure"), \
                 mock.patch.object(app_module, "_record_usage_safe"), \
                 mock.patch.object(app_module, "_note_account_pick"):
                return await collect_stream(
                    app_module._stream_proxy_with_failover_inner(
                        url="https://upstream.test/chat/completions",
                        body={"model": "grok", "messages": []},
                        chain=chain,
                        chat_id="chat_test",
                        model="grok",
                        created=1,
                        client_disconnected=disconnected,
                    )
                )
        finally:
            await client.aclose()

    output = asyncio.run(run())
    ok(attempts == ["first"], f"backup account is not attempted: {attempts}")
    ok('"content": "first"' in output, "committed first-account content is preserved")
    ok(app_module.ALL_ACCOUNTS_FAILED_CODE not in output, "mid-stream failure is not mislabeled as exhaustion")


def test_messages_stream_does_not_retry_after_model_output() -> None:
    print("[messages stream does not retry after model output]")
    chain = [
        GrokCredentials(token="first", auth_key="first"),
        GrokCredentials(token="backup", auth_key="backup"),
    ]
    attempts: list[str] = []

    async def upstream_response(request):
        token = request.headers["Authorization"].removeprefix("Bearer ")
        attempts.append(token)
        return app_module.httpx.Response(
            200,
            request=request,
            headers={"content-type": "text/event-stream"},
            stream=EventThenErrorStream(
                'data: {"choices":[{"delta":{"content":"first"}}]}\n\n'
            ),
        )

    client = app_module.httpx.AsyncClient(
        transport=app_module.httpx.MockTransport(upstream_response)
    )

    async def disconnected() -> bool:
        return False

    async def run() -> str:
        try:
            with mock.patch.object(app_module, "get_http_client", return_value=client), \
                 mock.patch.object(app_module.account_pool, "report_success"), \
                 mock.patch.object(app_module, "_report_upstream_failure"), \
                 mock.patch.object(app_module, "_record_usage_safe"), \
                 mock.patch.object(app_module, "_note_account_pick"):
                return await collect_stream(
                    app_module._stream_anthropic_with_failover_inner(
                        url="https://upstream.test/chat/completions",
                        body={"model": "grok", "messages": []},
                        chain=chain,
                        message_id="msg_test",
                        model="grok",
                        client_disconnected=disconnected,
                    )
                )
        finally:
            await client.aclose()

    output = asyncio.run(run())
    ok(attempts == ["first"], f"backup account is not attempted: {attempts}")
    ok('"text": "first"' in output, "committed first-account content is preserved")
    ok(app_module.ALL_ACCOUNTS_FAILED_CODE not in output, "mid-stream failure is not mislabeled as exhaustion")


def test_responses_stream_does_not_retry_after_model_output() -> None:
    print("[responses stream does not retry after model output]")
    chain = [
        GrokCredentials(token="first", auth_key="first"),
        GrokCredentials(token="backup", auth_key="backup"),
    ]
    attempts: list[str] = []

    async def upstream_response(request):
        token = request.headers["Authorization"].removeprefix("Bearer ")
        attempts.append(token)
        return app_module.httpx.Response(
            200,
            request=request,
            headers={"content-type": "text/event-stream"},
            stream=EventThenErrorStream(
                'data: {"choices":[{"delta":{"content":"first"}}]}\n\n'
            ),
        )

    client = app_module.httpx.AsyncClient(
        transport=app_module.httpx.MockTransport(upstream_response)
    )
    request_body = {"model": "grok", "input": "hi", "stream": True}

    async def run() -> str:
        try:
            with mock.patch.object(app_module, "_responses_affinity", return_value=(None, None, None, None, False)), \
                 mock.patch.object(app_module, "_pick_account_chain_timed", return_value=(chain, 0.0)), \
                 mock.patch.object(app_module.oai_resp, "responses_request_to_chat_body", return_value={"model": "grok", "messages": [{"role": "user", "content": "hi"}]}), \
                 mock.patch.object(app_module, "get_http_client", return_value=client), \
                 mock.patch.object(app_module.account_pool, "report_success"), \
                 mock.patch.object(app_module.conversation_affinity, "bind_response_chain"), \
                 mock.patch.object(app_module, "_report_upstream_failure"), \
                 mock.patch.object(app_module, "_record_usage_safe"), \
                 mock.patch.object(app_module, "_note_account_pick"):
                response = await app_module.openai_responses(
                    fastapi_request("/v1/responses", request_body),
                    None,
                )
                return await collect_stream(response.body_iterator)
        finally:
            await client.aclose()

    output = asyncio.run(run())
    ok(attempts == ["first"], f"backup account is not attempted: {attempts}")
    ok('"delta": "first"' in output, "committed first-account content is preserved")
    ok(app_module.ALL_ACCOUNTS_FAILED_CODE not in output, "mid-stream failure is not mislabeled as exhaustion")


def test_client_cancellation_does_not_retry() -> None:
    print("[client cancellation does not retry]")
    chain = [
        GrokCredentials(token="first", auth_key="first"),
        GrokCredentials(token="backup", auth_key="backup"),
    ]

    async def run_protocol(protocol: str) -> list[str]:
        attempts: list[str] = []

        async def upstream_response(request):
            token = request.headers["Authorization"].removeprefix("Bearer ")
            attempts.append(token)
            return app_module.httpx.Response(
                200,
                request=request,
                headers={"content-type": "text/event-stream"},
                stream=CancelledStream(),
            )

        client = app_module.httpx.AsyncClient(
            transport=app_module.httpx.MockTransport(upstream_response)
        )

        async def disconnected() -> bool:
            return False

        try:
            with mock.patch.object(app_module, "get_http_client", return_value=client), \
                 mock.patch.object(app_module, "_responses_affinity", return_value=(None, None, None, None, False)), \
                 mock.patch.object(app_module, "_pick_account_chain_timed", return_value=(chain, 0.0)), \
                 mock.patch.object(app_module.oai_resp, "responses_request_to_chat_body", return_value={"model": "grok", "messages": [{"role": "user", "content": "hi"}]}), \
                 mock.patch.object(app_module.conversation_affinity, "bind_response_chain"), \
                 mock.patch.object(app_module, "_report_upstream_failure"), \
                 mock.patch.object(app_module, "_record_usage_safe"), \
                 mock.patch.object(app_module, "_note_account_pick"):
                if protocol == "chat":
                    await collect_stream(
                        app_module._stream_proxy_with_failover_inner(
                            url="https://upstream.test/chat/completions",
                            body={"model": "grok", "messages": []},
                            chain=chain,
                            chat_id="chat_test",
                            model="grok",
                            created=1,
                            client_disconnected=disconnected,
                        )
                    )
                elif protocol == "messages":
                    await collect_stream(
                        app_module._stream_anthropic_with_failover_inner(
                            url="https://upstream.test/chat/completions",
                            body={"model": "grok", "messages": []},
                            chain=chain,
                            message_id="msg_test",
                            model="grok",
                            client_disconnected=disconnected,
                        )
                    )
                else:
                    response = await app_module.openai_responses(
                        fastapi_request(
                            "/v1/responses",
                            {"model": "grok", "input": "hi", "stream": True},
                        ),
                        None,
                    )
                    await collect_stream(response.body_iterator)
        finally:
            await client.aclose()
        return attempts

    for protocol in ("chat", "messages", "responses"):
        attempts = asyncio.run(run_protocol(protocol))
        ok(attempts == ["first"], f"{protocol} cancellation stops before backup: {attempts}")


def test_chat_retries_transport_parse_and_empty_failures() -> None:
    print("[chat retries transport parse and empty failures]")
    parse_secret = "parse-secret-must-not-be-observed"
    scenarios = {
        "network": None,
        "malformed": app_module.httpx.Response(
            200,
            text='{"credential":"' + parse_secret + '"',
            headers={"content-type": "application/json"},
        ),
        "empty-body": app_module.httpx.Response(
            200,
            content=b"",
            headers={"content-type": "application/json"},
        ),
        "empty-model": app_module.httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {"content": ""},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"completion_tokens": 0},
            },
            headers={"content-type": "application/json"},
        ),
    }

    for name, scripted_failure in scenarios.items():
        chain = [
            GrokCredentials(token="bad", auth_key="bad"),
            GrokCredentials(token="ok-zero", auth_key="ok-zero"),
            GrokCredentials(token="unused", auth_key="unused"),
        ]
        attempts: list[str] = []

        async def upstream_response(request):
            token = request.headers["Authorization"].removeprefix("Bearer ")
            attempts.append(token)
            if token == "bad":
                if scripted_failure is None:
                    raise app_module.httpx.ConnectError(
                        "scripted connection failure",
                        request=request,
                    )
                return app_module.httpx.Response(
                    scripted_failure.status_code,
                    request=request,
                    content=scripted_failure.content,
                    headers=scripted_failure.headers,
                )
            return app_module.httpx.Response(
                200,
                request=request,
                headers={"content-type": "text/event-stream"},
                text=(
                    'data: {"choices":[{"delta":{"content":"ok-zero"},'
                    '"finish_reason":"stop"}],"usage":{"completion_tokens":0}}'
                    '\n\ndata: [DONE]\n\n'
                ),
            )

        client = app_module.httpx.AsyncClient(
            transport=app_module.httpx.MockTransport(upstream_response)
        )

        failure_reporter = mock.Mock()
        usage_recorder = mock.Mock()

        async def run():
            try:
                with mock.patch.object(app_module, "_resolve_conversation_affinity", return_value=(None, None, None, False)), \
                     mock.patch.object(app_module, "_pick_account_chain_timed", return_value=(chain, 0.0)), \
                     mock.patch.object(app_module, "build_upstream_body", return_value={"model": "grok", "messages": []}), \
                     mock.patch.object(app_module, "get_http_client", return_value=client), \
                     mock.patch.object(app_module.account_pool, "report_success"), \
                     mock.patch.object(app_module, "_report_upstream_failure", new=failure_reporter), \
                     mock.patch.object(app_module, "_record_usage_safe", new=usage_recorder), \
                     mock.patch.object(app_module, "_note_account_pick"):
                    return await app_module.chat_completions(
                        app_module.ChatCompletionRequest(
                            model="grok",
                            messages=[{"role": "user", "content": "hi"}],
                        ),
                        fastapi_request("/v1/chat/completions"),
                        None,
                    )
            finally:
                await client.aclose()

        response = asyncio.run(run())
        content = json.loads(response.body)["choices"][0]["message"]["content"]
        ok(content == "ok-zero", f"{name} recovers with valid zero-token content")
        ok(
            attempts == ["bad", "ok-zero"],
            f"{name} retries once and does not use extra backup: {attempts}",
        )
        if name == "malformed":
            observations = repr(failure_reporter.call_args_list) + repr(usage_recorder.call_args_list)
            ok(parse_secret not in observations, "malformed-body observations hide upstream data")


def test_responses_reasoning_only_completes_without_retry_or_leak() -> None:
    print("[responses reasoning-only completes without retry or leak]")
    chain = [
        GrokCredentials(token="reasoning", auth_key="reasoning"),
        GrokCredentials(token="backup", auth_key="backup"),
    ]
    attempts: list[str] = []
    private_reasoning = "private reasoning must stay metadata-only"

    async def upstream_response(request):
        token = request.headers["Authorization"].removeprefix("Bearer ")
        attempts.append(token)
        if token == "reasoning":
            text = (
                'data: {"choices":[{"delta":{"reasoning_content":"'
                + private_reasoning
                + '"},"finish_reason":"stop"}],'
                '"usage":{"completion_tokens":0}}\n\ndata: [DONE]\n\n'
            )
        else:
            text = (
                'data: {"choices":[{"delta":{"content":"backup"},'
                '"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n'
            )
        return app_module.httpx.Response(
            200,
            request=request,
            text=text,
            headers={"content-type": "text/event-stream"},
        )

    client = app_module.httpx.AsyncClient(
        transport=app_module.httpx.MockTransport(upstream_response)
    )
    request_body = {"model": "grok", "input": "hi", "stream": True}

    async def run() -> str:
        try:
            with mock.patch.object(app_module, "_responses_affinity", return_value=(None, None, None, None, False)), \
                 mock.patch.object(app_module, "_pick_account_chain_timed", return_value=(chain, 0.0)), \
                 mock.patch.object(app_module.oai_resp, "responses_request_to_chat_body", return_value={"model": "grok", "messages": [{"role": "user", "content": "hi"}]}), \
                 mock.patch.object(app_module, "get_http_client", return_value=client), \
                 mock.patch.object(app_module.account_pool, "report_success"), \
                 mock.patch.object(app_module.conversation_affinity, "bind_response_chain"), \
                 mock.patch.object(app_module, "_report_upstream_failure"), \
                 mock.patch.object(app_module, "_record_usage_safe"), \
                 mock.patch.object(app_module, "_note_account_pick"):
                response = await app_module.openai_responses(
                    fastapi_request("/v1/responses", request_body),
                    None,
                )
                return await collect_stream(response.body_iterator)
        finally:
            await client.aclose()

    output = asyncio.run(run())
    ok(attempts == ["reasoning"], f"reasoning-only result does not use backup: {attempts}")
    ok("event: response.completed" in output, "reasoning-only stream completes")
    ok("event: response.failed" not in output, "reasoning-only stream is not failed")
    ok("response.output_text" not in output, "reasoning is not exposed as output_text")
    ok('"x_grok2api_reasoning": "' + private_reasoning + '"' in output, "reasoning stays in non-output metadata")
    ok("data: [DONE]" in output, "reasoning-only stream emits DONE")


def test_responses_does_not_retry_after_reasoning_then_read_error() -> None:
    print("[responses does not retry after reasoning then read error]")
    chain = [
        GrokCredentials(token="reasoning", auth_key="reasoning"),
        GrokCredentials(token="backup", auth_key="backup"),
    ]
    attempts: list[str] = []

    async def upstream_response(request):
        token = request.headers["Authorization"].removeprefix("Bearer ")
        attempts.append(token)
        if token == "reasoning":
            return app_module.httpx.Response(
                200,
                request=request,
                headers={"content-type": "text/event-stream"},
                stream=EventThenErrorStream(
                    'data: {"choices":[{"delta":{"reasoning_content":"private"}}]}\n\n'
                ),
            )
        return app_module.httpx.Response(
            200,
            request=request,
            headers={"content-type": "text/event-stream"},
            text=(
                'data: {"choices":[{"delta":{"content":"backup"},'
                '"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n'
            ),
        )

    client = app_module.httpx.AsyncClient(
        transport=app_module.httpx.MockTransport(upstream_response)
    )
    request_body = {"model": "grok", "input": "hi", "stream": True}

    async def run() -> str:
        try:
            with mock.patch.object(app_module, "_responses_affinity", return_value=(None, None, None, None, False)), \
                 mock.patch.object(app_module, "_pick_account_chain_timed", return_value=(chain, 0.0)), \
                 mock.patch.object(app_module.oai_resp, "responses_request_to_chat_body", return_value={"model": "grok", "messages": [{"role": "user", "content": "hi"}]}), \
                 mock.patch.object(app_module, "get_http_client", return_value=client), \
                 mock.patch.object(app_module.conversation_affinity, "bind_response_chain"), \
                 mock.patch.object(app_module, "_report_upstream_failure"), \
                 mock.patch.object(app_module, "_record_usage_safe"), \
                 mock.patch.object(app_module, "_note_account_pick"):
                response = await app_module.openai_responses(
                    fastapi_request("/v1/responses", request_body),
                    None,
                )
                return await collect_stream(response.body_iterator)
        finally:
            await client.aclose()

    output = asyncio.run(run())
    ok(attempts == ["reasoning"], f"reasoning result blocks backup retry: {attempts}")
    ok("backup" not in output, "backup account content is not spliced into response")
    ok(app_module.ALL_ACCOUNTS_FAILED_CODE not in output, "reasoning interruption is not mislabeled as exhaustion")


def test_confirmed_client_disconnect_does_not_retry() -> None:
    print("[confirmed client disconnect does not retry]")
    chain = [
        GrokCredentials(token="first", auth_key="first"),
        GrokCredentials(token="backup", auth_key="backup"),
    ]

    async def run_protocol(protocol: str) -> list[str]:
        attempts: list[str] = []

        async def upstream_response(request):
            token = request.headers["Authorization"].removeprefix("Bearer ")
            attempts.append(token)
            text = "data: [DONE]\n\n"
            if token == "backup":
                text = (
                    'data: {"choices":[{"delta":{"content":"backup"},'
                    '"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n'
                )
            return app_module.httpx.Response(
                200,
                request=request,
                text=text,
                headers={"content-type": "text/event-stream"},
            )

        client = app_module.httpx.AsyncClient(
            transport=app_module.httpx.MockTransport(upstream_response)
        )

        async def disconnected() -> bool:
            return True

        request = fastapi_request(
            "/v1/responses",
            {"model": "grok", "input": "hi", "stream": True},
        )
        request.is_disconnected = disconnected
        try:
            with mock.patch.dict(os.environ, {"GROK2API_DISCONNECT_HITS": "1"}), \
                 mock.patch.object(app_module, "get_http_client", return_value=client), \
                 mock.patch.object(app_module, "_responses_affinity", return_value=(None, None, None, None, False)), \
                 mock.patch.object(app_module, "_pick_account_chain_timed", return_value=(chain, 0.0)), \
                 mock.patch.object(app_module.oai_resp, "responses_request_to_chat_body", return_value={"model": "grok", "messages": [{"role": "user", "content": "hi"}]}), \
                 mock.patch.object(app_module.conversation_affinity, "bind_response_chain"), \
                 mock.patch.object(app_module, "_report_upstream_failure"), \
                 mock.patch.object(app_module, "_record_usage_safe"), \
                 mock.patch.object(app_module, "_note_account_pick"):
                if protocol == "chat":
                    await collect_stream(
                        app_module._stream_proxy_with_failover_inner(
                            url="https://upstream.test/chat/completions",
                            body={"model": "grok", "messages": []},
                            chain=chain,
                            chat_id="chat_test",
                            model="grok",
                            created=1,
                            client_disconnected=disconnected,
                        )
                    )
                elif protocol == "messages":
                    await collect_stream(
                        app_module._stream_anthropic_with_failover_inner(
                            url="https://upstream.test/chat/completions",
                            body={"model": "grok", "messages": []},
                            chain=chain,
                            message_id="msg_test",
                            model="grok",
                            client_disconnected=disconnected,
                        )
                    )
                else:
                    response = await app_module.openai_responses(request, None)
                    await collect_stream(response.body_iterator)
        finally:
            await client.aclose()
        return attempts

    for protocol in ("chat", "messages", "responses"):
        attempts = asyncio.run(run_protocol(protocol))
        ok(attempts == ["first"], f"{protocol} disconnect stops before backup: {attempts}")


def test_admin_ui_uses_upstream_retry_count() -> None:
    print("[admin UI uses upstream retry count]")
    html = (ROOT / "static" / "admin" / "settings.html").read_text(encoding="utf-8")
    js = (ROOT / "static" / "js" / "core.js").read_text(encoding="utf-8")
    ok('id="set-upstream-retry-count"' in html, "settings page has retry count input")
    ok('min="0" max="63"' in html, "retry input accepts 0 through 63")
    ok("上游报错自动换号重试次数" in html, "settings page names automatic account retry")
    ok(
        "设为 3 时，最多尝试 4 个不同账号；0 表示不重试" in html,
        "settings page explains N+1 attempts and zero",
    )
    ok("pol.upstream_retry_count" in js, "settings loader reads upstream_retry_count")
    ok(
        "Number(pol.max_failover_attempts) - 1" in js,
        "settings loader converts legacy total attempts",
    )
    ok("patch.upstream_retry_count" in js, "settings saver writes upstream_retry_count")
    ok("patch.max_failover_attempts" not in js, "settings saver no longer writes legacy key")


def main() -> int:
    tests = [
        test_new_retry_count_wins_including_zero,
        test_legacy_attempt_count_is_converted,
        test_missing_settings_use_default,
        test_sticky_chain_honors_zero_retries,
        test_sticky_chain_falls_back_when_warm_backups_are_incomplete,
        test_three_retries_select_four_distinct_accounts,
        test_max_retry_count_selects_sixty_four_accounts,
        test_all_upstream_http_errors_are_retryable,
        test_chat_endpoint_retries_upstream_300,
        test_chat_endpoint_exhaustion_is_standard_error,
        test_messages_endpoint_exhaustion_is_standard_error,
        test_responses_endpoint_exhaustion_is_standard_error,
        test_messages_stream_retries_empty_before_model_output,
        test_responses_stream_retries_empty_before_model_output,
        test_chat_stream_exhaustion_is_standard_error,
        test_messages_stream_exhaustion_is_standard_error,
        test_responses_stream_exhaustion_is_standard_error,
        test_chat_stream_does_not_retry_after_model_output,
        test_messages_stream_does_not_retry_after_model_output,
        test_responses_stream_does_not_retry_after_model_output,
        test_client_cancellation_does_not_retry,
        test_chat_retries_transport_parse_and_empty_failures,
        test_responses_reasoning_only_completes_without_retry_or_leak,
        test_responses_does_not_retry_after_reasoning_then_read_error,
        test_confirmed_client_disconnect_does_not_retry,
        test_admin_ui_uses_upstream_retry_count,
    ]
    failed = 0
    for test in tests:
        try:
            test()
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {test.__name__}: {exc}")
    if failed:
        print(f"\n{failed}/{len(tests)} failed")
        return 1
    print(f"\nall {len(tests)} passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
