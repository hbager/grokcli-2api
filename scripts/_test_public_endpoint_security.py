#!/usr/bin/env python3
"""Security regression tests for public status, health, and settings payloads."""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient
from starlette.requests import Request
from starlette.responses import Response


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SYNTHETIC_SECRET = "synthetic-public-endpoint-secret-9f52c7"


def _request(path: str, *, scheme: str = "https", headers: dict[str, str] | None = None) -> Request:
    raw_headers = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "scheme": scheme,
            "path": path,
            "raw_path": path.encode(),
            "query_string": b"",
            "headers": raw_headers,
            "client": ("203.0.113.10", 43123),
            "server": ("example.test", 443 if scheme == "https" else 80),
        }
    )


def _json_text(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def test_public_status_is_allowlisted() -> None:
    from grok2api.admin import admin_routes as routes

    store_status = {
        "backend": "hybrid",
        "redis": {"ok": True},
        "postgres": {"ok": True},
        "internal": SYNTHETIC_SECRET,
    }
    with (
        patch.object(routes, "is_setup_needed", return_value=False),
        patch.object(routes.accounts, "account_status", return_value={"secret": SYNTHETIC_SECRET}),
        patch.object(routes.apikeys, "stats", return_value={"secret": SYNTHETIC_SECRET}),
        patch.object(routes.account_pool, "pool_summary", return_value={"secret": SYNTHETIC_SECRET}),
        patch.object(routes, "get_public_settings", return_value={"api_key": SYNTHETIC_SECRET}),
        patch.object(routes, "load_models_from_cache", return_value=[]),
        patch.object(routes, "_usage_light", return_value={"secret": SYNTHETIC_SECRET}),
        patch("grok2api.store.store_status", return_value=store_status),
    ):
        payload = asyncio.run(routes.admin_status(_request("/admin/api/status")))

    assert set(payload) == {"ok", "setup_needed", "version"}, sorted(payload)
    assert SYNTHETIC_SECRET not in _json_text(payload)


def test_public_health_is_allowlisted() -> None:
    from grok2api import app as app_module

    creds = SimpleNamespace(
        email="private@example.test",
        expires_at=9_999_999_999,
        auth_key=SYNTHETIC_SECRET,
    )
    with (
        patch.object(app_module.account_pool, "pool_summary", return_value={"secret": SYNTHETIC_SECRET}),
        patch.object(app_module.account_pool, "acquire", return_value=creds),
        patch("grok2api.store.store_status", return_value={"multi_worker_ready": True}),
        patch("grok2api.store.leader.status", return_value={"leader_id": SYNTHETIC_SECRET}),
    ):
        payload = asyncio.run(app_module.health())

    assert isinstance(payload, dict), type(payload)
    assert set(payload) == {"status", "version"}, sorted(payload)
    assert payload["status"] == "ok"
    assert SYNTHETIC_SECRET not in _json_text(payload)


def test_public_settings_request_redacted_configs() -> None:
    from grok2api.admin import settings_store as settings

    calls: dict[str, list[bool]] = {"registration": [], "outbound": []}

    def registration(*, include_secrets: bool = True) -> dict[str, object]:
        calls["registration"].append(include_secrets)
        if include_secrets:
            return {"api_key": SYNTHETIC_SECRET, "proxy_password": SYNTHETIC_SECRET}
        return {
            "api_key": "synt…52c7",
            "api_key_set": True,
            "proxy": "socks5://proxy.example:1080",
            "proxy_set": True,
            "proxy_password": "synt…52c7",
        }

    enabled = True

    def outbound(*, include_secrets: bool = True) -> dict[str, object]:
        calls["outbound"].append(include_secrets)
        if include_secrets:
            return {"enabled": enabled, "proxy_password": SYNTHETIC_SECRET}
        return {"enabled": enabled, "proxy_password": "synt…52c7", "proxy_password_set": True}

    with (
        patch.object(settings, "_load", return_value={}),
        patch.object(settings, "_get_setting_value", side_effect=lambda _key, default=None: default),
        patch.object(settings, "get_registration_config", side_effect=registration),
        patch.object(settings, "get_outbound_proxy_config", side_effect=outbound),
        patch.object(settings, "has_admin_password", return_value=True),
        patch.object(settings, "is_setup_needed", return_value=False),
        patch.object(settings, "_stored_admin_hash_present", return_value=True),
    ):
        payload = settings.get_public_settings()

    assert calls == {"registration": [False], "outbound": [False]}, calls
    assert SYNTHETIC_SECRET not in _json_text(payload)
    assert payload["outbound_proxy_pool"]["source"] == "registration"
    assert payload["outbound_proxy_pool"]["count"] == 1
    assert payload["outbound_proxy_pool"]["enabled"] is True

    enabled = False
    calls["registration"].clear()
    calls["outbound"].clear()
    with (
        patch.object(settings, "_load", return_value={}),
        patch.object(settings, "_get_setting_value", side_effect=lambda _key, default=None: default),
        patch.object(settings, "get_registration_config", side_effect=registration),
        patch.object(settings, "get_outbound_proxy_config", side_effect=outbound),
        patch.object(settings, "has_admin_password", return_value=True),
        patch.object(settings, "is_setup_needed", return_value=False),
        patch.object(settings, "_stored_admin_hash_present", return_value=True),
    ):
        disabled_payload = settings.get_public_settings()
    assert disabled_payload["outbound_proxy_pool"]["enabled"] is False


def test_proxy_text_masks_embedded_passwords() -> None:
    from grok2api.admin import settings_store as settings

    raw = (
        f"socks5://alice:{SYNTHETIC_SECRET}@proxy.example:1080\n"
        f"proxy2.example:8080:bob:{SYNTHETIC_SECRET}"
    )
    with patch.object(
        settings,
        "_get_setting_value",
        return_value={"enabled": True, "proxy": raw, "proxy_strategy": "round_robin"},
    ):
        public = settings.get_outbound_proxy_config(include_secrets=False)

    assert SYNTHETIC_SECRET not in _json_text(public)
    assert public["proxy_set"] is True


def test_registration_proxy_masks_embedded_password_and_preserves_saved_value() -> None:
    from grok2api.admin import settings_store as settings

    raw = f"socks5://alice:{SYNTHETIC_SECRET}@proxy.example:1080"
    stored = {"mail_provider": "yyds", "proxy": raw, "proxy_strategy": "round_robin"}
    with patch.object(settings, "_get_setting_value", return_value=stored):
        public = settings.get_registration_config(include_secrets=False)
        resolved = settings.resolve_registration_inputs({"proxy": public["proxy"]})

    assert SYNTHETIC_SECRET not in _json_text(public)
    assert public["proxy_set"] is True
    assert resolved["proxy"] == raw


def test_health_failure_is_generic() -> None:
    from grok2api import app as app_module

    with patch("grok2api.store.store_status", side_effect=RuntimeError(SYNTHETIC_SECRET)):
        response = asyncio.run(app_module.health())

    assert response.status_code == 503
    payload = json.loads(response.body)
    assert payload == {"status": "unavailable", "version": app_module.APP_VERSION}
    assert SYNTHETIC_SECRET not in _json_text(payload)


def test_health_allows_explicit_single_worker_file_mode() -> None:
    from grok2api import app as app_module

    with (
        patch.object(app_module._config, "REQUIRE_SHARED_STORES", False),
        patch.object(app_module._config, "STORE_BACKEND", "file"),
        patch.object(app_module._config, "WORKERS", 1),
        patch.object(app_module.account_pool, "acquire", return_value=SimpleNamespace()),
        patch("grok2api.store.store_status", return_value={"multi_worker_ready": False}),
    ):
        payload = asyncio.run(app_module.health())

    assert payload == {"status": "ok", "version": app_module.APP_VERSION}


def test_masked_env_registration_secret_survives_save() -> None:
    from grok2api.admin import settings_store as settings

    env_cfg = {
        "mail_provider": "yyds",
        "api_key": SYNTHETIC_SECRET,
        "yyds_api_key": SYNTHETIC_SECRET,
    }
    saved: dict[str, object] = {}
    with (
        patch.object(settings, "_get_setting_value", return_value={}),
        patch.object(settings, "_env_registration_defaults", return_value=env_cfg),
        patch.object(settings, "_set_setting_value", side_effect=lambda _key, value: saved.update(value)),
        patch.object(settings, "apply_registration_config_to_runtime"),
    ):
        public = settings.get_registration_config(include_secrets=False)
        settings.set_registration_config(
            {
                "mail_provider": "yyds",
                "api_key": public["api_key"],
                "yyds_api_key": public["yyds_api_key"],
            }
        )

    assert saved["api_key"] == SYNTHETIC_SECRET
    assert saved["yyds_api_key"] == SYNTHETIC_SECRET


def test_inactive_env_registration_key_survives_empty_field_save() -> None:
    from grok2api.admin import settings_store as settings

    env_cfg = {
        "mail_provider": "moemail",
        "yyds_api_key": SYNTHETIC_SECRET,
    }
    saved: dict[str, object] = {}
    with (
        patch.object(settings, "_get_setting_value", return_value={}),
        patch.object(settings, "_env_registration_defaults", return_value=env_cfg),
        patch.object(settings, "_set_setting_value", side_effect=lambda _key, value: saved.update(value)),
        patch.object(settings, "apply_registration_config_to_runtime"),
    ):
        settings.set_registration_config(
            {"mail_provider": "moemail", "yyds_api_key": ""}
        )

    assert saved["yyds_api_key"] == SYNTHETIC_SECRET


def test_inactive_captcha_key_survives_empty_field_save() -> None:
    from grok2api.admin import settings_store as settings

    stored = {
        "mail_provider": "yyds",
        "captcha_provider": "local",
        "yescaptcha_key": SYNTHETIC_SECRET,
    }
    saved: dict[str, object] = {}
    with (
        patch.object(settings, "_get_setting_value", return_value=stored),
        patch.object(settings, "_set_setting_value", side_effect=lambda _key, value: saved.update(value)),
        patch.object(settings, "apply_registration_config_to_runtime"),
    ):
        settings.set_registration_config(
            {"captcha_provider": "local", "yescaptcha_key": ""}
        )
    assert saved["yescaptcha_key"] == SYNTHETIC_SECRET

    saved.clear()
    with (
        patch.object(settings, "_get_setting_value", return_value=stored),
        patch.object(settings, "_set_setting_value", side_effect=lambda _key, value: saved.update(value)),
        patch.object(settings, "apply_registration_config_to_runtime"),
    ):
        settings.set_registration_config(
            {"captcha_provider": "yescaptcha", "yescaptcha_key": ""}
        )
    assert saved["yescaptcha_key"] == ""


def test_registration_start_can_clear_active_captcha_key() -> None:
    from grok2api.admin import settings_store as settings

    stored = {
        "mail_provider": "yyds",
        "captcha_provider": "yescaptcha",
        "yescaptcha_key": SYNTHETIC_SECRET,
    }
    with patch.object(settings, "_get_setting_value", return_value=stored):
        resolved = settings.resolve_registration_inputs(
            {"captcha_provider": "yescaptcha", "yescaptcha_key": ""}
        )
        local = settings.resolve_registration_inputs(
            {"captcha_provider": "local", "yescaptcha_key": ""}
        )
    assert resolved["yescaptcha_key"] == ""
    assert local["yescaptcha_key"] == SYNTHETIC_SECRET

    core_source = Path("static/js/core.js").read_text(encoding="utf-8")
    build_start = core_source.index("function buildRegBody(config)")
    build_end = core_source.index("function buildProxyTestBody", build_start)
    build_source = core_source[build_start:build_end]
    assert "body.yescaptcha_key = config.yescaptcha_key == null ? \"\"" in build_source
    assert "else if (config.yescaptcha_key)" not in build_source


def test_masked_env_outbound_proxy_survives_save() -> None:
    from grok2api.admin import settings_store as settings

    env_cfg = {
        "enabled": True,
        "proxy": f"socks5://alice:{SYNTHETIC_SECRET}@proxy.example:1080",
        "proxy_password": SYNTHETIC_SECRET,
        "proxy_strategy": "round_robin",
    }
    saved: dict[str, object] = {}
    with (
        patch.object(settings, "_get_setting_value", return_value={}),
        patch.object(settings, "_env_outbound_proxy_defaults", return_value=env_cfg),
        patch.object(settings, "_set_setting_value", side_effect=lambda _key, value: saved.update(value)),
        patch.object(settings, "apply_outbound_proxy_config_to_runtime"),
    ):
        public = settings.get_outbound_proxy_config(include_secrets=False)
        settings.set_outbound_proxy_config(
            {
                "proxy": public["proxy"],
                "proxy_password": public["proxy_password"],
            }
        )

    assert SYNTHETIC_SECRET in str(saved["proxy"])
    assert saved["proxy_password"] == SYNTHETIC_SECRET


def test_masked_proxy_pool_supports_incremental_edits() -> None:
    from grok2api.admin import settings_store as settings

    original = (
        f"http://alice:{SYNTHETIC_SECRET}@proxy-a.example:8080\n"
        "http://proxy-b.example:8080"
    )

    registration_saved: dict[str, object] = {}
    with (
        patch.object(settings, "_get_setting_value", return_value={"proxy": original}),
        patch.object(settings, "_set_setting_value", side_effect=lambda _key, value: registration_saved.update(value)),
        patch.object(settings, "apply_registration_config_to_runtime"),
    ):
        public = settings.get_registration_config(include_secrets=False)
        edited = public["proxy"] + "\nhttp://proxy-c.example:8080"
        settings.set_registration_config({"proxy": edited})

    assert SYNTHETIC_SECRET in str(registration_saved["proxy"])
    assert "proxy-c.example:8080" in str(registration_saved["proxy"])

    outbound_saved: dict[str, object] = {}
    with (
        patch.object(settings, "_get_setting_value", return_value={"enabled": True, "proxy": original}),
        patch.object(settings, "_set_setting_value", side_effect=lambda _key, value: outbound_saved.update(value)),
        patch.object(settings, "apply_outbound_proxy_config_to_runtime"),
    ):
        public = settings.get_outbound_proxy_config(include_secrets=False)
        credentialed_only = str(public["proxy"]).splitlines()[0]
        settings.set_outbound_proxy_config({"proxy": credentialed_only})

    assert SYNTHETIC_SECRET in str(outbound_saved["proxy"])
    assert "proxy-b.example" not in str(outbound_saved["proxy"])

    duplicate_display_original = (
        "http://alice:first-secret@same.example:8080\n"
        "http://alice:second-secret@same.example:8080"
    )
    with patch.object(
        settings,
        "_get_setting_value",
        return_value={"proxy": duplicate_display_original},
    ):
        public = settings.get_registration_config(include_secrets=False)
        resolved = settings.resolve_registration_inputs({"proxy": public["proxy"]})
    assert "first-secret" in resolved["proxy"]
    assert "second-secret" in resolved["proxy"]
    assert len(str(resolved["proxy"]).splitlines()) == 2

    duplicate_masked_line = str(public["proxy"]).splitlines()[0]
    with patch.object(
        settings,
        "_get_setting_value",
        return_value={"proxy": duplicate_display_original},
    ):
        try:
            settings.resolve_registration_inputs({"proxy": duplicate_masked_line})
        except ValueError:
            pass
        else:
            raise AssertionError("ambiguous masked proxy deletion must require full credentials")

    with patch.object(settings, "_get_setting_value", return_value={"proxy": original}):
        public = settings.get_registration_config(include_secrets=False)
        changed_host = str(public["proxy"]).replace("proxy-a.example", "other.example")
        try:
            settings.set_registration_config({"proxy": changed_host})
        except ValueError:
            pass
        else:
            raise AssertionError("changed masked proxy host must require full credentials")


def test_registration_proxy_password_can_be_cleared() -> None:
    from grok2api.admin import settings_store as settings

    stored = {
        "mail_provider": "yyds",
        "proxy": "http://proxy.example:8080",
        "proxy_username": "old-user",
        "proxy_password": "old-password",
    }
    saved: dict[str, object] = {}
    with (
        patch.object(settings, "_get_setting_value", return_value=stored),
        patch.object(settings, "_set_setting_value", side_effect=lambda _key, value: saved.update(value)),
        patch.object(settings, "apply_registration_config_to_runtime"),
    ):
        settings.set_registration_config(
            {"proxy": "", "proxy_username": "", "proxy_password": ""}
        )

    assert saved["proxy"] == ""
    assert saved["proxy_username"] == ""
    assert saved["proxy_password"] == ""


def test_settings_frontend_sends_empty_outbound_proxy_password() -> None:
    core_source = Path("static/js/core.js").read_text(encoding="utf-8")
    collect_start = core_source.index("function collectSystemSettingsPatch()")
    collect_end = core_source.index("function countOutboundProxyLines", collect_start)
    collect_source = core_source[collect_start:collect_end]
    assert 'patch.outbound_proxy_password = pw;' in collect_source
    assert 'if (pw) patch.outbound_proxy_password' not in collect_source


def test_health_requires_an_available_account() -> None:
    from grok2api import app as app_module

    with (
        patch("grok2api.store.store_status", return_value={"multi_worker_ready": True}),
        patch.object(
            app_module.account_pool,
            "acquire",
            side_effect=app_module.AuthError("synthetic unavailable account detail"),
        ),
    ):
        response = asyncio.run(app_module.health())

    assert response.status_code == 503
    assert json.loads(response.body) == {
        "status": "unavailable",
        "version": app_module.APP_VERSION,
    }


def test_active_registration_key_edit_ignores_masked_provider_slot() -> None:
    from grok2api.admin import settings_store as settings

    stored = {
        "mail_provider": "yyds",
        "api_key": "old-provider-secret",
        "yyds_api_key": "old-provider-secret",
    }
    with patch.object(settings, "_get_setting_value", return_value=stored):
        public = settings.get_registration_config(include_secrets=False)
    public["api_key"] = "new-provider-secret"

    saved: dict[str, object] = {}
    with (
        patch.object(settings, "_get_setting_value", return_value=stored),
        patch.object(settings, "_set_setting_value", side_effect=lambda _key, value: saved.update(value)),
        patch.object(settings, "apply_registration_config_to_runtime"),
    ):
        settings.set_registration_config(public)

    assert saved["api_key"] == "new-provider-secret"
    assert saved["yyds_api_key"] == "new-provider-secret"


def test_cleared_outbound_proxy_auth_is_removed_from_runtime_env() -> None:
    from grok2api.admin import settings_store as settings
    from grok2api import config

    env_names = (
        "GROK2API_XAI_PROXY_USERNAME",
        "GROK2API_XAI_PROXY_PASSWORD",
    )
    old_config = (config.XAI_PROXY_USERNAME, config.XAI_PROXY_PASSWORD)
    with patch.dict(
        os.environ,
        {
            env_names[0]: "stale-user",
            env_names[1]: "stale-password",
        },
        clear=False,
    ):
        config.XAI_PROXY_USERNAME = "stale-user"
        config.XAI_PROXY_PASSWORD = "stale-password"
        try:
            settings.apply_outbound_proxy_config_to_runtime(
                {
                    "enabled": True,
                    "proxy": "http://proxy.example:8080",
                    "proxy_username": "",
                    "proxy_password": "",
                    "proxy_strategy": "round_robin",
                }
            )
            assert env_names[0] not in os.environ
            assert env_names[1] not in os.environ
            assert config.XAI_PROXY_USERNAME == ""
            assert config.XAI_PROXY_PASSWORD == ""
        finally:
            config.XAI_PROXY_USERNAME, config.XAI_PROXY_PASSWORD = old_config


def test_models_endpoint_supplies_protected_health_contract() -> None:
    from grok2api.admin import admin_routes as routes

    with (
        patch.object(routes, "require_admin"),
        patch.object(routes, "load_models_from_cache", return_value=[]),
        patch.object(routes.model_health, "status", return_value={"enabled": False}),
        patch.object(routes, "get_public_settings", return_value={"model_health_enabled": False}),
    ):
        payload = asyncio.run(routes.admin_models(_request("/admin/api/models")))

    assert payload["model_health"] == {"enabled": False}
    assert payload["settings"] == {"model_health_enabled": False}

    core_source = Path("static/js/core.js").read_text(encoding="utf-8")
    load_start = core_source.index("async function loadModels()")
    load_end = core_source.index("function renderModels()", load_start)
    load_source = core_source[load_start:load_end]
    assert "r.model_health" in load_source
    assert "r.settings" in load_source


def test_registration_put_only_forwards_submitted_fields() -> None:
    from grok2api.admin import admin_routes as routes

    body = routes.RegistrationConfigBody(
        mail_provider="yyds",
        api_key="****",
    )
    captured: dict[str, object] = {}
    with (
        patch.object(routes, "require_admin"),
        patch.object(routes, "set_registration_config", side_effect=lambda patch, replace=False: captured.update(patch)),
        patch.object(routes, "get_registration_config", return_value={"api_key": "****"}),
        patch.object(routes, "audit_log"),
    ):
        asyncio.run(
            routes.put_email_registration_config(
                body,
                _request("/admin/api/accounts/register-email/config"),
                None,
            )
        )

    assert captured == {"mail_provider": "yyds", "api_key": "****"}


def test_diagnostics_are_not_anonymous() -> None:
    from grok2api import app as app_module
    from grok2api.admin import admin_routes as routes

    with patch.object(routes, "is_setup_needed", return_value=False):
        client = TestClient(app_module.app)
        try:
            assert client.get("/metrics").status_code == 401
            assert client.get("/docs").status_code == 404
            assert client.get("/redoc").status_code == 404
            assert client.get("/openapi.json").status_code == 404
            response = client.get(
                "/admin/api/status",
                headers={"Origin": "https://attacker.example"},
            )
        finally:
            client.close()

    assert response.status_code == 200
    assert "access-control-allow-origin" not in response.headers


def test_metrics_accepts_only_a_valid_key() -> None:
    from grok2api import app as app_module

    with patch.object(
        app_module.apikeys,
        "verify_key",
        return_value=SimpleNamespace(id="test"),
    ) as verify:
        client = TestClient(app_module.app)
        try:
            response = client.get(
                "/metrics",
                headers={"Authorization": "Bearer synthetic-valid-key"},
            )
        finally:
            client.close()

    assert response.status_code == 200
    verify.assert_called_once_with("synthetic-valid-key")


def test_admin_cookie_is_secure_for_external_https() -> None:
    from grok2api.admin import admin_routes as routes

    cases = [
        (_request("/admin/api/login", scheme="https"), True),
        (
            _request(
                "/admin/api/login",
                scheme="http",
                headers={"X-Forwarded-Proto": "https"},
            ),
            True,
        ),
        (_request("/admin/api/login", scheme="http"), False),
    ]
    for request, secure in cases:
        response = Response()
        routes._set_admin_cookie(response, "synthetic-session", request)
        cookie = response.headers["set-cookie"].lower()
        assert "httponly" in cookie
        assert "samesite=lax" in cookie
        assert ("secure" in cookie) is secure


def test_admin_frontend_uses_minimal_public_bootstrap() -> None:
    auth_source = Path("static/js/auth.js").read_text(encoding="utf-8")
    core_source = Path("static/js/core.js").read_text(encoding="utf-8")
    refresh_start = core_source.index("async function refreshOverviewStatus")
    refresh_end = core_source.index("async function loadDashboard()", refresh_start)
    refresh_source = core_source[refresh_start:refresh_end]
    auto_start = core_source.index("function startAutoUiRefresh()")
    auto_end = core_source.index("function renderGuide()", auto_start)
    auto_source = core_source[auto_start:auto_end]
    load_start = core_source.index("async function loadDashboard()")
    load_end = core_source.index("function fmtNum", load_start)
    load_source = core_source[load_start:load_end]

    assert "fetchHealth" not in auth_source
    assert "renderConnList" not in auth_source
    assert 'await api("/dashboard")' in refresh_source
    assert 'await api("/status")' not in refresh_source
    assert 'await api("/dashboard")' in auto_source
    assert 'await api("/status")' not in auto_source
    guide_branch = load_source[load_source.index('page === "guide"'):]
    assert 'await api("/dashboard")' in guide_branch
    assert "renderGuide()" in guide_branch


def test_cors_origin_config_rejects_non_origins() -> None:
    from grok2api import config

    with patch.dict(
        os.environ,
        {"GROK2API_CORS_ORIGINS": "https://admin.example,http://localhost:40081/,http://[::1]"},
    ):
        assert config._cors_origins() == [
            "https://admin.example",
            "http://localhost:40081",
            "http://[::1]",
        ]

    for invalid in (
        "*",
        "https://admin.example/path",
        "https://admin.example?leak=1",
        "https://admin.example:",
    ):
        with patch.dict(os.environ, {"GROK2API_CORS_ORIGINS": invalid}):
            try:
                config._cors_origins()
            except ValueError:
                pass
            else:
                raise AssertionError(f"invalid CORS origin accepted: {invalid}")


def main() -> int:
    tests = [
        test_public_status_is_allowlisted,
        test_public_health_is_allowlisted,
        test_public_settings_request_redacted_configs,
        test_proxy_text_masks_embedded_passwords,
        test_registration_proxy_masks_embedded_password_and_preserves_saved_value,
        test_health_failure_is_generic,
        test_health_allows_explicit_single_worker_file_mode,
        test_masked_env_registration_secret_survives_save,
        test_inactive_env_registration_key_survives_empty_field_save,
        test_inactive_captcha_key_survives_empty_field_save,
        test_registration_start_can_clear_active_captcha_key,
        test_masked_env_outbound_proxy_survives_save,
        test_masked_proxy_pool_supports_incremental_edits,
        test_registration_proxy_password_can_be_cleared,
        test_settings_frontend_sends_empty_outbound_proxy_password,
        test_health_requires_an_available_account,
        test_active_registration_key_edit_ignores_masked_provider_slot,
        test_cleared_outbound_proxy_auth_is_removed_from_runtime_env,
        test_models_endpoint_supplies_protected_health_contract,
        test_registration_put_only_forwards_submitted_fields,
        test_diagnostics_are_not_anonymous,
        test_metrics_accepts_only_a_valid_key,
        test_admin_cookie_is_secure_for_external_https,
        test_admin_frontend_uses_minimal_public_bootstrap,
        test_cors_origin_config_rejects_non_origins,
    ]
    for test in tests:
        test()
        print("PASS", test.__name__)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
