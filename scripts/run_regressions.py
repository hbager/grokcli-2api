#!/usr/bin/env python3
"""Compatibility / contract checks after Python public main removal.

Legacy scripts/_test_*.py regressions were deleted with the Python public API.
This runner keeps CI green by validating the remaining contract artifacts and
version pin files that release automation still depends on.
"""

from __future__ import annotations

import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"FAIL: {msg}", file=sys.stderr)
    return 1


def main() -> int:
    if sys.flags.optimize:
        print("regressions require assertions; do not use python -O", file=sys.stderr)
        return 2

    # Contract schema
    try:
        import jsonschema  # type: ignore
    except ImportError:
        return _fail("jsonschema not installed (pip install jsonschema)")

    root = ROOT / "contracts"
    schema = json.loads((root / "manifest.schema.json").read_text(encoding="utf-8"))
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    jsonschema.validate(manifest, schema)
    print("ok contracts/manifest.json schema")

    env_manifest = json.loads((root / "env-manifest.json").read_text(encoding="utf-8"))
    runtime = next(
        (e for e in env_manifest.get("env", env_manifest.get("variables", [])) if e.get("name") == "GROK2API_RUNTIME"),
        None,
    )
    # Support both top-level shapes used historically.
    if runtime is None and isinstance(env_manifest.get("variables"), list):
        runtime = next((e for e in env_manifest["variables"] if e.get("name") == "GROK2API_RUNTIME"), None)
    if runtime is None and isinstance(env_manifest.get("env"), list):
        runtime = next((e for e in env_manifest["env"] if e.get("name") == "GROK2API_RUNTIME"), None)
    # Fall back: scan any list of dicts under common keys.
    if runtime is None:
        for key, val in env_manifest.items():
            if isinstance(val, list):
                for item in val:
                    if isinstance(item, dict) and item.get("name") == "GROK2API_RUNTIME":
                        runtime = item
                        break
            if runtime is not None:
                break
    if runtime is None:
        return _fail("GROK2API_RUNTIME missing from env-manifest.json")
    values = runtime.get("values") or []
    if values != ["go"]:
        return _fail(f"GROK2API_RUNTIME values must be ['go'], got {values!r}")
    print("ok GROK2API_RUNTIME is go-only")

    # Version pins: grok2api/app.py APP_VERSION == internal/buildinfo.Version
    py = (ROOT / "grok2api" / "app.py").read_text(encoding="utf-8")
    go = (ROOT / "internal" / "buildinfo" / "buildinfo.go").read_text(encoding="utf-8")
    pv = re.search(r'APP_VERSION\s*=\s*"([^"]+)"', py)
    gv = re.search(r'Version\s*=\s*"([^"]+)"', go)
    if not pv or not gv:
        return _fail("could not parse APP_VERSION / Version")
    if pv.group(1) != gv.group(1):
        return _fail(f"version mismatch python={pv.group(1)} go={gv.group(1)}")
    print(f"ok version pin {pv.group(1)}")

    # Sidecar modules still importable (no full FastAPI public app required)
    sys.path.insert(0, str(ROOT))
    try:
        from grok2api.admin import settings_store, sso_import  # noqa: F401
        from grok2api.upstream import grok_build_adapter, oidc_auth, proxy_pool  # noqa: F401
        import scripts.registration_service as regsvc  # noqa: F401
    except Exception as exc:  # pragma: no cover
        return _fail(f"sidecar import failed: {exc}")
    print("ok sidecar imports (sso_import, grok_build_adapter, registration_service)")

    # Explicitly disabled outbound proxy settings override env/auto discovery.
    original_get_outbound = settings_store.get_outbound_proxy_config
    original_env_proxy = proxy_pool._env_proxy_text
    original_first_working = proxy_pool.first_working_proxy
    try:
        settings_store.get_outbound_proxy_config = lambda **_: {"enabled": False}
        proxy_pool._env_proxy_text = lambda: "http://env-proxy.example:8080"
        proxy_pool.first_working_proxy = lambda *_, **__: "http://auto-proxy.example:8080"
        with proxy_pool._lock:
            proxy_pool._outbound_proxy_cache_key = None
            proxy_pool._outbound_proxy_cache_value = None
        disabled_proxy = proxy_pool.get_outbound_proxy_source()
    finally:
        settings_store.get_outbound_proxy_config = original_get_outbound
        proxy_pool._env_proxy_text = original_env_proxy
        proxy_pool.first_working_proxy = original_first_working
        with proxy_pool._lock:
            proxy_pool._outbound_proxy_cache_key = None
            proxy_pool._outbound_proxy_cache_value = None
    if (
        disabled_proxy.get("enabled") is not False
        or disabled_proxy.get("source") != "settings"
        or disabled_proxy.get("pool")
    ):
        return _fail(f"disabled outbound proxy was overridden: {disabled_proxy!r}")
    print("ok explicit outbound proxy disable precedence")

    # Device-login polling exposes a strict projection, never raw provider output.
    device_secrets = {"device-secret", "access-secret", "refresh-secret"}
    try:
        public_device = oidc_auth._public_device_session(
            {
                "id": "device-1",
                "status": "success",
                "device_code": "device-secret",
                "access_token": "access-secret",
                "refresh_token": "refresh-secret",
                "output": json.dumps(
                    {
                        "device_code": "device-secret",
                        "access_token": "access-secret",
                        "refresh_token": "refresh-secret",
                    }
                ),
            },
            "device-1",
        )
    except AttributeError:
        return _fail("device session public projection missing")
    encoded_device = json.dumps(public_device, ensure_ascii=False)
    leaked_device = sorted(value for value in device_secrets if value in encoded_device)
    if leaked_device or "output_tail" in public_device:
        return _fail(f"device session leaked secrets: {leaked_device!r}")
    try:
        public_error = oidc_auth._public_device_poll_error(400, "device_code=SECRET")
    except AttributeError:
        return _fail("device poll public error projection missing")
    if "SECRET" in public_error or "device_code" in public_error:
        return _fail(f"device poll error leaked provider content: {public_error!r}")
    refresh_error = oidc_auth._summarize_refresh_error_body(
        400,
        json.dumps(
            {
                "error": "invalid_grant",
                "error_description": "refresh_token=SECRET",
            }
        ),
    )
    if "SECRET" in refresh_error or "refresh_token" in refresh_error:
        return _fail(f"OIDC error summary leaked provider content: {refresh_error!r}")
    if "invalid_grant" not in refresh_error:
        return _fail(f"OIDC error summary lost safe error code: {refresh_error!r}")
    oidc_source = (ROOT / "grok2api" / "upstream" / "oidc_auth.py").read_text(
        encoding="utf-8"
    )
    forbidden_device_output = (
        '"output": json.dumps(data',
        'sess["output"]',
        '"output_tail"',
        "unexpected device response: {data}",
        "error_description",
        "body_text[:200]",
    )
    present_device = [item for item in forbidden_device_output if item in oidc_source]
    if present_device:
        return _fail(f"device flow retains raw provider output: {present_device!r}")
    print("ok device session public state redaction")

    # Registration runtime state keeps secrets internally for resume, but public
    # session/batch projections must never expose them.
    secret_values = {
        "password-secret",
        "captcha-secret",
        "proxy-secret",
        "mail-secret",
        "sso-secret",
        "oauth-secret",
        "future-secret",
    }
    session = {
        "id": "s1",
        "password": "password-secret",
        "yescaptcha_key": "captcha-secret",
        "proxy": "http://alice:proxy-secret@proxy.example:8080",
        "sso": "sso-secret",
        "oauth": {"access_token": "oauth-secret"},
        "future_secret": "future-secret",
        "status": "running",
    }
    batch = {
        "id": "b1",
        "status": "running",
        "reg_config": {
            "yescaptcha_key": "captcha-secret",
            "proxy": "http://alice:proxy-secret@proxy.example:8080",
            "moemail_api_key": "mail-secret",
        },
        "future_secret": "future-secret",
    }
    public_session = grok_build_adapter._compact_session(session)
    public_batch = grok_build_adapter._compact_batch(batch)
    import inspect

    if "include_auth_json" in inspect.signature(
        grok_build_adapter.get_registration_session
    ).parameters:
        return _fail("registration session API still accepts include_auth_json")
    sidecar_source = (ROOT / "scripts" / "registration_service.py").read_text(
        encoding="utf-8"
    )
    if "include_auth_json" in sidecar_source:
        return _fail("registration sidecar still exposes include_auth_json query")
    import subprocess

    tracked = subprocess.run(
        ["git", "-C", str(ROOT), "grep", "-l", "include_auth_json", "--", "*.go", "*.py"],
        capture_output=True,
        text=True,
        check=False,
    )
    offenders = [
        line
        for line in tracked.stdout.splitlines()
        if line.strip() and line.strip() != "scripts/run_regressions.py"
    ]
    if offenders:
        return _fail(f"include_auth_json bypass still referenced: {offenders!r}")
    encoded = json.dumps(
        {"session": public_session, "batch": public_batch}, ensure_ascii=False
    )
    leaked = sorted(value for value in secret_values if value in encoded)
    if leaked:
        return _fail(f"registration public state leaked secrets: {leaked!r}")
    if "reg_config" in public_batch:
        return _fail("registration public batch exposed reg_config")
    adapter_source = (ROOT / "grok2api" / "upstream" / "grok_build_adapter.py").read_text(
        encoding="utf-8"
    )
    forbidden_diagnostics = (
        "sso[:60]",
        "sso_prefix=",
        "rsc_preview",
        ".create_account.rsc.txt",
        "create_account rsc_body preview",
        "body_preview=",
        "parse_all_set_cookie_urls",
        "parse_sso_jwt_url",
        "debug=True",
        "code received: {code}",
        "fresh code received: {code}",
        "code shape: {code!r}",
        "fresh email code invalid: {code!r}",
    )
    present = [item for item in forbidden_diagnostics if item in adapter_source]
    if present:
        return _fail(f"registration adapter exposes secret diagnostics: {present!r}")
    print("ok registration session/batch public state redaction")

    print("\nall contract regressions passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
