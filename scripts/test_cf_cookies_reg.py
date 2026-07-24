"""TDD: registration must keep Cloudflare cookies in session_cookies."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "grok-build-auth"))


def test_extract_keeps_cf_keys():
    from xconsole_client.oauth_protocol import extract_cookies_from_auth_client

    class Jar:
        def __init__(self):
            self._items = {
                "sso": "S",
                "cf_clearance": "CF",
                "__cf_bm": "BM",
                "foo": "bar",
            }

        def get_dict(self):
            return dict(self._items)

        def items(self):
            return self._items.items()

    class Client:
        def __init__(self):
            self._t = type("T", (), {"cookies": Jar()})()

        def _read_sso_from_jar(self):
            return "S"

    out = extract_cookies_from_auth_client(Client())
    assert out.get("cf_clearance") == "CF", out
    assert out.get("__cf_bm") == "BM", out
    assert out.get("sso") == "S", out


def test_pick_cloudflare_cookies():
    # helper we will add / use from adapter path
    try:
        from grok2api.pool.accounts import pick_cloudflare_cookies
    except ImportError:
        # fallback expected during red phase
        raise AssertionError("pick_cloudflare_cookies missing")

    sc = {
        "sso": "S",
        "sso-rw": "S",
        "cf_clearance": "CF",
        "__cf_bm": "BM",
        "other": "x",
    }
    got = pick_cloudflare_cookies({"session_cookies": sc})
    assert got == {"cf_clearance": "CF", "__cf_bm": "BM"}, got


def test_soft_success_keeps_cf_in_session_cookies():
    """Soft-success import payload must not wipe CF cookies down to sso-only."""
    try:
        from grok2api.pool.accounts import merge_session_cookies_with_sso
    except ImportError:
        raise AssertionError("merge_session_cookies_with_sso missing")

    jar = {"sso": "old", "cf_clearance": "CF", "__cf_bm": "BM"}
    merged = merge_session_cookies_with_sso(jar, "NEWSSO")
    assert merged["sso"] == "NEWSSO"
    assert merged["sso-rw"] == "NEWSSO"
    assert merged["cf_clearance"] == "CF"
    assert merged["__cf_bm"] == "BM"


if __name__ == "__main__":
    failed = 0
    for fn in (
        test_extract_keeps_cf_keys,
        test_pick_cloudflare_cookies,
        test_soft_success_keeps_cf_in_session_cookies,
    ):
        try:
            fn()
            print("PASS", fn.__name__)
        except Exception as e:
            failed += 1
            print("FAIL", fn.__name__, "->", e)
    raise SystemExit(failed)
