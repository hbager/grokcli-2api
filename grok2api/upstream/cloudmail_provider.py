"""CloudMail / SkyMail mailbox provider helpers for protocol registration."""
from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse, urlunparse

from grok2api.config import MOEMAIL_API_KEY

CLOUDMAIL_DEFAULT_BASE_URL = ""


def normalize_cloudmail_base_url(base_url: str | None = None) -> str:
    """CloudMail / SkyMail API origin (self-hosted)."""
    raw = (base_url or "").strip()
    if not raw:
        return CLOUDMAIL_DEFAULT_BASE_URL
    lower = raw.lower()
    if any(
        x in lower
        for x in (
            "maliapi.215.im",
            "vip.215.im",
            "mail.chatgpt.org.uk",
            "tempmail.lol",
            "api.tempmail",
            "moemail.example.com",
            "moemail.521884.xyz",
        )
    ):
        return CLOUDMAIL_DEFAULT_BASE_URL
    try:
        parsed = urlparse(raw if "://" in raw else f"https://{raw}")
        origin = urlunparse((parsed.scheme or "https", parsed.netloc, "", "", "", ""))
    except Exception:
        return CLOUDMAIL_DEFAULT_BASE_URL
    return (origin or CLOUDMAIL_DEFAULT_BASE_URL).rstrip("/")


def _cloudmail_client(
    *,
    api_key: str | None = None,
    base_url: str | None = None,
    timeout: float = 30.0,
):
    from grok2api.upstream.cloudmail import CloudMail

    key = (api_key or MOEMAIL_API_KEY or "").strip()
    base = normalize_cloudmail_base_url(base_url)
    if not base:
        raise ValueError(
            "CloudMail Base URL missing. Set 协议注册 → CloudMail Base URL "
            "(SkyMail OpenAPI root)."
        )
    if not key:
        raise ValueError(
            "CloudMail token missing. Set 协议注册 → CloudMail Token "
            "(Authorization; generate via /api/public/genToken or admin panel)."
        )
    return CloudMail(base, key, timeout=int(timeout or 30))


def secrets_token_hex_local() -> str:
    import secrets as _secrets

    return _secrets.token_hex(5).lower()


def cloudmail_list_domains(
    *,
    api_key: str | None = None,
    base_url: str | None = None,
) -> list[str]:
    """List domains from CloudMail websiteConfig.domainList."""
    client = _cloudmail_client(api_key=api_key, base_url=base_url)
    try:
        return list(client.list_domains() or [])
    finally:
        try:
            client.close()
        except Exception:
            pass


def cloudmail_create_mailbox(
    *,
    name: str | None = None,
    domain: str | None = None,
    expiry_ms: int | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
    proxy: str | None = None,
    proxy_username: str | None = None,
    proxy_password: str | None = None,
    pick_domain_from_list=None,
) -> dict[str, Any]:
    """Create a CloudMail / SkyMail mailbox user.

    Docs: https://doc.skymail.ink
    - POST /api/public/addUser
    - POST /api/public/emailList
    """
    _ = (proxy, proxy_username, proxy_password)
    client = _cloudmail_client(api_key=api_key, base_url=base_url)
    if pick_domain_from_list is None:
        from grok2api.upstream.moemail import pick_domain_from_list as _pick

        pick_domain_from_list = _pick
    dom = pick_domain_from_list(domain) if domain else ""
    if not dom:
        dom = (domain or "").strip().lstrip("@").strip(".")
    if not dom:
        # Empty domain => auto-pick from websiteConfig.domainList (like YYDS/GPTMail).
        try:
            catalog = list(client.list_domains() or [])
        except Exception as e:  # noqa: BLE001
            raise ValueError(
                "CloudMail domain missing and auto-fetch failed "
                f"({client.base_url}): {e}"
            ) from e
        if not catalog:
            raise ValueError(
                "CloudMail domain missing and websiteConfig.domainList is empty "
                f"(base={client.base_url})."
            )
        import random as _random

        dom = str(_random.choice(catalog) or "").strip().lstrip("@").strip(".")
    if not dom:
        raise ValueError(
            "CloudMail domain missing. Fill CloudMail 域名 in 协议注册 "
            f"(base={client.base_url})."
        )
    local = (name or "").strip().lower()
    if not local:
        local = secrets_token_hex_local()
    local = re.sub(r"[^a-z0-9._+-]", "", local) or secrets_token_hex_local()
    address = f"{local}@{dom}"
    import secrets as _secrets

    password = _secrets.token_urlsafe(12)
    try:
        raw = client.add_user(email=address, password=password)
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"CloudMail create failed ({client.base_url}): {e}") from e
    finally:
        try:
            client.close()
        except Exception:
            pass
    return {
        "id": str(address),
        "email": str(address),
        "token": str(api_key or MOEMAIL_API_KEY or "").strip(),
        "provider": "cloudmail",
        "password": password,
        "raw": raw if isinstance(raw, dict) else {"result": raw},
        "expiry_ms": 0 if expiry_ms is None else int(expiry_ms),
    }


def cloudmail_fetch_messages(
    email_id: str,
    *,
    api_key: str | None = None,
    base_url: str | None = None,
    include_details: bool = True,
    address: str | None = None,
    token: str | None = None,
    extract_codes_and_links=None,
) -> list[dict[str, Any]]:
    """List inbox messages for a CloudMail address."""
    _ = include_details
    key = (token or api_key or MOEMAIL_API_KEY or "").strip()
    target = (address or email_id or "").strip()
    if not target or "@" not in target:
        return []
    if not key:
        return []
    if extract_codes_and_links is None:
        from grok2api.upstream.moemail import _extract_codes_and_links as extract_codes_and_links
    client = _cloudmail_client(api_key=key, base_url=base_url)
    try:
        from grok2api.upstream.cloudmail import MailType

        rows = client.list_emails(toEmail=target, type=MailType.INBOX, num=1, size=30)
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"CloudMail list failed ({client.base_url}): {e}") from e
    finally:
        try:
            client.close()
        except Exception:
            pass
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    nl = chr(10)
    for raw in rows[:30]:
        if not isinstance(raw, dict):
            continue
        item = dict(raw)
        if item.get("text") and not item.get("content"):
            item["content"] = item.get("text")
        if item.get("sendEmail") and not item.get("from"):
            item["from"] = item.get("sendEmail")
        if item.get("toEmail") and not item.get("to"):
            item["to"] = item.get("toEmail")
        text_blob = nl.join(
            str(item.get(k) or "")
            for k in (
                "subject",
                "text",
                "content",
                "html",
                "sendEmail",
                "from",
                "toEmail",
            )
        )
        item["extracted"] = extract_codes_and_links(text_blob)
        if not item.get("id"):
            item["id"] = str(
                item.get("emailId")
                or item.get("createTime")
                or hash(text_blob) & 0xFFFFFFFF
            )
        out.append(item)
    return out
