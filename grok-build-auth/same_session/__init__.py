# -*- coding: utf-8 -*-
"""Same-session xAI sign-up (Camoufox page-fetch + Castle mint).

Primary registration path: real browser context mints Castle and posts
signup via in-page fetch so Castle/TLS/cookie stay aligned.

Protocol HTTP (empty castle) remains available as fallback in the adapter.
"""
from __future__ import annotations

from .same_session_register import same_session_register, parse_proxy_spec, shutdown_camoufox_pool
from .castle_service import (
    is_castle_token_usable,
    mint_signup_signals,
    new_conversion_id,
    MIN_CASTLE_LEN,
)

__all__ = [
    "same_session_register",
    "parse_proxy_spec",
    "shutdown_camoufox_pool",
    "is_castle_token_usable",
    "mint_signup_signals",
    "new_conversion_id",
    "MIN_CASTLE_LEN",
]
