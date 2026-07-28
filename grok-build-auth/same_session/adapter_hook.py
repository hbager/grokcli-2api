# -*- coding: utf-8 -*-
"""Adapter hook: run same_session registration and return SSO."""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import Any, Callable, Optional

GBA = Path(__file__).resolve().parents[1]


def run_same_session_signup(
    *,
    email: str,
    password: str,
    proxy: str = "",
    receiver: Any,
    solve_turnstile: Callable[[str], str],
    log: Optional[Callable[[str], None]] = None,
    check_cancel: Optional[Callable[[], None]] = None,
    timeout_s: int = 150,
) -> dict[str, Any]:
    if str(GBA) not in sys.path:
        sys.path.insert(0, str(GBA))
    from same_session import same_session_register

    def _fetch_code(em: str) -> str:
        if check_cancel:
            check_cancel()
        if log:
            try:
                log(f"waiting mailbox code for {em}")
            except Exception:
                pass
        code = None
        try:
            code = receiver.wait_for_code(
                timeout=180.0,
                poll_interval=0.5,
                should_cancel=(lambda: (check_cancel() or False) if check_cancel else False),
            )
        except TypeError:
            try:
                code = receiver.wait_for_code(timeout=180.0, poll_interval=0.5)
            except TypeError:
                code = receiver.wait_for_code(timeout=180.0)
        if not code:
            raise RuntimeError("email verification code timeout")
        code = str(code or "").strip().upper().replace(" ", "").replace("-", "")
        if len(code) != 6:
            raise RuntimeError("invalid email verification code shape (expected 6 alnum chars)")
        if log:
            try:
                log(f"mailbox code received {code[:2]}****")
            except Exception:
                pass
        return code

    def _solve(sk: str) -> str:
        if check_cancel:
            check_cancel()
        return solve_turnstile(sk)

    def _log(msg: str) -> None:
        if log:
            try:
                log(msg)
                return
            except Exception:
                pass
        print(f"[same_session] {msg}", flush=True)

    return same_session_register(
        email=email,
        password=password,
        given_name="User",
        family_name="Grok",
        fetch_code=_fetch_code,
        solve_turnstile=_solve,
        proxy=proxy or None,
        log=_log,
        timeout_s=timeout_s,
    )

