#!/usr/bin/env python3
"""grok-build-auth — 快速批量註冊 x.ai 帳號 + SSO + Grok Build OAuth（CLIProxyAPI 可用）

流程:
  Phase 0: 共享頁面刮取（一次，所有 worker 共用 action_id / router_tree）
  Phase 1: 預熱池（背景線程持續填充）
    - TurnstilePool: 預解 Cloudflare Turnstile token
    - SignupPool:    預建信箱 + 寄驗證碼 + 輪詢收碼 + verify_code
  Phase 2: Worker 並行註冊（從池中取已就緒套件，執行 create_account + SSO）
  Phase 3: 背景 OAuth 線程（token 交換 + CLIProxyAPI 匯出）

關鍵優化:
  - 共享頁面刮取，跳過每個 worker 的 JS chunk 下載（省 ~2-3s/worker）
  - Turnstile 預解，worker 不需等待解題（省 ~5-15s/worker）
  - 信箱+驗證碼預備，worker 拿到的套件已含 email+code（省 ~5-10s/worker）
  - 跳過 visit_home() 和 validate_password()（非必要步驟）
  - OAuth 在背景處理，不阻塞下一個註冊
  - 輪詢間隔降至 1s（tempmail）/ 1.5s（YesCaptcha）
  - 支援本地過盾（CAPTCHA_PROVIDER=local）

環境變數:
    CAPTCHA_PROVIDER       local | yescaptcha (default: local)
    LOCAL_SOLVER_URL       本地過盾 URL (default: http://127.0.0.1:5072)
    YESCAPTCHA_API_KEY     YesCaptcha API key (provider=yescaptcha 時必須)
    TEMPMAIL_API_KEY       Tempmail.lol API key (邮箱後端)
    CLOUDFLARE_API_TOKEN   Cloudflare API token (alias_mail 邮箱後端)
    CLIPROXYAPI_AUTH_DIR   CLIProxyAPI data/auth 目录（可选）
    HTTPS_PROXY / HTTP_PROXY  代理（OAuth 换 token）
"""
from __future__ import annotations

import sys
import os
import json
import base64
import time
import uuid
import threading
import argparse
import queue
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_ROOT))

# Load local .env if present (optional dependency).
try:
    from dotenv import load_dotenv

    load_dotenv(_ROOT / ".env")
except Exception:
    pass

from xconsole_client import XConsoleAuthClient, YesCaptchaSolver, config as C
from xconsole_client.xai_oauth import (
    CLIPROXYAPI_GROK_BASE_URL,
    complete_build_oauth,
    default_cliproxyapi_auth_dir,
)
from xconsole_client.oauth_protocol import extract_cookies_from_auth_client
from xconsole_client.tempmail_transport import TempmailInbox

# ── captcha / secrets from environment ───────────────────────
CAPTCHA_PROVIDER = os.environ.get("CAPTCHA_PROVIDER", "").strip().lower() or "local"
LOCAL_SOLVER_URL = (
    os.environ.get("LOCAL_SOLVER_URL")
    or os.environ.get("GROK2API_LOCAL_SOLVER_URL")
    or "http://127.0.0.1:5072"
).rstrip("/")
YESCAPTCHA_KEY = os.environ.get("YESCAPTCHA_API_KEY", "")
TEMPMAIL_KEY = os.environ.get("TEMPMAIL_API_KEY", "")
CLOUDFLARE_TOKEN = os.environ.get("CLOUDFLARE_API_TOKEN", "")
SIGNUP_URL = "https://accounts.x.ai/sign-up?redirect=grok-com"
PROXY = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or ""

_cf_lock = threading.Lock()


def _resolve_captcha_key() -> str:
    if CAPTCHA_PROVIDER == "local":
        return "local"
    return YESCAPTCHA_KEY


CAPTCHA_KEY = _resolve_captcha_key()


def _make_solver(*, poll_interval: float = 1.5, debug: bool = False) -> YesCaptchaSolver:
    """根據 provider 建立 solver。"""
    if CAPTCHA_PROVIDER == "local":
        return YesCaptchaSolver(
            api_key="local",
            endpoint=LOCAL_SOLVER_URL,
            poll_interval=poll_interval,
            debug=debug,
            auto_fallback_endpoint=False,
        )
    return YesCaptchaSolver(
        YESCAPTCHA_KEY, poll_interval=poll_interval, debug=debug,
    )


# ═══════════════════════════════════════════════════════════════
# Data structures
# ═══════════════════════════════════════════════════════════════

@dataclass
class ScrapeData:
    """共享頁面刮取結果 — 同一批次所有 worker 共用。"""
    action_id: str
    router_tree: str
    sitekey: str


@dataclass
class SignupPackage:
    """預備好的註冊套件：含 cookies 的 client + 已驗證的 email + code。"""
    client: XConsoleAuthClient
    email: str
    password: str
    code: str
    created_at: float = field(default_factory=time.time)


@dataclass
class OAuthJob:
    """背景 OAuth 處理作業。"""
    client: XConsoleAuthClient
    email: str
    password: str
    sso: str
    index: int
    result: dict


# ═══════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════

def shared_scrape(debug: bool = False) -> ScrapeData:
    """刮取 signup 頁面一次；所有 worker 重用結果。"""
    c = XConsoleAuthClient(debug=debug, signup_url=SIGNUP_URL, proxy=PROXY or None)
    try:
        c.load_signup_page()
        data = ScrapeData(
            action_id=c.next_action_id,
            router_tree=c.next_router_state_tree,
            sitekey=c.turnstile_sitekey or C.TURNSTILE_SITEKEY,
        )
        print(f"  [scrape] action_id={data.action_id[:16]}... "
              f"sitekey={data.sitekey}")
        return data
    finally:
        c.close()


def light_load_signup_page(client: XConsoleAuthClient, scrape: ScrapeData) -> None:
    """輕量 GET signup 頁面（僅取 cookies），注入預先刮取的 action_id / router_tree。

    省去 JS chunk 下載 + 搜尋（~2-3s），但仍獲取 CF cookies。
    """
    h = client._base_headers()
    h.update({
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "sec-fetch-site": "same-site",
        "sec-fetch-mode": "navigate",
        "sec-fetch-dest": "document",
        "referer": "https://console.x.ai/",
    })
    client._request("GET", SIGNUP_URL, headers=h)
    client._next_action_id = scrape.action_id
    client._next_router_state_tree = scrape.router_tree
    client.turnstile_sitekey = scrape.sitekey


def _make_email_inbox(backend: str) -> tuple[str, object]:
    """Return (email, receiver) — receiver has .wait_for_code(timeout)."""
    if backend == "tempmail":
        if not TEMPMAIL_KEY:
            raise RuntimeError("TEMPMAIL_API_KEY 环境变量未设置")
        inbox = TempmailInbox(
            api_key=TEMPMAIL_KEY, prefix="xai",
            interval=1.0, timeout=90, debug=False,
        )
        email = inbox.create()
        return email, inbox
    elif backend == "cloudflare":
        from xconsole_client.mailbox import AliasMailAccount, AliasMailCodeReceiver
        with _cf_lock:
            cf = AliasMailAccount.ensure_cf()
            alloc = AliasMailAccount(cf)
            address = alloc.create(prefix="xai")
        receiver = AliasMailCodeReceiver(
            cf, address=address, timeout=120, interval=3, since_now=True,
        )
        return address, receiver
    else:
        raise ValueError(f"unknown email backend: {backend}")


def _save_account_bundle(result: dict, output_dir: Path) -> Path:
    """Persist a combined signup+oauth record for later tooling."""
    output_dir.mkdir(parents=True, exist_ok=True)
    email = str(result.get("email") or "unknown")
    safe = "".join(ch if ch.isalnum() or ch in "._-@" else "_" for ch in email) or "unknown"
    ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    path = output_dir / f"account_{safe}_{ts}.json"
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


# ═══════════════════════════════════════════════════════════════
# TurnstilePool — 預解 Turnstile token
# ═══════════════════════════════════════════════════════════════

class TurnstilePool:
    """背景線程持續預解 Turnstile token，填充 queue。

    local solver 模式：solver 內部已有 Camoufox 瀏覽器池（--thread 控制），
    這裡的線程數控制的是並發請求數，應與 solver 的 --thread 對齊。
    """

    def __init__(
        self,
        sitekey: str,
        website_url: str,
        *,
        target_size: int = 4,
        threads: int = 6,
        debug: bool = False,
    ):
        self._sitekey = sitekey
        self._url = website_url
        self._target = target_size
        self._debug = debug
        self._pool: queue.Queue[str] = queue.Queue()
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._n_threads = threads

    def start(self) -> None:
        for i in range(self._n_threads):
            t = threading.Thread(target=self._worker, name=f"turnstile-{i}", daemon=True)
            t.start()
            self._threads.append(t)

    def _worker(self) -> None:
        while not self._stop.is_set():
            if self._pool.qsize() < self._target:
                try:
                    solver = _make_solver(poll_interval=1.5, debug=self._debug)
                    token = solver.solve_turnstile(
                        website_url=self._url,
                        website_key=self._sitekey,
                        premium=True,
                        fallback_non_premium=True,
                    )
                    self._pool.put(token)
                except Exception as e:
                    if self._debug:
                        print(f"  [turnstile-pool] solve failed: {e}")
                    time.sleep(1.0)
            else:
                time.sleep(0.5)

    def get(self, timeout: float = 60) -> str:
        return self._pool.get(timeout=timeout)

    @property
    def size(self) -> int:
        return self._pool.qsize()

    def stop(self) -> None:
        self._stop.set()


# ═══════════════════════════════════════════════════════════════
# SignupPool — 預建信箱 + 寄驗證碼 + 輪詢收碼 + verify
# ═══════════════════════════════════════════════════════════════

class SignupPool:
    """背景線程預備完整註冊套件：client(cookies) + email + 已驗證 code。

    每個套件的製備流程：
      1. 建立 XConsoleAuthClient + 輕量載入頁面（取 CF cookies）
      2. 建立信箱（tempmail 或 cloudflare）
      3. 呼叫 CreateEmailValidationCode 寄驗證碼
      4. 輪詢信箱收碼（1s 間隔）
      5. 呼叫 VerifyEmailValidationCode 驗證
      6. 生成密碼，放入 pool
    """

    def __init__(
        self,
        scrape: ScrapeData,
        email_backend: str = "tempmail",
        *,
        target_size: int = 4,
        threads: int = 4,
        debug: bool = False,
    ):
        self._scrape = scrape
        self._email_backend = email_backend
        self._target = target_size
        self._debug = debug
        self._pool: queue.Queue[SignupPackage] = queue.Queue()
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._n_threads = threads

    def start(self) -> None:
        for i in range(self._n_threads):
            t = threading.Thread(target=self._worker, name=f"signup-prep-{i}", daemon=True)
            t.start()
            self._threads.append(t)

    def _worker(self) -> None:
        while not self._stop.is_set():
            if self._pool.qsize() < self._target:
                try:
                    pkg = self._prepare_one()
                    if pkg:
                        self._pool.put(pkg)
                except Exception as e:
                    if self._debug:
                        print(f"  [signup-pool] prepare failed: {e}")
                    time.sleep(1.0)
            else:
                time.sleep(0.3)

    def _prepare_one(self) -> Optional[SignupPackage]:
        # 1. 建立 client + 輕量載入（不刮取 JS chunks）
        client = XConsoleAuthClient(
            debug=self._debug, signup_url=SIGNUP_URL, proxy=PROXY or None,
        )
        light_load_signup_page(client, self._scrape)

        # 2. 建立信箱
        email, receiver = _make_email_inbox(self._email_backend)

        # 3. 寄驗證碼
        client.create_email_validation_code(email)

        # 4. 輪詢收碼
        code = receiver.wait_for_code(timeout=90)

        # 5. 驗證碼
        result = client.verify_email_validation_code(email, code)
        if not result.ok:
            raise RuntimeError(
                f"verify_email_validation_code failed: grpc_status={result.grpc_status}"
            )

        # 6. 生成密碼
        password = f"Pw{os.urandom(6).hex()}!a#A"

        return SignupPackage(
            client=client, email=email, password=password, code=code,
        )

    def get(self, timeout: float = 90) -> SignupPackage:
        """取一個新鮮套件；丟棄超過 5 分鐘的（code 可能已過期）。"""
        while True:
            pkg = self._pool.get(timeout=timeout)
            if time.time() - pkg.created_at < 300:
                return pkg
            if self._debug:
                print(f"  [signup-pool] discard stale: {pkg.email}")
            try:
                pkg.client.close()
            except Exception:
                pass

    @property
    def size(self) -> int:
        return self._pool.qsize()

    def stop(self) -> None:
        self._stop.set()


# ═══════════════════════════════════════════════════════════════
# OAuthWorker — 背景 OAuth token 交換
# ═══════════════════════════════════════════════════════════════

class OAuthWorker:
    """單一背景線程，序向處理 OAuth 作業（curl_cffi session 安全）。"""

    def __init__(
        self,
        *,
        cliproxyapi_auth_dir: str,
        cliproxyapi_base_url: str,
        debug: bool = False,
    ):
        self._queue: queue.Queue[OAuthJob] = queue.Queue()
        self._auth_dir = cliproxyapi_auth_dir
        self._base_url = cliproxyapi_base_url
        self._debug = debug
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._pending = 0
        self._lock = threading.Lock()

    def start(self) -> None:
        self._thread = threading.Thread(target=self._worker, name="oauth-worker", daemon=True)
        self._thread.start()

    def submit(self, job: OAuthJob) -> None:
        with self._lock:
            self._pending += 1
        self._queue.put(job)

    def _worker(self) -> None:
        while not self._stop.is_set():
            try:
                job = self._queue.get(timeout=2.0)
            except queue.Empty:
                continue
            try:
                session_cookies = extract_cookies_from_auth_client(job.client)
                if job.sso:
                    session_cookies = dict(session_cookies or {})
                    session_cookies.setdefault("sso", job.sso)
                oauth = complete_build_oauth(
                    job.email,
                    job.password,
                    cliproxyapi_auth_dir=self._auth_dir,
                    cliproxyapi_base_url=self._base_url,
                    headless=True,
                    timeout=180.0,
                    proxy=PROXY,
                    yescaptcha_key=CAPTCHA_KEY if CAPTCHA_PROVIDER == "yescaptcha" else "",
                    protocol=True,
                    debug=self._debug,
                    session_cookies=session_cookies,
                    auth_client=job.client,
                )
                job.result["oauth_access_token"] = oauth.access_token
                job.result["oauth_refresh_token"] = oauth.refresh_token
                job.result["oauth_record"] = str(oauth.path) if oauth.path else None
                job.result["cliproxyapi_auth"] = (
                    str(oauth.cliproxyapi_path) if oauth.cliproxyapi_path else None
                )
            except Exception as e:
                job.result["error"] = f"OAuth: {e}"
                if self._debug:
                    print(f"  [oauth-worker] #{job.index} failed: {e}")
            finally:
                try:
                    job.client.close()
                except Exception:
                    pass
                with self._lock:
                    self._pending -= 1

    def wait_done(self, timeout: float = 600) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                if self._pending == 0:
                    return
            time.sleep(1.0)

    def stop(self) -> None:
        self._stop.set()

    @property
    def pending(self) -> int:
        with self._lock:
            return self._pending


# ═══════════════════════════════════════════════════════════════
# Worker — 最短臨界路徑
# ═══════════════════════════════════════════════════════════════

def register_fast(
    index: int,
    scrape: ScrapeData,
    turnstile_pool: TurnstilePool,
    signup_pool: SignupPool,
    oauth_worker: Optional[OAuthWorker],
    debug: bool = False,
) -> dict:
    """從預熱池取已就緒套件 → create_account → SSO → 提交背景 OAuth。

    臨界路徑（無等待）:
      1. 取 SignupPackage（pool 已備）        — instant
      2. 取 Turnstile token（pool 已解）       — instant
      3. create_account                        — ~1-2s
      4. fetch_sso_token                       — ~2-3s
      5. 提交 OAuth job（背景）                 — instant
    """
    result: dict = {
        "email": "",
        "password": "",
        "sso": None,
        "oauth_access_token": None,
        "oauth_refresh_token": None,
        "oauth_record": None,
        "cliproxyapi_auth": None,
        "build_base_url": CLIPROXYAPI_GROK_BASE_URL,
        "error": None,
    }

    client: Optional[XConsoleAuthClient] = None
    try:
        # 1. 取預備好的註冊套件
        pkg = signup_pool.get(timeout=90)
        result["email"] = pkg.email
        result["password"] = pkg.password
        client = pkg.client

        # 2. 取預解 Turnstile token
        turnstile = turnstile_pool.get(timeout=60)

        # 3. create_account
        res = client.create_account(
            email=pkg.email,
            given_name="Test",
            family_name="User",
            password=pkg.password,
            email_validation_code=pkg.code,
            turnstile_token=turnstile,
            castle_request_token="",
            conversion_id=str(uuid.uuid4()),
        )
        if not res.ok:
            result["error"] = f"create_account HTTP {res.http_status}"
            client.close()
            return result

        # 4. SSO 提取
        sso = client.fetch_sso_token(
            email=pkg.email, password=pkg.password, save=True, retries=2,
        )
        if not sso:
            result["error"] = "SSO failed"
            client.close()
            return result
        result["sso"] = sso

        # 5. 提交背景 OAuth 或關閉 client
        if oauth_worker:
            oauth_worker.submit(OAuthJob(
                client=client, email=pkg.email, password=pkg.password,
                sso=sso, index=index, result=result,
            ))
        else:
            client.close()

        return result

    except Exception as e:
        result["error"] = str(e)
        if client:
            try:
                client.close()
            except Exception:
                pass
        return result


# ═══════════════════════════════════════════════════════════════
# Local solver helpers
# ═══════════════════════════════════════════════════════════════

def _check_solver_running(url: str = LOCAL_SOLVER_URL) -> bool:
    """檢查本地過盾是否已在運行。"""
    import requests as _req
    try:
        r = _req.get(f"{url}/health", timeout=3)
        return r.status_code == 200
    except Exception:
        try:
            r = _req.get(f"{url}/", timeout=3)
            return r.status_code in (200, 404)
        except Exception:
            return False


def _start_local_solver(
    solver_dir: Path,
    *,
    threads: int = 5,
    port: int = 5072,
    debug: bool = False,
) -> Optional[subprocess.Popen]:
    """啟動本地 turnstile-solver 子進程（Windows / Linux 通用）。"""
    api_solver = solver_dir / "api_solver.py"
    if not api_solver.exists():
        print(f"  [solver] {api_solver} 不存在，跳過自動啟動")
        return None

    cmd = [
        sys.executable, str(api_solver),
        "--browser_type", "camoufox",
        "--thread", str(threads),
        "--host", "127.0.0.1",
        "--port", str(port),
    ]
    if debug:
        cmd.append("--debug")

    print(f"  [solver] 启动: {' '.join(cmd)}")
    creationflags = 0
    if sys.platform == "win32":
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    proc = subprocess.Popen(
        cmd,
        cwd=str(solver_dir),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        creationflags=creationflags,
    )

    for _ in range(60):
        if proc.poll() is not None:
            print(f"  [solver] 进程提前退出 (code={proc.returncode})")
            return None
        if _check_solver_running():
            print(f"  [solver] 就绪 on 127.0.0.1:{port}")
            return proc
        time.sleep(1.0)

    print("  [solver] 60s 内未就绪，继续但不保证可用")
    return proc


def _stop_local_solver(proc: Optional[subprocess.Popen]) -> None:
    if proc is None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

def main():
    global CAPTCHA_PROVIDER, LOCAL_SOLVER_URL, CAPTCHA_KEY
    default_auth = str(default_cliproxyapi_auth_dir())
    p = argparse.ArgumentParser(
        description="grok-build-auth: x.ai register + SSO + Grok Build OAuth (CLIProxyAPI-ready)",
    )
    p.add_argument("-n", "--count", type=int, default=10, help="账号数量 (default: 10)")
    p.add_argument("-w", "--workers", type=int, default=8, help="并行 worker 数 (default: 8)")
    p.add_argument("-t", "--threads", type=int, default=None, help="兼容旧参数，等同 --workers")
    p.add_argument(
        "-e", "--email",
        choices=["tempmail", "cloudflare"],
        default="tempmail",
        help="邮箱后端: tempmail | cloudflare (default: tempmail)",
    )
    p.add_argument(
        "--captcha-provider",
        choices=["local", "yescaptcha"],
        default=CAPTCHA_PROVIDER,
        help=f"过盾方式 (default: {CAPTCHA_PROVIDER})",
    )
    p.add_argument(
        "--local-solver-url",
        default=LOCAL_SOLVER_URL,
        help=f"本地过盾 URL (default: {LOCAL_SOLVER_URL})",
    )
    p.add_argument(
        "--auto-solver",
        action="store_true",
        help="自动启动本地 turnstile-solver 子进程（provider=local 时生效）",
    )
    p.add_argument(
        "--solver-threads",
        type=int,
        default=5,
        help="自动启动 solver 时的浏览器池大小 (default: 5)",
    )
    p.add_argument(
        "--turnstile-threads", type=int, default=6,
        help="Turnstile 预解线程数 (= 并发请求数; local 应与 solver --thread 对齐)",
    )
    p.add_argument("--turnstile-pool-size", type=int, default=4, help="Turnstile 池目标大小")
    p.add_argument(
        "--signup-threads", type=int, default=4,
        help="信箱预备线程数 (越多并行寄信+收码越快)",
    )
    p.add_argument("--signup-pool-size", type=int, default=4, help="信箱池目标大小")
    p.add_argument("--warmup-timeout", type=float, default=120, help="预热等待超时秒数")
    p.add_argument(
        "--no-oauth",
        action="store_true",
        help="只注册+SSO，不走 Build OAuth / CLIProxyAPI 导出",
    )
    p.add_argument("--no-warmup", action="store_true", help="跳过预热等待，直接开始")
    p.add_argument("--debug", action="store_true", help="印除错日志")
    p.add_argument(
        "--cliproxyapi-auth-dir",
        default=default_auth,
        help=f"CLIProxyAPI auth 目录（默认: {default_auth}）",
    )
    p.add_argument(
        "--cliproxyapi-base-url",
        default=CLIPROXYAPI_GROK_BASE_URL,
        help="Build 上游 base_url（默认 cli-chat-proxy.grok.com/v1）",
    )
    p.add_argument(
        "--oauth-headed",
        action="store_true",
        help="Playwright 有头模式（仅非协议回退时使用）",
    )
    p.add_argument(
        "--oauth-timeout",
        type=float,
        default=180.0,
        help="OAuth 等待超时秒数",
    )
    p.add_argument(
        "--no-oauth-protocol",
        action="store_true",
        help="禁用纯协议 OAuth（默认用 YesCaptcha+CreateSession，不启浏览器）",
    )
    p.add_argument(
        "--oauth-interactive-fallback",
        action="store_true",
        help="协议/Playwright 失败时回退到系统浏览器手动登录",
    )
    p.add_argument(
        "--oauth-debug",
        action="store_true",
        help="打印协议 OAuth 调试日志",
    )
    p.add_argument(
        "--accounts-output-dir",
        default=str(_ROOT / "accounts_output"),
        help="合并账号记录输出目录",
    )
    args = p.parse_args()

    # ── resolve workers (support legacy -t flag) ──────────────
    workers = args.workers if args.workers is not None else (args.threads or 8)
    workers = min(workers, args.count)

    # ── resolve captcha provider ──────────────────────────────
    CAPTCHA_PROVIDER = args.captcha_provider
    LOCAL_SOLVER_URL = args.local_solver_url.rstrip("/")
    if CAPTCHA_PROVIDER == "yescaptcha":
        CAPTCHA_KEY = YESCAPTCHA_KEY
        if not CAPTCHA_KEY:
            print("ERROR: provider=yescaptcha 但 YESCAPTCHA_API_KEY 未设置")
            sys.exit(1)
    else:
        CAPTCHA_KEY = "local"

    if not TEMPMAIL_KEY and args.email == "tempmail":
        print("ERROR: TEMPMAIL_API_KEY 环境变量未设置")
        sys.exit(1)

    do_oauth = not args.no_oauth
    auth_dir = args.cliproxyapi_auth_dir
    output_dir = Path(args.accounts_output_dir)

    # ── auto-start local solver ───────────────────────────────
    solver_proc: Optional[subprocess.Popen] = None
    if CAPTCHA_PROVIDER == "local":
        if args.auto_solver:
            solver_dir = _ROOT.parent / "turnstile-solver"
            if not _check_solver_running():
                print("[pre] 自动启动本地过盾...")
                solver_proc = _start_local_solver(
                    solver_dir,
                    threads=args.solver_threads,
                    port=int(LOCAL_SOLVER_URL.rsplit(":", 1)[-1]) if ":" in LOCAL_SOLVER_URL else 5072,
                    debug=args.debug,
                )
            else:
                print(f"[pre] 本地过盾已在 {LOCAL_SOLVER_URL} 运行")
        else:
            if not _check_solver_running():
                print(f"WARNING: 本地过盾 {LOCAL_SOLVER_URL} 未运行！")
                print(f"  请先启动: cd turnstile-solver && python api_solver.py --browser_type camoufox --thread {args.solver_threads}")
                print(f"  或加 --auto-solver 让脚本自动启动")
                sys.exit(1)
            else:
                print(f"[pre] 本地过盾 {LOCAL_SOLVER_URL} 已就绪")

    print(f"\ngrok-build-auth FAST: {args.count} accounts, {workers} workers")
    print(f"  email:     {args.email}")
    print(f"  captcha:   {CAPTCHA_PROVIDER}  {LOCAL_SOLVER_URL if CAPTCHA_PROVIDER == 'local' else ''}")
    print(f"  turnstile: {args.turnstile_threads} threads, pool={args.turnstile_pool_size}")
    print(f"  signup:    {args.signup_threads} threads, pool={args.signup_pool_size}")
    print(f"  oauth:     {'on' if do_oauth else 'off'}")
    print(f"  auth_dir:  {auth_dir}")
    print()

    t0 = time.time()

    # Phase 0: 共享刮取
    print("[phase 0] 共享页面刮取...")
    scrape = shared_scrape(debug=args.debug)

    # Phase 1: 啟動預熱池
    print("[phase 1] 启动预热池...")
    turnstile_pool = TurnstilePool(
        sitekey=scrape.sitekey,
        website_url=SIGNUP_URL,
        target_size=args.turnstile_pool_size,
        threads=args.turnstile_threads,
        debug=args.debug,
    )
    signup_pool = SignupPool(
        scrape=scrape,
        email_backend=args.email,
        target_size=args.signup_pool_size,
        threads=args.signup_threads,
        debug=args.debug,
    )
    turnstile_pool.start()
    signup_pool.start()

    # Phase 1b: 預熱 — 等待池填充
    if not args.no_warmup:
        target_t = min(workers, args.turnstile_pool_size)
        target_s = min(workers, args.signup_pool_size)
        print(f"[warmup] 等待池填充 (T>={target_t} S>={target_s}, timeout={args.warmup_timeout}s)...")
        warmup_deadline = time.time() + args.warmup_timeout
        while time.time() < warmup_deadline:
            ts = turnstile_pool.size
            sp = signup_pool.size
            elapsed = time.time() - t0
            print(
                f"\r  warmup: turnstile={ts}/{target_t}  signup={sp}/{target_s}  "
                f"({elapsed:.0f}s)",
                end="",
                flush=True,
            )
            if ts >= target_t and sp >= target_s:
                break
            time.sleep(1.0)
        print(
            f"\n[warmup] done in {time.time()-t0:.0f}s  "
            f"(T={turnstile_pool.size} S={signup_pool.size})"
        )

    # Phase 2: 啟動 OAuth worker
    oauth_worker: Optional[OAuthWorker] = None
    if do_oauth:
        print("[phase 2] 启动 OAuth worker...")
        oauth_worker = OAuthWorker(
            cliproxyapi_auth_dir=auth_dir,
            cliproxyapi_base_url=args.cliproxyapi_base_url,
            debug=args.debug or args.oauth_debug,
        )
        oauth_worker.start()

    # Phase 3: 並行註冊
    print(f"[phase 3] 启动 {workers} workers, 注册 {args.count} 账号...")
    results: list[dict] = []
    results_lock = threading.Lock()
    done_count = 0

    def progress():
        nonlocal done_count
        with results_lock:
            done_count += 1
        elapsed = time.time() - t0
        rate = done_count / elapsed if elapsed > 0 else 0
        oauth_pending = oauth_worker.pending if oauth_worker else 0
        print(
            f"\r  [{done_count}/{args.count}] {rate:.1f}/s  "
            f"pools: T={turnstile_pool.size} S={signup_pool.size}  "
            f"oauth_pending={oauth_pending}",
            end="",
            flush=True,
        )

    try:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {
                ex.submit(
                    register_fast, i, scrape,
                    turnstile_pool, signup_pool, oauth_worker, args.debug,
                ): i
                for i in range(1, args.count + 1)
            }
            for f in as_completed(futures):
                result = f.result()
                with results_lock:
                    results.append(result)
                progress()
    except KeyboardInterrupt:
        print("\n\nInterrupted! 停止中...")
        turnstile_pool.stop()
        signup_pool.stop()
        if oauth_worker:
            oauth_worker.stop()
        _stop_local_solver(solver_proc)
        sys.exit(1)

    reg_time = time.time() - t0
    print(f"\n[phase 3] 注册完成 in {reg_time:.0f}s")

    # 註冊完成立即停止預熱池，避免 OAuth 等待期間浪費額度
    turnstile_pool.stop()
    signup_pool.stop()

    # Phase 4: 等待 OAuth 完成
    if oauth_worker:
        pending = oauth_worker.pending
        if pending > 0:
            print(f"[phase 4] 等待 {pending} 个 OAuth 作业完成...")
        oauth_worker.wait_done(timeout=600)
        oauth_worker.stop()
        print(f"[phase 4] OAuth 完成 in {time.time()-t0:.0f}s")

    # 儲存帳號 bundle
    for r in results:
        if r.get("email"):
            bundle = _save_account_bundle(r, output_dir)
            r["account_bundle"] = str(bundle)

    # 摘要
    total_time = time.time() - t0
    ok_sso = [r for r in results if r.get("sso")]
    ok_build = [r for r in results if r.get("cliproxyapi_auth")]
    fail = [r for r in results if r.get("error")]
    rate = len(ok_sso) / total_time if total_time > 0 else 0
    reg_rate = len(ok_sso) / reg_time if reg_time > 0 else 0

    print(f"\n{'='*60}")
    print(f"Done in {total_time:.0f}s  (注册 {reg_time:.0f}s + OAuth {total_time-reg_time:.0f}s)")
    print(f"  注册速率:   {reg_rate:.1f} accounts/s")
    print(f"  总体速率:   {rate:.1f} accounts/s")
    print(f"  SSO OK:     {len(ok_sso)}")
    print(f"  BUILD OK:   {len(ok_build)}")
    print(f"  FAIL:       {len(fail)}")
    print(f"{'='*60}")
    for r in results:
        email = r.get("email") or "?"
        if r.get("cliproxyapi_auth"):
            print(f"  {email:40s}  BUILD  {r['cliproxyapi_auth']}")
        elif r.get("sso") and not do_oauth:
            print(f"  {email:40s}  SSO    {r['sso'][:36]}...")
        elif r.get("sso") and r.get("error"):
            print(f"  {email:40s}  SSO-ok OAuth-FAIL: {r.get('error')}")
        else:
            print(f"  {email:40s}  FAIL: {r.get('error', '?')}")

    # 停止本地 solver（如有自動啟動）
    _stop_local_solver(solver_proc)


if __name__ == "__main__":
    main()
