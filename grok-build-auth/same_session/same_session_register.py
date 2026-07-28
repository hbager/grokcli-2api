# -*- coding: utf-8 -*-
"""
同会话协议注册（主路径，默认直连、不用代理）

根因（旧路径必 deny / 无 sso）:
1. 拆会话: Chrome mint castle → 拷到 curl_cffi signup
   → BOT_FLAG_SOURCE_CASTLE policy=deny
2. page.request 发码: Playwright 独立 TLS ≠ 页面 Chrome → CF 403
3. set-cookie 截断: RSC `18:T9d5,<url>` 必须按 hex 长度取完整 URL
   → 短正则/独立 curl 无注册会话 → 400/auth-error，jar 无 sso

正确路径（已验证能拿 clean sso）:
  同一 Chrome persistent context:
    sign-up 加载 → 等 React 水合
    → page.evaluate mint castle（不点 UI、不填表单）
    → 页内 fetch(gRPC 发码/验码)
    → 页内 fetch(signup + 同页 castle)
    → context.request 跟完整 set-cookie 链落 sso
  禁止：点「Sign up with email」、type 邮箱、鼠标乱点。
  成功门槛在注册机侧：risk 未标记才计 success。
"""
from __future__ import annotations

import base64
import json
import os
import random
import re
import shutil
import string
import struct
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Optional

from .castle_service import (
    MIN_CASTLE_LEN,
    SITE,
    _MINT_JS,
    is_castle_token_usable,
    mint_js_args,
    new_conversion_id,
)

_BASE = Path(__file__).resolve().parents[1]
DEFAULT_PROFILE_ROOT = _BASE / "logs" / "same_session_profiles"

# 日志里别再喷 JWT / Playwright Call log
_JWT_RE = re.compile(
    r"eyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{8,}"
)
_URL_QUERY_RE = re.compile(r"(https?://[^\s?'\"]+\?)[^\s'\"]+")

# ---------- Camoufox 线程本地池（每 worker 线程自用）----------
# Playwright Sync API 绑定 greenlet + 线程内 event loop：
# 1) 禁止跨线程复用 browser
# 2) **同一线程同一时刻只能有 1 个存活的 Playwright Sync 驱动**
#    若旧 Camoufox 还在池里又冷启第二个 →
#    「Sync API inside the asyncio loop」——这就是成功号之后下一号炸掉的根因。
#
# 打散：限次复用 + 概率换新；换新前必须先关干净旧实例，再 Camoufox()。
_CAMOUFOX_POOL_LOCK = threading.Lock()
# tid -> 至多 1 条: {"cm","browser","is_browser","uses","owner_tid","fp_os","locale","proxy","gen"}
_CAMOUFOX_POOLS: dict[int, Optional[dict[str, Any]]] = {}
_CAMOUFOX_GEN: dict[int, int] = {}


def _browser_pool_enabled() -> bool:
    raw = (os.getenv("GROK_SAME_SESSION_BROWSER_POOL") or "1").strip().lower()
    return raw not in ("0", "false", "off", "no")


def _pool_max_uses() -> int:
    """单 browser 最多服务几号；默认 2。达上限先关再冷启。"""
    try:
        n = int(
            (
                os.getenv("GROK_SAME_SESSION_BROWSER_POOL_MAX_USES")
                or os.getenv("GROK_SS_POOL_MAX_USES")
                or "2"
            ).strip()
            or "2"
        )
    except ValueError:
        n = 2
    return max(1, min(8, n))


def _pool_force_cold_prob() -> float:
    """强制换新概率（关旧再启）。默认 0.35。"""
    raw = (
        os.getenv("GROK_SAME_SESSION_BROWSER_POOL_COLD_PROB")
        or os.getenv("GROK_SS_POOL_COLD_PROB")
        or "0.35"
    ).strip()
    try:
        p = float(raw)
    except ValueError:
        p = 0.35
    return max(0.0, min(1.0, p))


def _clear_thread_event_loop() -> None:
    """
    Playwright Sync 退出后，线程上可能仍挂着已关闭的 loop。
    清掉，避免下一次 Camoufox() 误判「inside asyncio loop」。
    """
    try:
        import asyncio

        try:
            loop = asyncio.get_event_loop_policy().get_event_loop()
        except RuntimeError:
            return
        if loop is None:
            return
        try:
            if loop.is_running():
                # 理论上 Sync 退出后不应仍 running；强行不碰 running loop
                return
        except Exception:
            pass
        try:
            if not loop.is_closed():
                loop.close()
        except Exception:
            pass
        try:
            asyncio.set_event_loop(None)
        except Exception:
            pass
    except Exception:
        pass


def _close_pool_entry(ent: Optional[dict[str, Any]], log_fn=None, reason: str = "") -> None:
    if not ent:
        return
    cm = ent.get("cm")
    if cm is None:
        return
    try:
        cm.__exit__(None, None, None)
    except Exception as e:
        if log_fn:
            try:
                log_fn(f"camoufox 关闭异常 · {_compact_log(e, 60)}")
            except Exception:
                pass
    finally:
        # 关键驱动后清 loop，下一号才能安全冷启
        _clear_thread_event_loop()
        if log_fn and reason:
            try:
                log_fn(f"camoufox 已关闭 · {reason}")
            except Exception:
                pass


def _get_thread_ent() -> Optional[dict[str, Any]]:
    tid = threading.get_ident()
    with _CAMOUFOX_POOL_LOCK:
        return _CAMOUFOX_POOLS.get(tid)


def _set_thread_ent(ent: Optional[dict[str, Any]]) -> None:
    tid = threading.get_ident()
    with _CAMOUFOX_POOL_LOCK:
        if ent is None:
            _CAMOUFOX_POOLS.pop(tid, None)
        else:
            _CAMOUFOX_POOLS[tid] = ent


def _drop_thread_pool(log_fn=None, reason: str = "") -> None:
    """关掉本线程唯一实例（冷启前必调）。"""
    ent = _get_thread_ent()
    _set_thread_ent(None)
    _close_pool_entry(ent, log_fn=log_fn, reason=reason or "换新前清理")


def shutdown_camoufox_pool() -> None:
    """进程退出 / 批末释放所有线程池内的 Camoufox。"""
    with _CAMOUFOX_POOL_LOCK:
        items = [e for e in _CAMOUFOX_POOLS.values() if e]
        _CAMOUFOX_POOLS.clear()
        _CAMOUFOX_GEN.clear()
    for ent in items:
        _close_pool_entry(ent, reason="")


def _next_gen(tid: int) -> int:
    with _CAMOUFOX_POOL_LOCK:
        g = int(_CAMOUFOX_GEN.get(tid, 0) or 0) + 1
        _CAMOUFOX_GEN[tid] = g
        return g


def _acquire_camoufox_pooled(
    *,
    fp_os_val: str,
    locale: str,
    humanize_val: bool,
    vp: dict[str, int],
    pw_proxy: Optional[dict[str, str]],
    timezone_id: str,
    log_fn,
) -> tuple[Any, Any, Any, bool, float]:
    """
    返回 (camoufox_cm_or_None, ctx, page, from_pool, launch_s)
    from_pool=True：只关 context，browser 留在本线程池。

    硬规则：本线程同时最多 1 个存活 Camoufox/Playwright Sync。
    需要换新时先 __exit__ 旧的并清 event loop，再冷启。
    """
    proxy_server = (pw_proxy or {}).get("server") if pw_proxy else None
    proxy_s = (proxy_server or "direct").strip()
    loc_main = (locale or "en-US").strip().lower().split(",")[0].strip() or "en-us"
    tid = threading.get_ident()
    t_launch = time.time()
    max_uses = _pool_max_uses()
    cold_prob = _pool_force_cold_prob()
    pool_on = _browser_pool_enabled()

    ent = _get_thread_ent()

    # 池坏/超次/指纹 OS 或代理变了 → 必须关旧（os/proxy 是 launch 级，不能靠 new_context）
    need_drop = False
    drop_reason = ""
    if ent is not None:
        if int(ent.get("owner_tid") or 0) != tid:
            need_drop, drop_reason = True, "owner 不匹配"
        elif int(ent.get("uses") or 0) >= max_uses:
            need_drop, drop_reason = True, f"达上限 uses={ent.get('uses')}/{max_uses}"
        elif str(ent.get("fp_os") or "") != fp_os_val:
            need_drop, drop_reason = True, f"换 OS {ent.get('fp_os')}→{fp_os_val}"
        elif str(ent.get("proxy") or "direct").strip() != proxy_s:
            need_drop, drop_reason = True, "换代理"
        elif not ent.get("is_browser") or ent.get("browser") is None:
            need_drop, drop_reason = True, "browser 无效"

    force_cold = (not pool_on) or (random.random() < cold_prob)
    if force_cold and ent is not None and pool_on:
        need_drop = True
        drop_reason = drop_reason or f"强制打散 p={cold_prob:.2f}"

    if need_drop:
        _drop_thread_pool(log_fn, reason=drop_reason)
        ent = None

    # —— 尝试复用（仅 new_context：locale/timezone/viewport 可每号不同）——
    if pool_on and ent is not None and not force_cold:
        browser = ent.get("browser")
        try:
            if ent.get("is_browser") and hasattr(browser, "new_context"):
                ctx = browser.new_context(
                    viewport={"width": int(vp["width"]), "height": int(vp["height"])},
                    locale=locale,
                    timezone_id=timezone_id,
                )
                page = ctx.new_page()
                ent["uses"] = int(ent.get("uses") or 0) + 1
                ent["locale"] = loc_main
                uses_now = int(ent["uses"])
                launch_s = round(time.time() - t_launch, 2)
                log_fn(
                    f"camoufox 池复用 · tid={tid} · gen={ent.get('gen')} · "
                    f"{fp_os_val}/{locale} · #{uses_now}/{max_uses} · {launch_s}s"
                )
                return None, ctx, page, True, launch_s
        except Exception as e:
            log_fn(f"camoufox 池失效 · {_compact_log(e, 80)}")
            _drop_thread_pool(log_fn, reason="复用失败")
            ent = None

    # —— 冷启：先确保本线程没有残留 Sync 驱动 ——
    if _get_thread_ent() is not None:
        _drop_thread_pool(log_fn, reason="冷启前残留清理")
    else:
        # 即便池空，也可能有脏 loop
        _clear_thread_event_loop()

    from camoufox.sync_api import Camoufox

    gen = _next_gen(tid)
    log_fn(
        f"启动 camoufox · tid={tid} · gen={gen} · {fp_os_val}/{locale} · "
        f"vp={int(vp['width'])}x{int(vp['height'])}"
    )
    fox_kwargs: dict[str, Any] = {
        "headless": True,
        "os": fp_os_val,
        "locale": locale,
        "humanize": bool(humanize_val),
        "window": (int(vp["width"]), int(vp["height"])),
        "block_webrtc": True,
    }
    if pw_proxy:
        fox_kwargs["proxy"] = pw_proxy
        # geoip extra is optional; only enable when package is installed
        try:
            import importlib.util as _ilu
            if _ilu.find_spec("geoip2") is not None:
                fox_kwargs["geoip"] = True
        except Exception:
            pass

    t_launch = time.time()
    last_err: Optional[BaseException] = None
    camoufox_cm = None
    camoufox_browser = None
    for attempt in range(1, 3):
        try:
            _clear_thread_event_loop()
            camoufox_cm = Camoufox(**fox_kwargs)
            camoufox_browser = camoufox_cm.__enter__()
            last_err = None
            break
        except BaseException as e:
            last_err = e
            msg = str(e)
            # 典型：旧 loop 没清干净 / 同线程双开
            if "asyncio loop" in msg or "Async API" in msg:
                log_fn(
                    f"camoufox Sync/loop 冲突 try#{attempt} · 清 loop 重试 · "
                    f"{_compact_log(e, 80)}"
                )
                try:
                    if camoufox_cm is not None:
                        camoufox_cm.__exit__(None, None, None)
                except Exception:
                    pass
                camoufox_cm = None
                camoufox_browser = None
                _clear_thread_event_loop()
                time.sleep(0.15 * attempt)
                continue
            raise
    if last_err is not None or camoufox_cm is None or camoufox_browser is None:
        _clear_thread_event_loop()
        raise last_err or RuntimeError("camoufox 启动失败")

    is_browser = hasattr(camoufox_browser, "new_context")
    if is_browser:
        ctx = camoufox_browser.new_context(
            viewport={"width": int(vp["width"]), "height": int(vp["height"])},
            locale=locale,
            timezone_id=timezone_id,
        )
        page = ctx.new_page()
    else:
        ctx = camoufox_browser
        page = ctx.new_page()
    launch_s = round(time.time() - t_launch, 2)
    log_fn(f"camoufox 就绪 · tid={tid} · gen={gen} · {launch_s}s")

    if pool_on and is_browser:
        _set_thread_ent(
            {
                "cm": camoufox_cm,
                "browser": camoufox_browser,
                "is_browser": True,
                "uses": 1,
                "owner_tid": tid,
                "fp_os": fp_os_val,
                "locale": loc_main,
                "proxy": proxy_s,
                "gen": gen,
                "born": time.time(),
            }
        )
        # 池接管生命周期；本号只关 ctx
        return None, ctx, page, True, launch_s

    return camoufox_cm, ctx, page, False, launch_s


def _compact_log(msg: Any, max_len: int = 160) -> str:
    """把异常/日志压成一行短文案，去掉 Call log、JWT、超长 query。"""
    s = str(msg or "")
    if "Call log:" in s:
        s = s.split("Call log:", 1)[0]
    # Playwright 常把整段 request dump 拼进 message
    for sep in ("\n", " - → ", " - user-agent:", " - accept:"):
        if sep in s:
            s = s.split(sep, 1)[0]
    s = _JWT_RE.sub("[JWT]", s)
    s = _URL_QUERY_RE.sub(r"\1…", s)
    s = re.sub(r"\s+", " ", s).strip()
    # 常见代理超时：只留关键
    m = re.search(
        r"connect ETIMEDOUT\s+(\d+\.\d+\.\d+\.\d+:\d+)", s, re.I
    )
    if m:
        s = f"connect ETIMEDOUT {m.group(1)} (proxy?)"
    elif "ETIMEDOUT" in s.upper():
        s = "connect ETIMEDOUT"
    if len(s) > max_len:
        s = s[: max_len - 1] + "…"
    return s


def parse_proxy_spec(spec: str) -> Optional[dict[str, str]]:
    """
    解析代理配置，支持:
      host:port:user:pass
      user:pass@host:port
      http://user:pass@host:port
      socks5://user:pass@host:port
    返回 Playwright proxy dict: {server, username?, password?}
    以及 server_url（带鉴权的完整 URL，给 curl 用）。
    """
    s = (spec or "").strip()
    if not s:
        return None
    # 完整 URL
    if "://" in s:
        # http://user:pass@host:port
        m = re.match(
            r"^(?P<scheme>https?|socks5h?|socks4)://"
            r"(?:(?P<user>[^:@/]+):(?P<pass>[^@/]+)@)?"
            r"(?P<host>[^:/]+):(?P<port>\d+)/?$",
            s,
            re.I,
        )
        if not m:
            # 无端口也接受
            return {"server": s, "server_url": s}
        scheme = m.group("scheme")
        host = m.group("host")
        port = m.group("port")
        user = m.group("user")
        pw = m.group("pass")
        server = f"{scheme}://{host}:{port}"
        out = {"server": server}
        if user:
            out["username"] = user
            out["password"] = pw or ""
            from urllib.parse import quote

            out["server_url"] = (
                f"{scheme}://{quote(user, safe='')}:{quote(pw or '', safe='')}@"
                f"{host}:{port}"
            )
        else:
            out["server_url"] = server
        return out

    # user:pass@host:port
    if "@" in s:
        cred, hostport = s.rsplit("@", 1)
        if ":" in cred and ":" in hostport:
            user, pw = cred.split(":", 1)
            host, port = hostport.rsplit(":", 1)
            from urllib.parse import quote

            server = f"http://{host}:{port}"
            return {
                "server": server,
                "username": user,
                "password": pw,
                "server_url": (
                    f"http://{quote(user, safe='')}:{quote(pw, safe='')}@{host}:{port}"
                ),
            }

    # host:port:user:pass  （user 可能含冒号以外字符，port 是数字段）
    parts = s.split(":")
    if len(parts) >= 4:
        host = parts[0]
        port = parts[1]
        # 剩余拼 user:pass —— 但密码一般不含冒号，user 可能含 region 空格
        # 约定：host:port:user:pass 且 pass 是最后一段
        user = ":".join(parts[2:-1])
        pw = parts[-1]
        if port.isdigit():
            from urllib.parse import quote

            server = f"http://{host}:{port}"
            return {
                "server": server,
                "username": user,
                "password": pw,
                "server_url": (
                    f"http://{quote(user, safe='')}:{quote(pw, safe='')}@{host}:{port}"
                ),
            }

    # host:port
    if len(parts) == 2 and parts[1].isdigit():
        server = f"http://{parts[0]}:{parts[1]}"
        return {"server": server, "server_url": server}
    return None


def resolve_proxy() -> Optional[dict[str, str]]:
    """GROK_PROXY / XAI_PROXY / SAME_SESSION_PROXY 优先。"""
    for key in ("GROK_PROXY", "XAI_PROXY", "SAME_SESSION_PROXY", "HTTPS_PROXY", "HTTP_PROXY"):
        raw = (os.getenv(key) or "").strip()
        if raw:
            parsed = parse_proxy_spec(raw)
            if parsed:
                return parsed
    return None

# 页内二进制 POST（gRPC-web）：body 以 base64 传入，在页面还原 Uint8Array 后 fetch
_PAGE_BINARY_POST_JS = """
async ({url, bodyB64, headers}) => {
  try {
    const bin = atob(bodyB64);
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    const r = await fetch(url, {
      method: 'POST',
      credentials: 'include',
      mode: 'cors',
      headers: headers || {},
      body: bytes,
    });
    const ab = await r.arrayBuffer();
    const u8 = new Uint8Array(ab);
    let bodyB64Out = '';
    // 分块 btoa，避免大 body 爆栈
    const chunk = 0x8000;
    for (let i = 0; i < u8.length; i += chunk) {
      bodyB64Out += String.fromCharCode.apply(null, u8.subarray(i, i + chunk));
    }
    bodyB64Out = btoa(bodyB64Out);
    const hdrs = {};
    try {
      r.headers.forEach((v, k) => { hdrs[k] = v; });
    } catch (e) {}
    return {
      ok: r.ok,
      status: r.status,
      bodyB64: bodyB64Out,
      headers: hdrs,
      error: null,
    };
  } catch (e) {
    return { ok: false, status: 0, bodyB64: '', headers: {}, error: String(e) };
  }
}
"""

# 页内文本 POST（Next.js server action signup）
_PAGE_TEXT_POST_JS = """
async ({url, body, headers}) => {
  try {
    const r = await fetch(url, {
      method: 'POST',
      credentials: 'include',
      mode: 'cors',
      headers: headers || {},
      body: body,
    });
    const text = await r.text();
    const hdrs = {};
    try {
      r.headers.forEach((v, k) => { hdrs[k] = v; });
    } catch (e) {}
    return {
      ok: r.ok,
      status: r.status,
      text: text,
      headers: hdrs,
      error: null,
    };
  } catch (e) {
    return { ok: false, status: 0, text: '', headers: {}, error: String(e) };
  }
}
"""

_PAGE_GET_JS = """
async ({url}) => {
  try {
    const r = await fetch(url, { method: 'GET', credentials: 'include', mode: 'cors', redirect: 'follow' });
    return { ok: r.ok, status: r.status, error: null };
  } catch (e) {
    return { ok: false, status: 0, error: String(e) };
  }
}
"""


def _encode_grpc_string(field_id: int, value: str) -> bytes:
    key = (field_id << 3) | 2
    raw = value.encode("utf-8")
    # protobuf varint length（邮箱/验证码都短，单字节即可；长串走 varint）
    length = len(raw)
    length_bytes = b""
    while True:
        b = length & 0x7F
        length >>= 7
        length_bytes += bytes([b | (0x80 if length else 0)])
        if not length:
            break
    return struct.pack("B", key) + length_bytes + raw


def encode_grpc_create_email(email: str) -> bytes:
    payload = _encode_grpc_string(1, email)
    return b"\x00" + struct.pack(">I", len(payload)) + payload


def encode_grpc_verify_email(email: str, code: str) -> bytes:
    payload = _encode_grpc_string(1, email) + _encode_grpc_string(2, code)
    return b"\x00" + struct.pack(">I", len(payload)) + payload


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _rand_password(n: int = 14) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(random.choices(alphabet, k=n)) + "aA1!"


def _rand_name() -> tuple[str, str]:
    first = random.choice(
        ["James", "Emma", "Liam", "Olivia", "Noah", "Ava", "Ethan", "Mia", "Lucas", "Sophia"]
    )
    last = random.choice(
        ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]
    )
    return first, last


def _load_action_cache() -> dict[str, str]:
    """
    读 action 缓存；文件不存在/损坏时返回空壳。
    真正可用的 action_id/state_tree 会在打开 sign-up 后由
    _refresh_action_from_page 实时抽取并回写。
    """
    default = {
        "action_id": "",
        "site_key": "0x4AAAAAAAhr9JGVDZbrZOo0",
        "state_tree": "",
    }
    path = _BASE / "logs" / "action_id_cache.json"
    try:
        if not path.is_file():
            return default
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return default
        return {
            "action_id": (data.get("action_id") or "").strip(),
            "site_key": (data.get("site_key") or default["site_key"]).strip()
            or default["site_key"],
            "state_tree": (data.get("state_tree") or "").strip(),
        }
    except Exception:
        return default


def _save_action_cache(cfg: dict[str, str]) -> None:
    path = _BASE / "logs" / "action_id_cache.json"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "action_id": cfg.get("action_id"),
                    "site_key": cfg.get("site_key"),
                    "state_tree": cfg.get("state_tree"),
                    "ts": time.time(),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    except Exception:
        pass


def _refresh_action_from_page(page, cfg: dict[str, str], log=None) -> dict[str, str]:
    """
    从当前已打开的 sign-up 页实时抽 action_id / state_tree / site_key。
    缓存过期时 server action 仍 200，但 body 只是 RSC 碎片、没有 set-cookie。
    """
    def _l(m: str) -> None:
        m = _compact_log(m, max_len=120)
        if not m:
            return
        if log:
            try:
                log(m)
                return
            except Exception:
                pass
        print(m, flush=True)

    try:
        html = page.content() or ""
    except Exception:
        html = ""

    # site key
    m = re.search(r'sitekey":"(0x4[a-zA-Z0-9_-]+)"', html)
    if m:
        cfg["site_key"] = m.group(1)

    # state tree（页面 meta / 内联脚本）
    m = re.search(r'next-router-state-tree":"([^"]+)"', html)
    if m:
        cfg["state_tree"] = m.group(1)

    # action id：优先扫当前页已加载的 script src 内容（与 grok.initialize 同规则）
    action = None
    try:
        scripts = page.evaluate(
            """() => Array.from(document.querySelectorAll('script[src]'))
                .map(s => s.src)
                .filter(u => u.includes('/_next/static/'))"""
        ) or []
    except Exception:
        scripts = []

    for src in scripts:
        try:
            # 页内 fetch 同源 chunk，避免外部 TLS
            txt = page.evaluate(
                """async (url) => {
                    try {
                      const r = await fetch(url, {credentials:'include'});
                      return await r.text();
                    } catch (e) { return ''; }
                }""",
                src,
            )
            if not txt:
                continue
            m = re.search(r"7f[a-fA-F0-9]{40}", txt)
            if m:
                action = m.group(0)
                break
        except Exception:
            continue

    # 兜底：整页 HTML 里找
    if not action:
        m = re.search(r"7f[a-fA-F0-9]{40}", html)
        if m:
            action = m.group(0)

    if action:
        old = cfg.get("action_id") or ""
        cfg["action_id"] = action
        if action != old:
            _l(f"action 刷新 {action[:12]}…")
            _save_action_cache(cfg)
    # 未命中时静默沿用缓存，不刷屏

    return cfg


def _extract_sso_from_set_cookie_url(url: str) -> Optional[str]:
    """从 auth.*.set-cookie?q=JWT 的 payload 里抠 sso token（兜底）。"""
    try:
        from urllib.parse import urlparse, parse_qs

        q = parse_qs(urlparse(url).query).get("q", [""])[0]
        if not q or q.count(".") < 2:
            return None
        # JWT payload
        pad = "=" * (-len(q.split(".")[1]) % 4)
        payload = json.loads(
            base64.urlsafe_b64decode(q.split(".")[1] + pad).decode("utf-8", "ignore")
        )
        # 常见结构: {config:{token:"..."}} 或直接 token 字段
        if isinstance(payload, dict):
            cfg = payload.get("config") or {}
            tok = cfg.get("token") or payload.get("token") or payload.get("sso")
            if isinstance(tok, str) and tok.count(".") >= 2:
                return tok
            # 有时 token 就是整个 cookie value 在别处
            for k in ("sso", "value", "jwt"):
                v = payload.get(k)
                if isinstance(v, str) and v.startswith("eyJ"):
                    return v
        return None
    except Exception:
        return None


def _cookies_sso(cookies: list[dict]) -> tuple[str, str]:
    sso = ""
    sso_rw = ""
    for c in cookies or []:
        name = c.get("name") or ""
        val = c.get("value") or ""
        if name == "sso" and val and not sso:
            sso = val
        if name == "sso-rw" and val and not sso_rw:
            sso_rw = val
    return sso, sso_rw or sso


def _normalize_flight_text(text: str) -> str:
    raw = text or ""
    return (
        raw.replace("\\/", "/")
        .replace("\\u0026", "&")
        .replace("\\u003d", "=")
        .replace("\\u003f", "?")
    )


def _extract_set_cookie_urls(text: str) -> list[str]:
    """
    从 Next.js RSC / flight 响应抠 **完整** set-cookie 链接。

    关键：链接在 `18:T9d5,<url>` 这种 text chunk 里，长度常 >2500，
    旧正则/截断会得到残缺 JWT → 跟跳 400/auth-error，永远没 sso。
    必须优先按 flight text chunk 整段取，不要靠短正则。
    """
    raw = _normalize_flight_text(text)
    cleaned: list[str] = []

    def _push(u: str) -> None:
        u = (u or "").strip().rstrip("\\").rstrip('"').rstrip("'").rstrip("1:")
        u = re.sub(r"[\r\n].*$", "", u)
        if "set-cookie?q=" not in u:
            return
        if not u.startswith("http"):
            return
        if u not in cleaned:
            cleaned.append(u)

    # 1) flight text chunk 精确长度：`18:T9d5,<payload>`（hex 长度）
    for m in re.finditer(r"\d+:T([0-9a-fA-F]+),", raw):
        try:
            ln = int(m.group(1), 16)
        except Exception:
            continue
        if ln <= 0 or ln > 200_000:
            continue
        chunk = raw[m.end() : m.end() + ln]
        if "set-cookie?q=" not in chunk:
            continue
        # chunk 本身就是完整 URL
        if chunk.startswith("http"):
            _push(chunk.strip())
        else:
            for u in re.findall(
                r"https://[A-Za-z0-9.-]+/set-cookie\?q=[A-Za-z0-9_\-\.]+",
                chunk,
            ):
                _push(u)

    # 2) 兜底：非精确 T 块 / 全文
    if not cleaned:
        for m in re.finditer(
            r"\d+:T[0-9a-fA-F]+,(.+?)(?=\n\d+:|\Z)", raw, re.S
        ):
            chunk = (m.group(1) or "").strip()
            if "set-cookie?q=" in chunk and chunk.startswith("http"):
                _push(chunk.split()[0] if chunk.split() else chunk)
    if not cleaned:
        for u in re.findall(
            r"https://[A-Za-z0-9.-]+/set-cookie\?q=[A-Za-z0-9_\-\.]+",
            raw,
        ):
            _push(u)

    # 最长优先（完整链入口在最外层，q 最长）
    cleaned.sort(key=len, reverse=True)
    return cleaned


def _expand_set_cookie_chain(url: str, max_depth: int = 8) -> list[str]:
    """解码 q= JWT 里嵌套的 success_url，得到完整跳转链。"""
    chain = [url]
    cur = url
    for _ in range(max_depth):
        try:
            from urllib.parse import parse_qs, urlparse

            q = parse_qs(urlparse(cur).query).get("q", [""])[0]
            if not q or q.count(".") < 2:
                break
            pad = "=" * ((4 - len(q.split(".")[1]) % 4) % 4)
            payload = json.loads(
                base64.urlsafe_b64decode(q.split(".")[1] + pad).decode(
                    "utf-8", "ignore"
                )
            )
            su = ((payload or {}).get("config") or {}).get("success_url")
            if not su or not str(su).startswith("http"):
                break
            su = str(su)
            if su in chain:
                break
            chain.append(su)
            cur = su
        except Exception:
            break
    return chain


def _page_binary_post(page, url: str, body: bytes, headers: dict[str, str]) -> dict[str, Any]:
    raw = page.evaluate(
        _PAGE_BINARY_POST_JS,
        {"url": url, "bodyB64": _b64(body), "headers": headers},
    )
    if not isinstance(raw, dict):
        return {"ok": False, "status": 0, "error": f"bad evaluate: {raw!r}", "body": b""}
    body_out = b""
    if raw.get("bodyB64"):
        try:
            body_out = base64.b64decode(raw["bodyB64"])
        except Exception:
            body_out = b""
    return {
        "ok": bool(raw.get("ok")),
        "status": int(raw.get("status") or 0),
        "error": raw.get("error"),
        "headers": raw.get("headers") or {},
        "body": body_out,
    }


def _page_text_post(page, url: str, body: str, headers: dict[str, str]) -> dict[str, Any]:
    raw = page.evaluate(
        _PAGE_TEXT_POST_JS,
        {"url": url, "body": body, "headers": headers},
    )
    if not isinstance(raw, dict):
        return {"ok": False, "status": 0, "error": f"bad evaluate: {raw!r}", "text": ""}
    return {
        "ok": bool(raw.get("ok")),
        "status": int(raw.get("status") or 0),
        "error": raw.get("error"),
        "headers": raw.get("headers") or {},
        "text": str(raw.get("text") or ""),
    }


def _env_bool(name: str, default: Optional[bool] = None) -> Optional[bool]:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    return default


def _parse_timing_profile(raw: Optional[str] = None) -> dict[str, Any]:
    """
    时序档位（影响 castle 采样窗口 / 行为节奏）:
      fast | normal | slow | human | random
    默认 fast（压空等）；GROK_SAME_SESSION_TIMING 可覆盖。
    """
    name = (raw or os.getenv("GROK_SAME_SESSION_TIMING") or "fast").strip().lower()
    if name in ("rand", "random", "rotate"):
        name = random.choice(["fast", "normal", "slow", "human"])
    presets = {
        # 压秒但仍等 CastleProvider：仅 hasReact 就 mint 会 len=0
        "turbo": {
            "pre_mint_ms": (80, 220),
            "post_react_ms": (60, 180),
            "between_mint_ms": (200, 450),
            "pre_code_ms": (0, 20),
            "pre_verify_ms": (0, 20),
            "pre_turnstile_ms": (0, 20),
            "pre_signup_ms": (0, 40),
            "mint_attempts": 5,
            "networkidle_ms": 1500,
            "react_poll_ms": 150,
            "react_max_waits": 40,  # ~6s，慢代理/慢水合仍等 fiber
            "react_fallback_ms": 1200,
            "require_fiber_token": True,
        },
        # 量产默认：等 createRequestToken 再 mint；Turnstile 并行
        "fast": {
            "pre_mint_ms": (120, 350),
            "post_react_ms": (80, 250),
            "between_mint_ms": (250, 550),
            "pre_code_ms": (0, 40),
            "pre_verify_ms": (0, 30),
            "pre_turnstile_ms": (0, 40),  # 仅同步补解时用；并行成功则跳过
            "pre_signup_ms": (0, 80),
            "mint_attempts": 6,
            "networkidle_ms": 2000,
            "react_poll_ms": 160,
            "react_max_waits": 45,  # ~7s
            "react_fallback_ms": 1500,
            "require_fiber_token": True,
        },
        "normal": {
            "pre_mint_ms": (800, 1800),
            "post_react_ms": (1800, 3200),
            "between_mint_ms": (900, 1600),
            "pre_code_ms": (400, 1000),
            "pre_verify_ms": (400, 1000),
            "pre_turnstile_ms": (600, 1400),
            "pre_signup_ms": (900, 2000),
            "mint_attempts": 7,
            "networkidle_ms": 12000,
            "react_poll_ms": 700,
            "react_max_waits": 25,
            "react_fallback_ms": 3000,
            "require_fiber_token": True,
        },
        "slow": {
            "pre_mint_ms": (1800, 3200),
            "post_react_ms": (3000, 5500),
            "between_mint_ms": (1500, 2800),
            "pre_code_ms": (800, 1600),
            "pre_verify_ms": (800, 1600),
            "pre_turnstile_ms": (1200, 2400),
            "pre_signup_ms": (1800, 3500),
            "mint_attempts": 8,
            "networkidle_ms": 20000,
            "react_poll_ms": 1000,
            "react_max_waits": 30,
            "react_fallback_ms": 5000,
            "require_fiber_token": True,
        },
        "human": {
            "pre_mint_ms": (1200, 2800),
            "post_react_ms": (2500, 5000),
            "between_mint_ms": (1200, 2500),
            "pre_code_ms": (700, 1800),
            "pre_verify_ms": (700, 1800),
            "pre_turnstile_ms": (1000, 2600),
            "pre_signup_ms": (1500, 4000),
            "mint_attempts": 8,
            "networkidle_ms": 15000,
            "react_poll_ms": 800,
            "react_max_waits": 28,
            "react_fallback_ms": 4000,
            "require_fiber_token": True,
        },
    }
    base = presets.get(name, presets["fast"])
    # env 可单独压 networkidle（毫秒）；0 = 跳过
    ni_env = (os.getenv("GROK_SAME_SESSION_NETWORKIDLE_MS") or "").strip()
    if ni_env.isdigit():
        base = {**base, "networkidle_ms": max(0, int(ni_env))}
    return {
        "name": name if name in presets else "fast",
        **base,
    }


def _ts_parallel_enabled() -> bool:
    """Turnstile 与发码/收码并行；GROK_SAME_SESSION_TS_PARALLEL=0 可关。"""
    raw = (os.getenv("GROK_SAME_SESSION_TS_PARALLEL") or "1").strip().lower()
    return raw not in ("0", "false", "off", "no")


def _rand_viewport() -> dict[str, int]:
    # 常见桌面分辨率附近抖动，避免固定 1365x900 指纹簇
    bases = [
        (1366, 768),
        (1440, 900),
        (1536, 864),
        (1600, 900),
        (1680, 1050),
        (1920, 1080),
        (1280, 800),
        (1280, 720),
    ]
    w, h = random.choice(bases)
    w += random.randint(-12, 12)
    h += random.randint(-10, 10)
    return {"width": max(1100, w), "height": max(700, h)}


def same_session_register(
    *,
    email: str,
    password: Optional[str] = None,
    given_name: Optional[str] = None,
    family_name: Optional[str] = None,
    verification_code: Optional[str] = None,
    fetch_code: Optional[Any] = None,
    turnstile_token: Optional[str] = None,
    solve_turnstile: Optional[Any] = None,
    headless: Optional[bool] = None,
    timeout_s: int = 120,
    profile_dir: Optional[Path] = None,
    fresh_profile: bool = True,
    proxy: Optional[str] = None,
    log: Optional[Any] = None,
    # 可选覆盖（矩阵测试 / 一号一换）。不传则读环境变量。
    browser: Optional[str] = None,
    fp_os: Optional[str] = None,
    locale: Optional[str] = None,
    timezone_id: Optional[str] = None,
    humanize: Optional[bool] = None,
    timing: Optional[str] = None,
    viewport: Optional[dict[str, int]] = None,
) -> dict[str, Any]:
    """
    同会话协议注册（页内 fetch，不走 page.request）。

    默认 camoufox 真无头（页面跑不弹窗；已验证 CLEAN）。
    GROK_SAME_SESSION_HEADLESS=offscreen → 有头 Chrome 挪屏外（可能闪窗）。
    GROK_SAME_SESSION_HEADLESS=1/headless → Chrome --headless=new。
    GROK_SAME_SESSION_HEADLESS=0/gui → 可见 Chrome 窗口。
    默认直连、不用代理。proxy 仅在显式传入时启用（可选）。

    一号一换旋钮（env 或参数）:
      browser/fp_os/locale/timezone/humanize/timing/viewport
    """
    def _log(msg: str) -> None:
        """只打一行：有外部 log 回调就只走回调，绝不双份 print。"""
        msg = _compact_log(msg, max_len=160)
        if not msg:
            return
        if log:
            try:
                log(msg)
                return
            except Exception:
                pass
        print(msg, flush=True)

    def _sleep_ms(lo_hi: tuple[int, int], tag: str = "") -> int:
        lo, hi = int(lo_hi[0]), int(lo_hi[1])
        if hi < lo:
            lo, hi = hi, lo
        ms = random.randint(lo, hi)
        if ms > 0:
            try:
                # page 未就绪时用 time.sleep
                time.sleep(ms / 1000.0)
            except Exception:
                pass
        if tag:
            out_steps_ref.append(f"wait:{tag}:{ms}ms")
        return ms

    # 显示/引擎：
    # - 默认 / camoufox：Camoufox 真无头（页面不弹窗，已验证 CLEAN）
    # - offscreen：Chrome 有头 + 屏外（可能闪窗/多屏仍可见）
    # - 1/true/headless：Chrome --headless=new
    # - 0/gui/headed：Chrome 可见窗口
    display_mode = "camoufox"
    browser_engine = "camoufox"
    raw_browser = (browser or os.getenv("GROK_SAME_SESSION_BROWSER") or "").strip().lower()
    if headless is True:
        display_mode = "headless"
        browser_engine = "chrome"
    elif headless is False:
        display_mode = "gui"
        browser_engine = "chrome"
    else:
        raw = (
            os.getenv("GROK_SAME_SESSION_HEADLESS") or "camoufox"
        ).strip().lower()
        # 参数 browser 优先
        if raw_browser in ("camoufox", "cf", "fox"):
            raw = "camoufox"
        elif raw_browser in ("chrome", "offscreen"):
            raw = "offscreen"
        elif raw_browser in ("chrome-headless", "headless"):
            raw = "headless"
        elif raw_browser in ("gui", "headed"):
            raw = "gui"
        eng_raw = raw_browser
        if eng_raw in ("camoufox", "cf", "fox") or raw in (
            "camoufox",
            "cf",
            "camoufox-headless",
            "cf-headless",
        ):
            display_mode = "camoufox"
            browser_engine = "camoufox"
        elif raw in ("1", "true", "yes", "headless"):
            display_mode = "headless"
            browser_engine = "chrome"
        elif raw in ("0", "false", "off", "no", "gui", "headed"):
            display_mode = "gui"
            browser_engine = "chrome"
        elif raw in ("offscreen", "chrome-offscreen"):
            display_mode = "offscreen"
            browser_engine = "chrome"
        else:
            # 未知值 → camoufox，别误开有头窗
            display_mode = "camoufox"
            browser_engine = "camoufox"
    use_headless = display_mode in ("headless", "camoufox")

    given = given_name or _rand_name()[0]
    family = family_name or _rand_name()[1]
    password = password or _rand_password()
    conversion_id = new_conversion_id()
    cfg = _load_action_cache()

    # proxy 显式传入优先；否则认 GROK_PROXY / XAI_PROXY（与注册机业务代理对齐）
    if proxy:
        proxy_info = parse_proxy_spec(proxy)
    else:
        proxy_info = resolve_proxy()

    locale = (locale or os.getenv("GROK_LOCALE") or "").strip() or "en-US"
    timezone_id = (timezone_id or os.getenv("GROK_TIMEZONE") or "").strip() or "America/New_York"
    timing_cfg = _parse_timing_profile(timing)
    out_steps_ref: list[str] = []

    # 指纹 OS（Camoufox）：默认 windows；禁止 linux（超哥硬规则）
    fp_os_raw = (fp_os or os.getenv("GROK_SAME_SESSION_OS") or "windows").strip().lower()
    if fp_os_raw in ("rand", "random", "rotate", "any"):
        # 随机池也不含 linux
        fp_os_val = random.choice(["windows", "macos"])
    elif fp_os_raw in ("win", "windows"):
        fp_os_val = "windows"
    elif fp_os_raw in ("mac", "macos", "osx", "darwin"):
        fp_os_val = "macos"
    elif fp_os_raw in ("lin", "linux"):
        # 禁止 linux 指纹：强制回落 windows，避免 silent 污染
        try:
            _log("fp_os=linux 不支持，已改 windows")
        except Exception:
            pass
        fp_os_val = "windows"
    else:
        fp_os_val = "windows"

    humanize_val = humanize if humanize is not None else _env_bool("GROK_SAME_SESSION_HUMANIZE", True)
    if humanize_val is None:
        humanize_val = True
    vp = viewport or _rand_viewport()

    out: dict[str, Any] = {
        "ok": False,
        "email": email,
        "password": password,
        "given": given,
        "family": family,
        "conversion_id": conversion_id,
        "castle_len": 0,
        "castle_method": None,
        "signup_status": None,
        "sso": None,
        "sso_rw": None,
        "error": None,
        "steps": out_steps_ref,
        "elapsed_s": 0.0,
        "mode": "same_session_page_fetch",
        "browser_engine": browser_engine,
        "display_mode": display_mode,
        "proxy_server": None,
        "locale": locale,
        "timezone": timezone_id,
        "fp_os": fp_os_val,
        "humanize": bool(humanize_val),
        "timing": timing_cfg["name"],
        "viewport": vp,
    }
    t0 = time.time()

    if profile_dir is None:
        profile_dir = DEFAULT_PROFILE_ROOT / uuid.uuid4().hex[:10]
    profile_dir = Path(profile_dir)
    if fresh_profile and profile_dir.exists():
        shutil.rmtree(profile_dir, ignore_errors=True)
    profile_dir.mkdir(parents=True, exist_ok=True)
    out["profile"] = str(profile_dir)

    # 只清系统杂代理，保留 GROK_PROXY/XAI_PROXY（多账号时下一号还要用）
    _saved_biz = {
        k: os.environ.get(k)
        for k in ("GROK_PROXY", "XAI_PROXY", "SAME_SESSION_PROXY")
        if os.environ.get(k)
    }
    for k in (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ):
        os.environ.pop(k, None)
    # Playwright 走 launch_kwargs.proxy，进程级业务代理先摘掉避免双重代理
    for k in ("GROK_PROXY", "XAI_PROXY", "SAME_SESSION_PROXY"):
        os.environ.pop(k, None)
    os.environ["NO_PROXY"] = "*"
    os.environ["no_proxy"] = "*"
    out["steps"].append("env:clear_sys_proxy")

    if proxy_info and proxy_info.get("server"):
        out["proxy_server"] = proxy_info.get("server")
        out["steps"].append(f"proxy:{proxy_info.get('server')}")
        _log(f"代理 {proxy_info.get('server')}")
    else:
        out["steps"].append("proxy:off")
        _log("代理 直连")

    pw_proxy: Optional[dict[str, str]] = None
    if proxy_info and proxy_info.get("server"):
        pw_proxy = {"server": proxy_info["server"]}
        if proxy_info.get("username"):
            pw_proxy["username"] = proxy_info["username"]
            pw_proxy["password"] = proxy_info.get("password") or ""

    castle = ""
    ctx = None
    camoufox_cm = None
    camoufox_browser = None
    from_pool = False
    try:
        # ---------- 启动浏览器：Camoufox 真无头 or Chrome ----------
        if browser_engine == "camoufox":
            try:
                from camoufox.sync_api import Camoufox  # noqa: F401
            except Exception as e:
                out["error"] = f"camoufox 不可用: {e}"
                return out
            try:
                camoufox_cm, ctx, page, from_pool, launch_s = _acquire_camoufox_pooled(
                    fp_os_val=fp_os_val,
                    locale=locale,
                    humanize_val=bool(humanize_val),
                    vp=vp,
                    pw_proxy=pw_proxy,
                    timezone_id=timezone_id,
                    log_fn=_log,
                )
            except Exception as e:
                out["error"] = f"camoufox 启动失败: {e}"
                return out
            out["browser_from_pool"] = bool(from_pool)
            out["browser_launch_s"] = launch_s
            out["steps"].append(
                f"engine:camoufox-headless:os={fp_os_val}:hum={int(bool(humanize_val))}"
                f":pool={int(bool(from_pool))}:launch={launch_s}s"
            )
        else:
            try:
                from playwright.sync_api import sync_playwright
            except Exception as e:
                out["error"] = f"playwright 不可用: {e}"
                return out
            _log(f"启动 chrome · {display_mode}")
            p = sync_playwright().start()
            out["_pw"] = p
            chrome_args = [
                "--disable-blink-features=AutomationControlled",
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-dev-shm-usage",
                f"--window-size={int(vp['width'])},{int(vp['height'])}",
                f"--lang={locale}",
            ]
            if display_mode == "headless":
                chrome_args.append("--headless=new")
            elif display_mode == "offscreen":
                # 有头 Chrome 挪到屏外：过 CF 接近真有头。
                # 不要 --start-minimized：最小化会节流 rAF/定时器，Castle 起不来。
                chrome_args.extend(
                    [
                        "--window-position=-2400,-2400",
                    ]
                )

            launch_kwargs: dict[str, Any] = {
                "user_data_dir": str(profile_dir),
                "channel": "chrome",
                "headless": use_headless and display_mode == "headless",
                "viewport": {"width": int(vp["width"]), "height": int(vp["height"])},
                "locale": locale,
                "timezone_id": timezone_id,
                "args": chrome_args,
            }
            if pw_proxy:
                launch_kwargs["proxy"] = pw_proxy
            t_launch = time.time()
            ctx = p.chromium.launch_persistent_context(**launch_kwargs)
            _log(f"chrome 就绪 · {time.time()-t_launch:.1f}s")
            out["steps"].append(f"engine:chrome-{display_mode}")

        try:
            if browser_engine != "camoufox":
                page = ctx.pages[0] if ctx.pages else ctx.new_page()
            page.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
            )
            if display_mode == "offscreen" and browser_engine == "chrome":
                try:
                    # 再保险：把所有页窗口推到屏外
                    for pg in ctx.pages:
                        try:
                            pg.evaluate(
                                "() => { try { window.moveTo(-2400,-2400); "
                                "window.resizeTo(1365,900); } catch(e){} }"
                            )
                        except Exception:
                            pass
                except Exception:
                    pass

            nav_timeout = min(90000, max(30000, int(timeout_s) * 1000))
            t_goto = time.time()
            page.goto(
                f"{SITE}/sign-up",
                wait_until="domcontentloaded",
                timeout=nav_timeout,
            )
            _log(f"sign-up 就绪 · {time.time()-t_goto:.1f}s")
            # networkidle 太长会空烧；fast 默认 5s，可用 env 覆盖
            ni_ms = int(timing_cfg.get("networkidle_ms") or 5000)
            if ni_ms > 0:
                try:
                    page.wait_for_load_state("networkidle", timeout=ni_ms)
                except Exception:
                    pass

            # 协议路径：不点 UI。必须等 CastleProvider.createRequestToken 上树，
            # 仅 hasReact 就 mint → fiber mint failed len=0（慢代理 sign-up 20s+ 时必现）。
            _FIBER_READY_JS = r"""() => {
              const out = {
                hasReact: false,
                enableCastle: false,
                hasCastle: !!(window.Castle || window._castle || window.__castle),
                castleScripts: 0,
                hasFiberNode: false,
                hasCreateRequestToken: false,
                via: null,
                bodyLen: (document.body && document.body.innerHTML || '').length,
              };
              try {
                out.enableCastle = (document.documentElement.innerHTML||'')
                  .includes('enableCastle');
                out.castleScripts = [...document.scripts]
                  .map(s => s.src || '')
                  .filter(s => /castle/i.test(s)).length;
                if (window.__castleCreate && typeof window.__castleCreate === 'function') {
                  out.hasCreateRequestToken = true;
                  out.via = 'cache';
                }
                const roots = [
                  document.documentElement, document.body,
                  document.getElementById('root'), document.getElementById('__next'),
                ].filter(Boolean);
                const extra = [...document.querySelectorAll('div,main,section')].slice(0, 120);
                function walk(node, depth, bag) {
                  if (!node || depth > 70 || bag.fn) return;
                  try {
                    const props = node.memoizedProps || node.pendingProps || {};
                    const val = props.value || props;
                    if (val && typeof val.createRequestToken === 'function') {
                      bag.fn = val.createRequestToken; bag.via = 'value';
                    } else if (typeof props.createRequestToken === 'function') {
                      bag.fn = props.createRequestToken; bag.via = 'props';
                    }
                  } catch (e) {}
                  if (bag.fn) return;
                  if (node.child) walk(node.child, depth + 1, bag);
                  if (!bag.fn && node.sibling) walk(node.sibling, depth + 1, bag);
                }
                const bag = {fn: null, via: null};
                for (const el of [...roots, ...extra]) {
                  for (const k of Object.keys(el)) {
                    if (k.startsWith('__react') || k.startsWith('__reactFiber')
                        || k.startsWith('__reactContainer')
                        || k.startsWith('__reactInternalInstance')) {
                      out.hasReact = true;
                      out.hasFiberNode = true;
                      if (!bag.fn) walk(el[k], 0, bag);
                    }
                  }
                  if (bag.fn) break;
                }
                if (bag.fn) {
                  try { window.__castleCreate = bag.fn; } catch (e) {}
                  out.hasCreateRequestToken = true;
                  out.via = bag.via;
                }
              } catch (e) {
                out.err = String(e && e.message || e);
              }
              return out;
            }"""
            react_ok = False
            fiber_token_ok = False
            react_poll = int(timing_cfg.get("react_poll_ms") or 350)
            react_max = int(timing_cfg.get("react_max_waits") or 18)
            require_fiber = bool(timing_cfg.get("require_fiber_token", True))
            # 慢导航（sign-up 20s+）再多给一轮预算
            try:
                nav_s = float(out.get("steps") and 0)
            except Exception:
                nav_s = 0
            t_react = time.time()
            last_st: dict[str, Any] = {}
            for w in range(1, react_max + 1):
                try:
                    st = page.evaluate(_FIBER_READY_JS)
                except Exception as e:
                    st = {"err": str(e)}
                if isinstance(st, dict):
                    last_st = st
                    ready_core = bool(st.get("hasReact") or st.get("hasFiberNode"))
                    fiber_token_ok = bool(st.get("hasCreateRequestToken"))
                    ready_castle_hint = bool(
                        st.get("enableCastle")
                        or st.get("hasCastle")
                        or int(st.get("castleScripts") or 0) > 0
                    )
                    # 硬门槛：有 createRequestToken 才算真就绪（默认真）
                    if fiber_token_ok:
                        react_ok = True
                        _log(
                            f"React/Castle 就绪 · fiber=ok via={st.get('via')} · "
                            f"{time.time()-t_react:.1f}s"
                        )
                        break
                    # 兼容：明确关闭 require 时，React+线索也可进（不推荐）
                    if (
                        not require_fiber
                        and ready_core
                        and ready_castle_hint
                    ):
                        react_ok = True
                        _log(
                            f"React/Castle 就绪(宽松) · {time.time()-t_react:.1f}s"
                        )
                        break
                page.wait_for_timeout(react_poll)
            if not react_ok:
                try:
                    title = page.title()
                    body_len = len(page.content() or "")
                except Exception:
                    title, body_len = "?", 0
                _log(
                    f"CastleProvider 未就绪 · title={title!r} len={body_len} "
                    f"fiberToken={last_st.get('hasCreateRequestToken')} "
                    f"react={last_st.get('hasReact')} · 再等/软刷新"
                )
                page.wait_for_timeout(int(timing_cfg.get("react_fallback_ms") or 1200))
                # 软刷新一次：慢代理下 Castle chunk 常晚到，刷新后 fiber 更稳
                try:
                    page.reload(wait_until="domcontentloaded", timeout=nav_timeout)
                    out["steps"].append("reload:castle_wait")
                    _log("sign-up 软刷新以挂 CastleProvider…")
                    ni_ms2 = int(timing_cfg.get("networkidle_ms") or 2000)
                    if ni_ms2 > 0:
                        try:
                            page.wait_for_load_state(
                                "networkidle", timeout=min(ni_ms2, 8000)
                            )
                        except Exception:
                            pass
                    for w in range(1, max(12, react_max // 2) + 1):
                        try:
                            st = page.evaluate(_FIBER_READY_JS)
                        except Exception as e:
                            st = {"err": str(e)}
                        if isinstance(st, dict) and st.get("hasCreateRequestToken"):
                            react_ok = True
                            fiber_token_ok = True
                            last_st = st
                            _log(
                                f"React/Castle 就绪(刷新后) · via={st.get('via')} · "
                                f"{time.time()-t_react:.1f}s"
                            )
                            break
                        page.wait_for_timeout(react_poll)
                except Exception as reload_err:
                    # 禁止 as re：会遮蔽 import re，后续 re.search 直接 UnboundLocalError
                    out["steps"].append(f"reload_err:{type(reload_err).__name__}")
            else:
                pr = timing_cfg["post_react_ms"]
                lo_pr, hi_pr = int(pr[0]), int(pr[1])
                if hi_pr > 0:
                    ms = random.randint(max(0, lo_pr), max(lo_pr, hi_pr))
                    page.wait_for_timeout(ms)
                    out["steps"].append(f"wait:post_react:{ms}ms")
            out["steps"].append(
                "page_loaded"
                + (":fiber_token" if fiber_token_ok else (":react" if react_ok else ":no_react"))
            )
            out["steps"].append(f"timing:{timing_cfg['name']}")

            # 实时刷新 action_id / state_tree（缓存过期 → signup 200 但无 set-cookie）
            cfg = _refresh_action_from_page(page, cfg, log=_log)
            out["action_id"] = (cfg.get("action_id") or "")[:24]
            out["steps"].append(f"action:{(cfg.get('action_id') or '')[:16]}")

            # 1) 同页 mint castle：纯 page.evaluate，禁止 click/type/鼠标交互
            pm = timing_cfg["pre_mint_ms"]
            lo_pm, hi_pm = int(pm[0]), int(pm[1])
            if hi_pm > 0:
                ms = random.randint(max(0, lo_pm), max(lo_pm, hi_pm))
                page.wait_for_timeout(ms)
                out["steps"].append(f"wait:pre_mint:{ms}ms")
            last_raw: Any = None
            mint_n = int(timing_cfg.get("mint_attempts") or 3)
            mint_args = mint_js_args()  # 默认 fiber-only，短 token 不外传
            t_mint = time.time()
            for attempt in range(1, mint_n + 1):
                # 每轮 mint 前再确认 fiber（Provider 晚挂）
                if attempt > 1 or not fiber_token_ok:
                    try:
                        st2 = page.evaluate(_FIBER_READY_JS)
                        if isinstance(st2, dict) and st2.get("hasCreateRequestToken"):
                            fiber_token_ok = True
                    except Exception:
                        pass
                raw = page.evaluate(_MINT_JS, mint_args)
                last_raw = raw
                if isinstance(raw, dict):
                    if raw.get("token"):
                        cand = str(raw["token"])
                        if is_castle_token_usable(cand):
                            castle = cand
                            out["castle_method"] = str(
                                raw.get("method") or "react_fiber"
                            )
                            if attempt > 1:
                                out["castle_method"] += f"_try{attempt}"
                            break
                        out["steps"].append(f"castle_short:{len(cand)}")
                    elif raw.get("error"):
                        out["steps"].append(
                            f"castle_try{attempt}:{_compact_log(raw.get('error'), 48)}"
                        )
                    elif raw.get("raw_len"):
                        out["steps"].append(f"castle_short:{raw.get('raw_len')}")
                bm = timing_cfg["between_mint_ms"]
                # fiber 空时多等：Provider 还在挂
                extra = 400 * attempt if not fiber_token_ok else attempt * 120
                wait_m = random.randint(int(bm[0]), int(bm[1])) + extra
                page.wait_for_timeout(wait_m)
            if not is_castle_token_usable(castle):
                dbg = ""
                if isinstance(last_raw, dict):
                    dbg = (
                        f" method={last_raw.get('method')} "
                        f"err={_compact_log(last_raw.get('error'), 60)}"
                    )
                out["error"] = (
                    f"same-session castle 过短/失败 len={len(castle or '')} "
                    f"min={MIN_CASTLE_LEN}{dbg}"
                )
                out["castle_len"] = len(castle or "")
                return out
            out["castle_len"] = len(castle)
            out["steps"].append(
                f"castle_ok:{out['castle_len']}:{out['castle_method']}"
            )
            _log(
                f"castle ok · {out['castle_len']} · "
                f"{out['castle_method']} · {time.time()-t_mint:.1f}s"
            )

            # 公共头：与页内 connect-es 一致（referer 用当前页 URL）
            page_url = page.url or f"{SITE}/sign-up"
            grpc_headers = {
                "content-type": "application/grpc-web+proto",
                "x-grpc-web": "1",
                "x-user-agent": "connect-es/2.1.1",
                "accept": "*/*",
            }

            # Turnstile 并行预解：与发码/收码/验码重叠（本地 solver 通常 15–20s）
            ts_token = turnstile_token
            ts_holder: dict[str, Any] = {"token": None, "error": None, "t0": 0.0}
            ts_thread: Optional[threading.Thread] = None
            do_ts_parallel = (
                _ts_parallel_enabled()
                and not ts_token
                and callable(solve_turnstile)
            )

            def _ts_worker(sk: str) -> None:
                ts_holder["t0"] = time.time()
                try:
                    tok = solve_turnstile(sk)
                    ts_holder["token"] = tok
                except Exception as e:
                    ts_holder["error"] = str(e)

            if do_ts_parallel:
                sk = (cfg.get("site_key") or "0x4AAAAAAAhr9JGVDZbrZOo0").strip()
                _log("Turnstile 并行预解中…")
                ts_thread = threading.Thread(
                    target=_ts_worker, args=(sk,), daemon=True, name="ts-presolve"
                )
                ts_thread.start()
                out["steps"].append("turnstile_parallel:start")

            # 2) CreateEmailValidationCode —— 页内 fetch（真 Chrome TLS + cookie）
            pc = timing_cfg["pre_code_ms"]
            lo_c, hi_c = int(pc[0]), int(pc[1])
            if hi_c > 0:
                page.wait_for_timeout(random.randint(max(0, lo_c), max(lo_c, hi_c)))
            r1 = _page_binary_post(
                page,
                f"{SITE}/auth_mgmt.AuthManagement/CreateEmailValidationCode",
                encode_grpc_create_email(email),
                grpc_headers,
            )
            out["steps"].append(f"create_code:{r1['status']}")
            if r1.get("error"):
                out["error"] = f"CreateEmailValidationCode fetch err: {r1['error']}"
                return out
            if r1["status"] != 200:
                # 附带 body 前缀方便排障
                head = (r1.get("body") or b"")[:120]
                out["error"] = (
                    f"CreateEmailValidationCode HTTP {r1['status']} body={head!r}"
                )
                return out
            _log(f"发码 ok · {r1['status']}")

            # 3) 取验证码
            code = verification_code
            if not code and fetch_code:
                code = fetch_code(email)
            if not code:
                out["error"] = "no verification code"
                return out
            code = str(code).strip()
            out["code"] = code
            out["steps"].append(f"code:{code}")
            _log(f"验证码 {code}")

            # 4) VerifyEmailValidationCode
            r2 = _page_binary_post(
                page,
                f"{SITE}/auth_mgmt.AuthManagement/VerifyEmailValidationCode",
                encode_grpc_verify_email(email, code),
                grpc_headers,
            )
            out["steps"].append(f"verify_code:{r2['status']}")
            if r2.get("error"):
                out["error"] = f"VerifyEmailValidationCode fetch err: {r2['error']}"
                return out
            if r2["status"] != 200:
                head = (r2.get("body") or b"")[:120]
                out["error"] = (
                    f"VerifyEmailValidationCode HTTP {r2['status']} body={head!r}"
                )
                return out
            _log(f"验码 ok · {r2['status']}")

            # 5) 收 Turnstile：优先吃并行结果；并行已好则不再空等
            if ts_thread is not None:
                # 邮箱快时 solver 可能还没完；最多再等 90s
                remain = max(1.0, 90.0 - (time.time() - (ts_holder.get("t0") or time.time())))
                ts_thread.join(timeout=remain)
                if ts_thread.is_alive():
                    out["error"] = "turnstile parallel timeout"
                    return out
                if ts_holder.get("token") and ts_holder["token"] != "CAPTCHA_FAIL":
                    ts_token = ts_holder["token"]
                    elapsed_ts = round(time.time() - float(ts_holder.get("t0") or t0), 1)
                    out["steps"].append(f"turnstile_parallel:ok:{elapsed_ts}s")
                    _log(f"Turnstile ok · 并行 {elapsed_ts}s")
                else:
                    err = ts_holder.get("error") or "empty"
                    out["steps"].append(f"turnstile_parallel:fail:{_compact_log(err, 40)}")
                    _log(f"Turnstile 并行失败，同步补解 · {_compact_log(err, 60)}")
                    ts_token = None
            if not ts_token and solve_turnstile:
                # 仅同步补解前做极短抖动
                pt = timing_cfg["pre_turnstile_ms"]
                lo, hi = int(pt[0]), int(pt[1])
                if hi > 0:
                    page.wait_for_timeout(random.randint(max(0, lo), max(lo, hi)))
                _log("Turnstile 同步求解…")
                t_ts = time.time()
                ts_token = solve_turnstile(cfg["site_key"])
                out["steps"].append(f"turnstile_sync:{round(time.time()-t_ts,1)}s")
            if not ts_token or ts_token == "CAPTCHA_FAIL":
                out["error"] = "turnstile fail"
                return out
            out["turnstile_len"] = len(ts_token)
            out["steps"].append("turnstile_ok")
            if "turnstile_parallel:ok" not in " ".join(out["steps"]):
                _log("Turnstile ok · 同步")

            # 提交前再铸一次 castle（更贴近点击 Create 瞬间）；fast 档几乎不空等
            ps = timing_cfg["pre_signup_ms"]
            lo_ps, hi_ps = int(ps[0]), int(ps[1])
            if hi_ps > 0:
                page.wait_for_timeout(random.randint(max(0, lo_ps), max(lo_ps, hi_ps)))
            raw3 = page.evaluate(_MINT_JS, mint_js_args())
            if isinstance(raw3, dict) and raw3.get("token"):
                cand = str(raw3["token"])
                if is_castle_token_usable(cand):
                    castle = cand
                    out["castle_method"] = (
                        str(raw3.get("method") or out["castle_method"]) + "+pre_submit"
                    )
                    out["castle_len"] = len(castle)
                    out["steps"].append(f"castle_refresh:{len(castle)}")

            # 6) signup —— 页内 fetch（同 cookie / 同 TLS / 同页 castle）
            if not cfg.get("action_id"):
                out["error"] = "missing next-action id"
                return out
            body = {
                "emailValidationCode": code,
                "createUserAndSessionRequest": {
                    "email": email,
                    "givenName": given,
                    "familyName": family,
                    "clearTextPassword": password,
                    "tosAcceptedVersion": "$undefined",
                },
                "turnstileToken": ts_token,
                "conversionId": conversion_id,
                "promptOnDuplicateEmail": True,
                "castleRequestToken": castle,
            }
            signup_headers = {
                "accept": "text/x-component",
                "content-type": "text/plain;charset=UTF-8",
                "next-router-state-tree": cfg["state_tree"],
                "next-action": cfg["action_id"],
            }
            payload = json.dumps([body], ensure_ascii=False)

            def _read_sso_jar() -> tuple[str, str]:
                try:
                    cookies = ctx.cookies()
                except Exception:
                    cookies = []
                return _cookies_sso(cookies)

            def _signup_hard_fail(text_body: str) -> Optional[str]:
                """
                不可重试的业务错误。

                严禁在完整 RSC/JS 大包上做 "duplicate"+"email" 宽松匹配：
                成功响应对常夹带 i18n 文案（"Duplicate account"、
                "account already exists"、promptOnDuplicateEmail），
                会把好号误杀成 duplicate（23:56 批次 #1/#2）。
                """
                t = text_body or ""
                if not t:
                    return None
                # 验证码失效：字段名稳定，可在任意 body 上认
                if (
                    "invalid-validation" in t
                    or "Email validation code is invalid" in t
                    or "email:invalid" in t
                ):
                    return "email validation code invalid"

                # 大 body（>4KB）多半是 RSC 碎片 + i18n 字典，不当 hard fail
                # 真 duplicate 业务错误通常是短 E{...} / digest 包
                if len(t) > 4000:
                    return None

                low = t.lower()
                # 短错误体上的明确业务码
                hard_patterns = (
                    (r'"code"\s*:\s*"email_already[^"]*"', "duplicate email"),
                    (r"emailalreadyexists", "duplicate email"),
                    (r"email_already_exists", "duplicate email"),
                    (r"duplicate_email", "duplicate email"),
                    (r"email\s+is\s+already\s+registered", "duplicate email"),
                )
                for pat, label in hard_patterns:
                    if re.search(pat, low):
                        return label
                return None

            # signup 重试：
            # 1) 网络断流 Failed to fetch
            # 2) HTTP 200 但 body 无 set-cookie（#14 类：action 过期 / RSC digest 空响）
            # 重试策略：刷新 action_id + 重 mint castle（不重解 turnstile，验证码仍有效）
            try:
                max_signup_tries = max(1, min(5, int(os.getenv("GROK_SIGNUP_RETRIES") or "3")))
            except ValueError:
                max_signup_tries = 3
            r3: dict[str, Any] = {"status": 0, "error": "not_started", "text": ""}
            text = ""
            set_urls: list[str] = []
            for s_try in range(1, max_signup_tries + 1):
                if s_try > 1:
                    _log(f"signup 重试 #{s_try}/{max_signup_tries}")
                    out["steps"].append(f"signup_retry:{s_try}")
                    try:
                        # 短退避：1.0 / 1.6 / 2.2s …
                        page.wait_for_timeout(600 + s_try * 400)
                    except Exception:
                        time.sleep(0.6 + s_try * 0.4)
                    # action_id 过期时 server action 仍 200 但无 set-cookie → 必须刷新
                    try:
                        cfg = _refresh_action_from_page(page, cfg, log=_log)
                        if cfg.get("action_id"):
                            signup_headers["next-action"] = cfg["action_id"]
                            signup_headers["next-router-state-tree"] = cfg.get(
                                "state_tree"
                            ) or signup_headers.get("next-router-state-tree") or ""
                            out["action_id"] = (cfg.get("action_id") or "")[:24]
                            out["steps"].append(
                                f"action_refresh:{(cfg.get('action_id') or '')[:16]}"
                            )
                    except Exception as e:
                        out["steps"].append(f"action_refresh_err:{type(e).__name__}")
                    # 再 mint castle
                    try:
                        raw_r = page.evaluate(_MINT_JS, mint_js_args())
                        if isinstance(raw_r, dict) and raw_r.get("token"):
                            cand_r = str(raw_r["token"])
                            if is_castle_token_usable(cand_r):
                                castle = cand_r
                                body["castleRequestToken"] = castle
                                out["castle_len"] = len(castle)
                                out["steps"].append(f"castle_retry:{len(castle)}")
                    except Exception:
                        pass
                    payload = json.dumps([body], ensure_ascii=False)

                r3 = _page_text_post(
                    page,
                    f"{SITE}/sign-up",
                    payload,
                    signup_headers,
                )
                text = r3.get("text") or ""
                st = int(r3.get("status") or 0)
                out["signup_status"] = st
                out["signup_body_head"] = text[:800]
                if st:
                    out["steps"].append(f"signup:{st}")
                else:
                    out["steps"].append("signup:0")

                if r3.get("error"):
                    err_s = str(r3.get("error") or "")
                    out["steps"].append(f"signup_net:{_compact_log(err_s, 48)}")
                    _log(f"signup 网络失败 try#{s_try}: {_compact_log(err_s, 80)}")
                    # 非网络类不重试
                    if (
                        err_s
                        and "Failed to fetch" not in err_s
                        and "NetworkError" not in err_s
                        and "net::" not in err_s.lower()
                    ):
                        out["error"] = f"signup fetch err: {_compact_log(err_s, 100)}"
                        return out
                    if s_try >= max_signup_tries:
                        out["error"] = f"signup fetch err: {_compact_log(err_s, 100)}"
                        return out
                    continue

                # 落盘最近一次 body
                try:
                    dump = _BASE / "logs" / "same_session_last_signup_body.txt"
                    dump.write_text(text, encoding="utf-8", errors="ignore")
                    out["signup_body_dump"] = str(dump)
                except Exception:
                    pass

                # 先看 set-cookie / jar —— 成功体常夹 i18n「Duplicate account」
                set_urls = _extract_set_cookie_urls(text)
                out["set_cookie_urls"] = len(set_urls)
                if set_urls:
                    out["set_cookie_url0_len"] = len(set_urls[0])
                    out["set_cookie_url0_host"] = re.sub(
                        r"^https?://([^/]+).*$", r"\1", set_urls[0]
                    )
                    _log(f"signup {st} · castle={len(castle)} · set-cookie={len(set_urls)}")
                    break

                # body 无 URL 时先 peek jar（偶发 cookie 已写）
                s_peek, _ = _read_sso_jar()
                if s_peek:
                    out["steps"].append("signup:jar_after_empty_body")
                    _log(f"signup {st} · body 无 set-cookie 但 jar 已有 SSO")
                    break

                # 无 set-cookie 才判 hard fail（短错误体）
                hard = _signup_hard_fail(text)
                if hard:
                    out["error"] = hard
                    out["steps"].append(f"signup_hard:{hard}")
                    return out

                # #14：200 + RSC digest / 空业务响 → 重试
                digest_m = re.search(r'"digest"\s*:\s*"(\d+)"', text)
                digest_note = f" digest={digest_m.group(1)}" if digest_m else ""
                out["steps"].append(f"signup_no_setcookie:try{s_try}")
                _log(
                    f"signup {st} 无 set-cookie · try#{s_try}/{max_signup_tries}"
                    f"{digest_note} · 刷新 action 后重试"
                )
                if s_try >= max_signup_tries:
                    out["error"] = (
                        f"signup {st} 但 body 无 set-cookie URL "
                        f"body={text[:200]}"
                    )
                    return out
                # 继续下一轮
                continue

            # 跟随 set-cookie —— 必须同浏览器会话跟完整嵌套链
            # 链：grokipedia → grokusercontent → grok.com → auth.x.ai → accounts
            # 独立 curl 无注册会话 cookie → 400/auth-error，废的

            def _follow_set_cookie(urls: list[str], tag: str = "set_cookie") -> tuple[str, str]:
                """
                最短路径（已验证有效）：
                1) 完整 T-chunk URL
                2) context.request.get（与 Chrome 同 cookie 罐，自动跟 302）
                3) 失败再 page.goto 一次入口
                禁止：独立 curl、禁止手动 5 站 hop
                """
                if not urls:
                    return "", ""
                entry = urls[0]
                chain = _expand_set_cookie_chain(entry)
                hosts = [re.sub(r"^https?://([^/]+).*$", r"\1", x) for x in chain]
                out["set_cookie_chain"] = hosts
                out["set_cookie_url0_len"] = len(entry)
                short_hosts = "→".join(h.split(".")[0] for h in hosts[:5]) if hosts else "?"

                # 先 peek jar：有时 set-cookie 已同步写进 context，可省一次 request
                s1, s2 = _read_sso_jar()
                if s1:
                    out["steps"].append(f"{tag}:jar_peek_ok")
                    _log(f"cookie 链完成 · jar 直取 · {short_hosts}")
                    return s1, s2 or s1

                # --- 主路径：context.request 共享 cookie 罐 ---
                req_status: Any = None
                try:
                    # 超时收紧：有 sso 时通常几秒内落 jar
                    resp = ctx.request.get(entry, timeout=25000, max_redirects=15)
                    req_status = resp.status
                    out["steps"].append(f"{tag}:req:{resp.status}")
                except Exception as e:
                    out["steps"].append(f"{tag}:req_err:{type(e).__name__}")
                    req_status = f"err:{type(e).__name__}"

                s1, s2 = _read_sso_jar()
                if s1:
                    out["steps"].append(f"{tag}:jar_ok")
                    # 403 常见但 jar 已有 sso，不算失败，不吓人
                    note = f"http={req_status}" if req_status not in (200, 302, 303, 307, 308) else "ok"
                    _log(f"cookie 链完成 · {short_hosts} · {note}")
                    return s1, s2 or s1

                # request 后短轮询（约 1.2s），常比立刻 page.goto 更稳更快
                for _ in range(6):
                    try:
                        page.wait_for_timeout(200)
                    except Exception:
                        time.sleep(0.2)
                    s1, s2 = _read_sso_jar()
                    if s1:
                        out["steps"].append(f"{tag}:jar_poll_ok")
                        _log(f"cookie 链完成 · 短轮询 · {short_hosts}")
                        return s1, s2 or s1

                _log(f"cookie 链未落 jar · {short_hosts} · http={req_status}，改 page.goto")

                # --- 辅：page.goto 入口一次，浏览器自己跟 302 ---
                try:
                    page.goto(entry, wait_until="commit", timeout=45000)
                    out["steps"].append(f"{tag}:goto")
                except Exception as e:
                    out["steps"].append(f"{tag}:goto_err:{type(e).__name__}")
                    _log(f"page.goto 跟链失败 · {_compact_log(e, 80)}")
                    # 最后：location 赋值
                    try:
                        page.evaluate(
                            """(u) => { window.location.href = u; return true; }""",
                            entry,
                        )
                        page.wait_for_load_state("domcontentloaded", timeout=30000)
                        out["steps"].append(f"{tag}:location")
                    except Exception as e2:
                        out["steps"].append(f"{tag}:loc_err:{type(e2).__name__}")

                # 最多约 2.4s 轮询 jar
                for _ in range(8):
                    s1, s2 = _read_sso_jar()
                    if s1:
                        out["steps"].append(f"{tag}:jar_ok")
                        _log(f"cookie 链完成 · page.goto · {short_hosts}")
                        return s1, s2 or s1
                    try:
                        page.wait_for_timeout(300)
                    except Exception:
                        time.sleep(0.3)

                try:
                    out["cookie_names"] = [
                        f"{c.get('name')}@{c.get('domain')}"
                        for c in (ctx.cookies() or [])
                    ][:40]
                except Exception:
                    pass
                return "", ""

            sso = sso_rw = ""
            if set_urls:
                sso, sso_rw = _follow_set_cookie(set_urls)
            else:
                # 重试循环里 jar_after_empty_body 已确认 jar 有 SSO
                sso, sso_rw = _read_sso_jar()
                if sso:
                    out["steps"].append("set_cookie:skip_jar_ready")
                    _log("cookie 链跳过 · jar 已有 SSO")
                else:
                    out["error"] = (
                        f"signup {out.get('signup_status') or 200} 但 body 无 set-cookie URL "
                        f"body={text[:200]}"
                    )
                    return out

            if not sso:
                # 再短等 jar（约 1.5s）
                for _ in range(5):
                    sso, sso_rw = _read_sso_jar()
                    if sso:
                        break
                    page.wait_for_timeout(300)

            if not sso:
                out["error"] = (
                    f"no sso after set-cookie follow "
                    f"status={r3['status']} set_urls={len(set_urls)} "
                    f"url0_len={out.get('set_cookie_url0_len')} "
                    f"cookies={out.get('cookie_names')}"
                )
                return out

            out["sso"] = sso
            out["sso_rw"] = sso_rw
            out["sso_len"] = len(sso)
            out["ok"] = True
            out["steps"].append("sso_ok")
            out["page_url"] = page_url
            _log(f"SSO 到手 · {len(sso)}")
        finally:
            # 关 context；池化时不关 camoufox 进程
            try:
                if ctx is not None:
                    ctx.close()
            except Exception:
                pass
            if camoufox_cm is not None and not from_pool:
                try:
                    camoufox_cm.__exit__(None, None, None)
                except Exception:
                    pass
            pw = out.pop("_pw", None)
            if pw is not None:
                try:
                    pw.stop()
                except Exception:
                    pass
    except Exception as e:
        out["error"] = f"same_session 异常: {e}"
        out["steps"].append(f"exc:{e}")
        # 异常路径也尽量收尾浏览器资源
        try:
            if ctx is not None:
                ctx.close()
        except Exception:
            pass
        if camoufox_cm is not None and not from_pool:
            try:
                camoufox_cm.__exit__(None, None, None)
            except Exception:
                pass
        pw = out.pop("_pw", None)
        if pw is not None:
            try:
                pw.stop()
            except Exception:
                pass
    finally:
        out["elapsed_s"] = round(time.time() - t0, 2)
        # 恢复业务代理环境（多账号循环下一号还要读 GROK_PROXY）
        for k, v in (_saved_biz or {}).items():
            if v:
                os.environ[k] = v
        # 可选清理临时 profile
        if fresh_profile and (os.getenv("GROK_SAME_SESSION_KEEP_PROFILE") or "0") not in (
            "1",
            "true",
            "on",
        ):
            try:
                if profile_dir and profile_dir.exists() and "same_session_profiles" in str(
                    profile_dir
                ):
                    shutil.rmtree(profile_dir, ignore_errors=True)
            except Exception:
                pass

    return out
