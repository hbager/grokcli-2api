"""
CloudMail (SkyMail) API Client — reusable client for SkyMail/CloudMail OpenAPI.

API docs: https://doc.skymail.ink

Usage:
    from grok2api.upstream.cloudmail import CloudMail, MailType

    client = CloudMail(api_url, token)
    result = client.list_emails(toEmail="user@example.com", type=MailType.INBOX)
    msg = client.wait_for_email(toEmail="user@example.com", timeout=120)
    token = client.gen_token(email, password)
    client.add_user(email="user@example.com", password="...")
"""

from __future__ import annotations

import time
from enum import IntEnum
from typing import Any

import requests


class MailType(IntEnum):
    """郵件類型"""
    INBOX = 0  # 收件
    SENT = 1   # 發件


class MailStatus(IntEnum):
    """郵件狀態"""
    NORMAL = 0   # 正常
    DELETED = 2  # 已刪除


class CloudMailError(Exception):
    """CloudMail API 例外基底類別"""

    def __init__(self, status: int, message: str):
        self.status = status
        self.message = message
        super().__init__(f"[{status}] {message}")


class CloudMail:
    """CloudMail (SkyMail) OpenAPI 客戶端"""

    def __init__(
        self,
        api_url: str,
        token: str | None = None,
        *,
        timeout: int = 30,
        session: requests.Session | None = None,
    ):
        """
        :param api_url: API 根網址（自架 SkyMail/CloudMail OpenAPI origin）
        :param token:   身份令牌（Authorization）；未提供時僅能呼叫 gen_token
        :param timeout: 單次請求逾時秒數
        :param session: 可傳入自訂 requests.Session（例如設定 proxy）
        """
        self.base_url = api_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self._session = session or requests.Session()
        self._session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json, text/plain, */*",
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                "Origin": self.base_url,
                "Referer": f"{self.base_url}/",
            }
        )
        if token:
            self._session.headers.update({"Authorization": token})

    # ------------------------------------------------------------------ #
    #  模糊匹配便利方法
    # ------------------------------------------------------------------ #
    @staticmethod
    def contains(text: str) -> str:
        """包含匹配：``%text%``"""
        return f"%{text}%"

    @staticmethod
    def startswith(text: str) -> str:
        """開頭匹配：``text%``"""
        return f"{text}%"

    @staticmethod
    def endswith(text: str) -> str:
        """結尾匹配：``%text``"""
        return f"%{text}"

    # ------------------------------------------------------------------ #
    #  內部
    # ------------------------------------------------------------------ #
    def _request(
        self,
        path: str,
        *,
        json_body: dict | None = None,
        need_auth: bool = True,
    ) -> Any:
        if need_auth and not self.token:
            raise CloudMailError(401, "未設定 token，請先呼叫 gen_token 或在建構子傳入 token")
        url = f"{self.base_url}/{path.lstrip('/')}"
        resp = self._session.post(url, json=json_body, timeout=self.timeout)
        if not resp.ok:
            raise CloudMailError(resp.status_code, resp.text)
        if not resp.content:
            return {"success": True}
        result = resp.json()
        # API 統一回傳 {code, message, data}
        code = result.get("code", resp.status_code)
        if code != 200:
            raise CloudMailError(code, result.get("message", "unknown error"))
        return result.get("data")

    # ------------------------------------------------------------------ #
    #  認證
    # ------------------------------------------------------------------ #
    def gen_token(self, email: str, password: str) -> str:
        """POST /api/public/genToken — 生成身份令牌

        生成後會自動更新當前客戶端的 token，舊 token 立即失效。
        :return: 新 token 字串
        """
        data = self._request(
            "/api/public/genToken",
            json_body={"email": email, "password": password},
            need_auth=False,
        )
        token = data["token"]
        self.token = token
        self._session.headers.update({"Authorization": token})
        return token

    @classmethod
    def from_login(cls, api_url: str, email: str, password: str, **kwargs) -> "CloudMail":
        """便利建構：直接用帳密登入並回傳已認證的客戶端"""
        client = cls(api_url, **kwargs)
        client.gen_token(email, password)
        return client

    # ------------------------------------------------------------------ #
    #  郵件查詢
    # ------------------------------------------------------------------ #
    def list_emails(
        self,
        *,
        toEmail: str | None = None,
        sendName: str | None = None,
        sendEmail: str | None = None,
        subject: str | None = None,
        content: str | None = None,
        timeSort: str = "desc",
        type: int | MailType | None = None,
        isDel: int | MailStatus | None = None,
        num: int = 1,
        size: int = 20,
    ) -> list[dict]:
        """POST /api/public/emailList — 郵件查詢（分頁）

        所有篩選欄位支援模糊匹配，可搭配 ``CloudMail.contains()`` 等。
        :param type:   MailType.INBOX / MailType.SENT / None（全部）
        :param isDel:  MailStatus.NORMAL / MailStatus.DELETED / None（全部）
        :param timeSort: ``desc``（最新優先）或 ``asc``（最舊優先）
        :param num:    頁碼（從 1 開始）
        :param size:   每頁數量
        :return: 郵件 list，每封含 emailId, sendEmail, subject, content, text 等
        """
        payload: dict[str, Any] = {"timeSort": timeSort, "num": num, "size": size}
        if toEmail is not None:
            payload["toEmail"] = toEmail
        if sendName is not None:
            payload["sendName"] = sendName
        if sendEmail is not None:
            payload["sendEmail"] = sendEmail
        if subject is not None:
            payload["subject"] = subject
        if content is not None:
            payload["content"] = content
        if type is not None:
            payload["type"] = int(type)
        if isDel is not None:
            payload["isDel"] = int(isDel)
        return self._request("/api/public/emailList", json_body=payload)

    def list_emails_all(
        self,
        *,
        toEmail: str | None = None,
        sendName: str | None = None,
        sendEmail: str | None = None,
        subject: str | None = None,
        content: str | None = None,
        timeSort: str = "desc",
        type: int | MailType | None = None,
        isDel: int | MailStatus | None = None,
        size: int = 50,
    ) -> list[dict]:
        """自動翻頁，回傳符合條件的所有郵件 list"""
        all_mails: list[dict] = []
        num = 1
        while True:
            page = self.list_emails(
                toEmail=toEmail, sendName=sendName, sendEmail=sendEmail,
                subject=subject, content=content, timeSort=timeSort,
                type=type, isDel=isDel, num=num, size=size,
            )
            if not page:
                break
            all_mails.extend(page)
            if len(page) < size:
                break
            num += 1
        return all_mails

    def get_email(self, email_id: int) -> dict | None:
        """以 emailId 取得單封郵件（從查詢結果中篩選）"""
        for mail in self.list_emails_all():
            if mail["emailId"] == email_id:
                return mail
        return None

    def wait_for_email(
        self,
        *,
        timeout: int = 120,
        interval: int = 5,
        toEmail: str | None = None,
        sendName: str | None = None,
        sendEmail: str | None = None,
        subject: str | None = None,
        content: str | None = None,
        type: int | MailType | None = None,
    ) -> dict | None:
        """輪詢等待新郵件

        先記錄現有郵件 ID 集合，持續查詢直到出現新 ID 的郵件。
        :param timeout:  最長等待秒數
        :param interval: 輪詢間隔秒數
        :param 其餘篩選:  同 list_emails（不含分頁參數）
        :return: 第一封新郵件 dict，逾時回 None
        """
        filter_kwargs = dict(
            toEmail=toEmail, sendName=sendName, sendEmail=sendEmail,
            subject=subject, content=content, type=type,
        )
        existing_ids = {m["emailId"] for m in self.list_emails_all(**filter_kwargs)}
        deadline = time.time() + timeout
        while time.time() < deadline:
            for mail in self.list_emails_all(**filter_kwargs):
                if mail["emailId"] not in existing_ids:
                    return mail
            time.sleep(interval)
        return None

    # ------------------------------------------------------------------ #
    #  用戶管理
    # ------------------------------------------------------------------ #

    def website_config(self) -> dict:
        """GET /api/setting/websiteConfig — includes domainList."""
        url = f"{self.base_url}/api/setting/websiteConfig"
        resp = self._session.get(url, timeout=self.timeout)
        if not resp.ok:
            raise CloudMailError(resp.status_code, resp.text)
        result = resp.json() if resp.content else {}
        code = result.get("code", resp.status_code)
        if code != 200:
            raise CloudMailError(code, result.get("message", "unknown error"))
        data = result.get("data")
        return data if isinstance(data, dict) else {}

    def list_domains(self) -> list[str]:
        """Mailbox domains from websiteConfig.domainList."""
        cfg = self.website_config()
        raw = cfg.get("domainList") or cfg.get("domains") or []
        out: list[str] = []
        seen: set[str] = set()
        if isinstance(raw, str):
            raw = [raw]
        if not isinstance(raw, list):
            return []
        for item in raw:
            s = str(item or "").strip().lstrip("@").strip().lower().strip(".")
            if s and s not in seen:
                seen.add(s)
                out.append(s)
        return out

    def add_user(
        self,
        *,
        email: str,
        password: str | None = None,
        role_name: str | None = None,
    ) -> dict:
        """POST /api/public/addUser — 添加單一用戶

        :param email:     郵箱地址（必填）
        :param password:  密碼，不填則自動生成
        :param role_name: 權限身份名，不填則使用預設權限
        """
        user: dict[str, Any] = {"email": email}
        if password is not None:
            user["password"] = password
        if role_name is not None:
            user["roleName"] = role_name
        return self.add_users([user])

    def add_users(self, users: list[dict]) -> dict:
        """POST /api/public/addUser — 批量添加用戶

        :param users: ``[{"email": ..., "password": ..., "roleName": ...}, ...]``
        """
        return self._request("/api/public/addUser", json_body={"list": users})

    # ------------------------------------------------------------------ #
    #  生命週期
    # ------------------------------------------------------------------ #
    def close(self):
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
