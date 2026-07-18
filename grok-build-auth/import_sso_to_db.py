#!/usr/bin/env python3
"""
Read all SSO JSON files from sso_output/ and import them into the running
Docker service's Postgres DB via the admin API.

Usage:
    python import_sso_to_db.py                  # default: localhost:40081, password from .env
    python import_sso_to_db.py --host 127.0.0.1 --port 40081 --password change-me
"""
import argparse
import glob
import json
import os
import sys
import time
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. pip install requests")
    sys.exit(1)

BASE_DIR = Path(__file__).resolve().parent
SSO_DIR = BASE_DIR / "sso_output"


def load_env():
    """Load .env for ADMIN_PASSWORD."""
    env_path = BASE_DIR.parent / ".env"
    if not env_path.exists():
        env_path = BASE_DIR / ".env"
    if not env_path.exists():
        return {}
    env = {}
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip("'\"")
    return env


def collect_sso_files(sso_dir: Path) -> list[Path]:
    """Collect unique SSO JSON files, dedup by email (keep latest)."""
    files = sorted(sso_dir.glob("sso_*.json"))
    by_email: dict[str, Path] = {}
    for f in files:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            email = data.get("email", "")
            sso = data.get("sso", "")
            if email and sso and sso.startswith("eyJ"):
                # keep latest (sorted by filename timestamp)
                by_email[email] = f
        except Exception:
            continue
    return list(by_email.values())


def admin_login(base_url: str, password: str) -> str:
    """Login to admin API, return session token."""
    r = requests.post(
        f"{base_url}/admin/api/login",
        json={"password": password},
        timeout=10,
    )
    r.raise_for_status()
    token = r.json().get("token")
    if not token:
        raise RuntimeError(f"Login failed: {r.text}")
    return token


def import_sso(base_url: str, token: str, sso_cookies: list[str]) -> str:
    """Submit SSO import job, return job_id."""
    r = requests.post(
        f"{base_url}/admin/api/accounts/import-sso",
        json={
            "sso_cookies": sso_cookies,
            "merge": True,
            "max_workers": 8,
        },
        headers={"X-Admin-Token": token},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    job_id = data.get("job_id")
    if not job_id:
        raise RuntimeError(f"Import failed: {r.text}")
    return job_id


def poll_job(base_url: str, token: str, job_id: str, timeout: int = 300) -> dict:
    """Poll import job until done."""
    start = time.time()
    last_pct = -1
    while time.time() - start < timeout:
        r = requests.get(
            f"{base_url}/admin/api/accounts/import-sso/jobs/{job_id}",
            headers={"X-Admin-Token": token},
            timeout=10,
        )
        r.raise_for_status()
        job = r.json()
        status = job.get("status", "")
        pct = job.get("percent", 0)
        done = job.get("done", 0)
        total = job.get("total", 0)
        success = job.get("success", 0)
        fail = job.get("fail", 0)

        if pct != last_pct or status in ("done", "error"):
            print(f"  [{status}] {done}/{total} ({pct}%)  ok={success} fail={fail}")
            last_pct = pct

        if status == "done":
            return job
        if status == "error":
            print(f"  ERROR: {job.get('error')}")
            return job

        time.sleep(2)

    print(f"  TIMEOUT after {timeout}s")
    return {"status": "timeout"}


def main():
    parser = argparse.ArgumentParser(description="Import SSO accounts to Docker DB")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=40081)
    parser.add_argument("--password", default=None, help="Admin password (default: from .env)")
    parser.add_argument("--sso-dir", default=str(SSO_DIR), help="SSO output directory")
    args = parser.parse_args()

    base_url = f"http://{args.host}:{args.port}"

    # Get password
    password = args.password
    if not password:
        env = load_env()
        password = env.get("GROK2API_ADMIN_PASSWORD", "change-me")
    print(f"[*] Target: {base_url}")
    print(f"[*] Password: {'*' * len(password)}")

    # Collect SSO files
    sso_dir = Path(args.sso_dir)
    if not sso_dir.exists():
        print(f"ERROR: SSO directory not found: {sso_dir}")
        sys.exit(1)

    files = collect_sso_files(sso_dir)
    if not files:
        print(f"ERROR: No valid SSO files in {sso_dir}")
        sys.exit(1)

    print(f"[*] Found {len(files)} unique SSO accounts")

    # Load SSO JWTs — use email----sso format so _parse_sso_lines extracts email
    sso_cookies = []
    email_map = {}
    for f in files:
        data = json.loads(f.read_text(encoding="utf-8"))
        email = data["email"]
        sso = data["sso"]
        # Format: email----sso  (parsed by _parse_sso_lines in admin_routes.py)
        sso_cookies.append(f"{email}----{sso}")
        email_map[sso] = email

    for line in sso_cookies:
        email, sso = line.split("----")
        print(f"  {email:42s}  sso={sso[:30]}...")

    # Login
    print(f"\n[1/3] Admin login...")
    token = admin_login(base_url, password)
    print(f"  token={token[:20]}...")

    # Import
    print(f"\n[2/3] Submitting {len(sso_cookies)} SSO cookies...")
    job_id = import_sso(base_url, token, sso_cookies)
    print(f"  job_id={job_id}")

    # Poll
    print(f"\n[3/3] Polling import job...")
    job = poll_job(base_url, token, job_id)
    print(f"\n{'='*60}")
    print(f"  status:  {job.get('status')}")
    print(f"  total:   {job.get('total')}")
    print(f"  success: {job.get('success')}")
    print(f"  fail:    {job.get('fail')}")
    print(f"  imported: {len(job.get('imported', []))}")

    # Show imported accounts
    for item in job.get("imported", []):
        if isinstance(item, dict):
            email = str(item.get("email") or "?")
            uid = str(item.get("id") or "?")
            print(f"    + {email:42s}  id={uid[:40]}")
        elif item:
            print(f"    + {item}")

    if job.get("results"):
        print(f"\n  Results detail:")
        for r in job.get("results", []):
            if isinstance(r, dict):
                ok = r.get("ok", False)
                email = r.get("email", "?")
                err = r.get("error", "")
                mark = "OK" if ok else "FAIL"
                print(f"    [{mark}] {email:42s}  {err}")

    print(f"{'='*60}")
    if job.get("status") == "done" and job.get("fail", 0) == 0:
        print("  ALL DONE — accounts are in the DB and ready to use")
    else:
        print(f"  Partial success: {job.get('success',0)} ok, {job.get('fail',0)} failed")


if __name__ == "__main__":
    main()
