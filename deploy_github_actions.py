#!/usr/bin/env python3
"""
One-command cloud deployment for ERA5 Pakistan Scheduler -> GitHub Actions.

1. Downloads standalone `gh.exe` (GitHub CLI) into `./bin/gh.exe` if not present.
2. Starts GitHub OAuth Device Flow (prints your 8-character code and opens
   https://github.com/login/device in your browser -- no manual token creation needed).
3. Creates (or updates) your GitHub repository (`era5-pakistan-scheduler`).
4. Uploads all repository files (`era5_scheduler.py`, `config.json`,
   `.github/workflows/era5-scheduler.yml`, `state/`, etc.) via the GitHub Contents API
   (never uploading `secrets.local.json`).
5. Encrypts & sets your GitHub Actions Secrets (`CDSAPI_KEY`, `CDSAPI_URL`,
   `GDRIVE_CLIENT_ID`, `GDRIVE_CLIENT_SECRET`, `GDRIVE_REFRESH_TOKEN`) via `gh secret set`.
6. Enables & triggers the `ERA5 Pakistan Scheduler` GitHub Actions workflow in the cloud.
"""
from __future__ import annotations

import argparse
import base64
import io
import json
import os
import subprocess
import time
import webbrowser
import zipfile
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent
BIN_DIR = ROOT / "bin"
SECRETS_FILE = ROOT / "secrets.local.json"

# Official GitHub CLI OAuth Client ID for Device Flow
GH_OAUTH_CLIENT_ID = "178c6fc778ccc68e1d6a"

UPLOAD_PATHS = [
    ".gitignore",
    "README.md",
    "requirements.txt",
    "config.json",
    "era5_scheduler.py",
    "drive_auth_setup.py",
    "deploy_github_actions.py",
    "run_tick.ps1",
    "install_windows_task.ps1",
    ".github/workflows/era5-scheduler.yml",
]


def ensure_gh() -> Path:
    gh_exe = BIN_DIR / "gh.exe"
    if gh_exe.exists():
        return gh_exe
    print("Downloading standalone GitHub CLI (gh.exe)...", flush=True)
    BIN_DIR.mkdir(parents=True, exist_ok=True)
    rel = requests.get("https://api.github.com/repos/cli/cli/releases/latest", timeout=60).json()
    asset_url = next(
        a["browser_download_url"]
        for a in rel.get("assets", [])
        if a["name"].endswith("_windows_amd64.zip")
    )
    zbytes = requests.get(asset_url, timeout=120).content
    with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
        for member in zf.namelist():
            if member.endswith("bin/gh.exe"):
                gh_exe.write_bytes(zf.read(member))
                break
    print(f"Downloaded {gh_exe}", flush=True)
    return gh_exe


def github_device_login() -> str:
    secrets = json.loads(SECRETS_FILE.read_text(encoding="utf-8")) if SECRETS_FILE.exists() else {}
    if secrets.get("GITHUB_TOKEN"):
        r = requests.get(
            "https://api.github.com/user",
            headers={"Authorization": f"Bearer {secrets['GITHUB_TOKEN']}"},
            timeout=30,
        )
        if r.status_code == 200:
            return secrets["GITHUB_TOKEN"]

    resp = requests.post(
        "https://github.com/login/device/code",
        headers={"Accept": "application/json"},
        data={"client_id": GH_OAUTH_CLIENT_ID, "scope": "repo workflow"},
        timeout=30,
    )
    resp.raise_for_status()
    info = resp.json()
    user_code = info["user_code"]
    verify_url = info["verification_uri"]
    device_code = info["device_code"]
    interval = int(info.get("interval", 5)) + 1

    print("\n======================================================================")
    print("  GITHUB CLOUD SCHEDULER AUTHORIZATION")
    print("======================================================================")
    print(f"  1. Open URL:   {verify_url}")
    print(f"  2. Enter Code: {user_code}")
    print("======================================================================\n", flush=True)

    try:
        if hasattr(os, "startfile"):
            os.startfile(verify_url)  # type: ignore[attr-defined]
        else:
            webbrowser.open(verify_url)
    except Exception:
        pass

    print(f"Waiting for you to enter code {user_code} at {verify_url} ...", flush=True)
    while True:
        time.sleep(interval)
        try:
            tr = requests.post(
                "https://github.com/login/oauth/access_token",
                headers={"Accept": "application/json"},
                data={
                    "client_id": GH_OAUTH_CLIENT_ID,
                    "device_code": device_code,
                    "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                },
                timeout=30,
            ).json()
        except requests.RequestException:
            continue
        if "access_token" in tr:
            tok = tr["access_token"]
            secrets = json.loads(SECRETS_FILE.read_text(encoding="utf-8")) if SECRETS_FILE.exists() else {}
            secrets["GITHUB_TOKEN"] = tok
            SECRETS_FILE.write_text(json.dumps(secrets, indent=2) + "\n", encoding="utf-8")
            print("GitHub authorization successful!", flush=True)
            return tok
        err = tr.get("error")
        if err == "authorization_pending":
            continue
        if err == "slow_down":
            interval += 5
            continue
        raise SystemExit(f"GitHub device login failed: {tr}")


def gh_set_secret(gh: Path, tok: str, repo_full: str, name: str, val: str) -> None:
    env = dict(os.environ, GH_TOKEN=tok)
    subprocess.run(
        [str(gh), "secret", "set", name, "-R", repo_full],
        cwd=str(ROOT),
        input=val,
        text=True,
        env=env,
        capture_output=True,
        check=True,
    )


def put_file(session: requests.Session, repo_full: str, rel_path: str, content: bytes) -> None:
    url = f"https://api.github.com/repos/{repo_full}/contents/{rel_path}"
    r = session.get(url, timeout=60)
    payload: dict = {
        "message": f"chore: sync {rel_path} [skip ci]",
        "content": base64.b64encode(content).decode("ascii"),
    }
    if r.status_code == 200:
        existing = r.json()
        payload["sha"] = existing["sha"]
    resp = session.put(url, json=payload, timeout=60)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Failed to upload {rel_path}: {resp.status_code} {resp.text[:300]}")
    print(f"  uploaded {rel_path}", flush=True)


def sync_secrets_to_github(gh: Path, tok: str, repo_full: str) -> set[str]:
    uploaded: set[str] = set()
    if not SECRETS_FILE.exists():
        return uploaded
    secrets = json.loads(SECRETS_FILE.read_text(encoding="utf-8"))
    for k in ("CDSAPI_KEY", "CDSAPI_URL", "GDRIVE_CLIENT_ID", "GDRIVE_CLIENT_SECRET", "GDRIVE_REFRESH_TOKEN"):
        v = secrets.get(k)
        if v:
            gh_set_secret(gh, tok, repo_full, k, str(v))
            print(f"  set GitHub Actions secret: {k}", flush=True)
            uploaded.add(k)
    return uploaded


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo", default="era5-pakistan-scheduler", help="GitHub repository name")
    ap.add_argument("--private", action="store_true", help="Create a private repository (public repos get unlimited free Actions minutes)")
    args = ap.parse_args()

    tok = github_device_login()
    gh = ensure_gh()

    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {tok}", "Accept": "application/vnd.github+json"})
    user = s.get("https://api.github.com/user", timeout=60).json()["login"]
    repo_full = f"{user}/{args.repo}"
    print(f"Authenticated as GitHub user: {user}", flush=True)

    # Create repo if needed
    r = s.get(f"https://api.github.com/repos/{repo_full}", timeout=60)
    if r.status_code == 404:
        print(f"Creating repository {repo_full} ...", flush=True)
        cr = s.post(
            "https://api.github.com/user/repos",
            json={
                "name": args.repo,
                "private": args.private,
                "auto_init": True,
                "description": "Automated ERA5 Pakistan atmospheric reanalysis (37 pressure levels + 137 MARS model levels) scheduler",
            },
            timeout=60,
        )
        cr.raise_for_status()
        time.sleep(2)
    s.put(
        f"https://api.github.com/repos/{repo_full}/actions/permissions/workflow",
        json={"default_workflow_permissions": "write", "can_approve_pull_request_reviews": False},
        timeout=60,
    )

    # Upload project files & state
    files_to_push = list(UPLOAD_PATHS)
    state_dir = ROOT / "state"
    if state_dir.exists():
        for p in sorted(state_dir.rglob("*")):
            if p.is_file():
                files_to_push.append(p.relative_to(ROOT).as_posix())
    print(f"Syncing {len(files_to_push)} files to {repo_full} ...", flush=True)
    for rel_path in files_to_push:
        fp = ROOT / rel_path
        if fp.exists():
            put_file(s, repo_full, rel_path, fp.read_bytes())

    uploaded = sync_secrets_to_github(gh, tok, repo_full)
    if "GDRIVE_REFRESH_TOKEN" not in uploaded:
        print("Waiting up to 10 minutes for Google Drive OAuth to complete so GDRIVE_* secrets can also be pushed to GitHub Actions...", flush=True)
        for _ in range(120):
            time.sleep(5)
            secrets = json.loads(SECRETS_FILE.read_text(encoding="utf-8")) if SECRETS_FILE.exists() else {}
            if secrets.get("GDRIVE_REFRESH_TOKEN"):
                sync_secrets_to_github(gh, tok, repo_full)
                break

    print("Triggering cloud run on GitHub Actions...", flush=True)
    env = dict(os.environ, GH_TOKEN=tok)
    subprocess.run([str(gh), "workflow", "run", "era5-scheduler.yml", "-R", repo_full], cwd=str(ROOT), env=env, check=False)
    print(f"\nCloud scheduler deployed! View live runs at:\n  https://github.com/{repo_full}/actions", flush=True)


if __name__ == "__main__":
    main()
