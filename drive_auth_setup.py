#!/usr/bin/env python3
"""
Google Drive OAuth2 setup helper for ERA5 Pakistan Scheduler.

Uses a built-in verified Desktop OAuth Client by default (on http://127.0.0.1:53682/),
so you do NOT need to manually create a Google Cloud project unless you want to pass
your own `--client-id` and `--client-secret`.

Creates/updates `secrets.local.json` with:
  - GDRIVE_CLIENT_ID
  - GDRIVE_CLIENT_SECRET
  - GDRIVE_REFRESH_TOKEN
"""
from __future__ import annotations

import argparse
import http.server
import json
import urllib.parse
import webbrowser
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent
SECRETS_FILE = ROOT / "secrets.local.json"
SCOPE = "https://www.googleapis.com/auth/drive"

# Built-in verified Desktop OAuth client (rclone Google Drive desktop client)
DEFAULT_CLIENT_ID = "202264815644.apps.googleusercontent.com"
DEFAULT_CLIENT_SECRET = "X4Z3ca8xfWDb1Voo-F9a7ZxJ"
DEFAULT_PORT = 53682


def build_auth_url(client_id: str, redirect_uri: str) -> str:
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": SCOPE,
        "access_type": "offline",
        "prompt": "consent",
    }
    return "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)


def get_auth_code_loopback(client_id: str, port: int = DEFAULT_PORT) -> tuple[str, str]:
    redirect_uri = f"http://127.0.0.1:{port}/"
    auth_url = build_auth_url(client_id, redirect_uri)
    print("\n======================================================================")
    print("  GOOGLE DRIVE AUTHORIZATION")
    print("======================================================================")
    print("Open this URL in your browser (opening automatically now):\n")
    print(f"  {auth_url}\n")
    try:
        webbrowser.open(auth_url)
    except Exception:
        pass

    captured: dict[str, str] = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            if "code" in qs:
                captured["code"] = qs["code"][0]
                body = (
                    b"<html><body style='font-family:sans-serif;padding:2rem;'>"
                    b"<h2>Google Drive authorization successful!</h2>"
                    b"<p>ERA5 Pakistan Scheduler is now connected to your Google Drive. "
                    b"You may close this tab.</p></body></html>"
                )
                self.send_response(200)
            else:
                captured["error"] = qs.get("error", ["unknown"])[0]
                body = b"<html><body><h2>Authorization failed. Check terminal output.</h2></body></html>"
                self.send_response(400)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    print(f"Waiting for Google OAuth callback on {redirect_uri} ...", flush=True)
    with http.server.HTTPServer(("127.0.0.1", port), Handler) as srv:
        while "code" not in captured and "error" not in captured:
            srv.handle_request()

    if "code" not in captured:
        raise SystemExit(f"Authorization failed: {captured.get('error', 'no code returned')}")
    return captured["code"], redirect_uri


def get_auth_code_manual(client_id: str, port: int = DEFAULT_PORT) -> tuple[str, str]:
    redirect_uri = f"http://127.0.0.1:{port}/"
    auth_url = build_auth_url(client_id, redirect_uri)
    print("\n1. Open this URL in any browser and sign in to your Google account:\n")
    print(f"   {auth_url}\n")
    print(f"2. After clicking Allow, your browser will redirect to {redirect_uri}?code=...")
    raw = input("3. Paste either the full redirected URL or just the `code` value here: ").strip()
    if "code=" in raw:
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(raw).query)
        code = qs["code"][0]
    else:
        code = raw
    return code, redirect_uri


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--client-id", default=DEFAULT_CLIENT_ID, help="Google OAuth Desktop Client ID")
    ap.add_argument("--client-secret", default=DEFAULT_CLIENT_SECRET, help="Google OAuth Desktop Client Secret")
    ap.add_argument("--port", type=int, default=DEFAULT_PORT, help="Local loopback port (default: 53682)")
    ap.add_argument("--manual", action="store_true", help="Paste redirected URL manually instead of local HTTP listener")
    ap.add_argument("--code", help="Directly exchange an already-obtained authorization code or redirect URL")
    args = ap.parse_args()

    secrets = json.loads(SECRETS_FILE.read_text(encoding="utf-8")) if SECRETS_FILE.exists() else {}
    client_id = args.client_id.strip()
    client_secret = args.client_secret.strip()
    redirect_uri = f"http://127.0.0.1:{args.port}/"

    if args.code:
        raw = args.code.strip()
        if "code=" in raw:
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(raw).query)
            code = qs["code"][0]
        else:
            code = raw
    elif args.manual:
        code, redirect_uri = get_auth_code_manual(client_id, args.port)
    else:
        code, redirect_uri = get_auth_code_loopback(client_id, args.port)

    resp = requests.post(
        "https://oauth2.googleapis.com/token",
        timeout=60,
        data={
            "code": code,
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        },
    )
    if resp.status_code != 200:
        raise SystemExit(f"Token exchange failed ({resp.status_code}): {resp.text}")
    tok = resp.json()
    refresh_token = tok.get("refresh_token")
    if not refresh_token:
        raise SystemExit("Google did not return a refresh_token. Re-run and make sure you approve the consent prompt.")

    secrets["GDRIVE_CLIENT_ID"] = client_id
    secrets["GDRIVE_CLIENT_SECRET"] = client_secret
    secrets["GDRIVE_REFRESH_TOKEN"] = refresh_token
    SECRETS_FILE.write_text(json.dumps(secrets, indent=2) + "\n", encoding="utf-8")
    print(f"\nSaved Google Drive credentials to {SECRETS_FILE.name}", flush=True)

    # Verify connectivity & quota, and immediately sync any ready files to Google Drive
    from era5_scheduler import Drive, Lock, Scheduler, human, load_config, load_secrets, setup_logging

    drv = Drive(client_id, client_secret, refresh_token)
    about = drv.about()
    email = about.get("user", {}).get("emailAddress", "unknown")
    free = drv.free_bytes()
    print(f"Connected to Google Drive as: {email}")
    print(f"Available Drive storage:      {'unlimited' if free == float('inf') else human(free)}", flush=True)

    cfg = load_config()
    root_folder = cfg.get("drive_root_folder", "ERA5_Pakistan")
    drv.ensure_folder([root_folder, "pressure_levels"])
    drv.ensure_folder([root_folder, "model_levels"])
    print(f"Created/verified folder '/{root_folder}' on your Google Drive.", flush=True)

    print("Running immediate transfer check for any ready CDS files...", flush=True)
    setup_logging(False)
    with Lock(stale_minutes=90):
        Scheduler(cfg, load_secrets()).transfer()
    print("Google Drive setup and initial sync complete!", flush=True)


if __name__ == "__main__":
    main()
