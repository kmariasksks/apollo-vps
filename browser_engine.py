import os
import json
import threading
import time
from urllib.parse import urlencode

from patchright.sync_api import sync_playwright

PROXY_SERVER = os.environ.get("PROXY_SERVER", "").strip()
PROXY_USER = os.environ.get("PROXY_USER", "").strip()
PROXY_PASS = os.environ.get("PROXY_PASS", "").strip()
PROFILE_DIR = os.environ.get("PROFILE_DIR", "/app/profile")

APOLLO_HOME = "https://app.apollo.io/"
APOLLO_PEOPLE = "https://app.apollo.io/#/people"

_playwright = None
_context = None
_page = None
_browser_lock = threading.RLock()
_started = False


def _proxy_kwargs():
    if not PROXY_SERVER:
        return {}
    proxy = {"server": PROXY_SERVER}
    if PROXY_USER:
        proxy["username"] = PROXY_USER
    if PROXY_PASS:
        proxy["password"] = PROXY_PASS
    return {"proxy": proxy}


def start():
    global _playwright, _context, _page, _started
    with _browser_lock:
        if _started:
            return
        print("[browser] стартую постійний Chrome через проксі...", flush=True)
        _playwright = sync_playwright().start()
        _context = _playwright.chromium.launch_persistent_context(
            user_data_dir=PROFILE_DIR,
            channel="chrome",
            headless=False,
            no_viewport=True,
            **_proxy_kwargs(),
        )
        _page = _context.pages[0] if _context.pages else _context.new_page()
        try:
            _page.goto(APOLLO_HOME, wait_until="domcontentloaded", timeout=60000)
        except Exception as e:
            print(f"[browser] попередження при відкритті Apollo: {e}", flush=True)
        _started = True
        print("[browser] Chrome запущено і Apollo відкрито.", flush=True)


def stop():
    global _started
    with _browser_lock:
        try:
            if _context:
                _context.close()
            if _playwright:
                _playwright.stop()
        except Exception as e:
            print(f"[browser] помилка при закритті: {e}", flush=True)
        _started = False


def _ensure_on_apollo():
    try:
        url = _page.url or ""
        if "apollo.io" not in url:
            _page.goto(APOLLO_PEOPLE, wait_until="domcontentloaded", timeout=60000)
            time.sleep(2)
    except Exception as e:
        print(f"[browser] _ensure_on_apollo: {e}", flush=True)


def check_ip():
    with _browser_lock:
        if not _started:
            return {"error": "browser not started"}
        try:
            result = _page.evaluate(
                """
                async () => {
                    try {
                        const r = await fetch("https://api.ipify.org?format=json");
                        return await r.text();
                    } catch (e) { return JSON.stringify({error: String(e)}); }
                }
                """
            )
            try:
                return json.loads(result)
            except Exception:
                return {"raw": result}
        except Exception as e:
            return {"error": str(e)}


def is_logged_in():
    with _browser_lock:
        if not _started:
            return False
        try:
            url = _page.url or ""
            if "/login" in url or "/sign" in url:
                return False
            return True
        except Exception:
            return False


def browser_fetch(method, url, params=None, json_body=None, timeout_ms=60000):
    with _browser_lock:
        if not _started:
            return None, -1
        _ensure_on_apollo()

        full_url = url
        if params:
            full_url = url + ("&" if "?" in url else "?") + urlencode(params)

        try:
            result = _page.evaluate(
                """
                async ({method, url, body}) => {
                    try {
                        const opts = {
                            method: method,
                            headers: { "Content-Type": "application/json" },
                            credentials: "include"
                        };
                        if (body !== null) { opts.body = JSON.stringify(body); }
                        const resp = await fetch(url, opts);
                        const text = await resp.text();
                        return { status: resp.status, body: text };
                    } catch (e) {
                        return { status: -1, body: String(e) };
                    }
                }
                """,
                {"method": method, "url": full_url, "body": json_body},
            )
        except Exception as e:
            print(f"[browser_fetch] evaluate error: {e}", flush=True)
            return None, -1

        status = result.get("status", -1)
        body = result.get("body", "") or ""

        if status == -1:
            print(f"[browser_fetch] fetch failed: {body[:200]}", flush=True)
            return None, -1

        low = body[:2000].lower()
        if ("turnstile" in low or "cf-challenge" in low
                or "just a moment" in low or "challenges.cloudflare.com" in low):
            print("[browser_fetch] виявлено капчу/челендж Cloudflare", flush=True)
            return None, -2

        if status in (401, 403, 429):
            print(f"[browser_fetch] status {status}", flush=True)
            return None, status

        if status != 200:
            print(f"[browser_fetch] unexpected status {status}: {body[:200]}", flush=True)
            return None, status

        try:
            return json.loads(body), 200
        except ValueError:
            print(f"[browser_fetch] 200 але не JSON: {body[:200]}", flush=True)
            return None, -2


def probe():
    with _browser_lock:
        return {
            "started": _started,
            "url": (_page.url if (_started and _page) else None),
            "logged_in": is_logged_in() if _started else False,
        }