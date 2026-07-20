"""
browser_engine.py (v2 — потокобезпечний через виділений потік)
"""

import os
import json
import threading
import queue
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

_cmd_queue = queue.Queue()
_browser_thread = None
_started_event = threading.Event()
_start_error = [None]
_stop_flag = threading.Event()


def _proxy_kwargs():
    if not PROXY_SERVER:
        return {}
    proxy = {"server": PROXY_SERVER}
    if PROXY_USER:
        proxy["username"] = PROXY_USER
    if PROXY_PASS:
        proxy["password"] = PROXY_PASS
    return {"proxy": proxy}


def _op_ensure_on_apollo():
    try:
        url = _page.url or ""
        if "apollo.io" not in url:
            _page.goto(APOLLO_PEOPLE, wait_until="domcontentloaded", timeout=60000)
            time.sleep(2)
    except Exception as e:
        print(f"[browser] _ensure_on_apollo: {e}", flush=True)


def _op_check_ip():
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


def _op_is_logged_in():
    try:
        url = _page.url or ""
        if "/login" in url or "/sign" in url:
            return False
        return True
    except Exception:
        return False


def _op_probe():
    return {
        "started": True,
        "url": (_page.url if _page else None),
        "logged_in": _op_is_logged_in(),
    }


def _op_browser_fetch(method, url, params, json_body):
    _op_ensure_on_apollo()

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


def _browser_loop():
    global _playwright, _context, _page
    try:
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
        print("[browser] Chrome запущено і Apollo відкрито.", flush=True)
    except Exception as e:
        _start_error[0] = e
        _started_event.set()
        print(f"[browser] ПОМИЛКА старту: {e}", flush=True)
        return

    _started_event.set()

    while not _stop_flag.is_set():
        try:
            fn, args, result_holder, done = _cmd_queue.get(timeout=1)
        except queue.Empty:
            continue
        try:
            result_holder[0] = fn(*args)
        except Exception as e:
            result_holder[0] = e
        finally:
            done.set()
            _cmd_queue.task_done()

    try:
        if _context:
            _context.close()
        if _playwright:
            _playwright.stop()
    except Exception as e:
        print(f"[browser] помилка при закритті: {e}", flush=True)


def _run_in_browser_thread(fn, *args, timeout=120):
    result_holder = [None]
    done = threading.Event()
    _cmd_queue.put((fn, args, result_holder, done))
    if not done.wait(timeout=timeout):
        return None
    res = result_holder[0]
    if isinstance(res, Exception):
        print(f"[browser] операція впала: {res}", flush=True)
        return None
    return res


def start():
    global _browser_thread
    if _browser_thread and _browser_thread.is_alive():
        return
    _stop_flag.clear()
    _started_event.clear()
    _start_error[0] = None
    _browser_thread = threading.Thread(target=_browser_loop, daemon=True)
    _browser_thread.start()
    _started_event.wait(timeout=120)
    if _start_error[0] is not None:
        raise _start_error[0]


def stop():
    _stop_flag.set()
    if _browser_thread:
        _browser_thread.join(timeout=30)


def _is_ready():
    return _started_event.is_set() and _start_error[0] is None and \
        _browser_thread and _browser_thread.is_alive()


def browser_fetch(method, url, params=None, json_body=None, timeout_ms=60000):
    if not _is_ready():
        return None, -1
    res = _run_in_browser_thread(_op_browser_fetch, method, url, params, json_body)
    if res is None:
        return None, -1
    return res


def check_ip():
    if not _is_ready():
        return {"error": "browser not started"}
    res = _run_in_browser_thread(_op_check_ip)
    return res if res is not None else {"error": "timeout"}


def is_logged_in():
    if not _is_ready():
        return False
    res = _run_in_browser_thread(_op_is_logged_in)
    return bool(res)


def probe():
    if not _is_ready():
        return {"started": False, "url": None, "logged_in": False}
    res = _run_in_browser_thread(_op_probe)
    return res if res is not None else {"started": False, "url": None, "logged_in": False}