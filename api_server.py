from flask import Flask, request, jsonify
import requests
import time
import random
import os
import threading
import queue
import uuid
from dotenv import load_dotenv
load_dotenv()

import browser_engine as be

app = Flask(__name__)

# ── Захист: перевірка X-API-Key на «робочих» ендпоінтах ──────────────────
_PROTECTED_PREFIXES = ("/search", "/company", "/jobs", "/batch-search")


@app.before_request
def _check_api_key():
    # Якщо ключ не заданий у .env — захист вимкнено (щоб не зламати).
    if not API_KEY:
        return None
    path = request.path or ""
    if any(path.startswith(p) for p in _PROTECTED_PREFIXES):
        provided = request.headers.get("X-API-Key", "")
        if provided != API_KEY:
            return jsonify({"error": "unauthorized"}), 401
    return None

PROXY_CONFIGURED = bool(os.environ.get("PROXY_SERVER", "").strip())

NORMAL_DELAY_MIN = float(os.environ.get("DELAY_MIN", 3))
NORMAL_DELAY_MAX = float(os.environ.get("DELAY_MAX", 8))
PAUSE_EVERY = int(os.environ.get("PAUSE_EVERY", 30))
PAUSE_MIN = float(os.environ.get("PAUSE_MIN", 15))
PAUSE_MAX = float(os.environ.get("PAUSE_MAX", 30))

MAX_RETRIES = int(os.environ.get("MAX_RETRIES", 5))
BACKOFF_BASE = float(os.environ.get("BACKOFF_BASE", 10))
BACKOFF_MAX = float(os.environ.get("BACKOFF_MAX", 300))

BATCH_ORG_CHUNK = int(os.environ.get("BATCH_ORG_CHUNK", 10))
BATCH_PER_PAGE = int(os.environ.get("BATCH_PER_PAGE", 100))

CLAY_WEBHOOK_URL = os.environ.get("CLAY_WEBHOOK_URL", "").strip()
CLAY_WEBHOOK_TOKEN = os.environ.get("CLAY_WEBHOOK_TOKEN", "").strip()
NUM_WORKERS = int(os.environ.get("NUM_WORKERS", 2))
API_KEY = os.environ.get("API_KEY", "").strip()
CAPTCHA_WEBHOOK_URL = os.environ.get("CAPTCHA_WEBHOOK_URL", "").strip()
CAPTCHA_PAUSE = float(os.environ.get("CAPTCHA_PAUSE", 120))
_last_captcha_notify = [0.0]
_captcha_active = threading.Event()  # глобальний стоп-кран: капча активна

_throttle_lock = threading.Lock()
_next_allowed = 0.0
_request_count = 0
_session_problem = False


def _throttle():
    global _next_allowed, _request_count
    with _throttle_lock:
        now = time.time()
        delay = random.uniform(NORMAL_DELAY_MIN, NORMAL_DELAY_MAX)
        _request_count += 1
        if PAUSE_EVERY > 0 and _request_count % PAUSE_EVERY == 0:
            extra = random.uniform(PAUSE_MIN, PAUSE_MAX)
            print(f"[throttle] periodic pause +{extra:.1f}s after {_request_count} requests")
            delay += extra
        start_at = max(now, _next_allowed)
        _next_allowed = start_at + delay
        wait = start_at - now
    if wait > 0:
        time.sleep(wait)


def _backoff(attempt):
    wait = min(BACKOFF_MAX, BACKOFF_BASE * (2 ** attempt))
    print(f"[backoff] attempt {attempt + 1} -> sleep {wait:.0f}s")
    time.sleep(wait)

def notify_captcha():
    """Сповістити n8n про капчу (не частіше разу на 5 хв, щоб не спамити)."""
    if not CAPTCHA_WEBHOOK_URL:
        return
    now = time.time()
    if now - _last_captcha_notify[0] < 300:
        return
    _last_captcha_notify[0] = now
    try:
        requests.post(CAPTCHA_WEBHOOK_URL, json={"event": "captcha"}, timeout=10)
        print("[captcha] сповіщення надіслано в n8n")
    except Exception as e:
        print(f"[captcha] не вдалось сповістити: {e}")

def apollo_request(method, url, params=None, json=None, **_ignore):
    global _session_problem
    last_status = None
    for attempt in range(MAX_RETRIES):
        _throttle()
        data, status = be.browser_fetch(method, url, params=params, json_body=json)
        last_status = status

        if status == 200 and data is not None:
            _session_problem = False
            _captcha_active.clear()  # капча точно пройдена — гасимо стоп-кран
            return data, 200

        if status == -2 or status in (401, 403):
            _session_problem = True
            print(f"[apollo_request] status {status} — капча або сесія впала")
            if status == -2:
                _captcha_active.set()  # вмикаємо стоп-кран для всієї черги
                notify_captcha()
                print(f"[apollo_request] чекаю {CAPTCHA_PAUSE:.0f}s щоб встигли пройти капчу")
                time.sleep(CAPTCHA_PAUSE)
            elif attempt < MAX_RETRIES - 1:
                _backoff(attempt)
            continue

        if status == 429:
            print("[apollo_request] 429 — ліміт, чекаю")
            if attempt < MAX_RETRIES - 1:
                _backoff(attempt)
            continue

        print(f"[apollo_request] status {status}, спроба {attempt + 1}")
        if attempt < MAX_RETRIES - 1:
            _backoff(attempt)

    return None, last_status


def cors_response(data, status=200):
    response = jsonify(data)
    response.status_code = status
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Admin-Key"
    response.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
    return response


def get_organization_data(domain):
    data, status = apollo_request(
        "GET",
        "https://app.apollo.io/api/v1/organizations/search",
        params={
            "q_organization_fuzzy_name": domain,
            "display_mode": "fuzzy_select_mode",
            "cacheKey": int(time.time() * 1000),
        },
    )
    if data is None:
        print(f"Org search failed for domain={domain} (status={status})")
        return None, False

    organizations = data.get("organizations", [])
    if not organizations:
        return None, False

    for org in organizations:
        if org.get("domain") == domain or domain in org.get("website_url", ""):
            return org, True

    return organizations[0], False


def get_organization_full(org_id):
    data, status = apollo_request(
        "GET",
        f"https://app.apollo.io/api/v1/organizations/{org_id}",
        params={
            "exclude_info[]": "organization_account_info",
            "cacheKey": int(time.time() * 1000),
        },
    )
    if data is None:
        print(f"get_organization_full failed (status={status})")
        return None
    return data.get("organization", {}) or {}


def get_account_info(org_id):
    org = get_organization_full(org_id)
    if not org:
        return None, None

    num_employees = org.get("estimated_num_employees")
    industry = org.get("industry")

    if isinstance(industry, str):
        industries = [industry] if industry else []
    elif isinstance(industry, list):
        industries = industry
    else:
        industries = []

    print(f"account info: employees={num_employees}, industries={industries}")
    return num_employees, industries


def search_people(organization_id, seniorities, titles, max_pages=1, extra_filters=None):
    all_people = []
    page = 1
    extra_filters = extra_filters or {}

    while True:
        body = {
            "organization_ids": [organization_id],
            "person_seniorities": seniorities,
            "page": page,
            "per_page": 25,
            "display_mode": "explorer_mode",
            "context": "people-index-page",
            "finder_version": 2,
        }
        if titles:
            body["person_titles"] = titles
        body.update(extra_filters)

        data, status = apollo_request(
            "POST",
            "https://app.apollo.io/api/v1/mixed_people/search",
            json=body,
        )
        if data is None:
            print(f"search_people failed (status={status})")
            break

        people_raw = data.get("people", [])
        pagination = data.get("pagination", {})
        total_pages = pagination.get("total_pages", 1)

        for person in people_raw:
            org = person.get("organization", {}) or {}
            all_people.append({
                "name": person.get("name", ""),
                "first_name": person.get("first_name", ""),
                "last_name": person.get("last_name", ""),
                "title": person.get("title", ""),
                "seniority": person.get("seniority", ""),
                "linkedin_url": person.get("linkedin_url", ""),
                "company": org.get("name", ""),
                "company_domain": org.get("website_url", ""),
                "country": person.get("country", ""),
                "city": person.get("city", ""),
                "state": person.get("state", ""),
                "headline": person.get("headline", ""),
            })

        if page >= total_pages or page >= max_pages:
            break
        page += 1

    return all_people


def resolve_domains_to_orgs(domains):
    org_map = {}
    unresolved = []
    for d in domains:
        org_basic, matched = get_organization_data(d)
        if org_basic and org_basic.get("id"):
            org_map[org_basic["id"]] = {"domain": d, "matched": matched}
        else:
            unresolved.append(d)
    return org_map, unresolved


def search_people_bulk(org_ids, seniorities, titles, max_pages, extra_filters=None):
    all_people = []
    page = 1
    extra_filters = extra_filters or {}

    while True:
        body = {
            "organization_ids": org_ids,
            "person_seniorities": seniorities,
            "page": page,
            "per_page": BATCH_PER_PAGE,
            "display_mode": "explorer_mode",
            "context": "people-index-page",
            "finder_version": 2,
        }
        if titles:
            body["person_titles"] = titles
        body.update(extra_filters)

        data, status = apollo_request(
            "POST",
            "https://app.apollo.io/api/v1/mixed_people/search",
            json=body,
        )
        if data is None:
            print(f"search_people_bulk failed (status={status})")
            break

        people_raw = data.get("people", [])
        pagination = data.get("pagination", {})
        total_pages = pagination.get("total_pages", 1)

        for person in people_raw:
            org = person.get("organization", {}) or {}
            all_people.append({
                "organization_id": org.get("id", ""),
                "name": person.get("name", ""),
                "first_name": person.get("first_name", ""),
                "last_name": person.get("last_name", ""),
                "title": person.get("title", ""),
                "seniority": person.get("seniority", ""),
                "linkedin_url": person.get("linkedin_url", ""),
                "company": org.get("name", ""),
                "company_domain": org.get("website_url", ""),
                "country": person.get("country", ""),
                "city": person.get("city", ""),
                "state": person.get("state", ""),
                "headline": person.get("headline", ""),
            })

        if page >= total_pages or page >= max_pages:
            break
        page += 1

    return all_people


@app.route("/search", methods=["POST"])
def search():
    data = request.json
    domain = data.get("domain", "")
    seniorities = data.get("seniorities", ["c_suite", "vp", "director"])
    titles = data.get("titles", [])
    max_pages = data.get("max_pages", 1)
    extra_filters = data.get("extra_filters", {})
    min_employees = data.get("min_employees")
    max_employees = data.get("max_employees")
    excluded_industries = data.get("excluded_industries", [])

    if not domain:
        return jsonify({"error": "domain is required"}), 400

    print(f"Запит /search: {domain}")

    org_basic, _ = get_organization_data(domain)
    if not org_basic:
        return jsonify({"error": f"Company not found: {domain}"}), 404

    org_id = org_basic["id"]
    num_employees, industries = get_account_info(org_id)

    if min_employees is not None or max_employees is not None:
        if num_employees is None:
            return jsonify({"domain": domain, "total": 0, "people": [],
                            "skipped_reason": "employee count unknown"})
        if min_employees is not None and num_employees < min_employees:
            return jsonify({"domain": domain, "total": 0, "people": [],
                            "skipped_reason": f"employees={num_employees} below min={min_employees}"})
        if max_employees is not None and num_employees > max_employees:
            return jsonify({"domain": domain, "total": 0, "people": [],
                            "skipped_reason": f"employees={num_employees} above max={max_employees}"})

    if excluded_industries and industries:
        excluded_lower = [i.lower() for i in excluded_industries]
        for ind in industries:
            if ind.lower() in excluded_lower:
                return jsonify({"domain": domain, "total": 0, "people": [],
                                "skipped_reason": f"industry '{ind}' is excluded"})

    people = search_people(org_id, seniorities, titles, max_pages, extra_filters)
    print(f"Знайдено: {len(people)} для {domain}")

    return jsonify({"domain": domain, "total": len(people), "people": people})


@app.route("/company", methods=["POST"])
def company():
    data = request.json
    domain = data.get("domain", "")
    if not domain:
        return jsonify({"error": "domain is required"}), 400

    print(f"Запит /company: {domain}")
    org_basic, domain_matched = get_organization_data(domain)
    if not org_basic:
        return jsonify({"error": f"Company not found: {domain}"}), 404

    org = get_organization_full(org_basic["id"]) or org_basic

    return jsonify({
        "domain": domain,
        "domain_matched": domain_matched,
        "name": org.get("name"),
        "description": org.get("short_description"),
        "website_url": org.get("website_url"),
        "linkedin_url": org.get("linkedin_url"),
        "twitter_url": org.get("twitter_url"),
        "facebook_url": org.get("facebook_url"),
        "industry": org.get("industry"),
        "industries": org.get("industries", []),
        "secondary_industries": org.get("secondary_industries", []),
        "estimated_num_employees": org.get("estimated_num_employees"),
        "city": org.get("city"),
        "state": org.get("state"),
        "country": org.get("country"),
        "raw_address": org.get("raw_address"),
        "founded_year": org.get("founded_year"),
        "publicly_traded_symbol": org.get("publicly_traded_symbol"),
        "organization_revenue": org.get("organization_revenue"),
        "organization_revenue_printed": org.get("organization_revenue_printed"),
        "total_funding": org.get("total_funding"),
        "total_funding_printed": org.get("total_funding_printed"),
        "latest_funding_stage": org.get("latest_funding_stage"),
        "latest_funding_round_date": org.get("latest_funding_round_date"),
        "technology_names": org.get("technology_names", []),
    })


@app.route("/jobs", methods=["POST"])
def jobs():
    data = request.json
    domain = data.get("domain", "")
    max_pages = data.get("max_pages", 1)
    if not domain:
        return jsonify({"error": "domain is required"}), 400

    print(f"Запит /jobs: {domain}")
    org_basic, _ = get_organization_data(domain)
    if not org_basic:
        return jsonify({"error": f"Company not found: {domain}"}), 404

    org_id = org_basic["id"]
    all_jobs = []
    page = 1

    while True:
        body = {
            "organization_ids": [org_id],
            "newsfeed_event_types": ["job_added"],
            "page": page,
            "per_page": 25,
        }
        data, status = apollo_request(
            "POST",
            "https://app.apollo.io/api/v1/newsfeed_events/search",
            json=body,
        )
        if data is None:
            print(f"jobs failed (status={status})")
            break

        events = data.get("newsfeed_events", [])
        pagination = data.get("pagination", {})
        total_pages = pagination.get("total_pages", 1)

        for event in events:
            job = event.get("job", {}) or {}
            all_jobs.append({
                "title": job.get("title") or event.get("title", ""),
                "url": job.get("url") or event.get("url", ""),
                "city": event.get("city", ""),
                "state": event.get("state", ""),
                "country": event.get("country", ""),
                "posted_at": event.get("posted_at", ""),
            })

        if page >= total_pages or page >= max_pages:
            break
        page += 1

    print(f"Знайдено вакансій: {len(all_jobs)} для {domain}")
    return jsonify({"domain": domain, "total": len(all_jobs), "jobs": all_jobs})


@app.route("/batch-search", methods=["POST"])
def batch_search():
    data = request.json or {}
    domains = data.get("domains", [])
    seniorities = data.get("seniorities", ["c_suite", "vp", "director"])
    titles = data.get("titles", [])
    max_pages = data.get("max_pages", 2)
    extra_filters = data.get("extra_filters", {})
    chunk_size = data.get("batch_size", BATCH_ORG_CHUNK)

    if not domains or not isinstance(domains, list):
        return jsonify({"error": "domains (non-empty list) is required"}), 400

    print(f"Запит /batch-search: {len(domains)} доменів, chunk={chunk_size}")
    org_map, unresolved = resolve_domains_to_orgs(domains)
    org_ids = list(org_map.keys())
    print(f"Резолвлено {len(org_ids)}/{len(domains)}, не знайдено: {len(unresolved)}")

    results = {d: [] for d in domains}
    for i in range(0, len(org_ids), chunk_size):
        chunk = org_ids[i:i + chunk_size]
        people = search_people_bulk(chunk, seniorities, titles, max_pages, extra_filters)
        for p in people:
            oid = p.get("organization_id")
            info = org_map.get(oid)
            if info:
                results[info["domain"]].append(p)
        print(f"  chunk {i // chunk_size + 1}: {len(chunk)} компаній -> {len(people)} людей")

    total_people = sum(len(v) for v in results.values())
    print(f"Разом: {total_people} людей по {len(domains)} доменах")

    return jsonify({
        "total_domains": len(domains),
        "resolved": len(org_ids),
        "unresolved": unresolved,
        "total_people": total_people,
        "results": results,
    })


_jobs = {}
_jobs_lock = threading.Lock()
_job_queue = queue.Queue()


def send_person_to_clay(person, domain, webhook_url, webhook_token):
    if not webhook_url:
        print("[clay] webhook_url не заданий — пропускаю")
        return False

    payload = dict(person)
    payload["domain"] = domain
    payload.pop("organization_id", None)

    headers = {"Content-Type": "application/json"}
    if webhook_token:
        headers["x-clay-webhook-auth"] = webhook_token

    for attempt in range(3):
        try:
            resp = requests.post(webhook_url, json=payload, headers=headers, timeout=30)
            if resp.status_code < 300:
                return True
            print(f"[clay] webhook status {resp.status_code}: {resp.text[:150]}")
        except requests.RequestException as e:
            print(f"[clay] webhook error: {e}")
        time.sleep(2 * (attempt + 1))
    return False


def _set_job(job_id, **fields):
    with _jobs_lock:
        job = _jobs.setdefault(job_id, {})
        job.update(fields)


def process_search_job(job_id, params):
    domain = params["domain"]
    webhook_url = (params.get("webhook_url") or CLAY_WEBHOOK_URL or "").strip()
    webhook_token = (params.get("webhook_token") or CLAY_WEBHOOK_TOKEN or "").strip()

    _set_job(job_id, status="processing")
    try:
        org_basic, _ = get_organization_data(domain)
        if not org_basic:
            _set_job(job_id, status="not_found", people_sent=0,
                     error=f"Company not found: {domain}")
            return

        org_id = org_basic["id"]
        min_e = params.get("min_employees")
        max_e = params.get("max_employees")
        excluded = params.get("excluded_industries", [])
        if min_e is not None or max_e is not None or excluded:
            num_employees, industries = get_account_info(org_id)
            if (min_e is not None and (num_employees or 0) < min_e) or \
               (max_e is not None and (num_employees or 10**9) > max_e):
                _set_job(job_id, status="skipped", people_sent=0,
                         error=f"employees={num_employees} поза діапазоном")
                return
            if excluded and industries:
                ex = [i.lower() for i in excluded]
                if any(ind.lower() in ex for ind in industries):
                    _set_job(job_id, status="skipped", people_sent=0,
                             error="industry excluded")
                    return

        people = search_people(
            org_id,
            params.get("seniorities", ["c_suite", "vp", "director"]),
            params.get("titles", []),
            params.get("max_pages", 1),
            params.get("extra_filters", {}),
        )

        sent = 0
        for p in people:
            if send_person_to_clay(p, domain, webhook_url, webhook_token):
                sent += 1

        _set_job(job_id, status="done", people_found=len(people), people_sent=sent)
        print(f"[job {job_id[:8]}] {domain}: знайдено {len(people)}, відправлено {sent}")

    except Exception as e:
        _set_job(job_id, status="error", error=str(e))
        print(f"[job {job_id[:8]}] помилка: {e}")


def _worker(worker_id):
    print(f"[worker {worker_id}] запущено")
    while True:
        job_id, params = _job_queue.get()
        try:
            # Стоп-кран: якщо капча активна — чекаємо, поки її пройдуть.
            while _captcha_active.is_set():
                print(f"[worker {worker_id}] капча активна — чекаю, черга на паузі")
                notify_captcha()  # нагадати (спрацює не частіше 5хв)
                time.sleep(15)
            process_search_job(job_id, params)
        finally:
            _job_queue.task_done()


@app.route("/search-async", methods=["POST"])
def search_async():
    data = request.json or {}
    domain = data.get("domain", "")
    if not domain:
        return jsonify({"error": "domain is required"}), 400

    job_id = str(uuid.uuid4())
    _set_job(job_id, status="queued", domain=domain, people_sent=0, error=None)
    _job_queue.put((job_id, data))
    return jsonify({"job_id": job_id, "status": "queued", "domain": domain})


@app.route("/status/<job_id>", methods=["GET"])
def job_status(job_id):
    with _jobs_lock:
        job = _jobs.get(job_id)
    if not job:
        return jsonify({"error": "job not found"}), 404
    return jsonify({"job_id": job_id, **job})


@app.route("/whoami", methods=["GET"])
def whoami():
    browser_ip = be.check_ip()
    return jsonify({
        "proxy_configured": PROXY_CONFIGURED,
        "browser_ip": browser_ip,
        "note": "browser_ip має бути резидентським IP проксі, не IP сервера Contabo",
    })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "proxy_configured": PROXY_CONFIGURED,
        "requests_made": _request_count,
        "session_problem": _session_problem,
        "browser": be.probe(),
        "webhook_default_configured": bool(CLAY_WEBHOOK_URL),
        "queue_size": _job_queue.qsize(),
        "workers": NUM_WORKERS,
        "throttle": {
            "delay_range": [NORMAL_DELAY_MIN, NORMAL_DELAY_MAX],
            "pause_every": PAUSE_EVERY,
            "pause_range": [PAUSE_MIN, PAUSE_MAX],
        },
    })


def _startup():
    print("Стартую браузерний двигун...")
    be.start()
    print("Двигун готовий. Запускаю воркери черги...")
    for _i in range(NUM_WORKERS):
        t = threading.Thread(target=_worker, args=(_i + 1,), daemon=True)
        t.start()


if __name__ == "__main__":
    _startup()
    print("Сервер запущено")
    print(f"Proxy: {'налаштовано' if PROXY_CONFIGURED else 'НЕ налаштовано'}")
    print(f"Throttle: {NORMAL_DELAY_MIN}-{NORMAL_DELAY_MAX}s, пауза кожні {PAUSE_EVERY}")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5001)),
            debug=False, threaded=True, use_reloader=False)