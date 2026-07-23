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

# Базові обмежувачі затримок (щоб log-normal не давав абсурдів)
NORMAL_DELAY_MIN = float(os.environ.get("DELAY_MIN", 5))
NORMAL_DELAY_MAX = float(os.environ.get("DELAY_MAX", 90))

# Log-normal параметри (mu=2.5, sigma=0.4 → медіана ~12с, пік 8-20с, хвіст до ~40с)
DELAY_LOGNORM_MU = float(os.environ.get("DELAY_LOGNORM_MU", 2.5))
DELAY_LOGNORM_SIGMA = float(os.environ.get("DELAY_LOGNORM_SIGMA", 0.4))

# Періодична пауза "перерва на каву"
PAUSE_EVERY_MIN = int(os.environ.get("PAUSE_EVERY_MIN", 25))
PAUSE_EVERY_MAX = int(os.environ.get("PAUSE_EVERY_MAX", 35))
PAUSE_MIN = float(os.environ.get("PAUSE_MIN", 60))
PAUSE_MAX = float(os.environ.get("PAUSE_MAX", 120))

# Робочі години (Київ, UTC+3 літом)
WORK_HOURS_START = int(os.environ.get("WORK_HOURS_START", 7))
WORK_HOURS_END = int(os.environ.get("WORK_HOURS_END", 18))
WORK_HOURS_JITTER_MIN = int(os.environ.get("WORK_HOURS_JITTER_MIN", 15))
WORK_TZ_OFFSET = int(os.environ.get("WORK_TZ_OFFSET", 3))

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
CLAY_WEBHOOK_URL = os.environ.get("CLAY_WEBHOOK_URL", "").strip()
CLAY_WEBHOOK_TOKEN = os.environ.get("CLAY_WEBHOOK_TOKEN", "").strip()
NUM_WORKERS = int(os.environ.get("NUM_WORKERS", 2))
API_KEY = os.environ.get("API_KEY", "").strip()
CAPTCHA_WEBHOOK_URL = os.environ.get("CAPTCHA_WEBHOOK_URL", "").strip()
CAPTCHA_PAUSE = float(os.environ.get("CAPTCHA_PAUSE", 120))
_last_captcha_notify = [0.0]
_captcha_active = threading.Event()  # глобальний стоп-кран: капча активна
ADMIN_KEY = os.environ.get("ADMIN_KEY", "").strip()
_worker_paused = threading.Event()  # ручна пауза воркера (для VNC-роботи)

_throttle_lock = threading.Lock()
_next_allowed = 0.0
_request_count = 0
_next_pause_at = random.randint(PAUSE_EVERY_MIN, PAUSE_EVERY_MAX)  # коли зробити наступну "перерву"
_session_problem = False

def _work_window_today():
    """Повертає (start_minute, end_minute) сьогоднішнього робочого вікна у Києві.
    Jitter детермінований за датою — щоб протягом одного дня межі не стрибали.
    """
    import datetime as _dt
    now_kyiv = _dt.datetime.utcnow() + _dt.timedelta(hours=WORK_TZ_OFFSET)
    # Детерміністичний seed з дати — jitter однаковий весь день
    day_seed = now_kyiv.year * 10000 + now_kyiv.month * 100 + now_kyiv.day
    rng = random.Random(day_seed)
    start_jitter = rng.randint(-WORK_HOURS_JITTER_MIN, WORK_HOURS_JITTER_MIN)
    end_jitter = rng.randint(-WORK_HOURS_JITTER_MIN, WORK_HOURS_JITTER_MIN)
    start_minute = WORK_HOURS_START * 60 + start_jitter
    end_minute = WORK_HOURS_END * 60 + end_jitter
    return start_minute, end_minute


def _is_work_hours():
    """Чи зараз робочий час у Києві (з денним jitter)?"""
    import datetime as _dt
    now_kyiv = _dt.datetime.utcnow() + _dt.timedelta(hours=WORK_TZ_OFFSET)
    now_minute = now_kyiv.hour * 60 + now_kyiv.minute
    start_min, end_min = _work_window_today()
    return start_min <= now_minute < end_min


def _seconds_to_next_work_start():
    """Скільки секунд спати до початку наступного робочого дня."""
    import datetime as _dt
    now_kyiv = _dt.datetime.utcnow() + _dt.timedelta(hours=WORK_TZ_OFFSET)
    start_min, end_min = _work_window_today()
    now_minute = now_kyiv.hour * 60 + now_kyiv.minute

    if now_minute < start_min:
        # ще не почалось сьогодні
        seconds_to_start = (start_min - now_minute) * 60 - now_kyiv.second
    else:
        # вже вечір, чекаємо до завтра — але jitter завтрашнього дня буде інший
        # тому просто беремо "завтра 7:00" плюс приблизна оцінка
        tomorrow_min = start_min + 24 * 60
        seconds_to_start = (tomorrow_min - now_minute) * 60 - now_kyiv.second

    return max(60, seconds_to_start)  # не менше 60с щоб уникнути циклів


def _pick_delay():
    """Log-normal затримка з обмежувачами."""
    delay = random.lognormvariate(DELAY_LOGNORM_MU, DELAY_LOGNORM_SIGMA)
    return max(NORMAL_DELAY_MIN, min(NORMAL_DELAY_MAX, delay))


def _throttle():
    global _next_allowed, _request_count, _next_pause_at

    # 1. Перевірка робочих годин — якщо ні, чекаємо до початку робочого дня
    if not _is_work_hours():
        sleep_seconds = _seconds_to_next_work_start()
        wake_at = time.strftime("%H:%M", time.localtime(time.time() + sleep_seconds))
        print(f"[throttle] поза робочими годинами, сплю до {wake_at} ({int(sleep_seconds)}с)", flush=True)
        time.sleep(sleep_seconds)

    # 2. Log-normal затримка + періодична пауза
    with _throttle_lock:
        now = time.time()
        delay = _pick_delay()
        _request_count += 1

        if _request_count >= _next_pause_at:
            extra = random.uniform(PAUSE_MIN, PAUSE_MAX)
            print(f"[throttle] periodic pause +{extra:.1f}s after {_request_count} requests", flush=True)
            delay += extra
            # наступна пауза буде через PAUSE_EVERY_MIN..PAUSE_EVERY_MAX запитів
            _next_pause_at = _request_count + random.randint(PAUSE_EVERY_MIN, PAUSE_EVERY_MAX)

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

_last_done_notify = [0.0]


def notify_done():
    """Сповістити n8n, що черга опрацьована (не частіше разу на 2 хв)."""
    if not CAPTCHA_WEBHOOK_URL:
        return
    now = time.time()
    if now - _last_done_notify[0] < 120:
        return
    _last_done_notify[0] = now
    try:
        requests.post(CAPTCHA_WEBHOOK_URL, json={"event": "done"}, timeout=10)
        print("[done] сповіщення про завершення надіслано в n8n")
    except Exception as e:
        print(f"[done] не вдалось сповістити: {e}")

def _extract_domain_from_url(url):
    """Витягує чистий домен з website_url (без http://, без www.)."""
    if not url:
        return ""
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url if "://" in url else "http://" + url)
        host = (parsed.netloc or parsed.path or "").lower().strip()
        if host.startswith("www."):
            host = host[4:]
        # відрізати порт якщо є
        host = host.split(":")[0].split("/")[0]
        return host
    except Exception:
        return ""


def _map_person_to_domain(person, input_domains):
    """Мінімальна стратегія мапінгу organization → domain.
    Якщо website_url точно збігається з якимось з input_domains — беремо його.
    Якщо ні — беремо чистий домен з website_url.
    Це treated as tech debt — потім доопрацювати.
    """
    org = person.get("organization") or {}
    website_url = org.get("website_url") or ""
    extracted = _extract_domain_from_url(website_url)

    # нормалізуємо вхідні домени для порівняння
    normalized_input = {d.lower().strip().lstrip("www.") for d in input_domains if d}

    # точний матч
    if extracted and extracted in normalized_input:
        return extracted

    # fallback — беремо що витягли з website_url
    return extracted or "(unknown)"

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

        if status == 422:
            print("[apollo_request] 422 — сторінки закінчились (page limit)")
            return None, 422

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
            if status == 422:
                print(f"search_people: досягнуто ліміт сторінок на сторінці {page}, зупиняюсь")
            else:
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

def search_people_by_domains(
    domains,
    seniorities=None,
    titles=None,
    max_pages=5,
    extra_filters=None,
    num_employees_ranges=None,
):
    """Оптимізований пошук людей через q_organization_domains_list.

    Один запит на групу з N доменів замість триетапного ланцюга.
    Пагінує до max_pages, повертає список людей з полем matched_domain.
    """
    if not domains:
        return []

    all_people = []

    body_base = {
        "q_organization_domains_list": list(domains),
        "page": 1,
        "per_page": BATCH_PER_PAGE,
        "display_mode": "explorer_mode",
        "context": "people-index-page",
        "finder_version": 2,
    }
    if seniorities:
        body_base["person_seniorities"] = list(seniorities)
    if titles:
        body_base["person_titles"] = list(titles)
    if num_employees_ranges:
        body_base["organization_num_employees_ranges"] = list(num_employees_ranges)
    if extra_filters and isinstance(extra_filters, dict):
        body_base.update(extra_filters)

    page = 1
    while page <= max_pages:
        body = dict(body_base)
        body["page"] = page

        data, status = apollo_request(
            "POST",
            "https://app.apollo.io/api/v1/mixed_people/search",
            json=body,
        )
        if data is None:
            print(f"[search_by_domains] сторінка {page}: запит не пройшов (status={status})", flush=True)
            break

        people_on_page = data.get("people", []) or []
        pagination = data.get("pagination", {}) or {}
        total_pages = pagination.get("total_pages", 0) or 0

        for p in people_on_page:
            matched = _map_person_to_domain(p, domains)
            org = p.get("organization") or {}
            first = p.get("first_name") or ""
            last = p.get("last_name") or ""
            full_name = (first + " " + last).strip() or (p.get("name") or "")
            all_people.append({
                "name": full_name,
                "first_name": first,
                "last_name": last,
                "title": p.get("title") or "",
                "seniority": p.get("seniority") or "",
                "linkedin_url": p.get("linkedin_url"),
                "city": p.get("city"),
                "state": p.get("state"),
                "country": p.get("country"),
                "headline": p.get("headline"),
                "company": org.get("name") or "",
                "company_domain": org.get("website_url") or "",
                "matched_domain": matched,
            })

        print(f"[search_by_domains] сторінка {page}/{total_pages}: +{len(people_on_page)} людей", flush=True)

        # якщо це остання сторінка або порожня
        if page >= total_pages or len(people_on_page) == 0:
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
            if status == 422:
                print(f"search_people_bulk: досягнуто ліміт сторінок на сторінці {page}, зупиняюсь ({len(all_people)} вже зібрано)")
            else:
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

    if not domain:
        return jsonify({"error": "domain is required"}), 400

    print(f"Запит /search: {domain}")

    # === ОПТИМІЗОВАНИЙ ШЛЯХ ===
    num_ranges = None
    if min_employees is not None or max_employees is not None:
        lo = int(min_employees) if min_employees is not None else 1
        hi = int(max_employees) if max_employees is not None else 100000
        num_ranges = [f"{lo},{hi}"]

    people = search_people_by_domains(
        domains=[domain],
        seniorities=seniorities,
        titles=titles,
        max_pages=max_pages,
        extra_filters=extra_filters,
        num_employees_ranges=num_ranges,
    )

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
        # === ОПТИМІЗОВАНИЙ ШЛЯХ через q_organization_domains_list ===
        # Один запит замість триетапного ланцюга resolve→account_info→search.
        min_e = params.get("min_employees")
        max_e = params.get("max_employees")
        num_ranges = None
        if min_e is not None or max_e is not None:
            lo = int(min_e) if min_e is not None else 1
            hi = int(max_e) if max_e is not None else 100000
            num_ranges = [f"{lo},{hi}"]

        people = search_people_by_domains(
            domains=[domain],
            seniorities=params.get("seniorities", ["c_suite", "vp", "director"]),
            titles=params.get("titles", []),
            max_pages=params.get("max_pages", 1),
            extra_filters=params.get("extra_filters", {}),
            num_employees_ranges=num_ranges,
        )

        if not people:
            _set_job(job_id, status="done", people_found=0, people_sent=0)
            print(f"[job {job_id[:8]}] {domain}: 0 людей (можливо не в Apollo або поза фільтрами)")
            return

        # Відправка в Clay
        sent = 0
        skipped_unknown = 0
        for p in people:
            person_domain = p.get("matched_domain") or ""
            # Пропускаємо людей без чіткої привʼязки
            if not person_domain or person_domain == "(unknown)":
                skipped_unknown += 1
                continue
            if send_person_to_clay(p, person_domain, webhook_url, webhook_token):
                sent += 1
        if skipped_unknown:
            print(f"[job {job_id[:8]}] {domain}: пропущено {skipped_unknown} людей без чіткого домену")

        _set_job(job_id, status="done", people_found=len(people), people_sent=sent)
        print(f"[job {job_id[:8]}] {domain}: знайдено {len(people)}, відправлено {sent}")

    except Exception as e:
        _set_job(job_id, status="error", error=str(e))
        print(f"[job {job_id[:8]}] помилка: {e}")

# ═══════════════════════════════════════════════════════════════════
# БАЛК-БУФЕР: накопичує домени і обробляє їх пачками
# ═══════════════════════════════════════════════════════════════════

BATCH_SIZE_MIN = int(os.environ.get("BATCH_SIZE_MIN", 30))
BATCH_SIZE_MAX = int(os.environ.get("BATCH_SIZE_MAX", 50))
BATCH_TIMEOUT_MIN = float(os.environ.get("BATCH_TIMEOUT_MIN", 30))
BATCH_TIMEOUT_MAX = float(os.environ.get("BATCH_TIMEOUT_MAX", 90))

def _pick_batch_size():
    return random.randint(BATCH_SIZE_MIN, BATCH_SIZE_MAX)

def _pick_batch_timeout():
    return random.uniform(BATCH_TIMEOUT_MIN, BATCH_TIMEOUT_MAX)

_batch_buffer = []
_batch_lock = threading.Lock()
_batch_last_add = [0.0]
_current_batch_size = [_pick_batch_size()]
_current_batch_timeout = [_pick_batch_timeout()]


def add_to_batch(job_id, params):
    fire = False
    with _batch_lock:
        if not _batch_buffer:
            # старт нового буфера — обираємо новий випадковий розмір і таймаут
            _current_batch_size[0] = _pick_batch_size()
            _current_batch_timeout[0] = _pick_batch_timeout()
        bs = _current_batch_size[0]
        _batch_buffer.append((job_id, params))
        _batch_last_add[0] = time.time()
        n = len(_batch_buffer)
        if n >= bs:
            fire = True
    print(f"[batch] +{params.get('domain')} (у буфері {n}/{bs})")
    if fire:
        _flush_batch(f"досягнуто {bs}")


def _flush_batch(reason):
    with _batch_lock:
        if not _batch_buffer:
            return
        items = _batch_buffer[:]
        _batch_buffer.clear()
    batch_id = str(uuid.uuid4())
    print(f"[batch {batch_id[:8]}] запуск балку ({reason}): {len(items)} доменів")
    _set_job(batch_id, status="queued", kind="batch", size=len(items))
    _job_queue.put((batch_id, {"__batch_items__": items}))


def _batch_watcher():
    while True:
        time.sleep(1)
        with _batch_lock:
            n = len(_batch_buffer)
            quiet = (time.time() - _batch_last_add[0]) if _batch_buffer else 0
            current_timeout = _current_batch_timeout[0]
            timeout_hit = n > 0 and quiet >= current_timeout
        if timeout_hit:
            _flush_batch(f"тиша {int(quiet)}с (порог {int(current_timeout)}с), {n} доменів")


def process_batch_job(batch_id, items):
    first = items[0][1]
    webhook_url = (first.get("webhook_url") or CLAY_WEBHOOK_URL or "").strip()
    webhook_token = (first.get("webhook_token") or CLAY_WEBHOOK_TOKEN or "").strip()
    seniorities = first.get("seniorities", ["c_suite", "vp", "director"])
    titles = first.get("titles", [])
    extra_filters = first.get("extra_filters", {})
    max_pages = first.get("max_pages", 5)
    min_e = first.get("min_employees")
    max_e = first.get("max_employees")

    # Фільтр розміру передаємо прямо в Apollo-запит
    num_ranges = None
    if min_e is not None or max_e is not None:
        lo = int(min_e) if min_e is not None else 1
        hi = int(max_e) if max_e is not None else 100000
        num_ranges = [f"{lo},{hi}"]

    domains = [p["domain"] for _, p in items]
    print(f"[batch {batch_id[:8]}] пошук по {len(domains)} доменах одним запитом...")

    # === ОПТИМІЗОВАНИЙ ШЛЯХ ===
    # Один запит замість резолву + account_info + окремого пошуку.
    people = search_people_by_domains(
        domains=domains,
        seniorities=seniorities,
        titles=titles,
        max_pages=max_pages,
        extra_filters=extra_filters,
        num_employees_ranges=num_ranges,
    )

    if not people:
        print(f"[batch {batch_id[:8]}] 0 людей за фільтрами")
        _set_job(batch_id, status="done", people_found=0, people_sent=0)
        return

    # Відправка в Clay з правильним matched_domain
    sent = 0
    skipped_unknown = 0
    per_domain_count = {}
    for p in people:
        person_domain = p.get("matched_domain") or ""
        # Пропускаємо людей без чіткої привʼязки до домену (organization.website_url був порожній)
        if not person_domain or person_domain == "(unknown)":
            skipped_unknown += 1
            continue
        per_domain_count[person_domain] = per_domain_count.get(person_domain, 0) + 1
        if send_person_to_clay(p, person_domain, webhook_url, webhook_token):
            sent += 1
    if skipped_unknown:
        print(f"[batch {batch_id[:8]}] пропущено {skipped_unknown} людей без чіткого домену")

    _set_job(batch_id, status="done", people_found=len(people), people_sent=sent)
    print(f"[batch {batch_id[:8]}] знайдено {len(people)}, відправлено {sent}")
    for d, c in sorted(per_domain_count.items(), key=lambda x: -x[1]):
        print(f"[batch {batch_id[:8]}]   {d}: {c} людей")

def _worker(worker_id):
    print(f"[worker {worker_id}] запущено")
    while True:
        job_id, params = _job_queue.get()
        try:
            while _worker_paused.is_set():
                print(f"[worker {worker_id}] ВОРКЕР НА ПАУЗІ — чекаю /resume")
                time.sleep(5)
            while _captcha_active.is_set():
                print(f"[worker {worker_id}] капча активна — чекаю, черга на паузі")
                notify_captcha()
                time.sleep(15)
            if isinstance(params, dict) and "__batch_items__" in params:
                process_batch_job(job_id, params["__batch_items__"])
            else:
                process_search_job(job_id, params)
        finally:
            _job_queue.task_done()
            # Якщо черга спорожніла — сповістити, що прогін завершено
            if _job_queue.empty():
                notify_done()


@app.route("/search-async", methods=["POST"])
def search_async():
    data = request.json or {}
    domain = data.get("domain", "")
    if not domain:
        return jsonify({"error": "domain is required"}), 400

    job_id = str(uuid.uuid4())
    _set_job(job_id, status="queued", domain=domain, people_sent=0, error=None)

    if data.get("batch"):
        add_to_batch(job_id, data)
        return jsonify({"job_id": job_id, "status": "buffered", "domain": domain})

    _job_queue.put((job_id, data))
    return jsonify({"job_id": job_id, "status": "queued", "domain": domain})


@app.route("/status/<job_id>", methods=["GET"])
def job_status(job_id):
    with _jobs_lock:
        job = _jobs.get(job_id)
    if not job:
        return jsonify({"error": "job not found"}), 404
    return jsonify({"job_id": job_id, **job})

@app.route("/debug-bulk", methods=["POST"])
def debug_bulk():
    """ДІАГНОСТИКА: балковий запит з УСІМА фільтрами всередині, повертає сиру статистику Apollo."""
    data = request.json or {}
    domains = data.get("domains", [])
    if not domains:
        return jsonify({"error": "domains list required"}), 400

    # резолв доменів у ID
    org_map, unresolved = resolve_domains_to_orgs(domains)
    org_ids = list(org_map.keys())
    if not org_ids:
        return jsonify({"error": "no orgs resolved", "unresolved": unresolved}), 404

    # будуємо балковий запит з УСІМА фільтрами (людські + компанії разом)
    body = {
        "organization_ids": org_ids,
        "person_seniorities": data.get("seniorities", []),
        "page": 1,
        "per_page": BATCH_PER_PAGE,
        "display_mode": "explorer_mode",
        "context": "people-index-page",
        "finder_version": 2,
    }
    # фільтри компанії — те, що раніше конфліктувало; пробуємо додати
    if data.get("num_employees_ranges"):
        body["organization_num_employees_ranges"] = data["num_employees_ranges"]
    body.update(data.get("extra_filters", {}))

    result, status = apollo_request(
        "POST", "https://app.apollo.io/api/v1/mixed_people/search", json=body
    )
    if result is None:
        return jsonify({"apollo_status": status, "note": "запит не пройшов (можливо капча/422)"}), 200

    pagination = result.get("pagination", {})
    people = result.get("people", [])
    # зберемо унікальні компанії, що реально повернулись (щоб бачити чи фільтр компанії відсік)
    companies = {}
    for p in people:
        org = p.get("organization", {}) or {}
        companies[org.get("id", "")] = org.get("name", "")

    return jsonify({
        "resolved_orgs": len(org_ids),
        "unresolved": unresolved,
        "apollo_total_entries": pagination.get("total_entries"),
        "apollo_total_pages": pagination.get("total_pages"),
        "people_on_page1": len(people),
        "companies_returned": list(companies.values()),
        "sample_people": [{"name": p.get("name"), "title": p.get("title"),
                           "company": (p.get("organization") or {}).get("name"),
                           "country": p.get("country")} for p in people[:10]],
    })

@app.route("/debug-raw", methods=["POST"])
def debug_raw():
    """ДІАГНОСТИКА: сирі поля organization у даних людини (балк)."""
    data = request.json or {}
    domains = data.get("domains", [])
    org_map, unresolved = resolve_domains_to_orgs(domains)
    org_ids = list(org_map.keys())
    if not org_ids:
        return jsonify({"error": "no orgs"}), 404

    body = {
        "organization_ids": org_ids,
        "person_seniorities": data.get("seniorities", []),
        "page": 1,
        "per_page": 5,
        "display_mode": "explorer_mode",
        "context": "people-index-page",
        "finder_version": 2,
    }
    result, status = apollo_request(
        "POST", "https://app.apollo.io/api/v1/mixed_people/search", json=body
    )
    if result is None:
        return jsonify({"apollo_status": status}), 200

    people = result.get("people", [])
    if not people:
        return jsonify({"note": "нема людей", "keys_top": list(result.keys())})

    p = people[0]
    org = p.get("organization", {}) or {}
    return jsonify({
        "person_keys": list(p.keys()),
        "organization_keys": list(org.keys()),
        "organization_sample": {
            "name": org.get("name"),
            "estimated_num_employees": org.get("estimated_num_employees"),
            "industry": org.get("industry"),
            "industries": org.get("industries"),
            "num_employees": org.get("num_employees"),
        },
    })

@app.route("/debug-domain-search", methods=["POST"])
def debug_domain_search():
    """ДІАГНОСТИКА: пошук людей БЕЗ резолву доменів у ID.
    Використовує q_organization_domains замість organization_ids."""
    data = request.json or {}
    domains = data.get("domains", [])
    if not domains:
        return jsonify({"error": "domains list required"}), 400

    body = {
        "q_organization_domains_list": domains,
        "person_seniorities": data.get("seniorities", []),
        "page": 1,
        "per_page": 10,
        "display_mode": "explorer_mode",
        "context": "people-index-page",
        "finder_version": 2,
    }
    if data.get("num_employees_ranges"):
        body["organization_num_employees_ranges"] = data["num_employees_ranges"]
    body.update(data.get("extra_filters", {}))

    result, status = apollo_request(
        "POST", "https://app.apollo.io/api/v1/mixed_people/search", json=body
    )
    if result is None:
        return jsonify({"apollo_status": status, "note": "запит не пройшов"}), 200

    pagination = result.get("pagination", {})
    people = result.get("people", [])
    companies = {}
    for p in people:
        org = p.get("organization", {}) or {}
        companies[org.get("id", "")] = org.get("name", "(null)")

    return jsonify({
        "apollo_total_entries": pagination.get("total_entries"),
        "apollo_total_pages": pagination.get("total_pages"),
        "people_on_page1": len(people),
        "companies_returned": list(companies.values()),
        "sample_people": [{
            "name": p.get("name"),
            "title": p.get("title"),
            "company": (p.get("organization") or {}).get("name") or "(null)",
        } for p in people[:5]],
    })

@app.route("/debug-companies-search", methods=["POST"])
def debug_companies_search():
    """ДІАГНОСТИКА: пошук компаній через mixed_companies/search + q_organization_domains_list."""
    data = request.json or {}
    domains = data.get("domains", [])
    if not domains:
        return jsonify({"error": "domains list required"}), 400

    body = {
        "q_organization_domains_list": domains,
        "page": 1,
        "per_page": 25,
        "display_mode": "explorer_mode",
    }

    result, status = apollo_request(
        "POST", "https://app.apollo.io/api/v1/mixed_companies/search", json=body
    )
    if result is None:
        return jsonify({"apollo_status": status, "note": "запит не пройшов"}), 200

    pagination = result.get("pagination", {})
    orgs = result.get("organizations", []) or result.get("accounts", []) or []

    sample_data = []
    for org in orgs[:5]:
        sample_data.append({
            "name": org.get("name"),
            "domain": org.get("primary_domain") or org.get("website_url"),
            "industry": org.get("industry"),
            "employees": org.get("estimated_num_employees"),
            "revenue": org.get("organization_revenue_printed") or org.get("organization_revenue"),
            "funding": org.get("total_funding_printed") or org.get("total_funding"),
            "technologies_count": len(org.get("technology_names", []) or []),
            "has_linkedin": bool(org.get("linkedin_url")),
            "founded_year": org.get("founded_year"),
        })

    return jsonify({
        "apollo_total_entries": pagination.get("total_entries"),
        "apollo_total_pages": pagination.get("total_pages"),
        "orgs_returned": len(orgs),
        "sample_orgs": sample_data,
        "keys_in_first_org": list(orgs[0].keys()) if orgs else [],
    })

@app.route("/debug-bulk-enrich", methods=["POST"])
def debug_bulk_enrich():
    """ДІАГНОСТИКА: bulk enrichment через internal API.
    Тестуємо чи internal /organizations/bulk_enrich працює через browser session
    і чи повертає повний набір полів без витрачання credits."""
    data = request.json or {}
    domains = data.get("domains", [])
    if not domains:
        return jsonify({"error": "domains list required"}), 400
    if len(domains) > 50:
        return jsonify({"error": "max 50 domains per bulk_enrich"}), 400

    # Спроба 1: query params з domains[]
    from urllib.parse import urlencode
    params = [("domains[]", d) for d in domains]
    url = "https://app.apollo.io/api/v1/organizations/bulk_enrich?" + urlencode(params)

    result, status = apollo_request("POST", url, json={})

    if result is None:
        return jsonify({
            "apollo_status": status,
            "note": "запит не пройшов через internal API",
            "url_tried": url,
        }), 200

    orgs = result.get("organizations", []) or []

    sample_data = []
    for org in orgs[:5]:
        sample_data.append({
            "name": org.get("name"),
            "domain": org.get("primary_domain") or org.get("website_url"),
            "industry": org.get("industry"),
            "industries": org.get("industries"),
            "employees": org.get("estimated_num_employees"),
            "revenue": org.get("organization_revenue_printed") or org.get("organization_revenue"),
            "funding": org.get("total_funding_printed") or org.get("total_funding"),
            "funding_stage": org.get("latest_funding_stage"),
            "technologies_count": len(org.get("technology_names", []) or []),
            "founded_year": org.get("founded_year"),
            "has_linkedin": bool(org.get("linkedin_url")),
            "city": org.get("city"),
            "state": org.get("state"),
            "country": org.get("country"),
        })

    return jsonify({
        "orgs_returned": len(orgs),
        "response_top_keys": list(result.keys()),
        "sample_orgs": sample_data,
        "keys_in_first_org": list(orgs[0].keys()) if orgs else [],
        "has_credit_message": "credit" in str(result).lower() or "upgrade" in str(result).lower(),
    })

@app.route("/whoami", methods=["GET"])
def whoami():
    browser_ip = be.check_ip()
    return jsonify({
        "proxy_configured": PROXY_CONFIGURED,
        "browser_ip": browser_ip,
        "note": "browser_ip має бути резидентським IP проксі, не IP сервера Contabo",
    })

@app.route("/pause", methods=["POST"])
def pause_worker():
    provided = request.headers.get("X-Admin-Key", "")
    if not ADMIN_KEY or provided != ADMIN_KEY:
        return jsonify({"error": "unauthorized"}), 401
    _worker_paused.set()
    print("[admin] воркер поставлено на ПАУЗУ через /pause")
    return jsonify({"status": "paused", "queue_size": _job_queue.qsize()})


@app.route("/resume", methods=["POST"])
def resume_worker():
    provided = request.headers.get("X-Admin-Key", "")
    if not ADMIN_KEY or provided != ADMIN_KEY:
        return jsonify({"error": "unauthorized"}), 401
    _worker_paused.clear()
    print("[admin] воркер знято з ПАУЗИ через /resume")
    return jsonify({"status": "resumed", "queue_size": _job_queue.qsize()})

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "proxy_configured": PROXY_CONFIGURED,
        "requests_made": _request_count,
        "session_problem": _session_problem,
        "paused": _worker_paused.is_set(),
        "browser": be.probe(),
        "webhook_default_configured": bool(CLAY_WEBHOOK_URL),
        "queue_size": _job_queue.qsize(),
        "workers": NUM_WORKERS,
        "throttle": {
            "delay_range": [NORMAL_DELAY_MIN, NORMAL_DELAY_MAX],
            "delay_lognorm": {"mu": DELAY_LOGNORM_MU, "sigma": DELAY_LOGNORM_SIGMA},
            "pause_every_range": [PAUSE_EVERY_MIN, PAUSE_EVERY_MAX],
            "pause_range": [PAUSE_MIN, PAUSE_MAX],
            "next_pause_at": _next_pause_at,
            "requests_made": _request_count,
            "work_hours": [WORK_HOURS_START, WORK_HOURS_END],
            "is_work_hours": _is_work_hours(),
        },
    })


def _startup():
    print("Стартую браузерний двигун...")
    be.start()
    print("Двигун готовий. Запускаю воркери черги...")
    for _i in range(NUM_WORKERS):
        t = threading.Thread(target=_worker, args=(_i + 1,), daemon=True)
        t.start()
    t_watch = threading.Thread(target=_batch_watcher, daemon=True)
    t_watch.start()
    print("Балк-watcher запущено")


if __name__ == "__main__":
    _startup()
    print("Сервер запущено")
    print(f"Proxy: {'налаштовано' if PROXY_CONFIGURED else 'НЕ налаштовано'}")
    print(f"Throttle: log-normal (μ={DELAY_LOGNORM_MU}, σ={DELAY_LOGNORM_SIGMA}), pause every {PAUSE_EVERY_MIN}-{PAUSE_EVERY_MAX} req")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5001)),
            debug=False, threaded=True, use_reloader=False)