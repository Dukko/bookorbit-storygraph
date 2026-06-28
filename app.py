"""
BookOrbit → StoryGraph Sync Service

Polls your BookOrbit instance for in-progress books/audiobooks and pushes
reading progress to StoryGraph via its undeclared web API (session cookies).

Based on: https://github.com/Dukko/abs-storygraph-sync
"""

from flask import Flask, jsonify, request, render_template, Response, session, redirect, url_for
from urllib.parse import urlparse
import os, re, json, logging, threading, time, hmac
from collections import deque
from datetime import datetime
import requests as req
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────

DATA_DIR = "/app/data"
CONFIG_FILE = f"{DATA_DIR}/config.json"
SYNC_STATE_FILE = f"{DATA_DIR}/sync_state.json"
_runtime_config: dict = {}
_config_lock = threading.Lock()


def _load_file_config() -> dict:
    try:
        with open(CONFIG_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_file_config(data: dict):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CONFIG_FILE, "w") as f:
        json.dump(data, f, indent=2)


def cfg(key: str, default: str = "") -> str:
    with _config_lock:
        return _runtime_config.get(key) or os.environ.get(key, default)


# Load file config on startup (env vars as fallback, file config takes precedence)
with _config_lock:
    _runtime_config.update(_load_file_config())

# Tracks the last progress % successfully pushed to StoryGraph, persisted across restarts
_synced_pct: dict[str, float] = {}
_synced_pct_lock = threading.Lock()


def _load_sync_state():
    try:
        with open(SYNC_STATE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_sync_state(data: dict):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(SYNC_STATE_FILE, "w") as f:
        json.dump(data, f, indent=2)


with _synced_pct_lock:
    _synced_pct.update(_load_sync_state())

POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", 600))
SYNC_THRESHOLD = float(os.environ.get("SYNC_THRESHOLD_PERCENT", 0.5))
STORYGRAPH_BASE = "https://app.thestorygraph.com"


def _get_secret_key() -> bytes:
    key_file = f"{DATA_DIR}/secret_key"
    try:
        with open(key_file, "rb") as f:
            key = f.read()
            if len(key) >= 32:
                return key
    except FileNotFoundError:
        pass
    key = os.urandom(32)
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(key_file, "wb") as f:
        f.write(key)
    return key

# ── Logging ───────────────────────────────────────────────────────────────────

class LogBuffer(logging.Handler):
    def __init__(self, maxlen=300):
        super().__init__()
        self._records: deque = deque(maxlen=maxlen)
        self._lock = threading.Lock()

    def emit(self, record):
        with self._lock:
            self._records.append({
                "time": datetime.fromtimestamp(record.created).strftime("%H:%M:%S"),
                "level": record.levelname,
                "msg": record.getMessage(),
            })

    def get(self):
        with self._lock:
            return list(self._records)


_log_buffer = LogBuffer()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger().addHandler(_log_buffer)
logging.getLogger("werkzeug").setLevel(logging.WARNING)

# ── BookOrbit auth token cache ────────────────────────────────────────────────

_bo_token: str | None = None
_bo_token_lock = threading.Lock()


def _bo_login() -> str | None:
    """Log in to BookOrbit and return a JWT access token."""
    url = cfg("BOOKORBIT_URL")
    username = cfg("BOOKORBIT_USERNAME")
    password = cfg("BOOKORBIT_PASSWORD")
    if not all([url, username, password]):
        return None
    try:
        r = req.post(
            f"{url}/api/v1/auth/login",
            json={"username": username, "password": password},
            timeout=10,
        )
        r.raise_for_status()
        token = r.json().get("accessToken")
        if token:
            logger.info("BookOrbit: logged in as %s", username)
        return token
    except Exception as e:
        logger.error("BookOrbit login failed: %s", e)
        return None


def _get_bo_token() -> str | None:
    """Return cached token, refreshing if needed."""
    global _bo_token
    with _bo_token_lock:
        if _bo_token:
            return _bo_token
        _bo_token = _bo_login()
        return _bo_token


def _clear_bo_token():
    global _bo_token
    with _bo_token_lock:
        _bo_token = None

# ── BookOrbit API ─────────────────────────────────────────────────────────────

def _bo_get(path: str, retry: bool = True):
    """Authenticated GET to BookOrbit API."""
    token = _get_bo_token()
    if not token:
        raise RuntimeError("No BookOrbit token available")
    r = req.get(
        f"{cfg('BOOKORBIT_URL')}/api/v1{path}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )
    if r.status_code == 401 and retry:
        _clear_bo_token()
        return _bo_get(path, retry=False)
    r.raise_for_status()
    return r


def _bo_post(path: str, body: dict, retry: bool = True):
    """Authenticated POST to BookOrbit API."""
    token = _get_bo_token()
    if not token:
        raise RuntimeError("No BookOrbit token available")
    r = req.post(
        f"{cfg('BOOKORBIT_URL')}/api/v1{path}",
        json=body,
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )
    if r.status_code == 401 and retry:
        _clear_bo_token()
        return _bo_post(path, body, retry=False)
    r.raise_for_status()
    return r


_AUDIO_FORMATS = {"m4b", "mp3", "aax", "flac", "ogg", "opus", "m4a"}


def get_bo_in_progress() -> list[dict]:
    """
    Fetch all BookOrbit books (paginated) and return those with readStatus.status == "reading".
    Progress and audio flag are included in the listing — no per-book calls needed.
    """
    all_items: list[dict] = []
    page = 0
    while True:
        body = {
            "pagination": {"page": page, "pageSize": 50},
        }
        data = _bo_post("/books/query", body).json()
        items = data.get("items", [])
        all_items.extend(items)
        if not items or len(all_items) >= data.get("total", 0):
            break
        page += 1

    books = []
    for item in all_items:
        read_status = item.get("readStatus") or {}
        if read_status.get("status") != "reading":
            continue

        book_id = item.get("id")
        title = (item.get("title") or "").strip()
        if not title or not book_id:
            continue

        authors = item.get("authors") or []
        author = ", ".join(a for a in authors if a)

        pct = float(item.get("readingProgress") or 0)

        files = item.get("files") or []
        is_audio = any(
            f.get("role") == "primary" and f.get("format") in _AUDIO_FORMATS
            for f in files
        )

        books.append({
            "id": book_id,
            "title": title,
            "author": author,
            "progress_percent": round(pct, 1),
            "is_audio": is_audio,
        })

    return books

# ── StoryGraph client ─────────────────────────────────────────────────────────

class StoryGraphClient:
    def __init__(self):
        self._session = req.Session()
        for name, val in [
            ("_storygraph_session", cfg("STORYGRAPH_SESSION")),
            ("remember_user_token", cfg("STORYGRAPH_REMEMBER_TOKEN")),
            ("cookies_popup_seen", "yes"),
            ("plus_popup_seen", "yes"),
        ]:
            if val:
                self._session.cookies.set(name, val, domain="app.thestorygraph.com")
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept-Language": "en",
            "Origin": STORYGRAPH_BASE,
        })
        self._last_csrf = None

    def _extract_csrf(self, html: str):
        m = (
            re.search(r'<meta[^>]+name=["\']csrf-token["\'][^>]+content=["\']([^"\']+)["\']', html)
            or re.search(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']csrf-token["\']', html)
            or re.search(r'<input[^>]+name=["\']authenticity_token["\'][^>]+value=["\']([^"\']+)["\']', html)
        )
        if m:
            self._last_csrf = m.group(1)
        return self._last_csrf

    def _get(self, path: str):
        resp = self._session.get(f"{STORYGRAPH_BASE}{path}", timeout=15)
        self._extract_csrf(resp.text)
        if "_storygraph_session" in resp.cookies:
            self._session.cookies.set(
                "_storygraph_session",
                resp.cookies["_storygraph_session"],
                domain="app.thestorygraph.com",
            )
        return resp

    def _post(self, path: str, data: dict):
        return self._session.post(
            f"{STORYGRAPH_BASE}{path}",
            data=data,
            headers={
                "X-CSRF-Token": self._last_csrf,
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "text/javascript, application/javascript, */*; q=0.01",
                "Referer": STORYGRAPH_BASE,
            },
            allow_redirects=False,
            timeout=15,
        )

    def check_auth(self) -> bool:
        resp = self._get("/")
        return "sign_in" not in resp.url

    _TITLE_NOISE = re.compile(
        r'\s*[\(\[].*?[\)\]]'           # anything in parens/brackets: (Unabridged), [Audiobook]
        r'|\s*:\s*(A |An |The )?'       # subtitle separator after colon
        r'(?:unabridged|abridged|audiobook|a novel|a memoir)'
        r'.*$',
        re.IGNORECASE,
    )

    @staticmethod
    def _clean_title(title: str) -> str:
        return StoryGraphClient._TITLE_NOISE.sub("", title).strip()

    def search_book(self, title: str, author: str) -> str | None:
        clean = self._clean_title(title)
        query = req.utils.quote(f"{clean} {author}".strip())
        resp = self._get(f"/browse?search_term={query}")
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        link = soup.find("a", class_="book-title-link")
        if not link:
            container = soup.find(class_="book-title-author-and-series")
            if container:
                link = container.find("a", href=re.compile(r"^/books/"))
        if link:
            m = re.search(r"/books/([^/?]+)", link.get("href", ""))
            if m:
                logger.info("Found '%s' on StoryGraph → id=%s", clean, m.group(1))
                return m.group(1)
        logger.warning("No StoryGraph result for '%s' (searched: '%s')", title, clean)
        return None

    def ensure_currently_reading(self, book_id: str):
        resp = self._get(f"/books/{book_id}")
        m = re.search(r'class="read-status-label"[^>]*>([^<]+)<', resp.text)
        status = m.group(1).strip().lower() if m else ""
        if "currently reading" in status or "rereading" in status:
            return True
        r = self._post(
            f"/update-status.js?book_id={book_id}&status=currently-reading",
            {"authenticity_token": self._last_csrf},
        )
        logger.info("Set currently-reading for %s: HTTP %s", book_id, r.status_code)
        return r.status_code in (200, 302)

    def update_progress(self, book_id: str, progress_percent: float) -> bool:
        resp = self._get(f"/books/{book_id}")
        m = re.search(
            r'(?:name="read_status\[book_num_of_pages\]"|class="read-status-book-num-of-pages")[^>]*value="([^"]*)"',
            resp.text,
        )
        book_pages = m.group(1) if m else "0"
        r = self._post("/update-progress", {
            "read_status[progress_number]": str(round(progress_percent, 1)),
            "read_status[progress_type]": "percentage",
            "read_status[book_num_of_pages]": book_pages,
            "book_id": book_id,
            "on_book_page": "true",
            "authenticity_token": self._last_csrf,
        })
        ok = r.status_code in (200, 302)
        logger.info("Progress for %s → %.1f%%: HTTP %s", book_id, progress_percent, r.status_code)
        return ok

# ── Sync logic ────────────────────────────────────────────────────────────────

_status_cache: dict = {"books": [], "bo_ok": False, "ts": 0.0}
_status_cache_lock = threading.Lock()
STATUS_CACHE_TTL = 60  # seconds


def get_cached_books() -> tuple[list[dict], bool]:
    with _status_cache_lock:
        if time.time() - _status_cache["ts"] < STATUS_CACHE_TTL:
            return _status_cache["books"], _status_cache["bo_ok"]
    try:
        books = get_bo_in_progress()
        with _status_cache_lock:
            _status_cache.update({"books": books, "bo_ok": True, "ts": time.time()})
        return books, True
    except Exception as e:
        logger.error("BookOrbit fetch failed: %s", e)
        with _status_cache_lock:
            _status_cache["bo_ok"] = False
        return _status_cache["books"], False


def do_sync(books: list[dict]) -> list[dict]:
    if not cfg("STORYGRAPH_SESSION"):
        return [{"title": b["title"], "status": "no_sg_session"} for b in books]

    client = StoryGraphClient()
    if not client.check_auth():
        logger.error("StoryGraph session invalid — update cookies in Settings")
        return [{"title": b["title"], "status": "auth_error"} for b in books]

    results = []
    for book in books:
        try:
            pct = book["progress_percent"]
            key = book["title"]

            with _synced_pct_lock:
                prev_pct = _synced_pct.get(key)

            if prev_pct is not None and abs(pct - prev_pct) < SYNC_THRESHOLD:
                logger.info("'%s' unchanged at %.1f%% — skipping", key, pct)
                results.append({"title": key, "status": "unchanged", "progress_percent": pct})
                continue

            book_id = client.search_book(book["title"], book["author"])
            if not book_id:
                results.append({"title": key, "status": "not_found"})
                continue

            client.ensure_currently_reading(book_id)
            ok = client.update_progress(book_id, pct)

            if ok:
                with _synced_pct_lock:
                    _synced_pct[key] = pct
                    _save_sync_state(dict(_synced_pct))

            results.append({
                "title": key,
                "status": "success" if ok else "failed",
                "progress_percent": pct,
                "is_audio": book.get("is_audio", False),
            })
        except Exception as e:
            logger.error("Error syncing '%s': %s", book.get("title", "?"), e)
            results.append({"title": book.get("title", "?"), "status": "error", "error": str(e)})

    return results


def _poll_loop():
    logger.info(
        "Auto-sync started: polling every %ds, threshold %.1f%%",
        POLL_INTERVAL, SYNC_THRESHOLD,
    )
    while True:
        time.sleep(POLL_INTERVAL)
        if not all([cfg("BOOKORBIT_URL"), cfg("BOOKORBIT_USERNAME"), cfg("BOOKORBIT_PASSWORD"), cfg("STORYGRAPH_SESSION")]):
            continue
        try:
            books = get_bo_in_progress()
            with _status_cache_lock:
                _status_cache.update({"books": books, "bo_ok": True, "ts": time.time()})

            to_sync = []
            for b in books:
                with _synced_pct_lock:
                    prev = _synced_pct.get(b["title"])
                if prev is None or abs(b["progress_percent"] - prev) >= SYNC_THRESHOLD:
                    to_sync.append(b)

            if to_sync:
                results = do_sync(to_sync)
                synced = sum(1 for r in results if r["status"] == "success")
                logger.info("Auto-sync: %d/%d synced", synced, len(to_sync))
        except Exception as e:
            logger.error("Auto-sync error: %s", e)

# ── Flask app ─────────────────────────────────────────────────────────────────

app = Flask(__name__)
app.secret_key = _get_secret_key()


@app.before_request
def check_ui_auth():
    if not os.environ.get("UI_PASSWORD"):
        return
    if request.endpoint in ("login", "logout", "static"):
        return
    if not session.get("authenticated"):
        return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if not os.environ.get("UI_PASSWORD"):
        return redirect(url_for("index"))
    error = None
    if request.method == "POST":
        username = os.environ.get("UI_USERNAME", "admin")
        password = os.environ.get("UI_PASSWORD")
        ok = (
            hmac.compare_digest(request.form.get("username", ""), username)
            and hmac.compare_digest(request.form.get("password", ""), password)
        )
        if ok:
            session["authenticated"] = True
            return redirect(url_for("index"))
        error = "Invalid username or password."
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
def index():
    return render_template("index.html", auth_enabled=bool(os.environ.get("UI_PASSWORD")))


@app.route("/api/status")
def api_status():
    books, bo_ok = [], False
    if cfg("BOOKORBIT_URL") and cfg("BOOKORBIT_USERNAME"):
        books, bo_ok = get_cached_books()

    with _synced_pct_lock:
        last = dict(_synced_pct)

    return jsonify({
        "bo_ok": bo_ok,
        "sg_ok": bool(cfg("STORYGRAPH_SESSION")),
        "poll_interval": POLL_INTERVAL,
        "sync_threshold": SYNC_THRESHOLD,
        "books": books,
        "last_synced": last,
    })


@app.route("/api/sync", methods=["POST"])
def api_sync():
    missing = [k for k in ("BOOKORBIT_URL", "BOOKORBIT_USERNAME", "BOOKORBIT_PASSWORD", "STORYGRAPH_SESSION") if not cfg(k)]
    if missing:
        return jsonify({"error": f"Missing config: {', '.join(missing)}"}), 400
    try:
        books = get_bo_in_progress()
        # Invalidate cache so next status call is fresh
        with _status_cache_lock:
            _status_cache["ts"] = 0.0
        if not books:
            return jsonify({"message": "No books in progress", "synced": 0, "total": 0, "results": []})
        results = do_sync(books)
        synced = sum(1 for r in results if r["status"] == "success")
        return jsonify({"message": "Sync complete", "synced": synced, "total": len(books), "results": results})
    except Exception as e:
        logger.error("Sync failed: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/logs")
def api_logs():
    return jsonify({"logs": _log_buffer.get()})


ALLOWED_SETTINGS = {
    "BOOKORBIT_URL",
    "BOOKORBIT_USERNAME",
    "BOOKORBIT_PASSWORD",
    "STORYGRAPH_SESSION",
    "STORYGRAPH_REMEMBER_TOKEN",
}


@app.route("/api/settings", methods=["GET"])
def api_settings_get():
    return jsonify({
        "BOOKORBIT_URL": cfg("BOOKORBIT_URL"),
        "BOOKORBIT_USERNAME": cfg("BOOKORBIT_USERNAME"),
        "BOOKORBIT_PASSWORD": "set" if cfg("BOOKORBIT_PASSWORD") else "",
        "STORYGRAPH_SESSION": "set" if cfg("STORYGRAPH_SESSION") else "",
        "STORYGRAPH_REMEMBER_TOKEN": "set" if cfg("STORYGRAPH_REMEMBER_TOKEN") else "",
    })


@app.route("/api/settings", methods=["POST"])
def api_settings_post():
    data = request.json or {}

    if "BOOKORBIT_URL" in data and data["BOOKORBIT_URL"]:
        parsed = urlparse(data["BOOKORBIT_URL"])
        if parsed.scheme not in ("http", "https"):
            return jsonify({"error": "BOOKORBIT_URL must use http or https"}), 400
        if not parsed.hostname:
            return jsonify({"error": "BOOKORBIT_URL must include a hostname"}), 400

    with _config_lock:
        for k, v in data.items():
            if k in ALLOWED_SETTINGS and v:
                _runtime_config[k] = v
        _save_file_config({k: v for k, v in _runtime_config.items() if k in ALLOWED_SETTINGS and v})

    # Clear cached BookOrbit token so next call re-authenticates with new creds
    if any(k in data for k in ("BOOKORBIT_URL", "BOOKORBIT_USERNAME", "BOOKORBIT_PASSWORD")):
        _clear_bo_token()
        with _status_cache_lock:
            _status_cache["ts"] = 0.0

    logger.info("Settings updated via UI")
    return jsonify({"ok": True})


if __name__ == "__main__":
    if all([cfg("BOOKORBIT_URL"), cfg("BOOKORBIT_USERNAME"), cfg("BOOKORBIT_PASSWORD"), cfg("STORYGRAPH_SESSION")]):
        threading.Thread(target=_poll_loop, daemon=True).start()
    else:
        logger.warning("Missing config — auto-sync disabled. Configure via the web UI.")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
