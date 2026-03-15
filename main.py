# main.py (Flask)
from flask import Flask, request, redirect, abort, make_response
import os
import time
import requests
from threading import Lock, Event

app = Flask(__name__)
API_KEY = os.environ["PLACES_API_KEY"]

SESSION = requests.Session()

# -----------------------------
# 設定
# -----------------------------
CACHE_TTL_SEC = int(os.environ.get("PHOTO_URI_CACHE_TTL_SEC", "7200"))  # 60分
MAX_CACHE_SIZE = int(os.environ.get("PHOTO_URI_CACHE_MAX", "5000"))

NEGATIVE_CACHE_TTL_SEC = int(
    os.environ.get("PHOTO_NEGATIVE_CACHE_TTL_SEC", "60")
)  # 失敗は短期だけキャッシュ
MAX_NEGATIVE_CACHE_SIZE = int(
    os.environ.get("PHOTO_NEGATIVE_CACHE_MAX", "1000")
)

INFLIGHT_WAIT_SEC = int(
    os.environ.get("PHOTO_INFLIGHT_WAIT_SEC", "15")
)  # 同時MISS待機時間

# -----------------------------
# キャッシュ類
# -----------------------------
# 正常系: key -> (photo_uri, expires_at)
_CACHE = {}

# 異常系: key -> (status_code, message, expires_at)
_NEG_CACHE = {}

# single-flight: key -> Event
_INFLIGHT = {}

_LOCK = Lock()


# -----------------------------
# 例外
# -----------------------------
class UpstreamPhotoError(Exception):
    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.message = message


# -----------------------------
# 共通ヘルパー
# -----------------------------
def normalize_maxw(raw: str) -> str:
    """
    maxw を固定バケットに丸める。
    例:
      1..240   -> 240
      241..400 -> 400
      401以上  -> 800
    """
    try:
        w = int(raw)
    except Exception:
        w = 800

    if w <= 0:
        w = 800

    if w <= 240:
        return "240"
    if w <= 400:
        return "400"
    return "800"


def build_cache_key(ref: str | None, name: str | None, maxw: str) -> str:
    if ref:
        return f"ref:{ref}|maxw:{maxw}"
    return f"name:{name}|maxw:{maxw}"


def build_redirect_response(photo_uri: str, cache_status: str):
    resp = make_response(redirect(photo_uri, code=302))
    resp.headers["Cache-Control"] = f"public, max-age={CACHE_TTL_SEC}"
    resp.headers["X-Photo-Cache"] = cache_status
    return resp


# -----------------------------
# 正常系キャッシュ
# -----------------------------
def cache_get(key: str):
    now = time.time()
    with _LOCK:
        v = _CACHE.get(key)
        if not v:
            return None

        uri, exp = v
        if exp <= now:
            _CACHE.pop(key, None)
            return None

        return uri


def cache_set(key: str, uri: str):
    now = time.time()
    with _LOCK:
        if len(_CACHE) >= MAX_CACHE_SIZE:
            _CACHE.clear()
        _CACHE[key] = (uri, now + CACHE_TTL_SEC)


# -----------------------------
# 失敗系キャッシュ（短期）
# -----------------------------
def neg_cache_get(key: str):
    now = time.time()
    with _LOCK:
        v = _NEG_CACHE.get(key)
        if not v:
            return None

        status_code, message, exp = v
        if exp <= now:
            _NEG_CACHE.pop(key, None)
            return None

        return status_code, message


def neg_cache_set(key: str, status_code: int, message: str):
    now = time.time()
    with _LOCK:
        if len(_NEG_CACHE) >= MAX_NEGATIVE_CACHE_SIZE:
            _NEG_CACHE.clear()
        _NEG_CACHE[key] = (status_code, message, now + NEGATIVE_CACHE_TTL_SEC)


def neg_cache_clear(key: str):
    with _LOCK:
        _NEG_CACHE.pop(key, None)


# -----------------------------
# single-flight
# -----------------------------
def inflight_get_or_create(key: str):
    """
    戻り値:
      (event, is_owner)
    """
    with _LOCK:
        ev = _INFLIGHT.get(key)
        if ev is not None:
            return ev, False

        ev = Event()
        _INFLIGHT[key] = ev
        return ev, True


def inflight_release(key: str):
    with _LOCK:
        ev = _INFLIGHT.pop(key, None)
        if ev is not None:
            ev.set()


# -----------------------------
# Places 呼び出し
# -----------------------------
def fetch_photo_uri(ref: str | None, name: str | None, maxw: str) -> str:
    if ref:
        url = "https://maps.googleapis.com/maps/api/place/photo"
        params = {
            "key": API_KEY,
            "photoreference": ref,
            "maxwidth": maxw,
        }
        r = SESSION.get(url, params=params, timeout=10, allow_redirects=False)

        if r.status_code in (301, 302) and "Location" in r.headers:
            return r.headers["Location"]

        raise UpstreamPhotoError(
            r.status_code,
            f"legacy photo unexpected response: {r.status_code}",
        )

    if name:
        url = f"https://places.googleapis.com/v1/{name}/media"
        params = {
            "key": API_KEY,
            "maxWidthPx": maxw,
            "skipHttpRedirect": "true",
        }
        r = SESSION.get(url, params=params, timeout=10)

        if r.status_code != 200:
            raise UpstreamPhotoError(r.status_code, r.text[:200])

        data = r.json()
        photo_uri = data.get("photoUri")
        if not photo_uri:
            raise UpstreamPhotoError(502, "no photoUri")

        return photo_uri

    raise UpstreamPhotoError(400, "missing ref or name")


# -----------------------------
# ルート
# -----------------------------
@app.get("/place-photo")
def place_photo():
    maxw = normalize_maxw(request.args.get("maxw", "800"))
    ref = request.args.get("ref")
    name = request.args.get("name")

    if not ref and not name:
        abort(400, "missing ref or name")

    key = build_cache_key(ref, name, maxw)

    # 1) 正常系キャッシュ
    cached = cache_get(key)
    if cached:
        return build_redirect_response(cached, "HIT")

    # 2) 失敗系キャッシュ
    neg = neg_cache_get(key)
    if neg:
        status_code, message = neg
        resp = make_response(message, status_code)
        resp.headers["X-Photo-Cache"] = "NEG_HIT"
        return resp

    # 3) single-flight
    inflight_ev, is_owner = inflight_get_or_create(key)

    if not is_owner:
        finished = inflight_ev.wait(timeout=INFLIGHT_WAIT_SEC)

        # owner 完了後、結果を再参照
        cached = cache_get(key)
        if cached:
            return build_redirect_response(cached, "WAIT_HIT")

        neg = neg_cache_get(key)
        if neg:
            status_code, message = neg
            resp = make_response(message, status_code)
            resp.headers["X-Photo-Cache"] = "WAIT_NEG_HIT"
            return resp

        if not finished:
            resp = make_response("photo fetch in progress timeout", 504)
            resp.headers["X-Photo-Cache"] = "WAIT_TIMEOUT"
            return resp

        resp = make_response("photo fetch completed but no cache populated", 502)
        resp.headers["X-Photo-Cache"] = "WAIT_EMPTY"
        return resp

    # 4) owner だけが Places を叩く
    try:
        # owner 取得後に再チェック（ごく短い競合対策）
        cached = cache_get(key)
        if cached:
            return build_redirect_response(cached, "RACE_HIT")

        neg = neg_cache_get(key)
        if neg:
            status_code, message = neg
            resp = make_response(message, status_code)
            resp.headers["X-Photo-Cache"] = "RACE_NEG_HIT"
            return resp

        photo_uri = fetch_photo_uri(ref=ref, name=name, maxw=maxw)

        cache_set(key, photo_uri)
        neg_cache_clear(key)

        return build_redirect_response(photo_uri, "MISS")

    except UpstreamPhotoError as e:
        neg_cache_set(key, e.status_code, e.message)
        abort(e.status_code, e.message)

    finally:
        inflight_release(key)