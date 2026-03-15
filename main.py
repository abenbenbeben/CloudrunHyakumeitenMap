# main.py (Flask + local memory cache + Firestore shared cache)
from flask import Flask, request, redirect, abort, make_response
import os
import time
import hashlib
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from threading import Lock, Event

import requests
from google.cloud import firestore

app = Flask(__name__)
API_KEY = os.environ["PLACES_API_KEY"]

SESSION = requests.Session()
FIRESTORE_PROJECT_ID = os.environ.get("FIRESTORE_PROJECT_ID")
FIRESTORE_DATABASE_ID = os.environ.get("FIRESTORE_DATABASE_ID", "(default)")

DB = firestore.Client(
    project=FIRESTORE_PROJECT_ID,
    database=FIRESTORE_DATABASE_ID,
)

# -----------------------------
# 設定
# -----------------------------
CACHE_TTL_SEC = int(os.environ.get("PHOTO_URI_CACHE_TTL_SEC", "7200"))  # 120分
MAX_CACHE_SIZE = int(os.environ.get("PHOTO_URI_CACHE_MAX", "5000"))

NEGATIVE_CACHE_TTL_SEC = int(
    os.environ.get("PHOTO_NEGATIVE_CACHE_TTL_SEC", "60")
)  # 失敗は短期だけキャッシュ
MAX_NEGATIVE_CACHE_SIZE = int(
    os.environ.get("PHOTO_NEGATIVE_CACHE_MAX", "1000")
)

INFLIGHT_WAIT_SEC = int(
    os.environ.get("PHOTO_INFLIGHT_WAIT_SEC", "15")
)  # 同一インスタンス内の同時MISS待機時間

PHOTO_CACHE_COLLECTION = os.environ.get(
    "PHOTO_CACHE_COLLECTION",
    "placePhotoCache",
)

# -----------------------------
# ローカルキャッシュ類
# -----------------------------
# 正常系: key -> (photo_uri, expires_at_epoch)
_CACHE = OrderedDict()

# 異常系: key -> (status_code, message, expires_at_epoch)
_NEG_CACHE = OrderedDict()

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
def utc_now():
    return datetime.now(timezone.utc)


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


def build_doc_id(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def build_redirect_response(photo_uri: str, cache_status: str):
    resp = make_response(redirect(photo_uri, code=302))
    resp.headers["Cache-Control"] = f"public, max-age={CACHE_TTL_SEC}"
    resp.headers["X-Photo-Cache"] = cache_status
    return resp


def build_error_response(status_code: int, message: str, cache_status: str):
    resp = make_response(message, status_code)
    resp.headers["X-Photo-Cache"] = cache_status
    return resp


# -----------------------------
# ローカル正常系キャッシュ
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

        # LRUっぽく最後尾へ
        _CACHE.move_to_end(key)
        return uri


def cache_set(key: str, uri: str):
    now = time.time()
    with _LOCK:
        _CACHE[key] = (uri, now + CACHE_TTL_SEC)
        _CACHE.move_to_end(key)

        while len(_CACHE) > MAX_CACHE_SIZE:
            _CACHE.popitem(last=False)


# -----------------------------
# ローカル失敗系キャッシュ（短期）
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

        _NEG_CACHE.move_to_end(key)
        return status_code, message


def neg_cache_set(key: str, status_code: int, message: str):
    now = time.time()
    with _LOCK:
        _NEG_CACHE[key] = (status_code, message, now + NEGATIVE_CACHE_TTL_SEC)
        _NEG_CACHE.move_to_end(key)

        while len(_NEG_CACHE) > MAX_NEGATIVE_CACHE_SIZE:
            _NEG_CACHE.popitem(last=False)


def neg_cache_clear(key: str):
    with _LOCK:
        _NEG_CACHE.pop(key, None)


# -----------------------------
# Firestore 共有キャッシュ
# -----------------------------
def fs_doc_ref(key: str):
    doc_id = build_doc_id(key)
    return DB.collection(PHOTO_CACHE_COLLECTION).document(doc_id)


def _normalize_dt(dt):
    if dt is None:
        return None
    if getattr(dt, "tzinfo", None) is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def fs_cache_get(key: str):
    """
    戻り値:
      ("ok", photo_uri) or ("neg", (status_code, message)) or None
    """
    try:
        snap = fs_doc_ref(key).get()
        if not snap.exists:
            return None

        data = snap.to_dict() or {}
        expires_at = _normalize_dt(data.get("expiresAt"))

        # expiresAt がない / 期限切れなら使わない
        if not expires_at or expires_at <= utc_now():
            return None

        kind = data.get("kind")
        if kind == "ok":
            photo_uri = data.get("photoUri")
            if photo_uri:
                return ("ok", photo_uri)
            return None

        if kind == "neg":
            status_code = int(data.get("statusCode", 502))
            message = str(data.get("message", "upstream error"))
            return ("neg", (status_code, message))

        return None

    except Exception:
        app.logger.exception("firestore cache get failed")
        return None


def fs_cache_set_ok(key: str, ref: str | None, name: str | None, maxw: str, photo_uri: str):
    try:
        fs_doc_ref(key).set(
            {
                "cacheKey": key,
                "kind": "ok",
                "ref": ref,
                "name": name,
                "maxw": maxw,
                "photoUri": photo_uri,
                "statusCode": None,
                "message": None,
                "expiresAt": utc_now() + timedelta(seconds=CACHE_TTL_SEC),
                "updatedAt": firestore.SERVER_TIMESTAMP,
            }
        )
    except Exception:
        app.logger.exception("firestore cache set ok failed")


def fs_cache_set_neg(key: str, ref: str | None, name: str | None, maxw: str, status_code: int, message: str):
    try:
        fs_doc_ref(key).set(
            {
                "cacheKey": key,
                "kind": "neg",
                "ref": ref,
                "name": name,
                "maxw": maxw,
                "photoUri": None,
                "statusCode": int(status_code),
                "message": str(message)[:500],
                "expiresAt": utc_now() + timedelta(seconds=NEGATIVE_CACHE_TTL_SEC),
                "updatedAt": firestore.SERVER_TIMESTAMP,
            }
        )
    except Exception:
        app.logger.exception("firestore cache set neg failed")


# -----------------------------
# single-flight（同一インスタンス内だけ）
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
# Firestore -> ローカル反映
# -----------------------------
def hydrate_from_firestore(key: str):
    """
    Firestoreの共有キャッシュから取り出して、ローカルキャッシュにも載せる
    戻り値:
      ("ok", photo_uri) / ("neg", (status_code, message)) / None
    """
    hit = fs_cache_get(key)
    if not hit:
        return None

    kind, payload = hit
    if kind == "ok":
        cache_set(key, payload)
        neg_cache_clear(key)
        return ("ok", payload)

    status_code, message = payload
    neg_cache_set(key, status_code, message)
    return ("neg", (status_code, message))


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

    # 1) ローカル正常系キャッシュ
    cached = cache_get(key)
    if cached:
        return build_redirect_response(cached, "HIT")

    # 2) ローカル失敗系キャッシュ
    neg = neg_cache_get(key)
    if neg:
        status_code, message = neg
        return build_error_response(status_code, message, "NEG_HIT")

    # 3) Firestore共有キャッシュ
    shared = hydrate_from_firestore(key)
    if shared:
        kind, payload = shared
        if kind == "ok":
            return build_redirect_response(payload, "FIRESTORE_HIT")

        status_code, message = payload
        return build_error_response(status_code, message, "FIRESTORE_NEG_HIT")

    # 4) 同一インスタンス内 single-flight
    inflight_ev, is_owner = inflight_get_or_create(key)

    if not is_owner:
        finished = inflight_ev.wait(timeout=INFLIGHT_WAIT_SEC)

        # owner 完了後、再参照
        cached = cache_get(key)
        if cached:
            return build_redirect_response(cached, "WAIT_HIT")

        neg = neg_cache_get(key)
        if neg:
            status_code, message = neg
            return build_error_response(status_code, message, "WAIT_NEG_HIT")

        shared = hydrate_from_firestore(key)
        if shared:
            kind, payload = shared
            if kind == "ok":
                return build_redirect_response(payload, "WAIT_FIRESTORE_HIT")

            status_code, message = payload
            return build_error_response(status_code, message, "WAIT_FIRESTORE_NEG_HIT")

        if not finished:
            return build_error_response(504, "photo fetch in progress timeout", "WAIT_TIMEOUT")

        return build_error_response(502, "photo fetch completed but no cache populated", "WAIT_EMPTY")

    # 5) owner だけが Places を叩く
    try:
        # 取得前に再チェック
        cached = cache_get(key)
        if cached:
            return build_redirect_response(cached, "RACE_HIT")

        neg = neg_cache_get(key)
        if neg:
            status_code, message = neg
            return build_error_response(status_code, message, "RACE_NEG_HIT")

        shared = hydrate_from_firestore(key)
        if shared:
            kind, payload = shared
            if kind == "ok":
                return build_redirect_response(payload, "RACE_FIRESTORE_HIT")

            status_code, message = payload
            return build_error_response(status_code, message, "RACE_FIRESTORE_NEG_HIT")

        photo_uri = fetch_photo_uri(ref=ref, name=name, maxw=maxw)

        # 成功したらローカル + Firestore に保存
        cache_set(key, photo_uri)
        neg_cache_clear(key)
        fs_cache_set_ok(key, ref, name, maxw, photo_uri)

        return build_redirect_response(photo_uri, "MISS")

    except UpstreamPhotoError as e:
        # 失敗したらローカル + Firestore に短期保存
        neg_cache_set(key, e.status_code, e.message)
        fs_cache_set_neg(key, ref, name, maxw, e.status_code, e.message)
        return build_error_response(e.status_code, e.message, "MISS_NEG")

    finally:
        inflight_release(key)