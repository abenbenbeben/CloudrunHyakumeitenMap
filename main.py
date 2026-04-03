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
# レスポンスのブラウザ/CDN向けキャッシュ時間
RESPONSE_MAX_AGE_SEC = int(os.environ.get("PHOTO_RESPONSE_MAX_AGE_SEC", "3600"))  # 1時間

# ローカルメモリキャッシュ
LOCAL_OK_TTL_SEC = int(os.environ.get("PHOTO_LOCAL_OK_TTL_SEC", "7200"))  # 2時間
LOCAL_NEG_TTL_SEC = int(os.environ.get("PHOTO_LOCAL_NEG_TTL_SEC", "60"))  # 1分
MAX_CACHE_SIZE = int(os.environ.get("PHOTO_URI_CACHE_MAX", "5000"))
MAX_NEG_CACHE_SIZE = int(os.environ.get("PHOTO_NEGATIVE_CACHE_MAX", "1000"))

# Firestore共有キャッシュ
FIRESTORE_OK_TTL_SEC = int(os.environ.get("PHOTO_FIRESTORE_OK_TTL_SEC", "864000"))  # 10日 CloudRun側の変数を編集すること

# Firestore I/O timeout
FIRESTORE_GET_TIMEOUT_SEC = float(os.environ.get("PHOTO_FIRESTORE_GET_TIMEOUT_SEC", "1.5"))
FIRESTORE_SET_TIMEOUT_SEC = float(os.environ.get("PHOTO_FIRESTORE_SET_TIMEOUT_SEC", "2.0"))

INFLIGHT_WAIT_SEC = int(os.environ.get("PHOTO_INFLIGHT_WAIT_SEC", "15"))

PHOTO_CACHE_COLLECTION = os.environ.get("PHOTO_CACHE_COLLECTION", "placePhotoCache")
PHOTO_CACHE_COL = DB.collection(PHOTO_CACHE_COLLECTION)

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
    resp.headers["Cache-Control"] = f"public, max-age={RESPONSE_MAX_AGE_SEC}"
    resp.headers["X-Photo-Cache"] = cache_status
    return resp


def build_error_response(status_code: int, message: str, cache_status: str):
    resp = make_response(message, status_code)
    resp.headers["X-Photo-Cache"] = cache_status
    return resp

def parse_bool_arg(name: str) -> bool:
    raw = str(request.args.get(name, "")).strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def get_existing_ok_uri(key: str):
    cached = cache_get(key)
    if cached:
        return cached

    shared = fs_cache_get(key)
    if shared and shared[0] == "ok":
        photo_uri = shared[1]
        cache_set(key, photo_uri)
        neg_cache_clear(key)
        return photo_uri

    return None

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

        _CACHE.move_to_end(key)
        return uri


def cache_set(key: str, uri: str):
    now = time.time()
    with _LOCK:
        _CACHE[key] = (uri, now + LOCAL_OK_TTL_SEC)
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
        _NEG_CACHE[key] = (status_code, message, now + LOCAL_NEG_TTL_SEC)
        _NEG_CACHE.move_to_end(key)

        while len(_NEG_CACHE) > MAX_NEG_CACHE_SIZE:
            _NEG_CACHE.popitem(last=False)


def neg_cache_clear(key: str):
    with _LOCK:
        _NEG_CACHE.pop(key, None)


# -----------------------------
# Firestore 共有キャッシュ
# -----------------------------
def fs_doc_ref(key: str):
    return PHOTO_CACHE_COL.document(build_doc_id(key))


def _normalize_dt(dt):
    if dt is None:
        return None
    if getattr(dt, "tzinfo", None) is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt

def fs_cache_get(key: str):
    """
    戻り値:
      ("ok", photo_uri) or None
    ※ Firestore では正常系のみ扱う
    """
    try:
        snap = fs_doc_ref(key).get(
            field_paths=["kind", "photoUri", "expiresAt"],
            timeout=FIRESTORE_GET_TIMEOUT_SEC,
        )

        if not snap.exists:
            return None

        data = snap.to_dict() or {}
        expires_at = _normalize_dt(data.get("expiresAt"))

        if not expires_at or expires_at <= utc_now():
            return None

        kind = data.get("kind")
        if kind == "ok":
            photo_uri = data.get("photoUri")
            if photo_uri:
                return ("ok", photo_uri)

        # neg は Firestore 上では無視
        return None

    except Exception:
        app.logger.exception("firestore cache get failed")
        return None


def fs_cache_set_ok(key: str, photo_uri: str):
    try:
        expires_at = utc_now() + timedelta(seconds=FIRESTORE_OK_TTL_SEC)

        # Firestore TTL policy は gcAt に対して設定する
        fs_doc_ref(key).set(
            {
                "kind": "ok",
                "photoUri": photo_uri,
                "expiresAt": expires_at,  # アプリ側の有効期限判定
                "gcAt": expires_at,       # Firestore TTL policy 用
                "updatedAt": firestore.SERVER_TIMESTAMP,
            },
            timeout=FIRESTORE_SET_TIMEOUT_SEC,
        )
    except Exception:
        app.logger.exception("firestore cache set ok failed")


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
      ("ok", photo_uri) / None
    """
    hit = fs_cache_get(key)
    if not hit:
        return None

    kind, payload = hit
    if kind == "ok":
        cache_set(key, payload)
        neg_cache_clear(key)
        return ("ok", payload)

    return None


# -----------------------------
# ルート
# -----------------------------
@app.get("/place-photo")
def place_photo():
    maxw = normalize_maxw(request.args.get("maxw", "800"))
    ref = request.args.get("ref")
    name = request.args.get("name")
    refresh = parse_bool_arg("refresh")

    if not ref and not name:
        abort(400, "missing ref or name")

    key = build_cache_key(ref, name, maxw)

    # refresh時は既存OKキャッシュだけ退避しておく
    fallback_ok_uri = get_existing_ok_uri(key) if refresh else None

    # 通常時だけ cache hit を返す
    if not refresh:
        cached = cache_get(key)
        if cached:
            return build_redirect_response(cached, "HIT")

        neg = neg_cache_get(key)
        if neg:
            status_code, message = neg
            return build_error_response(status_code, message, "NEG_HIT")

        shared = hydrate_from_firestore(key)
        if shared:
            kind, payload = shared
            if kind == "ok":
                return build_redirect_response(payload, "FIRESTORE_HIT")

            status_code, message = payload
            return build_error_response(status_code, message, "FIRESTORE_NEG_HIT")

    inflight_ev, is_owner = inflight_get_or_create(key)

    if not is_owner:
        finished = inflight_ev.wait(timeout=INFLIGHT_WAIT_SEC)

        cached = cache_get(key)
        if cached:
            return build_redirect_response(
                cached,
                "WAIT_REFRESH_HIT" if refresh else "WAIT_HIT",
            )

        neg = neg_cache_get(key)
        if neg:
            status_code, message = neg
            return build_error_response(
                status_code,
                message,
                "WAIT_REFRESH_NEG_HIT" if refresh else "WAIT_NEG_HIT",
            )

        shared = hydrate_from_firestore(key)
        if shared:
            kind, payload = shared
            if kind == "ok":
                return build_redirect_response(
                    payload,
                    "WAIT_REFRESH_FIRESTORE_HIT" if refresh else "WAIT_FIRESTORE_HIT",
                )

            status_code, message = payload
            return build_error_response(
                status_code,
                message,
                "WAIT_REFRESH_FIRESTORE_NEG_HIT" if refresh else "WAIT_FIRESTORE_NEG_HIT",
            )

        if refresh and fallback_ok_uri:
            return build_redirect_response(fallback_ok_uri, "WAIT_REFRESH_FALLBACK_HIT")

        if not finished:
            return build_error_response(504, "photo fetch in progress timeout", "WAIT_TIMEOUT")

        return build_error_response(502, "photo fetch completed but no cache populated", "WAIT_EMPTY")

    try:
        if not refresh:
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

        cache_set(key, photo_uri)
        neg_cache_clear(key)
        fs_cache_set_ok(key, photo_uri)

        return build_redirect_response(photo_uri, "REFRESH_MISS" if refresh else "MISS")
    # except UpstreamPhotoError の箇所
    except UpstreamPhotoError as e:
        if refresh and fallback_ok_uri:
            # 既存の正常キャッシュは壊さず、それを返す
            return build_redirect_response(fallback_ok_uri, "REFRESH_FALLBACK_HIT")

        neg_cache_set(key, e.status_code, e.message)
        # fs_cache_set_neg(key, e.status_code, e.message)  ← 削除

        return build_error_response(
            e.status_code,
            e.message,
            "REFRESH_MISS_NEG" if refresh else "MISS_NEG",
        )

    finally:
        inflight_release(key)