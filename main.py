# main.py (Flask)
from flask import Flask, request, redirect, abort, make_response
import os, requests, time
from threading import Lock

app = Flask(__name__)
API_KEY = os.environ["PLACES_API_KEY"]

# ---- 簡易TTLキャッシュ（インスタンス内） ----
_CACHE = {}  # key -> (photo_uri, expires_at)
_LOCK = Lock()

CACHE_TTL_SEC = int(os.environ.get("PHOTO_URI_CACHE_TTL_SEC", "3600"))  # 60分
MAX_CACHE_SIZE = int(os.environ.get("PHOTO_URI_CACHE_MAX", "5000"))

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
            # 雑に間引き（本気ならLRUに）
            _CACHE.clear()
        _CACHE[key] = (uri, now + CACHE_TTL_SEC)

SESSION = requests.Session()

@app.get("/place-photo")
def place_photo():
    maxw = request.args.get("maxw", "800")
    ref = request.args.get("ref")
    name = request.args.get("name")

    if not ref and not name:
        abort(400, "missing ref or name")

    key = f"ref:{ref}|maxw:{maxw}" if ref else f"name:{name}|maxw:{maxw}"
    cached = cache_get(key)
    if cached:
        resp = make_response(redirect(cached, code=302))
        # CDNを使うなら s-maxage を付ける（後述）
        resp.headers["Cache-Control"] = f"public, max-age={CACHE_TTL_SEC}"
        resp.headers["X-Photo-Cache"] = "HIT"
        return resp

    # ---- Placesへ問い合わせ（MISS） ----
    if ref:
        url = "https://maps.googleapis.com/maps/api/place/photo"
        params = {"key": API_KEY, "photoreference": ref, "maxwidth": maxw}
        r = SESSION.get(url, params=params, timeout=10, allow_redirects=False)

        if r.status_code in (301, 302) and "Location" in r.headers:
            photo_uri = r.headers["Location"]
        else:
            abort(r.status_code, f"unexpected response: {r.status_code}")

    else:
        url = f"https://places.googleapis.com/v1/{name}/media"
        params = {"key": API_KEY, "maxWidthPx": maxw, "skipHttpRedirect": "true"}
        r = SESSION.get(url, params=params, timeout=10)
        if r.status_code != 200:
            abort(r.status_code, r.text[:200])
        data = r.json()
        photo_uri = data.get("photoUri")
        if not photo_uri:
            abort(502, "no photoUri")

    cache_set(key, photo_uri)

    resp = make_response(redirect(photo_uri, code=302))
    resp.headers["Cache-Control"] = f"public, max-age={CACHE_TTL_SEC}"
    resp.headers["X-Photo-Cache"] = "MISS"
    return resp

    # --- New API photo name (任意) ---
    name = request.args.get("name")
    if name:
        url = f"https://places.googleapis.com/v1/{name}/media"
        params = {
            "key": API_KEY,
            "maxWidthPx": maxw,
            "skipHttpRedirect": "true",
        }
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200:
            abort(r.status_code, r.text[:200])
        data = r.json()
        photo_uri = data.get("photoUri")
        if not photo_uri:
            abort(502, "no photoUri")

        resp = make_response(redirect(photo_uri, code=302))
        resp.headers["Cache-Control"] = "public, max-age=86400"
        return resp

    abort(400, "missing ref or name")
