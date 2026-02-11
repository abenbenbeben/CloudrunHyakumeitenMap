from flask import Flask, request, redirect, abort, make_response
import os, requests

app = Flask(__name__)
API_KEY = os.environ["PLACES_API_KEY"]

@app.get("/place-photo")
def place_photo():
    name = request.args.get("name")   # places/.../photos/...
    maxw = request.args.get("maxw", "800")
    if not name:
        abort(400, "missing name")

    # Places Photos (New): media
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

    resp = redirect(photo_uri, code=302)
    # キャッシュ推奨（コスト削減）
    resp.headers["Cache-Control"] = "public, max-age=86400"
    return resp
