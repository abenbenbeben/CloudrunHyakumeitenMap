# main.py (Flask)
from flask import Flask, request, redirect, abort, make_response
import os, requests

app = Flask(__name__)
API_KEY = os.environ["PLACES_API_KEY"]

@app.get("/place-photo")
def place_photo():
    """
    Legacy photo_reference: /place-photo?ref=...&maxw=800
    New photo name (optional): /place-photo?name=places/.../photos/...&maxw=800
    """
    maxw = request.args.get("maxw", "800")

    # --- Legacy photo_reference ---
    ref = request.args.get("ref")
    if ref:
        url = "https://maps.googleapis.com/maps/api/place/photo"
        params = {
            "key": API_KEY,
            "photoreference": ref,
            "maxwidth": maxw,
        }
        # 302 を追従せず、Locationだけ取る
        r = requests.get(url, params=params, timeout=10, allow_redirects=False)

        if r.status_code not in (301, 302) or "Location" not in r.headers:
            # 200で画像バイトが返る場合もあるが、その場合はプロキシ配信が必要
            # 今回は “302前提”で設計しているので、分岐させるなら別途対応
            abort(r.status_code, f"unexpected response: {r.status_code}")

        photo_uri = r.headers["Location"]
        resp = make_response(redirect(photo_uri, code=302))
        resp.headers["Cache-Control"] = "public, max-age=86400"  # 1日キャッシュ例
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
