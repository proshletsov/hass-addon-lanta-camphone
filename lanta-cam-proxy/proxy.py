import asyncio, html, json, logging, os, re, socket, ssl, time, uuid
import aiohttp
import aiomqtt
from datetime import datetime, timezone, timedelta
from aiohttp import web
from aiohttp.abc import AbstractResolver

FFMPEG = os.environ.get("FFMPEG_PATH", "ffmpeg")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("lanta-proxy")

api_ssl = ssl.create_default_context()

stream_ssl = ssl.create_default_context()
stream_ssl.check_hostname = False
stream_ssl.verify_mode = ssl.CERT_NONE

API_HOST = "mobile.lanta-net.ru"
API_IP = "37.128.241.40"
API_BASE = f"https://{API_HOST}"

PHONE_CFG = os.environ.get("LANTA_PHONE", "").strip()
TOKEN_CFG = os.environ.get("LANTA_TOKEN", "").strip()
HOUSE_CFG = os.environ.get("LANTA_HOUSE_ID", "").strip()
FLAT_CFG = os.environ.get("LANTA_FLAT_ID", "").strip()
REFRESH_INT = int(os.environ.get("TOKEN_REFRESH_INTERVAL", "5400"))
PORT = int(os.environ.get("PORT", "8199"))
STATE_FILE = "/data/lanta_auth.json"

# MQTT / домофон
MQTT_HOST = os.environ.get("MQTT_HOST", "").strip()
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883") or 1883)
MQTT_USER = os.environ.get("MQTT_USER", "")
MQTT_PASS = os.environ.get("MQTT_PASS", "")
PLOG_POLL_INTERVAL = int(os.environ.get("PLOG_POLL_INTERVAL", "7"))
DEDUP_WINDOW = int(os.environ.get("DEDUP_WINDOW", "180"))
INTERCOM_POLL_INTERVAL = int(os.environ.get("INTERCOM_POLL_INTERVAL", "30"))

cameras = {}
last_refresh = 0
_api_session = None
_stream_session = None


class LantaResolver(AbstractResolver):
    async def resolve(self, host, port=0, family=socket.AF_INET):
        if host == API_HOST:
            return [{
                "hostname": host,
                "host": API_IP,
                "port": port or 443,
                "family": socket.AF_INET,
                "proto": 0,
                "flags": socket.AI_NUMERICHOST,
            }]
        loop = asyncio.get_running_loop()
        infos = await loop.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        return [{
            "hostname": host,
            "host": info[4][0],
            "port": info[4][1],
            "family": info[0],
            "proto": info[2],
            "flags": 0,
        } for info in infos]

    async def close(self):
        pass


MSK = timezone(timedelta(hours=3))

# ---------------------------------------------------------------------------
# База имён людей (faces / keys / phones)
# ---------------------------------------------------------------------------
PEOPLE_FILE = "/data/lanta_people.json"
people = {"faces": {}, "keys": {}, "phones": {}}


def load_people() -> dict:
    if os.path.exists(PEOPLE_FILE):
        try:
            with open(PEOPLE_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            for sec in ("faces", "keys", "phones"):
                if isinstance(d.get(sec), dict):
                    people[sec] = {str(k): str(v) for k, v in d[sec].items()}
        except Exception as e:
            log.warning("people read error: %s", e)
    return people


def save_people():
    os.makedirs("/data", exist_ok=True)
    try:
        with open(PEOPLE_FILE, "w", encoding="utf-8") as f:
            json.dump(people, f, indent=2, ensure_ascii=False)
    except Exception as e:
        log.warning("people write error: %s", e)


load_people()

# ---------------------------------------------------------------------------
# Глобальное состояние домофона / MQTT
# ---------------------------------------------------------------------------
door_state = {
    "settings": {},
    "last_event": None,
    "events": [],
    "last_uuid": "",
    "last_preview_bytes": b"",
    "last_preview_url": "",
}
faces_cache = {}          # faceId -> image url
flat_id_cache = ""
mqtt_client = None
mqtt_out = asyncio.Queue()
last_seen = {}            # person -> {"ts": epoch, "objectId": str}
token_status = {"valid": None, "reason": ""}
_plog_initialized = False
_mqtt_states_inited = False

EVENT_TYPES = {
    "1": "Звонок в домофон",
    "2": "Звонок в домофон",
    "3": "Открытие ключом",
    "4": "Открытие из приложения",
    "5": "Распознано лицо",
    "6": "Открытие по коду",
}


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------
def norm_phone(p: str) -> str:
    d = "".join(c for c in (p or "") if c.isdigit())
    if len(d) == 11 and d.startswith("8"):
        return "7" + d[1:]
    return ("7" + d) if len(d) == 10 else d


def parse_dt(s) -> "datetime | None":
    if not s:
        return None
    try:
        return datetime.strptime(str(s), "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK)
    except Exception:
        return None


def _detailx(ev: dict) -> dict:
    dx = ev.get("detailX")
    return dx if isinstance(dx, dict) else {}


def esc(s) -> str:
    return html.escape(str(s), quote=True)


PHONE_KEYS = {"phone", "userphone", "phonenumber", "mobilephone", "mobile", "phonestr"}


def find_phones(node, out=None):
    if out is None:
        out = set()
    if isinstance(node, list):
        for x in node:
            find_phones(x, out)
    elif isinstance(node, dict):
        for k, v in node.items():
            if k.lower() in PHONE_KEYS and isinstance(v, str):
                p = norm_phone(v)
                if len(p) >= 10:
                    out.add(p)
            if isinstance(v, (list, dict)):
                find_phones(v, out)
    return out


def save_state(st: dict):
    os.makedirs("/data", exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(st, f, indent=2, ensure_ascii=False)


def load_state() -> dict:
    st = {"token": "", "device_token": str(uuid.uuid4()), "phone": norm_phone(PHONE_CFG)}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                st.update(json.load(f))
        except Exception as e:
            log.warning("State read error: %s", e)
    if TOKEN_CFG:
        clean = TOKEN_CFG.replace("Bearer ", "").strip()
        if clean and st.get("token") != clean:
            st["token"] = clean
    if PHONE_CFG and not st.get("phone"):
        st["phone"] = norm_phone(PHONE_CFG)
    save_state(st)
    return st


state = load_state()


def set_token_status(valid: bool, reason: str = ""):
    if token_status.get("valid") != valid or token_status.get("reason") != reason:
        token_status["valid"] = valid
        token_status["reason"] = reason


# ---------------------------------------------------------------------------
# HTTP-сессии и API
# ---------------------------------------------------------------------------
async def api_sess() -> aiohttp.ClientSession:
    global _api_session
    if _api_session is None or _api_session.closed:
        conn = aiohttp.TCPConnector(ssl=api_ssl, resolver=LantaResolver())
        _api_session = aiohttp.ClientSession(
            connector=conn,
            headers={
                "Host": API_HOST,
                "Origin": API_BASE,
                "Referer": f"{API_BASE}/addresses",
                "Accept": "application/json, text/plain, */*",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/130.0.0.0",
            },
        )
    return _api_session


async def http_sess() -> aiohttp.ClientSession:
    global _stream_session
    if _stream_session is None or _stream_session.closed:
        _stream_session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=stream_ssl),
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/130.0.0.0"},
        )
    return _stream_session


async def api_post(path: str, payload=None):
    tok = state.get("token", "").strip()
    if not tok:
        set_token_status(False, "no_token")
    hdr = {"Authorization": f"Bearer {tok}" if tok else "Bearer"}
    s = await api_sess()
    try:
        if payload is None:
            req_kw = {"headers": hdr, "data": b""}
        else:
            hdr["Content-Type"] = "application/json"
            req_kw = {"headers": hdr, "json": payload}
        async with s.post(f"{API_BASE}{path}", **req_kw) as r:
            txt = await r.text()
            if r.status == 401:
                set_token_status(False, "unauthorized")
            elif r.status < 400:
                set_token_status(True, "")
            try:
                return r.status, (json.loads(txt) if txt else {})
            except Exception:
                return r.status, {"raw": txt[:300]}
    except Exception as e:
        log.error("api_post %s error: %s", path, e)
        return -1, {}


async def _get_flat_id() -> str:
    if FLAT_CFG:
        return FLAT_CFG
    for ep in ("/api/address/getSettingsList", "/api/address/getAddressList"):
        st, d = await api_post(ep, None)
        if st == 200 and isinstance(d, dict) and isinstance(d.get("data"), list):
            for f in d["data"]:
                if f.get("flatId"):
                    return str(f["flatId"])
    return ""


async def get_flat_id_cached() -> str:
    global flat_id_cache
    if not flat_id_cache:
        try:
            flat_id_cache = await _get_flat_id()
        except Exception as e:
            log.warning("flat_id resolve error: %s", e)
    return flat_id_cache


async def guest_access(request):
    flat_id = request.query.get("flatId") or await get_flat_id_cached()
    if not flat_id:
        return web.json_response(
            {
                "is_active": False,
                "error": "flatId not found. Set flat_id in add-on configuration or pass ?flatId=...",
            },
            status=400,
        )

    now_msk = datetime.now(MSK)

    if request.method == "POST":
        body = await _read_input(request) if request.can_read_body else {}
        enable = body.get("enable", True)
        if isinstance(enable, str):
            enable = enable.lower() not in ("false", "0", "off", "no")
        target = now_msk + (timedelta(hours=1) if enable else timedelta(seconds=-5))
        dt_str = target.strftime("%Y-%m-%d %H:%M:%S")
        payload = {"flatId": str(flat_id), "settings": {"autoOpen": dt_str}}
        st, d = await api_post("/api/address/intercom", payload)
    else:
        st, d = await api_post("/api/address/intercom", {"flatId": str(flat_id)})

    data = d.get("data", {}) if isinstance(d, dict) else {}
    auto_str = data.get("autoOpen", "") if isinstance(data, dict) else ""
    is_active = False
    if auto_str:
        try:
            exp = datetime.strptime(auto_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK)
            is_active = exp > datetime.now(MSK)
        except Exception:
            pass

    return web.json_response({
        "is_active": is_active,
        "autoOpen": auto_str,
        "flatId": str(flat_id),
        "status": st,
    })


def extract_cams(node, prefix="", out=None):
    if out is None:
        out = []
    if isinstance(node, list):
        for x in node:
            extract_cams(x, prefix, out)
    elif isinstance(node, dict):
        u = node.get("url") or node.get("hls") or node.get("link_video")
        if isinstance(u, str) and u.startswith("http"):
            u_clean = u.rstrip("/")
            cam_tok = node.get("token", "")
            cid = node.get("id") or node.get("cameraId")
            nm = node.get("name") or node.get("title") or f"Cam {cid}"
            if ".m3u8" in u_clean:
                vid = u_clean
            elif cam_tok:
                vid = f"{u_clean}/index.m3u8?token={cam_tok}"
            else:
                vid = f"{u_clean}/index.m3u8"
            prv = node.get("shot") or node.get("preview") or node.get("screenshot") or ""
            if not prv:
                prv = f"{u_clean}/preview.jpg?token={cam_tok}" if cam_tok else f"{u_clean}/preview.jpg"
            out.append({
                "id": cid,
                "title": f"{prefix} {nm}".strip(),
                "video": vid,
                "preview": prv,
            })
        else:
            grp = node.get("groupName") or node.get("name") or ""
            np = f"{prefix} {grp}".strip() if grp else prefix
            for k in ("cameras", "childGroups", "data", "items", "list"):
                if k in node:
                    extract_cams(node[k], np, out)
    return out


async def refresh_cameras():
    global cameras, last_refresh
    if not state.get("token"):
        log.warning("No token! Open /auth or set bearer_token in config.")
        return
    try:
        found, hids = [], []
        if HOUSE_CFG:
            for h in HOUSE_CFG.split(","):
                if h.strip():
                    hids.append(h.strip())
        for ep in ("/api/address/getSettingsList", "/api/address/getAddressList"):
            st, d = await api_post(ep, None)
            if st == 401:
                log.error("Token unauthorized (401). Update token at /auth.")
                return
            if st == 200 and isinstance(d, dict) and isinstance(d.get("data"), list):
                for a in d["data"]:
                    hid = a.get("houseId")
                    if hid and str(hid) not in hids:
                        hids.append(str(hid))
                if hids:
                    break
        for hid in hids:
            st, cd = await api_post("/api/cctv/all", {"houseId": str(hid)})
            if st == 200 and cd:
                found.extend(extract_cams(cd))
        if not found:
            log.warning("0 cameras returned. Check /debug_api (hids=%s)", hids)
            return

        new_cams = {}
        for i, it in enumerate(found, 1):
            cid = int(it["id"]) if str(it["id"]).isdigit() else i
            new_cams[cid] = {
                "title": it["title"],
                "link_video": it["video"],
                "preview_url": it["preview"],
            }
        cameras = new_cams
        last_refresh = time.time()
        log.info("Loaded %d cameras (houses: %s)", len(cameras), hids)
    except Exception as e:
        log.error("Refresh error: %s", e)


async def bg_loop():
    while True:
        try:
            await refresh_cameras()
        except Exception as e:
            log.error("bg_loop error: %s", e)
        await asyncio.sleep(REFRESH_INT)


# ---------------------------------------------------------------------------
# Авторизация /auth
# ---------------------------------------------------------------------------
async def auth_ui(request):
    tok_ok = "ДА" if state.get("token") else "НЕТ"
    ph = state.get("phone", "")
    msg = request.query.get("msg", "")
    lines = [
        '<!doctype html><meta charset="utf-8"><title>LanTa Auth</title>',
        '<body style="font-family:sans-serif;max-width:520px;margin:30px auto">',
        f"<h3>LanTa Proxy | Токен: {tok_ok} | Камер: {len(cameras)}</h3>",
        '<p><a href="/cameras">/cameras</a> | <a href="/go2rtc.yaml">/go2rtc.yaml</a> | <a href="/people">/people</a> | <a href="/door_events">/door_events</a> | <a href="/debug_api">/debug_api</a></p>',
        f'<p style="background:#eef;padding:8px">{msg}</p>' if msg else "",
        "<hr><h4>Способ 1: Вход по звонку</h4>",
        '<form method="post" action="/auth/req">',
        f'Телефон: <input name="phone" value="{ph}" placeholder="79001234567"> ',
        '<button type="submit">1. Запросить звонок</button></form>',
        "<p>Позвоните на <b>88002220668</b>, дождитесь сброса и нажмите:</p>",
        '<form method="post" action="/auth/chk">',
        f'<input type="hidden" name="phone" value="{ph}">',
        '<button type="submit">2. Я позвонил — получить токен</button></form>',
        "<hr><h4>Способ 2: Готовый токен</h4>",
        '<form method="post" action="/auth/tok">',
        '<input name="token" placeholder="UUID без Bearer" style="width:70%"> ',
        '<button type="submit">Сохранить</button></form></body>',
    ]
    return web.Response(text="\n".join(lines), content_type="text/html")


async def _read_input(request):
    if "json" in request.content_type:
        return await request.json()
    return dict(await request.post())


async def auth_req(request):
    b = await _read_input(request)
    state["phone"] = norm_phone(b.get("phone", ""))
    save_state(state)
    st, d = await api_post("/api/user/requestCode", {"userPhone": state["phone"]})
    raise web.HTTPFound(f"/auth?msg=Код_ответа_{st}_Звоните_на_88002220668")


async def auth_chk(request):
    b = await _read_input(request)
    ph = norm_phone(b.get("phone") or state.get("phone", ""))
    payload = {"userPhone": ph, "deviceToken": state["device_token"], "code": ""}
    st, d = await api_post("/api/user/checkPhone", payload)
    inner = d.get("data") if isinstance(d, dict) else None
    tok = ""
    if isinstance(inner, dict):
        tok = inner.get("accessToken") or inner.get("token") or ""
    elif isinstance(inner, str):
        tok = inner
    if not tok and isinstance(d, dict):
        tok = d.get("accessToken") or ""
    if tok:
        state["token"], state["phone"] = tok, ph
        save_state(state)
        set_token_status(True, "")
        await refresh_cameras()
        raise web.HTTPFound(f"/auth?msg=Токен_сохранен_Камер_{len(cameras)}")
    raise web.HTTPFound(f"/auth?msg=Ошибка_{st}_Проверьте_звонок")


async def auth_tok(request):
    b = await _read_input(request)
    tok = (b.get("token") or "").replace("Bearer ", "").strip()
    if tok:
        state["token"] = tok
        save_state(state)
        set_token_status(True, "")
        await refresh_cameras()
        raise web.HTTPFound(f"/auth?msg=Токен_сохранен_Камер_{len(cameras)}")
    raise web.HTTPFound("/auth?msg=Пустой_токен")


async def debug_api(request):
    res = {}
    st1, d1 = await api_post("/api/address/getSettingsList", None)
    res["getSettingsList"] = {"status": st1, "data": d1}
    st2, d2 = await api_post("/api/address/getAddressList", None)
    res["getAddressList"] = {"status": st2, "data": d2}
    return web.json_response(res)


async def list_cams(request):
    h = request.headers.get("Host", f"0.0.0.0:{PORT}")
    res = []
    for k, v in cameras.items():
        res.append({
            "id": k,
            "title": v["title"],
            "stream": f"http://{h}/stream/{k}/index.m3u8",
            "ts": f"http://{h}/ts/{k}",
            "preview": f"http://{h}/preview/{k}",
        })
    return web.json_response({
        "cameras": res,
        "last_refresh": last_refresh,
        "auth_url": f"http://{h}/auth",
    })


async def go2rtc_cfg(request):
    h = request.headers.get("Host", f"0.0.0.0:{PORT}")
    lines = ["streams:"]
    for k, v in cameras.items():
        lines.append(f"  cam_{k}: http://{h}/ts/{k} # {v['title']}")
    return web.Response(text="\n".join(lines) + "\n", content_type="text/yaml")


async def proxy_stream(request):
    cid = int(request.match_info["cam_id"])
    path = request.match_info.get("path", "index.m3u8")
    if cid not in cameras:
        return web.Response(status=404, text="Not found")
    u_path, _, qs = cameras[cid]["link_video"].partition("?")
    base = os.path.dirname(u_path)
    url = f"{base}/{path}?{qs}" if qs else f"{base}/{path}"
    s = await http_sess()
    async with s.get(url) as r:
        if r.status == 403:
            await refresh_cameras()
            if cid not in cameras:
                return web.Response(status=404, text="Lost after refresh")
            u_path, _, qs = cameras[cid]["link_video"].partition("?")
            base = os.path.dirname(u_path)
            url = f"{base}/{path}?{qs}" if qs else f"{base}/{path}"
            r = await s.get(url)
        body = await r.read()
        ct = r.headers.get("Content-Type", "application/vnd.apple.mpegurl")
        if "mpegurl" in ct or path.endswith(".m3u8"):
            decoded = body.decode("utf-8", "replace").replace(base, f"/stream/{cid}")
            txt = re.sub(r'\?token=[^\s"]+', "", decoded)
            body = txt.encode("utf-8")
        hdrs = {"Content-Type": ct, "Access-Control-Allow-Origin": "*"}
        return web.Response(body=body, status=r.status, headers=hdrs)


async def proxy_preview(request):
    cid = int(request.match_info["cam_id"])
    cam = cameras.get(cid)
    if not cam or not cam.get("preview_url"):
        return web.Response(status=404)
    s = await http_sess()
    async with s.get(cam["preview_url"]) as r:
        ctype = r.headers.get("Content-Type", "image/jpeg")
        return web.Response(body=await r.read(), content_type=ctype)


async def proxy_ts(request):
    cid = int(request.match_info["cam_id"])
    if cid not in cameras:
        return web.Response(status=404)
    hdrs = {
        "Content-Type": "video/mp2t",
        "Cache-Control": "no-cache",
        "Access-Control-Allow-Origin": "*",
    }
    resp = web.StreamResponse(status=200, headers=hdrs)
    await resp.prepare(request)
    cmd = [
        FFMPEG, "-hide_banner", "-loglevel", "error",
        "-re", "-fflags", "nobuffer+genpts", "-flags", "low_delay",
        "-i", cameras[cid]["link_video"],
        "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
        "-g", "30", "-c:a", "aac", "-vf", "fps=15",
        "-f", "mpegts", "-flush_packets", "1", "-",
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        while chunk := await proc.stdout.read(4096):
            await resp.write(chunk)
    except Exception:
        pass
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()
    return resp


async def force_ref(request):
    await refresh_cameras()
    return web.json_response({"ok": True, "cameras": len(cameras)})


# ---------------------------------------------------------------------------
# /people — имена людей (лица / ключи / телефоны)
# ---------------------------------------------------------------------------
async def load_faces() -> dict:
    flat = await get_flat_id_cached()
    if not flat:
        return {}
    out = {}
    try:
        st, d = await api_post("/api/frs/listFaces", {"flatId": flat})
        arr = d.get("data") if isinstance(d, dict) else d
        if st == 200 and isinstance(arr, list):
            for f in arr:
                if isinstance(f, dict):
                    fid = str(f.get("faceId") or "")
                    img = f.get("image") or ""
                    if fid:
                        out[fid] = img
                        faces_cache[fid] = img
    except Exception as e:
        log.warning("load_faces error: %s", e)
    return out


def _collect_keys_from(events, keys):
    for ev in events:
        if str(ev.get("event")) == "3":
            dx = _detailx(ev)
            k = str(dx.get("key") or ev.get("detail") or "")
            if k:
                keys.add(k)


async def collect_keys():
    keys = set(people.get("keys", {}).keys())
    evs = door_state.get("events", [])
    if not evs:
        # журнал ещё не накоплен (например, сразу после старта) — подтягиваем сегодня и вчера
        flat = await get_flat_id_cached()
        if flat:
            now = datetime.now(MSK)
            for day in (now.strftime("%Y-%m-%d"), (now - timedelta(days=1)).strftime("%Y-%m-%d")):
                fetched = await fetch_plog(flat, day)
                _collect_keys_from(fetched, keys)
    else:
        _collect_keys_from(evs, keys)
    return sorted(keys)


async def collect_phones():
    phones = set(people.get("phones", {}).keys())
    flat = await get_flat_id_cached()
    if flat:
        try:
            st, d = await api_post("/api/address/getSettingsList", None)
            if st == 200:
                find_phones(d, phones)
        except Exception as e:
            log.warning("collect_phones error: %s", e)
    return sorted(phones)


async def people_page(request):
    faces = await load_faces()
    keys = await collect_keys()
    phones = await collect_phones()
    msg = request.query.get("msg", "")
    parts = [
        '<!doctype html><meta charset="utf-8"><title>LanTa Люди</title>',
        '<body style="font-family:sans-serif;max-width:720px;margin:30px auto">',
        '<h3>LanTa Proxy | Имена людей</h3>',
        '<p><a href="/auth">/auth</a> | <a href="/cameras">/cameras</a> | <a href="/door_events">/door_events</a></p>',
        f'<p style="background:#eef;padding:8px">{esc(msg)}</p>' if msg else "",
        '<form method="post" action="/people/save">',
    ]
    parts.append('<h4>Лица (распознавание)</h4>')
    if faces:
        parts.append('<table cellpadding="4">')
        for fid, _img in sorted(faces.items()):
            name = people.get("faces", {}).get(fid, "")
            parts.append('<tr>')
            parts.append(f'<td><img src="/face_img/{esc(fid)}" width="72" height="72" style="object-fit:cover"></td>')
            parts.append(f'<td>faceId<br><small>{esc(fid)}</small></td>')
            parts.append(f'<td><input name="face_{esc(fid)}" value="{esc(name)}" placeholder="Имя"></td>')
            parts.append('</tr>')
        parts.append('</table>')
    else:
        parts.append('<p><i>Список лиц пуст (нет ответа /api/frs/listFaces или нет зарегистрированных лиц).</i></p>')
    parts.append('<h4>Ключи / брелоки (RFID)</h4>')
    if keys:
        for k in keys:
            name = people.get("keys", {}).get(k, "")
            parts.append(f'<div>Ключ <b>{esc(k)}</b>: <input name="key_{esc(k)}" value="{esc(name)}" placeholder="Имя"></div>')
    else:
        parts.append('<p><i>Ключи не найдены в журнале (события "Открытие ключом").</i></p>')
    parts.append('<h4>Телефоны (открытие из приложения)</h4>')
    if phones:
        for p in phones:
            name = people.get("phones", {}).get(p, "")
            parts.append(f'<div>Телефон <b>{esc(p)}</b>: <input name="phone_{esc(p)}" value="{esc(name)}" placeholder="Имя"></div>')
    else:
        parts.append('<p><i>Телефоны не найдены (getSettingsList → roommates).</i></p>')
    parts.append('<p><button type="submit">Сохранить имена</button></p></form>')
    parts.append('</body>')
    return web.Response(text="\n".join(parts), content_type="text/html")


async def people_save(request):
    data = await request.post()
    for k, v in data.items():
        if isinstance(v, (list, tuple)):
            v = v[0] if v else ""
        v = str(v).strip()
        if k.startswith("face_"):
            fid = k[len("face_"):]
            if v:
                people.setdefault("faces", {})[fid] = v
            else:
                people.setdefault("faces", {}).pop(fid, None)
        elif k.startswith("key_"):
            kid = k[len("key_"):]
            if v:
                people.setdefault("keys", {})[kid] = v
            else:
                people.setdefault("keys", {}).pop(kid, None)
        elif k.startswith("phone_"):
            pid = k[len("phone_"):]
            if v:
                people.setdefault("phones", {})[pid] = v
            else:
                people.setdefault("phones", {}).pop(pid, None)
    save_people()
    raise web.HTTPFound("/people?msg=Сохранено")


async def face_img(request):
    fid = request.match_info["face_id"]
    img = faces_cache.get(fid)
    if not img:
        await load_faces()
        img = faces_cache.get(fid)
    if not img:
        return web.Response(status=404, text="not found")
    url = img if img.startswith("http") else f"{API_BASE}{img}"
    try:
        s = await http_sess()
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status != 200:
                return web.Response(status=r.status)
            return web.Response(body=await r.read(), content_type=r.headers.get("Content-Type", "image/jpeg"))
    except Exception as e:
        log.warning("face_img %s error: %s", fid, e)
        return web.Response(status=502, text="fetch error")


# ---------------------------------------------------------------------------
# Парсер plog: склейка фото, определение человека, дедупликация
# ---------------------------------------------------------------------------
def _find_face_candidates(ev: dict, all_events, window=35):
    code = str(ev.get("event") or "")
    if code not in ("1", "2", "3", "6"):
        return []
    ev_dt = parse_dt(ev.get("date"))
    obj = str(ev.get("objectId") or "")
    if ev_dt is None:
        return []
    out = []
    for f in all_events:
        if str(f.get("event") or "") != "5":
            continue
        if str(f.get("objectId") or "") != obj:
            continue
        ft = parse_dt(f.get("date"))
        if ft is None:
            continue
        delta = abs((ev_dt - ft).total_seconds())
        if delta <= window:
            out.append((delta, f))
    return out


def _face_tier(f: dict) -> int:
    dx = _detailx(f)
    fid = str(dx.get("faceId") or "")
    flags = dx.get("flags")
    flags = flags if isinstance(flags, list) else []
    if fid in people.get("faces", {}):
        return 3
    if "liked" in flags:
        return 2
    return 1


def stitch_preview(ev: dict, all_events) -> str:
    cands = _find_face_candidates(ev, all_events)
    if not cands:
        return ""
    best = None
    best_tier = -1
    best_delta = None
    for delta, f in cands:
        tier = _face_tier(f)
        if tier > best_tier or (tier == best_tier and (best_delta is None or delta < best_delta)):
            best = f
            best_tier = tier
            best_delta = delta
    if best:
        return best.get("preview") or ""
    return ""


def match_known_face(ev: dict, all_events) -> str:
    """Для события 6 (код): ищем знакомое лицо в окне ±35с на той же двери."""
    cands = _find_face_candidates(ev, all_events)
    best_name = ""
    best_delta = None
    for delta, f in cands:
        dx = _detailx(f)
        fid = str(dx.get("faceId") or "")
        nm = people.get("faces", {}).get(fid, "")
        if nm and (best_delta is None or delta < best_delta):
            best_name = nm
            best_delta = delta
    return best_name


async def fetch_plog(flat: str, day: str):
    try:
        st, d = await api_post("/api/address/plog", {"flatId": flat, "day": day})
    except Exception as e:
        log.warning("plog fetch error: %s", e)
        return []
    if st != 200:
        return []
    arr = d.get("data") if isinstance(d, dict) else d
    if not isinstance(arr, list):
        return []
    out = [dict(x) for x in arr if isinstance(x, dict)]
    out.sort(key=lambda x: (str(x.get("date") or ""), str(x.get("uuid") or "")), reverse=True)
    return out


_flash_tasks = {}


async def flash_binary(topic: str, seconds=15):
    old = _flash_tasks.get(topic)
    if old and not old.done():
        old.cancel()
    await mqtt_publish(topic, "ON", retain=True)
    t = asyncio.create_task(_flash_off(topic, seconds))
    _flash_tasks[topic] = t
    t.add_done_callback(lambda _t, top=topic: _flash_tasks.pop(top, None))


async def _flash_off(topic: str, seconds: int):
    await asyncio.sleep(seconds)
    await mqtt_publish(topic, "OFF", retain=True)


async def finalize_event(ev, code, person, is_known, is_dup, face_id, key, phone, preview_url, dx):
    obj_id = str(ev.get("objectId") or "")
    door = ev.get("mechanizmaDescription") or obj_id
    date_str = ev.get("date") or ""
    dt = parse_dt(date_str)
    iso = dt.isoformat() if dt else ""
    code_val = str(dx.get("code") or ev.get("detail") or "")
    door_code = str((door_state.get("settings") or {}).get("doorCode", "") or "")

    preview_bytes = b""
    if preview_url:
        try:
            url = preview_url if preview_url.startswith("http") else f"{API_BASE}{preview_url}"
            s = await http_sess()
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status == 200:
                    preview_bytes = await r.read()
        except Exception as e:
            log.warning("preview download failed (%s): %s", url, e)

    event_data = {
        "uuid": str(ev.get("uuid") or ""),
        "event_code": code,
        "event_type": EVENT_TYPES.get(code, code),
        "person": person,
        "is_known": is_known,
        "is_duplicate_pass": is_dup,
        "door": door,
        "objectId": obj_id,
        "code": code_val,
        "key": key,
        "phone": phone,
        "face_id": face_id,
        "date": date_str,
        "iso_date": iso,
        "preview_url": preview_url,
        "local_preview": "/event_preview",
        "door_code": door_code,
    }
    door_state["last_event"] = event_data
    if preview_bytes:
        door_state["last_preview_bytes"] = preview_bytes
        door_state["last_preview_url"] = preview_url

    if MQTT_HOST:
        flat = flat_id_cache
        if flat:
            root = f"lanta/{flat}"
            await mqtt_publish(f"{root}/sensor/last_event/state", event_data["event_type"], retain=True)
            await mqtt_publish(f"{root}/sensor/last_event/attributes", json.dumps(event_data, ensure_ascii=False), retain=True)
            if preview_bytes:
                await mqtt_publish(f"{root}/camera/last_frame", preview_bytes, retain=True)
            if code == "6":
                await flash_binary(f"{root}/binary_sensor/door_code/state")
            elif code in ("1", "2"):
                await flash_binary(f"{root}/binary_sensor/call/state")
    log.info("event %s type=%s person=%r dup=%s", event_data["uuid"], event_data["event_type"], person, is_dup)


async def process_events(all_events, new_events):
    for ev in new_events:
        code = str(ev.get("event") or "")
        if code not in ("1", "2", "3", "4", "5", "6"):
            continue
        dx = _detailx(ev)
        person = ""
        is_known = False
        face_id = ""
        key = ""
        phone = ""

        if code == "5":
            face_id = str(dx.get("faceId") or "")
            person = people.get("faces", {}).get(face_id, "")
            is_known = bool(person)
            if not is_known:
                # незнакомое лицо без связи с кодом/звонком — не триггерим (анти-спам)
                continue
        elif code == "3":
            key = str(dx.get("key") or ev.get("detail") or "")
            person = people.get("keys", {}).get(key, "")
            is_known = bool(person)
        elif code == "4":
            phone = norm_phone(str(dx.get("phone") or ""))
            person = people.get("phones", {}).get(phone, "")
            is_known = bool(person)
        elif code == "6":
            person = match_known_face(ev, all_events)
            is_known = bool(person)
            if not is_known:
                person = "Неизвестный (по коду)"
        elif code in ("1", "2"):
            pass

        # дедупликация одного прохода (двор 1063 -> подъезд 1059 -> лифт 1061)
        is_dup = False
        now_ts = time.time()
        if is_known:
            prev = last_seen.get(person)
            if prev and (now_ts - prev.get("ts", 0)) <= DEDUP_WINDOW:
                is_dup = True
            last_seen[person] = {"ts": now_ts, "objectId": str(ev.get("objectId") or "")}

        preview_url = ev.get("preview") or ""
        if not preview_url:
            preview_url = stitch_preview(ev, all_events)

        await finalize_event(ev, code, person, is_known, is_dup, face_id, key, phone, preview_url, dx)


async def poll_plog():
    global _plog_initialized
    flat = await get_flat_id_cached()
    if not flat:
        return
    now = datetime.now(MSK)
    day = now.strftime("%Y-%m-%d")
    events = await fetch_plog(flat, day)

    # Первый запуск: инициализируем last_uuid, не рассылая старые события
    if not _plog_initialized:
        if events:
            door_state["last_uuid"] = events[0].get("uuid")
        else:
            yday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
            yev = await fetch_plog(flat, yday)
            if yev:
                door_state["last_uuid"] = yev[0].get("uuid")
        _plog_initialized = True
        log.info("plog initialized, last_uuid=%s", door_state.get("last_uuid"))
        return

    if not events:
        return

    newest_uuid = events[0].get("uuid")
    last_uuid = door_state.get("last_uuid", "")
    new_events = []
    for ev in events:
        if ev.get("uuid") == last_uuid:
            break
        new_events.append(ev)

    door_state["events"] = (door_state["events"] + [dict(e) for e in new_events])[-300:]
    if new_events:
        new_events.reverse()  # обрабатываем от старых к новым
        await process_events(events, new_events)
    door_state["last_uuid"] = newest_uuid


async def plog_loop():
    while True:
        try:
            await poll_plog()
        except Exception as e:
            log.error("plog loop error: %s", e)
        await asyncio.sleep(PLOG_POLL_INTERVAL)


# ---------------------------------------------------------------------------
# Управление домофоном / опрос состояния
# ---------------------------------------------------------------------------
def _guest_active(s: dict) -> bool:
    auto = s.get("autoOpen", "")
    if not auto:
        return False
    try:
        exp = datetime.strptime(auto, "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK)
        return exp > datetime.now(MSK)
    except Exception:
        return False


async def set_intercom(key: str, val: str) -> bool:
    flat = await get_flat_id_cached()
    if not flat:
        return False
    try:
        st, d = await api_post("/api/address/intercom", {"flatId": flat, "settings": {key: val}})
        log.info("set_intercom %s=%s -> status %s", key, val, st)
        return st == 200
    except Exception as e:
        log.error("set_intercom error: %s", e)
        return False


async def poll_intercom():
    flat = await get_flat_id_cached()
    if not flat:
        return
    try:
        st, d = await api_post("/api/address/intercom", {"flatId": flat})
        if st == 200 and isinstance(d, dict) and isinstance(d.get("data"), dict):
            door_state["settings"] = d["data"]
    except Exception as e:
        log.warning("poll_intercom error: %s", e)
    await publish_switches()


async def publish_switches():
    if not MQTT_HOST:
        return
    flat = flat_id_cache
    if not flat:
        return
    root = f"lanta/{flat}"
    s = door_state.get("settings") or {}
    await mqtt_publish(f"{root}/switch/guest_access/state", "ON" if _guest_active(s) else "OFF", retain=True)
    await mqtt_publish(f"{root}/switch/cms/state", "ON" if s.get("CMS") == "t" else "OFF", retain=True)
    await mqtt_publish(f"{root}/switch/voip/state", "ON" if s.get("VoIP") == "t" else "OFF", retain=True)
    await mqtt_publish(f"{root}/switch/allow_door_code/state", "ON" if s.get("allowDoorCode") == "t" else "OFF", retain=True)
    tok_ok = token_status.get("valid") is not False and bool(state.get("token"))
    await mqtt_publish(f"{root}/binary_sensor/token/state", "OFF" if tok_ok else "ON", retain=True)


async def intercom_loop():
    while True:
        try:
            await poll_intercom()
        except Exception as e:
            log.error("intercom loop error: %s", e)
        await asyncio.sleep(INTERCOM_POLL_INTERVAL)


# ---------------------------------------------------------------------------
# MQTT Auto-Discovery
# ---------------------------------------------------------------------------
async def mqtt_publish(topic: str, payload, retain=False):
    if not MQTT_HOST:
        return
    await mqtt_out.put((topic, payload, retain))


async def publish_discovery(client, flat: str):
    root = f"lanta/{flat}"
    uid = f"lanta_intercom_{flat}"
    dev = {
        "identifiers": [uid],
        "name": "Домофон LanTa",
        "manufacturer": "LanTa (SmartYard)",
        "model": "Domophone Proxy v2.1.0",
    }

    def sw(name, entity, icon):
        return {
            "name": name,
            "unique_id": f"{uid}_{entity}",
            "device": dev,
            "command_topic": f"{root}/switch/{entity}/set",
            "state_topic": f"{root}/switch/{entity}/state",
            "payload_on": "ON", "payload_off": "OFF",
            "state_on": "ON", "state_off": "OFF",
            "icon": icon,
        }

    def bin_(name, entity, devclass, icon=None):
        c = {
            "name": name,
            "unique_id": f"{uid}_{entity}",
            "device": dev,
            "state_topic": f"{root}/binary_sensor/{entity}/state",
            "payload_on": "ON", "payload_off": "OFF",
            "device_class": devclass,
        }
        if icon:
            c["icon"] = icon
        return c

    configs = {
        f"homeassistant/switch/{uid}/guest_access/config": sw("Гостевой доступ (1 час)", "guest_access", "mdi:account-clock-outline"),
        f"homeassistant/switch/{uid}/cms/config": sw("Трубка в квартире (КМС)", "cms", "mdi:phone-classic"),
        f"homeassistant/switch/{uid}/voip/config": sw("Видеозвонки в приложение", "voip", "mdi:video"),
        f"homeassistant/switch/{uid}/allow_door_code/config": sw("Вход по коду", "allow_door_code", "mdi:numeric"),
        f"homeassistant/sensor/{uid}/last_event/config": {
            "name": "Последнее событие домофона",
            "unique_id": f"{uid}_last_event",
            "device": dev,
            "state_topic": f"{root}/sensor/last_event/state",
            "json_attributes_topic": f"{root}/sensor/last_event/attributes",
            "icon": "mdi:doorbell",
        },
        f"homeassistant/binary_sensor/{uid}/door_code/config": bin_("Открытие по коду", "door_code", "opening", "mdi:door-open"),
        f"homeassistant/binary_sensor/{uid}/call/config": bin_("Звонок в домофон", "call", "sound", "mdi:phone-ring"),
        f"homeassistant/binary_sensor/{uid}/token/config": bin_("Статус токена LanTa", "token", "problem"),
        f"homeassistant/camera/{uid}/last_frame/config": {
            "name": "Последний кадр домофона",
            "unique_id": f"{uid}_last_frame",
            "device": dev,
            "topic": f"{root}/camera/last_frame",
        },
    }
    for topic, cfg in configs.items():
        await client.publish(topic, json.dumps(cfg, ensure_ascii=False), qos=1, retain=True)
    log.info("MQTT discovery published (%s entities) for flat %s", len(configs), flat)


async def handle_command(topic: str, payload):
    flat = flat_id_cache or await get_flat_id_cached()
    if not flat:
        return
    p = ""
    if isinstance(payload, (bytes, bytearray)):
        p = payload.decode("utf-8", "replace").strip().upper()
    else:
        p = str(payload).strip().upper()
    on = p in ("ON", "1", "TRUE", "OPEN")

    if topic.endswith("/switch/guest_access/set"):
        now = datetime.now(MSK)
        target = now + (timedelta(hours=1) if on else timedelta(seconds=-5))
        await set_intercom("autoOpen", target.strftime("%Y-%m-%d %H:%M:%S"))
        await poll_intercom()
    elif topic.endswith("/switch/cms/set"):
        await set_intercom("CMS", "t" if on else "f")
        await poll_intercom()
    elif topic.endswith("/switch/voip/set"):
        await set_intercom("VoIP", "t" if on else "f")
        await poll_intercom()
    elif topic.endswith("/switch/allow_door_code/set"):
        await set_intercom("allowDoorCode", "t" if on else "f")
        await poll_intercom()


async def mqtt_publisher(client):
    while True:
        topic, payload, retain = await mqtt_out.get()
        try:
            await client.publish(topic, payload, qos=1, retain=retain)
        except aiomqtt.MqttError:
            # вернуть сообщение в очередь и переподключиться
            await mqtt_out.put((topic, payload, retain))
            raise


async def mqtt_receiver(client):
    async for message in client.messages:
        try:
            topic = getattr(message.topic, "value", message.topic)
            await handle_command(str(topic), message.payload)
        except Exception as e:
            log.warning("MQTT command error: %s", e)


async def discovery_repeater(client):
    while True:
        await asyncio.sleep(60)
        flat = await get_flat_id_cached()
        if flat:
            try:
                await publish_discovery(client, flat)
            except Exception as e:
                log.warning("discovery republish error: %s", e)


async def mqtt_loop():
    global mqtt_client, _mqtt_states_inited
    if not MQTT_HOST:
        log.info("MQTT not configured; Auto-Discovery disabled")
        return
    while True:
        try:
            client = aiomqtt.Client(
                hostname=MQTT_HOST,
                port=MQTT_PORT,
                username=MQTT_USER or None,
                password=MQTT_PASS or None,
            )
            async with client:
                mqtt_client = client
                log.info("MQTT connected to %s:%s", MQTT_HOST, MQTT_PORT)
                flat = await get_flat_id_cached()
                if flat:
                    await publish_discovery(client, flat)
                    if not _mqtt_states_inited:
                        root = f"lanta/{flat}"
                        await client.publish(f"{root}/binary_sensor/door_code/state", "OFF", qos=1, retain=True)
                        await client.publish(f"{root}/binary_sensor/call/state", "OFF", qos=1, retain=True)
                        await poll_intercom()
                        _mqtt_states_inited = True
                    await client.subscribe("lanta/+/switch/+/set")
                else:
                    log.warning("flat_id unknown; MQTT discovery postponed")
                pub = asyncio.create_task(mqtt_publisher(client))
                recv = asyncio.create_task(mqtt_receiver(client))
                rep = asyncio.create_task(discovery_repeater(client))
                try:
                    await asyncio.gather(pub, recv, rep)
                finally:
                    for t in (pub, recv, rep):
                        if not t.done():
                            t.cancel()
        except aiomqtt.MqttError as e:
            log.error("MQTT connection error: %s", e)
        except Exception as e:
            log.error("MQTT loop error: %s", e)
        mqtt_client = None
        await asyncio.sleep(5)


# ---------------------------------------------------------------------------
# Отладочные HTTP-эндпоинты
# ---------------------------------------------------------------------------
async def door_events(request):
    return web.json_response({
        "flat_id": flat_id_cache,
        "settings": door_state.get("settings"),
        "last_event": door_state.get("last_event"),
        "events": door_state.get("events", []),
        "people": people,
        "token_ok": bool(state.get("token")),
        "token_status": token_status,
    })


async def event_preview(request):
    if door_state.get("last_preview_bytes"):
        return web.Response(body=door_state["last_preview_bytes"], content_type="image/jpeg")
    url = door_state.get("last_preview_url")
    if url:
        try:
            s = await http_sess()
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                return web.Response(body=await r.read(), content_type=r.headers.get("Content-Type", "image/jpeg"))
        except Exception as e:
            log.warning("event_preview error: %s", e)
    return web.Response(status=404)


# ---------------------------------------------------------------------------
# Запуск
# ---------------------------------------------------------------------------
async def on_start(app):
    app["bg"] = asyncio.create_task(bg_loop())
    app["plog"] = asyncio.create_task(plog_loop())
    app["intercom"] = asyncio.create_task(intercom_loop())
    app["mqtt"] = asyncio.create_task(mqtt_loop())
    asyncio.create_task(get_flat_id_cached())


async def on_stop(app):
    for name in ("bg", "plog", "intercom", "mqtt"):
        t = app.get(name)
        if t and not t.done():
            t.cancel()
    for s in (_api_session, _stream_session):
        if s and not s.closed:
            await s.close()


app = web.Application()
app.add_routes([
    web.get("/", list_cams),
    web.get("/cameras", list_cams),
    web.get("/go2rtc.yaml", go2rtc_cfg),
    web.get("/auth", auth_ui),
    web.post("/auth/req", auth_req),
    web.post("/auth/chk", auth_chk),
    web.post("/auth/tok", auth_tok),
    web.get("/debug_api", debug_api),
    web.post("/refresh", force_ref),
    web.get("/guest_access", guest_access),
    web.post("/guest_access", guest_access),
    web.get("/preview/{cam_id}", proxy_preview),
    web.get("/ts/{cam_id}", proxy_ts),
    web.get("/stream/{cam_id}/{path:.+}", proxy_stream),
    web.get("/people", people_page),
    web.post("/people/save", people_save),
    web.get("/face_img/{face_id}", face_img),
    web.get("/door_events", door_events),
    web.get("/event_preview", event_preview),
])
app.on_startup.append(on_start)
app.on_cleanup.append(on_stop)

if __name__ == "__main__":
    log.info("Starting Lanta Camera Proxy v2.1.0 on port %d", PORT)
    web.run_app(app, host="0.0.0.0", port=PORT)
