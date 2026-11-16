import asyncio, json, logging, os, re, ssl, time, uuid
import aiohttp
from aiohttp import web

FFMPEG = os.environ.get("FFMPEG_PATH", "ffmpeg")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("lanta-proxy")

ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE

API_BASE = "https://lanta-net.ru"
PHONE_CFG = os.environ.get("LANTA_PHONE", "").strip()
TOKEN_CFG = os.environ.get("LANTA_TOKEN", "").strip()
REFRESH_INT = int(os.environ.get("TOKEN_REFRESH_INTERVAL", "5400"))
PORT = int(os.environ.get("PORT", "8199"))
STATE_FILE = "/data/lanta_auth.json"

cameras = {}
last_refresh = 0
_session = None


def norm_phone(p: str) -> str:
    d = "".join(c for c in (p or "") if c.isdigit())
    if len(d) == 11 and d.startswith("8"):
        return "7" + d[1:]
    return ("7" + d) if len(d) == 10 else d


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


async def http_sess() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=ssl_ctx),
            headers={
                "Origin": API_BASE,
                "Referer": f"{API_BASE}/",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/130.0.0.0",
            },
        )
    return _session


async def api_post(path: str, payload: dict | None = None):
    tok = state.get("token", "").strip()
    hdr = {
        "Authorization": f"Bearer {tok}" if tok else "Bearer",
        "Content-Type": "application/json",
    }
    s = await http_sess()
    async with s.post(f"{API_BASE}{path}", headers=hdr, json=payload or {}) as r:
        txt = await r.text()
        try:
            return r.status, (json.loads(txt) if txt else {})
        except Exception:
            return r.status, {"raw": txt}
def extract_cams(node, prefix="", out=None):
    if out is None:
        out = []
    if isinstance(node, list):
        for x in node:
            extract_cams(x, prefix, out)
    elif isinstance(node, dict):
        u = node.get("url") or node.get("hls") or node.get("link_video")
        if isinstance(u, str) and u.startswith("http"):
            cid = node.get("id") or node.get("cameraId")
            nm = node.get("name") or node.get("title") or f"Cam {cid}"
            prv = node.get("shot") or node.get("preview") or node.get("screenshot") or ""
            out.append({
                "id": cid,
                "title": f"{prefix} {nm}".strip(),
                "video": u,
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
        for ep in ("/api/address/getAddressList", "/api/address/getSettingsList"):
            st, d = await api_post(ep)
            if st == 401:
                log.error("Token unauthorized (401). Update token at /auth.")
                return
            if st == 200 and isinstance(d, dict) and isinstance(d.get("data"), list):
                hids = [a.get("houseId") for a in d["data"] if a.get("houseId")]
                if hids:
                    break
        if hids:
            for hid in dict.fromkeys(hids):
                for ep in ("/api/cctv/all", "/api/cctv/allTree"):
                    st, cd = await api_post(ep, {"houseId": hid})
                    if st == 200 and cd:
                        c = extract_cams(cd)
                        if c:
                            found.extend(c)
                            break
        if not found:
            for ep in ("/api/cctv/all", "/api/cctv/allTree", "/api/cctv/overview"):
                st, cd = await api_post(ep)
                if st == 200 and cd:
                    found.extend(extract_cams(cd))
        if not found:
            log.warning("0 cameras returned. Open /debug_api to inspect response.")
            return

        new_cams, used = {}, set()
        for i, it in enumerate(found, 1):
            try:
                cid = int(it["id"])
            except Exception:
                cid = i
            while cid in used:
                cid += 10000
            used.add(cid)
            new_cams[cid] = {
                "title": it["title"],
                "link_video": it["video"],
                "preview_url": it["preview"],
            }
        cameras = new_cams
        last_refresh = time.time()
        log.info("Loaded %d cameras", len(cameras))
    except Exception as e:
        log.error("Refresh error: %s", e)


async def bg_loop():
    while True:
        await refresh_cameras()
        await asyncio.sleep(REFRESH_INT)


async def auth_ui(request):
    tok_ok = "ДА" if state.get("token") else "НЕТ"
    ph = state.get("phone", "")
    msg = request.query.get("msg", "")
    lines = [
        '<!doctype html><meta charset="utf-8"><title>LanTa Auth</title>',
        '<body style="font-family:sans-serif;max-width:520px;margin:30px auto">',
        f"<h3>LanTa Proxy | Токен: {tok_ok} | Камер: {len(cameras)}</h3>",
        '<p><a href="/cameras">/cameras</a> | <a href="/go2rtc.yaml">/go2rtc.yaml</a> | <a href="/debug_api">/debug_api</a></p>',
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
        await refresh_cameras()
        raise web.HTTPFound(f"/auth?msg=Токен_сохранен_Камер_{len(cameras)}")
    raise web.HTTPFound(f"/auth?msg=Ошибка_{st}_Проверьте_звонок")


async def auth_tok(request):
    b = await _read_input(request)
    tok = (b.get("token") or "").replace("Bearer ", "").strip()
    if tok:
        state["token"] = tok
        save_state(state)
        await refresh_cameras()
        raise web.HTTPFound(f"/auth?msg=Токен_сохранен_Камер_{len(cameras)}")
    raise web.HTTPFound("/auth?msg=Пустой_токен")
async def debug_api(request):
    res = {}
    eps = (
        "/api/address/getAddressList",
        "/api/cctv/all",
        "/api/cctv/allTree",
        "/api/cctv/overview",
    )
    for ep in eps:
        st, d = await api_post(ep)
        res[ep] = {"status": st, "data": d}
    return web.json_response(res)


async def list_cams(request):
    h = request.headers.get("Host", f"0.0.0.0:{PORT}")
    res = []
    for k, v in cameras.items():
        res.append({
            "id": k,
            "title": v["title"],
            "stream": f"http://{h}/stream/{k}/index.fmp4.m3u8",
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
    path = request.match_info.get("path", "index.fmp4.m3u8")
    if cid not in cameras:
        return web.Response(status=404, text="Not found")
    u_path, _, qs = cameras[cid]["link_video"].partition("?")
    base = u_path.rsplit("/", 1)[0]
    url = f"{base}/{path}?{qs}" if qs else f"{base}/{path}"
    s = await http_sess()
    async with s.get(url) as r:
        if r.status == 403:
            await refresh_cameras()
            if cid not in cameras:
                return web.Response(status=404, text="Lost after refresh")
            u_path, _, qs = cameras[cid]["link_video"].partition("?")
            base = u_path.rsplit("/", 1)[0]
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
    hdr = {}
    if cam["preview_url"].startswith(API_BASE):
        hdr = {"Authorization": f"Bearer {state['token']}"}
    async with s.get(cam["preview_url"], headers=hdr) as r:
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


async def on_start(app):
    app["bg"] = asyncio.create_task(bg_loop())


async def on_stop(app):
    if _session and not _session.closed:
        await _session.close()


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
    web.get("/preview/{cam_id}", proxy_preview),
    web.get("/ts/{cam_id}", proxy_ts),
    web.get("/stream/{cam_id}/{path:.+}", proxy_stream),
])
app.on_startup.append(on_start)
app.on_cleanup.append(on_stop)

if __name__ == "__main__":
    log.info("Starting Lanta Camera Proxy v2.0 on port %d", PORT)
    web.run_app(app, host="0.0.0.0", port=PORT)
