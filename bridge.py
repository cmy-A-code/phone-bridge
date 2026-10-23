#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Chatbox phone bridge (bridge) v0.1 — self-hosted

链路: 手机(Chatbox App 自定义模型) -> 本机桥(OpenAI 兼容接口) -> 注入桌面 Chatbox「桥会话」
      -> 会话内 Agent(带全套工具)执行 -> 自报 POST /v1/reply/<id> -> 结果原路回给手机。

安全: 默认仅绑定 127.0.0.1；除 /health 外全部要求 Bearer token；所有任务留档 bridge/tasks/。
状态: v0.1 骨架（服务端）。注入器 inject.ps1 尚未标定，chat 请求会返回"注入器未就绪"。
"""
import base64, json, mimetypes, os, queue, re, secrets, select, shutil, socket, subprocess, sys, threading, time, urllib.parse, urllib.request

try:
    import websocket  # websocket-client (CDP)
    HAS_WS = True
except Exception:
    HAS_WS = False
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BASE = os.path.dirname(os.path.abspath(__file__))
CONF_PATH = os.path.join(BASE, "config.json")
LOG_PATH = os.path.join(BASE, "bridge.log")
TASKS_DIR = os.path.join(BASE, "tasks")

DEFAULTS = {
    "host": "127.0.0.1",
    "port": 8780,
    "token": "",                    # 空 = 首次启动自动生成并写回 config.json
    "conv_name": "phone-task",
    "conv_id": "",
    "inject_mode": "cdp",
    "inject_script": "inject.ps1",  # 相对本目录；由桥以 powershell -File 调用
    "inject_timeout_s": 150,
    "reply_timeout_s": 900,
    "public_base": "",              # https tunnel prefix (e.g. https://xxx.trycloudflare.com); empty = auto-detect local ngrok API
    "media_ttl_days": 14,           # /media 文件保留天数，过期自动清理
    "dsh_node": "",                 # optional: node.exe path for the DSH engine (empty = disabled)
    "dsh_bin": "",                  # optional: DSH bin.js path
    "dsh_home": "",
    "dsh_cwd": "",
}

TEMPLATE = """【手机任务 #{tid}】手机端发来一条任务（用户不在电脑前），请在电脑上执行，完成后把最终答复正文回传给桥。

任务内容：
{text}

回传方式（必须执行，桥在等这个信号）：
1) 用 run_command 把答复正文 POST 到 http://127.0.0.1:{port}/v1/reply/{tid}（UTF-8 纯文本）：
   Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:{port}/v1/reply/{tid}' -Body '<答复正文>' -ContentType 'text/plain; charset=utf-8'
   若正文含引号/换行易出转义问题：先把正文写入临时文件，再用 -InFile '<文件路径>' 发送。
2) 任务较长（预计超过约 30 秒或分多步）时，请随手发进度，手机端可实时看到（每完成一步发一条，一行字即可）：
   Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:{port}/v1/progress/{tid}' -Body '<例如：已查到资料，正在整理>' -ContentType 'text/plain; charset=utf-8'
3) 聊天里可简短回复一两句（方便旁观查看），最终答复以 POST 内容为准。

【发图/文件给手机（不用微信）】一行命令把文件发布成公网链接，写进回传正文即可（手机点开即看，图片直接预览）：
   powershell -NoProfile -ExecutionPolicy Bypass -File send-media.ps1 -Path '<path-to-file>'

【快反模式】用户可能随时发任务，请按此节奏：
- 先动起来：第一个动作就发一行进度（/v1/progress），一句话报“收到＋正在做什么”。
- 分级响应：简单操作类（截图/发文件/查一项/开关某物）→ 少分析、快执行、结果精简（几行内）；复杂任务 → 先回“预计需要几分钟”，再扎实完成。
- 少铺垫、少客套、结论先行；不展示冗长思考。但“快”绝不牺牲准确与完整：该查证的要查、该用工具的要调用、不确定就说明。

注意：
- 需要 GUI 操作就照常走你的安全流程（公告/徽标/收尾）；若预检提示已有守候在运行，等几秒重试。
- 破坏性或对外不可逆操作不要直接做，先回传说明并询问。
- 来源区分：带【手机任务 #编号】标记的消息＝来自手机（需回传）；本会话里没有该标记的消息＝电脑本地用户直接发言，按普通对话处理，不要回传。
"""


def log(msg):
    line = time.strftime("%Y-%m-%d %H:%M:%S") + " " + msg
    try:
        print(line, flush=True)
    except Exception:
        pass
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def load_conf():
    conf = dict(DEFAULTS)
    if os.path.exists(CONF_PATH):
        try:
            with open(CONF_PATH, "r", encoding="utf-8") as f:
                conf.update(json.load(f))
        except Exception as e:
            log("config.json 读取失败，用默认值: %s" % e)
    if not conf.get("token"):
        conf["token"] = secrets.token_hex(16)
        with open(CONF_PATH, "w", encoding="utf-8") as f:
            json.dump(conf, f, ensure_ascii=False, indent=2)
        log("已生成新 token 并写入 config.json")
    return conf


class Task:
    def __init__(self, tid, text, target=""):
        self.id = tid
        self.text = text
        self.target = target
        self.done = threading.Event()
        self.reply = None
        self.error = None
        self.created = time.time()
        self.waiters = 0        # 当前正在等待此任务的活跃连接数
        self.stop_timer = None  # 断开后延迟评估的 Timer
        self.cancelled = False  # 已被判定放弃（未注入时阻止注入）
        self.injected = False   # 是否已成功注入到电脑端
        self.delivered = 0      # 有连接成功写出回复的次数（用于未送达判定）
        self.deadline_timer = None  # 总超时兜底 Timer
        self.progress = []      # 可见进度行（等待期间发给客户端，避免只转圈）
        self.t_inject_done = None   # 注入完成时间
        self.live = []          # 结构化活动流（reasoning/tool/text），SSE 实时推给手机
        self.engine = "chatbox" # "chatbox" | "dsh"
        self.dsh_sid = None     # DSH sessionId（跨轮续跑）
        self.dsh_mode = "headless"  # "headless" | "acp"
        self.fingerprint = ""   # 手机对话指纹（映射 DSH 会话）


CONF = load_conf()
TASKS = {}
TASK_Q = queue.Queue()
LAST_ACTIVITY = time.time()
STARTED_AT = time.time()


def probe_port(port, timeout=0.4):
    """快速探测本机端口是否有进程在监听（用于 /health 报告 9222 状态）。"""
    try:
        s = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        s.close()
        return True
    except Exception:
        return False


# ---------------- 媒体直发通道（v0.7）：/media/<mid> ----------------
# 把电脑上的文件发布成公网链接（手机直接点开/预览），绕开微信等桌面通道。
# 存储：MEDIA_DIR/<mid(32hex)>__<原文件名>；mid=128bit 随机，不可枚举=天然访问控制。
MEDIA_DIR = os.path.join(BASE, "media")
MEDIA_MIME = {
    ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".gif": "image/gif",
    ".webp": "image/webp", ".bmp": "image/bmp", ".svg": "image/svg+xml",
    ".mp4": "video/mp4", ".webm": "video/webm", ".mov": "video/quicktime",
    ".mp3": "audio/mpeg", ".m4a": "audio/mp4", ".wav": "audio/wav",
    ".pdf": "application/pdf", ".txt": "text/plain; charset=utf-8",
    ".md": "text/markdown; charset=utf-8", ".json": "application/json; charset=utf-8",
    ".csv": "text/csv; charset=utf-8", ".zip": "application/zip",
    ".apk": "application/vnd.android.package-archive",
}
_PUBLIC_BASE_CACHE = {"v": "", "t": 0.0}


def _safe_media_name(name):
    name = os.path.basename(str(name or "")).strip() or "file.bin"
    name = re.sub(r'[\r\n\t"<>|:*?]+', "_", name)
    if len(name) > 120:
        stem, ext = os.path.splitext(name)
        name = stem[:100] + ext
    return name


def _media_mime(name):
    ext = os.path.splitext(name)[1].lower()
    if ext in MEDIA_MIME:
        return MEDIA_MIME[ext]
    mt, _ = mimetypes.guess_type(name)
    return mt or "application/octet-stream"


def _media_find(mid):
    try:
        pre = mid + "__"
        for fn in os.listdir(MEDIA_DIR):
            if fn.startswith(pre):
                return os.path.join(MEDIA_DIR, fn)
    except Exception:
        pass
    return None


def _media_cleanup():
    ttl = int(CONF.get("media_ttl_days", 14)) * 86400
    now = time.time()
    n = 0
    try:
        for fn in os.listdir(MEDIA_DIR):
            p = os.path.join(MEDIA_DIR, fn)
            try:
                if os.path.isfile(p) and now - os.path.getmtime(p) > ttl:
                    os.remove(p)
                    n += 1
            except Exception:
                pass
    except Exception:
        pass
    if n:
        log("MEDIA cleanup removed %d expired file(s)" % n)


def _public_base():
    """解析公网前缀：config.public_base 优先；否则查本机 ngrok API（缓存 2 分钟）。"""
    pb = (CONF.get("public_base") or "").strip().rstrip("/")
    if pb:
        return pb
    now = time.time()
    if _PUBLIC_BASE_CACHE["v"] and now - _PUBLIC_BASE_CACHE["t"] < 120:
        return _PUBLIC_BASE_CACHE["v"]
    v = ""
    try:
        with urllib.request.urlopen("http://127.0.0.1:4040/api/tunnels", timeout=1.5) as r:
            data = json.loads(r.read().decode("utf-8", "replace"))
        for t in data.get("tunnels", []):
            pu = str(t.get("public_url") or "")
            if pu.startswith("https://"):
                v = pu.rstrip("/")
                break
    except Exception:
        v = ""
    _PUBLIC_BASE_CACHE["v"] = v
    _PUBLIC_BASE_CACHE["t"] = now
    return v


def _media_publish(path=None, data=None, name=None):
    mid = secrets.token_hex(16)
    if path is not None:
        if not os.path.isfile(path):
            raise ValueError("file not found: %s" % path)
        src_name = name or os.path.basename(path)
        dst = os.path.join(MEDIA_DIR, mid + "__" + _safe_media_name(src_name))
        shutil.copyfile(path, dst)
    elif data is not None:
        dst = os.path.join(MEDIA_DIR, mid + "__" + _safe_media_name(name or "file.bin"))
        with open(dst, "wb") as f:
            f.write(data)
    else:
        raise ValueError("no path or data")
    sz = os.path.getsize(dst)
    rel = "/media/" + mid
    pb = _public_base()
    return {"ok": True, "mid": mid, "name": os.path.basename(dst).split("__", 1)[-1],
            "size": sz, "url": (pb + rel) if pb else "",
            "local_url": "http://127.0.0.1:%d%s" % (CONF["port"], rel)}


def _media_list(limit=50):
    rows = []
    try:
        for fn in os.listdir(MEDIA_DIR):
            p = os.path.join(MEDIA_DIR, fn)
            if "__" not in fn or not os.path.isfile(p):
                continue
            mid, _, name = fn.partition("__")
            try:
                rows.append({"mid": mid, "name": name, "size": os.path.getsize(p),
                             "mtime": int(os.path.getmtime(p))})
            except Exception:
                pass
    except Exception:
        pass
    rows.sort(key=lambda r: r["mtime"], reverse=True)
    pb = _public_base()
    for r in rows:
        r["url"] = (pb + "/media/" + r["mid"]) if pb else ""
    return rows[:limit]

# 未送达回复队列（客户端断开导致没写出去的回复，下次请求时补发；持久化，重启不丢）
HEARTBEAT_S = int(CONF.get("heartbeat_s", 15))              # 可见心跳间隔（秒）
DEDUP_WINDOW_S = int(CONF.get("dedup_window_s", 600))       # 同内容重试挂靠窗口（秒）
DONE_WINDOW_S = int(CONF.get("done_window_s", 1800))        # 已完成任务同内容再问 → 再次送达窗口（秒）
UNDELIVERED_FILE = os.path.join(BASE, "undelivered.json")


def _save_undelivered():
    try:
        with open(UNDELIVERED_FILE, "w", encoding="utf-8") as f:
            json.dump(UNDELIVERED, f, ensure_ascii=False, indent=1)
    except Exception:
        pass


def _load_undelivered():
    try:
        if os.path.exists(UNDELIVERED_FILE):
            data = json.load(open(UNDELIVERED_FILE, encoding="utf-8"))
            if isinstance(data, list):
                now = time.time()
                return [u for u in data if isinstance(u, dict) and now - u.get("ts", 0) < 86400][-10:]
    except Exception:
        pass
    return []


UNDELIVERED = _load_undelivered()
U_LOCK = threading.Lock()

LOST_GRACE_S = int(CONF.get("lost_grace_s", 45))            # 断开后评估宽限（秒）
UNDELIVER_CHECK_S = int(CONF.get("undeliver_check_s", 10))  # 完成后多久判定"未送达"


def norm_text(t):
    """规范化任务文本（按行去空白去重排序）——识别 App 重试合并等场景下的“同内容”。"""
    lines = sorted(set(x.strip() for x in (t or "").splitlines() if x.strip()))
    return "\n".join(lines)


def _client_closed(conn):
    """True if the peer connection is closed (RST or FIN)。用 select 探测（loopback/隧道环境可靠）。"""
    try:
        r, _, _ = select.select([conn], [], [], 0)
        if not r:
            return False
        try:
            d = conn.recv(1, socket.MSG_PEEK)
            return d == b""      # FIN
        except Exception:
            return True          # RST / error
    except Exception:
        return True


def _client_rst(conn):
    """True only for hard errors (RST)。FIN（正常读完关闭）不算——数据已写全。"""
    try:
        r, _, _ = select.select([conn], [], [], 0)
        if not r:
            return False
        try:
            conn.recv(1, socket.MSG_PEEK)
            return False         # data 或 FIN
        except Exception:
            return True          # RST
    except Exception:
        return True


def _queue_undelivered(task):
    """任务完成后延迟评估：若没有任何连接确认收到回复 → 进入补发队列。"""
    try:
        if not task.reply or task.delivered > 0:
            return
        with U_LOCK:
            now = time.time()
            UNDELIVERED[:] = [u for u in UNDELIVERED if now - u["ts"] < 86400]
            if len(UNDELIVERED) >= 10:
                UNDELIVERED.pop(0)
            if not any(u["id"] == task.id for u in UNDELIVERED):
                UNDELIVERED.append({"id": task.id, "text": task.text,
                                    "reply": task.reply, "ts": now})
                log("UNDELIVERED + %s（无连接确认接收，进入补发队列）" % task.id)
                _save_undelivered()
    except Exception as e:
        log("_queue_undelivered err: %r" % (e,))


def take_pending(limit=3):
    """取待补发列表快照（不删除）。"""
    with U_LOCK:
        now = time.time()
        UNDELIVERED[:] = [u for u in UNDELIVERED if now - u["ts"] < 86400]
        return list(UNDELIVERED[:limit])


def drop_pending(ids):
    """从待补发列表移除（已确认写出）。"""
    with U_LOCK:
        UNDELIVERED[:] = [u for u in UNDELIVERED if u["id"] not in ids]
    _save_undelivered()



# ---------------- DSH 通道（B=headless 一次性；C=ACP 常驻） ----------------
DSH_NODE = CONF.get("dsh_node", "")
DSH_BIN = CONF.get("dsh_bin", "")
DSH_HOME = CONF.get("dsh_home", "")
DSH_CWD = CONF.get("dsh_cwd", "")
DSH_SESSIONS = os.path.join(BASE, "dsh-sessions.json")
_DSH_SESS_LOCK = threading.Lock()


def _dsh_sessions_load():
    try:
        with open(DSH_SESSIONS, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _dsh_sessions_save(m):
    try:
        tmp = DSH_SESSIONS + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(m, f, ensure_ascii=False, indent=1)
        os.replace(tmp, DSH_SESSIONS)
    except Exception as e:
        log("DSH 会话映射写入失败: %s" % e)


def _dsh_fingerprint(messages):
    """手机对话指纹：取第一条 user 消息（同一对话内稳定）。"""
    try:
        first = ""
        for m in messages or []:
            if isinstance(m, dict) and m.get("role") == "user":
                c = m.get("content")
                first = c if isinstance(c, str) else json.dumps(c, ensure_ascii=False)
                break
        import hashlib
        return hashlib.sha1((first or "").encode("utf-8")).hexdigest()[:16]
    except Exception:
        return ""


def _drain_stderr(p, task):
    try:
        for line in p.stderr:
            line = (line or "").strip()
            if line and ("error" in line.lower() or "fatal" in line.lower()):
                log("DSH-STDERR #%s %s" % (task.id, line[:300]))
    except Exception:
        pass


def run_dsh_headless(task):
    """B 方案：dsh --profile headless --json，逐行解析事件并写入 task.live。"""
    global LAST_ACTIVITY
    fp = getattr(task, "fingerprint", "")
    sid = None
    if fp:
        with _DSH_SESS_LOCK:
            sid = _dsh_sessions_load().get(fp)
    cmd = [DSH_NODE, DSH_BIN, "--profile", "headless", "--json"]
    if sid:
        cmd += ["--session-id", sid]
    cmd.append(task.text)
    env = dict(os.environ)
    env["DSH_HOME"] = DSH_HOME
    try:
        os.makedirs(DSH_CWD, exist_ok=True)
    except Exception:
        pass
    log("DSH-HEADLESS #%s start sid=%s fp=%s" % (task.id, sid, fp))
    task.live.append({"kind": "tool", "text": "DSH 已启动（%s）" % ("续跑会话 " + sid[-8:] if sid else "新会话")})
    try:
        p = subprocess.Popen(cmd, cwd=DSH_CWD, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             text=True, encoding="utf-8", errors="replace", bufsize=1,
                             creationflags=subprocess.CREATE_NO_WINDOW)
    except Exception as e:
        task.error = "DSH 启动失败：%s" % e
        finish_task(task)
        return False
    threading.Thread(target=_drain_stderr, args=(p, task), daemon=True).start()
    final_text = None
    got_session = None
    try:
        for line in p.stdout:
            line = (line or "").strip()
            if not line.startswith("{"):
                continue
            try:
                ev = json.loads(line)
            except Exception:
                continue
            typ = ev.get("type")
            if typ == "session":
                got_session = ev.get("sessionId")
                task.dsh_sid = got_session
            elif typ == "thinking":
                txt = (ev.get("text") or "").strip()
                if txt:
                    task.live.append({"kind": "reasoning", "text": txt})
            elif typ == "tool_call":
                name = ev.get("tool") or "tool"
                inp = ev.get("input") or {}
                desc = ""
                if isinstance(inp, dict):
                    desc = str(inp.get("description") or inp.get("command") or inp.get("path") or "")[:180]
                task.live.append({"kind": "tool", "text": "%s%s" % (name, ("：" + desc) if desc else "")})
            elif typ == "tool_result":
                st = ev.get("status") or "done"
                res = ev.get("result")
                res = res.strip().replace("\n", " ")[:120] if isinstance(res, str) else ""
                task.live.append({"kind": "tool_result", "text": "%s%s" % (st, (" · " + res) if res else "")})
            elif typ == "text":
                txt = (ev.get("text") or "").strip()
                if txt:
                    task.live.append({"kind": "text", "text": txt})
            elif typ == "final":
                final_text = ev.get("text")
        try:
            p.wait(timeout=20)
        except Exception:
            pass
    except Exception as e:
        task.error = "DSH 执行异常：%s" % e
    if got_session and fp:
        with _DSH_SESS_LOCK:
            smap = _dsh_sessions_load()
            smap[fp] = got_session
            _dsh_sessions_save(smap)
    if final_text:
        task.reply = final_text
        finish_task(task)
        return True
    if not task.error:
        task.error = "DSH 未返回最终答复（rc=%s）" % getattr(p, "returncode", "?")
    finish_task(task)
    return False


def run_dsh_acp(task):
    """C 方案：调用常驻 ACP 服务（127.0.0.1:8791），把 SSE 事件写入 task.live。"""
    global LAST_ACTIVITY
    payload = json.dumps({"text": task.text}, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request("http://127.0.0.1:8791/prompt", data=payload,
                                 headers={"Content-Type": "application/json"})
    task.live.append({"kind": "tool", "text": "DSH ACP 常驻通道已连接（热启动）"})
    try:
        resp = urllib.request.urlopen(req, timeout=CONF["reply_timeout_s"] + 60)
    except Exception as e:
        task.error = "ACP 服务不可用（%s）：请确认 dsh-acp-agent 已启动" % e
        finish_task(task)
        return False
    final = None
    try:
        for raw in resp:
            line = raw.decode("utf-8", "replace").strip()
            if not line.startswith("data:"):
                continue
            data = line[5:].strip()
            if data == "[DONE]":
                break
            try:
                ev = json.loads(data)
            except Exception:
                continue
            typ = ev.get("type")
            if typ == "reasoning":
                txt = (ev.get("text") or "").strip()
                if txt:
                    task.live.append({"kind": "reasoning", "text": txt})
            elif typ == "tool":
                nm = ev.get("name") or "tool"
                st = ev.get("status") or ""
                task.live.append({"kind": "tool", "text": ("%s（%s）" % (nm, st)) if st else nm})
            elif typ == "tool_update":
                st = ev.get("status") or ""
                if st:
                    task.live.append({"kind": "tool_result", "text": st})
            elif typ == "text":
                txt = ev.get("text") or ""
                if txt.strip():
                    task.live.append({"kind": "text", "text": txt})
            elif typ == "final":
                final = ev.get("text")
            elif typ == "error":
                task.error = "ACP 错误：%s" % ev.get("text")
    except Exception as e:
        task.error = "ACP 流中断：%s" % e
    try:
        resp.close()
    except Exception:
        pass
    LAST_ACTIVITY = time.time()
    if final:
        task.reply = final
        finish_task(task)
        return True
    if not task.error:
        task.error = "ACP 未返回最终答复"
    finish_task(task)
    return False


def run_dsh(task):
    if getattr(task, "dsh_mode", "headless") == "acp":
        return run_dsh_acp(task)
    return run_dsh_headless(task)


def run_inject(task):
    """调用注入器：把任务写文件 -> inject_cdp.py 负责注入到桥会话。返回 True=注入成功。"""
    global LAST_ACTIVITY
    if getattr(task, "engine", "") == "dsh":
        return run_dsh(task)
    os.makedirs(TASKS_DIR, exist_ok=True)
    task_file = os.path.join(TASKS_DIR, task.id + ".txt")
    body = TEMPLATE.format(tid=task.id, port=CONF["port"], text=task.text)
    with open(task_file, "w", encoding="utf-8") as f:
        f.write(body)

    inject_path = CONF["inject_script"]
    if not os.path.isabs(inject_path):
        inject_path = os.path.join(BASE, inject_path)

    if CONF.get("inject_mode") == "cdp":
        cmd = [sys.executable, os.path.join(BASE, "inject_cdp.py"), task_file, task.id]
        tgt = getattr(task, "target", "")
        if tgt:
            cmd.append(tgt)
            log("INJECT(cdp) 目标会话=%s" % tgt)
        log("INJECT(cdp) 启动 task=%s" % task.id)
        ok = False
        for attempt in range(1, 4):
            out = err = ""
            try:
                p = subprocess.run(cmd, capture_output=True, timeout=CONF["inject_timeout_s"], creationflags=subprocess.CREATE_NO_WINDOW)
                out = (p.stdout or b"").decode("utf-8", "replace").strip()
                err = (p.stderr or b"").decode("utf-8", "replace").strip()
                log("INJECT(cdp) 第%d次 rc=%d out=%s err=%s" % (attempt, p.returncode, out[-400:], err[-300:]))
                if p.returncode == 0:
                    ok = True
                    if attempt > 1:
                        log("INJECT(cdp) 重试成功（第 %d 次）" % attempt)
                    break
                retryable = (p.returncode == 2) or ("refused" in (out + err).lower()) or ("timed out" in (out + err).lower())
                if (not retryable) or attempt >= 3:
                    task.error = "CDP 注入失败（rc=%d）：%s" % (p.returncode, (out or err)[-200:])
                    if retryable:
                        task.error += "（已自动重试 3 次，请约 1 分钟后重发）"
                    break
                log("INJECT(cdp) 可重试错误（第 %d 次），20 秒后重试…" % attempt)
                time.sleep(20)
            except subprocess.TimeoutExpired:
                log("INJECT(cdp) 第%d次 超时" % attempt)
                if attempt >= 3:
                    task.error = "CDP 注入超时（已自动重试 3 次，请约 1 分钟后重发）"
                else:
                    time.sleep(20)
        LAST_ACTIVITY = time.time()
        return ok

    if not os.path.exists(inject_path):
        task.error = "注入器未就绪（inject.ps1 未安装/未标定，桥仍在搭建中）"
        return False

    cmd = ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", inject_path,
           "-TaskFile", task_file, "-TaskId", task.id, "-ConvName", CONF["conv_name"]]
    log("INJECT 启动 %s" % " ".join(cmd[4:]))
    ok = True
    try:
        p = subprocess.run(cmd, capture_output=True, timeout=CONF["inject_timeout_s"], creationflags=subprocess.CREATE_NO_WINDOW)
        out = (p.stdout or b"").decode("utf-8", "replace").strip()
        err = (p.stderr or b"").decode("utf-8", "replace").strip()
        log("INJECT rc=%d out=%s err=%s" % (p.returncode, out[-800:], err[-400:]))
        if p.returncode != 0:
            task.error = "注入失败（rc=%d）：%s" % (p.returncode, (out or err)[-300:])
            ok = False
    except subprocess.TimeoutExpired:
        task.error = "注入超时（%ds）" % CONF["inject_timeout_s"]
        ok = False
    LAST_ACTIVITY = time.time()
    return ok


def cdp_stop():
    """Ask Chatbox (via CDP) to stop generating in the fixed conversation."""
    if CONF.get("stop_dry_run"):
        log("CDP-STOP (dry-run, skipped)")
        return
    if not HAS_WS:
        log("CDP-STOP skipped: websocket-client missing")
        return
    try:
        t = json.load(urllib.request.urlopen("http://127.0.0.1:9222/json", timeout=5))
        wsurl = [x for x in t if x.get("type") == "page"][0]["webSocketDebuggerUrl"]
        w = websocket.create_connection(wsurl, timeout=10, suppress_origin=True)
        w.send(json.dumps({"id": 1, "method": "Runtime.evaluate", "params": {
            "expression": """(() => { const b = [...document.querySelectorAll('button')].find(b => b.offsetParent !== null && (((b.getAttribute('aria-label')||'') + (b.innerText||'')).includes('停止'))); if (!b) return 'no-stop-button'; b.click(); return 'stopped'; })()""",
            "returnByValue": True}}))
        while True:
            m = json.loads(w.recv())
            if m.get("id") == 1:
                val = m.get("result", {}).get("result", {}).get("value")
                log("CDP-STOP result: %s" % val)
                break
        w.close()
    except Exception as e:
        log("CDP-STOP failed: %r" % (e,))


def prune_tasks():
    """清理历史任务，防止内存无限增长（保留窗口大于“再次送达”窗口）。"""
    try:
        now = time.time()
        for k in list(TASKS.keys()):
            t = TASKS.get(k)
            if t is not None and t.done.is_set() and now - t.created > 7200:
                TASKS.pop(k, None)
        if len(TASKS) > 400:
            done_old = sorted([t for t in TASKS.values() if t.done.is_set()], key=lambda x: x.created)
            for t in done_old[: len(TASKS) - 400]:
                TASKS.pop(t.id, None)
    except Exception as e:
        log("prune_tasks err: %r" % (e,))


def finish_task(task, error=None):
    """任务收尾（幂等）：设置结束标志并记录日志。"""
    if task.done.is_set():
        return
    if error and not task.error:
        task.error = error
    task.done.set()
    log("TASK %s 结束 reply=%s error=%s"
        % (task.id, (task.reply or "")[:120], task.error))
    prune_tasks()
    if task.deadline_timer is not None:
        try:
            task.deadline_timer.cancel()
        except Exception:
            pass
    if task.reply:
        # 完成后评估：没有连接成功接收 → 进入补发队列
        t = threading.Timer(UNDELIVER_CHECK_S, _queue_undelivered, args=[task])
        t.daemon = True
        t.start()


def maybe_schedule_stop(task):
    """连接断开后的延迟评估（LOST_GRACE_S 秒，容重试/重发认领）。
    保护策略：不自动停止电脑端生成（避免把还在跑的长任务误杀）；
    尚未注入的 → 取消；已注入的 → 转为异步（继续等回传，结果走补发队列）。"""
    if task.done.is_set() or task.waiters > 0 or task.stop_timer is not None:
        return
    def _check():
        task.stop_timer = None
        if task.done.is_set() or task.waiters > 0:
            return
        if not task.injected:
            log("CLIENT-LOST #%s 无重连（尚未注入）→ 按新策略继续注入（后台继续处理，不取消）" % task.id)
        else:
            log("CLIENT-LOST #%s 无重连（已注入）→ 转异步送达（不停止电脑端）" % task.id)
    task.stop_timer = threading.Timer(LOST_GRACE_S, _check)
    task.stop_timer.daemon = True
    task.stop_timer.start()
    log("CLIENT-LOST 评估已安排 #%s（%d 秒后确认）" % (task.id, LOST_GRACE_S))


def worker():
    while True:
        task = TASK_Q.get()
        try:
            ok = run_inject(task)
            if ok:
                task.injected = True
                task.t_inject_done = time.time()
                if task.done.is_set():
                    log("TASK %s 注入完成（任务已提前收尾）" % task.id)
                else:
                    task.progress.append("📨 已送达电脑端（%s），开始处理…" % (task.target or "📱手机任务"))
                    log("TASK %s 注入完成（等待回传，不阻塞队列）" % task.id)
            else:
                finish_task(task)
        except Exception as e:
            finish_task(task, "桥内部错误：%s" % e)
        finally:
            global LAST_ACTIVITY
            LAST_ACTIVITY = time.time()


def _is_true_local(handler):
    """真·本机直连：回环地址 + 无反向代理特征头（ngrok 等隧道转发必带 X-Forwarded-*）。"""
    try:
        if handler.client_address[0] not in ("127.0.0.1", "::1"):
            return False
        h = handler.headers
        for k in ("X-Forwarded-For", "X-Forwarded-Proto", "X-Forwarded-Host", "X-Real-Ip", "Ngrok-Trace-Id"):
            if h.get(k):
                return False
        return True
    except Exception:
        return False


def check_auth(handler):
    url = handler.path
    q = ""
    if "?" in url:
        q = url.split("?", 1)[1]
        url = url.split("?", 1)[0]
    if url == "/health":
        return True
    # /media/<mid> 完全公开（capability URL：mid 为 128bit 随机，不知道链接无法访问）
    if url.startswith("/media/"):
        return True
    # /v1/media/* 与 /v1/reply /v1/progress：仅"真·本机直连"免 token；经隧道转发的一律要 token
    if url.startswith("/v1/media/") or url.startswith("/v1/reply/") or url.startswith("/v1/progress/"):
        if _is_true_local(handler):
            return True
    auth = handler.headers.get("Authorization", "")
    tok = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
    if not tok:
        for kv in q.split("&"):
            if kv.startswith("token="):
                tok = kv[6:]
    return tok == CONF["token"]


def last_user_text(messages):
    if not isinstance(messages, list):
        return ""
    for m in reversed(messages):
        if isinstance(m, dict) and m.get("role") == "user":
            c = m.get("content", "")
            if isinstance(c, str):
                return c
            if isinstance(c, list):
                parts = [p.get("text", "") for p in c if isinstance(p, dict) and p.get("type") in ("text", "input_text")]
                return "\n".join(parts)
    return ""


class Server(ThreadingHTTPServer):
    def handle_error(self, request, client_address):
        e = sys.exc_info()[1]
        if isinstance(e, (ConnectionResetError, ConnectionAbortedError)):
            return
        log("SERVER-ERR %r %s" % (e, client_address))


class Handler(BaseHTTPRequestHandler):
    server_version = "ChatboxBridge/0.1"
    protocol_version = "HTTP/1.1"

    CORS_HEADERS = (
        ("Access-Control-Allow-Methods", "GET, POST, OPTIONS, HEAD"),
        ("Access-Control-Max-Age", "86400"),
    )

    def _cors(self):
        # 2026-10-18: phone App (Capacitor/WKWebView + ai-sdk runtime/browser) does CORS preflight.
        # Echo Origin (and allow credentials) so `fetch` with credentials is not rejected;
        # echo Access-Control-Request-Headers because ai-sdk may ask for x-api-key / x-stainless-* etc.
        origin = (self.headers.get("Origin") or "").strip()
        if origin:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Access-Control-Allow-Credentials", "true")
            self.send_header("Vary", "Origin")
        else:
            self.send_header("Access-Control-Allow-Origin", "*")
        req_h = (self.headers.get("Access-Control-Request-Headers") or "").strip()
        self.send_header("Access-Control-Allow-Headers", req_h or "Authorization, Content-Type, X-API-Key")
        for k, v in self.CORS_HEADERS:
            self.send_header(k, v)

    def do_OPTIONS(self):
        # 2026-10-18: CORS preflight from the phone App's WebView fetch; respond 204 + allow headers.
        log("REQ OPTIONS %s UA=%s origin=%s acrh=%s from %s" % (
            self.path, self.headers.get("User-Agent", ""), self.headers.get("Origin", ""),
            self.headers.get("Access-Control-Request-Headers", ""), self.client_address[0]))
        self.send_response(204)
        self._cors()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _json(self, code, obj):
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self._cors()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        if code >= 400:
            # v0.7c: 错误响应强制关连接——防止未消费的请求体残留在 keep-alive 连接里，
            # 导致客户端下一个请求被解析错位（曾实测出现 "Unsupported method" 501）。
            self.send_header("Connection", "close")
            self.close_connection = True
        self.end_headers()
        try:
            self.wfile.write(data)
        except Exception:
            pass

    def _respond_quick(self, req, content):
        """Bypass path: answer immediately without injecting any task."""
        stream = bool(req.get("stream"))
        model = req.get("model") or "desktop-agent"
        if not stream:
            return self._json(200, {
                "id": "chatcmpl-quick", "object": "chat.completion", "created": int(time.time()),
                "model": model,
                "choices": [{"index": 0, "finish_reason": "stop",
                             "message": {"role": "assistant", "content": content}}]})
        self.send_response(200)
        self._cors()
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()

        def chunk(delta, finish=None):
            obj = {"id": "chatcmpl-quick", "object": "chat.completion.chunk", "created": int(time.time()),
                   "model": model,
                   "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}
            return ("data: " + json.dumps(obj, ensure_ascii=False) + "\n\n").encode("utf-8")

        try:
            self.wfile.write(chunk({"role": "assistant", "content": ""}))
            self.wfile.write(chunk({"content": content}))
            self.wfile.write(chunk({}, "stop"))
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
        except Exception:
            pass

    def _respond_quick2(self, req, content):
        """同 _respond_quick，但返回是否成功写出（用于补发队列的送达确认）。"""
        stream = bool(req.get("stream"))
        model = req.get("model") or "desktop-agent"
        if not stream:
            try:
                data = json.dumps({
                    "id": "chatcmpl-quick", "object": "chat.completion", "created": int(time.time()),
                    "model": model,
                    "choices": [{"index": 0, "finish_reason": "stop",
                                 "message": {"role": "assistant", "content": content}}]},
                    ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                self.wfile.flush()
                return True
            except Exception:
                return False
        self.send_response(200)
        self._cors()
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()

        def chunk2(delta, finish=None):
            obj = {"id": "chatcmpl-quick", "object": "chat.completion.chunk", "created": int(time.time()),
                   "model": model,
                   "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}
            return ("data: " + json.dumps(obj, ensure_ascii=False) + "\n\n").encode("utf-8")

        try:
            self.wfile.write(chunk2({"role": "assistant", "content": ""}))
            self.wfile.write(chunk2({"content": content}))
            self.wfile.write(chunk2({}, "stop"))
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
            return True
        except Exception:
            return False

    def _read_body(self):
        try:
            n = int(self.headers.get("Content-Length") or 0)
        except Exception:
            n = 0
        raw = self.rfile.read(n) if n > 0 else b""
        for enc in ("utf-8", "gbk", "latin-1"):
            try:
                return raw.decode(enc)
            except Exception:
                continue
        return ""

    # ------------- v0.7 媒体直发：/media/<mid> 公开；put 本机免 token，外部需 token -------------
    def _serve_media(self, mid, query="", body=True):
        if not re.match(r"^[0-9a-f]{32}$", mid or ""):
            return self._json(404, {"error": "not found"})
        fp = _media_find(mid)
        if not fp:
            return self._json(404, {"error": "not found"})
        name = os.path.basename(fp).split("__", 1)[-1]
        try:
            sz = os.path.getsize(fp)
        except OSError:
            return self._json(404, {"error": "not found"})
        dl = ("dl=1" in query) or ("download=1" in query)
        self.send_response(200)
        self.send_header("Content-Type", _media_mime(name))
        self.send_header("Content-Length", str(sz))
        ascii_name = name.encode("ascii", "ignore").decode() or "file"
        self.send_header("Content-Disposition",
                         "%s; filename=%s; filename*=UTF-8''%s"
                         % ("attachment" if dl else "inline", '"' + ascii_name + '"', urllib.parse.quote(name)))
        self.send_header("Cache-Control", "private, max-age=3600")
        self.end_headers()
        log("MEDIA GET %s name=%s %s size=%d ua=%s" % (mid, name, "dl" if dl else "inline", sz, self.headers.get("User-Agent", "")[:60]))
        if not body:
            return
        try:
            with open(fp, "rb") as f:
                while True:
                    part = f.read(262144)
                    if not part:
                        break
                    self.wfile.write(part)
        except Exception:
            pass

    def _handle_media_put(self):
        try:
            n = int(self.headers.get("Content-Length") or 0)
        except Exception:
            n = 0
        if n > 200 * 1024 * 1024:
            return self._json(413, {"error": "too large"})
        raw = self.rfile.read(n) if n > 0 else b""
        ctype = (self.headers.get("Content-Type") or "").lower()
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        is_local = _is_true_local(self)
        try:
            if "json" in ctype or raw.lstrip()[:1] == b"{":
                if not raw:
                    return self._json(400, {"error": "empty body"})
                req = json.loads(raw.decode("utf-8", "replace"))
                path = req.get("path")
                name = req.get("name")
                data_b64 = req.get("data")
                if path:
                    if not is_local:
                        return self._json(403, {"error": "path mode is local-only"})
                    info = _media_publish(path=path, name=name)
                elif data_b64:
                    try:
                        blob = base64.b64decode(data_b64)
                    except Exception as e:
                        return self._json(400, {"error": "bad base64: %s" % e})
                    if len(blob) > 64 * 1024 * 1024:
                        return self._json(413, {"error": "too large (max 64MB)"})
                    info = _media_publish(data=blob, name=name)
                else:
                    return self._json(400, {"error": "need 'path' (local) or 'data' (base64)"})
            else:
                if not raw:
                    return self._json(400, {"error": "empty body"})
                name = ((qs.get("name") or qs.get("filename") or [""])[0]
                        or self.headers.get("X-Filename") or "file.bin")
                info = _media_publish(data=raw, name=name)
            _media_cleanup()
            log("MEDIA PUT %s name=%s size=%d from=%s fwd=%s host=%s" % (
                info["mid"], info["name"], info["size"], self.client_address[0],
                self.headers.get("X-Forwarded-For") or "-", self.headers.get("Host") or "-"))
            return self._json(200, info)
        except Exception as e:
            log("MEDIA PUT error: %r" % (e,))
            return self._json(500, {"error": "publish failed: %s" % e})

    def _handle_media_put_get(self):
        is_local = _is_true_local(self)
        if not is_local:
            return self._json(403, {"error": "path mode is local-only"})
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        path = (qs.get("path") or [""])[0]
        name = (qs.get("name") or [""])[0] or None
        if not path:
            return self._json(400, {"error": "need ?path=<local file>"})
        try:
            info = _media_publish(path=path, name=name)
            _media_cleanup()
            log("MEDIA PUT(GET) %s name=%s size=%d fwd=%s" % (
                info["mid"], info["name"], info["size"], self.headers.get("X-Forwarded-For") or "-"))
            return self._json(200, info)
        except Exception as e:
            return self._json(500, {"error": "publish failed: %s" % e})

    def do_HEAD(self):
        _p = self.path.split("?", 1)[0]
        if _p.startswith("/media/"):
            if not check_auth(self):
                return
            return self._serve_media(_p[len("/media/"):], query=self.path, body=False)
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        log("REQ GET %s UA=%s from %s" % (self.path, self.headers.get("User-Agent", ""), self.client_address[0]))
        if not check_auth(self):
            return self._json(401, {"error": "unauthorized"})
        _p = self.path.split("?", 1)[0]
        if _p.startswith("/media/"):
            return self._serve_media(_p[len("/media/"):], query=self.path)
        if _p.startswith("/v1/media/put"):
            return self._handle_media_put_get()
        if _p.startswith("/v1/media/list"):
            return self._json(200, {"media": _media_list()})
        if self.path.startswith("/health"):
            return self._json(200, {"ok": True, "service": "chatbox-bridge", "version": "0.7",
                                    "queue": TASK_Q.qsize(), "tasks": len(TASKS),
                                    "uptime_s": int(time.time() - STARTED_AT),
                                    "cb9222": probe_port(9222),
                                    "pending": len(take_pending(limit=50)),
                                    "injector": os.path.exists(os.path.join(BASE, CONF["inject_script"]))})
        if self.path.startswith("/v1/models"):
            return self._json(200, {"object": "list",
                                    "data": [{"id": m, "object": "model", "created": 1730000000,
                                              "owned_by": "local-bridge"}
                                             for m in ("desktop-agent", "dsh", "dsh-acp")]})
        if self.path.startswith("/tasks"):
            rows = [{"id": t.id, "text": t.text[:200], "done": t.done.is_set(),
                     "reply": (t.reply or "")[:200], "error": t.error} for t in TASKS.values()]
            return self._json(200, {"tasks": rows[-30:], "pending": len(take_pending(limit=10))})
        return self._json(404, {"error": "not found"})

    def do_POST(self):
        log("REQ POST %s UA=%s len=%s from %s" % (self.path, self.headers.get("User-Agent", ""), self.headers.get("Content-Length", ""), self.client_address[0]))
        if not check_auth(self):
            return self._json(401, {"error": "unauthorized"})
        path = self.path.split("?", 1)[0]

        if path.startswith("/v1/media/put"):
            return self._handle_media_put()

        if path.startswith("/v1/progress/"):
            tid = path[len("/v1/progress/"):].strip("/")
            body = (self._read_body() or "").strip()
            t = TASKS.get(tid)
            log("PROGRESS %s: %s" % (tid, body[:200].replace("\n", "\\n")))
            if t is None:
                return self._json(404, {"error": "unknown task id"})
            if t.done.is_set() or not body:
                return self._json(200, {"ok": True, "stale": True})
            for ln in body.splitlines():
                ln = ln.strip()
                if ln:
                    t.progress.append("✳ " + ln)
            return self._json(200, {"ok": True})

        if path.startswith("/v1/reply/"):
            tid = path[len("/v1/reply/"):].strip("/")
            body = self._read_body()
            t = TASKS.get(tid)
            log("REPLY %s len=%d body=%s" % (tid, len(body), body[:200].replace("\n", "\\n")))
            if t is None:
                return self._json(404, {"error": "unknown task id"})
            if t.done.is_set():
                log("REPLY %s 迟到（任务已结束，忽略）" % tid)
                return self._json(200, {"ok": True, "stale": True})
            t.reply = body
            finish_task(t)
            return self._json(200, {"ok": True})

        if path.startswith("/v1/chat/completions"):
            raw = self._read_body()
            try:
                req = json.loads(raw) if raw else {}
            except Exception as e:
                return self._json(400, {"error": "bad json: %s" % e})
            text = last_user_text(req.get("messages"))
            if not text.strip():
                return self._json(400, {"error": "empty user message"})
            # Bypass: client's "auto name this conversation" request -> reply locally, do NOT inject a task
            if "give this conversation a name" in text.lower() and len(text) < 2000:
                qname = "手机 " + time.strftime("%H:%M")
                log("NAMEREQ -> '%s'" % qname)
                return self._respond_quick(req, qname)
            # /stop 指令：手动停止电脑端当前会话的生成（不注入任务）
            if text.strip().lower() in ("/stop", "／stop"):
                log("STOPCMD 收到 /stop → 尝试停止电脑端生成")
                cdp_stop()
                return self._respond_quick2(req, "⏹ 已向电脑端发送停止信号（尝试停止当前会话的生成）。")
            # 取结果：把补发队列里的已得结果一次性取回（不注入任务）
            if text.strip().lower() in ("取结果", "结果", "/结果", "/results", "results"):
                pend = take_pending(limit=10)
                log("FETCHRESULT 收到（待补发 %d 条）" % len(pend))
                if not pend:
                    return self._respond_quick2(req, "📭 当前没有待取回的结果。")
                block = "".join(
                    "📮【补发】你之前发的「%s」的回复：\n\n%s\n\n————\n\n"
                    % ((u["text"].splitlines()[0][:30] if u["text"] else ""), u["reply"])
                    for u in pend)
                ok = self._respond_quick2(req, block.strip())
                if ok and not _client_rst(self.connection):
                    drop_pending([u["id"] for u in pend])
                log("FETCHRESULT 送出 %d 条" % len(pend))
                return
            stream = bool(req.get("stream"))
            tid = time.strftime("%y%m%d-%H%M%S") + "-" + secrets.token_hex(3)
            model_name = str(req.get("model") or "")
            engine = "dsh" if model_name.lower() in ("dsh", "dsh-headless", "dsh-acp") else "chatbox"
            target = "" if model_name in ("", "desktop-agent") or engine == "dsh" else model_name
            ntext = norm_text(text)
            is_attach = False
            # 去重挂靠：内容（规范化后）相同、目标相同、尚未完成、窗口内 → 视为手机端重试/重发
            task = None
            for t in list(TASKS.values()):
                if (not t.done.is_set()) and norm_text(t.text) == ntext and t.target == target and (time.time() - t.created) < DEDUP_WINDOW_S and getattr(t, "waiters", 0) == 0:  # 2026-10-11 fix: only true retry attaches
                    task = t
                    break
            if task is not None:
                is_attach = True
                if task.stop_timer is not None:
                    task.stop_timer.cancel()
                    task.stop_timer = None
                log("CHAT 重试挂靠 #%s（同内容重发，不重复注入）" % task.id)
            else:
                # 2026-10-11 fix: 删除"按内容自动补发"——不同对话发相同内容会误取别人的未送达回复；
                # 未送达回复改用显式入口："取结果"（FETCHRESULT 分支）。
                # 2026-10-11 fix: 删除"同内容已完成再送"——不同对话发相同内容会误拿旧回复且不执行；
                # 现在相同内容会正常新建任务重新执行（可预期、无串台）。
                task = Task(tid, text, target)
                task.engine = engine
                task.dsh_mode = "acp" if model_name.lower() == "dsh-acp" else "headless"
                task.fingerprint = _dsh_fingerprint(req.get("messages"))
                TASKS[tid] = task
                TASK_Q.put(task)
                dt = threading.Timer(CONF["inject_timeout_s"] + CONF["reply_timeout_s"] + 60,
                                     finish_task, args=[task, "超时：电脑端无回传"])
                dt.daemon = True
                dt.start()
                task.deadline_timer = dt
                log("CHAT #%s len=%d stream=%s text=%s" % (tid, len(text), stream, text[:150].replace("\n", "\\n")))

            if not stream:
                deadline = CONF["inject_timeout_s"] + CONF["reply_timeout_s"] + 30
                t0 = time.time()
                while not task.done.is_set() and time.time() - t0 < deadline:
                    time.sleep(0.5)
                content = task.reply if task.reply is not None else ("⚠ " + (task.error or "未知错误"))
                if not _client_rst(self.connection):
                    task.delivered += 1
                return self._json(200, {
                    "id": "chatcmpl-" + task.id, "object": "chat.completion", "created": int(time.time()),
                    "model": req.get("model") or "desktop-agent",
                    "choices": [{"index": 0, "finish_reason": "stop",
                                 "message": {"role": "assistant", "content": content}}]})

            # 流式：先回执 + 周期保活，最终结果追加，最后 [DONE]
            self.send_response(200)
            self._cors()
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.end_headers()

            def chunk(delta, finish=None):
                obj = {"id": "chatcmpl-" + task.id, "object": "chat.completion.chunk", "created": int(time.time()),
                       "model": req.get("model") or "desktop-agent",
                       "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}
                return ("data: " + json.dumps(obj, ensure_ascii=False) + "\n\n").encode("utf-8")

            def keepalive():
                return b": keep-alive\n\n"

            task.waiters += 1
            disconnected = False
            try:
                self.wfile.write(chunk({"role": "assistant", "content": ""}))
                # 2026-10-11 fix: 删除"流打开时自动补发"——它会把任意未送达回复推给当前新流（跨对话串台）；
                # 未送达回复的入口：显式发"取结果"（FETCHRESULT 分支）。
                tid_short = task.id.split("-")[-1]
                if is_attach:
                    sent_prog = len(task.progress)
                    st = "已送达电脑端，处理中" if task.injected else "正在送达电脑端"
                    self.wfile.write(chunk({"content": "🔁 接回未完成的任务（#%s，%s），继续等待结果…\n\n" % (tid_short, st)}))
                else:
                    sent_prog = 0
                    self.wfile.write(chunk({"content": "⏳ 任务已收到（#%s），正在电脑上执行…\n\n" % tid_short}))
                sent_live = len(task.live) if is_attach else 0
                self.wfile.flush()
                t0 = time.time()
                last_tick = t0
                while not task.done.is_set() and time.time() - t0 < CONF["inject_timeout_s"] + CONF["reply_timeout_s"] + 30:
                    task.done.wait(0.5)
                    # 发送新增的进度行（注入完成/电脑端发来的进度等）
                    while sent_prog < len(task.progress):
                        self.wfile.write(chunk({"content": task.progress[sent_prog] + "\n\n"}))
                        sent_prog += 1
                        self.wfile.flush()
                    # DSH 实时活动流：思考 -> reasoning_content（Chatbox 显示为思考过程）；工具/正文 -> content
                    while sent_live < len(task.live):
                        ev = task.live[sent_live]
                        sent_live += 1
                        k = ev.get("kind")
                        txt = ev.get("text", "")
                        if k == "reasoning":
                            self.wfile.write(chunk({"reasoning_content": txt + "\n"}))
                        elif k == "tool":
                            self.wfile.write(chunk({"content": "\n🔧 " + txt + "\n\n"}))
                        elif k == "tool_result":
                            self.wfile.write(chunk({"content": "✅ " + txt + "\n\n"}))
                        else:
                            self.wfile.write(chunk({"content": txt + "\n\n"}))
                        self.wfile.flush()
                    if task.done.is_set():
                        break
                    if _client_closed(self.connection):
                        raise ConnectionError("client closed (probe)")
                    # 每 HEARTBEAT_S 秒一条可见心跳（手机端可见“没卡住”）
                    if time.time() - last_tick >= HEARTBEAT_S:
                        waited = int(time.time() - t0)
                        self.wfile.write(chunk({"content": "⏱ 仍在处理（已等 %d 分 %d 秒）…\n\n" % (waited // 60, waited % 60)}))
                        self.wfile.flush()
                        last_tick = time.time()
                    self.wfile.write(keepalive())
                    self.wfile.flush()
                content = task.reply if task.reply is not None else ("⚠ " + (task.error or "未知错误"))
                self.wfile.write(chunk({"content": content}))
                self.wfile.write(chunk({}, "stop"))
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.flush()
                time.sleep(0.6)   # 稍候片刻再判定送达（捕获写出后立刻的 RST）
                if not _client_rst(self.connection):
                    task.delivered += 1
            except Exception as e:
                disconnected = True
                log("STREAM 断开 #%s（连接中断；%d 秒内无重试认领将转异步继续处理、不停止电脑端）: %s" % (task.id, LOST_GRACE_S, e))
            finally:
                task.waiters -= 1
                if disconnected and not task.done.is_set():
                    maybe_schedule_stop(task)
            return

        return self._json(404, {"error": "not found"})

    def log_message(self, fmt, *args):
        try:
            log("SRV " + (fmt % args))
        except Exception:
            pass


def main():
    os.makedirs(TASKS_DIR, exist_ok=True)
    os.makedirs(MEDIA_DIR, exist_ok=True)
    _media_cleanup()
    threading.Thread(target=worker, daemon=True).start()
    log("BRIDGE 启动 host=%s port=%s conv=%s injector=%s"
        % (CONF["host"], CONF["port"], CONF["conv_name"],
           os.path.exists(os.path.join(BASE, CONF["inject_script"]))))
    try:
        srv = Server((CONF["host"], CONF["port"]), Handler)
    except Exception as e:
        log("BRIDGE 启动失败（端口被占或其他原因）：%r" % (e,))
        raise SystemExit(2)
    srv.serve_forever()


if __name__ == "__main__":
    main()
