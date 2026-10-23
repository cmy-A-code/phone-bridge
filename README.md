# phone-bridge

**A small HTTP bridge between a phone client and an AI agent running on your desktop.**

You send a task from your phone; the bridge queues it, injects it into a local agent
(Chatbox via CDP, or DSH headless/ACP), then streams progress and the final answer
back to the phone — with retry, offline queueing and a media channel for files.

```
phone  ──HTTPS──►  tunnel  ──►  bridge (8780)  ──►  agent
   ▲                                │
   └────── progress / reply ◄───────┘
```

## Why

Phone clients lose connections, agents take minutes, and you still want to send a
file back. This bridge handles the boring parts:

- **Task queue + offline buffer**: tasks are persisted; undelivered replies can be fetched later.
- **Attach window**: a retried request with the same fingerprint attaches to the running task instead of spawning a duplicate.
- **Progress streaming**: the phone can render "已送达 / 处理中 / 结果" without polling every second.
- **Media channel**: publish a local file to an unguessable `/media/<id>` URL; images render inline.
- **Fail-closed by default**: binds to `127.0.0.1`, everything except `/health` and `/media/<id>` requires `Authorization: Bearer <token>`.

## Endpoints

| Method | Path | Auth | Purpose |
|---|---|---|---|
| POST | `/v1/task` | token | submit a task `{text, target?}`; returns `{id}` and a stream of progress |
| POST | `/v1/reply/<id>` | token | final answer from the agent |
| POST | `/v1/progress/<id>` | token | one-line progress update |
| GET | `/v1/task/<id>` | token | task status / result |
| POST | `/v1/media/put` | token (external) / local allow | `{path}` or `{data, name}` → public URL |
| GET | `/media/<id>` | public | serve the media (unguessable 128-bit id, `?dl=1` to download) |
| GET | `/health` | public | liveness (`ok`, `version`, `pending`) |
| GET | `/v1/media/list` | token / local | recent media |

Only loopback connections **without** `X-Forwarded-*` headers count as "local";
anything arriving through a tunnel must present the token.

## Quick start

```bash
python bridge.py            # first run writes config.json with a random token
```

Minimal `config.json`:

```json
{
  "host": "127.0.0.1",
  "port": 8780,
  "token": "auto-generated-on-first-run",
  "inject_mode": "cdp",
  "inject_script": "inject.ps1",
  "public_base": "https://your-tunnel.example.com"
}
```

Expose it to the phone with any HTTPS tunnel (ngrok, cloudflared, Tailscale Funnel):

```bash
tailscale funnel --bg 8780      # → https://machine.your-tailnet.ts.net
```

Point the phone client's API base at `<tunnel>/v1` with the bearer token.

## How injection works

`inject_mode` selects the engine:

- **`cdp`** — `inject.ps1` writes the task into a local Chatbox window over the
  DevTools protocol (Chatbox must run with `--remote-debugging-port=9222`).
- **`dsh`** — spawns a local DSH agent (`dsh_node` / `dsh_bin` in config) in
  headless or ACP mode and streams its output back.

Both are optional; if neither is configured the bridge still queues tasks and
the queue endpoints stay usable.

## Media channel

```bash
# from the machine running the bridge
powershell -NoProfile -ExecutionPolicy Bypass -File send-media.ps1 -Path "C:\path\photo.jpg"
# -> https://your-tunnel/media/<id>   (paste it into the reply body)
```

Files live under `media/<id>__<name>` and are cleaned up after `media_ttl_days`.
Ids are 128-bit random — unguessable, but the URL is public, so don't publish
anything sensitive.

## Configuration

| Key | Default | Meaning |
|---|---|---|
| `host` / `port` | `127.0.0.1` / `8780` | bind address |
| `token` | auto | bearer token; auto-generated and persisted when empty |
| `inject_mode` | `cdp` | `cdp` or `dsh` |
| `inject_script` | `inject.ps1` | relative to this directory |
| `inject_timeout_s` | 150 | injection timeout |
| `reply_timeout_s` | 900 | how long a task waits for the final answer |
| `public_base` | `""` | public HTTPS prefix used in media URLs |
| `media_ttl_days` | 14 | media retention |
| `dsh_node` / `dsh_bin` / `dsh_home` / `dsh_cwd` | `""` | DSH engine paths (optional) |

## Security notes

- The bridge is a remote-control surface: keep the token secret and the tunnel private.
- `/media/<id>` is public by design (so the phone can open images); ids are random.
- Do not expose the port on a LAN interface without a tunnel that terminates TLS.
- The agent side (CDP injection) runs with your user rights — treat the token like a password.

## License

MIT — see [LICENSE](LICENSE).
