#!/usr/bin/env python3
"""Small Alertmanager-v2 receiver that routes vmalert alerts to chat channels."""

import argparse, base64, hashlib, hmac, json, logging, os, queue, re
import threading, time, tomllib, urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

LOG = logging.getLogger("vlalert-adapter")
WORK_QUEUE = queue.Queue()

def parse_duration(value):
    match = re.fullmatch(r"(\d+)([smh])", str(value))
    if not match:
        raise ValueError(f"invalid duration {value!r}; expected e.g. 30m, 2h, or 90s")
    return int(match.group(1)) * {"s": 1, "m": 60, "h": 3600}[match.group(2)]

def load_config(path):
    with open(path, "rb") as handle:
        cfg = tomllib.load(handle)
    cfg.setdefault("listen", "127.0.0.1:9094")
    cfg.setdefault("state_file", "/var/lib/vlalert-adapter/state.json")
    cfg.setdefault("mute_file", "/run/vlalert-mute")
    cfg["repeat_seconds"] = parse_duration(cfg.get("repeat_interval", "30m"))
    cfg["ttl_seconds"] = parse_duration(cfg.get("state_ttl", "24h"))
    cfg["timeout_seconds"] = parse_duration(cfg.get("request_timeout", "5s"))
    channels = cfg.get("channels", {})
    routes = cfg.get("route", [])
    if not isinstance(channels, dict) or not isinstance(routes, list):
        raise ValueError("channels must be a table and route must be an array of tables")
    for name, channel in channels.items():
        if not isinstance(channel, dict):
            raise ValueError(f"channel {name!r}: configuration must be a table")
        kind = channel.get("type")
        if kind == "feishu" and not channel.get("webhook"):
            raise ValueError(f"channel {name!r}: feishu webhook is required")
        if kind == "bark" and not channel.get("key"):
            raise ValueError(f"channel {name!r}: bark key is required")
        if kind not in ("feishu", "bark"):
            raise ValueError(f"channel {name!r}: unsupported type {kind!r}")
    for index, route in enumerate(routes):
        if not isinstance(route, dict):
            raise ValueError(f"route {index}: configuration must be a table")
        if not isinstance(route.get("match", {}), dict):
            raise ValueError(f"route {index}: match must be a table")
        if not isinstance(route.get("channels"), list):
            raise ValueError(f"route {index}: channels must be an array")
        missing = [name for name in route["channels"] if name not in channels]
        if missing:
            raise ValueError(f"route {index}: unknown channels: {', '.join(missing)}")
    state_path = Path(cfg["state_file"])
    state_path.parent.mkdir(parents=True, exist_ok=True)
    return cfg

def load_state(path):
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
        if not isinstance(data, dict):
            raise ValueError("top-level value is not an object")
        return data
    except FileNotFoundError:
        LOG.warning("state file %s does not exist; starting empty", path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        LOG.warning("cannot load state file %s; starting empty: %s", path, exc)
    return {}

def save_state(state, cfg):
    cutoff = time.time() - cfg["ttl_seconds"]
    for key in list(state):
        if not isinstance(state[key], dict) or state[key].get("last_sent", 0) < cutoff:
            del state[key]
    path = Path(cfg["state_file"])
    temp = Path(str(path) + ".tmp")
    with open(temp, "w", encoding="utf-8") as handle:
        json.dump(state, handle, ensure_ascii=False, separators=(",", ":"))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, path)

def fingerprint(labels):
    raw = "|".join(f"{key}={labels[key]}" for key in sorted(labels))
    return hashlib.sha1(raw.encode()).hexdigest()[:16]

def is_resolved(alert, now):
    value = alert.get("endsAt")
    if not value:
        return False
    try:
        ending = datetime.fromisoformat(value)
        if ending.tzinfo is None:
            raise ValueError("timezone is missing")
        return ending <= datetime.fromtimestamp(now, timezone.utc)
    except (TypeError, ValueError) as exc:
        LOG.warning("invalid endsAt %r; treating alert as firing: %s", value, exc)
        return False

def select_channels(labels, cfg):
    for route in cfg.get("route", []):
        if all(labels.get(key) == value for key, value in route.get("match", {}).items()):
            return route["channels"]
    return None

def render(alert, resolved):
    labels = alert.get("labels") or {}
    name = str(labels.get("alertname", "Alert"))
    title = f"✅ {name} 已恢复" if resolved else f"⚠️ {name}"
    tags = ", ".join(
        f"{key}={value}" for key, value in sorted(labels.items())
        if key not in ("alertname", "severity")
    )
    if resolved:
        return title, tags or name
    summary = str((alert.get("annotations") or {}).get("summary") or name)
    parts = [summary]
    if tags:
        parts.append(tags)
    if alert.get("generatorURL"):
        parts.append(str(alert["generatorURL"]))
    return title, "\n".join(parts)

def post_json(url, payload, timeout):
    data = json.dumps(payload, ensure_ascii=False).encode()
    request = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read()
    try:
        return json.loads(body) if body else {}
    except json.JSONDecodeError:
        return {}

def send_channel(channel, title, body, severity, timeout):
    if channel["type"] == "feishu":
        payload = {"msg_type": "text", "content": {"text": f"{title}\n{body}"}}
        if channel.get("secret"):
            timestamp = str(int(time.time()))
            key = f"{timestamp}\n{channel['secret']}".encode()
            payload.update(timestamp=timestamp, sign=base64.b64encode(hmac.new(key, b"", hashlib.sha256).digest()).decode())
        result = post_json(channel["webhook"], payload, timeout)
        code = result.get("code", result.get("StatusCode", 0))
        if code not in (0, "0", None):
            raise RuntimeError(f"Feishu rejected request: {result}")
    else:
        url = f"{channel.get('server', 'https://api.day.app').rstrip('/')}/{channel['key']}"
        payload = {"title": title, "body": body, "group": "vlalert", "level": "critical" if severity == "critical" else "timeSensitive", "isArchive": 1}
        result = post_json(url, payload, timeout)
        if result.get("code", 200) not in (200, "200"):
            raise RuntimeError(f"Bark rejected request: {result}")

def deliver(alert, channel_names, cfg, resolved=None):
    labels = alert.get("labels") or {}
    title, body = render(alert, is_resolved(alert, time.time()) if resolved is None else resolved)
    all_succeeded = True
    for name in channel_names:
        for attempt, delay in enumerate((0, 2, 5)):
            if delay:
                time.sleep(delay)
            try:
                send_channel(cfg["channels"][name], title, body, labels.get("severity"), cfg["timeout_seconds"])
                break
            except Exception as exc:  # keep the worker alive on all transport failures
                if attempt == 2:
                    LOG.error("channel %s failed after 3 attempts: %s", name, exc)
                    all_succeeded = False
                    break
                LOG.warning("channel %s attempt %d failed: %s", name, attempt + 1, exc)
    return all_succeeded

def muted(cfg, now):
    try:
        return float(Path(cfg["mute_file"]).read_text(encoding="utf-8").strip()) > now
    except FileNotFoundError:
        return False
    except (OSError, ValueError) as exc:
        LOG.warning("cannot read mute file %s: %s", cfg["mute_file"], exc)
        return False

def process_alert(alert, state, cfg):
    if not isinstance(alert, dict) or not isinstance(alert.get("labels", {}), dict):
        LOG.error("ignoring malformed alert item: %r", alert)
        return
    labels = alert.get("labels") or {}
    key = fingerprint(labels)
    now = time.time()
    resolved = is_resolved(alert, now)
    existing = state.get(key)
    if existing is not None and not isinstance(existing, dict):
        LOG.warning("discarding malformed state entry %s", key)
        state.pop(key)
        existing = None
    if resolved and not existing:
        LOG.debug("ignoring unseen resolved alert %s", key)
        return
    if not resolved and existing and now - existing.get("last_sent", 0) < cfg["repeat_seconds"]:
        LOG.debug("suppressing repeated firing alert %s", key)
        return
    if muted(cfg, now):
        if resolved:
            state.pop(key, None)
        else:
            state[key] = {"last_sent": now, "labels": labels, "first_seen": existing.get("first_seen", now) if existing else now}
        save_state(state, cfg)
        LOG.info("notification muted for alert %s", key)
        return
    channel_names = select_channels(labels, cfg)
    if channel_names is None:
        LOG.warning("no route matched alert %s; dropping", key)
        return
    if not deliver(alert, channel_names, cfg, resolved):
        return
    if resolved:
        state.pop(key, None)
    else:
        sent = time.time()
        state[key] = {"last_sent": sent, "labels": labels, "first_seen": existing.get("first_seen", now) if existing else now}
    save_state(state, cfg)
    LOG.info("delivered %s alert %s", "resolved" if resolved else "firing", key)

def worker(state, cfg):
    while True:
        try:
            process_alert(WORK_QUEUE.get(), state, cfg)
        except Exception:
            LOG.exception("unexpected error while processing alert")
        finally:
            WORK_QUEUE.task_done()

def heartbeat(cfg):
    url = cfg.get("heartbeat_url")
    while url:
        try:
            with urllib.request.urlopen(url, timeout=cfg["timeout_seconds"]) as response:
                response.read()
        except Exception as exc:
            LOG.warning("heartbeat failed: %s", exc)
        time.sleep(60)

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/healthz":
            self.send_error(404)
            return
        self.reply(200, {"status": "ok", "queue_length": WORK_QUEUE.qsize()})

    def do_POST(self):
        if self.path != "/api/v2/alerts":
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length))
            if not isinstance(payload, list):
                raise ValueError("JSON body must be an array")
        except (ValueError, json.JSONDecodeError) as exc:
            LOG.error("invalid alert request: %s", exc)
            self.reply(400, {"error": str(exc)})
            return
        for alert in payload:
            WORK_QUEUE.put(alert)
        self.reply(200, {"status": "accepted", "count": len(payload)})

    def reply(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        LOG.info("%s - %s", self.client_address[0], fmt % args)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", default="/etc/vlalert-adapter.toml")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        cfg = load_config(args.config)
        host, port = cfg["listen"].rsplit(":", 1)
        state = load_state(cfg["state_file"])
    except (OSError, ValueError, tomllib.TOMLDecodeError) as exc:
        parser.error(str(exc))
    threading.Thread(target=worker, args=(state, cfg), daemon=True, name="delivery-worker").start()
    if cfg.get("heartbeat_url"):
        threading.Thread(target=heartbeat, args=(cfg,), daemon=True, name="heartbeat").start()
    server = ThreadingHTTPServer((host, int(port)), Handler)
    LOG.info("listening on %s:%s", host, port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

if __name__ == "__main__":
    main()
