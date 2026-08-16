#!/usr/bin/env python3
"""Small Alertmanager-v2 receiver that routes vmalert alerts to chat channels."""

import argparse, base64, hashlib, hmac, json, logging, queue, re
import threading, time, tomllib, urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOG = logging.getLogger("vlalert-adapter")
WORK_QUEUE = queue.Queue()
_EMOJI_BY_SEVERITY = {"critical": "🚨", "warning": "⚠️", "info": "ℹ️"}
_KEEP_LABELS = ("service", "instance", "job", "node", "environment")

def parse_duration(value):
    match = re.fullmatch(r"(\d+)([smh])", str(value))
    if not match:
        raise ValueError(f"invalid duration {value!r}; expected e.g. 30m, 2h, or 90s")
    return int(match.group(1)) * {"s": 1, "m": 60, "h": 3600}[match.group(2)]

def load_config(path):
    with open(path, "rb") as handle:
        cfg = tomllib.load(handle)
    cfg.setdefault("listen", "127.0.0.1:9094")
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
    return cfg

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

def render(alert):
    labels = alert.get("labels") or {}
    name = str(labels.get("alertname", "Alert"))
    severity = str(labels.get("severity", "warning"))
    emoji = _EMOJI_BY_SEVERITY.get(severity, "⚠️")
    title = f"{emoji} {name}"
    tags = ", ".join(
        f"{key}={value}" for key, value in sorted(labels.items())
        if key in _KEEP_LABELS
    )
    summary = str((alert.get("annotations") or {}).get("summary") or name)
    parts = [summary]
    if tags:
        parts.append(tags)
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

def deliver(alert, channel_names, cfg):
    labels = alert.get("labels") or {}
    title, body = render(alert)
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

def process_alert(alert, cfg):
    if not isinstance(alert, dict) or not isinstance(alert.get("labels", {}), dict):
        LOG.error("ignoring malformed alert item: %r", alert)
        return
    labels = alert.get("labels") or {}
    name = labels.get("alertname", "Alert")
    if is_resolved(alert, time.time()):
        LOG.debug("ignoring resolved alert %s", name)
        return
    channel_names = select_channels(labels, cfg)
    if channel_names is None:
        LOG.warning("no route matched alert %s; dropping", name)
        return
    if not deliver(alert, channel_names, cfg):
        return
    LOG.info("delivered firing alert %s", name)

def worker(cfg):
    while True:
        try:
            process_alert(WORK_QUEUE.get(), cfg)
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
    except (OSError, ValueError, tomllib.TOMLDecodeError) as exc:
        parser.error(str(exc))
    threading.Thread(target=worker, args=(cfg,), daemon=True, name="delivery-worker").start()
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
