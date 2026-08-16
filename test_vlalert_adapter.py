import json
import queue
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest import mock

import vlalert_adapter as app


class AdapterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.cfg = {
            "timeout_seconds": 5,
            "channels": {"test": {"type": "feishu", "webhook": "https://example.invalid"}},
            "route": [{"match": {}, "channels": ["test"]}],
        }
        self.firing = {
            "labels": {"alertname": "Test", "severity": "warning", "service": "api"},
            "annotations": {"summary": "test summary"},
            "endsAt": "2099-01-01T00:00:00Z",
            "generatorURL": "http://example.test/alert",
        }

    def tearDown(self):
        self.temp.cleanup()

    @mock.patch.object(app, "deliver", return_value=True)
    def test_every_firing_is_forwarded_and_resolved_is_ignored(self, deliver):
        app.process_alert(self.firing, self.cfg)
        app.process_alert(self.firing, self.cfg)
        self.assertEqual(deliver.call_count, 2)
        resolved = {**self.firing, "endsAt": "2000-01-01T00:00:00Z"}
        app.process_alert(resolved, self.cfg)
        self.assertEqual(deliver.call_count, 2)

    @mock.patch.object(app, "deliver", return_value=False)
    def test_failed_delivery_is_handled(self, deliver):
        app.process_alert(self.firing, self.cfg)
        deliver.assert_called_once()

    def test_duration_parser(self):
        self.assertEqual(app.parse_duration("90s"), 90)
        self.assertEqual(app.parse_duration("2h"), 7200)
        with self.assertRaises(ValueError):
            app.parse_duration("1d")

    def test_render_single_line_and_channel_payloads(self):
        message = app.render({
            "labels": {"alertname": "ErrorLogs", "service": "blog-studio", "severity": "critical", "pod": "hidden"},
            "annotations": {"summary": "近 1 分钟出现 1 条 ERROR"},
        })
        self.assertEqual(message, "🚨 blog-studio 近 1 分钟出现 1 条 ERROR")
        self.assertEqual(app.render({"labels": {"alertname": "Bare"}}), "⚠️ Bare")
        self.assertEqual(app.render({"labels": {"alertname": "Info", "severity": "info"}}), "ℹ️ Info")
        with mock.patch.object(app, "post_json", return_value={"code": 0}) as post:
            app.send_channel({"type": "feishu", "webhook": "https://hook", "secret": "secret"}, message, "warning", 5)
            payload = post.call_args.args[1]
            self.assertIn("timestamp", payload)
            self.assertIn("sign", payload)
            self.assertEqual(payload["content"]["text"], message)
        with mock.patch.object(app, "post_json", return_value={"code": 200}) as post:
            app.send_channel({"type": "bark", "server": "https://bark", "key": "key"}, message, "critical", 5)
            self.assertEqual(post.call_args.args[1], {"body": message, "group": "vlalert", "level": "critical", "isArchive": 1})

    def test_failed_channel_retries_and_does_not_block_other_channel(self):
        cfg = dict(self.cfg)
        cfg["channels"] = {"bad": {"name": "bad"}, "good": {"name": "good"}}
        calls = []

        def fake_send(channel, *_args):
            calls.append(channel["name"])
            if channel["name"] == "bad":
                raise OSError("offline")

        with mock.patch.object(app, "send_channel", side_effect=fake_send), mock.patch.object(app.time, "sleep") as sleep:
            self.assertFalse(app.deliver(self.firing, ["bad", "good"], cfg))
        self.assertEqual(calls, ["bad", "bad", "bad", "good"])
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [2, 5])

    def test_config_rejects_unknown_route_channel(self):
        config = Path(self.temp.name) / "config.toml"
        config.write_text(
            '[[route]]\nmatch = {}\nchannels = ["missing"]\n',
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "unknown channels"):
            app.load_config(config)

    def test_default_listen_is_all_interfaces(self):
        config = Path(self.temp.name) / "empty.toml"
        config.write_text("", encoding="utf-8")
        self.assertEqual(app.load_config(config)["listen"], "0.0.0.0:9094")

    def test_http_contract_and_health(self):
        while True:
            try:
                app.WORK_QUEUE.get_nowait()
                app.WORK_QUEUE.task_done()
            except queue.Empty:
                break
        server = app.ThreadingHTTPServer(("127.0.0.1", 0), app.Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = f"http://127.0.0.1:{server.server_port}"
        try:
            req = urllib.request.Request(base + "/api/v2/alerts", data=json.dumps([self.firing]).encode(), headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=2) as response:
                self.assertEqual(response.status, 200)
            self.assertEqual(app.WORK_QUEUE.get_nowait(), self.firing)
            app.WORK_QUEUE.task_done()
            with urllib.request.urlopen(base + "/health", timeout=2) as response:
                self.assertEqual(json.load(response)["queue_length"], 0)
            with urllib.request.urlopen(base + "/healthz", timeout=2) as response:
                self.assertEqual(response.status, 200)
            bad = urllib.request.Request(base + "/api/v2/alerts", data=b"{}", headers={"Content-Type": "application/json"})
            with self.assertRaises(urllib.error.HTTPError) as caught:
                urllib.request.urlopen(bad, timeout=2)
            self.assertEqual(caught.exception.code, 400)
            caught.exception.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()
