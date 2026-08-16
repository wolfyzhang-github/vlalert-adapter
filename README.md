# vlalert-adapter

`vlalert-adapter` 接收 vmalert 发往 Alertmanager v2 API 的裸告警数组，并按标签将每条 firing 告警投递到飞书自定义机器人和 Bark。服务仅依赖 Python 3.11+ 标准库。

## 安装

```sh
sudo install -m 0755 vlalert_adapter.py /usr/local/bin/vlalert_adapter.py
sudo install -m 0644 config.example.toml /etc/vlalert-adapter.toml
sudo install -m 0644 vlalert-adapter.service /etc/systemd/system/vlalert-adapter.service
sudo editor /etc/vlalert-adapter.toml
sudo systemctl daemon-reload
sudo systemctl enable --now vlalert-adapter
```

unit 使用 `User=nobody`，默认只监听 `127.0.0.1:9094`。

查看状态和日志：

```sh
curl -s http://127.0.0.1:9094/healthz
journalctl -u vlalert-adapter -f
```

## 配置 vmalert

为 vmalert 增加：

```sh
-notifier.url=http://127.0.0.1:9094
```

vmalert 会请求 `/api/v2/alerts`。修改配置后执行 `sudo systemctl restart vlalert-adapter`。路由按书写顺序匹配，第一条标签全部精确相等的规则生效；空 `match` 可作为兜底。

通知统一渲染成一行：`{severity 图标} {service} {annotations.summary}`，例如 `🚨 blog-studio 近 1 分钟出现 1 条 ERROR`。缺少 `service` 时回退到 `alertname`。

## 手工投递

先在配置中填入真实渠道凭据并启动服务，然后从仓库目录执行：

```sh
curl -i -H 'Content-Type: application/json' --data-binary @test_payload.json http://127.0.0.1:9094/api/v2/alerts
```

测试载荷的 `endsAt` 在未来，因此会被判定为 firing。每次执行都会投递一次；服务不做去重或重发抑制。resolved 告警直接丢弃，不发送恢复通知。

## 运维注意事项

- 所有出站请求默认 5 秒超时，可用 `request_timeout` 调整。投递失败会按 2 秒、5 秒间隔再试两次。
- `heartbeat_url` 非空时，独立线程每 60 秒 GET 一次该地址。
- 本服务自己的失败日志可能包含 `ERROR`。如果这些日志也被 VictoriaLogs 采集，而告警规则匹配 `ERROR`，会形成“投递失败 → 日志告警 → 再次投递失败”的自激风暴。务必在日志采集侧排除本服务，或在 vmalert LogsQL 规则中明确排除它。
