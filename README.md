# vlalert-adapter

`vlalert-adapter` 接收 vmalert 发往 Alertmanager v2 API 的裸告警数组，做持久化重发抑制，并按标签将通知投递到飞书自定义机器人和 Bark。服务仅依赖 Python 3.11+ 标准库。

## 安装

```sh
sudo install -m 0755 vlalert_adapter.py /usr/local/bin/vlalert_adapter.py
sudo install -m 0644 config.example.toml /etc/vlalert-adapter.toml
sudo install -m 0644 vlalert-adapter.service /etc/systemd/system/vlalert-adapter.service
sudo editor /etc/vlalert-adapter.toml
sudo systemctl daemon-reload
sudo systemctl enable --now vlalert-adapter
```

unit 使用 `User=nobody` 和 `StateDirectory=vlalert-adapter`。systemd 会创建并授权 `/var/lib/vlalert-adapter`；若不使用该 unit，请自行确保运行用户可写状态文件目录。默认只监听 `127.0.0.1:9094`。

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

## 手工投递

先在配置中填入真实渠道凭据并启动服务，然后从仓库目录执行：

```sh
curl -i -H 'Content-Type: application/json' --data-binary @test_payload.json http://127.0.0.1:9094/api/v2/alerts
```

测试载荷的 `endsAt` 在未来，因此会被判定为 firing。重复执行会受 `repeat_interval` 抑制。测试 resolved 时，把 `endsAt` 改成过去的 RFC3339 时间；恢复成功后状态条目会删除，再发 firing 可立即通知。

## 全局静音

下面的命令静音两小时。静音期间 firing 和 resolved 都不投递，但去重状态仍正常更新，静音结束不会补发积压通知。

```sh
echo $(($(date +%s)+7200)) | sudo tee /run/vlalert-mute
```

提前解除静音可执行 `sudo rm /run/vlalert-mute`。文件内容必须是未来的 Unix 时间戳。

## 运维注意事项

- 所有出站请求默认 5 秒超时，可用 `request_timeout` 调整。投递失败会按 2 秒、5 秒间隔再试两次；完全失败不会推进 `last_sent`。
- `heartbeat_url` 非空时，独立线程每 60 秒 GET 一次该地址。
- 状态通过同目录临时文件和 `os.replace()` 原子写入；超过 `state_ttl` 的记录在落盘前清理。
- 本服务自己的失败日志可能包含 `ERROR`。如果这些日志也被 VictoriaLogs 采集，而告警规则匹配 `ERROR`，会形成“投递失败 → 日志告警 → 再次投递失败”的自激风暴。务必在日志采集侧排除本服务，或在 vmalert LogsQL 规则中明确排除它。
