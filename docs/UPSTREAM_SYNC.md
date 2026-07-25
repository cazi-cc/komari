# 上游更新维护

本 fork 保留 `upstream=https://github.com/komari-monitor/komari.git`，自定义功能使用
独立提交维护，不把上游代码复制成大块重写。

每周和手动触发的 `Sync Upstream` 工作流会：

1. 从 `upstream/main` 合并到 `sync/upstream-main`；
2. 运行完整 `go test ./...`；
3. 更新一个待人工检查的 Pull Request；
4. 不自动部署生产环境。

合并前重点检查：

- 访客真实 IP、可信代理、归属地、Telegram、白名单和封禁；
- 公开接口不返回监测目标、客户端私有地址或 DNS 配置；
- 旧 agent 的单值 ping 上报仍兼容；
- 新 agent 的多样本损失率和 HTTP 状态指标可入库；
- 自定义镜像构建成功后再滚动替换生产容器。
