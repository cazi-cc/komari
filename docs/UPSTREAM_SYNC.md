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
- TCP 质量目录仍只在后端私有缓存，公开接口不包含目标 IP、域名或端口；
- TCP 质量任务仍按资源门槛限频、每节点最多 4 并发且禁止重叠运行；
- TCP 评分仍由服务端预计算，并能读取当前主题的 v3 评分参数；
- 后台一键命令、Agent 安装器、自更新和 Docker 镜像仍只指向
  `cazi-cc/komari-agent`，并默认保留 `--disable-web-ssh`；
- 管理前端仍从 `cazi-cc/komari-web` 构建，且通过后端分发清单取得
  Agent 参数，不恢复上游硬编码；
- 自定义镜像构建成功后再滚动替换生产容器。
