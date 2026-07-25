# 扩展探测设计

## 目标

扩展探测把三个容易混淆的结果分开保存：

- `网络可达性`：是否完成 DNS、TCP、TLS 并收到 HTTP 响应。HTTP 403 也属于网络可达。
- `HTTP 状态健康`：响应状态是否符合任务配置，默认接受 200-399。
- `链路质量`：多样本延迟、丢包、抖动、各阶段耗时和连接级 TCP 重传。

监测目标、任务 DNS 服务器和解析出的原始 IP 只存在于管理员任务配置和 agent
内存中。后端不会把解析结果的原始 IP 或短指纹写入指标库；公开指标只保存地址族、
`system`/`custom` DNS 模式和受控错误类别。

## 采样与资源

- 单任务最多 10 个样本，默认 1 个。
- ICMP 负载最多 1400 字节，避免默认制造 IP 分片。
- 单次样本超时最多 10 秒。
- 推荐大小包配置：56 字节和 1200 字节，各 5 个样本，每 5 分钟一次。
- HTTP DNS 对照配置：系统 DNS 与指定公共 DNS，各 5 个样本，每 5 分钟一次。

探测由 agent 定时执行，访客页面只读取后端已缓存的指标，不会因页面访问重复执行。
ICMP 没有“TCP 重传”概念：大小包任务用于观察不同负载下的丢包、抖动和路径 MTU
问题；TCP 重传只从 HTTP/TCP 任务创建的那条连接读取。

## 实现来源

- ICMP 多样本继续复用 agent 已有的
  [prometheus-community/pro-bing](https://github.com/prometheus-community/pro-bing)。
- 设计参考
  [Prometheus blackbox_exporter](https://github.com/prometheus/blackbox_exporter)
  对 HTTP 阶段耗时、ICMP payload size 和状态策略的划分。
- TCP 重传使用本次连接的 Linux `TCP_INFO`，不需要常驻 eBPF、内核头文件或
  `CAP_SYS_ADMIN`。需要观察整机所有连接时，可另行部署
  [iovisor/bcc tcpretrans](https://github.com/iovisor/bcc)，但不作为低资源 VPS
  的默认模式。

## ChatGPT DNS 对照

本实例仅对设置了商家 AI 分流 DNS 的 `Zouter HK Debut 2C 2G`
建立两条同目标任务，不能误绑定到 `Zouter HK Storage 2C 2G` 或其他节点：

1. 系统 DNS：保留 `dns_server` 为空。
2. 公共 DNS：显式设置公共解析器。

两条任务应绑定同一个节点、采用相同 URL、样本数和间隔。比较：

- 网络失败率，而不是把 HTTP 403 误算为丢包；
- HTTP 状态合格率；
- DNS、TCP、TLS、TTFB 和总耗时；
- 连接级 TCP 重传次数。

这能衡量 DNS 分流是否改变路径及稳定性，但不能绕过或替代服务方的地区使用政策。

推荐任务配置：

```json
{
  "sample_count": 5,
  "timeout_ms": 5000,
  "preferred_ip": "4"
}
```

公共 DNS 对照任务在上述配置中增加：

```json
{
  "dns_server": "1.1.1.1:53"
}
```

大小包任务分别增加 `"packet_size": 56` 和 `"packet_size": 1200`，任务类型为
`icmp`，间隔使用 300 秒。
