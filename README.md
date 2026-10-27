# MaiBot Reply Meme Plugin

从 `aigengtu.com` 的公开图库按关键词、情绪和角色筛选表情包，发送到**工具调用所在的聊天流**。

## 特性与边界

- 仅访问 `aigengtu.com` 与 `img.aigengtu.com` 的 HTTPS 资源，手动校验每一次跳转。
- 限制索引和图片大小、图片格式与尺寸；不把图库元数据当成指令。
- `mode=reply` 遵循单会话冷却；`mode=request` 只应在用户明确点图时使用。
- 只经 MaiBot `send.image` 投递给当前 `stream_id`，没有任意收件人参数；发送未确认时不会重试。

## 安装

复制仓库目录到 MaiBot `plugins/` 后重启 Core。插件运行器会按 `_manifest.json` 安装 `aiohttp` 依赖。
