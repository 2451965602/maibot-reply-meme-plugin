"""从 aigengtu.com 筛选表情包候选，并发送到发起工具调用的 MaiBot 聊天流。"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import random
import re
import secrets
import time
from collections import OrderedDict
from typing import Any, ClassVar
from urllib.parse import urljoin, urlparse

import aiohttp
from maibot_sdk import MaiBotPlugin, PluginConfigBase, Tool
from maibot_sdk.types import ToolParameterInfo, ToolParamType
from pydantic import Field


INDEX_URL = "https://aigengtu.com/gallery-index.json"
ALLOWED_HOSTS = {"aigengtu.com", "img.aigengtu.com"}
MAX_BYTES = 12 * 1024 * 1024
MAX_CANDIDATE_BYTES = 1024 * 1024
MAX_SELECTIONS = 16
MAX_SELECTION_IMAGES = 4
SELECTION_TTL = 300.0
SEEN_DIGEST_LIMIT = 80
# 不低于 cooldown_seconds 的上限（3600 秒），因此按空闲时间回收不会提前解除主动配图冷却。
STREAM_CACHE_TTL = 3600.0
MAX_STREAM_CACHES = 512
ROLE_ALIASES = {
    "deepseek": ("deepseek娘", "deepseek", "鲸鱼娘", "蓝色大肥鱼", "大肥鱼"),
    "claude": ("claude娘", "claude"), "gpt": ("gpt娘", "chatgpt", "gpt"),
    "豆包": ("豆包", "豆大妈"), "kimi": ("kimi娘", "kimi"),
    "通义千问": ("通义千问", "通义", "千问", "qwen"),
    "智谱清言": ("智谱清言", "智谱", "glm"), "gemini": ("gemini娘", "gemini"),
    "grok": ("grok娘", "grok"), "ai娘化": ("ai娘化",),
}


class PluginSectionConfig(PluginConfigBase):
    """MaiBot 要求的带版本插件配置。"""

    enabled: bool = Field(default=True, description="是否启用本插件的表情包候选与发送工具")
    config_version: str = Field(default="1.0.5", description="插件配置结构版本；由插件用于配置兼容管理")


class MemeSettings(PluginConfigBase):
    auto_reply: bool = Field(default=True, description="允许模型在轻松对话中主动查找配图；严肃或技术内容不应主动调用")
    cooldown_seconds: int = Field(default=60, ge=0, le=3600, description="同一聊天流主动配图的冷却秒数，范围 0–3600")
    default_count: int = Field(default=1, ge=1, le=3, description="兼容配置项；候选流程每次最多返回四张，最终只发送一张")
    max_count: int = Field(default=3, ge=1, le=3, description="兼容配置项；候选流程每次最多返回四张，最终只发送一张")
    default_character: str = Field(default="auto", description="默认角色筛选；auto 表示不限定角色")


class MemeConfig(PluginConfigBase):
    plugin: PluginSectionConfig = Field(default_factory=PluginSectionConfig, title="插件设置", description="插件启用状态及配置版本")
    meme: MemeSettings = Field(default_factory=MemeSettings, title="回复配图设置", description="主动配图冷却与候选筛选选项")


def _trusted_url(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme != "https" or parsed.hostname not in ALLOWED_HOSTS or parsed.username or parsed.password or parsed.port not in (None, 443):
        raise ValueError("图源地址不在 aigengtu.com 白名单内")
    return value


def _image_size(data: bytes) -> tuple[int, int] | None:
    if len(data) >= 24 and data.startswith(b"\x89PNG\r\n\x1a\n"):
        return int.from_bytes(data[16:20], "big"), int.from_bytes(data[20:24], "big")
    if len(data) >= 10 and data[:4] == b"GIF8":
        return int.from_bytes(data[6:8], "little"), int.from_bytes(data[8:10], "little")
    if len(data) >= 30 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        kind = data[12:16]
        if kind == b"VP8X": return 1 + int.from_bytes(data[24:27], "little"), 1 + int.from_bytes(data[27:30], "little")
        if kind == b"VP8 ": return int.from_bytes(data[26:28], "little") & 0x3FFF, int.from_bytes(data[28:30], "little") & 0x3FFF
    if len(data) >= 10 and data[:2] == b"\xff\xd8":
        index = 2
        while index + 9 < len(data):
            if data[index] != 0xFF:
                index += 1; continue
            marker = data[index + 1]
            if marker in (0xD8, 0x01) or 0xD0 <= marker <= 0xD7:
                index += 2; continue
            length = int.from_bytes(data[index + 2:index + 4], "big")
            if length < 2 or index + length + 2 > len(data): break
            if marker in (*range(0xC0, 0xC4), *range(0xC5, 0xC8), *range(0xC9, 0xCC), *range(0xCD, 0xD0)):
                return int.from_bytes(data[index + 7:index + 9], "big"), int.from_bytes(data[index + 5:index + 7], "big")
            index += length + 2
    return None


def _image_mime_type(data: bytes) -> str | None:
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith(b"\xff\xd8"):
        return "image/jpeg"
    if data.startswith(b"GIF8"):
        return "image/gif"
    if data.startswith(b"RIFF") and data[8:12] == b"WEBP":
        return "image/webp"
    return None


class _StreamCache:
    """按 stream_id 保存去重摘要和主动配图时间，并统一做惰性回收。

    - 空闲超过 ``ttl`` 秒的聊天流在下次访问时被整体丢弃；
    - 聊天流条目数超过 ``limit`` 时淘汰最久未写入的聊天流；
    - 单个聊天流最多保留最近 ``SEEN_DIGEST_LIMIT`` 条去重摘要。

    两类缓存共用同一份存活记录，任何一侧写入都会刷新空闲计时，避免两份
    dict 各自增长或彼此不同步。上限只是兜底：若同一时刻活跃流多于
    ``limit``，最久未写入的流会重新参与候选，冷却也可能提前复位。
    """

    def __init__(self, *, ttl: float = STREAM_CACHE_TTL, limit: int = MAX_STREAM_CACHES) -> None:
        self._ttl = ttl
        self._limit = limit
        self._seen: dict[str, list[str]] = {}
        self._last_auto: dict[str, float] = {}
        self._touched: dict[str, float] = {}

    def prune(self, now: float | None = None) -> None:
        """惰性回收：先按空闲时间 TTL，再按条目数硬上限。"""
        now = time.monotonic() if now is None else now
        for stream_id, touched in list(self._touched.items()):
            if now - touched > self._ttl:
                self._forget(stream_id)
        overflow = len(self._touched) - self._limit
        if overflow > 0:
            for stream_id, _ in sorted(self._touched.items(), key=lambda pair: pair[1])[:overflow]:
                self._forget(stream_id)

    def seen(self, stream_id: str) -> list[str]:
        return self._seen.get(stream_id, [])

    def last_auto(self, stream_id: str) -> float:
        return self._last_auto.get(stream_id, 0.0)

    def remember(self, stream_id: str, digest: str, now: float | None = None) -> None:
        now = time.monotonic() if now is None else now
        digests = self._seen.setdefault(stream_id, [])
        digests.append(digest)
        del digests[:-SEEN_DIGEST_LIMIT]
        self._touch(stream_id, now)
        self.prune(now)

    def mark_auto(self, stream_id: str, now: float | None = None) -> None:
        now = time.monotonic() if now is None else now
        self._last_auto[stream_id] = self._touch(stream_id, now)
        self.prune(now)

    def _touch(self, stream_id: str, now: float | None = None) -> float:
        now = time.monotonic() if now is None else now
        self._touched[stream_id] = now
        return now

    def _forget(self, stream_id: str) -> None:
        self._touched.pop(stream_id, None)
        self._seen.pop(stream_id, None)
        self._last_auto.pop(stream_id, None)


class _MemeSelection:
    def __init__(self, stream_id: str, mode: str, candidates: list[dict[str, Any]], created: float) -> None:
        self.stream_id = stream_id
        self.mode = mode
        self.candidates = candidates
        self.created = created


class ReplyMemePlugin(MaiBotPlugin):
    config_model: ClassVar[type[PluginConfigBase] | None] = MemeConfig

    def __init__(self) -> None:
        super().__init__()
        self._gallery: list[dict[str, str]] = []
        self._gallery_at = 0.0
        self._gallery_lock = asyncio.Lock()
        self._cache = _StreamCache()
        self._running: set[str] = set()
        self._selections: OrderedDict[str, _MemeSelection] = OrderedDict()

    async def on_load(self) -> None:
        """Required MaiBot lifecycle hook; initializes only in-memory caches."""
        self.ctx.logger.info("回复配图工具已加载")

    async def on_unload(self) -> None:
        """Required MaiBot lifecycle hook; no network session survives between calls."""
        self._running.clear()
        self._selections.clear()
        self.ctx.logger.info("回复配图工具已卸载")

    async def on_config_update(self, scope: str, config_data: dict[str, Any], version: str) -> None:
        """Apply WebUI changes to this plugin's own configuration."""
        if scope == "self":
            self.set_plugin_config(config_data)

    async def _read(self, session: aiohttp.ClientSession, url: str, limit: int) -> bytes:
        current = _trusted_url(url)
        for _ in range(5):
            for attempt in range(3):
                try:
                    async with session.get(current, allow_redirects=False) as response:
                        if response.status in {301, 302, 303, 307, 308}:
                            location = response.headers.get("Location")
                            if not location:
                                raise ValueError("图源跳转缺少地址")
                            current = _trusted_url(urljoin(current, location))
                            break
                        if response.status != 200:
                            raise ValueError(f"图源 HTTP {response.status}")
                        if int(response.headers.get("Content-Length") or 0) > limit:
                            raise ValueError("图源文件超过大小限制")
                        chunks: list[bytes] = []
                        total = 0
                        async for chunk in response.content.iter_chunked(65536):
                            total += len(chunk)
                            if total > limit:
                                raise ValueError("图源文件超过大小限制")
                            chunks.append(chunk)
                        if not chunks:
                            raise ValueError("图源文件为空")
                        return b"".join(chunks)
                except (
                    aiohttp.ClientConnectionResetError,
                    aiohttp.ClientOSError,
                    aiohttp.ClientConnectorError,
                    aiohttp.ServerDisconnectedError,
                    asyncio.TimeoutError,
                ) as exc:
                    if attempt == 2:
                        raise ValueError("图源连接失败，请继续文字回复") from exc
                    await asyncio.sleep(0.1 * (attempt + 1))
        raise ValueError("图源跳转次数过多")

    async def _load_gallery(self, session: aiohttp.ClientSession) -> list[dict[str, str]]:
        if self._gallery and time.monotonic() - self._gallery_at < 900:
            return self._gallery
        async with self._gallery_lock:
            if self._gallery and time.monotonic() - self._gallery_at < 900:
                return self._gallery
            raw = await self._read(session, INDEX_URL, 5 * 1024 * 1024)
            import json
            document = json.loads(raw.decode("utf-8"))
            gallery = document.get("gallery")
            if not isinstance(gallery, dict):
                raise ValueError("图库索引格式已变更")
            items: list[dict[str, str]] = []
            seen: set[str] = set()
            for category, group in gallery.items():
                images = group.get("images") if isinstance(group, dict) else None
                if not isinstance(images, list): continue
                for image in images:
                    if not isinstance(image, dict) or not isinstance(image.get("original"), str): continue
                    try:
                        original = _trusted_url(image["original"])
                    except ValueError:
                        continue
                    if not re.search(r"/meme/\d+\.(?:webp|png|jpe?g|gif)$", urlparse(original).path, re.I) or original in seen:
                        continue
                    seen.add(original)
                    items.append({"original": original, "title": str(image.get("name") or image.get("alt") or "")[:300], "alt": str(image.get("alt") or "")[:300], "story": str(image.get("story") or "")[:1600], "category": str(category)[:80]})
                    if len(items) >= 20000: break
            if not items: raise ValueError("图库没有可用图片")
            self._gallery, self._gallery_at = items, time.monotonic()
            return items

    def _rank(self, items: list[dict[str, str]], keyword: str, emotion: str, character: str, random_pick: bool) -> list[dict[str, str]]:
        query = f"{keyword} {emotion}".lower().strip()
        requested_role = (character or self.config.meme.default_character or "auto").lower().strip()
        role = None
        if requested_role not in {"", "auto", "不限", "全部"}:
            for canonical, aliases in ROLE_ALIASES.items():
                if requested_role == canonical or requested_role in aliases:
                    role = canonical; break
            if role is None: raise ValueError("不支持的角色筛选")
        terms = [part for part in re.split(r"[\s,，、;；。！!？?]+", query) if part and part not in {"表情", "表情包", "梗图", "图片", "来一张"}][:6]
        if not terms and not role and not random_pick:
            return []
        ranked: list[tuple[int, dict[str, str]]] = []
        for item in items:
            if role and item["category"].lower() != role: continue
            text = f'{item["title"]} {item["alt"]} {item["story"]}'.lower()
            hits = sum(term in text for term in terms)
            if terms and not hits: continue
            ranked.append((hits * 10 + (5 if any(term in f'{item["title"]} {item["alt"]}'.lower() for term in terms) else 0), item))
        ranked.sort(key=lambda pair: pair[0], reverse=True)
        return [item for _, item in ranked]

    def _prune_selections(self, now: float | None = None) -> None:
        now = time.monotonic() if now is None else now
        for selection_id, selection in list(self._selections.items()):
            if now - selection.created > SELECTION_TTL:
                self._selections.pop(selection_id, None)

    def _remember_selection(self, stream_id: str, mode: str, candidates: list[dict[str, Any]]) -> str:
        self._prune_selections()
        while len(self._selections) >= MAX_SELECTIONS:
            self._selections.popitem(last=False)
        selection_id = secrets.token_urlsafe(12)
        self._selections[selection_id] = _MemeSelection(stream_id, mode, candidates, time.monotonic())
        return selection_id

    @Tool(
        name="find_meme",
        platforms="qq",
        core_tool=True,
        description="从 aigengtu.com 查找最多四张候选图并以图片内容返回供视觉模型观察；此工具绝不发送图片。成功结果同时含 selection_id 和 candidates 时，Planner 下一步只能按返回候选调用一次 send_meme(selection_id, choice)，禁止 stop、文字回复或重复 find_meme；仅 send_meme 返回 sent=1 才算完成。失败、skipped 或没有 selection_id 时，才可文字回复或 stop。mode=request 仅用于用户明确要图；mode=reply 只用于轻松聊天，严肃或技术内容不要调用。",
        parameters=[
            ToolParameterInfo(name="mode", param_type=ToolParamType.STRING, description="reply=轻松回复配图；request=用户明确点图", required=True),
            ToolParameterInfo(name="keyword", param_type=ToolParamType.STRING, description="1–3 个短关键词", required=False),
            ToolParameterInfo(name="emotion", param_type=ToolParamType.STRING, description="要表达的情绪", required=False),
            ToolParameterInfo(name="character", param_type=ToolParamType.STRING, description="可选角色，如 DeepSeek、Claude、GPT", required=False),
            ToolParameterInfo(name="count", param_type=ToolParamType.INTEGER, description="点图张数，1–3", required=False),
            ToolParameterInfo(name="random", param_type=ToolParamType.BOOLEAN, description="仅当用户明确要求随机时为 true", required=False),
        ],
    )
    async def find_meme(self, mode: str = "", keyword: str = "", emotion: str = "", character: str = "", count: int = 1, random: bool = False, stream_id: str = "", **kwargs: Any) -> dict[str, Any]:
        del kwargs
        if not self.config.plugin.enabled:
            return {"success": False, "sent": 0, "error": "回复配图工具已禁用"}
        if mode not in {"reply", "request"}: return {"success": False, "sent": 0, "error": "mode 必须是 reply 或 request"}
        if not stream_id: return {"success": False, "sent": 0, "error": "缺少当前聊天流"}
        now = time.monotonic()
        self._cache.prune(now)
        self._prune_selections(now)
        if stream_id in self._running: return {"success": True, "sent": 0, "skipped": True, "reason": "当前会话已有配图任务"}
        if mode == "reply" and not self.config.meme.auto_reply: return {"success": True, "sent": 0, "skipped": True, "reason": "主动配图已关闭"}
        if mode == "reply" and now - self._cache.last_auto(stream_id) < self.config.meme.cooldown_seconds:
            return {"success": True, "sent": 0, "skipped": True, "reason": "主动配图冷却中"}
        self._running.add(stream_id)
        try:
            timeout = aiohttp.ClientTimeout(total=12)
            async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": "maibot-reply-meme/1.0"}, connector=aiohttp.TCPConnector(limit=10), trust_env=True) as session:
                candidates = self._rank(await self._load_gallery(session), keyword[:120], emotion[:40], character[:40], mode == "request" and bool(random))
                if not candidates: return {"success": True, "sent": 0, "skipped": True, "reason": "没有贴切表情包，继续文字回复"}
                unseen = [item for item in candidates if hashlib.sha256(item["original"].encode()).hexdigest() not in self._cache.seen(stream_id)]
                queue = unseen or candidates
                if mode == "request" and random: random.shuffle(queue)
                prepared: list[dict[str, Any]] = []
                for item in queue[:10]:
                    if len(prepared) >= MAX_SELECTION_IMAGES: break
                    try:
                        data = await self._read(session, item["original"], MAX_BYTES)
                        size = _image_size(data)
                        mime_type = _image_mime_type(data)
                        if not size or not mime_type or not all(0 < side <= 20000 for side in size): continue
                        if len(data) > MAX_CANDIDATE_BYTES: continue
                    except Exception as exc:
                        self.ctx.logger.debug("跳过不可用表情包: %s", exc)
                        continue
                    digest = hashlib.sha256(item["original"].encode()).hexdigest()
                    prepared.append({"data": data, "title": item["title"], "source": item["original"], "digest": digest, "mime_type": mime_type})
            if not prepared:
                return {
                    "success": True,
                    "sent": 0,
                    "skipped": True,
                    "reason": "没有符合大小及校验条件的新候选图片",
                    "fallback": "继续文字回复；不要调用 send_meme",
                }
            selection_id = self._remember_selection(stream_id, mode, prepared)
            return {
                "success": True,
                "sent": 0,
                "selection_id": selection_id,
                "candidates": [{"choice": index, "title": item["title"]} for index, item in enumerate(prepared, 1)],
                "content_items": [
                    {"type": "image", "data": base64.b64encode(item["data"]).decode("ascii"), "mime_type": item["mime_type"], "name": f"candidate-{index}.{item['mime_type'].split('/')[-1]}"}
                    for index, item in enumerate(prepared, 1)
                ],
                "instruction": "已成功返回 selection_id 和 candidates：请观察候选图片，下一步只能按候选调用一次 send_meme(selection_id, choice)，choice 从 1 开始；禁止 stop、文字回复或重复 find_meme。仅 send_meme 返回 sent=1 才算完成。",
            }
        except Exception as exc:
            self.ctx.logger.warning("表情包工具失败: %s", exc)
            return {
                "success": False,
                "sent": 0,
                "error": f"配图未完成：{exc}",
                "fallback": "继续文字回复；不要调用 send_meme",
            }
        finally:
            self._running.discard(stream_id)

    @Tool(
        name="send_meme",
        platforms="qq",
        core_tool=True,
        description="仅在 find_meme 成功返回 selection_id 和 candidates 后调用一次，用返回的 selection_id 与从 1 开始的 choice 发送一张候选图；该阶段禁止 stop、文字回复或重复 find_meme。仅返回 sent=1 才算完成；失败时才可回退为文字回复或 stop。仅可用于原聊天流且候选 5 分钟内有效。",
        parameters=[
            ToolParameterInfo(name="selection_id", param_type=ToolParamType.STRING, description="find_meme 返回的 selection_id", required=True),
            ToolParameterInfo(name="choice", param_type=ToolParamType.INTEGER, description="所选候选序号，从 1 开始", required=True),
        ],
    )
    async def send_meme(self, selection_id: str = "", choice: int = 0, stream_id: str = "", **kwargs: Any) -> dict[str, Any]:
        del kwargs
        if not self.config.plugin.enabled:
            return {"success": False, "sent": 0, "error": "回复配图工具已禁用"}
        if not selection_id or not stream_id:
            return {"success": False, "sent": 0, "error": "缺少候选编号或当前聊天流"}
        now = time.monotonic()
        self._prune_selections(now)
        selection = self._selections.get(selection_id)
        if selection is None:
            return {"success": False, "sent": 0, "error": "候选已过期或不存在，请重新调用 find_meme"}
        if selection.stream_id != stream_id:
            return {"success": False, "sent": 0, "error": "该候选属于其他聊天流，不能跨聊天发送"}
        try:
            choice = int(choice)
        except (TypeError, ValueError):
            return {"success": False, "sent": 0, "error": "choice 必须是从 1 开始的候选序号"}
        if choice < 1 or choice > len(selection.candidates):
            return {"success": False, "sent": 0, "error": f"choice 超出范围，请选择 1 到 {len(selection.candidates)}"}
        if selection.mode == "reply":
            if not self.config.meme.auto_reply:
                self._selections.pop(selection_id, None)
                return {"success": False, "sent": 0, "error": "主动配图已关闭"}
            if now - self._cache.last_auto(stream_id) < self.config.meme.cooldown_seconds:
                return {"success": False, "sent": 0, "error": "主动配图冷却中，请稍后重试"}
        if stream_id in self._running:
            return {"success": False, "sent": 0, "error": "当前会话已有配图任务，请稍后重试"}
        self._running.add(stream_id)
        self._selections.pop(selection_id, None)
        candidate = selection.candidates[choice - 1]
        try:
            encoded = base64.b64encode(candidate["data"]).decode("ascii")
            if await self.ctx.send.image(encoded, stream_id) is not True:
                return {"success": False, "sent": 0, "sendUnconfirmed": True, "error": "平台未确认图片发送；不会自动重发"}
            self._cache.remember(stream_id, candidate["digest"])
            if selection.mode == "reply": self._cache.mark_auto(stream_id)
            return {"success": True, "sent": 1, "image": {"title": candidate["title"], "source": candidate["source"]}}
        except Exception as exc:
            self.ctx.logger.warning("表情包发送失败: %s", exc)
            return {"success": False, "sent": 0, "error": f"图片发送失败：{exc}", "fallback": "继续文字回复；不要重试发送"}
        finally:
            self._running.discard(stream_id)


def create_plugin() -> ReplyMemePlugin:
    return ReplyMemePlugin()
