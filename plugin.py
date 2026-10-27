"""Fetch a relevant aigengtu.com meme and send it to the invoking MaiBot chat."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import random
import re
import time
from typing import Any, ClassVar
from urllib.parse import urljoin, urlparse

import aiohttp
from maibot_sdk import MaiBotPlugin, PluginConfigBase, Tool
from maibot_sdk.types import ToolParameterInfo, ToolParamType
from pydantic import Field


INDEX_URL = "https://aigengtu.com/gallery-index.json"
ALLOWED_HOSTS = {"aigengtu.com", "img.aigengtu.com"}
MAX_BYTES = 12 * 1024 * 1024
ROLE_ALIASES = {
    "deepseek": ("deepseek娘", "deepseek", "鲸鱼娘", "蓝色大肥鱼", "大肥鱼"),
    "claude": ("claude娘", "claude"), "gpt": ("gpt娘", "chatgpt", "gpt"),
    "豆包": ("豆包", "豆大妈"), "kimi": ("kimi娘", "kimi"),
    "通义千问": ("通义千问", "通义", "千问", "qwen"),
    "智谱清言": ("智谱清言", "智谱", "glm"), "gemini": ("gemini娘", "gemini"),
    "grok": ("grok娘", "grok"), "ai娘化": ("ai娘化",),
}


class PluginSectionConfig(PluginConfigBase):
    """MaiBot's mandatory versioned plugin section."""

    enabled: bool = Field(default=True, description="是否启用回复配图工具")
    config_version: str = Field(default="1.0.2", description="插件配置版本")


class MemeSettings(PluginConfigBase):
    auto_reply: bool = Field(default=True, description="允许模型在轻松对话中主动配一张图")
    cooldown_seconds: int = Field(default=60, ge=0, le=3600, description="同一聊天流主动配图冷却时间")
    default_count: int = Field(default=1, ge=1, le=3, description="明确点图时默认张数")
    max_count: int = Field(default=3, ge=1, le=3, description="单次点图最大张数")
    default_character: str = Field(default="auto", description="默认角色；auto 表示不限")


class MemeConfig(PluginConfigBase):
    plugin: PluginSectionConfig = Field(default_factory=PluginSectionConfig)
    meme: MemeSettings = Field(default_factory=MemeSettings)


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


class ReplyMemePlugin(MaiBotPlugin):
    config_model: ClassVar[type[PluginConfigBase] | None] = MemeConfig

    def __init__(self) -> None:
        super().__init__()
        self._gallery: list[dict[str, str]] = []
        self._gallery_at = 0.0
        self._gallery_lock = asyncio.Lock()
        self._last_auto: dict[str, float] = {}
        self._seen: dict[str, list[str]] = {}
        self._running: set[str] = set()

    async def on_load(self) -> None:
        """Required MaiBot lifecycle hook; initializes only in-memory caches."""
        self.ctx.logger.info("回复配图工具已加载")

    async def on_unload(self) -> None:
        """Required MaiBot lifecycle hook; no network session survives between calls."""
        self._running.clear()
        self.ctx.logger.info("回复配图工具已卸载")

    async def on_config_update(self, scope: str, config_data: dict[str, Any], version: str) -> None:
        """Apply WebUI changes to this plugin's own configuration."""
        if scope == "self":
            self.set_plugin_config(config_data)

    async def _read(self, session: aiohttp.ClientSession, url: str, limit: int) -> bytes:
        current = _trusted_url(url)
        for _ in range(4):
            async with session.get(current, allow_redirects=False) as response:
                if response.status in {301, 302, 303, 307, 308}:
                    location = response.headers.get("Location")
                    if not location:
                        raise ValueError("图源跳转缺少地址")
                    current = _trusted_url(urljoin(current, location))
                    continue
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
        raise ValueError("图源跳转次数过多")

    async def _load_gallery(self) -> list[dict[str, str]]:
        if self._gallery and time.monotonic() - self._gallery_at < 900:
            return self._gallery
        async with self._gallery_lock:
            if self._gallery and time.monotonic() - self._gallery_at < 900:
                return self._gallery
            timeout = aiohttp.ClientTimeout(total=12)
            async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": "maibot-reply-meme/1.0"}) as session:
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

    @Tool(
        name="find_meme",
        platforms="qq",
        description="从 aigengtu.com 选择贴切表情包并只发送到当前聊天流。mode=request 仅用于用户明确要图；mode=reply 只用于轻松聊天，严肃或技术内容不要调用。每次最多调用一次，发送成功后继续原本的文字回复。",
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
        if stream_id in self._running: return {"success": True, "sent": 0, "skipped": True, "reason": "当前会话已有配图任务"}
        if mode == "reply" and not self.config.meme.auto_reply: return {"success": True, "sent": 0, "skipped": True, "reason": "主动配图已关闭"}
        if mode == "reply" and time.monotonic() - self._last_auto.get(stream_id, 0) < self.config.meme.cooldown_seconds:
            return {"success": True, "sent": 0, "skipped": True, "reason": "主动配图冷却中"}
        self._running.add(stream_id)
        try:
            wanted = 1 if mode == "reply" else max(1, min(int(count or self.config.meme.default_count), self.config.meme.max_count))
            candidates = self._rank(await self._load_gallery(), keyword[:120], emotion[:40], character[:40], mode == "request" and bool(random))
            if not candidates: return {"success": True, "sent": 0, "skipped": True, "reason": "没有贴切表情包，继续文字回复"}
            unseen = [item for item in candidates if hashlib.sha256(item["original"].encode()).hexdigest() not in self._seen.get(stream_id, [])]
            queue = unseen or candidates
            if mode == "request" and random: random.shuffle(queue)
            sent: list[dict[str, str]] = []
            timeout = aiohttp.ClientTimeout(total=12)
            async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": "maibot-reply-meme/1.0"}) as session:
                for item in queue[:10]:
                    if len(sent) >= wanted: break
                    try:
                        data = await self._read(session, item["original"], MAX_BYTES)
                        size = _image_size(data)
                        if not size or not all(0 < side <= 20000 for side in size): continue
                        if await self.ctx.send.image(base64.b64encode(data).decode("ascii"), stream_id) is not True:
                            return {"success": False, "sent": len(sent), "sendUnconfirmed": True, "error": "平台未确认图片发送；不会自动重发"}
                    except Exception as exc:
                        self.ctx.logger.debug("跳过不可用表情包: %s", exc)
                        continue
                    digest = hashlib.sha256(item["original"].encode()).hexdigest()
                    self._seen.setdefault(stream_id, []).append(digest)
                    self._seen[stream_id] = self._seen[stream_id][-80:]
                    sent.append({"title": item["title"], "source": item["original"]})
            if mode == "reply" and sent: self._last_auto[stream_id] = time.monotonic()
            return {"success": bool(sent), "sent": len(sent), "requested": wanted, "partial": len(sent) < wanted, "images": sent}
        except Exception as exc:
            self.ctx.logger.warning("表情包工具失败: %s", exc)
            return {"success": False, "sent": 0, "error": f"配图未完成：{exc}"}
        finally:
            self._running.discard(stream_id)


def create_plugin() -> ReplyMemePlugin:
    return ReplyMemePlugin()
