"""受限的远端文件抓取器。

该模块只处理用户提供的公开 HTTP(S) 地址。它与上游 Grok 会话完全隔离，
不会携带 Cookie、Authorization、Cloudflare clearance 或代理池凭证。
"""

from __future__ import annotations

import asyncio
import ipaddress
import mimetypes
import re
import socket
import ssl
from dataclasses import dataclass
from pathlib import PurePosixPath
from urllib.parse import unquote, urljoin, urlparse, urlunparse

import aiohttp
import certifi
from aiohttp.abc import AbstractResolver, ResolveResult

from app.platform.errors import UpstreamError, ValidationError


_DEFAULT_MAX_BYTES = 20 * 1024 * 1024
_DEFAULT_MAX_REDIRECTS = 5
_MAX_URL_LENGTH = 4096
_SAFE_PORTS = {80, 443}
_BLOCKED_HOST_SUFFIXES = (".localhost", ".local", ".internal")
_FILENAME_SAFE_RE = re.compile(r"[^A-Za-z0-9._ -]+")

# ipaddress.is_global 会随 Python 的地址分类数据库演进；显式保留这些范围，
# 确保 NAT64、基准测试网段和文档网段不会因运行时差异而放行。
_BLOCKED_NETWORKS = tuple(
    ipaddress.ip_network(value)
    for value in (
        "0.0.0.0/8",
        "10.0.0.0/8",
        "100.64.0.0/10",
        "127.0.0.0/8",
        "169.254.0.0/16",
        "172.16.0.0/12",
        "192.0.0.0/24",
        "192.0.2.0/24",
        "192.168.0.0/16",
        "198.18.0.0/15",
        "198.51.100.0/24",
        "203.0.113.0/24",
        "224.0.0.0/4",
        "240.0.0.0/4",
        "::/128",
        "::1/128",
        "64:ff9b::/96",
        "64:ff9b:1::/48",
        "100::/64",
        "2001::/23",
        "2001:db8::/32",
        "2002::/16",
        "3fff::/20",
        "5f00::/16",
        "fc00::/7",
        "fe80::/10",
        "ff00::/8",
    )
)

_SIGNED_MIME_TYPES = {
    "image/jpeg",
    "image/png",
    "image/gif",
    "image/webp",
    "image/avif",
    "application/pdf",
    "application/zip",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "audio/mpeg",
    "audio/wav",
    "audio/ogg",
    "audio/webm",
    "video/mp4",
    "video/webm",
}
_TEXT_MIME_TYPES = {
    "text/plain",
    "text/csv",
    "text/markdown",
    "application/json",
}
_GENERIC_MIME_TYPES = {"", "application/octet-stream", "binary/octet-stream"}

_fetch_slots: asyncio.Semaphore | None = None


def _get_fetch_slots() -> asyncio.Semaphore:
    """返回跨请求共享的远端抓取并发门限。"""
    global _fetch_slots
    if _fetch_slots is None:
        _fetch_slots = asyncio.Semaphore(4)
    return _fetch_slots


@dataclass(frozen=True, slots=True)
class RemoteAsset:
    """已验证的远端文件内容。"""

    content: bytes
    filename: str
    mime_type: str
    final_url: str


def _normalized_ip(value: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address:
    """解析地址，并将 IPv4-mapped IPv6 归一化为 IPv4。"""
    try:
        address = ipaddress.ip_address(value.split("%", 1)[0])
    except ValueError as exc:
        raise ValidationError("Remote URL resolved to an invalid IP address", param="content") from exc
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
        return address.ipv4_mapped
    return address


def is_public_address(value: str) -> bool:
    """判断地址是否为可公开路由的单播地址。"""
    address = _normalized_ip(value)
    if not address.is_global:
        return False
    return not any(address in network for network in _BLOCKED_NETWORKS if address.version == network.version)


def validate_public_http_url(value: str) -> str:
    """规范化并验证公开 HTTP(S) URL。"""
    if not isinstance(value, str) or not value.strip():
        raise ValidationError("Remote URL is empty", param="content")
    if len(value) > _MAX_URL_LENGTH:
        raise ValidationError("Remote URL is too long", param="content")

    try:
        parsed = urlparse(value.strip())
        port = parsed.port
    except ValueError as exc:
        raise ValidationError("Remote URL contains an invalid port", param="content") from exc

    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        raise ValidationError("Remote URL must use http or https", param="content")
    if parsed.username is not None or parsed.password is not None:
        raise ValidationError("Remote URL must not contain user information", param="content")
    if "%" in parsed.hostname:
        raise ValidationError("Remote URL must not contain an IPv6 zone identifier", param="content")

    host = parsed.hostname.rstrip(".").lower()
    if host == "localhost" or host.endswith(_BLOCKED_HOST_SUFFIXES):
        raise ValidationError("Remote URL host is not public", param="content")
    if port is not None and port not in _SAFE_PORTS:
        raise ValidationError("Remote URL port must be 80 or 443", param="content")

    # IP 字面量在连接器中可能绕过 resolver，因此必须在 URL 入口单独校验。
    try:
        literal = ipaddress.ip_address(host)
    except ValueError:
        literal = None
    if literal is not None and not is_public_address(str(literal)):
        raise ValidationError("Remote URL host is not public", param="content")

    netloc_host = f"[{host}]" if ":" in host else host
    netloc = f"{netloc_host}:{port}" if port is not None else netloc_host
    path = parsed.path or "/"
    return urlunparse((parsed.scheme.lower(), netloc, path, parsed.params, parsed.query, ""))


class PublicAddressResolver(AbstractResolver):
    """只返回全部通过公开地址校验的 DNS 结果。"""

    def __init__(self, delegate: AbstractResolver | None = None) -> None:
        self._delegate = delegate or aiohttp.resolver.DefaultResolver()

    async def resolve(
        self,
        host: str,
        port: int = 0,
        family: socket.AddressFamily = socket.AF_INET,
    ) -> list[ResolveResult]:
        """解析主机名，并在连接前阻断任一非公开地址。"""
        results = await self._delegate.resolve(host, port, family)
        if not results:
            raise OSError(f"DNS returned no address for {host}")
        if any(not is_public_address(item["host"]) for item in results):
            raise ValidationError("Remote URL resolves to a non-public address", param="content")
        return results

    async def close(self) -> None:
        """释放底层解析器资源。"""
        await self._delegate.close()


def _ssl_context() -> ssl.SSLContext:
    """创建使用项目 CA 集的 TLS 上下文。"""
    context = ssl.create_default_context()
    context.load_verify_locations(certifi.where())
    return context


async def _read_limited_body(response: aiohttp.ClientResponse, max_bytes: int) -> bytes:
    """流式读取响应，并在超过上限时立即终止。"""
    content_length = response.content_length
    if content_length is not None and content_length > max_bytes:
        raise ValidationError("Remote file exceeds the size limit", param="content")

    chunks: list[bytes] = []
    size = 0
    async for chunk in response.content.iter_chunked(64 * 1024):
        size += len(chunk)
        if size > max_bytes:
            raise ValidationError("Remote file exceeds the size limit", param="content")
        chunks.append(chunk)
    if size == 0:
        raise ValidationError("Remote file is empty", param="content")
    return b"".join(chunks)


def _sniff_mime(content: bytes) -> str | None:
    """基于文件签名识别允许上传的二进制格式。"""
    if content.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if content.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if content.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if len(content) >= 12 and content.startswith(b"RIFF") and content[8:12] == b"WEBP":
        return "image/webp"
    if len(content) >= 12 and content[4:12] in {b"ftypavif", b"ftypavis"}:
        return "image/avif"
    if content.startswith(b"%PDF-"):
        return "application/pdf"
    if content.startswith((b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")):
        return "application/zip"
    if content.startswith(b"ID3") or (len(content) >= 2 and content[0] == 0xFF and content[1] & 0xE0 == 0xE0):
        return "audio/mpeg"
    if len(content) >= 12 and content.startswith(b"RIFF") and content[8:12] == b"WAVE":
        return "audio/wav"
    if content.startswith(b"OggS"):
        return "audio/ogg"
    if content.startswith(b"\x1aE\xdf\xa3"):
        return "video/webm"
    if len(content) >= 12 and content[4:8] == b"ftyp":
        return "video/mp4"
    return None


def _normalize_mime(value: str) -> str:
    """规范化 Content-Type 中的 MIME 主类型。"""
    mime = value.split(";", 1)[0].strip().lower()
    aliases = {
        "image/jpg": "image/jpeg",
        "audio/mp3": "audio/mpeg",
        "audio/x-wav": "audio/wav",
        "application/x-zip-compressed": "application/zip",
        "text/json": "application/json",
    }
    return aliases.get(mime, mime)


def validate_remote_content(content: bytes, declared_mime: str) -> str:
    """校验远端内容类型与文件签名，并返回可信 MIME。"""
    declared = _normalize_mime(declared_mime)
    detected = _sniff_mime(content)

    if declared in _TEXT_MIME_TYPES:
        prefix = content[:512].lstrip().lower()
        if b"\x00" in content[:4096] or prefix.startswith((b"<!doctype html", b"<html", b"<script")):
            raise ValidationError("Remote text content is not an allowed file type", param="content")
        try:
            content[:4096].decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValidationError("Remote text content is not valid UTF-8", param="content") from exc
        return declared

    if declared in _SIGNED_MIME_TYPES:
        if detected is None:
            raise ValidationError("Remote file signature does not match its content type", param="content")
        if declared.startswith("application/vnd.openxmlformats-") and detected == "application/zip":
            return declared
        if declared == "audio/webm" and detected == "video/webm":
            return declared
        if detected != declared:
            raise ValidationError("Remote file signature does not match its content type", param="content")
        return declared

    if declared in _GENERIC_MIME_TYPES and detected is not None:
        return detected
    raise ValidationError("Remote file content type is not allowed", param="content")


def _safe_filename(url: str, mime_type: str) -> str:
    """从最终 URL 生成不含路径控制字符的文件名。"""
    raw_name = unquote(PurePosixPath(urlparse(url).path).name).strip()
    name = _FILENAME_SAFE_RE.sub("_", raw_name)[:160].strip(". ")
    if not name:
        extension = mimetypes.guess_extension(mime_type) or ".bin"
        return f"download{extension}"
    return name


async def fetch_remote_asset(
    url: str,
    *,
    max_bytes: int = _DEFAULT_MAX_BYTES,
    max_redirects: int = _DEFAULT_MAX_REDIRECTS,
) -> RemoteAsset:
    """抓取公开远端文件，逐跳校验重定向、DNS、大小和文件签名。"""
    if max_bytes <= 0 or max_redirects < 0:
        raise ValueError("max_bytes must be positive and max_redirects must not be negative")
    async with _get_fetch_slots():
        return await _fetch_remote_asset_inner(
            url,
            max_bytes=max_bytes,
            max_redirects=max_redirects,
        )


async def _fetch_remote_asset_inner(
    url: str,
    *,
    max_bytes: int,
    max_redirects: int,
) -> RemoteAsset:
    """在已取得并发槽后执行一次受限抓取。"""

    current_url = validate_public_http_url(url)
    resolver = PublicAddressResolver()
    connector = aiohttp.TCPConnector(
        resolver=resolver,
        family=socket.AF_UNSPEC,
        use_dns_cache=False,
        force_close=True,
        limit=4,
        ssl=_ssl_context(),
    )
    timeout = aiohttp.ClientTimeout(total=30.0, connect=10.0, sock_read=15.0)
    headers = {
        "Accept": ", ".join(sorted(_SIGNED_MIME_TYPES | _TEXT_MIME_TYPES)),
        "User-Agent": "grok2api-remote-fetch/1.0",
    }

    try:
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            for redirect_count in range(max_redirects + 1):
                async with session.get(current_url, headers=headers, allow_redirects=False) as response:
                    if response.status in {301, 302, 303, 307, 308}:
                        location = response.headers.get("Location", "").strip()
                        if not location:
                            raise UpstreamError("Remote URL returned a redirect without Location", status=502)
                        if redirect_count >= max_redirects:
                            raise UpstreamError("Remote URL exceeded the redirect limit", status=502)
                        current_url = validate_public_http_url(urljoin(current_url, location))
                        continue
                    if response.status != 200:
                        raise UpstreamError(
                            f"Failed to fetch input URL: {response.status}",
                            status=response.status,
                        )

                    content = await _read_limited_body(response, max_bytes)
                    mime_type = validate_remote_content(
                        content,
                        response.headers.get("Content-Type", ""),
                    )
                    return RemoteAsset(
                        content=content,
                        filename=_safe_filename(current_url, mime_type),
                        mime_type=mime_type,
                        final_url=current_url,
                    )
    except (UpstreamError, ValidationError):
        raise
    except (aiohttp.ClientError, TimeoutError, OSError) as exc:
        raise UpstreamError(f"Asset fetch transport error: {exc}", status=502) from exc
    finally:
        await resolver.close()

    raise UpstreamError("Remote URL did not produce a file", status=502)


__all__ = [
    "PublicAddressResolver",
    "RemoteAsset",
    "fetch_remote_asset",
    "is_public_address",
    "validate_public_http_url",
    "validate_remote_content",
]
