"""按出口身份和目标域名管理 Cloudflare clearance。"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from urllib.parse import urlparse

from app.platform.config.snapshot import get_config
from app.platform.logging.logger import logger
from app.platform.runtime.clock import now_ms
from app.platform.runtime.ids import next_hex

from .config import resolve_clearance_config
from .models import (
    ClearanceBundle,
    ClearanceBundleState,
    ClearanceMode,
    ProxyFeedback,
    ProxyFeedbackKind,
    ProxyLease,
    ProxyProvider,
    ProxyScope,
    RequestKind,
)
from .providers.flaresolverr import FlareSolverrClearanceProvider
from .providers.manual import ManualClearanceProvider


_DEFAULT_ORIGIN = "https://grok.com"
BundleKey = tuple[str, str, str]


def clearance_host(origin: str | None) -> str:
    """返回 clearance 隔离使用的规范化目标主机。"""
    host = urlparse(origin or _DEFAULT_ORIGIN).hostname
    return (host or "grok.com").lower()


class ProxyClearanceManager:
    """维护 provider、出口身份和 origin 三维隔离的 clearance。"""

    def __init__(self) -> None:
        self._bundles: OrderedDict[BundleKey, ClearanceBundle] = OrderedDict()
        self._targets: dict[BundleKey, tuple[str, str]] = {}
        self._refresh_events: dict[BundleKey, asyncio.Event] = {}
        self._lock = asyncio.Lock()
        self._manual = ManualClearanceProvider()
        self._flare = FlareSolverrClearanceProvider()
        self._mode = ClearanceMode.NONE
        self._config_sig: tuple[object, ...] | None = None

    async def load(self) -> None:
        """热加载 clearance 配置并使旧 bundle 失效。"""
        cfg = get_config()
        mode = ClearanceMode.parse(cfg.get_str("proxy.clearance.mode", "none"))
        clearance = resolve_clearance_config(cfg)
        signature = (
            mode.value,
            cfg.get_str("proxy.egress.mode", "direct"),
            cfg.get_str("proxy.resin.url_template", ""),
            cfg.get_str("proxy.clearance.flaresolverr_url", ""),
            clearance.cf_cookies,
            clearance.user_agent,
            clearance.cf_clearance,
            clearance.browser,
            cfg.get_int("proxy.clearance.timeout_sec", 60),
        )
        async with self._lock:
            if signature == self._config_sig:
                return
            self._mode = mode
            self._config_sig = signature
            self._bundles = OrderedDict(
                (
                    key,
                    bundle.model_copy(
                        update={"state": ClearanceBundleState.INVALID}
                    ),
                )
                for key, bundle in self._bundles.items()
            )

    async def acquire_lease(
        self,
        *,
        proxy_url: str | None,
        affinity_key: str,
        provider: ProxyProvider,
        account_key: str,
        scope: ProxyScope,
        kind: RequestKind,
        clearance_origin: str | None,
    ) -> ProxyLease:
        """为已选定出口生成附带 clearance 的代理租约。"""
        await self.load()
        origin = clearance_origin or _DEFAULT_ORIGIN
        bundle = await self._get_or_build_bundle(
            provider=provider,
            affinity_key=affinity_key,
            proxy_url=proxy_url or "",
            origin=origin,
        )
        return ProxyLease(
            lease_id=next_hex(),
            proxy_url=proxy_url,
            cf_cookies=bundle.cf_cookies if bundle else "",
            user_agent=bundle.user_agent if bundle else "",
            clearance_host=clearance_host(origin),
            scope=scope,
            kind=kind,
            acquired_at=now_ms(),
            provider=provider,
            affinity_key=affinity_key,
            account_key=account_key,
            origin=origin,
        )

    async def feedback(self, lease: ProxyLease, result: ProxyFeedback) -> None:
        """根据目标级挑战反馈使对应 clearance 失效。"""
        if result.kind != ProxyFeedbackKind.CHALLENGE:
            return
        key = (
            lease.provider.value,
            lease.affinity_key,
            lease.clearance_host,
        )
        async with self._lock:
            bundle = self._bundles.get(key)
            if bundle:
                self._bundles[key] = bundle.model_copy(
                    update={"state": ClearanceBundleState.INVALID}
                )

    async def _get_or_build_bundle(
        self,
        *,
        provider: ProxyProvider,
        affinity_key: str,
        proxy_url: str,
        origin: str,
    ) -> ClearanceBundle | None:
        """按出口身份单飞构建 clearance bundle。"""
        if self._mode == ClearanceMode.NONE:
            return None
        host = clearance_host(origin)
        key: BundleKey = (provider.value, affinity_key, host)
        while True:
            async with self._lock:
                self._evict_superseded_affinity_locked(
                    provider=provider,
                    affinity_key=affinity_key,
                )
                bundle = self._bundles.get(key)
                if bundle and self._bundle_is_fresh(bundle):
                    self._bundles.move_to_end(key)
                    return bundle
                if bundle and bundle.state == ClearanceBundleState.VALID:
                    self._bundles[key] = bundle.model_copy(
                        update={"state": ClearanceBundleState.STALE}
                    )
                event = self._refresh_events.get(key)
                if event is None:
                    event = asyncio.Event()
                    self._refresh_events[key] = event
                    break
            await event.wait()

        try:
            if self._mode == ClearanceMode.MANUAL:
                bundle = self._manual.build_bundle(
                    affinity_key=affinity_key,
                    clearance_host=host,
                )
            else:
                bundle = await self._flare.refresh_bundle(
                    affinity_key=affinity_key,
                    proxy_url=proxy_url,
                    target_url=origin,
                )
            if bundle:
                async with self._lock:
                    self._bundles[key] = bundle
                    self._targets[key] = (proxy_url, origin)
                    self._bundles.move_to_end(key)
                    self._prune_locked()
            return bundle
        finally:
            async with self._lock:
                self._refresh_events.pop(key, None)
            event.set()

    @staticmethod
    def _bundle_is_fresh(bundle: ClearanceBundle) -> bool:
        """按 clearance 刷新周期判断缓存是否仍可直接复用。"""
        if bundle.state != ClearanceBundleState.VALID:
            return False
        refreshed_at = bundle.last_refresh_at
        if refreshed_at is None:
            return False
        ttl_ms = max(
            1,
            get_config().get_int("proxy.clearance.refresh_interval", 3600),
        ) * 1000
        return now_ms() - refreshed_at < ttl_ms

    def _prune_locked(self) -> None:
        """在锁内按 LRU 上限清理 clearance 和目标元数据。"""
        limit = max(
            1,
            get_config().get_int("proxy.clearance.max_cached_bundles", 2048),
        )
        while len(self._bundles) > limit:
            key, _ = self._bundles.popitem(last=False)
            self._targets.pop(key, None)

    def _evict_superseded_affinity_locked(
        self,
        *,
        provider: ProxyProvider,
        affinity_key: str,
    ) -> None:
        """淘汰同一托管节点旧 generation 或 runtime epoch 的 bundle。"""
        if provider != ProxyProvider.MANAGED_POOL:
            return
        parts = affinity_key.split(":", 2)
        if len(parts) < 3:
            return
        prefix = f"managed:{parts[1]}:"
        stale = [
            key
            for key in self._bundles
            if key[0] == provider.value
            and key[1].startswith(prefix)
            and key[1] != affinity_key
        ]
        for key in stale:
            self._bundles.pop(key, None)
            self._targets.pop(key, None)

    async def invalidate_clearance(self) -> None:
        """使全部缓存失效，下一次请求按需刷新。"""
        async with self._lock:
            self._bundles = OrderedDict(
                (
                    key,
                    bundle.model_copy(
                        update={"state": ClearanceBundleState.INVALID}
                    ),
                )
                for key, bundle in self._bundles.items()
            )

    async def warm_up(self) -> None:
        """统一池按账号延迟建 bundle，启动阶段仅完成配置加载。"""
        await self.load()

    async def refresh_clearance_safe(self) -> None:
        """刷新当前仍在 LRU 中的 bundle，失败时保留旧值。"""
        await self.load()
        if self._mode == ClearanceMode.NONE:
            return
        async with self._lock:
            targets = list(self._targets.items())
        for key, (proxy_url, origin) in targets:
            _, affinity_key, host = key
            if self._mode == ClearanceMode.MANUAL:
                bundle = self._manual.build_bundle(
                    affinity_key=affinity_key,
                    clearance_host=host,
                )
            else:
                bundle = await self._flare.refresh_bundle(
                    affinity_key=affinity_key,
                    proxy_url=proxy_url,
                    target_url=origin,
                )
            if bundle:
                async with self._lock:
                    if key in self._targets:
                        self._bundles[key] = bundle
                        self._bundles.move_to_end(key)
            else:
                logger.warning("clearance refresh failed, keeping old bundle")

    @property
    def bundles(self) -> dict[BundleKey, ClearanceBundle]:
        """返回只读 clearance 缓存快照。"""
        return dict(self._bundles)


__all__ = ["BundleKey", "ProxyClearanceManager", "clearance_host"]
