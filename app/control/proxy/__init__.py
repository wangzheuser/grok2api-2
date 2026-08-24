"""ProxyDirectory — control-plane proxy pool coordinator.

Maintains the list of EgressNodes and ClearanceBundles.
Selection delegates to the dataplane ProxyTable; this module owns
configuration loading and clearance refresh lifecycle.
"""

import asyncio
import random
from urllib.parse import urlparse

from app.platform.errors import UpstreamError
from app.platform.logging.logger import logger
from app.platform.config.snapshot import get_config
from app.platform.runtime.clock import now_ms
from app.platform.runtime.ids import next_hex
from .config import resolve_clearance_config
from .models import (
    EgressMode,
    EgressRotationStrategy,
    ClearanceMode,
    EgressNode,
    ClearanceBundle,
    ProxyLease,
    ProxyFeedback,
    ProxyFeedbackKind,
    RequestKind,
    ProxyScope,
)
from .providers.manual import ManualClearanceProvider
from .providers.flaresolverr import FlareSolverrClearanceProvider
from .validation import ProxyConfigIssue, validate_egress_config

_DEFAULT_CLEARANCE_ORIGIN = "https://grok.com"
BundleKey = tuple[str, str]


def _clearance_host(clearance_origin: str | None) -> str:
    host = urlparse(clearance_origin or _DEFAULT_CLEARANCE_ORIGIN).hostname
    return (host or "grok.com").lower()


class ProxyDirectory:
    """Owns egress nodes and clearance bundles.

    Thread-safety: all mutations are protected by ``_lock``.
    """

    def __init__(self) -> None:
        self._nodes: list[EgressNode] = []
        self._resource_nodes: list[EgressNode] = []  # for media downloads
        self._bundles: dict[BundleKey, ClearanceBundle] = {}
        self._lock = asyncio.Lock()
        # Single-flight guard: at most one FlareSolverr call per proxy+host key.
        # Other coroutines wait on the Event until the active refresh completes.
        self._refresh_events: dict[BundleKey, asyncio.Event] = {}
        self._manual = ManualClearanceProvider()
        self._flare = FlareSolverrClearanceProvider()
        self._egress_mode: EgressMode = EgressMode.DIRECT
        self._rotation_strategy = EgressRotationStrategy.STICKY_FAILOVER
        self._clearance_mode: ClearanceMode = ClearanceMode.NONE
        self._config_error: ProxyConfigIssue | None = None
        self._config_sig: tuple | None = None
        # Keep API and resource rotation independent so their traffic does not
        # advance each other's cursor.
        self._pool_cursors: dict[str, int] = {"global": 0, "global_resource": 0}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def load(self) -> None:
        """Load proxy configuration from the current config snapshot."""
        cfg = get_config()
        config_error: ProxyConfigIssue | None = None
        try:
            validated = validate_egress_config(
                {
                    "mode": cfg.get_str("proxy.egress.mode", "direct"),
                    "rotation_strategy": cfg.get_str(
                        "proxy.egress.rotation_strategy",
                        EgressRotationStrategy.STICKY_FAILOVER.value,
                    ),
                    "proxy_url": cfg.get_str("proxy.egress.proxy_url", ""),
                    "proxy_pool": cfg.get_list("proxy.egress.proxy_pool", []),
                    "resource_proxy_url": cfg.get_str(
                        "proxy.egress.resource_proxy_url", ""
                    ),
                    "resource_proxy_pool": cfg.get_list(
                        "proxy.egress.resource_proxy_pool", []
                    ),
                }
            )
        except ProxyConfigIssue as exc:
            # 保持管理后台可启动，但所有依赖全局选点的请求必须失败关闭。
            config_error = exc
            validated = None

        egress_mode = EgressMode(validated.mode) if validated else EgressMode.DIRECT
        rotation_strategy = (
            EgressRotationStrategy(validated.rotation_strategy)
            if validated
            else EgressRotationStrategy.STICKY_FAILOVER
        )
        clearance_mode = ClearanceMode.parse(
            cfg.get_str("proxy.clearance.mode", "none")
        )
        base_url = validated.proxy_url if validated else ""
        res_url = validated.resource_proxy_url if validated else ""
        base_pool = validated.proxy_pool if validated else ()
        res_pool = validated.resource_proxy_pool if validated else ()
        clearance = resolve_clearance_config(cfg)
        config_sig = (
            str(config_error or ""),
            egress_mode.value,
            rotation_strategy.value,
            clearance_mode.value,
            base_url,
            res_url,
            base_pool,
            res_pool,
            cfg.get_str("proxy.clearance.flaresolverr_url", ""),
            clearance.cf_cookies,
            clearance.user_agent,
            clearance.cf_clearance,
            clearance.browser,
            cfg.get_int("proxy.clearance.timeout_sec", 60),
        )

        nodes: list[EgressNode] = []
        resource_nodes: list[EgressNode] = []

        if egress_mode == EgressMode.SINGLE_PROXY:
            if base_url:
                nodes.append(EgressNode(node_id="single", proxy_url=base_url))
            if res_url:
                resource_nodes.append(
                    EgressNode(node_id="res-single", proxy_url=res_url)
                )

        elif egress_mode == EgressMode.PROXY_POOL:
            for i, url in enumerate(base_pool):
                nodes.append(EgressNode(node_id=f"pool-{i}", proxy_url=url))
            for i, url in enumerate(res_pool):
                resource_nodes.append(
                    EgressNode(node_id=f"res-pool-{i}", proxy_url=url)
                )

        valid_affinities = {n.proxy_url or "direct" for n in [*nodes, *resource_nodes]}
        if not valid_affinities:
            valid_affinities = {"direct"}

        async with self._lock:
            if self._config_sig == config_sig:
                return
            from .models import ClearanceBundleState

            self._egress_mode = egress_mode
            self._rotation_strategy = rotation_strategy
            self._clearance_mode = clearance_mode
            self._config_error = config_error
            self._nodes = nodes
            self._resource_nodes = resource_nodes
            self._pool_cursors = {"global": 0, "global_resource": 0}
            self._bundles = {
                key: bundle.model_copy(update={"state": ClearanceBundleState.INVALID})
                for key, bundle in self._bundles.items()
                if key[0] in valid_affinities
            }
            self._refresh_events = {
                key: event
                for key, event in self._refresh_events.items()
                if key[0] in valid_affinities
            }
            self._config_sig = config_sig

        logger.info(
            "proxy directory loaded: egress_mode={} clearance_mode={} node_count={} resource_node_count={} config_error={}",
            egress_mode,
            clearance_mode,
            len(nodes),
            len(resource_nodes),
            str(config_error or "-"),
        )

    # ------------------------------------------------------------------
    # Acquisition
    # ------------------------------------------------------------------

    async def acquire(
        self,
        *,
        scope: ProxyScope = ProxyScope.APP,
        kind: RequestKind = RequestKind.HTTP,
        resource: bool = False,
        clearance_origin: str | None = None,
        proxy_url_override: str | None = None,
    ) -> ProxyLease:
        """Return a ProxyLease for the next request.

        For DIRECT mode, returns a lease with no proxy or clearance.
        """
        proxy_pool = ""
        proxy_id = ""
        if proxy_url_override is not None:
            proxy_url = proxy_url_override
        else:
            if self._config_error is not None:
                raise UpstreamError(
                    f"Egress proxy configuration is invalid: {self._config_error}",
                    status=503,
                    code="egress_proxy_unavailable",
                )
            proxy_url, proxy_id, proxy_pool = await self._pick_proxy(resource=resource)
        affinity = proxy_url or "direct"
        clearance_host = _clearance_host(clearance_origin)

        bundle = await self._get_or_build_bundle(
            affinity_key=affinity,
            proxy_url=proxy_url or "",
            clearance_origin=clearance_origin or _DEFAULT_CLEARANCE_ORIGIN,
        )

        return ProxyLease(
            lease_id=next_hex(),
            proxy_url=proxy_url,
            cf_cookies=bundle.cf_cookies if bundle else "",
            user_agent=bundle.user_agent if bundle else "",
            clearance_host=clearance_host,
            scope=scope,
            kind=kind,
            acquired_at=now_ms(),
            proxy_pool=proxy_pool,
            proxy_id=proxy_id,
        )

    async def feedback(self, lease: ProxyLease, result: ProxyFeedback) -> None:
        """Apply upstream feedback to the appropriate egress node."""
        if result.kind in (
            ProxyFeedbackKind.CHALLENGE,
            ProxyFeedbackKind.UNAUTHORIZED,
        ):
            # Invalidate associated clearance bundle.
            key = (lease.proxy_url or "direct", lease.clearance_host)
            async with self._lock:
                from .models import ClearanceBundleState

                bundle = self._bundles.get(key)
                if bundle:
                    self._bundles[key] = bundle.model_copy(
                        update={"state": ClearanceBundleState.INVALID}
                    )

        # In PROXY_POOL mode, rotate to the next node on any failure so the
        # next acquire() prefers a different egress rather than hammering the
        # same broken node.
        if (
            self._egress_mode == EgressMode.PROXY_POOL
            and self._rotation_strategy == EgressRotationStrategy.STICKY_FAILOVER
            and lease.proxy_pool in {"global", "global_resource"}
            and lease.proxy_url
            and result.kind
            in (
                ProxyFeedbackKind.CHALLENGE,
                ProxyFeedbackKind.UNAUTHORIZED,
                ProxyFeedbackKind.FORBIDDEN,
                ProxyFeedbackKind.TRANSPORT_ERROR,
            )
        ):
            async with self._lock:
                nodes = (
                    self._resource_nodes
                    if lease.proxy_pool == "global_resource"
                    else self._nodes
                )
                cursor = self._pool_cursors[lease.proxy_pool]
                if (
                    not nodes
                    or nodes[cursor % len(nodes)].proxy_url != lease.proxy_url
                ):
                    return
                self._pool_cursors[lease.proxy_pool] = cursor + 1
                logger.debug(
                    "proxy pool cursor advanced: pool={} proxy={} kind={} cursor={}",
                    lease.proxy_pool,
                    lease.proxy_url,
                    result.kind,
                    cursor + 1,
                )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _pick_proxy_url(self, resource: bool = False) -> str | None:
        """Compatibility wrapper for callers that only need the proxy URL."""
        proxy_url, _, _ = await self._pick_proxy(resource=resource)
        return proxy_url

    async def _pick_proxy(
        self,
        *,
        resource: bool = False,
    ) -> tuple[str | None, str, str]:
        """Select a proxy and return its URL, node ID, and pool identifier."""
        if self._egress_mode == EgressMode.DIRECT:
            return None, "", ""
        async with self._lock:
            # Prefer resource-specific nodes when available; fall back to base nodes.
            use_resource_pool = resource and bool(self._resource_nodes)
            nodes = self._resource_nodes if use_resource_pool else self._nodes
            if not nodes:
                raise UpstreamError(
                    "No configured egress proxy is available",
                    status=503,
                    code="egress_proxy_unavailable",
                )
            pool_name = "global_resource" if use_resource_pool else "global"
            if self._egress_mode == EgressMode.SINGLE_PROXY:
                node = nodes[0]
                return node.proxy_url, node.node_id, pool_name

            if self._rotation_strategy == EgressRotationStrategy.RANDOM:
                node = random.choice(nodes)
            else:
                cursor = self._pool_cursors[pool_name]
                node = nodes[cursor % len(nodes)]
                if self._rotation_strategy == EgressRotationStrategy.ROUND_ROBIN:
                    self._pool_cursors[pool_name] = cursor + 1
            return node.proxy_url, node.node_id, pool_name

    async def _get_or_build_bundle(
        self,
        *,
        affinity_key: str,
        proxy_url: str,
        clearance_origin: str,
    ) -> ClearanceBundle | None:
        if self._clearance_mode == ClearanceMode.NONE:
            return None
        clearance_host = _clearance_host(clearance_origin)
        key: BundleKey = (affinity_key, clearance_host)

        # Single-flight: only one coroutine fetches clearance per proxy+host key.
        # Concurrent callers wait on the Event and retry once it fires.
        while True:
            async with self._lock:
                bundle = self._bundles.get(key)
                if bundle and bundle.state.value == 0:  # VALID
                    return bundle
                event = self._refresh_events.get(key)
                if event is None:
                    # This coroutine wins the right to refresh.
                    event = asyncio.Event()
                    self._refresh_events[key] = event
                    break
            # Another coroutine is already refreshing — wait for it, then retry.
            await event.wait()

        try:
            if self._clearance_mode == ClearanceMode.MANUAL:
                bundle = self._manual.build_bundle(
                    affinity_key=affinity_key,
                    clearance_host=clearance_host,
                )
            else:
                bundle = await self._flare.refresh_bundle(
                    affinity_key=affinity_key,
                    proxy_url=proxy_url,
                    target_url=clearance_origin,
                )
            if bundle:
                async with self._lock:
                    self._bundles[key] = bundle
            return bundle
        finally:
            async with self._lock:
                self._refresh_events.pop(key, None)
            event.set()  # Wake all waiters so they retry with the new bundle.

    # ------------------------------------------------------------------
    # Clearance lifecycle helpers (used by ProxyClearanceScheduler)
    # ------------------------------------------------------------------

    async def invalidate_clearance(self) -> None:
        """Mark all cached clearance bundles as invalid.

        The next ``acquire()`` call for each affinity key will trigger a fresh
        FlareSolverr fetch (serialised by the single-flight guard).
        """
        from .models import ClearanceBundleState

        async with self._lock:
            self._bundles = {
                k: b.model_copy(update={"state": ClearanceBundleState.INVALID})
                for k, b in self._bundles.items()
            }
        logger.debug("clearance bundles invalidated: count={}", len(self._bundles))

    async def warm_up(self) -> None:
        """Pre-fetch clearance bundles for all configured affinity keys.

        Called once at startup so the first real request does not have to wait
        for FlareSolverr.  Does NOT invalidate existing bundles first.
        """
        if self._clearance_mode == ClearanceMode.NONE:
            return
        async with self._lock:
            nodes = list(self._nodes) + list(self._resource_nodes)
        # Warm each unique egress once, including resource-only nodes.
        proxy_urls = list(dict.fromkeys(n.proxy_url or "" for n in nodes))
        affinity_keys = (
            [(proxy_url or "direct", proxy_url) for proxy_url in proxy_urls]
            if proxy_urls
            else [("direct", "")]
        )
        for affinity, proxy_url in affinity_keys:
            await self._get_or_build_bundle(
                affinity_key=affinity,
                proxy_url=proxy_url,
                clearance_origin=_DEFAULT_CLEARANCE_ORIGIN,
            )

    async def refresh_clearance_safe(self) -> None:
        """Scheduled clearance refresh: build new bundles then swap atomically.

        Unlike ``invalidate_clearance() + warm_up()``, this never discards a
        working bundle before a replacement is ready.  If FlareSolverr is
        temporarily unavailable the old bundle remains valid and continues to
        serve requests.
        """
        if self._clearance_mode == ClearanceMode.NONE:
            return
        async with self._lock:
            nodes = list(self._nodes) + list(self._resource_nodes)
            existing = list(self._bundles.keys())

        refresh_targets: dict[BundleKey, tuple[str, str]] = {}
        proxy_urls = list(dict.fromkeys(n.proxy_url or "" for n in nodes))
        default_items = (
            [(proxy_url or "direct", proxy_url) for proxy_url in proxy_urls]
            if proxy_urls
            else [("direct", "")]
        )
        for affinity, proxy_url in default_items:
            key: BundleKey = (affinity, _clearance_host(_DEFAULT_CLEARANCE_ORIGIN))
            refresh_targets[key] = (proxy_url, _DEFAULT_CLEARANCE_ORIGIN)
        for key in existing:
            affinity, clearance_host = key
            refresh_targets.setdefault(
                key,
                ("" if affinity == "direct" else affinity, f"https://{clearance_host}"),
            )

        for key, (proxy_url, clearance_origin) in refresh_targets.items():
            affinity, clearance_host = key
            if self._clearance_mode == ClearanceMode.MANUAL:
                new_bundle = self._manual.build_bundle(
                    affinity_key=affinity,
                    clearance_host=clearance_host,
                )
            else:
                new_bundle = await self._flare.refresh_bundle(
                    affinity_key=affinity,
                    proxy_url=proxy_url,
                    target_url=clearance_origin,
                )
            if new_bundle:
                async with self._lock:
                    self._bundles[key] = new_bundle
                logger.debug("clearance bundle refreshed: bundle={}", key)
            else:
                logger.warning(
                    "clearance refresh failed, keeping old bundle: bundle={}",
                    key,
                )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def egress_mode(self) -> EgressMode:
        return self._egress_mode

    @property
    def clearance_mode(self) -> ClearanceMode:
        return self._clearance_mode

    @property
    def node_count(self) -> int:
        return len(self._nodes)

    @property
    def nodes(self) -> list[EgressNode]:
        """Read-only snapshot of the current egress node list."""
        return list(self._nodes)

    @property
    def bundles(self) -> dict[BundleKey, ClearanceBundle]:
        """Read-only snapshot of the current clearance bundles."""
        return dict(self._bundles)


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_directory: ProxyDirectory | None = None


async def get_proxy_directory() -> ProxyDirectory:
    """Return the module-level ProxyDirectory, reloading config if it changed."""
    global _directory
    if _directory is None:
        _directory = ProxyDirectory()
    await _directory.load()
    return _directory


__all__ = ["ProxyDirectory", "get_proxy_directory"]
