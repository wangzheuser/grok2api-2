"""统一出口的 Cloudflare clearance 刷新调度器。"""

import asyncio

from app.platform.logging.logger import logger
from app.platform.config.snapshot import get_config
from app.control.proxy.clearance import ProxyClearanceManager


class ProxyClearanceScheduler:
    """定期刷新统一代理服务维护的 clearance bundle。"""

    def __init__(self, manager: ProxyClearanceManager) -> None:
        """绑定唯一 clearance 管理器。"""
        self._manager = manager
        self._task: asyncio.Task | None = None
        self._running = False

    def start(self) -> None:
        """启动后台刷新循环。"""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info("proxy clearance scheduler started")

    def stop(self) -> None:
        """停止后台刷新循环。"""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
        logger.info("proxy clearance scheduler stopped")

    async def _loop(self) -> None:
        """按当前热配置循环刷新已使用的 bundle。"""
        # 启动时先加载配置，避免首个业务请求额外执行初始化。
        await self._warm_up()
        while self._running:
            try:
                interval = self._get_interval()
                await asyncio.sleep(interval)
                if not self._running:
                    break
                await self._refresh()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(
                    "proxy clearance scheduler loop failed: error_type={} error={}",
                    type(exc).__name__,
                    exc,
                )
                await asyncio.sleep(60)

    async def _warm_up(self) -> None:
        """加载 clearance 配置；bundle 仍按账号与出口延迟创建。"""
        try:
            await self._manager.load()
            await self._manager.warm_up()
            logger.debug("proxy clearance warm-up completed")
        except Exception as exc:
            logger.warning("proxy clearance warm-up failed: error={}", exc)

    async def _refresh(self) -> None:
        """构建新 bundle 后原子替换，刷新异常时保留现有值。"""
        try:
            await self._manager.load()
            await self._manager.refresh_clearance_safe()
            logger.debug("proxy clearance refresh completed")
        except Exception as exc:
            logger.warning("proxy clearance refresh failed: error={}", exc)

    def _get_interval(self) -> int:
        """返回热配置中的刷新间隔秒数。"""
        cfg = get_config()
        return cfg.get_int("proxy.clearance.refresh_interval", 600)


__all__ = ["ProxyClearanceScheduler"]
