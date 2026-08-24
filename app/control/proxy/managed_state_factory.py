"""托管代理共享状态仓储工厂。"""

from __future__ import annotations

import os
from pathlib import Path

from app.control.account.backends.factory import get_repository_backend
from app.platform.paths import data_path

from .managed_state import ManagedProxyStateRepository


def create_managed_proxy_state_repository() -> ManagedProxyStateRepository:
    """创建与 ACCOUNT_STORAGE 一致的托管代理状态仓储。"""
    backend = get_repository_backend()
    if backend == "local":
        from .managed_state_local import LocalManagedProxyStateRepository

        path_str = os.getenv(
            "ACCOUNT_LOCAL_PATH",
            str(data_path("accounts.db")),
        ).strip()
        path = Path(path_str)
        if not path.is_absolute():
            path = Path(__file__).resolve().parents[3] / path
        return LocalManagedProxyStateRepository(path)

    if backend == "redis":
        from redis.asyncio import Redis

        from .managed_state_redis import RedisManagedProxyStateRepository

        url = os.getenv("ACCOUNT_REDIS_URL", "").strip()
        if not url:
            raise ValueError("Redis managed proxy state requires ACCOUNT_REDIS_URL")
        return RedisManagedProxyStateRepository(
            Redis.from_url(url, decode_responses=False)
        )

    from app.control.account.backends.sql import (
        create_mysql_engine,
        create_pgsql_engine,
    )

    from .managed_state_sql import SqlManagedProxyStateRepository

    if backend == "mysql":
        url = os.getenv("ACCOUNT_MYSQL_URL", "").strip()
        if not url:
            raise ValueError("MySQL managed proxy state requires ACCOUNT_MYSQL_URL")
        engine = create_mysql_engine(url)
    else:
        url = os.getenv("ACCOUNT_POSTGRESQL_URL", "").strip()
        if not url:
            raise ValueError(
                "PostgreSQL managed proxy state requires ACCOUNT_POSTGRESQL_URL"
            )
        engine = create_pgsql_engine(url)
    return SqlManagedProxyStateRepository(
        engine,
        dialect=backend,
        dispose_engine=False,
    )


__all__ = ["create_managed_proxy_state_repository"]
