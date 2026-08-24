"""账号秘密的稳定脱敏身份。"""

import hashlib


def account_key_for_token(token: str) -> str:
    """去除可选 sso= 前缀后生成稳定 SHA-256 账号 key。"""
    normalized = token[4:] if token.startswith("sso=") else token
    return hashlib.sha256(normalized.encode("utf-8", "ignore")).hexdigest()


def account_log_key(token: str) -> str:
    """返回适合日志关联且不包含原 token 的短账号 key。"""
    return account_key_for_token(token)[:12]


__all__ = ["account_key_for_token", "account_log_key"]
