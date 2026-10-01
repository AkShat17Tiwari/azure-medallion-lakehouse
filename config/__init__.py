"""Configuration package for the Lakehouse project."""
from config.settings import settings


def __getattr__(name: str):
    if name == "get_spark_session":
        from config.spark_session import get_spark_session
        return get_spark_session
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


__all__ = ["settings", "get_spark_session"]
