from __future__ import annotations

from dataclasses import dataclass, field, fields
import os
from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv()

ENV_MAPPING = {
    "account_name": "AZURE_STORAGE_ACCOUNT_NAME",
    "access_key": "AZURE_STORAGE_ACCESS_KEY",
    "local_path": "LOCAL_PATH",
    "landing_container": "LANDING_CONTAINER",
    "bronze_container": "BRONZE_CONTAINER",
    "silver_container": "SILVER_CONTAINER",
    "gold_container": "GOLD_CONTAINER",
    "endpoint": "ENDPOINT",
    "root": "ROOT",
    "connection_string": "AZURE_CONNECTION_STRING",
    "simple_patterns": "SIMPLE_PATTERNS",
}


def read_env(env_name: str) -> str | None:
    value = os.getenv(env_name)

    if value is None:
        return None

    normalized_value = value.strip()
    return normalized_value or None


@dataclass(slots=True)
class Settings:
    account_name: str | None = field(default_factory=lambda: read_env("AZURE_STORAGE_ACCOUNT_NAME"))
    access_key: str | None = field(default_factory=lambda: read_env("AZURE_STORAGE_ACCESS_KEY"))
    local_path: str | None = field(default_factory=lambda: read_env("LOCAL_PATH"))
    landing_container: str | None = field(default_factory=lambda: read_env("LANDING_CONTAINER"))
    bronze_container: str | None = field(default_factory=lambda: read_env("BRONZE_CONTAINER"))
    silver_container: str | None = field(default_factory=lambda: read_env("SILVER_CONTAINER"))
    gold_container: str | None = field(default_factory=lambda: read_env("GOLD_CONTAINER"))
    endpoint: str | None = field(default_factory=lambda: read_env("ENDPOINT"))
    root: str | None = field(default_factory=lambda: read_env("ROOT"))
    connection_string: str | None = field(default_factory=lambda: read_env("AZURE_CONNECTION_STRING"))
    simple_patterns: str | None = field(default_factory=lambda: read_env("SIMPLE_PATTERNS"))

    def __post_init__(self) -> None:
        for config_field in fields(self):
            value = getattr(self, config_field.name)

            if isinstance(value, str):
                normalized_value = value.strip() or None
                setattr(self, config_field.name, normalized_value)

        self._validate_endpoint_format()

    def _validate_endpoint_format(self) -> None:
        if not self.endpoint:
            return

        parsed_url = urlparse(self.endpoint)

        if not parsed_url.scheme or not parsed_url.netloc:
            raise ValueError(
                "ENDPOINT must be a valid URL, for example "
                "https://<account>.dfs.core.windows.net"
            )

    def get_required(self, name: str) -> str:
        if not hasattr(self, name):
            raise AttributeError(f"Unknown settings field: {name}")

        value = getattr(self, name)

        if value is None:
            env_name = ENV_MAPPING.get(name, name.upper())
            raise ValueError(f"Required environment variable is not set: {env_name}")

        return value

    def validate_required(self, *names: str) -> None:
        for name in names:
            self.get_required(name)

    def as_dict(self) -> dict[str, str | None]:
        return {
            config_field.name: getattr(self, config_field.name)
            for config_field in fields(self)
        }

settings = Settings()